//! End-to-end CLI integration tests for `vela-ctl` (task 10.4).
//!
//! These drive the public command surface against in-process fake `VelaClient`
//! gRPC servers (the harness pattern established by `cli.rs`'s `example_tests`,
//! `consume.rs`'s edge-case tests, and `admin.rs`'s routing tests):
//!
//! - **Admin** — the four operator commands (`create`, `delete`, `list`,
//!   `describe`) are driven through [`vela_ctl::cli::run`], asserting the
//!   [`CtlError`]-or-`Ok` outcome that determines the process exit status. Two
//!   of them (`create`/`delete`) are additionally driven against a `NotLeader`
//!   redirect: a first node returns a `NotLeader` hint pointing at a second
//!   node, which then serves the request. The two nodes are registered as
//!   `id=url` endpoints so the hinted leader id resolves to a dialable address
//!   (Requirement 4.1, 13.1–13.6).
//! - **Produce** — the backward-compatible one-shot `produce --value V` is
//!   driven through [`run`] against a fake whose `produce` records the value and
//!   returns an offset (Requirement 6.2). The interactive REPL over real stdin
//!   is covered by `produce.rs`'s unit tests and `tests/prop_produce_repl.rs`,
//!   so it is not re-driven here.
//! - **Consume** — the continuous, multi-partition consumer is exercised at the
//!   [`vela_ctl::consume::run_consume`] seam rather than through [`run`]. The
//!   `consume` path [`run`] wires up uses the production seams (a real `CtrlC`
//!   signal and the wall-clock `TokioClock`) and an unbounded poll loop that
//!   blocks until Ctrl+C, so driving it through [`run`] to completion is
//!   impractical in a test. `run_consume` is the public, dependency-injected
//!   seam (a controllable [`Clock`] and triggerable [`Signal`]), so the
//!   multi-partition late-append / eventual-delivery / interrupt behavior is
//!   driven there: a fake serves scripted per-partition batches, late records
//!   are appended after the loop drains the initial log, and a triggered signal
//!   stops the session (Requirement 9.3, 9.6, 11.1). The Producer REPL is
//!   likewise exercised via the `run_repl` seam in its own tests for the same
//!   reason (real stdin/signals block).
//!
//! Each fake binds an OS-chosen localhost port and is served on a background
//! task before its endpoint is returned, so the CLI never races server startup.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use vela_client::{KeylessStrategy, LogBackend, VelaClient};
use vela_ctl::cli::{run, Cli, Command, CtlError};
use vela_ctl::consume::{run_consume, OffsetReset};
use vela_ctl::seams::{Clock, Signal};
use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
use vela_proto::v1::{
    ConsumeRequest, ConsumeResponse, ConsumedRecord, CreateTopicRequest, CreateTopicResponse,
    DeleteTopicRequest, DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse,
    DescribeTopicRequest, DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse,
    ListTopicsRequest, ListTopicsResponse, PartitionInfo, ProduceRequest, ProduceResponse, Record,
    TopicInfo,
};

// ---------------------------------------------------------------------------
// Fake `VelaClient` node
// ---------------------------------------------------------------------------

/// How a fake node answers a topic-mutating admin RPC (`create`/`delete`).
#[derive(Clone)]
enum Mutating {
    /// Serve the request successfully — a node that owns (or forwards to) the
    /// metadata leader.
    Succeed,
    /// Reject with a `NotLeader` redirect carrying the hinted leader node id,
    /// shaped exactly as the server emits it (a typed `VelaError` in the status
    /// details).
    NotLeader(String),
    /// Reject with a non-retryable application error, standing in for a cluster
    /// that actively rejects the request (Requirement 13.7).
    Reject,
}

/// One produce request a fake node received, captured verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Produced {
    partition: u32,
    key: Option<Vec<u8>>,
    value: Vec<u8>,
}

/// Per-node admin call counters, so a test can assert which node served a
/// redirected request.
#[derive(Default)]
struct Counts {
    create: AtomicU32,
    delete: AtomicU32,
}

/// An in-process fake of the client-facing `VelaClient` service for one node.
///
/// A single configurable fake backs every scenario in this file: it answers the
/// admin RPCs per [`Mutating`], advertises `partition_count` partitions (each led
/// by `leader_id`) through `describe_topic`, serves scripted per-partition
/// committed logs through `consume`, and records every `produce` it receives.
/// Cloning shares the same state (the counters, the mutable log, and the produce
/// capture live behind `Arc`), so a test holds a clone to assert against while
/// the server task owns another.
#[derive(Clone)]
struct FakeNode {
    mutating: Mutating,
    leader_id: String,
    partition_count: u32,
    /// `logs[p][i]` is partition `p`'s committed record value at offset `i`.
    logs: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
    produced: Arc<Mutex<Vec<Produced>>>,
    counts: Arc<Counts>,
}

impl FakeNode {
    /// A node serving the admin plane: `mutating` decides how `create`/`delete`
    /// respond; `list`/`describe` always succeed with a canned two-partition
    /// topic.
    fn admin(mutating: Mutating) -> Self {
        Self {
            mutating,
            leader_id: "node-leader".to_string(),
            partition_count: 2,
            logs: Arc::new(Mutex::new(Vec::new())),
            produced: Arc::new(Mutex::new(Vec::new())),
            counts: Arc::new(Counts::default()),
        }
    }

    /// A node serving the data plane: `find_leader`/`describe_topic` name
    /// `leader_id` as the leader of every one of `partition_count` partitions,
    /// `consume` serves `logs`, and `produce` records each request.
    fn data(leader_id: &str, partition_count: u32, logs: Vec<Vec<Vec<u8>>>) -> Self {
        Self {
            mutating: Mutating::Succeed,
            leader_id: leader_id.to_string(),
            partition_count,
            logs: Arc::new(Mutex::new(logs)),
            produced: Arc::new(Mutex::new(Vec::new())),
            counts: Arc::new(Counts::default()),
        }
    }

    fn create_calls(&self) -> u32 {
        self.counts.create.load(Ordering::SeqCst)
    }

    fn delete_calls(&self) -> u32 {
        self.counts.delete.load(Ordering::SeqCst)
    }

    /// The produce requests this node received, in arrival order.
    fn produced(&self) -> Vec<Produced> {
        self.produced
            .lock()
            .expect("produced mutex poisoned")
            .clone()
    }

    /// A canned topic with `partition_count` partitions, each led by `leader_id`
    /// — enough for `describe`/discovery to format and route.
    fn topic(&self, name: &str) -> TopicInfo {
        let partitions = (0..self.partition_count)
            .map(|index| PartitionInfo {
                index,
                replicas: vec![self.leader_id.clone()],
                leader: Some(self.leader_id.clone()),
            })
            .collect();
        TopicInfo {
            name: name.to_string(),
            partition_count: self.partition_count,
            partitions,
            log_backend: LogBackend::Durable.to_wire(),
        }
    }

    /// Decide how a mutating admin RPC responds: `Ok(())` means build a success
    /// response, `Err` means reject with the scripted `NotLeader` redirect.
    #[allow(clippy::result_large_err)]
    fn mutating_outcome(&self) -> Result<(), Status> {
        match &self.mutating {
            Mutating::Succeed => Ok(()),
            Mutating::NotLeader(hint) => Err(not_leader_status(hint)),
            // A non-`Unavailable` status is classified as a non-retryable
            // application error, so dispatch surfaces it without retrying.
            Mutating::Reject => Err(Status::not_found("no such topic")),
        }
    }
}

#[tonic::async_trait]
impl VelaClientService for FakeNode {
    async fn create_topic(
        &self,
        request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        self.counts.create.fetch_add(1, Ordering::SeqCst);
        self.mutating_outcome()?;
        let request = request.into_inner();
        let mut topic = self.topic(&request.name);
        // Echo the requested backend so the printed description reflects it.
        topic.log_backend = request.log_backend;
        Ok(Response::new(CreateTopicResponse { topic: Some(topic) }))
    }

    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> Result<Response<DeleteTopicResponse>, Status> {
        self.counts.delete.fetch_add(1, Ordering::SeqCst);
        self.mutating_outcome()?;
        Ok(Response::new(DeleteTopicResponse {}))
    }

    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        Ok(Response::new(ListTopicsResponse {
            topics: vec![self.topic("orders"), self.topic("events")],
        }))
    }

    async fn describe_topic(
        &self,
        request: Request<DescribeTopicRequest>,
    ) -> Result<Response<DescribeTopicResponse>, Status> {
        let name = request.into_inner().name;
        Ok(Response::new(DescribeTopicResponse {
            topic: Some(self.topic(&name)),
        }))
    }

    async fn find_leader(
        &self,
        _request: Request<FindLeaderRequest>,
    ) -> Result<Response<FindLeaderResponse>, Status> {
        Ok(Response::new(FindLeaderResponse {
            leader: Some(self.leader_id.clone()),
        }))
    }

    async fn describe_cluster(
        &self,
        _request: Request<DescribeClusterRequest>,
    ) -> Result<Response<DescribeClusterResponse>, Status> {
        // No member addresses: the client falls back to its `id=url` registry,
        // which already maps each node id to its address (Requirement 13.3).
        Ok(Response::new(DescribeClusterResponse {
            members: vec![],
            epoch: 0,
        }))
    }

    async fn produce(
        &self,
        request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let request = request.into_inner();
        let record = request.record.unwrap_or_default();
        let mut produced = self.produced.lock().expect("produced mutex poisoned");
        // The assigned offset is the count of prior records on this partition.
        let offset = produced
            .iter()
            .filter(|p| p.partition == request.partition)
            .count() as u64;
        produced.push(Produced {
            partition: request.partition,
            key: record.key,
            value: record.value,
        });
        Ok(Response::new(ProduceResponse { offset }))
    }

    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        let request = request.into_inner();
        let logs = self.logs.lock().expect("logs mutex poisoned");
        let Some(log) = logs.get(request.partition as usize) else {
            // A partition outside the current topic is served as an empty poll.
            return Ok(Response::new(ConsumeResponse {
                records: vec![],
                next_offset: request.offset,
            }));
        };
        let start = request.offset as usize;
        // Serve every committed record from the requested offset to the end of
        // the log; `next_offset` is just past the last record returned, matching
        // the server's `request.offset + records_returned` contract so the loop's
        // `Next_Offset` advances correctly.
        let records: Vec<ConsumedRecord> = log
            .iter()
            .enumerate()
            .skip(start)
            .map(|(i, value)| ConsumedRecord {
                offset: i as u64,
                record: Some(Record {
                    key: None,
                    value: value.clone(),
                }),
            })
            .collect();
        let next_offset = request.offset + records.len() as u64;
        Ok(Response::new(ConsumeResponse {
            records,
            next_offset,
        }))
    }
}

/// Hand-encode a `VelaError { code: NOT_LEADER, leader: hint }` into protobuf
/// wire bytes, matching what the server puts in a `NotLeader` status's details
/// (and what the client's `not_leader_hint` decodes).
///
/// `vela-ctl`'s test crate intentionally takes no `prost` dependency, so the
/// message is encoded by hand. The wire format is small and stable: field 1
/// (`code`) is a varint, field 3 (`leader`) is a length-delimited string; the
/// human-readable `message` (field 2) is omitted, since it is optional on the
/// wire and the client ignores it.
fn not_leader_details(hint: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    // Field 1 (`code`), wire type 0 (varint): tag `(1 << 3) | 0 == 0x08`.
    bytes.push(0x08);
    encode_varint(
        vela_proto::v1::ErrorCode::NotLeader as i32 as u64,
        &mut bytes,
    );
    // Field 3 (`leader`), wire type 2 (length-delimited): tag `(3 << 3) | 2 == 0x1a`.
    bytes.push(0x1a);
    encode_varint(hint.len() as u64, &mut bytes);
    bytes.extend_from_slice(hint.as_bytes());
    bytes
}

/// Append `value` to `out` as a base-128 protobuf varint.
fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// A `NotLeader` redirect status shaped exactly as the server emits it: a typed
/// `VelaError` (code `NotLeader`, leader hint) encoded into the status details,
/// so the client's `classify`/`not_leader_hint` decode it identically.
///
/// `with_details` takes a `bytes::Bytes`; the encoded `Vec<u8>` is converted
/// with `.into()` (whose target is fixed by the parameter type), so the test
/// never has to name the `bytes` crate it does not depend on directly.
fn not_leader_status(hint: &str) -> Status {
    Status::with_details(
        tonic::Code::FailedPrecondition,
        "not leader",
        not_leader_details(hint).into(),
    )
}

/// Bind a fake node on an OS-chosen localhost port and serve it on a background
/// task. The listener is bound before returning, so the endpoint is already
/// accepting connections — no startup race.
async fn serve(node: FakeNode) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    let service = VelaClientServer::new(node);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("fake server serves");
    });
    format!("http://127.0.0.1:{port}")
}

/// Build a [`Cli`] over the given `id=url` endpoints with the default TTL.
fn cli(endpoints: Vec<String>, command: Command) -> Cli {
    Cli {
        endpoints,
        metadata_ttl: Duration::from_secs(30),
        command,
    }
}

// ---------------------------------------------------------------------------
// Admin commands through `run`
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn create_succeeds_through_run() {
    // Requirement 13.1 (create) + 13.5 (success → Ok → exit 0).
    let node = FakeNode::admin(Mutating::Succeed);
    let endpoint = serve(node.clone()).await;
    let result = run(cli(
        vec![format!("node-leader={endpoint}")],
        Command::Create {
            name: "orders".to_string(),
            partitions: 4,
            backend: LogBackend::Durable,
        },
    ))
    .await;
    assert!(
        matches!(result, Ok(())),
        "create should succeed: {result:?}"
    );
    assert_eq!(node.create_calls(), 1, "the create reached the node once");
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_succeeds_through_run() {
    // Requirement 13.2 (delete) + 13.5.
    let node = FakeNode::admin(Mutating::Succeed);
    let endpoint = serve(node.clone()).await;
    let result = run(cli(
        vec![format!("node-leader={endpoint}")],
        Command::Delete {
            name: "orders".to_string(),
        },
    ))
    .await;
    assert!(
        matches!(result, Ok(())),
        "delete should succeed: {result:?}"
    );
    assert_eq!(node.delete_calls(), 1, "the delete reached the node once");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_succeeds_through_run() {
    // Requirement 13.3 (list) + 13.5. The fake returns two topics, so the
    // non-empty list-formatting branch runs.
    let endpoint = serve(FakeNode::admin(Mutating::Succeed)).await;
    let result = run(cli(vec![format!("node-leader={endpoint}")], Command::List)).await;
    assert!(matches!(result, Ok(())), "list should succeed: {result:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_succeeds_through_run() {
    // Requirement 13.4 (describe) + 13.5. The canned topic has a led partition,
    // so the leader-formatting branch runs.
    let endpoint = serve(FakeNode::admin(Mutating::Succeed)).await;
    let result = run(cli(
        vec![format!("node-leader={endpoint}")],
        Command::Describe {
            name: "orders".to_string(),
        },
    ))
    .await;
    assert!(
        matches!(result, Ok(())),
        "describe should succeed: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn create_redirects_on_not_leader_then_succeeds() {
    // Requirement 4.1 + 13.1–13.3: a `create` that lands on a non-leader is
    // redirected to the hinted metadata leader, dialed by its `id=url` address,
    // which then serves it.
    let node_a = FakeNode::admin(Mutating::NotLeader("node-b".to_string()));
    let node_b = FakeNode::admin(Mutating::Succeed);
    let a_addr = serve(node_a.clone()).await;
    let b_addr = serve(node_b.clone()).await;

    let result = run(cli(
        vec![format!("node-a={a_addr}"), format!("node-b={b_addr}")],
        Command::Create {
            name: "orders".to_string(),
            partitions: 1,
            backend: LogBackend::Durable,
        },
    ))
    .await;

    assert!(
        matches!(result, Ok(())),
        "the redirect is followed and the metadata leader serves the create: {result:?}",
    );
    assert_eq!(node_a.create_calls(), 1, "the non-leader was tried once");
    assert_eq!(
        node_b.create_calls(),
        1,
        "the create was redirected to and served by the hinted metadata leader",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_redirects_on_not_leader_then_succeeds() {
    // Requirement 4.1 + 13.1–13.3: same redirect for `delete`.
    let node_a = FakeNode::admin(Mutating::NotLeader("node-b".to_string()));
    let node_b = FakeNode::admin(Mutating::Succeed);
    let a_addr = serve(node_a.clone()).await;
    let b_addr = serve(node_b.clone()).await;

    let result = run(cli(
        vec![format!("node-a={a_addr}"), format!("node-b={b_addr}")],
        Command::Delete {
            name: "orders".to_string(),
        },
    ))
    .await;

    assert!(
        matches!(result, Ok(())),
        "the redirect is followed and the metadata leader serves the delete: {result:?}",
    );
    assert_eq!(node_a.delete_calls(), 1, "the non-leader was tried once");
    assert_eq!(
        node_b.delete_calls(),
        1,
        "the delete was redirected to and served by the hinted metadata leader",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_rejection_exits_non_zero() {
    // Requirement 13.7: an error the cluster returns is a cluster error (which
    // `report` maps to a non-zero exit), distinct from a connection failure.
    let node = FakeNode::admin(Mutating::Reject);
    let endpoint = serve(node).await;
    let result = run(cli(
        vec![format!("node-a={endpoint}")],
        Command::Delete {
            name: "missing".to_string(),
        },
    ))
    .await;
    assert!(
        matches!(result, Err(CtlError::Cluster(_))),
        "a cluster-returned error should surface as a cluster error: {result:?}",
    );
}

// ---------------------------------------------------------------------------
// One-shot produce through `run`
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn produce_one_shot_through_run_records_the_value() {
    // Requirement 6.2: `produce --value V` produces a single record and exits;
    // the record's value reaches the cluster and the command returns Ok.
    let node = FakeNode::data("node-leader", 1, vec![Vec::new()]);
    let endpoint = serve(node.clone()).await;

    let result = run(cli(
        vec![format!("node-leader={endpoint}")],
        Command::Produce {
            name: "orders".to_string(),
            key: Some("user-1".to_string()),
            value: Some("hello".to_string()),
            keyless: KeylessStrategy::RoundRobin,
        },
    ))
    .await;

    assert!(
        matches!(result, Ok(())),
        "a one-shot produce should succeed: {result:?}"
    );
    let produced = node.produced();
    assert_eq!(produced.len(), 1, "exactly one record was produced");
    assert_eq!(produced[0].value, b"hello", "the value reached the cluster");
    assert_eq!(
        produced[0].key.as_deref(),
        Some(b"user-1".as_slice()),
        "the key reached the cluster",
    );
}

// ---------------------------------------------------------------------------
// Multi-partition consume with late appends through `run_consume`
// ---------------------------------------------------------------------------

/// An instant virtual [`Clock`]: `sleep` advances virtual time and yields rather
/// than blocking, so the loop's empty-poll `Polling_Interval` costs no
/// wall-clock time. `now` reports the accumulated virtual time so any elapsed
/// comparison still progresses (mirrors the consume property tests).
struct InstantClock {
    base: Instant,
    elapsed: Mutex<Duration>,
}

impl InstantClock {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            elapsed: Mutex::new(Duration::ZERO),
        }
    }
}

#[tonic::async_trait]
impl Clock for InstantClock {
    fn now(&self) -> Instant {
        self.base + *self.elapsed.lock().expect("virtual clock mutex poisoned")
    }

    async fn sleep(&self, dur: Duration) {
        *self.elapsed.lock().expect("virtual clock mutex poisoned") += dur;
        tokio::task::yield_now().await;
    }
}

/// A triggerable [`Signal`] whose `interrupted` future resolves once
/// [`Notify::notify_one`] has been called, used to bound the consume session.
#[derive(Clone, Default)]
struct TriggerSignal {
    notify: Arc<Notify>,
}

#[tonic::async_trait]
impl Signal for TriggerSignal {
    async fn interrupted(&self) {
        self.notify.notified().await;
    }
}

/// An in-memory [`Write`] sink shared with the test, so the printed output can
/// be inspected while the loop runs (to decide when to interrupt) and after.
#[derive(Clone)]
struct SharedSink(Arc<Mutex<Vec<u8>>>);

impl Write for SharedSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("sink mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The completed output lines printed so far.
fn lines(buf: &Arc<Mutex<Vec<u8>>>) -> Vec<String> {
    String::from_utf8(buf.lock().expect("sink mutex poisoned").clone())
        .expect("utf8 output")
        .lines()
        .map(str::to_string)
        .collect()
}

/// Wait until at least `target` lines have been printed, or a generous deadline
/// elapses (after which the caller proceeds and its assertions surface the
/// shortfall). Real short sleeps; the loop itself runs on the virtual clock.
async fn wait_for_lines(buf: &Arc<Mutex<Vec<u8>>>, target: usize) {
    for _ in 0..3000 {
        if lines(buf).len() >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Parse one printed line `partition {p} offset {o} value {v}` into its parts.
fn parse_line(line: &str) -> (u32, u64, String) {
    let rest = line.strip_prefix("partition ").expect("partition prefix");
    let (partition, rest) = rest.split_once(' ').expect("partition field");
    let rest = rest.strip_prefix("offset ").expect("offset prefix");
    let (offset, rest) = rest.split_once(' ').expect("offset field");
    let value = rest.strip_prefix("value ").expect("value prefix");
    (
        partition.parse().expect("numeric partition"),
        offset.parse().expect("numeric offset"),
        value.to_string(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn consume_delivers_multi_partition_late_appends_then_stops_on_interrupt() {
    // Requirement 9.3 (eventual delivery of late records), 9.6 (renders
    // partition/offset/value), 11.1 (interrupt stops the session cleanly).
    const TOPIC: &str = "events";
    const LEADER_ID: &str = "node-leader";

    // A two-partition topic with an initial committed log per partition.
    let initial: Vec<Vec<Vec<u8>>> =
        vec![vec![b"a0".to_vec(), b"a1".to_vec()], vec![b"b0".to_vec()]];
    let node = FakeNode::data(LEADER_ID, 2, initial.clone());
    let addr = serve(node.clone()).await;

    let client = VelaClient::new([(LEADER_ID.to_string(), addr.clone())]);
    // Seed each partition's believed leader so per-partition dispatch reaches the
    // fake directly without a `FindLeader` round trip, keeping the run
    // deterministic.
    for p in 0..2u32 {
        client.core().leaders().insert(TOPIC, p, addr.as_str());
    }

    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let signal = TriggerSignal::default();
    let clock: Arc<dyn Clock> = Arc::new(InstantClock::new());

    let handle = {
        let buf = Arc::clone(&buf);
        let signal = signal.clone();
        tokio::spawn(async move {
            let mut sink = SharedSink(buf);
            run_consume(
                client,
                TOPIC.to_string(),
                None,
                OffsetReset::Earliest,
                Duration::from_millis(500),
                clock,
                signal,
                &mut sink,
            )
            .await
        })
    };

    // Phase 1: every initial record (3 across the two partitions) is delivered.
    wait_for_lines(&buf, 3).await;

    // Append late records now that the consumer has drained the initial log;
    // they must be delivered on a subsequent poll (Requirement 9.3).
    {
        let mut logs = node.logs.lock().expect("logs mutex poisoned");
        logs[0].push(b"a2".to_vec());
        logs[1].push(b"b1".to_vec());
        logs[1].push(b"b2".to_vec());
    }

    // Phase 2: the late records reach the operator too (6 total).
    wait_for_lines(&buf, 6).await;

    // Interrupt the loop and let it wind down cleanly (Requirement 11.1).
    signal.notify.notify_one();
    let result = handle.await.expect("consume task joins");
    assert!(matches!(result, Ok(())), "interrupt exits zero: {result:?}");

    // Group the delivered records by partition, preserving per-partition order.
    let mut grouped: BTreeMap<u32, Vec<(u64, String)>> = BTreeMap::new();
    for line in lines(&buf) {
        let (partition, offset, value) = parse_line(&line);
        grouped.entry(partition).or_default().push((offset, value));
    }

    // Both the initial and the late-appended records are delivered exactly once,
    // in ascending offset order, on each partition (Requirement 9.3, 9.6).
    assert_eq!(
        grouped.get(&0).cloned().unwrap_or_default(),
        vec![
            (0, "a0".to_string()),
            (1, "a1".to_string()),
            (2, "a2".to_string()),
        ],
        "partition 0 delivers its initial and late records in order",
    );
    assert_eq!(
        grouped.get(&1).cloned().unwrap_or_default(),
        vec![
            (0, "b0".to_string()),
            (1, "b1".to_string()),
            (2, "b2".to_string()),
        ],
        "partition 1 delivers its initial and late records in order",
    );
}
