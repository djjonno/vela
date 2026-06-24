//! Property tests for the Producer REPL (`produce::run_repl`).
//!
//! Feature: ctl-client-routing-and-repl, Property 14, 15, 16
//!
//! Property 14: REPL produces exactly one record per input line, in order. For
//! any finite sequence of input lines, the REPL produces exactly one record per
//! line — no more, no fewer — and the cluster receives those records' values in
//! the same order the operator typed them (Requirement 6.3).
//!
//! Property 15: Keyed REPL applies the key to every record. When the session is
//! started with a key, every record the REPL produces carries that exact key,
//! for every input line (Requirement 6.4).
//!
//! Property 16: REPL survives per-line produce errors. When the cluster rejects
//! some lines with a (non-retryable) produce error, the REPL reports the error
//! and continues: it still attempts every subsequent line exactly once, and it
//! keeps prompting through to end-of-input rather than terminating early
//! (Requirement 6.5).
//!
//! The REPL is driven through the public [`vela_ctl::produce::run_repl`] seam
//! with a scripted [`LineSource`] (a `VecDeque` of lines that ends in `None`,
//! i.e. EOF) and a never-firing [`Signal`], so each session runs to its
//! end-of-input exit. The producer is a real [`vela_client::Producer`] pointed
//! at an in-process fake `VelaClient` gRPC server that records every `produce`
//! it receives — partition, key, and value — and assigns increasing per-partition
//! offsets. Asserting on what the *server* received exercises the whole produce
//! path end to end, not just the REPL's local bookkeeping.
//!
//! Rejected lines use an `InvalidArgument` status, which the client's dispatch
//! engine classifies as a non-retryable `Fatal` error: dispatch makes exactly
//! one attempt and surfaces it, so a rejected line costs the cluster exactly one
//! produce attempt (no retry/backoff), and the REPL's error-and-continue
//! behaviour is what keeps the session alive.
//!
//! Validates: Requirements 6.3, 6.4, 6.5

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::{Arc, Mutex, OnceLock};

use proptest::prelude::*;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use vela_client::{ClientCore, Producer, TopicMeta};
use vela_ctl::produce::run_repl;
use vela_ctl::seams::{LineSource, Signal};
use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
use vela_proto::v1::{
    ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse, DeleteTopicRequest,
    DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse, DescribeTopicRequest,
    DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse, ListTopicsRequest,
    ListTopicsResponse, ProduceBatchRequest, ProduceBatchResponse, ProduceRequest, ProduceResponse,
};

/// The node id the fake server's address is registered under in the client
/// registry and the believed leader for every partition.
const NODE_ID: &str = "node-a";

/// The topic the REPL produces to throughout these properties.
const TOPIC: &str = "orders";

/// One produce request the fake server received, captured verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Produced {
    partition: u32,
    key: Option<Vec<u8>>,
    value: Vec<u8>,
}

/// Shared, per-case state the fake server reads and writes.
///
/// `received` records every produce attempt in arrival order (including ones the
/// server then rejects, so a rejected line still counts as an attempt). `fail`
/// holds the set of values the server rejects with a non-retryable error.
/// `offsets` assigns increasing per-partition offsets to accepted records.
///
/// proptest runs its cases sequentially on the calling thread, so a single
/// shared server with state reset at the start of each case is race-free.
#[derive(Default)]
struct ServerState {
    received: Vec<Produced>,
    fail: HashSet<Vec<u8>>,
    offsets: HashMap<u32, u64>,
}

/// An in-process fake of the client-facing `VelaClient` service.
///
/// Only `produce` is exercised — the believed leader is seeded into the client's
/// cache so dispatch never calls `FindLeader` — but the full service trait is
/// implemented so the type can be served by `tonic`. Every other RPC is
/// `unimplemented` and never reached.
#[derive(Clone)]
struct RecordingServer {
    state: Arc<Mutex<ServerState>>,
}

#[tonic::async_trait]
impl VelaClientService for RecordingServer {
    async fn produce(
        &self,
        request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let request = request.into_inner();
        let record = request.record.unwrap_or_default();
        let value = record.value;
        let mut state = self.state.lock().expect("server state mutex poisoned");

        // Record the attempt before deciding the outcome, so a rejected line is
        // still counted as one attempt (Property 16).
        state.received.push(Produced {
            partition: request.partition,
            key: record.key,
            value: value.clone(),
        });

        // A rejected value returns a non-retryable status (classified `Fatal`),
        // so dispatch surfaces it after exactly one attempt with no retry.
        if state.fail.contains(&value) {
            return Err(Status::invalid_argument("record rejected"));
        }

        // Accepted records get an increasing per-partition offset.
        let next = state.offsets.entry(request.partition).or_insert(0);
        let offset = *next;
        *next += 1;
        Ok(Response::new(ProduceResponse { offset }))
    }

    async fn produce_batch(
        &self,
        _request: Request<ProduceBatchRequest>,
    ) -> Result<Response<ProduceBatchResponse>, Status> {
        Err(Status::unimplemented(
            "produce_batch is not used by this test",
        ))
    }

    async fn consume(
        &self,
        _request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        Err(Status::unimplemented("consume is not used by this test"))
    }

    async fn create_topic(
        &self,
        _request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        Err(Status::unimplemented(
            "create_topic is not used by this test",
        ))
    }

    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> Result<Response<DeleteTopicResponse>, Status> {
        Err(Status::unimplemented(
            "delete_topic is not used by this test",
        ))
    }

    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        Err(Status::unimplemented(
            "list_topics is not used by this test",
        ))
    }

    async fn describe_topic(
        &self,
        _request: Request<DescribeTopicRequest>,
    ) -> Result<Response<DescribeTopicResponse>, Status> {
        Err(Status::unimplemented(
            "describe_topic is not used by this test",
        ))
    }

    async fn find_leader(
        &self,
        _request: Request<FindLeaderRequest>,
    ) -> Result<Response<FindLeaderResponse>, Status> {
        Err(Status::unimplemented(
            "find_leader is not used by this test",
        ))
    }

    async fn describe_cluster(
        &self,
        _request: Request<DescribeClusterRequest>,
    ) -> Result<Response<DescribeClusterResponse>, Status> {
        Err(Status::unimplemented(
            "describe_cluster is not used by this test",
        ))
    }
}

/// A scripted [`LineSource`] that yields its queued lines in order then `None`
/// (EOF), standing in for stdin (mirrors the design's `VecDeque<String>` impl).
struct ScriptedLines {
    lines: VecDeque<String>,
}

#[tonic::async_trait]
impl LineSource for ScriptedLines {
    async fn next_line(&mut self) -> io::Result<Option<String>> {
        Ok(self.lines.pop_front())
    }
}

/// A [`Signal`] that never fires, so the REPL runs to end-of-input.
struct NeverSignal;

#[tonic::async_trait]
impl Signal for NeverSignal {
    async fn interrupted(&self) {
        std::future::pending::<()>().await;
    }
}

/// A shared multi-thread runtime hosting one fake server bound on an OS-chosen
/// port. Built once and reused across every proptest case (so the cases share a
/// single server rather than binding a fresh port each); the per-case state in
/// [`ServerState`] is reset at the start of each case.
struct Harness {
    rt: tokio::runtime::Runtime,
    addr: String,
    state: Arc<Mutex<ServerState>>,
}

fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build multi-thread runtime");
        let state = Arc::new(Mutex::new(ServerState::default()));
        let server = RecordingServer {
            state: Arc::clone(&state),
        };
        // Bind before returning so the URL is already accepting connections — the
        // client never races server startup.
        let addr = rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind ephemeral port");
            let port = listener.local_addr().expect("local addr").port();
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(VelaClientServer::new(server))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .expect("fake server serves");
            });
            format!("http://127.0.0.1:{port}")
        });
        Harness { rt, addr, state }
    })
}

/// Build a [`Producer`] whose dispatch reaches the fake server directly: the
/// topic's partition count is seeded as fresh metadata (so `produce` resolves a
/// partition without a `DescribeTopic`), and every partition's believed leader
/// is seeded to the server's address (so dispatch never calls `FindLeader`).
fn producer(addr: &str, partition_count: u32) -> Producer {
    // A short TTL is irrelevant here — the entry is stamped `now`, so it stays
    // fresh for the brief lifetime of one case; the default TTL is plenty.
    let core = ClientCore::new([(NODE_ID.to_string(), addr.to_string())]);
    core.metadata().put(
        TOPIC,
        TopicMeta {
            partition_count,
            leaders: vec![Some(NODE_ID.to_string()); partition_count as usize],
            learned_at: std::time::Instant::now(),
        },
    );
    for partition in 0..partition_count {
        core.leaders().insert(TOPIC, partition, addr);
    }
    Producer::new(Arc::new(core))
}

/// Serializes proptest cases across the three test functions, which cargo runs
/// on parallel threads. They share one global [`Harness`] (and its mutable
/// per-case [`ServerState`]), so a case must hold this lock for the whole
/// reset→run→read cycle to keep it atomic with respect to the other cases.
fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Drive one REPL session over `lines` with optional `key`, rejecting any value
/// in `fail`, and return the produce requests the server received in order.
fn run_session(
    lines: Vec<String>,
    key: Option<Vec<u8>>,
    partition_count: u32,
    fail: HashSet<Vec<u8>>,
) -> (Vec<Produced>, String) {
    let harness = harness();
    // Held for the whole cycle so concurrent cases can't interleave their
    // resets and reads against the shared server state. Recover from a poisoned
    // lock (a prior case's panic) rather than cascading the failure.
    let _guard = test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    {
        // Reset the shared server state for this case.
        let mut state = harness.state.lock().expect("server state mutex poisoned");
        *state = ServerState {
            fail,
            ..ServerState::default()
        };
    }

    let out = harness.rt.block_on(async {
        let producer = producer(&harness.addr, partition_count);
        let mut out = Vec::<u8>::new();
        let result = run_repl(
            producer,
            TOPIC.to_string(),
            key,
            ScriptedLines {
                lines: lines.into_iter().collect(),
            },
            NeverSignal,
            &mut out,
        )
        .await;
        // The REPL always exits zero at EOF — a per-line produce error never ends
        // the session (Requirement 6.5, 6.6).
        assert!(
            result.is_ok(),
            "REPL should exit cleanly at EOF: {result:?}"
        );
        out
    });

    let received = harness
        .state
        .lock()
        .expect("server state mutex poisoned")
        .received
        .clone();
    (received, String::from_utf8(out).expect("utf8 REPL output"))
}

/// A line is any printable, newline-free string (what a `LineSource` yields).
fn line_strategy() -> impl Strategy<Value = String> {
    "[ -~]{0,16}"
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: ctl-client-routing-and-repl, Property 14
    #[test]
    fn repl_produces_exactly_one_record_per_line_in_order(
        lines in proptest::collection::vec(line_strategy(), 0..16),
        partition_count in 1u32..5,
    ) {
        let (received, _out) = run_session(lines.clone(), None, partition_count, HashSet::new());

        // Exactly one produce per input line — no more, no fewer.
        prop_assert_eq!(
            received.len(),
            lines.len(),
            "the REPL must produce exactly one record per input line",
        );

        // The values reach the cluster in the order the operator typed them: the
        // REPL produces each line before reading the next, so arrival order is
        // input order.
        let received_values: Vec<Vec<u8>> =
            received.iter().map(|p| p.value.clone()).collect();
        let expected_values: Vec<Vec<u8>> =
            lines.iter().map(|line| line.clone().into_bytes()).collect();
        prop_assert_eq!(received_values, expected_values, "records must arrive in input order");
    }

    // Feature: ctl-client-routing-and-repl, Property 15
    #[test]
    fn keyed_repl_applies_the_key_to_every_record(
        lines in proptest::collection::vec(line_strategy(), 0..16),
        key in proptest::collection::vec(any::<u8>(), 1..12),
        partition_count in 1u32..5,
    ) {
        let (received, _out) =
            run_session(lines.clone(), Some(key.clone()), partition_count, HashSet::new());

        // One record per line, as in Property 14.
        prop_assert_eq!(
            received.len(),
            lines.len(),
            "the keyed REPL must still produce exactly one record per line",
        );

        // Every record carries the session key — not just the first
        // (Requirement 6.4).
        for produced in &received {
            prop_assert_eq!(
                produced.key.as_deref(),
                Some(key.as_slice()),
                "every keyed record must carry the session key",
            );
        }
    }

    // Feature: ctl-client-routing-and-repl, Property 16
    #[test]
    fn repl_survives_per_line_produce_errors(
        lines in proptest::collection::vec(line_strategy(), 1..16),
        // Which line indices the cluster rejects.
        reject_mask in proptest::collection::vec(any::<bool>(), 1..16),
        partition_count in 1u32..5,
    ) {
        // Reject the distinct values at the masked positions. Using a value set
        // keeps the server stateless about ordering; duplicate values rejected at
        // any position are rejected everywhere, which only strengthens the
        // "survives errors" guarantee.
        let fail: HashSet<Vec<u8>> = lines
            .iter()
            .zip(reject_mask.iter().cycle())
            .filter(|(_, &reject)| reject)
            .map(|(line, _)| line.clone().into_bytes())
            .collect();

        let (received, out) = run_session(lines.clone(), None, partition_count, fail.clone());

        // Every line was attempted exactly once despite some being rejected: the
        // REPL did not abort mid-session (Requirement 6.5), and a rejected line
        // costs exactly one attempt (the `Fatal` classification means no retry).
        prop_assert_eq!(
            received.len(),
            lines.len(),
            "every line must be attempted exactly once even when some are rejected",
        );
        let received_values: Vec<Vec<u8>> =
            received.iter().map(|p| p.value.clone()).collect();
        let expected_values: Vec<Vec<u8>> =
            lines.iter().map(|line| line.clone().into_bytes()).collect();
        prop_assert_eq!(received_values, expected_values, "all lines attempted, in order");

        // The REPL kept prompting through to EOF: one prompt before each line plus
        // a final prompt before the EOF read returns. So the prompt count is
        // exactly `lines.len() + 1`, proving it never terminated early.
        let prompts = out.matches("> ").count();
        prop_assert_eq!(
            prompts,
            lines.len() + 1,
            "the REPL must keep prompting through every line to EOF",
        );
    }
}
