//! Property tests for the continuous Consumer (`consume::run_consume`).
//!
//! Feature: ctl-client-routing-and-repl, Property 17, 18, 19, 20
//!
//! Property 17: Consumer per-partition offset monotonicity and no-gap delivery.
//! For any per-partition stream of committed records, the records the
//! `Consume_Loop` emits for that partition are in strictly ascending offset
//! order with no gaps and no duplicates, and each poll requests the
//! `next_offset` returned by the previous poll (the in-memory `Next_Offset` is
//! non-decreasing) (Requirements 8.3, 9.1, 9.4).
//!
//! Property 18: Eventual delivery of late records. For any records appended to a
//! partition after the `Consume_Loop` has reached the end of that partition's
//! log, every such record is eventually delivered exactly once and in offset
//! order on a subsequent poll (Requirement 9.3).
//!
//! Property 19: Partition polling isolation and coverage. For any topic with `N`
//! partitions and any single partition that stalls indefinitely, the
//! `Consume_Loop` still polls and delivers records from every other partition
//! `0..N` — one stuck partition never starves the others (Requirements 8.2,
//! 10.4, 10.5).
//!
//! Property 20: Consumer output renders partition, offset, and value. For any
//! delivered record, the line the `Consume_Loop` prints contains that record's
//! partition index, its offset, and its value, so offsets remain distinguishable
//! across partitions (Requirement 9.6).
//!
//! The loop is driven through the public [`vela_ctl::consume::run_consume`] seam
//! against an in-process fake `VelaClient` gRPC server (the harness pattern from
//! `vela-client`'s routing tests and `vela-ctl`'s produce-REPL property tests).
//! The fake advertises a topic's partition count via `describe_topic` and serves
//! scripted, per-partition committed logs via `consume`, in bounded batches so a
//! partition is drained across several polls. A "stalled" partition's `consume`
//! never returns, standing in for a stuck/dead leader. The session reads from
//! [`OffsetReset::Earliest`] so each partition starts at offset `0` and every
//! committed record from the start of the log is delivered with no probe step.
//!
//! Timing is made deterministic without real waiting by injecting an instant
//! virtual [`Clock`]: its `sleep` advances virtual time and yields rather than
//! blocking, so the empty-poll `Polling_Interval` between re-polls costs no
//! wall-clock time (the polling cadence is paced only by the loopback RPC). Each
//! session is bounded by triggering the interrupt [`Signal`] once the expected
//! records have been observed on the captured output, mirroring the approach the
//! task calls out for composing a controllable clock with an async gRPC server.
//!
//! Validates: Requirements 8.2, 8.3, 9.1, 9.3, 9.4, 9.6, 10.4, 10.5

use std::collections::{BTreeMap, HashSet};
use std::io::{self, Write};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use proptest::prelude::*;
use tokio::sync::Notify;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use vela_client::VelaClient;
use vela_ctl::consume::{run_consume, OffsetReset};
use vela_ctl::seams::{Clock, Signal};
use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
use vela_proto::v1::{
    ConsumeRequest, ConsumeResponse, ConsumedRecord, CreateTopicRequest, CreateTopicResponse,
    DeleteTopicRequest, DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse,
    DescribeTopicRequest, DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse,
    ListTopicsRequest, ListTopicsResponse, Member, ProduceBatchRequest, ProduceBatchResponse,
    ProduceRequest, ProduceResponse, Record, TopicInfo,
};

/// The node id the fake server's address is registered under and the believed
/// leader of every partition (seeded into the leader cache so dispatch never
/// needs `FindLeader`).
const NODE_ID: &str = "node-a";

/// The topic the consumer reads throughout these properties.
const TOPIC: &str = "events";

/// The `Polling_Interval` passed to the loop. Its real value is immaterial: the
/// injected instant clock makes every interval wait cost no wall-clock time.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// How many committed records the fake serves per `consume` poll, so a partition
/// with more than this many records is drained across several batches
/// (exercising the loop's batch-advance path, Requirement 9.4).
const BATCH: usize = 4;

/// Per-case cluster state the fake server reads and the test mutates.
///
/// `logs[p]` is partition `p`'s committed log: the value at index `i` is the
/// record at offset `i`. `stalls` holds the partitions whose `consume` hangs
/// forever (a stuck/dead leader). proptest runs cases sequentially under a lock,
/// so a single shared server with state reset at the start of each case is
/// race-free.
#[derive(Default)]
struct ClusterState {
    partition_count: u32,
    logs: Vec<Vec<Vec<u8>>>,
    stalls: HashSet<u32>,
}

/// An in-process fake of the client-facing `VelaClient` service.
///
/// Exercises only `describe_topic` (to advertise the partition count) and
/// `consume` (to serve scripted per-partition batches, including a stalled
/// partition). Every other RPC is `unimplemented` — leaders are pre-seeded into
/// the client cache, so neither `find_leader` nor `describe_cluster` is reached.
#[derive(Clone)]
struct ConsumeServer {
    state: Arc<Mutex<ClusterState>>,
}

#[tonic::async_trait]
impl VelaClientService for ConsumeServer {
    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        let request = request.into_inner();
        let partition = request.partition;
        let offset = request.offset;

        // A stalled partition never answers, standing in for a stuck or dead
        // leader (Property 19). Read the flag, drop the lock, then hang.
        let stalled = {
            let state = self.state.lock().expect("server state mutex poisoned");
            state.stalls.contains(&partition)
        };
        if stalled {
            std::future::pending::<()>().await;
        }

        let state = self.state.lock().expect("server state mutex poisoned");
        // A request for a partition outside the current topic is served as an
        // empty poll. This only ever happens for a stale in-flight request left
        // over from a prior case (whose client has already gone away) reaching
        // the shared server after the per-case state was reset to fewer
        // partitions; answering empty keeps it harmless rather than panicking.
        let Some(log) = state.logs.get(partition as usize) else {
            return Ok(Response::new(ConsumeResponse {
                records: vec![],
                next_offset: offset,
            }));
        };
        let start = offset as usize;
        // Past the end of the committed log: an empty poll. `next_offset` stays
        // at the requested offset, so the consumer holds its `Next_Offset`.
        if start >= log.len() {
            return Ok(Response::new(ConsumeResponse {
                records: vec![],
                next_offset: offset,
            }));
        }
        // Serve a bounded batch in ascending offset order; `next_offset` is the
        // offset just past the last record returned (the server's contract).
        let end = (start + BATCH).min(log.len());
        let records = (start..end)
            .map(|i| ConsumedRecord {
                offset: i as u64,
                record: Some(Record {
                    key: None,
                    value: log[i].clone(),
                }),
            })
            .collect();
        Ok(Response::new(ConsumeResponse {
            records,
            next_offset: end as u64,
        }))
    }

    async fn describe_topic(
        &self,
        _request: Request<DescribeTopicRequest>,
    ) -> Result<Response<DescribeTopicResponse>, Status> {
        let partition_count = self
            .state
            .lock()
            .expect("server state mutex poisoned")
            .partition_count;
        Ok(Response::new(DescribeTopicResponse {
            topic: Some(TopicInfo {
                name: TOPIC.to_string(),
                partition_count,
                ..Default::default()
            }),
        }))
    }

    async fn find_leader(
        &self,
        _request: Request<FindLeaderRequest>,
    ) -> Result<Response<FindLeaderResponse>, Status> {
        // Pre-seeded leaders mean this is never reached, but answer correctly so
        // the fake is a faithful node.
        Ok(Response::new(FindLeaderResponse {
            leader: Some(NODE_ID.to_string()),
        }))
    }

    async fn describe_cluster(
        &self,
        _request: Request<DescribeClusterRequest>,
    ) -> Result<Response<DescribeClusterResponse>, Status> {
        Ok(Response::new(DescribeClusterResponse {
            members: vec![Member {
                id: NODE_ID.to_string(),
                addr: String::new(),
                advertised_addr: String::new(),
                availability: 0,
            }],
            epoch: 0,
        }))
    }

    async fn produce(
        &self,
        _request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        Err(Status::unimplemented("produce is not used by this test"))
    }

    async fn produce_batch(
        &self,
        _request: Request<ProduceBatchRequest>,
    ) -> Result<Response<ProduceBatchResponse>, Status> {
        Err(Status::unimplemented(
            "produce_batch is not used by this test",
        ))
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
}

/// An instant virtual [`Clock`]: `sleep` advances virtual time and yields rather
/// than blocking, so the loop's empty-poll `Polling_Interval` costs no
/// wall-clock time. `now` reports the accumulated virtual time so any elapsed
/// comparison still progresses. Mirrors the `VirtualClock` used in the
/// `vela-client` admin routing tests.
struct InstantClock {
    base: tokio::time::Instant,
    elapsed: Mutex<Duration>,
}

impl InstantClock {
    fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            elapsed: Mutex::new(Duration::ZERO),
        }
    }
}

#[tonic::async_trait]
impl Clock for InstantClock {
    fn now(&self) -> tokio::time::Instant {
        self.base + *self.elapsed.lock().expect("virtual clock mutex poisoned")
    }

    async fn sleep(&self, dur: Duration) {
        *self.elapsed.lock().expect("virtual clock mutex poisoned") += dur;
        // Yield so the busy re-poll loop cooperates with the runtime rather than
        // monopolising it, while still returning effectively instantly.
        tokio::task::yield_now().await;
    }
}

/// A triggerable [`Signal`] whose `interrupted` future resolves once
/// [`Notify::notify_one`] has been called, used to bound each session.
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

/// A shared multi-thread runtime hosting one fake server bound on an OS-chosen
/// port, built once and reused across every proptest case; the per-case
/// [`ClusterState`] is reset at the start of each case.
struct Harness {
    rt: tokio::runtime::Runtime,
    addr: String,
    state: Arc<Mutex<ClusterState>>,
}

fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build multi-thread runtime");
        let state = Arc::new(Mutex::new(ClusterState::default()));
        let server = ConsumeServer {
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

/// Serializes proptest cases across the property functions, which cargo runs on
/// parallel threads. They share one global [`Harness`] (and its mutable per-case
/// [`ClusterState`]), so a case holds this lock for its whole reset→run→read
/// cycle to keep it atomic with respect to the other cases.
fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Render bytes the way the loop's printer does (lossy UTF-8), so expected
/// values compare equal to the printed `value` field.
fn lossy(value: &[u8]) -> String {
    String::from_utf8_lossy(value).into_owned()
}

/// Count completed output lines currently in the buffer.
fn line_count(buf: &Arc<Mutex<Vec<u8>>>) -> usize {
    buf.lock()
        .expect("sink mutex poisoned")
        .iter()
        .filter(|&&b| b == b'\n')
        .count()
}

/// Wait until at least `target` lines have been printed, or a generous deadline
/// elapses (after which the caller proceeds and its assertions surface the
/// shortfall). Uses real short sleeps; the loop itself runs on the virtual clock.
async fn wait_for_lines(buf: &Arc<Mutex<Vec<u8>>>, target: usize) {
    for _ in 0..3000 {
        if line_count(buf) >= target {
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

/// Group parsed records by partition, preserving each partition's delivery order
/// (the single printer writes lines sequentially, so per-partition order is the
/// order the loop emitted them).
fn group(records: &[(u32, u64, String)]) -> BTreeMap<u32, Vec<(u64, String)>> {
    let mut grouped: BTreeMap<u32, Vec<(u64, String)>> = BTreeMap::new();
    for (partition, offset, value) in records {
        grouped
            .entry(*partition)
            .or_default()
            .push((*offset, value.clone()));
    }
    grouped
}

/// Drive one continuous-consume session and return the raw printed lines.
///
/// Resets the shared server with `initial` per-partition logs and `stalls`,
/// starts [`run_consume`] over all partitions reading from `earliest`, waits for
/// every initial record (from non-stalled partitions) to be delivered, appends
/// the `late` records (exercising eventual delivery), waits for those too, then
/// interrupts the loop and returns the captured output.
fn run_session(
    initial: Vec<Vec<Vec<u8>>>,
    late: Vec<Vec<Vec<u8>>>,
    stalls: HashSet<u32>,
) -> Vec<String> {
    let n = initial.len();
    assert_eq!(
        late.len(),
        n,
        "late logs must be sized to the partition count"
    );
    let harness = harness();

    // Held for the whole reset→run→read cycle so concurrent cases cannot
    // interleave. Recover from a poisoned lock rather than cascading a prior
    // case's panic.
    let _guard = test_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Expected counts cover only the partitions that actually deliver (a stalled
    // partition delivers nothing).
    let delivered_indices: Vec<usize> = (0..n).filter(|p| !stalls.contains(&(*p as u32))).collect();
    let expected_initial: usize = delivered_indices.iter().map(|&p| initial[p].len()).sum();
    let expected_total: usize = expected_initial
        + delivered_indices
            .iter()
            .map(|&p| late[p].len())
            .sum::<usize>();

    {
        let mut state = harness.state.lock().expect("server state mutex poisoned");
        *state = ClusterState {
            partition_count: n as u32,
            logs: initial.clone(),
            stalls,
        };
    }

    harness.rt.block_on(async move {
        let client = VelaClient::new([(NODE_ID.to_string(), harness.addr.clone())]);
        // Seed each partition's believed leader so dispatch reaches the fake
        // directly without a `FindLeader` round trip.
        for p in 0..n as u32 {
            client
                .core()
                .leaders()
                .insert(TOPIC, p, harness.addr.as_str());
        }

        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let signal = TriggerSignal::default();
        let clock: Arc<dyn Clock> = Arc::new(InstantClock::new());

        let task_buf = Arc::clone(&buf);
        let task_signal = signal.clone();
        let handle = tokio::spawn(async move {
            let mut sink = SharedSink(task_buf);
            run_consume(
                client,
                TOPIC.to_string(),
                None,
                OffsetReset::Earliest,
                POLL_INTERVAL,
                clock,
                task_signal,
                &mut sink,
            )
            .await
        });

        // Phase 1: every initial record reaches the operator.
        wait_for_lines(&buf, expected_initial).await;

        // Append the late records now that the consumer has drained the initial
        // log; they must be delivered on a subsequent poll (Requirement 9.3).
        {
            let mut state = harness.state.lock().expect("server state mutex poisoned");
            for (p, extra) in late.iter().enumerate() {
                state.logs[p].extend(extra.iter().cloned());
            }
        }

        // Phase 2: the late records reach the operator too.
        wait_for_lines(&buf, expected_total).await;

        // Bound the run: interrupt the loop and let it wind down cleanly.
        signal.notify.notify_one();
        let result = handle.await.expect("consume task joins");
        assert!(result.is_ok(), "interrupt exits zero: {result:?}");

        let raw = String::from_utf8(buf.lock().expect("sink mutex poisoned").clone())
            .expect("utf8 consume output");
        raw.lines().map(str::to_string).collect()
    })
}

/// A single committed record value: a non-empty printable byte string, so the
/// printed `value` field is visible and round-trips losslessly through UTF-8.
fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(0x20u8..=0x7e, 1..6)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: ctl-client-routing-and-repl, Property 17
    #[test]
    fn per_partition_offsets_are_monotonic_and_gapless(
        logs in (1usize..=4).prop_flat_map(|n| {
            proptest::collection::vec(proptest::collection::vec(value_strategy(), 1..6), n)
        }),
    ) {
        let n = logs.len();
        let lines = run_session(logs.clone(), vec![Vec::new(); n], HashSet::new());
        let records: Vec<(u32, u64, String)> = lines.iter().map(|l| parse_line(l)).collect();
        let grouped = group(&records);

        for (p, log) in logs.iter().enumerate() {
            let got = grouped.get(&(p as u32)).cloned().unwrap_or_default();

            // The delivered offsets are strictly ascending with no gaps and no
            // duplicates: exactly 0,1,..,k-1 in order (Requirement 8.3, 9.4).
            let offsets: Vec<u64> = got.iter().map(|(o, _)| *o).collect();
            let expected_offsets: Vec<u64> = (0..log.len() as u64).collect();
            prop_assert_eq!(
                &offsets,
                &expected_offsets,
                "partition {} offsets must be ascending and gapless", p,
            );

            // Each delivered value matches the committed record at that offset.
            let expected: Vec<(u64, String)> = log
                .iter()
                .enumerate()
                .map(|(i, v)| (i as u64, lossy(v)))
                .collect();
            prop_assert_eq!(got, expected, "partition {} values must match the log", p);
        }
    }

    // Feature: ctl-client-routing-and-repl, Property 18
    #[test]
    fn late_records_are_eventually_delivered(
        (initial, late) in (2usize..=4).prop_flat_map(|n| {
            (
                proptest::collection::vec(proptest::collection::vec(value_strategy(), 0..4), n),
                proptest::collection::vec(proptest::collection::vec(value_strategy(), 1..4), n),
            )
        }),
    ) {
        let n = initial.len();
        let lines = run_session(initial.clone(), late.clone(), HashSet::new());
        let records: Vec<(u32, u64, String)> = lines.iter().map(|l| parse_line(l)).collect();
        let grouped = group(&records);

        for p in 0..n {
            let got = grouped.get(&(p as u32)).cloned().unwrap_or_default();

            // Every record — those committed before the consumer reached the end
            // of the log, and the late ones appended afterwards — is delivered
            // exactly once, in offset order, with no gaps (Requirement 9.3).
            let full: Vec<(u64, String)> = initial[p]
                .iter()
                .chain(late[p].iter())
                .enumerate()
                .map(|(i, v)| (i as u64, lossy(v)))
                .collect();
            prop_assert_eq!(
                got,
                full,
                "partition {} must eventually deliver every late record once, in order", p,
            );
        }
    }

    // Feature: ctl-client-routing-and-repl, Property 19
    #[test]
    fn a_stalled_partition_never_starves_the_others(
        (logs, stalled) in (2usize..=4).prop_flat_map(|n| {
            (
                proptest::collection::vec(proptest::collection::vec(value_strategy(), 1..5), n),
                0usize..n,
            )
        }),
    ) {
        let n = logs.len();
        let stalled = stalled as u32;
        let mut stalls = HashSet::new();
        stalls.insert(stalled);

        // The session only completes — `run_session` joins the loop after the
        // interrupt — because the stalled partition's hung poll never blocks the
        // others or the printer (Requirement 10.5).
        let lines = run_session(logs.clone(), vec![Vec::new(); n], stalls);
        let records: Vec<(u32, u64, String)> = lines.iter().map(|l| parse_line(l)).collect();
        let grouped = group(&records);

        for (p, log) in logs.iter().enumerate() {
            if p as u32 == stalled {
                // The stuck partition delivers nothing.
                prop_assert!(
                    grouped.get(&(p as u32)).is_none_or(Vec::is_empty),
                    "stalled partition {} must deliver no records", p,
                );
                continue;
            }
            // Every other partition is fully delivered despite the stall
            // (Requirement 8.2, 10.4, 10.5).
            let got = grouped.get(&(p as u32)).cloned().unwrap_or_default();
            let expected: Vec<(u64, String)> = log
                .iter()
                .enumerate()
                .map(|(i, v)| (i as u64, lossy(v)))
                .collect();
            prop_assert_eq!(
                got,
                expected,
                "partition {} must be fully delivered while partition {} stalls", p, stalled,
            );
        }
    }

    // Feature: ctl-client-routing-and-repl, Property 20
    #[test]
    fn output_renders_partition_offset_and_value(
        logs in (1usize..=4).prop_flat_map(|n| {
            proptest::collection::vec(proptest::collection::vec(value_strategy(), 1..6), n)
        }),
    ) {
        let n = logs.len();
        let lines = run_session(logs.clone(), vec![Vec::new(); n], HashSet::new());

        // Every line is exactly `partition {p} offset {o} value {v}` for an
        // actually-committed record: it carries the partition, the offset, and
        // the value, so offsets stay distinguishable across partitions
        // (Requirement 9.6).
        for line in &lines {
            let (partition, offset, value) = parse_line(line);
            prop_assert!((partition as usize) < n, "partition {} in range", partition);
            let log = &logs[partition as usize];
            prop_assert!((offset as usize) < log.len(), "offset {} in range", offset);
            let expected_value = lossy(&log[offset as usize]);
            prop_assert_eq!(&value, &expected_value, "rendered value matches the record");
            prop_assert_eq!(
                line,
                &format!("partition {partition} offset {offset} value {expected_value}"),
                "the line renders partition, offset, and value",
            );
        }

        // And every committed record is rendered on its own line.
        let total: usize = logs.iter().map(Vec::len).sum();
        prop_assert_eq!(lines.len(), total, "one rendered line per committed record");
    }
}
