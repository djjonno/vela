//! Single-node metadata-group bootstrap smoke test (task 9.1).
//!
//! This is the non-optional gate that proves a **1-voter `__meta/0` metadata
//! Raft group self-elects through the normal asynchronous election path** —
//! the same `TimerClock`-driven election/heartbeat loop every partition group
//! uses — rather than depending on the legacy inline `BootstrapClock` step, and
//! that with a metadata leader in place a single node serves the full
//! `CreateTopic → Produce → Consume` pipeline end-to-end (H3).
//!
//! ## Why this exercises the async self-election path
//!
//! Topic admin no longer commits to a node-local single-node `__meta`. A
//! `CreateTopic` is a leader-routed proposal: `NodeShared::create_topic` sends a
//! `ProposeCluster` command to the spawned `__meta/0` **driver task** and awaits
//! its commit. That proposal can only succeed when the driver's own metadata
//! replica is `Role::Leader`; while the metadata group has not yet elected a
//! leader the proposal is rejected with a "no metadata leader available" error
//! and nothing is committed (Requirement 4.2). So a `CreateTopic` that *commits*
//! is direct evidence the async metadata driver reached leadership on its own
//! election timer and committed the entry to a majority-of-one — exactly the
//! Raft-native bootstrap this feature requires (Requirements 1.1, 2.4, 3.4).
//!
//! To make that observable without racing the election, the test retries
//! `CreateTopic` within a bounded window: early attempts may see the
//! no-metadata-leader condition while the group is still electing, and the
//! first attempt that succeeds is the moment the metadata group has
//! self-elected and committed through the driver. A metadata group that never
//! self-elects fails the test promptly rather than hanging.
//!
//! All waits are bounded retries with short sleeps (no unbounded blocking).

use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;

/// Monotonic counter making per-test data directories unique within a process.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, writable data directory under the system temp directory for one
/// test node.
///
/// Topics default to the Durable backend, so creating a topic opens a real
/// `DurableWal` under the node's data directory, and `__meta/0` recovers its
/// durable metadata WAL from there too; rooting that beneath a unique temp
/// directory lets the test drive the genuine durable path without depending on a
/// fixed, possibly-unwritable location. The directory is created lazily; cleanup
/// is left to the OS temp reaper because the spawned server task outlives the
/// test and holds it open.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!(
            "vela-server-meta-boot-{}-{n}-{nanos}",
            process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address the server should bind. Mirrors the helper in
/// `cluster_smoke.rs`; the small race between releasing the port and `serve`
/// re-binding it is reliable on localhost in a test.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated single-node [`Config`] through the same CLI path the daemon
/// uses (no peers), so the test exercises real configuration parsing and a
/// 1-voter metadata group.
fn config(node_id: &str, addr: SocketAddr) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        peers: Vec::new(),
        replication_factor: Some("1".to_string()),
        data_dir: Some(unique_data_dir()),
    })
    .expect("valid test configuration")
}

/// Spawn a node serving `config` on a background task. The task runs until the
/// test runtime is torn down; an early return from `serve` is a real failure.
fn spawn_server(config: Config) {
    tokio::spawn(async move {
        if let Err(error) = serve(config).await {
            panic!("server exited unexpectedly: {error}");
        }
    });
}

/// Connect a `VelaClient` to `addr`, retrying until the freshly spawned listener
/// accepts connections or a bounded number of attempts elapse.
async fn connect_client(addr: SocketAddr) -> VelaClientClient<Channel> {
    let url = format!("http://{addr}");
    for _ in 0..100 {
        if let Ok(client) = VelaClientClient::connect(url.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("VelaClient at {addr} did not become reachable");
}

/// Poll `FindLeader` for `(topic, partition)` until a leader is reported or the
/// bounded attempt budget is exhausted, returning the elected leader's node id.
///
/// Election on the real clock fires in the randomized 150–300 ms window, so
/// ~50 attempts at 20 ms (≈1 s) comfortably covers it while staying bounded.
async fn await_leader(
    client: &mut VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
) -> String {
    for _ in 0..50 {
        let leader = client
            .find_leader(v1::FindLeaderRequest {
                topic: topic.to_string(),
                partition,
            })
            .await
            .expect("find_leader RPC succeeds")
            .into_inner()
            .leader;
        if let Some(leader) = leader {
            return leader;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("partition {topic}/{partition} did not elect a leader within the bounded window");
}

/// Create `topic` (with `partitions` partitions), retrying while the metadata
/// group is still electing its leader.
///
/// A `CreateTopic` is a leader-routed proposal through the `__meta/0` driver: it
/// fails with a "no metadata leader available" `NotLeader` status until the
/// metadata group's async driver self-elects (Requirement 4.2), then commits.
/// Retrying within a bounded window therefore *waits for the metadata group to
/// self-elect through the normal election path* and returns the first committed
/// create — the bootstrap evidence task 9.1 requires. A metadata group that
/// never self-elects exhausts the budget and fails the test rather than hanging.
async fn create_topic_awaiting_metadata_leader(
    client: &mut VelaClientClient<Channel>,
    topic: &str,
    partitions: u32,
) -> v1::TopicInfo {
    let mut last_status = None;
    for _ in 0..100 {
        match client
            .create_topic(v1::CreateTopicRequest {
                name: topic.to_string(),
                partitions,
                log_backend: v1::LogBackend::Unspecified as i32,
            })
            .await
        {
            Ok(response) => {
                return response
                    .into_inner()
                    .topic
                    .expect("a committed create returns the applied topic");
            }
            // While `__meta/0` has not yet elected a leader the proposal is
            // rejected and commits nothing; retry until the async driver
            // self-elects (Requirement 4.2).
            Err(status) => {
                last_status = Some(status);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "metadata group did not self-elect a leader to commit CreateTopic within the bounded \
         window (last error: {last_status:?})"
    );
}

/// Requirements 1.1, 2.4, 3.4 — a single-node cluster's 1-voter `__meta/0`
/// metadata group self-elects through the normal asynchronous election path and
/// serves create → produce → consume end-to-end.
///
/// Steps:
/// 1. Bring up one `velad` node with no peers (a 1-voter metadata group).
/// 2. Issue `CreateTopic`, retrying while the metadata group elects. The first
///    success proves the async metadata driver self-elected and committed the
///    `ClusterCommand` to a majority-of-one through the driver propose path —
///    not through any inline `BootstrapClock` shortcut (the proposal is rejected
///    until the *driver's* replica is the elected leader).
/// 3. Wait for the new topic's partition Raft group to elect a leader, then
///    produce a record and assert it commits at offset 0, and consume it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_metadata_group_self_elects_and_serves_admin_produce_consume() {
    let addr = free_addr();
    spawn_server(config("node-a", addr));

    let mut client = connect_client(addr).await;

    // A fresh single node has no topics.
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds against a bound listener")
        .into_inner()
        .topics;
    assert!(topics.is_empty(), "a fresh node lists no topics");

    // Create a topic, waiting for the metadata group to self-elect a leader and
    // commit the create through the driver propose path (Requirements 1.1, 2.4,
    // 3.4). A successful commit here is the bootstrap evidence: the async
    // `__meta/0` driver reached leadership on its own election timer.
    let created = create_topic_awaiting_metadata_leader(&mut client, "orders", 1).await;
    assert_eq!(created.name, "orders");
    assert_eq!(created.partition_count, 1);

    // The committed create is applied to the served catalogue and visible to
    // clients (Requirement 5.1).
    let listed = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics;
    assert_eq!(listed.len(), 1, "the committed topic is in the catalogue");
    assert_eq!(listed[0].name, "orders");

    // The commit-driven reconciler started the partition driver on this node;
    // wait for the partition's Raft group to self-elect on the real clock.
    let leader = await_leader(&mut client, "orders", 0).await;
    assert_eq!(
        leader, "node-a",
        "the sole node leads its own single-replica partition"
    );

    // Produce a record; with rf=1 the lone replica is a majority of one, so it
    // commits at the first offset, 0. Retry briefly to absorb the instant
    // between FindLeader reporting a leader and the produce path observing it.
    let value = b"order-payload".to_vec();
    let mut produced_offset = None;
    for _ in 0..50 {
        match client
            .produce(v1::ProduceRequest {
                topic: "orders".to_string(),
                partition: 0,
                record: Some(v1::Record {
                    key: Some(b"order-key".to_vec()),
                    value: value.clone(),
                }),
            })
            .await
        {
            Ok(response) => {
                produced_offset = Some(response.into_inner().offset);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let produced_offset = produced_offset.expect("produce commits within the bounded window");
    assert_eq!(
        produced_offset, 0,
        "the first committed record gets offset 0"
    );

    // Consume from offset 0 and assert the produced value round-trips back
    // through the committed log (Requirement 3.4 end-to-end coverage).
    let consumed = client
        .consume(v1::ConsumeRequest {
            topic: "orders".to_string(),
            partition: 0,
            offset: 0,
            max_count: None,
        })
        .await
        .expect("consume succeeds")
        .into_inner();

    assert_eq!(consumed.records.len(), 1, "exactly one committed record");
    assert_eq!(
        consumed.next_offset, 1,
        "next offset advances past the record"
    );
    let record = consumed.records[0]
        .record
        .as_ref()
        .expect("consumed record carries a payload");
    assert_eq!(consumed.records[0].offset, 0, "the record sits at offset 0");
    assert_eq!(
        record.value, value,
        "the consumed value matches what was produced"
    );
}
