//! End-to-end cluster smoke test for the `vela-server` node daemon (task 18.4).
//!
//! Where [`integration.rs`](./integration.rs) exercises the request/response
//! surface of a bound listener, this smoke test drives the *consensus* path of a
//! live node over real `tokio` timers and `tonic` transport: it brings a node
//! up via [`serve`](vela_server::serve), waits for the per-partition Raft group
//! to **elect a leader on the real clock** (election fires in the randomized
//! 150–300 ms window, Requirement 7.2), then runs a full
//! **produce → commit → consume** round-trip — confirming that the
//! election → replication → produce → consume pipeline validated deterministically
//! in the `vela-raft` `SimCluster` harness behaves identically when driven by
//! wall-clock timers and gRPC (Requirement 14.5). A second node is brought up as
//! a configured peer and answered over `Heartbeat` to touch the multi-node
//! discovery substrate (Requirement 14.3).
//!
//! ## Scope notes
//!
//! The current server seeds each node as the sole member of its own cluster, so
//! a topic created on a node has `replication_factor = 1` and its single-replica
//! Raft group commits as soon as the lone replica appends — a majority of one.
//! This is the largest end-to-end produce/consume the server supports today, and
//! it is sufficient to prove the real-timer election + replication + produce +
//! consume pipeline. A genuine multi-node `rf>1` produce/consume (records
//! replicated to followers on *other* nodes and committed by a cross-node
//! majority) requires membership wiring that lets nodes co-host one topic's
//! partition replicas; that wiring was intentionally scoped minimally (each node
//! seeds as sole member), so cross-node `rf>1` produce/consume is **deferred**.
//! The deterministic `SimCluster` property tests in `vela-raft` already cover
//! multi-replica election, replication, commit advancement, and log matching
//! across a group; this test confirms the same logic runs on real timers, and
//! the second-node `Heartbeat` reachability check exercises the server-to-server
//! transport a future multi-member topic would replicate over.
//!
//! All waits are bounded retries with short sleeps (no unbounded blocking), so a
//! genuinely broken node fails the test promptly rather than hanging.

use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_proto::v1::vela_peer_client::VelaPeerClient;

/// Monotonic counter making per-test data directories unique within a process.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, writable data directory under the system temp directory for one
/// test node.
///
/// Topics default to the Durable backend, so creating a topic opens a real
/// `DurableWal` under the node's data directory; rooting that beneath a unique
/// temp directory lets the smoke test drive a genuine durable produce/consume
/// round-trip without depending on a fixed, possibly-unwritable location. The
/// directory is created lazily by the WAL; cleanup is left to the OS temp reaper
/// because the spawned server task outlives the test and holds it open.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("vela-server-smoke-{}-{n}-{nanos}", process::id()))
        .to_string_lossy()
        .into_owned()
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address the server should bind. Mirrors the helper
/// in `integration.rs`; there is a small race between releasing the port and
/// `serve` re-binding it, but on localhost in a test this is reliable.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated [`Config`] through the same CLI path the daemon uses, so
/// the test exercises real configuration parsing.
fn config(node_id: &str, addr: SocketAddr, peers: &[&str], rf: u32) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        advertised_addr: None,
        peers: peers.iter().map(|p| p.to_string()).collect(),
        replication_factor: Some(rf.to_string()),
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

/// Connect a `VelaPeer` to `addr` with the same bounded retry as
/// [`connect_client`].
async fn connect_peer(addr: SocketAddr) -> VelaPeerClient<Channel> {
    let url = format!("http://{addr}");
    for _ in 0..100 {
        if let Ok(client) = VelaPeerClient::connect(url.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("VelaPeer at {addr} did not become reachable");
}

/// Poll `FindLeader` for `(topic, partition)` until a leader is reported or the
/// bounded attempt budget is exhausted, returning the elected leader's node id.
///
/// Election on the real clock fires in the randomized 150–300 ms window
/// (Requirement 7.2), so ~50 attempts at 20 ms (≈1 s) comfortably covers it
/// while staying bounded — a partition that never elects fails the test instead
/// of hanging.
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

/// Issue `CreateTopic`, retrying while `__meta/0` has not yet elected a leader.
///
/// With the inline `BootstrapClock` bootstrap removed, even a single-node
/// metadata group elects through the normal asynchronous election path, so a
/// `CreateTopic` proposal is rejected with a "no metadata leader available"
/// status until that election fires (Requirement 4.2). Retrying within a
/// bounded window waits out the randomized 150–300 ms election timeout and
/// returns the first committed create; a group that never elects exhausts the
/// budget and fails the test rather than hanging.
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

/// Requirement 14.5 / 7.2 — end-to-end consensus over real timers and tonic.
///
/// Bring up a single `velad` node, create a 1-partition topic, wait for the
/// partition's Raft group to elect a leader on the real clock, then produce a
/// record and assert it commits at offset 0, and consume from offset 0 and
/// assert the exact record comes back. This walks the full
/// election → replication → produce → consume pipeline against wall-clock timers,
/// confirming the `SimCluster`-validated logic matches real-clock behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_cluster_elects_then_produces_and_consumes_end_to_end() {
    let addr = free_addr();
    spawn_server(config("node-a", addr, &[], 1));

    let mut client = connect_client(addr).await;

    // Create a single-partition topic. With rf=1 the sole node is the only
    // replica, so its Raft group is a majority of one and commits locally.
    // The 1-voter `__meta/0` group self-elects asynchronously, so retry the
    // create until that metadata election fires.
    let created = create_topic_awaiting_metadata_leader(&mut client, "smoke", 1).await;
    assert_eq!(created.name, "smoke");
    assert_eq!(created.partition_count, 1);

    // Wait for the real-clock election to elect this node as the leader of p0.
    let leader = await_leader(&mut client, "smoke", 0).await;
    assert_eq!(
        leader, "node-a",
        "the sole node leads its own single-replica partition"
    );

    // Produce a record carrying both a key and a value; it must commit at the
    // first offset, 0 (Requirement 4.4, 4.7). The leader was just confirmed
    // elected, so a single attempt suffices, but we retry briefly to absorb the
    // instant between FindLeader reporting a leader and the produce path
    // observing the same.
    //
    // NOTE: the in-memory log persists only a record's **value** bytes this
    // milestone — keys are deliberately not persisted (documented in
    // `vela-server`'s `convert.rs` and covered by its unit tests). So consume
    // below asserts the value round-trips exactly while the key comes back
    // absent; producing *with* a key still exercises the key-carrying produce
    // path (including the 1 MiB size check, which counts key + value bytes).
    let key = b"order-key".to_vec();
    let value = b"order-payload".to_vec();
    let mut produced_offset = None;
    for _ in 0..50 {
        match client
            .produce(v1::ProduceRequest {
                topic: "smoke".to_string(),
                partition: 0,
                record: Some(v1::Record {
                    key: Some(key.clone()),
                    value: value.clone(),
                }),
            })
            .await
        {
            Ok(response) => {
                produced_offset = Some(response.into_inner().offset);
                break;
            }
            // The only expected transient is a not-yet-committed/leader race; any
            // produce that keeps failing past the budget fails the test below.
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let produced_offset = produced_offset.expect("produce commits within the bounded window");
    assert_eq!(
        produced_offset, 0,
        "the first committed record gets offset 0"
    );

    // Consume from offset 0 and assert the exact produced record round-trips
    // back through the committed log (Requirement 5.1, 5.2).
    let consumed = client
        .consume(v1::ConsumeRequest {
            topic: "smoke".to_string(),
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
    // Keys are not persisted this milestone, so the round-tripped record is
    // value-only regardless of the key supplied at produce time.
    assert_eq!(
        record.key, None,
        "the milestone log persists value bytes only; the key comes back absent"
    );
}

/// Requirement 14.3 — a two-node cluster's discovery substrate is reachable.
///
/// Two nodes are configured as each other's peer, so both membership loops dial
/// the other on startup. We assert each node answers `Heartbeat` over the peer
/// service (self-identifying correctly) and keeps serving client traffic, which
/// exercises the server-to-server transport multi-node discovery rides on. The
/// single-node-seeded server does not co-host one topic across both nodes, so a
/// cross-node `rf>1` produce/consume is deferred (see module scope notes); the
/// reachability check is the multi-node aspect the current server supports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_node_cluster_peers_are_reachable_for_discovery() {
    let addr_a = free_addr();
    let addr_b = free_addr();

    spawn_server(config("node-a", addr_a, &[&addr_b.to_string()], 1));
    spawn_server(config("node-b", addr_b, &[&addr_a.to_string()], 1));

    let mut peer_a = connect_peer(addr_a).await;
    let mut peer_b = connect_peer(addr_b).await;

    let reply_a = peer_a
        .heartbeat(v1::HeartbeatRequest {
            node_id: "node-b".to_string(),
        })
        .await
        .expect("node-a answers heartbeats")
        .into_inner();
    assert_eq!(reply_a.node_id, "node-a");

    let reply_b = peer_b
        .heartbeat(v1::HeartbeatRequest {
            node_id: "node-a".to_string(),
        })
        .await
        .expect("node-b answers heartbeats")
        .into_inner();
    assert_eq!(reply_b.node_id, "node-b");

    // Each node still serves client traffic while its membership loop runs.
    // Topic admin in a multi-node cluster now commits through the dedicated
    // metadata Raft group's elected leader (wired in a later task) rather than a
    // node-local single-node `__meta`, so this smoke test asserts client
    // reachability here and leaves multi-node create/produce/consume to the
    // cross-node integration tests.
    let mut client_a = connect_client(addr_a).await;
    client_a
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("node-a serves clients alongside membership");
}
