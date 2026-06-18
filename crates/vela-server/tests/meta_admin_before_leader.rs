//! Integration test: topic-admin is rejected cleanly before a metadata leader
//! exists (task 9.2, Requirement 4.2).
//!
//! The cluster agrees its catalogue through one dedicated, cluster-wide Raft
//! group `("__meta", 0)` whose voters are **all** statically configured nodes
//! (design §1, §4). When fewer than a majority of those voters are running or
//! mutually reachable, that group can elect **no** leader and therefore commit
//! **no** `ClusterCommand` (Requirement 2.5). Topic-admin
//! (`CreateTopic` / `DeleteTopic`) routes its proposal to the metadata leader
//! and never commits on a non-leader (design §4); so with no leader, the
//! request must fail **cleanly** rather than commit nowhere or hang (H3):
//!
//! - It returns promptly — the metadata replica is not the leader, so the
//!   propose path replies `NotLeader { leader: None }` immediately instead of
//!   appending and awaiting a commit that can never happen (no
//!   `COMMIT_TIMEOUT` wait, no unbounded block).
//! - That outcome surfaces as the "no metadata leader is currently available"
//!   error — wire classification [`v1::ErrorCode::NotLeader`] with no leader
//!   hint (`service.rs::admin_error_to_status`, Requirement 4.2).
//! - Nothing is committed.
//!
//! ## How "no metadata leader" is staged
//!
//! `node-a` is configured with **two dead peers** (reserved-but-unbound
//! localhost ports). That makes `("__meta", 0)` a **3-voter** group needing a
//! majority of 2, while only `node-a` is actually up — so the metadata group
//! can never assemble a majority and never elects a leader, exactly the
//! condition Requirement 4.2 covers. The dead peers never bind, so `node-a`'s
//! metadata replica campaigns and fails forever, staying a leaderless
//! candidate/follower.
//!
//! The topic's replication factor is **1** so that admission validation (which
//! checks available members against the replication factor, and a fresh node
//! seeds itself as the sole available member) passes and a create reaches the
//! leader-routed propose path — the behaviour under test — rather than
//! short-circuiting on `InsufficientNodes`. The metadata voter set (3, from the
//! configured peers) is independent of this topic replication factor.
//!
//! ## Why only the create case
//!
//! `DeleteTopic`'s leader-routed rejection would need a topic *present* in the
//! served catalogue (an absent-topic delete is an idempotent no-op, H2, that
//! proposes nothing and never reaches the leader check). With no metadata
//! leader no create can commit, and `SyncMetadata` is now a reserved no-op that
//! does not adopt a pushed snapshot (Requirement 1.3) — so there is no way to
//! seed a present topic on a leaderless node. The create-rejection case below,
//! which depends on no seeding, fully exercises the leader-routed propose
//! path's "no metadata leader" rejection (Requirement 4.2).

use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;
use tonic::Code;

use vela_server::{convert, serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;

/// Monotonic counter making per-test data directories unique within a process.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, writable data directory under the system temp directory for one
/// test node. Mirrors the helper in `integration.rs` / `cluster_smoke.rs`: the
/// durable `__meta` WAL is rooted beneath a unique temp directory so each test
/// node recovers a clean, empty metadata log. Cleanup is left to the OS temp
/// reaper because the spawned server task outlives the test.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!(
            "vela-server-meta-admin-{}-{n}-{nanos}",
            process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address. Used both for the node's own listener and,
/// crucially here, to stand up addresses that nothing is listening on so the
/// configured metadata peers are unreachable (dead voters).
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated [`Config`] through the same CLI path the daemon uses, so
/// the test exercises real configuration parsing. `peers` become the metadata
/// group's co-voters; `rf` is the topic replication factor.
fn config(node_id: &str, addr: SocketAddr, peers: &[&str], rf: u32) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
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

/// The bounded wait for an admin call to return. Deliberately **larger** than
/// the driver's `COMMIT_TIMEOUT_MS` (5 s): if a regression ever routed this
/// no-leader case through the append-and-await-commit path instead of replying
/// `NotLeader` immediately, we still want to observe the resulting (clean)
/// error rather than have the test's own bound fire first and mask it. In the
/// correct implementation the call returns near-instantly.
const ADMIN_DEADLINE: Duration = Duration::from_secs(15);

/// Let the metadata replica fire several election timeouts (the randomized
/// window is ~150-300 ms) and fail each for lack of a majority, so the
/// leaderless condition under test is genuine rather than mere not-yet-started
/// startup before the first election.
const SETTLE: Duration = Duration::from_millis(1500);

/// Assert a failed admin RPC is the clean "no metadata leader available"
/// rejection (Requirement 4.2): gRPC `FailedPrecondition`, a typed
/// [`v1::VelaError`] classified [`v1::ErrorCode::NotLeader`] with **no** leader
/// hint and a message naming the missing metadata leader.
fn assert_no_metadata_leader(status: &tonic::Status, op: &str) {
    assert_eq!(
        status.code(),
        Code::FailedPrecondition,
        "{op} should fail as a leadership precondition, got code {:?}: {}",
        status.code(),
        status.message()
    );

    let vela_error = convert::vela_error_from_status(status)
        .unwrap_or_else(|| panic!("{op} status should carry a typed VelaError in its details"));
    assert_eq!(
        vela_error.code,
        v1::ErrorCode::NotLeader as i32,
        "{op} should be classified NotLeader, got {:?}",
        vela_error.code
    );
    assert_eq!(
        vela_error.leader, None,
        "{op} carries no leader hint when no metadata leader exists"
    );
    assert!(
        vela_error.message.contains("no metadata leader"),
        "{op} message should name the missing metadata leader, got: {}",
        vela_error.message
    );
}

/// Bring up `node-a` with two dead metadata co-voters, returning a connected
/// client once the (forever-leaderless) 3-voter metadata group has had time to
/// campaign and fail.
async fn node_without_metadata_leader(rf: u32) -> (SocketAddr, VelaClientClient<Channel>) {
    let addr = free_addr();
    // Two reserved-but-unbound addresses: configured metadata voters that are
    // never reachable, so `("__meta", 0)` is a 3-voter group with only one
    // voter up — short of the majority of 2 it needs to elect a leader.
    let dead_peer_1 = free_addr();
    let dead_peer_2 = free_addr();
    spawn_server(config(
        "node-a",
        addr,
        &[&dead_peer_1.to_string(), &dead_peer_2.to_string()],
        rf,
    ));

    let client = connect_client(addr).await;
    tokio::time::sleep(SETTLE).await;
    (addr, client)
}

/// Requirement 4.2 (H3) — before `("__meta", 0)` has elected a leader,
/// `CreateTopic` fails cleanly with "no metadata leader available" and commits
/// nothing: no hang, no commit-nowhere.
///
/// Topic replication factor 1 lets admission validation pass (the node is its
/// own sole available member), so the request reaches the leader-routed propose
/// path; the leaderless 3-voter metadata group then rejects it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topic_rejected_cleanly_before_metadata_leader_exists() {
    let (_addr, mut client) = node_without_metadata_leader(1).await;

    // The catalogue starts empty.
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds against a bound listener")
        .into_inner()
        .topics;
    assert!(topics.is_empty(), "a fresh node lists no topics");

    // CreateTopic must return promptly with the no-metadata-leader rejection and
    // must not hang. The bounded wait exceeds COMMIT_TIMEOUT so a clean error is
    // observed rather than the test's own deadline firing.
    let create = tokio::time::timeout(
        ADMIN_DEADLINE,
        client.create_topic(v1::CreateTopicRequest {
            name: "orders".to_string(),
            partitions: 1,
            log_backend: v1::LogBackend::Unspecified as i32,
        }),
    )
    .await
    .expect("create_topic must return promptly, not hang, when no metadata leader exists");
    let status = create.expect_err("create_topic must be rejected with no metadata leader");
    assert_no_metadata_leader(&status, "create_topic");

    // Nothing was committed: the catalogue is still empty (no commit-nowhere).
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics;
    assert!(
        topics.is_empty(),
        "no topic is committed while the metadata group has no leader, got: {topics:?}"
    );
}

// Requirement 4.2 (H3) — before `("__meta", 0)` has elected a leader,
// `DeleteTopic` of an existing topic would fail cleanly with "no metadata
// leader available" and commit nothing. That sub-case is **omitted** here: it
// requires a topic *present* in the served catalogue, but with no metadata
// leader no create can commit and `SyncMetadata` is a reserved no-op that does
// not adopt a pushed snapshot (Requirement 1.3), so a present topic cannot be
// seeded on a leaderless node. The create-rejection test above exercises the
// same leader-routed "no metadata leader" rejection without seeding.
