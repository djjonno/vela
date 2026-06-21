//! Restart recovery of the **full** committed catalogue for the `vela-server`
//! node daemon (task 9.4).
//!
//! **Property 6 — Durable recovery of the full catalogue.** *For any* node
//! restarted after a set of committed admin commands, the node recovers the
//! full committed catalogue — including topics originated by peers — from its
//! durable metadata log and re-spawns a driver for every recovered partition
//! whose replica set contains it (Requirements 9.2, 9.3).
//!
//! Where [`cold_restart`](./cold_restart.rs) and
//! [`durable_driver_restart`](./durable_driver_restart.rs) restart a *single*
//! node and assert it recovers the topics it created itself, this test exercises
//! the cross-node half of recovery: topics are created through the dedicated
//! `__meta/0` metadata Raft group of a **three-node** cluster, so each
//! `CreateTopic` commits only once it has replicated to a majority of the
//! metadata voters (Requirement 3.2) and every voter persists it to its own
//! durable `__meta` log before acknowledging the replication (Requirement 9.1).
//! One node is then fully torn down and **restarted on the same data
//! directory** and the same address. The restarted node must:
//!
//! - recover the **whole** committed catalogue from its durable metadata log —
//!   not only the topics it may have originated but every topic any node created
//!   (`ListTopics` shows them all), proving recovery reads the replicated log
//!   rather than only locally-originated state (Requirement 9.2); and
//! - re-spawn a partition driver for every recovered partition whose
//!   `Replica_Set` contains it, so each partition becomes serveable again —
//!   asserted by `FindLeader` resolving to a live Raft-elected leader on the
//!   restarted node, which can only happen once its driver is running and has
//!   rejoined the partition's Raft group (Requirement 9.3).
//!
//! ## Why three nodes on three runtimes
//!
//! With `replication_factor = 3` and three configured nodes, every topic's lone
//! partition is replicated on all three nodes, and the metadata group's voter
//! set is all three. A `CreateTopic` therefore commits through a genuine
//! cross-node majority and lands in every node's durable `__meta` log — so the
//! catalogue a restarted node recovers genuinely includes peer-originated
//! topics, which a single-node test cannot exercise.
//!
//! A `DurableWal` holds an exclusive lock on its data directory for its
//! lifetime, so a node can only reopen its paths once its previous incarnation's
//! WALs are dropped. To tear down **one** node without disturbing the other two,
//! each node runs on its **own** `tokio` runtime; dropping that runtime drops
//! every task the node spawned (its gRPC server, membership loops, the metadata
//! driver, and every partition driver) and the `NodeShared` they share, which in
//! turn drops every WAL and releases its lock. The surviving two nodes — a
//! majority — keep the cluster live across the restart, and the restarted node
//! rebinds the same address so its peers reconnect to it. A separate client
//! runtime drives the gRPC calls.
//!
//! All waits are bounded retries with short sleeps, so a genuinely broken node
//! fails the test promptly rather than hanging.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::runtime::Runtime;
use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;

/// Monotonic counter making temp-dir names unique within a single process.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// An owned temporary directory recursively removed when dropped.
///
/// Cleanup is best-effort so a removal failure never masks an assertion. Each
/// guard outlives every incarnation of its node, so it is removed only after
/// the node's WALs have been dropped and their locks released.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the unix epoch")
            .as_nanos();
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "vela-server-meta-restart-{tag}-{}-{unique}-{nanos}",
            process::id()
        );
        Self {
            path: std::env::temp_dir().join(name),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener. There is a small race before `serve` re-binds, but on localhost in
/// a test this is reliable; the restarted node deliberately rebinds the *same*
/// reserved address so its peers reconnect.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated multi-node [`Config`] through the same CLI path the daemon
/// uses. `peers` are the other nodes as `(id, addr)`, encoded as the
/// `id@host:port` form the daemon accepts so identities line up across the
/// cluster. `replication_factor = 3` makes every partition replicate on all
/// three nodes.
fn config(
    node_id: &str,
    addr: SocketAddr,
    peers: &[(&str, SocketAddr)],
    data_dir: &Path,
) -> Config {
    let peers = peers
        .iter()
        .map(|(id, addr)| format!("{id}@{addr}"))
        .collect();
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        advertised_addr: None,
        peers,
        replication_factor: Some("3".to_string()),
        data_dir: Some(data_dir.to_string_lossy().into_owned()),
    })
    .expect("valid test configuration")
}

/// A multi-threaded runtime for one node incarnation. Dropping it tears that
/// node down completely, releasing every WAL lock it held, without disturbing
/// the other nodes' runtimes.
fn incarnation_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build a tokio runtime for one node incarnation")
}

/// Spawn a node serving `config` on `runtime`, returning immediately while the
/// server runs on the runtime's background threads. An early `serve` return is a
/// real failure (including a failed durable bootstrap), surfaced as a panic on
/// the node's runtime.
fn spawn_node(runtime: &Runtime, config: Config) {
    runtime.spawn(async move {
        if let Err(error) = serve(config).await {
            panic!("server exited unexpectedly: {error}");
        }
    });
}

/// Connect a `VelaClient` to `addr`, retrying until the freshly spawned listener
/// accepts connections or a bounded budget elapses.
async fn connect_client(addr: SocketAddr) -> VelaClientClient<Channel> {
    let url = format!("http://{addr}");
    for _ in 0..200 {
        if let Ok(client) = VelaClientClient::connect(url.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("VelaClient at {addr} did not become reachable");
}

/// Create `name` (single partition) through the cluster, tolerating metadata
/// leadership churn.
///
/// A `CreateTopic` only commits on the current metadata leader; a request to a
/// follower is redirected with `NotLeader` and one sent while no leader is
/// elected yet is rejected with "no metadata leader available" (Requirement
/// 4.1, 4.2). Rather than parse the leader hint, the helper cycles the request
/// across every node's client and retries on any error until one node commits
/// it — the commit returns success only once the entry has replicated to a
/// majority of the metadata voters (Requirement 3.2, 3.4). A topic that already
/// exists is idempotent (`TopicExists`), so a retry after an indeterminate
/// commit timeout cannot corrupt the catalogue (H2).
async fn create_topic_on_cluster(clients: &mut [VelaClientClient<Channel>], name: &str) {
    for _ in 0..200 {
        for client in clients.iter_mut() {
            if client
                .create_topic(v1::CreateTopicRequest {
                    name: name.to_string(),
                    partitions: 1,
                    log_backend: v1::LogBackend::Durable as i32,
                })
                .await
                .is_ok()
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("topic {name} was not committed through the cluster within the bounded window");
}

/// The set of topic names a node currently serves from its applied catalogue.
async fn listed_topics(client: &mut VelaClientClient<Channel>) -> Vec<String> {
    let mut names: Vec<String> = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics RPC succeeds")
        .into_inner()
        .topics
        .into_iter()
        .map(|t| t.name)
        .collect();
    names.sort();
    names
}

/// Poll `client`'s `ListTopics` until it shows every name in `expected`, proving
/// the node has applied (and therefore durably persisted) every committed
/// `CreateTopic` before we tear it down.
async fn await_catalogue(client: &mut VelaClientClient<Channel>, expected: &[&str]) {
    let mut want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    want.sort();
    for _ in 0..200 {
        if listed_topics(client).await == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let have = listed_topics(client).await;
    panic!("node did not converge on the full catalogue; expected {want:?}, has {have:?}");
}

/// Poll `FindLeader` on `client` for `(topic, partition)` until it reports an
/// elected leader or the bounded budget is exhausted.
///
/// On the node hosting the partition's replica, a non-`None` answer means the
/// driver is running and has rejoined the partition's Raft group (it leads, or
/// it learned the leader from an `AppendEntries`); a node that never re-spawned
/// the driver would answer `None` forever. Election plus cross-node catch-up on
/// the real clock takes longer than a single election timeout, so the budget is
/// generous while staying bounded.
async fn await_leader(client: &mut VelaClientClient<Channel>, topic: &str, partition: u32) {
    for _ in 0..400 {
        let leader = client
            .find_leader(v1::FindLeaderRequest {
                topic: topic.to_string(),
                partition,
            })
            .await
            .expect("find_leader RPC succeeds")
            .into_inner()
            .leader;
        if leader.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("partition {topic}/{partition} did not resolve a leader on the restarted node");
}

/// Requirements 9.2, 9.3 / Property 6 — a node restarted after several topics
/// were committed through a three-node metadata Raft group recovers the **full**
/// committed catalogue (including peer-originated topics) from its durable
/// metadata log and re-spawns a driver for every recovered partition whose
/// replica set contains it (each partition becomes serveable again).
#[test]
fn restarted_node_recovers_full_catalogue_and_respawns_drivers() {
    // Topics committed through the cluster. Each commits through the metadata
    // group's elected leader (whichever node that is) and replicates to every
    // voter's durable `__meta` log, so they are peer-originated from the point
    // of view of any node that did not happen to lead at commit time.
    let topics = ["alpha", "beta", "gamma"];

    // Distinct durable data directories, one per node, outliving every
    // incarnation so locks are released before cleanup.
    let data_a = TempDir::new("node-a");
    let data_b = TempDir::new("node-b");
    let data_c = TempDir::new("node-c");

    // Reserve a stable address per node. The restarted node rebinds the SAME
    // address so its peers reconnect to it without reconfiguration.
    let addr_a = free_addr();
    let addr_b = free_addr();
    let addr_c = free_addr();

    let peers_a = [("node-b", addr_b), ("node-c", addr_c)];
    let peers_b = [("node-a", addr_a), ("node-c", addr_c)];
    let peers_c = [("node-a", addr_a), ("node-b", addr_b)];

    // Each node runs on its own runtime so it can be torn down independently.
    let rt_a = incarnation_runtime();
    let rt_b = incarnation_runtime();
    let rt_c = incarnation_runtime();

    spawn_node(&rt_a, config("node-a", addr_a, &peers_a, data_a.path()));
    spawn_node(&rt_b, config("node-b", addr_b, &peers_b, data_b.path()));
    spawn_node(&rt_c, config("node-c", addr_c, &peers_c, data_c.path()));

    // A dedicated runtime drives the client-side gRPC calls, independent of any
    // node's runtime so dropping a node does not disturb the test driver.
    let client_rt = incarnation_runtime();
    client_rt.block_on(async {
        let mut client_a = connect_client(addr_a).await;
        let mut client_b = connect_client(addr_b).await;
        let mut client_c = connect_client(addr_c).await;

        // Commit each topic through the cluster's metadata group. Cycling the
        // request across nodes drives it to the current metadata leader.
        let mut admins = [client_a.clone(), client_b.clone(), client_c.clone()];
        for topic in topics {
            create_topic_on_cluster(&mut admins, topic).await;
        }

        // Every node must converge on the full catalogue (Requirement 5.3); in
        // particular node-c, before we restart it, must have applied — and so
        // durably persisted — every committed CreateTopic, so its recovery has
        // the complete log to rebuild from.
        await_catalogue(&mut client_a, &topics).await;
        await_catalogue(&mut client_b, &topics).await;
        await_catalogue(&mut client_c, &topics).await;
    });

    // ---- Restart node-c on the SAME data directory and address. ----
    //
    // Dropping node-c's runtime cancels every task it spawned and drops the
    // `NodeShared` they share, releasing every `__meta`/partition WAL lock. The
    // surviving node-a and node-b are a majority, so the cluster stays live
    // across the restart.
    drop(rt_c);
    let rt_c2 = incarnation_runtime();
    spawn_node(&rt_c2, config("node-c", addr_c, &peers_c, data_c.path()));

    client_rt.block_on(async {
        let mut client_c = connect_client(addr_c).await;

        // (9.2) The restarted node rebuilds the WHOLE committed catalogue from
        // its durable metadata log, including topics it received only by
        // replication from a peer — `ListTopics` shows every topic any node
        // created, not just locally-originated state.
        await_catalogue(&mut client_c, &topics).await;
        let recovered = listed_topics(&mut client_c).await;
        let mut expected: Vec<String> = topics.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(
            recovered, expected,
            "the restarted node recovers the full committed catalogue from its \
             durable metadata log, including peer-originated topics (Req 9.2)"
        );

        // (9.3) The post-recovery reconcile re-spawns a partition driver for
        // every recovered partition whose replica set contains this node (with
        // rf=3 that is every topic's partition 0). Each becomes serveable again:
        // FindLeader on the restarted node resolves to a live Raft-elected
        // leader, which is only possible once its driver is running and has
        // rejoined the partition's Raft group.
        for topic in topics {
            await_leader(&mut client_c, topic, 0).await;
        }
    });

    // Tear the rest of the cluster down explicitly (also dropped at scope end).
    drop(rt_c2);
    drop(rt_a);
    drop(rt_b);
    drop(client_rt);
}
