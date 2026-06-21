//! Concurrent-admin-from-different-nodes convergence integration test (task 9.7).
//!
//! This test captures the capability the architectural pivot to a single,
//! dedicated `__meta/0` metadata Raft group *gained*: topic-admin requests
//! issued **concurrently against different nodes** all serialize through the one
//! metadata leader, every one of them commits, and every node converges to the
//! same catalogue. Before the pivot, an admin change committed only to the
//! node-local metadata group it was sent to, so two creates aimed at two
//! different nodes could never agree on a single catalogue.
//!
//! The flow it exercises:
//!
//! 1. **Serialize through the single metadata leader (Requirement 3.1).** Two
//!    `CreateTopic` requests for *different* topic names are issued concurrently,
//!    each starting at a *different* node and each following `NotLeader`
//!    redirects to whichever node leads `__meta/0` (Raft §8). Both append a
//!    `ClusterCommand` to the one metadata log and both commit — there is a
//!    single leader appending both entries, so the two creates are ordered by
//!    the metadata log rather than racing two independent catalogues.
//! 2. **Convergence to one catalogue (Requirement 5.3).** Once both creates
//!    commit, *every* node applies both committed `ClusterCommand`s to its served
//!    `ClusterMetadata`, so `ListTopics` on all three nodes — leader and
//!    followers alike — eventually shows **both** topics with the same partition
//!    counts.
//!
//! The cluster is a **3-node in-process cluster** over real `tokio` timers and
//! `tonic` transport, each node configured with the other two as `id@addr` peers
//! and `replication_factor = 3`, so `__meta/0` is a genuine cluster-wide Raft
//! group that must elect one leader and replicate to a majority. All waits are
//! bounded retries with short sleeps, so a broken cluster fails the test
//! promptly rather than hanging. Topic-admin redirects are followed manually
//! (each create chases `NotLeader` hints to the metadata leader), keeping the
//! test in full control of which node each request starts at so the
//! "different nodes" precondition is exact.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as _;
use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;

/// Monotonic counter making per-test data directories unique within a process.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, writable data directory under the system temp directory for one
/// test node. Each node needs its own root so its durable `__meta` log and any
/// durable partition logs do not collide with a peer's.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!(
            "vela-server-concurrent-{}-{n}-{nanos}",
            process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address the server should bind. There is a small
/// race between releasing the port and `serve` re-binding it, but on localhost
/// in a test this is reliable.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated [`Config`] through the same CLI path the daemon uses.
///
/// `peers` are `id@host:port` strings so each peer is known by the *same* stable
/// id it uses for itself — that shared identity is what lets every node derive a
/// consistent numeric raft id, so the metadata Raft group addresses one
/// another's replicas and reports the leader consistently across the cluster.
fn config(node_id: &str, addr: SocketAddr, peers: &[String], rf: u32) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        advertised_addr: None,
        peers: peers.to_vec(),
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

/// Decode the typed [`v1::VelaError`] a server packs into a [`tonic::Status`]'s
/// details, if present.
fn vela_error(status: &tonic::Status) -> Option<v1::VelaError> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    v1::VelaError::decode(details).ok()
}

/// If `status` is a `NotLeader` redirect, return its optional leader-id hint
/// (`Some(Some(id))` with a known leader, `Some(None)` when no leader is yet
/// known); return `None` for any non-`NotLeader` status.
fn not_leader_hint(status: &tonic::Status) -> Option<Option<String>> {
    let error = vela_error(status)?;
    (error.code == v1::ErrorCode::NotLeader as i32).then_some(error.leader)
}

/// One `CreateTopic` attempt against `client`. Returns the created topic on
/// success, or the raw [`tonic::Status`] so the caller can classify a
/// `NotLeader` redirect.
async fn create_topic_attempt(
    mut client: VelaClientClient<Channel>,
    name: &str,
    partitions: u32,
) -> Result<v1::TopicInfo, tonic::Status> {
    let response = client
        .create_topic(v1::CreateTopicRequest {
            name: name.to_string(),
            partitions,
            log_backend: v1::LogBackend::Unspecified as i32,
        })
        .await?
        .into_inner();
    Ok(response
        .topic
        .expect("a successful CreateTopic returns the created topic"))
}

/// Create `topic` by starting at node `start` and following `NotLeader`
/// redirects to the metadata leader, exactly as a redirect-following client
/// would (Raft §8). Returns the created [`v1::TopicInfo`] reported by the leader
/// that committed it (Requirement 3.1, 4.1, 4.3).
///
/// Taking an explicit `start` node lets the caller aim two concurrent creates at
/// two *different* nodes, so they demonstrably serialize through the one
/// metadata leader rather than committing to two independent node-local groups.
async fn create_topic_via_leader(
    clients: HashMap<String, VelaClientClient<Channel>>,
    start: String,
    topic: String,
    partitions: u32,
) -> v1::TopicInfo {
    let mut current = start;
    for _ in 0..400 {
        match create_topic_attempt(clients[&current].clone(), &topic, partitions).await {
            Ok(created) => return created,
            Err(status) => match not_leader_hint(&status) {
                // Redirected to the known leader: aim the next attempt there.
                Some(Some(leader)) => current = leader,
                // No metadata leader elected yet: wait and retry the same node.
                Some(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                None => panic!("unexpected error creating topic {topic}: {status:?}"),
            },
        }
    }
    panic!("metadata group did not elect a leader / commit the create for {topic} in time");
}

/// A node's converged catalogue reduced to the dimensions metadata commits:
/// each topic's name, partition count, and the ordered replica set of every
/// partition. Two nodes holding an equal [`CatalogueShape`] have converged.
type CatalogueShape = Vec<(String, u32, Vec<Vec<String>>)>;

/// List the [`v1::TopicInfo`] a node currently serves, keyed by name.
async fn list_topics(mut client: VelaClientClient<Channel>) -> HashMap<String, v1::TopicInfo> {
    client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics
        .into_iter()
        .map(|t| (t.name.clone(), t))
        .collect()
}

/// Wait until **every** node in `ids` lists each `(topic, partition_count)` in
/// `expected`, proving both committed creates were applied on every node and the
/// nodes converged to one catalogue containing both topics (Requirement 5.3).
async fn await_topics_on_all_nodes(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    expected: &[(String, u32)],
) {
    for _ in 0..400 {
        let mut all = true;
        'nodes: for id in ids {
            let catalogue = list_topics(clients[id].clone()).await;
            for (topic, partitions) in expected {
                match catalogue.get(topic) {
                    Some(info) if info.partition_count == *partitions => {}
                    _ => {
                        all = false;
                        break 'nodes;
                    }
                }
            }
        }
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nodes did not converge on a catalogue containing every expected topic in time");
}

/// Requirement 3.1, 5.3 — two `CreateTopic` requests for different topics issued
/// concurrently against different nodes both serialize through the single
/// metadata leader, both commit, and every node converges to the same catalogue
/// containing both topics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_on_different_nodes_converge_on_one_catalogue() {
    // --- Stand up a 3-node cluster, each node peering the other two. ---------
    let ids = [
        "node-a".to_string(),
        "node-b".to_string(),
        "node-c".to_string(),
    ];
    let addrs = [free_addr(), free_addr(), free_addr()];

    // For each node, the peer list is the other two as `id@addr`.
    for i in 0..3 {
        let peers: Vec<String> = (0..3)
            .filter(|&j| j != i)
            .map(|j| format!("{}@{}", ids[j], addrs[j]))
            .collect();
        spawn_server(config(&ids[i], addrs[i], &peers, 3));
    }

    // One client per node, keyed by node id. Channels are cheap to clone and
    // share the underlying connection, so each concurrent create gets its own
    // copy of the full client map to follow redirects with.
    let mut clients = HashMap::new();
    for i in 0..3 {
        clients.insert(ids[i].clone(), connect_client(addrs[i]).await);
    }

    // --- Issue two creates concurrently, each starting at a DIFFERENT node. ---
    // `orders` starts at node-a and `payments` starts at node-b. Whichever node
    // leads `__meta/0`, at least one of these starts at a follower and must be
    // redirected — and both entries are appended by the one leader, so they
    // serialize through a single metadata log (Requirement 3.1).
    let orders = tokio::spawn(create_topic_via_leader(
        clients.clone(),
        ids[0].clone(),
        "orders".to_string(),
        3,
    ));
    let payments = tokio::spawn(create_topic_via_leader(
        clients.clone(),
        ids[1].clone(),
        "payments".to_string(),
        2,
    ));

    let created_orders = orders.await.expect("the orders create task does not panic");
    let created_payments = payments
        .await
        .expect("the payments create task does not panic");

    // Both creates committed and reported their own topic back (Requirement 3.1).
    assert_eq!(created_orders.name, "orders");
    assert_eq!(created_orders.partition_count, 3);
    assert_eq!(created_payments.name, "payments");
    assert_eq!(created_payments.partition_count, 2);

    // --- Requirement 5.3: every node converges on a catalogue with BOTH. ------
    let expected = [("orders".to_string(), 3), ("payments".to_string(), 2)];
    await_topics_on_all_nodes(&clients, &ids, &expected).await;

    // Read the converged catalogue back from every node and assert they are
    // byte-for-byte identical on the dimensions metadata commits: each topic's
    // partition count and the ordered replica set of every partition. Equal
    // catalogues on all three nodes is the convergence the pivot guarantees.
    let mut reference: Option<CatalogueShape> = None;
    for id in &ids {
        let catalogue = list_topics(clients[id].clone()).await;
        let mut shape: CatalogueShape = catalogue
            .into_values()
            .map(|info| {
                let replica_sets = info.partitions.iter().map(|p| p.replicas.clone()).collect();
                (info.name, info.partition_count, replica_sets)
            })
            .collect();
        // Order-independent comparison: sort topics by name for a stable shape.
        shape.sort_by(|a, b| a.0.cmp(&b.0));

        // Both topics are present on this node (Requirement 5.3).
        let names: Vec<&String> = shape.iter().map(|(name, _, _)| name).collect();
        assert!(
            names.contains(&&"orders".to_string()) && names.contains(&&"payments".to_string()),
            "node {id} serves both concurrently-created topics, got {names:?}"
        );

        match &reference {
            None => reference = Some(shape),
            Some(expected_shape) => assert_eq!(
                &shape, expected_shape,
                "node {id}'s catalogue diverges from the others; all nodes must converge (Req 5.3)"
            ),
        }
    }
}
