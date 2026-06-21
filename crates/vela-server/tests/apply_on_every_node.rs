//! Apply-on-every-node and driver-presence integration test (task 9.6).
//!
//! This test pins down two consequences of agreeing the topic catalogue through
//! the dedicated `__meta/0` metadata Raft group, the pair the
//! cross-node-metadata-propagation feature exists to guarantee once a
//! `CreateTopic` **commits**:
//!
//! 1. **Apply on every node (Requirement 5.3 — convergence).** A topic created
//!    against the metadata leader is replicated and committed through `__meta/0`,
//!    and *every* node applies that committed `ClusterCommand` to its served
//!    `ClusterMetadata`. So `ListTopics` on every node — leader and followers
//!    alike — eventually shows the topic with the same partition count.
//! 2. **Driver presence on every replica (Requirement 6.1).** Applying the
//!    committed create makes each node's `Partition_Reconciler` start a
//!    `Partition_Driver` for every partition whose `Replica_Set` contains that
//!    node. With `replication_factor = 3` on a 3-node cluster, every node is a
//!    replica of every partition, so every node must run a driver for every
//!    partition.
//!
//! Driver presence is asserted **through the public API** via `FindLeader`: a
//! node answers `FindLeader` with a leader **only if it hosts that partition's
//! driver** (`VelaClientService::find_leader` resolves the live leader through
//! `node.handle(topic, partition)`, which returns `None` when the node runs no
//! driver for the partition). So a node resolving `FindLeader` to an elected
//! leader is direct evidence that its reconciler started the partition's driver.
//! Requiring *every* replica node to resolve the *same* leader for a partition
//! therefore proves every replica is running its driver and the partition's Raft
//! group reached quorum across nodes (Requirement 6.1, and the 7.1/7.2 quorum it
//! enables).
//!
//! The cluster is a **3-node in-process cluster** over real `tokio` timers and
//! `tonic` transport, each node configured with the other two as `id@addr` peers
//! and `replication_factor = 3`, so `__meta/0` and each topic partition are
//! genuine cluster-wide Raft groups. All waits are bounded retries with short
//! sleeps, so a broken cluster fails the test promptly rather than hanging.
//! Topic-admin redirects are followed manually (the create chases `NotLeader`
//! hints to the metadata leader), keeping the test in full control of which node
//! each request is aimed at.

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
        .join(format!("vela-server-apply-{}-{n}-{nanos}", process::id()))
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
/// consistent numeric raft id, so the metadata and partition Raft groups address
/// one another's replicas and report leaders consistently across the cluster.
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

/// Create `topic` by starting at `ids[0]` and following `NotLeader` redirects to
/// the metadata leader, exactly as a redirect-following client would (Raft §8).
/// Returns the created [`v1::TopicInfo`] reported by the leader that committed
/// it (Requirement 4.1, 4.3).
async fn create_topic_via_leader(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    topic: &str,
    partitions: u32,
) -> v1::TopicInfo {
    let mut current = ids[0].clone();
    for _ in 0..400 {
        match create_topic_attempt(clients[&current].clone(), topic, partitions).await {
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

/// One `FindLeader` lookup against `client`. `Ok(Some(id))` names the partition's
/// live leader, `Ok(None)` means this node hosts the partition's replica but it
/// has no elected leader yet (or this node runs no driver for it), and `Err(_)`
/// means the topic is not yet visible on this node.
async fn find_leader(
    mut client: VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
) -> Result<Option<String>, tonic::Status> {
    let response = client
        .find_leader(v1::FindLeaderRequest {
            topic: topic.to_string(),
            partition,
        })
        .await?
        .into_inner();
    Ok(response.leader)
}

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

/// Wait until **every** node in `ids` lists `topic` with `expected_partitions`
/// partitions, proving the committed create was applied on every node and the
/// nodes converged to one catalogue (Requirement 5.3).
async fn await_topic_on_all_nodes(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    topic: &str,
    expected_partitions: u32,
) {
    for _ in 0..400 {
        let mut all = true;
        for id in ids {
            match list_topics(clients[id].clone()).await.get(topic) {
                Some(info) if info.partition_count == expected_partitions => {}
                _ => {
                    all = false;
                    break;
                }
            }
        }
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("topic {topic} was not applied with {expected_partitions} partitions on every node");
}

/// Wait until every node agrees on a single live leader for `(topic, partition)`
/// and return it, requiring *all* `replicas` to report the *same* leader.
///
/// Because `FindLeader` resolves a leader only on a node that **hosts the
/// partition's driver**, every replica reporting the same elected leader is
/// direct evidence that each replica node started the partition's driver
/// (Requirement 6.1) and that the partition's Raft group reached quorum across
/// nodes (Requirement 7.1, 7.2).
async fn await_driver_present_on_all_replicas(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    replicas: &[String],
    topic: &str,
    partition: u32,
) -> String {
    for _ in 0..400 {
        let mut leaders = Vec::with_capacity(replicas.len());
        for id in replicas {
            match find_leader(clients[id].clone(), topic, partition).await {
                Ok(Some(leader)) => leaders.push(leader),
                _ => break,
            }
        }
        if leaders.len() == replicas.len() && leaders.iter().all(|l| *l == leaders[0]) {
            return leaders.swap_remove(0);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "partition {topic}/{partition} did not have a driver on every replica reporting one \
         agreed leader in time"
    );
}

/// Requirement 5.3, 6.1 — after a `CreateTopic` commits through `__meta/0`, every
/// node applies it (so `ListTopics` shows the topic everywhere) and every replica
/// node runs the partition's driver (so `FindLeader` resolves on each of them).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_applies_on_every_node_and_starts_drivers_on_every_replica() {
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

    // One client per node, keyed by node id.
    let mut clients = HashMap::new();
    for i in 0..3 {
        clients.insert(ids[i].clone(), connect_client(addrs[i]).await);
    }

    // --- Create a topic, following NotLeader redirects to the metadata leader.
    // Three partitions exercise convergence over several catalogue entries; with
    // rf=3 on three nodes every partition is replicated by every node.
    let topic = "orders";
    let partitions = 3;
    let created = create_topic_via_leader(&clients, &ids, topic, partitions).await;
    assert_eq!(created.name, topic);
    assert_eq!(created.partition_count, partitions);

    // --- Requirement 5.3: the committed create is applied on EVERY node. ------
    await_topic_on_all_nodes(&clients, &ids, topic, partitions).await;

    // Read the committed catalogue (replica assignments) back from the leader's
    // view via any node — convergence above guarantees they all agree.
    let info = list_topics(clients[&ids[0]].clone())
        .await
        .remove(topic)
        .expect("the applied topic is listed");
    assert_eq!(
        info.partitions.len(),
        partitions as usize,
        "the applied topic carries all its partitions"
    );

    // --- Requirement 6.1: each replica node runs the partition's driver. ------
    // For every partition, assert that every node in its Replica_Set resolves
    // FindLeader to one agreed elected leader — only a node hosting the driver
    // answers with a leader, so this proves driver presence on each replica.
    for partition in &info.partitions {
        assert_eq!(
            partition.replicas.len(),
            ids.len(),
            "with rf=3 on a 3-node cluster every node replicates partition {}",
            partition.index
        );
        for replica in &partition.replicas {
            assert!(
                ids.contains(replica),
                "partition {}'s replica {replica} is a configured cluster node",
                partition.index
            );
        }

        let leader = await_driver_present_on_all_replicas(
            &clients,
            &partition.replicas,
            topic,
            partition.index,
        )
        .await;
        assert!(
            ids.contains(&leader),
            "the elected leader of partition {} is one of the cluster nodes",
            partition.index
        );
    }
}
