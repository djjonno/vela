//! Delete-propagation integration test (task 9.8).
//!
//! This test pins down the delete half of agreeing the topic catalogue through
//! the dedicated `__meta/0` metadata Raft group — the symmetric counterpart of
//! the create flow proven by `cross_node_produce_consume.rs` and
//! `apply_on_every_node.rs`. It is the end-to-end evidence for **Requirement
//! 6.2**: when a `DeleteTopic` **commits** through `__meta/0`, every node applies
//! the committed `ClusterCommand` and its `Partition_Reconciler` **stops** the
//! deleted topic's drivers (the `running \ desired` side of the reconcile diff,
//! design §5).
//!
//! The flow it exercises on a **3-node in-process cluster** (real `tokio` timers,
//! `tonic` transport, `replication_factor = 3`, so `__meta/0` and the topic
//! partition are genuine cluster-wide Raft groups):
//!
//! 1. **Create + converge.** A topic is created by routing the `CreateTopic` to
//!    the metadata leader through `NotLeader` redirects (Raft §8). The test then
//!    waits until the topic is applied on *every* node (`ListTopics`) and every
//!    replica node runs the partition's driver — asserted through `FindLeader`,
//!    which resolves a leader only on a node that hosts the partition's driver
//!    (`VelaClientService::find_leader` consults `node.handle(topic, partition)`).
//!    This establishes the precondition: drivers are present everywhere.
//! 2. **Delete (Requirement 6.2).** The `DeleteTopic` is likewise routed to the
//!    metadata leader through `NotLeader` redirects; the leader reports success
//!    only once the removal **commits** to a majority (design §4). After the
//!    commit the test asserts, with bounded polling on every node:
//!    - the topic is **applied on every node** — `ListTopics` no longer shows it
//!      anywhere (the committed `DeleteTopic` removed it from each served
//!      catalogue), and
//!    - each node **stopped the topic's drivers** — `FindLeader` for the gone
//!      partition now fails with `PartitionNotFound` on every node, and a
//!      `Produce` to it likewise fails as not-found. Before the delete every node
//!      resolved a leader for that partition (a running driver); afterwards the
//!      partition is absent from the catalogue and no driver answers for it.
//!
//! All waits are bounded retries with short sleeps, so a cluster that fails to
//! propagate the delete fails the test promptly rather than hanging. Topic-admin
//! redirects are followed manually (chasing `NotLeader` hints to the metadata
//! leader), keeping the test in full control of which node each request targets.

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
        .join(format!("vela-server-delete-{}-{n}-{nanos}", process::id()))
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
/// details, if present. The server encodes every domain error this way so a
/// client can recover the precise classification and leader hint.
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

/// Return the wire [`v1::ErrorCode`] carried in `status`, if any.
fn error_code(status: &tonic::Status) -> Option<v1::ErrorCode> {
    let error = vela_error(status)?;
    v1::ErrorCode::try_from(error.code).ok()
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

/// One `DeleteTopic` attempt against `client`. Returns `Ok(())` once the leader
/// reports the removal committed, or the raw status so the caller can classify a
/// `NotLeader` redirect.
async fn delete_topic_attempt(
    mut client: VelaClientClient<Channel>,
    name: &str,
) -> Result<(), tonic::Status> {
    client
        .delete_topic(v1::DeleteTopicRequest {
            name: name.to_string(),
        })
        .await?;
    Ok(())
}

/// Create `topic` by starting at `ids[0]` and following `NotLeader` redirects to
/// the metadata leader, exactly as a redirect-following client would (Raft §8).
/// Returns the created [`v1::TopicInfo`] reported by the leader that committed it
/// (Requirement 4.1, 4.3).
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

/// Delete `topic` by starting at `ids[0]` and following `NotLeader` redirects to
/// the metadata leader (Raft §8). Returns once the leader reports the removal
/// **committed** (Requirement 6.2; design §4).
async fn delete_topic_via_leader(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    topic: &str,
) {
    let mut current = ids[0].clone();
    for _ in 0..400 {
        match delete_topic_attempt(clients[&current].clone(), topic).await {
            Ok(()) => return,
            Err(status) => match not_leader_hint(&status) {
                // Redirected to the known leader: aim the next attempt there.
                Some(Some(leader)) => current = leader,
                // No metadata leader elected yet: wait and retry the same node.
                Some(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                None => panic!("unexpected error deleting topic {topic}: {status:?}"),
            },
        }
    }
    panic!("metadata group did not elect a leader / commit the delete for {topic} in time");
}

/// One `FindLeader` lookup against `client`. `Ok(Some(id))` names the partition's
/// live leader, `Ok(None)` means this node hosts the partition's replica but it
/// has no elected leader yet, and `Err(_)` carries the raw status (e.g.
/// `PartitionNotFound` once the topic is gone from this node's catalogue).
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

/// One `Produce` attempt against `client`. Returns the committed offset or the
/// raw status (so a not-found classification can be inspected after a delete).
async fn produce_attempt(
    mut client: VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
    value: &[u8],
) -> Result<u64, tonic::Status> {
    let response = client
        .produce(v1::ProduceRequest {
            topic: topic.to_string(),
            partition,
            record: Some(v1::Record {
                key: None,
                value: value.to_vec(),
            }),
        })
        .await?
        .into_inner();
    Ok(response.offset)
}

/// List the topic names a node currently serves.
async fn list_topic_names(mut client: VelaClientClient<Channel>) -> Vec<String> {
    client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics
        .into_iter()
        .map(|t| t.name)
        .collect()
}

/// Wait until every node in `ids` lists `topic`, proving the committed create
/// was applied on every node.
async fn await_topic_on_all_nodes(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    topic: &str,
) {
    for _ in 0..400 {
        let mut all = true;
        for id in ids {
            if !list_topic_names(clients[id].clone())
                .await
                .iter()
                .any(|n| n == topic)
            {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("topic {topic} was not applied on every node in time");
}

/// Wait until **no** node in `ids` lists `topic`, proving the committed delete
/// was applied on every node — its `ClusterCommand::DeleteTopic` removed the
/// topic from each node's served catalogue (Requirement 6.2).
async fn await_topic_gone_on_all_nodes(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    topic: &str,
) {
    for _ in 0..400 {
        let mut gone_everywhere = true;
        for id in ids {
            if list_topic_names(clients[id].clone())
                .await
                .iter()
                .any(|n| n == topic)
            {
                gone_everywhere = false;
                break;
            }
        }
        if gone_everywhere {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("topic {topic} was still listed on some node after delete");
}

/// Wait until every node agrees on a single live leader for `(topic, partition)`
/// and return it, requiring *all* `ids` to report the *same* leader.
///
/// Because `FindLeader` resolves a leader only on a node that **hosts the
/// partition's driver**, every replica reporting the same elected leader is
/// direct evidence that each node started the partition's driver and the
/// partition's Raft group reached quorum across nodes. This is the precondition
/// the delete must later tear down.
async fn await_agreed_partition_leader(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    topic: &str,
    partition: u32,
) -> String {
    for _ in 0..400 {
        let mut leaders = Vec::with_capacity(ids.len());
        for id in ids {
            match find_leader(clients[id].clone(), topic, partition).await {
                Ok(Some(leader)) => leaders.push(leader),
                _ => break,
            }
        }
        if leaders.len() == ids.len() && leaders.iter().all(|l| *l == leaders[0]) {
            return leaders.swap_remove(0);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("partition {topic}/{partition} did not converge on a single leader in time");
}

/// Wait until **every** node reports the deleted partition's driver as gone:
/// `FindLeader` for `(topic, partition)` fails with `PartitionNotFound` on each
/// node. The partition is absent from every served catalogue (so `find_leader`
/// short-circuits to `PartitionNotFound`) and no node runs a driver for it —
/// the reconciler stopped each one when it applied the committed delete
/// (Requirement 6.2).
async fn await_partition_gone_on_all_nodes(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    topic: &str,
    partition: u32,
) {
    for _ in 0..400 {
        let mut gone_everywhere = true;
        for id in ids {
            match find_leader(clients[id].clone(), topic, partition).await {
                Err(status) if error_code(&status) == Some(v1::ErrorCode::PartitionNotFound) => {}
                // Still resolving the partition (leader or no-leader-yet), or a
                // transient status: the driver/catalogue entry has not been torn
                // down on this node yet.
                _ => {
                    gone_everywhere = false;
                    break;
                }
            }
        }
        if gone_everywhere {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "partition {topic}/{partition} still resolved on some node after delete \
         (its driver was not stopped / catalogue entry not removed)"
    );
}

/// Requirement 6.2 — deleting a topic commits through `__meta/0`, is applied on
/// every node, and every node stops the deleted topic's drivers.
///
/// The test first creates a topic and waits until it is applied on every node
/// with a driver running on every replica (every node resolves the same live
/// partition leader). It then deletes the topic via the metadata leader and
/// asserts the delete propagates: the topic disappears from `ListTopics` on
/// every node and `FindLeader`/`Produce` for the gone partition fail as
/// not-found on every node, evidencing each node stopped the topic's drivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_propagates_and_stops_drivers_on_every_node() {
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

    // --- Precondition: create a topic and converge with drivers everywhere. --
    // A single partition with rf=3 means every node replicates it and must run
    // a driver, so every node resolves the same live leader before the delete.
    let topic = "orders";
    let created = create_topic_via_leader(&clients, &ids, topic, 1).await;
    assert_eq!(created.name, topic);
    assert_eq!(created.partition_count, 1);

    // The committed create is applied on every node (it lists the topic) ...
    await_topic_on_all_nodes(&clients, &ids, topic).await;
    // ... and every replica node runs the partition's driver, agreeing on one
    // live leader — the drivers we expect the delete to stop.
    let partition_leader = await_agreed_partition_leader(&clients, &ids, topic, 0).await;
    assert!(
        ids.contains(&partition_leader),
        "the elected partition leader is one of the cluster nodes"
    );

    // --- Delete the topic, routing the DeleteTopic to the metadata leader. ---
    // The leader returns only once the removal commits to a majority (design §4);
    // a non-leader redirects via `NotLeader` and is followed here.
    delete_topic_via_leader(&clients, &ids, topic).await;

    // --- Requirement 6.2: applied on every node — the topic is gone from each
    // node's served catalogue. ------------------------------------------------
    await_topic_gone_on_all_nodes(&clients, &ids, topic).await;

    // --- Requirement 6.2: every node stopped the topic's drivers. ------------
    // `FindLeader` for the now-absent partition fails with `PartitionNotFound`
    // on every node (the partition left the catalogue and no driver answers for
    // it), where moments ago every node resolved a live leader for it.
    await_partition_gone_on_all_nodes(&clients, &ids, topic, 0).await;

    // A produce to the deleted topic likewise fails as not-found on every node:
    // there is no producible topic and no driver to accept the record. Accept
    // either not-found classification (the topic-admission check raises
    // `TopicNotFound`; the partition lookup raises `PartitionNotFound`).
    for id in &ids {
        let status = produce_attempt(clients[id].clone(), topic, 0, b"payload")
            .await
            .expect_err("produce to a deleted topic must fail on every node");
        assert!(
            matches!(
                error_code(&status),
                Some(v1::ErrorCode::PartitionNotFound) | Some(v1::ErrorCode::TopicNotFound)
            ),
            "produce on node {id} after delete must be a not-found error, got {status:?}"
        );
    }
}
