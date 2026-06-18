//! Cross-node create → produce → consume integration test (task 9.3).
//!
//! This is the **crux** end-to-end test for the cross-node-metadata-propagation
//! feature: it proves that the dedicated `__meta/0` metadata Raft group and the
//! per-partition Raft groups together let a topic created on one node be
//! produced to and consumed from a partition whose leader lives on a *different*
//! node, with leadership routing handled by `NotLeader` redirects.
//!
//! It stands up a **3-node in-process cluster** over real `tokio` timers and
//! `tonic` transport — each node configured with the other two as peers and
//! `replication_factor = 3` — so the metadata group and each topic partition are
//! genuine cluster-wide Raft groups that must elect a leader and replicate to a
//! majority across nodes. The flow it exercises:
//!
//! 1. **Metadata-leader redirect (Requirement 4.1, 4.3).** A `CreateTopic`
//!    issued to a node that is *not* the metadata leader is rejected with a
//!    `NotLeader` status that identifies the metadata leader, and the redirected
//!    request, replayed against that leader, commits the topic.
//! 2. **Apply on every node (Requirement 5.1).** Once the create commits through
//!    `__meta/0`, every node applies it and lists the topic.
//! 3. **Partition quorum (Requirement 7.1, 7.2).** With the topic applied on all
//!    three replicas, the partition's Raft group runs a driver on every replica
//!    and elects exactly one leader, agreed by all nodes.
//! 4. **Cross-node produce/consume with live-leader redirect (Requirement 7.4,
//!    7.5).** A produce (by key) and a consume issued to a partition *follower*
//!    are redirected to the live partition leader; the record commits by
//!    replication to a majority and round-trips back through the leader — which
//!    is a different node than the follower the requests were first sent to.
//!
//! All waits are bounded retries with short sleeps, so a genuinely broken
//! cluster fails the test promptly rather than hanging. Topic-admin redirects
//! are driven manually here (the `vela-client` `AdminClient` sends admin RPCs to
//! a bootstrap node and does not itself follow `NotLeader` hints), which keeps
//! the test in full control of *which* node each request is aimed at so the
//! cross-node leadership assertions are exact.

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
        .join(format!("vela-server-xnode-{}-{n}-{nanos}", process::id()))
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

/// One `FindLeader` lookup against `client`. `Ok(Some(id))` names the partition's
/// live leader, `Ok(None)` means the partition exists but has no leader yet, and
/// `Err(_)` means the topic is not yet visible on this node.
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

/// One `Produce` attempt against `client`, carrying a key so the keyed produce
/// path is exercised. Returns the committed offset or the raw status.
async fn produce_attempt(
    mut client: VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
    key: &[u8],
    value: &[u8],
) -> Result<u64, tonic::Status> {
    let response = client
        .produce(v1::ProduceRequest {
            topic: topic.to_string(),
            partition,
            record: Some(v1::Record {
                key: Some(key.to_vec()),
                value: value.to_vec(),
            }),
        })
        .await?
        .into_inner();
    Ok(response.offset)
}

/// One `Consume` attempt against `client`. Returns the full response or the raw
/// status (so a `NotLeader` redirect can be classified).
async fn consume_attempt(
    mut client: VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
    offset: u64,
) -> Result<v1::ConsumeResponse, tonic::Status> {
    let response = client
        .consume(v1::ConsumeRequest {
            topic: topic.to_string(),
            partition,
            offset,
            max_count: None,
        })
        .await?
        .into_inner();
    Ok(response)
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

/// Discover the current metadata leader by creating a throwaway probe topic,
/// starting at `ids[0]` and following any `NotLeader` redirect to the leader.
///
/// Returns the id of the node that accepted (committed) the create — the
/// metadata leader. Done first so the real test can then aim its create at a
/// node it *knows* is a follower, making the redirect deterministic rather than
/// dependent on which node happened to win the metadata election.
async fn discover_metadata_leader(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    probe_topic: &str,
) -> String {
    let mut current = ids[0].clone();
    for _ in 0..400 {
        match create_topic_attempt(clients[&current].clone(), probe_topic, 1).await {
            Ok(_) => return current,
            Err(status) => match not_leader_hint(&status) {
                // Redirected to the known leader: aim the next attempt there.
                Some(Some(leader)) => current = leader,
                // No metadata leader elected yet: wait and retry the same node.
                Some(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                None => panic!("unexpected error creating probe topic {probe_topic}: {status:?}"),
            },
        }
    }
    panic!("metadata group did not elect a leader / accept the probe create in time");
}

/// Wait until every node in `ids` lists `topic`, proving the committed create
/// was applied on every node (Requirement 5.1).
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

/// Wait until every node agrees on a single live leader for `(topic, partition)`
/// and return it (Requirement 7.1, 7.2).
///
/// Requiring *all* replicas to report the *same* leader confirms the partition's
/// Raft group reached quorum across nodes and that every replica knows the live
/// leader — so a subsequent produce/consume aimed at any follower will redirect
/// to exactly this leader.
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

/// Requirement 4.1, 4.3, 5.1, 7.1, 7.2, 7.4, 7.5 — create a topic on a metadata
/// follower (redirecting to the leader), then produce and consume across nodes
/// through a partition whose leader is a different node than the one the
/// produce/consume requests were first aimed at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_cross_node_produce_consume_with_leader_redirect() {
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
    // share the underlying connection.
    let mut clients = HashMap::new();
    for i in 0..3 {
        clients.insert(ids[i].clone(), connect_client(addrs[i]).await);
    }

    // --- 1. Metadata-leader redirect on create (Requirement 4.1, 4.3). -------
    // Find the metadata leader, then deliberately aim the real create at a node
    // we know is a follower so the `NotLeader` redirect is guaranteed.
    let meta_leader = discover_metadata_leader(&clients, &ids, "leader-probe").await;
    let follower = ids
        .iter()
        .find(|id| **id != meta_leader)
        .expect("a 3-node cluster has a metadata follower")
        .clone();

    // The create aimed at the follower must be rejected with a `NotLeader`
    // status that identifies a leader to redirect to — it must NOT commit
    // locally (Requirement 4.1). A brief window after election may leave the
    // follower not-yet-knowing the leader, surfaced as `NotLeader` with no hint;
    // retry until it names one.
    let redirect_hint = {
        let mut hint = None;
        for _ in 0..200 {
            let status = create_topic_attempt(clients[&follower].clone(), "orders", 1)
                .await
                .expect_err("a create on a metadata follower must be redirected, not accepted");
            match not_leader_hint(&status) {
                Some(Some(leader)) => {
                    hint = Some(leader);
                    break;
                }
                Some(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                None => panic!("expected a NotLeader redirect from the follower, got {status:?}"),
            }
        }
        hint.expect("the follower eventually identifies the metadata leader to redirect to")
    };
    assert_ne!(
        redirect_hint, follower,
        "the create must redirect to a different node, the metadata leader (Req 4.1)"
    );

    // The redirected request, replayed against the hinted leader, commits the
    // topic (Requirement 4.3).
    let created = create_topic_attempt(clients[&redirect_hint].clone(), "orders", 1)
        .await
        .expect("the redirected create commits on the metadata leader");
    assert_eq!(created.name, "orders");
    assert_eq!(created.partition_count, 1);

    // --- 2. The committed create is applied on every node (Requirement 5.1). -
    await_topic_on_all_nodes(&clients, &ids, "orders").await;

    // --- 3. The partition reaches quorum and elects one leader (Req 7.1,7.2). -
    let partition_leader = await_agreed_partition_leader(&clients, &ids, "orders", 0).await;
    assert!(
        ids.contains(&partition_leader),
        "the elected partition leader is one of the cluster nodes"
    );

    // --- 4. Cross-node produce/consume with live-leader redirect. ------------
    // Aim the produce/consume at a partition *follower* so the redirect to the
    // live leader is exercised, and so the record demonstrably round-trips
    // through a leader on a *different* node (Requirement 7.4, 7.5, 8.3).
    let partition_follower = ids
        .iter()
        .find(|id| **id != partition_leader)
        .expect("a 3-replica partition has a follower")
        .clone();

    let key = b"order-key";
    let value = b"order-payload";

    // A produce aimed at the follower redirects to the live partition leader,
    // not the node it was sent to (Requirement 7.4, 8.3). With a single
    // partition the keyed produce routes to partition 0.
    let produce_redirect = produce_attempt(
        clients[&partition_follower].clone(),
        "orders",
        0,
        key,
        value,
    )
    .await
    .expect_err("a produce on a partition follower must redirect to the leader");
    assert_eq!(
        not_leader_hint(&produce_redirect).flatten().as_deref(),
        Some(partition_leader.as_str()),
        "produce redirects to the live Raft-elected partition leader (Req 7.4, 8.3)"
    );

    // The produce replayed against the live leader commits the first record at
    // offset 0 — committed by replication to a majority of the 3 replicas
    // (Requirement 7.4). A short bounded retry absorbs the instant between
    // FindLeader reporting the leader and the produce path observing it.
    let mut offset = None;
    for _ in 0..100 {
        match produce_attempt(clients[&partition_leader].clone(), "orders", 0, key, value).await {
            Ok(o) => {
                offset = Some(o);
                break;
            }
            Err(status) if not_leader_hint(&status).is_some() => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("produce on the partition leader failed: {status:?}"),
        }
    }
    assert_eq!(
        offset.expect("produce commits on the partition leader within the bounded window"),
        0,
        "the first committed record gets offset 0"
    );

    // A consume aimed at the follower likewise redirects to the live leader
    // (Requirement 7.5, 8.3).
    let consume_redirect = consume_attempt(clients[&partition_follower].clone(), "orders", 0, 0)
        .await
        .expect_err("a consume on a partition follower must redirect to the leader");
    assert_eq!(
        not_leader_hint(&consume_redirect).flatten().as_deref(),
        Some(partition_leader.as_str()),
        "consume redirects to the live Raft-elected partition leader (Req 7.5, 8.3)"
    );

    // The consume replayed against the leader returns the committed record in
    // ascending offset order (Requirement 7.5).
    let consumed = {
        let mut out = None;
        for _ in 0..100 {
            match consume_attempt(clients[&partition_leader].clone(), "orders", 0, 0).await {
                Ok(resp) if !resp.records.is_empty() => {
                    out = Some(resp);
                    break;
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                Err(status) if not_leader_hint(&status).is_some() => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => panic!("consume on the partition leader failed: {status:?}"),
            }
        }
        out.expect("the committed record is consumable from the leader within the bounded window")
    };

    assert_eq!(consumed.records.len(), 1, "exactly one committed record");
    assert_eq!(
        consumed.next_offset, 1,
        "next offset advances past the single record"
    );
    let record = consumed.records[0]
        .record
        .as_ref()
        .expect("the consumed record carries a payload");
    assert_eq!(consumed.records[0].offset, 0, "the record sits at offset 0");
    assert_eq!(
        record.value, value,
        "the consumed value matches what was produced across nodes"
    );

    // --- The crux: the record round-tripped through a partition whose leader
    // is a *different* node than the follower we produced/consumed against. ---
    assert_ne!(
        partition_leader, partition_follower,
        "the partition leader is a different node than the one produce/consume were aimed at \
         (cross-node produce/consume with redirect)"
    );
}
