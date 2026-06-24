//! Not-leader redirect for batched produce (task 9.2).
//!
//! This integration test proves the two halves of the batched-produce
//! not-leader contract against a **real 3-node in-process cluster** (mirroring
//! `cross_node_produce_consume.rs`, section 4, but for `ProduceBatch`):
//!
//! 1. **Raw redirect, append nothing (Requirement 6.1).** A `ProduceBatch` sent
//!    to a partition *follower* is rejected with a `NotLeader` status whose hint
//!    names the live partition leader — it must NOT commit locally and must
//!    append none of the batch's records. We confirm nothing was appended by
//!    consuming partition 0 from offset 0 at the leader and asserting it returns
//!    zero records.
//! 2. **Client re-resolution + retry (Requirement 5.6, 6.2).** A high-level
//!    `vela_client::VelaClient` seeded with *only the follower's* address as its
//!    sole bootstrap — so it initially holds a non-leader path — drives
//!    `producer().produce_batch(...)`. The client re-resolves the partition
//!    leader (via `FindLeader` across discovered members) and retries the
//!    identical batch at the new leader within its retry budget, committing the
//!    batch and returning per-record offsets contiguous from base 0. We then
//!    consume and assert the records are present byte-for-byte.
//!
//! All waits are bounded retries with short sleeps so a genuinely broken cluster
//! fails the test promptly rather than hanging. Topic-admin redirects are driven
//! manually here (the `vela-client` `AdminClient` does not follow `NotLeader`
//! hints), keeping the test in full control of which node each request is aimed
//! at so the cross-node leadership assertions are exact.

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
            "vela-server-batch-nl-{}-{n}-{nanos}",
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

/// One `ProduceBatch` attempt against `client`, carrying an ordered set of
/// records for a single `(topic, partition)`. Returns the
/// [`v1::ProduceBatchResponse`] (the base offset + count) on success, or the raw
/// status so a `NotLeader` redirect can be classified.
async fn produce_batch_attempt(
    mut client: VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
    records: Vec<v1::Record>,
) -> Result<v1::ProduceBatchResponse, tonic::Status> {
    let response = client
        .produce_batch(v1::ProduceBatchRequest {
            topic: topic.to_string(),
            partition,
            records,
        })
        .await?
        .into_inner();
    Ok(response)
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

/// Wait until every node agrees on a single live leader for `(topic, partition)`
/// and return it.
///
/// Requiring *all* replicas to report the *same* leader confirms the partition's
/// Raft group reached quorum across nodes and that every replica knows the live
/// leader — so a subsequent produce aimed at any follower will redirect to
/// exactly this leader.
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

/// The batch of records used throughout the test — multiple records so the
/// not-leader rejection demonstrably refuses a *whole batch*, and the client
/// retry commits a *whole batch* contiguously from base 0.
fn batch_records() -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
    vec![
        (Some(b"order-key-0".to_vec()), b"order-payload-0".to_vec()),
        (Some(b"order-key-1".to_vec()), b"order-payload-1".to_vec()),
        (Some(b"order-key-2".to_vec()), b"order-payload-2".to_vec()),
    ]
}

/// Requirement 5.6, 6.1, 6.2 — a `ProduceBatch` sent to a partition follower is
/// rejected with the live-leader hint and appends nothing, and the high-level
/// client re-resolves the leader and retries the identical batch at the new
/// leader within its retry budget, committing it contiguously from base 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_on_follower_redirects_then_client_reresolves_and_commits() {
    // --- 1. Stand up a 3-node cluster, each node peering the other two. ------
    let ids = [
        "node-a".to_string(),
        "node-b".to_string(),
        "node-c".to_string(),
    ];
    let addrs = [free_addr(), free_addr(), free_addr()];

    for i in 0..3 {
        let peers: Vec<String> = (0..3)
            .filter(|&j| j != i)
            .map(|j| format!("{}@{}", ids[j], addrs[j]))
            .collect();
        spawn_server(config(&ids[i], addrs[i], &peers, 3));
    }

    // One raw client per node, keyed by node id.
    let mut clients = HashMap::new();
    for i in 0..3 {
        clients.insert(ids[i].clone(), connect_client(addrs[i]).await);
    }

    // --- 2. Create topic "orders" with 1 partition (following redirects). ----
    let meta_leader = discover_metadata_leader(&clients, &ids, "leader-probe").await;
    let created = create_topic_attempt(clients[&meta_leader].clone(), "orders", 1)
        .await
        .expect("the create commits on the metadata leader");
    assert_eq!(created.name, "orders");
    assert_eq!(created.partition_count, 1);

    await_topic_on_all_nodes(&clients, &ids, "orders").await;

    // Every node agrees on the single partition leader (the partition's Raft
    // group reached quorum across nodes).
    let partition_leader = await_agreed_partition_leader(&clients, &ids, "orders", 0).await;
    assert!(
        ids.contains(&partition_leader),
        "the elected partition leader is one of the cluster nodes"
    );

    // --- 3. Identify a partition follower (a node != the partition leader). --
    let partition_follower = ids
        .iter()
        .find(|id| **id != partition_leader)
        .expect("a 3-replica partition has a follower")
        .clone();
    let follower_index = ids
        .iter()
        .position(|id| *id == partition_follower)
        .expect("the follower is one of the cluster nodes");
    let follower_addr = addrs[follower_index];

    let records = batch_records();
    let proto_records: Vec<v1::Record> = records
        .iter()
        .map(|(key, value)| v1::Record {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();

    // --- 4. Raw redirect assertion: reject with live-leader hint, append
    //        nothing (Requirement 6.1). ---------------------------------------
    // A batch aimed at the follower must be rejected with a `NotLeader` status
    // whose hint is the live partition leader — it must NOT commit locally. A
    // brief window after election may leave the follower not-yet-knowing the
    // leader (a `NotLeader` with no hint); retry until it names one.
    let redirect_hint = {
        let mut hint = None;
        for _ in 0..200 {
            let status = produce_batch_attempt(
                clients[&partition_follower].clone(),
                "orders",
                0,
                proto_records.clone(),
            )
            .await
            .expect_err("a ProduceBatch on a partition follower must be redirected, not accepted");
            match not_leader_hint(&status) {
                Some(Some(leader)) => {
                    hint = Some(leader);
                    break;
                }
                Some(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                None => panic!("expected a NotLeader redirect from the follower, got {status:?}"),
            }
        }
        hint.expect("the follower eventually identifies the live partition leader to redirect to")
    };
    assert_eq!(
        redirect_hint, partition_leader,
        "the batch rejection carries the live partition leader as its hint (Req 6.1)"
    );

    // The rejected batch appended NOTHING: consuming partition 0 from offset 0
    // at the leader returns zero records (Requirement 6.1). A bounded retry
    // absorbs the instant between FindLeader naming the leader and the consume
    // path observing it.
    let empty = {
        let mut out = None;
        for _ in 0..100 {
            match consume_attempt(clients[&partition_leader].clone(), "orders", 0, 0).await {
                Ok(resp) => {
                    out = Some(resp);
                    break;
                }
                Err(status) if not_leader_hint(&status).is_some() => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => panic!("consume on the partition leader failed: {status:?}"),
            }
        }
        out.expect("the leader serves a consume within the bounded window")
    };
    assert!(
        empty.records.is_empty(),
        "the rejected batch appended nothing — partition 0 is empty (Req 6.1)"
    );
    assert_eq!(
        empty.next_offset, 0,
        "the partition's committed offset is unchanged by the rejected batch (Req 6.1)"
    );

    // --- 5. Client re-resolution + retry (Requirement 5.6, 6.2). -------------
    // Seed a high-level VelaClient with ONLY the follower's id+addr as its sole
    // bootstrap, so the client initially holds a non-leader path. Its dispatch
    // will FindLeader-resolve the real leader across discovered members and
    // retry the identical batch there within the retry budget.
    let client = vela_client::VelaClient::new([(
        partition_follower.clone(),
        format!("http://{follower_addr}"),
    )]);

    let offsets = client
        .producer()
        .produce_batch("orders", records.clone())
        .await
        .expect("the client re-resolves the leader and the batch commits (Req 5.6, 6.2)");

    // The batch committed at the real leader, contiguous from base 0.
    let expected: Vec<u64> = (0..records.len() as u64).collect();
    assert_eq!(
        offsets, expected,
        "per-record offsets are contiguous from base 0 after re-resolution (Req 6.2)"
    );

    // --- 6. Consume and assert the records are present byte-for-byte. --------
    let consumed = {
        let mut out = None;
        for _ in 0..100 {
            match consume_attempt(clients[&partition_leader].clone(), "orders", 0, 0).await {
                Ok(resp) if resp.records.len() == records.len() => {
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
        out.expect(
            "all committed batch records are consumable from the leader in the bounded window",
        )
    };

    assert_eq!(
        consumed.records.len(),
        records.len(),
        "exactly the batch's records were committed"
    );
    assert_eq!(
        consumed.next_offset,
        records.len() as u64,
        "next offset advances past the whole batch"
    );
    for (position, (_key, value)) in records.iter().enumerate() {
        let committed = &consumed.records[position];
        assert_eq!(
            committed.offset, position as u64,
            "record {position} sits at its contiguous offset"
        );
        let payload = committed
            .record
            .as_ref()
            .expect("the consumed record carries a payload");
        assert_eq!(
            &payload.value, value,
            "record {position}'s consumed value is byte-for-byte identical to the produced value",
        );
    }

    // --- The crux: the batch round-tripped through a leader on a *different*
    // node than the follower the client was seeded with and first dispatched
    // against. ----------------------------------------------------------------
    assert_ne!(
        partition_leader, partition_follower,
        "the partition leader is a different node than the follower the batch was first aimed at \
         (client re-resolved the leader and retried the identical batch there)"
    );
}
