//! Concurrent batched-produce group-commit integration test (task 5.5).
//!
//! This is the cross-node end-to-end test for the **wal-group-commit** feature.
//! It proves that under concurrent batched produce load against the *durable*
//! log backend — the backend the bug only ever reproduced on, because of its
//! inline `fsync` — the cluster commits every record without the spurious
//! leadership churn / `NoLeader` stalls the old per-append inline-fsync path
//! caused, and that a subsequent consume returns the records in order.
//!
//! It stands up a **3-node in-process cluster** over real `tokio` timers and
//! `tonic` transport — each node configured with the other two as peers and
//! `replication_factor = 3` — so the metadata group and the topic partition are
//! genuine cluster-wide Raft groups that must elect a leader and replicate to a
//! majority across nodes. The flow it exercises:
//!
//! 1. **Stand up the cluster and create a DURABLE-backend topic** with a single
//!    partition (`v1::LogBackend::Durable`). The durable backend now runs
//!    `SyncPolicy::Grouped`, so the per-partition driver owns forcing and group-
//!    commits queued appends with a single offloaded `fsync`.
//! 2. **Discover the partition leader** and confirm all nodes agree on it.
//! 3. **Drive concurrent batched produce (Requirement 1.1).** Several concurrent
//!    `tokio` tasks each fire many `ProduceBatch` requests (multiple records per
//!    batch) at the partition leader, so produces queue together and exercise
//!    the group-commit drain-and-batch path.
//! 4. **Assert every record commits** (each `ProduceBatch` returns a base
//!    offset and a count), that the union of committed offsets is a complete,
//!    gap-free, duplicate-free range `0..total` (records committed exactly once
//!    in one consistent offset space), and the produced *content* round-trips.
//! 5. **Assert no spurious leadership change (Requirement 4.1).** The agreed
//!    partition leader before the load equals the agreed leader after it, and
//!    all nodes still agree on that single leader — the key assertion for the
//!    bug (no `NoLeader` churn under load).
//! 6. **Consume parity.** All records consume back from the leader in ascending,
//!    gap-free offset order, the count matches, and the value multiset is
//!    exactly what was produced.
//!
//! All waits are bounded retries with short sleeps, so a genuinely broken
//! cluster fails the test promptly rather than hanging. The test uses the
//! multi-thread runtime because the durable `Grouped` backend's driver forces
//! via `block_in_place` (which requires it) and the concurrent producers need
//! real threads.

use std::collections::{HashMap, HashSet};
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
/// test node. Each node needs its own root so its durable `__meta` log and its
/// durable partition logs do not collide with a peer's.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!(
            "vela-server-grpcommit-{}-{n}-{nanos}",
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

/// One `CreateTopic` attempt against `client`, selecting an explicit log
/// `backend`. Returns the created topic on success, or the raw [`tonic::Status`]
/// so the caller can classify a `NotLeader` redirect.
async fn create_topic_attempt(
    mut client: VelaClientClient<Channel>,
    name: &str,
    partitions: u32,
    backend: v1::LogBackend,
) -> Result<v1::TopicInfo, tonic::Status> {
    let response = client
        .create_topic(v1::CreateTopicRequest {
            name: name.to_string(),
            partitions,
            log_backend: backend as i32,
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

/// One `ProduceBatch` attempt against `client`: append an ordered batch of
/// records to a resolved `(topic, partition)` as one unit. Returns the compact
/// `{ base_offset, count }` response or the raw status (so a `NotLeader`
/// redirect can be classified).
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
    max_count: Option<u32>,
) -> Result<v1::ConsumeResponse, tonic::Status> {
    let response = client
        .consume(v1::ConsumeRequest {
            topic: topic.to_string(),
            partition,
            offset,
            max_count,
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
/// The probe topic uses the default backend; only its acceptance identifies the
/// metadata leader.
async fn discover_metadata_leader(
    clients: &HashMap<String, VelaClientClient<Channel>>,
    ids: &[String],
    probe_topic: &str,
) -> String {
    let mut current = ids[0].clone();
    for _ in 0..400 {
        match create_topic_attempt(
            clients[&current].clone(),
            probe_topic,
            1,
            v1::LogBackend::Unspecified,
        )
        .await
        {
            Ok(_) => return current,
            Err(status) => match not_leader_hint(&status) {
                Some(Some(leader)) => current = leader,
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
/// leader.
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

// --- Concurrent-produce workload sizing. ------------------------------------
//
// Enough records that the old inline-fsync path would stall the runtime under
// concurrent load, but bounded so the group-commit path runs quickly.
const PRODUCERS: usize = 8;
const BATCHES_PER_PRODUCER: usize = 20;
const RECORDS_PER_BATCH: usize = 10;
const TOTAL_RECORDS: usize = PRODUCERS * BATCHES_PER_PRODUCER * RECORDS_PER_BATCH;

/// The globally-unique value bytes for the record produced by `producer`, in its
/// `batch`, at within-batch index `idx`. Uniqueness lets the final consume
/// assert exactly-once content delivery regardless of cross-producer interleave.
fn record_value(producer: usize, batch: usize, idx: usize) -> Vec<u8> {
    format!("p{producer}-b{batch}-r{idx}").into_bytes()
}

/// Requirement 1.1, 4.1 — under concurrent batched produce against the durable
/// (`Grouped`) backend, a 3-node cluster commits every record exactly once in
/// one gap-free offset space, with no spurious leadership change, and consume
/// returns them in ascending order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_batched_produce_group_commits_without_leadership_churn() {
    // --- Stand up a 3-node cluster, each node peering the other two. ---------
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

    let mut clients = HashMap::new();
    for i in 0..3 {
        clients.insert(ids[i].clone(), connect_client(addrs[i]).await);
    }

    // --- 1. Create a DURABLE-backend topic with a single partition. ----------
    // The durable backend is the crux: the bug only reproduced there, where the
    // driver now group-commits queued appends with a single offloaded fsync.
    let meta_leader = discover_metadata_leader(&clients, &ids, "leader-probe").await;
    let created = create_topic_attempt(
        clients[&meta_leader].clone(),
        "events",
        1,
        v1::LogBackend::Durable,
    )
    .await
    .expect("the durable-backend create commits on the metadata leader");
    assert_eq!(created.name, "events");
    assert_eq!(created.partition_count, 1);
    assert_eq!(
        created.log_backend,
        v1::LogBackend::Durable as i32,
        "the topic is created on the durable backend (the backend the bug reproduced on)"
    );

    await_topic_on_all_nodes(&clients, &ids, "events").await;

    // --- 2. Discover the partition leader (all nodes must agree). ------------
    let leader_before = await_agreed_partition_leader(&clients, &ids, "events", 0).await;
    assert!(
        ids.contains(&leader_before),
        "the elected partition leader is one of the cluster nodes"
    );

    // --- 3. Drive CONCURRENT batched produce at the leader. ------------------
    // Each producer fires many ProduceBatch requests so produces queue together
    // and exercise the group-commit drain-and-batch path. NotLeader is retried
    // a bounded number of times: a stable leader never yields it, but bounding
    // keeps a churning cluster failing promptly instead of hanging.
    let mut tasks = Vec::with_capacity(PRODUCERS);
    for producer in 0..PRODUCERS {
        let client = clients[&leader_before].clone();
        let topic = "events".to_string();
        tasks.push(tokio::spawn(async move {
            // Each element is the (base_offset, count) of one committed batch.
            let mut committed: Vec<(u64, u32)> = Vec::with_capacity(BATCHES_PER_PRODUCER);
            for batch in 0..BATCHES_PER_PRODUCER {
                let records: Vec<v1::Record> = (0..RECORDS_PER_BATCH)
                    .map(|idx| v1::Record {
                        key: Some(format!("p{producer}-b{batch}-k{idx}").into_bytes()),
                        value: record_value(producer, batch, idx),
                    })
                    .collect();

                let mut done = None;
                for _ in 0..200 {
                    match produce_batch_attempt(client.clone(), &topic, 0, records.clone()).await {
                        Ok(resp) => {
                            done = Some(resp);
                            break;
                        }
                        Err(status) if not_leader_hint(&status).is_some() => {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        Err(status) => {
                            panic!("producer {producer} batch {batch} failed: {status:?}")
                        }
                    }
                }
                let resp = done.unwrap_or_else(|| {
                    panic!("producer {producer} batch {batch} never committed (leadership churn?)")
                });
                assert_eq!(
                    resp.count as usize, RECORDS_PER_BATCH,
                    "every record in the batch is reported committed (Req 1.1)"
                );
                committed.push((resp.base_offset, resp.count));
            }
            committed
        }));
    }

    // Collect every committed batch across all producers.
    let mut all_offsets: Vec<u64> = Vec::with_capacity(TOTAL_RECORDS);
    for task in tasks {
        let committed = task.await.expect("producer task joins cleanly");
        for (base, count) in committed {
            for i in 0..count as u64 {
                all_offsets.push(base + i);
            }
        }
    }

    // --- 4. Every record committed exactly once in one gap-free offset space. -
    assert_eq!(
        all_offsets.len(),
        TOTAL_RECORDS,
        "all {TOTAL_RECORDS} records across concurrent producers committed (Req 1.1)"
    );
    all_offsets.sort_unstable();
    let unique: HashSet<u64> = all_offsets.iter().copied().collect();
    assert_eq!(
        unique.len(),
        TOTAL_RECORDS,
        "no offset is assigned twice — records commit exactly once (Req 1.1, 3.3)"
    );
    for (expected, actual) in all_offsets.iter().enumerate() {
        assert_eq!(
            *actual, expected as u64,
            "committed offsets form a complete, gap-free range 0..{TOTAL_RECORDS}"
        );
    }

    // --- 5. No spurious leadership change under load (Req 4.1). --------------
    // The agreed leader after the produce storm equals the one before it, and
    // all nodes still agree on a single leader. This is the key bug assertion:
    // the old inline-fsync path starved the runtime, missed heartbeats, and
    // churned leadership into NoLeader.
    let leader_after = await_agreed_partition_leader(&clients, &ids, "events", 0).await;
    assert_eq!(
        leader_after, leader_before,
        "the partition leader is unchanged across concurrent batched produce load \
         — no spurious leadership change / NoLeader churn (Req 4.1)"
    );

    // --- 6. Consume every record back in ascending, gap-free order. ----------
    // Page through from offset 0 following next_offset until all records are
    // collected, asserting ascending offsets as we go, then confirm the value
    // multiset is exactly what was produced (exactly-once content).
    let leader_client = clients[&leader_after].clone();
    let mut consumed_values: Vec<Vec<u8>> = Vec::with_capacity(TOTAL_RECORDS);
    let mut next = 0u64;
    'outer: for _ in 0..400 {
        match consume_attempt(leader_client.clone(), "events", 0, next, Some(256)).await {
            Ok(resp) => {
                for entry in &resp.records {
                    assert_eq!(
                        entry.offset,
                        consumed_values.len() as u64,
                        "records come back in ascending, gap-free offset order"
                    );
                    let record = entry
                        .record
                        .as_ref()
                        .expect("each consumed entry carries a record payload");
                    consumed_values.push(record.value.clone());
                }
                next = resp.next_offset;
                if consumed_values.len() >= TOTAL_RECORDS {
                    break 'outer;
                }
                if resp.records.is_empty() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            Err(status) if not_leader_hint(&status).is_some() => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("consume on the leader failed: {status:?}"),
        }
    }

    assert_eq!(
        consumed_values.len(),
        TOTAL_RECORDS,
        "consume returns exactly the {TOTAL_RECORDS} produced records in order"
    );

    // Exactly-once content: the consumed value multiset equals the produced one.
    let produced: HashSet<Vec<u8>> = (0..PRODUCERS)
        .flat_map(|p| {
            (0..BATCHES_PER_PRODUCER)
                .flat_map(move |b| (0..RECORDS_PER_BATCH).map(move |i| record_value(p, b, i)))
        })
        .collect();
    let consumed_set: HashSet<Vec<u8>> = consumed_values.iter().cloned().collect();
    assert_eq!(
        consumed_set.len(),
        TOTAL_RECORDS,
        "every consumed value is distinct (no duplicate content)"
    );
    assert_eq!(
        consumed_set, produced,
        "the consumed value multiset is exactly what was produced (exactly-once content)"
    );
}
