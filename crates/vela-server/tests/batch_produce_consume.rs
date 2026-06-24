//! Happy-path batch produce + consume parity integration test (task 9.1).
//!
//! This is the crux end-to-end test for the **batched-produce** feature: it
//! proves that a multi-record batch produced through the `ProduceBatch` RPC
//! commits as a contiguous, gap-free offset range, interleaves correctly with
//! single-record produces on the same partition, and consumes back in ascending
//! offset order with values byte-for-byte identical to what was produced.
//!
//! It stands up a **single-node in-process cluster** over real `tokio` timers
//! and `tonic` transport — `replication_factor = 1`, no peers — so the metadata
//! Raft group and the topic partition each elect that one node as their leader
//! quickly, with no cross-node redirect bookkeeping. The single node keeps
//! routing trivial so the parity assertions stay the focus. The flow:
//!
//! 1. **Stand up the node and create `orders` with 1 partition** on the metadata
//!    leader (the only node), waiting for the create to apply and the partition
//!    to elect a leader.
//! 2. **Batch produce (Requirement 1.1, 1.2, 1.3, 1.4).** Produce a 5-record
//!    batch to partition 0; assert the response reports `base_offset == 0` and
//!    `count == 5`, so the per-record offsets `base_offset + i` are contiguous
//!    `0,1,2,3,4`.
//! 3. **Interleave with single produce (Requirement 1.3, 10.4).** Produce one
//!    single record (offset 5), then a second batch of 3 (`base_offset == 6`,
//!    `count == 3`, offsets `6,7,8`), proving batch/single interleave is
//!    gap-free.
//! 4. **Consume parity (Requirement 10.1, 10.2, 10.4).** Consume partition 0 from
//!    offset 0 and assert all 9 records return in ascending offset order
//!    `0..9`, gap-free, each value byte-for-byte identical to what was produced
//!    in that order. Keys are not persisted in this milestone (consume returns
//!    `key: None`), matching the single-record path semantics, so only values
//!    are asserted.
//!
//! All waits are bounded retries with short sleeps, so a genuinely broken
//! cluster fails the test promptly rather than hanging.

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
        .join(format!("vela-server-batch-{}-{n}-{nanos}", process::id()))
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

/// One single-record `Produce` attempt against `client`, carrying a key so the
/// keyed produce path is exercised. Returns the committed offset or the raw
/// status.
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
///
/// Returns the id of the node that accepted (committed) the create — the
/// metadata leader. On a single node this resolves to that node once its
/// metadata group has elected itself.
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
/// and return it. On a single node this is that node once its partition Raft
/// group has elected itself.
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

/// Build `count` records with distinct values derived from `label`, each
/// carrying a key (keys are not persisted in this milestone, but the produce
/// path accepts them). The value is what the consume path returns verbatim.
fn batch_records(label: &str, count: usize) -> Vec<v1::Record> {
    (0..count)
        .map(|i| v1::Record {
            key: Some(format!("{label}-key-{i}").into_bytes()),
            value: format!("{label}-value-{i}").into_bytes(),
        })
        .collect()
}

/// Requirement 1.1, 1.2, 1.3, 1.4, 8.2, 10.1, 10.2, 10.4 — a multi-record batch
/// produced to a partition commits as a contiguous offset range from the
/// captured base, interleaves gap-free with single-record produces, and consumes
/// back in ascending offset order with values byte-for-byte identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_produce_then_consume_parity_with_single_interleave() {
    // --- 1. Stand up a single-node cluster (rf=1, no peers). -----------------
    // A lone node elects itself the metadata leader and the partition leader
    // quickly, so routing is trivial and the parity assertions are the focus.
    let ids = ["node-a".to_string()];
    let addrs = [free_addr()];
    spawn_server(config(&ids[0], addrs[0], &[], 1));

    let mut clients = HashMap::new();
    clients.insert(ids[0].clone(), connect_client(addrs[0]).await);

    // Create `orders` with a single partition on the metadata leader (the only
    // node), then wait for the create to apply and the partition to elect a
    // leader so produce/consume route trivially to partition 0.
    let meta_leader = discover_metadata_leader(&clients, &ids, "leader-probe").await;
    let created = create_topic_attempt(clients[&meta_leader].clone(), "orders", 1)
        .await
        .expect("the create commits on the single-node metadata leader");
    assert_eq!(created.name, "orders");
    assert_eq!(created.partition_count, 1);

    await_topic_on_all_nodes(&clients, &ids, "orders").await;
    let leader = await_agreed_partition_leader(&clients, &ids, "orders", 0).await;
    assert_eq!(leader, ids[0], "the single node leads its own partition");

    let client = clients[&leader].clone();

    // `expected` accumulates every value in produced order, so the final consume
    // can assert ascending offset == produced order, byte-for-byte.
    let mut expected: Vec<Vec<u8>> = Vec::new();

    // --- 2. Produce a 5-record batch; assert a contiguous range from base 0. -
    let first_batch = batch_records("batch-a", 5);
    let first = {
        let mut out = None;
        for _ in 0..100 {
            match produce_batch_attempt(client.clone(), "orders", 0, first_batch.clone()).await {
                Ok(resp) => {
                    out = Some(resp);
                    break;
                }
                Err(status) if not_leader_hint(&status).is_some() => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => panic!("batch produce on the leader failed: {status:?}"),
            }
        }
        out.expect("the first batch commits on the leader within the bounded window")
    };
    assert_eq!(
        first.base_offset, 0,
        "the first batch's base offset is the partition's initial next offset (Req 1.1, 2.4)"
    );
    assert_eq!(first.count, 5, "the batch reports all 5 records committed");
    // Per-record offsets are base + i, contiguous 0,1,2,3,4 (Req 1.3, 1.4, 8.2).
    let first_offsets: Vec<u64> = (0..first.count as u64)
        .map(|i| first.base_offset + i)
        .collect();
    assert_eq!(
        first_offsets,
        vec![0, 1, 2, 3, 4],
        "per-record offsets are contiguous from the base (Req 1.3, 1.4)"
    );
    expected.extend(first_batch.iter().map(|r| r.value.clone()));

    // --- 3. Interleave a single produce (offset 5), then a batch of 3. -------
    // A single-record produce on the same partition takes the next offset after
    // the batch, proving batch/single interleave is gap-free (Req 1.3, 10.4).
    let single_value = b"single-value-5".to_vec();
    let single_offset = {
        let mut out = None;
        for _ in 0..100 {
            match produce_attempt(client.clone(), "orders", 0, b"single-key-5", &single_value).await
            {
                Ok(o) => {
                    out = Some(o);
                    break;
                }
                Err(status) if not_leader_hint(&status).is_some() => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => panic!("single produce on the leader failed: {status:?}"),
            }
        }
        out.expect("the single record commits on the leader within the bounded window")
    };
    assert_eq!(
        single_offset, 5,
        "the single record takes the offset right after the first batch (Req 1.3, 10.4)"
    );
    expected.push(single_value);

    // A second batch of 3 starts at base 6 and is contiguous 6,7,8 (Req 1.3).
    let second_batch = batch_records("batch-b", 3);
    let second = produce_batch_attempt(client.clone(), "orders", 0, second_batch.clone())
        .await
        .expect("the second batch commits on the leader");
    assert_eq!(
        second.base_offset, 6,
        "the second batch begins right after the interleaved single record (Req 1.3, 10.4)"
    );
    assert_eq!(
        second.count, 3,
        "the second batch reports 3 records committed"
    );
    let second_offsets: Vec<u64> = (0..second.count as u64)
        .map(|i| second.base_offset + i)
        .collect();
    assert_eq!(
        second_offsets,
        vec![6, 7, 8],
        "the second batch's offsets are contiguous from its base (Req 1.3)"
    );
    expected.extend(second_batch.iter().map(|r| r.value.clone()));

    // Nine records produced in total: 5 (batch) + 1 (single) + 3 (batch).
    assert_eq!(expected.len(), 9);

    // --- 4. Consume from offset 0; assert ascending, gap-free, value parity. -
    // Retry until all 9 committed records are returned, then assert offsets are
    // ascending and gap-free 0..9 and each value matches the produced order
    // byte-for-byte (Req 10.1, 10.2, 10.4). Keys are not persisted this
    // milestone, so only values are asserted (matching the single-record path).
    let consumed = {
        let mut out = None;
        for _ in 0..100 {
            match consume_attempt(client.clone(), "orders", 0, 0, Some(64)).await {
                Ok(resp) if resp.records.len() >= expected.len() => {
                    out = Some(resp);
                    break;
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                Err(status) if not_leader_hint(&status).is_some() => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => panic!("consume on the leader failed: {status:?}"),
            }
        }
        out.expect("all produced records are consumable from the leader within the window")
    };

    assert_eq!(
        consumed.records.len(),
        expected.len(),
        "consume returns exactly the records produced, with no extras"
    );
    assert_eq!(
        consumed.next_offset, 9,
        "next offset advances past all nine committed records"
    );

    for (i, entry) in consumed.records.iter().enumerate() {
        // Ascending and gap-free: the i-th returned record sits at offset i
        // (Req 10.1, 10.4).
        assert_eq!(
            entry.offset, i as u64,
            "records come back in ascending, gap-free offset order"
        );
        let record = entry
            .record
            .as_ref()
            .expect("each consumed entry carries a record payload");
        // Byte-for-byte value parity in produced order, regardless of whether the
        // record was produced in a batch or singly (Req 10.2, 10.4).
        assert_eq!(
            record.value, expected[i],
            "the consumed value matches what was produced at this offset, byte-for-byte"
        );
        // Keys are not persisted in this milestone (single-record path parity).
        assert_eq!(
            record.key, None,
            "keys are not persisted; consume returns key: None for batch and single alike"
        );
    }
}
