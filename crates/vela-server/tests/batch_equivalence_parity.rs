// Feature: batched-produce, Property 7: A batch of N is equivalent to N single
// produces, and consumes back identically.
//
// Property 7: *For any* ordered sequence of N >= 1 records produced to a fresh
// single-partition topic, driving them once as **one** `ProduceBatch` of N and
// once as **N** single-record `Produce` calls yields the *identical* committed
// offset sequence (`0, 1, ..., N-1`) and the *identical* stored values — a
// one-record batch takes exactly the single-record offset (model-based
// equivalence, Requirement 4.2, 10.3). Consuming either partition from offset 0
// returns all N records in ascending, gap-free offset order with values
// byte-for-byte identical to the produced input and identical to each other
// (Requirement 10.1, 10.2). A third **mixed** topic interleaves a batch and
// single produces and consumes back as one contiguous, gap-free, ascending
// sequence in produced order (Requirement 10.4).
//
// Each proptest case drives a **real** in-process single-node cluster (rf=1, no
// peers) over `tokio` + `tonic`: the lone node leads the metadata group and
// every partition, so routing is trivial and the equivalence assertions are the
// focus. The cluster and its `tokio` runtime are stood up **once** in a shared
// `OnceLock<Harness>` and reused across cases (mirroring
// `vela-client/tests/prop_per_record_offsets.rs`); each case creates **fresh**,
// uniquely-named topics so every partition's offsets start at 0. Because each
// case does real RPC and topic creation, the case count and record counts are
// kept modest (16 cases, N in 1..=8, small values). All cluster waits are
// bounded retries with short sleeps, so a genuinely broken cluster fails fast
// rather than hanging.
//
// Keys are not persisted in this milestone (consume returns `key: None`),
// matching the single-record path; the property asserts values only and that
// every consumed key is `None`.
//
// Validates: Requirements 4.2, 10.1, 10.2, 10.3, 10.4

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proptest::prelude::*;
use prost::Message as _;
use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;

/// Monotonic counter making per-test data directories unique within a process.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Monotonic counter making each proptest case's topics unique, so every case
/// produces to fresh partitions whose offsets start at 0.
static TOPIC_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A consumed record reduced to the fields the parity property asserts:
/// `(offset, key, value)`.
type Consumed = Vec<(u64, Option<Vec<u8>>, Vec<u8>)>;

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
            "vela-server-batch-eqv-{}-{n}-{nanos}",
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

/// One single-record `Produce` attempt against `client`. Returns the committed
/// offset or the raw status.
async fn produce_attempt(
    mut client: VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
    key: Option<&[u8]>,
    value: &[u8],
) -> Result<u64, tonic::Status> {
    let response = client
        .produce(v1::ProduceRequest {
            topic: topic.to_string(),
            partition,
            record: Some(v1::Record {
                key: key.map(|k| k.to_vec()),
                value: value.to_vec(),
            }),
        })
        .await?
        .into_inner();
    Ok(response.offset)
}

/// One `ProduceBatch` attempt against `client`: append an ordered batch of
/// records to a resolved `(topic, partition)` as one unit. Returns the compact
/// `{ base_offset, count }` response or the raw status.
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
/// On a single node this resolves to that node once its metadata group has
/// elected itself.
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

/// A long-lived single-node in-process cluster plus the `tokio` runtime hosting
/// it, built once and reused across every proptest case. `meta_leader` is the
/// node that leads the metadata group (the lone node) and `ids` is the single
/// node id; both let the per-case helpers create topics and await partition
/// leaders without rediscovery.
struct Harness {
    rt: tokio::runtime::Runtime,
    clients: HashMap<String, VelaClientClient<Channel>>,
    ids: Vec<String>,
    meta_leader: String,
}

/// Build (once) and return the shared single-node cluster harness.
fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build multi-thread runtime");

        let ids = vec!["node-a".to_string()];
        let addr = free_addr();

        let (clients, meta_leader) = rt.block_on(async {
            // rf=1, no peers: the lone node leads the metadata group and every
            // partition, so routing is trivial and offsets are deterministic.
            spawn_server(config(&ids[0], addr, &[], 1));

            let mut clients = HashMap::new();
            clients.insert(ids[0].clone(), connect_client(addr).await);

            let meta_leader = discover_metadata_leader(&clients, &ids, "leader-probe-eqv").await;
            (clients, meta_leader)
        });

        Harness {
            rt,
            clients,
            ids,
            meta_leader,
        }
    })
}

/// Create `topic` with a single partition on the metadata leader, wait for it to
/// apply on every node and elect a partition leader, and return a client aimed
/// at that leader (the lone node).
async fn create_ready_topic(harness: &Harness, topic: &str) -> VelaClientClient<Channel> {
    let created = create_topic_attempt(harness.clients[&harness.meta_leader].clone(), topic, 1)
        .await
        .expect("the create commits on the single-node metadata leader");
    assert_eq!(created.name, topic);
    assert_eq!(created.partition_count, 1);

    await_topic_on_all_nodes(&harness.clients, &harness.ids, topic).await;
    let leader = await_agreed_partition_leader(&harness.clients, &harness.ids, topic, 0).await;
    harness.clients[&leader].clone()
}

/// Produce `records` as one batch to partition 0 of `topic`, retrying through a
/// transient `NotLeader` window, and return the per-record offsets
/// `base_offset + i`.
async fn produce_batch_offsets(
    client: &VelaClientClient<Channel>,
    topic: &str,
    records: Vec<v1::Record>,
) -> Vec<u64> {
    for _ in 0..100 {
        match produce_batch_attempt(client.clone(), topic, 0, records.clone()).await {
            Ok(resp) => {
                return (0..u64::from(resp.count))
                    .map(|i| resp.base_offset + i)
                    .collect();
            }
            Err(status) if not_leader_hint(&status).is_some() => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("batch produce on the leader failed: {status:?}"),
        }
    }
    panic!("batch produce did not commit within the bounded window");
}

/// Produce one single record to partition 0 of `topic`, retrying through a
/// transient `NotLeader` window, and return its committed offset.
async fn produce_single_offset(
    client: &VelaClientClient<Channel>,
    topic: &str,
    key: Option<&[u8]>,
    value: &[u8],
) -> u64 {
    for _ in 0..100 {
        match produce_attempt(client.clone(), topic, 0, key, value).await {
            Ok(offset) => return offset,
            Err(status) if not_leader_hint(&status).is_some() => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("single produce on the leader failed: {status:?}"),
        }
    }
    panic!("single produce did not commit within the bounded window");
}

/// Consume partition 0 of `topic` from offset 0, retrying until at least
/// `expected` records are returned, then reduce them to `(offset, key, value)`.
async fn consume_all(client: &VelaClientClient<Channel>, topic: &str, expected: usize) -> Consumed {
    for _ in 0..100 {
        match consume_attempt(client.clone(), topic, 0, 0, Some(64)).await {
            Ok(resp) if resp.records.len() >= expected => {
                return resp
                    .records
                    .into_iter()
                    .map(|entry| {
                        let record = entry
                            .record
                            .expect("each consumed entry carries a record payload");
                        (entry.offset, record.key, record.value)
                    })
                    .collect();
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(status) if not_leader_hint(&status).is_some() => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("consume on the leader failed: {status:?}"),
        }
    }
    panic!("the produced records were not all consumable within the bounded window");
}

/// The data one property case gathers from driving all three arms (batch,
/// singles, mixed) against the real cluster, asserted in the proptest body.
struct CaseOutcome {
    /// The N input record values, in input order.
    input_values: Vec<Vec<u8>>,
    /// Arm A: per-record offsets from the single N-record batch.
    batch_offsets: Vec<u64>,
    /// Arm B: per-record offsets from N single-record produces.
    single_offsets: Vec<u64>,
    /// Arm A consumed back: `(offset, key, value)` ascending.
    batch_consumed: Consumed,
    /// Arm B consumed back: `(offset, key, value)` ascending.
    single_consumed: Consumed,
    /// Mixed arm consumed back: `(offset, key, value)` ascending.
    mixed_consumed: Consumed,
}

/// Drive one property case end-to-end against the shared cluster on fresh,
/// uniquely-named topics, returning everything the body asserts.
fn run_case(records: Vec<(Option<Vec<u8>>, Vec<u8>)>) -> CaseOutcome {
    let harness = harness();
    let seq = TOPIC_COUNTER.fetch_add(1, Ordering::Relaxed);

    // Build the v1::Record list once; both arms produce the identical sequence.
    let records_v1: Vec<v1::Record> = records
        .iter()
        .map(|(key, value)| v1::Record {
            key: key.clone(),
            value: value.clone(),
        })
        .collect();
    let input_values: Vec<Vec<u8>> = records.iter().map(|(_, value)| value.clone()).collect();
    let n = records_v1.len();

    harness.rt.block_on(async {
        // --- Arm A: one batch of N to a fresh topic. -------------------------
        let batch_topic = format!("batch-eqv-{seq}");
        let batch_client = create_ready_topic(harness, &batch_topic).await;
        let batch_offsets =
            produce_batch_offsets(&batch_client, &batch_topic, records_v1.clone()).await;
        let batch_consumed = consume_all(&batch_client, &batch_topic, n).await;

        // --- Arm B: N single produces of the same sequence to a fresh topic. -
        let single_topic = format!("single-eqv-{seq}");
        let single_client = create_ready_topic(harness, &single_topic).await;
        let mut single_offsets = Vec::with_capacity(n);
        for (key, value) in &records {
            let offset =
                produce_single_offset(&single_client, &single_topic, key.as_deref(), value).await;
            single_offsets.push(offset);
        }
        let single_consumed = consume_all(&single_client, &single_topic, n).await;

        // --- Mixed arm: interleave a batch and singles on one fresh topic. ---
        // Produce the first half (always >= 1 record, so never an empty batch)
        // as one batch, then the remaining records one at a time. The consumed
        // order is the original input order, so the sequence stays contiguous
        // and gap-free across mixed batch and single produces (Req 10.4).
        let mixed_topic = format!("mixed-eqv-{seq}");
        let mixed_client = create_ready_topic(harness, &mixed_topic).await;
        let split = n.div_ceil(2);
        let _ =
            produce_batch_offsets(&mixed_client, &mixed_topic, records_v1[..split].to_vec()).await;
        for (key, value) in &records[split..] {
            produce_single_offset(&mixed_client, &mixed_topic, key.as_deref(), value).await;
        }
        let mixed_consumed = consume_all(&mixed_client, &mixed_topic, n).await;

        CaseOutcome {
            input_values,
            batch_offsets,
            single_offsets,
            batch_consumed,
            single_consumed,
            mixed_consumed,
        }
    })
}

/// Strategy: an ordered, non-empty sequence of `(key, value)` records with modest
/// sizes, since each case drives real in-process RPC and topic creation. N runs
/// from 1 to 8; keys are sometimes present (they are not persisted, so consume
/// returns `key: None` regardless) and values are small byte strings.
fn records_strategy() -> impl Strategy<Value = Vec<(Option<Vec<u8>>, Vec<u8>)>> {
    let key = prop::option::of(prop::collection::vec(any::<u8>(), 1..=6));
    let value = prop::collection::vec(any::<u8>(), 0..=16);
    prop::collection::vec((key, value), 1..=8)
}

proptest! {
    // Each case does real RPC + topic creation, so keep the case count modest.
    #![proptest_config(ProptestConfig::with_cases(16))]

    // Feature: batched-produce, Property 7: A batch of N is equivalent to N
    // single produces, and consumes back identically.
    //
    // Validates: Requirements 4.2, 10.1, 10.2, 10.3, 10.4
    #[test]
    fn batch_of_n_equivalent_to_n_singles_and_consumes_identically(
        records in records_strategy(),
    ) {
        let n = records.len();
        let outcome = run_case(records);
        let CaseOutcome {
            input_values,
            batch_offsets,
            single_offsets,
            batch_consumed,
            single_consumed,
            mixed_consumed,
        } = outcome;

        // The contiguous offsets a fresh single-partition topic assigns N
        // records: 0, 1, ..., N-1.
        let expected_offsets: Vec<u64> = (0..n as u64).collect();

        // --- Model-based equivalence (Req 4.2, 10.3). ------------------------
        // The batch's per-record offset sequence equals the singles' offset
        // sequence exactly — and both equal 0..N. A one-record batch (N == 1)
        // takes the single-record offset [0].
        prop_assert_eq!(
            &batch_offsets,
            &single_offsets,
            "a batch of N assigns the same committed offset sequence as N single produces"
        );
        prop_assert_eq!(
            &batch_offsets,
            &expected_offsets,
            "the batch's per-record offsets are contiguous 0..N from the fresh base"
        );
        prop_assert_eq!(
            &single_offsets,
            &expected_offsets,
            "the singles' offsets are contiguous 0..N from the fresh base"
        );

        // --- Consume parity for each arm (Req 10.1, 10.2). -------------------
        // Each arm returns exactly N records, ascending and gap-free, with
        // values byte-for-byte identical to the produced input in order. Keys
        // are not persisted this milestone, so every consumed key is None.
        for (arm, consumed) in [("batch", &batch_consumed), ("single", &single_consumed)] {
            prop_assert_eq!(
                consumed.len(),
                n,
                "{} arm returns exactly the N produced records, no extras",
                arm
            );
            for (i, (offset, key, value)) in consumed.iter().enumerate() {
                prop_assert_eq!(
                    *offset,
                    i as u64,
                    "{} arm returns records in ascending, gap-free offset order",
                    arm
                );
                prop_assert_eq!(
                    value,
                    &input_values[i],
                    "{} arm consumed value matches the produced input byte-for-byte",
                    arm
                );
                prop_assert_eq!(
                    key,
                    &None,
                    "{} arm: keys are not persisted; consume returns key: None",
                    arm
                );
            }
        }

        // --- Cross-arm identity (Req 10.2, 10.3). ----------------------------
        // The two arms consume back to the identical (offset, key, value)
        // sequence, so a batch is observationally indistinguishable from the
        // equivalent single produces.
        prop_assert_eq!(
            &batch_consumed,
            &single_consumed,
            "batch and single arms consume back to the identical record sequence"
        );

        // --- Mixed interleave parity (Req 10.4). -----------------------------
        // Records produced via a batch and via single produces to one partition
        // come back as one contiguous, gap-free, ascending sequence in produced
        // order — which here equals the original input order.
        prop_assert_eq!(
            mixed_consumed.len(),
            n,
            "the mixed arm returns exactly the N produced records"
        );
        for (i, (offset, key, value)) in mixed_consumed.iter().enumerate() {
            prop_assert_eq!(
                *offset,
                i as u64,
                "mixed batch+single produces consume back contiguous and gap-free ascending"
            );
            prop_assert_eq!(
                value,
                &input_values[i],
                "mixed arm consumed value matches the produced order byte-for-byte"
            );
            prop_assert_eq!(
                key,
                &None,
                "mixed arm: keys are not persisted; consume returns key: None"
            );
        }
    }
}
