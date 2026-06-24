//! Batch admission-error integration tests (task 9.3).
//!
//! These tests pin down the **caller-visible rejections** of the `ProduceBatch`
//! path against a real in-process cluster, proving each rejection surfaces the
//! matching typed [`v1::VelaError`] code and appends **nothing** to a partition
//! that does exist:
//!
//! - **Missing topic (Requirement 6.4).** A batch to a never-created topic is
//!   rejected with `ErrorCode::TopicNotFound`.
//! - **Missing partition (Requirement 6.5).** A batch to a partition index that
//!   does not exist in the topic is rejected with
//!   `ErrorCode::PartitionNotFound`, and the topic's real partition 0 is left
//!   empty (nothing was appended).
//! - **Per-partition independence (Requirement 5.8, positive case).** A
//!   multi-partition batch driven through the high-level client commits each
//!   partition's group independently, returning per-record offsets.
//!
//! The cluster is a **single node** (`replication_factor = 1`, no peers): it
//! elects itself the metadata leader and every partition leader quickly, so the
//! admission assertions are deterministic with no cross-node redirect
//! bookkeeping. All waits are bounded retries with short sleeps, so a genuinely
//! broken cluster fails the test promptly rather than hanging.
//!
//! ## Cases delegated to existing tests (deliberately not forced here)
//!
//! Three of the criteria in task 9.3 cannot be observed deterministically
//! through the public RPC surface without a racy, flaky test. Rather than
//! fabricate timing-dependent cases, coverage is delegated to existing tests
//! that prove the same behavior directly and reliably:
//!
//! - **Deleting topic (Requirement 6.6).** Deletion *removes* the topic, so a
//!   batch after a completed delete observes `TopicNotFound`, not
//!   `TopicDeleting`; the transient `Deleting` state is not reliably observable
//!   end-to-end through the public RPCs within a non-flaky test. The handler
//!   shares the single-record path's `ensure_producible` admission, which is
//!   proven to reject a `Deleting` topic by the `vela-core` unit test
//!   `produce::tests::produce_batch_to_a_deleting_topic_is_rejected` and the
//!   `vela-server` handler unit test
//!   `service::tests::produce_batch_topic_deleting_is_rejected`.
//! - **Zero-partition topic routing error (Requirement 5.7).** `create_topic`
//!   validates `partitions >= 1`, so a true zero-partition topic cannot be
//!   staged via the cluster. The client-side
//!   `Producer::produce_batch` -> `resolve_partition` -> `RouteError::ZeroPartitions`
//!   path that surfaces `ClientError::NoPartitions` is covered by the
//!   `vela-client` `producer.rs` zero-partition discovery test
//!   (`producer::tests::produce_batch` zero-partition / `NoPartitions` coverage).
//! - **Leaderless partition unknown-leader error (Requirement 5.8, error
//!   case).** Forcing a partition to have *no* elected leader at the instant a
//!   batch is dispatched is inherently racy. The pure leaderless-resolution case
//!   that surfaces `ClientError::NoLeader` is covered by the `vela-client`
//!   consumer/dispatch tests
//!   (`consumer::tests::consume_partition_unavailable_is_distinct_from_transport`).
//!   This file instead asserts the reliably-observable *positive* independence
//!   property: a multi-partition batch commits each partition's group
//!   independently.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as _;
use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_client::VelaClient;
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
            "vela-server-batch-admit-{}-{n}-{nanos}",
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
/// details, if present. The server encodes every domain error this way so a
/// client can recover the precise classification and leader hint.
fn vela_error(status: &tonic::Status) -> Option<v1::VelaError> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    v1::VelaError::decode(details).ok()
}

/// The typed [`v1::ErrorCode`] (as its `i32` discriminant) carried in a
/// [`tonic::Status`]'s details, if the status carries a [`v1::VelaError`].
/// Lets a test assert the exact caller-visible classification of a rejection.
fn error_code(status: &tonic::Status) -> Option<i32> {
    vela_error(status).map(|error| error.code)
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

/// One `ProduceBatch` attempt against `client`: append an ordered batch of
/// records to a resolved `(topic, partition)` as one unit. Returns the compact
/// `{ base_offset, count }` response or the raw status (so a rejection's typed
/// error code can be classified).
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

/// Requirement 6.4 — a `ProduceBatch` targeting a topic that was never created
/// is rejected with the caller-visible `TopicNotFound` error and appends
/// nothing (there is no partition to append to).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_to_missing_topic_is_rejected_with_topic_not_found() {
    let ids = ["node-a".to_string()];
    let addrs = [free_addr()];
    spawn_server(config(&ids[0], addrs[0], &[], 1));

    let mut clients = HashMap::new();
    clients.insert(ids[0].clone(), connect_client(addrs[0]).await);

    // Ensure the node's metadata group is live (so a rejection is a real
    // admission decision, not a not-yet-ready node) by discovering the leader.
    let leader = discover_metadata_leader(&clients, &ids, "leader-probe").await;
    let client = clients[&leader].clone();

    // "ghost" was never created: the batch must be rejected with TopicNotFound.
    let status = produce_batch_attempt(
        client,
        "ghost",
        0,
        vec![v1::Record {
            key: None,
            value: b"x".to_vec(),
        }],
    )
    .await
    .expect_err("a batch to a never-created topic must be rejected (Req 6.4)");

    assert_eq!(
        error_code(&status),
        Some(v1::ErrorCode::TopicNotFound as i32),
        "a missing topic surfaces a caller-visible TopicNotFound error (Req 6.4)"
    );
}

/// Requirement 6.5 — a `ProduceBatch` targeting a partition index that does not
/// exist in the topic is rejected with the caller-visible `PartitionNotFound`
/// error and appends nothing; the topic's real partition 0 is left empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_to_missing_partition_is_rejected_and_appends_nothing() {
    let ids = ["node-a".to_string()];
    let addrs = [free_addr()];
    spawn_server(config(&ids[0], addrs[0], &[], 1));

    let mut clients = HashMap::new();
    clients.insert(ids[0].clone(), connect_client(addrs[0]).await);

    // Create `orders` with exactly one partition (index 0), then wait for the
    // create to apply and partition 0 to elect a leader so a produce to a real
    // partition would succeed — isolating the rejection to the bad index.
    let meta_leader = discover_metadata_leader(&clients, &ids, "leader-probe").await;
    let created = create_topic_attempt(clients[&meta_leader].clone(), "orders", 1)
        .await
        .expect("the create commits on the single-node metadata leader");
    assert_eq!(created.partition_count, 1);

    await_topic_on_all_nodes(&clients, &ids, "orders").await;
    let leader = await_agreed_partition_leader(&clients, &ids, "orders", 0).await;
    let client = clients[&leader].clone();

    // Partition 9 does not exist (the topic has only partition 0): reject with
    // PartitionNotFound (Req 6.5).
    let status = produce_batch_attempt(
        client.clone(),
        "orders",
        9,
        vec![v1::Record {
            key: None,
            value: b"x".to_vec(),
        }],
    )
    .await
    .expect_err("a batch to a non-existent partition must be rejected (Req 6.5)");

    assert_eq!(
        error_code(&status),
        Some(v1::ErrorCode::PartitionNotFound as i32),
        "a missing partition surfaces a caller-visible PartitionNotFound error (Req 6.5)"
    );

    // Nothing was appended: the topic's real partition 0 is still empty. Consume
    // from offset 0 and assert no records and a next offset of 0.
    let consumed = {
        let mut out = None;
        for _ in 0..100 {
            match consume_attempt(client.clone(), "orders", 0, 0, Some(64)).await {
                Ok(resp) => {
                    out = Some(resp);
                    break;
                }
                Err(status) if not_leader_hint(&status).is_some() => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(status) => panic!("consume on the leader failed: {status:?}"),
            }
        }
        out.expect("partition 0 is consumable from the leader within the bounded window")
    };
    assert!(
        consumed.records.is_empty(),
        "a rejected batch appends nothing — partition 0 stays empty (Req 6.5)"
    );
    assert_eq!(
        consumed.next_offset, 0,
        "the rejected batch left partition 0's committed offset unchanged (Req 6.5)"
    );
}

/// Requirement 5.8 (positive independence case) — a multi-partition batch driven
/// through the high-level [`VelaClient`] commits each partition's group
/// independently: every input record gets a committed offset, and per partition
/// the committed records form a contiguous, gap-free run from offset 0,
/// demonstrating that each partition's batch commits independently of the
/// others.
///
/// The pure "leaderless -> unknown-leader error" timing case is racy to force
/// end-to-end and is delegated to `vela-client`'s `ClientError::NoLeader`
/// coverage (see the module-level note); this asserts the reliably-observable
/// property instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_partition_batch_commits_each_partition_independently() {
    let ids = ["node-a".to_string()];
    let addrs = [free_addr()];
    spawn_server(config(&ids[0], addrs[0], &[], 1));

    let mut clients = HashMap::new();
    clients.insert(ids[0].clone(), connect_client(addrs[0]).await);

    // Create a 3-partition topic and wait until every partition has elected a
    // leader (the single node leads all three), so dispatch routes without a
    // leaderless stall.
    const PARTITIONS: u32 = 3;
    let meta_leader = discover_metadata_leader(&clients, &ids, "leader-probe").await;
    let created = create_topic_attempt(clients[&meta_leader].clone(), "events", PARTITIONS)
        .await
        .expect("the 3-partition create commits on the single-node metadata leader");
    assert_eq!(created.partition_count, PARTITIONS);

    await_topic_on_all_nodes(&clients, &ids, "events").await;
    for partition in 0..PARTITIONS {
        await_agreed_partition_leader(&clients, &ids, "events", partition).await;
    }

    // Drive the high-level client against the same node. Keyless records
    // round-robin across the 3 partitions, so a 6-record batch produces two
    // records per partition — exercising the per-partition fan-out and grouping.
    let client = VelaClient::new([(ids[0].clone(), format!("http://{}", addrs[0]))]);

    const RECORD_COUNT: usize = 6;
    let inputs: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..RECORD_COUNT)
        .map(|i| (None, format!("event-{i}").into_bytes()))
        .collect();

    let offsets = client
        .producer()
        .produce_batch("events", inputs)
        .await
        .expect("a multi-partition batch commits each partition's group (Req 5.8)");

    // Every input record got a committed offset, in input order (Req 8.2).
    assert_eq!(
        offsets.len(),
        RECORD_COUNT,
        "every input record receives exactly one committed offset"
    );

    // Independence: consume each partition and assert its committed records form
    // a contiguous, gap-free run from offset 0, and the per-partition counts sum
    // to the total produced. Each partition's batch committed on its own group.
    let raw = clients[&ids[0]].clone();
    let mut total: usize = 0;
    for partition in 0..PARTITIONS {
        let consumed = {
            let mut out = None;
            for _ in 0..100 {
                match consume_attempt(raw.clone(), "events", partition, 0, Some(64)).await {
                    Ok(resp) => {
                        out = Some(resp);
                        break;
                    }
                    Err(status) if not_leader_hint(&status).is_some() => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(status) => panic!("consume on partition {partition} failed: {status:?}"),
                }
            }
            out.expect("each partition is consumable from the leader within the window")
        };

        // Contiguous, gap-free from offset 0 — this partition's batch committed
        // independently as its own contiguous run (Req 5.8 independence).
        for (i, entry) in consumed.records.iter().enumerate() {
            assert_eq!(
                entry.offset, i as u64,
                "partition {partition}'s records are contiguous and gap-free from 0"
            );
        }
        assert_eq!(
            consumed.next_offset as usize,
            consumed.records.len(),
            "partition {partition}'s next offset matches its independently committed count"
        );
        total += consumed.records.len();
    }

    assert_eq!(
        total, RECORD_COUNT,
        "the per-partition committed records sum to the total batch — each partition's \
         group committed its own share independently (Req 5.8)"
    );
}
