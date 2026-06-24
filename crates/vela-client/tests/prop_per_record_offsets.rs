//! Property test for per-record offsets returned by [`Producer::produce_batch`].
//!
//! Feature: batched-produce, Property 5: Per-record offsets are returned in
//! input order.
//!
//! Property 5: *For any* ordered, multi-partition input of records, the
//! `Vec<u64>` `produce_batch` returns aligns one-to-one with the input in input
//! order, and each record's offset equals its batch's `base_offset` plus that
//! record's 0-based position within its partition's batch. Concretely, against a
//! fresh server every partition's base is `0`, so the j-th record (in input
//! order) that routed to a given partition is assigned offset `j` — the records
//! of one partition receive contiguous, ascending offsets in input order, and
//! the returned vector scatters those offsets back into the original input
//! positions (Requirement 1.4, 8.2, 8.3).
//!
//! The property is exercised through the public client API only: a fresh
//! [`ClientCore`] is seeded so `produce_batch` can route and dispatch with **no**
//! `DescribeTopic`/`FindLeader` round-trips — the metadata cache holds the
//! topic's partition count and the leader cache points every partition at one
//! in-process fake `VelaClient` server. That `FakeNode` answers `ProduceBatch`
//! by assigning each partition a contiguous run of offsets from a per-partition
//! base (0 for a fresh server), exactly as the real cluster does. Every other
//! RPC is stubbed `unimplemented`, since seeding the caches means none is
//! reached.
//!
//! Records are generated with **non-empty keys** so routing is deterministic
//! (keyed records hash to a stable partition), letting the test independently
//! recompute each record's resolved partition via the public `Producer::route`
//! and so derive the expected offsets without trusting `produce_batch`'s own
//! grouping. The shared server's per-partition offset map is reset before each
//! case so every partition's base starts at 0.
//!
//! Validates: Requirements 1.4, 8.2, 8.3

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use proptest::prelude::*;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use vela_client::{ClientCore, Producer, TopicMeta};
use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
use vela_proto::v1::{
    ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse, DeleteTopicRequest,
    DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse, DescribeTopicRequest,
    DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse, ListTopicsRequest,
    ListTopicsResponse, ProduceBatchRequest, ProduceBatchResponse, ProduceRequest, ProduceResponse,
};

/// The node id every partition's leader is seeded to; resolves to the fake
/// server's bound address via the client registry.
const NODE_ID: &str = "node-a";

/// An in-process fake of the client-facing `VelaClient` service that answers
/// only `ProduceBatch`, assigning each partition a contiguous run of offsets.
///
/// `offsets` maps a partition to its next base offset. A `ProduceBatch` for a
/// partition takes the partition's current base, advances it by the batch's
/// record count, and returns `{ base_offset, count }` — exactly the contiguous
/// per-partition offset assignment the real cluster performs (batched-produce
/// Requirement 1.3, 2.4). The map is shared with the test harness so it can be
/// reset to empty (every partition's base back to 0) before each property case.
#[derive(Clone, Default)]
struct FakeNode {
    /// partition -> next base offset to assign for that partition.
    offsets: Arc<Mutex<HashMap<u32, u64>>>,
}

#[tonic::async_trait]
impl VelaClientService for FakeNode {
    async fn produce_batch(
        &self,
        request: Request<ProduceBatchRequest>,
    ) -> Result<Response<ProduceBatchResponse>, Status> {
        let req = request.into_inner();
        let count = req.records.len() as u32;
        let mut offsets = self.offsets.lock().expect("offsets mutex poisoned");
        let base = *offsets.get(&req.partition).unwrap_or(&0);
        offsets.insert(req.partition, base + u64::from(count));
        Ok(Response::new(ProduceBatchResponse {
            base_offset: base,
            count,
        }))
    }

    async fn produce(
        &self,
        _request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        Err(Status::unimplemented("produce is not used by this test"))
    }

    async fn consume(
        &self,
        _request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        Err(Status::unimplemented("consume is not used by this test"))
    }

    async fn find_leader(
        &self,
        _request: Request<FindLeaderRequest>,
    ) -> Result<Response<FindLeaderResponse>, Status> {
        Err(Status::unimplemented(
            "find_leader is not used by this test (leader cache is pre-seeded)",
        ))
    }

    async fn describe_cluster(
        &self,
        _request: Request<DescribeClusterRequest>,
    ) -> Result<Response<DescribeClusterResponse>, Status> {
        Err(Status::unimplemented(
            "describe_cluster is not used by this test",
        ))
    }

    async fn create_topic(
        &self,
        _request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        Err(Status::unimplemented(
            "create_topic is not used by this test",
        ))
    }

    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> Result<Response<DeleteTopicResponse>, Status> {
        Err(Status::unimplemented(
            "delete_topic is not used by this test",
        ))
    }

    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        Err(Status::unimplemented(
            "list_topics is not used by this test",
        ))
    }

    async fn describe_topic(
        &self,
        _request: Request<DescribeTopicRequest>,
    ) -> Result<Response<DescribeTopicResponse>, Status> {
        Err(Status::unimplemented(
            "describe_topic is not used by this test (metadata cache is pre-seeded)",
        ))
    }
}

/// A shared multi-thread runtime hosting one fake `VelaClient` server bound on
/// an OS-chosen localhost port, built once and reused across every proptest
/// case. `offsets` is the server's per-partition base-offset map, exposed so a
/// case can reset it before running (so every partition's base starts at 0).
struct Harness {
    rt: tokio::runtime::Runtime,
    addr: String,
    offsets: Arc<Mutex<HashMap<u32, u64>>>,
}

fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build multi-thread runtime");
        let offsets: Arc<Mutex<HashMap<u32, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        let node = FakeNode {
            offsets: Arc::clone(&offsets),
        };
        // Bind the listener before returning so the URL is already accepting
        // connections — the client never races server startup.
        let addr = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind ephemeral port");
            let port = listener.local_addr().expect("local addr").port();
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(VelaClientServer::new(node))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .expect("fake server serves");
            });
            format!("http://127.0.0.1:{port}")
        });
        Harness { rt, addr, offsets }
    })
}

/// Run one `produce_batch` over `records` against the shared fake server, on a
/// fresh core seeded so routing and dispatch need no network metadata lookups.
///
/// Returns `(offsets, expected, partitions)`:
/// - `offsets` — what `produce_batch` returned;
/// - `expected` — the offsets independently recomputed for a fresh server (every
///   partition base 0): the j-th record routing to a partition, in input order,
///   gets offset `j`;
/// - `partitions[i]` — the partition `records[i]` resolved to (via the public
///   `Producer::route`), so the case can assert per-partition contiguity.
fn run_case(
    topic: &str,
    partition_count: u32,
    records: Vec<(Option<Vec<u8>>, Vec<u8>)>,
) -> (Vec<u64>, Vec<u64>, Vec<u32>) {
    let harness = harness();
    // Reset the server's per-partition offsets so every partition's base starts
    // at 0 for this case — the assumption the expected offsets are derived from.
    harness
        .offsets
        .lock()
        .expect("offsets mutex poisoned")
        .clear();

    harness.rt.block_on(async {
        // A fresh core per case: seed the metadata cache with the topic's
        // partition count and point every partition's believed leader at the
        // fake server, so `partition_count` and `dispatch` both hit cache and
        // never issue a `DescribeTopic`/`FindLeader` RPC.
        let core = Arc::new(ClientCore::new([(
            NODE_ID.to_string(),
            harness.addr.clone(),
        )]));
        core.metadata().put(
            topic,
            TopicMeta {
                partition_count,
                leaders: vec![Some(NODE_ID.to_string()); partition_count as usize],
                learned_at: std::time::Instant::now(),
            },
        );
        for partition in 0..partition_count {
            core.leaders()
                .insert(topic, partition, harness.addr.clone());
        }
        let producer = Producer::new(Arc::clone(&core));

        // Independently recompute each record's resolved partition (keyed
        // routing is deterministic) and the offset a fresh server assigns it:
        // each partition hands out 0, 1, 2, ... in input order.
        let mut next: HashMap<u32, u64> = HashMap::new();
        let mut expected = Vec::with_capacity(records.len());
        let mut partitions = Vec::with_capacity(records.len());
        for (key, _value) in &records {
            let partition = producer
                .route(topic, key.as_deref(), partition_count)
                .expect("non-zero partition count routes");
            let offset = *next.get(&partition).unwrap_or(&0);
            expected.push(offset);
            partitions.push(partition);
            next.insert(partition, offset + 1);
        }

        let offsets = producer
            .produce_batch(topic, records)
            .await
            .expect("produce_batch succeeds against the fake server");

        (offsets, expected, partitions)
    })
}

/// Strategy: an ordered list of `(key, value)` records with **non-empty** keys
/// (so keyed routing is deterministic) and modest sizes, since each case drives
/// real in-process RPC. Lengths run from empty up to a modest cap.
fn records_strategy() -> impl Strategy<Value = Vec<(Option<Vec<u8>>, Vec<u8>)>> {
    let key = prop::collection::vec(any::<u8>(), 1..=8).prop_map(Some);
    let value = prop::collection::vec(any::<u8>(), 0..=16);
    prop::collection::vec((key, value), 0..=40)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: batched-produce, Property 5: Per-record offsets are returned in
    // input order.
    //
    // Validates: Requirements 1.4, 8.2, 8.3
    #[test]
    fn per_record_offsets_align_with_input_order(
        topic in "[a-z]{1,8}",
        partition_count in 1u32..=6,
        records in records_strategy(),
    ) {
        let n = records.len();
        let (offsets, expected, partitions) = run_case(&topic, partition_count, records);

        // (1) One offset per input record, aligned one-to-one (Requirement 8.2).
        prop_assert_eq!(offsets.len(), n, "one committed offset per input record");

        // (2) Each returned offset equals its batch's base_offset (0 for a fresh
        // server) plus its 0-based position within its partition's batch, in
        // input order (Requirement 1.4, 8.3).
        prop_assert_eq!(&offsets, &expected);

        // (3) Within each partition the offsets — taken in input order — are
        // contiguous and strictly ascending from the base (0, 1, 2, ...), with
        // no gap, so a partition's batch occupies a contiguous offset run
        // (Requirement 8.3).
        let mut by_partition: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
        for (index, &partition) in partitions.iter().enumerate() {
            by_partition.entry(partition).or_default().push(offsets[index]);
        }
        for (partition, partition_offsets) in by_partition {
            for (position, &offset) in partition_offsets.iter().enumerate() {
                prop_assert_eq!(
                    offset,
                    position as u64,
                    "partition {} record at position {} must take a contiguous offset from base 0",
                    partition,
                    position,
                );
            }
        }
    }
}
