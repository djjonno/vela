//! The produce client.
//!
//! [`Producer::produce`] wraps partition routing so the partition is resolved
//! *before* dispatch (Requirement 4.1, 4.2): it looks up the topic's partition
//! count, asks the client-side [`PartitionRouter`](crate::router::PartitionRouter)
//! for the target partition, resolves that partition's believed leader, and
//! sends the `Produce` RPC straight to it (Requirement 11.1).
//!
//! Routing also guards the zero-partition case (Requirement 1.8, 1.9, 5.3): a
//! topic that exists but reports no partitions yet is not a hard failure. The
//! producer invalidates the topic's cached metadata and re-fetches its partition
//! count on a fixed retry interval until a partition appears or a discovery
//! timeout elapses, surfacing [`ClientError::NoPartitions`] only on timeout.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use vela_proto::v1::{ProduceBatchRequest, ProduceRequest, Record};

use crate::core::ClientCore;
use crate::error::{ClientError, Result};
use crate::retry::RetryBudget;
use crate::router::RouteError;

/// One per-`(topic, partition)` batch produced by [`group_by_partition`].
///
/// Because a single `produce_batch` call targets one topic, the topic is
/// constant across every batch, so the grouping keys on `partition` alone —
/// `(topic, partition)` collapses to just `partition` (Requirement 5.4, 5.5).
///
/// `records` holds the records that routed to `partition`, each paired with its
/// original index in the producer's input slice, in preserved per-partition
/// input order. Keeping the original index lets the caller scatter each
/// committed offset (`base_offset + position`) back into input order once the
/// batch commits (Requirement 5.4).
//
// Consumed by [`Producer::produce_batch`] (task 6.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionBatch {
    /// The resolved partition every record in this batch routed to.
    pub partition: u32,
    /// `(original input index, key, value)` triples in preserved per-partition
    /// input order.
    pub records: Vec<(usize, Option<Vec<u8>>, Vec<u8>)>,
}

/// Group `records` — each paired with the partition resolved for it by the
/// existing [`PartitionRouter`](crate::router::PartitionRouter) and supplied as
/// `partitions[i]` — into one [`PartitionBatch`] per distinct resolved
/// partition.
///
/// This is a pure, deterministic grouping function: it performs no routing
/// itself (the keyed/keyless routing rules stay in the router, Requirement
/// 5.1–5.3) and no I/O. It takes the per-record resolved partition as input so
/// the grouping can be tested in isolation.
///
/// The grouping guarantees (Requirement 5.4, 5.5):
/// - each record is placed in **exactly one** batch;
/// - there is **exactly one** batch per distinct resolved partition;
/// - per-partition **input order is preserved** (records are visited in input
///   order, so each batch's records keep their relative input order);
/// - each record's **original input index** is recorded alongside it, so
///   per-record offsets can later be scattered back into input order from each
///   batch's `base_offset + position`.
///
/// Batches are returned in **first-seen partition order**: the partition of the
/// first input record comes first, then the next not-yet-seen partition, and so
/// on. First-seen order is deterministic given the input, which lets the
/// property test assert determinism without sorting.
///
/// # Panics
///
/// Panics if `records.len() != partitions.len()`; the caller resolves one
/// partition per record, so an unequal length is a programming error.
//
// Consumed by [`Producer::produce_batch`] (task 6.3).
pub(crate) fn group_by_partition(
    records: Vec<(Option<Vec<u8>>, Vec<u8>)>,
    partitions: &[u32],
) -> Vec<PartitionBatch> {
    assert_eq!(
        records.len(),
        partitions.len(),
        "every record must have exactly one resolved partition",
    );

    // `slot` maps a partition to the index of its batch in `batches`, so each
    // distinct partition gets exactly one batch and the first-seen order is
    // preserved by `batches`' push order.
    let mut slot: HashMap<u32, usize> = HashMap::new();
    let mut batches: Vec<PartitionBatch> = Vec::new();

    for (index, (key, value)) in records.into_iter().enumerate() {
        let partition = partitions[index];
        let batch = match slot.get(&partition) {
            Some(&pos) => &mut batches[pos],
            None => {
                slot.insert(partition, batches.len());
                batches.push(PartitionBatch {
                    partition,
                    records: Vec::new(),
                });
                batches
                    .last_mut()
                    .expect("a batch was just pushed for this partition")
            }
        };
        batch.records.push((index, key, value));
    }

    batches
}

/// Interval between zero-partition discovery retries. Mirrors the retry budget's
/// base backoff so the producer re-checks for newly-created partitions at the
/// same cadence the dispatch engine retries (Requirement 1.8).
const DISCOVERY_INTERVAL: Duration = RetryBudget::DEFAULT_BASE;

/// Ceiling on the zero-partition discovery wait before the producer gives up and
/// surfaces [`ClientError::NoPartitions`]. Mirrors the retry budget's total
/// elapsed-time budget (Requirement 1.8, 8.5).
const DISCOVERY_TIMEOUT: Duration = RetryBudget::DEFAULT_TOTAL;

/// Produces records to topics, routing each record to a partition leader.
#[derive(Debug, Clone)]
pub struct Producer {
    core: Arc<ClientCore>,
}

impl Producer {
    /// Create a producer over a shared client core.
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }

    /// Produce `value` to `topic`, optionally keyed by `key`.
    ///
    /// Routing happens client-side before dispatch:
    /// 1. resolve the topic's partition count (cached after first lookup);
    /// 2. route `(topic, key)` to a partition — a non-empty key maps
    ///    deterministically, a `None`/empty key round-robins (Requirement 4.1,
    ///    4.2);
    /// 3. send `Produce` to that partition's believed leader, retrying through
    ///    [`ClientCore::dispatch`] if the leader redirects us with `NotLeader`
    ///    (Requirement 11.1–11.4).
    ///
    /// Returns the committed 0-based offset assigned by the leader
    /// (Requirement 4.4).
    pub async fn produce(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64> {
        // Resolve the target partition, applying the zero-partition discovery
        // retry before routing (Requirement 1.8, 1.9, 5.3).
        let partition = self.resolve_partition(topic, key).await?;

        // Captured by the per-attempt closure; cloned on each attempt so a
        // redirect can re-send the identical request to the new leader.
        let core = Arc::clone(&self.core);
        let topic_owned = topic.to_string();
        let key_owned = key.map(<[u8]>::to_vec);
        let value = value.into();

        self.core
            .dispatch(topic, partition, move |addr| {
                let core = Arc::clone(&core);
                let topic = topic_owned.clone();
                let key = key_owned.clone();
                let value = value.clone();
                async move {
                    let mut client = core.client_for(&addr)?;
                    let response = client
                        .produce(ProduceRequest {
                            topic,
                            partition,
                            record: Some(Record { key, value }),
                        })
                        .await?
                        .into_inner();
                    Ok(response.offset)
                }
            })
            .await
    }

    /// Produce an ordered collection of `records` to `topic`, returning each
    /// input record's committed offset **in input order** (Requirement 8.1,
    /// 8.2).
    ///
    /// Each record is a `(key, value)` pair; a non-empty key routes
    /// deterministically and a `None`/empty key round-robins, reusing the exact
    /// keyed/keyless rules of [`produce`](Self::produce) (Requirement 5.1–5.3).
    /// The records that resolve to one `(topic, partition)` are grouped into a
    /// single [`PartitionBatch`] preserving their input order (Requirement 5.4),
    /// and exactly one `ProduceBatch` RPC is dispatched per distinct resolved
    /// partition (Requirement 5.5) through [`ClientCore::dispatch`], so every
    /// batch inherits `NotLeader` redirection, transport re-resolution, and the
    /// retry budget (Requirement 5.6, 6.2).
    ///
    /// On success each batch reports its `base_offset`; the j-th record of a
    /// batch (0-based position `j`) is assigned `base_offset + j`, scattered back
    /// into the original input order so the returned `Vec<u64>` aligns one-to-one
    /// with `records` (Requirement 8.2, 8.3).
    ///
    /// Errors surface per the existing [`ClientError`] variants: a routing
    /// failure (the topic reports no partitions) surfaces
    /// [`ClientError::NoPartitions`] (Requirement 5.7); an unresolvable partition
    /// leader surfaces [`ClientError::NoLeader`] (Requirement 5.8). On any batch
    /// failure the call returns that batch's error and reports **no** offsets —
    /// the partially-filled offset vector is dropped (Requirement 8.4). The
    /// single-record [`produce`](Self::produce) is unchanged.
    pub async fn produce_batch(
        &self,
        topic: &str,
        records: Vec<(Option<Vec<u8>>, Vec<u8>)>,
    ) -> Result<Vec<u64>> {
        // No records means no batches and no offsets — return early before any
        // routing or dispatch.
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Resolve every record to a partition IN INPUT ORDER, so keyless
        // round-robin advances in the order records were supplied (the router
        // round-robins per call). A routing failure here propagates as the
        // error (Requirement 5.1–5.3, 5.7).
        let mut partitions = Vec::with_capacity(records.len());
        for (key, _value) in &records {
            partitions.push(self.resolve_partition(topic, key.as_deref()).await?);
        }

        // Group the records that resolved to each partition into exactly one
        // batch per partition, preserving per-partition input order and each
        // record's original input index (Requirement 5.4, 5.5).
        let n = records.len();
        let batches = group_by_partition(records, &partitions);

        // Per-input committed offsets, scattered back into input order from each
        // batch's `base_offset + position` (Requirement 8.2, 8.3).
        let mut offsets = vec![0u64; n];

        // Dispatch one ProduceBatch per resolved partition. Sequential dispatch
        // is the simplest correct ordering; a per-partition failure surfaces that
        // partition's error and aborts (Requirement 5.5, 5.8, 8.4).
        for batch in batches {
            let partition = batch.partition;

            // The proto records to send, and the original input index for the
            // j-th record in the batch (parallel to `proto_records`).
            let mut proto_records = Vec::with_capacity(batch.records.len());
            let mut indices = Vec::with_capacity(batch.records.len());
            for (index, key, value) in batch.records {
                indices.push(index);
                proto_records.push(Record { key, value });
            }

            // Captured by the per-attempt closure; cloned on each attempt so a
            // redirect can re-send the identical batch to the new leader.
            let core = Arc::clone(&self.core);
            let topic_owned = topic.to_string();
            let (base_offset, count) = self
                .core
                .dispatch(topic, partition, move |addr| {
                    let core = Arc::clone(&core);
                    let topic = topic_owned.clone();
                    let records = proto_records.clone();
                    async move {
                        let mut client = core.client_for(&addr)?;
                        let response = client
                            .produce_batch(ProduceBatchRequest {
                                topic,
                                partition,
                                records,
                            })
                            .await?
                            .into_inner();
                        Ok((response.base_offset, response.count))
                    }
                })
                .await?;

            debug_assert_eq!(
                count as usize,
                indices.len(),
                "the batch response count must match the records dispatched",
            );

            // Scatter `base_offset + position` back into input order.
            for (position, index) in indices.into_iter().enumerate() {
                offsets[index] = base_offset + position as u64;
            }
        }

        Ok(offsets)
    }

    /// Resolve `(topic, key)` to a partition, applying the zero-partition
    /// discovery retry (Requirement 1.8, 1.9, 5.3).
    ///
    /// On the common path the topic's `partition_count` is non-zero and the
    /// canonical partitioner resolves a partition immediately, with no waiting.
    /// When the topic exists but reports zero partitions the router rejects the
    /// record ([`RouteError::ZeroPartitions`]) rather than computing a partition
    /// against a zero count (Requirement 1.9, 5.3); instead of failing outright,
    /// the producer invalidates the topic's cached metadata so the next lookup
    /// performs a `Metadata_Refresh`, then re-checks the partition count on the
    /// [`DISCOVERY_INTERVAL`] until a partition appears or [`DISCOVERY_TIMEOUT`]
    /// elapses — returning [`ClientError::NoPartitions`] on timeout
    /// (Requirement 1.8, 8.5).
    ///
    /// The discovery waits go through `tokio::time` directly because
    /// [`ClientCore`] exposes no public clock accessor; under a paused tokio
    /// runtime they consume only virtual time, so the loop is deterministically
    /// testable.
    async fn resolve_partition(&self, topic: &str, key: Option<&[u8]>) -> Result<u32> {
        let start = tokio::time::Instant::now();
        loop {
            let partition_count = self.core.partition_count(topic).await?;
            match self.core.router().resolve(topic, key, partition_count) {
                // Normal path: a non-zero count routes to a partition at once.
                Ok(partition) => return Ok(partition),
                // The topic exists but has no partitions yet. Re-discover until
                // one appears or the discovery window is spent (Requirement 1.8).
                Err(RouteError::ZeroPartitions { .. }) => {
                    if start.elapsed() >= DISCOVERY_TIMEOUT {
                        return Err(ClientError::NoPartitions {
                            topic: topic.to_string(),
                        });
                    }
                    // Drop the cached zero-partition metadata so the next
                    // `partition_count` re-fetches it (Requirement 1.5, 1.6),
                    // then wait the retry interval before re-checking.
                    self.core.metadata().invalidate(topic);
                    tokio::time::sleep(DISCOVERY_INTERVAL).await;
                }
            }
        }
    }

    /// The partition this producer would route `(topic, key)` to, given a known
    /// `partition_count`. Exposed for callers and tests that want to inspect
    /// routing without dispatching. Returns [`RouteError`] when
    /// `partition_count == 0` (Requirement 1.9, 5.3).
    pub fn route(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        partition_count: u32,
    ) -> std::result::Result<u32, RouteError> {
        self.core.router().resolve(topic, key, partition_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer() -> Producer {
        Producer::new(Arc::new(ClientCore::new([(
            "node-a".to_string(),
            "http://node-a:50051".to_string(),
        )])))
    }

    #[test]
    fn keyed_routing_is_deterministic_through_the_producer() {
        let p = producer();
        let first = p
            .route("orders", Some(b"user-1"), 8)
            .expect("non-zero count");
        for _ in 0..50 {
            assert_eq!(
                p.route("orders", Some(b"user-1"), 8)
                    .expect("non-zero count"),
                first
            );
        }
    }

    #[test]
    fn keyless_routing_covers_partitions_through_the_producer() {
        let p = producer();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4 {
            seen.insert(p.route("orders", None, 4).expect("non-zero count"));
        }
        let expected: std::collections::HashSet<u32> = (0..4u32).collect();
        assert_eq!(seen, expected);
    }

    // --- Routing/grouping helper (task 6.1, Requirement 5.4, 5.5) --------

    /// A record `(key, value)` with a string value, for terse test fixtures.
    fn rec(key: Option<&str>, value: &str) -> (Option<Vec<u8>>, Vec<u8>) {
        (
            key.map(|k| k.as_bytes().to_vec()),
            value.as_bytes().to_vec(),
        )
    }

    #[test]
    fn group_by_partition_preserves_per_partition_input_order() {
        // Three partitions interleaved across the input; each batch must keep its
        // records in the order they appeared in the input, tagged with the
        // original input index (Requirement 5.4).
        let records = vec![
            rec(None, "a"), // idx 0 -> p1
            rec(None, "b"), // idx 1 -> p0
            rec(None, "c"), // idx 2 -> p1
            rec(None, "d"), // idx 3 -> p2
            rec(None, "e"), // idx 4 -> p0
        ];
        let partitions = [1, 0, 1, 2, 0];

        let batches = group_by_partition(records, &partitions);

        // First-seen partition order: p1 (idx 0), then p0 (idx 1), then p2 (idx 3).
        assert_eq!(
            batches,
            vec![
                PartitionBatch {
                    partition: 1,
                    records: vec![(0, None, b"a".to_vec()), (2, None, b"c".to_vec()),],
                },
                PartitionBatch {
                    partition: 0,
                    records: vec![(1, None, b"b".to_vec()), (4, None, b"e".to_vec()),],
                },
                PartitionBatch {
                    partition: 2,
                    records: vec![(3, None, b"d".to_vec())],
                },
            ],
        );
    }

    #[test]
    fn group_by_partition_places_each_record_in_exactly_one_batch() {
        // Every input index appears once and only once across all batches, and
        // exactly one batch exists per distinct partition (Requirement 5.4, 5.5).
        let records = vec![
            rec(Some("k0"), "v0"),
            rec(Some("k1"), "v1"),
            rec(Some("k2"), "v2"),
            rec(Some("k3"), "v3"),
        ];
        let partitions = [2, 2, 0, 2];

        let batches = group_by_partition(records, &partitions);

        // Two distinct partitions -> two batches.
        assert_eq!(batches.len(), 2);
        let distinct: std::collections::HashSet<u32> =
            batches.iter().map(|b| b.partition).collect();
        assert_eq!(distinct, std::collections::HashSet::from([2, 0]));

        // Each original index appears exactly once across all batches.
        let mut indices: Vec<usize> = batches
            .iter()
            .flat_map(|b| b.records.iter().map(|(i, _, _)| *i))
            .collect();
        indices.sort_unstable();
        assert_eq!(indices, vec![0, 1, 2, 3]);
    }

    #[test]
    fn group_by_partition_empty_input_yields_no_batches() {
        let batches = group_by_partition(Vec::new(), &[]);
        assert!(batches.is_empty());
    }

    #[test]
    fn group_by_partition_single_partition_keeps_one_batch_in_order() {
        // All records routing to one partition collapse to a single batch that
        // preserves the full input order and indices.
        let records = vec![rec(None, "x"), rec(None, "y"), rec(None, "z")];
        let partitions = [3, 3, 3];

        let batches = group_by_partition(records, &partitions);

        assert_eq!(
            batches,
            vec![PartitionBatch {
                partition: 3,
                records: vec![
                    (0, None, b"x".to_vec()),
                    (1, None, b"y".to_vec()),
                    (2, None, b"z".to_vec()),
                ],
            }],
        );
    }

    #[test]
    #[should_panic(expected = "every record must have exactly one resolved partition")]
    fn group_by_partition_panics_on_length_mismatch() {
        let records = vec![rec(None, "a"), rec(None, "b")];
        // Only one resolved partition for two records — a programming error.
        group_by_partition(records, &[0]);
    }

    // --- produce_batch (task 6.3, Requirement 8) -------------------------

    /// An empty input does no routing and no dispatch — it returns an empty
    /// offset vector immediately (no server contact required).
    #[tokio::test]
    async fn produce_batch_empty_input_returns_no_offsets() {
        let p = producer();
        let offsets = p
            .produce_batch("orders", Vec::new())
            .await
            .expect("empty input yields an empty result without dispatch");
        assert!(offsets.is_empty());
    }

    // --- Zero-partition discovery retry (task 4.2, Requirement 1.8) -------

    use crate::metadata_cache::TopicMeta;

    /// Topic metadata reporting `partition_count` partitions, stamped fresh
    /// against the (paused) tokio clock so [`ClientCore::partition_count`] serves
    /// it from cache without a network round-trip.
    fn topic_meta(partition_count: u32) -> TopicMeta {
        TopicMeta {
            partition_count,
            leaders: vec![None; partition_count as usize],
            learned_at: tokio::time::Instant::now().into_std(),
        }
    }

    /// A zero-partition topic that never gains a partition surfaces
    /// [`ClientError::NoPartitions`] once the discovery timeout elapses
    /// (Requirement 1.8, 8.5).
    ///
    /// A background task emulates the cluster, keeping the metadata cache
    /// populated with a fresh zero-partition entry at a cadence finer than the
    /// producer's discovery interval, so each re-check the producer makes after
    /// invalidating still reads a zero count (never falling through to a network
    /// fetch). The producer must give up and report no partitions rather than
    /// loop forever.
    #[tokio::test(start_paused = true)]
    async fn zero_partition_topic_times_out_with_no_partitions() {
        let p = producer();
        let core = Arc::clone(&p.core);
        // Seed a fresh zero-partition entry so the first lookup sees the topic
        // exists but has no partitions yet.
        core.metadata().put("orders", topic_meta(0));

        let server = {
            let core = Arc::clone(&core);
            async move {
                loop {
                    core.metadata().put("orders", topic_meta(0));
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        };

        let result = tokio::select! {
            res = p.resolve_partition("orders", None) => res,
            _ = server => unreachable!("the server emulation runs until the producer gives up"),
        };

        assert!(
            matches!(result, Err(ClientError::NoPartitions { topic }) if topic == "orders"),
            "a topic that never gains a partition must time out with NoPartitions",
        );
    }

    /// A topic that starts with zero partitions but gains them mid-discovery
    /// resolves successfully once a partition appears, instead of failing
    /// (Requirement 1.8).
    ///
    /// The emulated cluster reports zero partitions for the first 250 ms, then
    /// four partitions thereafter. The producer's discovery loop must pick up the
    /// new count on a subsequent re-check and route the record within range.
    #[tokio::test(start_paused = true)]
    async fn zero_partition_topic_then_gains_a_partition_routes() {
        let p = producer();
        let core = Arc::clone(&p.core);
        // Seed a fresh zero-partition entry for the first lookup.
        core.metadata().put("orders", topic_meta(0));

        let started = tokio::time::Instant::now();
        let server = {
            let core = Arc::clone(&core);
            async move {
                loop {
                    let count = if started.elapsed() >= Duration::from_millis(250) {
                        4
                    } else {
                        0
                    };
                    core.metadata().put("orders", topic_meta(count));
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        };

        let partition = tokio::select! {
            res = p.resolve_partition("orders", Some(b"user-1")) => {
                res.expect("routing succeeds once a partition appears")
            }
            _ = server => unreachable!("the server emulation runs until the producer resolves"),
        };

        assert!(
            partition < 4,
            "the record must route within the discovered partition range, got {partition}",
        );
    }
}

#[cfg(test)]
mod prop_grouping {
    //! Property test for the pure routing/grouping helper [`group_by_partition`]
    //! (task 6.1). Because the helper and [`PartitionBatch`] are `pub(crate)`,
    //! this test lives in-file as a sibling `#[cfg(test)]` module rather than in
    //! the crate's `tests/` directory (integration tests only see the public
    //! API). `proptest` is available as a dev-dependency.
    //!
    //! Routing itself is the router's tested concern, so the resolved
    //! `partitions` vector is generated directly; the grouping is the unit under
    //! test.

    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    /// An input record `(key, value)` paired with the raw partition seed used to
    /// derive its resolved partition (reduced modulo the partition count). Keys
    /// are arbitrary optional bytes, values arbitrary bytes, both modest in size
    /// so the many iterations stay fast.
    type RecordSeed = ((Option<Vec<u8>>, Vec<u8>), u32);

    /// Strategy: an arbitrary ordered list of records, each carrying a raw
    /// partition seed. Lengths run from empty up to a modest cap.
    fn record_seeds() -> impl Strategy<Value = Vec<RecordSeed>> {
        let key = proptest::option::of(prop::collection::vec(any::<u8>(), 0..=16));
        let value = prop::collection::vec(any::<u8>(), 0..=32);
        let record = (key, value);
        let seeded = (record, any::<u32>());
        prop::collection::vec(seeded, 0..=64)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: batched-produce, Property 4: Routing and grouping partition
        // every record exactly once and preserve per-partition input order.
        //
        // Validates: Requirements 5.1, 5.4, 5.5
        #[test]
        fn grouping_partitions_every_record_once_and_preserves_input_order(
            seeds in record_seeds(),
            partition_count in 1u32..=16,
        ) {
            let n = seeds.len();

            // Split the seeds into the records and the resolved partitions, with
            // each resolved partition in `0..partition_count`.
            let records: Vec<(Option<Vec<u8>>, Vec<u8>)> =
                seeds.iter().map(|(record, _)| record.clone()).collect();
            let partitions: Vec<u32> =
                seeds.iter().map(|(_, raw)| raw % partition_count).collect();

            let batches = group_by_partition(records.clone(), &partitions);

            // (1) Exactly once: collecting every recorded index across all
            // batches yields exactly the multiset {0..n}, each once.
            let mut all_indices: Vec<usize> = batches
                .iter()
                .flat_map(|batch| batch.records.iter().map(|(index, _, _)| *index))
                .collect();
            all_indices.sort_unstable();
            prop_assert_eq!(all_indices, (0..n).collect::<Vec<usize>>());

            // (2) One batch per distinct partition: the set of batch partitions
            // equals the set of distinct resolved partitions, the batch count
            // equals the number of distinct partitions, and no two batches share
            // a partition.
            let distinct_partitions: HashSet<u32> = partitions.iter().copied().collect();
            let batch_partitions: HashSet<u32> =
                batches.iter().map(|batch| batch.partition).collect();
            prop_assert_eq!(&batch_partitions, &distinct_partitions);
            prop_assert_eq!(batches.len(), distinct_partitions.len());
            prop_assert_eq!(
                batch_partitions.len(),
                batches.len(),
                "no two batches may share a partition",
            );

            for batch in &batches {
                // (3a) Per-partition input order preserved: the recorded indices
                // within a batch are strictly increasing (records are visited in
                // input order).
                for window in batch.records.windows(2) {
                    prop_assert!(
                        window[0].0 < window[1].0,
                        "indices within a batch must strictly increase",
                    );
                }

                // (3b) Each batch's records match the input filtered to that
                // partition, in the same relative order.
                let expected: Vec<(usize, Option<Vec<u8>>, Vec<u8>)> = records
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| partitions[*index] == batch.partition)
                    .map(|(index, (key, value))| (index, key.clone(), value.clone()))
                    .collect();
                prop_assert_eq!(&batch.records, &expected);

                // (4) Value/key fidelity: each (index, key, value) in a batch
                // equals the original input record at that index.
                for (index, key, value) in &batch.records {
                    prop_assert_eq!(key, &records[*index].0);
                    prop_assert_eq!(value, &records[*index].1);
                }
            }

            // (5) Determinism: grouping the identical input twice yields the
            // identical Vec<PartitionBatch>.
            let again = group_by_partition(records, &partitions);
            prop_assert_eq!(again, batches);
        }
    }
}
