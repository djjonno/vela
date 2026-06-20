//! Seed-driven workload generator: client operations over the cluster.
//!
//! The generator turns the run's `workload` RNG stream
//! ([`SeedStreams::workload`](crate::rng::SeedStreams::workload), a
//! [`SplitMix64`]) and the [`ScenarioParameters`] into a [`Workload`] of exactly
//! [`ScenarioParameters::workload_size`] [`ClientOperation`]s, composed of topic
//! create / delete, produce, and consume operations selected as a deterministic
//! function of the seed (Requirement 8.1). Because every byte and every choice
//! is drawn from a single stream in a fixed order, the same seed reproduces an
//! identical workload.
//!
//! # Routing and record shape
//!
//! - **Keyed produce (Requirement 8.2):** a keyed record routes to its partition
//!   through the production [`PartitionRouter`] (in-house FNV-1a,
//!   `hash(key) % partition_count`) — the *same* rule production uses, so the
//!   harness verifies the real partitioning rather than a parallel model. The
//!   router's keyed path is a pure function of the key bytes and the partition
//!   count, so a fresh router yields the production mapping with no shared state.
//! - **Keyless produce (Requirement 8.2):** a keyless record's partition is
//!   drawn directly from the `workload` stream over `0..partition_count`, keeping
//!   selection a pure function of the seed rather than relying on the router's
//!   process-global round-robin counter.
//! - **Record shape (Requirement 8.3):** a produce's value length is drawn in
//!   `0..=`[`MAX_VALUE_LEN`]; a keyed record's key length is drawn in
//!   [`MIN_KEY_LEN`]`..=`[`MAX_KEY_LEN`]. Both the lengths and the byte contents
//!   come from the `workload` stream.
//!
//! # Interleaving with the fault schedule (Requirement 8.7)
//!
//! Generation never consults fault state: it produces the full, ordered op
//! sequence up front and does not pause or gate on crashes, restarts, or
//! partitions. The runtime (task 12.1) schedules these operations across the
//! run's virtual timeline interleaved with the `Fault_Schedule`, so produce and
//! consume operations continue to be issued *while* faults are in effect rather
//! than waiting for a heal.
//!
//! # Leader redirection (Requirements 8.4, 8.5, 8.6)
//!
//! Following a leader redirect is execution behaviour performed by the runtime
//! when it issues an op against the cluster. This module defines the operation
//! set, the routing, the record shape, and the [`MAX_REDIRECT_HOPS`] bound (5).
//! When the runtime issues an op it follows the cluster's redirect toward the
//! current leader for up to [`MAX_REDIRECT_HOPS`] successive hops; an
//! unresolved-redirection (5 hops without reaching a leader) or a no-leader
//! response is recorded by the [`History`](crate::history) as a *valid response*,
//! never a property violation.
//!
//! # Coordination with the runtime
//!
//! The [`Scheduler`](crate::scheduler::Scheduler) carries a client operation as
//! [`Event::ClientOp`](crate::scheduler::Event::ClientOp) holding only a 0-based
//! `seq`. That `seq` indexes into this [`Workload`]'s op list
//! ([`Workload::op`]): when the runtime pops a `ClientOp { seq }` it looks up
//! `workload.op(seq)` to obtain the concrete [`ClientOperation`] to issue, then
//! records the response into the `History`. Keeping the heavyweight operation
//! arguments here (and only an index on the timeline) keeps the event queue
//! cheap and the workload a single source of truth.

use vela_core::consume::{MAX_MAX_RECORDS, MIN_MAX_RECORDS};
use vela_core::{Offset, PartitionIndex, PartitionRouter};

use crate::rng::SplitMix64;
use crate::scenario::ScenarioParameters;

/// The maximum number of successive leader redirections the runtime follows for
/// a single [`ClientOperation`] before recording an unresolved-redirection
/// response (Requirement 8.4, 8.5).
pub const MAX_REDIRECT_HOPS: u8 = 5;

/// The largest produce value length, in bytes (Requirement 8.3). Value lengths
/// are drawn inclusively from `0..=MAX_VALUE_LEN`.
pub const MAX_VALUE_LEN: usize = 65_536;

/// The smallest key length, in bytes, for a keyed produce (Requirement 8.3).
pub const MIN_KEY_LEN: usize = 1;

/// The largest key length, in bytes, for a keyed produce (Requirement 8.3).
pub const MAX_KEY_LEN: usize = 256;

/// The number of distinct topic names the workload targets.
///
/// The generator creates, deletes, produces to, and consumes from a fixed pool
/// of topic names so that produce / consume operations always target a topic the
/// workload also creates, keeping the generated workload self-consistent. The
/// names themselves (`dst-topic-{i}`) are valid topic names under the production
/// rules (1–255 chars of `[A-Za-z0-9_-]`).
const WORKLOAD_TOPIC_COUNT: u32 = 4;

/// The exclusive upper bound on a consume's start offset.
///
/// Consume start offsets are drawn from `0..CONSUME_START_OFFSET_BOUND`. Reading
/// past a partition's highest committed offset is a successful empty read in the
/// production consume path, so an out-of-range start is a valid recorded
/// response, not an error.
const CONSUME_START_OFFSET_BOUND: u64 = 64;

/// Relative selection weights for the four operation kinds, summing to
/// [`OP_KIND_WEIGHT_TOTAL`]. Produce and consume dominate so the cluster is
/// exercised with data traffic, while create / delete keep the catalogue
/// changing under load.
const WEIGHT_CREATE_TOPIC: u64 = 15;
const WEIGHT_DELETE_TOPIC: u64 = 5;
const WEIGHT_PRODUCE: u64 = 50;
const WEIGHT_CONSUME: u64 = 30;
const OP_KIND_WEIGHT_TOTAL: u64 =
    WEIGHT_CREATE_TOPIC + WEIGHT_DELETE_TOPIC + WEIGHT_PRODUCE + WEIGHT_CONSUME;

/// One generated client operation, with all the arguments needed to issue it.
///
/// This is the workload-layer description of an operation; the runtime maps each
/// variant onto the production issue path (a produce proposes a record to the
/// target partition's Raft group, a consume reads its committed log, a topic
/// create / delete proposes a `ClusterCommand` to the `__meta/0` group). It is
/// deliberately independent of the `History`'s recorded-response types
/// (task 14.1): this type is the *request*, the History owns the *response*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientOperation {
    /// Create a topic with `partition_count` partitions and `replication_factor`
    /// replicas per partition.
    CreateTopic {
        /// The topic name (a valid `[A-Za-z0-9_-]` name from the topic pool).
        name: String,
        /// The number of partitions to create the topic with.
        partition_count: u32,
        /// The replication factor each partition is created with.
        replication_factor: usize,
    },
    /// Delete a topic by name.
    DeleteTopic {
        /// The topic name to delete.
        name: String,
    },
    /// Produce one record to a specific partition of a topic.
    Produce {
        /// The target topic name.
        topic: String,
        /// The resolved target partition: from [`PartitionRouter`] for a keyed
        /// record (Requirement 8.2), or a seed-drawn index for a keyless one.
        partition: PartitionIndex,
        /// The optional partition key. `Some` for a keyed record (length in
        /// [`MIN_KEY_LEN`]`..=`[`MAX_KEY_LEN`]); `None` for a keyless record.
        key: Option<Vec<u8>>,
        /// The record value (length in `0..=`[`MAX_VALUE_LEN`]).
        value: Vec<u8>,
    },
    /// Consume committed records from a specific partition of a topic.
    Consume {
        /// The target topic name.
        topic: String,
        /// The target partition.
        partition: PartitionIndex,
        /// The 0-based offset to begin reading from.
        start_offset: Offset,
        /// The maximum number of records to return (in
        /// [`MIN_MAX_RECORDS`]`..=`[`MAX_MAX_RECORDS`]).
        max_records: u32,
    },
}

/// A generated sequence of exactly [`ScenarioParameters::workload_size`]
/// [`ClientOperation`]s (Requirement 8.1).
///
/// The list is ordered: the `seq` carried by an
/// [`Event::ClientOp`](crate::scheduler::Event::ClientOp) indexes into it via
/// [`Workload::op`]. The whole workload is a pure function of the run seed and
/// the scenario parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Workload {
    ops: Vec<ClientOperation>,
}

impl Workload {
    /// The operations in issuance order.
    #[must_use]
    pub fn ops(&self) -> &[ClientOperation] {
        &self.ops
    }

    /// The number of operations in the workload.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the workload is empty (a workload size of zero).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// The operation at 0-based position `seq`, or `None` if out of range.
    ///
    /// The runtime resolves an
    /// [`Event::ClientOp`](crate::scheduler::Event::ClientOp)'s `seq` through
    /// this accessor to obtain the operation to issue.
    #[must_use]
    pub fn op(&self, seq: u64) -> Option<&ClientOperation> {
        usize::try_from(seq).ok().and_then(|i| self.ops.get(i))
    }
}

/// Generate a [`Workload`] of exactly `params.workload_size` operations from the
/// `workload_stream`, deterministically (Requirements 8.1, 8.2, 8.3).
///
/// All randomness is drawn from `workload_stream` in a fixed order, so the same
/// stream state (i.e. the same run seed) produces an identical workload. Keyed
/// produces are routed by the production [`PartitionRouter`]; keyless produces
/// and consume targets are drawn from the stream. The generator does not consult
/// any fault state: the runtime interleaves issuance with the fault schedule
/// (Requirement 8.7).
#[must_use]
pub fn generate(params: &ScenarioParameters, workload_stream: &mut SplitMix64) -> Workload {
    // A topic's partition count is always at least 1 in a validated scenario;
    // guard defensively so a stray 0 cannot cause a modulo-by-zero downstream.
    let partition_count = params.partition_count.max(1);

    // Keyed routing reuses the production rule. The router's keyed path keeps no
    // state, so a single instance reused across the whole workload yields the
    // exact production mapping for every key.
    let router = PartitionRouter::new();

    let topics = topic_pool();

    let mut ops = Vec::with_capacity(params.workload_size);
    for _ in 0..params.workload_size {
        ops.push(generate_op(
            workload_stream,
            &router,
            &topics,
            partition_count,
            params.replication_factor,
        ));
    }

    Workload { ops }
}

/// The fixed pool of topic names the workload targets.
fn topic_pool() -> Vec<String> {
    (0..WORKLOAD_TOPIC_COUNT)
        .map(|i| format!("dst-topic-{i}"))
        .collect()
}

/// Generate a single operation, drawing its kind and arguments from `stream`.
fn generate_op(
    stream: &mut SplitMix64,
    router: &PartitionRouter,
    topics: &[String],
    partition_count: u32,
    replication_factor: usize,
) -> ClientOperation {
    let topic = pick_topic(stream, topics);
    let roll = stream.next_below(OP_KIND_WEIGHT_TOTAL);

    if roll < WEIGHT_CREATE_TOPIC {
        ClientOperation::CreateTopic {
            name: topic,
            partition_count,
            replication_factor,
        }
    } else if roll < WEIGHT_CREATE_TOPIC + WEIGHT_DELETE_TOPIC {
        ClientOperation::DeleteTopic { name: topic }
    } else if roll < WEIGHT_CREATE_TOPIC + WEIGHT_DELETE_TOPIC + WEIGHT_PRODUCE {
        generate_produce(stream, router, topic, partition_count)
    } else {
        generate_consume(stream, topic, partition_count)
    }
}

/// Draw a topic name from the pool. The pool is always non-empty.
fn pick_topic(stream: &mut SplitMix64, topics: &[String]) -> String {
    let idx = stream.next_below(topics.len() as u64) as usize;
    topics[idx].clone()
}

/// Generate a produce operation: a keyed or keyless record with a seed-drawn
/// value, routed to a partition (Requirements 8.2, 8.3).
fn generate_produce(
    stream: &mut SplitMix64,
    router: &PartitionRouter,
    topic: String,
    partition_count: u32,
) -> ClientOperation {
    // Half the produces are keyed, half keyless, so a workload exercises both
    // routing rules (Requirement 8.2).
    let keyed = stream.next_u64() & 1 == 0;

    let (key, partition) = if keyed {
        // Key length in MIN_KEY_LEN..=MAX_KEY_LEN, then its contents
        // (Requirement 8.3).
        let key_span = (MAX_KEY_LEN - MIN_KEY_LEN + 1) as u64;
        let key_len = MIN_KEY_LEN + stream.next_below(key_span) as usize;
        let key = fill_bytes(stream, key_len);
        // Route via the production partitioning rule (Requirement 8.2).
        let partition = router.resolve(&topic, Some(&key), partition_count);
        (Some(key), partition)
    } else {
        // Keyless: draw the partition directly from the stream over the topic's
        // partition set (Requirement 8.2).
        let partition = PartitionIndex(stream.next_below(u64::from(partition_count)) as u32);
        (None, partition)
    };

    // Value length in 0..=MAX_VALUE_LEN, then its contents (Requirement 8.3).
    let value_len = stream.next_below(MAX_VALUE_LEN as u64 + 1) as usize;
    let value = fill_bytes(stream, value_len);

    ClientOperation::Produce {
        topic,
        partition,
        key,
        value,
    }
}

/// Generate a consume operation with a seed-drawn partition, start offset, and
/// (valid) max-records bound.
fn generate_consume(
    stream: &mut SplitMix64,
    topic: String,
    partition_count: u32,
) -> ClientOperation {
    let partition = PartitionIndex(stream.next_below(u64::from(partition_count)) as u32);
    let start_offset = stream.next_below(CONSUME_START_OFFSET_BOUND);
    // max_records in MIN_MAX_RECORDS..=MAX_MAX_RECORDS so the consume parameters
    // are always valid under the production consume contract.
    let span = u64::from(MAX_MAX_RECORDS - MIN_MAX_RECORDS + 1);
    let max_records = MIN_MAX_RECORDS + stream.next_below(span) as u32;

    ClientOperation::Consume {
        topic,
        partition,
        start_offset,
        max_records,
    }
}

/// Draw `len` bytes from `stream`.
///
/// Bytes are produced eight at a time from each 64-bit draw (little-endian) so a
/// large value does not require one draw per byte, while remaining a fixed,
/// deterministic function of the stream.
fn fill_bytes(stream: &mut SplitMix64, len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        let chunk = stream.next_u64().to_le_bytes();
        let take = (len - bytes.len()).min(chunk.len());
        bytes.extend_from_slice(&chunk[..take]);
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::SeedStreams;

    /// Build the workload stream for `seed`, exactly as a run would.
    fn workload_stream(seed: u64) -> SplitMix64 {
        SeedStreams::new(seed).workload
    }

    /// Requirement 8.1: the workload contains exactly `workload_size` ops.
    #[test]
    fn generates_exactly_workload_size_ops() {
        for &size in &[0usize, 1, 7, 100, 250] {
            let params = ScenarioParameters {
                workload_size: size,
                ..ScenarioParameters::default()
            };
            let mut stream = workload_stream(0xA11CE);
            let workload = generate(&params, &mut stream);
            assert_eq!(workload.len(), size);
            assert_eq!(workload.ops().len(), size);
            assert_eq!(workload.is_empty(), size == 0);
        }
    }

    /// Requirement 8.1 / 1.1: generation is a deterministic function of
    /// (params, seed) — the same seed reproduces an identical workload.
    #[test]
    fn same_seed_reproduces_identical_workload() {
        let params = ScenarioParameters::default();

        let first = generate(&params, &mut workload_stream(0xDEAD_BEEF));
        let second = generate(&params, &mut workload_stream(0xDEAD_BEEF));

        assert_eq!(first, second);
    }

    /// Different seeds yield different workloads, so the seed selects the run.
    #[test]
    fn different_seeds_produce_different_workloads() {
        let params = ScenarioParameters::default();

        let a = generate(&params, &mut workload_stream(1));
        let b = generate(&params, &mut workload_stream(2));

        assert_ne!(a, b);
    }

    /// Requirement 8.3: every produce's value length is in `0..=MAX_VALUE_LEN`
    /// and every keyed key length is in `MIN_KEY_LEN..=MAX_KEY_LEN`.
    #[test]
    fn produce_ops_respect_length_bounds() {
        let params = ScenarioParameters {
            workload_size: 500,
            ..ScenarioParameters::default()
        };
        let workload = generate(&params, &mut workload_stream(0x5EED));

        let mut saw_keyed = false;
        let mut saw_keyless = false;
        for op in workload.ops() {
            if let ClientOperation::Produce { key, value, .. } = op {
                assert!(value.len() <= MAX_VALUE_LEN, "value length out of range");
                match key {
                    Some(k) => {
                        assert!(
                            (MIN_KEY_LEN..=MAX_KEY_LEN).contains(&k.len()),
                            "key length {} out of range",
                            k.len()
                        );
                        saw_keyed = true;
                    }
                    None => saw_keyless = true,
                }
            }
        }
        // A 500-op workload exercises both keyed and keyless produces
        // (Requirement 8.2).
        assert!(saw_keyed, "expected at least one keyed produce");
        assert!(saw_keyless, "expected at least one keyless produce");
    }

    /// Requirement 8.2: every keyed produce routes to exactly the partition the
    /// production `PartitionRouter` selects for its key.
    #[test]
    fn keyed_produce_matches_partition_router() {
        let params = ScenarioParameters {
            workload_size: 500,
            ..ScenarioParameters::default()
        };
        let partition_count = params.partition_count;
        let workload = generate(&params, &mut workload_stream(0xC0DE));

        let router = PartitionRouter::new();
        let mut checked = 0;
        for op in workload.ops() {
            if let ClientOperation::Produce {
                topic,
                partition,
                key: Some(key),
                ..
            } = op
            {
                let expected = router.resolve(topic, Some(key), partition_count);
                assert_eq!(*partition, expected, "keyed routing diverged from router");
                checked += 1;
            }
        }
        assert!(checked > 0, "expected at least one keyed produce to check");
    }

    /// Every produce / consume partition index is within the topic's partition
    /// range, regardless of how it was selected.
    #[test]
    fn all_partitions_are_in_range() {
        let params = ScenarioParameters {
            workload_size: 500,
            partition_count: 7,
            ..ScenarioParameters::default()
        };
        let workload = generate(&params, &mut workload_stream(0xBEEF));

        for op in workload.ops() {
            let PartitionIndex(idx) = match op {
                ClientOperation::Produce { partition, .. } => *partition,
                ClientOperation::Consume { partition, .. } => *partition,
                _ => continue,
            };
            assert!(idx < params.partition_count, "partition {idx} out of range");
        }
    }

    /// Consume operations always carry valid parameters under the production
    /// consume contract (max_records in `MIN_MAX_RECORDS..=MAX_MAX_RECORDS`).
    #[test]
    fn consume_ops_have_valid_max_records() {
        let params = ScenarioParameters {
            workload_size: 500,
            ..ScenarioParameters::default()
        };
        let workload = generate(&params, &mut workload_stream(0xF00D));

        let mut checked = 0;
        for op in workload.ops() {
            if let ClientOperation::Consume { max_records, .. } = op {
                assert!(
                    (MIN_MAX_RECORDS..=MAX_MAX_RECORDS).contains(max_records),
                    "max_records {max_records} out of range"
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "expected at least one consume to check");
    }

    /// Create-topic operations carry the scenario's partition count and
    /// replication factor and a valid pooled topic name.
    #[test]
    fn create_topic_ops_use_scenario_shape() {
        let params = ScenarioParameters {
            workload_size: 500,
            partition_count: 5,
            ..ScenarioParameters::default()
        };
        let workload = generate(&params, &mut workload_stream(0x1234));

        let pool = topic_pool();
        let mut checked = 0;
        for op in workload.ops() {
            if let ClientOperation::CreateTopic {
                name,
                partition_count,
                replication_factor,
            } = op
            {
                assert_eq!(*partition_count, params.partition_count);
                assert_eq!(*replication_factor, params.replication_factor);
                assert!(pool.contains(name), "created topic outside the pool");
                checked += 1;
            }
        }
        assert!(checked > 0, "expected at least one create-topic to check");
    }

    /// All four operation kinds appear in a sufficiently large workload, so the
    /// generator produces a mix of create / delete / produce / consume
    /// (Requirement 8.1).
    #[test]
    fn workload_contains_all_op_kinds() {
        let params = ScenarioParameters {
            workload_size: 1000,
            ..ScenarioParameters::default()
        };
        let workload = generate(&params, &mut workload_stream(0xABCD));

        let (mut creates, mut deletes, mut produces, mut consumes) = (0, 0, 0, 0);
        for op in workload.ops() {
            match op {
                ClientOperation::CreateTopic { .. } => creates += 1,
                ClientOperation::DeleteTopic { .. } => deletes += 1,
                ClientOperation::Produce { .. } => produces += 1,
                ClientOperation::Consume { .. } => consumes += 1,
            }
        }
        assert!(creates > 0, "no create-topic ops generated");
        assert!(deletes > 0, "no delete-topic ops generated");
        assert!(produces > 0, "no produce ops generated");
        assert!(consumes > 0, "no consume ops generated");
    }

    /// `op(seq)` indexes the workload the way the scheduler's `ClientOp.seq`
    /// does, and is `None` past the end.
    #[test]
    fn op_accessor_indexes_by_seq() {
        let params = ScenarioParameters {
            workload_size: 10,
            ..ScenarioParameters::default()
        };
        let workload = generate(&params, &mut workload_stream(0x42));

        for (i, op) in workload.ops().iter().enumerate() {
            assert_eq!(workload.op(i as u64), Some(op));
        }
        assert_eq!(workload.op(workload.len() as u64), None);
    }

    /// The redirect-hop bound is exactly 5 (Requirement 8.4).
    #[test]
    fn max_redirect_hops_is_five() {
        assert_eq!(MAX_REDIRECT_HOPS, 5);
    }

    /// The record-shape bounds match Requirement 8.3.
    #[test]
    fn shape_bounds_match_requirement() {
        assert_eq!(MAX_VALUE_LEN, 65_536);
        assert_eq!(MIN_KEY_LEN, 1);
        assert_eq!(MAX_KEY_LEN, 256);
    }

    /// `fill_bytes` returns exactly the requested length deterministically,
    /// including the zero-length boundary.
    #[test]
    fn fill_bytes_is_exact_and_deterministic() {
        for &len in &[0usize, 1, 7, 8, 9, 64, 65_536] {
            let a = fill_bytes(&mut workload_stream(7), len);
            let b = fill_bytes(&mut workload_stream(7), len);
            assert_eq!(a.len(), len);
            assert_eq!(a, b);
        }
    }
}
