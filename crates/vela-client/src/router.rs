//! Client-side partition routing.
//!
//! The client resolves a `(topic, key)` to a partition *before* dispatch
//! (Requirement 4.1, 4.2) so the produce request can be sent straight to that
//! partition's leader. Because `vela-client` depends inward on `vela-proto`
//! only — and must not depend on `vela-core` — the keyed key→partition mapping
//! is delegated to the canonical partitioner in
//! [`vela_proto::partition`], the one crate both `vela-client` and `vela-core`
//! share, so client- and server-side resolution agree byte-for-byte
//! (Requirement 5.1, 5.5):
//!
//! - **Keyed routing** (Requirement 5.1): a non-empty key maps deterministically
//!   to `fnv1a(key) % partition_count` via
//!   [`vela_proto::partition::partition_for_key`]. The mapping depends only on
//!   the key bytes and the partition count, so it is stable across calls and
//!   processes.
//! - **Keyless routing** (Requirement 5.2): a `None` or empty key uses a
//!   per-topic keyless position so a run of keyless produces is spread across
//!   every partition of the topic. The position is driven by the configured
//!   [`KeylessStrategy`] — per-record `RoundRobin` (the default, preserving the
//!   historical behavior) or a `Sticky` partitioner that assigns a run of
//!   consecutive records to one partition before rotating (Requirement 5.6).
//!
//! Resolution fails fast when `partition_count == 0` (Requirement 1.9, 5.3):
//! [`PartitionRouter::resolve`] returns [`RouteError`] rather than clamping the
//! count up to `1`, so a record routed against stale or invalid metadata is
//! rejected instead of silently landing on partition `0`.

use std::collections::HashMap;
use std::sync::Mutex;

use vela_proto::partition::partition_for_key;

/// Error returned when a record cannot be routed to a partition.
///
/// Today the only failure is a zero `partition_count`: a valid topic always has
/// at least one partition, so a zero count signals stale or invalid metadata.
/// The router refuses to compute a partition assignment against it (no
/// modulo-by-zero, no clamp to `1`) and instead fails fast so the caller can
/// refresh metadata or surface a no-partitions error (Requirement 1.9, 5.3).
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    /// The topic's partition count was zero, so no partition could be assigned.
    #[error("cannot route a record for topic `{topic}`: partition count is zero")]
    ZeroPartitions {
        /// The topic whose partition count was zero.
        topic: String,
    },
}

/// The strategy used to spread keyless (no-key or empty-key) records across a
/// topic's partitions (Requirement 5.2, 5.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeylessStrategy {
    /// Per-record round-robin: each keyless record advances to the next
    /// partition in cyclic order `0, 1, …, N-1, 0, …`. This is the default and
    /// preserves the router's historical behavior.
    #[default]
    RoundRobin,
    /// Sticky partitioning: assign a run of `run_length` consecutive keyless
    /// records to a single partition before rotating to the next, distributing
    /// records evenly across every partition over time while preserving batching
    /// (Requirement 5.6). A `run_length` of `0` is treated as `1`.
    Sticky {
        /// The number of consecutive keyless records assigned to one partition
        /// before rotating to the next.
        run_length: u32,
    },
}

/// The per-topic keyless routing position, matching the configured
/// [`KeylessStrategy`]. Held behind the router's `Mutex` and advanced on each
/// keyless resolution.
#[derive(Debug, Clone, Copy)]
enum KeylessPosition {
    /// Round-robin counter: the next pre-increment value to hand out, so the
    /// first keyless call for a topic yields partition `0`.
    RoundRobin(u64),
    /// Sticky run state: the partition currently assigned and how many records
    /// remain in the current run before rotating to the next partition.
    Sticky {
        current_partition: u32,
        remaining_in_run: u32,
    },
}

/// Resolves `(topic, key)` to a partition index for a topic of a known size.
///
/// Keyed resolution is a pure function of the key bytes and the partition count
/// (delegated to the canonical partitioner). Keyless resolution keeps one
/// position per topic, behind a `Mutex`-guarded map, advanced according to the
/// configured [`KeylessStrategy`].
#[derive(Debug, Default)]
pub struct PartitionRouter {
    /// The keyless routing strategy applied to no-key / empty-key records.
    keyless: KeylessStrategy,
    /// Per-topic keyless routing positions, keyed by topic name.
    positions: Mutex<HashMap<String, KeylessPosition>>,
}

impl PartitionRouter {
    /// Create a router with the default keyless strategy (`RoundRobin`) and no
    /// per-topic state yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a router with an explicit keyless [`KeylessStrategy`].
    pub fn with_strategy(keyless: KeylessStrategy) -> Self {
        Self {
            keyless,
            positions: Mutex::new(HashMap::new()),
        }
    }

    /// The keyless strategy this router applies to no-key / empty-key records.
    pub fn keyless_strategy(&self) -> KeylessStrategy {
        self.keyless
    }

    /// Resolve `(topic, key)` to a partition index in `0..partition_count`.
    ///
    /// - A non-empty `key` maps deterministically to
    ///   `fnv1a(key) % partition_count` via the canonical partitioner
    ///   (Requirement 5.1).
    /// - A `None` or empty `key` selects a partition using the configured
    ///   [`KeylessStrategy`], so keyless produces cover all partitions
    ///   (Requirement 5.2, 5.6).
    ///
    /// Returns [`RouteError::ZeroPartitions`] when `partition_count == 0` rather
    /// than clamping: a valid topic always has at least one partition, so a zero
    /// count is rejected and the record fails fast (Requirement 1.9, 5.3).
    pub fn resolve(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        partition_count: u32,
    ) -> Result<u32, RouteError> {
        if partition_count == 0 {
            return Err(RouteError::ZeroPartitions {
                topic: topic.to_string(),
            });
        }

        let partition = match key {
            // A present, non-empty key uses the deterministic canonical mapping.
            // With a non-zero `partition_count` (guarded above) it always yields
            // `Some`.
            Some(bytes) if !bytes.is_empty() => {
                partition_for_key(bytes, partition_count).expect("partition_count is non-zero")
            }
            // A missing or empty key uses the keyless strategy.
            _ => self.next_keyless(topic, partition_count),
        };
        Ok(partition)
    }

    /// Advance and return the keyless partition for `topic`, given a non-zero
    /// `partition_count`.
    fn next_keyless(&self, topic: &str, partition_count: u32) -> u32 {
        let initial = self.initial_position();
        let mut positions = self
            .positions
            .lock()
            .expect("partition router position mutex poisoned");
        let position = positions.entry(topic.to_string()).or_insert(initial);
        let (partition, next) = self.advance(*position, partition_count);
        *position = next;
        partition
    }

    /// The starting keyless position for a topic, matching the configured
    /// strategy. The first keyless resolution for any topic yields partition `0`.
    fn initial_position(&self) -> KeylessPosition {
        match self.keyless {
            KeylessStrategy::RoundRobin => KeylessPosition::RoundRobin(0),
            KeylessStrategy::Sticky { run_length } => KeylessPosition::Sticky {
                current_partition: 0,
                remaining_in_run: run_length.max(1),
            },
        }
    }

    /// Compute the partition for the current keyless `position` and the position
    /// to store for the next keyless resolution, given a non-zero
    /// `partition_count`.
    fn advance(&self, position: KeylessPosition, partition_count: u32) -> (u32, KeylessPosition) {
        match position {
            KeylessPosition::RoundRobin(counter) => {
                let partition = (counter % u64::from(partition_count)) as u32;
                (
                    partition,
                    KeylessPosition::RoundRobin(counter.wrapping_add(1)),
                )
            }
            KeylessPosition::Sticky {
                current_partition,
                remaining_in_run,
            } => {
                // The run length comes from the strategy; a `Sticky` position is
                // only ever stored by a `Sticky` strategy, so the fallback is
                // unreachable in practice.
                let run_length = match self.keyless {
                    KeylessStrategy::Sticky { run_length } => run_length.max(1),
                    KeylessStrategy::RoundRobin => 1,
                };
                // When the current run is exhausted, rotate to the next partition
                // and start a fresh run; otherwise stay and decrement the run.
                let (partition, remaining) = if remaining_in_run == 0 {
                    let next_partition = (current_partition + 1) % partition_count;
                    (next_partition, run_length - 1)
                } else {
                    (current_partition % partition_count, remaining_in_run - 1)
                };
                (
                    partition,
                    KeylessPosition::Sticky {
                        current_partition: partition,
                        remaining_in_run: remaining,
                    },
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn keyed_routing_is_deterministic_across_calls() {
        let router = PartitionRouter::new();
        let key = b"user-42";
        let first = router
            .resolve("orders", Some(key), 8)
            .expect("non-zero count");
        for _ in 0..100 {
            assert_eq!(
                router
                    .resolve("orders", Some(key), 8)
                    .expect("non-zero count"),
                first
            );
        }
    }

    #[test]
    fn keyed_routing_is_deterministic_across_routers() {
        // A fresh router with no state resolves identically — keyed routing
        // depends only on the key bytes and partition count.
        let a = PartitionRouter::new();
        let b = PartitionRouter::new();
        for count in [1u32, 2, 3, 7, 16, 1000, 10_000] {
            assert_eq!(
                a.resolve("orders", Some(b"abc"), count)
                    .expect("non-zero count"),
                b.resolve("orders", Some(b"abc"), count)
                    .expect("non-zero count"),
            );
        }
    }

    #[test]
    fn keyed_routing_stays_within_partition_range() {
        let router = PartitionRouter::new();
        let partition_count = 7u32;
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            let p = router
                .resolve("orders", Some(key.as_bytes()), partition_count)
                .expect("non-zero count");
            assert!(p < partition_count, "partition {p} out of range");
        }
    }

    #[test]
    fn keyed_routing_does_not_advance_the_round_robin_counter() {
        // Keyed calls must not perturb keyless round-robin: a later keyless call
        // still starts at partition 0.
        let router = PartitionRouter::new();
        let _ = router
            .resolve("orders", Some(b"abc"), 4)
            .expect("non-zero count");
        let _ = router
            .resolve("orders", Some(b"def"), 4)
            .expect("non-zero count");
        assert_eq!(
            router.resolve("orders", None, 4).expect("non-zero count"),
            0
        );
    }

    #[test]
    fn keyless_routing_covers_all_partitions() {
        let router = PartitionRouter::new();
        let partition_count = 5u32;
        let mut seen = HashSet::new();
        for _ in 0..partition_count {
            seen.insert(
                router
                    .resolve("orders", None, partition_count)
                    .expect("non-zero count"),
            );
        }
        let expected: HashSet<u32> = (0..partition_count).collect();
        assert_eq!(seen, expected, "keyless routing must cover every partition");
    }

    #[test]
    fn empty_key_is_treated_as_keyless() {
        // An explicit empty key falls through to the keyless strategy, not the
        // hash rule.
        let router = PartitionRouter::new();
        assert_eq!(
            router
                .resolve("orders", Some(&[]), 3)
                .expect("non-zero count"),
            0
        );
        assert_eq!(
            router
                .resolve("orders", Some(&[]), 3)
                .expect("non-zero count"),
            1
        );
        assert_eq!(
            router
                .resolve("orders", Some(&[]), 3)
                .expect("non-zero count"),
            2
        );
        assert_eq!(
            router
                .resolve("orders", Some(&[]), 3)
                .expect("non-zero count"),
            0
        );
    }

    #[test]
    fn keyless_routing_is_per_topic() {
        // Each topic has its own counter, so interleaving topics does not skip
        // partitions within a topic.
        let router = PartitionRouter::new();
        assert_eq!(router.resolve("a", None, 3).expect("non-zero count"), 0);
        assert_eq!(router.resolve("b", None, 3).expect("non-zero count"), 0);
        assert_eq!(router.resolve("a", None, 3).expect("non-zero count"), 1);
        assert_eq!(router.resolve("b", None, 3).expect("non-zero count"), 1);
    }

    #[test]
    fn zero_partition_count_fails_fast() {
        // Requirement 1.9, 5.3: a zero partition count is rejected, never clamped
        // to 1 and never a modulo-by-zero.
        let router = PartitionRouter::new();
        assert!(matches!(
            router.resolve("orders", Some(b"k"), 0),
            Err(RouteError::ZeroPartitions { .. })
        ));
        assert!(matches!(
            router.resolve("orders", None, 0),
            Err(RouteError::ZeroPartitions { .. })
        ));
    }

    #[test]
    fn single_partition_topic_always_resolves_to_zero() {
        // Requirement 5.3: every produced record lands within `0..partition_count`.
        // With exactly one partition the only valid index is `0`, for both the
        // keyed and the keyless branches. This pins the modulo: a mutant that
        // dropped the `% partition_count` or clamped differently would let a keyed
        // hash escape past `0`.
        let router = PartitionRouter::new();
        // Keyed: distinct keys must all collapse onto the sole partition.
        for key in [&b"a"[..], b"user-42", b"another-key", &[0u8, 255, 7]] {
            assert_eq!(
                router
                    .resolve("orders", Some(key), 1)
                    .expect("non-zero count"),
                0,
                "keyed routing must resolve to partition 0 for a single-partition topic"
            );
        }
        // Keyless: repeated keyless produces must stay on partition `0` rather than
        // wrapping to a non-existent partition.
        for _ in 0..10 {
            assert_eq!(
                router.resolve("orders", None, 1).expect("non-zero count"),
                0,
                "keyless routing must resolve to partition 0 for a single-partition topic"
            );
            assert_eq!(
                router
                    .resolve("orders", Some(&[]), 1)
                    .expect("non-zero count"),
                0,
                "empty-key routing must resolve to partition 0 for a single-partition topic"
            );
        }
    }

    #[test]
    fn sticky_assigns_a_run_then_rotates() {
        // Requirement 5.6: the Sticky strategy assigns a run of `run_length`
        // consecutive keyless records to one partition before rotating to the
        // next, covering every partition over successive runs.
        let run_length = 3u32;
        let partition_count = 3u32;
        let router = PartitionRouter::with_strategy(KeylessStrategy::Sticky { run_length });
        assert_eq!(
            router.keyless_strategy(),
            KeylessStrategy::Sticky { run_length },
            "the router must report the strategy it was constructed with"
        );

        // Collect four full runs (one extra to observe the wrap back to 0).
        let total = run_length * (partition_count + 1);
        let observed: Vec<u32> = (0..total)
            .map(|_| {
                router
                    .resolve("orders", None, partition_count)
                    .expect("non-zero count")
            })
            .collect();

        // Exact sequence: a run of three 0s, then three 1s, then three 2s, then
        // back to three 0s. This pins both the run length (no early rotation) and
        // the rotation order (no skipped or repeated partition), and the final run
        // confirms the wrap `2 -> 0`.
        assert_eq!(
            observed,
            vec![0, 0, 0, 1, 1, 1, 2, 2, 2, 0, 0, 0],
            "sticky must assign full runs per partition and rotate in order"
        );

        // Over `partition_count` runs every partition is covered exactly once as a
        // run target (Requirement 5.6).
        let seen: HashSet<u32> = observed.into_iter().collect();
        assert_eq!(
            seen,
            (0..partition_count).collect::<HashSet<u32>>(),
            "sticky must cover every partition over successive runs"
        );
    }

    #[test]
    fn sticky_run_length_zero_is_treated_as_one() {
        // A `run_length` of `0` is documented to behave as `1`: each keyless
        // record rotates to the next partition. This pins the `run_length.max(1)`
        // guard — a mutant dropping it would divide/decrement against `0`.
        let router = PartitionRouter::with_strategy(KeylessStrategy::Sticky { run_length: 0 });
        let partition_count = 3u32;
        let observed: Vec<u32> = (0..6)
            .map(|_| {
                router
                    .resolve("orders", None, partition_count)
                    .expect("non-zero count")
            })
            .collect();
        assert_eq!(
            observed,
            vec![0, 1, 2, 0, 1, 2],
            "a zero run length must behave as a run length of one (per-record rotation)"
        );
    }

    #[test]
    fn sticky_routing_is_per_topic() {
        // Each topic keeps its own sticky run state, so interleaving topics never
        // bleeds one topic's run position into another's.
        let run_length = 2u32;
        let router = PartitionRouter::with_strategy(KeylessStrategy::Sticky { run_length });
        assert_eq!(router.resolve("a", None, 3).expect("non-zero count"), 0);
        assert_eq!(router.resolve("b", None, 3).expect("non-zero count"), 0);
        assert_eq!(router.resolve("a", None, 3).expect("non-zero count"), 0);
        assert_eq!(router.resolve("b", None, 3).expect("non-zero count"), 0);
        // Each topic's first run (length 2) is now exhausted; both rotate to 1.
        assert_eq!(router.resolve("a", None, 3).expect("non-zero count"), 1);
        assert_eq!(router.resolve("b", None, 3).expect("non-zero count"), 1);
    }
}
