//! Partition routing: resolving a `(topic, partition_key)` pair to a concrete
//! partition of that topic.
//!
//! The [`PartitionRouter`] implements the two routing rules from the design and
//! requirements:
//!
//! - **Keyed routing** (Requirement 4.1, 10.2): when a non-empty partition key
//!   is supplied, the partition is chosen deterministically as
//!   `hash(key) % partition_count`. The mapping is stable across calls, so the
//!   same key on the same topic (with an unchanged partition count) always
//!   resolves to the same partition.
//! - **Keyless routing** (Requirement 4.2, 10.3): when the key is `None` or
//!   empty, the router falls back to round-robin distribution using a per-topic
//!   atomic counter, so a run of keyless produce requests is spread across every
//!   partition of the topic rather than landing on the deterministic key
//!   mapping.
//!
//! The router is thread-safe: the per-topic counters live behind a `Mutex`, and
//! resolution takes `&self` so a single router can be shared across the produce
//! tasks of every hosted partition.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::model::PartitionIndex;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Deterministic, dependency-free 64-bit hash of a byte slice (FNV-1a).
///
/// FNV-1a is used instead of [`std::collections::hash_map::DefaultHasher`] so
/// the keyed mapping is fully deterministic and reproducible — it depends only
/// on the key bytes, not on any per-process random seed.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Resolves a `(topic, partition_key)` pair to a partition of the topic.
///
/// Keyed resolution is purely a function of the key bytes and the partition
/// count, so it needs no shared state. Keyless resolution maintains one atomic
/// counter per topic name, kept in a `Mutex`-guarded map, to drive round-robin
/// selection across calls.
#[derive(Debug, Default)]
pub struct PartitionRouter {
    /// Per-topic round-robin counters for keyless routing, keyed by topic name.
    counters: Mutex<HashMap<String, AtomicU64>>,
}

impl PartitionRouter {
    /// Create a router with no per-topic state yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve `topic` and an optional `key` to a partition, given the topic's
    /// `partition_count`.
    ///
    /// - A non-empty `key` maps deterministically to
    ///   `hash(key) % partition_count` (Requirement 4.1, 10.2).
    /// - A `None` or empty `key` selects the next partition in round-robin order
    ///   for that topic (Requirement 4.2, 10.3).
    ///
    /// The returned index is always within `0..partition_count`. A
    /// `partition_count` of zero is not a valid topic configuration (topics have
    /// at least one partition); it is handled defensively by returning partition
    /// `0` rather than panicking.
    pub fn resolve(&self, topic: &str, key: Option<&[u8]>, partition_count: u32) -> PartitionIndex {
        if partition_count == 0 {
            return PartitionIndex(0);
        }
        let count = u64::from(partition_count);

        match key {
            // A present, non-empty key uses the deterministic hash mapping.
            Some(bytes) if !bytes.is_empty() => {
                let slot = fnv1a_64(bytes) % count;
                PartitionIndex(slot as u32)
            }
            // A missing or empty key uses keyless round-robin.
            _ => {
                let n = self.next_round_robin(topic);
                PartitionIndex((n % count) as u32)
            }
        }
    }

    /// Fetch-and-increment the round-robin counter for `topic`, returning the
    /// pre-increment value (so the first keyless call for a topic yields 0).
    fn next_round_robin(&self, topic: &str) -> u64 {
        let mut counters = self
            .counters
            .lock()
            .expect("partition router counter mutex poisoned");
        let counter = counters
            .entry(topic.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyed_routing_is_deterministic_across_calls() {
        let router = PartitionRouter::new();
        let key = b"user-42";
        let first = router.resolve("orders", Some(key), 8);
        // Repeated resolution of the same topic + key returns the same partition.
        for _ in 0..100 {
            assert_eq!(router.resolve("orders", Some(key), 8), first);
        }
    }

    #[test]
    fn keyed_routing_stays_within_partition_range() {
        let router = PartitionRouter::new();
        let partition_count = 7u32;
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            let PartitionIndex(idx) = router.resolve("t", Some(key.as_bytes()), partition_count);
            assert!(idx < partition_count, "index {idx} out of range");
        }
    }

    #[test]
    fn keyed_routing_does_not_use_the_round_robin_counter() {
        // Resolving with a key must not advance the keyless counter, so a later
        // keyless call still starts the round-robin at partition 0.
        let router = PartitionRouter::new();
        let _ = router.resolve("orders", Some(b"abc"), 4);
        let _ = router.resolve("orders", Some(b"def"), 4);
        assert_eq!(router.resolve("orders", None, 4), PartitionIndex(0));
    }

    #[test]
    fn keyless_routing_covers_all_partitions() {
        let router = PartitionRouter::new();
        let partition_count = 5u32;
        let mut seen = std::collections::HashSet::new();
        // N keyless resolutions must cover every partition index 0..N.
        for _ in 0..partition_count {
            let PartitionIndex(idx) = router.resolve("events", None, partition_count);
            seen.insert(idx);
        }
        let expected: std::collections::HashSet<u32> = (0..partition_count).collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn keyless_routing_is_round_robin_in_order() {
        let router = PartitionRouter::new();
        let partition_count = 3u32;
        // The counter starts at 0 and wraps modulo the partition count.
        let expected = [0u32, 1, 2, 0, 1, 2, 0];
        for &want in &expected {
            assert_eq!(
                router.resolve("t", None, partition_count),
                PartitionIndex(want)
            );
        }
    }

    #[test]
    fn empty_key_is_treated_as_keyless() {
        let router = PartitionRouter::new();
        // An empty (but present) key must use the round-robin rule, not the hash.
        assert_eq!(router.resolve("t", Some(&[]), 4), PartitionIndex(0));
        assert_eq!(router.resolve("t", Some(&[]), 4), PartitionIndex(1));
    }

    #[test]
    fn round_robin_counters_are_independent_per_topic() {
        let router = PartitionRouter::new();
        assert_eq!(router.resolve("a", None, 4), PartitionIndex(0));
        assert_eq!(router.resolve("b", None, 4), PartitionIndex(0));
        assert_eq!(router.resolve("a", None, 4), PartitionIndex(1));
        assert_eq!(router.resolve("b", None, 4), PartitionIndex(1));
    }

    #[test]
    fn single_partition_topic_always_resolves_to_zero() {
        let router = PartitionRouter::new();
        assert_eq!(router.resolve("t", Some(b"anything"), 1), PartitionIndex(0));
        assert_eq!(router.resolve("t", None, 1), PartitionIndex(0));
    }

    #[test]
    fn zero_partition_count_is_handled_without_panicking() {
        let router = PartitionRouter::new();
        assert_eq!(router.resolve("t", Some(b"k"), 0), PartitionIndex(0));
        assert_eq!(router.resolve("t", None, 0), PartitionIndex(0));
    }
}
