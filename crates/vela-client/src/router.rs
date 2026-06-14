//! Client-side partition routing.
//!
//! The client resolves a `(topic, key)` to a partition *before* dispatch
//! (Requirement 4.1, 4.2) so the produce request can be sent straight to that
//! partition's leader. Because `vela-client` depends inward on `vela-proto`
//! only — and must not depend on `vela-core` — this is a small, self-contained
//! reimplementation of the same routing rules `vela-core`'s `PartitionRouter`
//! uses, so client- and server-side resolution agree:
//!
//! - **Keyed routing** (Requirement 4.1): a non-empty key maps deterministically
//!   to `fnv1a(key) % partition_count`. The mapping depends only on the key
//!   bytes and the partition count, so it is stable across calls and processes.
//! - **Keyless routing** (Requirement 4.2): a `None` or empty key uses a
//!   per-topic round-robin counter so a run of keyless produces is spread across
//!   every partition of the topic.
//!
//! The FNV-1a constants and algorithm match `vela-core::router` exactly; keeping
//! them byte-for-byte identical is what makes client routing consistent with the
//! server's view of which partition a key belongs to.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// FNV-1a 64-bit offset basis (matches `vela-core::router`).
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime (matches `vela-core::router`).
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Deterministic, dependency-free 64-bit FNV-1a hash of a byte slice.
///
/// Used instead of [`std::collections::hash_map::DefaultHasher`] so the keyed
/// mapping is reproducible across processes — it depends only on the key bytes,
/// never on a per-process random seed.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Resolves `(topic, key)` to a partition index for a topic of a known size.
///
/// Keyed resolution is a pure function of the key bytes and the partition count.
/// Keyless resolution keeps one atomic counter per topic, behind a `Mutex`-
/// guarded map, to drive round-robin selection across calls.
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

    /// Resolve `(topic, key)` to a partition index in `0..partition_count`.
    ///
    /// - A non-empty `key` maps deterministically to
    ///   `fnv1a(key) % partition_count` (Requirement 4.1).
    /// - A `None` or empty `key` selects the next partition in round-robin order
    ///   for `topic`, so keyless produces cover all partitions (Requirement 4.2).
    ///
    /// `partition_count` is clamped to a minimum of 1 to avoid a divide-by-zero;
    /// a valid topic always has at least one partition.
    pub fn resolve(&self, topic: &str, key: Option<&[u8]>, partition_count: u32) -> u32 {
        let count = u64::from(partition_count.max(1));
        match key {
            // A present, non-empty key uses the deterministic hash mapping.
            Some(bytes) if !bytes.is_empty() => (fnv1a_64(bytes) % count) as u32,
            // A missing or empty key uses keyless round-robin.
            _ => (self.next_round_robin(topic) % count) as u32,
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
    use std::collections::HashSet;

    #[test]
    fn keyed_routing_is_deterministic_across_calls() {
        let router = PartitionRouter::new();
        let key = b"user-42";
        let first = router.resolve("orders", Some(key), 8);
        for _ in 0..100 {
            assert_eq!(router.resolve("orders", Some(key), 8), first);
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
                a.resolve("orders", Some(b"abc"), count),
                b.resolve("orders", Some(b"abc"), count),
            );
        }
    }

    #[test]
    fn keyed_routing_stays_within_partition_range() {
        let router = PartitionRouter::new();
        let partition_count = 7u32;
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            let p = router.resolve("orders", Some(key.as_bytes()), partition_count);
            assert!(p < partition_count, "partition {p} out of range");
        }
    }

    #[test]
    fn keyed_routing_does_not_advance_the_round_robin_counter() {
        // Keyed calls must not perturb keyless round-robin: a later keyless call
        // still starts at partition 0.
        let router = PartitionRouter::new();
        let _ = router.resolve("orders", Some(b"abc"), 4);
        let _ = router.resolve("orders", Some(b"def"), 4);
        assert_eq!(router.resolve("orders", None, 4), 0);
    }

    #[test]
    fn keyless_routing_covers_all_partitions() {
        let router = PartitionRouter::new();
        let partition_count = 5u32;
        let mut seen = HashSet::new();
        for _ in 0..partition_count {
            seen.insert(router.resolve("orders", None, partition_count));
        }
        let expected: HashSet<u32> = (0..partition_count).collect();
        assert_eq!(seen, expected, "keyless routing must cover every partition");
    }

    #[test]
    fn empty_key_is_treated_as_keyless() {
        // An explicit empty key falls through to round-robin, not the hash rule.
        let router = PartitionRouter::new();
        assert_eq!(router.resolve("orders", Some(&[]), 3), 0);
        assert_eq!(router.resolve("orders", Some(&[]), 3), 1);
        assert_eq!(router.resolve("orders", Some(&[]), 3), 2);
        assert_eq!(router.resolve("orders", Some(&[]), 3), 0);
    }

    #[test]
    fn keyless_routing_is_per_topic() {
        // Each topic has its own counter, so interleaving topics does not skip
        // partitions within a topic.
        let router = PartitionRouter::new();
        assert_eq!(router.resolve("a", None, 3), 0);
        assert_eq!(router.resolve("b", None, 3), 0);
        assert_eq!(router.resolve("a", None, 3), 1);
        assert_eq!(router.resolve("b", None, 3), 1);
    }
}
