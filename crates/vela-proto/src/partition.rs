//! Canonical partitioner shared across crates.
//!
//! `vela-client`'s `PartitionRouter` and `vela-core`'s `PartitionRouter` both
//! historically carried byte-identical FNV-1a constants and the same
//! `hash(key) % partition_count` keyed-routing rule. Requirement 5.5 demands a
//! *single* shared implementation so the same key on the same topic resolves to
//! the same partition regardless of which producer (or internal repartition /
//! `key_by` stage) routed the record.
//!
//! Because `vela-client` must depend inward on `vela-proto` only — and must not
//! depend on `vela-core` — the canonical function lives here, in the one crate
//! that sits at the bottom of both dependency chains. The module is small and
//! dependency-free so it can flow outward to every caller unchanged.
//!
//! The keyless round-robin / sticky routing state remains process-local in each
//! `PartitionRouter`; only the keyed key→partition mapping must agree
//! byte-for-byte across processes, and that mapping lives here.

/// FNV-1a 64-bit offset basis (pinned; `Canonical_Partitioner`).
pub const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime (pinned; `Canonical_Partitioner`).
pub const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Deterministic, dependency-free 64-bit FNV-1a hash of a byte slice.
///
/// Used instead of [`std::collections::hash_map::DefaultHasher`] so the keyed
/// mapping is reproducible across processes — it depends only on the key bytes,
/// never on a per-process random seed. The algorithm matches the original
/// implementations in `vela-client::router` and `vela-core::router`
/// byte-for-byte.
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The `Canonical_Partitioner`: map a key to a partition of a topic with
/// `partition_count` partitions (Requirement 5.1, 5.5).
///
/// Returns `Some(partition)` with `partition < partition_count` for every
/// `partition_count >= 1`, computed as `fnv1a_64(key) % partition_count`.
///
/// Returns `None` when `partition_count == 0` so callers fail fast rather than
/// dividing by zero (Requirement 1.9). A valid topic always has at least one
/// partition, so `None` signals stale or invalid metadata that the caller must
/// reject or refresh rather than route against.
pub fn partition_for_key(key: &[u8], partition_count: u32) -> Option<u32> {
    (partition_count != 0).then(|| (fnv1a_64(key) % u64::from(partition_count)) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_64_matches_known_vectors() {
        // FNV-1a 64-bit reference vectors.
        assert_eq!(fnv1a_64(b""), FNV_OFFSET_BASIS);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x85944171_f73967e8);
    }

    #[test]
    fn fnv1a_64_is_deterministic() {
        let key = b"user-42";
        let first = fnv1a_64(key);
        for _ in 0..100 {
            assert_eq!(fnv1a_64(key), first);
        }
    }

    #[test]
    fn partition_for_key_is_deterministic() {
        let key = b"user-42";
        let first = partition_for_key(key, 8);
        for _ in 0..100 {
            assert_eq!(partition_for_key(key, 8), first);
        }
    }

    #[test]
    fn partition_for_key_stays_within_range() {
        let partition_count = 7u32;
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            let p = partition_for_key(key.as_bytes(), partition_count)
                .expect("non-zero partition count resolves");
            assert!(p < partition_count, "partition {p} out of range");
        }
    }

    #[test]
    fn partition_for_key_fails_fast_on_zero_count() {
        // Requirement 1.9: never a modulo against zero — fail fast instead.
        assert_eq!(partition_for_key(b"anything", 0), None);
        assert_eq!(partition_for_key(b"", 0), None);
    }

    #[test]
    fn single_partition_topic_always_resolves_to_zero() {
        assert_eq!(partition_for_key(b"anything", 1), Some(0));
        assert_eq!(partition_for_key(b"", 1), Some(0));
    }
}
