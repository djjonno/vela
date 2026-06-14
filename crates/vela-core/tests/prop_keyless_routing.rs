//! Property test for keyless partition routing in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 11
//!
//! Property 11: Keyless routing distributes across all partitions. For any topic
//! with partition count `N`, resolving a sequence of `N` (or more) keyless
//! requests selects partitions such that every index in `0..N` is chosen — i.e.
//! the round-robin rule gives full coverage of the topic's partitions rather
//! than concentrating keyless records on a subset (Requirement 4.2, 10.3).
//!
//! The keyless case is exercised two ways that must behave identically: a `None`
//! key and a present-but-empty (`Some(&[])`) key. Both fall through to the
//! round-robin rule per Requirement 10.3 ("null or empty Partition_Key").
//!
//! The requirement bounds a topic's partition count at `1..=10_000`
//! (Requirement 2.1). To keep each of the many iterations fast while still
//! covering single-partition, small, and larger topics, `N` is drawn from
//! `1..=256`; `extra` keyless requests beyond `N` are also issued to confirm
//! coverage still holds when the counter wraps past a full cycle.
//!
//! Validates: Requirements 4.2, 10.3

use std::collections::HashSet;

use proptest::prelude::*;
use vela_core::{PartitionIndex, PartitionRouter};

/// The keyless inputs that must both route via the round-robin rule: a missing
/// key and a present-but-empty key (Requirement 10.3).
const KEYLESS_INPUTS: [Option<&[u8]>; 2] = [None, Some(&[])];

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 11
    #[test]
    fn keyless_routing_covers_every_partition(
        partition_count in 1u32..=256,
        extra in 0u32..=64,
        keyless_idx in 0usize..KEYLESS_INPUTS.len(),
    ) {
        let keyless_key = KEYLESS_INPUTS[keyless_idx];

        // A fresh router so the per-topic round-robin counter starts at 0.
        let router = PartitionRouter::new();

        // Issue N (or more) keyless resolves for one topic and collect the
        // partitions chosen. `N + extra` requests still cover exactly the same
        // index set; the extra requests simply wrap the counter.
        let requests = partition_count + extra;
        let mut seen: HashSet<u32> = HashSet::new();
        for _ in 0..requests {
            let PartitionIndex(idx) = router.resolve("events", keyless_key, partition_count);
            // Every resolved index must be a valid partition of the topic.
            prop_assert!(
                idx < partition_count,
                "resolved index {idx} is out of range for partition_count {partition_count}"
            );
            seen.insert(idx);
        }

        // Full coverage: the set of chosen partitions is exactly {0, 1, .., N-1}.
        let expected: HashSet<u32> = (0..partition_count).collect();
        prop_assert_eq!(seen, expected);
    }
}
