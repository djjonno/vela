//! Property tests for the client-side keyless partition routing of the
//! [`PartitionRouter`] in `vela-client`.
//!
//! Feature: ctl-client-routing-and-repl, Property 4: Keyed routing does not
//! advance keyless position — for any interleaving of keyed and keyless
//! resolutions on a topic, the sequence of partitions produced by the keyless
//! resolutions equals the sequence produced if the keyed resolutions were
//! removed entirely.
//!
//! Feature: ctl-client-routing-and-repl, Property 5: Round-robin keyless routing
//! covers all partitions in order — for any partition count `N >= 1` and any
//! positive multiple `k`, `k * N` consecutive keyless round-robin resolutions
//! visit every partition `0..N` and follow the exact cyclic order
//! `0, 1, ..., N-1, 0, ...`.
//!
//! Feature: ctl-client-routing-and-repl, Property 6: Sticky keyless routing
//! batches then distributes evenly — for any partition count `N >= 1` and run
//! length `R >= 1`, sticky keyless routing assigns each run of `R` consecutive
//! keyless records to a single partition before rotating to the next, and over
//! `N` runs covers every partition `0..N`.
//!
//! Each property is driven by `proptest` with at least 100 cases. Partition
//! counts and sequence lengths are bounded so the many iterations stay fast
//! while still covering single-partition, small, and larger topics.
//!
//! Validates: Requirements 5.2, 5.4, 5.6

use std::collections::HashSet;

use proptest::prelude::*;
use vela_client::{KeylessStrategy, PartitionRouter};

/// A single routing operation in an interleaved keyed/keyless sequence.
///
/// `Keyed` carries a non-empty key (routed via the canonical partitioner);
/// `Keyless` carries no key (routed via the keyless strategy).
#[derive(Debug, Clone)]
enum Op {
    Keyed(Vec<u8>),
    Keyless,
}

/// Strategy producing an interleaving of keyed (non-empty key) and keyless
/// operations.
fn ops_strategy() -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        // Keyed: a non-empty key so it takes the canonical-partitioner branch
        // rather than falling through to the keyless strategy.
        prop::collection::vec(any::<u8>(), 1..=16).prop_map(Op::Keyed),
        Just(Op::Keyless),
    ];
    prop::collection::vec(op, 0..=200)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 4: Keyed routing does not
    // advance keyless position.
    #[test]
    fn keyed_routing_does_not_advance_keyless_position(
        partition_count in 1u32..=256,
        ops in ops_strategy(),
    ) {
        const TOPIC: &str = "orders";

        // Interleaved run: resolve every op on one router, collecting only the
        // partitions chosen by the keyless resolutions.
        let interleaved = PartitionRouter::new();
        let mut interleaved_keyless: Vec<u32> = Vec::new();
        for op in &ops {
            match op {
                Op::Keyed(key) => {
                    let p = interleaved
                        .resolve(TOPIC, Some(key.as_slice()), partition_count)
                        .expect("non-zero partition count");
                    prop_assert!(p < partition_count, "keyed index {p} out of range");
                }
                Op::Keyless => {
                    let p = interleaved
                        .resolve(TOPIC, None, partition_count)
                        .expect("non-zero partition count");
                    interleaved_keyless.push(p);
                }
            }
        }

        // Keyless-only run: a fresh router resolving just the keyless ops, in
        // order, with the keyed ops removed entirely.
        let keyless_only = PartitionRouter::new();
        let mut expected_keyless: Vec<u32> = Vec::new();
        for op in &ops {
            if let Op::Keyless = op {
                expected_keyless.push(
                    keyless_only
                        .resolve(TOPIC, None, partition_count)
                        .expect("non-zero partition count"),
                );
            }
        }

        // The keyless subsequence is identical whether or not keyed routing was
        // interleaved: keyed resolutions leave the keyless position unchanged.
        prop_assert_eq!(interleaved_keyless, expected_keyless);
    }

    // Feature: ctl-client-routing-and-repl, Property 5: Round-robin keyless
    // routing covers all partitions in order.
    #[test]
    fn round_robin_keyless_covers_all_partitions_in_cyclic_order(
        partition_count in 1u32..=128,
        k in 1u32..=8,
    ) {
        const TOPIC: &str = "events";

        // The default strategy is round-robin; a fresh router starts at 0.
        let router = PartitionRouter::new();
        prop_assert_eq!(router.keyless_strategy(), KeylessStrategy::RoundRobin);

        let total = k * partition_count;
        let mut seen: HashSet<u32> = HashSet::new();
        for i in 0..total {
            let p = router
                .resolve(TOPIC, None, partition_count)
                .expect("non-zero partition count");
            // Exact cyclic order: the i-th keyless resolution yields i % N.
            prop_assert_eq!(
                p,
                i % partition_count,
                "round-robin out of order at step {}",
                i
            );
            seen.insert(p);
        }

        // k * N resolutions cover exactly {0, 1, .., N-1}.
        let expected: HashSet<u32> = (0..partition_count).collect();
        prop_assert_eq!(seen, expected);
    }

    // Feature: ctl-client-routing-and-repl, Property 6: Sticky keyless routing
    // batches then distributes evenly.
    #[test]
    fn sticky_keyless_batches_then_rotates(
        partition_count in 1u32..=64,
        run_length in 1u32..=16,
    ) {
        const TOPIC: &str = "events";

        let router = PartitionRouter::with_strategy(KeylessStrategy::Sticky { run_length });
        prop_assert_eq!(
            router.keyless_strategy(),
            KeylessStrategy::Sticky { run_length }
        );

        // Over N runs of R records each, every record in run r lands on the same
        // partition, runs rotate 0, 1, .., N-1, and all partitions are covered.
        let total = partition_count * run_length;
        let mut seen: HashSet<u32> = HashSet::new();
        for i in 0..total {
            let p = router
                .resolve(TOPIC, None, partition_count)
                .expect("non-zero partition count");
            // Record i belongs to run (i / R); each run sticks to one partition,
            // and consecutive runs rotate cyclically through 0..N.
            let expected_partition = (i / run_length) % partition_count;
            prop_assert_eq!(
                p,
                expected_partition,
                "sticky mismatch at record {} (run {})",
                i,
                i / run_length
            );
            seen.insert(p);
        }

        // N runs cover every partition exactly once as the run boundary.
        let expected: HashSet<u32> = (0..partition_count).collect();
        prop_assert_eq!(seen, expected);
    }
}
