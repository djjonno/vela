//! Property tests for the `Canonical_Partitioner` in `vela-proto`.
//!
//! Feature: ctl-client-routing-and-repl, Property 1: Canonical partitioner
//! determinism and range — for any non-empty key and any partition count
//! `N >= 1`, the canonical partitioner resolves the key to the same partition
//! on every call (and on a freshly observed call) and that index is always
//! within `0..N`.
//!
//! Feature: ctl-client-routing-and-repl, Property 3: Zero-partition routing
//! fails fast — for any key (present or absent), resolving against a partition
//! count of `0` returns `None` (never a partition index) and never performs a
//! modulo against zero or panics.
//!
//! Both properties test the pure `vela_proto::partition::partition_for_key`
//! function, so they run without a live server. The generators constrain inputs
//! to exactly the space each property quantifies over: a key of `0..=256`
//! arbitrary bytes (covering both empty and non-empty keys), a non-zero
//! partition count (`1..=10_000`, the topic partition bound) for Property 1, and
//! a fixed zero count for Property 3.
//!
//! Validates: Requirements 5.1, 5.3, 1.9

use proptest::prelude::*;
use vela_proto::partition::partition_for_key;

/// Generate a non-empty key: between 1 and 256 arbitrary bytes. Property 1
/// quantifies over non-empty keys, so the generator never produces an empty
/// slice.
fn non_empty_key_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..=256)
}

/// Generate any key, including the empty slice. Property 3 holds for a key that
/// is present or absent, so this generator spans `0..=256` bytes.
fn any_key_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=256)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 1: Canonical partitioner
    // determinism and range.
    #[test]
    fn canonical_partitioner_is_deterministic_and_in_range(
        key in non_empty_key_strategy(),
        partition_count in 1u32..=10_000,
    ) {
        // The first resolution establishes the partition this (key, count)
        // must map to.
        let first = partition_for_key(&key, partition_count)
            .expect("non-zero partition count always resolves (Property 1, N >= 1)");

        // The resolved index is always within `0..partition_count`
        // (Requirement 5.3).
        prop_assert!(
            first < partition_count,
            "partition index {first} is out of range for count {partition_count}"
        );

        // Repeated resolution returns the same partition every time — the
        // canonical partitioner is a pure function of the key bytes and count
        // (Requirement 5.1).
        for _ in 0..16 {
            let again = partition_for_key(&key, partition_count)
                .expect("non-zero partition count always resolves");
            prop_assert_eq!(again, first);
            prop_assert!(again < partition_count);
        }
    }

    // Feature: ctl-client-routing-and-repl, Property 3: Zero-partition routing
    // fails fast.
    #[test]
    fn canonical_partitioner_fails_fast_on_zero_count(
        key in any_key_strategy(),
    ) {
        // A zero partition count must yield a fail-fast rejection rather than a
        // partition index, and the call must not panic or divide by zero
        // (Requirements 1.9, 5.3). Reaching this assertion at all proves no
        // panic occurred.
        prop_assert_eq!(partition_for_key(&key, 0), None);
    }
}
