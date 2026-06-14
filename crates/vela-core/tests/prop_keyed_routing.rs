//! Property test for deterministic keyed routing in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 10
//!
//! Property 10: Keyed routing is deterministic. For any topic with a fixed
//! partition count and any non-empty partition key, resolving the topic and key
//! repeatedly returns the same partition, and that partition index is always
//! within `0..partition_count`.
//!
//! The generators constrain inputs to the exact space the property quantifies
//! over: a non-empty byte key (`1..=256` bytes), a partition count of at least
//! one (`1..=10_000`, the topic bound from Requirement 2.1), and an arbitrary
//! topic name. Determinism is checked two ways — repeated calls on a single
//! router, and a call on a freshly constructed router — because keyed routing
//! must be a pure function of the key bytes and partition count, independent of
//! any per-router state.
//!
//! Validates: Requirements 4.1, 10.2

use proptest::prelude::*;
use vela_core::{PartitionIndex, PartitionRouter};

/// Generate a non-empty key: between 1 and 256 arbitrary bytes. An empty key
/// would select the keyless round-robin rule, which Property 10 explicitly
/// excludes, so the generator never produces one.
fn non_empty_key_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..=256)
}

/// Generate a topic name. The bytes of the name do not affect keyed routing
/// (the partition is a function of the key and count only), so any non-empty
/// printable name exercises the property.
fn topic_strategy() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Za-z0-9_-]{1,64}").expect("valid regex")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 10
    #[test]
    fn keyed_routing_is_deterministic_and_in_range(
        topic in topic_strategy(),
        key in non_empty_key_strategy(),
        partition_count in 1u32..=10_000,
    ) {
        let router = PartitionRouter::new();

        // The first resolution establishes the partition this (topic, key,
        // count) must map to.
        let PartitionIndex(first) = router.resolve(&topic, Some(&key), partition_count);

        // The resolved index is always within `0..partition_count`
        // (Requirement 10.2).
        prop_assert!(
            first < partition_count,
            "partition index {first} is out of range for count {partition_count}"
        );

        // Repeated resolution on the same router returns the same partition
        // every time (Requirement 4.1, 10.2).
        for _ in 0..16 {
            let PartitionIndex(again) = router.resolve(&topic, Some(&key), partition_count);
            prop_assert_eq!(again, first);
            prop_assert!(again < partition_count);
        }

        // Keyed routing depends only on the key bytes and partition count, not
        // on any per-router state: a fresh router resolves identically. This
        // also guards against keyed calls being perturbed by the keyless
        // round-robin counter.
        let fresh = PartitionRouter::new();
        let PartitionIndex(fresh_idx) = fresh.resolve(&topic, Some(&key), partition_count);
        prop_assert_eq!(fresh_idx, first);
    }
}
