// Feature: throughput-benchmark, Property 6: Key mode determines key presence for every record
//! Property test for [`vela_bench::workload::key_for`] under each
//! [`vela_bench::params::KeyMode`].
//!
//! Feature: throughput-benchmark, Property 6: Key mode determines key presence
//! for every record — for any record position `p`, `key_for(p, Keyed)` is
//! `Some` (the cluster routes the record by its keyed partitioning rule) and
//! `key_for(p, Keyless)` is `None` (the record is routed without a key). The
//! choice is total over all positions and depends only on the mode, so keyed
//! keys are also deterministic across repeated calls.
//!
//! Driven by `proptest` with at least 100 cases over arbitrary `u64`
//! positions, covering the full position space (0, small, and very large).
//!
//! Validates: Requirements 4.3

use proptest::prelude::*;
use vela_bench::params::KeyMode;
use vela_bench::workload::key_for;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Keyed mode yields a key for every position; keyless mode never does.
    #[test]
    fn key_mode_determines_key_presence(position in any::<u64>()) {
        prop_assert!(
            key_for(position, KeyMode::Keyed).is_some(),
            "keyed mode must attach a key at position {position}"
        );
        prop_assert!(
            key_for(position, KeyMode::Keyless).is_none(),
            "keyless mode must not attach a key at position {position}"
        );
    }

    /// The keyed key depends only on the position, so repeated calls agree.
    #[test]
    fn keyed_key_is_deterministic(position in any::<u64>()) {
        prop_assert_eq!(
            key_for(position, KeyMode::Keyed),
            key_for(position, KeyMode::Keyed)
        );
    }
}
