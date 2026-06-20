#![cfg(feature = "sim")]
//! Property test for scenario-parameter validation in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 22: Scenario-parameter
//! validation
//!
//! Property 22: *For any* `ScenarioParameters`, the DST harness accepts the set
//! if and only if the replication factor is in `1..=node_count` and the
//! partition count is at least 1. A replication factor equal to the node count
//! is accepted; a replication factor greater than the node count, a replication
//! factor of zero, or a partition count below 1 are each rejected with the
//! matching [`ScenarioError`] variant before any run executes.
//!
//! This is the direct realization of Requirement 15.5: the harness rejects an
//! internally inconsistent parameter set with an error rather than executing an
//! invalid run, while accepting a replication factor equal to the node count.
//!
//! The generator deliberately concentrates probability on the interesting
//! boundaries — `replication_factor` of `0`, exactly `node_count`, and
//! `node_count + 1`, and `partition_count` of `0` and `1` — alongside uniform
//! draws, so the inclusive upper boundary (`rf == node_count`) and each
//! rejection edge are exercised rather than reached only by chance.
//!
//! Validates: Requirements 15.5

use proptest::prelude::*;
use vela_sim::scenario::{ScenarioError, ScenarioParameters};

/// Generate `(node_count, replication_factor, partition_count)` triples that
/// cover the validation boundaries as well as clearly-valid and clearly-invalid
/// interiors.
///
/// `replication_factor` is biased toward `0`, `node_count`, and
/// `node_count + 1` (the accept/reject edges around the inclusive upper bound),
/// and `partition_count` toward `0` and `1` (the reject/accept edge), with
/// uniform draws mixed in so the interior of each range is also visited.
fn params_strategy() -> impl Strategy<Value = (usize, usize, u32)> {
    (0usize..=8).prop_flat_map(|node_count| {
        let replication_factor = prop_oneof![
            Just(0usize),
            Just(node_count),
            Just(node_count.saturating_add(1)),
            0usize..=10,
        ];
        let partition_count = prop_oneof![Just(0u32), Just(1u32), 0u32..=8];
        (Just(node_count), replication_factor, partition_count)
    })
}

/// The expected outcome of [`ScenarioParameters::validate`], derived
/// independently from Requirement 15.5's rule and the documented precedence of
/// the [`ScenarioError`] variants (zero replication factor before too-high
/// before zero partition count).
fn expected(
    node_count: usize,
    replication_factor: usize,
    partition_count: u32,
) -> Result<(), ScenarioError> {
    if replication_factor == 0 {
        Err(ScenarioError::ReplicationFactorZero)
    } else if replication_factor > node_count {
        Err(ScenarioError::ReplicationFactorTooHigh {
            replication_factor,
            node_count,
        })
    } else if partition_count < 1 {
        Err(ScenarioError::PartitionCountZero)
    } else {
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: deterministic-simulation-testing, Property 22: Scenario-parameter
    // validation
    #[test]
    fn validate_accepts_iff_replication_factor_and_partition_count_in_range(
        (node_count, replication_factor, partition_count) in params_strategy(),
    ) {
        let params = ScenarioParameters {
            node_count,
            replication_factor,
            partition_count,
            ..ScenarioParameters::default()
        };

        let result = params.validate();

        // The accept/reject decision matches the iff rule exactly: Ok precisely
        // when the replication factor is in `1..=node_count` and the partition
        // count is at least 1.
        let in_range = (1..=node_count).contains(&replication_factor);
        prop_assert_eq!(result.is_ok(), in_range && partition_count >= 1);

        // And every rejection carries the specific variant the rule names.
        prop_assert_eq!(result, expected(node_count, replication_factor, partition_count));
    }
}
