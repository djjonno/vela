// Feature: throughput-benchmark, Property 7: Outcome is determined solely by errors, integrity, time budget, and configured floors
//!
//! Property test for outcome determination in `vela-bench`.
//!
//! Property 7: Outcome is determined solely by errors, integrity, time budget,
//! and configured floors. The Outcome is `Passed` if and only if no operation
//! error occurred, the cluster became ready within the startup budget, the time
//! budget was not exceeded, data integrity holds, and no configured floor was
//! breached; otherwise it is `Failed` carrying the corresponding typed reason. A
//! `Failed` Outcome never presents a measured throughput as a successful result,
//! and when no floor is configured a measured throughput alone never flips a
//! `Passed` Outcome to `Failed`.
//!
//! This exercises the pure decision rule [`determine_outcome`] together with the
//! pure floor predicate [`check_floor`] across the three orthogonal regimes the
//! property names:
//!
//! 1. A prior failure (any error / integrity / time-budget / warmup / zero-window
//!    reason detected before floor gating) always wins: the Outcome is `Failed`
//!    carrying exactly that reason regardless of the throughput and floor inputs,
//!    so throughput is never presented as success (Requirements 5.3, 5.4, 8.3,
//!    9.4).
//! 2. With no prior failure and no configured floors, the Outcome is `Passed`
//!    for any measured produce/consume rate — throughput alone never fails the
//!    run (Requirement 11.3).
//! 3. With no prior failure, the Outcome is `Passed` if and only if neither
//!    configured floor is breached, where a breach is exactly `check_floor`
//!    returning `true` for a present measured rate (a `None` measured rate skips
//!    its floor check). This keeps `determine_outcome` and `check_floor`
//!    consistent over arbitrary rate/floor inputs (Requirement 9.4).
//!
//! Validates: Requirements 5.3, 5.4, 8.3, 9.4, 11.3

use proptest::prelude::*;
use vela_bench::outcome::{check_floor, determine_outcome, FailureReason, Outcome, Phase};

/// A finite, non-negative range of records/sec figures. Throughput is never
/// negative, NaN, or infinite (see `metrics::throughput`), so generators stay
/// inside the domain the decision rule actually receives.
const MAX_RPS: f64 = 1_000_000.0;

/// Strategy over an optional measured/floor rate: `None` (phase produced no
/// measurement / no floor configured) or `Some(finite non-negative rps)`.
fn opt_rps() -> impl Strategy<Value = Option<f64>> {
    prop_oneof![Just(None), (0.0..=MAX_RPS).prop_map(Some)]
}

/// Strategy over a representative spread of `FailureReason` variants — one per
/// shape of carried data (named string, count pair, position, phase, floor
/// figures) — with arbitrary fields. A prior failure of any of these shapes must
/// be carried through unchanged, so the precedence guarantee is checked across
/// the variant space rather than for a single example.
fn failure_reason() -> impl Strategy<Value = FailureReason> {
    prop_oneof![
        "[a-z][a-z0-9-]{0,32}".prop_map(|topic| FailureReason::TopicAlreadyExists { topic }),
        ("[a-z_]{1,32}", ".{0,48}")
            .prop_map(|(name, detail)| FailureReason::InvalidParameter { name, detail }),
        ("[a-z][a-z0-9-]{0,32}", 0u32..10_000, ".{0,48}").prop_map(|(topic, partition, cause)| {
            FailureReason::ProduceError {
                topic,
                partition,
                cause,
            }
        }),
        ("[a-z][a-z0-9-]{0,32}", 0u32..10_000, ".{0,48}").prop_map(|(topic, partition, cause)| {
            FailureReason::ConsumeError {
                topic,
                partition,
                cause,
            }
        }),
        (0u64..600).prop_map(|budget_secs| FailureReason::ClusterNotReady { budget_secs }),
        prop_oneof![Just(Phase::Produce), Just(Phase::Consume)]
            .prop_map(|phase| FailureReason::ZeroMeasurementWindow { phase }),
        (0u64..1_000, 0u64..1_000).prop_map(|(read, expected)| {
            FailureReason::IntegrityCountMismatch { read, expected }
        }),
        (0u64..1_000_000).prop_map(|position| FailureReason::IntegrityPayloadMismatch { position }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Regime 1: a prior failure always wins, carrying its exact reason, for any
    /// throughput and floor inputs — including floors the measured rates would
    /// otherwise breach. A `Failed` Outcome therefore never presents throughput
    /// as a successful result (Requirements 5.3, 5.4, 8.3, 9.4).
    #[test]
    fn prior_failure_wins_and_is_carried_unchanged(
        reason in failure_reason(),
        produce_rps in opt_rps(),
        consume_rps in opt_rps(),
        floor_produce_rps in opt_rps(),
        floor_consume_rps in opt_rps(),
    ) {
        let outcome = determine_outcome(
            Some(reason.clone()),
            produce_rps,
            consume_rps,
            floor_produce_rps,
            floor_consume_rps,
        );

        prop_assert_eq!(outcome.clone(), Outcome::Failed { reason: reason.clone() });
        prop_assert!(!outcome.is_passed());
        prop_assert_eq!(outcome.failure_reason(), Some(&reason));
    }

    /// Regime 2: with no prior failure and no configured floors, the Outcome is
    /// `Passed` for any measured produce/consume rate — a measured throughput
    /// alone never flips `Passed` to `Failed` (Requirement 11.3).
    #[test]
    fn no_floors_always_passes_regardless_of_throughput(
        produce_rps in opt_rps(),
        consume_rps in opt_rps(),
    ) {
        let outcome = determine_outcome(None, produce_rps, consume_rps, None, None);

        prop_assert_eq!(outcome.clone(), Outcome::Passed);
        prop_assert!(outcome.is_passed());
        prop_assert_eq!(outcome.failure_reason(), None);
    }

    /// Regime 3: with no prior failure, the Outcome is `Passed` if and only if
    /// neither configured floor is breached, where a breach for a present
    /// measured rate is exactly `check_floor == true` (a `None` measured rate
    /// skips its floor check). This holds `determine_outcome` consistent with the
    /// pure `check_floor` predicate over arbitrary rate/floor inputs
    /// (Requirements 9.4, 11.3).
    #[test]
    fn passes_iff_no_configured_floor_breached(
        produce_rps in opt_rps(),
        consume_rps in opt_rps(),
        floor_produce_rps in opt_rps(),
        floor_consume_rps in opt_rps(),
    ) {
        let outcome = determine_outcome(
            None,
            produce_rps,
            consume_rps,
            floor_produce_rps,
            floor_consume_rps,
        );

        // A present measured rate is breached exactly when `check_floor` says so;
        // a missing measured rate cannot breach (its floor is not applied).
        let produce_breached =
            produce_rps.is_some_and(|rps| check_floor(rps, floor_produce_rps));
        let consume_breached =
            consume_rps.is_some_and(|rps| check_floor(rps, floor_consume_rps));
        let expected_pass = !produce_breached && !consume_breached;

        prop_assert_eq!(outcome.is_passed(), expected_pass);

        if expected_pass {
            prop_assert_eq!(outcome, Outcome::Passed);
        } else {
            // A breach yields a `Failed` Outcome carrying a floor-breach reason;
            // the produce floor is gated before the consume floor.
            match outcome.failure_reason() {
                Some(FailureReason::FloorBreachProduce { measured_rps, floor_rps }) => {
                    prop_assert!(produce_breached);
                    prop_assert_eq!(Some(*measured_rps), produce_rps);
                    prop_assert_eq!(Some(*floor_rps), floor_produce_rps);
                }
                Some(FailureReason::FloorBreachConsume { measured_rps, floor_rps }) => {
                    // Consume is only reported when produce did not breach.
                    prop_assert!(!produce_breached);
                    prop_assert!(consume_breached);
                    prop_assert_eq!(Some(*measured_rps), consume_rps);
                    prop_assert_eq!(Some(*floor_rps), floor_consume_rps);
                }
                other => prop_assert!(
                    false,
                    "expected a floor-breach failure reason, got {other:?}"
                ),
            }
        }
    }
}
