// Feature: throughput-benchmark, Property 8: Floor gating fails strictly below the floor and passes at or above it
//!
//! Property 8: Floor gating fails strictly below the floor and passes at or
//! above it. The throughput floor is a strict lower bound: a measured rate that
//! is strictly below the configured floor breaches it, while a rate equal to or
//! above the floor passes. When no floor is configured, throughput never causes
//! a breach. This holds independently for the Produce_Throughput floor and the
//! Consume_Throughput floor.
//!
//! This test exercises both the pure predicate [`check_floor`] and the gating
//! decision surfaced through [`determine_outcome`], checking each floor in
//! isolation by leaving the other floor unconfigured so only one phase can gate
//! the Outcome.
//!
//! Validates: Requirements 11.1, 11.2

use proptest::prelude::*;
use vela_bench::outcome::{check_floor, determine_outcome, FailureReason, Outcome};

/// Finite, non-negative throughput / floor values in a bounded range. Floors
/// and measured rates live in the same space so the strict-below boundary is
/// exercised from both sides, including values straddling each other.
fn rate() -> impl Strategy<Value = f64> {
    0.0f64..=1_000_000.0f64
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// `check_floor(m, Some(f))` is true exactly when `m < f`: strictly below
    /// breaches, equal-or-above passes. `check_floor(m, None)` is never a
    /// breach. (Requirements 11.1, 11.2)
    #[test]
    fn check_floor_is_strict_less_than(m in rate(), f in rate()) {
        prop_assert_eq!(check_floor(m, Some(f)), m < f);
        prop_assert!(!check_floor(m, None));
    }

    /// `check_floor` passes at and above the floor: feeding the floor value
    /// itself, and any non-negative excess above it, never breaches.
    #[test]
    fn check_floor_passes_at_or_above(f in rate(), excess in rate()) {
        prop_assert!(!check_floor(f, Some(f)));
        prop_assert!(!check_floor(f + excess, Some(f)));
    }

    /// The produce floor gates the Outcome independently: with no prior failure
    /// and no consume floor configured, a measured produce rate strictly below
    /// the floor yields `FloorBreachProduce` carrying the measured rate and the
    /// floor, while a rate at or above the floor yields `Passed`. The arbitrary
    /// consume rate must never affect the result. (Requirement 11.1)
    #[test]
    fn produce_floor_gates_independently(
        m in rate(),
        f in rate(),
        consume_any in rate(),
    ) {
        let outcome = determine_outcome(None, Some(m), Some(consume_any), Some(f), None);
        if m < f {
            prop_assert_eq!(
                outcome,
                Outcome::Failed {
                    reason: FailureReason::FloorBreachProduce {
                        measured_rps: m,
                        floor_rps: f,
                    }
                }
            );
        } else {
            prop_assert_eq!(outcome, Outcome::Passed);
        }
    }

    /// The consume floor gates the Outcome independently: with no prior failure
    /// and no produce floor configured (so the produce phase can never breach),
    /// a measured consume rate strictly below the floor yields
    /// `FloorBreachConsume` carrying the measured rate and the floor, while a
    /// rate at or above the floor yields `Passed`. The arbitrary produce rate
    /// must never affect the result. (Requirement 11.2)
    #[test]
    fn consume_floor_gates_independently(
        m in rate(),
        f in rate(),
        produce_any in rate(),
    ) {
        let outcome = determine_outcome(None, Some(produce_any), Some(m), None, Some(f));
        if m < f {
            prop_assert_eq!(
                outcome,
                Outcome::Failed {
                    reason: FailureReason::FloorBreachConsume {
                        measured_rps: m,
                        floor_rps: f,
                    }
                }
            );
        } else {
            prop_assert_eq!(outcome, Outcome::Passed);
        }
    }
}
