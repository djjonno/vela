// Feature: throughput-benchmark, Property 12: Exit code reflects the Outcome
//!
//! Property test for the Outcome → process exit-code mapping in `vela-bench`.
//!
//! Property 12: Exit code reflects the Outcome. A passing run maps to exit code
//! `0`; a failing run maps to a non-zero exit code, so a failing Benchmark_Run
//! marks the CI_Workflow as failed (Requirement 7.3). The mapping is total over
//! the Outcome space: `exit_code(outcome) == 0` if and only if the Outcome is
//! `Passed`.
//!
//! This exercises the pure mapping [`exit_code`] across both Outcome variants:
//!
//! 1. The single `Passed` Outcome deterministically maps to `0`.
//! 2. Every `Failed { reason }` Outcome — across a representative spread of
//!    `FailureReason` variants with arbitrary fields (including finite floor
//!    floats) — maps to a non-zero exit code.
//! 3. Over a mixed strategy generating both variants, `exit_code == 0` if and
//!    only if `outcome.is_passed()`, holding the mapping consistent with the
//!    Outcome's own pass predicate.
//!
//! Validates: Requirements 7.3

use proptest::prelude::*;
use vela_bench::outcome::{exit_code, FailureReason, Outcome, Phase};

/// Finite, non-negative records/sec figures. Throughput is never negative, NaN,
/// or infinite (see `metrics::throughput`), so floor-breach reasons carry rates
/// from the domain the mapping actually receives.
const MAX_RPS: f64 = 1_000_000.0;

/// Strategy over a representative spread of `FailureReason` variants — one per
/// shape of carried data (named string, topic/partition/cause, count pair,
/// position, phase, finite floor figures) — with arbitrary fields. Every shape
/// of failure must map to a non-zero exit code, so the guarantee is checked
/// across the variant space rather than for a single example.
fn failure_reason() -> impl Strategy<Value = FailureReason> {
    prop_oneof![
        "[a-z][a-z0-9-]{0,32}".prop_map(|topic| FailureReason::TopicAlreadyExists { topic }),
        ("[a-z_]{1,32}", ".{0,48}")
            .prop_map(|(name, detail)| FailureReason::InvalidParameter { name, detail }),
        ("[a-z][a-z0-9-]{0,32}", ".{0,48}")
            .prop_map(|(topic, cause)| FailureReason::TopicCreationFailed { topic, cause }),
        (0u64..600).prop_map(|budget_secs| FailureReason::ClusterNotReady { budget_secs }),
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
        ("[a-z][a-z0-9-]{0,48}", phase())
            .prop_map(|(cause, phase)| FailureReason::WarmupFailed { phase, cause }),
        phase().prop_map(|phase| FailureReason::ZeroMeasurementWindow { phase }),
        (0u64..86_400, 0u64..1_000_000, 0u64..1_000_000).prop_map(
            |(budget_secs, read, expected)| FailureReason::TimeBudgetExceeded {
                budget_secs,
                read,
                expected,
            }
        ),
        (0u64..1_000_000, 0u64..1_000_000)
            .prop_map(|(read, expected)| FailureReason::IntegrityCountMismatch { read, expected }),
        (0u64..1_000_000).prop_map(|position| FailureReason::IntegrityPayloadMismatch { position }),
        (0.0..=MAX_RPS, 0.0..=MAX_RPS).prop_map(|(measured_rps, floor_rps)| {
            FailureReason::FloorBreachProduce {
                measured_rps,
                floor_rps,
            }
        }),
        (0.0..=MAX_RPS, 0.0..=MAX_RPS).prop_map(|(measured_rps, floor_rps)| {
            FailureReason::FloorBreachConsume {
                measured_rps,
                floor_rps,
            }
        }),
    ]
}

/// Strategy over the two `Phase` values.
fn phase() -> impl Strategy<Value = Phase> {
    prop_oneof![Just(Phase::Produce), Just(Phase::Consume)]
}

/// Strategy over arbitrary Outcomes: the single `Passed` Outcome or a
/// `Failed { reason }` over the representative failure-reason spread.
fn outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        Just(Outcome::Passed),
        failure_reason().prop_map(|reason| Outcome::Failed { reason }),
    ]
}

/// Regime 1 (deterministic): the `Passed` Outcome maps to exit code `0`.
#[test]
fn passed_maps_to_zero() {
    assert_eq!(exit_code(&Outcome::Passed), 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Regime 2: every `Failed` Outcome maps to a non-zero exit code, so a
    /// failing Benchmark_Run marks the CI_Workflow as failed (Requirement 7.3).
    #[test]
    fn failed_maps_to_non_zero(reason in failure_reason()) {
        let outcome = Outcome::Failed { reason };
        prop_assert_ne!(exit_code(&outcome), 0);
    }

    /// Regime 3: over both variants, `exit_code == 0` if and only if the Outcome
    /// is `Passed` — the mapping is total and consistent with the Outcome's own
    /// pass predicate (Requirement 7.3).
    #[test]
    fn exit_code_zero_iff_passed(outcome in outcome()) {
        prop_assert_eq!(exit_code(&outcome) == 0, outcome.is_passed());
    }
}
