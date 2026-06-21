//! Outcome determination: errors, integrity, time budget, and floor gating.
//!
//! A Benchmark_Run reaches exactly one [`Outcome`]: [`Outcome::Passed`] when
//! every gating condition holds, or [`Outcome::Failed`] carrying the typed
//! [`FailureReason`] that explains why. The reason is surfaced verbatim as a
//! named field on the Benchmark_Report, on the stdout summary, and in the
//! HTML_Report, so it derives `serde` (de)serialization alongside `Debug`,
//! `Clone`, and `PartialEq`.
//!
//! [`determine_outcome`] is the pure decision rule (Requirement 5.3, 5.4, 8.3,
//! 9.4, 11.3): the Outcome is `Passed` if and only if no operation error
//! occurred, the cluster became ready within the startup budget, the time
//! budget was not exceeded, data integrity holds, and no configured throughput
//! floor was breached. Any of those conditions failing yields `Failed` with the
//! corresponding reason, and a `Failed` Outcome never presents a measured
//! throughput as a successful result. [`check_floor`] is the pure floor-gating
//! predicate (Requirement 11.1, 11.2): a floor is breached if and only if the
//! measured rate is strictly below it, so a rate equal to or above the floor
//! passes, and when no floor is configured throughput never flips a `Passed`
//! Outcome to `Failed` (Requirement 11.3).

use serde::{Deserialize, Serialize};

/// Which phase of a Benchmark_Run a [`FailureReason`] refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// The Producer_Phase (topic data in).
    Produce,
    /// The Consumer_Phase (topic data out).
    Consume,
}

/// The pass/fail result of a Benchmark_Run.
///
/// A `Failed` Outcome always carries the typed [`FailureReason`] that explains
/// the failure; a `Passed` Outcome carries no reason. Because a `Failed`
/// Outcome is a distinct variant from `Passed`, a measured throughput can never
/// be presented as a successful result for a failing run (Requirement 9.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Outcome {
    /// Every gating condition held: the run succeeded.
    Passed,
    /// A gating condition failed; `reason` explains which.
    Failed {
        /// The typed reason the Benchmark_Run failed.
        reason: FailureReason,
    },
}

impl Outcome {
    /// Whether this Outcome is [`Outcome::Passed`].
    pub fn is_passed(&self) -> bool {
        matches!(self, Outcome::Passed)
    }

    /// The [`FailureReason`] when this Outcome is [`Outcome::Failed`], else
    /// `None`.
    pub fn failure_reason(&self) -> Option<&FailureReason> {
        match self {
            Outcome::Passed => None,
            Outcome::Failed { reason } => Some(reason),
        }
    }
}

/// Typed, named failure reasons for a Benchmark_Run.
///
/// Each variant captures exactly the fields the Benchmark_Report, stdout
/// summary, and HTML_Report need to explain the failure (Requirement 6.4): the
/// failed operation's topic and partition, the read/expected counts retained on
/// a time-budget or count violation, the affected record position on a payload
/// mismatch, and the measured/floor rates on a floor breach.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FailureReason {
    /// The target topic already existed at the start of the run, so the
    /// benchmark refused to measure against a pre-populated topic
    /// (Requirement 3.4).
    TopicAlreadyExists {
        /// The target topic name.
        topic: String,
    },
    /// A supplied Workload_Parameter was outside its accepted range
    /// (Requirement 3.5, 4.5, 4.6, 10.5).
    InvalidParameter {
        /// The offending parameter's name.
        name: String,
        /// A human-readable description of the violation.
        detail: String,
    },
    /// Target topic creation did not succeed, so the Producer_Phase was never
    /// entered (Requirement 3.6).
    TopicCreationFailed {
        /// The target topic name.
        topic: String,
        /// The underlying error cause.
        cause: String,
    },
    /// The Cluster_Under_Test did not become ready within the startup budget
    /// (Requirement 9.3).
    ClusterNotReady {
        /// The configured startup time budget, in seconds.
        budget_secs: u64,
    },
    /// A produce operation surfaced an error the client retry path did not
    /// resolve (Requirement 3.7, 9.1).
    ProduceError {
        /// The target topic.
        topic: String,
        /// The target partition.
        partition: u32,
        /// The underlying error cause.
        cause: String,
    },
    /// A consume operation surfaced an error the client retry path did not
    /// resolve (Requirement 3.7, 9.2).
    ConsumeError {
        /// The target topic.
        topic: String,
        /// The target partition.
        partition: u32,
        /// The underlying error cause.
        cause: String,
    },
    /// A warmup operation failed before its phase's Measurement_Window opened
    /// (Requirement 10.6).
    WarmupFailed {
        /// The phase whose warmup failed.
        phase: Phase,
        /// The underlying error cause.
        cause: String,
    },
    /// A phase's Measurement_Window duration was zero, so its throughput is
    /// undefined (Requirement 1.5, 2.4).
    ZeroMeasurementWindow {
        /// The phase with the zero-length Measurement_Window.
        phase: Phase,
    },
    /// The per-run time budget elapsed before the records read reached the
    /// Acknowledged_Record count; the counts are retained (Requirement 5.4,
    /// 8.3).
    TimeBudgetExceeded {
        /// The configured per-run time budget, in seconds.
        budget_secs: u64,
        /// The number of records read when the budget elapsed.
        read: u64,
        /// The number of Acknowledged_Records expected to be read.
        expected: u64,
    },
    /// The number of records read did not equal the Acknowledged_Record count
    /// (Requirement 5.1, 5.5); the counts are retained.
    IntegrityCountMismatch {
        /// The number of records read.
        read: u64,
        /// The number of Acknowledged_Records expected.
        expected: u64,
    },
    /// A read record's payload did not equal the payload expected for its
    /// position (Requirement 5.2, 5.5).
    IntegrityPayloadMismatch {
        /// The 0-based position of the record whose payload mismatched.
        position: u64,
    },
    /// The measured Produce_Throughput fell strictly below the configured floor
    /// (Requirement 11.1).
    FloorBreachProduce {
        /// The measured Produce_Throughput, in records/sec.
        measured_rps: f64,
        /// The configured Produce_Throughput floor, in records/sec.
        floor_rps: f64,
    },
    /// The measured Consume_Throughput fell strictly below the configured floor
    /// (Requirement 11.2).
    FloorBreachConsume {
        /// The measured Consume_Throughput, in records/sec.
        measured_rps: f64,
        /// The configured Consume_Throughput floor, in records/sec.
        floor_rps: f64,
    },
}

/// Whether a measured rate breaches a configured floor (Requirement 11.1,
/// 11.2, 11.3).
///
/// A floor is breached if and only if `measured_rps` is strictly below `floor`:
/// a value equal to or above the floor passes. When no floor is configured
/// (`floor == None`), nothing is ever breached, so a measured throughput never
/// flips a `Passed` Outcome to `Failed` (Requirement 11.3). This is a pure
/// predicate over `(measured_rps, floor)`.
pub fn check_floor(measured_rps: f64, floor: Option<f64>) -> bool {
    match floor {
        Some(floor_rps) => measured_rps < floor_rps,
        None => false,
    }
}

/// Determine the [`Outcome`] of a Benchmark_Run from its gating signals
/// (Requirement 5.3, 5.4, 8.3, 9.4, 11.3).
///
/// `prior_failure` carries any failure detected before floor gating — an
/// operation error, an unready cluster, an exceeded time budget, a warmup
/// failure, a zero Measurement_Window, or a data-integrity violation. When it
/// is `Some`, that reason wins and the Outcome is `Failed`, so a measured
/// throughput is never presented as a successful result (Requirement 9.4).
///
/// When no prior failure occurred, both phases produced a measured throughput,
/// and the configured floors are applied (Requirement 11.1, 11.2): the
/// Produce_Throughput floor is checked first, then the Consume_Throughput floor.
/// A floor is breached only when the measured rate is strictly below it (see
/// [`check_floor`]). When no floor is configured for a phase, that phase's
/// throughput cannot fail the run (Requirement 11.3). With no prior failure and
/// no breached floor, the Outcome is `Passed`.
///
/// `produce_rps` and `consume_rps` are the measured records/sec figures for the
/// respective phases, or `None` when a phase produced no measured throughput
/// (in which case the corresponding floor is not applied — any such incomplete
/// phase is already represented by a `prior_failure`).
pub fn determine_outcome(
    prior_failure: Option<FailureReason>,
    produce_rps: Option<f64>,
    consume_rps: Option<f64>,
    floor_produce_rps: Option<f64>,
    floor_consume_rps: Option<f64>,
) -> Outcome {
    if let Some(reason) = prior_failure {
        return Outcome::Failed { reason };
    }

    if let Some(measured_rps) = produce_rps {
        if check_floor(measured_rps, floor_produce_rps) {
            return Outcome::Failed {
                reason: FailureReason::FloorBreachProduce {
                    measured_rps,
                    // Safe: `check_floor` only returns true when the floor is `Some`.
                    floor_rps: floor_produce_rps.expect("produce floor configured"),
                },
            };
        }
    }

    if let Some(measured_rps) = consume_rps {
        if check_floor(measured_rps, floor_consume_rps) {
            return Outcome::Failed {
                reason: FailureReason::FloorBreachConsume {
                    measured_rps,
                    // Safe: `check_floor` only returns true when the floor is `Some`.
                    floor_rps: floor_consume_rps.expect("consume floor configured"),
                },
            };
        }
    }

    Outcome::Passed
}

/// Map an [`Outcome`] to the binary's process exit status (Requirement 7.3).
///
/// A passing run yields `0`; a failing run yields a non-zero status (`1`) so a
/// failing Benchmark_Run marks the CI_Workflow as failed. This is the pure
/// mapping the binary (`main.rs`) feeds to [`std::process::ExitCode::from`];
/// keeping it in the library makes the Outcome → exit-code rule reachable from
/// the property-based tests under `tests/`.
pub fn exit_code(outcome: &Outcome) -> u8 {
    if outcome.is_passed() {
        0
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- check_floor (Requirement 11.1, 11.2, 11.3) ----------------------

    #[test]
    fn no_floor_is_never_breached() {
        assert!(!check_floor(0.0, None));
        assert!(!check_floor(1_000_000.0, None));
    }

    #[test]
    fn strictly_below_floor_is_a_breach() {
        assert!(check_floor(99.9, Some(100.0)));
        assert!(check_floor(0.0, Some(0.1)));
    }

    #[test]
    fn equal_to_floor_passes() {
        assert!(!check_floor(100.0, Some(100.0)));
    }

    #[test]
    fn above_floor_passes() {
        assert!(!check_floor(100.1, Some(100.0)));
    }

    // ----- determine_outcome (Requirement 5.3, 5.4, 8.3, 9.4, 11.3) --------

    #[test]
    fn passes_with_no_failure_and_no_floors() {
        let outcome = determine_outcome(None, Some(500.0), Some(400.0), None, None);
        assert_eq!(outcome, Outcome::Passed);
        assert!(outcome.is_passed());
    }

    #[test]
    fn passes_when_both_phases_meet_their_floors() {
        let outcome = determine_outcome(None, Some(500.0), Some(400.0), Some(500.0), Some(400.0));
        assert_eq!(outcome, Outcome::Passed);
    }

    #[test]
    fn prior_failure_wins_over_floor_checks() {
        // Even though the produce rate would breach the floor, the prior
        // failure is the reason carried on the Outcome.
        let outcome = determine_outcome(
            Some(FailureReason::IntegrityCountMismatch {
                read: 9,
                expected: 10,
            }),
            Some(1.0),
            Some(1.0),
            Some(1_000.0),
            Some(1_000.0),
        );
        assert_eq!(
            outcome,
            Outcome::Failed {
                reason: FailureReason::IntegrityCountMismatch {
                    read: 9,
                    expected: 10,
                }
            }
        );
    }

    #[test]
    fn failed_outcome_never_presents_passed() {
        let outcome = determine_outcome(
            Some(FailureReason::ClusterNotReady { budget_secs: 60 }),
            None,
            None,
            None,
            None,
        );
        assert!(!outcome.is_passed());
        assert!(outcome.failure_reason().is_some());
    }

    #[test]
    fn produce_floor_breach_fails_with_measured_and_floor() {
        let outcome = determine_outcome(None, Some(450.0), Some(400.0), Some(500.0), None);
        assert_eq!(
            outcome,
            Outcome::Failed {
                reason: FailureReason::FloorBreachProduce {
                    measured_rps: 450.0,
                    floor_rps: 500.0,
                }
            }
        );
    }

    #[test]
    fn consume_floor_breach_fails_with_measured_and_floor() {
        let outcome = determine_outcome(None, Some(500.0), Some(350.0), Some(500.0), Some(400.0));
        assert_eq!(
            outcome,
            Outcome::Failed {
                reason: FailureReason::FloorBreachConsume {
                    measured_rps: 350.0,
                    floor_rps: 400.0,
                }
            }
        );
    }

    #[test]
    fn produce_floor_is_checked_before_consume_floor() {
        // Both floors are breached; the produce breach is reported first.
        let outcome = determine_outcome(None, Some(1.0), Some(1.0), Some(100.0), Some(100.0));
        assert_eq!(
            outcome,
            Outcome::Failed {
                reason: FailureReason::FloorBreachProduce {
                    measured_rps: 1.0,
                    floor_rps: 100.0,
                }
            }
        );
    }

    #[test]
    fn missing_measured_throughput_skips_floor_check() {
        // No measured produce rate → its floor is not applied.
        let outcome = determine_outcome(None, None, Some(400.0), Some(500.0), Some(400.0));
        assert_eq!(outcome, Outcome::Passed);
    }

    // ----- FailureReason variants carry their fields (Requirement 6.4) -----

    #[test]
    fn failure_reasons_round_trip_through_json() {
        let reasons = vec![
            FailureReason::TopicAlreadyExists {
                topic: "vela-bench".to_string(),
            },
            FailureReason::InvalidParameter {
                name: "record_count".to_string(),
                detail: "0 is outside range".to_string(),
            },
            FailureReason::TopicCreationFailed {
                topic: "vela-bench".to_string(),
                cause: "timeout".to_string(),
            },
            FailureReason::ClusterNotReady { budget_secs: 60 },
            FailureReason::ProduceError {
                topic: "vela-bench".to_string(),
                partition: 2,
                cause: "not leader".to_string(),
            },
            FailureReason::ConsumeError {
                topic: "vela-bench".to_string(),
                partition: 3,
                cause: "transport".to_string(),
            },
            FailureReason::WarmupFailed {
                phase: Phase::Produce,
                cause: "rejected".to_string(),
            },
            FailureReason::ZeroMeasurementWindow {
                phase: Phase::Consume,
            },
            FailureReason::TimeBudgetExceeded {
                budget_secs: 60,
                read: 42,
                expected: 100,
            },
            FailureReason::IntegrityCountMismatch {
                read: 101,
                expected: 100,
            },
            FailureReason::IntegrityPayloadMismatch { position: 7 },
            FailureReason::FloorBreachProduce {
                measured_rps: 450.0,
                floor_rps: 500.0,
            },
            FailureReason::FloorBreachConsume {
                measured_rps: 350.0,
                floor_rps: 400.0,
            },
        ];

        for reason in reasons {
            let outcome = Outcome::Failed {
                reason: reason.clone(),
            };
            let json = serde_json::to_string(&outcome).expect("serialize");
            let back: Outcome = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(outcome, back);
            assert_eq!(outcome.failure_reason(), Some(&reason));
        }
    }

    #[test]
    fn passed_outcome_round_trips_through_json() {
        let outcome = Outcome::Passed;
        let json = serde_json::to_string(&outcome).expect("serialize");
        let back: Outcome = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(outcome, back);
        assert_eq!(back.failure_reason(), None);
    }

    // ----- exit_code (Requirement 7.3) -------------------------------------

    #[test]
    fn passed_outcome_maps_to_zero_exit_code() {
        assert_eq!(exit_code(&Outcome::Passed), 0);
    }

    #[test]
    fn failed_outcome_maps_to_non_zero_exit_code() {
        let outcome = Outcome::Failed {
            reason: FailureReason::ClusterNotReady { budget_secs: 60 },
        };
        assert_ne!(exit_code(&outcome), 0);
    }
}
