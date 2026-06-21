//! Integration tests asserting every [`FailureReason`] variant carries the
//! exact fields the Benchmark_Report, stdout summary, and HTML_Report need to
//! explain a failure (Requirement 6.4).
//!
//! Each test constructs a variant with representative field values, pattern
//! matches to assert each field is accessible and equals what was set, and
//! asserts a `serde_json` round-trip (serialize then deserialize) preserves the
//! fields. A few variants are also wrapped in [`Outcome::Failed`] so that
//! [`Outcome::failure_reason`] is exercised end to end. The variants emphasize
//! the field coverage called out in the requirements:
//!
//! - `ProduceError` / `ConsumeError` carry topic + partition + cause
//!   (Requirements 9.1, 9.2).
//! - `TimeBudgetExceeded` / `IntegrityCountMismatch` carry read + expected
//!   counts (Requirement 5.4).
//! - `IntegrityPayloadMismatch` carries the affected position (Requirement 5.4).
//! - `FloorBreachProduce` / `FloorBreachConsume` carry measured + floor rps
//!   (Requirements 11.1, 11.2).
//! - `WarmupFailed` carries phase + cause (Requirement 10.6).
//! - `ClusterNotReady` carries the startup budget (Requirement 9.3).

use vela_bench::outcome::{FailureReason, Outcome, Phase};

/// Assert a `FailureReason` survives a JSON serialize/deserialize round-trip
/// with all fields intact, and that the same value wrapped in `Outcome::Failed`
/// is recoverable via `Outcome::failure_reason`.
fn assert_round_trip(reason: FailureReason) {
    let json = serde_json::to_string(&reason).expect("serialize FailureReason");
    let back: FailureReason = serde_json::from_str(&json).expect("deserialize FailureReason");
    assert_eq!(reason, back, "JSON round-trip must preserve every field");

    let outcome = Outcome::Failed {
        reason: reason.clone(),
    };
    assert!(!outcome.is_passed());
    assert_eq!(outcome.failure_reason(), Some(&reason));

    let outcome_json = serde_json::to_string(&outcome).expect("serialize Outcome");
    let outcome_back: Outcome = serde_json::from_str(&outcome_json).expect("deserialize Outcome");
    assert_eq!(outcome, outcome_back);
}

#[test]
fn topic_already_exists_carries_topic() {
    let reason = FailureReason::TopicAlreadyExists {
        topic: "vela-bench".to_string(),
    };
    let FailureReason::TopicAlreadyExists { topic } = &reason else {
        panic!("expected TopicAlreadyExists");
    };
    assert_eq!(topic, "vela-bench");
    assert_round_trip(reason);
}

#[test]
fn invalid_parameter_carries_name_and_detail() {
    let reason = FailureReason::InvalidParameter {
        name: "record_count".to_string(),
        detail: "0 is outside range 1..=1_000_000_000".to_string(),
    };
    let FailureReason::InvalidParameter { name, detail } = &reason else {
        panic!("expected InvalidParameter");
    };
    assert_eq!(name, "record_count");
    assert_eq!(detail, "0 is outside range 1..=1_000_000_000");
    assert_round_trip(reason);
}

#[test]
fn topic_creation_failed_carries_topic_and_cause() {
    let reason = FailureReason::TopicCreationFailed {
        topic: "vela-bench".to_string(),
        cause: "admin timeout".to_string(),
    };
    let FailureReason::TopicCreationFailed { topic, cause } = &reason else {
        panic!("expected TopicCreationFailed");
    };
    assert_eq!(topic, "vela-bench");
    assert_eq!(cause, "admin timeout");
    assert_round_trip(reason);
}

#[test]
fn cluster_not_ready_carries_startup_budget() {
    // Requirement 9.3: the startup budget is retained on the reason.
    let reason = FailureReason::ClusterNotReady { budget_secs: 60 };
    let FailureReason::ClusterNotReady { budget_secs } = &reason else {
        panic!("expected ClusterNotReady");
    };
    assert_eq!(*budget_secs, 60);
    assert_round_trip(reason);
}

#[test]
fn produce_error_carries_topic_partition_and_cause() {
    // Requirement 9.1: a produce failure names the topic, partition, and cause.
    let reason = FailureReason::ProduceError {
        topic: "vela-bench".to_string(),
        partition: 2,
        cause: "not leader for partition".to_string(),
    };
    let FailureReason::ProduceError {
        topic,
        partition,
        cause,
    } = &reason
    else {
        panic!("expected ProduceError");
    };
    assert_eq!(topic, "vela-bench");
    assert_eq!(*partition, 2);
    assert_eq!(cause, "not leader for partition");
    assert_round_trip(reason);
}

#[test]
fn consume_error_carries_topic_partition_and_cause() {
    // Requirement 9.2: a consume failure names the topic, partition, and cause.
    let reason = FailureReason::ConsumeError {
        topic: "vela-bench".to_string(),
        partition: 3,
        cause: "transport closed".to_string(),
    };
    let FailureReason::ConsumeError {
        topic,
        partition,
        cause,
    } = &reason
    else {
        panic!("expected ConsumeError");
    };
    assert_eq!(topic, "vela-bench");
    assert_eq!(*partition, 3);
    assert_eq!(cause, "transport closed");
    assert_round_trip(reason);
}

#[test]
fn warmup_failed_carries_phase_and_cause() {
    // Requirement 10.6: a warmup failure names the phase and cause; check both
    // phases so the `Phase` field is exercised in either position.
    for phase in [Phase::Produce, Phase::Consume] {
        let reason = FailureReason::WarmupFailed {
            phase,
            cause: "rejected during warmup".to_string(),
        };
        let FailureReason::WarmupFailed {
            phase: got_phase,
            cause,
        } = &reason
        else {
            panic!("expected WarmupFailed");
        };
        assert_eq!(*got_phase, phase);
        assert_eq!(cause, "rejected during warmup");
        assert_round_trip(reason);
    }
}

#[test]
fn zero_measurement_window_carries_phase() {
    for phase in [Phase::Produce, Phase::Consume] {
        let reason = FailureReason::ZeroMeasurementWindow { phase };
        let FailureReason::ZeroMeasurementWindow { phase: got_phase } = &reason else {
            panic!("expected ZeroMeasurementWindow");
        };
        assert_eq!(*got_phase, phase);
        assert_round_trip(reason);
    }
}

#[test]
fn time_budget_exceeded_carries_budget_and_counts() {
    // Requirement 5.4: the read/expected counts are retained when the time
    // budget elapses.
    let reason = FailureReason::TimeBudgetExceeded {
        budget_secs: 60,
        read: 42,
        expected: 100,
    };
    let FailureReason::TimeBudgetExceeded {
        budget_secs,
        read,
        expected,
    } = &reason
    else {
        panic!("expected TimeBudgetExceeded");
    };
    assert_eq!(*budget_secs, 60);
    assert_eq!(*read, 42);
    assert_eq!(*expected, 100);
    assert_round_trip(reason);
}

#[test]
fn integrity_count_mismatch_carries_counts() {
    // Requirement 5.4: read/expected counts are retained on a count mismatch,
    // including the over-read case (read > expected).
    let reason = FailureReason::IntegrityCountMismatch {
        read: 101,
        expected: 100,
    };
    let FailureReason::IntegrityCountMismatch { read, expected } = &reason else {
        panic!("expected IntegrityCountMismatch");
    };
    assert_eq!(*read, 101);
    assert_eq!(*expected, 100);
    assert_round_trip(reason);
}

#[test]
fn integrity_payload_mismatch_carries_position() {
    // Requirement 5.4: the affected record position is retained on a payload
    // mismatch.
    let reason = FailureReason::IntegrityPayloadMismatch { position: 7 };
    let FailureReason::IntegrityPayloadMismatch { position } = &reason else {
        panic!("expected IntegrityPayloadMismatch");
    };
    assert_eq!(*position, 7);
    assert_round_trip(reason);
}

#[test]
fn floor_breach_produce_carries_measured_and_floor() {
    // Requirement 11.1: the measured and floor produce rates are retained.
    let reason = FailureReason::FloorBreachProduce {
        measured_rps: 450.0,
        floor_rps: 500.0,
    };
    let FailureReason::FloorBreachProduce {
        measured_rps,
        floor_rps,
    } = &reason
    else {
        panic!("expected FloorBreachProduce");
    };
    assert_eq!(*measured_rps, 450.0);
    assert_eq!(*floor_rps, 500.0);
    assert_round_trip(reason);
}

#[test]
fn floor_breach_consume_carries_measured_and_floor() {
    // Requirement 11.2: the measured and floor consume rates are retained.
    let reason = FailureReason::FloorBreachConsume {
        measured_rps: 350.0,
        floor_rps: 400.0,
    };
    let FailureReason::FloorBreachConsume {
        measured_rps,
        floor_rps,
    } = &reason
    else {
        panic!("expected FloorBreachConsume");
    };
    assert_eq!(*measured_rps, 350.0);
    assert_eq!(*floor_rps, 400.0);
    assert_round_trip(reason);
}
