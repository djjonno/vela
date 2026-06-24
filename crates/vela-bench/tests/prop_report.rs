// Feature: throughput-benchmark, Property 10: The Benchmark_Report carries every required field and round-trips through JSON
//
// Property 10 asserts that the single source of truth for all three
// Benchmark_Run outputs — the [`BenchmarkReport`] — is both complete and
// lossless: every reported value is a separately named, individually
// addressable top-level field (Requirements 6.1, 6.2), and serializing the
// report to JSON then deserializing it yields an equal report (the round-trip
// is lossless). A phase that did not complete carries its throughput as
// [`Option::None`], which must serialize as JSON `null` rather than a
// misleading measured `0` (Requirement 6.5).
//
// The generated reports use finite, non-NaN floats throughout so that
// structural `PartialEq` over the deserialized report holds (NaN != NaN would
// otherwise break round-trip equality without indicating any real defect).
//
// Validates: Requirements 6.1, 6.2, 6.5

use std::time::Duration;

use proptest::prelude::*;
use vela_bench::metrics::Throughput;
use vela_bench::outcome::{FailureReason, Outcome, Phase};
use vela_bench::params::{KeyMode, WorkloadParameters};
use vela_bench::report::BenchmarkReport;

/// The required top-level fields of a serialized [`BenchmarkReport`]
/// (Requirements 6.1, 6.2): every reported value is a separately named field.
const REQUIRED_FIELDS: &[&str] = &[
    "params",
    "outcome",
    "produce_throughput",
    "consume_throughput",
    "acknowledged_records",
    "total_payload_bytes",
    "total_elapsed",
    "failure_reason",
];

/// A finite, non-NaN `f64` drawn from a bounded range of integer-valued floats.
///
/// Round-trip equality compares floats structurally, so NaN (never equal to
/// itself) and the infinities are excluded — they would break the equality
/// assertion without signalling any real serialization defect. The values are
/// further restricted to integers in `±1e9`: every such value is exactly
/// representable as an `f64` (well within `2^53`) and its JSON text round-trips
/// to the identical `f64`, so the assertion exercises the report's losslessness
/// rather than the JSON number parser's last-ULP rounding behavior.
fn finite_f64() -> impl Strategy<Value = f64> {
    (-1_000_000_000i64..=1_000_000_000).prop_map(|n| n as f64)
}

/// A short, arbitrary text fragment (no control characters) used for the topic
/// name and the string-bearing [`FailureReason`] fields.
fn text() -> impl Strategy<Value = String> {
    "\\PC{0,16}"
}

/// An arbitrary, finite [`Duration`] built from generated whole seconds and a
/// valid sub-second nanosecond remainder (`0..1_000_000_000`).
fn duration() -> impl Strategy<Value = Duration> {
    (0u64..1_000_000, 0u32..1_000_000_000).prop_map(|(secs, nanos)| Duration::new(secs, nanos))
}

/// One of the two [`KeyMode`] values.
fn key_mode() -> impl Strategy<Value = KeyMode> {
    prop_oneof![Just(KeyMode::Keyed), Just(KeyMode::Keyless)]
}

/// One of the two [`Phase`] values.
fn phase() -> impl Strategy<Value = Phase> {
    prop_oneof![Just(Phase::Produce), Just(Phase::Consume)]
}

/// An arbitrary [`Throughput`] with finite, non-NaN rates so the report
/// round-trips by value.
fn throughput() -> impl Strategy<Value = Throughput> {
    (finite_f64(), finite_f64()).prop_map(|(records_per_sec, bytes_per_sec)| Throughput {
        records_per_sec,
        bytes_per_sec,
    })
}

/// Arbitrary [`WorkloadParameters`].
///
/// Round-trip correctness does not depend on the values being *valid* (the
/// report carries whatever parameters a run used), only on them being finite,
/// so fields range freely while floors stay finite and durations stay valid.
fn workload_parameters() -> impl Strategy<Value = WorkloadParameters> {
    (
        any::<u64>(),
        any::<usize>(),
        key_mode(),
        any::<u32>(),
        any::<u32>(),
        text(),
        any::<u64>(),
        duration(),
        duration(),
        proptest::option::of(finite_f64()),
        proptest::option::of(finite_f64()),
    )
        .prop_map(
            |(
                record_count,
                value_size,
                key_mode,
                partition_count,
                producer_concurrency,
                topic,
                warmup,
                time_budget,
                startup_budget,
                floor_produce_rps,
                floor_consume_rps,
            )| WorkloadParameters {
                record_count,
                value_size,
                key_mode,
                partition_count,
                producer_concurrency,
                batch_size: 1,
                topic,
                warmup,
                time_budget,
                startup_budget,
                floor_produce_rps,
                floor_consume_rps,
            },
        )
}

/// An arbitrary [`FailureReason`] covering every variant, with finite rates on
/// the floor-breach variants.
fn failure_reason() -> impl Strategy<Value = FailureReason> {
    prop_oneof![
        text().prop_map(|topic| FailureReason::TopicAlreadyExists { topic }),
        (text(), text())
            .prop_map(|(name, detail)| FailureReason::InvalidParameter { name, detail }),
        (text(), text())
            .prop_map(|(topic, cause)| FailureReason::TopicCreationFailed { topic, cause }),
        any::<u64>().prop_map(|budget_secs| FailureReason::ClusterNotReady { budget_secs }),
        (text(), any::<u32>(), text()).prop_map(|(topic, partition, cause)| {
            FailureReason::ProduceError {
                topic,
                partition,
                cause,
            }
        }),
        (text(), any::<u32>(), text()).prop_map(|(topic, partition, cause)| {
            FailureReason::ConsumeError {
                topic,
                partition,
                cause,
            }
        }),
        (phase(), text()).prop_map(|(phase, cause)| FailureReason::WarmupFailed { phase, cause }),
        phase().prop_map(|phase| FailureReason::ZeroMeasurementWindow { phase }),
        (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(budget_secs, read, expected)| {
            FailureReason::TimeBudgetExceeded {
                budget_secs,
                read,
                expected,
            }
        }),
        (any::<u64>(), any::<u64>())
            .prop_map(|(read, expected)| FailureReason::IntegrityCountMismatch { read, expected }),
        any::<u64>().prop_map(|position| FailureReason::IntegrityPayloadMismatch { position }),
        (finite_f64(), finite_f64()).prop_map(|(measured_rps, floor_rps)| {
            FailureReason::FloorBreachProduce {
                measured_rps,
                floor_rps,
            }
        }),
        (finite_f64(), finite_f64()).prop_map(|(measured_rps, floor_rps)| {
            FailureReason::FloorBreachConsume {
                measured_rps,
                floor_rps,
            }
        }),
    ]
}

/// An arbitrary [`Outcome`]: either `Passed` or `Failed` with a generated
/// reason.
fn outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        Just(Outcome::Passed),
        failure_reason().prop_map(|reason| Outcome::Failed { reason }),
    ]
}

/// An arbitrary [`BenchmarkReport`] with finite, non-NaN floats so the JSON
/// round-trip preserves value equality.
fn benchmark_report() -> impl Strategy<Value = BenchmarkReport> {
    (
        workload_parameters(),
        outcome(),
        proptest::option::of(throughput()),
        proptest::option::of(throughput()),
        any::<u64>(),
        any::<u64>(),
        duration(),
        proptest::option::of(failure_reason()),
    )
        .prop_map(
            |(
                params,
                outcome,
                produce_throughput,
                consume_throughput,
                acknowledged_records,
                total_payload_bytes,
                total_elapsed,
                failure_reason,
            )| BenchmarkReport {
                params,
                outcome,
                produce_throughput,
                consume_throughput,
                acknowledged_records,
                total_payload_bytes,
                total_elapsed,
                failure_reason,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: throughput-benchmark, Property 10: The Benchmark_Report carries every required field and round-trips through JSON
    #[test]
    fn report_is_complete_and_round_trips_through_json(report in benchmark_report()) {
        // 1. JSON round-trip is lossless (Requirement 6.1): serializing then
        //    deserializing yields an equal report. Finite, non-NaN floats keep
        //    structural equality meaningful.
        let json = report.to_json().expect("serialize report to JSON");
        let back: BenchmarkReport =
            serde_json::from_str(&json).expect("deserialize report from JSON");
        prop_assert_eq!(&back, &report);

        // 2. Completeness (Requirements 6.1, 6.2): every reported value is a
        //    separately named top-level field. A `None` field is present as an
        //    explicit `null`, so presence (not non-null) is what we assert.
        let value: serde_json::Value =
            serde_json::from_str(&json).expect("parse report JSON as a value");
        let object = value
            .as_object()
            .expect("the serialized report is a JSON object");
        for field in REQUIRED_FIELDS {
            prop_assert!(
                object.contains_key(*field),
                "report JSON is missing required field `{}`",
                field
            );
        }

        // 3. Absent-not-zero (Requirement 6.5): a phase that did not complete
        //    carries `None`, which must serialize as JSON `null` rather than a
        //    misleading measured `0`.
        if report.produce_throughput.is_none() {
            prop_assert!(
                value["produce_throughput"].is_null(),
                "absent produce_throughput must serialize as null, not zero"
            );
        }
        if report.consume_throughput.is_none() {
            prop_assert!(
                value["consume_throughput"].is_null(),
                "absent consume_throughput must serialize as null, not zero"
            );
        }
    }
}
