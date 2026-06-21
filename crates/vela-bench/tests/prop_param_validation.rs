// Feature: throughput-benchmark, Property 4: Parameter validation accepts in-range inputs and rejects out-of-range ones by name
//!
//! Property 4: [`WorkloadParameters::validate`] is a pure range check. For any
//! fully in-range configuration it returns `Ok(())` and leaves the parameters
//! untouched (it "echoes" its inputs — there is nothing to mutate and no side
//! effect such as starting a cluster or creating a topic). For any
//! configuration that pushes exactly one field out of its accepted range it
//! returns `Err` whose `name` identifies that field, again with no side effects
//! (Requirements 3.5, 4.1, 4.5, 4.6, 10.5).
//!
//! The two generators constrain inputs to the quantified space:
//!
//! * `valid_params` draws every field inside its inclusive range and `warmup`
//!   strictly below `record_count`, so the whole struct is a valid Benchmark_Run
//!   configuration.
//! * `out_of_range_case` starts from a valid configuration and perturbs exactly
//!   one field past its bound — covering each field, including
//!   `partition_count == 0` (Requirement 3.5) and `warmup >= record_count`
//!   (Requirement 4.6) — pairing the mutated parameters with the field name
//!   `validate` must report. Because every other field stays valid, the
//!   validator's first failing check is the perturbed one, so the reported name
//!   is deterministic.
//!
//! "No side effects" is asserted by comparing the parameters before and after
//! the call: `validate` borrows `&self`, so an unchanged struct demonstrates it
//! neither mutates its input nor does anything else observable.
//!
//! Validates: Requirements 3.5, 4.1, 4.5, 4.6, 10.5

use std::time::Duration;

use proptest::option;
use proptest::prelude::*;
use vela_bench::params::{KeyMode, WorkloadParameters};

/// Generate a fully in-range [`WorkloadParameters`]: every field within its
/// accepted range and `warmup` strictly below `record_count`. The time and
/// startup budgets are whole-second `Duration`s (zero sub-second nanos), which
/// is what `validate` accepts.
fn valid_params() -> impl Strategy<Value = WorkloadParameters> {
    (
        1u64..=1_000_000_000u64,         // record_count
        0usize..=10_485_760usize,        // value_size
        any::<bool>(),                   // keyed?
        1u32..=10_000u32,                // partition_count
        1u32..=10_000u32,                // producer_concurrency
        "[a-zA-Z0-9_-]{1,255}",          // topic (1..=255 ASCII chars => 1..=255 chars)
        1u64..=86_400u64,                // time_budget seconds
        1u64..=600u64,                   // startup_budget seconds
        option::of(0.0f64..1_000_000.0), // floor_produce_rps
        option::of(0.0f64..1_000_000.0), // floor_consume_rps
    )
        .prop_flat_map(|(rc, vs, keyed, pc, conc, topic, tb, sb, fp, fc)| {
            // warmup is bounded relative to record_count: `0..record_count`.
            (0u64..rc).prop_map(move |warmup| WorkloadParameters {
                record_count: rc,
                value_size: vs,
                key_mode: if keyed {
                    KeyMode::Keyed
                } else {
                    KeyMode::Keyless
                },
                partition_count: pc,
                producer_concurrency: conc,
                topic: topic.clone(),
                warmup,
                time_budget: Duration::from_secs(tb),
                startup_budget: Duration::from_secs(sb),
                floor_produce_rps: fp,
                floor_consume_rps: fc,
            })
        })
}

/// A single-field perturbation that pushes one parameter out of its range.
#[derive(Debug, Clone)]
enum OutOfRange {
    RecordCountBelow,
    RecordCountAbove(u64),
    ValueSizeAbove(usize),
    PartitionCountZero,
    ProducerConcurrencyAbove(u32),
    TopicEmpty,
    TopicTooLong(usize),
    WarmupGeRecordCount(u64),
    TimeBudgetZero,
    TimeBudgetAbove(u64),
    StartupBudgetAbove(u64),
}

/// Apply one out-of-range perturbation to an otherwise-valid configuration,
/// returning the mutated parameters paired with the field name `validate` must
/// report. Every untouched field remains valid, so the validator's first
/// failing check is exactly the perturbed field.
fn apply(mut p: WorkloadParameters, oor: OutOfRange) -> (WorkloadParameters, &'static str) {
    let name = match oor {
        OutOfRange::RecordCountBelow => {
            p.record_count = 0;
            "record_count"
        }
        OutOfRange::RecordCountAbove(n) => {
            p.record_count = 1_000_000_000 + n; // n >= 1 => above the inclusive max
            "record_count"
        }
        OutOfRange::ValueSizeAbove(n) => {
            p.value_size = 10_485_760 + n;
            "value_size"
        }
        OutOfRange::PartitionCountZero => {
            p.partition_count = 0;
            "partition_count"
        }
        OutOfRange::ProducerConcurrencyAbove(n) => {
            p.producer_concurrency = 10_000 + n;
            "producer_concurrency"
        }
        OutOfRange::TopicEmpty => {
            p.topic = String::new();
            "topic"
        }
        OutOfRange::TopicTooLong(n) => {
            p.topic = "x".repeat(255 + n); // n >= 1 => length >= 256 chars
            "topic"
        }
        OutOfRange::WarmupGeRecordCount(n) => {
            p.warmup = p.record_count + n; // n >= 0 => warmup >= record_count
            "warmup"
        }
        OutOfRange::TimeBudgetZero => {
            p.time_budget = Duration::from_secs(0);
            "time_budget"
        }
        OutOfRange::TimeBudgetAbove(n) => {
            p.time_budget = Duration::from_secs(86_400 + n);
            "time_budget"
        }
        OutOfRange::StartupBudgetAbove(n) => {
            p.startup_budget = Duration::from_secs(600 + n);
            "startup_budget"
        }
    };
    (p, name)
}

/// From a valid configuration, derive a case that violates exactly one range,
/// paired with the field name `validate` must name.
fn out_of_range_case(
    params: WorkloadParameters,
) -> impl Strategy<Value = (WorkloadParameters, &'static str)> {
    prop_oneof![
        Just(OutOfRange::RecordCountBelow),
        (1u64..=1_000_000_000).prop_map(OutOfRange::RecordCountAbove),
        (1usize..=10_000_000).prop_map(OutOfRange::ValueSizeAbove),
        Just(OutOfRange::PartitionCountZero),
        (1u32..=10_000).prop_map(OutOfRange::ProducerConcurrencyAbove),
        Just(OutOfRange::TopicEmpty),
        (1usize..=64).prop_map(OutOfRange::TopicTooLong),
        (0u64..=1_000).prop_map(OutOfRange::WarmupGeRecordCount),
        Just(OutOfRange::TimeBudgetZero),
        (1u64..=100_000).prop_map(OutOfRange::TimeBudgetAbove),
        (1u64..=1_000).prop_map(OutOfRange::StartupBudgetAbove),
    ]
    .prop_map(move |oor| apply(params.clone(), oor))
}

proptest! {
    // Well above the >=100 floor; proptest's default is 256.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// An in-range configuration validates and is left untouched (echoes its
    /// inputs, no side effects).
    #[test]
    fn in_range_validates_without_mutation(params in valid_params()) {
        let before = params.clone();
        prop_assert!(params.validate().is_ok());
        // `validate` borrows `&self`; an unchanged struct shows no side effect.
        prop_assert_eq!(&params, &before);
    }

    /// A single out-of-range field is rejected by name, with no side effects.
    #[test]
    fn out_of_range_rejected_by_name(
        (params, expected) in valid_params().prop_flat_map(out_of_range_case)
    ) {
        let before = params.clone();
        let result = params.validate();
        prop_assert!(result.is_err());
        let err = result.unwrap_err();
        prop_assert_eq!(err.name.as_str(), expected);
        prop_assert_eq!(&params, &before);
    }
}
