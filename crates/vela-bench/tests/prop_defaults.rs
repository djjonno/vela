// Feature: throughput-benchmark, Property 5: Defaults are in range and a fully-defaulted configuration validates
//!
//! Property 5: *For any* parameter left unsupplied, the documented default
//! value lies within that parameter's accepted range, and a configuration built
//! entirely from defaults passes validation.
//!
//! The benchmark's defaults are fixed constants (`DEFAULT_*`), so the core of
//! this property has no input space: it is checked by deterministic assertions
//! that each documented default is `.contains()`-ed by its corresponding range
//! (the topic by character length, the time/startup budgets by `.as_secs()`),
//! that the default warmup is strictly less than the default record count, and
//! that `WorkloadParameters::default().validate()` is `Ok`.
//!
//! A `proptest` complements those deterministic checks by confirming the
//! defaulted configuration still validates regardless of which optional
//! throughput floors are supplied — the floors are the only inputs that may be
//! "left unsupplied" yet vary — exercising the property across that input space
//! at the workspace-standard minimum of 100 iterations.
//!
//! Validates: Requirements 4.2

use proptest::prelude::*;
use vela_bench::params::{
    KeyMode, WorkloadParameters, DEFAULT_KEY_MODE, DEFAULT_PARTITION_COUNT,
    DEFAULT_PRODUCER_CONCURRENCY, DEFAULT_RECORD_COUNT, DEFAULT_STARTUP_BUDGET,
    DEFAULT_TIME_BUDGET, DEFAULT_TOPIC, DEFAULT_VALUE_SIZE, DEFAULT_WARMUP, PARTITION_COUNT_RANGE,
    PRODUCER_CONCURRENCY_RANGE, RECORD_COUNT_RANGE, STARTUP_BUDGET_SECS_RANGE,
    TIME_BUDGET_SECS_RANGE, TOPIC_LEN_RANGE, VALUE_SIZE_RANGE,
};

/// Each documented default lies within its accepted range. These are constant
/// checks (no input space), so they are asserted directly rather than generated.
#[test]
fn each_default_is_within_its_range() {
    assert!(
        RECORD_COUNT_RANGE.contains(&DEFAULT_RECORD_COUNT),
        "DEFAULT_RECORD_COUNT {DEFAULT_RECORD_COUNT} outside {RECORD_COUNT_RANGE:?}"
    );
    assert!(
        VALUE_SIZE_RANGE.contains(&DEFAULT_VALUE_SIZE),
        "DEFAULT_VALUE_SIZE {DEFAULT_VALUE_SIZE} outside {VALUE_SIZE_RANGE:?}"
    );
    assert!(
        PARTITION_COUNT_RANGE.contains(&DEFAULT_PARTITION_COUNT),
        "DEFAULT_PARTITION_COUNT {DEFAULT_PARTITION_COUNT} outside {PARTITION_COUNT_RANGE:?}"
    );
    assert!(
        PRODUCER_CONCURRENCY_RANGE.contains(&DEFAULT_PRODUCER_CONCURRENCY),
        "DEFAULT_PRODUCER_CONCURRENCY {DEFAULT_PRODUCER_CONCURRENCY} outside \
         {PRODUCER_CONCURRENCY_RANGE:?}"
    );

    let topic_len = DEFAULT_TOPIC.chars().count();
    assert!(
        TOPIC_LEN_RANGE.contains(&topic_len),
        "DEFAULT_TOPIC length {topic_len} outside {TOPIC_LEN_RANGE:?}"
    );

    // Warmup's range is relative: `0..record_count`. The default must be
    // strictly below the default record count for a fully-defaulted config to
    // validate (Requirement 4.6). Both operands are constants, so this is a
    // compile-time check.
    const {
        assert!(DEFAULT_WARMUP < DEFAULT_RECORD_COUNT);
    }

    assert!(
        TIME_BUDGET_SECS_RANGE.contains(&DEFAULT_TIME_BUDGET.as_secs()),
        "DEFAULT_TIME_BUDGET {:?} outside {TIME_BUDGET_SECS_RANGE:?} seconds",
        DEFAULT_TIME_BUDGET
    );
    assert!(
        STARTUP_BUDGET_SECS_RANGE.contains(&DEFAULT_STARTUP_BUDGET.as_secs()),
        "DEFAULT_STARTUP_BUDGET {:?} outside {STARTUP_BUDGET_SECS_RANGE:?} seconds",
        DEFAULT_STARTUP_BUDGET
    );

    // The budget defaults must be whole seconds: `validate()` rejects any
    // sub-second component, so a fractional default would fail validation.
    assert_eq!(
        DEFAULT_TIME_BUDGET.subsec_nanos(),
        0,
        "DEFAULT_TIME_BUDGET must be a whole number of seconds"
    );
    assert_eq!(
        DEFAULT_STARTUP_BUDGET.subsec_nanos(),
        0,
        "DEFAULT_STARTUP_BUDGET must be a whole number of seconds"
    );

    // The key-mode default is one of the two valid variants by construction.
    assert!(matches!(
        DEFAULT_KEY_MODE,
        KeyMode::Keyed | KeyMode::Keyless
    ));
}

/// A configuration built entirely from defaults passes validation, and the
/// constructed struct echoes every documented default.
#[test]
fn fully_defaulted_configuration_validates() {
    let params = WorkloadParameters::default();

    assert!(
        params.validate().is_ok(),
        "fully-defaulted configuration must validate, got {:?}",
        params.validate()
    );

    assert_eq!(params.record_count, DEFAULT_RECORD_COUNT);
    assert_eq!(params.value_size, DEFAULT_VALUE_SIZE);
    assert_eq!(params.key_mode, DEFAULT_KEY_MODE);
    assert_eq!(params.partition_count, DEFAULT_PARTITION_COUNT);
    assert_eq!(params.producer_concurrency, DEFAULT_PRODUCER_CONCURRENCY);
    assert_eq!(params.topic, DEFAULT_TOPIC);
    assert_eq!(params.warmup, DEFAULT_WARMUP);
    assert_eq!(params.time_budget, DEFAULT_TIME_BUDGET);
    assert_eq!(params.startup_budget, DEFAULT_STARTUP_BUDGET);
    // Unsupplied optional floors default to absent.
    assert_eq!(params.floor_produce_rps, None);
    assert_eq!(params.floor_consume_rps, None);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// The defaulted configuration still validates regardless of which optional
    /// throughput floors are left unsupplied or supplied with any finite value:
    /// the floors are unconstrained inputs, so a fully-defaulted run with any
    /// floor choice must validate (floors gate the Outcome, not validation).
    #[test]
    fn defaulted_config_validates_for_any_floor_choice(
        produce_floor in proptest::option::of(0.0f64..1e12),
        consume_floor in proptest::option::of(0.0f64..1e12),
    ) {
        let params = WorkloadParameters {
            floor_produce_rps: produce_floor,
            floor_consume_rps: consume_floor,
            ..WorkloadParameters::default()
        };
        prop_assert!(
            params.validate().is_ok(),
            "defaulted config with floors ({produce_floor:?}, {consume_floor:?}) must validate"
        );
    }
}
