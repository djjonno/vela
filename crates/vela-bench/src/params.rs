//! Workload_Parameters: the model, documented defaults, and range validation.
//!
//! [`WorkloadParameters`] is the validated set of inputs to one Benchmark_Run
//! (Requirement 4.1). [`WorkloadParameters::default`] supplies a documented
//! default for every field, each within that field's accepted range
//! (Requirement 4.2), and [`WorkloadParameters::validate`] enforces every range
//! as a pure check that names the offending field and performs no side effects
//! (Requirements 3.5, 4.5, 4.6, 10.5). A validation failure maps later to
//! `FailureReason::InvalidParameter`.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Whether produced records carry a key (and are therefore routed by the
/// cluster's keyed partitioning rule) or are keyless (Requirement 4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyMode {
    /// A deterministic key is attached to every produced record.
    Keyed,
    /// No key is attached; records are routed without a key.
    Keyless,
}

// ---------------------------------------------------------------------------
// Accepted ranges (Requirements 4.1, 5.4, 9.3, 10.5).
// ---------------------------------------------------------------------------

/// Inclusive accepted range for `record_count`.
pub const RECORD_COUNT_RANGE: std::ops::RangeInclusive<u64> = 1..=1_000_000_000;
/// Inclusive accepted range for `value_size`, in bytes (0 to 10 MiB).
pub const VALUE_SIZE_RANGE: std::ops::RangeInclusive<usize> = 0..=10_485_760;
/// Inclusive accepted range for `partition_count`.
pub const PARTITION_COUNT_RANGE: std::ops::RangeInclusive<u32> = 1..=10_000;
/// Inclusive accepted range for `producer_concurrency`.
pub const PRODUCER_CONCURRENCY_RANGE: std::ops::RangeInclusive<u32> = 1..=10_000;
/// Inclusive accepted range for the target topic name length, in characters.
pub const TOPIC_LEN_RANGE: std::ops::RangeInclusive<usize> = 1..=255;
/// Inclusive accepted range for the per-run time budget, in seconds.
pub const TIME_BUDGET_SECS_RANGE: std::ops::RangeInclusive<u64> = 1..=86_400;
/// Inclusive accepted range for the cluster startup budget, in seconds.
pub const STARTUP_BUDGET_SECS_RANGE: std::ops::RangeInclusive<u64> = 1..=600;

// ---------------------------------------------------------------------------
// Documented defaults (Requirement 4.2). Every value below lies within the
// corresponding accepted range above, and the warmup default is strictly less
// than the record-count default, so a fully-defaulted configuration validates.
// ---------------------------------------------------------------------------

/// Default number of records produced and consumed in a Benchmark_Run.
pub const DEFAULT_RECORD_COUNT: u64 = 100_000;
/// Default record value size, in bytes.
pub const DEFAULT_VALUE_SIZE: usize = 256;
/// Default key mode.
pub const DEFAULT_KEY_MODE: KeyMode = KeyMode::Keyless;
/// Default partition count for the target topic.
pub const DEFAULT_PARTITION_COUNT: u32 = 4;
/// Default number of produce requests kept in flight concurrently.
pub const DEFAULT_PRODUCER_CONCURRENCY: u32 = 16;
/// Default target topic name.
pub const DEFAULT_TOPIC: &str = "vela-bench";
/// Default warmup operation count per phase.
pub const DEFAULT_WARMUP: u64 = 0;
/// Default per-run time budget (Requirement 5.4).
pub const DEFAULT_TIME_BUDGET: Duration = Duration::from_secs(60);
/// Default cluster startup budget (Requirement 9.3).
pub const DEFAULT_STARTUP_BUDGET: Duration = Duration::from_secs(60);

/// A range-validation failure naming the offending Workload_Parameter.
///
/// This is a pure value produced by [`WorkloadParameters::validate`]; it maps
/// later to `FailureReason::InvalidParameter { name, detail }` so the offending
/// field surfaces in the Benchmark_Report, the stdout summary, and the
/// HTML_Report.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid workload parameter `{name}`: {detail}")]
pub struct ValidationError {
    /// The name of the offending parameter (e.g. `"record_count"`).
    pub name: String,
    /// A human-readable description of how the value violated its range.
    pub detail: String,
}

impl ValidationError {
    fn new(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            detail: detail.into(),
        }
    }
}

/// Validated inputs to one Benchmark_Run (Requirement 4.1).
///
/// Construct directly or from [`WorkloadParameters::default`], then call
/// [`WorkloadParameters::validate`] before entering any phase. Validation is a
/// pure check with no side effects (no cluster start, no topic creation), so an
/// out-of-range value is rejected before the Producer_Phase begins
/// (Requirements 4.5, 4.6).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadParameters {
    /// Number of records to produce and consume (`1..=1_000_000_000`).
    pub record_count: u64,
    /// Record value size in bytes (`0..=10_485_760`).
    pub value_size: usize,
    /// Whether records are keyed or keyless.
    pub key_mode: KeyMode,
    /// Target topic partition count (`1..=10_000`).
    pub partition_count: u32,
    /// Produce requests kept in flight concurrently (`1..=10_000`).
    pub producer_concurrency: u32,
    /// Target topic name (`1..=255` characters).
    pub topic: String,
    /// Warmup operations per phase, excluded from the Measurement_Window
    /// (`0..record_count`).
    pub warmup: u64,
    /// Per-run time budget (`1..=86_400` seconds; default 60 s).
    pub time_budget: Duration,
    /// Cluster startup budget (`1..=600` seconds; default 60 s).
    pub startup_budget: Duration,
    /// Optional Produce_Throughput floor in records/sec (Requirement 11.1).
    pub floor_produce_rps: Option<f64>,
    /// Optional Consume_Throughput floor in records/sec (Requirement 11.2).
    pub floor_consume_rps: Option<f64>,
}

impl Default for WorkloadParameters {
    /// The documented defaults (Requirement 4.2). Every field lies within its
    /// accepted range and a configuration built entirely from these defaults
    /// passes [`WorkloadParameters::validate`].
    fn default() -> Self {
        Self {
            record_count: DEFAULT_RECORD_COUNT,
            value_size: DEFAULT_VALUE_SIZE,
            key_mode: DEFAULT_KEY_MODE,
            partition_count: DEFAULT_PARTITION_COUNT,
            producer_concurrency: DEFAULT_PRODUCER_CONCURRENCY,
            topic: DEFAULT_TOPIC.to_string(),
            warmup: DEFAULT_WARMUP,
            time_budget: DEFAULT_TIME_BUDGET,
            startup_budget: DEFAULT_STARTUP_BUDGET,
            floor_produce_rps: None,
            floor_consume_rps: None,
        }
    }
}

impl WorkloadParameters {
    /// Enforce every parameter range, returning `Err` naming the first
    /// offending field (Requirements 3.5, 4.1, 4.5, 4.6, 10.5).
    ///
    /// This is a pure function: it inspects `self` and produces no side effects.
    /// The `warmup` bound is relative (`0..record_count`), so a warmup count
    /// greater than or equal to the record count is rejected (Requirement 4.6),
    /// and `partition_count < 1` is rejected (Requirement 3.5). The time and
    /// startup budgets are validated in whole seconds against their ranges.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if !RECORD_COUNT_RANGE.contains(&self.record_count) {
            return Err(ValidationError::new(
                "record_count",
                format!(
                    "{} is outside the accepted range {}..={}",
                    self.record_count,
                    RECORD_COUNT_RANGE.start(),
                    RECORD_COUNT_RANGE.end()
                ),
            ));
        }

        if !VALUE_SIZE_RANGE.contains(&self.value_size) {
            return Err(ValidationError::new(
                "value_size",
                format!(
                    "{} is outside the accepted range {}..={} bytes",
                    self.value_size,
                    VALUE_SIZE_RANGE.start(),
                    VALUE_SIZE_RANGE.end()
                ),
            ));
        }

        if !PARTITION_COUNT_RANGE.contains(&self.partition_count) {
            return Err(ValidationError::new(
                "partition_count",
                format!(
                    "{} is outside the accepted range {}..={}",
                    self.partition_count,
                    PARTITION_COUNT_RANGE.start(),
                    PARTITION_COUNT_RANGE.end()
                ),
            ));
        }

        if !PRODUCER_CONCURRENCY_RANGE.contains(&self.producer_concurrency) {
            return Err(ValidationError::new(
                "producer_concurrency",
                format!(
                    "{} is outside the accepted range {}..={}",
                    self.producer_concurrency,
                    PRODUCER_CONCURRENCY_RANGE.start(),
                    PRODUCER_CONCURRENCY_RANGE.end()
                ),
            ));
        }

        let topic_len = self.topic.chars().count();
        if !TOPIC_LEN_RANGE.contains(&topic_len) {
            return Err(ValidationError::new(
                "topic",
                format!(
                    "name length {} is outside the accepted range {}..={} characters",
                    topic_len,
                    TOPIC_LEN_RANGE.start(),
                    TOPIC_LEN_RANGE.end()
                ),
            ));
        }

        // Warmup is bounded relative to record_count: `0..record_count`
        // (Requirement 4.6). record_count >= 1 here, so the range is non-empty.
        if self.warmup >= self.record_count {
            return Err(ValidationError::new(
                "warmup",
                format!(
                    "{} must be strictly less than record_count {}",
                    self.warmup, self.record_count
                ),
            ));
        }

        let time_budget_secs = self.time_budget.as_secs();
        if self.time_budget.subsec_nanos() != 0
            || !TIME_BUDGET_SECS_RANGE.contains(&time_budget_secs)
        {
            return Err(ValidationError::new(
                "time_budget",
                format!(
                    "{:?} is outside the accepted range {}..={} seconds",
                    self.time_budget,
                    TIME_BUDGET_SECS_RANGE.start(),
                    TIME_BUDGET_SECS_RANGE.end()
                ),
            ));
        }

        let startup_budget_secs = self.startup_budget.as_secs();
        if self.startup_budget.subsec_nanos() != 0
            || !STARTUP_BUDGET_SECS_RANGE.contains(&startup_budget_secs)
        {
            return Err(ValidationError::new(
                "startup_budget",
                format!(
                    "{:?} is outside the accepted range {}..={} seconds",
                    self.startup_budget,
                    STARTUP_BUDGET_SECS_RANGE.start(),
                    STARTUP_BUDGET_SECS_RANGE.end()
                ),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_configuration_validates() {
        assert!(WorkloadParameters::default().validate().is_ok());
    }

    #[test]
    fn documented_defaults_are_in_range() {
        assert!(RECORD_COUNT_RANGE.contains(&DEFAULT_RECORD_COUNT));
        assert!(VALUE_SIZE_RANGE.contains(&DEFAULT_VALUE_SIZE));
        assert!(PARTITION_COUNT_RANGE.contains(&DEFAULT_PARTITION_COUNT));
        assert!(PRODUCER_CONCURRENCY_RANGE.contains(&DEFAULT_PRODUCER_CONCURRENCY));
        assert!(TOPIC_LEN_RANGE.contains(&DEFAULT_TOPIC.chars().count()));
        const { assert!(DEFAULT_WARMUP < DEFAULT_RECORD_COUNT) };
        assert!(TIME_BUDGET_SECS_RANGE.contains(&DEFAULT_TIME_BUDGET.as_secs()));
        assert!(STARTUP_BUDGET_SECS_RANGE.contains(&DEFAULT_STARTUP_BUDGET.as_secs()));
    }

    fn valid() -> WorkloadParameters {
        WorkloadParameters::default()
    }

    #[test]
    fn record_count_below_range_is_rejected() {
        let mut p = valid();
        p.record_count = 0;
        assert_eq!(p.validate().unwrap_err().name, "record_count");
    }

    #[test]
    fn record_count_above_range_is_rejected() {
        let mut p = valid();
        p.record_count = 1_000_000_001;
        assert_eq!(p.validate().unwrap_err().name, "record_count");
    }

    #[test]
    fn value_size_above_range_is_rejected() {
        let mut p = valid();
        p.value_size = 10_485_761;
        assert_eq!(p.validate().unwrap_err().name, "value_size");
    }

    #[test]
    fn value_size_zero_is_accepted() {
        let mut p = valid();
        p.value_size = 0;
        assert!(p.validate().is_ok());
    }

    #[test]
    fn partition_count_below_one_is_rejected() {
        let mut p = valid();
        p.partition_count = 0;
        assert_eq!(p.validate().unwrap_err().name, "partition_count");
    }

    #[test]
    fn producer_concurrency_above_range_is_rejected() {
        let mut p = valid();
        p.producer_concurrency = 10_001;
        assert_eq!(p.validate().unwrap_err().name, "producer_concurrency");
    }

    #[test]
    fn empty_topic_is_rejected() {
        let mut p = valid();
        p.topic = String::new();
        assert_eq!(p.validate().unwrap_err().name, "topic");
    }

    #[test]
    fn overlong_topic_is_rejected() {
        let mut p = valid();
        p.topic = "x".repeat(256);
        assert_eq!(p.validate().unwrap_err().name, "topic");
    }

    #[test]
    fn warmup_equal_to_record_count_is_rejected() {
        let mut p = valid();
        p.record_count = 10;
        p.warmup = 10;
        assert_eq!(p.validate().unwrap_err().name, "warmup");
    }

    #[test]
    fn warmup_just_below_record_count_is_accepted() {
        let mut p = valid();
        p.record_count = 10;
        p.warmup = 9;
        assert!(p.validate().is_ok());
    }

    #[test]
    fn time_budget_zero_is_rejected() {
        let mut p = valid();
        p.time_budget = Duration::from_secs(0);
        assert_eq!(p.validate().unwrap_err().name, "time_budget");
    }

    #[test]
    fn time_budget_above_range_is_rejected() {
        let mut p = valid();
        p.time_budget = Duration::from_secs(86_401);
        assert_eq!(p.validate().unwrap_err().name, "time_budget");
    }

    #[test]
    fn startup_budget_above_range_is_rejected() {
        let mut p = valid();
        p.startup_budget = Duration::from_secs(601);
        assert_eq!(p.validate().unwrap_err().name, "startup_budget");
    }

    #[test]
    fn parameters_round_trip_through_json() {
        let p = valid();
        let json = serde_json::to_string(&p).unwrap();
        let back: WorkloadParameters = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}
