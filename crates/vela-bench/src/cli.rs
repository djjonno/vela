//! The `clap` CLI argument model, mapped to [`WorkloadParameters`].
//!
//! [`Cli`] is the `clap` derive surface for the `vela-bench` binary. It exposes
//! every Workload_Parameter (Requirement 4.1) — applying the documented
//! `DEFAULT_*` values from [`crate::params`] when an argument is omitted
//! (Requirement 4.2) — plus the CI-facing knobs that are *not* part of the
//! validated workload: the machine-readable and HTML report output paths
//! (`--report-json` / `--report-html`), the per-run time budget, the cluster
//! startup budget, and the optional throughput floors (Requirement 8.1).
//!
//! Parsing here is intentionally permissive about ranges: [`Cli`] only turns
//! text into typed values. Range enforcement is the sole responsibility of
//! [`WorkloadParameters::validate`], so an out-of-range value supplied on the
//! command line is mapped into a `WorkloadParameters` by [`Cli::to_params`] and
//! then rejected by validation before any phase begins (Requirements 4.5, 4.6).
//!
//! The report output paths are deliberately kept off [`WorkloadParameters`]
//! (they do not influence what is measured) and are exposed separately as the
//! public [`Cli::report_json`] / [`Cli::report_html`] fields for `main.rs`.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

use crate::params::{
    KeyMode, WorkloadParameters, DEFAULT_KEY_MODE, DEFAULT_PARTITION_COUNT,
    DEFAULT_PRODUCER_CONCURRENCY, DEFAULT_RECORD_COUNT, DEFAULT_STARTUP_BUDGET,
    DEFAULT_TIME_BUDGET, DEFAULT_TOPIC, DEFAULT_VALUE_SIZE, DEFAULT_WARMUP,
};

/// The key mode as parsed from the command line.
///
/// A thin, `clap`-aware mirror of [`KeyMode`] (the params module stays free of
/// any CLI dependency). The `ValueEnum` derive accepts `keyed` / `keyless` on
/// the command line; [`From<KeyModeArg>`] maps it onto the domain enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum KeyModeArg {
    /// Attach a deterministic key to every produced record.
    Keyed,
    /// Produce keyless records.
    Keyless,
}

impl Default for KeyModeArg {
    /// Mirrors [`DEFAULT_KEY_MODE`] so the CLI default tracks the documented
    /// workload default in one place.
    fn default() -> Self {
        Self::from_key_mode(DEFAULT_KEY_MODE)
    }
}

impl KeyModeArg {
    const fn from_key_mode(mode: KeyMode) -> Self {
        match mode {
            KeyMode::Keyed => Self::Keyed,
            KeyMode::Keyless => Self::Keyless,
        }
    }
}

impl From<KeyModeArg> for KeyMode {
    fn from(arg: KeyModeArg) -> Self {
        match arg {
            KeyModeArg::Keyed => KeyMode::Keyed,
            KeyModeArg::Keyless => KeyMode::Keyless,
        }
    }
}

impl std::fmt::Display for KeyModeArg {
    /// Renders the canonical `clap` value name (`keyed` / `keyless`), which is
    /// what `default_value_t` needs to display the default in `--help`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_possible_value()
            .expect("KeyModeArg has no skipped variants")
            .get_name()
            .fmt(f)
    }
}

/// The `vela-bench` command-line surface.
///
/// Construct via [`Cli::parse`] in the binary, or [`Cli::try_parse_from`] in
/// tests. Use [`Cli::to_params`] to obtain the [`WorkloadParameters`] for the
/// Benchmark_Run; read [`Cli::report_json`] / [`Cli::report_html`] for the
/// (optional) report output paths.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "vela-bench",
    about = "Measure Vela's end-to-end produce and consume throughput.",
    version
)]
pub struct Cli {
    /// Number of records to produce and consume (1..=1_000_000_000).
    #[arg(long, default_value_t = DEFAULT_RECORD_COUNT, env = "VELA_BENCH_RECORD_COUNT")]
    pub record_count: u64,

    /// Record value size in bytes (0..=10_485_760).
    #[arg(long, default_value_t = DEFAULT_VALUE_SIZE, env = "VELA_BENCH_VALUE_SIZE")]
    pub value_size: usize,

    /// Whether produced records are keyed or keyless.
    #[arg(long, value_enum, default_value_t = KeyModeArg::default(), env = "VELA_BENCH_KEY_MODE")]
    pub key_mode: KeyModeArg,

    /// Target topic partition count (1..=10_000).
    #[arg(long, default_value_t = DEFAULT_PARTITION_COUNT, env = "VELA_BENCH_PARTITION_COUNT")]
    pub partition_count: u32,

    /// Produce requests kept in flight concurrently (1..=10_000).
    #[arg(long, default_value_t = DEFAULT_PRODUCER_CONCURRENCY, env = "VELA_BENCH_PRODUCER_CONCURRENCY")]
    pub producer_concurrency: u32,

    /// Target topic name (1..=255 characters).
    #[arg(long, default_value = DEFAULT_TOPIC, env = "VELA_BENCH_TOPIC")]
    pub topic: String,

    /// Warmup operations per phase, excluded from the Measurement_Window
    /// (0..record_count).
    #[arg(long, default_value_t = DEFAULT_WARMUP, env = "VELA_BENCH_WARMUP")]
    pub warmup: u64,

    /// Per-run time budget, in seconds (1..=86_400; default 60).
    #[arg(long, default_value_t = DEFAULT_TIME_BUDGET.as_secs(), env = "VELA_BENCH_TIME_BUDGET_SECS")]
    pub time_budget_secs: u64,

    /// Cluster startup budget, in seconds (1..=600; default 60).
    #[arg(long, default_value_t = DEFAULT_STARTUP_BUDGET.as_secs(), env = "VELA_BENCH_STARTUP_BUDGET_SECS")]
    pub startup_budget_secs: u64,

    /// Optional Produce_Throughput floor in records/sec; the run fails if the
    /// measured produce throughput is below this value.
    #[arg(long, env = "VELA_BENCH_FLOOR_PRODUCE_RPS")]
    pub floor_produce_rps: Option<f64>,

    /// Optional Consume_Throughput floor in records/sec; the run fails if the
    /// measured consume throughput is below this value.
    #[arg(long, env = "VELA_BENCH_FLOOR_CONSUME_RPS")]
    pub floor_consume_rps: Option<f64>,

    /// Path to write the machine-readable JSON Benchmark_Report. Not part of
    /// the workload; surfaced separately for the entry point.
    #[arg(long, value_name = "PATH", env = "VELA_BENCH_REPORT_JSON")]
    pub report_json: Option<PathBuf>,

    /// Path to write the self-contained HTML_Report. Not part of the workload;
    /// surfaced separately for the entry point.
    #[arg(long, value_name = "PATH", env = "VELA_BENCH_REPORT_HTML")]
    pub report_html: Option<PathBuf>,
}

impl Cli {
    /// Map the parsed arguments onto the validated workload model.
    ///
    /// The time and startup budgets are carried on the command line as whole
    /// seconds and reconstituted here as [`Duration`]s. Report output paths are
    /// intentionally excluded — they are read directly from [`Cli`]. The
    /// returned value is *not* yet range-checked; the caller must invoke
    /// [`WorkloadParameters::validate`] (Requirements 4.5, 4.6).
    pub fn to_params(&self) -> WorkloadParameters {
        WorkloadParameters {
            record_count: self.record_count,
            value_size: self.value_size,
            key_mode: self.key_mode.into(),
            partition_count: self.partition_count,
            producer_concurrency: self.producer_concurrency,
            topic: self.topic.clone(),
            warmup: self.warmup,
            time_budget: Duration::from_secs(self.time_budget_secs),
            startup_budget: Duration::from_secs(self.startup_budget_secs),
            floor_produce_rps: self.floor_produce_rps,
            floor_consume_rps: self.floor_consume_rps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Defaults are applied for every Workload_Parameter when no arguments are
    /// supplied, and they match the documented `DEFAULT_*` consts
    /// (Requirement 4.2). Report paths and floors default to absent.
    #[test]
    fn defaults_apply_when_args_omitted() {
        let cli = Cli::try_parse_from(["vela-bench"]).expect("defaults parse");
        let params = cli.to_params();

        assert_eq!(params, WorkloadParameters::default());
        assert!(cli.report_json.is_none());
        assert!(cli.report_html.is_none());
        assert!(params.floor_produce_rps.is_none());
        assert!(params.floor_consume_rps.is_none());
    }

    /// A representative, fully-specified arg vector maps onto the expected
    /// `WorkloadParameters`, with second-valued budgets reconstituted as
    /// `Duration`s and report paths surfaced separately (Requirement 4.1, 8.1).
    #[test]
    fn full_arg_vector_maps_to_parameters() {
        let cli = Cli::try_parse_from([
            "vela-bench",
            "--record-count",
            "5000",
            "--value-size",
            "1024",
            "--key-mode",
            "keyed",
            "--partition-count",
            "8",
            "--producer-concurrency",
            "32",
            "--topic",
            "bench-topic",
            "--warmup",
            "100",
            "--time-budget-secs",
            "120",
            "--startup-budget-secs",
            "30",
            "--floor-produce-rps",
            "1000.5",
            "--floor-consume-rps",
            "900.25",
            "--report-json",
            "/tmp/report.json",
            "--report-html",
            "/tmp/report.html",
        ])
        .expect("full arg vector parses");

        let params = cli.to_params();

        assert_eq!(
            params,
            WorkloadParameters {
                record_count: 5000,
                value_size: 1024,
                key_mode: KeyMode::Keyed,
                partition_count: 8,
                producer_concurrency: 32,
                topic: "bench-topic".to_string(),
                warmup: 100,
                time_budget: Duration::from_secs(120),
                startup_budget: Duration::from_secs(30),
                floor_produce_rps: Some(1000.5),
                floor_consume_rps: Some(900.25),
            }
        );
        assert_eq!(
            cli.report_json.as_deref(),
            Some(PathBuf::from("/tmp/report.json").as_path())
        );
        assert_eq!(
            cli.report_html.as_deref(),
            Some(PathBuf::from("/tmp/report.html").as_path())
        );
    }

    /// The `--key-mode` value enum maps onto the domain [`KeyMode`] for both
    /// variants (Requirement 4.1).
    #[test]
    fn key_mode_enum_maps_correctly() {
        let keyed = Cli::try_parse_from(["vela-bench", "--key-mode", "keyed"])
            .expect("keyed parses")
            .to_params();
        assert_eq!(keyed.key_mode, KeyMode::Keyed);

        let keyless = Cli::try_parse_from(["vela-bench", "--key-mode", "keyless"])
            .expect("keyless parses")
            .to_params();
        assert_eq!(keyless.key_mode, KeyMode::Keyless);
    }

    /// The default key mode tracks [`DEFAULT_KEY_MODE`].
    #[test]
    fn key_mode_default_tracks_const() {
        assert_eq!(KeyMode::from(KeyModeArg::default()), DEFAULT_KEY_MODE);
    }

    /// Out-of-range values parse fine here (parsing is permissive); rejection
    /// is left to `validate` (Requirements 4.5, 4.6).
    #[test]
    fn out_of_range_value_parses_but_fails_validation() {
        let params = Cli::try_parse_from(["vela-bench", "--record-count", "0"])
            .expect("zero record-count still parses")
            .to_params();
        assert_eq!(params.validate().unwrap_err().name, "record_count");
    }
}
