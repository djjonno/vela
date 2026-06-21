//! The [`BenchmarkReport`] model, JSON serialization, and stdout summary.
//!
//! [`BenchmarkReport`] is the single source of truth all three Benchmark_Run
//! outputs render from (Requirement 6.1): the machine-readable JSON report, the
//! human-readable stdout summary, and the self-contained HTML_Report (rendered
//! by [`crate::html`] from this same struct). Every reported value is a
//! separately named, individually addressable field (Requirement 6.2), and a
//! phase that did not complete carries its throughput as [`Option::None`] so it
//! serializes as `null`/absent rather than a misleading measured `0`
//! (Requirement 6.5).
//!
//! The stdout summary states the Produce_Throughput and Consume_Throughput in
//! both records/s and bytes/s, or an explicit `not measured` for any figure
//! that is unavailable, and prints the failure reason on a failing Outcome
//! (Requirements 6.3, 6.4). [`failure_reason_text`] is the shared human-readable
//! rendering of a [`FailureReason`] so the stdout summary and the HTML_Report
//! present identical failure text.

use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::metrics::Throughput;
use crate::outcome::{FailureReason, Outcome, Phase};
use crate::params::WorkloadParameters;

/// The literal rendered for a phase figure that was never measured
/// (Requirements 6.3, 6.8).
pub const NOT_MEASURED: &str = "not measured";

/// The single source of truth all three Benchmark_Run outputs render from
/// (Requirements 6.1, 6.2).
///
/// Each field is individually named and addressable. The two throughput figures
/// are [`Option`]s so that a phase which did not complete is represented as
/// explicitly absent (`null` in JSON, `not measured` in the stdout and HTML
/// renderings) rather than a measured zero (Requirements 6.5, 6.8). Serializing
/// the report to JSON and deserializing it yields an equal report — the `serde`
/// derives and the `null`-for-`None` encoding keep the round-trip lossless
/// (Property 10).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkReport {
    /// The validated Workload_Parameters the Benchmark_Run used.
    pub params: WorkloadParameters,
    /// The pass/fail Outcome of the Benchmark_Run.
    pub outcome: Outcome,
    /// Produce_Throughput, or `None` when the Producer_Phase did not complete.
    pub produce_throughput: Option<Throughput>,
    /// Consume_Throughput, or `None` when the Consumer_Phase did not complete.
    pub consume_throughput: Option<Throughput>,
    /// The number of Acknowledged_Records produced during the Producer_Phase.
    pub acknowledged_records: u64,
    /// The total payload bytes produced during the Producer_Phase.
    pub total_payload_bytes: u64,
    /// The total elapsed wall-clock time of the Benchmark_Run.
    pub total_elapsed: Duration,
    /// The failure reason as a named report field, or `None` on a passing
    /// Outcome (Requirement 6.4).
    pub failure_reason: Option<FailureReason>,
}

impl BenchmarkReport {
    /// Serialize the report to a pretty-printed JSON string (Requirements 6.1,
    /// 6.2).
    ///
    /// Absent throughput figures serialize as `null` rather than `0`
    /// (Requirement 6.5), and the result round-trips back to an equal report
    /// via [`serde_json::from_str`].
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    /// Write the report as pretty-printed JSON to an arbitrary writer.
    ///
    /// Pass [`std::io::stdout`] to emit the machine-readable report to standard
    /// output, or any other [`Write`] sink. Use [`BenchmarkReport::write_json_file`]
    /// to write the CI artifact to a path.
    pub fn write_json<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        serde_json::to_writer_pretty(&mut *writer, self).map_err(io::Error::other)?;
        writer.write_all(b"\n")
    }

    /// Write the report as pretty-printed JSON to the file at `path`
    /// (the CI Benchmark_Report artifact).
    pub fn write_json_file(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(file);
        self.write_json(&mut writer)?;
        writer.flush()
    }

    /// Write the human-readable stdout summary to an arbitrary writer
    /// (Requirements 6.3, 6.4).
    ///
    /// The summary states the Produce_Throughput and Consume_Throughput in both
    /// records/s and bytes/s, or the literal `not measured` for any figure that
    /// is unavailable, and — on a failing Outcome — prints the failure reason
    /// text. Writing to a generic [`Write`] keeps the rendering testable;
    /// [`BenchmarkReport::print_summary`] is the convenience that writes to
    /// standard output.
    pub fn write_summary<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let status = if self.outcome.is_passed() {
            "PASSED"
        } else {
            "FAILED"
        };
        writeln!(writer, "Benchmark outcome: {status}")?;
        writeln!(writer, "Topic: {}", self.params.topic)?;
        writeln!(
            writer,
            "Acknowledged records: {}",
            self.acknowledged_records
        )?;
        writeln!(writer, "Total payload bytes: {}", self.total_payload_bytes)?;
        writeln!(
            writer,
            "Total elapsed: {:.3} s",
            self.total_elapsed.as_secs_f64()
        )?;
        write_throughput_line(writer, "Produce throughput", self.produce_throughput)?;
        write_throughput_line(writer, "Consume throughput", self.consume_throughput)?;

        if let Some(reason) = self.report_failure_reason() {
            writeln!(writer, "Failure reason: {}", failure_reason_text(reason))?;
        }

        Ok(())
    }

    /// Write the human-readable summary to standard output (Requirement 6.3).
    pub fn print_summary(&self) -> io::Result<()> {
        let mut stdout = io::stdout().lock();
        self.write_summary(&mut stdout)
    }

    /// The failure reason to surface, preferring the named report field and
    /// falling back to the one carried by the Outcome so the two stay
    /// consistent.
    fn report_failure_reason(&self) -> Option<&FailureReason> {
        self.failure_reason
            .as_ref()
            .or_else(|| self.outcome.failure_reason())
    }
}

/// Write one phase's throughput line: both records/s and bytes/s when measured,
/// or the literal `not measured` when the figure is absent (Requirements 6.3,
/// 6.5).
fn write_throughput_line<W: Write>(
    writer: &mut W,
    label: &str,
    throughput: Option<Throughput>,
) -> io::Result<()> {
    match throughput {
        Some(t) => writeln!(
            writer,
            "{label}: {:.2} records/s, {:.2} bytes/s",
            t.records_per_sec, t.bytes_per_sec
        ),
        None => writeln!(writer, "{label}: {NOT_MEASURED}"),
    }
}

/// The shared human-readable rendering of a [`FailureReason`] (Requirement
/// 6.4).
///
/// Both the stdout summary and the HTML_Report ([`crate::html`]) render the
/// failure reason from this function so the two present identical text (the
/// HTML_Report HTML-escapes the result on substitution).
pub fn failure_reason_text(reason: &FailureReason) -> String {
    match reason {
        FailureReason::TopicAlreadyExists { topic } => {
            format!("target topic `{topic}` already exists")
        }
        FailureReason::InvalidParameter { name, detail } => {
            format!("invalid parameter `{name}`: {detail}")
        }
        FailureReason::TopicCreationFailed { topic, cause } => {
            format!("target topic `{topic}` creation failed: {cause}")
        }
        FailureReason::ClusterNotReady { budget_secs } => {
            format!("cluster did not become ready within the {budget_secs} s startup budget")
        }
        FailureReason::ProduceError {
            topic,
            partition,
            cause,
        } => format!("produce error on topic `{topic}` partition {partition}: {cause}"),
        FailureReason::ConsumeError {
            topic,
            partition,
            cause,
        } => format!("consume error on topic `{topic}` partition {partition}: {cause}"),
        FailureReason::WarmupFailed { phase, cause } => {
            format!("{} warmup failed: {cause}", phase_text(*phase))
        }
        FailureReason::ZeroMeasurementWindow { phase } => {
            format!("{} measurement window duration was zero", phase_text(*phase))
        }
        FailureReason::TimeBudgetExceeded {
            budget_secs,
            read,
            expected,
        } => format!(
            "time budget of {budget_secs} s exceeded: read {read} of {expected} expected records"
        ),
        FailureReason::IntegrityCountMismatch { read, expected } => {
            format!("integrity count mismatch: read {read} records, expected {expected}")
        }
        FailureReason::IntegrityPayloadMismatch { position } => {
            format!("integrity payload mismatch at record position {position}")
        }
        FailureReason::FloorBreachProduce {
            measured_rps,
            floor_rps,
        } => format!(
            "produce throughput {measured_rps} records/s is below the floor of {floor_rps} records/s"
        ),
        FailureReason::FloorBreachConsume {
            measured_rps,
            floor_rps,
        } => format!(
            "consume throughput {measured_rps} records/s is below the floor of {floor_rps} records/s"
        ),
    }
}

/// The lowercase human-readable name of a [`Phase`].
fn phase_text(phase: Phase) -> &'static str {
    match phase {
        Phase::Produce => "produce",
        Phase::Consume => "consume",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::WorkloadParameters;

    fn passed_report() -> BenchmarkReport {
        BenchmarkReport {
            params: WorkloadParameters::default(),
            outcome: Outcome::Passed,
            produce_throughput: Some(Throughput {
                records_per_sec: 500.0,
                bytes_per_sec: 128_000.0,
            }),
            consume_throughput: Some(Throughput {
                records_per_sec: 750.0,
                bytes_per_sec: 192_000.0,
            }),
            acknowledged_records: 100_000,
            total_payload_bytes: 25_600_000,
            total_elapsed: Duration::from_millis(1_500),
            failure_reason: None,
        }
    }

    fn failed_report() -> BenchmarkReport {
        let reason = FailureReason::ProduceError {
            topic: "vela-bench".to_string(),
            partition: 2,
            cause: "not leader".to_string(),
        };
        BenchmarkReport {
            params: WorkloadParameters::default(),
            outcome: Outcome::Failed {
                reason: reason.clone(),
            },
            // A failing Producer_Phase leaves both throughput figures absent.
            produce_throughput: None,
            consume_throughput: None,
            acknowledged_records: 0,
            total_payload_bytes: 0,
            total_elapsed: Duration::from_millis(42),
            failure_reason: Some(reason),
        }
    }

    fn summary_of(report: &BenchmarkReport) -> String {
        let mut buf = Vec::new();
        report.write_summary(&mut buf).expect("write summary");
        String::from_utf8(buf).expect("utf8 summary")
    }

    #[test]
    fn passed_report_round_trips_through_json() {
        let report = passed_report();
        let json = report.to_json().expect("serialize");
        let back: BenchmarkReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(report, back);
    }

    #[test]
    fn failed_report_round_trips_through_json() {
        let report = failed_report();
        let json = report.to_json().expect("serialize");
        let back: BenchmarkReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(report, back);
    }

    #[test]
    fn absent_throughput_serializes_as_null_never_zero() {
        let report = failed_report();
        let value: serde_json::Value =
            serde_json::from_str(&report.to_json().unwrap()).expect("json value");
        assert!(value["produce_throughput"].is_null());
        assert!(value["consume_throughput"].is_null());
    }

    #[test]
    fn summary_shows_both_throughput_figures_when_measured() {
        let summary = summary_of(&passed_report());
        assert!(summary.contains("Produce throughput: 500.00 records/s, 128000.00 bytes/s"));
        assert!(summary.contains("Consume throughput: 750.00 records/s, 192000.00 bytes/s"));
        assert!(summary.contains("PASSED"));
        // A passing run prints no failure reason.
        assert!(!summary.contains("Failure reason:"));
    }

    #[test]
    fn summary_shows_not_measured_for_absent_figures() {
        let summary = summary_of(&failed_report());
        assert!(summary.contains("Produce throughput: not measured"));
        assert!(summary.contains("Consume throughput: not measured"));
    }

    #[test]
    fn summary_prints_failure_reason_on_failing_outcome() {
        let summary = summary_of(&failed_report());
        assert!(summary.contains("FAILED"));
        assert!(summary.contains(
            "Failure reason: produce error on topic `vela-bench` partition 2: not leader"
        ));
    }

    #[test]
    fn write_json_appends_trailing_newline() {
        let report = passed_report();
        let mut buf = Vec::new();
        report.write_json(&mut buf).expect("write json");
        let text = String::from_utf8(buf).expect("utf8");
        assert!(text.ends_with('\n'));
        let back: BenchmarkReport = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(report, back);
    }

    #[test]
    fn write_json_file_writes_round_trippable_report() {
        let report = passed_report();
        let mut path = std::env::temp_dir();
        path.push(format!("vela-bench-report-{}.json", std::process::id()));
        report.write_json_file(&path).expect("write file");
        let text = std::fs::read_to_string(&path).expect("read file");
        let back: BenchmarkReport = serde_json::from_str(&text).expect("deserialize");
        assert_eq!(report, back);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn failure_reason_text_covers_every_variant() {
        let reasons = [
            FailureReason::TopicAlreadyExists {
                topic: "t".to_string(),
            },
            FailureReason::InvalidParameter {
                name: "record_count".to_string(),
                detail: "0".to_string(),
            },
            FailureReason::TopicCreationFailed {
                topic: "t".to_string(),
                cause: "x".to_string(),
            },
            FailureReason::ClusterNotReady { budget_secs: 60 },
            FailureReason::ProduceError {
                topic: "t".to_string(),
                partition: 1,
                cause: "x".to_string(),
            },
            FailureReason::ConsumeError {
                topic: "t".to_string(),
                partition: 1,
                cause: "x".to_string(),
            },
            FailureReason::WarmupFailed {
                phase: Phase::Produce,
                cause: "x".to_string(),
            },
            FailureReason::ZeroMeasurementWindow {
                phase: Phase::Consume,
            },
            FailureReason::TimeBudgetExceeded {
                budget_secs: 60,
                read: 1,
                expected: 2,
            },
            FailureReason::IntegrityCountMismatch {
                read: 1,
                expected: 2,
            },
            FailureReason::IntegrityPayloadMismatch { position: 3 },
            FailureReason::FloorBreachProduce {
                measured_rps: 1.0,
                floor_rps: 2.0,
            },
            FailureReason::FloorBreachConsume {
                measured_rps: 1.0,
                floor_rps: 2.0,
            },
        ];
        for reason in &reasons {
            assert!(!failure_reason_text(reason).is_empty());
        }
    }
}
