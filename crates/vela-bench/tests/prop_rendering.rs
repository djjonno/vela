// Feature: throughput-benchmark, Property 11: stdout and HTML renderings present every figure, the failure reason, and "not measured" for incomplete phases
//!
//! Property 11 ties the two human-facing renderings of a Benchmark_Run — the
//! stdout summary ([`BenchmarkReport::write_summary`]) and the self-contained
//! HTML_Report ([`vela_bench::html::render_html`]) — to a single completeness
//! contract. For *any* [`BenchmarkReport`], both renderings must:
//!
//! 1. present, for each phase (produce and consume), a records/s and a bytes/s
//!    figure when that phase produced a measured throughput, or the literal
//!    [`vela_bench::report::NOT_MEASURED`] (`not measured`) when it did not
//!    (Requirements 6.3, 6.8);
//! 2. contain the failure reason text on a `Failed` Outcome — verbatim in the
//!    stdout summary and HTML-escaped via [`vela_bench::html::escape_html`] in
//!    the HTML_Report (Requirements 6.4, 6.7); and
//! 3. (HTML only) be self-contained, referencing no external assets — no
//!    `http://`/`https://` URLs, no `<script>` and no `<link>` tags — while
//!    carrying its styling inline in a `<style>` block (Requirement 6.6).
//!
//! The generators build arbitrary reports across this space: each phase
//! throughput is independently `Some` (finite, non-NaN records/s and bytes/s)
//! or `None`; the Outcome is `Passed` or `Failed` carrying a generated
//! [`FailureReason`]; and the dynamic string fields (topic, names, causes) draw
//! from an alphabet that deliberately *includes* HTML metacharacters
//! (`< > & " '`) to exercise escaping but *excludes* `:` and `/` so generated
//! content can never itself spell a `http://`/`https://` URL and spuriously
//! trip the self-contained assertion. Figure substrings are checked by
//! formatting the expected value with the same `"{:.2}"` the renderers use and
//! asserting `.contains(..)`.
//!
//! Validates: Requirements 6.3, 6.4, 6.6, 6.7, 6.8

use std::time::Duration;

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use vela_bench::html::{escape_html, render_html};
use vela_bench::metrics::Throughput;
use vela_bench::outcome::{FailureReason, Outcome, Phase};
use vela_bench::params::{KeyMode, WorkloadParameters};
use vela_bench::report::{failure_reason_text, BenchmarkReport, NOT_MEASURED};

/// A short string drawn from an alphabet that includes the HTML
/// metacharacters (`< > & " '`) — so escaping is exercised — but excludes `:`
/// and `/`, so a generated value can never spell a `http://`/`https://` URL and
/// falsely trip the "self-contained" assertion (those metacharacters are always
/// escaped before reaching the HTML body).
fn safe_string() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9 _<>&'\"-]{1,24}").expect("valid regex")
}

/// A finite, non-negative, non-NaN throughput figure (records/s or bytes/s).
fn finite_f64() -> impl Strategy<Value = f64> {
    0.0f64..1_000_000.0
}

/// An optional phase throughput: either absent (the phase did not complete) or
/// a pair of finite figures.
fn throughput_opt() -> impl Strategy<Value = Option<Throughput>> {
    prop_oneof![
        Just(None),
        (finite_f64(), finite_f64()).prop_map(|(records_per_sec, bytes_per_sec)| Some(
            Throughput {
                records_per_sec,
                bytes_per_sec,
            }
        )),
    ]
}

/// Either phase.
fn phase_strategy() -> impl Strategy<Value = Phase> {
    prop_oneof![Just(Phase::Produce), Just(Phase::Consume)]
}

/// An arbitrary [`FailureReason`], one variant of each kind, with finite floats
/// and URL-free strings.
fn failure_reason_strategy() -> impl Strategy<Value = FailureReason> {
    prop_oneof![
        safe_string().prop_map(|topic| FailureReason::TopicAlreadyExists { topic }),
        (safe_string(), safe_string())
            .prop_map(|(name, detail)| FailureReason::InvalidParameter { name, detail }),
        (safe_string(), safe_string())
            .prop_map(|(topic, cause)| FailureReason::TopicCreationFailed { topic, cause }),
        (1u64..=600).prop_map(|budget_secs| FailureReason::ClusterNotReady { budget_secs }),
        (safe_string(), any::<u32>(), safe_string()).prop_map(|(topic, partition, cause)| {
            FailureReason::ProduceError {
                topic,
                partition,
                cause,
            }
        }),
        (safe_string(), any::<u32>(), safe_string()).prop_map(|(topic, partition, cause)| {
            FailureReason::ConsumeError {
                topic,
                partition,
                cause,
            }
        }),
        (phase_strategy(), safe_string())
            .prop_map(|(phase, cause)| FailureReason::WarmupFailed { phase, cause }),
        phase_strategy().prop_map(|phase| FailureReason::ZeroMeasurementWindow { phase }),
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

/// An Outcome paired with the report's `failure_reason` field. A `Failed`
/// Outcome carries the same reason in both places so the stdout summary and the
/// HTML_Report render identical failure text (both prefer the report field,
/// falling back to the Outcome's reason).
fn outcome_strategy() -> impl Strategy<Value = (Outcome, Option<FailureReason>)> {
    prop_oneof![
        Just((Outcome::Passed, None)),
        failure_reason_strategy().prop_map(|reason| (
            Outcome::Failed {
                reason: reason.clone()
            },
            Some(reason)
        )),
    ]
}

/// Arbitrary Workload_Parameters. Rendering does not validate, so only the
/// fields that influence the rendered output meaningfully are varied: the topic
/// (escaping), the key mode, and the optional floors. The rest use the
/// documented defaults.
fn params_strategy() -> impl Strategy<Value = WorkloadParameters> {
    (
        safe_string(),
        any::<bool>(),
        prop::option::of(finite_f64()),
        prop::option::of(finite_f64()),
    )
        .prop_map(
            |(topic, keyed, floor_produce_rps, floor_consume_rps)| WorkloadParameters {
                topic,
                key_mode: if keyed {
                    KeyMode::Keyed
                } else {
                    KeyMode::Keyless
                },
                floor_produce_rps,
                floor_consume_rps,
                ..WorkloadParameters::default()
            },
        )
}

/// An arbitrary [`BenchmarkReport`] spanning the rendering input space.
fn report_strategy() -> impl Strategy<Value = BenchmarkReport> {
    (
        params_strategy(),
        outcome_strategy(),
        throughput_opt(),
        throughput_opt(),
        any::<u64>(),
        any::<u64>(),
        0u64..10_000,
    )
        .prop_map(
            |(
                params,
                (outcome, failure_reason),
                produce_throughput,
                consume_throughput,
                acknowledged_records,
                total_payload_bytes,
                elapsed_ms,
            )| BenchmarkReport {
                params,
                outcome,
                produce_throughput,
                consume_throughput,
                acknowledged_records,
                total_payload_bytes,
                total_elapsed: Duration::from_millis(elapsed_ms),
                failure_reason,
            },
        )
}

/// Assert one phase's rendering completeness in both the stdout summary and the
/// HTML_Report: when measured, both contain the records/s and bytes/s figures
/// formatted as the renderers format them (`"{:.2}"`); when absent, both
/// contain the literal `not measured`.
fn check_phase(
    label: &str,
    stdout: &str,
    html: &str,
    throughput: Option<Throughput>,
) -> Result<(), TestCaseError> {
    match throughput {
        Some(t) => {
            let records = format!("{:.2}", t.records_per_sec);
            let bytes = format!("{:.2}", t.bytes_per_sec);
            prop_assert!(
                stdout.contains(records.as_str()),
                "stdout missing {label} records/s figure {records}"
            );
            prop_assert!(
                stdout.contains(bytes.as_str()),
                "stdout missing {label} bytes/s figure {bytes}"
            );
            prop_assert!(
                html.contains(records.as_str()),
                "html missing {label} records/s figure {records}"
            );
            prop_assert!(
                html.contains(bytes.as_str()),
                "html missing {label} bytes/s figure {bytes}"
            );
        }
        None => {
            prop_assert!(
                stdout.contains(NOT_MEASURED),
                "stdout missing `{NOT_MEASURED}` for {label}"
            );
            prop_assert!(
                html.contains(NOT_MEASURED),
                "html missing `{NOT_MEASURED}` for {label}"
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: throughput-benchmark, Property 11
    #[test]
    fn renderings_present_figures_failure_and_not_measured(report in report_strategy()) {
        // Render both human-facing outputs from the single report.
        let mut buf = Vec::new();
        report.write_summary(&mut buf).expect("write summary");
        let stdout = String::from_utf8(buf).expect("stdout summary is valid utf-8");
        let html = render_html(&report);

        // (3) The HTML_Report is self-contained: no external asset references,
        // styling carried inline (Requirement 6.6).
        prop_assert!(!html.contains("http://"), "html references an http asset");
        prop_assert!(!html.contains("https://"), "html references an https asset");
        prop_assert!(!html.contains("<script"), "html embeds a script");
        prop_assert!(!html.contains("<link"), "html links an external asset");
        prop_assert!(html.contains("<style>"), "html is missing its inline style block");

        // (1) Per-phase figures or the explicit `not measured` indication in
        // both renderings (Requirements 6.3, 6.8).
        check_phase("produce", &stdout, &html, report.produce_throughput)?;
        check_phase("consume", &stdout, &html, report.consume_throughput)?;

        // (2) The failure reason text on a `Failed` Outcome: verbatim in stdout,
        // HTML-escaped in the HTML_Report (Requirements 6.4, 6.7).
        if let Outcome::Failed { ref reason } = report.outcome {
            let text = failure_reason_text(reason);
            prop_assert!(
                stdout.contains(text.as_str()),
                "stdout missing failure reason text: {text}"
            );
            let escaped = escape_html(&text);
            prop_assert!(
                html.contains(escaped.as_str()),
                "html missing escaped failure reason text: {escaped}"
            );
        }
    }
}
