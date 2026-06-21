//! The self-contained HTML_Report rendering.
//!
//! Every Benchmark_Run renders an HTML_Report in addition to the
//! machine-readable Benchmark_Report and the human-readable stdout summary
//! (Requirement 6.6). [`render_html`] turns a [`BenchmarkReport`] into a single
//! browser-viewable HTML document that a developer can open directly: it is
//! **self-contained**, carrying only an inline `<style>` block and referencing
//! no external assets (no `<link href>`, no `<script src>`, no CDN URLs), so it
//! renders identically offline and as a CI artifact.
//!
//! The document presents every required figure (Requirement 6.6): the
//! Workload_Parameters, the Outcome, the Produce_Throughput and
//! Consume_Throughput each in records/s and bytes/s, the Acknowledged_Record
//! count, the total payload bytes, and the total elapsed wall-clock time. A
//! phase that did not complete renders its throughput as the explicit
//! [`report::NOT_MEASURED`] indication rather than a misleading `0`
//! (Requirement 6.8), and a failing Outcome renders its failure reason
//! (Requirement 6.7).
//!
//! The failure reason and the "not measured" indication are sourced from
//! [`report::failure_reason_text`] and [`report::NOT_MEASURED`] so the
//! HTML_Report and the stdout summary present identical text (Property 11).
//! Every dynamic value substituted into the document — the topic name, the
//! failure reason text, and so on — is HTML-escaped via [`escape_html`].

use std::io::{self, Write};
use std::path::Path;

use crate::metrics::Throughput;
use crate::params::{KeyMode, WorkloadParameters};
use crate::report::{self, BenchmarkReport, NOT_MEASURED};

/// HTML-escape a substituted value: replace `&`, `<`, `>`, `"`, and `'` with
/// their character references.
///
/// Applied to every dynamic value rendered into the HTML_Report (topic name,
/// failure reason text, etc.) so attacker- or workload-controlled content
/// cannot inject markup and the document stays well-formed (Requirement 6.6,
/// 6.7).
pub fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// Render a [`BenchmarkReport`] as a self-contained HTML_Report document
/// (Requirement 6.6, 6.7, 6.8).
///
/// The returned string is a complete HTML document with an inline `<style>`
/// block and no external asset references. It renders the Workload_Parameters,
/// the Outcome, both throughput figures (each in records/s and bytes/s, or the
/// literal `not measured` for a phase that did not complete), the
/// Acknowledged_Record count, the total payload bytes, the total elapsed time,
/// and — on a failing Outcome — the failure reason.
pub fn render_html(report: &BenchmarkReport) -> String {
    let passed = report.outcome.is_passed();
    let status = if passed { "PASSED" } else { "FAILED" };
    let status_class = if passed { "passed" } else { "failed" };

    let mut html = String::with_capacity(4096);
    html.push_str("<!DOCTYPE html>\n");
    html.push_str("<html lang=\"en\">\n<head>\n");
    html.push_str("<meta charset=\"utf-8\">\n");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    html.push_str("<title>Vela Throughput Benchmark Report</title>\n");
    html.push_str(STYLE);
    html.push_str("</head>\n<body>\n");
    html.push_str("<main>\n");
    html.push_str("<h1>Vela Throughput Benchmark Report</h1>\n");

    // Outcome.
    html.push_str(&format!(
        "<p class=\"outcome\">Outcome: <span class=\"badge {status_class}\">{status}</span></p>\n"
    ));

    // Failure reason on a failing Outcome (Requirement 6.7).
    if let Some(reason) = report_failure_reason(report) {
        let text = escape_html(&report::failure_reason_text(reason));
        html.push_str(&format!(
            "<p class=\"failure\"><strong>Failure reason:</strong> {text}</p>\n"
        ));
    }

    // Workload_Parameters (Requirement 6.6).
    html.push_str("<h2>Workload Parameters</h2>\n");
    html.push_str("<table>\n<tbody>\n");
    render_params_rows(&mut html, &report.params);
    html.push_str("</tbody>\n</table>\n");

    // Throughput figures (Requirement 6.6, 6.8).
    html.push_str("<h2>Throughput</h2>\n");
    html.push_str("<table>\n");
    html.push_str(
        "<thead>\n<tr><th>Phase</th><th>Records / s</th><th>Bytes / s</th></tr>\n</thead>\n",
    );
    html.push_str("<tbody>\n");
    render_throughput_row(&mut html, "Produce", report.produce_throughput);
    render_throughput_row(&mut html, "Consume", report.consume_throughput);
    html.push_str("</tbody>\n</table>\n");

    // Totals (Requirement 6.6).
    html.push_str("<h2>Totals</h2>\n");
    html.push_str("<table>\n<tbody>\n");
    row(
        &mut html,
        "Acknowledged records",
        &report.acknowledged_records.to_string(),
    );
    row(
        &mut html,
        "Total payload bytes",
        &report.total_payload_bytes.to_string(),
    );
    row(
        &mut html,
        "Total elapsed",
        &format!("{:.3} s", report.total_elapsed.as_secs_f64()),
    );
    html.push_str("</tbody>\n</table>\n");

    html.push_str("</main>\n</body>\n</html>\n");
    html
}

/// Write the rendered HTML_Report to the file at `path` (the CI artifact).
pub fn write_html_file(report: &BenchmarkReport, path: impl AsRef<Path>) -> io::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);
    writer.write_all(render_html(report).as_bytes())?;
    writer.flush()
}

/// The inline stylesheet. Kept inline (no external CSS) so the document is
/// self-contained (Requirement 6.6).
const STYLE: &str = "<style>\n\
body { font-family: system-ui, -apple-system, sans-serif; margin: 2rem; color: #1a1a1a; background: #fff; }\n\
main { max-width: 48rem; margin: 0 auto; }\n\
h1 { font-size: 1.5rem; }\n\
h2 { font-size: 1.15rem; margin-top: 1.75rem; border-bottom: 1px solid #ddd; padding-bottom: 0.25rem; }\n\
table { border-collapse: collapse; width: 100%; margin-top: 0.5rem; }\n\
th, td { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #eee; }\n\
th { background: #f5f5f5; }\n\
.badge { padding: 0.15rem 0.6rem; border-radius: 0.25rem; font-weight: 700; color: #fff; }\n\
.badge.passed { background: #1a7f37; }\n\
.badge.failed { background: #b42318; }\n\
.failure { background: #fdecea; border-left: 4px solid #b42318; padding: 0.6rem 0.8rem; }\n\
.not-measured { color: #888; font-style: italic; }\n\
</style>\n";

/// The failure reason to surface, preferring the named report field and falling
/// back to the one carried by the Outcome (mirrors the stdout summary so the two
/// renderings stay consistent).
fn report_failure_reason(report: &BenchmarkReport) -> Option<&crate::outcome::FailureReason> {
    report
        .failure_reason
        .as_ref()
        .or_else(|| report.outcome.failure_reason())
}

/// Render the Workload_Parameters as table rows (Requirement 6.6).
fn render_params_rows(html: &mut String, params: &WorkloadParameters) {
    row(html, "Topic", &escape_html(&params.topic));
    row(html, "Record count", &params.record_count.to_string());
    row(html, "Value size (bytes)", &params.value_size.to_string());
    row(html, "Key mode", key_mode_text(params.key_mode));
    row(html, "Partition count", &params.partition_count.to_string());
    row(
        html,
        "Producer concurrency",
        &params.producer_concurrency.to_string(),
    );
    row(html, "Warmup operations", &params.warmup.to_string());
    row(
        html,
        "Time budget",
        &format!("{} s", params.time_budget.as_secs()),
    );
    row(
        html,
        "Startup budget",
        &format!("{} s", params.startup_budget.as_secs()),
    );
    row(
        html,
        "Produce floor (records/s)",
        &floor_text(params.floor_produce_rps),
    );
    row(
        html,
        "Consume floor (records/s)",
        &floor_text(params.floor_consume_rps),
    );
}

/// Render one throughput row: both rates when measured, or an explicit
/// `not measured` indication when the figure is absent (Requirement 6.8).
fn render_throughput_row(html: &mut String, phase: &str, throughput: Option<Throughput>) {
    match throughput {
        Some(t) => html.push_str(&format!(
            "<tr><td>{phase}</td><td>{:.2}</td><td>{:.2}</td></tr>\n",
            t.records_per_sec, t.bytes_per_sec
        )),
        None => html.push_str(&format!(
            "<tr><td>{phase}</td><td class=\"not-measured\" colspan=\"2\">{NOT_MEASURED}</td></tr>\n"
        )),
    }
}

/// Append a two-column label/value table row. `value` must already be escaped
/// when it carries dynamic content.
fn row(html: &mut String, label: &str, value: &str) {
    html.push_str(&format!("<tr><th>{label}</th><td>{value}</td></tr>\n"));
}

/// The human-readable name of a [`KeyMode`].
fn key_mode_text(mode: KeyMode) -> &'static str {
    match mode {
        KeyMode::Keyed => "Keyed",
        KeyMode::Keyless => "Keyless",
    }
}

/// Render an optional throughput floor, or the explicit "not configured"
/// indication when no floor was supplied.
fn floor_text(floor: Option<f64>) -> String {
    match floor {
        Some(rps) => format!("{rps:.2}"),
        None => "not configured".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outcome::{FailureReason, Outcome};
    use crate::params::WorkloadParameters;
    use std::time::Duration;

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
            produce_throughput: None,
            consume_throughput: None,
            acknowledged_records: 0,
            total_payload_bytes: 0,
            total_elapsed: Duration::from_millis(42),
            failure_reason: Some(reason),
        }
    }

    /// Assert the document references no external assets (Requirement 6.6 /
    /// Property 11): no remote URLs and no external `<script>`/`<link>` tags.
    fn assert_self_contained(html: &str) {
        assert!(!html.contains("http://"), "must not reference http assets");
        assert!(
            !html.contains("https://"),
            "must not reference https assets"
        );
        assert!(!html.contains("<script"), "must not embed scripts");
        assert!(!html.contains("<link"), "must not link external assets");
        // The only styling is the inline <style> block.
        assert!(html.contains("<style>"), "expected an inline style block");
    }

    #[test]
    fn escapes_all_special_characters() {
        assert_eq!(escape_html("<&\">'"), "&lt;&amp;&quot;&gt;&#39;");
    }

    #[test]
    fn escape_leaves_plain_text_unchanged() {
        assert_eq!(escape_html("vela-bench"), "vela-bench");
    }

    #[test]
    fn passing_report_renders_both_throughput_figures_and_is_self_contained() {
        let html = render_html(&passed_report());
        assert_self_contained(&html);
        // Outcome.
        assert!(html.contains("PASSED"));
        // Produce throughput: records/s and bytes/s.
        assert!(html.contains("500.00"));
        assert!(html.contains("128000.00"));
        // Consume throughput: records/s and bytes/s.
        assert!(html.contains("750.00"));
        assert!(html.contains("192000.00"));
        // Totals.
        assert!(html.contains("100000"));
        assert!(html.contains("25600000"));
        assert!(html.contains("1.500 s"));
        // A passing run renders no failure reason.
        assert!(!html.contains("Failure reason"));
        // The "not measured" indication does not appear when both phases measured.
        assert!(!html.contains(NOT_MEASURED));
    }

    #[test]
    fn renders_every_workload_parameter() {
        let html = render_html(&passed_report());
        for label in [
            "Topic",
            "Record count",
            "Value size (bytes)",
            "Key mode",
            "Partition count",
            "Producer concurrency",
            "Warmup operations",
            "Time budget",
            "Startup budget",
            "Produce floor (records/s)",
            "Consume floor (records/s)",
        ] {
            assert!(html.contains(label), "missing parameter row: {label}");
        }
    }

    #[test]
    fn failing_report_renders_escaped_failure_reason_and_not_measured() {
        let html = render_html(&failed_report());
        assert_self_contained(&html);
        assert!(html.contains("FAILED"));
        // Failure reason text matches the shared stdout rendering.
        assert!(html.contains("produce error on topic `vela-bench` partition 2: not leader"));
        // Both absent phases render the explicit "not measured" indication.
        assert_eq!(html.matches(NOT_MEASURED).count(), 2);
    }

    #[test]
    fn escapes_topic_with_special_characters() {
        let mut report = passed_report();
        report.params.topic = "<&\">".to_string();
        let html = render_html(&report);
        // The raw special characters never appear unescaped in the topic cell.
        assert!(html.contains("&lt;&amp;&quot;&gt;"));
        assert!(!html.contains("<td><&"));
        // Still self-contained after substituting markup-like content.
        assert_self_contained(&html);
    }

    #[test]
    fn escapes_failure_reason_with_special_characters() {
        let reason = FailureReason::TopicCreationFailed {
            topic: "t<script>".to_string(),
            cause: "boom & <crash>".to_string(),
        };
        let mut report = failed_report();
        report.outcome = Outcome::Failed {
            reason: reason.clone(),
        };
        report.failure_reason = Some(reason);
        let html = render_html(&report);
        // The injected markup is escaped, keeping the document script-free.
        assert_self_contained(&html);
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("boom &amp; &lt;crash&gt;"));
    }

    #[test]
    fn write_html_file_writes_rendered_document() {
        let report = passed_report();
        let mut path = std::env::temp_dir();
        path.push(format!("vela-bench-report-{}.html", std::process::id()));
        write_html_file(&report, &path).expect("write html file");
        let text = std::fs::read_to_string(&path).expect("read file");
        assert_eq!(text, render_html(&report));
        let _ = std::fs::remove_file(&path);
    }
}
