//! `vela-bench` — the Throughput_Benchmark binary.
//!
//! Thin entry point over the [`vela_bench`] library. It initializes tracing,
//! parses the CLI, drives exactly one Benchmark_Run on a multi-threaded tokio
//! runtime, and maps the run's [`Outcome`] to a process exit status: a passing
//! run exits `0`, a failing run exits non-zero so a failing Benchmark_Run marks
//! the CI_Workflow as failed (Requirement 7.3).
//!
//! All measurement, sequencing, and output emission live in the library
//! (`run::run` returns the single [`BenchmarkReport`]); this file only wires the
//! parsed arguments into the harness and converts the resulting Outcome into an
//! [`ExitCode`] via the library's pure [`exit_code`] mapping.

use std::process::ExitCode;

use clap::Parser;

use vela_bench::cli::Cli;
use vela_bench::outcome::exit_code;
use vela_bench::run::run;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    // Best-effort structured logging for the run's tracing diagnostics. The
    // shared initializer honours `RUST_LOG` (defaulting to `info`) and is
    // non-panicking, so a subscriber already being set is harmless.
    vela_server::init_tracing();

    let cli = Cli::parse();
    let params = cli.to_params();

    // `run` validates the parameters before any side effect, drives the full
    // Benchmark_Run, emits the JSON/stdout/HTML outputs, and returns the single
    // report. Range violations surface as a failing Outcome rather than a panic.
    let report = run(params, cli.report_json, cli.report_html).await;

    // Map the Outcome to the process exit status (Requirement 7.3).
    ExitCode::from(exit_code(&report.outcome))
}
