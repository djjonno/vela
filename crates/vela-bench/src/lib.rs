//! `vela-bench` — the Throughput_Benchmark for Vela.
//!
//! This crate measures Vela's end-to-end produce and consume throughput by
//! driving the public `vela-client` `Producer` and `Consumer` APIs against an
//! in-process Cluster_Under_Test, verifying that every produced record is read
//! back, and emitting a machine-readable Benchmark_Report (JSON), a
//! human-readable stdout summary, and a self-contained HTML_Report.
//!
//! Following the `vela-ctl` split, the benchmark logic lives in this library so
//! the pure-logic modules are reachable from the property-based tests under
//! `tests/`. The `vela-bench` binary ([`src/main.rs`]) is a thin entry point
//! that parses arguments, runs one Benchmark_Run, and maps the Outcome to a
//! process exit code.
//!
//! The modules are:
//!
//! - [`params`] — `WorkloadParameters`, defaults, and range validation.
//! - [`cli`] — the [`clap`] argument model mapped to `WorkloadParameters`.
//! - [`workload`] — deterministic payload generation and key assignment.
//! - [`cluster`] — the `Cluster` seam and the in-process Cluster_Under_Test.
//! - [`produce_phase`] — the Producer_Phase (warmup, concurrency, window).
//! - [`consume_phase`] — the Consumer_Phase (per-partition reads, window).
//! - [`verify`] — count and per-position payload integrity verification.
//! - [`metrics`] — throughput arithmetic and Measurement_Window math.
//! - [`report`] — the `BenchmarkReport` model, JSON, and stdout summary.
//! - [`html`] — the self-contained HTML_Report rendering.
//! - [`outcome`] — Outcome determination (errors, integrity, budget, floor).
//! - [`run`] — the harness that sequences a Benchmark_Run.

pub mod cli;
pub mod cluster;
pub mod consume_phase;
pub mod html;
pub mod metrics;
pub mod outcome;
pub mod params;
pub mod produce_phase;
pub mod report;
pub mod run;
pub mod verify;
pub mod workload;

use thiserror::Error;

/// The library error type for the Throughput_Benchmark.
///
/// Recoverable, run-affecting failures are modeled as typed `FailureReason`
/// values on the Benchmark_Report (see [`outcome`]); `BenchError` carries the
/// lower-level error families surfaced while sequencing a Benchmark_Run. Per
/// the tech steering, library code uses `thiserror` for typed errors and
/// reserves `anyhow` for the binary entry point.
#[derive(Debug, Error)]
pub enum BenchError {
    /// A supplied Workload_Parameter was outside its accepted range.
    #[error("invalid workload parameter `{name}`: {detail}")]
    InvalidParameter {
        /// The offending parameter's name.
        name: String,
        /// A human-readable description of the violation.
        detail: String,
    },

    /// The in-process Cluster_Under_Test could not be started — reserving the
    /// ephemeral port or validating the single-node configuration failed before
    /// the server task was spawned.
    #[error("failed to start the in-process cluster: {detail}")]
    ClusterStartup {
        /// A human-readable description of the startup failure.
        detail: String,
    },

    /// The Cluster_Under_Test did not become ready within the startup budget.
    #[error("cluster did not become ready within {budget_secs}s startup budget")]
    ClusterNotReady {
        /// The configured startup time budget, in seconds.
        budget_secs: u64,
    },

    /// A produce or consume operation surfaced an error the client retry path
    /// did not resolve.
    #[error("{operation} operation failed on topic `{topic}` partition {partition}: {cause}")]
    Operation {
        /// The failed operation kind (e.g. "produce" or "consume").
        operation: String,
        /// The target topic.
        topic: String,
        /// The target partition.
        partition: u32,
        /// The underlying error cause.
        cause: String,
    },
}
