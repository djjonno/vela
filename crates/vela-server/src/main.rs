//! `velad` — the Vela node daemon binary.
//!
//! Startup sequence: initialize structured logging, parse and validate
//! configuration, then bind the gRPC listener and serve the `VelaClient` and
//! `VelaPeer` services. Invalid or missing configuration produces a structured
//! error log and a non-zero exit (Requirement 15.2); a successful bind emits a
//! readiness log (Requirement 15.1, 15.3). A listener bind failure (or other
//! fatal serve error) is logged and exits non-zero.

use std::process::ExitCode;

use vela_server::{init_tracing, load_config, serve, CliArgs};

#[tokio::main]
async fn main() -> ExitCode {
    use clap::Parser;

    init_tracing();

    // `clap` handles `--help`/`--version` and unknown-flag usage errors itself;
    // value validation (missing-required, malformed, out-of-range) is handled
    // by `load_config` so it surfaces as a structured log + non-zero exit.
    let args = CliArgs::parse();

    let config = match load_config(args) {
        Ok(config) => config,
        Err(_) => return ExitCode::FAILURE,
    };

    // Bind the listener and serve both gRPC services (Requirement 15.1). `serve`
    // emits the readiness log once the listener is bound and runs until the
    // process is terminated.
    match serve(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "velad terminated with an error");
            ExitCode::FAILURE
        }
    }
}
