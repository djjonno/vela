//! `vela-ctl` — the command-line control tool for administering a cluster.
//!
//! Thin entry point: parse arguments ([`vela_ctl::cli::Cli`]), run the chosen
//! command against the cluster, and translate the outcome into a process exit
//! status (zero on success, non-zero on connection failure or cluster rejection
//! — Requirements 13.5–13.7). All command logic lives in the [`vela_ctl`]
//! library crate.

use std::process::ExitCode;

use vela_ctl::cli;

#[tokio::main]
async fn main() -> ExitCode {
    let args = cli::Cli::parse_args();
    cli::report(cli::run(args).await)
}
