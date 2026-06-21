//! `vela-ctl` — the command-line control tool for administering a cluster.
//!
//! This crate exposes its command logic as a library so it can be exercised by
//! integration tests under `tests/` (a binary crate's modules are not reachable
//! from there). The `vela-ctl` binary ([`src/main.rs`]) is a thin entry point
//! that parses arguments and dispatches into [`cli`].
//!
//! The modules are:
//!
//! - [`cli`] — the [`clap`] argument model and the `run`/`report` dispatcher.
//! - [`produce`] — the Producer REPL (`run_repl`).
//! - [`consume`] — the continuous per-partition Consumer loop.
//! - [`seams`] — deterministic [`Clock`](seams::Clock)/[`LineSource`](seams::LineSource)/
//!   [`Signal`](seams::Signal) traits at the time/stdin/signal seams, so the
//!   long-running loops are testable on a paused virtual clock with scripted
//!   input and a triggerable interrupt.

pub mod cli;
pub mod consume;
pub mod produce;
pub mod seams;
