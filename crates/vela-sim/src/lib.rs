//! Deterministic Simulation Testing (DST) harness for Vela.
//!
//! `vela-sim` composes the production `vela-core` / `vela-raft` / `vela-log`
//! types into an in-process, single-threaded, discrete-event `SimRuntime`. It
//! drives them through the `Clock` / `Transport` / `LogStorage` seams with
//! seed-derived faults, records a client `History`, and asserts the correctness
//! properties enumerated in the design.
//!
//! Dependency edges point strictly inward (`vela-sim -> vela-core -> vela-raft
//! -> vela-log`); the harness never depends on `vela-server`, so no wall clock,
//! real network, or OS scheduler can leak into a run.
//!
//! This module tree is currently a skeleton; each submodule is implemented by a
//! later task in the DST implementation plan.
//!
//! The entire harness is sim-only: its modules compose the `sim` surfaces of
//! `vela-log` / `vela-core` (`vela_log::sim::*`, `vela_core::PartitionLog::sim`,
//! `SimWalClock`), which only exist under those crates' `sim` features. So the
//! whole module tree is gated behind this crate's `sim` feature. Without the
//! feature `vela-sim` is intentionally an empty crate, which lets
//! `cargo build --workspace` (no features) compile it cleanly; the DST suite is
//! run explicitly with `--features sim`.

#[cfg(feature = "sim")]
pub mod artifact;
#[cfg(feature = "sim")]
pub mod checker;
#[cfg(feature = "sim")]
pub mod clock;
#[cfg(feature = "sim")]
pub mod cluster;
#[cfg(feature = "sim")]
pub mod codec;
#[cfg(feature = "sim")]
pub mod history;
#[cfg(feature = "sim")]
pub mod network;
#[cfg(feature = "sim")]
pub mod rng;
#[cfg(feature = "sim")]
pub mod runtime;
#[cfg(feature = "sim")]
pub mod scenario;
#[cfg(feature = "sim")]
pub mod scheduler;
#[cfg(feature = "sim")]
pub mod storage;
#[cfg(feature = "sim")]
pub mod strategy;
#[cfg(feature = "sim")]
pub mod workload;
