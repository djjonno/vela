#![cfg(feature = "sim")]
//! Defaults coverage test for `vela-sim` (task 22.3).
//!
//! Feature: deterministic-simulation-testing — a run built from the documented
//! defaults (unspecified parameters) executes within budget.
//!
//! Where the per-preset integration tests exercise the deliberately
//! fault-heavy named scenarios, this test pins the *baseline*: a
//! [`RunConfig::default`] — seed `0` paired with [`ScenarioParameters::default`]
//! (a healthy three-node, fully-replicated cluster with no injected faults) —
//! runs to completion and [`Passed`](Outcome::Passed). It is the regression
//! guard for Requirement 15.4: every unspecified parameter resolves to its
//! documented default and the resulting run is a valid, self-consistent run that
//! the harness can carry from start to its [`Budget`] without ever short-
//! circuiting to [`Outcome::Invalid`] or tripping a checked property.
//!
//! # Why `Passed` is the "within budget" assertion
//!
//! [`SimRuntime::run`] reports only the run's [`Outcome`]. A healthy cluster
//! never goes quiescent (heartbeat / election timers keep re-arming), so a
//! default run naturally ends when it reaches the (generous, 60 s logical /
//! 200_000 event) default budget. The meaningful question Requirement 15.4 asks
//! is not *which* end-reason fires but whether the run *made progress within
//! that budget*: the harness's own [`LivenessChecker`] fails the run
//! ([`Outcome::Failed`]) if a favorable group does not elect a leader and commit
//! produced records / topic admin within its bounded budget, and the safety /
//! Kafka-parity checkers fail it on any consistency breach. A [`Passed`] outcome
//! therefore *is* the statement "the default run executed within budget and made
//! progress with no property violated" — it cannot be reached by a run that
//! merely exhausted its budget while stuck.
//!
//! Validates: Requirements 15.4
//!
//! [`LivenessChecker`]: vela_sim
//! [`Budget`]: vela_sim::scenario::Budget

use vela_sim::runtime::{Outcome, SimRuntime};
use vela_sim::scenario::{RunConfig, ScenarioParameters};

/// The documented default parameters are an internally-consistent set, so a run
/// built from them is *performable* — it can never short-circuit to
/// [`Outcome::Invalid`] for a bad shape (Requirement 15.4, 15.5).
///
/// A cheap, fast guard placed first so that, if a default constant is ever
/// mutated into an inconsistent shape (e.g. a replication factor above the node
/// count), this fails immediately with a clear cause rather than surfacing as an
/// `Invalid` outcome from the full run below.
#[test]
fn default_parameters_are_valid() {
    assert!(
        ScenarioParameters::default().validate().is_ok(),
        "the documented default parameters must form a valid, runnable set"
    );
}

/// A run built entirely from defaults executes within its budget and passes.
///
/// [`RunConfig::default`] is seed `0` plus [`ScenarioParameters::default`], with
/// every field left unspecified and therefore resolved to its documented default
/// (Requirement 15.4). The run must finish with [`Outcome::Passed`]: not
/// [`Invalid`](Outcome::Invalid) (the defaults are a valid shape) and not
/// [`Failed`](Outcome::Failed) (a healthy, fault-free cluster makes progress and
/// violates no checked property within the default budget).
#[test]
fn defaults_run_passes_within_budget() {
    let outcome = SimRuntime::run(RunConfig::default());

    assert_eq!(
        outcome,
        Outcome::Passed,
        "a default-configured run must execute within budget and pass; got {outcome:?}"
    );
}

/// Default parameters paired with a handful of distinct seeds each execute
/// within budget and pass.
///
/// The defaults fix the cluster shape, fault intensities (none), workload size,
/// and budget; only the seed varies. Exercising several seeds confirms the
/// defaults-built run is robustly within budget across the seed space, not just
/// for the single default seed `0` — every run a caller can obtain by leaving
/// the parameters unspecified and choosing any seed is a valid, passing run
/// (Requirement 15.4).
#[test]
fn defaults_run_passes_within_budget_across_seeds() {
    for seed in 0..4u64 {
        let outcome = SimRuntime::run(RunConfig {
            seed,
            params: ScenarioParameters::default(),
        });

        assert_eq!(
            outcome,
            Outcome::Passed,
            "default-configured run (seed={seed}) must execute within budget and pass; \
             got {outcome:?}"
        );
    }
}
