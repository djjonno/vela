#![cfg(feature = "sim")]
//! Per-run event-budget environment-override test for `vela-sim` (task 23.2).
//!
//! Feature: deterministic-simulation-testing — the `VELA_DST_MAX_EVENTS`
//! environment variable bounds the number of processed [`Event`]s for a run,
//! overriding the configured [`Budget::max_events`] when it is set and parses
//! as a `u64` (Requirement 14.5).
//!
//! [`SimRuntime::run`] reads `VELA_DST_MAX_EVENTS` once at run start (its
//! "step 3" budget override) and, when it parses, uses it in place of
//! `params.budget.max_events` for that run; the scheduler then stops handing
//! out events once that bound is reached. The CI `dst` job sets the variable so
//! a run's length is capped without touching code. This file pins both halves
//! of the contract:
//!
//! - **Unset → params govern.** With the variable removed, the run is bounded by
//!   `params.budget.max_events`.
//! - **Set → the override wins.** With the variable set to a *tiny* value, the
//!   run is bounded by that value even when `params.budget.max_events` is far
//!   larger — the override replaces the configured budget rather than tightening
//!   or being tightened by it.
//!
//! # Why this is the *only* test in this file
//!
//! Environment variables are process-global, and the integration tests in a
//! single test binary run in parallel threads that share that process
//! environment. To keep the override deterministic and free of cross-test
//! interference, all env-mutating logic lives in exactly **one** `#[test]` here:
//! it captures the variable's prior value, performs both runs, and restores the
//! variable before returning. (Each `tests/*.rs` file is a *separate* binary /
//! process, so the mutation here cannot leak into any other test file.)
//!
//! # Why the processed-event count is observable here
//!
//! [`SimRuntime::run`] returns only the run's [`Outcome`], which does not expose
//! how many events the run processed. [`SimRuntime::run_observed`] is the
//! behavior-preserving observation wrapper over the same orchestration: it runs
//! the identical pipeline (including reading `VELA_DST_MAX_EVENTS` at run start)
//! and additionally returns the scheduler's processed-event count, so this test
//! can assert the bound directly rather than inferring it from a coarse
//! `Outcome`.
//!
//! Validates: Requirements 14.5
//!
//! [`Event`]: vela_sim::scheduler::Event
//! [`Budget`]: vela_sim::scenario::Budget

use vela_sim::runtime::{Outcome, SimRuntime};
use vela_sim::scenario::{Budget, RunConfig, ScenarioParameters};

/// The environment variable the CI `dst` job sets to bound a run's length.
const MAX_EVENTS_ENV: &str = "VELA_DST_MAX_EVENTS";

/// A small per-run event budget that binds exactly for a healthy default
/// cluster: large enough that the run is a genuine, performable run, small
/// enough that the event budget (not the 60 s virtual-time budget, and not
/// quiescence — a default cluster's election / heartbeat timers re-arm forever)
/// is what ends the run. Used as the configured `params.budget.max_events` for
/// the unset case.
const PARAMS_BUDGET: u64 = 3_000;

/// A *tiny* `VELA_DST_MAX_EVENTS` value, far below both [`PARAMS_BUDGET`] and the
/// default budget ([`200_000`]), so when it binds it is unmistakably the
/// override — not the configured budget — that did so. A handful of events is
/// nowhere near enough to elect a leader and commit the workload, but the run
/// still returns a value (no property can be violated in so few events).
const ENV_BUDGET: u64 = 64;

/// A [`RunConfig`] for a healthy default cluster with `params.budget.max_events`
/// set to `max_events`; every other field resolves to its documented default.
fn config(max_events: u64) -> RunConfig {
    RunConfig {
        seed: 0xB0D,
        params: ScenarioParameters {
            budget: Budget {
                max_events,
                ..Budget::default()
            },
            ..ScenarioParameters::default()
        },
    }
}

/// `VELA_DST_MAX_EVENTS` overrides the configured per-run event budget: when
/// unset, `params.budget.max_events` governs; when set to a tiny value, that
/// value bounds the run even against a far larger configured budget
/// (Requirement 14.5).
///
/// All environment mutation is confined to this single test (see the file-level
/// note) and the variable's prior value is restored before returning.
#[test]
fn env_override_overrides_configured_event_budget() {
    // Capture the prior value so the process environment is left exactly as it
    // was found, regardless of what (if anything) set the variable before us.
    let prior = std::env::var_os(MAX_EVENTS_ENV);

    // --- 1. Unset: the configured `params.budget.max_events` governs. ---------
    std::env::remove_var(MAX_EVENTS_ENV);
    let (unset_outcome, unset_events) = SimRuntime::run_observed(config(PARAMS_BUDGET));

    // --- 2. Set tiny override against the *large* default budget: it wins. ----
    // `config(...)` here uses the default 200_000-event budget, so the only way
    // the run can stop at ENV_BUDGET events is if the override replaced it.
    std::env::set_var(MAX_EVENTS_ENV, ENV_BUDGET.to_string());
    let (set_outcome, set_events) = SimRuntime::run_observed(config(Budget::default().max_events));

    // Restore the environment before asserting, so a failed assertion cannot
    // leave the variable set for any later code in this process.
    match prior {
        Some(value) => std::env::set_var(MAX_EVENTS_ENV, value),
        None => std::env::remove_var(MAX_EVENTS_ENV),
    }

    // Both runs must have been performable (a valid default shape that builds),
    // so each event count reflects a genuine run the budget cut short.
    assert!(
        !matches!(unset_outcome, Outcome::Invalid { .. }),
        "the unset run must be performable, got {unset_outcome:?}"
    );
    assert!(
        !matches!(set_outcome, Outcome::Invalid { .. }),
        "the overridden run must be performable, got {set_outcome:?}"
    );

    // Unset: the configured budget binds exactly. A healthy default cluster keeps
    // generating events and does not reach the virtual-time budget this early, so
    // the run ends because it hit `params.budget.max_events`.
    assert_eq!(
        unset_events, PARAMS_BUDGET,
        "with VELA_DST_MAX_EVENTS unset, params.budget.max_events must govern the run"
    );

    // Set: the override bounds the run...
    assert!(
        set_events <= ENV_BUDGET,
        "VELA_DST_MAX_EVENTS={ENV_BUDGET} must bound processed events, but the run \
         processed {set_events}"
    );
    // ...and binds *exactly*, even though the configured budget (the default
    // 200_000) is far larger — proving the override replaced it. Were the
    // override ignored, this run would instead process up to the default budget.
    assert_eq!(
        set_events, ENV_BUDGET,
        "the tiny VELA_DST_MAX_EVENTS override must bind exactly, overriding the larger \
         configured params.budget.max_events"
    );

    // And the override genuinely changed the run length versus the configured
    // budget: the tiny override produced a strictly shorter run than the unset
    // run governed by `params.budget.max_events`.
    assert!(
        set_events < unset_events,
        "the override ({set_events} events) must shorten the run relative to the \
         configured budget ({unset_events} events)"
    );
}
