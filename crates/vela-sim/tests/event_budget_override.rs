#![cfg(feature = "sim")]
//! Per-run event-budget environment-override test for `vela-sim` (task 23.2).
//!
//! Feature: deterministic-simulation-testing — the `VELA_DST_MAX_EVENTS`
//! environment variable bounds the number of processed [`Event`]s for a run.
//!
//! The CI `dst` job sets `VELA_DST_MAX_EVENTS` so a run's length is capped
//! without touching code (Requirement 14.5). [`SimRuntime::run`] reads the
//! override once at run start and, when it parses as a `u64`, uses it in place
//! of [`Budget::max_events`] for that run; the scheduler then stops handing out
//! events once that bound is reached. This test pins that contract: with the
//! override set to a small value, a run processes no more events than the bound.
//!
//! # Why this is the *only* test in this file
//!
//! Environment variables are process-global, and the integration tests in a
//! single test binary run in parallel threads that share that process
//! environment. To keep the override deterministic and free of cross-test
//! interference, this file contains exactly **one** test: it sets the variable,
//! performs the run, then removes the variable, and no other test in this binary
//! reads or depends on it. (Each `tests/*.rs` file is a *separate* binary /
//! process, so setting the variable here cannot leak into any other test file.)
//!
//! # Why the processed-event count is observable here
//!
//! [`SimRuntime::run`] returns only the run's [`Outcome`], which does not expose
//! how many events the run processed. [`SimRuntime::run_observed`] is the
//! behavior-preserving observation wrapper over the same orchestration: it runs
//! the identical pipeline (including reading `VELA_DST_MAX_EVENTS` at run start)
//! and additionally returns the scheduler's processed-event count, so this test
//! can assert the bound directly.
//!
//! Validates: Requirements 14.5
//!
//! [`Event`]: vela_sim::scheduler::Event
//! [`Budget`]: vela_sim::scenario::Budget

use vela_sim::runtime::{Outcome, SimRuntime};
use vela_sim::scenario::{RunConfig, ScenarioParameters};

/// The environment variable the CI `dst` job sets to bound a run's length.
const MAX_EVENTS_ENV: &str = "VELA_DST_MAX_EVENTS";

/// A small per-run event budget, far below the default ([`200_000`]) so the
/// override is unmistakably the binding constraint. A healthy default cluster
/// never goes quiescent (election / heartbeat timers continually re-arm) and a
/// few thousand events advance virtual time well under the 60 s virtual-time
/// budget, so this event budget — not quiescence or the virtual-time limit — is
/// what ends the run.
const SMALL_BUDGET: u64 = 2_000;

/// Setting `VELA_DST_MAX_EVENTS` to a small value bounds the number of events a
/// run processes to that value (Requirement 14.5).
///
/// The run uses the documented default parameters (a healthy three-node,
/// fully-replicated cluster), so it is a valid, performable run that keeps
/// producing events — its length is decided purely by the budget. With the
/// override set to [`SMALL_BUDGET`] the run must process **no more than** that
/// many events; and because such a cluster never quiesces and never approaches
/// the virtual-time budget this early, the event budget binds exactly, so the
/// processed count equals the bound. Were the override ignored, the run would
/// instead process up to the default `200_000`-event budget, far exceeding the
/// bound and failing the assertion.
#[test]
fn env_override_bounds_processed_events() {
    // Set the override, perform the run, then immediately remove the variable so
    // nothing else in this process observes it. (This is the only test in this
    // binary, so no parallel sibling can race on the shared environment.)
    std::env::set_var(MAX_EVENTS_ENV, SMALL_BUDGET.to_string());
    let (outcome, events_processed) = SimRuntime::run_observed(RunConfig {
        seed: 0xD57,
        params: ScenarioParameters::default(),
    });
    std::env::remove_var(MAX_EVENTS_ENV);

    // The run must have been performable — bounded, not short-circuited to
    // `Invalid` by a bad shape or a build failure — so the event count reflects
    // a genuine run that the budget cut short.
    assert!(
        !matches!(outcome, Outcome::Invalid { .. }),
        "a default-configured run must be performable, got {outcome:?}"
    );

    // The core contract: the override bounds the number of processed events.
    assert!(
        events_processed <= SMALL_BUDGET,
        "VELA_DST_MAX_EVENTS={SMALL_BUDGET} must bound processed events, but the run \
         processed {events_processed}"
    );

    // And it binds exactly: a healthy default cluster keeps generating events and
    // does not reach the virtual-time budget this early, so the run ends because
    // it hit the event budget — proving the override took effect (an ignored
    // override would have allowed the default 200_000-event budget instead).
    assert_eq!(
        events_processed, SMALL_BUDGET,
        "the small event budget must be the binding limit for a default run"
    );
}
