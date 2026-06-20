#![cfg(feature = "sim")]
//! Property test for run reproducibility in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 1: Reproducibility (a run
//! is a pure function of its Seed)
//!
//! Property 1: *For any* 64-bit seed and any small, valid set of
//! [`ScenarioParameters`], a [`Simulation_Run`] is a pure function of its
//! `(seed, params)` — running the identical [`RunConfig`] twice in the same
//! process yields the identical [`Outcome`]. There is no wall clock, no real
//! I/O, and no threads in the run, so the only inputs are the seed and the
//! parameters; everything random in the run (election and heartbeat timers, the
//! network fault decisions, the seed-derived fault schedule, the workload, and
//! the scheduler's simultaneous-event tie-break) is derived from that one seed.
//!
//! The test deliberately samples configs whose generated [`FaultIntensities`]
//! include some non-zero crash and partition probabilities, so the deterministic
//! [`Fault_Schedule`] and the faulty code paths (crash/restart, partition/heal,
//! message drop/duplicate) are exercised — reproducibility must hold *under
//! faults*, not just on a healthy cluster. Each run is bounded by a small
//! [`Budget`] so the property covers a broad seed / shape / fault space quickly.
//!
//! The two calls receive an identical, `Copy` [`RunConfig`], and the asserted
//! witness is `Outcome` equality (the core reproducibility claim). This is the
//! property-test generalization of the `runtime.rs` unit tests
//! `run_is_deterministic_for_a_config` and `faulty_run_is_deterministic_and_passes`,
//! which fix single seeds; here the claim is quantified over the seed and the
//! parameter space.
//!
//! Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 2.2, 5.8, 7.5, 9.5

use proptest::prelude::*;

use vela_sim::runtime::{Outcome, SimRuntime};
use vela_sim::strategy;

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // crash/restart + partition run brisk while covering a broad seed / shape /
    // fault-intensity space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 1: Reproducibility (a run is a pure function of its Seed)
    #[test]
    fn run_is_a_pure_function_of_its_config(config in strategy::run_config()) {
        // The shared `strategy::run_config()` generator produces an always-valid
        // `(seed, params)` (its dependent `replication_factor` draw keeps
        // `RF <= node_count`), so a run never short-circuits to
        // `Outcome::Invalid` for a bad shape — which would make the equality
        // assertion trivial — and its generated `FaultIntensities` include
        // non-zero crash / partition / drop / duplicate intensities, so
        // reproducibility is asserted *under faults*. `RunConfig` is `Copy`, so
        // it stays usable in the failure message after both runs.

        // Run the identical config twice in-process.
        let first: Outcome = SimRuntime::run(config);
        let second: Outcome = SimRuntime::run(config);

        // The core reproducibility claim: same `(seed, params)` => same Outcome.
        prop_assert_eq!(
            first,
            second,
            "a run must be a pure function of its (seed, params): identical config \
             must yield the identical Outcome (seed={}, params={:?})",
            config.seed,
            config.params,
        );
    }
}
