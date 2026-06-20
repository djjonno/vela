#![cfg(feature = "sim")]
//! Master run-level DST property test for `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 1 (run-level): a
//! generated run never reports a property violation
//!
//! This is the top-level fuzz over the *whole* harness: rather than focusing on
//! a single safety/liveness property (as the per-property tests in
//! `prop_election_safety.rs`, `prop_ack_durability.rs`, `prop_liveness.rs`, …
//! do), it generates a structured [`ScenarioParameters`] plus a 64-bit seed and
//! asserts that running the entire [`SimRuntime`] over that config never ends in
//! [`Outcome::Failed`] — i.e. *none* of the 22 checked properties is violated on
//! any generated `(seed, params)`. It complements, and does not replace, the
//! focused per-property tests: those pinpoint *which* property a regression
//! breaks; this one widens the search over the cluster-shape / fault-intensity /
//! seed space for *any* breach.
//!
//! # Shrinking toward a minimal counterexample (Req 2.4, 13.3)
//!
//! The inputs are generated through the shared, composed `proptest` strategies
//! in [`vela_sim::strategy`] rather than a raw tuple, so that a failing case
//! shrinks *meaningfully* toward a smaller cluster, milder faults, and a smaller
//! workload while still reproducing the same failure:
//!
//! - [`strategy::scenario_params`] draws `node_count` `3..=5` and
//!   `replication_factor` `1..=node_count` in a *dependent* second stage
//!   (`prop_flat_map`), so the replica set is always a valid shape and shrinking
//!   reduces the cluster (toward 3 nodes / RF 1) without ever producing an
//!   inconsistent `RF > node_count`.
//! - The `FaultIntensities` and `Budget` sub-strategies shrink each probability
//!   independently toward `0.0` (a milder, then fault-free cluster) and the
//!   workload / budget toward their lower bounds.
//!
//! Because every knob has a defined shrink target, `proptest` walks a found
//! failure down to a near-minimal scenario that still trips the same property.
//! Centralising these generators in the library (rather than copy-pasting a
//! `prop_compose!` block per test file) keeps the run-level fuzz here and the
//! reproducibility fuzz in `prop_reproducibility.rs` exploring the identical
//! input space.
//!
//! # Regression persistence + replay (Req 2.2, 2.4)
//!
//! `proptest` persists a failing case automatically under
//! `crates/vela-sim/proptest-regressions/prop_dst_runs.txt` (only failing runs
//! are persisted) and replays the persisted cases first on every subsequent run
//! of this test. Because [`SimRuntime::run`] is a *pure function of its
//! `(seed, params)`* — single-threaded, no wall clock, no real I/O, no threads —
//! a persisted case re-executes to the byte-identical failing [`Outcome`] and
//! the identical violated property, so the regression reproduces the original
//! failure exactly. (We do not commit a regressions file here: a clean run
//! leaves none, and one only appears the moment a real failure is found.)
//!
//! Validates: Requirements 2.2, 2.4, 13.3

use proptest::prelude::*;

use vela_sim::runtime::{Outcome, SimRuntime};
use vela_sim::scenario::RunConfig;
use vela_sim::strategy;

proptest! {
    // At least 100 cases (property-test requirement). 100 run-level fuzz
    // iterations cover a broad seed / cluster-shape / fault-intensity space
    // while the modest per-run Budget keeps the whole test brisk.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 1 (run-level): a generated run never reports a property violation
    #[test]
    fn a_generated_run_never_reports_a_property_violation(
        seed in any::<u64>(),
        params in strategy::scenario_params(),
    ) {
        // The whole harness over a generated config. `RunConfig` is `Copy`, so
        // `params` remains usable in the failure message below.
        let outcome = SimRuntime::run(RunConfig { seed, params });

        // A `Passed` or an `Invalid` are both acceptable: `Invalid` only arises
        // from an inconsistent shape, which the dependent `replication_factor`
        // strategy never produces. Only a `Failed` — a genuine Safety_/
        // Kafka-parity / Liveness_Property breach — fails this test. The message
        // carries the seed, the params, and the violated property/detail (via
        // the `Failed` `Debug`) so the persisted regression + shrink output is
        // immediately actionable.
        prop_assert!(
            !matches!(outcome, Outcome::Failed { .. }),
            "generated run reported a property violation: {outcome:?} \
             (seed={seed}, params={params:?})",
        );
    }
}
