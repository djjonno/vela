//! Reusable `proptest` [`Strategy`] helpers for generating DST run inputs.
//!
//! A [`Simulation_Run`] is a pure function of its `(seed, params)` (Requirement
//! 1.3), so the natural thing for a `proptest`-based property test to generate
//! is exactly that pair: a 64-bit seed and a structured, **always-valid**
//! [`ScenarioParameters`]. This module provides those generators once, in the
//! library, so every property test (`prop_dst_runs.rs`, `prop_reproducibility.rs`,
//! and any future run-level fuzz) shares the *same* input space instead of
//! copy-pasting a `prop_compose!` block into each test file.
//!
//! # Why these strategies shrink well (Requirement 2.4, 13.3)
//!
//! The generators are composed from sub-strategies with defined shrink targets
//! so that a `proptest` failure walks down toward a **minimal counterexample**
//! that still reproduces the same violated property:
//!
//! - [`scenario_params`] draws `node_count` first (`3..=5`) and then
//!   `replication_factor` *dependently* in `1..=node_count` (a
//!   [`prop_flat_map`](Strategy::prop_flat_map)). The replica set is therefore a
//!   valid shape **by construction** — a generated run never short-circuits to
//!   [`Outcome::Invalid`](crate::runtime::Outcome::Invalid) for an inconsistent
//!   `RF > node_count` — and shrinking reduces the cluster toward `3` nodes / RF
//!   `1` rather than wandering into an invalid shape.
//! - [`fault_intensities`] ranges every probability from `0.0` upward, so a
//!   failure shrinks each fault knob independently toward `0.0` — a milder, then
//!   fault-free cluster — while still being able to generate the non-zero crash
//!   / partition / drop / duplicate / reorder intensities a run must survive.
//! - [`budget`] keeps both bounds modest (a few thousand events / a handful of
//!   logical seconds) so 100+ runs finish quickly, and both shrink toward their
//!   lower end.
//!
//! # Regression persistence + replay (Requirement 2.2)
//!
//! Because the property tests draw their inputs from these strategies,
//! `proptest`'s automatic regression persistence
//! (`crates/vela-sim/proptest-regressions/<test>.txt`, written **only** when a
//! case fails) records the seed + the structured parameters of a failing run and
//! replays them first on every subsequent run. As the run is a pure function of
//! `(seed, params)`, a persisted case re-executes to the byte-identical failing
//! [`Outcome`](crate::runtime::Outcome) and the same violated property — the
//! `(seed, params)` pair *is* the replayable trace.
//!
//! [`Simulation_Run`]: crate

use proptest::prelude::*;

use crate::scenario::{Budget, FaultIntensities, RunConfig, ScenarioParameters};

/// The inclusive `node_count` range the run-level strategies explore.
///
/// The lower bound is `3` — the smallest cluster whose per-partition Raft group
/// tolerates the failure of a minority of its replicas (Requirement 15.2) — and
/// the upper bound is kept small so the broad seed / shape / fault space is
/// covered quickly. Shrinking targets the lower bound (`3`).
const NODE_COUNT_RANGE: std::ops::RangeInclusive<usize> = 3..=5;

/// The inclusive `partition_count` range. At least `1` (Requirement 15.5); kept
/// small so runs stay brisk. Shrinks toward `1`.
const PARTITION_COUNT_RANGE: std::ops::RangeInclusive<u32> = 1..=3;

/// The inclusive `workload_size` range. Small enough to keep each run fast while
/// still issuing several interleaved client operations. Shrinks toward `5`.
const WORKLOAD_SIZE_RANGE: std::ops::RangeInclusive<usize> = 5..=25;

/// A strategy for bounded per-fault-class [`FaultIntensities`].
///
/// Every probability ranges from `0.0` upward so the strategy can generate the
/// non-zero crash / partition / drop / duplicate / reorder intensities the run
/// must survive, yet shrinks each one toward `0.0` (a milder, ultimately
/// fault-free cluster) when minimizing a failure. The base one-way latency stays
/// small and bounded so deliveries are quick. Fields not drawn here resolve to
/// their [`FaultIntensities::default`] (no clock skew, no storage faults).
pub fn fault_intensities() -> impl Strategy<Value = FaultIntensities> {
    (
        250_000u64..=2_000_000,   // base_latency_nanos
        0.0f64..=0.40,            // crash_prob
        0.0f64..=0.40,            // partition_prob
        0.0f64..=0.10,            // drop_prob
        0.0f64..=0.05,            // duplicate_prob
        0.0f64..=0.10,            // reorder_prob
        1_000_000u64..=5_000_000, // max_reorder_nanos
    )
        .prop_map(
            |(
                base_latency_nanos,
                crash_prob,
                partition_prob,
                drop_prob,
                duplicate_prob,
                reorder_prob,
                max_reorder_nanos,
            )| {
                FaultIntensities {
                    base_latency_nanos,
                    crash_prob,
                    partition_prob,
                    drop_prob,
                    duplicate_prob,
                    reorder_prob,
                    max_reorder_nanos,
                    ..FaultIntensities::default()
                }
            },
        )
}

/// A strategy for a modest per-run [`Budget`].
///
/// Kept small (a few thousand events / a handful of logical seconds) so 100+
/// runs finish quickly, while still leaving room for several elections,
/// replication rounds, and the scheduled crash / restart / partition / heal
/// faults to play out. Both bounds shrink toward their lower end.
pub fn budget() -> impl Strategy<Value = Budget> {
    (
        4_000u64..=12_000,                 // max_events
        6_000_000_000u64..=12_000_000_000, // max_virtual_nanos
    )
        .prop_map(|(max_events, max_virtual_nanos)| Budget {
            max_events,
            max_virtual_nanos,
        })
}

/// A strategy for a structured, **always-valid** [`ScenarioParameters`].
///
/// `node_count` is drawn first and `replication_factor` is then drawn
/// *dependently* in `1..=node_count`, so the set is internally consistent
/// (`RF <= node_count`, `partition_count >= 1`) by construction — every value it
/// produces passes [`ScenarioParameters::validate`]. The [`FaultIntensities`]
/// and [`Budget`] come from their own composed sub-strategies
/// ([`fault_intensities`], [`budget`]).
pub fn scenario_params() -> impl Strategy<Value = ScenarioParameters> {
    NODE_COUNT_RANGE
        .prop_flat_map(|node_count| {
            (
                Just(node_count),
                1usize..=node_count, // replication_factor depends on node_count
                PARTITION_COUNT_RANGE,
                WORKLOAD_SIZE_RANGE,
                fault_intensities(),
                budget(),
            )
        })
        .prop_map(
            |(node_count, replication_factor, partition_count, workload_size, faults, budget)| {
                ScenarioParameters {
                    node_count,
                    replication_factor,
                    partition_count,
                    faults,
                    workload_size,
                    budget,
                }
            },
        )
}

/// A strategy for a 64-bit run seed: the full `u64` space.
///
/// Every random decision in a run is derived from this one seed, so sampling the
/// whole space exercises distinct timer jitter, fault schedules, and workloads.
pub fn seed() -> impl Strategy<Value = u64> {
    any::<u64>()
}

/// A strategy for a complete, always-valid [`RunConfig`]: a [`seed`] paired with
/// a [`scenario_params`] set.
///
/// This is the single generator a run-level property test needs — the
/// `(seed, params)` pair that fully determines a run's [`Outcome`](crate::runtime::Outcome)
/// and that `proptest` persists and replays on failure.
pub fn run_config() -> impl Strategy<Value = RunConfig> {
    (seed(), scenario_params()).prop_map(|(seed, params)| RunConfig { seed, params })
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest! {
        /// Every [`ScenarioParameters`] the shared strategy produces is a valid,
        /// internally-consistent set: it always passes `validate()`, so a
        /// generated run never short-circuits to `Outcome::Invalid` for a bad
        /// shape. This is what keeps the run-level fuzz exercising real runs and
        /// lets a failure shrink toward a smaller *valid* cluster.
        #[test]
        fn generated_scenario_params_are_always_valid(params in scenario_params()) {
            prop_assert!(
                params.validate().is_ok(),
                "strategy produced an invalid parameter set: {params:?}"
            );
            // The dependent draw guarantees a consistent replica-set shape.
            prop_assert!(params.replication_factor >= 1);
            prop_assert!(params.replication_factor <= params.node_count);
            prop_assert!(params.partition_count >= 1);
        }

        /// A generated [`RunConfig`] carries an always-valid parameter set, so
        /// `(seed, params)` is directly replayable.
        #[test]
        fn generated_run_config_carries_valid_params(config in run_config()) {
            prop_assert!(config.params.validate().is_ok());
        }
    }
}
