#![cfg(feature = "sim")]
//! Regression replay + shrinking tests for `vela-sim`.
//!
//! Feature: deterministic-simulation-testing — Requirements 2.2, 2.4, 13.3.
//!
//! This is a dedicated integration suite for the two properties `proptest`'s
//! regression machinery relies on, demonstrated with *concrete* tests rather
//! than another run-level fuzz (that lives in `prop_dst_runs.rs` and
//! `prop_reproducibility.rs`):
//!
//! 1. **Replay determinism (Req 2.2, 13.3).** A fixed `(seed, params)`
//!    [`RunConfig`] re-executes to the byte-identical [`Outcome`] every time —
//!    same `Passed`/`Failed`, and on a `Failed` the same violated `property` and
//!    the same detection instant `at`. This is exactly the contract `proptest`'s
//!    [persisted regression seeds](#regression-persistence) depend on: a
//!    persisted failing seed must reproduce the original failing run.
//!
//! 2. **Shrinking (Req 2.4).** A `proptest` strategy mirroring the composed
//!    generators in [`vela_sim::strategy`] shrinks a *found* failure toward a
//!    minimal counterexample. Because the production harness is intended to be
//!    correct (it has no real run failure to shrink), the failures here are
//!    driven by **contrived predicates** that exist *only* to exercise the
//!    shrinking machinery — see [`contrived_node_count_floor`] and
//!    [`always_fails`]. They make no claim about `vela-sim` behavior.
//!
//! # Why replay works: a run is a pure function of `(seed, params)`
//!
//! [`SimRuntime::run`] is single-threaded with no wall clock, no real network,
//! no real filesystem, and no threads: every random decision (timer jitter, the
//! fault schedule, the workload, the simultaneous-event tie-break) is derived
//! from the run's 64-bit seed, and the scenario parameters are the only other
//! input. Two runs of an identical [`RunConfig`] in the same process therefore
//! produce the identical [`Outcome`] — the invariant these tests pin down for
//! several configs, including fault-injected ones (a persisted regression must
//! replay *under faults* too, not only on a healthy cluster).
//!
//! # <a name="regression-persistence"></a>Regression persistence (Req 2.4)
//!
//! `proptest` persists a failing case automatically (under
//! `crates/vela-sim/proptest-regressions/`, written **only** when a case fails)
//! and replays it first on every subsequent run of that test. Combined with the
//! purity above, a persisted failing seed re-executes to the same failing
//! `Outcome` and the same violated property. These tests deliberately leave **no**
//! regression file behind: the replay tests are plain `#[test]`s that never use
//! `proptest`, and the shrinking tests drive `proptest` through a
//! [`TestRunner`] configured with `failure_persistence: None`, so the contrived
//! failures they provoke are never written to disk.

use proptest::prelude::*;
use proptest::test_runner::{
    Config as ProptestConfig, RngAlgorithm, TestError, TestRng, TestRunner,
};

use vela_sim::runtime::{Outcome, SimRuntime};
use vela_sim::scenario::{Budget, FaultIntensities, RunConfig, ScenarioParameters};

// ---------------------------------------------------------------------------
// Part 1: Replay determinism (Req 2.2, 13.3)
// ---------------------------------------------------------------------------

/// A small per-run budget so each fixed-config run — and the several replays of
/// it — stay fast while still electing leaders, replicating, and playing out the
/// deterministic crash / restart / partition / heal schedule.
fn fast_budget() -> Budget {
    Budget {
        max_events: 8_000,
        max_virtual_nanos: 8_000_000_000,
    }
}

/// Build a valid [`ScenarioParameters`] with the given fault intensities and a
/// small, brisk budget / workload. The shape (3 nodes, RF 3, 2 partitions) keeps
/// a majority of every partition group running across a single crash so the
/// cluster keeps making progress.
fn params_with(faults: FaultIntensities) -> ScenarioParameters {
    let params = ScenarioParameters {
        node_count: 3,
        replication_factor: 3,
        partition_count: 2,
        faults,
        workload_size: 20,
        budget: fast_budget(),
    };
    // Guard the fixtures themselves: a run on an invalid shape would
    // short-circuit to `Outcome::Invalid` and make the replay assertion trivial.
    assert!(
        params.validate().is_ok(),
        "test fixture must be a valid scenario: {params:?}",
    );
    params
}

/// Re-execute `config` `1 + replays` times and assert every execution returns
/// the identical [`Outcome`]. On a `Failed` outcome the assertion additionally
/// pins the violated `property` and the detection instant `at` (the fields a
/// replayed regression must reproduce, Req 2.2, 2.3).
fn assert_replays_identically(label: &str, config: RunConfig, replays: usize) {
    let baseline = SimRuntime::run(config);

    for attempt in 1..=replays {
        let replay = SimRuntime::run(config);

        // Whole-`Outcome` equality is the core claim: `Outcome` derives `Eq`, so
        // this covers the `Passed`/`Failed` discriminant and, on a `Failed`,
        // every field including `property`, `at`, and `detail`.
        assert_eq!(
            baseline, replay,
            "{label}: replay #{attempt} diverged from the baseline outcome \
             (seed={}, params={:?})",
            config.seed, config.params,
        );

        // Spell out the regression-replay contract explicitly: a persisted
        // failing seed must re-execute to the *same violated property at the
        // same instant*, not merely to some other failure.
        if let (
            Outcome::Failed {
                property: base_prop,
                at: base_at,
                ..
            },
            Outcome::Failed {
                property: replay_prop,
                at: replay_at,
                ..
            },
        ) = (&baseline, &replay)
        {
            assert_eq!(
                base_prop, replay_prop,
                "{label}: replay #{attempt} violated a different property",
            );
            assert_eq!(
                base_at, replay_at,
                "{label}: replay #{attempt} detected the violation at a \
                 different instant",
            );
        }
    }
}

/// A fixed healthy-cluster config replays to the identical `Outcome` every time
/// (Req 2.2, 13.3). With no injected faults the only timing comes from the base
/// one-way latency and the seed-derived election jitter — both pure functions of
/// the seed — so repeated runs must agree.
#[test]
fn healthy_config_replays_to_identical_outcome() {
    for seed in [0u64, 1, 42, 9_999] {
        let config = RunConfig {
            seed,
            params: params_with(FaultIntensities::default()),
        };
        assert_replays_identically("healthy", config, 4);
    }
}

/// A fixed crash-injected config replays identically (Req 2.2, 13.3). The
/// deterministic schedule crashes a minority node and later restarts it,
/// recovering from the durable WAL; replay must reproduce the same recovery and
/// the same outcome, so a persisted regression replays under crash/restart too.
#[test]
fn crash_injected_config_replays_to_identical_outcome() {
    let faults = FaultIntensities {
        crash_prob: 0.3,
        ..FaultIntensities::default()
    };
    for seed in [3u64, 17, 2_024] {
        let config = RunConfig {
            seed,
            params: params_with(faults),
        };
        assert_replays_identically("crash", config, 4);
    }
}

/// A fixed partition-injected config replays identically (Req 2.2, 13.3). The
/// schedule severs the cluster and later heals it; the seed-derived per-message
/// drop / delivery decisions are pure in the seed, so the partitioned run and
/// its heal reproduce exactly across replays.
#[test]
fn partition_injected_config_replays_to_identical_outcome() {
    let faults = FaultIntensities {
        partition_prob: 0.4,
        ..FaultIntensities::default()
    };
    for seed in [5u64, 23, 7_777] {
        let config = RunConfig {
            seed,
            // Five nodes so a partition can still leave a committing majority.
            params: ScenarioParameters {
                node_count: 5,
                replication_factor: 5,
                ..params_with(faults)
            },
        };
        assert_replays_identically("partition", config, 4);
    }
}

/// A fixed config combining crash, partition, drop, and duplication faults
/// replays identically (Req 2.2, 13.3). This is the strongest replay case: even
/// with several fault classes active at once, the run remains a pure function of
/// `(seed, params)`, which is what lets `proptest` persist *any* failing seed and
/// reproduce it.
#[test]
fn combined_fault_config_replays_to_identical_outcome() {
    let faults = FaultIntensities {
        crash_prob: 0.3,
        partition_prob: 0.3,
        drop_prob: 0.05,
        duplicate_prob: 0.03,
        ..FaultIntensities::default()
    };
    for seed in [11u64, 101, 555_555] {
        let config = RunConfig {
            seed,
            params: ScenarioParameters {
                node_count: 5,
                replication_factor: 5,
                ..params_with(faults)
            },
        };
        assert_replays_identically("combined", config, 4);
    }
}

// ---------------------------------------------------------------------------
// Part 2: Shrinking toward a minimal scenario (Req 2.4)
// ---------------------------------------------------------------------------
//
// The contrived predicates below exist ONLY to exercise `proptest`'s shrinking
// machinery against the composed, range-based generators the DST suite uses;
// they assert nothing about `vela-sim`'s real behavior. Each shrinking test
// drives `proptest` through a `TestRunner` directly (rather than the `proptest!`
// macro) so that:
//   * a *deliberately failing* case can be provoked without failing the cargo
//     test (we inspect the shrunk counterexample and then return normally), and
//   * `failure_persistence: None` guarantees nothing is written to
//     `crates/vela-sim/proptest-regressions/`.
// A deterministic `TestRng` makes the shrink result fixed, so these never flake.

/// A deterministic `proptest` runner that never persists a failure.
///
/// `failure_persistence: None` is what keeps these contrived failures off disk;
/// the deterministic RNG makes the generated cases — and therefore the shrink
/// outcome — reproducible run to run.
fn non_persisting_runner() -> TestRunner {
    let config = ProptestConfig {
        // Disable regression persistence so the contrived failures provoked here
        // never write a `proptest-regressions/` file (Req 2.4 cleanliness).
        failure_persistence: None,
        // Plenty of cases to surely generate a value above the contrived floor.
        cases: 256,
        ..ProptestConfig::default()
    };
    TestRunner::new_with_rng(config, TestRng::deterministic_rng(RngAlgorithm::ChaCha))
}

/// A composed [`ScenarioParameters`] strategy in the style of
/// [`vela_sim::strategy::scenario_params`]: every field is drawn from a bounded
/// range and mapped into a valid parameter set, so each field has a defined
/// shrink target at its range's lower bound. `replication_factor` is clamped to
/// `node_count` to stay a valid shape.
fn composed_params() -> impl Strategy<Value = ScenarioParameters> {
    (
        3usize..=6,   // node_count   -> shrinks toward 3
        1usize..=5,   // replication  -> shrinks toward 1 (clamped <= node_count)
        1u32..=4,     // partitions   -> shrinks toward 1
        5usize..=40,  // workload     -> shrinks toward 5
        0.0f64..=0.5, // crash_prob   -> shrinks toward 0.0
        0.0f64..=0.5, // partition_prob -> shrinks toward 0.0
    )
        .prop_map(|(nc, rf, pc, wl, crash, partition)| ScenarioParameters {
            node_count: nc,
            replication_factor: rf.min(nc),
            partition_count: pc,
            faults: FaultIntensities {
                crash_prob: crash,
                partition_prob: partition,
                ..FaultIntensities::default()
            },
            workload_size: wl,
            budget: Budget::default(),
        })
}

/// **Contrived predicate** (Req 2.4): a found failure shrinks `node_count` toward
/// its lower bound.
///
/// The predicate "`node_count <= 3`" fails for any generated cluster larger than
/// three nodes (the generator draws `3..=6`). `proptest` finds such a failure and
/// shrinks it: the *minimal* `node_count` that still violates the predicate is
/// `4` (one above the bound), so the shrunk counterexample lands exactly there —
/// proving the strategy shrinks `node_count` toward its floor rather than
/// reporting whatever larger value was first generated.
#[test]
fn shrinking_drives_node_count_to_the_minimal_failing_value() {
    let mut runner = non_persisting_runner();

    let result = runner.run(&composed_params(), |params| {
        // Contrived: only here to give shrinking something to minimize.
        prop_assert!(
            params.node_count <= 3,
            "contrived failure: node_count {} exceeds the floor of 3",
            params.node_count,
        );
        Ok(())
    });

    let shrunk = match result {
        Err(TestError::Fail(_, params)) => params,
        other => panic!("expected a contrived failure to shrink, got {other:?}"),
    };

    // The minimal value that still violates `node_count <= 3` is 4.
    assert_eq!(
        shrunk.node_count, 4,
        "shrinking should minimize node_count to the smallest failing value (4), \
         got {shrunk:?}",
    );
}

/// **Contrived predicate** (Req 2.4): an always-failing case shrinks *every*
/// field of the composed scenario to its minimum.
///
/// With a predicate that fails for all inputs, `proptest` is free to minimize
/// every independently-generated knob, so the shrunk counterexample is the
/// smallest scenario the strategy can produce: the smallest cluster (`3` nodes,
/// replication `1`), the fewest partitions (`1`), the smallest workload (`5`),
/// and fault probabilities driven down toward `0.0`. This demonstrates that the
/// composed generators shrink a found failure toward a *minimal* reproducer
/// across all dimensions at once, not just one.
#[test]
fn shrinking_minimizes_every_field_of_a_found_failure() {
    let mut runner = non_persisting_runner();

    let result = runner.run(&composed_params(), |_params| {
        // Contrived: always fails, so shrinking minimizes every field.
        Err(proptest::test_runner::TestCaseError::fail(
            "contrived failure: minimize every scenario field",
        ))
    });

    let shrunk = match result {
        Err(TestError::Fail(_, params)) => params,
        other => panic!("expected a contrived failure to shrink, got {other:?}"),
    };

    // Integer / size fields shrink to the exact lower bound of their range.
    assert_eq!(
        shrunk.node_count, 3,
        "node_count should shrink to 3: {shrunk:?}"
    );
    assert_eq!(
        shrunk.replication_factor, 1,
        "replication_factor should shrink to 1: {shrunk:?}",
    );
    assert_eq!(
        shrunk.partition_count, 1,
        "partition_count should shrink to 1: {shrunk:?}",
    );
    assert_eq!(
        shrunk.workload_size, 5,
        "workload_size should shrink to 5: {shrunk:?}",
    );

    // Fault probabilities shrink toward 0.0 (a milder, ultimately fault-free
    // cluster). Asserted below their range midpoint rather than for exact 0.0 to
    // avoid a float-equality comparison while still proving the downward shrink.
    assert!(
        shrunk.faults.crash_prob < 0.25,
        "crash_prob should shrink toward 0.0: {shrunk:?}",
    );
    assert!(
        shrunk.faults.partition_prob < 0.25,
        "partition_prob should shrink toward 0.0: {shrunk:?}",
    );
}
