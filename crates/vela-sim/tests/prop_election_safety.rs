#![cfg(feature = "sim")]
//! Property test for Election Safety in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 11: Election Safety
//! (Raft §5.2)
//!
//! Property 11: *For any* seed and any small cluster shape (at least three
//! nodes, so a per-group election is a genuine multi-voter race), no two
//! distinct replicas of the same Raft group are ever leader in the same term at
//! any instant of the run — Election Safety, Raft §5.2 (Requirement 10.1).
//!
//! The test drives a real [`SimRuntime`] run end to end: it bootstraps the
//! cluster-wide `__meta/0` group's first election timers, then loops the
//! discrete-event dispatch to the event budget while injecting faults — a
//! per-seed mix of message reorder / duplication / drop on the
//! [`SimNetwork`](vela_sim::network), plus a deterministic crash-and-restart of
//! the elected leader that forces a fresh election in a higher term. After every
//! step it calls [`RaftSafetyChecker::observe`], which detects (never prevents)
//! a same-term double-leader condition. The run asserts the checker never
//! reports an [`PropertyId::ElectionSafety`] violation across the whole timeline.
//!
//! Duplicated and reordered vote / heartbeat messages are exactly the conditions
//! that could let two candidates each believe they won a term, and the
//! crash/restart manufactures multiple terms with real re-elections, so the
//! property is exercised against an adversarial-but-deterministic schedule
//! rather than only a quiescent happy path.
//!
//! Validates: Requirements 10.1

use proptest::prelude::*;

use vela_core::metadata_group_key;
use vela_raft::{Clock, TimerKind, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::{PropertyId, RaftSafetyChecker};
use vela_sim::cluster::SimulatedCluster;
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, FaultIntensities, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Step, VirtualInstant};

/// The per-run event budget. Heartbeats keep re-arming timers, so a healthy run
/// never goes quiescent; the budget is what bounds it. A few thousand events is
/// ample to drive several elections (including the crash-induced re-election)
/// while keeping 100+ cases fast.
const MAX_EVENTS: u64 = 6_000;

/// A bounded fraction of `0.0..=cap` derived from eight bits of `seed`. Used to
/// vary the network fault intensities per case without ever reaching `1.0`
/// (which — for drops — would wedge every election and make the run trivially
/// quiescent rather than an Election Safety stress).
fn fraction(seed: u64, shift: u32, cap: f64) -> f64 {
    let byte = ((seed >> shift) & 0xff) as f64 / 255.0;
    byte * cap
}

/// Seed-derived network fault intensities: bounded reorder, duplication, and
/// drop. Duplicated and reordered messages are the adversarial conditions for
/// Election Safety (a duplicated vote, a reordered heartbeat); the bounds keep
/// elections able to complete so the property is actually reached.
fn faults_for(seed: u64) -> FaultIntensities {
    FaultIntensities {
        reorder_prob: fraction(seed, 1, 0.5),
        duplicate_prob: fraction(seed, 9, 0.25),
        drop_prob: fraction(seed, 17, 0.1),
        ..FaultIntensities::default()
    }
}

/// Build a runtime over a fresh cluster of `node_count` nodes from `seed`, with
/// the seed-derived faults applied and the metadata group replicated on every
/// node (the default replication factor equals the node count).
fn runtime(seed: u64, node_count: usize) -> SimRuntime {
    let cluster = SimulatedCluster::new(RunConfig {
        seed,
        params: ScenarioParameters {
            node_count,
            replication_factor: node_count,
            partition_count: 1,
            faults: faults_for(seed),
            ..ScenarioParameters::default()
        },
    })
    .expect("a valid cluster shape assembles");
    let budget = Budget {
        max_events: MAX_EVENTS,
        max_virtual_nanos: u64::MAX,
    };
    SimRuntime::new(cluster, budget)
}

/// Arm `__meta/0` election timers at instant `now` for every node in `indices`,
/// scheduling the resulting `TimerFire`s onto the timeline.
///
/// Mirrors the bootstrap the run orchestration performs: a freshly assembled (or
/// freshly restarted) `__meta/0` replica has no election timer until one is
/// armed, so this is how the metadata group starts — or restarts — an election.
fn arm_meta_elections(rt: &mut SimRuntime, indices: &[usize], now: VirtualInstant) {
    let meta = metadata_group_key();
    let ids: Vec<_> = indices
        .iter()
        .filter_map(|&i| rt.cluster().node(i).map(|n| n.id().clone()))
        .collect();
    for id in ids {
        rt.cluster_mut().clock_mut().set_now(now);
        rt.cluster_mut().clock_mut().set_active(id, meta.clone());
        rt.cluster_mut()
            .clock_mut()
            .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
    }
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
}

/// The index of the node whose `__meta/0` replica currently believes it is
/// leader, if any.
fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
    rt.cluster()
        .nodes()
        .iter()
        .position(|n| n.controller().and_then(|c| c.role()) == Some(vela_raft::Role::Leader))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 11: Election Safety
    // (Raft §5.2)
    #[test]
    fn election_safety_holds_across_a_faulty_run(
        seed in any::<u64>(),
        node_count in 3usize..=5,
    ) {
        let mut rt = runtime(seed, node_count);
        let mut checker = RaftSafetyChecker::new();

        // Bootstrap the metadata group's first elections.
        let all: Vec<usize> = (0..node_count).collect();
        arm_meta_elections(&mut rt, &all, VirtualInstant::ORIGIN);

        // No leader exists before any event; the initial observation must pass.
        check_election_safety(&mut checker, &rt)?;

        // Fault timeline: crash the elected leader roughly a third of the way in
        // to force a re-election in a higher term, then restart it (and re-arm
        // its election timer) roughly two thirds in so it rejoins the race.
        let crash_at = MAX_EVENTS / 3;
        let restart_at = (MAX_EVENTS * 2) / 3;
        let mut crashed: Option<usize> = None;
        let mut restarted = false;

        loop {
            // Inject the crash once a leader has actually been elected.
            if crashed.is_none()
                && rt.scheduler().events_processed() >= crash_at
            {
                if let Some(leader) = meta_leader_index(&rt) {
                    prop_assert!(rt.cluster_mut().crash_node(leader));
                    crashed = Some(leader);
                }
            }
            // Restart the crashed node and re-arm its election timer.
            if !restarted {
                if let Some(leader) = crashed {
                    if rt.scheduler().events_processed() >= restart_at {
                        let now = rt.scheduler().now();
                        prop_assert!(
                            rt.cluster_mut()
                                .restart_node(leader)
                                .expect("restart recovers cleanly")
                        );
                        arm_meta_elections(&mut rt, &[leader], now);
                        restarted = true;
                    }
                }
            }

            match rt.step().expect("dispatch never fails without metadata proposals") {
                Step::Event(_) => check_election_safety(&mut checker, &rt)?,
                Step::Done(_) => break,
            }
        }

        // The run is meaningful only if at least one election actually completed
        // along the way — otherwise the property would be vacuously true.
        prop_assert!(
            crashed.is_some(),
            "expected at least one elected metadata leader to drive the property"
        );
    }
}

/// Observe the cluster at the runtime's current instant and fail the property
/// only on an Election Safety violation (Property 11, Requirement 10.1).
///
/// `observe` bundles the incremental Election Safety and commit-monotonicity
/// checks; this test asserts solely on `ElectionSafety`, leaving the other
/// property to its own dedicated test.
fn check_election_safety(
    checker: &mut RaftSafetyChecker,
    rt: &SimRuntime,
) -> Result<(), TestCaseError> {
    if let Err(violation) = checker.observe(rt.cluster(), rt.scheduler().now()) {
        prop_assert_ne!(
            violation.property,
            PropertyId::ElectionSafety,
            "Election Safety violated: {}",
            violation.detail
        );
    }
    Ok(())
}
