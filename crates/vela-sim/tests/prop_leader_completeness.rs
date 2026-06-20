#![cfg(feature = "sim")]
//! Property test for Leader Completeness in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 13: Leader Completeness
//! (Raft §5.4)
//!
//! Property 13 (Requirement 10.3): once an entry is *committed* in a given term,
//! that entry is present — unchanged, at the same index — in the log of every
//! replica that is or later becomes leader in any later term. The
//! [`RaftSafetyChecker`] checks this *structurally* over a full snapshot of
//! every replica's log in [`check_logs`](RaftSafetyChecker::check_logs); a
//! breach is returned as a [`Violation`] naming
//! [`PropertyId::LeaderCompleteness`] and the detection instant.
//!
//! To exercise the property meaningfully a run must (a) commit entries in one
//! term and (b) then force a leadership change into a higher term, so the
//! checker can confirm the earlier-term commit survived into the new leader.
//! This test drives a small all-voter cluster (≥3 nodes, so a single crash
//! leaves a majority) through real consensus on the dedicated `__meta/0` group:
//!
//! 1. seed each node's first election timer and drive to a leader (term `T1`);
//! 2. commit a `CreateTopic` through that leader — an entry committed in `T1`;
//! 3. **crash the leader** — the fault that forces a leadership change *after*
//!    a commit;
//! 4. drive the surviving majority to a new leader (a strictly higher term
//!    `T2`); and
//! 5. commit a second `CreateTopic` through the new leader.
//!
//! After every dispatched event — and as a final pass — the test feeds the
//! cluster to both [`RaftSafetyChecker::observe`] (the incremental safety
//! checks) and [`RaftSafetyChecker::check_logs`] (the structural ones, which is
//! where Leader Completeness lives) and asserts no
//! [`PropertyId::LeaderCompleteness`] violation is ever reported: the `T1`
//! commit is present in the `T2` leader, exactly as Raft §5.4 requires.
//!
//! The run is driven through the public [`SimRuntime::step`] loop (which handles
//! all election / replication dispatch) with proposals and the crash injected
//! through the public cluster API, since the full run orchestration and fault
//! schedule are separate tasks. Every decision derives from the run seed, so a
//! failing case replays deterministically.
//!
//! Validates: Requirements 10.3

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, GroupKey, LogBackend, Partition, PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, RaftOutput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::{PropertyId, RaftSafetyChecker};
use vela_sim::cluster::SimulatedCluster;
use vela_sim::codec::{decode_cluster_command, encode_cluster_command};
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualInstant};

/// A generous per-phase cap on dispatched events. A 3–5 voter election (or a
/// re-election after one crash) and a single metadata commit each converge in a
/// few tens of events; this bound is large enough that even a seed that takes
/// several split-vote rounds resolves well within it, while still terminating a
/// pathological run rather than looping forever.
const DRIVE_CAP: usize = 50_000;

/// The first topic, committed under the original leader's term.
const TOPIC_A: &str = "orders";
/// The second topic, committed under the post-crash leader's (higher) term.
const TOPIC_B: &str = "events";

/// Build a [`SimRuntime`] over a freshly assembled all-voter cluster of
/// `node_count` nodes with `partition_count` partitions from `seed`, with every
/// other scenario parameter at its documented default (a healthy cluster — the
/// only fault is the crash this test injects explicitly).
///
/// `replication_factor == node_count` makes every node a voter of every group,
/// so crashing a single node always leaves a strict majority able to elect a
/// new leader (Requirement 6.5). The event budget is set high so the scheduler
/// never ends the run before the test's own [`DRIVE_CAP`] does.
fn build_runtime(seed: u64, node_count: usize, partition_count: u32) -> SimRuntime {
    let cluster = SimulatedCluster::new(RunConfig {
        seed,
        params: ScenarioParameters {
            node_count,
            replication_factor: node_count,
            partition_count,
            ..ScenarioParameters::default()
        },
    })
    .expect("an all-voter cluster of >=3 nodes assembles");
    let budget = Budget {
        max_events: u64::MAX,
        max_virtual_nanos: u64::MAX,
    };
    SimRuntime::new(cluster, budget)
}

/// Seed an initial election timer for every running node's `__meta/0` replica at
/// the origin, so the metadata group can start electing.
///
/// Mirrors the bootstrap the run orchestration performs: arm each replica's
/// first election timer through the [`Clock`] seam (so its generation matches
/// the clock's `is_current` check) and schedule the resulting `TimerFire`s onto
/// the timeline.
fn seed_meta_elections(rt: &mut SimRuntime) {
    let meta = metadata_group_key();
    let now = VirtualInstant::ORIGIN;
    let node_ids: Vec<_> = rt
        .cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .map(|n| n.id().clone())
        .collect();
    for node in node_ids {
        rt.cluster_mut().clock_mut().set_now(now);
        rt.cluster_mut().clock_mut().set_active(node, meta.clone());
        rt.cluster_mut()
            .clock_mut()
            .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
    }
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
}

/// The index of the node currently leading `__meta/0`, if any.
///
/// A crashed node has no live controller, so it can never be reported as leader;
/// any index returned is therefore a running leader.
fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
    rt.cluster()
        .nodes()
        .iter()
        .position(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
}

/// Whether any running node's served catalogue lists `topic`.
///
/// A node's served catalogue is updated only when a committed metadata entry is
/// applied, so this is `true` exactly when the `CreateTopic` has *committed*
/// (and been applied) somewhere — the signal the test uses to know an entry has
/// been committed under the current leader's term.
fn cluster_committed_topic(rt: &SimRuntime, topic: &str) -> bool {
    rt.cluster()
        .nodes()
        .iter()
        .any(|n| n.served().topics.contains_key(topic))
}

/// A `CreateTopic` command for `name` whose partitions carry the topology's
/// fixed Replica_Sets — the catalogue shape the metadata group commits in
/// production.
fn create_topic_for(cluster: &SimulatedCluster, name: &str) -> ClusterCommand {
    let topo = cluster.topology();
    let partitions = (0..topo.partition_count())
        .map(|p| {
            let index = PartitionIndex(p);
            Partition {
                index,
                replicas: topo
                    .replica_set_for(index)
                    .expect("partition index within range")
                    .to_vec(),
                leader: None,
            }
        })
        .collect();
    ClusterCommand::CreateTopic {
        name: name.to_string(),
        partitions,
        backend: LogBackend::Durable,
    }
}

/// Feed the cluster to both checkers at `now` and fail the case if either
/// reports a [`PropertyId::LeaderCompleteness`] violation.
///
/// [`observe`](RaftSafetyChecker::observe) runs the incremental safety checks
/// and [`check_logs`](RaftSafetyChecker::check_logs) the structural ones —
/// Leader Completeness is detected by the latter. Other properties' violations
/// (if any) are out of this test's scope, so the assertion is scoped to Leader
/// Completeness alone.
fn assert_leader_completeness_holds(
    checker: &mut RaftSafetyChecker,
    rt: &SimRuntime,
    now: VirtualInstant,
) -> Result<(), TestCaseError> {
    if let Err(v) = checker.observe(rt.cluster(), now) {
        prop_assert_ne!(
            v.property,
            PropertyId::LeaderCompleteness,
            "Leader Completeness violated (via observe): {}",
            v
        );
    }
    if let Err(v) = checker.check_logs(rt.cluster(), now) {
        prop_assert_ne!(
            v.property,
            PropertyId::LeaderCompleteness,
            "Leader Completeness violated (via check_logs): {}",
            v
        );
    }
    Ok(())
}

/// Step the runtime until `predicate` holds (returning `true`) or the run goes
/// quiescent / the cap is reached (returning `false`), checking Leader
/// Completeness after every dispatched event.
fn drive_until<F>(
    rt: &mut SimRuntime,
    checker: &mut RaftSafetyChecker,
    mut predicate: F,
) -> Result<bool, TestCaseError>
where
    F: FnMut(&SimRuntime) -> bool,
{
    for _ in 0..DRIVE_CAP {
        if predicate(rt) {
            return Ok(true);
        }
        match rt.step() {
            Ok(Step::Event(_)) => {
                let now = rt.scheduler().now();
                assert_leader_completeness_holds(checker, rt, now)?;
            }
            Ok(Step::Done(_)) => return Ok(predicate(rt)),
            Err(e) => {
                return Err(TestCaseError::fail(format!(
                    "event dispatch failed during a healthy run: {e}"
                )))
            }
        }
    }
    Ok(predicate(rt))
}

/// Apply the follow-on effects of a `replica.step` whose [`RaftOutput`] the test
/// produced out-of-band (a client proposal), in the same order the runtime's own
/// dispatch does: apply committed metadata (and reconcile), dispatch `out.sends`
/// through the replica's transport, then schedule the re-armed timers and the
/// buffered deliveries back onto the timeline.
///
/// This mirrors `SimRuntime`'s internal per-event processing for the one input
/// the public step loop cannot yet issue itself — a client `Propose` — so that
/// the proposal's replication flows through the very same scheduler queue as
/// every other event and the run stays a pure function of the seed.
fn apply_proposal_output(
    rt: &mut SimRuntime,
    index: usize,
    group: &GroupKey,
    out: RaftOutput,
) -> Result<(), TestCaseError> {
    if group == &metadata_group_key() {
        for entry in &out.committed {
            if entry.payload.kind == PayloadKind::Cluster {
                let command = decode_cluster_command(&entry.payload.bytes);
                rt.cluster_mut()
                    .apply_committed_metadata(&command)
                    .map_err(|e| {
                        TestCaseError::fail(format!(
                            "applying a committed metadata entry failed: {e}"
                        ))
                    })?;
            }
        }
    }

    if let Some(transport) = rt.cluster().transport_for(index, group) {
        let transport = transport.clone();
        for (to, msg) in out.sends {
            transport.send(to, msg);
        }
    }

    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();

    for (at, envelope) in rt.cluster().network().drain_pending() {
        rt.scheduler_mut()
            .schedule(at, Event::MessageDeliver(envelope));
    }
    Ok(())
}

/// Propose `command` to the `__meta/0` replica on the node at `leader`, routing
/// the resulting Raft effects back onto the timeline so it replicates and
/// commits as the run is driven on.
fn propose_to_meta_leader(
    rt: &mut SimRuntime,
    leader: usize,
    command: &ClusterCommand,
) -> Result<(), TestCaseError> {
    let meta = metadata_group_key();
    let now = rt.scheduler().now();
    rt.cluster_mut().network().set_now(now);
    let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(command));
    let out = rt
        .cluster_mut()
        .step_replica(leader, &meta, now, RaftInput::Propose(payload))
        .ok_or_else(|| TestCaseError::fail("the metadata leader did not accept the proposal"))?;
    apply_proposal_output(rt, leader, &meta, out)
}

/// Generate `(seed, node_count, partition_count)` over small all-voter shapes:
/// `node_count` in `3..=5` (so a single crash always leaves a majority),
/// `partition_count` in `1..=2`, paired with an arbitrary 64-bit seed.
fn shape() -> impl Strategy<Value = (u64, usize, u32)> {
    (any::<u64>(), 3usize..=5, 1u32..=2)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 13: Leader
    // Completeness (Raft §5.4)
    #[test]
    fn committed_entries_survive_into_later_leaders((seed, node_count, partition_count) in shape()) {
        let mut rt = build_runtime(seed, node_count, partition_count);
        let mut checker = RaftSafetyChecker::new();

        // --- Term T1: elect a leader and commit an entry through it. ---------
        seed_meta_elections(&mut rt);
        let elected = drive_until(&mut rt, &mut checker, |rt| meta_leader_index(rt).is_some())?;
        prop_assert!(elected, "the metadata group must elect an initial leader");
        let leader1 = meta_leader_index(&rt).expect("a leader was just elected");

        let create_a = create_topic_for(rt.cluster(), TOPIC_A);
        propose_to_meta_leader(&mut rt, leader1, &create_a)?;
        // Check Leader Completeness right after injecting the proposal too.
        assert_leader_completeness_holds(&mut checker, &rt, rt.scheduler().now())?;

        let committed_a = drive_until(&mut rt, &mut checker, |rt| cluster_committed_topic(rt, TOPIC_A))?;
        prop_assert!(committed_a, "the first topic must commit under the original leader");

        // --- The fault: crash the leader, forcing a leadership change. -------
        prop_assert!(rt.cluster_mut().crash_node(leader1), "the elected leader must crash");
        prop_assert!(
            !rt.cluster().node(leader1).unwrap().is_running(),
            "the crashed leader is no longer running"
        );

        // --- Term T2: the surviving majority elects a new (higher-term) leader.
        let reelected = drive_until(&mut rt, &mut checker, |rt| meta_leader_index(rt).is_some())?;
        prop_assert!(reelected, "the surviving majority must elect a new leader after the crash");
        let leader2 = meta_leader_index(&rt).expect("a new leader was just elected");
        prop_assert_ne!(leader2, leader1, "the new leader is a different, running node");

        // The T1 commit must already be present in the T2 leader's log; a final
        // structural pass asserts it explicitly (the heart of Property 13).
        assert_leader_completeness_holds(&mut checker, &rt, rt.scheduler().now())?;

        // --- Commit a second entry through the new leader, then re-check. ----
        let create_b = create_topic_for(rt.cluster(), TOPIC_B);
        propose_to_meta_leader(&mut rt, leader2, &create_b)?;
        let committed_b = drive_until(&mut rt, &mut checker, |rt| cluster_committed_topic(rt, TOPIC_B))?;
        prop_assert!(committed_b, "the second topic must commit under the new leader");

        // Final structural pass over every replica's full log: no entry
        // committed in an earlier term is missing from any later-term leader.
        let now = rt.scheduler().now();
        if let Err(v) = checker.check_logs(rt.cluster(), now) {
            prop_assert_ne!(
                v.property,
                PropertyId::LeaderCompleteness,
                "Leader Completeness violated on the final pass: {}",
                v
            );
        }
    }
}
