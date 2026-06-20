#![cfg(feature = "sim")]
//! Integration tests exercising each named scenario preset (task 22.2).
//!
//! Feature: deterministic-simulation-testing, Requirements 15.2, 15.3
//!
//! Task 22.1 added the named [`ScenarioParameters`] coverage presets — leader
//! election / failover, log replication / follower catch-up, network partition
//! / heal, node crash / durable restart, and concurrent topic administration.
//! This file proves each preset actually *exercises* the behavior it targets,
//! and that a run of the preset reports no property violation.
//!
//! Each preset gets one `#[test]` that does two complementary things:
//!
//! 1. **End-to-end run, asserting [`Outcome::Passed`].** The full preset — with
//!    its own [`FaultIntensities`](vela_sim::scenario::FaultIntensities) — is run
//!    through the real harness via [`SimRuntime::run`] over a handful of fixed
//!    seeds. Because a run is a pure function of `(seed, params)`, a clean pass
//!    is reproducible; a regression in any of the 22 checked properties would
//!    turn one of these into [`Outcome::Failed`].
//!
//! 2. **Instrumented run, asserting the targeted behavior occurs.** The preset's
//!    *cluster shape* (`node_count`, `replication_factor`, `partition_count`) is
//!    driven through a hand-stepped [`SimRuntime`] — the same per-event dispatch
//!    path the run orchestration uses — with the relevant fault applied
//!    explicitly through the public cluster / network API (crash, restart,
//!    partition, heal) so the behavior is demonstrated *deterministically*
//!    rather than left to a probabilistic fault schedule. The behavior is then
//!    observed through the read-only cluster surface already available to the
//!    checkers (replica roles, terms, and committed logs; the served
//!    catalogue). To keep this demonstration robust and seed-deterministic, the
//!    instrumented run uses a healthy network (default fault intensities) and
//!    injects only the one fault the preset targets — exactly as the
//!    `prop_liveness` and `prop_recovery` property tests do.
//!
//! In addition to the five named per-preset tests, one sweep test
//! ([`every_preset_runs_without_a_property_violation`]) iterates
//! [`ScenarioParameters::all_presets`] and asserts a (brisk-budget) run of every
//! preset passes, so a newly added preset is automatically covered by the
//! run-passes assertion without editing this file.
//!
//! The whole file is single-threaded and draws every random decision from the
//! one seed, so it is fully deterministic and never flakes.
//!
//! Validates: Requirements 15.2, 15.3

use vela_core::{
    metadata_group_key, ClusterCommand, GroupKey, LogBackend, NodeId, Partition, PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::cluster::SimulatedCluster;
use vela_sim::codec::encode_cluster_command;
use vela_sim::runtime::{Outcome, SimRuntime};
use vela_sim::scenario::{Budget, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, HealId, Step, VirtualInstant};

/// Upper bound on the discrete events any single "step until …" phase drives.
/// Heartbeats re-arm continuously so the timeline never quiesces; this cap keeps
/// each phase bounded while leaving ample room to elect a leader, commit a
/// `CreateTopic`, replicate records, and recover a restarted replica.
const MAX_PHASE_STEPS: usize = 80_000;

/// The seeds each preset's end-to-end run is asserted to pass on. A small fixed
/// set keeps the test brisk while covering distinct fault schedules; every run
/// is deterministic, so a pass here is reproducible.
const RUN_SEEDS: [u64; 4] = [0, 1, 7, 42];

/// The seed the instrumented (behavior-observing) run uses. Any fixed seed works
/// — the run is deterministic — and the explicit fault makes the targeted
/// behavior occur regardless of the seed's incidental choices.
const OBSERVE_SEED: u64 = 1;

// ----- End-to-end run helpers -----------------------------------------------

/// Run the full preset through the real harness on each of [`RUN_SEEDS`] and
/// assert every run reports [`Outcome::Passed`] — no Safety_/Kafka-parity/
/// Liveness_Property was violated, and the parameters were valid.
fn assert_preset_runs_pass(params: ScenarioParameters) {
    for &seed in &RUN_SEEDS {
        let outcome = SimRuntime::run(RunConfig { seed, params });
        assert_eq!(
            outcome,
            Outcome::Passed,
            "preset run must pass (seed={seed}, params={params:?})",
        );
    }
}

/// A brisk per-run event budget for the all-presets coverage sweep
/// ([`every_preset_runs_without_a_property_violation`]).
///
/// Only the *event* budget is shrunk; the virtual-time budget is left at the
/// default so the liveness checker's favorable-window logic is untouched and
/// can never fire falsely. A run is a pure function of its `(seed, params)`, so
/// a smaller event budget simply produces a *prefix* of the same deterministic
/// timeline the per-preset full-budget tests exercise — which cannot introduce a
/// violation those passing runs did not have, while keeping the
/// five-preset × four-seed sweep fast.
const SWEEP_MAX_EVENTS: u64 = 25_000;

/// Iterating run-passes coverage over **every** preset
/// [`ScenarioParameters::all_presets`] enumerates (Requirements 15.2, 15.3).
///
/// The per-preset tests below name and exercise each preset's targeted behavior
/// individually; this test instead drives the *whole* preset set through the
/// real harness by iterating `all_presets()`, so a newly added preset is
/// automatically covered by the "run passes" assertion without editing this
/// file. Each run must finish without any of the 22 checked properties being
/// violated — an [`Outcome::Failed`] fails the test with a message naming the
/// preset, seed, and violated property; an [`Outcome::Invalid`] (which no preset
/// produces, since every preset validates) likewise fails.
#[test]
fn every_preset_runs_without_a_property_violation() {
    let budget = Budget {
        max_events: SWEEP_MAX_EVENTS,
        ..Budget::default()
    };
    for (name, preset) in ScenarioParameters::all_presets() {
        let params = ScenarioParameters { budget, ..preset };
        for &seed in &RUN_SEEDS {
            match SimRuntime::run(RunConfig { seed, params }) {
                Outcome::Passed => {}
                Outcome::Failed {
                    property,
                    at,
                    detail,
                } => panic!(
                    "preset `{name}` (seed={seed}) ended in a property violation: \
                     {property:?} detected at {at:?}: {detail}"
                ),
                Outcome::Invalid { detail } => {
                    panic!("preset `{name}` (seed={seed}) was unexpectedly invalid: {detail}")
                }
            }
        }
    }
}

/// The preset's cluster *shape* paired with a healthy network and the documented
/// defaults for every other field, so the instrumented run's only adversity is
/// the one fault the test injects explicitly.
fn healthy_shape(preset: ScenarioParameters) -> ScenarioParameters {
    ScenarioParameters {
        node_count: preset.node_count,
        replication_factor: preset.replication_factor,
        partition_count: preset.partition_count,
        ..ScenarioParameters::default()
    }
}

/// Build a [`SimRuntime`] over a cluster of the given shape from `seed`.
fn build_rt(params: ScenarioParameters, seed: u64) -> SimRuntime {
    let cluster =
        SimulatedCluster::new(RunConfig { seed, params }).expect("a valid cluster shape assembles");
    SimRuntime::new(cluster, Budget::default())
}

// ----- Instrumented step loop -----------------------------------------------

/// Step the runtime until `pred` holds, the timeline ends, or `max_steps` is
/// reached. Returns whether `pred` holds at the end.
fn step_until(
    rt: &mut SimRuntime,
    max_steps: usize,
    mut pred: impl FnMut(&SimRuntime) -> bool,
) -> bool {
    if pred(rt) {
        return true;
    }
    for _ in 0..max_steps {
        match rt
            .step()
            .expect("event dispatch never fails in a healthy run")
        {
            Step::Event(_) => {
                if pred(rt) {
                    return true;
                }
            }
            Step::Done(_) => return pred(rt),
        }
    }
    pred(rt)
}

/// Arm an initial election timer for every node's `__meta/0` replica at the
/// origin, so the metadata group can start an election.
fn seed_meta_elections(rt: &mut SimRuntime) {
    let meta = metadata_group_key();
    let now = VirtualInstant::ORIGIN;
    let node_ids: Vec<NodeId> = rt
        .cluster()
        .nodes()
        .iter()
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

/// Arm a fresh election timer for `group` on every running node that hosts a
/// replica for it, so the group (re-)starts an election now — used both to
/// bootstrap a partition group and to drive a failover after a leader crash.
fn seed_partition_elections(rt: &mut SimRuntime, group: &GroupKey) {
    let now = rt.scheduler().now();
    let hosts: Vec<NodeId> = rt
        .cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running() && n.fleet_replicas().any(|(g, _)| g == group))
        .map(|n| n.id().clone())
        .collect();
    for node in hosts {
        rt.cluster_mut().clock_mut().set_now(now);
        rt.cluster_mut().clock_mut().set_active(node, group.clone());
        rt.cluster_mut()
            .clock_mut()
            .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
    }
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
}

/// Propose `payload` to the replica for `group` on the node at `index` and route
/// the follow-on effects back onto the scheduler timeline, exactly as the
/// runtime's per-step dispatch does. Returns `false` if the node did not step
/// the replica (it is not hosting the group or is crashed).
fn propose_and_route(
    rt: &mut SimRuntime,
    index: usize,
    group: &GroupKey,
    payload: EntryPayload,
) -> bool {
    let now = rt.scheduler().now();
    rt.cluster_mut().network().set_now(now);
    let Some(out) = rt
        .cluster_mut()
        .step_replica(index, group, now, RaftInput::Propose(payload))
    else {
        return false;
    };
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
    true
}

// ----- Observation helpers --------------------------------------------------

/// The index of the node currently leading `__meta/0`, if any.
fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
    rt.cluster()
        .nodes()
        .iter()
        .position(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
}

/// The index of a running node that currently leads `group`, if any.
fn partition_leader_index(rt: &SimRuntime, group: &GroupKey) -> Option<usize> {
    rt.cluster().nodes().iter().position(|n| {
        n.is_running()
            && n.fleet_replicas()
                .any(|(g, replica)| g == group && replica.raft().role() == Role::Leader)
    })
}

/// The current Raft term of the `group` replica on the node at `index`, if it
/// hosts one.
fn partition_term(rt: &SimRuntime, index: usize, group: &GroupKey) -> Option<u64> {
    rt.cluster()
        .node(index)?
        .fleet_replicas()
        .find_map(|(g, r)| (g == group).then(|| r.raft().current_term()))
}

/// Whether every running node hosts a replica for `group`.
fn all_running_nodes_host(rt: &SimRuntime, group: &GroupKey) -> bool {
    rt.cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .all(|n| n.fleet_replicas().any(|(g, _)| g == group))
}

/// Whether the running node at `index` has `value` in its committed `group` log.
fn committed_on_node(rt: &SimRuntime, index: usize, group: &GroupKey, value: &[u8]) -> bool {
    rt.cluster().node(index).is_some_and(|node| {
        node.is_running()
            && node.fleet_replicas().any(|(g, replica)| {
                g == group && replica.read(0, usize::MAX).iter().any(|r| r.value == value)
            })
    })
}

/// Whether **every** running replica of `group` has `value` committed — i.e.
/// every up voter (including any just-recovered or just-reconnected one) has
/// caught up to it.
fn committed_everywhere(rt: &SimRuntime, group: &GroupKey, value: &[u8]) -> bool {
    let hosts: Vec<usize> = (0..rt.cluster().node_count())
        .filter(|&i| {
            rt.cluster()
                .node(i)
                .is_some_and(|n| n.is_running() && n.fleet_replicas().any(|(g, _)| g == group))
        })
        .collect();
    !hosts.is_empty()
        && hosts
            .iter()
            .all(|&i| committed_on_node(rt, i, group, value))
}

// ----- Topic / produce helpers ----------------------------------------------

/// The `(topic, p)` group keys for every partition `0..partition_count`.
fn topic_groups(topic: &str, partition_count: u32) -> Vec<GroupKey> {
    (0..partition_count)
        .map(|p| (topic.to_string(), PartitionIndex(p)))
        .collect()
}

/// A `CreateTopic` command for `topic` whose partitions carry the topology's
/// fixed `Replica_Set`s and the given log `backend` — exactly the catalogue the
/// metadata group would commit, so the reconcile pass spawns a replica on each
/// assigned node.
fn create_topic_command(
    cluster: &SimulatedCluster,
    topic: &str,
    partition_count: u32,
    backend: LogBackend,
) -> ClusterCommand {
    let topology = cluster.topology();
    let partitions = (0..partition_count)
        .map(|p| {
            let index = PartitionIndex(p);
            Partition {
                index,
                replicas: topology
                    .replica_set_for(index)
                    .expect("partition index within range")
                    .to_vec(),
                leader: None,
            }
        })
        .collect();
    ClusterCommand::CreateTopic {
        name: topic.to_string(),
        partitions,
        backend,
    }
}

/// Bootstrap and elect the metadata leader, returning its node index.
fn elect_meta_leader(rt: &mut SimRuntime) -> usize {
    seed_meta_elections(rt);
    assert!(
        step_until(rt, MAX_PHASE_STEPS, |rt| meta_leader_index(rt).is_some()),
        "the metadata group must elect a leader",
    );
    meta_leader_index(rt).expect("a metadata leader was elected")
}

/// Commit a `CreateTopic` for `topic` through the metadata leader and step until
/// the reconcile pass has spawned its partition replicas on every running node.
fn create_topic(rt: &mut SimRuntime, meta_leader: usize, topic: &str, backend: LogBackend) {
    let partition_count = rt.cluster().topology().partition_count();
    let command = create_topic_command(rt.cluster(), topic, partition_count, backend);
    let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&command));
    assert!(
        propose_and_route(rt, meta_leader, &metadata_group_key(), payload),
        "the metadata leader must accept the CreateTopic proposal",
    );
    let groups = topic_groups(topic, partition_count);
    assert!(
        step_until(rt, MAX_PHASE_STEPS, |rt| groups
            .iter()
            .all(|g| all_running_nodes_host(rt, g))),
        "the committed CreateTopic must reconcile replicas onto every running node",
    );
}

/// Elect a leader for `group`, returning its node index.
fn elect_partition_leader(rt: &mut SimRuntime, group: &GroupKey) -> usize {
    seed_partition_elections(rt, group);
    assert!(
        step_until(rt, MAX_PHASE_STEPS, |rt| partition_leader_index(rt, group)
            .is_some()),
        "the partition group must elect a leader",
    );
    partition_leader_index(rt, group).expect("a partition leader was elected")
}

/// Propose `value` to the current leader of `group` and step until it is
/// committed on every running replica of the group.
fn produce_and_commit_everywhere(rt: &mut SimRuntime, group: &GroupKey, value: &[u8]) {
    let leader = partition_leader_index(rt, group).expect("a partition leader to produce to");
    let payload = EntryPayload::new(PayloadKind::Record, value.to_vec());
    assert!(
        propose_and_route(rt, leader, group, payload),
        "the partition leader must accept the produce proposal",
    );
    assert!(
        step_until(rt, MAX_PHASE_STEPS, |rt| committed_everywhere(
            rt, group, value
        )),
        "a produced record must commit on every running replica",
    );
}

/// Propose `value` to the current leader of `group` and step until it is
/// committed on the leader's node (a majority commit), without requiring the
/// (possibly partitioned or crashed) minority to have it.
fn produce_and_commit_on_leader(rt: &mut SimRuntime, group: &GroupKey, value: &[u8]) {
    let leader = partition_leader_index(rt, group).expect("a partition leader to produce to");
    let payload = EntryPayload::new(PayloadKind::Record, value.to_vec());
    assert!(
        propose_and_route(rt, leader, group, payload),
        "the partition leader must accept the produce proposal",
    );
    assert!(
        step_until(rt, MAX_PHASE_STEPS, |rt| committed_on_node(
            rt, leader, group, value
        )),
        "a produced record must commit on the leader's node (majority commit)",
    );
}

// ----- Preset 1: leader election / failover ---------------------------------

#[test]
fn leader_failover_preset_elects_a_new_leader_after_the_leader_crashes() {
    let preset = ScenarioParameters::leader_failover();
    assert_preset_runs_pass(preset);

    // Instrumented demonstration: a partition leader crash forces the surviving
    // majority to elect a new leader in a higher term (Requirement 15.3).
    let mut rt = build_rt(healthy_shape(preset), OBSERVE_SEED);
    let meta_leader = elect_meta_leader(&mut rt);
    create_topic(&mut rt, meta_leader, "orders", LogBackend::InMemory);

    let group: GroupKey = ("orders".to_string(), PartitionIndex(0));
    let first_leader = elect_partition_leader(&mut rt, &group);
    let first_term = partition_term(&rt, first_leader, &group).expect("the leader hosts the group");

    // Crash the current leader; a majority of the five-replica group survives.
    assert!(
        rt.cluster_mut().crash_node(first_leader),
        "the partition leader must crash",
    );

    // The survivors must elect a *new* leader (a different node) in a later term.
    seed_partition_elections(&mut rt, &group);
    assert!(
        step_until(&mut rt, MAX_PHASE_STEPS, |rt| {
            partition_leader_index(rt, &group).is_some_and(|idx| idx != first_leader)
        }),
        "the surviving majority must fail over to a new leader",
    );

    let new_leader = partition_leader_index(&rt, &group).expect("a new leader was elected");
    let new_term = partition_term(&rt, new_leader, &group).expect("the new leader hosts the group");
    assert_ne!(
        new_leader, first_leader,
        "failover must move leadership to a different node",
    );
    assert!(
        new_term > first_term,
        "the new leader's term ({new_term}) must exceed the crashed leader's ({first_term})",
    );
}

// ----- Preset 2: log replication / follower catch-up ------------------------

#[test]
fn log_replication_catch_up_preset_lets_a_restarted_follower_catch_up() {
    let preset = ScenarioParameters::log_replication_catch_up();
    assert_preset_runs_pass(preset);

    // Instrumented demonstration: a follower crashes, the leader keeps committing
    // on the surviving majority, and the durably-restarted follower catches up on
    // the entries it missed via replication (Requirement 15.3).
    let mut rt = build_rt(healthy_shape(preset), OBSERVE_SEED);
    let meta_leader = elect_meta_leader(&mut rt);
    create_topic(&mut rt, meta_leader, "orders", LogBackend::Durable);

    let group: GroupKey = ("orders".to_string(), PartitionIndex(0));
    let leader = elect_partition_leader(&mut rt, &group);

    // Replicate a baseline record to every replica.
    produce_and_commit_everywhere(&mut rt, &group, b"rec-0");

    // Crash a follower (not the leader).
    let follower = (0..rt.cluster().node_count())
        .find(|&i| i != leader)
        .expect("a follower exists in a multi-node group");
    assert!(
        rt.cluster_mut().crash_node(follower),
        "the follower crashes"
    );
    assert!(
        !committed_on_node(&rt, follower, &group, b"rec-1"),
        "the crashed follower cannot hold a record produced while it is down",
    );

    // The leader keeps committing on the surviving majority while it is down.
    produce_and_commit_on_leader(&mut rt, &group, b"rec-1");
    produce_and_commit_on_leader(&mut rt, &group, b"rec-2");

    // Restart the follower through the real durable-recovery path, then re-arm
    // its election timer so it rejoins the group and accepts replication.
    assert!(
        rt.cluster_mut()
            .restart_node(follower)
            .expect("the follower restarts cleanly from its retained disk"),
        "the crashed follower restarts",
    );
    seed_partition_elections(&mut rt, &group);

    // Catch-up: the restarted follower must converge on every record, including
    // the ones produced while it was down.
    assert!(
        step_until(&mut rt, MAX_PHASE_STEPS, |rt| {
            committed_on_node(rt, follower, &group, b"rec-2")
        }),
        "the restarted follower must catch up on the entries it missed",
    );
    assert!(
        committed_on_node(&rt, follower, &group, b"rec-0")
            && committed_on_node(&rt, follower, &group, b"rec-1"),
        "the caught-up follower must hold the full committed log",
    );
}

// ----- Preset 3: network partition / heal -----------------------------------

#[test]
fn network_partition_heal_preset_isolated_minority_catches_up_after_heal() {
    let preset = ScenarioParameters::network_partition_heal();
    assert_preset_runs_pass(preset);

    // Instrumented demonstration: a minority is partitioned off the leader's
    // majority, the majority keeps committing while the minority stalls, and once
    // the partition heals the minority reconnects and converges (Requirement
    // 15.3).
    let mut rt = build_rt(healthy_shape(preset), OBSERVE_SEED);
    let meta_leader = elect_meta_leader(&mut rt);
    create_topic(&mut rt, meta_leader, "orders", LogBackend::InMemory);

    let group: GroupKey = ("orders".to_string(), PartitionIndex(0));
    let leader = elect_partition_leader(&mut rt, &group);
    produce_and_commit_everywhere(&mut rt, &group, b"pre-partition");

    // Partition a strict minority (two nodes, neither the leader) off the rest;
    // the leader keeps a majority of three on its side of the five-node cluster.
    let node_count = rt.cluster().node_count();
    let minority_idx: Vec<usize> = (0..node_count)
        .filter(|&i| i != leader)
        .take(node_count.saturating_sub(1) / 2)
        .collect();
    assert!(
        !minority_idx.is_empty(),
        "the five-node partition preset must isolate a non-empty minority",
    );
    let ids: Vec<NodeId> = rt
        .cluster()
        .nodes()
        .iter()
        .map(|n| n.id().clone())
        .collect();
    let minority_ids: Vec<NodeId> = minority_idx.iter().map(|&i| ids[i].clone()).collect();
    let majority_ids: Vec<NodeId> = (0..node_count)
        .filter(|i| !minority_idx.contains(i))
        .map(|i| ids[i].clone())
        .collect();
    let heal_id = HealId(1);
    rt.cluster()
        .network()
        .install_partition(heal_id, minority_ids, majority_ids);

    // The majority commits a record the isolated minority cannot observe.
    produce_and_commit_on_leader(&mut rt, &group, b"during-partition");
    for &i in &minority_idx {
        assert!(
            !committed_on_node(&rt, i, &group, b"during-partition"),
            "an isolated minority node must not observe the majority's commit",
        );
    }

    // Heal the partition; the minority reconnects and must converge on the record
    // it missed.
    assert!(
        rt.cluster().network().heal(heal_id),
        "the installed partition must heal",
    );
    assert!(
        step_until(&mut rt, MAX_PHASE_STEPS, |rt| committed_everywhere(
            rt,
            &group,
            b"during-partition"
        )),
        "every replica must converge on the record after the partition heals",
    );
}

// ----- Preset 4: node crash / durable restart -------------------------------

#[test]
fn crash_durable_restart_preset_recovers_acknowledged_records() {
    let preset = ScenarioParameters::crash_durable_restart();
    assert_preset_runs_pass(preset);

    // Instrumented demonstration: a record acknowledged (committed) before a
    // crash survives a durable restart — the recovered replica still holds it,
    // exercising the durability boundary (Requirement 15.3).
    let mut rt = build_rt(healthy_shape(preset), OBSERVE_SEED);
    let meta_leader = elect_meta_leader(&mut rt);
    create_topic(&mut rt, meta_leader, "orders", LogBackend::Durable);

    let group: GroupKey = ("orders".to_string(), PartitionIndex(0));
    let leader = elect_partition_leader(&mut rt, &group);

    // Acknowledge a record on every durable replica before the crash.
    produce_and_commit_everywhere(&mut rt, &group, b"durable-record");

    // Crash a follower and restart it from its retained disk; the acknowledged
    // record was fsynced under `SyncPolicy::Always`, so it must survive recovery.
    let follower = (0..rt.cluster().node_count())
        .find(|&i| i != leader)
        .expect("a follower exists in a multi-node group");
    assert!(
        rt.cluster_mut().crash_node(follower),
        "the follower crashes"
    );
    assert!(
        rt.cluster().node(follower).is_some_and(|n| !n.is_running()),
        "the crashed follower is not running",
    );

    assert!(
        rt.cluster_mut()
            .restart_node(follower)
            .expect("the follower restarts cleanly from its retained disk"),
        "the crashed follower restarts",
    );

    // The restart runs the real WAL/Raft recovery path, which restores the
    // durably-committed record into the recovered replica's log.
    assert!(
        committed_on_node(&rt, follower, &group, b"durable-record"),
        "an acknowledged record must survive a crash and durable restart",
    );
}

// ----- Preset 5: concurrent topic administration ----------------------------

#[test]
fn concurrent_topic_admin_preset_commits_every_topic() {
    let preset = ScenarioParameters::concurrent_topic_admin();
    assert_preset_runs_pass(preset);

    // Instrumented demonstration: several topic-create commands proposed back to
    // back through the metadata group all commit and reconcile their per-partition
    // replicas across every node (Requirement 15.3).
    let mut rt = build_rt(healthy_shape(preset), OBSERVE_SEED);
    let meta_leader = elect_meta_leader(&mut rt);
    let partition_count = rt.cluster().topology().partition_count();

    let topics = ["alpha", "beta", "gamma"];

    // Propose every CreateTopic concurrently (back to back, before stepping), so
    // they are all in flight through the single metadata group at once.
    for topic in topics {
        let command =
            create_topic_command(rt.cluster(), topic, partition_count, LogBackend::InMemory);
        let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&command));
        assert!(
            propose_and_route(&mut rt, meta_leader, &metadata_group_key(), payload),
            "the metadata leader must accept the CreateTopic proposal for `{topic}`",
        );
    }

    // Every topic must commit (appear in every running node's served catalogue)
    // and reconcile its partition replicas onto every running node.
    let all_groups: Vec<GroupKey> = topics
        .iter()
        .flat_map(|t| topic_groups(t, partition_count))
        .collect();
    assert!(
        step_until(&mut rt, MAX_PHASE_STEPS, |rt| {
            all_groups.iter().all(|g| all_running_nodes_host(rt, g))
        }),
        "every concurrently-created topic must reconcile onto every node",
    );

    for node in rt.cluster().nodes() {
        for topic in topics {
            assert!(
                node.served().topics.contains_key(topic),
                "node {:?} must serve concurrently-created topic `{topic}`",
                node.id(),
            );
        }
    }
}
