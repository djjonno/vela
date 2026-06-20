#![cfg(feature = "sim")]
//! Property test for Liveness under healed faults in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 21: Liveness under healed
//! faults
//!
//! Property 21: *For any* seed and any small generated cluster — driven through
//! a real metadata election, a committed `CreateTopic`, a real per-partition
//! election, an acknowledged produce, a strict-minority crash, a second produce
//! on the surviving majority, a heal (restart) of the crashed minority, and a
//! third produce after the heal — the partition's Raft group, kept *favorable*
//! throughout (a majority of its voters running and mutually reachable), makes
//! the required progress within a bounded budget: exactly one leader is elected
//! (12.1), every produced record and topic admin command commits (12.2, 12.3),
//! and once the healed minority rejoins every lagging replica converges to the
//! leader's committed log (12.4). A group lacking a reachable majority is never
//! required to make progress (6.6, 12.6), and a stall is a violation only after
//! the bounded budget is exceeded (12.5).
//!
//! The run uses the real harness: a [`SimRuntime`] over a [`SimulatedCluster`]
//! of production replicas, stepped one discrete event at a time, driven through
//! the same path the run orchestration takes (seed `__meta/0` elections, commit
//! a `CreateTopic`, seed the partition election, produce by proposing `Record`
//! entries and routing the effects back onto the timeline, crash / restart a
//! strict minority). It uses **no network faults** so the only adversity is the
//! crash and heal — keeping a majority always available so the group stays
//! favorable and is genuinely *expected* to make progress.
//!
//! A [`LivenessChecker`] is fed an observation after **every** step and told the
//! instant of every applied fault ([`note_fault`]) and heal ([`note_heal`]), so
//! it tracks each group's "favorable since" window exactly as the run
//! orchestration would. The budget is generous — well above the election
//! timeout (`ELECTION_TIMEOUT_BASE` = 150 ms, jitter to 300 ms) plus a few
//! replication round trips — so a favorable, healthy group always converges
//! inside it. After the heal the run advances virtual time **well past the
//! budget** before the final [`check`], so the pass is meaningful: a group that
//! had truly stalled while favorable would by then be flagged.
//!
//! The whole run draws every random decision from the one seed and is
//! single-threaded, so it is fully deterministic and never flakes.
//!
//! Validates: Requirements 6.6, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, GroupKey, LogBackend, NodeId, Partition, PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::liveness::LivenessChecker;
use vela_sim::cluster::SimulatedCluster;
use vela_sim::codec::encode_cluster_command;
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualDuration, VirtualInstant};

/// The topic produced to during the run.
const TOPIC: &str = "orders";

/// The partition records are produced to (partition 0 of [`TOPIC`]).
const PARTITION: PartitionIndex = PartitionIndex(0);

/// Upper bound on the discrete events any single "step until …" phase drives.
/// Heartbeats re-arm continuously, so the timeline never quiesces; this cap
/// keeps each phase bounded while leaving ample room — well within the run's
/// event budget — to elect a leader, commit a `CreateTopic`, elect a partition
/// leader, commit a produce, and advance virtual time past the liveness budget.
const MAX_PHASE_STEPS: usize = 80_000;

/// The bounded budget the favorable group is given to make progress. Generous —
/// roughly 5 logical seconds, well above the worst-case election timeout
/// (`2 * ELECTION_TIMEOUT_BASE` = 300 ms) plus several replication round trips at
/// the default 1 ms one-way latency — so a healthy favorable group always
/// converges inside it. The run advances virtual time well past this before the
/// final check (see [`POST_HEAL_SLACK`]) so the pass is non-vacuous.
const LIVENESS_BUDGET: VirtualDuration = VirtualDuration::from_nanos(5_000_000_000);

/// How far past the budget the run advances virtual time after the heal before
/// the final [`LivenessChecker::check`]. Choosing more than the budget
/// guarantees that, had the favorable group stalled instead of converging, its
/// window would have exceeded the budget and the check *would* have flagged it —
/// so a clean pass is meaningful, not trivially-too-early.
const POST_HEAL_SLACK: VirtualDuration = VirtualDuration::from_nanos(3_000_000_000);

/// Build the run-configuration: a small cluster (3..=5 nodes, every partition
/// replicated on every node) under a **healthy** network (no drop / duplicate /
/// reorder / skew). The only adversity in the run is the deliberate
/// crash-then-heal of a strict minority, applied by the test body.
fn run_config(seed: u64, node_count: usize, partition_count: u32) -> RunConfig {
    RunConfig {
        seed,
        params: ScenarioParameters {
            node_count,
            replication_factor: node_count,
            partition_count,
            // Faults default to a healthy cluster (all probabilities 0.0), so a
            // majority always communicates and the group stays favorable.
            ..ScenarioParameters::default()
        },
    }
}

/// The partition group records are produced to.
fn partition_group() -> GroupKey {
    (TOPIC.to_string(), PARTITION)
}

/// The distinct payload for the `i`th produced record.
fn record_value(i: usize) -> Vec<u8> {
    format!("liveness-record-{i}").into_bytes()
}

/// Step the runtime until `pred` holds, the timeline ends, or `max_steps` is
/// reached, feeding the [`LivenessChecker`] an observation after **every** step
/// (and once before the first). Returns whether `pred` holds at the end.
fn step_observe_until(
    rt: &mut SimRuntime,
    checker: &mut LivenessChecker,
    max_steps: usize,
    mut pred: impl FnMut(&SimRuntime) -> bool,
) -> bool {
    checker.observe(rt.cluster(), rt.scheduler().now());
    if pred(rt) {
        return true;
    }
    for _ in 0..max_steps {
        match rt
            .step()
            .expect("event dispatch never fails in a healthy run")
        {
            Step::Event(_) => {
                checker.observe(rt.cluster(), rt.scheduler().now());
                if pred(rt) {
                    return true;
                }
            }
            Step::Done(_) => {
                checker.observe(rt.cluster(), rt.scheduler().now());
                return pred(rt);
            }
        }
    }
    pred(rt)
}

/// Seed an initial election timer for every node's `__meta/0` replica at the
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

/// Arm an election timer for `group` on every running node that hosts a replica
/// for it, so the partition group can start an election.
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

/// Every partition group of [`TOPIC`] (`partition 0 .. partition_count`). The
/// checker observes *all* of them, so every one must be driven to a leader and
/// kept favorable, not just the partition records are produced to.
fn all_partition_groups(partition_count: u32) -> Vec<GroupKey> {
    (0..partition_count)
        .map(|p| (TOPIC.to_string(), PartitionIndex(p)))
        .collect()
}

/// Whether every running node hosts a replica for `group`.
fn all_running_nodes_host(rt: &SimRuntime, group: &GroupKey) -> bool {
    rt.cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .all(|n| n.fleet_replicas().any(|(g, _)| g == group))
}

/// Whether every running node hosts a replica for every partition group.
fn all_running_nodes_host_all(rt: &SimRuntime, groups: &[GroupKey]) -> bool {
    groups.iter().all(|g| all_running_nodes_host(rt, g))
}

/// Whether every partition group currently has a running leader.
fn all_partitions_have_leader(rt: &SimRuntime, groups: &[GroupKey]) -> bool {
    groups
        .iter()
        .all(|g| partition_leader_index(rt, g).is_some())
}

/// Whether some running replica of `group` has `value` in its committed log.
fn committed_somewhere(rt: &SimRuntime, group: &GroupKey, value: &[u8]) -> bool {
    rt.cluster().nodes().iter().any(|node| {
        node.is_running()
            && node.fleet_replicas().any(|(g, replica)| {
                g == group && replica.read(0, usize::MAX).iter().any(|r| r.value == value)
            })
    })
}

/// Whether **every** running replica of `group` has `value` committed — i.e.
/// every up voter (including any just-recovered one) has caught up to it. This
/// is the observable convergence the liveness property requires of a favorable,
/// healed group (Requirement 12.4).
fn committed_everywhere(rt: &SimRuntime, group: &GroupKey, value: &[u8]) -> bool {
    rt.cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .filter(|n| n.fleet_replicas().any(|(g, _)| g == group))
        .all(|node| {
            node.fleet_replicas().any(|(g, replica)| {
                g == group && replica.read(0, usize::MAX).iter().any(|r| r.value == value)
            })
        })
}

/// A `CreateTopic` for [`TOPIC`] whose partitions carry the topology's fixed
/// replica sets.
fn create_topic_command(cluster: &SimulatedCluster, partition_count: u32) -> ClusterCommand {
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
        name: TOPIC.to_string(),
        partitions,
        backend: LogBackend::InMemory,
    }
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

/// Produce `value` to `group`: resolve the current partition leader (stepping to
/// one if a failover is in flight), propose the record, and step until it
/// commits on some running replica — observing the [`LivenessChecker`] after
/// every step. Returns `true` once the record is committed somewhere.
fn produce(
    rt: &mut SimRuntime,
    checker: &mut LivenessChecker,
    group: &GroupKey,
    value: Vec<u8>,
) -> bool {
    if !step_observe_until(rt, checker, MAX_PHASE_STEPS, |rt| {
        partition_leader_index(rt, group).is_some()
    }) {
        return false;
    }
    let Some(leader) = partition_leader_index(rt, group) else {
        return false;
    };
    let payload = EntryPayload::new(PayloadKind::Record, value.clone());
    if !propose_and_route(rt, leader, group, payload) {
        return false;
    }
    step_observe_until(rt, checker, MAX_PHASE_STEPS, |rt| {
        committed_somewhere(rt, group, &value)
    })
}

/// A strict-minority crash subset that never includes `leader`: the first
/// `floor((node_count - 1) / 2)` non-leader indices. A majority (and the
/// partition leader) always survives, so the group stays favorable throughout.
fn minority_excluding_leader(node_count: usize, leader: usize) -> Vec<usize> {
    let minority = node_count.saturating_sub(1) / 2;
    (0..node_count)
        .filter(|&i| i != leader)
        .take(minority)
        .collect()
}

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // election + produce + crash/heal run brisk while covering a broad
    // seed / shape space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 21: Liveness under
    // healed faults
    #[test]
    fn favorable_group_makes_progress_within_budget_after_heal(
        seed in any::<u64>(),
        node_count in 3usize..=5,
        partition_count in 1u32..=2,
    ) {
        let config = run_config(seed, node_count, partition_count);
        let cluster = SimulatedCluster::new(config).expect("a valid cluster shape assembles");
        let mut rt = SimRuntime::new(cluster, Budget::default());
        let group = partition_group();
        let groups = all_partition_groups(partition_count);
        let mut checker = LivenessChecker::new(LIVENESS_BUDGET);

        // 1. Bootstrap the metadata group and step to a leader.
        seed_meta_elections(&mut rt);
        let meta_elected =
            step_observe_until(&mut rt, &mut checker, MAX_PHASE_STEPS, |rt| {
                meta_leader_index(rt).is_some()
            });
        prop_assert!(meta_elected, "the metadata group must elect a leader");
        let meta_leader = meta_leader_index(&rt).expect("a metadata leader was elected");

        // 2. Commit a `CreateTopic`; reconcile spawns every partition's replicas
        //    on every assigned node (12.3: a topic admin command commits).
        let create = create_topic_command(rt.cluster(), partition_count);
        let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&create));
        let proposed = propose_and_route(&mut rt, meta_leader, &metadata_group_key(), payload);
        prop_assert!(proposed, "the metadata leader must accept the CreateTopic proposal");
        let reconciled = step_observe_until(&mut rt, &mut checker, MAX_PHASE_STEPS, |rt| {
            all_running_nodes_host_all(rt, &groups)
        });
        prop_assert!(
            reconciled,
            "the committed CreateTopic must reconcile every partition's replicas onto every node"
        );

        // 3. Bootstrap *every* partition group and step until each has a leader.
        //    The checker observes all of them, so each must be driven favorable
        //    and made to elect (12.1: exactly one leader is elected per group).
        for g in &groups {
            seed_partition_elections(&mut rt, g);
        }
        let all_elected =
            step_observe_until(&mut rt, &mut checker, MAX_PHASE_STEPS, |rt| {
                all_partitions_have_leader(rt, &groups)
            });
        prop_assert!(all_elected, "every partition group must elect a leader");

        // 4. Acknowledge a produce before any fault (12.2: a produced record
        //    commits under favorable conditions).
        let acked_pre = produce(&mut rt, &mut checker, &group, record_value(0));
        prop_assert!(acked_pre, "a record must commit before any fault is applied");

        // 5. Apply a fault: crash a strict minority (never the leader, so a
        //    majority — and the leader — survive and the group stays favorable).
        //    Note the fault instant so the budget clock restarts (Req 6.6/12.6:
        //    a minority loss never demands progress of the *lost* voters, while
        //    the surviving majority remains favorable).
        let leader = partition_leader_index(&rt, &group)
            .expect("a partition leader exists after the first produce");
        let crashed = minority_excluding_leader(node_count, leader);
        if !crashed.is_empty() {
            let crashed_count = rt.cluster_mut().crash_nodes(&crashed);
            prop_assert_eq!(
                crashed_count, crashed.len(),
                "the intended strict-minority crash must take effect"
            );
            checker.note_fault(rt.scheduler().now());
        }

        // Produce again on the surviving majority: a favorable group must still
        // make progress while the minority is down (12.2).
        let acked_during = produce(&mut rt, &mut checker, &group, record_value(1));
        prop_assert!(
            acked_during,
            "a record must commit on the surviving majority while a minority is crashed"
        );

        // 6. Heal: restart the crashed minority through the real WAL-recovery
        //    path and note the heal instant (the budget for the now-recovering
        //    group is measured from here).
        if !crashed.is_empty() {
            rt.cluster_mut()
                .restart_nodes(&crashed)
                .expect("a crashed minority restarts cleanly from its retained disk");
            checker.note_heal(rt.scheduler().now());
        }
        let heal_at = rt.scheduler().now();

        // Produce a third record after the heal (12.2/12.3 exercised post-heal)
        // and let every running replica — including the just-recovered minority —
        // commit it, demonstrating the lagging replicas converge (12.4).
        let acked_post = produce(&mut rt, &mut checker, &group, record_value(2));
        prop_assert!(acked_post, "a record must commit after the heal");
        let converged = step_observe_until(&mut rt, &mut checker, MAX_PHASE_STEPS, |rt| {
            committed_everywhere(rt, &group, &record_value(2))
        });
        prop_assert!(
            converged,
            "every running replica (including the recovered minority) must converge on the \
             post-heal record (Requirement 12.4)"
        );

        // 7. Advance virtual time well past the budget after the heal so the
        //    pass is meaningful: a favorable group that had stalled would by now
        //    have exceeded its budget and been flagged.
        let deadline = heal_at
            .saturating_add(LIVENESS_BUDGET)
            .saturating_add(POST_HEAL_SLACK);
        let reached_deadline =
            step_observe_until(&mut rt, &mut checker, MAX_PHASE_STEPS, |rt| {
                rt.scheduler().now() >= deadline
            });
        prop_assert!(
            reached_deadline,
            "the run must advance virtual time past the budget so the liveness pass is non-vacuous"
        );

        // Non-vacuity: the group genuinely reached a converged, favorable state —
        // a leader exists and all three records are present on every running
        // replica.
        prop_assert!(
            partition_leader_index(&rt, &group).is_some(),
            "the favorable group must hold a leader at the end of the run"
        );
        for i in 0..3 {
            prop_assert!(
                committed_everywhere(&rt, &group, &record_value(i)),
                "record {} must be committed on every running replica (non-vacuity)",
                i
            );
        }
        // The favorable window must have been open longer than the budget by the
        // end, so a real stall would have been catchable.
        prop_assert!(
            rt.scheduler().now().duration_since(heal_at).as_nanos() > LIVENESS_BUDGET.as_nanos(),
            "more than the budget of virtual time must have elapsed since the heal"
        );

        // Final assertion: no Liveness violation — the favorable, healed group
        // made the required progress within the bounded budget (Property 21).
        let now = rt.scheduler().now();
        let result = checker.check(now);
        prop_assert!(
            result.is_ok(),
            "a favorable, healed group must not raise a Liveness violation, but got: {:?}",
            result
        );
    }
}
