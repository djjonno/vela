#![cfg(feature = "sim")]
//! Property test for Acknowledged-record durability in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 16: Acknowledged-record
//! durability
//!
//! Property 16: *For any* seed and any small generated cluster — driven through
//! a real metadata election, a committed `CreateTopic`, a real per-partition
//! election, a sequence of acknowledged produces, and a strict-minority
//! crash/restart mid-stream, all under a bounded adverse (drop / duplicate /
//! reorder) network — every record the cluster acknowledges to the client
//! appears in that partition's committed log at the offset it was returned, and
//! remains there for the rest of the run (acknowledged-record durability;
//! Requirements 7.6, 11.1, 11.2). A minority of a partition's Replica_Set may
//! crash and restart at any point, but because a majority always survives, no
//! acknowledged record can be lost.
//!
//! The run uses the real harness: a [`SimRuntime`] over a [`SimulatedCluster`]
//! of production replicas, stepped one discrete event at a time. It is driven
//! through the same path the run orchestration takes —
//!
//! 1. seed every `__meta/0` replica's first election timer and step to a
//!    metadata leader;
//! 2. commit a `CreateTopic` through that leader, so reconcile spawns the
//!    partition's replicas on every assigned node;
//! 3. seed the partition group's election timers and step to a partition
//!    leader;
//! 4. produce records by proposing `Record` entries to the partition leader,
//!    routing the effects back onto the scheduler timeline, and stepping until
//!    each commits — recording every acknowledged produce (its returned offset
//!    and value) into the [`History`];
//! 5. crash a strict minority (never the partition leader, so a majority and the
//!    leader survive) part-way through, produce the remainder, then restart the
//!    minority through the real WAL-recovery path.
//!
//! At the end the production [`KafkaParityChecker`] takes a read-only snapshot
//! of every running replica's committed log and, against the recorded
//! [`History`], asserts every acknowledged record is still present at its
//! offset. The checker observes a partition's committed log from its
//! most-advanced *running* replica, so a crashed minority cannot hide an
//! acknowledged record (a surviving majority still holds it). `check` reports
//! the first violation of *any* Kafka-parity property; this test scopes its
//! assertion to [`PropertyId::AcknowledgedRecordDurability`] (the sibling
//! offset / consume / linearizability / convergence properties have their own
//! tests). The whole run draws every random decision from the one seed and is
//! single-threaded, so it is fully deterministic and never flakes.
//!
//! Validates: Requirements 7.6, 11.1, 11.2

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, GroupKey, LogBackend, NodeId, Offset, Partition,
    PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::kafka_parity::KafkaParityChecker;
use vela_sim::checker::PropertyId;
use vela_sim::cluster::SimulatedCluster;
use vela_sim::codec::encode_cluster_command;
use vela_sim::history::{History, OpArgs};
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, FaultIntensities, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualInstant};

/// The topic produced to during the run.
const TOPIC: &str = "orders";

/// The partition records are produced to (partition 0 of [`TOPIC`]).
const PARTITION: PartitionIndex = PartitionIndex(0);

/// Upper bound on the discrete events any single "step until …" phase drives.
/// Heartbeats re-arm continuously, so the timeline never quiesces; this cap
/// keeps each phase bounded while leaving ample room — well within the run's
/// event budget — to elect a leader, commit a `CreateTopic`, elect a partition
/// leader, and commit a produce even when the network drops and retries it.
const MAX_PHASE_STEPS: usize = 40_000;

/// The number of records produced to the partition during the run.
const RECORD_COUNT: usize = 4;

/// Build the run-configuration: a small cluster (3..=5 nodes, every partition
/// replicated on every node) under a bounded adverse network whose drop /
/// duplicate / reorder intensities come from the generated probabilities.
fn run_config(
    seed: u64,
    node_count: usize,
    partition_count: u32,
    drop_prob: f64,
    duplicate_prob: f64,
    reorder_prob: f64,
) -> RunConfig {
    RunConfig {
        seed,
        params: ScenarioParameters {
            node_count,
            replication_factor: node_count,
            partition_count,
            faults: FaultIntensities {
                drop_prob,
                duplicate_prob,
                reorder_prob,
                ..FaultIntensities::default()
            },
            ..ScenarioParameters::default()
        },
    }
}

/// The partition group records are produced to.
fn partition_group() -> GroupKey {
    (TOPIC.to_string(), PARTITION)
}

/// The distinct payload for the `i`th produced record. Distinct values let the
/// checker confirm each acknowledged record by value, not just by offset.
fn record_value(i: usize) -> Vec<u8> {
    format!("durable-record-{i}").into_bytes()
}

/// Step the runtime until `pred` holds, the timeline ends, or `max_steps` is
/// reached; returns whether `pred` holds at the end. One `step()` processes one
/// event and enqueues all its follow-on effects, so this drives the real
/// dispatch loop exactly as the run orchestration does.
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

/// Seed an initial election timer for every node's `__meta/0` replica at the
/// origin, so the metadata group can start an election — exactly what the run
/// orchestration does at bootstrap.
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
/// for it, so the partition group can start an election (the analogue of
/// [`seed_meta_elections`] for a client partition, which reconcile spawns
/// without an initial election timer).
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

/// Whether every running node hosts a replica for `group` (the reconcile spawn
/// has landed on every assigned node — all nodes here, since replication factor
/// equals the node count).
fn all_running_nodes_host(rt: &SimRuntime, group: &GroupKey) -> bool {
    rt.cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .all(|n| n.fleet_replicas().any(|(g, _)| g == group))
}

/// The committed offset of `value` in `group`, observed from any running
/// replica's committed log, or `None` if no running replica has committed it
/// yet. Committed logs agree across replicas (State Machine Safety), so the
/// offset found is authoritative.
fn committed_offset_of(rt: &SimRuntime, group: &GroupKey, value: &[u8]) -> Option<Offset> {
    for node in rt.cluster().nodes() {
        if !node.is_running() {
            continue;
        }
        for (g, replica) in node.fleet_replicas() {
            if g == group {
                for record in replica.read(0, usize::MAX) {
                    if record.value == value {
                        return Some(record.offset);
                    }
                }
            }
        }
    }
    None
}

/// A `CreateTopic` for [`TOPIC`] whose partitions carry the topology's fixed
/// replica sets — the catalogue the metadata group commits and reconciles into
/// partition replicas.
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
/// the follow-on effects the propose produced back onto the scheduler timeline:
/// the leader's `out.sends` through its transport, the timers it (re-)armed, and
/// the deliveries those sends buffered — exactly as the runtime's per-step
/// dispatch does. Returns `false` if the node did not step the replica (it is
/// not hosting the group or is crashed). The commit itself lands through the
/// subsequent step loop as the replication rounds complete.
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
    // Dispatch the proposal's sends through the leader's transport (cheap `Rc`
    // clone to release the cluster borrow first).
    if let Some(transport) = rt.cluster().transport_for(index, group) {
        let transport = transport.clone();
        for (to, msg) in out.sends {
            transport.send(to, msg);
        }
    }
    // Schedule the timers this step (re-)armed, then the deliveries the sends
    // buffered, exactly as the runtime's per-step dispatch does.
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
/// commits. On commit, record the acknowledged produce — its returned offset and
/// value — into `history` and return `true`. Returns `false` if no leader could
/// be resolved or the record did not commit within the step budget (an expected,
/// non-acknowledged outcome that is simply not recorded).
fn produce(rt: &mut SimRuntime, group: &GroupKey, history: &mut History, value: Vec<u8>) -> bool {
    // Resolve a leader to accept the produce.
    if !step_until(rt, MAX_PHASE_STEPS, |rt| {
        partition_leader_index(rt, group).is_some()
    }) {
        return false;
    }
    let Some(leader) = partition_leader_index(rt, group) else {
        return false;
    };

    let invoked_at = rt.scheduler().now();
    let payload = EntryPayload::new(PayloadKind::Record, value.clone());
    if !propose_and_route(rt, leader, group, payload) {
        return false;
    }

    // Step until the record is observed committed on some running replica.
    if !step_until(rt, MAX_PHASE_STEPS, |rt| {
        committed_offset_of(rt, group, &value).is_some()
    }) {
        return false;
    }

    let offset = committed_offset_of(rt, group, &value).expect("the record just committed");
    let responded_at = rt.scheduler().now();
    history.record_produce_success(
        OpArgs::Produce {
            topic: TOPIC.to_string(),
            partition: PARTITION,
            key: None,
            value,
        },
        invoked_at,
        responded_at,
        offset,
    );
    true
}

/// A strict-minority crash subset that never includes `leader`: the first
/// `floor((node_count - 1) / 2)` non-leader indices. A majority (and the
/// partition leader) always survives, so the group stays available and no
/// acknowledged record can be lost (Requirement 11.2).
fn minority_excluding_leader(node_count: usize, leader: usize) -> Vec<usize> {
    let minority = node_count.saturating_sub(1) / 2;
    (0..node_count)
        .filter(|&i| i != leader)
        .take(minority)
        .collect()
}

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // election + produce + crash/restart run brisk while covering a broad
    // seed / shape / fault-intensity space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 16: Acknowledged-record
    // durability
    #[test]
    fn every_acknowledged_record_survives_to_its_returned_offset(
        seed in any::<u64>(),
        node_count in 3usize..=5,
        partition_count in 1u32..=2,
        drop_prob in 0.0f64..0.05,
        duplicate_prob in 0.0f64..0.05,
        reorder_prob in 0.0f64..0.10,
    ) {
        let config = run_config(
            seed,
            node_count,
            partition_count,
            drop_prob,
            duplicate_prob,
            reorder_prob,
        );
        let cluster = SimulatedCluster::new(config).expect("a valid cluster shape assembles");
        let mut rt = SimRuntime::new(cluster, Budget::default());
        let group = partition_group();
        let mut history = History::new();

        // 1. Bootstrap the metadata group and step to a leader.
        seed_meta_elections(&mut rt);
        let meta_elected = step_until(&mut rt, MAX_PHASE_STEPS, |rt| meta_leader_index(rt).is_some());
        prop_assert!(meta_elected, "the metadata group must elect a leader");
        let meta_leader = meta_leader_index(&rt).expect("a metadata leader was elected");

        // 2. Commit a `CreateTopic` through the metadata leader; reconcile spawns
        //    the partition's replicas on every assigned node.
        let create = create_topic_command(rt.cluster(), partition_count);
        let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&create));
        let proposed = propose_and_route(&mut rt, meta_leader, &metadata_group_key(), payload);
        prop_assert!(proposed, "the metadata leader must accept the CreateTopic proposal");
        let reconciled =
            step_until(&mut rt, MAX_PHASE_STEPS, |rt| all_running_nodes_host(rt, &group));
        prop_assert!(
            reconciled,
            "the committed CreateTopic must reconcile partition replicas onto every node"
        );

        // 3. Bootstrap the partition group and step to a partition leader.
        seed_partition_elections(&mut rt, &group);
        let partition_elected =
            step_until(&mut rt, MAX_PHASE_STEPS, |rt| partition_leader_index(rt, &group).is_some());
        prop_assert!(partition_elected, "the partition group must elect a leader");

        // 4 & 5. Produce records, crashing a strict minority (never the leader)
        //        part-way through so durability is exercised under minority
        //        failure (Requirement 11.2).
        let crash_after = RECORD_COUNT / 2;
        let mut crashed: Vec<usize> = Vec::new();
        let mut acked = 0usize;
        for i in 0..RECORD_COUNT {
            if i == crash_after {
                if let Some(leader) = partition_leader_index(&rt, &group) {
                    let subset = minority_excluding_leader(node_count, leader);
                    if !subset.is_empty() {
                        rt.cluster_mut().crash_nodes(&subset);
                        crashed = subset;
                    }
                }
            }
            if produce(&mut rt, &group, &mut history, record_value(i)) {
                acked += 1;
            }
        }

        // Restart the crashed minority through the real WAL-recovery path; the
        // recovered nodes rejoin and catch up. Durability must hold across the
        // restart (the surviving majority — and the leader — never lost a record).
        if !crashed.is_empty() {
            rt.cluster_mut()
                .restart_nodes(&crashed)
                .expect("a crashed minority restarts cleanly from its retained disk");
            // Let the recovered nodes catch up on the committed prefix.
            let _ = step_until(&mut rt, MAX_PHASE_STEPS, |_| false);
        }

        // The run must have acknowledged at least one record, or the durability
        // assertion would be vacuous.
        prop_assert!(
            acked > 0,
            "the run must acknowledge at least one produced record (non-vacuity)"
        );

        // Final pass: no acknowledged record may be lost or overwritten. `check`
        // returns the first violation of *any* Kafka-parity property; this test
        // scopes its assertion to acknowledged-record durability.
        let now = rt.scheduler().now();
        let result = KafkaParityChecker::new().check(rt.cluster(), &history, now);
        if let Err(violation) = &result {
            prop_assert_ne!(
                violation.property,
                PropertyId::AcknowledgedRecordDurability,
                "acknowledged-record durability must never be violated, but a breach was \
                 reported: {:?}",
                violation
            );
        }
    }
}
