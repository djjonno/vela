#![cfg(feature = "sim")]
//! Property test for offset integrity in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 17: Offset integrity
//!
//! Property 17 (Requirement 11.3): *for any* seed and any small generated
//! cluster — driven through a real metadata election, a committed `CreateTopic`,
//! a real partition election, and a stream of produces that commit across a
//! follower crash/restart under an adverse (drop / duplicate / reorder) network
//! — a partition's committed offsets are **contiguous from 0, strictly
//! increasing, with no gaps**, and no offset is ever acknowledged for two
//! distinct records. The check is structural: the production
//! [`KafkaParityChecker`] takes a read-only view of the partition's committed
//! log (the most-advanced running replica) plus every acknowledged produce
//! recorded in the [`History`], and reports a [`PropertyId::OffsetIntegrity`]
//! [`Violation`] the instant the committed offsets are non-contiguous or an
//! offset is reused for two distinct acknowledged values.
//!
//! The run is driven with the real harness: a [`SimRuntime`] over a
//! [`SimulatedCluster`], bootstrapped exactly as the run orchestration will
//! (seed each `__meta/0` replica's first election timer), then stepped one
//! discrete event at a time. After the metadata group commits a `CreateTopic`
//! and reconcile starts the partition replicas on every assigned node, the test
//! arms the partition group's election timers, drives it to a single leader,
//! and then produces a stream of records — each through the real
//! `Propose -> replicate -> commit` path — recording every acknowledged produce
//! (its committed offset, read back from the partition's committed log) into the
//! [`History`]. A strict minority of *followers* (never the leader) is crashed
//! mid-stream and restarted later, so the leader stays available and produces
//! keep committing while the recovered replicas catch up through the real
//! WAL-recovery + replication path — exactly the churn under which offsets must
//! stay gap-free and unique. The network drops, duplicates, and reorders
//! messages throughout.
//!
//! Because [`KafkaParityChecker::check`] returns the *first* violation of *any*
//! Kafka-parity property (durability, offset integrity, consume validity,
//! linearizability, convergence), the assertion is scoped to Property 17: any
//! returned violation must not be [`PropertyId::OffsetIntegrity`]. The other
//! properties have their own dedicated tests. The run also asserts at least one
//! record was acknowledged, so the property is checked over a non-empty log.
//!
//! The whole run draws every random decision from the one seed and is
//! single-threaded, so it is fully deterministic and never flakes.
//!
//! Validates: Requirements 11.3

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, CommittedRecord, GroupKey, LogBackend, NodeId, Partition,
    PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::kafka_parity::KafkaParityChecker;
use vela_sim::checker::PropertyId;
use vela_sim::cluster::{SimulatedCluster, Topology};
use vela_sim::codec::encode_cluster_command;
use vela_sim::history::{History, OpArgs};
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, FaultIntensities, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualInstant};

/// The topic produced to during the run.
const TOPIC: &str = "orders";

/// The partition every produce in the run targets (the checker only inspects
/// partitions the [`History`] references, so a single focused partition keeps
/// the run brisk while fully exercising offset integrity).
const PARTITION: PartitionIndex = PartitionIndex(0);

/// How many records the run produces. Spread across a healthy prefix, a
/// follower-crash window, and a post-restart tail so offsets must stay
/// contiguous and unique across churn.
const NUM_PRODUCES: usize = 8;

/// Produce indices at which the seed-chosen follower minority is crashed and
/// then restarted. Both leave a healthy prefix of acknowledged produces (so the
/// run is never vacuous) and bracket a window of churn.
const CRASH_BEFORE: usize = 3;
const RESTART_BEFORE: usize = 6;

/// Step budgets for each driving phase. Each step processes exactly one discrete
/// event; the budgets are comfortably within the run's event budget and leave
/// ample room to elect leaders and commit produces even under an adverse
/// network.
const ELECT_STEPS: usize = 4_000;
const CREATE_STEPS: usize = 4_000;
const COMMIT_STEPS: usize = 4_000;

/// Build the run-configuration: a small cluster (3–5 nodes, every partition
/// replicated on every node so a majority always survives a follower-minority
/// crash) under an adverse network whose drop / duplicate / reorder intensities
/// come from the generated probabilities.
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

/// Seed an initial election timer for every node's `__meta/0` replica at the
/// origin, so the metadata group can start an election — exactly what the run
/// orchestration does at bootstrap (mirrors the runtime's own
/// `seed_meta_elections`).
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
        // Arm via the `Clock` seam so the generation matches `is_current`.
        rt.cluster_mut()
            .clock_mut()
            .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
    }
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
}

/// Arm an election timer for `group` on every running node that replicates it,
/// at the current instant, so the partition group can start an election — the
/// orchestration's analogue of seeding the first election timer for a
/// freshly-spawned replica.
fn seed_partition_elections(rt: &mut SimRuntime, group: &GroupKey) {
    let now = rt.scheduler().now();
    let node_ids: Vec<NodeId> = rt
        .cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running() && rt.cluster().topology().group_contains(group, n.id()))
        .map(|n| n.id().clone())
        .collect();
    for node in node_ids {
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

/// The index of a running node currently leading `group`'s partition replica,
/// if any (Election Safety guarantees at most one per term).
fn partition_leader_index(rt: &SimRuntime, group: &GroupKey) -> Option<usize> {
    rt.cluster().nodes().iter().position(|n| {
        n.is_running()
            && n.fleet_replicas()
                .any(|(g, replica)| g == group && replica.role() == Role::Leader)
    })
}

/// The partition's committed log as the checker observes it: the longest
/// committed prefix held by any running replica of `group`.
fn committed_records(rt: &SimRuntime, group: &GroupKey) -> Vec<CommittedRecord> {
    let mut best: Vec<CommittedRecord> = Vec::new();
    for node in rt.cluster().nodes() {
        if !node.is_running() {
            continue;
        }
        for (replica_group, replica) in node.fleet_replicas() {
            if replica_group == group {
                let records = replica.read(0, usize::MAX);
                if records.len() > best.len() {
                    best = records;
                }
            }
        }
    }
    best
}

/// Step the runtime until `pred` holds or `max_steps` are exhausted (or the
/// timeline ends), returning whether `pred` ultimately held.
fn drive_until(
    rt: &mut SimRuntime,
    max_steps: usize,
    mut pred: impl FnMut(&SimRuntime) -> bool,
) -> bool {
    for _ in 0..max_steps {
        if pred(rt) {
            return true;
        }
        match rt
            .step()
            .expect("event dispatch never fails in a healthy run")
        {
            Step::Event(_) => {}
            Step::Done(_) => return pred(rt),
        }
    }
    pred(rt)
}

/// A `CreateTopic` for [`TOPIC`] whose partitions carry the topology's fixed
/// replica sets — the catalogue the metadata group commits and replicates.
fn create_topic_command(topo: &Topology, partition_count: u32) -> ClusterCommand {
    let partitions = (0..partition_count)
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
        name: TOPIC.to_string(),
        partitions,
        backend: LogBackend::InMemory,
    }
}

/// Propose `input` to the replica of `group` on the node at `index` and route
/// the follow-on effects the propose produced back onto the scheduler timeline
/// (the sends through the replica's transport, the re-armed timers, and the
/// buffered deliveries), so the subsequent step loop replicates and commits the
/// entry. Mirrors the runtime's own per-step dispatch exactly.
fn propose_and_route(rt: &mut SimRuntime, index: usize, group: &GroupKey, input: RaftInput) {
    let now = rt.scheduler().now();
    rt.cluster_mut().network().set_now(now);
    let Some(out) = rt.cluster_mut().step_replica(index, group, now, input) else {
        return;
    };
    // Dispatch the resulting sends through this replica's transport (cheap `Rc`
    // clone to release the cluster borrow first), exactly as the runtime does.
    if let Some(transport) = rt.cluster().transport_for(index, group) {
        let transport = transport.clone();
        for (to, msg) in out.sends {
            transport.send(to, msg);
        }
    }
    // Schedule the timers this step (re-)armed, then the deliveries the sends
    // just buffered.
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
    for (at, envelope) in rt.cluster().network().drain_pending() {
        rt.scheduler_mut()
            .schedule(at, Event::MessageDeliver(envelope));
    }
}

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // produce-with-crash/restart-under-faults run brisk while covering a broad
    // seed / shape / fault-intensity space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 17: Offset integrity
    #[test]
    fn committed_offsets_are_contiguous_and_unique(
        seed in any::<u64>(),
        node_count in 3usize..=5,
        partition_count in 1u32..=2,
        drop_prob in 0.0f64..0.05,
        duplicate_prob in 0.0f64..0.03,
        reorder_prob in 0.0f64..0.05,
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

        let partition_group: GroupKey = (TOPIC.to_string(), PARTITION);
        let mut history = History::new();

        // --- Bootstrap: elect a metadata leader. ---
        seed_meta_elections(&mut rt);
        let elected_meta =
            drive_until(&mut rt, ELECT_STEPS, |rt| meta_leader_index(rt).is_some());
        prop_assert!(elected_meta, "the metadata group must elect a leader");
        let meta_leader = meta_leader_index(&rt).expect("a metadata leader exists");

        // --- Commit a CreateTopic and let reconcile start the partition replicas. ---
        let command = create_topic_command(rt.cluster().topology(), partition_count);
        let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&command));
        propose_and_route(&mut rt, meta_leader, &metadata_group_key(), RaftInput::Propose(payload));
        let started = drive_until(&mut rt, CREATE_STEPS, |rt| {
            rt.cluster()
                .nodes()
                .iter()
                .all(|n| n.transport(&partition_group).is_some())
        });
        prop_assert!(started, "every assigned node must start the partition replica");

        // --- Elect a partition leader. ---
        seed_partition_elections(&mut rt, &partition_group);
        let elected_partition = drive_until(&mut rt, ELECT_STEPS, |rt| {
            partition_leader_index(rt, &partition_group).is_some()
        });
        prop_assert!(elected_partition, "the partition group must elect a leader");

        // Choose a strict minority of *followers* (never the leader) to crash, so
        // the leader stays available and produces keep committing across the
        // churn while the crashed replicas later recover and catch up.
        let leader0 = partition_leader_index(&rt, &partition_group)
            .expect("a partition leader exists");
        let minority = (node_count - 1) / 2;
        let crash_subset: Vec<usize> = (0..node_count)
            .filter(|&i| i != leader0)
            .take(minority)
            .collect();

        let mut acked = 0usize;
        let mut crashed = false;
        let mut restarted = false;

        // --- Produce a stream of distinct records, recording each ack's offset. ---
        for i in 0..NUM_PRODUCES {
            // Crash a follower minority mid-stream, then restart it later.
            if !crashed && i == CRASH_BEFORE && !crash_subset.is_empty() {
                rt.cluster_mut().crash_nodes(&crash_subset);
                crashed = true;
            }
            if crashed && !restarted && i == RESTART_BEFORE {
                rt.cluster_mut()
                    .restart_nodes(&crash_subset)
                    .expect("a crashed follower minority restarts cleanly from its retained disk");
                restarted = true;
            }

            // Re-resolve the current leader (re-electing if a transient loss
            // occurred), then skip this produce if none is available.
            drive_until(&mut rt, ELECT_STEPS, |rt| {
                partition_leader_index(rt, &partition_group).is_some()
            });
            let Some(leader) = partition_leader_index(&rt, &partition_group) else {
                continue;
            };

            // Each record carries a globally distinct value, so a reused offset
            // (two distinct acknowledged records at one offset) is observable.
            let value = format!("offset-integrity-record-{i}").into_bytes();
            let before = committed_records(&rt, &partition_group).len();
            let invoked_at = rt.scheduler().now();
            let payload = EntryPayload::new(PayloadKind::Record, value.clone());
            propose_and_route(&mut rt, leader, &partition_group, RaftInput::Propose(payload));

            // Drive until the produced record commits (the committed log grows).
            drive_until(&mut rt, COMMIT_STEPS, |rt| {
                committed_records(rt, &partition_group).len() > before
            });

            // If it committed, acknowledge it at its real committed offset.
            let records = committed_records(&rt, &partition_group);
            if let Some(record) = records.iter().find(|r| r.value == value) {
                let responded_at = rt.scheduler().now();
                history.record_produce_success(
                    OpArgs::Produce {
                        topic: TOPIC.to_string(),
                        partition: PARTITION,
                        key: None,
                        value: value.clone(),
                    },
                    invoked_at,
                    responded_at,
                    record.offset,
                );
                acked += 1;
            }
        }

        // The run must acknowledge at least one record, so offset integrity is
        // checked over a non-empty committed log (non-vacuous).
        prop_assert!(
            acked >= 1,
            "the run must acknowledge at least one produced record (acked={acked})"
        );

        // --- Final pass: offset integrity must hold. ---
        let now = rt.scheduler().now();
        let result = KafkaParityChecker::new().check(rt.cluster(), &history, now);
        if let Err(violation) = result {
            // `check` returns the first violation of ANY Kafka-parity property;
            // this test is scoped to Property 17, so only an OffsetIntegrity
            // breach is a failure here (the rest have their own tests).
            prop_assert_ne!(
                violation.property,
                PropertyId::OffsetIntegrity,
                "offset integrity must never be violated, but a breach was reported: {}",
                violation
            );
        }
    }
}
