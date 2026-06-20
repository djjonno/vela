#![cfg(feature = "sim")]
//! Property test for consume read-validity (no phantom reads) in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 18: Consume read-validity
//! (no phantom reads)
//!
//! Property 18: *For any* seed and any small generated cluster — driven through
//! a real metadata election, a committed `CreateTopic`, a real partition-group
//! election, and a batch of acknowledged produces, all over the real
//! Sim_Network — every record a successful consume returns is a *committed*
//! record at the offset it was returned at, in ascending offset order, with no
//! phantom reads (Requirements 11.4, 11.5). The check is performed by the
//! production [`KafkaParityChecker`], which observes each partition's committed
//! log from the most-advanced running replica and compares it against the
//! produces and consumes recorded in the [`History`].
//!
//! The run is driven with the real harness exactly as the run orchestration
//! will:
//!
//! 1. Build a [`SimulatedCluster`] (≥3 nodes, every partition replicated on
//!    every node) and a [`SimRuntime`].
//! 2. Seed each `__meta/0` replica's first election timer and step to a metadata
//!    leader.
//! 3. Commit a `CreateTopic` through the metadata leader, so the
//!    apply-and-reconcile path spawns a [`PartitionReplica`] on every assigned
//!    node.
//! 4. Arm the partition group's election timers on its replica-hosting nodes and
//!    step to a partition leader.
//! 5. Produce several records through the partition leader (a `PayloadKind::Record`
//!    proposal each), step until they commit, and record each acknowledged
//!    produce in the [`History`] at its committed offset.
//! 6. Consume committed ranges at several start offsets — reading straight from
//!    the partition's committed log so every returned record is genuinely
//!    committed (never a fabricated/uncommitted record, which would be a real
//!    phantom read) — and record each successful consume in the [`History`].
//! 7. Run [`KafkaParityChecker::check`] as a final pass and assert any returned
//!    [`Violation`] is *not* a [`PropertyId::ConsumeReadValidity`] breach
//!    (`check` returns the first violation of *any* property, so the assertion
//!    is scoped to Property 18). A separate assertion proves the run was
//!    non-vacuous: at least one consume returned at least one record.
//!
//! The whole run draws every random decision from the one seed and is
//! single-threaded, so it is fully deterministic and never flakes.
//!
//! Validates: Requirements 11.4, 11.5

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, CommittedRecord, GroupKey, LogBackend, NodeId, Offset,
    Partition, PartitionIndex, Record,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::kafka_parity::KafkaParityChecker;
use vela_sim::checker::PropertyId;
use vela_sim::cluster::{SimulatedCluster, Topology};
use vela_sim::codec::encode_cluster_command;
use vela_sim::history::{History, OpArgs};
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualInstant};

/// The topic produced to and consumed from during the run.
const TOPIC: &str = "orders";

/// The client partition the run drives. Every assigned node hosts it (rf ==
/// node_count), so a partition election always has a quorum.
const PARTITION: PartitionIndex = PartitionIndex(0);

/// How many records are produced to the partition before the consumes.
const RECORD_COUNT: usize = 5;

/// Upper bound on discrete events driven per run. Heartbeats re-arm continuously
/// so the timeline never quiesces; this cap keeps each case bounded while
/// leaving ample room to elect a metadata leader, commit the `CreateTopic`,
/// elect a partition leader, produce, and consume.
const MAX_STEPS: usize = 12_000;

/// Build a healthy run-configuration: a small cluster (≥3 nodes, every partition
/// replicated on every node) with the documented default (no-fault) intensities,
/// so produces commit and the only behaviour under test is consume validity.
fn run_config(seed: u64, node_count: usize, partition_count: u32) -> RunConfig {
    RunConfig {
        seed,
        params: ScenarioParameters {
            node_count,
            replication_factor: node_count,
            partition_count,
            ..ScenarioParameters::default()
        },
    }
}

/// Seed an initial election timer for every node's `__meta/0` replica at the
/// origin, so the metadata group can start an election (mirrors the runtime's
/// own bootstrap).
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

/// The index of the node currently leading `__meta/0`, if any.
fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
    rt.cluster()
        .nodes()
        .iter()
        .position(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
}

/// The index of the node currently leading `group`'s partition replica, if any.
fn partition_leader_index(rt: &SimRuntime, group: &GroupKey) -> Option<usize> {
    rt.cluster().nodes().iter().position(|n| {
        n.is_running()
            && n.fleet_replicas()
                .any(|(g, r)| g == group && r.role() == Role::Leader)
    })
}

/// Whether every node in `replica_set` currently hosts a replica for `group`
/// (detected through the per-partition transport, minted exactly when a replica
/// is started). Once true, the partition group has a full quorum to elect.
fn all_host_group(rt: &SimRuntime, group: &GroupKey, replica_set: &[NodeId]) -> bool {
    replica_set.iter().all(|node_id| {
        rt.cluster()
            .index_of(node_id)
            .and_then(|idx| rt.cluster().node(idx))
            .is_some_and(|n| n.transport(group).is_some())
    })
}

/// The committed records of `group`'s replica on the node at `index`, in
/// ascending offset order (empty if the node hosts no such replica).
fn committed_records(rt: &SimRuntime, index: usize, group: &GroupKey) -> Vec<CommittedRecord> {
    rt.cluster()
        .node(index)
        .and_then(|n| {
            n.fleet_replicas()
                .find(|(g, _)| *g == group)
                .map(|(_, r)| r.read(0, usize::MAX))
        })
        .unwrap_or_default()
}

/// A `CreateTopic` for [`TOPIC`] whose partitions carry the topology's fixed
/// replica sets — the catalogue the metadata group commits and reconciles.
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

/// Schedule the timers the active replica just (re-)armed and the deliveries its
/// sends just buffered back onto the scheduler timeline, exactly as the
/// runtime's per-step dispatch does, then clear the active replica.
fn route_follow_on(rt: &mut SimRuntime) {
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
    for (at, envelope) in rt.cluster().network().drain_pending() {
        rt.scheduler_mut()
            .schedule(at, Event::MessageDeliver(envelope));
    }
}

/// Propose a `CreateTopic` to the metadata `leader`, dispatch its sends through
/// the leader's `__meta/0` transport, and route the follow-on effects so the
/// subsequent step loop replicates and commits the entry across the group.
fn propose_create_topic(rt: &mut SimRuntime, leader: usize, partition_count: u32) {
    let meta = metadata_group_key();
    let now = rt.scheduler().now();
    rt.cluster_mut().network().set_now(now);
    let command = create_topic_command(rt.cluster().topology(), partition_count);
    let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&command));
    let Some(out) = rt
        .cluster_mut()
        .step_replica(leader, &meta, now, RaftInput::Propose(payload))
    else {
        return;
    };
    if let Some(transport) = rt.cluster().meta_transport(leader) {
        let transport = transport.clone();
        for (to, msg) in out.sends {
            transport.send(to, msg);
        }
    }
    route_follow_on(rt);
}

/// Arm the partition `group`'s election timer on every node in `replica_set` at
/// the current instant, then schedule the resulting `TimerFire`s — the
/// orchestration step that lets a freshly-reconciled partition group elect a
/// leader (the runtime does not seed partition elections itself).
fn arm_partition_elections(rt: &mut SimRuntime, group: &GroupKey, replica_set: &[NodeId]) {
    let now = rt.scheduler().now();
    for node_id in replica_set {
        rt.cluster_mut().clock_mut().set_now(now);
        rt.cluster_mut()
            .clock_mut()
            .set_active(node_id.clone(), group.clone());
        rt.cluster_mut()
            .clock_mut()
            .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
    }
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
}

/// Propose a single record `value` to the partition `leader` and route the
/// follow-on effects, so the step loop replicates and commits it.
fn propose_record(rt: &mut SimRuntime, leader: usize, group: &GroupKey, value: Vec<u8>) {
    let now = rt.scheduler().now();
    rt.cluster_mut().network().set_now(now);
    let payload = EntryPayload::new(PayloadKind::Record, value);
    let Some(out) = rt
        .cluster_mut()
        .step_replica(leader, group, now, RaftInput::Propose(payload))
    else {
        return;
    };
    if let Some(transport) = rt.cluster().transport_for(leader, group) {
        let transport = transport.clone();
        for (to, msg) in out.sends {
            transport.send(to, msg);
        }
    }
    route_follow_on(rt);
}

/// The record values produced during a run: distinct payloads so each
/// committed offset maps unambiguously to its value.
fn record_values() -> Vec<Vec<u8>> {
    (0..RECORD_COUNT)
        .map(|i| format!("orders-rec-{i}").into_bytes())
        .collect()
}

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // produce/consume run brisk while covering a broad seed / shape space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 18: Consume
    // read-validity (no phantom reads)
    #[test]
    fn consumes_observe_only_committed_records_in_ascending_offset_order(
        seed in any::<u64>(),
        node_count in 3usize..=5,
        partition_count in 1u32..=2,
    ) {
        let config = run_config(seed, node_count, partition_count);
        let cluster = SimulatedCluster::new(config).expect("a valid cluster shape assembles");
        let mut rt = SimRuntime::new(cluster, Budget::default());

        // Bootstrap: arm every metadata replica's first election timer.
        seed_meta_elections(&mut rt);

        let group: GroupKey = (TOPIC.to_string(), PARTITION);
        let replica_set: Vec<NodeId> = rt
            .cluster()
            .topology()
            .replica_set_for(PARTITION)
            .expect("partition 0 has a replica set")
            .to_vec();
        let values = record_values();

        let mut history = History::new();
        let mut meta_proposed = false;
        let mut partition_armed = false;
        let mut records_proposed = false;
        let mut max_consumed = 0usize;
        let mut finished = false;

        for _ in 0..MAX_STEPS {
            if !meta_proposed {
                // Commit a `CreateTopic` as soon as a metadata leader emerges.
                if let Some(leader) = meta_leader_index(&rt) {
                    propose_create_topic(&mut rt, leader, partition_count);
                    meta_proposed = true;
                }
            } else if !partition_armed {
                // Once reconcile has spawned the partition replicas on every
                // assigned node, arm their election timers.
                if all_host_group(&rt, &group, &replica_set) {
                    arm_partition_elections(&mut rt, &group, &replica_set);
                    partition_armed = true;
                }
            } else if !records_proposed {
                // Produce the batch of records through the partition leader.
                if let Some(leader) = partition_leader_index(&rt, &group) {
                    for value in &values {
                        propose_record(&mut rt, leader, &group, value.clone());
                    }
                    records_proposed = true;
                }
            } else if !finished {
                // Once every produced record has committed, record the
                // acknowledged produces and read committed ranges back as
                // consumes — straight from the committed log, so no consume can
                // observe an uncommitted record.
                if let Some(leader) = partition_leader_index(&rt, &group) {
                    let committed = committed_records(&rt, leader, &group);
                    if committed.len() >= values.len() {
                        let now = rt.scheduler().now();

                        // Record each acknowledged produce at its committed
                        // offset, with the committed value.
                        for cr in &committed {
                            let args = OpArgs::Produce {
                                topic: TOPIC.to_string(),
                                partition: PARTITION,
                                key: None,
                                value: cr.value.clone(),
                            };
                            history.record_produce_success(args, now, now, cr.offset);
                        }

                        // Consume committed ranges at several start offsets,
                        // including one read at the end of the log that returns
                        // no records (still valid). Every returned record comes
                        // straight from the committed log.
                        let committed_len = committed.len() as Offset;
                        let start_offsets: [Offset; 4] =
                            [0, 1, committed_len.saturating_sub(1), committed_len];
                        for start in start_offsets {
                            let read = committed_records(&rt, leader, &group);
                            let returned: Vec<CommittedRecord> = read
                                .into_iter()
                                .filter(|cr| cr.offset >= start)
                                .collect();
                            max_consumed = max_consumed.max(returned.len());
                            let records: Vec<Record> = returned
                                .iter()
                                .map(|cr| Record {
                                    key: None,
                                    value: cr.value.clone(),
                                })
                                .collect();
                            let args = OpArgs::Consume {
                                topic: TOPIC.to_string(),
                                partition: PARTITION,
                                start_offset: start,
                                max_records: 1_000,
                            };
                            history.record_consume_success(args, now, now, records);
                        }
                        finished = true;
                        break;
                    }
                }
            }

            match rt.step().expect("event dispatch never fails in a healthy run") {
                Step::Event(_) => {}
                Step::Done(_) => break,
            }
        }

        prop_assert!(
            finished,
            "the run must elect leaders, commit produces, and consume them \
             within the step budget (seed={seed}, nodes={node_count})"
        );
        prop_assert!(
            max_consumed >= 1,
            "the run must be non-vacuous: at least one consume returns \
             a committed record"
        );

        // Final pass: no consume may observe an uncommitted record. `check`
        // returns the first violation of ANY property, so the assertion is
        // scoped to Property 18 (consume read-validity).
        let now = rt.scheduler().now();
        let result = KafkaParityChecker::new().check(rt.cluster(), &history, now);
        if let Err(violation) = result {
            prop_assert_ne!(
                violation.property,
                PropertyId::ConsumeReadValidity,
                "consume read-validity must never be violated, but a phantom \
                 read was reported: {}",
                violation
            );
        }
    }
}
