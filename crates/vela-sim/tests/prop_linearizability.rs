#![cfg(feature = "sim")]
//! Property test for per-partition linearizability in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 19: Per-partition
//! linearizability
//!
//! Property 19: *For any* seed and any small generated cluster — driven through
//! a real metadata election, a committed `CreateTopic`, a real per-partition
//! election, and a sequence of *non-overlapping* acknowledged produces — the
//! recorded history is consistent with a single linearizable per-partition
//! committed log: returned offsets index the total order, the real-time order of
//! non-overlapping operations is reflected by their offsets, and every
//! successful consume observes a prefix-range of that committed log
//! (Requirement 11.6).
//!
//! The decisive observable the checker exercises is the *real-time ordering*
//! rule: if produce `A` responded strictly before produce `B` was invoked
//! (`a.responded_at < b.invoked_at`), then `A` must hold the earlier offset. To
//! make that relation real (not vacuous), this run drives produces strictly
//! sequentially: produce `A`, step the real dispatch loop until it commits and
//! capture the virtual instant of its acknowledgement, *then* invoke produce `B`
//! at a strictly-later virtual instant. Because every `invoked_at` /
//! `responded_at` is the genuine [`VirtualInstant`] the scheduler reports at
//! proposal and at commit, consecutive produces are genuinely non-overlapping in
//! virtual time, so the ordering constraint the checker enforces is meaningfully
//! tested on every case.
//!
//! The run uses the real harness exactly as the run orchestration does:
//!
//! 1. seed every `__meta/0` replica's first election timer and step to a
//!    metadata leader;
//! 2. commit a `CreateTopic` through that leader, so reconcile spawns the
//!    partition's replicas on every assigned node;
//! 3. seed the partition group's election timers and step to a partition leader;
//! 4. produce records one at a time — proposing a `PayloadKind::Record` entry to
//!    the partition leader, routing the follow-on effects back onto the
//!    scheduler timeline, and stepping until each commits — recording each
//!    acknowledged produce (its real invocation/response instants and returned
//!    offset) into the [`History`];
//! 5. read committed prefix-ranges straight from the partition's committed log
//!    and record them as successful consumes — so every consumed record is
//!    genuinely committed (never a fabricated/uncommitted record).
//!
//! At the end the production [`KafkaParityChecker`] takes a read-only snapshot of
//! every running replica's committed log and, against the recorded [`History`],
//! asserts the linearizability facts hold. `check` reports the first violation of
//! *any* Kafka-parity property; this test scopes its assertion to
//! [`PropertyId::PerPartitionLinearizability`] (the sibling durability / offset /
//! consume / convergence properties have their own tests). The whole run draws
//! every random decision from the one seed and is single-threaded, so it is
//! fully deterministic and never flakes.
//!
//! Validates: Requirements 11.6

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, CommittedRecord, GroupKey, LogBackend, NodeId, Offset,
    Partition, PartitionIndex, Record,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::kafka_parity::KafkaParityChecker;
use vela_sim::checker::PropertyId;
use vela_sim::cluster::SimulatedCluster;
use vela_sim::codec::encode_cluster_command;
use vela_sim::history::{History, OpArgs};
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualInstant};

/// The topic produced to and consumed from during the run.
const TOPIC: &str = "orders";

/// The partition records are produced to (partition 0 of [`TOPIC`]).
const PARTITION: PartitionIndex = PartitionIndex(0);

/// Upper bound on the discrete events any single "step until …" phase drives.
/// Heartbeats re-arm continuously, so the timeline never quiesces; this cap
/// keeps each phase bounded while leaving ample room — well within the run's
/// event budget — to elect a leader, commit a `CreateTopic`, elect a partition
/// leader, and commit a produce.
const MAX_PHASE_STEPS: usize = 40_000;

/// The number of records produced (non-overlapping) to the partition.
const RECORD_COUNT: usize = 4;

/// Build a healthy run-configuration: a small cluster (3..=5 nodes, every
/// partition replicated on every node) with the documented default (no-fault)
/// intensities, so produces commit and the only behaviour under test is
/// linearizability of the recorded history.
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

/// The partition group records are produced to.
fn partition_group() -> GroupKey {
    (TOPIC.to_string(), PARTITION)
}

/// The distinct payload for the `i`th produced record. Distinct values let the
/// checker map each acknowledged offset unambiguously to its value.
fn record_value(i: usize) -> Vec<u8> {
    format!("linz-record-{i}").into_bytes()
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

/// The committed records of `group`, observed from the most-advanced running
/// replica, in ascending offset order. Committed logs agree across replicas
/// (State Machine Safety), so the longest running prefix is authoritative.
fn committed_records(rt: &SimRuntime, group: &GroupKey) -> Vec<CommittedRecord> {
    let mut best: Vec<CommittedRecord> = Vec::new();
    for node in rt.cluster().nodes() {
        if !node.is_running() {
            continue;
        }
        for (g, replica) in node.fleet_replicas() {
            if g == group {
                let records = replica.read(0, usize::MAX);
                if records.len() > best.len() {
                    best = records;
                }
            }
        }
    }
    best
}

/// The committed offset of `value` in `group`, observed from any running
/// replica's committed log, or `None` if no running replica has committed it
/// yet.
fn committed_offset_of(rt: &SimRuntime, group: &GroupKey, value: &[u8]) -> Option<Offset> {
    for record in committed_records(rt, group) {
        if record.value == value {
            return Some(record.offset);
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
/// the follow-on effects back onto the scheduler timeline: the leader's
/// `out.sends` through its transport, the timers it (re-)armed, and the
/// deliveries those sends buffered — exactly as the runtime's per-step dispatch
/// does. Returns `false` if the node did not step the replica (it is not hosting
/// the group or is crashed). The commit itself lands through the subsequent step
/// loop as the replication rounds complete.
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

/// Produce `value` to `group` and *block* (in virtual time) until it commits:
/// resolve the current partition leader, capture the real invocation instant,
/// propose the record, then step until it is observed committed and capture the
/// real response instant. On commit, record the acknowledged produce — its real
/// invocation/response instants and returned offset — into `history` and return
/// `true`. Returns `false` if no leader could be resolved or the record did not
/// commit within the step budget (an expected, non-acknowledged outcome that is
/// simply not recorded).
///
/// Because the caller drives produces strictly sequentially and this helper does
/// not return until the produce commits, each produce's `responded_at` precedes
/// the next produce's `invoked_at` in virtual time — the genuinely
/// non-overlapping real-time order the linearizability check measures.
fn produce(rt: &mut SimRuntime, group: &GroupKey, history: &mut History, value: Vec<u8>) -> bool {
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

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // election + sequential-produce + consume run brisk while covering a broad
    // seed / shape space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 19: Per-partition
    // linearizability
    #[test]
    fn history_is_consistent_with_a_single_linearizable_partition_log(
        seed in any::<u64>(),
        node_count in 3usize..=5,
        partition_count in 1u32..=2,
    ) {
        let config = run_config(seed, node_count, partition_count);
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

        // 4. Produce records strictly sequentially: each produce blocks until it
        //    commits before the next is invoked, so consecutive produces are
        //    genuinely non-overlapping in virtual time (A.responded_at <
        //    B.invoked_at). This exercises the linearizability real-time order
        //    rule meaningfully on every case.
        let mut acked = 0usize;
        for i in 0..RECORD_COUNT {
            if produce(&mut rt, &group, &mut history, record_value(i)) {
                acked += 1;
            }
        }
        prop_assert!(
            acked > 0,
            "the run must acknowledge at least one produced record (non-vacuity)"
        );

        // 5. Read committed prefix-ranges straight from the committed log and
        //    record them as successful consumes — so every consumed record is
        //    genuinely committed (never a fabricated/uncommitted record), and
        //    each consume observes a prefix-range of the committed log.
        let committed = committed_records(&rt, &group);
        let now = rt.scheduler().now();
        let committed_len = committed.len() as Offset;
        // Several start offsets, including one at the end of the log that returns
        // no records (still a valid prefix-range observation).
        let start_offsets: [Offset; 4] =
            [0, 1, committed_len.saturating_sub(1), committed_len];
        let mut max_consumed = 0usize;
        for start in start_offsets {
            let records: Vec<Record> = committed
                .iter()
                .filter(|cr| cr.offset >= start)
                .map(|cr| Record {
                    key: None,
                    value: cr.value.clone(),
                })
                .collect();
            max_consumed = max_consumed.max(records.len());
            history.record_consume_success(
                OpArgs::Consume {
                    topic: TOPIC.to_string(),
                    partition: PARTITION,
                    start_offset: start,
                    max_records: 1_000,
                },
                now,
                now,
                records,
            );
        }
        prop_assert!(
            max_consumed >= 1,
            "the run must be non-vacuous: at least one consume returns a committed record"
        );

        // Final pass: the recorded history must be consistent with a single
        // linearizable per-partition committed log. `check` returns the first
        // violation of *any* Kafka-parity property; this test scopes its
        // assertion to per-partition linearizability.
        let result = KafkaParityChecker::new().check(rt.cluster(), &history, now);
        if let Err(violation) = &result {
            prop_assert_ne!(
                violation.property,
                PropertyId::PerPartitionLinearizability,
                "per-partition linearizability must never be violated, but a breach was \
                 reported: {:?}",
                violation
            );
        }
    }
}
