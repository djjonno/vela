#![cfg(feature = "sim")]
//! Property test for State Machine Safety in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 14: State Machine Safety
//! (Raft §5.4.3)
//!
//! Property 14: *For any* seed and any small generated cluster — driven through
//! a real metadata election, a committed `CreateTopic`, and a minority
//! crash/restart, all under an adverse (drop / duplicate / reorder) network —
//! no two replicas ever apply a *different* committed entry at the same log
//! index (State Machine Safety, Raft §5.4.3). The check is structural: the
//! production [`RaftSafetyChecker`] takes a full, read-only snapshot of every
//! running replica's log and commit index and reports a
//! [`PropertyId::StateMachineSafety`] [`Violation`] the instant two replicas
//! disagree on a committed entry.
//!
//! The run is driven with the real harness: a [`SimRuntime`] over a
//! [`SimulatedCluster`], bootstrapped exactly as the run orchestration will
//! (seed each `__meta/0` replica's first election timer), then stepped one
//! discrete event at a time. The cluster-wide metadata group is replicated by
//! every node, so a committed `CreateTopic` lands the *same* entry on every
//! replica — the population over which State Machine Safety has teeth. A
//! seed-chosen strict minority is crashed and restarted mid-run (a majority
//! always survives, so the group stays available and the entry replicates to
//! the recovered nodes through the real WAL-recovery + catch-up path), and the
//! network drops, duplicates, and reorders messages throughout.
//!
//! [`RaftSafetyChecker::observe`] is called every step (the cheap incremental
//! pass) and [`RaftSafetyChecker::check_logs`] periodically and as a final pass
//! (the structural pass that decides State Machine Safety); the test asserts no
//! `StateMachineSafety` violation is ever reported. The whole run draws every
//! random decision from the one seed and is single-threaded, so it is fully
//! deterministic and never flakes.
//!
//! Validates: Requirements 10.4

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, LogBackend, NodeId, Partition, PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::{PropertyId, RaftSafetyChecker, Violation};
use vela_sim::cluster::{SimulatedCluster, Topology};
use vela_sim::codec::encode_cluster_command;
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, FaultIntensities, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualInstant};

/// The topic name committed through the metadata group during the run.
const TOPIC: &str = "orders";

/// Upper bound on the number of discrete events driven per run. Heartbeats
/// re-arm continuously, so the timeline never quiesces; this cap (well within
/// the run's virtual-time budget) keeps each case bounded and fast while
/// leaving ample room to elect a leader, commit the `CreateTopic`, and crash
/// then restart a minority.
const MAX_STEPS: usize = 5_000;

/// The step at which the seed-chosen minority is crashed, and the step at which
/// it is restarted — both comfortably after a leader is elected and the
/// `CreateTopic` has committed and begun replicating.
const CRASH_AT: usize = 1_500;
const RESTART_AT: usize = 3_000;

/// Build the run-configuration: a small cluster (≥3 nodes, every partition
/// replicated on every node) under an adverse network whose drop / duplicate /
/// reorder intensities come from the generated probabilities.
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
/// orchestration does at bootstrap (mirrors the runtime's own `seed_meta_elections`).
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

/// The index of the node currently leading `__meta/0`, if any.
fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
    rt.cluster()
        .nodes()
        .iter()
        .position(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
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

/// Propose a `CreateTopic` to the metadata `leader` and route the follow-on
/// effects the propose produced back onto the scheduler timeline (the sends
/// through the leader's `__meta/0` transport, plus the re-armed timers and
/// buffered deliveries), so the subsequent step loop replicates and commits the
/// entry across the group. The commit itself is applied by the runtime's own
/// per-step follow-on as the AppendEntries rounds complete.
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
    // Dispatch the proposal's AppendEntries through the leader's metadata
    // transport (cheap `Rc` clone to release the cluster borrow first).
    if let Some(transport) = rt.cluster().meta_transport(leader) {
        let transport = transport.clone();
        for (to, msg) in out.sends {
            transport.send(to, msg);
        }
    }
    // Schedule the timers this step (re-)armed, then the deliveries the sends
    // just buffered, exactly as the runtime's per-step dispatch does.
    for armed in rt.cluster_mut().clock_mut().drain_armed() {
        rt.scheduler_mut().schedule(armed.at, armed.to_event());
    }
    rt.cluster_mut().clock_mut().clear_active();
    for (at, envelope) in rt.cluster().network().drain_pending() {
        rt.scheduler_mut()
            .schedule(at, Event::MessageDeliver(envelope));
    }
}

/// A strict-minority crash subset chosen deterministically from the seed:
/// `floor((node_count - 1) / 2)` consecutive indices from a seed-derived
/// offset, so a majority always survives and the metadata group stays available
/// (mirrors the recovery test's `minority_crash_indices`).
fn minority_crash_indices(seed: u64, node_count: usize) -> Vec<usize> {
    let minority = node_count.saturating_sub(1) / 2;
    if minority == 0 {
        return Vec::new();
    }
    let start = (seed % node_count as u64) as usize;
    (0..minority).map(|i| (start + i) % node_count).collect()
}

/// Record the first `StateMachineSafety` violation a checker pass reports (if
/// any) into `sms`. Other properties (Election Safety, commit monotonicity, Log
/// Matching, Leader Completeness) are owned by their own property tests, so this
/// State-Machine-Safety test deliberately scopes its assertion to Property 14.
fn record_sms(result: Result<(), Violation>, sms: &mut Option<Violation>) {
    if let Err(v) = result {
        if v.property == PropertyId::StateMachineSafety && sms.is_none() {
            *sms = Some(v);
        }
    }
}

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // crash/restart-with-faults run brisk while covering a broad seed / shape /
    // fault-intensity space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 14: State Machine
    // Safety (Raft §5.4.3)
    #[test]
    fn no_replica_applies_a_differing_committed_entry(
        seed in any::<u64>(),
        node_count in 3usize..=5,
        partition_count in 1u32..=2,
        drop_prob in 0.0f64..0.10,
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
        let budget = Budget::default();
        let mut rt = SimRuntime::new(cluster, budget);

        // Bootstrap: arm every metadata replica's first election timer.
        seed_meta_elections(&mut rt);

        let mut checker = RaftSafetyChecker::new();
        let mut sms: Option<Violation> = None;

        let crash_subset = minority_crash_indices(seed, node_count);
        let mut proposed = false;
        let mut crashed = false;
        let mut restarted = false;

        for step in 0..MAX_STEPS {
            // Commit a `CreateTopic` through the metadata group as soon as a
            // leader emerges, so the same entry replicates to every replica.
            if !proposed {
                if let Some(leader) = meta_leader_index(&rt) {
                    propose_create_topic(&mut rt, leader, partition_count);
                    proposed = true;
                }
            }

            // Crash a strict minority mid-run, then restart it: the recovered
            // nodes rejoin and catch up through the real WAL-recovery path, so
            // committed entries must replicate to them unchanged.
            if proposed && !crashed && step >= CRASH_AT && !crash_subset.is_empty() {
                rt.cluster_mut().crash_nodes(&crash_subset);
                crashed = true;
            }
            if crashed && !restarted && step >= RESTART_AT {
                rt.cluster_mut()
                    .restart_nodes(&crash_subset)
                    .expect("a crashed minority restarts cleanly from its retained disk");
                restarted = true;
            }

            match rt.step().expect("event dispatch never fails in a healthy run") {
                Step::Event(_) => {
                    let now = rt.scheduler().now();
                    // Cheap incremental pass every step.
                    record_sms(checker.observe(rt.cluster(), now), &mut sms);
                    // Structural pass (the one that decides State Machine
                    // Safety) periodically across the run.
                    if step % 50 == 0 {
                        record_sms(checker.check_logs(rt.cluster(), now), &mut sms);
                    }
                }
                Step::Done(_) => break,
            }

            if sms.is_some() {
                break;
            }
        }

        // Final structural pass over the run's end state.
        let now = rt.scheduler().now();
        record_sms(checker.check_logs(rt.cluster(), now), &mut sms);

        prop_assert!(
            sms.is_none(),
            "State Machine Safety must never be violated, but a breach was reported: {:?}",
            sms
        );
    }
}
