#![cfg(feature = "sim")]
//! Property test for commit monotonicity in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 15: Commit monotonicity
//!
//! Property 15: *For any* seed and any small all-voter cluster — driven through
//! a real metadata election, a committed `CreateTopic`, and a minority
//! crash/restart, all under a bounded adverse (drop / duplicate / reorder)
//! network — no replica's commit index ever decreases over the course of the
//! run (Requirement 10.5). The check is incremental: the production
//! [`RaftSafetyChecker::observe`] folds each running replica's commit index into
//! a per-`(group, node)` high-water mark every step and reports a
//! [`PropertyId::CommitMonotonicity`] [`Violation`] the instant a later
//! observation falls below that mark.
//!
//! Because the checker keys its high-water mark by `(group, node)`, the
//! guarantee spans a crash gap: a node that is crashed and later restarted must
//! recover (through the real WAL/Raft recovery path) a commit index no lower
//! than the one it had reached before the crash, or the property fails. The run
//! keeps a majority of voters alive throughout, so the metadata group stays
//! available and commit indices keep advancing under churn rather than stalling.
//!
//! The run is driven with the real harness: a [`SimRuntime`] over a
//! [`SimulatedCluster`], bootstrapped exactly as the run orchestration will
//! (seed each `__meta/0` replica's first election timer), then stepped one
//! discrete event at a time, with [`RaftSafetyChecker::observe`] called every
//! step. The test asserts no `CommitMonotonicity` violation is ever reported —
//! `observe` also evaluates Election Safety, which is owned by a sibling test,
//! so the assertion is scoped to Property 15. It also asserts that at least one
//! commit actually advanced, so the property is never vacuously satisfied. The
//! whole run draws every random decision from the one seed and is
//! single-threaded, so it is fully deterministic and never flakes.
//!
//! Validates: Requirements 10.5

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

/// Build the run-configuration: a small all-voter cluster (≥3 nodes, every
/// partition replicated on every node) under a bounded adverse network whose
/// drop / duplicate / reorder intensities come from the generated probabilities.
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

/// The index of the node currently leading `__meta/0`, if any.
fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
    rt.cluster()
        .nodes()
        .iter()
        .position(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
}

/// The highest `__meta/0` commit index observed across every *running* node, if
/// any replica has committed an entry yet. `None` means no running replica has
/// advanced its commit index past the empty log.
fn max_meta_commit(rt: &SimRuntime) -> Option<u64> {
    rt.cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .filter_map(|n| n.controller().map(|c| c.commit_index()))
        .max()
        .flatten()
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

/// Record the first `CommitMonotonicity` violation an `observe` pass reports (if
/// any) into `mono`. Other properties (Election Safety in particular) are owned
/// by their own property tests, so this commit-monotonicity test deliberately
/// scopes its assertion to Property 15.
fn record_monotonicity(result: Result<(), Violation>, mono: &mut Option<Violation>) {
    if let Err(v) = result {
        if v.property == PropertyId::CommitMonotonicity && mono.is_none() {
            *mono = Some(v);
        }
    }
}

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // crash/restart-with-faults run brisk while covering a broad seed / shape /
    // fault-intensity space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 15: Commit monotonicity
    #[test]
    fn no_replica_commit_index_ever_decreases(
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
        let mut mono: Option<Violation> = None;

        let crash_subset = minority_crash_indices(seed, node_count);
        let mut proposed = false;
        let mut crashed = false;
        let mut restarted = false;
        // The high-water commit index the metadata group reached during the run.
        // It must become `Some(_)` for the property to be non-vacuous.
        let mut commit_high_water: Option<u64> = None;

        for step in 0..MAX_STEPS {
            // Commit a `CreateTopic` through the metadata group as soon as a
            // leader emerges, so commit indices advance across the group.
            if !proposed {
                if let Some(leader) = meta_leader_index(&rt) {
                    propose_create_topic(&mut rt, leader, partition_count);
                    proposed = true;
                }
            }

            // Crash a strict minority mid-run, then restart it: the recovered
            // nodes rejoin and catch up through the real WAL-recovery path, so
            // their recovered commit index must not fall below what they had
            // reached before the crash.
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
                    // Incremental pass every step — commit monotonicity (and
                    // Election Safety) live here.
                    record_monotonicity(checker.observe(rt.cluster(), now), &mut mono);
                    // Track the highest commit index observed so we can prove the
                    // run actually made progress (non-vacuous property).
                    commit_high_water = commit_high_water.max(max_meta_commit(&rt));
                }
                Step::Done(_) => break,
            }

            if mono.is_some() {
                break;
            }
        }

        prop_assert!(
            mono.is_none(),
            "commit monotonicity must never be violated, but a regression was reported: {:?}",
            mono
        );

        // Guard against a vacuous pass: at least one commit must have advanced
        // (the metadata group elected a leader and committed the `CreateTopic`).
        prop_assert!(
            commit_high_water.is_some(),
            "expected the metadata group to commit at least one entry so the \
             monotonicity property is exercised, but no commit index ever advanced"
        );
    }
}
