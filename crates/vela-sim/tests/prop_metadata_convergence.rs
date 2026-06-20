#![cfg(feature = "sim")]
//! Property test for Metadata catalogue convergence in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 20: Metadata catalogue
//! convergence
//!
//! Property 20: *For any* seed and any small generated cluster — driven through
//! a real metadata election and a sequence of committed metadata commands
//! (several `CreateTopic`s for distinct topics plus a `DeleteTopic`), all under
//! a bounded adverse (drop / duplicate / reorder) network and across a
//! strict-minority crash/restart — any two *running* nodes whose `__meta/0`
//! group is committed to the *same* commit index hold identical served
//! catalogues (metadata catalogue convergence; Requirement 11.7). The metadata
//! group is replicated by every node, so a committed command lands the same
//! catalogue mutation on every replica; a node that has caught up to a given
//! commit index must therefore expose the same served topic catalogue as every
//! other node at that index.
//!
//! The run uses the real harness: a [`SimRuntime`] over a [`SimulatedCluster`]
//! of production replicas, stepped one discrete event at a time, driven exactly
//! as the run orchestration does —
//!
//! 1. seed every `__meta/0` replica's first election timer and step to a
//!    metadata leader;
//! 2. commit a sequence of `CreateTopic`s (distinct topic names) and a
//!    `DeleteTopic` through that leader, stepping between each so the entries
//!    replicate and each node applies the mutation to its served catalogue;
//! 3. crash a strict minority mid-sequence (a majority always survives, so the
//!    group stays available), then restart it so the recovered nodes catch up
//!    through the real WAL-recovery + AppendEntries path;
//! 4. drive the run until every running node's metadata commit index has
//!    converged.
//!
//! At the end the production [`KafkaParityChecker`] takes a read-only snapshot
//! of every running node's `(commit index, served catalogue)` and asserts that
//! every pair sharing a commit index agrees on its served catalogue.
//! [`KafkaParityChecker::check_convergence`] is the direct method (it needs no
//! [`History`]); this test scopes its assertion to
//! [`PropertyId::MetadataConvergence`]. Non-vacuity is asserted explicitly: at
//! least two running nodes share a metadata commit index and at least one topic
//! is in the served catalogue, so the convergence check actually compares ≥2
//! non-empty catalogues. The whole run draws every random decision from the one
//! seed and is single-threaded, so it is fully deterministic and never flakes.
//!
//! Validates: Requirements 11.7

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, LogBackend, NodeId, Partition, PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};
use vela_sim::checker::kafka_parity::KafkaParityChecker;
use vela_sim::checker::PropertyId;
use vela_sim::cluster::SimulatedCluster;
use vela_sim::codec::encode_cluster_command;
use vela_sim::runtime::SimRuntime;
use vela_sim::scenario::{Budget, FaultIntensities, RunConfig, ScenarioParameters};
use vela_sim::scheduler::{Event, Step, VirtualInstant};

/// The topic names committed (created) through the metadata group during the
/// run. Distinct names exercise multiple catalogue mutations replicating to
/// every node.
const TOPICS: &[&str] = &["orders", "payments", "shipments"];

/// The topic that is deleted after all creates have committed, exercising a
/// catalogue removal in the convergence population as well.
const DELETED_TOPIC: &str = "payments";

/// Upper bound on the discrete events any single "step until …" phase drives.
/// Heartbeats re-arm continuously, so the timeline never quiesces; this cap
/// keeps each phase bounded while leaving ample room — well within the run's
/// event budget — to elect a leader, commit every metadata command, and let a
/// restarted minority catch up even when the network drops and retries.
const MAX_PHASE_STEPS: usize = 40_000;

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

/// The index of the node currently leading `__meta/0`, if any.
fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
    rt.cluster()
        .nodes()
        .iter()
        .position(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
}

/// A `CreateTopic` for `name` whose single partition carries the topology's
/// fixed replica set for partition 0 — the catalogue the metadata group commits
/// and every node applies.
fn create_topic_command(cluster: &SimulatedCluster, name: &str) -> ClusterCommand {
    let topology = cluster.topology();
    let index = PartitionIndex(0);
    let partitions = vec![Partition {
        index,
        replicas: topology
            .replica_set_for(index)
            .expect("partition 0 is within range")
            .to_vec(),
        leader: None,
    }];
    ClusterCommand::CreateTopic {
        name: name.to_string(),
        partitions,
        backend: LogBackend::InMemory,
    }
}

/// Propose `command` to the metadata replica on the node at `index` and route
/// the follow-on effects the propose produced back onto the scheduler timeline:
/// the leader's `out.sends` through its `__meta/0` transport, the timers it
/// (re-)armed, and the deliveries those sends buffered — exactly as the
/// runtime's per-step dispatch does. Returns `false` if the node did not step
/// the replica (it is not the metadata leader or is crashed). The commit itself
/// lands through the subsequent step loop as the replication rounds complete.
fn propose_meta_and_route(rt: &mut SimRuntime, index: usize, command: &ClusterCommand) -> bool {
    let meta = metadata_group_key();
    let now = rt.scheduler().now();
    rt.cluster_mut().network().set_now(now);
    let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(command));
    let Some(out) = rt
        .cluster_mut()
        .step_replica(index, &meta, now, RaftInput::Propose(payload))
    else {
        return false;
    };
    if let Some(transport) = rt.cluster().meta_transport(index) {
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

/// Whether every running node's served catalogue contains `topic` (the
/// committed `CreateTopic` has replicated and applied on every running node).
fn all_running_nodes_have_topic(rt: &SimRuntime, topic: &str) -> bool {
    let mut any = false;
    for node in rt.cluster().nodes() {
        if !node.is_running() {
            continue;
        }
        any = true;
        if !node.served().topics.contains_key(topic) {
            return false;
        }
    }
    any
}

/// Whether every running node's served catalogue lacks `topic` (the committed
/// `DeleteTopic` has replicated and applied on every running node).
fn all_running_nodes_lack_topic(rt: &SimRuntime, topic: &str) -> bool {
    let mut any = false;
    for node in rt.cluster().nodes() {
        if !node.is_running() {
            continue;
        }
        any = true;
        if node.served().topics.contains_key(topic) {
            return false;
        }
    }
    any
}

/// Commit `command` through the current metadata leader: resolve a leader
/// (stepping to one if a re-election is in flight), propose, and step until
/// `applied` holds on every running node. Returns whether the command applied
/// within the step budget.
fn commit_meta(
    rt: &mut SimRuntime,
    command: &ClusterCommand,
    applied: impl Fn(&SimRuntime) -> bool,
) -> bool {
    if !step_until(rt, MAX_PHASE_STEPS, |rt| meta_leader_index(rt).is_some()) {
        return false;
    }
    let Some(leader) = meta_leader_index(rt) else {
        return false;
    };
    if !propose_meta_and_route(rt, leader, command) {
        return false;
    }
    step_until(rt, MAX_PHASE_STEPS, applied)
}

/// A strict-minority crash subset chosen deterministically from the seed:
/// `floor((node_count - 1) / 2)` consecutive indices from a seed-derived
/// offset, so a majority always survives and the metadata group stays
/// available.
fn minority_crash_indices(seed: u64, node_count: usize) -> Vec<usize> {
    let minority = node_count.saturating_sub(1) / 2;
    if minority == 0 {
        return Vec::new();
    }
    let start = (seed % node_count as u64) as usize;
    (0..minority).map(|i| (start + i) % node_count).collect()
}

/// The metadata commit indices of every running node that has committed at least
/// one `__meta/0` entry.
fn running_meta_commit_indices(rt: &SimRuntime) -> Vec<u64> {
    rt.cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .filter_map(|n| n.controller().and_then(|c| c.commit_index()))
        .collect()
}

/// Whether every running node has committed `__meta/0` to the *same* commit
/// index, with at least two running nodes — the converged, non-vacuous state the
/// convergence check has teeth over.
fn running_nodes_converged(rt: &SimRuntime) -> bool {
    let running = rt
        .cluster()
        .nodes()
        .iter()
        .filter(|n| n.is_running())
        .count();
    let indices = running_meta_commit_indices(rt);
    running >= 2 && indices.len() == running && indices.iter().all(|&i| i == indices[0])
}

proptest! {
    // At least 100 cases (property-test requirement); 100 keeps the
    // multi-commit + crash/restart run brisk while covering a broad
    // seed / shape / fault-intensity space.
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: deterministic-simulation-testing, Property 20: Metadata catalogue
    // convergence
    #[test]
    fn running_nodes_at_the_same_meta_commit_index_agree(
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

        // 1. Bootstrap the metadata group and step to a leader.
        seed_meta_elections(&mut rt);
        let meta_elected =
            step_until(&mut rt, MAX_PHASE_STEPS, |rt| meta_leader_index(rt).is_some());
        prop_assert!(meta_elected, "the metadata group must elect a leader");

        // 2. Commit several CreateTopics, crashing a strict minority part-way
        //    through so convergence is exercised across catch-up.
        let crash_subset = minority_crash_indices(seed, node_count);
        let crash_after = TOPICS.len() / 2;
        let mut crashed = false;
        let mut topics_committed = 0usize;

        for (i, topic) in TOPICS.iter().enumerate() {
            if i == crash_after && !crash_subset.is_empty() {
                rt.cluster_mut().crash_nodes(&crash_subset);
                crashed = true;
            }
            let create = create_topic_command(rt.cluster(), topic);
            let topic_name = (*topic).to_string();
            if commit_meta(&mut rt, &create, move |rt| {
                all_running_nodes_have_topic(rt, &topic_name)
            }) {
                topics_committed += 1;
            }
        }
        prop_assert!(
            topics_committed > 0,
            "at least one CreateTopic must commit and apply (non-vacuity)"
        );

        // Delete one of the created topics so a removal is also in the converged
        // catalogue (best-effort: a re-election under faults may delay it).
        let delete = ClusterCommand::DeleteTopic {
            name: DELETED_TOPIC.to_string(),
        };
        let _ = commit_meta(&mut rt, &delete, |rt| {
            all_running_nodes_lack_topic(rt, DELETED_TOPIC)
        });

        // 3. Restart the crashed minority through the real WAL-recovery path so
        //    the recovered nodes rejoin and catch up.
        if crashed {
            rt.cluster_mut()
                .restart_nodes(&crash_subset)
                .expect("a crashed minority restarts cleanly from its retained disk");
        }

        // 4. Drive until every running node's metadata commit index converges,
        //    so the final pass compares fully-caught-up catalogues.
        let converged = step_until(&mut rt, MAX_PHASE_STEPS, running_nodes_converged);

        // Non-vacuity: at least two running nodes share a metadata commit index
        // and the served catalogue holds at least one topic, so the convergence
        // check actually compares ≥2 non-empty catalogues.
        let indices = running_meta_commit_indices(&rt);
        let shared = indices.iter().any(|a| indices.iter().filter(|b| *b == a).count() >= 2);
        prop_assert!(
            shared,
            "at least two running nodes must share a metadata commit index (non-vacuity); \
             converged={converged}, indices={indices:?}"
        );
        let some_topic = rt
            .cluster()
            .nodes()
            .iter()
            .filter(|n| n.is_running())
            .any(|n| !n.served().topics.is_empty());
        prop_assert!(
            some_topic,
            "at least one running node must serve a non-empty catalogue (non-vacuity)"
        );

        // Final pass: no two running nodes at the same metadata commit index may
        // disagree on their served catalogue.
        let now = rt.scheduler().now();
        let result = KafkaParityChecker::new().check_convergence(rt.cluster(), now);
        if let Err(violation) = &result {
            prop_assert_ne!(
                violation.property,
                PropertyId::MetadataConvergence,
                "metadata catalogue convergence must never be violated, but a breach was \
                 reported: {:?}",
                violation
            );
        }
    }
}
