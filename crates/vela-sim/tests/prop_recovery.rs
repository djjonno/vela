#![cfg(feature = "sim")]
//! Property test for crash/restart recovery round-trip in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 6: Crash/restart recovery
//! round-trip
//!
//! Property 6: *For any* seed and any small generated cluster shape, crashing
//! and restarting nodes recovers each node's durable state through the real
//! WAL/Raft recovery path, leaves the run-fixed topology untouched, and is a
//! pure function of the seed and parameters. Concretely the test exercises four
//! legs of the same round-trip:
//!
//! - **Durable recovery (Requirements 6.3, 6.4).** After a `CreateTopic` is
//!   committed durably to a single node's `__meta/0` WAL, a crash-then-restart
//!   recovers the applied catalogue from disk — the recovered served catalogue
//!   equals the durably-committed metadata — re-hosts the `__meta/0` group, and
//!   starts a partition replica (with its storage handle and transport) for
//!   every recovered assigned partition.
//! - **Topology fixed (Requirement 3.5).** A crash and a restart never mutate
//!   the run-fixed [`Topology`] (node set, replication factor, Replica_Sets).
//! - **Running flags (Requirement 6.1).** A crashed node is not running while
//!   every other node is; after restart every node is running again.
//! - **Determinism (Requirement 3.5 / 1).** Two clusters built from the same
//!   seed and parameters and driven through the identical crash/restart agree
//!   node-for-node on the served catalogue and the hosted partition (fleet)
//!   keys.
//!
//! The crash subset for the multi-node leg is chosen deterministically from the
//! seed and bounded to a strict minority of the cluster, so a majority always
//! survives (Requirement 6.5); the durable-recovery leg uses a single-node
//! cluster, where a self-vote is a majority and a proposal commits in one step,
//! exactly as the `cluster.rs` task-11.4 tests do.
//!
//! Validates: Requirements 3.5, 6.1, 6.3, 6.4

use std::collections::HashSet;

use proptest::prelude::*;

use vela_core::{
    metadata_group_key, ClusterCommand, GroupKey, LogBackend, Partition, PartitionIndex,
};
use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{RaftInput, TimerKind};
use vela_sim::cluster::{SimNode, SimulatedCluster, Topology};
use vela_sim::codec::encode_cluster_command;
use vela_sim::scenario::{RunConfig, ScenarioParameters};
use vela_sim::scheduler::VirtualInstant;

/// The topic name used throughout the test.
const TOPIC: &str = "orders";

/// Build a cluster of the given shape from `seed`, with the documented defaults
/// for every other parameter (so the only injected fault is the ones this test
/// applies explicitly — the default fault intensities are a healthy cluster).
fn fresh_cluster(
    seed: u64,
    node_count: usize,
    replication_factor: usize,
    partition_count: u32,
) -> SimulatedCluster {
    SimulatedCluster::new(RunConfig {
        seed,
        params: ScenarioParameters {
            node_count,
            replication_factor,
            partition_count,
            ..ScenarioParameters::default()
        },
    })
    .expect("a valid cluster shape assembles")
}

/// A `CreateTopic` command for `name` whose partitions carry the topology's
/// fixed Replica_Sets — exactly the catalogue the metadata group would commit,
/// so recovery's reconcile spawns a replica on each assigned node (mirrors the
/// `cluster.rs` `create_topic_for` test helper, built over the public API).
fn create_topic_for(topo: &Topology, name: &str) -> ClusterCommand {
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

/// Drive node 0's single-voter `__meta/0` group to leader and commit `command`
/// as a durable `Cluster` entry through real consensus, using only the public
/// [`SimulatedCluster::step_replica`] entry point.
///
/// Only valid for a one-node cluster, where the self-vote is a majority and a
/// proposal commits in the same step. The entry is fsynced into node 0's
/// `__meta/0` WAL under `SyncPolicy::Always`, so it survives a subsequent crash.
fn commit_meta(cluster: &mut SimulatedCluster, command: &ClusterCommand) {
    let meta = metadata_group_key();
    let now = VirtualInstant::from_nanos(1);

    // Single voter: a Tick wins the election outright and makes node 0 leader.
    cluster.step_replica(0, &meta, now, RaftInput::Tick(TimerKind::Election));

    // Propose the command as a `Cluster` entry; it commits immediately.
    let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(command));
    let out = cluster
        .step_replica(0, &meta, now, RaftInput::Propose(payload))
        .expect("node 0 hosts the metadata group");
    assert!(
        !out.committed.is_empty(),
        "a single-voter propose commits in one step"
    );
}

/// A single-node cluster that has durably committed a `CreateTopic` for
/// [`TOPIC`], ready to be crashed and restarted.
fn committed_single_node(seed: u64, partition_count: u32) -> SimulatedCluster {
    let mut cluster = fresh_cluster(seed, 1, 1, partition_count);
    let command = create_topic_for(cluster.topology(), TOPIC);
    commit_meta(&mut cluster, &command);
    cluster
}

/// The `(TOPIC, p)` group keys for every partition `0..partition_count`.
fn topic_groups(partition_count: u32) -> Vec<GroupKey> {
    (0..partition_count)
        .map(|p| (TOPIC.to_string(), PartitionIndex(p)))
        .collect()
}

/// The subset of `candidates` the node currently hosts a partition replica for,
/// detected through the public per-partition transport accessor (a transport is
/// minted exactly when a replica is started and dropped when it is stopped, so
/// it tracks the node's fleet keys exactly).
fn hosted_groups(node: &SimNode, candidates: &[GroupKey]) -> Vec<GroupKey> {
    candidates
        .iter()
        .filter(|group| node.transport(group).is_some())
        .cloned()
        .collect()
}

/// A strict-minority crash subset chosen deterministically from the seed.
///
/// Picks `floor((node_count - 1) / 2)` consecutive node indices starting at a
/// seed-derived offset, so the set is always a strict minority (a majority
/// survives, Requirement 6.5) and its members are distinct. Returns an empty
/// set for one- and two-node clusters, where no minority can be crashed.
fn minority_crash_indices(seed: u64, node_count: usize) -> Vec<usize> {
    let minority = node_count.saturating_sub(1) / 2;
    if minority == 0 {
        return Vec::new();
    }
    let start = (seed % node_count as u64) as usize;
    (0..minority).map(|i| (start + i) % node_count).collect()
}

/// Generate `(seed, node_count, replication_factor, partition_count)` over the
/// small shapes the property targets: `node_count` in `1..=5`,
/// `replication_factor` in `1..=node_count`, `partition_count` in `1..=4`.
fn shape_strategy() -> impl Strategy<Value = (u64, usize, usize, u32)> {
    (any::<u64>(), 1usize..=5).prop_flat_map(|(seed, node_count)| {
        (Just(seed), Just(node_count), 1usize..=node_count, 1u32..=4)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: deterministic-simulation-testing, Property 6: Crash/restart
    // recovery round-trip
    #[test]
    fn crash_restart_recovery_round_trip(
        (seed, node_count, replication_factor, partition_count) in shape_strategy(),
    ) {
        let candidates = topic_groups(partition_count);

        // --- Leg A: durable recovery round-trip (Requirements 6.3, 6.4) ------
        //
        // A single-node cluster lets us commit a `CreateTopic` through real
        // consensus, then prove a crash-then-restart recovers it from disk.
        let mut single = committed_single_node(seed, partition_count);

        // Before the crash the node is up with a live controller.
        prop_assert!(single.node(0).unwrap().is_running());
        prop_assert!(single.node(0).unwrap().controller().is_some());

        // The crash clears `running` and drops the volatile controller
        // (Requirement 6.1); the durable bytes survive on the retained disk.
        prop_assert!(single.crash_node(0));
        prop_assert!(!single.node(0).unwrap().is_running());
        prop_assert!(single.node(0).unwrap().controller().is_none());

        // The restart runs the real recovery path over the retained disk.
        prop_assert!(single.restart_node(0).expect("restart recovers cleanly"));

        {
            let node = single.node(0).unwrap();
            // The node is up again (Requirement 6.1) and re-hosts `__meta/0`
            // (Requirement 6.3).
            prop_assert!(node.is_running());
            prop_assert!(node.controller().unwrap().hosts_metadata_group());

            // The recovered served catalogue equals what was durably committed:
            // it holds the topic, and mirrors the controller's recovered applied
            // catalogue exactly (Requirement 6.3).
            prop_assert!(node.served().topics.contains_key(TOPIC));
            prop_assert_eq!(node.served(), node.controller().unwrap().metadata());

            // A replica is recovered for every assigned partition — on a lone
            // node that is every partition (Requirement 6.4).
            prop_assert_eq!(node.fleet_len(), partition_count as usize);
            prop_assert_eq!(
                hosted_groups(node, &candidates),
                candidates.clone()
            );
        }
        // Each recovered replica is reachable through both transport accessors,
        // confirming its storage handle and bus transport were re-minted.
        for group in &candidates {
            prop_assert!(single.node(0).unwrap().transport(group).is_some());
            prop_assert!(single.transport_for(0, group).is_some());
        }

        // Determinism (Requirement 3.5 / 1): an independent build driven through
        // the identical crash/restart agrees node-for-node on the recovered
        // served catalogue and the hosted partition (fleet) keys.
        let mut single_twin = committed_single_node(seed, partition_count);
        prop_assert!(single_twin.crash_node(0));
        prop_assert!(single_twin.restart_node(0).expect("twin restart recovers"));
        {
            let a = single.node(0).unwrap();
            let b = single_twin.node(0).unwrap();
            prop_assert_eq!(a.served(), b.served());
            prop_assert_eq!(a.fleet_len(), b.fleet_len());
            prop_assert_eq!(
                hosted_groups(a, &candidates),
                hosted_groups(b, &candidates)
            );
        }

        // --- Leg B: topology-fixed, running-flags, minority subset -----------
        //
        // A multi-node cluster exercises crashing/restarting a strict minority
        // and proves the run-fixed topology and the running flags behave.
        let mut multi = fresh_cluster(seed, node_count, replication_factor, partition_count);
        let topology_before = multi.topology().clone();
        let crashed = minority_crash_indices(seed, node_count);
        let crashed_set: HashSet<usize> = crashed.iter().copied().collect();

        // Crash the minority subset.
        let crashed_count = multi.crash_nodes(&crashed);
        prop_assert_eq!(crashed_count, crashed.len());

        // Exactly the crashed nodes are down; the majority stays up
        // (Requirement 6.1).
        for index in 0..node_count {
            prop_assert_eq!(
                multi.node(index).unwrap().is_running(),
                !crashed_set.contains(&index)
            );
        }
        // The crash never mutates the run-fixed topology (Requirement 3.5).
        prop_assert_eq!(multi.topology(), &topology_before);

        // Restart the same subset.
        let restarted = multi.restart_nodes(&crashed).expect("restart recovers cleanly");
        prop_assert_eq!(restarted, crashed.len());

        // Every node is running again (Requirement 6.1), and the topology is
        // still untouched (Requirement 3.5).
        for index in 0..node_count {
            prop_assert!(multi.node(index).unwrap().is_running());
        }
        prop_assert_eq!(multi.topology(), &topology_before);

        // Each restarted node re-hosts `__meta/0`; with nothing durably
        // committed it recovers an empty catalogue — the durable round-trip of
        // an empty state (Requirement 6.3).
        for &index in &crashed {
            let node = multi.node(index).unwrap();
            prop_assert!(node.controller().unwrap().hosts_metadata_group());
            prop_assert!(node.served().topics.is_empty());
        }

        // Determinism (Requirement 3.5 / 1): a twin driven through the identical
        // crash/restart agrees node-for-node and shares the same topology.
        let mut multi_twin =
            fresh_cluster(seed, node_count, replication_factor, partition_count);
        multi_twin.crash_nodes(&crashed);
        multi_twin
            .restart_nodes(&crashed)
            .expect("twin restart recovers");
        prop_assert_eq!(multi.topology(), multi_twin.topology());
        for index in 0..node_count {
            let a = multi.node(index).unwrap();
            let b = multi_twin.node(index).unwrap();
            prop_assert_eq!(a.is_running(), b.is_running());
            prop_assert_eq!(a.served(), b.served());
            prop_assert_eq!(a.fleet_len(), b.fleet_len());
        }
    }
}
