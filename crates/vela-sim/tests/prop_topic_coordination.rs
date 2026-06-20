#![cfg(feature = "sim")]
//! Property test for topic-create coordination across the simulated cluster.
//!
//! Feature: deterministic-simulation-testing, Property 8: Topic-create
//! coordination
//!
//! Property 8 (Requirement 3.4): *for any* cluster shape (`node_count` in
//! `1..=5`, `replication_factor` in `1..=node_count`, `partition_count` in
//! `1..=5`) and any seed, applying a committed `CreateTopic` whose partitions
//! carry the topology's fixed `Replica_Set`s — the same metadata-commit-and-
//! reconcile path production uses — makes **every** node host a running
//! `PartitionReplica` for exactly the partitions whose `Replica_Set` contains
//! it, places the new topic in every node's served catalogue, and never starts
//! the dedicated metadata group (`__meta/0`) as a fleet member. A subsequent
//! committed `DeleteTopic` then stops exactly those replicas on every node and
//! removes the topic from every served catalogue. Determinism: an identical
//! seed and identical parameters yield an identical per-node hosted-partition
//! set.
//!
//! Because a node's `fleet` map is crate-private, the test observes fleet
//! membership through the public surface that tracks it exactly: a client
//! partition is hosted iff [`SimNode::transport`] returns a handle for its
//! group (transports are minted on spawn and dropped on stop in lock-step with
//! the fleet), and the hosted **count** is [`SimNode::fleet_len`]. The metadata
//! group is asserted never to appear as a fleet transport, and to remain hosted
//! by the node's controller throughout — the public expression of "`__meta/0`
//! is never started or stopped by reconcile".
//!
//! Validates: Requirements 3.4

use std::collections::BTreeSet;

use proptest::prelude::*;
use vela_core::{
    metadata_group_key, ClusterCommand, ClusterMetadata, GroupKey, LogBackend, Partition,
    PartitionIndex,
};
use vela_sim::cluster::{SimNode, SimulatedCluster, Topology};
use vela_sim::scenario::{RunConfig, ScenarioParameters};

/// The topic name every generated run creates and deletes.
const TOPIC: &str = "orders";

/// Generate `(seed, node_count, replication_factor, partition_count)` covering
/// the full declared shape space: `node_count` in `1..=5`, `replication_factor`
/// in `1..=node_count` (so it is always a valid set), and `partition_count` in
/// `1..=5`, paired with an arbitrary 64-bit seed.
fn shape() -> impl Strategy<Value = (u64, usize, usize, u32)> {
    (any::<u64>(), 1usize..=5).prop_flat_map(|(seed, node_count)| {
        (Just(seed), Just(node_count), 1usize..=node_count, 1u32..=5)
    })
}

/// Build a [`SimulatedCluster`] for the given shape from `seed`, with all other
/// scenario parameters at their documented defaults (a healthy cluster: no
/// injected faults).
fn build_cluster(
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
    .expect("a valid shape (rf in 1..=node_count, partition_count >= 1) assembles a cluster")
}

/// A `CreateTopic` command for `name` whose partitions carry the topology's
/// fixed `Replica_Set`s — exactly the catalogue the metadata group would commit,
/// so the reconcile pass spawns a replica on each assigned node. Mirrors the
/// production catalogue shape (and the crate's own `create_topic_for` helper).
fn create_topic_for(topo: &Topology, name: &str) -> ClusterCommand {
    let partitions = (0..topo.partition_count())
        .map(|p| {
            let index = PartitionIndex(p);
            Partition {
                index,
                replicas: topo
                    .replica_set_for(index)
                    .expect("partition index within range has a replica set")
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

/// The set of partition indices `node` currently hosts a client replica for, in
/// `topic`, observed through the public transport surface (a transport exists
/// for a group iff the node's fleet hosts it).
fn hosted_partitions(node: &SimNode, topic: &str, partition_count: u32) -> BTreeSet<u32> {
    (0..partition_count)
        .filter(|p| {
            let group: GroupKey = (topic.to_string(), PartitionIndex(*p));
            node.transport(&group).is_some()
        })
        .collect()
}

/// Whether `meta` (the served catalogue) lists `topic`.
fn serves_topic(meta: &ClusterMetadata, topic: &str) -> bool {
    meta.topics.contains_key(topic)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: deterministic-simulation-testing, Property 8: Topic-create
    // coordination
    #[test]
    fn create_then_delete_coordinates_every_node((seed, node_count, rf, partition_count) in shape()) {
        let mut cluster = build_cluster(seed, node_count, rf, partition_count);
        let topology = cluster.topology().clone();
        let meta_group = metadata_group_key();

        // Before the create, no node hosts the topic and no fleet exists.
        for node in cluster.nodes() {
            prop_assert!(!serves_topic(node.served(), TOPIC));
            prop_assert_eq!(node.fleet_len(), 0);
        }

        // --- Apply a committed CreateTopic carrying the fixed replica sets. ---
        let create = create_topic_for(&topology, TOPIC);
        cluster
            .apply_committed_metadata(&create)
            .expect("a committed CreateTopic over Sim_Storage applies");

        for node in cluster.nodes() {
            // The exact partitions whose Replica_Set contains this node.
            let expected: BTreeSet<u32> = (0..partition_count)
                .filter(|p| {
                    topology
                        .replica_set_for(PartitionIndex(*p))
                        .expect("partition index within range")
                        .contains(node.id())
                })
                .collect();

            // (Req 3.4) The node hosts a replica for EXACTLY those partitions —
            // membership matches the replica-set predicate partition-for-
            // partition, and the hosted count agrees, so there are no extra and
            // no missing replicas.
            let hosted = hosted_partitions(node, TOPIC, partition_count);
            prop_assert_eq!(&hosted, &expected, "node {:?} fleet membership", node.id());
            prop_assert_eq!(node.fleet_len(), expected.len(), "node {:?} fleet size", node.id());

            // (Req 3.4) Every node's served catalogue contains the new topic.
            prop_assert!(serves_topic(node.served(), TOPIC), "node {:?} serves topic", node.id());

            // (Req 3.4) The metadata group is never a fleet member: it has no
            // fleet transport, and the controller still hosts `__meta/0`. So the
            // create neither started nor stopped the metadata group.
            prop_assert!(node.transport(&meta_group).is_none(), "node {:?} meta not in fleet", node.id());
            prop_assert!(
                node.controller().expect("running node has a controller").hosts_metadata_group(),
                "node {:?} controller still hosts meta", node.id()
            );
        }

        // Every assigned (partition, node) pair is covered by some node, and the
        // total number of hosted replicas across the cluster equals the sum of
        // replica-set sizes — a cross-check that coordination is cluster-wide,
        // not just locally consistent.
        let total_hosted: usize = cluster
            .nodes()
            .iter()
            .map(|n| hosted_partitions(n, TOPIC, partition_count).len())
            .sum();
        let total_assignments: usize = (0..partition_count)
            .map(|p| {
                topology
                    .replica_set_for(PartitionIndex(p))
                    .expect("partition index within range")
                    .len()
            })
            .sum();
        prop_assert_eq!(total_hosted, total_assignments);
        prop_assert_eq!(total_assignments, partition_count as usize * rf);

        // --- A subsequent committed DeleteTopic stops exactly those replicas. -
        let delete = ClusterCommand::DeleteTopic { name: TOPIC.to_string() };
        cluster
            .apply_committed_metadata(&delete)
            .expect("a committed DeleteTopic applies");

        for node in cluster.nodes() {
            // Every replica the create started is stopped on every node.
            prop_assert_eq!(node.fleet_len(), 0, "node {:?} fleet drained", node.id());
            prop_assert!(
                hosted_partitions(node, TOPIC, partition_count).is_empty(),
                "node {:?} hosts no replica after delete", node.id()
            );
            // The topic is removed from every node's served catalogue.
            prop_assert!(!serves_topic(node.served(), TOPIC), "node {:?} drops topic", node.id());
            // The metadata group is still hosted by the controller and never a
            // fleet member — delete did not touch `__meta/0` either.
            prop_assert!(node.transport(&meta_group).is_none());
            prop_assert!(
                node.controller().expect("running node has a controller").hosts_metadata_group()
            );
        }
    }

    // Feature: deterministic-simulation-testing, Property 8: Topic-create
    // coordination (determinism component)
    #[test]
    fn create_coordination_is_deterministic((seed, node_count, rf, partition_count) in shape()) {
        // Same seed + params => identical per-node hosted-partition sets.
        let build = || {
            let mut cluster = build_cluster(seed, node_count, rf, partition_count);
            let create = create_topic_for(&cluster.topology().clone(), TOPIC);
            cluster
                .apply_committed_metadata(&create)
                .expect("a committed CreateTopic applies");
            cluster
                .nodes()
                .iter()
                .map(|n| hosted_partitions(n, TOPIC, partition_count))
                .collect::<Vec<_>>()
        };

        prop_assert_eq!(build(), build());
    }
}
