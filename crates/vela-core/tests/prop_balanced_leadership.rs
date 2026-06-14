//! Property test for balanced partition leadership (Property 12).
//!
//! Feature: vela-streaming-platform, Property 12
//!
//! Property 12: Leadership is balanced across nodes per topic.
//! For any cluster with 2 or more member nodes and any created topic, the
//! difference between the maximum and minimum number of partition leaderships
//! assigned to any single node for that topic is at most one.
//!
//! **Validates: Requirements 10.1**
//!
//! ## How the balancing set is chosen
//!
//! `ClusterMetadata::create_topic` assigns partition `i`'s replicas starting at
//! offset `i % nodes.len()` over the *available member nodes*, and the first
//! replica is the leader. Leaders therefore cycle `nodes[0], nodes[1], ...`
//! across the available member nodes, so the balancing set is the full set of
//! available member nodes. The tally below seeds a count of `0` for every
//! available member (not only those that happen to receive a leadership) so a
//! node that receives zero leaderships still participates in the max/min, which
//! is what Requirement 10.1 measures.

use std::collections::BTreeMap;

use proptest::prelude::*;
use vela_core::{ClusterMetadata, Member, NodeAvailability, NodeId};

/// Build a cluster of `n` available members named `node-0..node-{n-1}`.
fn cluster(n: usize) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = (0..n)
        .map(|i| Member {
            id: NodeId::new(format!("node-{i}")),
            addr: format!("node-{i}:7001"),
            availability: NodeAvailability::Available,
        })
        .collect();
    meta
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Feature: vela-streaming-platform, Property 12
    ///
    /// For any cluster with >= 2 members, a valid replication factor, and a
    /// varied partition count, creating a topic leaves per-node leadership
    /// counts (measured over all available member nodes) differing by at most
    /// one.
    #[test]
    fn leadership_is_balanced_across_member_nodes(
        // 2+ member nodes (Requirement 10.1 applies while the cluster has >= 2).
        n_nodes in 2usize..=8,
        // A varied partition count; kept modest to keep iterations fast while
        // still exercising counts below, equal to, and well above node count.
        partition_count in 1u32..=300,
        // A replication factor in 1..=n_nodes is selected below from this seed.
        rf_seed in 0usize..8,
    ) {
        // Constrain the replication factor to the valid range 1..=n_nodes so
        // creation is never rejected for insufficient nodes.
        let replication_factor = (rf_seed % n_nodes) + 1;

        let mut meta = cluster(n_nodes);
        meta.create_topic("orders", partition_count, replication_factor)
            .expect("valid create_topic request should succeed");

        let topic = &meta.topics["orders"];

        // Seed a leadership count of 0 for every available member node, so
        // nodes that receive no leadership still count toward the min/max.
        let mut leader_counts: BTreeMap<NodeId, usize> = meta
            .members
            .iter()
            .filter(|m| matches!(m.availability, NodeAvailability::Available))
            .map(|m| (m.id.clone(), 0usize))
            .collect();

        for p in &topic.partitions {
            let leader = p.leader.clone().expect("each partition has a leader");
            // Every leader must be one of the partition's replicas...
            prop_assert!(
                p.replicas.contains(&leader),
                "leader {leader:?} must be one of the partition's replicas"
            );
            // ...and a known available member node (the balancing set).
            let count = leader_counts
                .get_mut(&leader)
                .expect("leader must be an available member node");
            *count += 1;
        }

        let max = *leader_counts.values().max().expect("non-empty cluster");
        let min = *leader_counts.values().min().expect("non-empty cluster");

        prop_assert!(
            max - min <= 1,
            "per-topic leadership imbalance: max {max} - min {min} > 1 \
             (nodes={n_nodes}, partitions={partition_count}, rf={replication_factor})"
        );
    }
}
