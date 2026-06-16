//! Property test for replica assignment in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 8
//!
//! Property 8: Replica assignment uses replication-factor distinct member
//! nodes. For any cluster with at least `replication_factor` members and any
//! topic created on it, every partition has exactly `replication_factor`
//! replicas placed on distinct nodes, all of which are current cluster members.
//!
//! Validates: Requirements 2.3, 9.6

use std::collections::BTreeSet;

use proptest::prelude::*;
use vela_core::{ClusterMetadata, LogBackend, Member, NodeAvailability, NodeId};

/// A cluster of `n` available members named `node-0..node-{n-1}`.
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

/// Generate a `(node_count, replication_factor, partition_count)` triple with
/// `1 <= replication_factor <= node_count`, varied partition counts, and modest
/// upper bounds so the cluster has at least `replication_factor` members while
/// iterations stay fast.
fn cluster_topic_strategy() -> impl Strategy<Value = (usize, usize, u32)> {
    (1usize..=8).prop_flat_map(|node_count| {
        // replication_factor in 1..=node_count, partition_count in 1..=60.
        (Just(node_count), 1usize..=node_count, 1u32..=60)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 8
    #[test]
    fn replica_assignment_uses_rf_distinct_member_nodes(
        (node_count, replication_factor, partition_count) in cluster_topic_strategy(),
    ) {
        let mut meta = cluster(node_count);

        // The set of current cluster member ids, for membership checks below.
        let members: BTreeSet<NodeId> = meta.members.iter().map(|m| m.id.clone()).collect();

        meta.create_topic("topic", partition_count, replication_factor, LogBackend::Durable)
            .expect("creation must succeed when members >= replication_factor");

        let topic = &meta.topics["topic"];

        for partition in &topic.partitions {
            // Exactly `replication_factor` replicas (Requirement 2.3).
            prop_assert_eq!(
                partition.replicas.len(),
                replication_factor,
                "partition {:?} must have exactly replication_factor replicas",
                partition.index
            );

            // All replicas are distinct nodes (Requirement 2.3).
            let distinct: BTreeSet<&NodeId> = partition.replicas.iter().collect();
            prop_assert_eq!(
                distinct.len(),
                replication_factor,
                "partition {:?} replicas must be distinct nodes",
                partition.index
            );

            // Every replica is a current cluster member (Requirement 9.6).
            for replica in &partition.replicas {
                prop_assert!(
                    members.contains(replica),
                    "replica {:?} must be a current cluster member",
                    replica
                );
            }
        }
    }
}
