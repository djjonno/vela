//! Property test for partition registration in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 7
//!
//! Property 7: Topic creation registers N partitions indexed 0..N-1. For any
//! valid topic name and any partition count `N`, a successful `create_topic`
//! registers exactly `N` partitions whose indices are precisely `0, 1, ..., N-1`
//! in ascending order.
//!
//! The requirement bounds the partition count at `1..=10_000` (Requirement 2.1,
//! 2.2). To keep each iteration fast while still exercising single-partition,
//! small, and large-but-representative topics, the generator draws `N` from
//! `1..=512`; the full upper bound of `10_000` is covered by the unit tests in
//! `vela-core` (`accepts_partition_count_at_boundaries`).
//!
//! Validates: Requirements 2.1, 2.2

use proptest::prelude::*;
use vela_core::{ClusterMetadata, LogBackend, Member, NodeAvailability, NodeId, PartitionIndex};

/// The replication factor used throughout this test. The cluster is built with
/// at least this many available members so that creation is never rejected for
/// insufficient nodes — keeping the property focused on partition registration.
const REPLICATION_FACTOR: usize = 3;

/// Build a cluster of `n` available members named `node-0..node-{n-1}`.
fn cluster(n: usize) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = (0..n)
        .map(|i| Member {
            id: NodeId::new(format!("node-{i}")),
            addr: format!("node-{i}:7001"),
            advertised_addr: format!("node-{i}:7001"),
            availability: NodeAvailability::Available,
        })
        .collect();
    meta
}

/// Generate a valid topic name: 1–255 characters drawn only from
/// `[A-Za-z0-9_-]` (Requirement 2.1). The regex constrains the generator to the
/// exact allowed input space so every generated name is accepted.
fn valid_name_strategy() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Za-z0-9_-]{1,255}").expect("valid regex")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 7
    #[test]
    fn topic_creation_registers_n_partitions_indexed_0_to_n_minus_1(
        name in valid_name_strategy(),
        partition_count in 1u32..=512,
    ) {
        // A cluster with enough available members to satisfy the replication
        // factor, so the only thing under test is partition registration.
        let mut meta = cluster(REPLICATION_FACTOR);

        // A valid name and in-range partition count must create successfully
        // (Requirement 2.1).
        meta.create_topic(&name, partition_count, REPLICATION_FACTOR, LogBackend::Durable)
            .expect("valid topic creation must succeed");

        let topic = meta
            .topics
            .get(&name)
            .expect("created topic must be registered in metadata");

        // Exactly N partitions are registered (Requirement 2.1, 2.2).
        prop_assert_eq!(topic.partitions.len(), partition_count as usize);

        // The partition indices are exactly 0..N-1, in ascending order with no
        // gaps, duplicates, or reordering (Requirement 2.2).
        for (i, partition) in topic.partitions.iter().enumerate() {
            prop_assert_eq!(partition.index, PartitionIndex(i as u32));
        }

        // Cross-check the index set independently of ordering: collecting the
        // raw index values must yield precisely {0, 1, ..., N-1}.
        let observed: Vec<u32> = topic.partitions.iter().map(|p| p.index.0).collect();
        let expected: Vec<u32> = (0..partition_count).collect();
        prop_assert_eq!(observed, expected);
    }
}
