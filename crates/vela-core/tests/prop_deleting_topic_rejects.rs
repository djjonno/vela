//! Property test for operations on a deleting topic in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 14
//!
//! Property 14: Operations on a deleting topic are rejected.
//! For any topic in the `Deleting` state, any produce or duplicate-delete
//! request for that topic is rejected with a "being deleted" error and changes
//! nothing — no log entry is appended and the cluster metadata is left
//! unchanged.
//!
//! At this domain layer the produce path is fronted by the produce-precheck
//! [`ClusterMetadata::ensure_producible`] (the guard the produce flow consults
//! before appending any log entry, task 11.x), so "appends no log entry" is
//! exercised as "the precheck rejects". The duplicate-delete request is the
//! second deletion submitted while one is already in progress, exercised
//! directly against [`ClusterMetadata::delete_topic`].
//!
//! Validates: Requirements 3.7

use proptest::prelude::*;
use vela_core::{ClusterMetadata, CoreError, LogBackend, Member, NodeAvailability, NodeId};

/// The maximum topic name length the production code accepts (Requirement 2.1).
const MAX_NAME_LEN: usize = 255;

/// Build cluster metadata with `n` distinctly named available members.
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

/// A valid topic name: 1–255 characters of `[A-Za-z0-9_-]`.
fn valid_name_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop::sample::select(
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".to_vec(),
        ),
        1..=MAX_NAME_LEN,
    )
    .prop_map(|bytes| bytes.into_iter().map(|b| b as char).collect())
}

/// A topic that exists in a valid cluster and can be driven into the `Deleting`
/// state: a cluster of `cluster_size` available members and a valid creation
/// request whose replication factor never exceeds the cluster size (so creation
/// succeeds before the topic is marked deleting). Partition count is kept small
/// to keep each case fast while still spanning multi-partition topics.
#[derive(Debug, Clone)]
struct DeletingScenario {
    cluster_size: usize,
    name: String,
    partition_count: u32,
    replication_factor: usize,
}

fn scenario_strategy() -> impl Strategy<Value = DeletingScenario> {
    (1usize..=5)
        .prop_flat_map(|cluster_size| {
            (
                Just(cluster_size),
                valid_name_strategy(),
                1u32..=24,
                1usize..=cluster_size,
            )
        })
        .prop_map(
            |(cluster_size, name, partition_count, replication_factor)| DeletingScenario {
                cluster_size,
                name,
                partition_count,
                replication_factor,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 14
    #[test]
    fn operations_on_a_deleting_topic_are_rejected(scenario in scenario_strategy()) {
        let mut meta = cluster(scenario.cluster_size);

        // Register the topic, then drive it into the `Deleting` state.
        meta.create_topic(
            &scenario.name,
            scenario.partition_count,
            scenario.replication_factor,
            LogBackend::Durable,
        )
        .expect("valid creation request must succeed");
        meta.begin_delete(&scenario.name)
            .expect("marking an active topic as deleting must succeed");

        // Snapshot the metadata *after* it is in the deleting state: the
        // rejected operations below must not change it from here.
        let before = meta.clone();
        let expected = Err(CoreError::TopicDeleting(scenario.name.clone()));

        // Produce-precheck for a deleting topic is rejected with "being
        // deleted" (Requirement 3.7) and, taking `&self`, appends/changes
        // nothing.
        prop_assert_eq!(meta.ensure_producible(&scenario.name), expected.clone());
        prop_assert_eq!(
            &meta,
            &before,
            "produce-precheck rejection must not mutate metadata"
        );

        // A duplicate deletion submitted while one is in progress is rejected
        // with the same error and leaves the topic registered and untouched
        // (Requirement 3.7).
        prop_assert_eq!(meta.delete_topic(&scenario.name), expected);
        prop_assert_eq!(
            &meta,
            &before,
            "duplicate-delete rejection must not mutate metadata"
        );
        prop_assert!(
            meta.topics.contains_key(&scenario.name),
            "the topic must remain registered after a rejected duplicate delete"
        );
    }
}
