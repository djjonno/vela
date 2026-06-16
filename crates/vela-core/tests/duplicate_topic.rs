//! Example test for duplicate-topic rejection in `vela-core`.
//!
//! Asserts that creating a topic whose name already exists is rejected with
//! [`CoreError::TopicExists`] and leaves the cluster metadata completely
//! unchanged — including the originally created topic and the metadata epoch.
//!
//! Validates: Requirements 2.4

use vela_core::{ClusterMetadata, CoreError, LogBackend, Member, NodeAvailability, NodeId};

/// Build cluster metadata with `n` available members named `node-0..node-{n-1}`,
/// enough to satisfy the replication factors used below.
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

/// Creating a topic whose name already exists returns `TopicExists` and leaves
/// the metadata byte-for-byte unchanged (Requirement 2.4).
#[test]
fn duplicate_topic_is_rejected_and_metadata_unchanged() {
    let mut meta = cluster(3);

    // First creation succeeds and registers the topic.
    meta.create_topic("orders", 4, 2, LogBackend::Durable)
        .expect("first create_topic should succeed");
    assert!(meta.topics.contains_key("orders"));

    // Snapshot the metadata after the successful creation.
    let snapshot = meta.clone();

    // A second creation of the *same name* with a different partition count
    // must be rejected as a duplicate.
    let result = meta.create_topic("orders", 8, 3, LogBackend::Durable);
    assert_eq!(
        result,
        Err(CoreError::TopicExists("orders".to_string())),
        "duplicate topic creation must return TopicExists(name)"
    );

    // The rejection must leave the metadata entirely unchanged: the original
    // topic (with its original 4 partitions) and the epoch are both untouched.
    assert_eq!(
        meta, snapshot,
        "metadata must be unchanged after a rejected duplicate create"
    );
    assert_eq!(
        meta.topics["orders"].partitions.len(),
        4,
        "the original topic's partitions must be preserved, not replaced"
    );
}
