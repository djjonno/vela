//! Example tests for topic-delete lifecycle ordering and not-found handling.
//!
//! These cover the deletion behaviours that are specific examples rather than
//! universal properties (see the design's Unit & Example Tests section):
//!
//! - The Raft group of each partition is stopped *before* that partition's
//!   in-memory log is released (Requirements 3.2, 3.3), verified with a mock
//!   fleet that records the order of teardown actions.
//! - Deleting a topic that does not exist returns a not-found error and leaves
//!   the cluster metadata unchanged (Requirement 3.4).
//!
//! The domain layer ([`ClusterMetadata`]) does not own the live Raft groups or
//! logs; it exposes [`ClusterMetadata::delete_topic_with`] whose
//! `on_stop_partition` hook is the seam where the server's fleet performs that
//! teardown. The `MockFleet` below stands in for that fleet: for each partition
//! it records `StopRaftGroup` then `ReleaseLog`, modelling the required
//! stop-before-release ordering, so the tests can assert the ordering and that
//! the topic is removed only after every partition has been torn down.

use vela_core::{ClusterMetadata, CoreError, Member, NodeAvailability, NodeId, Partition};

/// A single action the fleet performs while tearing down a partition.
///
/// The unit of consensus and the unit of the log are both the partition, so
/// each action is tagged with the partition index it applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FleetAction {
    /// The partition's Raft group was stopped (Requirement 3.2).
    StopRaftGroup(u32),
    /// The partition's in-memory log was released (Requirement 3.3).
    ReleaseLog(u32),
}

/// A stand-in for the server's `RaftGroupFleet` (task 11.4).
///
/// It owns no real Raft groups or logs; it only records, in order, the
/// teardown actions it is asked to perform. Wiring it through
/// `delete_topic_with`'s hook lets these tests observe the exact ordering of
/// stop-vs-release across every partition of a deleted topic.
#[derive(Debug, Default)]
struct MockFleet {
    /// The ordered log of actions performed across all partitions.
    events: Vec<FleetAction>,
}

impl MockFleet {
    /// Tear down one partition: stop its Raft group, then release its log.
    ///
    /// This models the fleet's required ordering (Requirement 3.2 before 3.3)
    /// and is the closure body wired into [`ClusterMetadata::delete_topic_with`].
    fn stop_partition(&mut self, partition: &Partition) {
        let idx = partition.index.0;
        self.events.push(FleetAction::StopRaftGroup(idx));
        self.events.push(FleetAction::ReleaseLog(idx));
    }
}

/// An available cluster member named `node-{i}`.
fn member(id: &str) -> Member {
    Member {
        id: NodeId::new(id),
        addr: format!("{id}:7001"),
        availability: NodeAvailability::Available,
    }
}

/// A cluster of `n` available members named `node-0..node-{n-1}`.
fn cluster(n: usize) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = (0..n).map(|i| member(&format!("node-{i}"))).collect();
    meta
}

/// For a topic with several partitions, the fleet stops each partition's Raft
/// group before releasing that partition's log, and the topic is removed only
/// after every partition has been torn down (Requirements 3.2, 3.3).
#[test]
fn raft_group_is_stopped_before_log_is_released_for_every_partition() {
    let mut meta = cluster(3);
    let partition_count = 5u32;
    meta.create_topic("orders", partition_count, 3).unwrap();

    let mut fleet = MockFleet::default();
    meta.delete_topic_with("orders", |partition| fleet.stop_partition(partition))
        .unwrap();

    // Two actions per partition: a stop and a release.
    assert_eq!(fleet.events.len(), (partition_count * 2) as usize);

    // For every partition, the StopRaftGroup action appears strictly before the
    // ReleaseLog action (Requirement 3.2 before 3.3).
    for index in 0..partition_count {
        let stop_at = fleet
            .events
            .iter()
            .position(|e| *e == FleetAction::StopRaftGroup(index))
            .unwrap_or_else(|| panic!("partition {index} was never stopped"));
        let release_at = fleet
            .events
            .iter()
            .position(|e| *e == FleetAction::ReleaseLog(index))
            .unwrap_or_else(|| panic!("partition {index} log was never released"));
        assert!(
            stop_at < release_at,
            "partition {index}: Raft group (at {stop_at}) must stop before the \
             log is released (at {release_at})"
        );
    }

    // The topic — and all of its partitions — is gone only after teardown.
    assert!(!meta.topics.contains_key("orders"));
}

/// Every partition of the topic is handed to the fleet exactly once, so no
/// partition's log is released without its Raft group first being stopped
/// (Requirements 3.2, 3.3).
#[test]
fn each_partition_is_stopped_and_released_exactly_once() {
    let mut meta = cluster(2);
    meta.create_topic("events", 4, 2).unwrap();

    let mut fleet = MockFleet::default();
    meta.delete_topic_with("events", |partition| fleet.stop_partition(partition))
        .unwrap();

    for index in 0..4u32 {
        let stops = fleet
            .events
            .iter()
            .filter(|e| **e == FleetAction::StopRaftGroup(index))
            .count();
        let releases = fleet
            .events
            .iter()
            .filter(|e| **e == FleetAction::ReleaseLog(index))
            .count();
        assert_eq!(stops, 1, "partition {index} must be stopped exactly once");
        assert_eq!(releases, 1, "partition {index} log released exactly once");
    }
}

/// Deleting a topic that does not exist returns `TopicNotFound` carrying the
/// requested name, performs no teardown, and leaves the metadata unchanged
/// (Requirement 3.4).
#[test]
fn deleting_a_missing_topic_returns_not_found_and_changes_nothing() {
    let mut meta = cluster(3);
    meta.create_topic("orders", 3, 3).unwrap();
    let before = meta.clone();

    // The plain convenience form returns the not-found error with the name.
    assert_eq!(
        meta.delete_topic("ghost"),
        Err(CoreError::TopicNotFound("ghost".to_string()))
    );
    // Topics and epoch are completely untouched.
    assert_eq!(meta, before);

    // And via the hook form, no partition teardown is attempted for a topic
    // that does not exist.
    let mut fleet = MockFleet::default();
    let result = meta.delete_topic_with("ghost", |partition| fleet.stop_partition(partition));
    assert_eq!(result, Err(CoreError::TopicNotFound("ghost".to_string())));
    assert!(
        fleet.events.is_empty(),
        "no Raft group or log teardown for a topic that does not exist"
    );
    assert_eq!(meta, before, "metadata must be unchanged on not-found");
}
