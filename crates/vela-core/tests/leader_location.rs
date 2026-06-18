//! Example/edge tests for `FindLeader` leader-location resolution in
//! `vela-core` ([`MetadataController::find_leader`]).
//!
//! These cover the two non-happy-path edges of leader location:
//!
//! - A request for a topic or partition that does not exist is rejected with
//!   [`CoreError::PartitionNotFound`] and returns no node (Requirement 10.5).
//! - A request for a partition whose Raft group is mid-election — its
//!   [`Partition::leader`] is `None` — is rejected with
//!   [`CoreError::PartitionUnavailable`], and the query leaves the cluster
//!   metadata unchanged (Requirement 10.6).
//!
//! `find_leader` takes `&self`, so it is structurally incapable of mutating the
//! controller's metadata; the "without mutating metadata" clause of Requirement
//! 10.6 is asserted directly by snapshotting `epoch` and `topics` before and
//! after the query and confirming they are identical. The 5-second deadline of
//! Requirement 10.6 is a wall-clock concern owned by the server crate; the core
//! resolution is synchronous and returns immediately, which the timing
//! assertion below confirms is well within the deadline.
//!
//! Validates: Requirements 10.5, 10.6

use std::time::{Duration, Instant};

use vela_core::{
    ClusterCommand, CoreError, LogBackend, MetadataController, NodeId, Partition, PartitionIndex,
};
use vela_raft::NodeId as RaftNodeId;

/// The Requirement 10.6 leader-unavailable deadline: `find_leader` must resolve
/// well within this 5-second wall-clock bound.
const LEADER_UNAVAILABLE_DEADLINE: Duration = Duration::from_secs(5);

/// Build a `Partition` with the given index and optional leader, using a fixed
/// two-replica set (the replica identities are irrelevant to leader location).
fn partition(index: u32, leader: Option<&str>) -> Partition {
    Partition {
        index: PartitionIndex(index),
        replicas: vec![NodeId::new("a"), NodeId::new("b")],
        leader: leader.map(NodeId::new),
    }
}

/// A controller seeded with a single `orders` topic via a committed
/// `CreateTopic` command, mirroring how metadata reaches the controller in
/// production (through the `__meta/p0` group's applied commands).
fn controller_with_orders(partitions: Vec<Partition>) -> MetadataController {
    let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
    controller.apply(&ClusterCommand::CreateTopic {
        name: "orders".to_string(),
        partitions,
        backend: LogBackend::Durable,
    });
    controller
}

/// `FindLeader` on a topic that does not exist returns `PartitionNotFound` and
/// no node (Requirement 10.5).
#[test]
fn find_leader_on_missing_topic_is_not_found() {
    // A topic exists, but we ask for a different, unknown one.
    let controller = controller_with_orders(vec![partition(0, Some("node-a"))]);

    let result = controller.find_leader("ghost", PartitionIndex(0));

    assert_eq!(
        result,
        Err(CoreError::PartitionNotFound {
            topic: "ghost".to_string(),
            index: 0,
        }),
        "an unknown topic must resolve to PartitionNotFound and no node"
    );
}

/// `FindLeader` on a known topic but an out-of-range partition index returns
/// `PartitionNotFound` and no node (Requirement 10.5).
#[test]
fn find_leader_on_out_of_range_partition_is_not_found() {
    // `orders` has exactly one partition (index 0); index 3 is out of range.
    let controller = controller_with_orders(vec![partition(0, Some("node-a"))]);

    let result = controller.find_leader("orders", PartitionIndex(3));

    assert_eq!(
        result,
        Err(CoreError::PartitionNotFound {
            topic: "orders".to_string(),
            index: 3,
        }),
        "a known topic with an out-of-range partition must resolve to \
         PartitionNotFound carrying that topic and index"
    );
}

/// `FindLeader` on a partition whose leader is `None` (an election is in
/// progress) returns `PartitionUnavailable`, does so well within the 5-second
/// deadline, and leaves the controller's metadata unchanged (Requirement 10.6).
#[test]
fn find_leader_mid_election_is_unavailable_within_deadline_and_does_not_mutate() {
    // Partition 0 has no elected leader: mid-election.
    let controller = controller_with_orders(vec![partition(0, None)]);

    // Snapshot the observable metadata state before the query so we can prove
    // the read did not mutate it (Requirement 10.6, "without mutating
    // metadata"). `find_leader` takes `&self`, so this is belt-and-braces.
    let epoch_before = controller.metadata().epoch;
    let topics_before = controller.metadata().topics.clone();

    let start = Instant::now();
    let result = controller.find_leader("orders", PartitionIndex(0));
    let elapsed = start.elapsed();

    // The leader is unavailable while the election is in progress.
    assert_eq!(
        result,
        Err(CoreError::PartitionUnavailable),
        "a partition with no elected leader must resolve to PartitionUnavailable"
    );

    // The unavailable answer is produced within the 5-second deadline
    // (Requirement 10.6) — the core resolution is synchronous and immediate.
    assert!(
        elapsed < LEADER_UNAVAILABLE_DEADLINE,
        "leader-unavailable must be returned within {LEADER_UNAVAILABLE_DEADLINE:?}, took {elapsed:?}"
    );

    // The query left the metadata epoch and topics untouched.
    assert_eq!(
        controller.metadata().epoch,
        epoch_before,
        "find_leader must not bump the metadata epoch"
    );
    assert_eq!(
        controller.metadata().topics,
        topics_before,
        "find_leader must not alter the topic/partition metadata"
    );
}
