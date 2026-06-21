//! Property test for non-leader produce redirection in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 16
//!
//! Property 16: Produce to a non-leader is redirected and writes nothing. For
//! any believed leader recorded in the partition's metadata and any record
//! payload, producing to a locally hosted replica that is **not** the partition
//! leader returns [`CoreError::NotLeader`] carrying that believed leader, and
//! the replica appends no Log_Entry (its high-water mark stays `None`).
//!
//! This is the domain-layer realization of two requirements that describe the
//! same redirect from the two ends of the wire:
//!
//! - Requirement 4.6 — "IF a Record is received by a Node that is not the Leader
//!   of the target Partition, THEN THE Vela SHALL return an error identifying
//!   the current Leader of that Partition and SHALL append no Log_Entry."
//! - Requirement 11.2 — "IF a client request reaches a Node that is not the
//!   Leader of the target Partition, THEN THE Vela SHALL respond with a
//!   redirection identifying the current Leader."
//!
//! The `leader` carried by [`CoreError::NotLeader`] is exactly the believed
//! leader the [`ClusterMetadata`] holds for the partition, so the producer can
//! redirect; "append no Log_Entry" is checked through the replica's
//! [`high_water_mark`](vela_core::PartitionReplica::high_water_mark) staying
//! `None` — nothing committed, nothing written.
//!
//! Validates: Requirements 4.6, 11.2

use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_core::{
    produce, ClusterMetadata, CoreError, Member, NodeAvailability, NodeId, Partition,
    PartitionIndex, RaftGroupFleet, Record, Topic, TopicState,
};
use vela_raft::{Clock, NodeId as RaftNodeId, Role, TimerKind};

/// A minimal [`Clock`] that never advances on its own; arming a timer is a
/// no-op. The replica under test is never driven to leader, so no timing is
/// exercised — the clock only satisfies the `produce` signature.
struct TestClock {
    now: Instant,
}

impl TestClock {
    fn new() -> Self {
        Self {
            now: Instant::now(),
        }
    }
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.now
    }
    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// Generate a node identity: a short, lowercase identifier. Used for the
/// believed leader recorded in metadata.
fn node_id_strategy() -> impl Strategy<Value = NodeId> {
    proptest::string::string_regex("[a-z][a-z0-9_-]{0,7}")
        .expect("valid regex")
        .prop_map(NodeId::new)
}

/// Generate the believed leader for the partition: either a concrete node
/// (`Some`) or `None` (an election in progress). The redirect must identify
/// whichever the metadata holds, so both cases are exercised.
fn believed_leader_strategy() -> impl Strategy<Value = Option<NodeId>> {
    proptest::option::of(node_id_strategy())
}

/// Generate a record whose combined key+value stays well under the 1 MiB limit,
/// so the produce path reaches the leadership check rather than failing payload
/// validation first (that ordering is a different property, 18).
fn record_strategy() -> impl Strategy<Value = Record> {
    let key = proptest::option::of(prop::collection::vec(any::<u8>(), 0..=64));
    let value = prop::collection::vec(any::<u8>(), 0..=256);
    (key, value).prop_map(|(key, value)| Record::new(key, value))
}

/// Build metadata holding a single-partition topic whose partition leader is
/// `believed_leader`, over a one-member cluster.
fn metadata_with_leader(topic: &str, believed_leader: Option<NodeId>) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = vec![Member {
        id: NodeId::new("node-0"),
        addr: "node-0:7001".to_string(),
        advertised_addr: "node-0:7001".to_string(),
        availability: NodeAvailability::Available,
    }];
    meta.topics.insert(
        topic.to_string(),
        Topic {
            name: topic.to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas: vec![NodeId::new("node-0")],
                leader: believed_leader,
            }],
            state: TopicState::Active,
            backend: vela_core::LogBackend::Durable,
        },
    );
    meta
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 16
    #[test]
    fn produce_to_a_non_leader_is_redirected_and_writes_nothing(
        believed_leader in believed_leader_strategy(),
        record in record_strategy(),
    ) {
        let topic = "orders";
        let mut clock = TestClock::new();
        let meta = metadata_with_leader(topic, believed_leader.clone());

        // Host a replica for the partition with peers, and never drive an
        // election: it stays a Follower, i.e. not the partition leader.
        let mut fleet = RaftGroupFleet::new();
        let key = (topic.to_string(), PartitionIndex(0));
        fleet
            .create_group(key.clone(), RaftNodeId(0), vec![RaftNodeId(1), RaftNodeId(2)])
            .expect("creating a fresh partition group must succeed");
        prop_assert_eq!(fleet.get(&key).unwrap().role(), Role::Follower);

        // Producing to the follower is redirected to the believed leader from
        // metadata, and nothing is appended (Requirements 4.6, 11.2).
        let result = produce(&meta, &mut fleet, topic, PartitionIndex(0), &record, &mut clock);
        prop_assert_eq!(
            result,
            Err(CoreError::NotLeader {
                leader: believed_leader,
            })
        );

        // The non-leader appended no Log_Entry: its high-water mark is still
        // `None` (nothing committed, nothing written).
        prop_assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }
}
