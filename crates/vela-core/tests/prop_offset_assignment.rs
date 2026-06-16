//! Property test for offset assignment on the produce path in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 15
//!
//! Property 15: Committed records receive unique, gap-free, monotonic 0-based
//! offsets. Producing a sequence of records to a single partition assigns the
//! offsets `0, 1, 2, ...` — each produce returns the next sequential offset, so
//! across the whole sequence the assigned offsets are unique, contiguous (no
//! gaps), monotonically increasing by exactly one, and start at zero for the
//! first committed record. Reading the partition back yields the same records
//! in offset order.
//!
//! This is the domain-layer realization of Requirements 4.3, 4.4, and 4.7:
//! a record received by a partition's leader is appended (4.3); once committed
//! the assigned zero-based offset is returned to the producer (4.4); and across
//! multiple committed records the offsets are unique and increase monotonically
//! by one in commit order, starting at zero (4.7).
//!
//! The test runs over a single-node Raft group driven to leader. In a
//! single-node group the leader is its own majority, so each proposed record
//! commits within the produce step and its offset is available immediately —
//! letting the offset-assignment invariant be exercised directly, free of
//! replication timing.
//!
//! Validates: Requirements 4.3, 4.4, 4.7

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_core::{
    produce, ClusterMetadata, Member, NodeAvailability, NodeId, Partition, PartitionIndex,
    RaftGroupFleet, Record, Topic, TopicState,
};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, Role, TimerKind};

/// A minimal [`Clock`] that never advances on its own; arming a timer is a
/// no-op. The test drives consensus with explicit [`RaftInput`]s, so no real
/// timing is needed and runs stay deterministic.
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

/// Build metadata holding a single-partition topic led by `node-0`, over a
/// cluster of one available member — matching the single-node leader fleet.
fn metadata_with_topic(name: &str) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = vec![Member {
        id: NodeId::new("node-0"),
        addr: "node-0:7001".to_string(),
        availability: NodeAvailability::Available,
    }];
    meta.topics.insert(
        name.to_string(),
        Topic {
            name: name.to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas: vec![NodeId::new("node-0")],
                leader: Some(NodeId::new("node-0")),
            }],
            state: TopicState::Active,
            backend: vela_core::LogBackend::Durable,
        },
    );
    meta
}

/// A fleet hosting one single-node group for `topic`/partition 0, driven to
/// leader (its lone self-vote is a majority of one).
fn fleet_with_leader(topic: &str, clock: &mut TestClock) -> RaftGroupFleet {
    let mut fleet = RaftGroupFleet::new();
    let key = (topic.to_string(), PartitionIndex(0));
    fleet
        .create_group(key.clone(), RaftNodeId(0), Vec::new())
        .expect("creating the single-node group must succeed");
    let replica = fleet.get_mut(&key).expect("the group must be hosted");
    replica.step(RaftInput::Tick(TimerKind::Election), clock);
    assert_eq!(replica.role(), Role::Leader);
    fleet
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 15
    #[test]
    fn committed_records_receive_unique_gap_free_monotonic_zero_based_offsets(
        // A varied number of records, each with arbitrary opaque value bytes
        // (bounded well under the 1 MiB limit so none is rejected for size).
        values in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..32),
            1..200,
        ),
    ) {
        let topic = "orders";
        let mut clock = TestClock::new();
        let meta = metadata_with_topic(topic);
        let mut fleet = fleet_with_leader(topic, &mut clock);

        let mut assigned = Vec::with_capacity(values.len());
        for (i, value) in values.iter().enumerate() {
            let record = Record::new(None, value.clone());
            let offset = produce(
                &meta,
                &mut fleet,
                topic,
                PartitionIndex(0),
                &record,
                &mut clock,
            )
            .expect("a record produced to a single-node leader commits");

            // 4.4 / 4.7: each produce returns the next sequential zero-based
            // offset, equal to the count of records committed before it.
            prop_assert_eq!(offset, i as u64);
            assigned.push(offset);
        }

        let n = values.len() as u64;

        // 4.7: the assigned offsets are exactly 0, 1, ..., n-1 — unique,
        // gap-free, monotonically increasing by one, and starting at zero.
        let expected: Vec<u64> = (0..n).collect();
        prop_assert_eq!(&assigned, &expected);

        // Read the partition back: every record is present, in offset order,
        // with the value that was produced at that offset (4.3, 4.7).
        let replica = fleet
            .get(&(topic.to_string(), PartitionIndex(0)))
            .expect("the partition group is hosted");
        prop_assert_eq!(replica.high_water_mark(), Some(n - 1));

        let read_back = replica.read(0, values.len());
        prop_assert_eq!(read_back.len(), values.len());
        for (i, (committed, value)) in read_back.iter().zip(values.iter()).enumerate() {
            prop_assert_eq!(committed.offset, i as u64);
            prop_assert_eq!(&committed.value, value);
        }
    }
}
