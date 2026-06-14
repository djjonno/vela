//! Property test for committed consume ordering on the consume path in
//! `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 19
//!
//! Property 19: Consume returns only committed records in ascending offset
//! order. For any sequence of committed records and any start offset, `consume`
//! returns exactly the committed records at offsets `>= start`, in strictly
//! ascending offset order, and never any uncommitted record nor anything beyond
//! the partition's highest committed offset.
//!
//! This is the domain-layer realization of Requirements 5.1 and 5.2:
//!
//! - 5.1: when the requested offset is within `0..=high_water_mark`, consume
//!   returns committed records in strictly ascending offset order beginning at
//!   the requested offset.
//! - 5.2: consume returns *only* committed records and excludes any record not
//!   committed by the partition's Raft group.
//!
//! The test produces a varied number of records to a single-node Raft group
//! driven to leader. In a single-node group the leader is its own majority, so
//! every produced record commits immediately, giving a known committed
//! high-water mark. It then consumes from varied start offsets with a large
//! max and asserts the returned offsets are exactly `start..=highest` in
//! strictly ascending order with values matching what was produced, and that
//! nothing past the committed high-water mark ever appears.
//!
//! Validates: Requirements 5.1, 5.2

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_core::{
    consume, produce, ClusterMetadata, Member, NodeAvailability, NodeId, Partition, PartitionIndex,
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

    // Feature: vela-streaming-platform, Property 19
    #[test]
    fn consume_returns_only_committed_records_in_ascending_offset_order(
        // A varied number of records, each with arbitrary opaque value bytes
        // (bounded well under the 1 MiB limit so none is rejected for size).
        values in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..32),
            1..200,
        ),
        // An arbitrary start offset spanning within, at, and beyond the
        // committed range to exercise mid-stream reads and past-the-end reads.
        start in 0u64..256,
    ) {
        let topic = "orders";
        let mut clock = TestClock::new();
        let meta = metadata_with_topic(topic);
        let mut fleet = fleet_with_leader(topic, &mut clock);

        // Produce every record to the single-node leader; each commits at once,
        // so the committed offsets are exactly 0..n.
        for value in &values {
            let record = Record::new(None, value.clone());
            produce(&meta, &mut fleet, topic, PartitionIndex(0), &record, &mut clock)
                .expect("a record produced to a single-node leader commits");
        }

        let n = values.len() as u64;

        // Consume from the chosen start offset with a max large enough to cover
        // everything committed, so the bound never truncates the result.
        let got = consume(
            &meta,
            &fleet,
            topic,
            PartitionIndex(0),
            start,
            Some(10_000),
        )
        .expect("consuming from a single-node leader succeeds");

        // The committed records are at offsets 0..n. Consuming from `start`
        // returns exactly those at offsets >= start (5.1), and nothing beyond
        // the highest committed offset n-1 (5.2): an empty result once start
        // passes the end.
        let expected_offsets: Vec<u64> = (start..n).collect();
        let got_offsets: Vec<u64> = got.iter().map(|r| r.offset).collect();
        prop_assert_eq!(&got_offsets, &expected_offsets);

        // Strictly ascending offset order (5.1): each offset is exactly one
        // greater than the previous, with no repeats or gaps.
        for pair in got.windows(2) {
            prop_assert!(pair[1].offset > pair[0].offset);
            prop_assert_eq!(pair[1].offset, pair[0].offset + 1);
        }

        // Only committed records appear, and each carries the value produced at
        // its offset (5.2): the record at offset `o` matches `values[o]`, and
        // no returned offset is outside the committed range.
        for record in &got {
            prop_assert!(record.offset < n);
            prop_assert!(record.offset >= start);
            prop_assert_eq!(&record.value, &values[record.offset as usize]);
        }

        // Reading at or past the highest committed offset yields no records and
        // never an uncommitted one (5.2, 5.3).
        if start >= n {
            prop_assert!(got.is_empty());
        }
    }
}
