//! Property test for the consume max-count bound in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 20
//!
//! Property 20: Consume respects the maximum-count bound. A consume request
//! never returns more records than the effective bound: at most the requested
//! maximum when one is supplied (an integer in `1..=10000`), and at most
//! [`DEFAULT_MAX_RECORDS`] (500) when none is supplied. It also never returns
//! more records than are actually available from the requested offset (the
//! committed records at offsets `offset, offset + 1, ...`). In fact the count
//! returned is exactly the smaller of those two limits.
//!
//! This is the domain-layer realization of Requirements 5.5 and 5.6: when a
//! consumer specifies a maximum count between 1 and 10,000 inclusive, no more
//! than that many records are returned (5.5); when a consumer specifies no
//! maximum, no more than 500 records are returned (5.6).
//!
//! The test runs over a single-node Raft group driven to leader. In a
//! single-node group the leader is its own majority, so each produced record
//! commits within the produce step and is immediately consumable — letting the
//! bound be exercised directly, free of replication timing. To make the cap
//! bite (rather than the count being limited only by what is available), the
//! generated record counts can exceed the requested maxima — including counts
//! above 500 so the default-500 cap is truly exercised.
//!
//! Validates: Requirements 5.5, 5.6

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_core::{
    consume, produce, ClusterMetadata, Member, NodeAvailability, NodeId, Partition, PartitionIndex,
    RaftGroupFleet, Record, Topic, TopicState, DEFAULT_MAX_RECORDS, MAX_MAX_RECORDS,
    MIN_MAX_RECORDS,
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
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: vela-streaming-platform, Property 20
    #[test]
    fn consume_respects_the_maximum_count_bound(
        // A varied number of committed records. The range spans counts both
        // below and above 500 so that both an explicit small cap and the
        // default-500 cap are exercised against a partition that actually holds
        // more records than the bound.
        num_records in 1u64..=600,
        // A starting offset that may sit before, within, or beyond the
        // committed range, so the "available from offset" limit is exercised
        // alongside the cap.
        offset in 0u64..=620,
        // A maximum count drawn from three buckets: a small explicit cap (often
        // smaller than what is available, so the cap bites), a full-range
        // explicit cap, and the unspecified case (defaulting to 500).
        max_count in prop_oneof![
            (MIN_MAX_RECORDS..=800u32).prop_map(Some),
            (MIN_MAX_RECORDS..=MAX_MAX_RECORDS).prop_map(Some),
            Just(None),
        ],
    ) {
        let topic = "orders";
        let mut clock = TestClock::new();
        let meta = metadata_with_topic(topic);
        let mut fleet = fleet_with_leader(topic, &mut clock);

        // Produce `num_records` committed records. In a single-node group each
        // commits immediately, so all are consumable.
        for i in 0..num_records {
            let record = Record::new(None, format!("v{i}").into_bytes());
            produce(&meta, &mut fleet, topic, PartitionIndex(0), &record, &mut clock)
                .expect("a record produced to a single-node leader commits");
        }

        // Any explicit max_count generated here is in 1..=10000, so consume
        // must succeed (no parameter rejection).
        let records = consume(&meta, &fleet, topic, PartitionIndex(0), offset, max_count)
            .expect("a valid consume request must succeed");

        // The effective bound: the requested maximum if supplied, else the
        // default of 500 (Requirement 5.5, 5.6).
        let effective_bound = max_count.unwrap_or(DEFAULT_MAX_RECORDS) as u64;

        // The records available from `offset`: the committed offsets are
        // 0..num_records, so an offset at or past num_records leaves none.
        let available = num_records.saturating_sub(offset);

        // 5.5 / 5.6: never more than the effective bound...
        prop_assert!(
            records.len() as u64 <= effective_bound,
            "returned {} records, exceeding the effective bound {}",
            records.len(),
            effective_bound,
        );
        // ...and never more than what is actually available from the offset.
        prop_assert!(
            records.len() as u64 <= available,
            "returned {} records, exceeding the {} available from offset {}",
            records.len(),
            available,
            offset,
        );

        // The count is exactly the smaller of the two limits, so the cap is
        // demonstrably the binding constraint whenever it is the smaller one.
        prop_assert_eq!(records.len() as u64, effective_bound.min(available));

        // The returned records begin at the requested offset and ascend by one,
        // confirming the bound truncates the tail rather than reordering.
        for (i, committed) in records.iter().enumerate() {
            prop_assert_eq!(committed.offset, offset + i as u64);
        }
    }
}
