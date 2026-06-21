//! Property test for invalid consume parameters in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 21
//!
//! Property 21: Invalid consume parameters are rejected. A consume request
//! whose `max_count` is `Some(0)` or `Some(n)` with `n > 10000` is rejected
//! with [`CoreError::InvalidConsumeParams`] and returns no records. The two
//! inclusive boundaries — `Some(1)` and `Some(10000)` — are *not* rejected as
//! invalid parameters.
//!
//! This is the domain-layer realization of Requirement 5.7: a maximum count
//! outside the valid `1..=10000` range is rejected before any partition read,
//! and no records are returned. The requested `offset` is a [`u64`], so the
//! "offset less than 0" half of Requirement 5.7 is unrepresentable at this type
//! boundary and cannot occur at runtime — it is noted here for traceability
//! rather than exercised.
//!
//! The test runs over a single-node Raft group driven to leader, hosting a
//! healthy, committed partition. Using a fully available partition ensures a
//! rejection can only be attributed to parameter validation: were validation
//! skipped, the valid boundaries would still succeed and the invalid values
//! would read records rather than error.
//!
//! Validates: Requirements 5.7

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_core::{
    consume, produce, ClusterMetadata, CoreError, Member, NodeAvailability, NodeId, Partition,
    PartitionIndex, RaftGroupFleet, Record, Topic, TopicState, MAX_MAX_RECORDS, MIN_MAX_RECORDS,
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
        advertised_addr: "node-0:7001".to_string(),
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

    // Feature: vela-streaming-platform, Property 21
    #[test]
    fn invalid_max_count_is_rejected_and_returns_no_records(
        // An invalid `max_count`: either zero, or strictly above the maximum.
        // `prop_oneof` mixes the two invalid regions Requirement 5.7 names so
        // both the `Some(0)` and the `Some(n > 10000)` cases are covered.
        invalid in prop_oneof![
            Just(0u32),
            (MAX_MAX_RECORDS + 1)..=u32::MAX,
        ],
        // A consume offset anywhere in range; validation precedes any read, so
        // the offset must not affect the rejection.
        offset in 0u64..1_000_000,
    ) {
        let topic = "orders";
        let mut clock = TestClock::new();
        let meta = metadata_with_topic(topic);
        let mut fleet = fleet_with_leader(topic, &mut clock);

        // Commit a few records so the partition genuinely holds data. A
        // rejection therefore reflects parameter validation, not an empty log.
        for i in 0..8u64 {
            let record = Record::new(None, format!("v{i}").into_bytes());
            produce(&meta, &mut fleet, topic, PartitionIndex(0), &record, &mut clock)
                .expect("a record produced to a single-node leader commits");
        }

        // 5.7: an out-of-range maximum is rejected with the invalid-parameters
        // error, and the call yields no records (the Err carries none).
        let result = consume(&meta, &fleet, topic, PartitionIndex(0), offset, Some(invalid));
        prop_assert_eq!(result, Err(CoreError::InvalidConsumeParams));
    }

    // Feature: vela-streaming-platform, Property 21
    #[test]
    fn inclusive_boundaries_are_not_rejected_as_invalid_params(
        // Exactly the two inclusive boundaries of the valid range: 1 and 10000.
        valid in prop_oneof![Just(MIN_MAX_RECORDS), Just(MAX_MAX_RECORDS)],
    ) {
        let topic = "orders";
        let mut clock = TestClock::new();
        let meta = metadata_with_topic(topic);
        let mut fleet = fleet_with_leader(topic, &mut clock);

        for i in 0..8u64 {
            let record = Record::new(None, format!("v{i}").into_bytes());
            produce(&meta, &mut fleet, topic, PartitionIndex(0), &record, &mut clock)
                .expect("a record produced to a single-node leader commits");
        }

        // 5.7: the valid boundaries 1 and 10000 are accepted — the request is
        // not rejected as invalid parameters (it succeeds on this healthy,
        // committed partition).
        let result = consume(&meta, &fleet, topic, PartitionIndex(0), 0, Some(valid));
        prop_assert!(result.is_ok());
        prop_assert_ne!(result, Err(CoreError::InvalidConsumeParams));
    }
}
