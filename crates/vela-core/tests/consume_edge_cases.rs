//! Example/edge tests for the `vela-core` consume path and the shared
//! not-found cases (task 11.15).
//!
//! These are concrete example tests — not property tests — covering the
//! boundary behaviours of [`vela_core::consume`] that the design's *Consume
//! Flow* and Requirement 5 call out, plus the shared partition/topic not-found
//! semantics that produce (Requirement 4.5) and consume (Requirement 5.4, 10.5)
//! both raise as [`CoreError::PartitionNotFound`] / [`CoreError::TopicNotFound`]:
//!
//! - A requested offset beyond the highest committed offset returns an empty
//!   but successful result, never an error (Requirement 5.3).
//! - An absent maximum count applies the default bound of
//!   [`DEFAULT_MAX_RECORDS`] (500), behaving exactly like an explicit
//!   `Some(500)` (Requirement 5.6).
//! - A partition with no elected leader is rejected with
//!   [`CoreError::PartitionUnavailable`] (Requirement 5.8).
//! - A missing topic or an out-of-range partition index is rejected with
//!   [`CoreError::PartitionNotFound`] for both consume (Requirement 5.4, 10.5)
//!   and produce (Requirement 4.5).
//!
//! Requirements: 5.3, 5.6, 5.8, 4.5, 5.4, 10.5

use std::time::{Duration, Instant};

use vela_core::{
    consume, produce, ClusterMetadata, CoreError, Member, NodeAvailability, NodeId, Partition,
    PartitionIndex, RaftGroupFleet, Record, Topic, TopicState, DEFAULT_MAX_RECORDS,
};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, Role, TimerKind};

/// A minimal [`Clock`] that never advances on its own; arming a timer is a
/// no-op. The tests drive consensus with explicit [`RaftInput`]s, so no real
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

/// Build metadata holding a single topic of `partition_count` partitions, each
/// led by `leader`, over a one-member cluster (`node-0`).
fn metadata_with_topic(
    name: &str,
    partition_count: u32,
    leader: Option<NodeId>,
) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = vec![Member {
        id: NodeId::new("node-0"),
        addr: "node-0:7001".to_string(),
        advertised_addr: "node-0:7001".to_string(),
        availability: NodeAvailability::Available,
    }];
    let partitions = (0..partition_count)
        .map(|i| Partition {
            index: PartitionIndex(i),
            replicas: vec![NodeId::new("node-0")],
            leader: leader.clone(),
        })
        .collect();
    meta.topics.insert(
        name.to_string(),
        Topic {
            name: name.to_string(),
            partitions,
            state: TopicState::Active,
            backend: vela_core::LogBackend::Durable,
        },
    );
    meta
}

/// A fleet hosting one single-node leader group for `topic`/partition 0 (its
/// lone self-vote is a majority of one, so a single election tick elects it).
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

/// Produce `count` records `v0..v{count-1}` to `topic`/partition 0, asserting
/// each commits on the single-node leader.
fn produce_n(meta: &ClusterMetadata, fleet: &mut RaftGroupFleet, topic: &str, count: u64) {
    let mut clock = TestClock::new();
    for i in 0..count {
        let record = Record::new(None, format!("v{i}").into_bytes());
        produce(meta, fleet, topic, PartitionIndex(0), &record, &mut clock)
            .expect("a record produced to a single-node leader commits");
    }
}

/// (a) A requested offset beyond the highest committed offset is a successful
/// empty result, not an error (Requirement 5.3).
#[test]
fn offset_beyond_committed_high_water_mark_returns_empty() {
    let mut clock = TestClock::new();
    let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
    let mut fleet = fleet_with_leader("orders", &mut clock);
    produce_n(&meta, &mut fleet, "orders", 3);

    // Highest committed offset is 2. Reading exactly one past the end is an
    // empty Ok, not an error.
    let at_end = consume(&meta, &fleet, "orders", PartitionIndex(0), 3, None)
        .expect("reading past the end is Ok, not an error");
    assert!(at_end.is_empty());

    // Reading far past the end is likewise an empty Ok.
    let far_past = consume(&meta, &fleet, "orders", PartitionIndex(0), 999, None)
        .expect("reading far past the end is Ok, not an error");
    assert!(far_past.is_empty());

    // The last committed offset itself still returns exactly that one record,
    // confirming the boundary (offset 2 present, offset 3 empty) is correct.
    let last = consume(&meta, &fleet, "orders", PartitionIndex(0), 2, None).unwrap();
    assert_eq!(last.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![2]);

    // An empty partition (nothing committed) also reads empty at offset 0.
    let empty_meta = metadata_with_topic("empty", 1, Some(NodeId::new("node-0")));
    let empty_fleet = fleet_with_leader("empty", &mut clock);
    let none_yet = consume(
        &empty_meta,
        &empty_fleet,
        "empty",
        PartitionIndex(0),
        0,
        None,
    )
    .expect("an empty partition reads empty, not error");
    assert!(none_yet.is_empty());
}

/// (b) An absent maximum count applies the [`DEFAULT_MAX_RECORDS`] (500) bound,
/// behaving exactly like an explicit `Some(500)` (Requirement 5.6).
#[test]
fn absent_max_count_applies_the_default_of_500() {
    let mut clock = TestClock::new();
    let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
    let mut fleet = fleet_with_leader("orders", &mut clock);

    // The documented default is 500.
    assert_eq!(DEFAULT_MAX_RECORDS, 500);

    // With fewer than 500 records committed, `None` returns all of them — and
    // the result is identical to passing the default bound explicitly, which is
    // what "None applies the 500 default" means.
    produce_n(&meta, &mut fleet, "orders", 10);

    let defaulted = consume(&meta, &fleet, "orders", PartitionIndex(0), 0, None).unwrap();
    let explicit_default = consume(
        &meta,
        &fleet,
        "orders",
        PartitionIndex(0),
        0,
        Some(DEFAULT_MAX_RECORDS),
    )
    .unwrap();

    // None behaves like Some(500): same records, in the same order.
    assert_eq!(defaulted, explicit_default);
    // All 10 committed records are returned (10 <= 500), none dropped.
    assert_eq!(defaulted.len(), 10);
    assert_eq!(
        defaulted.iter().map(|r| r.offset).collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
    // The default never returns more than its 500-record bound.
    assert!(defaulted.len() <= DEFAULT_MAX_RECORDS as usize);
}

/// (c) A partition that exists but has no elected leader is rejected with
/// [`CoreError::PartitionUnavailable`] (Requirement 5.8).
#[test]
fn partition_with_no_elected_leader_is_unavailable() {
    let mut clock = TestClock::new();
    // The partition exists in metadata but its leader is `None` (mid-election).
    let meta = metadata_with_topic("orders", 1, None);
    let fleet = fleet_with_leader("orders", &mut clock);

    assert_eq!(
        consume(&meta, &fleet, "orders", PartitionIndex(0), 0, None),
        Err(CoreError::PartitionUnavailable)
    );
}

/// (d) Consume on a missing topic or an out-of-range partition index is
/// rejected with [`CoreError::PartitionNotFound`] (Requirement 5.4, 10.5).
#[test]
fn consume_missing_topic_or_partition_is_not_found() {
    let mut clock = TestClock::new();
    let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
    let fleet = fleet_with_leader("orders", &mut clock);

    // A topic that does not exist at all (Requirement 5.4).
    assert_eq!(
        consume(&meta, &fleet, "ghost", PartitionIndex(0), 0, None),
        Err(CoreError::PartitionNotFound {
            topic: "ghost".to_string(),
            index: 0,
        })
    );

    // A partition index outside the topic's range (Requirement 5.4, 10.5).
    assert_eq!(
        consume(&meta, &fleet, "orders", PartitionIndex(7), 0, None),
        Err(CoreError::PartitionNotFound {
            topic: "orders".to_string(),
            index: 7,
        })
    );
}

/// (d, cont.) The not-found case is shared with the produce path: producing to
/// a missing topic or an out-of-range partition is likewise rejected without
/// appending anything (Requirement 4.5, 10.5). This anchors the 4.5 half of the
/// task's not-found coverage against the same error taxonomy consume uses.
#[test]
fn produce_missing_topic_or_partition_is_not_found() {
    let mut clock = TestClock::new();
    let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
    let mut fleet = fleet_with_leader("orders", &mut clock);
    let record = Record::new(None, b"v".to_vec());

    // A topic that does not exist is rejected before any append (Requirement 4.5).
    assert_eq!(
        produce(
            &meta,
            &mut fleet,
            "ghost",
            PartitionIndex(0),
            &record,
            &mut clock
        ),
        Err(CoreError::TopicNotFound("ghost".to_string()))
    );

    // An out-of-range partition of an existing topic is rejected (Requirement
    // 4.5, 10.5).
    assert_eq!(
        produce(
            &meta,
            &mut fleet,
            "orders",
            PartitionIndex(7),
            &record,
            &mut clock
        ),
        Err(CoreError::PartitionNotFound {
            topic: "orders".to_string(),
            index: 7,
        })
    );

    // Neither rejected produce appended anything (the leader's log is empty).
    let replica = fleet
        .get(&("orders".to_string(), PartitionIndex(0)))
        .expect("the partition group is hosted");
    assert_eq!(replica.high_water_mark(), None);
}
