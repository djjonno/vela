//! The consume path: read committed records from a partition starting at an
//! offset.
//!
//! This module implements the domain-layer consume semantics described in the
//! design's *Consume Flow* and Requirement 5. Consuming from a partition is the
//! composition of:
//!
//! 1. **Parameter validation** — the optional maximum count, when supplied,
//!    must lie in `1..=10000`; otherwise the request is rejected with
//!    [`CoreError::InvalidConsumeParams`] and no records are returned
//!    (Requirement 5.7). The requested offset is a [`u64`], so it is always
//!    `>= 0`; the "offset less than 0" half of Requirement 5.7 is therefore
//!    unrepresentable at this type boundary and cannot occur — it is documented
//!    here for traceability rather than checked at runtime.
//! 2. **Partition resolution** — the topic and partition must exist in the
//!    cluster metadata; a missing topic or partition is rejected with
//!    [`CoreError::PartitionNotFound`] and no records are returned
//!    (Requirement 5.4, 10.5).
//! 3. **Leadership** — consume is served by the partition leader (see the
//!    design's *Consume Flow*). A partition with no elected leader — its
//!    [`Partition::leader`](crate::model::Partition::leader) is `None` — is
//!    rejected with [`CoreError::PartitionUnavailable`] and no records are
//!    returned (Requirement 5.8).
//! 4. **Bounded, committed, ordered read** — the partition's
//!    [`StateMachine`](crate::fleet::StateMachine) holds only committed records,
//!    so a read returns only committed records (Requirement 5.2) in strictly
//!    ascending offset order beginning at the requested offset (Requirement
//!    5.1), bounded by the requested maximum or the default of
//!    [`DEFAULT_MAX_RECORDS`] (Requirement 5.5, 5.6). A requested offset beyond
//!    the highest committed offset yields an empty — but successful — result
//!    (Requirement 5.3).

use crate::fleet::{CommittedRecord, RaftGroupFleet};
use crate::model::{ClusterMetadata, PartitionIndex};
use crate::topic::CoreError;

/// The default maximum number of records returned when a consumer does not
/// specify a maximum count: 500 (Requirement 5.6).
pub const DEFAULT_MAX_RECORDS: u32 = 500;

/// The smallest valid explicit maximum count a consumer may request: 1
/// (Requirement 5.5, 5.7).
pub const MIN_MAX_RECORDS: u32 = 1;

/// The largest valid explicit maximum count a consumer may request: 10,000
/// (Requirement 5.5, 5.7).
pub const MAX_MAX_RECORDS: u32 = 10_000;

/// Consume committed records from `partition` of `topic`, starting at `offset`
/// and returning at most `max_count` (or [`DEFAULT_MAX_RECORDS`] when `None`).
///
/// This is the consume entry point composing parameter validation, partition
/// resolution, the leadership check, and the bounded committed read
/// (Requirement 5.1–5.8). The returned records are exactly the committed
/// records at offsets `offset, offset + 1, ...` in strictly ascending order, no
/// more than the effective maximum, and only those that have committed
/// (Requirement 5.1, 5.2, 5.5, 5.6).
///
/// `max_count` semantics (Requirement 5.5, 5.6, 5.7):
///
/// - `Some(n)` with `n` in `1..=10000` bounds the result to at most `n`
///   records.
/// - `None` applies the default bound of [`DEFAULT_MAX_RECORDS`] (500).
/// - `Some(n)` with `n == 0` or `n > 10000` is rejected with
///   [`CoreError::InvalidConsumeParams`].
///
/// The requested `offset` is a [`u64`] and so is always non-negative; the
/// "offset less than 0" rejection in Requirement 5.7 is unrepresentable at this
/// type boundary and therefore cannot occur (see the module docs).
///
/// Errors, none of which return any records:
///
/// - [`CoreError::InvalidConsumeParams`] — `max_count` is `Some(0)` or
///   `Some(n)` with `n > 10000` (Requirement 5.7).
/// - [`CoreError::PartitionNotFound`] — the topic has no such partition, or the
///   partition's Raft group is not hosted here (Requirement 5.4, 10.5).
/// - [`CoreError::PartitionUnavailable`] — the partition currently has no
///   elected leader (Requirement 5.8).
///
/// A successful read past the highest committed offset returns `Ok` with zero
/// records rather than an error (Requirement 5.3).
pub fn consume(
    metadata: &ClusterMetadata,
    fleet: &RaftGroupFleet,
    topic: &str,
    partition: PartitionIndex,
    offset: u64,
    max_count: Option<u32>,
) -> Result<Vec<CommittedRecord>, CoreError> {
    // 1. Validate parameters before resolving anything (Requirement 5.7). A
    //    supplied maximum must lie in 1..=10000; an absent maximum defaults to
    //    DEFAULT_MAX_RECORDS (Requirement 5.5, 5.6). `offset` is a u64 and so is
    //    always >= 0, leaving the "offset < 0" case unrepresentable.
    let max = match max_count {
        Some(n) if !(MIN_MAX_RECORDS..=MAX_MAX_RECORDS).contains(&n) => {
            return Err(CoreError::InvalidConsumeParams);
        }
        Some(n) => n,
        None => DEFAULT_MAX_RECORDS,
    };

    // 2. The partition must exist in the topic (Requirement 5.4, 10.5). Capture
    //    its leader to enforce the leadership requirement below.
    let leader = metadata
        .topics
        .get(topic)
        .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
        .ok_or_else(|| CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition.0,
        })?
        .leader
        .clone();

    // 3. The partition must have an elected leader; consume is served by the
    //    leader (Requirement 5.8). No known leader means the partition is
    //    currently unavailable for reads.
    if leader.is_none() {
        return Err(CoreError::PartitionUnavailable);
    }

    // 4. The partition's Raft group must be hosted here. A partition that
    //    exists in metadata but has no local replica is treated as not found on
    //    this node (Requirement 5.4, 10.5).
    let replica =
        fleet
            .get(&(topic.to_string(), partition))
            .ok_or_else(|| CoreError::PartitionNotFound {
                topic: topic.to_string(),
                index: partition.0,
            })?;

    // 5. Read only committed records in ascending offset order from `offset`,
    //    bounded by `max`. The state machine stores only committed records, so
    //    this returns committed records exclusively (Requirement 5.1, 5.2, 5.5,
    //    5.6); a start beyond the highest committed offset yields an empty
    //    result (Requirement 5.3).
    Ok(replica.read(offset, max as usize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, Role, TimerKind};

    use crate::model::{Member, NodeAvailability, NodeId, Partition, Record, Topic, TopicState};
    use crate::produce::produce;

    /// A minimal [`Clock`] that never advances on its own; arming a timer is a
    /// no-op. Tests drive consensus with explicit [`RaftInput`]s.
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

    /// Metadata with a single topic of `partition_count` partitions, each led by
    /// `leader`, over a one-member cluster.
    fn metadata_with_topic(
        name: &str,
        partition_count: u32,
        leader: Option<NodeId>,
    ) -> ClusterMetadata {
        let mut meta = ClusterMetadata::new();
        meta.members = vec![Member {
            id: NodeId::new("node-0"),
            addr: "node-0:7001".to_string(),
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
            },
        );
        meta
    }

    /// A fleet hosting one single-node leader group for `topic`/partition 0.
    fn fleet_with_leader(topic: &str, clock: &mut TestClock) -> RaftGroupFleet {
        let mut fleet = RaftGroupFleet::new();
        let key = (topic.to_string(), PartitionIndex(0));
        fleet
            .create_group(key.clone(), RaftNodeId(0), Vec::new())
            .unwrap();
        let replica = fleet.get_mut(&key).unwrap();
        replica.step(RaftInput::Tick(TimerKind::Election), clock);
        assert_eq!(replica.role(), Role::Leader);
        fleet
    }

    /// Produce `count` records `v0..v{count-1}` to `topic`/partition 0.
    fn produce_n(meta: &ClusterMetadata, fleet: &mut RaftGroupFleet, topic: &str, count: u64) {
        let mut clock = TestClock::new();
        for i in 0..count {
            let record = Record::new(None, format!("v{i}").into_bytes());
            produce(meta, fleet, topic, PartitionIndex(0), &record, &mut clock).unwrap();
        }
    }

    #[test]
    fn returns_committed_records_in_ascending_offset_order() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);
        produce_n(&meta, &mut fleet, "orders", 5);

        let records = consume(&meta, &fleet, "orders", PartitionIndex(0), 0, None).unwrap();

        // All five committed records, offsets strictly ascending from 0.
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(records[0].value, b"v0".to_vec());
        assert_eq!(records[4].value, b"v4".to_vec());

        // Reading from a mid-stream offset begins exactly there, ascending.
        let from_two = consume(&meta, &fleet, "orders", PartitionIndex(0), 2, None).unwrap();
        assert_eq!(
            from_two.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    #[test]
    fn respects_explicit_max_count_and_default() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);
        produce_n(&meta, &mut fleet, "orders", 10);

        // An explicit maximum bounds the result (Requirement 5.5).
        let bounded = consume(&meta, &fleet, "orders", PartitionIndex(0), 0, Some(3)).unwrap();
        assert_eq!(
            bounded.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        // The default bound (500) returns everything available here
        // (Requirement 5.6).
        let defaulted = consume(&meta, &fleet, "orders", PartitionIndex(0), 0, None).unwrap();
        assert_eq!(defaulted.len(), 10);
        assert_eq!(DEFAULT_MAX_RECORDS, 500);
    }

    #[test]
    fn rejects_invalid_max_count() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let fleet = fleet_with_leader("orders", &mut clock);

        // Zero and above-range maxima are invalid (Requirement 5.7).
        assert_eq!(
            consume(&meta, &fleet, "orders", PartitionIndex(0), 0, Some(0)),
            Err(CoreError::InvalidConsumeParams)
        );
        assert_eq!(
            consume(
                &meta,
                &fleet,
                "orders",
                PartitionIndex(0),
                0,
                Some(MAX_MAX_RECORDS + 1)
            ),
            Err(CoreError::InvalidConsumeParams)
        );

        // The inclusive boundaries 1 and 10000 are accepted (no param error).
        assert!(consume(
            &meta,
            &fleet,
            "orders",
            PartitionIndex(0),
            0,
            Some(MIN_MAX_RECORDS)
        )
        .is_ok());
        assert!(consume(
            &meta,
            &fleet,
            "orders",
            PartitionIndex(0),
            0,
            Some(MAX_MAX_RECORDS)
        )
        .is_ok());
    }

    #[test]
    fn offset_beyond_highest_committed_returns_empty() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);
        produce_n(&meta, &mut fleet, "orders", 3);

        // Highest committed offset is 2; reading at 3 (and beyond) is a
        // successful empty result, not an error (Requirement 5.3).
        let at_end = consume(&meta, &fleet, "orders", PartitionIndex(0), 3, None).unwrap();
        assert!(at_end.is_empty());
        let far_past = consume(&meta, &fleet, "orders", PartitionIndex(0), 999, None).unwrap();
        assert!(far_past.is_empty());

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
        .unwrap();
        assert!(none_yet.is_empty());
    }

    #[test]
    fn missing_topic_or_partition_is_not_found() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let fleet = fleet_with_leader("orders", &mut clock);

        // A topic that does not exist (Requirement 5.4).
        assert_eq!(
            consume(&meta, &fleet, "ghost", PartitionIndex(0), 0, None),
            Err(CoreError::PartitionNotFound {
                topic: "ghost".to_string(),
                index: 0,
            })
        );

        // A partition index outside the topic (Requirement 5.4, 10.5).
        assert_eq!(
            consume(&meta, &fleet, "orders", PartitionIndex(7), 0, None),
            Err(CoreError::PartitionNotFound {
                topic: "orders".to_string(),
                index: 7,
            })
        );
    }

    #[test]
    fn partition_with_no_elected_leader_is_unavailable() {
        let mut clock = TestClock::new();
        // The partition exists in metadata but has no elected leader.
        let meta = metadata_with_topic("orders", 1, None);
        let fleet = fleet_with_leader("orders", &mut clock);

        assert_eq!(
            consume(&meta, &fleet, "orders", PartitionIndex(0), 0, None),
            Err(CoreError::PartitionUnavailable)
        );
    }
}
