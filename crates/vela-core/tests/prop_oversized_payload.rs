//! Property test for oversized-payload rejection in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 18
//!
//! Property 18: Oversized payloads are rejected. For any record whose combined
//! key-and-value size exceeds [`MAX_RECORD_BYTES`] (1,048,576 bytes / 1 MiB),
//! [`produce`] rejects it with [`CoreError::RecordTooLarge`] carrying the exact
//! combined size and appends nothing — the partition's high-water mark stays
//! `None`. For any record at or under the limit, produce does *not* reject it
//! for size: on a single-node leader group (where the leader is its own
//! majority) the record commits and is assigned offset 0 (Requirement 4.8).
//!
//! The boundary at exactly 1 MiB is the interesting edge, so sizes are drawn to
//! straddle it: the combined size is `MAX_RECORD_BYTES + delta` for a small
//! signed `delta`, giving cases just under, exactly at, and just over the limit.
//! The combined size is split between the key and the value by a fraction so
//! both the key-only, value-only, and mixed contributions to the total are
//! exercised; a zero-length key is modelled as an absent (`None`) key, matching
//! how [`produce`] sums sizes (`key.map_or(0, len) + value.len()`).
//!
//! Payloads are kept within a few KiB of the 1 MiB limit so each of the many
//! iterations stays cheap while still crossing the boundary.
//!
//! Validates: Requirements 4.8

use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_core::{
    produce, ClusterMetadata, CoreError, Member, NodeAvailability, NodeId, Partition,
    PartitionIndex, RaftGroupFleet, Record, Topic, TopicState, MAX_RECORD_BYTES,
};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, Role, TimerKind};

/// A minimal [`Clock`] that never advances on its own; arming a timer is a
/// no-op. The test drives consensus with explicit [`RaftInput`]s, so no real
/// timing is needed (mirrors the produce-path unit tests).
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

/// Build metadata holding a single one-partition topic led by `node-0` over a
/// cluster of one available member.
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

/// A fleet hosting one single-node group for `topic`/partition 0, already driven
/// to leader (its lone self-vote is a majority).
fn fleet_with_leader(topic: &str, clock: &mut TestClock) -> RaftGroupFleet {
    let mut fleet = RaftGroupFleet::new();
    let key = (topic.to_string(), PartitionIndex(0));
    fleet
        .create_group(key.clone(), RaftNodeId(0), Vec::new())
        .expect("creating the single-node group must succeed");
    let replica = fleet.get_mut(&key).expect("the group was just created");
    replica.step(RaftInput::Tick(TimerKind::Election), clock);
    assert_eq!(replica.role(), Role::Leader);
    fleet
}

/// The largest amount, in bytes, by which the combined size is allowed to stray
/// from the 1 MiB limit in either direction. Keeps each iteration's allocations
/// just around 1 MiB while straddling the boundary.
const STRADDLE: i64 = 8192;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 18
    #[test]
    fn oversized_payloads_are_rejected(
        delta in -STRADDLE..=STRADDLE,
        key_frac in 0u32..=1000,
    ) {
        // Combined key+value size, straddling the 1 MiB limit. `delta >= -8192`
        // keeps this comfortably positive.
        let total = (MAX_RECORD_BYTES as i64 + delta) as usize;

        // Split the total between key and value. A zero-length key is expressed
        // as an absent key so it contributes 0 to the size, exactly as produce
        // computes it.
        let key_len = (total as u64 * key_frac as u64 / 1000) as usize;
        let value_len = total - key_len;
        let key = if key_len == 0 {
            None
        } else {
            Some(vec![1u8; key_len])
        };
        let record = Record::new(key, vec![2u8; value_len]);

        // A fresh single-node leader group per iteration, so the partition
        // starts empty (high-water mark `None`) and any append is observable.
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders");
        let mut fleet = fleet_with_leader("orders", &mut clock);
        let group_key = ("orders".to_string(), PartitionIndex(0));

        let result = produce(
            &meta,
            &mut fleet,
            "orders",
            PartitionIndex(0),
            &record,
            &mut clock,
        );

        let hwm = fleet
            .get(&group_key)
            .expect("the hosted group is present")
            .high_water_mark();

        if total > MAX_RECORD_BYTES {
            // Oversized: rejected with the exact combined size, nothing appended.
            prop_assert_eq!(
                result,
                Err(CoreError::RecordTooLarge(total)),
                "combined size {} (> {}) must be rejected for size",
                total,
                MAX_RECORD_BYTES
            );
            prop_assert_eq!(
                hwm,
                None,
                "an oversized record must append no Log_Entry (Requirement 4.8)"
            );
        } else {
            // At or under the limit: not rejected for size. On a single-node
            // leader the record commits and takes offset 0.
            prop_assert_ne!(
                result.clone(),
                Err(CoreError::RecordTooLarge(total)),
                "combined size {} (<= {}) must not be rejected for size",
                total,
                MAX_RECORD_BYTES
            );
            prop_assert_eq!(
                result,
                Ok(0),
                "a record at/under the limit must commit on a single-node leader"
            );
            prop_assert_eq!(
                hwm,
                Some(0),
                "the committed record advances the high-water mark to 0"
            );
        }
    }
}
