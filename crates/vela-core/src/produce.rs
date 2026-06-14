//! The produce path: append a record on a partition's leader and return the
//! committed offset.
//!
//! This module implements the domain-layer produce semantics described in the
//! design's *Produce Flow* and Requirement 4. Producing one record is the
//! composition of:
//!
//! 1. **Topic admission** — the topic must exist and be producible. A missing
//!    topic is rejected with [`CoreError::TopicNotFound`] (Requirement 4.5) and
//!    a topic mid-deletion with [`CoreError::TopicDeleting`] (Requirement 3.7),
//!    reusing [`ClusterMetadata::ensure_producible`]. Neither appends anything.
//! 2. **Partition resolution** — the target partition must exist in the topic;
//!    otherwise [`CoreError::PartitionNotFound`] (Requirement 4.5, 10.5). The
//!    `(topic, partition_key) -> partition` mapping itself is the
//!    [`PartitionRouter`](crate::router::PartitionRouter)'s job (task 11.1);
//!    this path takes the already-resolved [`PartitionIndex`].
//! 3. **Payload validation** — the combined key and value size must not exceed
//!    1 MiB; an oversized record is rejected with [`CoreError::RecordTooLarge`]
//!    and **no** log entry is appended (Requirement 4.8).
//! 4. **Leadership** — only the partition's leader may append. A non-leader
//!    replica is rejected with [`CoreError::NotLeader`] carrying the believed
//!    current leader for the client to redirect to, and appends nothing
//!    (Requirement 4.6, 11.2).
//! 5. **Append, replicate, commit** — the leader encodes the record into a
//!    `Record` [`EntryPayload`] and proposes it; the entry is appended at the
//!    next log index and replicated. Once it commits on a majority the
//!    partition's [`StateMachine`](crate::fleet::StateMachine) assigns it the
//!    next gap-free, 0-based [`Offset`], which is returned to the producer
//!    (Requirement 4.3, 4.4, 4.7).
//!
//! ## Commit timing and the commit timeout
//!
//! [`PartitionReplica::step`] drives consensus one input at a time. In a
//! single-node group the leader is its own majority, so a proposal commits
//! within the same step and the offset is available immediately. In a
//! multi-node group the entry commits only once a majority of followers
//! acknowledge it, which happens over later steps as the server's driver pumps
//! `AppendEntries` replies in. When this path proposes but the entry has not
//! committed by the time it observes the result, it returns
//! [`CoreError::CommitTimeout`] without advancing the committed offset and
//! without returning an offset (Requirement 4.9).
//!
//! The wall-clock 5-second deadline of [`COMMIT_TIMEOUT_MS`] is enforced by the
//! server driver (task 14.2), which stops pumping replies and surfaces
//! `CommitTimeout` once the deadline passes. At this core, in-memory level the
//! timeout is modelled structurally: an entry that is not committed when the
//! produce step is observed yields `CommitTimeout`. The uncommitted entry may
//! remain in the leader's log pending replication — that is ordinary Raft
//! behaviour and does not advance the committed offset.

use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role};

use crate::fleet::{PartitionReplica, RaftGroupFleet};
use crate::model::{ClusterMetadata, Offset, PartitionIndex, Record};
use crate::topic::CoreError;

/// The maximum combined key-and-value payload size of a single produced
/// record: 1,048,576 bytes (1 MiB) (Requirement 4.8). A record whose key length
/// plus value length exceeds this is rejected with
/// [`CoreError::RecordTooLarge`] and appends nothing.
pub const MAX_RECORD_BYTES: usize = 1_048_576;

/// The commit timeout for a produced record: 5,000 milliseconds
/// (Requirement 4.9).
///
/// If a record's log entry is not replicated to a majority within this window
/// the produce request fails with [`CoreError::CommitTimeout`] without
/// advancing the partition's committed offset. The wall-clock enforcement of
/// this deadline lives in the server driver; this crate exposes the constant
/// and models the not-committed outcome structurally (see the module docs).
pub const COMMIT_TIMEOUT_MS: u64 = 5_000;

/// The outcome of appending a record to a [`PartitionReplica`].
///
/// This is the leader-local result, independent of the believed-leader identity
/// and topic/partition admission checks that the [`produce`] entry point layers
/// on top. Keeping it separate lets a replica's produce behaviour be tested in
/// isolation while [`produce`] maps each outcome onto the appropriate
/// [`CoreError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProduceOutcome {
    /// The record committed and was assigned this gap-free, 0-based offset
    /// (Requirement 4.4, 4.7).
    Committed(Offset),
    /// This replica is not the partition leader, so it appended nothing; the
    /// caller redirects the producer to the current leader (Requirement 4.6).
    NotLeader,
    /// The entry was appended but had not committed when the result was
    /// observed; the committed offset is not advanced (Requirement 4.9).
    NotCommitted,
}

impl PartitionReplica {
    /// Append `value` as a record on this replica and report whether it
    /// committed, assigning the next gap-free 0-based offset on commit.
    ///
    /// Only a leader may append (Requirement 4.3): a non-leader returns
    /// [`ProduceOutcome::NotLeader`] and appends nothing. A leader encodes the
    /// value into a `Record` [`EntryPayload`] and proposes it; the entry is
    /// appended at the next log index and replicated. If it commits within this
    /// step (always so for a single-node group, where the leader is its own
    /// majority) the partition state machine has assigned it the next offset,
    /// returned as [`ProduceOutcome::Committed`]. Otherwise the entry is pending
    /// replication and [`ProduceOutcome::NotCommitted`] is returned without
    /// advancing the committed offset (Requirement 4.7, 4.9).
    ///
    /// The assigned offset is the partition's `next_offset` captured **before**
    /// the proposal: because record entries are appended and committed in
    /// order, the just-proposed record takes exactly that offset once it
    /// commits, keeping offsets unique, gap-free, and monotonic
    /// (Requirement 4.7).
    pub fn produce_value(&mut self, value: Vec<u8>, clock: &mut impl Clock) -> ProduceOutcome {
        if self.role() != Role::Leader {
            return ProduceOutcome::NotLeader;
        }

        // The offset this record will receive once it commits: its position
        // among record entries, captured before the append (Requirement 4.7).
        let expected = self.state_machine().next_offset();

        let payload = EntryPayload::new(PayloadKind::Record, value);
        self.step(RaftInput::Propose(payload), clock);

        // The state machine advances its record count only on commit. If it has
        // moved past `expected`, our record committed and holds that offset;
        // otherwise the entry is still pending a majority (Requirement 4.9).
        if self.state_machine().next_offset() > expected {
            ProduceOutcome::Committed(expected)
        } else {
            ProduceOutcome::NotCommitted
        }
    }
}

/// Produce `record` to `partition` of `topic`, returning the committed
/// [`Offset`].
///
/// This is the produce entry point composing topic admission, partition
/// resolution, payload validation, the leadership check, and the
/// append/replicate/commit cycle (Requirement 4.3–4.9). The partition is the
/// one a [`PartitionRouter`](crate::router::PartitionRouter) already resolved
/// from the record's partition key (Requirement 4.1, 4.2); `clock` drives the
/// replica's consensus step.
///
/// On success the record committed and the returned offset is its unique,
/// gap-free, 0-based position in the partition (Requirement 4.4, 4.7).
///
/// Errors, none of which return an offset and the first three of which append
/// nothing:
///
/// - [`CoreError::TopicNotFound`] — the topic does not exist (Requirement 4.5).
/// - [`CoreError::TopicDeleting`] — the topic is mid-deletion (Requirement 3.7).
/// - [`CoreError::PartitionNotFound`] — the topic has no such partition, or the
///   partition's Raft group is not hosted here (Requirement 4.5, 10.5).
/// - [`CoreError::RecordTooLarge`] — the combined key+value size exceeds 1 MiB
///   (Requirement 4.8).
/// - [`CoreError::NotLeader`] — this replica is not the partition leader; the
///   error carries the believed leader to redirect to (Requirement 4.6, 11.2).
/// - [`CoreError::CommitTimeout`] — the entry did not commit to a majority
///   within the commit timeout; the committed offset is not advanced
///   (Requirement 4.9).
pub fn produce(
    metadata: &ClusterMetadata,
    fleet: &mut RaftGroupFleet,
    topic: &str,
    partition: PartitionIndex,
    record: &Record,
    clock: &mut impl Clock,
) -> Result<Offset, CoreError> {
    // 1. The topic must exist and be producible: rejects missing (4.5) and
    //    mid-deletion (3.7) topics without appending anything.
    metadata.ensure_producible(topic)?;

    // 2. The partition must exist in the topic (Requirement 4.5, 10.5). Capture
    //    the believed leader now, in case the produce must redirect below.
    let leader_hint = metadata
        .topics
        .get(topic)
        .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
        .ok_or_else(|| CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition.0,
        })?
        .leader
        .clone();

    // 3. Validate the combined key+value payload size before any append
    //    (Requirement 4.8).
    let size = record.key.as_ref().map_or(0, |k| k.len()) + record.value.len();
    if size > MAX_RECORD_BYTES {
        return Err(CoreError::RecordTooLarge(size));
    }

    // 4. The partition's Raft group must be hosted here. A partition that
    //    exists in metadata but has no local replica is treated as not found on
    //    this node (Requirement 4.5, 10.5).
    let replica = fleet
        .get_mut(&(topic.to_string(), partition))
        .ok_or_else(|| CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition.0,
        })?;

    // 5. Append on the leader, replicate, and commit (Requirement 4.3, 4.4,
    //    4.7); a non-leader redirects (4.6) and a non-commit times out (4.9).
    match replica.produce_value(record.value.clone(), clock) {
        ProduceOutcome::Committed(offset) => Ok(offset),
        ProduceOutcome::NotLeader => Err(CoreError::NotLeader {
            leader: leader_hint,
        }),
        ProduceOutcome::NotCommitted => Err(CoreError::CommitTimeout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use vela_raft::{NodeId as RaftNodeId, RaftMessage, RequestVoteReply, TimerKind};

    use crate::model::{Member, NodeAvailability, NodeId, Partition, Topic, TopicState};

    /// A minimal [`Clock`] that never advances on its own; arming a timer is a
    /// no-op. Tests drive consensus with explicit [`RaftInput`]s, so no real
    /// timing is needed.
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

    /// Build metadata holding a single topic with `partition_count` partitions,
    /// each led by `leader`, over a cluster of one available member.
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

    /// A fleet hosting one single-node group for `topic`/partition 0, already
    /// driven to leader (its lone self-vote is a majority).
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

    #[test]
    fn single_node_leader_assigns_gap_free_zero_based_offsets() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // Each committed record receives the next contiguous 0-based offset.
        for expected in 0..5u64 {
            let record = Record::new(None, format!("v{expected}").into_bytes());
            let offset = produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock,
            )
            .unwrap();
            assert_eq!(offset, expected);
        }

        // The records are readable back in order at their assigned offsets.
        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), Some(4));
        let records = replica.read(0, 100);
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].value, b"v0".to_vec());
        assert_eq!(records[4].value, b"v4".to_vec());
    }

    #[test]
    fn oversized_payload_is_rejected_and_appends_nothing() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // A value one byte over the 1 MiB limit is rejected for size.
        let big = Record::new(None, vec![0u8; MAX_RECORD_BYTES + 1]);
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &big,
                &mut clock
            ),
            Err(CoreError::RecordTooLarge(MAX_RECORD_BYTES + 1))
        );

        // The combined key + value size is what counts: each is at the limit on
        // its own but together they exceed it.
        let combined = Record::new(Some(vec![1u8; MAX_RECORD_BYTES]), vec![2u8; 1]);
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &combined,
                &mut clock
            ),
            Err(CoreError::RecordTooLarge(MAX_RECORD_BYTES + 1))
        );

        // Nothing was appended (Requirement 4.8): the partition is still empty.
        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), None);

        // A record exactly at the limit is accepted (not rejected for size).
        let at_limit = Record::new(None, vec![3u8; MAX_RECORD_BYTES]);
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &at_limit,
                &mut clock
            ),
            Ok(0)
        );
    }

    #[test]
    fn produce_to_a_non_leader_is_redirected_and_appends_nothing() {
        let mut clock = TestClock::new();
        // The believed leader is node-1, but the locally hosted replica (node-0)
        // is a follower (never elected).
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-1")));
        let mut fleet = RaftGroupFleet::new();
        let key = ("orders".to_string(), PartitionIndex(0));
        fleet
            .create_group(
                key.clone(),
                RaftNodeId(0),
                vec![RaftNodeId(1), RaftNodeId(2)],
            )
            .unwrap();
        assert_eq!(fleet.get(&key).unwrap().role(), Role::Follower);

        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock
            ),
            Err(CoreError::NotLeader {
                leader: Some(NodeId::new("node-1")),
            })
        );

        // The non-leader appended nothing (Requirement 4.6).
        assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }

    #[test]
    fn produce_to_a_missing_topic_is_rejected() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let record = Record::new(None, b"v".to_vec());
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
    }

    #[test]
    fn produce_to_a_missing_partition_is_rejected() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let record = Record::new(None, b"v".to_vec());
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
    }

    #[test]
    fn produce_to_a_deleting_topic_is_rejected() {
        let mut clock = TestClock::new();
        let mut meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        meta.topics.get_mut("orders").unwrap().state = TopicState::Deleting;
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock
            ),
            Err(CoreError::TopicDeleting("orders".to_string()))
        );
    }

    #[test]
    fn uncommitted_record_in_a_multi_node_group_times_out() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));

        // A 3-node group whose leader cannot reach a commit majority because no
        // follower acknowledges.
        let mut fleet = RaftGroupFleet::new();
        let key = ("orders".to_string(), PartitionIndex(0));
        fleet
            .create_group(
                key.clone(),
                RaftNodeId(0),
                vec![RaftNodeId(1), RaftNodeId(2)],
            )
            .unwrap();

        // Drive node-0 to leader: an election self-vote plus one granted reply
        // is a majority of three.
        let replica = fleet.get_mut(&key).unwrap();
        replica.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        replica.step(
            RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                term: replica.raft().current_term(),
                vote_granted: true,
            })),
            &mut clock,
        );
        assert_eq!(replica.role(), Role::Leader);

        // The proposed record is appended but cannot commit without follower
        // acks, so produce times out without advancing the committed offset
        // (Requirement 4.9).
        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock
            ),
            Err(CoreError::CommitTimeout)
        );
        assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }
}
