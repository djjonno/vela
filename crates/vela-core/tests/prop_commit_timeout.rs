//! Property test for the produce commit timeout in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 17
//!
//! Property 17: A record not replicated to a majority within the commit timeout
//! is not committed. *For any* record whose log entry fails to reach a majority
//! of the partition's Raft group before the commit timeout, [`produce`] returns
//! a not-committed error ([`CoreError::CommitTimeout`]), does not advance the
//! partition's committed offset (its `high_water_mark` stays `None`), and
//! returns no offset.
//!
//! This is the node-local realization of Requirement 4.9 — "IF the Leader cannot
//! replicate a Record's Log_Entry to a majority of the Partition's Raft_Group
//! within a commit timeout of 5,000 milliseconds, THEN THE Vela SHALL return an
//! error indicating the Record was not committed, SHALL not advance the
//! Partition's committed Offset, and SHALL not return an Offset to the
//! Producer."
//!
//! ## How a "cannot reach a majority" leader is built
//!
//! Each iteration creates a multi-node Raft group (3–7 nodes) and drives node-0
//! to leader by delivering an election self-vote plus exactly the granted
//! [`RequestVoteReply`]s needed for a majority. No `AppendEntries` reply is ever
//! delivered, so no follower's `match_index` advances and a proposed record can
//! never reach a commit majority. With the deterministic [`TestClock`] never
//! advancing on its own, the entry is observed uncommitted, which the produce
//! path surfaces as [`CoreError::CommitTimeout`] (see `produce.rs` module docs
//! on how the 5 s wall-clock deadline is modelled structurally at this layer).
//!
//! The property is exercised over varied record payloads (optional key, varied
//! value bytes, all well under the 1 MiB limit so size is never the cause) and
//! over repeated produce attempts on the same stuck leader, asserting every
//! attempt times out and the committed offset never advances.
//!
//! Validates: Requirements 4.9

use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_core::{
    produce, ClusterMetadata, CoreError, Member, NodeAvailability, NodeId, Partition,
    PartitionIndex, RaftGroupFleet, Record, Topic, TopicState,
};
use vela_raft::{
    Clock, NodeId as RaftNodeId, RaftInput, RaftMessage, RequestVoteReply, Role, TimerKind,
};

/// A deterministic [`Clock`] that never advances on its own; arming a timer is a
/// no-op. Consensus is driven entirely by explicit [`RaftInput`]s, so the entry
/// is observed exactly as the test leaves it — uncommitted, because no follower
/// ack is ever delivered.
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

/// Build metadata holding `topic` with a single partition led by node-0 over a
/// cluster of one available member. The believed leader matters only so that a
/// timed-out produce is attributed to commit failure rather than a redirect.
fn metadata_with_topic(topic: &str) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = vec![Member {
        id: NodeId::new("node-0"),
        addr: "node-0:7001".to_string(),
        availability: NodeAvailability::Available,
    }];
    meta.topics.insert(
        topic.to_string(),
        Topic {
            name: topic.to_string(),
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

/// Create a `total_nodes`-node group for `topic`/partition 0 and drive its local
/// replica (node-0) to leader, delivering only the votes needed for a majority
/// and never any `AppendEntries` reply. The returned leader therefore cannot
/// commit any proposal, since no follower's match index will ever advance.
fn fleet_with_stuck_leader(topic: &str, total_nodes: u64, clock: &mut TestClock) -> RaftGroupFleet {
    let mut fleet = RaftGroupFleet::new();
    let key = (topic.to_string(), PartitionIndex(0));
    let peers: Vec<RaftNodeId> = (1..total_nodes).map(RaftNodeId).collect();
    fleet
        .create_group(key.clone(), RaftNodeId(0), peers)
        .expect("creating the partition's group must succeed");

    let replica = fleet.get_mut(&key).unwrap();

    // Start an election: this casts node-0's self-vote (one vote of `total`).
    replica.step(RaftInput::Tick(TimerKind::Election), clock);

    // A strict majority of `total_nodes`; the self-vote already counts as one,
    // so deliver `majority - 1` granted replies to cross the threshold.
    let majority = total_nodes / 2 + 1;
    let term = replica.raft().current_term();
    for _ in 0..(majority - 1) {
        replica.step(
            RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                term,
                vote_granted: true,
            })),
            clock,
        );
    }

    assert_eq!(
        replica.role(),
        Role::Leader,
        "node-0 must reach leadership with a majority of votes"
    );
    // Nothing has been produced yet, and an empty leader log has no committed
    // record.
    assert_eq!(replica.high_water_mark(), None);
    fleet
}

/// Generate a record whose payload is comfortably under the 1 MiB limit, so a
/// timeout is never a disguised size rejection. The key is optional and the
/// value length varies, covering empty through a few hundred bytes.
fn record_strategy() -> impl Strategy<Value = Record> {
    let key = prop::option::of(prop::collection::vec(any::<u8>(), 0..=80));
    let value = prop::collection::vec(any::<u8>(), 0..=300);
    (key, value).prop_map(|(key, value)| Record::new(key, value))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 17
    #[test]
    fn record_not_replicated_to_majority_times_out_and_does_not_commit(
        // An odd-or-even multi-node group of 3..=7 nodes: each needs at least one
        // follower ack to commit, which never arrives.
        total_nodes in 3u64..=7,
        records in prop::collection::vec(record_strategy(), 1..=5),
    ) {
        let topic = "orders";
        let mut clock = TestClock::new();
        let meta = metadata_with_topic(topic);
        let mut fleet = fleet_with_stuck_leader(topic, total_nodes, &mut clock);
        let key = (topic.to_string(), PartitionIndex(0));

        // Every produce attempt against the stuck leader must time out: the
        // entry cannot reach a commit majority. No call returns an offset, and
        // the partition's committed offset never advances past `None`
        // (Requirement 4.9).
        for record in &records {
            let result = produce(
                &meta,
                &mut fleet,
                topic,
                PartitionIndex(0),
                record,
                &mut clock,
            );

            // Returns a not-committed error and no offset.
            prop_assert_eq!(result, Err(CoreError::CommitTimeout));

            // The committed offset is not advanced after this attempt.
            prop_assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
        }

        // After all attempts the partition still has no committed record: the
        // commit index was never advanced by an uncommittable entry.
        prop_assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }
}
