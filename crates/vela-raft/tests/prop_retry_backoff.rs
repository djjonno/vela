//! Property test for replication retry backoff in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 30
//!
//! Property 30: Replication retries back up and use capped exponential backoff.
//! For any leader and any number `k` of consecutive unacknowledged/rejected
//! `AppendEntries` retries to a follower, the per-peer retry backoff grows
//! exponentially from the 1 s base, doubling on each rejection but never
//! exceeding the 5 s cap — exactly `min(5000ms, 1000ms * 2^k)` — and a
//! subsequent successful acknowledgment resets it to the 1 s base. On each
//! rejection the leader backs up that peer's `next_index`, which never
//! increases.
//!
//! The test drives a leader directly through its synchronous `step` function
//! against a simulated [`Clock`], feeding it a controlled sequence of `k`
//! rejected replies so the exact retry count — and therefore the exact expected
//! backoff — is deterministic, then a success to confirm the reset.
//!
//! Validates: Requirements 8.4

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogStorage, PayloadKind};
use vela_raft::{
    AppendEntriesReply, Clock, NodeId, RaftInput, RaftMessage, RaftNode, RequestVoteReply,
    TimerKind, REPLICATION_BACKOFF_BASE, REPLICATION_BACKOFF_MAX,
};

/// A trivial [`Clock`] for direct `step` tests: logical time never advances on
/// its own and armed timers are discarded. Backoff growth is driven purely by
/// the sequence of replies fed to the leader, not by wall-clock time, so a
/// no-op clock is sufficient and keeps the run deterministic.
#[derive(Default)]
struct TestClock;

impl Clock for TestClock {
    fn now(&self) -> Instant {
        // A fixed reference instant is enough; this test never reads it.
        Instant::now()
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// The leader's two peers in the 3-node group under test. A 3-node group has a
/// majority of 2, so a single granted vote (plus the candidate's self-vote)
/// elects the leader.
const PEER: NodeId = NodeId(1);
const VOTER: NodeId = NodeId(2);

/// Build a 3-node group leader whose own log already holds `log_len` entries,
/// so its per-peer `next_index` starts at `log_len` and backing it up on
/// rejection is observable.
///
/// The node is driven to leadership through the real state machine: an election
/// timeout promotes it to candidate (term 1, self-vote), and one granted
/// `RequestVoteReply` completes the majority. On becoming leader the per-peer
/// replication backoff is initialised to [`REPLICATION_BACKOFF_BASE`].
fn make_leader(log_len: u64) -> RaftNode<InMemoryLog> {
    let mut log = InMemoryLog::new();
    // Pre-populate the log at term 0 (strictly below the leadership term) so the
    // entries never auto-commit and only influence `next_index`.
    for _ in 0..log_len {
        log.append(EntryPayload::new(PayloadKind::Record, Vec::new()), 0)
            .expect("append to in-memory log");
    }

    let mut node = RaftNode::new(NodeId(0), vec![PEER, VOTER], log);
    let mut clock = TestClock;

    // Election timeout -> candidate, term 1, self-vote.
    node.step(RaftInput::Tick(TimerKind::Election), &mut clock);
    // One granted vote reaches the majority of 2 -> leader.
    let term = node.current_term();
    node.step(
        RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
            term,
            vote_granted: true,
            voter: VOTER,
        })),
        &mut clock,
    );
    assert_eq!(
        node.role(),
        vela_raft::Role::Leader,
        "node should have won the uncontested election"
    );
    node
}

/// The backoff expected after `rejections` consecutive rejected retries, stated
/// directly as the property's closed form `min(5000ms, 1000ms * 2^k)` rather
/// than by mirroring the implementation's doubling loop.
fn expected_backoff(rejections: u32) -> Duration {
    // 1000 * 2^k, saturating well before any overflow for the tested range.
    let doubled_ms = 1000u64.saturating_mul(1u64 << rejections.min(20));
    Duration::from_millis(doubled_ms.min(5000))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 30
    #[test]
    fn replication_retries_use_capped_exponential_backoff(
        // Length of the leader's initial log, so `next_index` backup is visible.
        log_len in 0u64..6,
        // Number of consecutive rejected retries; spans below, at, and beyond
        // the point where the doubling sequence saturates at the 5 s cap.
        k in 0u32..10,
        // Per-rejection conflict-hint shape: 0 = none (back up by one),
        // 1 = hint at the current next_index (stay), 2 = hint one earlier.
        // All are realistic hints that never exceed `next_index`.
        hint_shapes in proptest::collection::vec(0u8..3, 10),
    ) {
        let mut node = make_leader(log_len);
        let mut clock = TestClock;

        // On election the backoff starts at the 1 s base (k = 0 case).
        prop_assert_eq!(
            node.replication_backoff(PEER),
            Some(REPLICATION_BACKOFF_BASE),
            "fresh leader backoff should be the base"
        );
        prop_assert_eq!(REPLICATION_BACKOFF_BASE, expected_backoff(0));

        // Feed `k` consecutive rejections and check the backoff after each.
        for i in 0..k {
            let before_next = node.next_index(PEER);
            // A well-formed follower hint is never beyond the leader's cursor.
            let conflict_index = match hint_shapes[i as usize % hint_shapes.len()] {
                0 => None,
                1 => before_next,
                _ => before_next.map(|n| n.saturating_sub(1)),
            };
            let term = node.current_term();
            node.step(
                RaftInput::Message(RaftMessage::AppendEntriesReply(AppendEntriesReply {
                    from: PEER,
                    term,
                    success: false,
                    conflict_index,
                    match_index: None,
                })),
                &mut clock,
            );

            // The backoff equals min(5000ms, 1000ms * 2^(i+1)).
            let expected = expected_backoff(i + 1);
            prop_assert_eq!(
                node.replication_backoff(PEER),
                Some(expected),
                "after {} rejection(s) backoff should be {:?}",
                i + 1,
                expected
            );
            // Never exceed the cap.
            prop_assert!(
                node.replication_backoff(PEER).unwrap() <= REPLICATION_BACKOFF_MAX,
                "backoff must stay within the cap"
            );

            // The peer's next_index backs up (or stays) on rejection — never up.
            let after_next = node.next_index(PEER);
            if let (Some(before), Some(after)) = (before_next, after_next) {
                prop_assert!(
                    after <= before,
                    "next_index increased on rejection: {} -> {}",
                    before,
                    after
                );
            }
        }

        // A subsequent successful acknowledgment resets the backoff to the base.
        let term = node.current_term();
        let matched = node.next_index(PEER).map(|n| n.saturating_sub(1));
        node.step(
            RaftInput::Message(RaftMessage::AppendEntriesReply(AppendEntriesReply {
                from: PEER,
                term,
                success: true,
                conflict_index: None,
                match_index: matched,
            })),
            &mut clock,
        );
        prop_assert_eq!(
            node.replication_backoff(PEER),
            Some(REPLICATION_BACKOFF_BASE),
            "a success must reset the backoff to the base"
        );
    }
}
