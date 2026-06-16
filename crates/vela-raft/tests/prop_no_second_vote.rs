// Feature: per-topic-log-durability, Property 11
//
//! Property 11: a restored replica casts no second vote in its persisted term.
//!
//! Validates: Requirements 10.4
//!
//! A durable replica persists its hard state (`current_term`, `voted_for`)
//! through the [`LogStorage`] seam, so a replica that had already granted a
//! vote to candidate `C` in term `T` recovers with `voted_for = Some(C)` at
//! term `T`. Raft's at-most-one-vote-per-term safety then requires that the
//! restored replica refuse a vote to any *different* candidate `C' != C` in
//! that same term `T`. This property seeds a durable [`Disk`] with a persisted
//! `HardState { current_term: T, voted_for: Some(C) }`, recovers a [`RaftNode`]
//! over it via [`RaftNode::recover`], delivers a `RequestVote` from a different
//! candidate `C'` at term `T`, and asserts the reply does not grant the vote
//! and the recorded `voted_for` still equals `Some(C)`. As a companion check it
//! confirms a repeat `RequestVote` from the *same* candidate `C` at term `T` is
//! still granted (idempotent), so the rejection is specific to the conflicting
//! candidate rather than a blanket refusal.

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_log::{
    CommitIndex, EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage, Snapshot,
};
use vela_raft::{
    Clock, NodeId, RaftInput, RaftMessage, RaftNode, RequestVote, RequestVoteReply, TimerKind,
};

/// A trivial [`Clock`] for direct `step` tests: logical time never advances on
/// its own and armed timers are simply discarded. The vote-decision logic under
/// test never reads the clock's instant, so a fixed reference time keeps the
/// test deterministic.
#[derive(Default)]
struct TestClock;

impl Clock for TestClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// The persistent "disk" the recovered replica reads its hard state from: the
/// term and vote a durable log would have retained across a restart. Cloning
/// shares the same backing cell, so a `TestLog` opened over a clone observes
/// exactly what was seeded here.
#[derive(Clone, Default)]
struct Disk {
    hard_state: Rc<Cell<Option<HardState>>>,
}

/// An in-test [`LogStorage`] wrapping [`InMemoryLog`] whose `hard_state` returns
/// what the shared [`Disk`] holds, modelling a durable log reopened after a
/// restart. Every other operation delegates unchanged to the inner log, so the
/// recovered replica's empty log makes any candidate's (also empty) log
/// trivially up to date — isolating the vote decision to the persisted vote.
struct TestLog {
    inner: InMemoryLog,
    disk: Disk,
}

impl TestLog {
    /// Open a log backed by `disk`; recovers whatever hard state `disk` holds.
    fn open(disk: Disk) -> Self {
        Self {
            inner: InMemoryLog::new(),
            disk,
        }
    }
}

impl LogStorage for TestLog {
    fn append(&mut self, payload: EntryPayload, term: u64) -> Result<u64, LogError> {
        self.inner.append(payload, term)
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> Result<(), LogError> {
        self.inner.append_entries(entries)
    }

    fn read(&self, start: u64, end: u64) -> Vec<LogEntry> {
        self.inner.read(start, end)
    }

    fn entry(&self, index: u64) -> Option<LogEntry> {
        self.inner.entry(index)
    }

    fn last_index(&self) -> Option<u64> {
        self.inner.last_index()
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        self.inner.term_at(index)
    }

    fn commit_index(&self) -> CommitIndex {
        self.inner.commit_index()
    }

    fn commit(&mut self, index: u64) -> Result<(), LogError> {
        self.inner.commit(index)
    }

    fn revert(&mut self, index: u64) -> Result<(), LogError> {
        self.inner.revert(index)
    }

    fn snapshot(&self) -> Snapshot {
        self.inner.snapshot()
    }

    fn persist_hard_state(&mut self, hard_state: HardState) -> Result<(), LogError> {
        // A durable persist: record to the shared disk so the recorded vote
        // stays observable (the persist path always succeeds here).
        self.disk.hard_state.set(Some(hard_state));
        Ok(())
    }

    fn hard_state(&self) -> Option<HardState> {
        self.disk.hard_state.get()
    }
}

/// Deliver a `RequestVote` from `candidate` at `term` (empty candidate log) and
/// return the single `RequestVoteReply` the replica emits.
fn request_vote(
    node: &mut RaftNode<TestLog>,
    clock: &mut TestClock,
    term: u64,
    candidate: u64,
) -> RequestVoteReply {
    let out = node.step(
        RaftInput::Message(RaftMessage::RequestVote(RequestVote {
            term,
            candidate_id: NodeId(candidate),
            last_log_index: None,
            last_log_term: None,
        })),
        clock,
    );
    out.sends
        .iter()
        .find_map(|(_, m)| match m {
            RaftMessage::RequestVoteReply(r) => Some(r.clone()),
            _ => None,
        })
        .expect("a RequestVote step must emit exactly one RequestVoteReply")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 11
    #[test]
    fn restored_replica_casts_no_second_vote_in_persisted_term(
        term in 1u64..64,
        voted in 1u64..8,
        other in 1u64..8,
    ) {
        // Make the rival candidate distinct from the one already voted for.
        prop_assume!(voted != other);

        let peers = vec![NodeId(1), NodeId(2), NodeId(3)];

        // Seed the durable disk as if the replica had granted a vote to
        // candidate `voted` in term `term` before a restart, then recover over
        // it: the recovered replica holds `current_term = term`,
        // `voted_for = Some(voted)` (R10.1, R10.2, R10.4 precondition).
        let disk = Disk::default();
        disk.hard_state.set(Some(HardState {
            current_term: term,
            voted_for: Some(voted),
        }));
        let mut node = RaftNode::recover(NodeId(0), peers, TestLog::open(disk));
        let mut clock = TestClock;

        prop_assert_eq!(node.current_term(), term);
        prop_assert_eq!(node.voted_for(), Some(NodeId(voted)));

        // A vote request from a *different* candidate in the same term must be
        // refused, and the recorded vote must not change (Requirement 10.4).
        let reply = request_vote(&mut node, &mut clock, term, other);
        prop_assert!(
            !reply.vote_granted,
            "a restored replica must not grant a second vote to a different candidate in its persisted term"
        );
        prop_assert_eq!(node.voted_for(), Some(NodeId(voted)));
        prop_assert_eq!(node.current_term(), term);

        // The refusal is specific to the conflicting candidate: a repeat request
        // from the same candidate it already voted for is still granted, so the
        // vote remains idempotent rather than a blanket refusal.
        let repeat = request_vote(&mut node, &mut clock, term, voted);
        prop_assert!(
            repeat.vote_granted,
            "a repeat vote request from the already-voted candidate must still be granted"
        );
        prop_assert_eq!(node.voted_for(), Some(NodeId(voted)));
        prop_assert_eq!(node.current_term(), term);
    }
}
