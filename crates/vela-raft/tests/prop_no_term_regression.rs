// Feature: per-topic-log-durability, Property 12
//
//! Property 12: a restart never regresses the persisted term.
//!
//! Validates: Requirements 10.1, 10.2, 10.5
//!
//! A durable replica persists its hard state (`current_term`, `voted_for`)
//! through the [`LogStorage`] seam before emitting any dependent message, and
//! Raft only ever advances `current_term` monotonically, so the term a durable
//! log retains across a restart is the highest term the replica ever reached.
//! This property has two parts:
//!
//!   1. Drive a [`RaftNode`] over a `TestLog` through a random sequence of term
//!      advances (vote grants and higher-term `AppendEntries` adoptions), each
//!      persisted to a shared [`Disk`], recording the maximum persisted term.
//!      Recover a fresh node over the same disk via [`RaftNode::recover`] and
//!      assert the recovered `current_term` equals that maximum (R10.1, R10.2).
//!   2. After recovery at term `T`, deliver a message carrying a strictly lower
//!      term `T' < T` (an `AppendEntries` heartbeat or a `RequestVote`) and
//!      assert `current_term` stays `T` — no regression — and that a stale
//!      leader's `AppendEntries` is rejected (R10.5).

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_log::{
    CommitIndex, EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage, Snapshot,
};
use vela_raft::{
    AppendEntries, AppendEntriesReply, Clock, NodeId, RaftInput, RaftMessage, RaftNode,
    RequestVote, RequestVoteReply, TimerKind,
};

/// A trivial [`Clock`] for direct `step` tests: logical time never advances on
/// its own and armed timers are simply discarded. The term-adoption logic under
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

/// The persistent "disk" shared across a simulated restart: the hard state a
/// durable log would retain when the process stops and the log is later
/// reopened. Cloning shares the same backing cell, so a `TestLog` reopened over
/// a clone observes exactly what the original persisted.
#[derive(Clone, Default)]
struct Disk {
    hard_state: Rc<Cell<Option<HardState>>>,
}

/// An in-test [`LogStorage`] wrapping [`InMemoryLog`] whose `persist_hard_state`
/// records to a shared [`Disk`] and whose `hard_state` returns what the `Disk`
/// holds. Opening a fresh `TestLog` over the same `Disk` therefore recovers the
/// last persisted value, modelling a durable log reopened after a restart.
/// Every other operation delegates unchanged to the inner log.
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
        // A durable persist: record to the shared disk so a reopened log
        // recovers it (the persist-before-emit path always succeeds here).
        self.disk.hard_state.set(Some(hard_state));
        Ok(())
    }

    fn hard_state(&self) -> Option<HardState> {
        self.disk.hard_state.get()
    }
}

/// A single term-advancing operation applied to the running replica.
///
/// Both kinds leave the replica a follower (a granted vote re-arms the election
/// timer; a strictly higher term forces a step-down), and Raft only ever moves
/// `current_term` upward, so the persisted term is monotonically non-decreasing
/// and its final value is the maximum ever persisted.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// Deliver a `RequestVote` from `candidate` at `term`; the replica grants
    /// when the term is current-or-newer, the (empty) candidate log is up to
    /// date, and it has not already voted otherwise this term — persisting
    /// `term` before the grant.
    Vote { term: u64, candidate: u64 },
    /// Deliver an empty `AppendEntries` heartbeat at `term`; a strictly higher
    /// term is adopted (and persisted) before the reply is emitted.
    Adopt { term: u64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1u64..16, 1u64..4).prop_map(|(term, candidate)| Op::Vote { term, candidate }),
        (1u64..16).prop_map(|term| Op::Adopt { term }),
    ]
}

/// Apply one operation to the replica, driving the corresponding term-advance
/// mutation point.
fn apply(node: &mut RaftNode<TestLog>, op: Op, clock: &mut TestClock) {
    match op {
        Op::Vote { term, candidate } => {
            node.step(
                RaftInput::Message(RaftMessage::RequestVote(RequestVote {
                    term,
                    candidate_id: NodeId(candidate),
                    last_log_index: None,
                    last_log_term: None,
                })),
                clock,
            );
        }
        Op::Adopt { term } => {
            node.step(
                RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                    term,
                    leader_id: NodeId(1),
                    prev_log_index: None,
                    prev_log_term: None,
                    entries: Vec::new(),
                    leader_commit: None,
                })),
                clock,
            );
        }
    }
}

/// Deliver a stale (lower-term) `AppendEntries` heartbeat and return the single
/// `AppendEntriesReply` the replica emits.
fn stale_append_entries(
    node: &mut RaftNode<TestLog>,
    clock: &mut TestClock,
    term: u64,
) -> AppendEntriesReply {
    let out = node.step(
        RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
            term,
            leader_id: NodeId(1),
            prev_log_index: None,
            prev_log_term: None,
            entries: Vec::new(),
            leader_commit: None,
        })),
        clock,
    );
    out.sends
        .iter()
        .find_map(|(_, m)| match m {
            RaftMessage::AppendEntriesReply(r) => Some(r.clone()),
            _ => None,
        })
        .expect("an AppendEntries step must emit exactly one AppendEntriesReply")
}

/// Deliver a stale (lower-term) `RequestVote` and return the single
/// `RequestVoteReply` the replica emits.
fn stale_request_vote(
    node: &mut RaftNode<TestLog>,
    clock: &mut TestClock,
    term: u64,
) -> RequestVoteReply {
    let out = node.step(
        RaftInput::Message(RaftMessage::RequestVote(RequestVote {
            term,
            candidate_id: NodeId(2),
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

    // Feature: per-topic-log-durability, Property 12
    #[test]
    fn no_term_regression_after_restart(
        ops in prop::collection::vec(op_strategy(), 1..24),
        stale_via_append in any::<bool>(),
    ) {
        let disk = Disk::default();
        let peers = vec![NodeId(1), NodeId(2), NodeId(3)];

        // Drive a replica over the durable disk through the random sequence,
        // persisting every term advance. Track the maximum persisted term;
        // because Raft advances `current_term` monotonically, the running max
        // of `current_term()` after each step is exactly that maximum.
        let mut node = RaftNode::recover(NodeId(0), peers.clone(), TestLog::open(disk.clone()));
        let mut clock = TestClock;
        let mut max_persisted = 0u64;
        for op in ops {
            apply(&mut node, op, &mut clock);
            max_persisted = max_persisted.max(node.current_term());
        }

        // The final in-memory term equals the maximum persisted term (the last
        // persist wrote the highest, monotonically non-decreasing term).
        prop_assert_eq!(node.current_term(), max_persisted);
        drop(node);

        // Part 1: a restart restores `current_term` equal to the maximum
        // persisted term (Requirements 10.1, 10.2).
        let mut restarted = RaftNode::recover(NodeId(0), peers, TestLog::open(disk));
        prop_assert_eq!(restarted.current_term(), max_persisted);

        // A non-empty op sequence always advances the term at least once, so a
        // strictly lower term exists to test the no-regression guarantee.
        prop_assert!(max_persisted >= 1);
        let term = restarted.current_term();
        let stale_term = term - 1; // strictly lower than the recovered term

        // Part 2: a later message carrying a strictly lower term must not lower
        // `current_term`, and a stale leader's AppendEntries is rejected
        // (Requirement 10.5).
        if stale_via_append {
            let reply = stale_append_entries(&mut restarted, &mut clock, stale_term);
            prop_assert!(
                !reply.success,
                "a stale-term leader's AppendEntries must be rejected after restart"
            );
            prop_assert_eq!(reply.term, term);
        } else {
            let reply = stale_request_vote(&mut restarted, &mut clock, stale_term);
            prop_assert!(
                !reply.vote_granted,
                "a stale-term vote request must be refused after restart"
            );
            prop_assert_eq!(reply.term, term);
        }

        // The recovered term never regressed to the stale term (R10.5).
        prop_assert_eq!(restarted.current_term(), term);
    }
}
