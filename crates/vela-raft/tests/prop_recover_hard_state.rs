// Feature: per-topic-log-durability, Property 10
//
//! Property 10: a restart restores the exact pre-restart Raft hard state.
//!
//! Validates: Requirements 10.1, 10.2, 10.3
//!
//! A durable replica persists its hard state (`current_term`, `voted_for`)
//! through the [`LogStorage`] seam before emitting any dependent message, so
//! the state a durable log retains across a process restart is exactly the
//! state the replica held in memory. This property drives a [`RaftNode`] over a
//! `TestLog` through a random sequence of vote grants and term adoptions —
//! neither of which promotes the replica to leader, so its in-memory
//! `current_term`/`voted_for` stay equal to what was persisted — then simulates
//! a restart by opening a fresh `RaftNode` via [`RaftNode::recover`] over the
//! same persisted log state and asserts the recovered `current_term` and
//! `voted_for` equal the values held immediately before the restart.

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_log::{
    CommitIndex, EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage, Snapshot,
};
use vela_raft::{
    AppendEntries, Clock, NodeId, RaftInput, RaftMessage, RaftNode, RequestVote, TimerKind,
};

/// A trivial [`Clock`] for direct `step` tests: logical time never advances on
/// its own and armed timers are simply discarded. The hard-state logic under
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

/// A single hard-state-mutating operation applied to the running replica.
///
/// Both kinds leave the replica a follower (a granted vote re-arms the election
/// timer but does not start an election; a higher term forces a step-down), so
/// the in-memory hard state never diverges from what was persisted.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// Deliver a `RequestVote` from `candidate` at `term`; the replica grants
    /// when the term is current-or-newer, the (empty) candidate log is up to
    /// date, and it has not already voted otherwise this term.
    Vote { term: u64, candidate: u64 },
    /// Deliver an empty `AppendEntries` heartbeat at `term`; a strictly higher
    /// term is adopted (clearing the vote) before the reply is emitted.
    Adopt { term: u64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1u64..12, 1u64..4).prop_map(|(term, candidate)| Op::Vote { term, candidate }),
        (1u64..12).prop_map(|term| Op::Adopt { term }),
    ]
}

/// Apply one operation to the replica, driving the corresponding hard-state
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 10
    #[test]
    fn recover_restores_exact_pre_restart_hard_state(ops in prop::collection::vec(op_strategy(), 1..24)) {
        let disk = Disk::default();
        let peers = vec![NodeId(1), NodeId(2), NodeId(3)];

        // Drive a replica over the durable disk through the random sequence,
        // persisting every term/vote change before its dependent reply.
        let mut node = RaftNode::recover(NodeId(0), peers.clone(), TestLog::open(disk.clone()));
        let mut clock = TestClock;
        for op in ops {
            apply(&mut node, op, &mut clock);
        }

        // The hard state held immediately before the simulated restart.
        let term_before = node.current_term();
        let vote_before = node.voted_for();
        drop(node);

        // Simulate a restart: reopen a fresh replica over the same persisted
        // disk state. The restored hard state must match exactly (R10.1–10.3).
        let restarted = RaftNode::recover(NodeId(0), peers, TestLog::open(disk));

        prop_assert_eq!(restarted.current_term(), term_before);
        prop_assert_eq!(restarted.voted_for(), vote_before);
    }
}
