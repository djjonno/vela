// Feature: per-topic-log-durability, Property 9
//
//! Property 9: persist-before-emit and suppress-on-failure for Raft hard state.
//!
//! Validates: Requirements 9.1, 9.2, 9.4
//!
//! A durable replica persists its hard state (`current_term`, `voted_for`)
//! through the [`LogStorage`] seam *before* emitting any message that depends
//! on the new term or vote. This property drives a [`RaftNode`] over an in-test
//! `TestLog` — an `InMemoryLog` whose `persist_hard_state` can be forced to fail
//! — across the three hard-state mutation points (granting a vote, adopting a
//! higher term, and starting an election):
//!
//! - On a successful persist the dependent message is emitted and the node's
//!   in-memory `current_term`/`voted_for` reflect the persisted value.
//! - On a failed persist the term and vote are left unchanged, no dependent
//!   message is emitted, and [`RaftOutput::persist_error`] is set to the
//!   operation that failed.

use std::cell::Cell;
use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_log::{
    CommitIndex, EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage, Snapshot,
};
use vela_raft::{
    AppendEntries, Clock, NodeId, RaftInput, RaftMessage, RaftNode, RequestVote, TimerKind,
};

/// A trivial [`Clock`] for direct `step` tests: logical time never advances on
/// its own and armed timers are simply discarded. The persist-before-emit logic
/// under test never reads the clock's instant, so a fixed reference time keeps
/// the test fully deterministic.
#[derive(Default)]
struct TestClock;

impl Clock for TestClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// An in-test [`LogStorage`] wrapping [`InMemoryLog`], whose `persist_hard_state`
/// can be toggled to fail. Every other operation delegates unchanged to the
/// inner log, so consensus drives it exactly as the real in-memory path.
///
/// The fail toggle and the recorded persisted value use [`Cell`] so the test
/// can flip the toggle through the immutable [`RaftNode::log`] accessor and
/// inspect what was persisted after a step.
struct TestLog {
    inner: InMemoryLog,
    /// When `true`, `persist_hard_state` returns an [`LogError::Io`] instead of
    /// persisting, simulating a durable-storage failure.
    fail: Cell<bool>,
    /// The most recent hard state a *successful* `persist_hard_state` recorded.
    last_persisted: Cell<Option<HardState>>,
    /// Count of `persist_hard_state` calls (successful or not).
    persist_calls: Cell<usize>,
}

impl TestLog {
    fn new() -> Self {
        Self {
            inner: InMemoryLog::new(),
            fail: Cell::new(false),
            last_persisted: Cell::new(None),
            persist_calls: Cell::new(0),
        }
    }

    /// Toggle whether `persist_hard_state` fails.
    fn set_fail(&self, fail: bool) {
        self.fail.set(fail);
    }

    fn persist_calls(&self) -> usize {
        self.persist_calls.get()
    }

    fn last_persisted(&self) -> Option<HardState> {
        self.last_persisted.get()
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
        self.persist_calls.set(self.persist_calls.get() + 1);
        if self.fail.get() {
            return Err(LogError::Io {
                op: "persist_hard_state",
                source: std::io::Error::other("forced hard-state persist failure"),
            });
        }
        self.last_persisted.set(Some(hard_state));
        Ok(())
    }

    fn hard_state(&self) -> Option<HardState> {
        self.last_persisted.get()
    }
}

/// Build a three-node follower (`NodeId(0)` with peers `1` and `2`) over a
/// fresh `TestLog`, then raise it to `base_term` with a successful persist via a
/// heartbeat-style `AppendEntries` so the term advance is durable before the
/// mutation under test. Returns the node sitting as a follower at `base_term`
/// with no vote recorded.
fn follower_at_term(base_term: u64) -> RaftNode<TestLog> {
    let mut node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], TestLog::new());
    if base_term > 0 {
        let mut clock = TestClock;
        node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: base_term,
                leader_id: NodeId(1),
                prev_log_index: None,
                prev_log_term: None,
                entries: Vec::new(),
                leader_commit: None,
            })),
            &mut clock,
        );
    }
    node
}

/// The mutation point a generated scenario exercises.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// Grant a vote in the current term (no higher-term adoption first).
    GrantVote,
    /// Adopt a strictly higher term carried by an incoming message.
    AdoptTerm,
    /// Start an election from an election-timeout tick.
    StartElection,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        Just(Op::GrantVote),
        Just(Op::AdoptTerm),
        Just(Op::StartElection),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 9
    #[test]
    fn persist_before_emit_and_suppress_on_failure(
        op in op_strategy(),
        base_term in 0u64..40,
        delta in 1u64..40,
        candidate in 1u64..4,
        fail in any::<bool>(),
    ) {
        let mut clock = TestClock;

        match op {
            // --- Grant a vote in the current term (Requirement 9.1) --------
            Op::GrantVote => {
                let mut node = follower_at_term(base_term);
                let calls_before = node.log().persist_calls();
                node.log().set_fail(fail);

                let cand = NodeId(candidate);
                // term == current_term, so no higher-term adoption runs first;
                // the empty candidate log is trivially up to date and the node
                // has not yet voted, so the grant decision is `true`.
                let out = node.step(
                    RaftInput::Message(RaftMessage::RequestVote(RequestVote {
                        term: base_term,
                        candidate_id: cand,
                        last_log_index: None,
                        last_log_term: None,
                    })),
                    &mut clock,
                );

                // The grant path always attempts exactly one persist.
                prop_assert_eq!(node.log().persist_calls(), calls_before + 1);
                let granted = out.sends.iter().any(|(to, m)| {
                    *to == cand
                        && matches!(
                            m,
                            RaftMessage::RequestVoteReply(r) if r.vote_granted
                        )
                });

                if fail {
                    // Suppressed: no grant emitted, term/vote unchanged, error set.
                    prop_assert!(!granted, "no vote grant may be emitted on persist failure");
                    prop_assert_eq!(node.current_term(), base_term);
                    prop_assert_eq!(node.voted_for(), None);
                    let err = out.persist_error.expect("persist_error must be set");
                    prop_assert_eq!(err.op, "grant_vote");
                } else {
                    // Emitted: grant sent, in-memory state reflects persisted value.
                    prop_assert!(granted, "vote grant must be emitted on persist success");
                    prop_assert_eq!(node.current_term(), base_term);
                    prop_assert_eq!(node.voted_for(), Some(cand));
                    prop_assert!(out.persist_error.is_none());
                    prop_assert_eq!(
                        node.log().last_persisted(),
                        Some(HardState { current_term: base_term, voted_for: Some(candidate) })
                    );
                }
            }

            // --- Adopt a strictly higher term (Requirement 9.2) ------------
            Op::AdoptTerm => {
                let mut node = follower_at_term(base_term);
                let calls_before = node.log().persist_calls();
                node.log().set_fail(fail);

                let higher = base_term + delta;
                // A heartbeat at a strictly higher term forces term adoption
                // before anything term-dependent is emitted.
                let out = node.step(
                    RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                        term: higher,
                        leader_id: NodeId(1),
                        prev_log_index: None,
                        prev_log_term: None,
                        entries: Vec::new(),
                        leader_commit: None,
                    })),
                    &mut clock,
                );

                // Adoption always attempts exactly one persist.
                prop_assert_eq!(node.log().persist_calls(), calls_before + 1);

                if fail {
                    // Aborted: term unchanged, nothing term-dependent emitted, error set.
                    prop_assert_eq!(node.current_term(), base_term);
                    prop_assert!(
                        out.sends.is_empty(),
                        "no term-dependent message may be emitted on persist failure"
                    );
                    let err = out.persist_error.expect("persist_error must be set");
                    prop_assert_eq!(err.op, "adopt_term");
                } else {
                    // Adopted: term reflects persisted value and the append is acked.
                    prop_assert_eq!(node.current_term(), higher);
                    prop_assert!(
                        out.sends.iter().any(|(to, m)| *to == NodeId(1)
                            && matches!(m, RaftMessage::AppendEntriesReply(_))),
                        "the dependent append reply must be emitted on persist success"
                    );
                    prop_assert!(out.persist_error.is_none());
                    prop_assert_eq!(
                        node.log().last_persisted(),
                        Some(HardState { current_term: higher, voted_for: None })
                    );
                }
            }

            // --- Start an election (Requirement 9.2) -----------------------
            Op::StartElection => {
                let mut node = follower_at_term(base_term);
                let calls_before = node.log().persist_calls();
                node.log().set_fail(fail);

                let out = node.step(RaftInput::Tick(TimerKind::Election), &mut clock);

                // The election attempt always persists exactly once.
                prop_assert_eq!(node.log().persist_calls(), calls_before + 1);
                let broadcast = out
                    .sends
                    .iter()
                    .any(|(_, m)| matches!(m, RaftMessage::RequestVote(_)));

                if fail {
                    // Suppressed: stays a follower at the same term, broadcasts
                    // nothing, surfaces the failure.
                    prop_assert_eq!(node.current_term(), base_term);
                    prop_assert_eq!(node.role(), vela_raft::Role::Follower);
                    prop_assert_eq!(node.voted_for(), None);
                    prop_assert!(!broadcast, "no RequestVote may be broadcast on persist failure");
                    let err = out.persist_error.expect("persist_error must be set");
                    prop_assert_eq!(err.op, "start_election");
                } else {
                    // Started: bumped term, self-vote, RequestVote broadcast.
                    prop_assert_eq!(node.current_term(), base_term + 1);
                    prop_assert_eq!(node.role(), vela_raft::Role::Candidate);
                    prop_assert_eq!(node.voted_for(), Some(NodeId(0)));
                    prop_assert!(broadcast, "RequestVote must be broadcast on persist success");
                    prop_assert!(out.persist_error.is_none());
                    prop_assert_eq!(
                        node.log().last_persisted(),
                        Some(HardState { current_term: base_term + 1, voted_for: Some(0) })
                    );
                }
            }
        }
    }
}
