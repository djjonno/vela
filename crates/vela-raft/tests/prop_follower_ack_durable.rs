//! Property test for durable-gated follower acknowledgement in `vela-raft`.
//!
//! Feature: wal-group-commit, Property 3
//!
//! Property 3: every successful `AppendEntriesReply.match_index` a follower
//! emits is bounded by that follower's Durable_Index. Under the `Grouped` sync
//! policy a follower buffers the just-appended entries before they are
//! `fsync`ed, so its acknowledgement must report only the durable extent — a
//! follower never acks past what it has durably stored (design: success
//! `match_index = min(last_appended, durable_index)`; the post-flush
//! `RaftInput::Durable` produces the ack covering the newly-durable entries).
//!
//! The test drives a follower through the public `step` API across an
//! arbitrary interleaving of `AppendEntries` (which buffer entries) and
//! durable advances (`RaftInput::Durable`, which re-emit the deferred ack).
//! After every step it asserts that each emitted reply's `match_index` is
//! `<= durable_index`, treating `None` as below any `Some`.
//!
//! Validates: Requirements 1.2

use proptest::prelude::*;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use vela_log::{InMemoryLog, LogError, Snapshot};
use vela_raft::{
    AppendEntries, Clock, CommitIndex, EntryPayload, LogEntry, LogStorage, NodeId, PayloadKind,
    RaftInput, RaftMessage, RaftNode, TimerKind,
};

/// The id of the simulated leader whose `AppendEntries` the follower services.
const LEADER: NodeId = NodeId(1);

/// A trivial [`Clock`]: time never advances on its own and armed timers are
/// ignored.
struct TestClock;

impl Clock for TestClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// A shared, settable durable ceiling so a test can advance a log's
/// `durable_index` independently of its appended tail.
#[derive(Clone, Default)]
struct DurableHandle(Rc<Cell<CommitIndex>>);

impl DurableHandle {
    fn set(&self, index: CommitIndex) {
        self.0.set(index);
    }
    fn get(&self) -> CommitIndex {
        self.0.get()
    }
}

/// A [`LogStorage`] test double that delegates to [`InMemoryLog`] but reports a
/// `durable_index` capped to a settable ceiling, modelling the buffered
/// `Grouped` state in which the appended tail is not yet `fsync`ed.
struct DurableTestLog {
    inner: InMemoryLog,
    durable: DurableHandle,
}

impl DurableTestLog {
    fn new(durable: DurableHandle) -> Self {
        Self {
            inner: InMemoryLog::new(),
            durable,
        }
    }
}

impl LogStorage for DurableTestLog {
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
    fn durable_index(&self) -> CommitIndex {
        self.durable.get()
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
}

fn record(byte: u8) -> EntryPayload {
    EntryPayload::new(PayloadKind::Record, vec![byte])
}

/// `<=` over [`CommitIndex`], treating `None` as below any `Some`.
fn le(a: CommitIndex, b: CommitIndex) -> bool {
    match (a, b) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(x), Some(y)) => x <= y,
    }
}

/// Assert every reply in a step output respects the durable-ack bound.
fn assert_replies_bounded(
    out: &vela_raft::RaftOutput,
    durable: CommitIndex,
) -> Result<(), TestCaseError> {
    for (_, msg) in &out.sends {
        if let RaftMessage::AppendEntriesReply(reply) = msg {
            prop_assert!(
                le(reply.match_index, durable),
                "acked match_index {:?} exceeds durable_index {:?}",
                reply.match_index,
                durable
            );
        }
    }
    Ok(())
}

/// One scheduled action against the follower.
#[derive(Debug, Clone)]
enum Op {
    /// The leader replicates `count` fresh entries (contiguous, term 1) which
    /// the follower buffers but does not yet `fsync`.
    Append(u8),
    /// The driver forced buffered appends: advance the durable ceiling (kept
    /// monotonic and bounded by `last_index`) and re-emit the deferred ack.
    Durable(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..=4).prop_map(Op::Append),
        any::<u8>().prop_map(Op::Durable),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: wal-group-commit, Property 3
    #[test]
    fn follower_acked_match_index_never_exceeds_durable_index(
        ops in proptest::collection::vec(op_strategy(), 0..40),
    ) {
        let durable = DurableHandle::default();
        let mut node = RaftNode::new(
            NodeId(0),
            vec![LEADER, NodeId(2)],
            DurableTestLog::new(durable.clone()),
        );
        let mut clock = TestClock;

        for op in ops {
            match op {
                Op::Append(count) => {
                    // Build a log-matching AppendEntries that extends the
                    // follower's log contiguously, all entries in term 1.
                    let last = node.log().last_index();
                    let prev_log_index = last;
                    let prev_log_term = last.map(|_| 1);
                    let start = last.map_or(0, |i| i + 1);
                    let entries: Vec<LogEntry> = (0..u64::from(count))
                        .map(|j| {
                            let index = start + j;
                            LogEntry {
                                index,
                                term: 1,
                                payload: record(index as u8),
                            }
                        })
                        .collect();
                    let out = node.step(
                        RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                            term: 1,
                            leader_id: LEADER,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            // Keep commit out of scope: this property is about
                            // the ack bound, not commit propagation.
                            leader_commit: None,
                        })),
                        &mut clock,
                    );
                    assert_replies_bounded(&out, node.log().durable_index())?;
                }
                Op::Durable(raw) => {
                    if let Some(last) = node.log().last_index() {
                        let target = (u64::from(raw)).min(last);
                        let next = node
                            .log()
                            .durable_index()
                            .map_or(target, |cur| cur.max(target));
                        durable.set(Some(next));
                    }
                    let out = node.step(RaftInput::Durable, &mut clock);
                    // R1.2: the deferred ack covers only the newly-durable extent.
                    assert_replies_bounded(&out, node.log().durable_index())?;
                }
            }
        }
    }
}
