//! Property test for group-commit equivalence in `vela-raft`.
//!
//! Feature: wal-group-commit, Property 4
//!
//! Property 4: driving the same logical sequence of proposals with group
//! commit yields identical committed offsets and values as the per-append
//! drive. Group commit only changes *when* the `fsync` happens, never offset
//! assignment, ordering, or commit results (design / Requirement 3.3): a batch
//! of buffered appends forced by a single durable advance must commit exactly
//! the same `(index, payload)` sequence as forcing after every single append.
//!
//! Both runs use a single-node leader over the durable-aware log double, so the
//! leader is its own majority and the only variable is the force schedule:
//! - **per-append**: after every proposal the durable ceiling advances to
//!   `last_index` and `RaftInput::Durable` is stepped (durable tracks the tail);
//! - **group-commit**: several proposals are buffered, then one durable advance
//!   to `last_index` and a single `RaftInput::Durable` commits the whole batch.
//!
//! The test asserts the two committed sequences are identical to each other and
//! to the proposed sequence `[(0, p0), (1, p1), …]`.
//!
//! Validates: Requirements 3.3

use proptest::prelude::*;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use vela_log::{InMemoryLog, LogError, Snapshot};
use vela_raft::{
    Clock, CommitIndex, EntryPayload, LogEntry, LogStorage, NodeId, PayloadKind, RaftInput,
    RaftNode, RaftOutput, Role, TimerKind,
};

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

/// Append the entries a step surfaced as committed to `committed`, as
/// `(index, payload)` pairs in the order they were surfaced.
fn collect(committed: &mut Vec<(u64, EntryPayload)>, out: RaftOutput) {
    committed.extend(out.committed.into_iter().map(|e| (e.index, e.payload)));
}

/// Build a fresh single-node leader over the durable-aware log double, plus the
/// handle that controls its durable ceiling.
fn fresh_leader() -> (RaftNode<DurableTestLog>, DurableHandle, TestClock) {
    let durable = DurableHandle::default();
    let mut node = RaftNode::new(NodeId(0), vec![], DurableTestLog::new(durable.clone()));
    let mut clock = TestClock;
    // A single-node group reaches a majority with only the self-vote, so one
    // election tick promotes the node to leader at term 1.
    node.step(RaftInput::Tick(TimerKind::Election), &mut clock);
    debug_assert_eq!(node.role(), Role::Leader);
    (node, durable, clock)
}

/// Per-append drive: force (advance durable to `last_index` + step `Durable`)
/// after **every** proposal, so the durable extent tracks the appended tail.
fn run_per_append(payloads: &[u8]) -> Vec<(u64, EntryPayload)> {
    let (mut node, durable, mut clock) = fresh_leader();
    let mut committed = Vec::new();
    for &b in payloads {
        collect(
            &mut committed,
            node.step(RaftInput::Propose(record(b)), &mut clock),
        );
        durable.set(node.log().last_index());
        collect(&mut committed, node.step(RaftInput::Durable, &mut clock));
    }
    committed
}

/// Group-commit drive: buffer up to `batch` proposals, then force them with a
/// single durable advance + one `RaftInput::Durable`.
fn run_group_commit(payloads: &[u8], batch: usize) -> Vec<(u64, EntryPayload)> {
    let (mut node, durable, mut clock) = fresh_leader();
    let mut committed = Vec::new();
    for chunk in payloads.chunks(batch) {
        for &b in chunk {
            collect(
                &mut committed,
                node.step(RaftInput::Propose(record(b)), &mut clock),
            );
        }
        // One force for the whole buffered batch.
        durable.set(node.log().last_index());
        collect(&mut committed, node.step(RaftInput::Durable, &mut clock));
    }
    committed
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: wal-group-commit, Property 4
    #[test]
    fn group_commit_matches_per_append(
        payloads in proptest::collection::vec(any::<u8>(), 1..20),
        batch in 1usize..=8,
    ) {
        let per_append = run_per_append(&payloads);
        let grouped = run_group_commit(&payloads, batch);

        // The committed (index, payload) sequence is exactly the proposed
        // sequence, regardless of how the force was scheduled (R3.3).
        let expected: Vec<(u64, EntryPayload)> = payloads
            .iter()
            .enumerate()
            .map(|(i, &b)| (i as u64, record(b)))
            .collect();

        prop_assert_eq!(&per_append, &expected, "per-append commit diverged from the proposed sequence");
        prop_assert_eq!(&grouped, &expected, "group-commit diverged from the proposed sequence");
        prop_assert_eq!(&grouped, &per_append, "group-commit and per-append committed different sequences");
    }
}
