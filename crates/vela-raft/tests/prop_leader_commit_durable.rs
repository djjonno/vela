//! Property test for durable-gated leader commit in `vela-raft`.
//!
//! Feature: wal-group-commit, Property 2
//!
//! Property 2: a leader's `commit_index` never exceeds its own Durable_Index
//! across arbitrary schedules. Under the `Grouped` sync policy the leader's
//! appended tail can be buffered but not yet `fsync`ed, so commit must be
//! gated on what the leader has durably stored — "committed" continues to mean
//! "durable on a majority", counting the leader only for the prefix it has
//! itself forced to disk (design: leader `advance_commit` ceiling becomes
//! `min(last_index, durable_index)`).
//!
//! The test elects a single leader of a 3- or 5-node group through the public
//! `step` API, then drives an arbitrary interleaving of leader proposals, peer
//! acknowledgements (`AppendEntriesReply`), and durable advances
//! (`RaftInput::Durable`). After every step it asserts the gating invariant
//! `commit_index <= durable_index`, treating `None` (nothing committed /
//! nothing durable) as below any `Some`.
//!
//! Validates: Requirements 1.3

use proptest::prelude::*;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use vela_log::{InMemoryLog, LogError, Snapshot};
use vela_raft::{
    AppendEntriesReply, Clock, CommitIndex, EntryPayload, LogStorage, NodeId, PayloadKind,
    RaftInput, RaftMessage, RaftNode, RequestVoteReply, Role, TimerKind,
};

/// A trivial [`Clock`] for direct, single-node `step` tests: time never
/// advances on its own and armed timers are ignored.
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
    fn append_entries(&mut self, entries: &[vela_raft::LogEntry]) -> Result<(), LogError> {
        self.inner.append_entries(entries)
    }
    fn read(&self, start: u64, end: u64) -> Vec<vela_raft::LogEntry> {
        self.inner.read(start, end)
    }
    fn entry(&self, index: u64) -> Option<vela_raft::LogEntry> {
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
    // The whole point of this double: a durable extent that can lag the last
    // appended index.
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

/// `<=` over [`CommitIndex`], treating `None` as below any `Some`: nothing
/// committed / nothing durable is the bottom of the order.
fn le(a: CommitIndex, b: CommitIndex) -> bool {
    match (a, b) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(x), Some(y)) => x <= y,
    }
}

/// Drive node 0 to leader of an `n`-node group through the public API: one
/// election tick makes it a candidate at term 1, then enough peer grants reach
/// a majority and promote it.
fn elect_leader(node: &mut RaftNode<DurableTestLog>, clock: &mut TestClock, peers: &[NodeId]) {
    node.step(RaftInput::Tick(TimerKind::Election), clock);
    let total = peers.len() + 1;
    let majority = total / 2 + 1;
    let needed = majority - 1; // the candidate's own self-vote already counts.
    for &peer in peers.iter().take(needed) {
        node.step(
            RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                term: 1,
                vote_granted: true,
                voter: peer,
            })),
            clock,
        );
    }
}

/// One scheduled action against the leader.
#[derive(Debug, Clone)]
enum Op {
    /// A client proposal appended (buffered) at the leader.
    Propose(u8),
    /// A peer acknowledges replication up to a (clamped) match index.
    Ack { peer: u8, target: u8 },
    /// The driver forced buffered appends: advance the durable ceiling (kept
    /// monotonic and bounded by `last_index`) and re-drive consensus.
    Durable(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        any::<u8>().prop_map(Op::Propose),
        (any::<u8>(), any::<u8>()).prop_map(|(peer, target)| Op::Ack { peer, target }),
        any::<u8>().prop_map(Op::Durable),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: wal-group-commit, Property 2
    #[test]
    fn leader_commit_index_never_exceeds_durable_index(
        five_nodes in any::<bool>(),
        ops in proptest::collection::vec(op_strategy(), 0..40),
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let peers: Vec<NodeId> = (1..node_count).map(NodeId).collect();

        let durable = DurableHandle::default();
        let mut node = RaftNode::new(NodeId(0), peers.clone(), DurableTestLog::new(durable.clone()));
        let mut clock = TestClock;

        elect_leader(&mut node, &mut clock, &peers);
        prop_assert_eq!(node.role(), Role::Leader, "node 0 should win a clean election");

        // The invariant holds for the freshly elected, empty leader.
        prop_assert!(le(node.commit_index(), node.log().durable_index()));

        for op in ops {
            match op {
                Op::Propose(b) => {
                    node.step(RaftInput::Propose(record(b)), &mut clock);
                }
                Op::Ack { peer, target } => {
                    // An ack only means something once the leader has entries to
                    // be matched; clamp the matched index to the leader's log.
                    if let Some(last) = node.log().last_index() {
                        let p = peers[(peer as usize) % peers.len()];
                        let match_index = (u64::from(target)).min(last);
                        node.step(
                            RaftInput::Message(RaftMessage::AppendEntriesReply(
                                AppendEntriesReply {
                                    from: p,
                                    term: 1,
                                    success: true,
                                    conflict_index: None,
                                    match_index: Some(match_index),
                                },
                            )),
                            &mut clock,
                        );
                    }
                }
                Op::Durable(raw) => {
                    // A real `fsync` extent only grows and never outruns the
                    // appended tail: clamp to `last_index` and keep monotonic.
                    if let Some(last) = node.log().last_index() {
                        let target = (u64::from(raw)).min(last);
                        let next = node
                            .log()
                            .durable_index()
                            .map_or(target, |cur| cur.max(target));
                        durable.set(Some(next));
                    }
                    node.step(RaftInput::Durable, &mut clock);
                }
            }

            // R1.3: a leader never commits an index it has not itself `fsync`ed.
            prop_assert!(
                le(node.commit_index(), node.log().durable_index()),
                "commit_index {:?} exceeds durable_index {:?}",
                node.commit_index(),
                node.log().durable_index()
            );
        }
    }
}
