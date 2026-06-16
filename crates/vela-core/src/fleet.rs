//! The per-partition Raft-group fleet and partition state machine.
//!
//! This module owns the node-local composition of consensus and partition
//! state. Vela's defining trait is **one independent Raft group per partition**
//! (Requirement 7.1), and this module is where those groups live on a node:
//!
//! - [`StateMachine`] — the partition's state machine. It applies committed log
//!   entries in order, assigning each committed [`Record`](crate::model::Record)
//!   entry the next gap-free, 0-based [`Offset`], and keeps the committed
//!   payloads so they can be read back (Requirement 4.7, 5.1). Non-record
//!   entries (a leader's `Noop` or a `Cluster` command) are applied but do
//!   **not** consume a record offset.
//! - [`PartitionReplica`] — one partition replica hosted on this node: a
//!   [`RaftNode`] over an [`InMemoryLog`] paired with its [`StateMachine`].
//!   Driving it with a [`RaftInput`] steps consensus and folds any newly
//!   committed entries into the state machine, so offsets are assigned exactly
//!   when entries commit.
//! - [`RaftGroupFleet`] — the collection of [`PartitionReplica`]s on the node,
//!   keyed by `(topic, partition)`. It enforces that **at most one** Raft group
//!   exists per partition and provides the create/stop lifecycle a topic
//!   deletion drives (Requirement 3.2): stopping a group drops the replica,
//!   releasing both its Raft state and its in-memory log together.
//!
//! This crate stays free of gRPC: the fleet is driven step-by-step by the
//! server crate, which supplies the real [`Clock`] and [`Transport`]. The
//! consensus layer is identified by [`vela_raft::NodeId`] (a numeric id), which
//! the server maps to/from the domain's string [`NodeId`](crate::model::NodeId)
//! at its seam.

use std::collections::HashMap;

use vela_log::{InMemoryLog, LogEntry, LogStorage, PayloadKind};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, RaftNode, RaftOutput, Role};

use crate::model::{Offset, PartitionIndex};
use crate::partition_log::PartitionLog;

/// The key identifying one partition's Raft group within a [`RaftGroupFleet`]:
/// the topic name paired with the partition index (Requirement 7.1).
pub type GroupKey = (String, PartitionIndex);

/// A committed record returned by a [`StateMachine`] read.
///
/// Carries the gap-free 0-based [`Offset`] the state machine assigned on apply
/// and the committed payload bytes. The bytes are the opaque payload the
/// produce path appended; decoding them back into a
/// [`Record`](crate::model::Record) is the consume path's concern (task 11.11),
/// so this layer preserves them verbatim (Requirement 5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedRecord {
    /// The 0-based offset assigned to this record on commit.
    pub offset: Offset,
    /// The committed record's opaque payload bytes.
    pub value: Vec<u8>,
}

/// The partition state machine: applies committed entries in order and assigns
/// records gap-free, 0-based offsets (Requirement 4.7, 5.1, 8.8).
///
/// The Raft layer surfaces newly committed entries in ascending index order,
/// exactly once; [`StateMachine::apply`] folds each into partition state. A
/// committed record entry is assigned the next offset (its position **among
/// record entries**) and stored so it can be read back; a `Noop` or `Cluster`
/// entry is consumed without assigning an offset, which is what keeps record
/// offsets contiguous even though the underlying log interleaves non-record
/// entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateMachine {
    /// Committed record payloads, densely indexed by offset: `records[o]` is the
    /// record committed at offset `o`. Because non-record entries are skipped,
    /// this vector's positions are exactly the gap-free record offsets.
    records: Vec<Vec<u8>>,
}

impl StateMachine {
    /// Create an empty state machine: no records, next offset 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one committed log `entry`, returning the [`Offset`] assigned if it
    /// was a record entry, or `None` for a `Noop`/`Cluster` entry.
    ///
    /// Record entries receive the next gap-free 0-based offset and are stored
    /// for later reads (Requirement 4.7, 5.1); non-record entries are applied
    /// (acknowledged) but do not advance the record offset.
    pub fn apply(&mut self, entry: &LogEntry) -> Option<Offset> {
        match entry.payload.kind {
            PayloadKind::Record => {
                let offset = self.records.len() as Offset;
                self.records.push(entry.payload.bytes.clone());
                Some(offset)
            }
            PayloadKind::Noop | PayloadKind::Cluster => None,
        }
    }

    /// Apply a batch of newly committed entries in order (Requirement 8.8).
    ///
    /// Convenience over [`StateMachine::apply`] for the slice the Raft layer
    /// hands back in [`RaftOutput::committed`].
    pub fn apply_committed(&mut self, entries: &[LogEntry]) {
        for entry in entries {
            self.apply(entry);
        }
    }

    /// The offset the next committed record will receive (the count of records
    /// applied so far).
    pub fn next_offset(&self) -> Offset {
        self.records.len() as Offset
    }

    /// The highest committed offset, or `None` if no record has been committed.
    pub fn high_water_mark(&self) -> Option<Offset> {
        (self.records.len() as Offset).checked_sub(1)
    }

    /// Number of committed records applied.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether no records have been committed yet.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Read committed records in ascending offset order, beginning at `start`
    /// and returning at most `max` of them (Requirement 5.1).
    ///
    /// Returns an empty vector when `max` is 0 or when `start` is beyond the
    /// highest committed offset — a read past the end is not an error, it simply
    /// yields no records.
    pub fn read(&self, start: Offset, max: usize) -> Vec<CommittedRecord> {
        if max == 0 {
            return Vec::new();
        }
        let start_idx = start as usize;
        if start_idx >= self.records.len() {
            return Vec::new();
        }
        let end = start_idx.saturating_add(max).min(self.records.len());
        (start_idx..end)
            .map(|i| CommittedRecord {
                offset: i as Offset,
                value: self.records[i].clone(),
            })
            .collect()
    }
}

/// One partition replica hosted on this node: a [`RaftNode`] over a
/// [`PartitionLog`] paired with the partition's [`StateMachine`].
///
/// The log is injected at construction rather than hardcoded, so a replica can
/// be built over either backend the [`PartitionLog`] enum holds — durable or
/// in-memory — without `PartitionReplica` knowing which (Requirement 5.2).
///
/// Consensus and partition state are kept together so that committing entries
/// and assigning offsets cannot drift apart: [`PartitionReplica::step`] drives
/// the Raft state machine one input at a time and immediately folds any newly
/// committed entries into the state machine, assigning offsets at the moment of
/// commit (Requirement 4.7, 8.8).
pub struct PartitionReplica {
    /// The consensus state machine for this partition replica.
    raft: RaftNode<PartitionLog>,
    /// The partition state machine fed by committed entries.
    state: StateMachine,
}

impl PartitionReplica {
    /// Create a follower replica for a fresh partition Raft group over the
    /// injected `log`: a [`RaftNode`] with the given identity and peer set built
    /// on `log`, plus an empty [`StateMachine`] (Requirement 5.2, 13.2).
    ///
    /// This is the construction path the server uses once it has selected and
    /// built the topic's backend; for a fresh log no entries are committed yet,
    /// so the state machine starts empty.
    pub fn with_log(node_id: RaftNodeId, peers: Vec<RaftNodeId>, log: PartitionLog) -> Self {
        Self {
            raft: RaftNode::new(node_id, peers, log),
            state: StateMachine::new(),
        }
    }

    /// Recover a replica from an injected, already-opened `log`, restoring the
    /// partition state machine from the log's recovered committed prefix
    /// (Requirement 11.1, 11.2, 11.3).
    ///
    /// [`RaftNode::recover`] restores `current_term`/`voted_for` from the log's
    /// hard state and initialises `commit_index` from the recovered log. The
    /// committed prefix `[0..=commit_index]` is then re-applied to a fresh
    /// [`StateMachine`] exactly once, in ascending index order, so committed
    /// records regain the same gap-free offsets they held before the restart.
    /// `read` clamps to the log's retained range, so it yields exactly the
    /// committed prefix. A fresh/empty log has no commit index, so this is
    /// identical to [`PartitionReplica::with_log`] (an empty state machine).
    pub fn recover(node_id: RaftNodeId, peers: Vec<RaftNodeId>, log: PartitionLog) -> Self {
        let raft = RaftNode::recover(node_id, peers, log);
        let mut state = StateMachine::new();
        if let Some(commit) = raft.commit_index() {
            let entries = raft.log().read(0, commit);
            state.apply_committed(&entries);
        }
        Self { raft, state }
    }

    /// Create a follower replica for a fresh partition Raft group backed by a
    /// new in-memory log, plus an empty [`StateMachine`].
    ///
    /// A thin shim over [`PartitionReplica::with_log`] with an
    /// [`InMemoryLog`]-backed [`PartitionLog`], retained so existing call sites
    /// and tests that do not select a backend compile unchanged.
    pub fn new(node_id: RaftNodeId, peers: Vec<RaftNodeId>) -> Self {
        Self::with_log(node_id, peers, PartitionLog::InMemory(InMemoryLog::new()))
    }

    /// Drive the replica one step with `input`, using `clock` for timing, and
    /// apply any newly committed entries to the state machine.
    ///
    /// Returns the [`RaftOutput`] the consensus core produced — the messages the
    /// caller must dispatch through its transport and any role change — after
    /// the committed entries have been folded into the state machine so offsets
    /// are assigned on commit (Requirement 4.7, 8.8). The caller still owns
    /// dispatching `output.sends`; this crate performs no I/O.
    pub fn step(&mut self, input: RaftInput, clock: &mut impl Clock) -> RaftOutput {
        let output = self.raft.step(input, clock);
        if !output.committed.is_empty() {
            self.state.apply_committed(&output.committed);
        }
        output
    }

    /// Shared, read-only access to the underlying Raft node.
    pub fn raft(&self) -> &RaftNode<PartitionLog> {
        &self.raft
    }

    /// Shared, read-only access to the partition state machine.
    pub fn state_machine(&self) -> &StateMachine {
        &self.state
    }

    /// The role this replica currently holds in its Raft group.
    pub fn role(&self) -> Role {
        self.raft.role()
    }

    /// The highest committed offset for this partition, or `None` if no record
    /// has committed yet.
    pub fn high_water_mark(&self) -> Option<Offset> {
        self.state.high_water_mark()
    }

    /// Read committed records from this partition starting at `start`, returning
    /// at most `max` (Requirement 5.1).
    pub fn read(&self, start: Offset, max: usize) -> Vec<CommittedRecord> {
        self.state.read(start, max)
    }
}

/// Errors raised by [`RaftGroupFleet`] lifecycle operations.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum FleetError {
    /// A Raft group already exists for this partition. Creating a second would
    /// violate "exactly one Raft group per partition" (Requirement 7.1).
    #[error("a raft group already exists for {topic}/{partition}")]
    GroupExists {
        /// The topic of the partition.
        topic: String,
        /// The partition index within the topic.
        partition: u32,
    },
    /// No Raft group exists for this partition (e.g. stopping one that was never
    /// created or already stopped).
    #[error("no raft group exists for {topic}/{partition}")]
    GroupNotFound {
        /// The topic of the partition.
        topic: String,
        /// The partition index within the topic.
        partition: u32,
    },
}

/// The fleet of per-partition Raft groups hosted on a node.
///
/// Keyed by `(topic, partition)` so that **at most one** [`PartitionReplica`]
/// exists per partition: [`RaftGroupFleet::create_group`] rejects a duplicate
/// key, which together with the map keying guarantees exactly one Raft group
/// per partition (Requirement 7.1). The create/stop lifecycle is what a topic
/// deletion drives — [`RaftGroupFleet::stop_group`] removes the replica, and
/// dropping it releases both the Raft group and its in-memory log together
/// (Requirement 3.2, 3.3).
#[derive(Default)]
pub struct RaftGroupFleet {
    /// The hosted replicas, one per `(topic, partition)` key.
    groups: HashMap<GroupKey, PartitionReplica>,
}

impl RaftGroupFleet {
    /// Create an empty fleet hosting no partition groups.
    pub fn new() -> Self {
        Self::default()
    }

    /// Instantiate the Raft group for one partition, identified by `key`, with
    /// this node's consensus identity `node_id` and its `peers`.
    ///
    /// Exactly one group may exist per partition (Requirement 7.1): if a group
    /// already exists for `key`, the request is rejected with
    /// [`FleetError::GroupExists`] and the fleet is left unchanged. On success
    /// the new replica is registered as a follower, ready to be driven via
    /// [`RaftGroupFleet::get_mut`].
    ///
    /// A thin shim over [`RaftGroupFleet::create_group_with_log`] that injects a
    /// fresh in-memory backend, retained so call sites that do not select a
    /// backend stay unchanged.
    pub fn create_group(
        &mut self,
        key: GroupKey,
        node_id: RaftNodeId,
        peers: Vec<RaftNodeId>,
    ) -> Result<(), FleetError> {
        self.create_group_with_log(
            key,
            node_id,
            peers,
            PartitionLog::InMemory(InMemoryLog::new()),
        )
    }

    /// Instantiate the Raft group for one partition over the injected `log`,
    /// building a fresh replica with [`PartitionReplica::with_log`].
    ///
    /// Like [`RaftGroupFleet::create_group`] it enforces exactly one group per
    /// partition (Requirement 7.1): a duplicate `key` is rejected with
    /// [`FleetError::GroupExists`] and the fleet is left unchanged. This is the
    /// path the server takes once it has selected and constructed the topic's
    /// backend (Requirement 5.2).
    pub fn create_group_with_log(
        &mut self,
        key: GroupKey,
        node_id: RaftNodeId,
        peers: Vec<RaftNodeId>,
        log: PartitionLog,
    ) -> Result<(), FleetError> {
        if self.groups.contains_key(&key) {
            let (topic, partition) = key;
            return Err(FleetError::GroupExists {
                topic,
                partition: partition.0,
            });
        }
        self.groups
            .insert(key, PartitionReplica::with_log(node_id, peers, log));
        Ok(())
    }

    /// Instantiate the Raft group for one partition by **recovering** it from
    /// the injected, already-opened `log`, building the replica with
    /// [`PartitionReplica::recover`].
    ///
    /// Identical lifecycle guarantees to [`RaftGroupFleet::create_group_with_log`]
    /// — a duplicate `key` is rejected with [`FleetError::GroupExists`] and the
    /// fleet is left unchanged (Requirement 7.1) — but the replica restores its
    /// Raft hard state and re-applies the recovered log's committed prefix to
    /// its state machine, so a durable partition reopened after a restart
    /// regains its committed records at their original offsets (Requirement
    /// 11.1, 11.2, 11.3).
    pub fn create_recovered_group(
        &mut self,
        key: GroupKey,
        node_id: RaftNodeId,
        peers: Vec<RaftNodeId>,
        log: PartitionLog,
    ) -> Result<(), FleetError> {
        if self.groups.contains_key(&key) {
            let (topic, partition) = key;
            return Err(FleetError::GroupExists {
                topic,
                partition: partition.0,
            });
        }
        self.groups
            .insert(key, PartitionReplica::recover(node_id, peers, log));
        Ok(())
    }

    /// Stop and release the Raft group for the partition identified by `key`.
    ///
    /// Removing the [`PartitionReplica`] from the fleet drops it, releasing the
    /// Raft group and its in-memory log together — the teardown a topic deletion
    /// performs for each partition (Requirement 3.2, 3.3). Rejected with
    /// [`FleetError::GroupNotFound`] if no group exists for `key`.
    pub fn stop_group(&mut self, key: &GroupKey) -> Result<(), FleetError> {
        if self.groups.remove(key).is_some() {
            Ok(())
        } else {
            let (topic, partition) = key;
            Err(FleetError::GroupNotFound {
                topic: topic.clone(),
                partition: partition.0,
            })
        }
    }

    /// Whether a Raft group exists for the partition identified by `key`.
    pub fn contains(&self, key: &GroupKey) -> bool {
        self.groups.contains_key(key)
    }

    /// The total number of Raft groups hosted. Because the fleet is keyed by
    /// `(topic, partition)` and creation rejects duplicates, this also equals
    /// the number of distinct partitions hosted — exactly one group each
    /// (Requirement 7.1).
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Whether the fleet hosts no Raft groups.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Shared, read-only access to the replica for `key`, if hosted.
    pub fn get(&self, key: &GroupKey) -> Option<&PartitionReplica> {
        self.groups.get(key)
    }

    /// Mutable access to the replica for `key`, if hosted, so the caller can
    /// drive it with [`PartitionReplica::step`].
    pub fn get_mut(&mut self, key: &GroupKey) -> Option<&mut PartitionReplica> {
        self.groups.get_mut(key)
    }

    /// Iterate the keys of every hosted Raft group.
    pub fn keys(&self) -> impl Iterator<Item = &GroupKey> {
        self.groups.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use vela_log::EntryPayload;
    use vela_raft::TimerKind;

    /// A minimal [`Clock`] for driving a replica deterministically in tests.
    ///
    /// Time never advances on its own and arming a timer is a no-op: these tests
    /// drive consensus by feeding explicit [`RaftInput`]s, so no real timing is
    /// needed.
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

    fn record(bytes: &[u8]) -> LogEntry {
        LogEntry {
            index: 0,
            term: 1,
            payload: EntryPayload::new(PayloadKind::Record, bytes.to_vec()),
        }
    }

    fn noop() -> LogEntry {
        LogEntry {
            index: 0,
            term: 1,
            payload: EntryPayload::new(PayloadKind::Noop, Vec::new()),
        }
    }

    #[test]
    fn state_machine_assigns_gap_free_zero_based_offsets() {
        let mut sm = StateMachine::new();
        assert!(sm.is_empty());
        assert_eq!(sm.next_offset(), 0);
        assert_eq!(sm.high_water_mark(), None);

        // Each committed record takes the next contiguous 0-based offset.
        assert_eq!(sm.apply(&record(b"a")), Some(0));
        assert_eq!(sm.apply(&record(b"b")), Some(1));
        assert_eq!(sm.apply(&record(b"c")), Some(2));

        assert_eq!(sm.len(), 3);
        assert_eq!(sm.next_offset(), 3);
        assert_eq!(sm.high_water_mark(), Some(2));
    }

    #[test]
    fn state_machine_skips_non_record_payloads_without_consuming_an_offset() {
        let mut sm = StateMachine::new();
        // A Noop entry is applied but assigns no offset, so the surrounding
        // record offsets stay contiguous (0, 1) with no gap.
        assert_eq!(sm.apply(&record(b"first")), Some(0));
        assert_eq!(sm.apply(&noop()), None);
        assert_eq!(sm.apply(&record(b"second")), Some(1));

        assert_eq!(sm.len(), 2);
        assert_eq!(sm.high_water_mark(), Some(1));
        let read = sm.read(0, 10);
        assert_eq!(
            read,
            vec![
                CommittedRecord {
                    offset: 0,
                    value: b"first".to_vec()
                },
                CommittedRecord {
                    offset: 1,
                    value: b"second".to_vec()
                },
            ]
        );
    }

    #[test]
    fn state_machine_read_respects_start_offset_and_max_count() {
        let mut sm = StateMachine::new();
        for i in 0..5u8 {
            sm.apply(&record(&[i]));
        }

        // From a mid-stream offset, bounded by max.
        let got = sm.read(1, 2);
        assert_eq!(got.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(got[0].value, vec![1]);

        // max of 0 returns nothing.
        assert!(sm.read(0, 0).is_empty());
        // A start beyond the highest committed offset returns empty, not error.
        assert!(sm.read(5, 10).is_empty());
        // max larger than what remains is clamped to the available records.
        assert_eq!(sm.read(3, 100).len(), 2);
    }

    #[test]
    fn partition_replica_assigns_offsets_when_entries_commit() {
        // A single-node group: the lone replica is its own majority, so an
        // election makes it leader and a proposal commits immediately.
        let mut replica = PartitionReplica::new(RaftNodeId(0), Vec::new());
        let mut clock = TestClock::new();

        // Drive an election: with no peers the self-vote is a majority.
        replica.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        assert_eq!(replica.role(), Role::Leader);
        // Nothing committed yet, so no offsets assigned.
        assert_eq!(replica.high_water_mark(), None);

        // Propose two records; each commits and is assigned the next offset.
        let out = replica.step(
            RaftInput::Propose(EntryPayload::new(PayloadKind::Record, b"one".to_vec())),
            &mut clock,
        );
        assert_eq!(out.committed.len(), 1);
        replica.step(
            RaftInput::Propose(EntryPayload::new(PayloadKind::Record, b"two".to_vec())),
            &mut clock,
        );

        assert_eq!(replica.high_water_mark(), Some(1));
        let records = replica.read(0, 10);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value, b"one".to_vec());
        assert_eq!(records[1].offset, 1);
        assert_eq!(records[1].value, b"two".to_vec());
    }

    #[test]
    fn fleet_create_and_stop_lifecycle() {
        let mut fleet = RaftGroupFleet::new();
        assert!(fleet.is_empty());

        let key = ("orders".to_string(), PartitionIndex(0));
        fleet
            .create_group(
                key.clone(),
                RaftNodeId(0),
                vec![RaftNodeId(1), RaftNodeId(2)],
            )
            .unwrap();

        assert!(fleet.contains(&key));
        assert_eq!(fleet.group_count(), 1);
        assert!(fleet.get(&key).is_some());

        // Stopping removes the group, releasing the replica (and its log).
        fleet.stop_group(&key).unwrap();
        assert!(!fleet.contains(&key));
        assert_eq!(fleet.group_count(), 0);

        // Stopping a group that does not exist is rejected.
        assert_eq!(
            fleet.stop_group(&key),
            Err(FleetError::GroupNotFound {
                topic: "orders".to_string(),
                partition: 0,
            })
        );
    }

    #[test]
    fn fleet_rejects_a_second_group_for_the_same_partition() {
        let mut fleet = RaftGroupFleet::new();
        let key = ("orders".to_string(), PartitionIndex(3));

        fleet
            .create_group(key.clone(), RaftNodeId(0), Vec::new())
            .unwrap();
        // A duplicate create for the same partition is rejected, preserving the
        // "exactly one Raft group per partition" invariant (Requirement 7.1).
        assert_eq!(
            fleet.create_group(key.clone(), RaftNodeId(1), Vec::new()),
            Err(FleetError::GroupExists {
                topic: "orders".to_string(),
                partition: 3,
            })
        );
        assert_eq!(fleet.group_count(), 1);
    }

    #[test]
    fn fleet_hosts_exactly_one_group_per_partition() {
        let mut fleet = RaftGroupFleet::new();
        let partition_count = 6u32;

        // One Raft group per partition of the topic (Requirement 7.1).
        for p in 0..partition_count {
            fleet
                .create_group(
                    ("orders".to_string(), PartitionIndex(p)),
                    RaftNodeId(u64::from(p)),
                    Vec::new(),
                )
                .unwrap();
        }

        assert_eq!(fleet.group_count() as u32, partition_count);
        for p in 0..partition_count {
            assert!(fleet.contains(&("orders".to_string(), PartitionIndex(p))));
        }

        // Distinct topics with the same partition index are distinct groups.
        fleet
            .create_group(
                ("events".to_string(), PartitionIndex(0)),
                RaftNodeId(9),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(fleet.group_count() as u32, partition_count + 1);
    }
}
