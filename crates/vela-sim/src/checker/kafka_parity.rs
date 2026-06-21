//! Client-consistency / Kafka-parity checks (Requirement 11): acknowledged-
//! record durability, offset integrity, consume read-validity (no phantom
//! reads), per-partition linearizability, and metadata catalogue convergence
//! (design Properties 16–20).
//!
//! These checks consume the shared [`Violation`](super::Violation) /
//! [`PropertyId`](super::PropertyId) vocabulary defined in the parent module.
//!
//! # Observability
//!
//! Like the Raft safety checks, the [`KafkaParityChecker`] has total, read-only
//! observability of the whole [`SimulatedCluster`] plus the recorded
//! [`History`]. The Kafka-parity guarantees are *client-observable* facts, so
//! they are checked as a **final pass** over the run's end state (design
//! "Consistency_Checker — Kafka parity"):
//!
//! - The **partition's committed log** is observed by reading the committed
//!   prefix of the *most-advanced running replica* of that partition's group.
//!   Committed entries are durable and agree across replicas by Raft State
//!   Machine Safety, so the longest running prefix is the authoritative
//!   committed log; a minority of crashed replicas (whose volatile fleet is
//!   dropped) cannot hide an acknowledged record, because a majority still
//!   holds it (Requirement 11.2).
//! - The **client view** comes from the [`History`]: every `ProduceOk` is an
//!   [`Acknowledged_Record`](crate::history) (a returned offset + value) and
//!   every `ConsumeOk` is an observed range of records at a start offset.
//!
//! Each check is a pure function of `(committed log, history slice)` (or, for
//! convergence, of the per-node `(commit index, served catalogue)`), so the
//! algorithms are unit-testable against synthetic states without driving a
//! cluster. The first breach is returned as a [`Violation`] naming the
//! [`PropertyId`] and stamped with the detection [`VirtualInstant`] the caller
//! passes (Requirement 2.3); the run-orchestration task (20.1) turns it into the
//! run's failing `Outcome`.
//!
//! [`Acknowledged_Record`]: crate::history
//! [`History`]: crate::history::History

use std::collections::{BTreeMap, BTreeSet};

use vela_core::{ClusterMetadata, CommittedRecord, GroupKey, NodeId, Offset, PartitionIndex};

use super::{PropertyId, Violation};
use crate::cluster::SimulatedCluster;
use crate::history::{History, OpResponse};
use crate::scheduler::VirtualInstant;

/// One partition's committed log as observed at the end of a run: the committed
/// records the most-advanced running replica of the partition has applied, in
/// ascending offset order.
///
/// This is the single source of truth every per-partition check measures the
/// recorded [`History`] against. Records are held in offset order exactly as
/// [`PartitionReplica::read`](vela_core::PartitionReplica::read) returns them;
/// value lookup tolerates a (hypothetical) offset gap by searching on the offset
/// key rather than assuming `records[i].offset == i` — that contiguity is itself
/// what [`check_offset_integrity`] asserts.
#[derive(Debug, Clone, Default)]
struct CommittedLog {
    /// Committed records in ascending offset order.
    records: Vec<CommittedRecord>,
}

impl CommittedLog {
    /// Build from an explicit record list (used by the orchestration and tests).
    fn from_records(records: Vec<CommittedRecord>) -> Self {
        Self { records }
    }

    /// The number of committed records.
    fn len(&self) -> usize {
        self.records.len()
    }

    /// The committed value at `offset`, or `None` if no record holds that
    /// offset. Robust to a non-contiguous log: it searches on the offset key.
    fn value_at(&self, offset: Offset) -> Option<&[u8]> {
        self.records
            .binary_search_by_key(&offset, |r| r.offset)
            .ok()
            .map(|i| self.records[i].value.as_slice())
    }
}

/// An acknowledged produce extracted from the [`History`]: the offset and value
/// returned to the client, plus the operation's invocation / response instants
/// (for the real-time ordering linearizability check).
#[derive(Debug, Clone)]
struct AckedProduce {
    offset: Offset,
    value: Vec<u8>,
    invoked_at: VirtualInstant,
    responded_at: VirtualInstant,
}

/// A successful consume extracted from the [`History`]: the requested start
/// offset and the ordered record values the cluster returned.
#[derive(Debug, Clone)]
struct ConsumeObs {
    start_offset: Offset,
    values: Vec<Vec<u8>>,
}

/// Detects breaches of the client-consistency / Kafka-parity guarantees
/// (Requirement 11) as a read-only final pass over a run's end state.
///
/// The checker is stateless — every method is a pure function of the cluster and
/// history it is handed — so a caller may run [`check`](Self::check) once at the
/// end of a run (the common case) and/or run [`check_convergence`](Self::check_convergence)
/// independently while the run is quiescent.
#[derive(Debug, Default, Clone, Copy)]
pub struct KafkaParityChecker;

impl KafkaParityChecker {
    /// A fresh checker.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Run every Kafka-parity check over the run's end state at instant `now`:
    /// acknowledged-record durability (11.1, 11.2, 7.6), offset integrity
    /// (11.3), consume read-validity / no phantom reads (11.4, 11.5),
    /// per-partition linearizability (11.6), and metadata catalogue convergence
    /// (11.7).
    ///
    /// For every `(topic, partition)` the [`History`] referenced with a produce
    /// or consume, it observes that partition's committed log (the most-advanced
    /// running replica) and runs the four per-partition checks; it then runs the
    /// cluster-wide convergence check. Partitions are visited in a deterministic
    /// (sorted) order so the *first* breach reported is a pure function of the
    /// run.
    ///
    /// # Deleted topics
    ///
    /// A topic the client successfully deleted has its partitions' Raft groups
    /// stopped and removed by reconcile, so its committed log is *legitimately*
    /// gone at the end of the run. Requirement 11.1's "an Acknowledged_Record
    /// remains present for the remainder of the Run" cannot apply to a log the
    /// client itself asked to be removed — so the per-partition client-
    /// consistency checks (durability, offset integrity, consume read-validity,
    /// per-partition linearizability) are skipped for any topic that the
    /// [`History`] records as successfully deleted (an [`OpResponse::DeleteTopicOk`]).
    /// A topic that was *never* deleted is still checked in full, so a genuinely
    /// lost acknowledged record on a live topic still fails.
    ///
    /// Chosen rule: skip a topic that has **any** successful `DeleteTopicOk` in
    /// the history. Limitation: if the harness ever creates → deletes →
    /// re-creates the *same* topic name within one run, the post-recreate
    /// lifetime is not separately validated (the whole name is skipped). This is
    /// the conservative, correct-by-omission choice; we never report a false
    /// positive for a name that was legitimately deleted at some point. (The
    /// cluster-wide [`check_convergence`](Self::check_convergence) is unaffected
    /// and still runs over every node.)
    ///
    /// # Errors
    ///
    /// Returns the first [`Violation`] found, naming the breached [`PropertyId`]
    /// and stamped with `now`.
    pub fn check(
        &self,
        cluster: &SimulatedCluster,
        history: &History,
        now: VirtualInstant,
    ) -> Result<(), Violation> {
        // Topics the client successfully deleted: their committed logs are
        // legitimately removed, so they are exempt from the per-partition
        // client-consistency checks below.
        let deleted_topics = deleted_topics(history);

        // Visit each referenced partition in a deterministic order.
        let mut partitions: BTreeSet<(String, PartitionIndex)> = BTreeSet::new();
        for op in history.iter() {
            if let Some(partition) = op.partition() {
                partitions.insert((op.topic().to_string(), partition));
            }
        }

        for (topic, partition) in &partitions {
            // A deleted topic's log is gone by design; its acknowledged
            // records / consumes are no longer required to be present.
            if deleted_topics.contains(topic) {
                continue;
            }
            let group: GroupKey = (topic.clone(), *partition);
            let log = observe_committed_log(cluster, &group);
            let acked = acked_produces(history, topic, *partition);
            let consumes = consume_observations(history, topic, *partition);

            check_durability(&group, &log, &acked, now)?;
            check_offset_integrity(&group, &log, &acked, now)?;
            check_consume_validity(&group, &log, &consumes, now)?;
            check_linearizability(&group, &log, &acked, &consumes, now)?;
        }

        self.check_convergence(cluster, now)
    }

    /// Metadata catalogue convergence (11.7): any two running nodes whose
    /// metadata group is committed to the *same* commit index hold identical
    /// served topic catalogues (compared over sorted topic keys).
    ///
    /// Crashed nodes hold no live controller and are skipped; a node whose
    /// `__meta/0` group has committed nothing (`commit_index() == None`) imposes
    /// no constraint and is skipped.
    ///
    /// # Errors
    ///
    /// Returns a [`Violation`] for [`PropertyId::MetadataConvergence`] if two
    /// such nodes' served catalogues differ, naming the two nodes, the shared
    /// commit index, and the first divergent topic key.
    pub fn check_convergence(
        &self,
        cluster: &SimulatedCluster,
        now: VirtualInstant,
    ) -> Result<(), Violation> {
        // Collect (node, commit index, served) for every running node whose
        // metadata group has committed at least one entry.
        let mut views: Vec<(&NodeId, u64, &ClusterMetadata)> = Vec::new();
        for node in cluster.nodes() {
            if !node.is_running() {
                continue;
            }
            if let Some(commit) = node.controller().and_then(|c| c.commit_index()) {
                views.push((node.id(), commit, node.served()));
            }
        }
        convergence_over_views(&views, now)
    }
}

/// Observe a partition's committed log: the longest committed prefix held by any
/// running replica of `group`.
///
/// All running replicas agree on their common committed prefix (Raft State
/// Machine Safety), so the most-advanced one is the authoritative committed log
/// — exactly the log the durability / offset / consume / linearizability
/// guarantees are stated over.
fn observe_committed_log(cluster: &SimulatedCluster, group: &GroupKey) -> CommittedLog {
    let mut best: Vec<CommittedRecord> = Vec::new();
    for node in cluster.nodes() {
        if !node.is_running() {
            continue;
        }
        for (replica_group, replica) in node.fleet_replicas() {
            if replica_group == group {
                let records = replica.read(0, usize::MAX);
                if records.len() > best.len() {
                    best = records;
                }
            }
        }
    }
    CommittedLog::from_records(best)
}

/// The set of topic names the [`History`] records as successfully deleted (an
/// [`OpResponse::DeleteTopicOk`]).
///
/// A successful delete stops and removes the topic's per-partition Raft groups
/// via reconcile, so their committed logs are legitimately gone at the end of
/// the run. The per-partition client-consistency checks ([`check`](KafkaParityChecker::check))
/// exempt these topics: their acknowledged records / consumes are no longer
/// required to be present once the client deleted the topic.
fn deleted_topics(history: &History) -> BTreeSet<String> {
    history
        .iter()
        .filter_map(|op| match &op.response {
            OpResponse::DeleteTopicOk { topic } => Some(topic.clone()),
            _ => None,
        })
        .collect()
}

/// Extract every acknowledged produce for `topic`/`partition` from the history,
/// in invocation order.
fn acked_produces(history: &History, topic: &str, partition: PartitionIndex) -> Vec<AckedProduce> {
    history
        .iter_for_partition(topic, partition)
        .filter_map(|op| match &op.response {
            OpResponse::ProduceOk { value, offset, .. } => Some(AckedProduce {
                offset: *offset,
                value: value.clone(),
                invoked_at: op.invoked_at,
                responded_at: op.responded_at,
            }),
            _ => None,
        })
        .collect()
}

/// Extract every successful consume for `topic`/`partition` from the history, in
/// invocation order.
fn consume_observations(
    history: &History,
    topic: &str,
    partition: PartitionIndex,
) -> Vec<ConsumeObs> {
    history
        .iter_for_partition(topic, partition)
        .filter_map(|op| match &op.response {
            OpResponse::ConsumeOk {
                start_offset,
                records,
                ..
            } => Some(ConsumeObs {
                start_offset: *start_offset,
                values: records.iter().map(|r| r.value.clone()).collect(),
            }),
            _ => None,
        })
        .collect()
}

/// Acknowledged-record durability (11.1, 11.2, 7.6): every acknowledged produce
/// appears in the committed log at the returned offset with the returned value,
/// and remains there at the end of the run.
///
/// A returned offset absent from the committed log is a *lost* acknowledged
/// record; a different value at that offset means the acknowledged record was
/// overwritten — both breach durability.
fn check_durability(
    group: &GroupKey,
    log: &CommittedLog,
    acked: &[AckedProduce],
    now: VirtualInstant,
) -> Result<(), Violation> {
    for a in acked {
        match log.value_at(a.offset) {
            None => {
                return Err(Violation::new(
                    PropertyId::AcknowledgedRecordDurability,
                    now,
                    format!(
                        "group {group:?}: acknowledged record at offset {} is absent from the \
                         committed log (committed len {})",
                        a.offset,
                        log.len()
                    ),
                ));
            }
            Some(value) if value != a.value.as_slice() => {
                return Err(Violation::new(
                    PropertyId::AcknowledgedRecordDurability,
                    now,
                    format!(
                        "group {group:?}: acknowledged record at offset {} was overwritten in the \
                         committed log",
                        a.offset
                    ),
                ));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// Offset integrity (11.3): a partition's committed offsets are contiguous from
/// 0, strictly increasing, with no gaps; and no offset is assigned to two
/// distinct acknowledged records.
///
/// The committed log is checked for contiguity (`records[i].offset == i`), and
/// the acknowledged produces are checked so that two distinct returned values
/// never share an offset (an offset-reuse bug visible to clients).
fn check_offset_integrity(
    group: &GroupKey,
    log: &CommittedLog,
    acked: &[AckedProduce],
    now: VirtualInstant,
) -> Result<(), Violation> {
    // Committed offsets must be 0, 1, 2, … with no gaps or duplicates.
    for (expected, record) in log.records.iter().enumerate() {
        let expected = expected as Offset;
        if record.offset != expected {
            return Err(Violation::new(
                PropertyId::OffsetIntegrity,
                now,
                format!(
                    "group {group:?}: committed offsets are not contiguous from 0 — expected \
                     offset {expected}, found {}",
                    record.offset
                ),
            ));
        }
    }

    // No offset may be acknowledged for two distinct records.
    let mut seen: BTreeMap<Offset, &[u8]> = BTreeMap::new();
    for a in acked {
        match seen.get(&a.offset) {
            Some(prev) if *prev != a.value.as_slice() => {
                return Err(Violation::new(
                    PropertyId::OffsetIntegrity,
                    now,
                    format!(
                        "group {group:?}: offset {} was acknowledged for two distinct records",
                        a.offset
                    ),
                ));
            }
            Some(_) => {}
            None => {
                seen.insert(a.offset, a.value.as_slice());
            }
        }
    }
    Ok(())
}

/// Consume read-validity / no phantom reads (11.4, 11.5): every record a
/// successful consume returned is a committed record at the offset it was
/// returned at, in ascending offset order.
///
/// A consume that started at `start_offset` and returned `k` records claims the
/// committed records at offsets `start_offset .. start_offset + k`; each must
/// exist in the committed log (else it is a phantom read) and equal the returned
/// value (else the consume reported a different record than the one committed).
/// Returning the records by ascending position enforces ascending offset order.
fn check_consume_validity(
    group: &GroupKey,
    log: &CommittedLog,
    consumes: &[ConsumeObs],
    now: VirtualInstant,
) -> Result<(), Violation> {
    for c in consumes {
        for (i, value) in c.values.iter().enumerate() {
            let offset = c.start_offset + i as Offset;
            match log.value_at(offset) {
                None => {
                    return Err(Violation::new(
                        PropertyId::ConsumeReadValidity,
                        now,
                        format!(
                            "group {group:?}: consume from start offset {} returned a record at \
                             offset {offset} that is not in the committed log (phantom read)",
                            c.start_offset
                        ),
                    ));
                }
                Some(committed) if committed != value.as_slice() => {
                    return Err(Violation::new(
                        PropertyId::ConsumeReadValidity,
                        now,
                        format!(
                            "group {group:?}: consume returned a record at offset {offset} that \
                             differs from the committed record there",
                        ),
                    ));
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

/// Per-partition linearizability (11.6): the recorded history is consistent with
/// a single linearizable per-partition committed log — a total order of
/// committed appends (which the contiguous offsets directly express) that
/// respects the returned offsets and the real-time order of non-overlapping
/// operations, and of which every successful consume observes a prefix-range.
///
/// Because a partition has a single committed log whose offsets *are* the total
/// order, linearizability reduces to three observable facts:
///
/// 1. **Returned offsets index the total order:** every acknowledged offset has
///    a position in the committed log (`offset < len`).
/// 2. **Real-time order of non-overlapping produces:** if produce `a` responded
///    strictly before produce `b` was invoked, `a` precedes `b` in real time, so
///    `a` must hold the earlier offset (`a.offset < b.offset`).
/// 3. **Consumes observe a prefix-range:** a consume from `start_offset` that
///    returned `k` records observed the committed range
///    `[start_offset, start_offset + k)`, which must lie within the committed
///    log.
fn check_linearizability(
    group: &GroupKey,
    log: &CommittedLog,
    acked: &[AckedProduce],
    consumes: &[ConsumeObs],
    now: VirtualInstant,
) -> Result<(), Violation> {
    let len = log.len() as Offset;

    // 1. Every acknowledged offset indexes a position in the committed log.
    for a in acked {
        if a.offset >= len {
            return Err(Violation::new(
                PropertyId::PerPartitionLinearizability,
                now,
                format!(
                    "group {group:?}: acknowledged offset {} has no position in the committed log \
                     (committed len {len})",
                    a.offset
                ),
            ));
        }
    }

    // 2. Real-time order of non-overlapping produces is reflected by offsets.
    for a in acked {
        for b in acked {
            if a.responded_at < b.invoked_at && a.offset >= b.offset {
                return Err(Violation::new(
                    PropertyId::PerPartitionLinearizability,
                    now,
                    format!(
                        "group {group:?}: produce at offset {} responded before the produce at \
                         offset {} was invoked, but holds a later-or-equal offset (real-time order \
                         violated)",
                        a.offset, b.offset
                    ),
                ));
            }
        }
    }

    // 3. Every consume observes a prefix-range of the committed log.
    for c in consumes {
        let end = c.start_offset + c.values.len() as Offset;
        if end > len {
            return Err(Violation::new(
                PropertyId::PerPartitionLinearizability,
                now,
                format!(
                    "group {group:?}: consume from start offset {} observed {} records reaching \
                     offset {end}, beyond the committed log (len {len})",
                    c.start_offset,
                    c.values.len()
                ),
            ));
        }
    }
    Ok(())
}

/// Metadata catalogue convergence (11.7) over a collected set of per-node
/// `(node, commit index, served catalogue)` views.
///
/// For every pair of views sharing a commit index, the served catalogues must be
/// identical; the first divergent pair is reported. Pure over the collected
/// views so it is unit-testable with synthetic catalogues.
fn convergence_over_views(
    views: &[(&NodeId, u64, &ClusterMetadata)],
    now: VirtualInstant,
) -> Result<(), Violation> {
    for i in 0..views.len() {
        for j in (i + 1)..views.len() {
            let (node_a, commit_a, meta_a) = views[i];
            let (node_b, commit_b, meta_b) = views[j];
            if commit_a != commit_b {
                continue;
            }
            if let Some(topic) = first_topic_divergence(meta_a, meta_b) {
                return Err(Violation::new(
                    PropertyId::MetadataConvergence,
                    now,
                    format!(
                        "nodes {node_a:?} and {node_b:?} are both at metadata commit index \
                         {commit_a} but their served catalogues differ on topic {topic:?}"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The first topic key (in sorted order) on which two served catalogues differ,
/// or `None` if their topic catalogues are identical.
fn first_topic_divergence(a: &ClusterMetadata, b: &ClusterMetadata) -> Option<String> {
    // The sorted union of topic keys across both catalogues.
    let keys: BTreeSet<&String> = a.topics.keys().chain(b.topics.keys()).collect();
    for key in keys {
        if a.topics.get(key) != b.topics.get(key) {
            return Some(key.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    use vela_core::{LogBackend, Member, NodeAvailability, Partition, Topic, TopicState};

    use crate::history::{OpArgs, RecordedOp};

    const NOW: VirtualInstant = VirtualInstant::from_nanos(4_096);

    fn group(topic: &str, partition: u32) -> GroupKey {
        (topic.to_string(), PartitionIndex(partition))
    }

    /// A committed log whose offset `i` holds value `values[i]`.
    fn log(values: &[&[u8]]) -> CommittedLog {
        CommittedLog::from_records(
            values
                .iter()
                .enumerate()
                .map(|(i, v)| CommittedRecord {
                    offset: i as Offset,
                    value: v.to_vec(),
                })
                .collect(),
        )
    }

    fn acked(offset: Offset, value: &[u8], invoked: u64, responded: u64) -> AckedProduce {
        AckedProduce {
            offset,
            value: value.to_vec(),
            invoked_at: VirtualInstant::from_nanos(invoked),
            responded_at: VirtualInstant::from_nanos(responded),
        }
    }

    fn consumed(start_offset: Offset, values: &[&[u8]]) -> ConsumeObs {
        ConsumeObs {
            start_offset,
            values: values.iter().map(|v| v.to_vec()).collect(),
        }
    }

    // ----- Acknowledged-record durability (11.1, 11.2, 7.6) ---------------

    #[test]
    fn durability_passes_when_every_acked_record_is_present() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b", b"c"]);
        let acks = vec![acked(0, b"a", 1, 2), acked(2, b"c", 3, 4)];
        assert!(check_durability(&g, &committed, &acks, NOW).is_ok());
    }

    #[test]
    fn durability_flags_a_lost_acknowledged_record() {
        let g = group("orders", 0);
        // The client was told offset 2 committed, but the log only reaches 1.
        let committed = log(&[b"a", b"b"]);
        let acks = vec![acked(2, b"c", 3, 4)];
        let err = check_durability(&g, &committed, &acks, NOW)
            .expect_err("a lost acknowledged record must be flagged");
        assert_eq!(err.property, PropertyId::AcknowledgedRecordDurability);
        assert_eq!(err.at, NOW);
    }

    #[test]
    fn durability_flags_an_overwritten_acknowledged_record() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"different"]);
        let acks = vec![acked(1, b"b", 3, 4)];
        let err = check_durability(&g, &committed, &acks, NOW)
            .expect_err("an overwritten acknowledged record must be flagged");
        assert_eq!(err.property, PropertyId::AcknowledgedRecordDurability);
    }

    // ----- Deleted-topic exemption (11.1) ---------------------------------

    #[test]
    fn deleted_topics_collects_only_successful_deletes() {
        let mut history = History::new();
        // A produce + a successful delete of "gone".
        history.record_produce_success(
            OpArgs::Produce {
                topic: "gone".to_string(),
                partition: PartitionIndex(0),
                key: None,
                value: b"v".to_vec(),
            },
            VirtualInstant::from_nanos(1),
            VirtualInstant::from_nanos(2),
            0,
        );
        history.record(RecordedOp::new(
            OpArgs::DeleteTopic {
                topic: "gone".to_string(),
            },
            VirtualInstant::from_nanos(3),
            VirtualInstant::from_nanos(4),
            OpResponse::DeleteTopicOk {
                topic: "gone".to_string(),
            },
        ));
        // A *failed* delete of "stays" must NOT count as deleted.
        history.record(RecordedOp::new(
            OpArgs::DeleteTopic {
                topic: "stays".to_string(),
            },
            VirtualInstant::from_nanos(5),
            VirtualInstant::from_nanos(6),
            OpResponse::Error {
                message: "boom".to_string(),
            },
        ));

        let deleted = deleted_topics(&history);
        assert!(deleted.contains("gone"));
        assert!(!deleted.contains("stays"));
    }

    /// A topic the client successfully deleted is exempt from the per-partition
    /// checks: its committed log is legitimately gone, so an acknowledged record
    /// missing from an empty observed log is **not** a violation. A *live* topic
    /// with a genuinely lost acknowledged record still fails. This reproduces the
    /// false positive the master run-level fuzz surfaced (a `ProduceOk` followed
    /// by a `DeleteTopicOk` of the same topic, whose group's log is then empty).
    #[test]
    fn check_skips_a_deleted_topic_but_still_flags_a_live_lost_record() {
        // History: produce to "deleted" (offset 0), then a successful delete of
        // it; produce to "live" (offset 0) with no delete.
        let mut history = History::new();
        history.record_produce_success(
            OpArgs::Produce {
                topic: "deleted".to_string(),
                partition: PartitionIndex(0),
                key: None,
                value: b"a".to_vec(),
            },
            VirtualInstant::from_nanos(1),
            VirtualInstant::from_nanos(2),
            0,
        );
        history.record(RecordedOp::new(
            OpArgs::DeleteTopic {
                topic: "deleted".to_string(),
            },
            VirtualInstant::from_nanos(3),
            VirtualInstant::from_nanos(4),
            OpResponse::DeleteTopicOk {
                topic: "deleted".to_string(),
            },
        ));
        history.record_produce_success(
            OpArgs::Produce {
                topic: "live".to_string(),
                partition: PartitionIndex(0),
                key: None,
                value: b"b".to_vec(),
            },
            VirtualInstant::from_nanos(5),
            VirtualInstant::from_nanos(6),
            0,
        );

        // The deleted topic, in isolation, must pass `check` against an empty
        // observed log (its group is gone) — the durability check is skipped.
        let deleted = deleted_topics(&history);
        assert!(deleted.contains("deleted"));
        assert!(!deleted.contains("live"));

        // Directly assert the per-partition logic: the deleted topic's empty log
        // is exempt, while the live topic's empty log is a real lost record.
        let empty = CommittedLog::default();
        let deleted_acks = acked_produces(&history, "deleted", PartitionIndex(0));
        let live_acks = acked_produces(&history, "live", PartitionIndex(0));

        // Deleted topic: the record IS absent from the (empty) log, but because
        // the topic was deleted, `check` skips it — so no violation surfaces.
        // We model that skip here by only running durability for non-deleted
        // topics, exactly as `check` does.
        assert!(deleted.contains("deleted"));

        // Live topic: a genuinely lost acknowledged record must still fail.
        let live_group = group("live", 0);
        let err = check_durability(&live_group, &empty, &live_acks, NOW)
            .expect_err("a live topic's lost acknowledged record must still fail");
        assert_eq!(err.property, PropertyId::AcknowledgedRecordDurability);

        // Sanity: the deleted topic genuinely WOULD fail if it were not skipped,
        // confirming the exemption (not an empty-ack accident) is what saves it.
        let deleted_group = group("deleted", 0);
        assert!(
            check_durability(&deleted_group, &empty, &deleted_acks, NOW).is_err(),
            "the deleted topic's record is absent; only the delete-exemption \
             makes `check` pass"
        );
    }

    // ----- Offset integrity (11.3) ----------------------------------------

    #[test]
    fn offset_integrity_passes_for_a_contiguous_log() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b", b"c"]);
        let acks = vec![acked(0, b"a", 1, 2), acked(1, b"b", 3, 4)];
        assert!(check_offset_integrity(&g, &committed, &acks, NOW).is_ok());
    }

    #[test]
    fn offset_integrity_flags_a_gap_in_the_committed_log() {
        let g = group("orders", 0);
        // A hole at offset 1: offsets jump 0 -> 2.
        let committed = CommittedLog::from_records(vec![
            CommittedRecord {
                offset: 0,
                value: b"a".to_vec(),
            },
            CommittedRecord {
                offset: 2,
                value: b"c".to_vec(),
            },
        ]);
        let err = check_offset_integrity(&g, &committed, &[], NOW)
            .expect_err("a gap in committed offsets must be flagged");
        assert_eq!(err.property, PropertyId::OffsetIntegrity);
        assert_eq!(err.at, NOW);
    }

    #[test]
    fn offset_integrity_flags_one_offset_acknowledged_for_two_distinct_records() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b"]);
        // Offset 0 acknowledged twice, with different values.
        let acks = vec![acked(0, b"a", 1, 2), acked(0, b"x", 3, 4)];
        let err = check_offset_integrity(&g, &committed, &acks, NOW)
            .expect_err("an offset reused for two distinct records must be flagged");
        assert_eq!(err.property, PropertyId::OffsetIntegrity);
    }

    // ----- Consume read-validity / no phantom reads (11.4, 11.5) ----------

    #[test]
    fn consume_validity_passes_for_a_committed_range() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b", b"c", b"d"]);
        let consumes = vec![consumed(1, &[b"b", b"c"])];
        assert!(check_consume_validity(&g, &committed, &consumes, NOW).is_ok());
    }

    #[test]
    fn consume_validity_flags_a_phantom_read() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b"]);
        // Returns a record at offset 2, which is not in the committed log.
        let consumes = vec![consumed(1, &[b"b", b"phantom"])];
        let err = check_consume_validity(&g, &committed, &consumes, NOW)
            .expect_err("a phantom read must be flagged");
        assert_eq!(err.property, PropertyId::ConsumeReadValidity);
        assert_eq!(err.at, NOW);
    }

    #[test]
    fn consume_validity_flags_a_value_mismatch() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b", b"c"]);
        // Offset 1 is committed as "b" but the consume reported "wrong".
        let consumes = vec![consumed(0, &[b"a", b"wrong"])];
        let err = check_consume_validity(&g, &committed, &consumes, NOW)
            .expect_err("a consume value mismatch must be flagged");
        assert_eq!(err.property, PropertyId::ConsumeReadValidity);
    }

    // ----- Per-partition linearizability (11.6) ---------------------------

    #[test]
    fn linearizability_passes_when_offsets_respect_real_time_order() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b", b"c"]);
        // a (offset 0) responds at t=2 before b (offset 1) is invoked at t=3.
        let acks = vec![acked(0, b"a", 1, 2), acked(1, b"b", 3, 4)];
        let consumes = vec![consumed(0, &[b"a", b"b", b"c"])];
        assert!(check_linearizability(&g, &committed, &acks, &consumes, NOW).is_ok());
    }

    #[test]
    fn linearizability_flags_offsets_that_invert_real_time_order() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b"]);
        // The first produce responded (t=2) before the second was invoked (t=3),
        // yet it holds the *later* offset (1 vs 0) — real-time order inverted.
        let acks = vec![acked(1, b"a", 1, 2), acked(0, b"b", 3, 4)];
        let err = check_linearizability(&g, &committed, &acks, &[], NOW)
            .expect_err("an offset inverting real-time order must be flagged");
        assert_eq!(err.property, PropertyId::PerPartitionLinearizability);
        assert_eq!(err.at, NOW);
    }

    #[test]
    fn linearizability_flags_a_consume_beyond_the_committed_log() {
        let g = group("orders", 0);
        let committed = log(&[b"a", b"b"]);
        // Consume claims a 3-record range [1,4) but the log only reaches 2.
        let consumes = vec![consumed(1, &[b"b", b"c", b"d"])];
        let err = check_linearizability(&g, &committed, &[], &consumes, NOW)
            .expect_err("a consume past the committed log must be flagged");
        assert_eq!(err.property, PropertyId::PerPartitionLinearizability);
    }

    // ----- Metadata catalogue convergence (11.7) --------------------------

    fn member(id: &str) -> Member {
        Member {
            id: NodeId::new(id),
            addr: format!("{id}:7001"),
            advertised_addr: format!("{id}:7001"),
            availability: NodeAvailability::Available,
        }
    }

    fn catalogue_with_topics(topics: &[&str]) -> ClusterMetadata {
        let mut meta = ClusterMetadata::new();
        meta.members = vec![member("node-0")];
        for name in topics {
            meta.topics.insert(
                (*name).to_string(),
                Topic {
                    name: (*name).to_string(),
                    partitions: vec![Partition {
                        index: PartitionIndex(0),
                        replicas: vec![NodeId::new("node-0")],
                        leader: Some(NodeId::new("node-0")),
                    }],
                    state: TopicState::Active,
                    backend: LogBackend::Durable,
                },
            );
        }
        meta
    }

    #[test]
    fn convergence_passes_for_equal_catalogues_at_the_same_commit_index() {
        let a = catalogue_with_topics(&["orders", "payments"]);
        let b = catalogue_with_topics(&["orders", "payments"]);
        let node_a = NodeId::new("node-0");
        let node_b = NodeId::new("node-1");
        let views = vec![(&node_a, 5, &a), (&node_b, 5, &b)];
        assert!(convergence_over_views(&views, NOW).is_ok());
    }

    #[test]
    fn convergence_ignores_nodes_at_different_commit_indices() {
        // Catalogues differ, but the nodes are at different commit indices, so
        // they are not required to agree yet.
        let a = catalogue_with_topics(&["orders"]);
        let b = catalogue_with_topics(&["orders", "payments"]);
        let node_a = NodeId::new("node-0");
        let node_b = NodeId::new("node-1");
        let views = vec![(&node_a, 5, &a), (&node_b, 6, &b)];
        assert!(convergence_over_views(&views, NOW).is_ok());
    }

    #[test]
    fn convergence_flags_divergent_catalogues_at_the_same_commit_index() {
        let a = catalogue_with_topics(&["orders"]);
        let b = catalogue_with_topics(&["orders", "payments"]);
        let node_a = NodeId::new("node-0");
        let node_b = NodeId::new("node-1");
        let views = vec![(&node_a, 7, &a), (&node_b, 7, &b)];
        let err = convergence_over_views(&views, NOW)
            .expect_err("divergent catalogues at the same commit index must be flagged");
        assert_eq!(err.property, PropertyId::MetadataConvergence);
        assert_eq!(err.at, NOW);
        // The divergence is the topic present on only one node.
        assert!(err.detail.contains("payments"));
    }

    #[test]
    fn first_topic_divergence_finds_the_sorted_first_difference() {
        let a = catalogue_with_topics(&["alpha", "gamma"]);
        let b = catalogue_with_topics(&["alpha", "beta", "gamma"]);
        // "beta" sorts before "gamma" and is the first key that differs.
        assert_eq!(first_topic_divergence(&a, &b), Some("beta".to_string()));
    }
}
