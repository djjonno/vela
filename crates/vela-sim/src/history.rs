//! `History` recorder: per-operation invocation/response capture.
//!
//! The [`History`] is the harness's record of everything a client did during a
//! [`Simulation_Run`] and what the cluster answered. The
//! [`Consistency_Checker`](crate::checker) replays it after the run to assert
//! the Kafka-parity durability/ordering guarantees and per-partition
//! linearizability, so its completeness and reproducibility are load-bearing.
//!
//! For each [`Client_Operation`] the harness issues, it records (Requirement
//! 9.1):
//!
//! - the operation **type and arguments** ([`OpKind`] + [`OpArgs`]),
//! - the **invocation instant** on the [`VirtualInstant`] clock,
//! - the **response instant** on the same clock, and
//! - the **response** ([`OpResponse`]).
//!
//! A successful produce records the target topic/partition, the produced value,
//! and the committed offset returned to the client (Requirement 9.2); a
//! successful consume records the topic/partition, the requested start offset,
//! and the ordered records returned (Requirement 9.3). A failure or redirection
//! is recorded *as the response* rather than discarded (Requirement 9.4), so the
//! checker sees a complete trace of every issued operation, expected
//! errors/redirections included.
//!
//! [`History`] is a passive recorder: it appends ops in the order they are
//! issued and performs no internal nondeterminism (it is a [`Vec`], not a
//! `HashMap`, so iteration order is the deterministic issue order). Because every
//! input is seed-derived and the runtime is single-threaded and event-ordered,
//! recording the same seed + scenario parameters yields an identical `History`
//! (Requirement 9.5). The [`Property 10`] test (history completeness) lives in a
//! separate task.
//!
//! [`Simulation_Run`]: crate
//! [`Client_Operation`]: crate
//! [`Consistency_Checker`]: crate::checker
//! [`Property 10`]: crate

use vela_core::{NodeId, Offset, PartitionIndex, Record};

use crate::scheduler::VirtualInstant;

/// The kind of a recorded client operation.
///
/// A redundant-but-cheap tag derived from [`OpArgs`] ([`OpArgs::kind`]) so
/// consumers can match on the operation type without destructuring the
/// arguments. The recorder always keeps the two consistent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    /// A topic-create administration operation.
    CreateTopic,
    /// A topic-delete administration operation.
    DeleteTopic,
    /// A produce (append) operation against a partition.
    Produce,
    /// A consume (read) operation against a partition.
    Consume,
}

/// The arguments a client supplied for an operation, by kind.
///
/// These are the *request* parameters, captured verbatim regardless of how the
/// cluster responded. The matching outcome is held separately in
/// [`OpResponse`]; a produce that succeeds, is redirected, or errors all carry
/// the same [`OpArgs::Produce`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpArgs {
    /// Create `topic` with `partitions` partitions at `replication_factor`.
    CreateTopic {
        /// The topic name.
        topic: String,
        /// The requested partition count.
        partitions: u32,
        /// The requested replication factor.
        replication_factor: u32,
    },
    /// Delete `topic`.
    DeleteTopic {
        /// The topic name.
        topic: String,
    },
    /// Produce a (possibly keyed) record to `topic`/`partition`.
    Produce {
        /// The target topic name.
        topic: String,
        /// The target partition the workload routed this record to.
        partition: PartitionIndex,
        /// The record key, if keyed (`None` for a keyless produce).
        key: Option<Vec<u8>>,
        /// The record payload.
        value: Vec<u8>,
    },
    /// Consume up to `max_records` records from `topic`/`partition` starting at
    /// `start_offset`.
    Consume {
        /// The target topic name.
        topic: String,
        /// The target partition.
        partition: PartitionIndex,
        /// The requested starting offset.
        start_offset: Offset,
        /// The maximum number of records requested.
        max_records: u32,
    },
}

impl OpArgs {
    /// The [`OpKind`] of these arguments.
    #[must_use]
    pub fn kind(&self) -> OpKind {
        match self {
            OpArgs::CreateTopic { .. } => OpKind::CreateTopic,
            OpArgs::DeleteTopic { .. } => OpKind::DeleteTopic,
            OpArgs::Produce { .. } => OpKind::Produce,
            OpArgs::Consume { .. } => OpKind::Consume,
        }
    }

    /// The target topic of the operation.
    #[must_use]
    pub fn topic(&self) -> &str {
        match self {
            OpArgs::CreateTopic { topic, .. }
            | OpArgs::DeleteTopic { topic }
            | OpArgs::Produce { topic, .. }
            | OpArgs::Consume { topic, .. } => topic,
        }
    }

    /// The target partition, for partition-scoped operations (produce/consume).
    ///
    /// Returns `None` for topic administration, which targets a whole topic
    /// rather than a single partition.
    #[must_use]
    pub fn partition(&self) -> Option<PartitionIndex> {
        match self {
            OpArgs::Produce { partition, .. } | OpArgs::Consume { partition, .. } => {
                Some(*partition)
            }
            OpArgs::CreateTopic { .. } | OpArgs::DeleteTopic { .. } => None,
        }
    }
}

/// The response the cluster returned for a client operation.
///
/// Successful variants carry the values the checker needs (the committed offset
/// for a produce, the ordered records for a consume). The remaining variants
/// capture the *expected* in-simulation outcomes — a leader redirect, an
/// exhausted redirect chain, no available leader, a surfaced storage I/O error,
/// or a generic rejection. Every one of these is a **valid recorded response**,
/// never a property violation (Requirement 9.4; design "expected in-simulation
/// outcomes"); the run continues after recording it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResponse {
    /// A produce succeeded: the record was committed to `topic`/`partition` at
    /// `offset`. The `value` is recorded alongside the offset so the checker can
    /// confirm the acknowledged record appears at its returned offset
    /// (Requirement 9.2).
    ProduceOk {
        /// The topic the record committed to.
        topic: String,
        /// The partition the record committed to.
        partition: PartitionIndex,
        /// The produced payload that was acknowledged.
        value: Vec<u8>,
        /// The committed offset returned to the client.
        offset: Offset,
    },
    /// A consume succeeded: `records` were returned from `topic`/`partition`
    /// starting at `start_offset`, in ascending offset order (Requirement 9.3).
    ConsumeOk {
        /// The topic read from.
        topic: String,
        /// The partition read from.
        partition: PartitionIndex,
        /// The starting offset the read began at.
        start_offset: Offset,
        /// The ordered records returned to the client.
        records: Vec<Record>,
    },
    /// A topic create succeeded for `topic`.
    CreateTopicOk {
        /// The created topic name.
        topic: String,
    },
    /// A topic delete succeeded for `topic`.
    DeleteTopicOk {
        /// The deleted topic name.
        topic: String,
    },
    /// The contacted node was not the leader and redirected the client toward
    /// `leader` (Requirement 8.4). The workload follows redirects; a recorded
    /// redirect is the *final* response only when redirect-following stopped
    /// here.
    Redirect {
        /// The node the cluster pointed the client at.
        leader: NodeId,
    },
    /// Five successive redirections did not reach a current leader (Requirement
    /// 8.5). A valid recorded response, not a violation.
    UnresolvedRedirection,
    /// No leader was available for the target group when the operation was
    /// issued (Requirement 8.6). A valid recorded response, not a violation.
    NoLeader,
    /// A storage fault surfaced an I/O error through the `LogStorage` contract
    /// (Requirement 7.4). A valid recorded response, not a violation.
    IoError {
        /// A human-readable description of the surfaced error.
        message: String,
    },
    /// Any other rejection the cluster returned (e.g. an invalid request or a
    /// commit timeout), recorded rather than discarded.
    Error {
        /// A human-readable description of the rejection.
        message: String,
    },
}

/// One recorded client operation: its type, arguments, invocation and response
/// instants, and the response (Requirement 9.1).
///
/// `kind` is kept consistent with `args` by construction (see
/// [`RecordedOp::new`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedOp {
    /// The operation kind (derived from `args`).
    pub kind: OpKind,
    /// The request arguments the client supplied.
    pub args: OpArgs,
    /// The logical instant the operation was invoked.
    pub invoked_at: VirtualInstant,
    /// The logical instant the response was recorded.
    pub responded_at: VirtualInstant,
    /// The response the cluster returned.
    pub response: OpResponse,
}

impl RecordedOp {
    /// Build a recorded op, deriving [`kind`](RecordedOp::kind) from `args` so
    /// the tag can never drift from the arguments.
    #[must_use]
    pub fn new(
        args: OpArgs,
        invoked_at: VirtualInstant,
        responded_at: VirtualInstant,
        response: OpResponse,
    ) -> Self {
        Self {
            kind: args.kind(),
            args,
            invoked_at,
            responded_at,
            response,
        }
    }

    /// The target topic of this operation.
    #[must_use]
    pub fn topic(&self) -> &str {
        self.args.topic()
    }

    /// The target partition, for partition-scoped operations.
    #[must_use]
    pub fn partition(&self) -> Option<PartitionIndex> {
        self.args.partition()
    }
}

/// The recorded, logically time-stamped sequence of client operations and their
/// responses produced during a run (Requirement 9.1).
///
/// Operations are appended in **invocation order** and never reordered, so
/// iteration reflects the deterministic order the workload issued them — a
/// `Vec`, not a `HashMap`, precisely so no run-to-run iteration nondeterminism
/// can leak in (Requirement 9.5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    /// The recorded operations, in invocation order.
    pub ops: Vec<RecordedOp>,
}

impl History {
    /// An empty history.
    #[must_use]
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Append an already-built [`RecordedOp`] in invocation order.
    pub fn record(&mut self, op: RecordedOp) {
        self.ops.push(op);
    }

    /// Record a successful produce: the record committed to `topic`/`partition`
    /// at `offset`. The acknowledged `value` is taken from the [`OpArgs::Produce`]
    /// arguments and stored on the response (Requirement 9.2).
    ///
    /// # Panics
    ///
    /// Panics if `args` is not an [`OpArgs::Produce`]; the runtime only calls
    /// this for produce operations.
    pub fn record_produce_success(
        &mut self,
        args: OpArgs,
        invoked_at: VirtualInstant,
        responded_at: VirtualInstant,
        offset: Offset,
    ) {
        let OpArgs::Produce {
            topic,
            partition,
            value,
            ..
        } = &args
        else {
            panic!("record_produce_success requires OpArgs::Produce, got {args:?}");
        };
        let response = OpResponse::ProduceOk {
            topic: topic.clone(),
            partition: *partition,
            value: value.clone(),
            offset,
        };
        self.record(RecordedOp::new(args, invoked_at, responded_at, response));
    }

    /// Record a successful consume: `records` were returned from
    /// `topic`/`partition` starting at the requested offset (Requirement 9.3).
    ///
    /// # Panics
    ///
    /// Panics if `args` is not an [`OpArgs::Consume`]; the runtime only calls
    /// this for consume operations.
    pub fn record_consume_success(
        &mut self,
        args: OpArgs,
        invoked_at: VirtualInstant,
        responded_at: VirtualInstant,
        records: Vec<Record>,
    ) {
        let OpArgs::Consume {
            topic,
            partition,
            start_offset,
            ..
        } = &args
        else {
            panic!("record_consume_success requires OpArgs::Consume, got {args:?}");
        };
        let response = OpResponse::ConsumeOk {
            topic: topic.clone(),
            partition: *partition,
            start_offset: *start_offset,
            records,
        };
        self.record(RecordedOp::new(args, invoked_at, responded_at, response));
    }

    /// Record a leader redirect toward `leader` as the operation's response
    /// (Requirement 8.4, 9.4).
    pub fn record_redirect(
        &mut self,
        args: OpArgs,
        invoked_at: VirtualInstant,
        responded_at: VirtualInstant,
        leader: NodeId,
    ) {
        self.record(RecordedOp::new(
            args,
            invoked_at,
            responded_at,
            OpResponse::Redirect { leader },
        ));
    }

    /// Record an expected failure/redirection response (unresolved redirection,
    /// no leader, surfaced I/O error, or a generic rejection) rather than
    /// discarding the operation (Requirement 9.4).
    pub fn record_failure(
        &mut self,
        args: OpArgs,
        invoked_at: VirtualInstant,
        responded_at: VirtualInstant,
        response: OpResponse,
    ) {
        self.record(RecordedOp::new(args, invoked_at, responded_at, response));
    }

    /// The number of recorded operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether no operations have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Iterate the recorded operations in invocation order.
    pub fn iter(&self) -> impl Iterator<Item = &RecordedOp> {
        self.ops.iter()
    }

    /// Iterate the operations targeting `topic`, in invocation order.
    pub fn iter_for_topic<'a>(
        &'a self,
        topic: &'a str,
    ) -> impl Iterator<Item = &'a RecordedOp> + 'a {
        self.ops.iter().filter(move |op| op.topic() == topic)
    }

    /// Iterate the partition-scoped operations targeting `topic`/`partition`, in
    /// invocation order. Topic-administration operations (which have no
    /// partition) are excluded.
    pub fn iter_for_partition<'a>(
        &'a self,
        topic: &'a str,
        partition: PartitionIndex,
    ) -> impl Iterator<Item = &'a RecordedOp> + 'a {
        self.ops
            .iter()
            .filter(move |op| op.topic() == topic && op.partition() == Some(partition))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(nanos: u64) -> VirtualInstant {
        VirtualInstant::from_nanos(nanos)
    }

    fn produce_args(topic: &str, partition: u32, value: &[u8]) -> OpArgs {
        OpArgs::Produce {
            topic: topic.to_string(),
            partition: PartitionIndex(partition),
            key: None,
            value: value.to_vec(),
        }
    }

    /// Requirement 9.2: a successful produce records the target topic/partition,
    /// the produced value, and the committed offset as the response.
    #[test]
    fn produce_success_records_topic_partition_value_and_offset() {
        let mut history = History::new();
        let args = produce_args("orders", 2, b"hello");
        history.record_produce_success(args.clone(), t(10), t(15), 7);

        assert_eq!(history.len(), 1);
        let op = &history.ops[0];
        assert_eq!(op.kind, OpKind::Produce);
        assert_eq!(op.args, args);
        assert_eq!(op.invoked_at, t(10));
        assert_eq!(op.responded_at, t(15));
        assert_eq!(
            op.response,
            OpResponse::ProduceOk {
                topic: "orders".to_string(),
                partition: PartitionIndex(2),
                value: b"hello".to_vec(),
                offset: 7,
            }
        );
    }

    /// Requirement 9.3: a successful consume records the topic/partition, the
    /// requested start offset, and the ordered records returned.
    #[test]
    fn consume_success_records_start_offset_and_ordered_records() {
        let mut history = History::new();
        let args = OpArgs::Consume {
            topic: "orders".to_string(),
            partition: PartitionIndex(1),
            start_offset: 3,
            max_records: 10,
        };
        let records = vec![
            Record {
                key: None,
                value: b"a".to_vec(),
            },
            Record {
                key: Some(b"k".to_vec()),
                value: b"b".to_vec(),
            },
        ];
        history.record_consume_success(args.clone(), t(20), t(25), records.clone());

        let op = &history.ops[0];
        assert_eq!(op.kind, OpKind::Consume);
        assert_eq!(op.args, args);
        match &op.response {
            OpResponse::ConsumeOk {
                topic,
                partition,
                start_offset,
                records: returned,
            } => {
                assert_eq!(topic, "orders");
                assert_eq!(*partition, PartitionIndex(1));
                assert_eq!(*start_offset, 3);
                // Ordering is preserved exactly as returned.
                assert_eq!(returned, &records);
            }
            other => panic!("expected ConsumeOk, got {other:?}"),
        }
    }

    /// Requirement 9.4: a redirect is recorded as the response, not discarded.
    #[test]
    fn redirect_is_recorded_as_the_response() {
        let mut history = History::new();
        let args = produce_args("orders", 0, b"v");
        history.record_redirect(args.clone(), t(1), t(2), NodeId("node-3".to_string()));

        assert_eq!(history.len(), 1, "redirected op must not be dropped");
        assert_eq!(
            history.ops[0].response,
            OpResponse::Redirect {
                leader: NodeId("node-3".to_string()),
            }
        );
        // The original arguments are retained alongside the redirect response.
        assert_eq!(history.ops[0].args, args);
    }

    /// Requirement 9.4: failure responses (unresolved redirection, no leader,
    /// I/O error, generic error) are all recorded rather than discarded.
    #[test]
    fn failures_are_recorded_rather_than_discarded() {
        let mut history = History::new();
        history.record_failure(
            produce_args("t", 0, b"x"),
            t(1),
            t(2),
            OpResponse::UnresolvedRedirection,
        );
        history.record_failure(produce_args("t", 0, b"x"), t(3), t(4), OpResponse::NoLeader);
        history.record_failure(
            produce_args("t", 0, b"x"),
            t(5),
            t(6),
            OpResponse::IoError {
                message: "disk full".to_string(),
            },
        );
        history.record_failure(
            OpArgs::CreateTopic {
                topic: "t".to_string(),
                partitions: 1,
                replication_factor: 1,
            },
            t(7),
            t(8),
            OpResponse::Error {
                message: "already exists".to_string(),
            },
        );

        assert_eq!(history.len(), 4, "every failed op is retained");
        assert_eq!(history.ops[0].response, OpResponse::UnresolvedRedirection);
        assert_eq!(history.ops[1].response, OpResponse::NoLeader);
        assert!(matches!(
            history.ops[2].response,
            OpResponse::IoError { .. }
        ));
        assert!(matches!(history.ops[3].response, OpResponse::Error { .. }));
    }

    /// Requirement 9.1: ops are returned in invocation order (a `Vec`, never
    /// reordered), so the recorded sequence mirrors the issue order exactly.
    #[test]
    fn ops_are_recorded_in_invocation_order() {
        let mut history = History::new();
        // Record with deliberately interleaved instants; order must follow the
        // call order, not the instants.
        history.record_produce_success(produce_args("t", 0, b"0"), t(100), t(101), 0);
        history.record_produce_success(produce_args("t", 0, b"1"), t(10), t(11), 1);
        history.record_produce_success(produce_args("t", 0, b"2"), t(50), t(51), 2);

        let offsets: Vec<Offset> = history
            .iter()
            .map(|op| match &op.response {
                OpResponse::ProduceOk { offset, .. } => *offset,
                other => panic!("expected ProduceOk, got {other:?}"),
            })
            .collect();
        assert_eq!(offsets, vec![0, 1, 2], "iteration follows invocation order");
    }

    /// `record` appends a pre-built op; `kind` is derived from `args` and stays
    /// consistent.
    #[test]
    fn record_appends_prebuilt_op_with_kind_derived_from_args() {
        let mut history = History::new();
        let op = RecordedOp::new(
            OpArgs::DeleteTopic {
                topic: "gone".to_string(),
            },
            t(1),
            t(2),
            OpResponse::DeleteTopicOk {
                topic: "gone".to_string(),
            },
        );
        assert_eq!(op.kind, OpKind::DeleteTopic);
        history.record(op);
        assert_eq!(history.ops[0].kind, OpKind::DeleteTopic);
    }

    /// Read accessors: filtering by topic and by topic/partition returns the
    /// matching ops in invocation order; topic admin ops are excluded from the
    /// partition filter.
    #[test]
    fn filters_select_by_topic_and_partition() {
        let mut history = History::new();
        history.record(RecordedOp::new(
            OpArgs::CreateTopic {
                topic: "orders".to_string(),
                partitions: 2,
                replication_factor: 3,
            },
            t(1),
            t(2),
            OpResponse::CreateTopicOk {
                topic: "orders".to_string(),
            },
        ));
        history.record_produce_success(produce_args("orders", 0, b"a"), t(3), t(4), 0);
        history.record_produce_success(produce_args("orders", 1, b"b"), t(5), t(6), 0);
        history.record_produce_success(produce_args("other", 0, b"c"), t(7), t(8), 0);

        // By topic: the create plus the two "orders" produces (not "other").
        assert_eq!(history.iter_for_topic("orders").count(), 3);
        assert_eq!(history.iter_for_topic("other").count(), 1);

        // By topic + partition: only the partition-0 produce on "orders"; the
        // create (no partition) is excluded.
        let p0: Vec<&RecordedOp> = history
            .iter_for_partition("orders", PartitionIndex(0))
            .collect();
        assert_eq!(p0.len(), 1);
        assert_eq!(p0[0].kind, OpKind::Produce);
        assert_eq!(p0[0].partition(), Some(PartitionIndex(0)));
    }

    /// A default/empty history reports empty and yields no ops.
    #[test]
    fn empty_history_is_empty() {
        let history = History::default();
        assert!(history.is_empty());
        assert_eq!(history.len(), 0);
        assert_eq!(history.iter().count(), 0);
    }
}
