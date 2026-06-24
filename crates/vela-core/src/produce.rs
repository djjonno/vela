//! The produce path: append a record on a partition's leader and return the
//! committed offset.
//!
//! This module implements the domain-layer produce semantics described in the
//! design's *Produce Flow* and Requirement 4. Producing one record is the
//! composition of:
//!
//! 1. **Topic admission** — the topic must exist and be producible. A missing
//!    topic is rejected with [`CoreError::TopicNotFound`] (Requirement 4.5) and
//!    a topic mid-deletion with [`CoreError::TopicDeleting`] (Requirement 3.7),
//!    reusing [`ClusterMetadata::ensure_producible`]. Neither appends anything.
//! 2. **Partition resolution** — the target partition must exist in the topic;
//!    otherwise [`CoreError::PartitionNotFound`] (Requirement 4.5, 10.5). The
//!    `(topic, partition_key) -> partition` mapping itself is the
//!    [`PartitionRouter`](crate::router::PartitionRouter)'s job (task 11.1);
//!    this path takes the already-resolved [`PartitionIndex`].
//! 3. **Payload validation** — the combined key and value size must not exceed
//!    1 MiB; an oversized record is rejected with [`CoreError::RecordTooLarge`]
//!    and **no** log entry is appended (Requirement 4.8).
//! 4. **Leadership** — only the partition's leader may append. A non-leader
//!    replica is rejected with [`CoreError::NotLeader`] carrying the believed
//!    current leader for the client to redirect to, and appends nothing
//!    (Requirement 4.6, 11.2).
//! 5. **Append, replicate, commit** — the leader encodes the record into a
//!    `Record` [`EntryPayload`] and proposes it; the entry is appended at the
//!    next log index and replicated. Once it commits on a majority the
//!    partition's [`StateMachine`](crate::fleet::StateMachine) assigns it the
//!    next gap-free, 0-based [`Offset`], which is returned to the producer
//!    (Requirement 4.3, 4.4, 4.7).
//!
//! ## Commit timing and the commit timeout
//!
//! [`PartitionReplica::step`] drives consensus one input at a time. In a
//! single-node group the leader is its own majority, so a proposal commits
//! within the same step and the offset is available immediately. In a
//! multi-node group the entry commits only once a majority of followers
//! acknowledge it, which happens over later steps as the server's driver pumps
//! `AppendEntries` replies in. When this path proposes but the entry has not
//! committed by the time it observes the result, it returns
//! [`CoreError::CommitTimeout`] without advancing the committed offset and
//! without returning an offset (Requirement 4.9).
//!
//! The wall-clock 5-second deadline of [`COMMIT_TIMEOUT_MS`] is enforced by the
//! server driver (task 14.2), which stops pumping replies and surfaces
//! `CommitTimeout` once the deadline passes. At this core, in-memory level the
//! timeout is modelled structurally: an entry that is not committed when the
//! produce step is observed yields `CommitTimeout`. The uncommitted entry may
//! remain in the leader's log pending replication — that is ordinary Raft
//! behaviour and does not advance the committed offset.

use vela_log::{EntryPayload, PayloadKind};
use vela_raft::{Clock, RaftInput, Role};

use crate::fleet::{PartitionReplica, RaftGroupFleet};
use crate::model::{ClusterMetadata, Offset, PartitionIndex, Record};
use crate::topic::CoreError;

/// The maximum combined key-and-value payload size of a single produced
/// record: 1,048,576 bytes (1 MiB) (Requirement 4.8). A record whose key length
/// plus value length exceeds this is rejected with
/// [`CoreError::RecordTooLarge`] and appends nothing.
pub const MAX_RECORD_BYTES: usize = 1_048_576;

/// The maximum number of records permitted in a single produce batch: 10,000
/// (Requirement 3.3). A batch carrying more records than this is rejected and
/// appends nothing. Shared by the server handler and the batch validation so
/// both enforce the same ceiling.
pub const MAX_BATCH_RECORDS: usize = 10_000;

/// The maximum total encoded size of a single produce batch: 16,777,216 bytes
/// (16 MiB) (Requirement 3.4). A batch whose encoded size exceeds this is
/// rejected and appends nothing. Chosen comfortably under the 64 MiB wire
/// message limit, leaving headroom for proto and replication framing.
pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// The commit timeout for a produced record: 5,000 milliseconds
/// (Requirement 4.9).
///
/// If a record's log entry is not replicated to a majority within this window
/// the produce request fails with [`CoreError::CommitTimeout`] without
/// advancing the partition's committed offset. The wall-clock enforcement of
/// this deadline lives in the server driver; this crate exposes the constant
/// and models the not-committed outcome structurally (see the module docs).
pub const COMMIT_TIMEOUT_MS: u64 = 5_000;

/// The size, in bytes, of the length prefix that frames each record's value in
/// an encoded batch payload (a fixed-width `u32` length).
///
/// A batch is carried as one `RecordBatch` entry whose bytes are a
/// length-delimited concatenation of the batch's record **value** bytes (keys
/// remain unpersisted, matching the single-record path). The encoder
/// (`encode_record_batch`, task 3.4) writes this prefix before each value, so
/// the encoded-size check in [`validate_batch`] counts it per record. Keeping
/// the prefix width here is the single source of truth the validation and the
/// codec share, so the `MAX_BATCH_BYTES` limit is checked against exactly the
/// byte total the entry will occupy.
const BATCH_FRAME_HEADER_BYTES: usize = 4;

/// The reason a [`Produce_Batch`](crate) was rejected by [`validate_batch`],
/// carrying the values the requirements demand the error report.
///
/// Validation is pure and side-effect-free: returning any of these variants
/// appends nothing and leaves the target partition's log and committed offset
/// unchanged (Requirement 3.6). The server handler maps each variant onto the
/// matching caller-visible [`CoreError`](crate::topic::CoreError).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchRejection {
    /// The batch carried zero records (Requirement 2.2, 3.5).
    Empty,
    /// The record at 0-based `index` had a combined key-and-value `size` that
    /// exceeds the 1 MiB [`MAX_RECORD_BYTES`] per-record limit (Requirement
    /// 3.2). `index` is the first offending record in batch order.
    RecordTooLarge {
        /// The 0-based position of the offending record within the batch.
        index: usize,
        /// The offending record's submitted combined key + value size.
        size: usize,
    },
    /// The batch carried `submitted` records, more than the `max`
    /// ([`MAX_BATCH_RECORDS`]) permitted (Requirement 3.3).
    TooManyRecords {
        /// The configured maximum record count ([`MAX_BATCH_RECORDS`]).
        max: usize,
        /// The submitted record count.
        submitted: usize,
    },
    /// The batch's total encoded size `submitted` exceeded the `max`
    /// ([`MAX_BATCH_BYTES`]) permitted (Requirement 3.4).
    TooLarge {
        /// The configured maximum encoded size in bytes ([`MAX_BATCH_BYTES`]).
        max: usize,
        /// The submitted total encoded size in bytes.
        submitted: usize,
    },
}

/// The total encoded size, in bytes, of a batch's record values when framed as
/// the length-delimited concatenation that one `RecordBatch` entry carries: for
/// each record a fixed [`BATCH_FRAME_HEADER_BYTES`] length prefix plus the
/// record's value bytes (keys are not persisted).
///
/// This is the byte total the [`MAX_BATCH_BYTES`] limit is checked against
/// (Requirement 3.4); it matches what `encode_record_batch` (task 3.4) will
/// produce, so validation and the codec agree on one byte total. Returns
/// `usize::MAX` rather than overflowing if the framed total cannot fit a
/// `usize` (only reachable beyond the count and per-record limits), so an
/// unrepresentable batch is still reported as over the byte limit.
fn encoded_batch_size(records: &[Record]) -> usize {
    records.iter().fold(0usize, |acc, record| {
        acc.saturating_add(BATCH_FRAME_HEADER_BYTES)
            .saturating_add(record.value.len())
    })
}

/// Validate a batch's `records` against the empty, per-record, count, and total
/// encoded-byte limits, returning `Ok(())` only when **every** limit holds
/// (Requirement 3.1).
///
/// The function is pure and side-effect-free: it appends nothing and leaves all
/// partition state unchanged regardless of outcome (Requirement 3.6). It is the
/// testable core of the batch size limits, called by the server handler
/// **before** any append so a rejected batch never reaches the log.
///
/// Checks run in a fixed precedence so a batch failing several limits reports a
/// single, predictable reason:
///
/// 1. **Empty** — zero records is rejected with [`BatchRejection::Empty`]
///    (Requirement 2.2, 3.5).
/// 2. **Per-record size** — the first record whose combined key+value size
///    exceeds [`MAX_RECORD_BYTES`] (1 MiB) is rejected with
///    [`BatchRejection::RecordTooLarge`] naming its 0-based `index` and
///    submitted `size` (Requirement 3.2).
/// 3. **Record count** — more than [`MAX_BATCH_RECORDS`] records is rejected
///    with [`BatchRejection::TooManyRecords`] reporting the `max` and the
///    `submitted` count (Requirement 3.3).
/// 4. **Total encoded bytes** — an encoded size over [`MAX_BATCH_BYTES`] is
///    rejected with [`BatchRejection::TooLarge`] reporting the `max` and the
///    `submitted` encoded size (Requirement 3.4).
pub fn validate_batch(records: &[Record]) -> Result<(), BatchRejection> {
    // 1. An empty batch is rejected before any other check (Requirement 3.5).
    if records.is_empty() {
        return Err(BatchRejection::Empty);
    }

    // 2. The first record whose combined key + value size exceeds the 1 MiB
    //    per-record limit is reported by its 0-based position (Requirement 3.2).
    for (index, record) in records.iter().enumerate() {
        let size = record.key.as_ref().map_or(0, |k| k.len()) + record.value.len();
        if size > MAX_RECORD_BYTES {
            return Err(BatchRejection::RecordTooLarge { index, size });
        }
    }

    // 3. The batch must carry at most MAX_BATCH_RECORDS records (Requirement
    //    3.3).
    let submitted = records.len();
    if submitted > MAX_BATCH_RECORDS {
        return Err(BatchRejection::TooManyRecords {
            max: MAX_BATCH_RECORDS,
            submitted,
        });
    }

    // 4. The encoded batch must fit MAX_BATCH_BYTES (Requirement 3.4).
    let encoded = encoded_batch_size(records);
    if encoded > MAX_BATCH_BYTES {
        return Err(BatchRejection::TooLarge {
            max: MAX_BATCH_BYTES,
            submitted: encoded,
        });
    }

    Ok(())
}

/// Encode a batch's record **value** bytes into the opaque payload of one
/// `RecordBatch` [`EntryPayload`] (Requirement 1.2, 2.1, 7.3, 10.2).
///
/// The payload is a length-delimited concatenation of each record's value bytes
/// in batch order: for every record a fixed [`BATCH_FRAME_HEADER_BYTES`]-wide
/// little-endian `u32` length prefix, followed by exactly that many value
/// bytes. Keys are **not** persisted, matching the single-record produce path
/// (`convert.rs` appends only `record.value`), so consume parity holds across
/// the single and batch paths (Requirement 10.2).
///
/// This framing is the exact byte total [`validate_batch`] checks against
/// [`MAX_BATCH_BYTES`] via `encoded_batch_size` — `4 + value.len()` per record
/// — so validation and the codec share one source of truth. [`decode_record_batch`]
/// is the exact inverse.
///
/// A record value longer than `u32::MAX` cannot be framed; such a record is far
/// beyond the per-record limit [`validate_batch`] enforces, so this is
/// unreachable for any batch that passed validation. The length is truncated to
/// `u32` only as a defensive last resort.
pub fn encode_record_batch(records: &[Record]) -> Vec<u8> {
    let total: usize = records.iter().fold(0usize, |acc, record| {
        acc.saturating_add(BATCH_FRAME_HEADER_BYTES)
            .saturating_add(record.value.len())
    });
    let mut bytes = Vec::with_capacity(total);
    for record in records {
        let len = record.value.len() as u32;
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(&record.value);
    }
    bytes
}

/// Decode a `RecordBatch` payload's `bytes` back into the ordered value frames,
/// the exact inverse of [`encode_record_batch`] (used by the state machine on
/// apply).
///
/// Each frame is a [`BATCH_FRAME_HEADER_BYTES`]-wide little-endian `u32` length
/// prefix followed by that many value bytes; the values are returned in batch
/// order. A truncated trailing frame — a length prefix with fewer bytes
/// remaining than it claims, or a partial prefix — is ignored, so decoding
/// never panics on malformed input; for well-formed input produced by
/// [`encode_record_batch`] the round-trip is lossless.
pub fn decode_record_batch(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut values = Vec::new();
    let mut pos = 0usize;
    while pos + BATCH_FRAME_HEADER_BYTES <= bytes.len() {
        let mut len_buf = [0u8; BATCH_FRAME_HEADER_BYTES];
        len_buf.copy_from_slice(&bytes[pos..pos + BATCH_FRAME_HEADER_BYTES]);
        let len = u32::from_le_bytes(len_buf) as usize;
        pos += BATCH_FRAME_HEADER_BYTES;

        // A frame claiming more bytes than remain is a truncated tail; stop.
        if pos + len > bytes.len() {
            break;
        }
        values.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    values
}

/// The outcome of appending a record to a [`PartitionReplica`].
///
/// This is the leader-local result, independent of the believed-leader identity
/// and topic/partition admission checks that the [`produce`] entry point layers
/// on top. Keeping it separate lets a replica's produce behaviour be tested in
/// isolation while [`produce`] maps each outcome onto the appropriate
/// [`CoreError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProduceOutcome {
    /// The record committed and was assigned this gap-free, 0-based offset
    /// (Requirement 4.4, 4.7).
    Committed(Offset),
    /// This replica is not the partition leader, so it appended nothing; the
    /// caller redirects the producer to the current leader (Requirement 4.6).
    NotLeader,
    /// The entry was appended but had not committed when the result was
    /// observed; the committed offset is not advanced (Requirement 4.9).
    NotCommitted,
}

impl PartitionReplica {
    /// Append `value` as a record on this replica and report whether it
    /// committed, assigning the next gap-free 0-based offset on commit.
    ///
    /// Only a leader may append (Requirement 4.3): a non-leader returns
    /// [`ProduceOutcome::NotLeader`] and appends nothing. A leader encodes the
    /// value into a `Record` [`EntryPayload`] and proposes it; the entry is
    /// appended at the next log index and replicated. If it commits within this
    /// step (always so for a single-node group, where the leader is its own
    /// majority) the partition state machine has assigned it the next offset,
    /// returned as [`ProduceOutcome::Committed`]. Otherwise the entry is pending
    /// replication and [`ProduceOutcome::NotCommitted`] is returned without
    /// advancing the committed offset (Requirement 4.7, 4.9).
    ///
    /// The assigned offset is the partition's `next_offset` captured **before**
    /// the proposal: because record entries are appended and committed in
    /// order, the just-proposed record takes exactly that offset once it
    /// commits, keeping offsets unique, gap-free, and monotonic
    /// (Requirement 4.7).
    pub fn produce_value(&mut self, value: Vec<u8>, clock: &mut impl Clock) -> ProduceOutcome {
        if self.role() != Role::Leader {
            return ProduceOutcome::NotLeader;
        }

        // The offset this record will receive once it commits: its position
        // among record entries, captured before the append (Requirement 4.7).
        let expected = self.state_machine().next_offset();

        let payload = EntryPayload::new(PayloadKind::Record, value);
        self.step(RaftInput::Propose(payload), clock);

        // The state machine advances its record count only on commit. If it has
        // moved past `expected`, our record committed and holds that offset;
        // otherwise the entry is still pending a majority (Requirement 4.9).
        if self.state_machine().next_offset() > expected {
            ProduceOutcome::Committed(expected)
        } else {
            ProduceOutcome::NotCommitted
        }
    }
}

/// Produce `record` to `partition` of `topic`, returning the committed
/// [`Offset`].
///
/// This is the produce entry point composing topic admission, partition
/// resolution, payload validation, the leadership check, and the
/// append/replicate/commit cycle (Requirement 4.3–4.9). The partition is the
/// one a [`PartitionRouter`](crate::router::PartitionRouter) already resolved
/// from the record's partition key (Requirement 4.1, 4.2); `clock` drives the
/// replica's consensus step.
///
/// On success the record committed and the returned offset is its unique,
/// gap-free, 0-based position in the partition (Requirement 4.4, 4.7).
///
/// Errors, none of which return an offset and the first three of which append
/// nothing:
///
/// - [`CoreError::TopicNotFound`] — the topic does not exist (Requirement 4.5).
/// - [`CoreError::TopicDeleting`] — the topic is mid-deletion (Requirement 3.7).
/// - [`CoreError::PartitionNotFound`] — the topic has no such partition, or the
///   partition's Raft group is not hosted here (Requirement 4.5, 10.5).
/// - [`CoreError::RecordTooLarge`] — the combined key+value size exceeds 1 MiB
///   (Requirement 4.8).
/// - [`CoreError::NotLeader`] — this replica is not the partition leader; the
///   error carries the believed leader to redirect to (Requirement 4.6, 11.2).
/// - [`CoreError::CommitTimeout`] — the entry did not commit to a majority
///   within the commit timeout; the committed offset is not advanced
///   (Requirement 4.9).
pub fn produce(
    metadata: &ClusterMetadata,
    fleet: &mut RaftGroupFleet,
    topic: &str,
    partition: PartitionIndex,
    record: &Record,
    clock: &mut impl Clock,
) -> Result<Offset, CoreError> {
    // 1. The topic must exist and be producible: rejects missing (4.5) and
    //    mid-deletion (3.7) topics without appending anything.
    metadata.ensure_producible(topic)?;

    // 2. The partition must exist in the topic (Requirement 4.5, 10.5). Capture
    //    the believed leader now, in case the produce must redirect below.
    let leader_hint = metadata
        .topics
        .get(topic)
        .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
        .ok_or_else(|| CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition.0,
        })?
        .leader
        .clone();

    // 3. Validate the combined key+value payload size before any append
    //    (Requirement 4.8).
    let size = record.key.as_ref().map_or(0, |k| k.len()) + record.value.len();
    if size > MAX_RECORD_BYTES {
        return Err(CoreError::RecordTooLarge(size));
    }

    // 4. The partition's Raft group must be hosted here. A partition that
    //    exists in metadata but has no local replica is treated as not found on
    //    this node (Requirement 4.5, 10.5).
    let replica = fleet
        .get_mut(&(topic.to_string(), partition))
        .ok_or_else(|| CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition.0,
        })?;

    // 5. Append on the leader, replicate, and commit (Requirement 4.3, 4.4,
    //    4.7); a non-leader redirects (4.6) and a non-commit times out (4.9).
    match replica.produce_value(record.value.clone(), clock) {
        ProduceOutcome::Committed(offset) => Ok(offset),
        ProduceOutcome::NotLeader => Err(CoreError::NotLeader {
            leader: leader_hint,
        }),
        ProduceOutcome::NotCommitted => Err(CoreError::CommitTimeout),
    }
}

/// The leader-local outcome of appending a whole batch to a [`PartitionReplica`]
/// as one `RecordBatch` entry (the batch sibling of [`ProduceOutcome`]).
///
/// Like [`ProduceOutcome`], this is independent of the believed-leader identity
/// and the topic/partition admission checks that [`produce_batch`] layers on
/// top, so a replica's batch append can be tested in isolation. [`produce_batch`]
/// maps each outcome onto the matching [`CoreError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    /// The batch committed as one unit: its first record took `base_offset` and
    /// the batch contributed `count` contiguous records (Requirement 1.3, 2.1,
    /// 2.4).
    Committed {
        /// The offset assigned to the batch's first (0-based position 0) record;
        /// the Nth record takes `base_offset + N`.
        base_offset: Offset,
        /// The number of records the batch committed (`>= 1`).
        count: u32,
    },
    /// This replica is not the partition leader, so it appended nothing; the
    /// caller redirects the producer to the current leader (Requirement 6.1).
    NotLeader,
    /// The batch entry was appended but had not committed when the result was
    /// observed; the committed offset is not advanced (Requirement 6.3).
    NotCommitted,
}

impl PartitionReplica {
    /// Append `values` as a single multi-record batch on this replica and report
    /// whether the batch committed, assigning the first record the next gap-free
    /// 0-based offset on commit.
    ///
    /// This mirrors [`PartitionReplica::produce_value`] but proposes exactly
    /// **one** `RecordBatch` [`EntryPayload`] carrying all `values`
    /// length-delimited (via [`encode_record_batch`]), so the whole batch is one
    /// append, one fsync (under `SyncPolicy::Always`), one replication round, and
    /// one commit (Requirement 7.1, 7.2, 7.3). Only a leader may append
    /// (Requirement 6.1): a non-leader returns [`BatchOutcome::NotLeader`] and
    /// appends nothing.
    ///
    /// The `base_offset` is the partition's `next_offset` captured **before** the
    /// proposal. Because the batch is applied atomically in one `apply` call, on
    /// commit the state machine has advanced by exactly `count` records and the
    /// batch occupies the contiguous range `base_offset..base_offset + count`
    /// (Requirement 2.4, 2.5). If the entry has not committed (no majority yet)
    /// the committed offset is not advanced and [`BatchOutcome::NotCommitted`] is
    /// returned (Requirement 6.3).
    pub fn produce_batch_values(
        &mut self,
        values: Vec<Vec<u8>>,
        clock: &mut impl Clock,
    ) -> BatchOutcome {
        if self.role() != Role::Leader {
            return BatchOutcome::NotLeader;
        }

        let count = values.len() as u32;
        // The offset the batch's first record will receive once it commits:
        // the partition's record count captured before the append
        // (Requirement 2.4).
        let base = self.state_machine().next_offset();

        // Reconstruct keyless records so the batch reuses the single batch codec
        // (`encode_record_batch` frames record values only — keys are not
        // persisted, matching the single-record path).
        let records: Vec<Record> = values.into_iter().map(|v| Record::new(None, v)).collect();
        let payload = EntryPayload::new(PayloadKind::RecordBatch, encode_record_batch(&records));
        self.step(RaftInput::Propose(payload), clock);

        // The state machine advances its record count by `count` only when the
        // batch entry commits. If it has reached `base + count` all N records
        // committed as one unit; otherwise the entry is still pending a majority
        // (Requirement 6.3).
        if self.state_machine().next_offset() >= base + count as Offset {
            BatchOutcome::Committed {
                base_offset: base,
                count,
            }
        } else {
            BatchOutcome::NotCommitted
        }
    }
}

/// Produce an ordered `records` batch to `partition` of `topic` as a single
/// atomic unit, returning the `(base_offset, count)` of the committed batch.
///
/// This is the batch sibling of [`produce`]. It runs the **same** admission,
/// partition-resolution, and leadership checks, then validates the whole batch
/// with [`validate_batch`] **before** any append, so a rejected batch never
/// reaches the log (Requirement 3.6, 2.2). Only a fully valid batch is proposed
/// as one `RecordBatch` entry; on commit the first record takes `base_offset`
/// and the Nth record takes `base_offset + N` (Requirement 1.3, 2.4). The
/// single-record [`produce`] path is unchanged.
///
/// Validation rejections (Requirement 3), none of which append anything:
///
/// - [`CoreError::EmptyBatch`] — the batch carried zero records
///   (Requirement 2.2, 3.5).
/// - [`CoreError::RecordTooLargeAt`] — a record's combined key+value size
///   exceeds 1 MiB, naming its 0-based index and size (Requirement 3.2).
/// - [`CoreError::BatchTooManyRecords`] — more than [`MAX_BATCH_RECORDS`]
///   records (Requirement 3.3).
/// - [`CoreError::BatchTooLarge`] — encoded size over [`MAX_BATCH_BYTES`]
///   (Requirement 3.4).
///
/// Admission and append errors reuse the single-record mapping (Requirement 6):
///
/// - [`CoreError::TopicNotFound`] / [`CoreError::TopicDeleting`] — the topic does
///   not exist or is mid-deletion (Requirement 6.4, 6.6).
/// - [`CoreError::PartitionNotFound`] — the topic has no such partition, or the
///   partition's Raft group is not hosted here (Requirement 6.5).
/// - [`CoreError::NotLeader`] — this replica is not the leader; carries the
///   believed leader to redirect to (Requirement 6.1).
/// - [`CoreError::CommitTimeout`] — the batch entry did not commit to a majority
///   within the commit timeout; the committed offset is not advanced
///   (Requirement 6.3).
pub fn produce_batch(
    metadata: &ClusterMetadata,
    fleet: &mut RaftGroupFleet,
    topic: &str,
    partition: PartitionIndex,
    records: &[Record],
    clock: &mut impl Clock,
) -> Result<(Offset, u32), CoreError> {
    // 1. The topic must exist and be producible: rejects missing (6.4) and
    //    mid-deletion (6.6) topics without appending anything.
    metadata.ensure_producible(topic)?;

    // 2. The partition must exist in the topic (Requirement 6.5). Capture the
    //    believed leader now, in case the produce must redirect below.
    let leader_hint = metadata
        .topics
        .get(topic)
        .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
        .ok_or_else(|| CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition.0,
        })?
        .leader
        .clone();

    // 3. Validate the whole batch (empty/per-record/count/bytes) before any
    //    append, mapping each rejection onto its caller-visible CoreError so a
    //    rejected batch reaches the log untouched (Requirement 3).
    validate_batch(records).map_err(|rejection| match rejection {
        BatchRejection::Empty => CoreError::EmptyBatch,
        BatchRejection::RecordTooLarge { index, size } => {
            CoreError::RecordTooLargeAt { index, size }
        }
        BatchRejection::TooManyRecords { max, submitted } => {
            CoreError::BatchTooManyRecords { max, submitted }
        }
        BatchRejection::TooLarge { max, submitted } => CoreError::BatchTooLarge { max, submitted },
    })?;

    // 4. The partition's Raft group must be hosted here. A partition that exists
    //    in metadata but has no local replica is treated as not found on this
    //    node (Requirement 6.5).
    let replica = fleet
        .get_mut(&(topic.to_string(), partition))
        .ok_or_else(|| CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition.0,
        })?;

    // 5. Append the batch as one entry on the leader, replicate, and commit
    //    (Requirement 1.3, 2.4, 7.3); a non-leader redirects (6.1) and a
    //    non-commit times out (6.3).
    let values: Vec<Vec<u8>> = records.iter().map(|r| r.value.clone()).collect();
    match replica.produce_batch_values(values, clock) {
        BatchOutcome::Committed { base_offset, count } => Ok((base_offset, count)),
        BatchOutcome::NotLeader => Err(CoreError::NotLeader {
            leader: leader_hint,
        }),
        BatchOutcome::NotCommitted => Err(CoreError::CommitTimeout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use vela_raft::{NodeId as RaftNodeId, RaftMessage, RequestVoteReply, TimerKind};

    use crate::model::{Member, NodeAvailability, NodeId, Partition, Topic, TopicState};

    /// A minimal [`Clock`] that never advances on its own; arming a timer is a
    /// no-op. Tests drive consensus with explicit [`RaftInput`]s, so no real
    /// timing is needed.
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

    /// Build metadata holding a single topic with `partition_count` partitions,
    /// each led by `leader`, over a cluster of one available member.
    fn metadata_with_topic(
        name: &str,
        partition_count: u32,
        leader: Option<NodeId>,
    ) -> ClusterMetadata {
        let mut meta = ClusterMetadata::new();
        meta.members = vec![Member {
            id: NodeId::new("node-0"),
            addr: "node-0:7001".to_string(),
            advertised_addr: "node-0:7001".to_string(),
            availability: NodeAvailability::Available,
        }];
        let partitions = (0..partition_count)
            .map(|i| Partition {
                index: PartitionIndex(i),
                replicas: vec![NodeId::new("node-0")],
                leader: leader.clone(),
            })
            .collect();
        meta.topics.insert(
            name.to_string(),
            Topic {
                name: name.to_string(),
                partitions,
                state: TopicState::Active,
                backend: crate::model::LogBackend::Durable,
            },
        );
        meta
    }

    /// A fleet hosting one single-node group for `topic`/partition 0, already
    /// driven to leader (its lone self-vote is a majority).
    fn fleet_with_leader(topic: &str, clock: &mut TestClock) -> RaftGroupFleet {
        let mut fleet = RaftGroupFleet::new();
        let key = (topic.to_string(), PartitionIndex(0));
        fleet
            .create_group(key.clone(), RaftNodeId(0), Vec::new())
            .unwrap();
        let replica = fleet.get_mut(&key).unwrap();
        replica.step(RaftInput::Tick(TimerKind::Election), clock);
        assert_eq!(replica.role(), Role::Leader);
        fleet
    }

    #[test]
    fn single_node_leader_assigns_gap_free_zero_based_offsets() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // Each committed record receives the next contiguous 0-based offset.
        for expected in 0..5u64 {
            let record = Record::new(None, format!("v{expected}").into_bytes());
            let offset = produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock,
            )
            .unwrap();
            assert_eq!(offset, expected);
        }

        // The records are readable back in order at their assigned offsets.
        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), Some(4));
        let records = replica.read(0, 100);
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].value, b"v0".to_vec());
        assert_eq!(records[4].value, b"v4".to_vec());
    }

    #[test]
    fn oversized_payload_is_rejected_and_appends_nothing() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // A value one byte over the 1 MiB limit is rejected for size.
        let big = Record::new(None, vec![0u8; MAX_RECORD_BYTES + 1]);
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &big,
                &mut clock
            ),
            Err(CoreError::RecordTooLarge(MAX_RECORD_BYTES + 1))
        );

        // The combined key + value size is what counts: each is at the limit on
        // its own but together they exceed it.
        let combined = Record::new(Some(vec![1u8; MAX_RECORD_BYTES]), vec![2u8; 1]);
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &combined,
                &mut clock
            ),
            Err(CoreError::RecordTooLarge(MAX_RECORD_BYTES + 1))
        );

        // Nothing was appended (Requirement 4.8): the partition is still empty.
        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), None);

        // A record exactly at the limit is accepted (not rejected for size).
        let at_limit = Record::new(None, vec![3u8; MAX_RECORD_BYTES]);
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &at_limit,
                &mut clock
            ),
            Ok(0)
        );
    }

    #[test]
    fn produce_to_a_non_leader_is_redirected_and_appends_nothing() {
        let mut clock = TestClock::new();
        // The believed leader is node-1, but the locally hosted replica (node-0)
        // is a follower (never elected).
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-1")));
        let mut fleet = RaftGroupFleet::new();
        let key = ("orders".to_string(), PartitionIndex(0));
        fleet
            .create_group(
                key.clone(),
                RaftNodeId(0),
                vec![RaftNodeId(1), RaftNodeId(2)],
            )
            .unwrap();
        assert_eq!(fleet.get(&key).unwrap().role(), Role::Follower);

        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock
            ),
            Err(CoreError::NotLeader {
                leader: Some(NodeId::new("node-1")),
            })
        );

        // The non-leader appended nothing (Requirement 4.6).
        assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }

    #[test]
    fn produce_to_a_missing_topic_is_rejected() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "ghost",
                PartitionIndex(0),
                &record,
                &mut clock
            ),
            Err(CoreError::TopicNotFound("ghost".to_string()))
        );
    }

    #[test]
    fn produce_to_a_missing_partition_is_rejected() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(7),
                &record,
                &mut clock
            ),
            Err(CoreError::PartitionNotFound {
                topic: "orders".to_string(),
                index: 7,
            })
        );
    }

    #[test]
    fn produce_to_a_deleting_topic_is_rejected() {
        let mut clock = TestClock::new();
        let mut meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        meta.topics.get_mut("orders").unwrap().state = TopicState::Deleting;
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock
            ),
            Err(CoreError::TopicDeleting("orders".to_string()))
        );
    }

    #[test]
    fn uncommitted_record_in_a_multi_node_group_times_out() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));

        // A 3-node group whose leader cannot reach a commit majority because no
        // follower acknowledges.
        let mut fleet = RaftGroupFleet::new();
        let key = ("orders".to_string(), PartitionIndex(0));
        fleet
            .create_group(
                key.clone(),
                RaftNodeId(0),
                vec![RaftNodeId(1), RaftNodeId(2)],
            )
            .unwrap();

        // Drive node-0 to leader: an election self-vote plus one granted reply
        // is a majority of three.
        let replica = fleet.get_mut(&key).unwrap();
        replica.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        replica.step(
            RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                term: replica.raft().current_term(),
                vote_granted: true,
                voter: RaftNodeId(1),
            })),
            &mut clock,
        );
        assert_eq!(replica.role(), Role::Leader);

        // The proposed record is appended but cannot commit without follower
        // acks, so produce times out without advancing the committed offset
        // (Requirement 4.9).
        let record = Record::new(None, b"v".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &record,
                &mut clock
            ),
            Err(CoreError::CommitTimeout)
        );
        assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }

    /// A record exactly at the combined key + value limit (1 MiB) plus values
    /// at the per-record boundary, used to build batches around the limits.
    fn record_of_value_len(len: usize) -> Record {
        Record::new(None, vec![0u8; len])
    }

    #[test]
    fn validate_batch_rejects_empty_before_any_other_check() {
        // Zero records is rejected as Empty regardless of other state
        // (Requirement 2.2, 3.5).
        assert_eq!(validate_batch(&[]), Err(BatchRejection::Empty));
    }

    #[test]
    fn validate_batch_accepts_a_batch_within_every_limit() {
        // A single small record is well within all limits (Requirement 3.1).
        let batch = vec![Record::new(Some(b"k".to_vec()), b"v".to_vec())];
        assert_eq!(validate_batch(&batch), Ok(()));

        // A record at exactly the per-record limit is accepted (the limit is
        // inclusive): combined key + value == MAX_RECORD_BYTES.
        let at_limit = vec![Record::new(
            Some(vec![1u8; 16]),
            vec![2u8; MAX_RECORD_BYTES - 16],
        )];
        assert_eq!(validate_batch(&at_limit), Ok(()));

        // A batch of exactly MAX_BATCH_RECORDS tiny records is accepted, as long
        // as the encoded size still fits MAX_BATCH_BYTES (it does: ~4 bytes
        // each).
        let max_count: Vec<Record> = (0..MAX_BATCH_RECORDS)
            .map(|_| Record::new(None, Vec::new()))
            .collect();
        assert_eq!(validate_batch(&max_count), Ok(()));
    }

    #[test]
    fn validate_batch_reports_first_oversized_record_with_index_and_size() {
        // The combined key + value size is what counts, and the first offending
        // record (0-based) is reported (Requirement 3.2).
        let batch = vec![
            Record::new(None, b"ok".to_vec()),
            Record::new(Some(vec![1u8; 8]), vec![2u8; MAX_RECORD_BYTES - 7]),
        ];
        assert_eq!(
            validate_batch(&batch),
            Err(BatchRejection::RecordTooLarge {
                index: 1,
                size: MAX_RECORD_BYTES + 1,
            })
        );

        // A value alone one byte over the limit at position 0.
        let first_big = vec![record_of_value_len(MAX_RECORD_BYTES + 1)];
        assert_eq!(
            validate_batch(&first_big),
            Err(BatchRejection::RecordTooLarge {
                index: 0,
                size: MAX_RECORD_BYTES + 1,
            })
        );
    }

    #[test]
    fn validate_batch_rejects_too_many_records() {
        // One record over the count ceiling is rejected, reporting the max and
        // the submitted count (Requirement 3.3). Empty values keep the encoded
        // size tiny so the count check is what fires.
        let batch: Vec<Record> = (0..=MAX_BATCH_RECORDS)
            .map(|_| Record::new(None, Vec::new()))
            .collect();
        assert_eq!(
            validate_batch(&batch),
            Err(BatchRejection::TooManyRecords {
                max: MAX_BATCH_RECORDS,
                submitted: MAX_BATCH_RECORDS + 1,
            })
        );
    }

    #[test]
    fn validate_batch_rejects_oversized_encoded_batch() {
        // A handful of large (but individually legal) records whose framed
        // total exceeds MAX_BATCH_BYTES is rejected for total size, reporting
        // the max and the submitted encoded size (Requirement 3.4). Each record
        // is just under 1 MiB; enough of them overflow the 16 MiB batch ceiling
        // while staying under the record-count limit.
        let per_value = MAX_RECORD_BYTES; // 1 MiB value, legal per record
        let count = (MAX_BATCH_BYTES / per_value) + 2; // enough to exceed the cap
        let batch: Vec<Record> = (0..count).map(|_| record_of_value_len(per_value)).collect();

        let expected = count * (4 + per_value);
        assert_eq!(
            validate_batch(&batch),
            Err(BatchRejection::TooLarge {
                max: MAX_BATCH_BYTES,
                submitted: expected,
            })
        );
    }

    #[test]
    fn validate_batch_precedence_per_record_before_count_and_bytes() {
        // A batch that is simultaneously over the record count AND contains an
        // oversized record reports the per-record failure first (per-record
        // size precedes the count and byte checks).
        let mut batch: Vec<Record> = (0..=MAX_BATCH_RECORDS)
            .map(|_| Record::new(None, Vec::new()))
            .collect();
        batch[0] = record_of_value_len(MAX_RECORD_BYTES + 1);
        assert_eq!(
            validate_batch(&batch),
            Err(BatchRejection::RecordTooLarge {
                index: 0,
                size: MAX_RECORD_BYTES + 1,
            })
        );
    }

    #[test]
    fn encode_decode_round_trips_record_values_in_order() {
        // Keys are not persisted; only values are framed, in batch order.
        let batch = vec![
            Record::new(Some(b"k0".to_vec()), b"first".to_vec()),
            Record::new(None, b"second".to_vec()),
            Record::new(Some(b"k2".to_vec()), b"third".to_vec()),
        ];
        let decoded = decode_record_batch(&encode_record_batch(&batch));
        assert_eq!(
            decoded,
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
        );
    }

    #[test]
    fn encode_decode_preserves_empty_values_and_empty_batch() {
        // An empty batch encodes to no bytes and decodes back to no values.
        assert!(encode_record_batch(&[]).is_empty());
        assert_eq!(decode_record_batch(&[]), Vec::<Vec<u8>>::new());

        // Zero-length values are distinct, ordered frames that round-trip.
        let batch = vec![
            Record::new(None, Vec::new()),
            Record::new(None, b"x".to_vec()),
            Record::new(None, Vec::new()),
        ];
        let decoded = decode_record_batch(&encode_record_batch(&batch));
        assert_eq!(decoded, vec![Vec::new(), b"x".to_vec(), Vec::new()]);
    }

    #[test]
    fn encoded_size_matches_validate_batch_byte_accounting() {
        // The codec's output length equals the byte total `validate_batch`
        // checks against `MAX_BATCH_BYTES`: 4 (prefix) + value len per record.
        let batch = vec![
            Record::new(Some(b"key".to_vec()), b"hello".to_vec()),
            Record::new(None, vec![0u8; 100]),
        ];
        let encoded_len = encode_record_batch(&batch).len();
        assert_eq!(encoded_len, (4 + 5) + (4 + 100));
    }

    #[test]
    fn decode_ignores_a_truncated_trailing_frame() {
        // A well-formed frame followed by a prefix promising more bytes than
        // remain decodes the complete frame and drops the truncated tail.
        let mut bytes = encode_record_batch(&[Record::new(None, b"ok".to_vec())]);
        bytes.extend_from_slice(&99u32.to_le_bytes());
        bytes.extend_from_slice(b"short");
        assert_eq!(decode_record_batch(&bytes), vec![b"ok".to_vec()]);
    }

    #[test]
    fn produce_batch_commits_and_returns_contiguous_base_and_count() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // A first batch of three commits at base 0, count 3 (Requirement 1.3,
        // 2.4).
        let batch = vec![
            Record::new(None, b"a".to_vec()),
            Record::new(Some(b"k".to_vec()), b"b".to_vec()),
            Record::new(None, b"c".to_vec()),
        ];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Ok((0, 3))
        );

        // A second batch picks up immediately after, still contiguous and
        // gap-free (Requirement 2.5, 4.5).
        let batch2 = vec![
            Record::new(None, b"d".to_vec()),
            Record::new(None, b"e".to_vec()),
        ];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch2,
                &mut clock
            ),
            Ok((3, 2))
        );

        // The records are readable back in order at their assigned offsets,
        // values byte-for-byte intact (Requirement 1.2, 10.2).
        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), Some(4));
        let records = replica.read(0, 100);
        let values: Vec<Vec<u8>> = records.into_iter().map(|r| r.value).collect();
        assert_eq!(
            values,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
            ]
        );
    }

    #[test]
    fn produce_batch_single_record_matches_single_produce_offset() {
        // A one-record batch takes the same offset the single-record path would
        // assign for the same partition state (Requirement 4.2).
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let single = Record::new(None, b"first".to_vec());
        assert_eq!(
            produce(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &single,
                &mut clock
            ),
            Ok(0)
        );

        let batch = vec![Record::new(None, b"second".to_vec())];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Ok((1, 1))
        );
    }

    #[test]
    fn produce_batch_rejects_empty_and_appends_nothing() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &[],
                &mut clock
            ),
            Err(CoreError::EmptyBatch)
        );

        // Nothing was appended (Requirement 3.6).
        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), None);
    }

    #[test]
    fn produce_batch_rejects_oversized_record_with_index_and_size() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // The second record (index 1) is one byte over the 1 MiB limit
        // (Requirement 3.2).
        let batch = vec![
            Record::new(None, b"ok".to_vec()),
            Record::new(None, vec![0u8; MAX_RECORD_BYTES + 1]),
        ];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Err(CoreError::RecordTooLargeAt {
                index: 1,
                size: MAX_RECORD_BYTES + 1,
            })
        );

        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), None);
    }

    #[test]
    fn produce_batch_rejects_too_many_records() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // One record over the count ceiling (Requirement 3.3). Empty values keep
        // the encoded size tiny so the count check is what fires.
        let batch: Vec<Record> = (0..=MAX_BATCH_RECORDS)
            .map(|_| Record::new(None, Vec::new()))
            .collect();
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Err(CoreError::BatchTooManyRecords {
                max: MAX_BATCH_RECORDS,
                submitted: MAX_BATCH_RECORDS + 1,
            })
        );

        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), None);
    }

    #[test]
    fn produce_batch_rejects_oversized_encoded_batch() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        // Enough ~1 MiB records to overflow the 16 MiB batch ceiling while
        // staying under the record-count limit (Requirement 3.4).
        let per_value = MAX_RECORD_BYTES;
        let count = (MAX_BATCH_BYTES / per_value) + 2;
        let batch: Vec<Record> = (0..count)
            .map(|_| Record::new(None, vec![0u8; per_value]))
            .collect();
        let expected = count * (4 + per_value);
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Err(CoreError::BatchTooLarge {
                max: MAX_BATCH_BYTES,
                submitted: expected,
            })
        );

        let replica = fleet
            .get(&("orders".to_string(), PartitionIndex(0)))
            .unwrap();
        assert_eq!(replica.high_water_mark(), None);
    }

    #[test]
    fn produce_batch_to_a_non_leader_is_redirected_and_appends_nothing() {
        let mut clock = TestClock::new();
        // The believed leader is node-1, but the locally hosted replica (node-0)
        // is a follower (never elected).
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-1")));
        let mut fleet = RaftGroupFleet::new();
        let key = ("orders".to_string(), PartitionIndex(0));
        fleet
            .create_group(
                key.clone(),
                RaftNodeId(0),
                vec![RaftNodeId(1), RaftNodeId(2)],
            )
            .unwrap();
        assert_eq!(fleet.get(&key).unwrap().role(), Role::Follower);

        let batch = vec![Record::new(None, b"v".to_vec())];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Err(CoreError::NotLeader {
                leader: Some(NodeId::new("node-1")),
            })
        );

        // The non-leader appended nothing (Requirement 6.1).
        assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }

    #[test]
    fn produce_batch_to_a_missing_topic_is_rejected() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let batch = vec![Record::new(None, b"v".to_vec())];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "ghost",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Err(CoreError::TopicNotFound("ghost".to_string()))
        );
    }

    #[test]
    fn produce_batch_to_a_missing_partition_is_rejected() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let batch = vec![Record::new(None, b"v".to_vec())];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(7),
                &batch,
                &mut clock
            ),
            Err(CoreError::PartitionNotFound {
                topic: "orders".to_string(),
                index: 7,
            })
        );
    }

    #[test]
    fn produce_batch_to_a_deleting_topic_is_rejected() {
        let mut clock = TestClock::new();
        let mut meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));
        meta.topics.get_mut("orders").unwrap().state = TopicState::Deleting;
        let mut fleet = fleet_with_leader("orders", &mut clock);

        let batch = vec![Record::new(None, b"v".to_vec())];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Err(CoreError::TopicDeleting("orders".to_string()))
        );
    }

    #[test]
    fn uncommitted_batch_in_a_multi_node_group_times_out() {
        let mut clock = TestClock::new();
        let meta = metadata_with_topic("orders", 1, Some(NodeId::new("node-0")));

        // A 3-node group whose leader cannot reach a commit majority because no
        // follower acknowledges.
        let mut fleet = RaftGroupFleet::new();
        let key = ("orders".to_string(), PartitionIndex(0));
        fleet
            .create_group(
                key.clone(),
                RaftNodeId(0),
                vec![RaftNodeId(1), RaftNodeId(2)],
            )
            .unwrap();

        // Drive node-0 to leader: an election self-vote plus one granted reply
        // is a majority of three.
        let replica = fleet.get_mut(&key).unwrap();
        replica.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        replica.step(
            RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                term: replica.raft().current_term(),
                vote_granted: true,
                voter: RaftNodeId(1),
            })),
            &mut clock,
        );
        assert_eq!(replica.role(), Role::Leader);

        // The proposed batch entry is appended but cannot commit without
        // follower acks, so produce_batch times out without advancing the
        // committed offset (Requirement 6.3).
        let batch = vec![
            Record::new(None, b"a".to_vec()),
            Record::new(None, b"b".to_vec()),
        ];
        assert_eq!(
            produce_batch(
                &meta,
                &mut fleet,
                "orders",
                PartitionIndex(0),
                &batch,
                &mut clock
            ),
            Err(CoreError::CommitTimeout)
        );
        assert_eq!(fleet.get(&key).unwrap().high_water_mark(), None);
    }
}
