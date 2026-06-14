//! Conversions across the gRPC boundary.
//!
//! `vela-server` is the only crate that touches both the protobuf wire types in
//! [`vela_proto::v1`] and the in-memory consensus/domain types in
//! [`vela_raft`]/[`vela_core`]. This module is the single place those two worlds
//! are translated into one another:
//!
//! - **Raft RPCs** — [`vela_proto::v1::RequestVoteRequest`] /
//!   [`AppendEntriesRequest`](vela_proto::v1::AppendEntriesRequest) and their
//!   replies ↔ [`vela_raft::RaftMessage`] variants (Requirement 12.3).
//! - **Log entries and payloads** — [`vela_proto::v1::LogEntry`] /
//!   [`EntryPayload`](vela_proto::v1::EntryPayload) ↔ [`vela_raft::LogEntry`] /
//!   [`vela_raft::EntryPayload`].
//! - **Records** — [`vela_proto::v1::Record`] ↔ [`vela_core::Record`].
//! - **Topic / partition / cluster metadata** — the admin and `SyncMetadata`
//!   shapes ↔ [`vela_core`] model types.
//! - **Typed errors** — [`vela_core::CoreError`] → [`vela_proto::v1::VelaError`]
//!   and a [`tonic::Status`] carrying it (Requirement 12.4, 11.2). The storage
//!   error family ([`vela_log::LogError`]) gets its own typed mapping for the
//!   rare case it surfaces directly; see the error section below for why Raft
//!   and most log failures reach this boundary already folded into a
//!   `CoreError`.
//!
//! ## Payload note (key persistence)
//!
//! `vela-core`'s state machine stores a produced record's **value** bytes as the
//! opaque log payload (its produce path appends `record.value`). The wire
//! `Record` carries an optional `key`, but the in-memory log keeps only the
//! value, so converting a record payload onto and off the wire is value-only;
//! keys are not persisted in this milestone. The conversions are written to make
//! that explicit rather than silently lossy.

use prost::Message as _;

use vela_core::{
    ClusterMetadata, CoreError, Member, NodeAvailability, NodeId, Partition, PartitionIndex,
    Record, Topic, TopicState,
};
use vela_log::LogError;
use vela_raft::{
    AppendEntries, AppendEntriesReply, EntryPayload, LogEntry, NodeId as RaftNodeId, PayloadKind,
    RequestVote, RequestVoteReply,
};

use vela_proto::v1;

use crate::registry::raft_node_id;

// ---------------------------------------------------------------------------
// Records and payloads
// ---------------------------------------------------------------------------

/// Convert a wire [`v1::Record`] into a domain [`Record`].
pub fn record_from_proto(record: v1::Record) -> Record {
    Record::new(record.key, record.value)
}

/// Convert a domain [`Record`] into a wire [`v1::Record`].
pub fn record_to_proto(record: &Record) -> v1::Record {
    v1::Record {
        key: record.key.clone(),
        value: record.value.clone(),
    }
}

/// Convert a wire [`v1::EntryPayload`] into a consensus [`EntryPayload`].
///
/// A `Record` payload keeps only its value bytes (see the module note); a
/// `Noop` becomes an empty no-op payload; a `Cluster` command is re-encoded to
/// its protobuf bytes so it round-trips. An unset oneof defaults to a no-op.
pub fn entry_payload_from_proto(payload: v1::EntryPayload) -> EntryPayload {
    match payload.kind {
        Some(v1::entry_payload::Kind::Record(record)) => {
            EntryPayload::new(PayloadKind::Record, record.value)
        }
        Some(v1::entry_payload::Kind::Noop(_)) => EntryPayload::new(PayloadKind::Noop, Vec::new()),
        Some(v1::entry_payload::Kind::Cluster(command)) => {
            EntryPayload::new(PayloadKind::Cluster, command.encode_to_vec())
        }
        None => EntryPayload::new(PayloadKind::Noop, Vec::new()),
    }
}

/// Convert a consensus [`EntryPayload`] into a wire [`v1::EntryPayload`].
///
/// A `Record` payload is rebuilt as a keyless [`v1::Record`] carrying the stored
/// value bytes; a `Noop` becomes the no-op marker; a `Cluster` payload's bytes
/// are decoded back into a [`v1::ClusterCommand`] (falling back to an empty
/// command if the bytes are not a valid encoding).
pub fn entry_payload_to_proto(payload: &EntryPayload) -> v1::EntryPayload {
    let kind = match payload.kind {
        PayloadKind::Record => v1::entry_payload::Kind::Record(v1::Record {
            key: None,
            value: payload.bytes.clone(),
        }),
        PayloadKind::Noop => v1::entry_payload::Kind::Noop(v1::Noop {}),
        PayloadKind::Cluster => {
            let command = v1::ClusterCommand::decode(payload.bytes.as_slice())
                .unwrap_or(v1::ClusterCommand { command: None });
            v1::entry_payload::Kind::Cluster(command)
        }
    };
    v1::EntryPayload { kind: Some(kind) }
}

/// Convert a wire [`v1::LogEntry`] into a consensus [`LogEntry`].
pub fn log_entry_from_proto(entry: v1::LogEntry) -> LogEntry {
    LogEntry {
        index: entry.index,
        term: entry.term,
        payload: entry
            .payload
            .map(entry_payload_from_proto)
            .unwrap_or_else(|| EntryPayload::new(PayloadKind::Noop, Vec::new())),
    }
}

/// Convert a consensus [`LogEntry`] into a wire [`v1::LogEntry`].
pub fn log_entry_to_proto(entry: &LogEntry) -> v1::LogEntry {
    v1::LogEntry {
        index: entry.index,
        term: entry.term,
        payload: Some(entry_payload_to_proto(&entry.payload)),
    }
}

// ---------------------------------------------------------------------------
// Raft RPCs (Requirement 12.3)
// ---------------------------------------------------------------------------

/// Build a wire [`v1::RequestVoteRequest`] for a `(topic, partition)` from a
/// consensus [`RequestVote`]. `self_id` is the string identity of this node,
/// which is always the candidate for an outbound vote request.
pub fn request_vote_to_proto(
    rv: &RequestVote,
    topic: &str,
    partition: u32,
    self_id: &str,
) -> v1::RequestVoteRequest {
    v1::RequestVoteRequest {
        topic: topic.to_string(),
        partition,
        term: rv.term,
        candidate_id: self_id.to_string(),
        last_log_index: rv.last_log_index,
        last_log_term: rv.last_log_term,
    }
}

/// Parse a wire [`v1::RequestVoteRequest`] into a consensus [`RequestVote`],
/// mapping the string `candidate_id` to its numeric [`RaftNodeId`].
pub fn request_vote_from_proto(req: &v1::RequestVoteRequest) -> RequestVote {
    RequestVote {
        term: req.term,
        candidate_id: raft_node_id(&req.candidate_id),
        last_log_index: req.last_log_index,
        last_log_term: req.last_log_term,
    }
}

/// Convert a consensus [`RequestVoteReply`] into its wire form.
pub fn request_vote_reply_to_proto(reply: &RequestVoteReply) -> v1::RequestVoteReply {
    v1::RequestVoteReply {
        term: reply.term,
        vote_granted: reply.vote_granted,
    }
}

/// Convert a wire [`v1::RequestVoteReply`] into a consensus [`RequestVoteReply`].
pub fn request_vote_reply_from_proto(reply: &v1::RequestVoteReply) -> RequestVoteReply {
    RequestVoteReply {
        term: reply.term,
        vote_granted: reply.vote_granted,
    }
}

/// Build a wire [`v1::AppendEntriesRequest`] for a `(topic, partition)` from a
/// consensus [`AppendEntries`]. `self_id` is this node's string identity, which
/// is always the leader for an outbound append.
pub fn append_entries_to_proto(
    ae: &AppendEntries,
    topic: &str,
    partition: u32,
    self_id: &str,
) -> v1::AppendEntriesRequest {
    v1::AppendEntriesRequest {
        topic: topic.to_string(),
        partition,
        term: ae.term,
        leader_id: self_id.to_string(),
        prev_log_index: ae.prev_log_index,
        prev_log_term: ae.prev_log_term,
        entries: ae.entries.iter().map(log_entry_to_proto).collect(),
        leader_commit: ae.leader_commit,
    }
}

/// Parse a wire [`v1::AppendEntriesRequest`] into a consensus [`AppendEntries`],
/// mapping the string `leader_id` to its numeric [`RaftNodeId`].
pub fn append_entries_from_proto(req: &v1::AppendEntriesRequest) -> AppendEntries {
    AppendEntries {
        term: req.term,
        leader_id: raft_node_id(&req.leader_id),
        prev_log_index: req.prev_log_index,
        prev_log_term: req.prev_log_term,
        entries: req
            .entries
            .iter()
            .cloned()
            .map(log_entry_from_proto)
            .collect(),
        leader_commit: req.leader_commit,
    }
}

/// Convert a consensus [`AppendEntriesReply`] into its wire form.
///
/// The wire reply carries `term`, `success`, and a `conflict_hint`; the
/// consensus reply's `from` and `match_index` are not on the wire — the leader
/// reconstructs them from the request it sent (see
/// [`append_entries_reply_from_proto`]).
pub fn append_entries_reply_to_proto(reply: &AppendEntriesReply) -> v1::AppendEntriesReply {
    v1::AppendEntriesReply {
        term: reply.term,
        success: reply.success,
        conflict_hint: reply.conflict_index,
    }
}

/// Reconstruct a consensus [`AppendEntriesReply`] from its wire form plus the
/// context the leader holds from the originating request.
///
/// The wire reply omits the responder (`from`) and the matched index, so the
/// leader supplies `from` (the peer it called) and `match_index` (the last
/// index it sent, on success) when feeding the reply back into its Raft node.
pub fn append_entries_reply_from_proto(
    reply: &v1::AppendEntriesReply,
    from: RaftNodeId,
    match_index: Option<u64>,
) -> AppendEntriesReply {
    AppendEntriesReply {
        from,
        term: reply.term,
        success: reply.success,
        conflict_index: reply.conflict_hint,
        match_index: if reply.success { match_index } else { None },
    }
}

// ---------------------------------------------------------------------------
// Topic / partition / cluster metadata
// ---------------------------------------------------------------------------

/// Map a domain [`NodeAvailability`] to its wire enum value.
pub fn availability_to_proto(availability: NodeAvailability) -> v1::NodeAvailability {
    match availability {
        NodeAvailability::Available => v1::NodeAvailability::Available,
        NodeAvailability::Unavailable => v1::NodeAvailability::Unavailable,
    }
}

/// Map a wire availability enum value to a domain [`NodeAvailability`].
///
/// The unspecified/zero value is treated as unavailable, the conservative
/// default for a node whose state has not been established.
pub fn availability_from_proto(value: i32) -> NodeAvailability {
    match v1::NodeAvailability::try_from(value) {
        Ok(v1::NodeAvailability::Available) => NodeAvailability::Available,
        _ => NodeAvailability::Unavailable,
    }
}

/// Convert a domain [`Partition`] into a wire [`v1::PartitionInfo`].
pub fn partition_to_proto(partition: &Partition) -> v1::PartitionInfo {
    v1::PartitionInfo {
        index: partition.index.0,
        replicas: partition.replicas.iter().map(|n| n.0.clone()).collect(),
        leader: partition.leader.as_ref().map(|n| n.0.clone()),
    }
}

/// Convert a wire [`v1::PartitionInfo`] into a domain [`Partition`].
pub fn partition_from_proto(info: &v1::PartitionInfo) -> Partition {
    Partition {
        index: PartitionIndex(info.index),
        replicas: info.replicas.iter().map(NodeId::new).collect(),
        leader: info.leader.as_ref().map(NodeId::new),
    }
}

/// Convert a domain [`Topic`] into a wire [`v1::TopicInfo`].
pub fn topic_to_proto(topic: &Topic) -> v1::TopicInfo {
    v1::TopicInfo {
        name: topic.name.clone(),
        partition_count: topic.partitions.len() as u32,
        partitions: topic.partitions.iter().map(partition_to_proto).collect(),
    }
}

/// Convert a wire [`v1::TopicInfo`] into a domain [`Topic`] in the `Active`
/// state (the only state a propagated metadata snapshot carries).
pub fn topic_from_proto(info: &v1::TopicInfo) -> Topic {
    Topic {
        name: info.name.clone(),
        partitions: info.partitions.iter().map(partition_from_proto).collect(),
        state: TopicState::Active,
    }
}

/// Convert a domain [`Member`] into a wire [`v1::Member`].
pub fn member_to_proto(member: &Member) -> v1::Member {
    v1::Member {
        id: member.id.0.clone(),
        addr: member.addr.clone(),
        availability: availability_to_proto(member.availability) as i32,
    }
}

/// Convert a wire [`v1::Member`] into a domain [`Member`].
pub fn member_from_proto(member: &v1::Member) -> Member {
    Member {
        id: NodeId::new(&member.id),
        addr: member.addr.clone(),
        availability: availability_from_proto(member.availability),
    }
}

/// Convert a domain [`ClusterMetadata`] view into a wire [`v1::ClusterMetadata`].
pub fn cluster_metadata_to_proto(metadata: &ClusterMetadata) -> v1::ClusterMetadata {
    v1::ClusterMetadata {
        members: metadata.members.iter().map(member_to_proto).collect(),
        topics: metadata.topics.values().map(topic_to_proto).collect(),
        epoch: metadata.epoch,
    }
}

/// Convert a wire [`v1::ClusterMetadata`] snapshot into a domain
/// [`ClusterMetadata`] view.
pub fn cluster_metadata_from_proto(metadata: &v1::ClusterMetadata) -> ClusterMetadata {
    let mut out = ClusterMetadata::new();
    out.members = metadata.members.iter().map(member_from_proto).collect();
    out.epoch = metadata.epoch;
    for topic in &metadata.topics {
        out.topics
            .insert(topic.name.clone(), topic_from_proto(topic));
    }
    out
}

// ---------------------------------------------------------------------------
// Typed errors (Requirement 12.4, 11.2)
// ---------------------------------------------------------------------------
//
// The design's error taxonomy names three families — `CoreError`, `RaftError`,
// and `LogError` — and requires every one of them to map to the single wire
// `VelaError` (code + message + optional leader hint) at the gRPC boundary
// (Requirement 12.4, 11.2). In this codebase those families collapse onto two
// concrete types at this seam:
//
// - **`CoreError`** is the unified domain error and the only error a client RPC
//   handler returns. Consensus failures that the design sketched as a separate
//   `RaftError` (`NotLeader`, `CommitTimeout`) are surfaced *through* `CoreError`
//   (`CoreError::NotLeader`, `CoreError::CommitTimeout`, plus
//   `CoreError::PartitionUnavailable`); there is no standalone `RaftError` type
//   to map. `core_error_to_vela_error` therefore carries the whole leadership
//   and timeout story, including the leader redirect hint (Requirement 11.2).
//
// - **`LogError`** is the append-only log's storage error
//   ([`vela_log::LogError`]). It lives behind the `LogStorage` seam and is
//   consumed inside `vela-raft`/`vela-core`, so it is normally folded into a
//   `CoreError` long before a request unwinds to this boundary — a client never
//   sees a raw `CommitOutOfBounds`. It is mapped here anyway, as the `Internal`
//   classification, so the storage family has a typed `VelaError` path of its
//   own should one ever reach the wire (e.g. logged or bridged by a future
//   handler). This keeps "every error family maps to a `VelaError`" literally
//   true rather than relying on the `CoreError` funnel alone.

/// Map a [`CoreError`] onto the shared wire [`v1::VelaError`], preserving the
/// classification code, a human-readable message, and the leader redirect hint
/// where one applies (Requirement 12.4, 11.2).
pub fn core_error_to_vela_error(error: &CoreError) -> v1::VelaError {
    let (code, leader) = match error {
        CoreError::InvalidTopicName
        | CoreError::InvalidPartitionCount(_)
        | CoreError::InvalidConsumeParams => (v1::ErrorCode::Validation, None),
        CoreError::TopicNotFound(_) => (v1::ErrorCode::TopicNotFound, None),
        CoreError::PartitionNotFound { .. } => (v1::ErrorCode::PartitionNotFound, None),
        CoreError::TopicExists(_) => (v1::ErrorCode::TopicExists, None),
        CoreError::TopicDeleting(_) => (v1::ErrorCode::TopicDeleting, None),
        CoreError::RecordTooLarge(_) => (v1::ErrorCode::PayloadTooLarge, None),
        CoreError::InsufficientNodes { .. } => (v1::ErrorCode::InsufficientNodes, None),
        CoreError::PartitionUnavailable => (v1::ErrorCode::PartitionUnavailable, None),
        CoreError::NotLeader { leader } => (
            v1::ErrorCode::NotLeader,
            leader.as_ref().map(|n| n.0.clone()),
        ),
        CoreError::CommitTimeout => (v1::ErrorCode::CommitTimeout, None),
        CoreError::MetadataPropagation(_) => (v1::ErrorCode::PropagationTimeout, None),
    };
    v1::VelaError {
        code: code as i32,
        message: error.to_string(),
        leader,
    }
}

/// Map a [`LogError`] onto the shared wire [`v1::VelaError`].
///
/// Every storage failure is classified [`v1::ErrorCode::Internal`]: these are
/// log-invariant violations (a commit or revert that would corrupt the
/// committed prefix, or non-contiguous replicated entries) rather than
/// client-actionable conditions, and they are normally caught and folded into a
/// [`CoreError`] inside the consensus/domain layers before reaching this
/// boundary. The original message is preserved verbatim so the specific
/// invariant is still legible on the wire, and the mapping carries no leader
/// hint (a storage error is not a redirect). This exists so the storage error
/// family has a typed `VelaError` path of its own (Requirement 12.4).
pub fn log_error_to_vela_error(error: &LogError) -> v1::VelaError {
    v1::VelaError {
        code: v1::ErrorCode::Internal as i32,
        message: error.to_string(),
        leader: None,
    }
}

/// The gRPC status code best matching an [`ErrorCode`](v1::ErrorCode).
fn status_code_for(code: v1::ErrorCode) -> tonic::Code {
    use tonic::Code;
    match code {
        v1::ErrorCode::Validation | v1::ErrorCode::PayloadTooLarge => Code::InvalidArgument,
        v1::ErrorCode::TopicNotFound | v1::ErrorCode::PartitionNotFound => Code::NotFound,
        v1::ErrorCode::TopicExists => Code::AlreadyExists,
        v1::ErrorCode::TopicDeleting
        | v1::ErrorCode::NotLeader
        | v1::ErrorCode::InsufficientNodes => Code::FailedPrecondition,
        v1::ErrorCode::PartitionUnavailable => Code::Unavailable,
        v1::ErrorCode::CommitTimeout | v1::ErrorCode::PropagationTimeout => Code::DeadlineExceeded,
        v1::ErrorCode::Internal | v1::ErrorCode::Unspecified => Code::Internal,
    }
}

/// Wrap a typed [`v1::VelaError`] in a [`tonic::Status`] whose gRPC code matches
/// the error's classification and whose details carry the encoded `VelaError`,
/// so a client can recover the precise code and leader hint with
/// [`vela_error_from_status`]. This is the single place the wire error is put
/// onto a `Status`, shared by every per-family `*_to_status` helper.
pub fn vela_error_to_status(vela_error: &v1::VelaError) -> tonic::Status {
    let code = v1::ErrorCode::try_from(vela_error.code).unwrap_or(v1::ErrorCode::Internal);
    let details = bytes::Bytes::from(vela_error.encode_to_vec());
    tonic::Status::with_details(status_code_for(code), vela_error.message.clone(), details)
}

/// Map a [`CoreError`] onto a [`tonic::Status`] that carries the typed
/// [`v1::VelaError`] in its details, so a client can recover the precise
/// classification and leader hint (Requirement 12.4, 11.2).
pub fn core_error_to_status(error: &CoreError) -> tonic::Status {
    vela_error_to_status(&core_error_to_vela_error(error))
}

/// Map a [`LogError`] onto a [`tonic::Status`] carrying the typed
/// [`v1::VelaError`] in its details, the storage-family counterpart of
/// [`core_error_to_status`].
pub fn log_error_to_status(error: &LogError) -> tonic::Status {
    vela_error_to_status(&log_error_to_vela_error(error))
}

/// Recover the typed [`v1::VelaError`] carried in a [`tonic::Status`]'s details,
/// if present and well-formed. The inverse of [`core_error_to_status`], used by
/// the client to read the classification and leader hint off a failed RPC.
pub fn vela_error_from_status(status: &tonic::Status) -> Option<v1::VelaError> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    v1::VelaError::decode(details).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- payloads and records --------------------------------------------

    #[test]
    fn record_round_trips_through_proto() {
        let record = Record::new(Some(b"k".to_vec()), b"v".to_vec());
        let back = record_from_proto(record_to_proto(&record));
        assert_eq!(back, record);
    }

    #[test]
    fn record_entry_payload_keeps_value_bytes() {
        // A wire Record payload keeps only its value bytes on the consensus side
        // (keys are not persisted this milestone).
        let proto = v1::EntryPayload {
            kind: Some(v1::entry_payload::Kind::Record(v1::Record {
                key: Some(b"key".to_vec()),
                value: b"value".to_vec(),
            })),
        };
        let payload = entry_payload_from_proto(proto);
        assert_eq!(payload.kind, PayloadKind::Record);
        assert_eq!(payload.bytes, b"value".to_vec());

        // Converting back produces a keyless record carrying the value.
        let back = entry_payload_to_proto(&payload);
        match back.kind {
            Some(v1::entry_payload::Kind::Record(r)) => {
                assert_eq!(r.key, None);
                assert_eq!(r.value, b"value".to_vec());
            }
            other => panic!("expected a record payload, got {other:?}"),
        }
    }

    #[test]
    fn noop_payload_round_trips() {
        let payload = EntryPayload::new(PayloadKind::Noop, Vec::new());
        let proto = entry_payload_to_proto(&payload);
        assert!(matches!(proto.kind, Some(v1::entry_payload::Kind::Noop(_))));
        assert_eq!(entry_payload_from_proto(proto), payload);
    }

    #[test]
    fn cluster_payload_round_trips_through_proto_bytes() {
        // A Cluster payload's bytes are a prost-encoded ClusterCommand and must
        // survive a there-and-back conversion.
        let command = v1::ClusterCommand {
            command: Some(v1::cluster_command::Command::DeleteTopic(
                v1::DeleteTopicCommand {
                    name: "orders".to_string(),
                },
            )),
        };
        let payload = EntryPayload::new(PayloadKind::Cluster, command.encode_to_vec());
        let proto = entry_payload_to_proto(&payload);
        match proto.kind {
            Some(v1::entry_payload::Kind::Cluster(c)) => assert_eq!(c, command),
            other => panic!("expected a cluster payload, got {other:?}"),
        }
        assert_eq!(
            entry_payload_from_proto(entry_payload_to_proto(&payload)),
            payload
        );
    }

    #[test]
    fn unset_payload_oneof_defaults_to_noop() {
        let payload = entry_payload_from_proto(v1::EntryPayload { kind: None });
        assert_eq!(payload.kind, PayloadKind::Noop);
        assert!(payload.bytes.is_empty());
    }

    #[test]
    fn log_entry_round_trips_through_proto() {
        let entry = LogEntry {
            index: 7,
            term: 3,
            payload: EntryPayload::new(PayloadKind::Record, b"hello".to_vec()),
        };
        let back = log_entry_from_proto(log_entry_to_proto(&entry));
        assert_eq!(back, entry);
    }

    #[test]
    fn log_entry_without_payload_defaults_to_noop() {
        let proto = v1::LogEntry {
            index: 1,
            term: 1,
            payload: None,
        };
        let entry = log_entry_from_proto(proto);
        assert_eq!(entry.payload.kind, PayloadKind::Noop);
    }

    // ---- Raft RPCs --------------------------------------------------------

    #[test]
    fn request_vote_round_trips_with_id_mapping() {
        let rv = RequestVote {
            term: 5,
            candidate_id: raft_node_id("node-a"),
            last_log_index: Some(9),
            last_log_term: Some(4),
        };
        let proto = request_vote_to_proto(&rv, "orders", 2, "node-a");
        assert_eq!(proto.topic, "orders");
        assert_eq!(proto.partition, 2);
        assert_eq!(proto.candidate_id, "node-a");

        let back = request_vote_from_proto(&proto);
        assert_eq!(back, rv);
    }

    #[test]
    fn request_vote_reply_round_trips() {
        let reply = RequestVoteReply {
            term: 6,
            vote_granted: true,
        };
        let back = request_vote_reply_from_proto(&request_vote_reply_to_proto(&reply));
        assert_eq!(back, reply);
    }

    #[test]
    fn append_entries_round_trips_with_entries_and_ids() {
        let ae = AppendEntries {
            term: 8,
            leader_id: raft_node_id("node-leader"),
            prev_log_index: Some(2),
            prev_log_term: Some(7),
            entries: vec![
                LogEntry {
                    index: 3,
                    term: 8,
                    payload: EntryPayload::new(PayloadKind::Record, b"a".to_vec()),
                },
                LogEntry {
                    index: 4,
                    term: 8,
                    payload: EntryPayload::new(PayloadKind::Noop, Vec::new()),
                },
            ],
            leader_commit: Some(2),
        };
        let proto = append_entries_to_proto(&ae, "events", 1, "node-leader");
        assert_eq!(proto.topic, "events");
        assert_eq!(proto.partition, 1);
        assert_eq!(proto.leader_id, "node-leader");
        assert_eq!(proto.entries.len(), 2);

        let back = append_entries_from_proto(&proto);
        assert_eq!(back, ae);
    }

    #[test]
    fn append_entries_reply_to_proto_drops_unwired_fields() {
        let reply = AppendEntriesReply {
            from: raft_node_id("node-f"),
            term: 4,
            success: false,
            conflict_index: Some(2),
            match_index: None,
        };
        let proto = append_entries_reply_to_proto(&reply);
        assert_eq!(proto.term, 4);
        assert!(!proto.success);
        assert_eq!(proto.conflict_hint, Some(2));
    }

    #[test]
    fn append_entries_reply_from_proto_reconstructs_context() {
        let from = raft_node_id("node-f");

        // On success, the leader-supplied match index is carried through.
        let success = v1::AppendEntriesReply {
            term: 4,
            success: true,
            conflict_hint: None,
        };
        let reply = append_entries_reply_from_proto(&success, from, Some(10));
        assert_eq!(reply.from, from);
        assert!(reply.success);
        assert_eq!(reply.match_index, Some(10));
        assert_eq!(reply.conflict_index, None);

        // On rejection, the match index is forced to None regardless of input.
        let rejected = v1::AppendEntriesReply {
            term: 5,
            success: false,
            conflict_hint: Some(3),
        };
        let reply = append_entries_reply_from_proto(&rejected, from, Some(10));
        assert!(!reply.success);
        assert_eq!(reply.match_index, None);
        assert_eq!(reply.conflict_index, Some(3));
    }

    // ---- metadata ---------------------------------------------------------

    #[test]
    fn partition_round_trips_through_proto() {
        let partition = Partition {
            index: PartitionIndex(2),
            replicas: vec![NodeId::new("a"), NodeId::new("b")],
            leader: Some(NodeId::new("a")),
        };
        let back = partition_from_proto(&partition_to_proto(&partition));
        assert_eq!(back, partition);
    }

    #[test]
    fn topic_info_reports_partition_count() {
        let topic = Topic {
            name: "orders".to_string(),
            partitions: vec![
                Partition {
                    index: PartitionIndex(0),
                    replicas: vec![NodeId::new("a")],
                    leader: Some(NodeId::new("a")),
                },
                Partition {
                    index: PartitionIndex(1),
                    replicas: vec![NodeId::new("a")],
                    leader: None,
                },
            ],
            state: TopicState::Active,
        };
        let proto = topic_to_proto(&topic);
        assert_eq!(proto.partition_count, 2);
        assert_eq!(proto.partitions.len(), 2);

        let back = topic_from_proto(&proto);
        assert_eq!(back.name, topic.name);
        assert_eq!(back.partitions, topic.partitions);
        assert_eq!(back.state, TopicState::Active);
    }

    #[test]
    fn member_availability_round_trips_both_states() {
        for availability in [NodeAvailability::Available, NodeAvailability::Unavailable] {
            let member = Member {
                id: NodeId::new("node-a"),
                addr: "node-a:7001".to_string(),
                availability,
            };
            let back = member_from_proto(&member_to_proto(&member));
            assert_eq!(back, member);
        }
    }

    #[test]
    fn unspecified_availability_decodes_as_unavailable() {
        assert_eq!(
            availability_from_proto(v1::NodeAvailability::Unspecified as i32),
            NodeAvailability::Unavailable
        );
    }

    #[test]
    fn cluster_metadata_round_trips_through_proto() {
        let mut metadata = ClusterMetadata::new();
        metadata.members = vec![Member {
            id: NodeId::new("node-a"),
            addr: "node-a:7001".to_string(),
            availability: NodeAvailability::Available,
        }];
        metadata.epoch = 4;
        metadata.topics.insert(
            "orders".to_string(),
            Topic {
                name: "orders".to_string(),
                partitions: vec![Partition {
                    index: PartitionIndex(0),
                    replicas: vec![NodeId::new("node-a")],
                    leader: Some(NodeId::new("node-a")),
                }],
                state: TopicState::Active,
            },
        );

        let back = cluster_metadata_from_proto(&cluster_metadata_to_proto(&metadata));
        assert_eq!(back, metadata);
    }

    // ---- error mapping ----------------------------------------------------

    #[test]
    fn core_errors_map_to_their_error_codes() {
        let cases = [
            (CoreError::InvalidTopicName, v1::ErrorCode::Validation),
            (
                CoreError::InvalidPartitionCount(0),
                v1::ErrorCode::Validation,
            ),
            (CoreError::InvalidConsumeParams, v1::ErrorCode::Validation),
            (
                CoreError::TopicNotFound("x".to_string()),
                v1::ErrorCode::TopicNotFound,
            ),
            (
                CoreError::PartitionNotFound {
                    topic: "x".to_string(),
                    index: 0,
                },
                v1::ErrorCode::PartitionNotFound,
            ),
            (
                CoreError::TopicExists("x".to_string()),
                v1::ErrorCode::TopicExists,
            ),
            (
                CoreError::TopicDeleting("x".to_string()),
                v1::ErrorCode::TopicDeleting,
            ),
            (CoreError::RecordTooLarge(1), v1::ErrorCode::PayloadTooLarge),
            (
                CoreError::InsufficientNodes { have: 1, need: 3 },
                v1::ErrorCode::InsufficientNodes,
            ),
            (
                CoreError::PartitionUnavailable,
                v1::ErrorCode::PartitionUnavailable,
            ),
            (CoreError::CommitTimeout, v1::ErrorCode::CommitTimeout),
            (
                CoreError::MetadataPropagation(vec![NodeId::new("n")]),
                v1::ErrorCode::PropagationTimeout,
            ),
        ];
        for (error, expected) in cases {
            let vela = core_error_to_vela_error(&error);
            assert_eq!(vela.code, expected as i32, "mismatch for {error:?}");
            assert!(!vela.message.is_empty());
        }
    }

    #[test]
    fn not_leader_carries_leader_hint() {
        let error = CoreError::NotLeader {
            leader: Some(NodeId::new("node-7")),
        };
        let vela = core_error_to_vela_error(&error);
        assert_eq!(vela.code, v1::ErrorCode::NotLeader as i32);
        assert_eq!(vela.leader.as_deref(), Some("node-7"));
    }

    #[test]
    fn not_leader_without_a_known_leader_has_no_hint() {
        let vela = core_error_to_vela_error(&CoreError::NotLeader { leader: None });
        assert_eq!(vela.code, v1::ErrorCode::NotLeader as i32);
        assert_eq!(vela.leader, None);
    }

    #[test]
    fn status_carries_typed_error_in_details() {
        let error = CoreError::NotLeader {
            leader: Some(NodeId::new("node-7")),
        };
        let status = core_error_to_status(&error);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);

        // The typed error round-trips through the status details.
        let recovered = vela_error_from_status(&status).expect("details must carry a VelaError");
        assert_eq!(recovered.code, v1::ErrorCode::NotLeader as i32);
        assert_eq!(recovered.leader.as_deref(), Some("node-7"));
    }

    #[test]
    fn status_without_details_yields_no_vela_error() {
        let status = tonic::Status::new(tonic::Code::Internal, "boom");
        assert!(vela_error_from_status(&status).is_none());
    }

    /// Across a representative error from each behavioural family, the full
    /// `VelaError` — code, message, *and* leader hint — survives the trip onto a
    /// `tonic::Status` and back, the mapping this boundary owns (the universal
    /// round-trip property itself is Property 39 / task 14.6).
    #[test]
    fn typed_error_preserves_code_message_and_hint_through_status() {
        let cases = [
            CoreError::InvalidTopicName,
            CoreError::TopicNotFound("orders".to_string()),
            CoreError::PartitionNotFound {
                topic: "orders".to_string(),
                index: 2,
            },
            CoreError::RecordTooLarge(2 * 1024 * 1024),
            CoreError::NotLeader {
                leader: Some(NodeId::new("node-7")),
            },
            CoreError::NotLeader { leader: None },
            CoreError::CommitTimeout,
            CoreError::MetadataPropagation(vec![NodeId::new("node-9")]),
        ];
        for error in cases {
            let expected = core_error_to_vela_error(&error);
            let status = core_error_to_status(&error);
            // The status message mirrors the typed message ...
            assert_eq!(status.message(), expected.message, "message for {error:?}");
            // ... and the typed error decoded from the details is identical:
            // code, message and leader hint all preserved.
            let recovered =
                vela_error_from_status(&status).expect("details must carry a VelaError");
            assert_eq!(recovered.code, expected.code, "code for {error:?}");
            assert_eq!(recovered.message, expected.message, "message for {error:?}");
            assert_eq!(
                recovered.leader, expected.leader,
                "leader hint for {error:?}"
            );
        }
    }

    #[test]
    fn log_errors_map_to_internal_preserving_message() {
        // Every storage-family error classifies as Internal, keeps its message,
        // and carries no leader hint.
        let cases = [
            LogError::CommitOutOfBounds {
                requested: 9,
                current: Some(3),
                last: Some(5),
            },
            LogError::RevertBelowCommit {
                requested: 1,
                commit: Some(4),
            },
            LogError::NonContiguousEntries,
        ];
        for error in cases {
            let vela = log_error_to_vela_error(&error);
            assert_eq!(
                vela.code,
                v1::ErrorCode::Internal as i32,
                "code for {error:?}"
            );
            assert_eq!(vela.message, error.to_string(), "message for {error:?}");
            assert_eq!(vela.leader, None, "storage errors carry no leader hint");
            assert!(!vela.message.is_empty());
        }
    }

    #[test]
    fn log_error_round_trips_through_status_details() {
        let error = LogError::CommitOutOfBounds {
            requested: 9,
            current: Some(3),
            last: Some(5),
        };
        let status = log_error_to_status(&error);
        // An Internal classification maps to the gRPC Internal code.
        assert_eq!(status.code(), tonic::Code::Internal);

        let recovered = vela_error_from_status(&status).expect("details must carry a VelaError");
        assert_eq!(recovered.code, v1::ErrorCode::Internal as i32);
        assert_eq!(recovered.message, error.to_string());
        assert_eq!(recovered.leader, None);
    }
}
