//! Structural smoke test for `vela-proto`'s generated wire surface.
//!
//! This test asserts the structural invariants of Requirement 12 that
//! `vela-proto` is responsible for satisfying:
//!
//! * **Wire/error types live in `vela-proto`** — every protobuf message type,
//!   enum, and the shared typed [`VelaError`] are generated into
//!   `vela_proto::v1` (Requirements 12.1, 12.4). This crate is the single owner
//!   of the wire surface.
//! * **Both gRPC services are exposed** — the generated module surfaces the
//!   `VelaClient` (client-facing produce / consume / topic-admin) and `VelaPeer`
//!   (server-to-server Raft / membership / metadata) service surfaces, each with
//!   generated client stubs and server traits (Requirements 12.2, 12.3).
//!
//! The assertions are predominantly *compile-time*: naming a type or a generated
//! module path that does not exist fails the build, which is exactly the
//! structural guarantee we want to lock in. A handful of runtime assertions pin
//! down field/enum identities so the smoke test also catches accidental
//! type-level churn (e.g. a renamed `VelaError` field).

use vela_proto::v1;

// ---------------------------------------------------------------------------
// Wire/error types live in `vela-proto::v1` (Requirements 12.1, 12.4)
// ---------------------------------------------------------------------------

/// Naming each key wire type with its fully-qualified `vela_proto::v1` path is a
/// compile-time assertion that the type exists and is owned by this crate. If
/// any type moved out of `vela-proto` or was renamed, this stops compiling.
#[allow(dead_code)]
fn _wire_types_are_owned_by_vela_proto() {
    // Records, log entries, and payloads.
    let _: Option<v1::Record> = None;
    let _: Option<v1::Noop> = None;
    let _: Option<v1::EntryPayload> = None;
    let _: Option<v1::LogEntry> = None;

    // Raft RPCs and replies.
    let _: Option<v1::RequestVoteRequest> = None;
    let _: Option<v1::RequestVoteReply> = None;
    let _: Option<v1::AppendEntriesRequest> = None;
    let _: Option<v1::AppendEntriesReply> = None;

    // Produce / consume client messages.
    let _: Option<v1::ProduceRequest> = None;
    let _: Option<v1::ProduceResponse> = None;
    let _: Option<v1::ProduceBatchRequest> = None;
    let _: Option<v1::ProduceBatchResponse> = None;
    let _: Option<v1::RecordBatch> = None;
    let _: Option<v1::ConsumeRequest> = None;
    let _: Option<v1::ConsumedRecord> = None;
    let _: Option<v1::ConsumeResponse> = None;

    // Topic-admin messages.
    let _: Option<v1::CreateTopicRequest> = None;
    let _: Option<v1::CreateTopicResponse> = None;
    let _: Option<v1::DeleteTopicRequest> = None;
    let _: Option<v1::DeleteTopicResponse> = None;
    let _: Option<v1::ListTopicsRequest> = None;
    let _: Option<v1::ListTopicsResponse> = None;
    let _: Option<v1::DescribeTopicRequest> = None;
    let _: Option<v1::DescribeTopicResponse> = None;
    let _: Option<v1::FindLeaderRequest> = None;
    let _: Option<v1::FindLeaderResponse> = None;
    let _: Option<v1::TopicInfo> = None;
    let _: Option<v1::PartitionInfo> = None;

    // Cluster metadata, commands, and propagation.
    let _: Option<v1::Member> = None;
    let _: Option<v1::ClusterCommand> = None;
    let _: Option<v1::CreateTopicCommand> = None;
    let _: Option<v1::DeleteTopicCommand> = None;
    let _: Option<v1::SetAvailabilityCommand> = None;
    let _: Option<v1::ClusterMetadata> = None;
    let _: Option<v1::HeartbeatRequest> = None;
    let _: Option<v1::HeartbeatReply> = None;
    let _: Option<v1::SyncMetadataRequest> = None;
    let _: Option<v1::SyncMetadataReply> = None;

    // Shared typed error and enums.
    let _: Option<v1::VelaError> = None;
    let _: Option<v1::ErrorCode> = None;
    let _: Option<v1::NodeAvailability> = None;
    let _: Option<v1::LogBackend> = None;
}

/// The shared typed error is a `vela-proto`-owned message with a classification
/// `code`, a human-readable `message`, and an optional `leader` redirect hint
/// (Requirements 12.4, 11.2). Constructing one here pins those fields down.
#[test]
fn vela_error_is_owned_by_vela_proto_with_code_message_and_leader_hint() {
    let err = v1::VelaError {
        code: v1::ErrorCode::NotLeader as i32,
        message: "not the leader for this partition".to_string(),
        leader: Some("node-7".to_string()),
    };

    // The typed error carries an identifiable failure classification ...
    assert_eq!(err.code, v1::ErrorCode::NotLeader as i32);
    assert_eq!(
        v1::ErrorCode::try_from(err.code),
        Ok(v1::ErrorCode::NotLeader)
    );
    // ... a human-readable message ...
    assert_eq!(err.message, "not the leader for this partition");
    // ... and an optional leader hint used for redirection.
    assert_eq!(err.leader.as_deref(), Some("node-7"));
}

/// The error taxonomy is a `vela-proto`-owned enum. Naming the variants asserts
/// the classification surface used across both gRPC services (Requirement 12.4).
#[test]
fn error_code_taxonomy_is_exposed() {
    let codes = [
        v1::ErrorCode::Unspecified,
        v1::ErrorCode::Validation,
        v1::ErrorCode::TopicNotFound,
        v1::ErrorCode::PartitionNotFound,
        v1::ErrorCode::TopicExists,
        v1::ErrorCode::TopicDeleting,
        v1::ErrorCode::NotLeader,
        v1::ErrorCode::PartitionUnavailable,
        v1::ErrorCode::InsufficientNodes,
        v1::ErrorCode::PayloadTooLarge,
        v1::ErrorCode::CommitTimeout,
        v1::ErrorCode::PropagationTimeout,
        v1::ErrorCode::Internal,
    ];

    // Every code round-trips through its protobuf integer representation,
    // confirming the enum is fully generated in `vela-proto`.
    for code in codes {
        assert_eq!(v1::ErrorCode::try_from(code as i32), Ok(code));
    }
}

// ---------------------------------------------------------------------------
// Batched-produce wire surface (batched-produce Requirements 1.1, 1.4, 8.1, 8.3)
// ---------------------------------------------------------------------------

/// The batched-produce surface adds a `RecordBatch` payload arm, a
/// `ProduceBatchRequest` carrying an ordered set of records for one
/// (topic, partition), and a compact `ProduceBatchResponse { base_offset,
/// count }` (batched-produce Requirements 1.1, 1.4, 8.1, 8.3). Constructing each
/// message pins down its fields; setting the `record_batch` oneof arm asserts
/// the fourth `EntryPayload` variant exists so a batch replicates as one
/// `LogEntry`. The existing single-record `Produce` surface is left untouched.
#[test]
fn produce_batch_wire_surface_is_exposed() {
    let records = vec![
        v1::Record {
            key: None,
            value: b"a".to_vec(),
        },
        v1::Record {
            key: Some(b"k".to_vec()),
            value: b"b".to_vec(),
        },
    ];

    // The request carries the resolved (topic, partition) and the ordered batch.
    let request = v1::ProduceBatchRequest {
        topic: "orders".to_string(),
        partition: 2,
        records: records.clone(),
    };
    assert_eq!(request.records.len(), 2);

    // The response is the compact base-offset + count pair.
    let response = v1::ProduceBatchResponse {
        base_offset: 10,
        count: 2,
    };
    assert_eq!(response.base_offset, 10);
    assert_eq!(response.count, 2);

    // A batch replicates as one `LogEntry` via the fourth `EntryPayload` arm.
    let payload = v1::EntryPayload {
        kind: Some(v1::entry_payload::Kind::RecordBatch(v1::RecordBatch {
            records,
        })),
    };
    match payload.kind {
        Some(v1::entry_payload::Kind::RecordBatch(batch)) => {
            assert_eq!(batch.records.len(), 2);
        }
        _ => panic!("expected the record_batch payload arm"),
    }
}

// ---------------------------------------------------------------------------
// Log-backend wire surface (per-topic-log-durability Requirements 2.1, 2.4)
// ---------------------------------------------------------------------------

/// The per-topic log backend selection is carried on the create-topic request,
/// the topic-description type, and the replicated create-topic command
/// (per-topic-log-durability Requirements 2.1, 2.4). Constructing each message
/// with its `log_backend` field set is a compile-time assertion that the field
/// exists on every message that must carry it, plus a runtime check that the
/// value round-trips.
#[test]
fn log_backend_field_is_present_on_create_request_topic_info_and_command() {
    // `CreateTopicRequest` carries the requested backend (Requirement 2.1).
    let request = v1::CreateTopicRequest {
        name: "orders".to_string(),
        partitions: 3,
        log_backend: v1::LogBackend::Durable as i32,
    };
    assert_eq!(request.log_backend, v1::LogBackend::Durable as i32);

    // `TopicInfo` reports the topic's backend (Requirement 2.4).
    let info = v1::TopicInfo {
        name: "orders".to_string(),
        partition_count: 3,
        partitions: Vec::new(),
        log_backend: v1::LogBackend::InMemory as i32,
    };
    assert_eq!(info.log_backend, v1::LogBackend::InMemory as i32);

    // The replicated `CreateTopicCommand` carries the backend so every node
    // applying the committed command records the same selection (Requirement
    // 2.3).
    let command = v1::CreateTopicCommand {
        name: "orders".to_string(),
        partitions: Vec::new(),
        log_backend: v1::LogBackend::Durable as i32,
    };
    assert_eq!(command.log_backend, v1::LogBackend::Durable as i32);
}

/// The `LogBackend` enum exposes a zero-valued `UNSPECIFIED` sentinel so an
/// absent wire value decodes to the proto3 default, which the server treats as
/// durable (per-topic-log-durability Requirement 2.4, and Requirement 2.2 for
/// the server-side default). Pinning the discriminant to 0 locks the sentinel
/// in place.
#[test]
fn log_backend_has_zero_valued_unspecified_sentinel() {
    assert_eq!(v1::LogBackend::Unspecified as i32, 0);
    assert_eq!(v1::LogBackend::try_from(0), Ok(v1::LogBackend::Unspecified));

    // The two concrete backends are the non-sentinel values.
    assert_eq!(v1::LogBackend::Durable as i32, 1);
    assert_eq!(v1::LogBackend::InMemory as i32, 2);
}

// ---------------------------------------------------------------------------
// Both gRPC services are exposed (Requirements 12.2, 12.3)
// ---------------------------------------------------------------------------
/// The `VelaClient` client-facing service is generated into `vela-proto`: a
/// client stub (`vela_client_client::VelaClientClient`) and a server trait
/// (`vela_client_server::VelaClient`) (Requirement 12.2). Naming both paths is a
/// compile-time assertion that the service surface exists in this crate.
#[allow(dead_code)]
fn _vela_client_service_surface_exists() {
    // Generated client stub type.
    type _Client<T> = v1::vela_client_client::VelaClientClient<T>;
    // Generated server trait + service wrapper.
    fn _assert_server_trait<T: v1::vela_client_server::VelaClient>() {}
    type _Server<T> = v1::vela_client_server::VelaClientServer<T>;
}

/// The `VelaPeer` server-to-server service is generated into `vela-proto`: a
/// client stub (`vela_peer_client::VelaPeerClient`) and a server trait
/// (`vela_peer_server::VelaPeer`) (Requirement 12.3). Naming both paths is a
/// compile-time assertion that the service surface exists in this crate.
#[allow(dead_code)]
fn _vela_peer_service_surface_exists() {
    // Generated client stub type.
    type _Client<T> = v1::vela_peer_client::VelaPeerClient<T>;
    // Generated server trait + service wrapper.
    fn _assert_server_trait<T: v1::vela_peer_server::VelaPeer>() {}
    type _Server<T> = v1::vela_peer_server::VelaPeerServer<T>;
}

/// Runtime touch-point that references both generated service modules at once,
/// guaranteeing the two distinct service surfaces both exist in `vela-proto`
/// (Requirements 12.2, 12.3). The client-stub constructors only require a
/// transport channel, so we assert the constructor functions are nameable
/// rather than invoking them over a real connection.
#[test]
fn both_services_are_exposed_by_the_generated_module() {
    use tonic::transport::Channel;
    use v1::vela_client_client::VelaClientClient;
    use v1::vela_peer_client::VelaPeerClient;

    // Reference the client-stub constructors as function items. Naming them
    // fails to compile if either generated service module is absent, proving
    // both `VelaClient` and `VelaPeer` surfaces are generated in `vela-proto`.
    let client_ctor: fn(Channel) -> VelaClientClient<Channel> = VelaClientClient::<Channel>::new;
    let peer_ctor: fn(Channel) -> VelaPeerClient<Channel> = VelaPeerClient::<Channel>::new;

    // Two distinct, non-null function items: the two services are separate.
    assert_ne!(client_ctor as usize, 0);
    assert_ne!(peer_ctor as usize, 0);
}
