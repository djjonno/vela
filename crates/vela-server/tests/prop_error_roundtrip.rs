//! Property test for typed error round-tripping across the gRPC boundary.
//!
//! A client RPC failure leaves the server as a [`vela_core::CoreError`], is
//! mapped to the shared wire [`vela_proto::v1::VelaError`] and carried on a
//! [`tonic::Status`]'s details, then recovered by the client. That path must be
//! lossless for the fields the client relies on: the classification code, the
//! human-readable message, and the leader redirect hint (Requirement 12.4,
//! 11.2). Property 39 pins that the recovered error equals the direct
//! `CoreError -> VelaError` mapping for every error variant.

// Feature: vela-streaming-platform, Property 39

use proptest::prelude::*;

use vela_core::{CoreError, NodeId};
use vela_server::convert::{
    core_error_to_status, core_error_to_vela_error, vela_error_from_status,
};

/// Arbitrary node identity for leader hints and propagation lists.
fn node_id_strategy() -> impl Strategy<Value = NodeId> {
    "[A-Za-z0-9_-]{1,16}".prop_map(NodeId)
}

/// Arbitrary topic-like name; allowed to be empty/odd since the conversion must
/// preserve whatever message the error renders, not just valid names.
fn name_strategy() -> impl Strategy<Value = String> {
    "[A-Za-z0-9_-]{0,24}"
}

/// Generate a `CoreError` across every variant, including `NotLeader` with both
/// a known leader (`Some`) and an unknown one (`None`), and `MetadataPropagation`
/// with zero or more unacked nodes.
fn core_error_strategy() -> impl Strategy<Value = CoreError> {
    prop_oneof![
        name_strategy().prop_map(CoreError::TopicExists),
        name_strategy().prop_map(CoreError::TopicNotFound),
        name_strategy().prop_map(CoreError::TopicDeleting),
        (name_strategy(), any::<u32>())
            .prop_map(|(topic, index)| CoreError::PartitionNotFound { topic, index }),
        Just(CoreError::InvalidTopicName),
        any::<u32>().prop_map(CoreError::InvalidPartitionCount),
        any::<usize>().prop_map(CoreError::RecordTooLarge),
        Just(CoreError::InvalidConsumeParams),
        (any::<usize>(), any::<usize>())
            .prop_map(|(have, need)| CoreError::InsufficientNodes { have, need }),
        Just(CoreError::PartitionUnavailable),
        proptest::option::of(node_id_strategy()).prop_map(|leader| CoreError::NotLeader { leader }),
        Just(CoreError::CommitTimeout),
        proptest::collection::vec(node_id_strategy(), 0..4)
            .prop_map(CoreError::MetadataPropagation),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property 39: mapping a `CoreError` to a `VelaError` and onto a
    /// `tonic::Status` preserves the code, message, and leader hint, all
    /// recoverable via `vela_error_from_status`.
    ///
    /// **Validates: Requirements 12.4**
    #[test]
    fn typed_error_mapping_round_trips(error in core_error_strategy()) {
        // The direct mapping is the reference the wire path must reproduce.
        let direct = core_error_to_vela_error(&error);

        // Map onto a Status (encoding the VelaError into its details) and
        // recover it the way a client would.
        let status = core_error_to_status(&error);
        let recovered = vela_error_from_status(&status)
            .expect("a status built from a CoreError must carry a typed VelaError");

        prop_assert_eq!(recovered.code, direct.code);
        prop_assert_eq!(&recovered.message, &direct.message);
        prop_assert_eq!(&recovered.leader, &direct.leader);
    }
}
