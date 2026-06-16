//! Property test: backend selection picks the recorded variant.
//!
//! Feature: per-topic-log-durability, Property 4
//!
//! Property 4: the spawn-selection helper maps a topic recorded `Durable` to
//! the `Durable` variant and a topic recorded `In_Memory` to the `InMemory`
//! variant.
//!
//! The server reads a topic's recorded [`LogBackend`] back out of
//! [`ClusterMetadata`] when it spawns each partition replica, and constructs the
//! backend that [`PartitionLog::select`] names. This test routes an arbitrary
//! backend through the replicated create-topic command (create the topic with
//! that backend, read the recorded backend back out of metadata, then call the
//! selection helper on the *recorded* value) so the property is exercised end to
//! end from the metadata catalogue rather than on a bare enum value. Because the
//! backend domain is closed at exactly two values, the test also asserts the
//! total mapping holds for both variants directly.
//!
//! Validates: Requirements 3.4, 5.1, 5.4

use proptest::prelude::*;
use vela_core::{
    apply_command, ClusterCommand, ClusterMetadata, LogBackend, Partition, PartitionIndex,
    PartitionLog, PartitionLogKind,
};

/// The topic whose recorded backend drives the selection under test.
const TOPIC: &str = "selection-topic";

/// An arbitrary log backend: exactly one of the two closed values.
fn backend_strategy() -> impl Strategy<Value = LogBackend> {
    prop_oneof![Just(LogBackend::Durable), Just(LogBackend::InMemory)]
}

/// The [`PartitionLogKind`] that a topic recorded with `backend` must select.
/// Defined independently of [`PartitionLog::select`] so the test asserts the
/// intended mapping rather than re-deriving it from the code under test.
fn expected_kind(backend: LogBackend) -> PartitionLogKind {
    match backend {
        LogBackend::Durable => PartitionLogKind::Durable,
        LogBackend::InMemory => PartitionLogKind::InMemory,
    }
}

/// Build `n` trivial partitions (`0..n`); their shape is immaterial to which
/// backend variant the selection helper picks.
fn partitions(n: u32) -> Vec<Partition> {
    (0..n)
        .map(|i| Partition {
            index: PartitionIndex(i),
            replicas: Vec::new(),
            leader: None,
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 4
    #[test]
    fn selection_picks_the_recorded_variant(
        backend in backend_strategy(),
        part_count in 0u32..4,
    ) {
        // Route the backend through the replicated create-topic command so the
        // selection runs on the value recorded in the cluster catalogue, the
        // way the server reads it back at spawn time (Requirement 3.4, 5.1).
        let mut meta = ClusterMetadata::new();
        apply_command(
            &mut meta,
            &ClusterCommand::CreateTopic {
                name: TOPIC.to_string(),
                partitions: partitions(part_count),
                backend,
            },
        );

        let recorded = meta
            .topics
            .get(TOPIC)
            .expect("the created topic must be recorded")
            .backend;

        // The selection helper maps the recorded backend onto the matching
        // variant the server must construct for every replica (Requirement 5.4).
        prop_assert_eq!(PartitionLog::select(recorded), expected_kind(backend));
    }
}

// Feature: per-topic-log-durability, Property 4
//
// The backend domain is closed at exactly two values, so the selection mapping
// can be asserted in full: `Durable` selects the durable variant and
// `InMemory` selects the in-memory variant. Together with the round-tripped
// property above, this pins the helper as a total, exact mapping
// (Requirement 3.4, 5.4).
#[test]
fn selection_is_a_total_exact_mapping() {
    assert_eq!(
        PartitionLog::select(LogBackend::Durable),
        PartitionLogKind::Durable
    );
    assert_eq!(
        PartitionLog::select(LogBackend::InMemory),
        PartitionLogKind::InMemory
    );
}
