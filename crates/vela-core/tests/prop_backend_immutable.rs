//! Property test: a topic's recorded log backend is immutable after creation.
//!
//! Feature: per-topic-log-durability, Property 3
//!
//! Property 3: for any topic created with a given backend and any subsequent
//! sequence of commands that operate on *other* topics or on availability —
//! other-topic deletes, availability changes, and other-topic creates — the
//! target topic's recorded backend stays equal to the backend it was created
//! with.
//!
//! This realizes the design's guarantee that a topic's backend is fixed at
//! creation and cannot change for the topic's lifetime (Requirement 3.3). No
//! `ClusterCommand` re-creates or mutates the target topic's backend: the only
//! command that could touch the target — `DeleteTopic` for its own name —
//! would remove it entirely rather than change its backend, so (consistent with
//! the property statement) the subsequent commands target names distinct from
//! the target topic.
//!
//! Validates: Requirements 3.3

use proptest::prelude::*;
use vela_core::{
    apply_command, ClusterCommand, ClusterMetadata, LogBackend, Member, NodeAvailability, NodeId,
    Partition, PartitionIndex,
};

/// The topic created up front whose backend must never change. Its name uses a
/// prefix (`target-`) distinct from every later command's (`other-*`), so no
/// subsequent command can ever name it.
const TARGET_TOPIC: &str = "target-topic";

/// The number of known members availability commands may target.
const MEMBER_COUNT: u32 = 4;

/// An arbitrary log backend: exactly one of the two closed values.
fn backend_strategy() -> impl Strategy<Value = LogBackend> {
    prop_oneof![Just(LogBackend::Durable), Just(LogBackend::InMemory)]
}

/// Build `n` trivial partitions (`0..n`); their exact shape is immaterial to
/// the backend-immutability property under test.
fn partitions(n: u32) -> Vec<Partition> {
    (0..n)
        .map(|i| Partition {
            index: PartitionIndex(i),
            replicas: vec![NodeId::new("a")],
            leader: None,
        })
        .collect()
}

/// `MEMBER_COUNT` known members, all initially available, so `SetAvailability`
/// commands in the sequence act on real members rather than being no-ops.
fn members() -> Vec<Member> {
    (0..MEMBER_COUNT)
        .map(|i| Member {
            id: NodeId::new(format!("node-{i}")),
            addr: format!("node-{i}:7001"),
            advertised_addr: format!("node-{i}:7001"),
            availability: NodeAvailability::Available,
        })
        .collect()
}

/// One arbitrary command that operates on something *other* than the target
/// topic: an other-topic delete, an availability change, or an other-topic
/// create. Every generated name is `other-*`, so it can never equal
/// [`TARGET_TOPIC`].
fn other_command_strategy() -> impl Strategy<Value = ClusterCommand> {
    prop_oneof![
        // Delete some other topic (whether or not it exists).
        (0u32..50).prop_map(|idx| ClusterCommand::DeleteTopic {
            name: format!("other-{idx}"),
        }),
        // Flip a known member's availability.
        (0u32..MEMBER_COUNT, prop::bool::ANY).prop_map(|(node, available)| {
            ClusterCommand::SetAvailability {
                node: NodeId::new(format!("node-{node}")),
                availability: if available {
                    NodeAvailability::Available
                } else {
                    NodeAvailability::Unavailable
                },
            }
        }),
        // Create some other topic with an arbitrary backend.
        (0u32..50, backend_strategy(), 0u32..3).prop_map(|(idx, backend, part_count)| {
            ClusterCommand::CreateTopic {
                name: format!("other-{idx}"),
                partitions: partitions(part_count),
                backend,
            }
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 3
    #[test]
    fn backend_is_immutable_after_creation(
        target_backend in backend_strategy(),
        target_partitions in 0u32..4,
        subsequent in prop::collection::vec(other_command_strategy(), 0..24),
    ) {
        // Start from metadata with known members so availability commands bite.
        let mut meta = ClusterMetadata::new();
        meta.members = members();

        // Create the target topic with the chosen backend.
        apply_command(
            &mut meta,
            &ClusterCommand::CreateTopic {
                name: TARGET_TOPIC.to_string(),
                partitions: partitions(target_partitions),
                backend: target_backend,
            },
        );

        // The backend is recorded as chosen immediately after creation.
        prop_assert_eq!(meta.topics[TARGET_TOPIC].backend, target_backend);

        // Apply the arbitrary sequence of other-topic / availability commands.
        for command in &subsequent {
            // The fixture must never name the target topic, so the property
            // only exercises commands that cannot delete or recreate it.
            match command {
                ClusterCommand::CreateTopic { name, .. }
                | ClusterCommand::DeleteTopic { name } => {
                    prop_assert_ne!(name.as_str(), TARGET_TOPIC);
                }
                ClusterCommand::SetAvailability { .. } => {}
            }
            apply_command(&mut meta, command);

            // After every step the target topic still exists and its recorded
            // backend is unchanged (Requirement 3.3).
            let topic = meta
                .topics
                .get(TARGET_TOPIC)
                .expect("the target topic must never be removed by other-topic commands");
            prop_assert_eq!(topic.backend, target_backend);
        }

        // And the backend equals its creation value after the whole sequence.
        prop_assert_eq!(meta.topics[TARGET_TOPIC].backend, target_backend);
    }
}
