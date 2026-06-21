//! Property test: a topic's log backend round-trips through the replicated
//! `CreateTopic` command.
//!
//! Feature: per-topic-log-durability, Property 2
//!
//! Property 2: for any backend value and any starting [`ClusterMetadata`],
//! building a [`ClusterCommand::CreateTopic`] command carrying that backend and
//! applying it via [`apply_command`] to two independent metadata views records,
//! on both views, a topic whose backend equals the command's backend — and the
//! same backend is recovered when the committed command sequence is replayed
//! onto a fresh metadata.
//!
//! This is the domain-layer realization of Requirements 2.3, 3.1, 3.2, and
//! 18.3: the replicated create command carries the backend (2.3); the metadata
//! records exactly one backend per topic (3.1); every node applying the
//! committed command records the same backend (3.2); and re-applying the
//! committed prefix on restart rebuilds the identical recorded backend (18.3).
//!
//! Validates: Requirements 2.3, 3.1, 3.2, 18.3

use proptest::prelude::*;
use vela_core::{
    apply_command, ClusterCommand, ClusterMetadata, LogBackend, Member, NodeAvailability, NodeId,
    Partition, PartitionIndex,
};

/// The reserved name of the topic this property creates and inspects. Prelude
/// topics are all named `pre-*`, so this never collides with them.
const TARGET: &str = "target";

/// Generate one of the two valid log backends.
fn log_backend() -> impl Strategy<Value = LogBackend> {
    prop_oneof![Just(LogBackend::Durable), Just(LogBackend::InMemory)]
}

/// A description of a topic to create as part of the arbitrary starting view:
/// a backend and a partition count. The name is derived from the prelude index
/// so prelude topics are uniquely named and disjoint from [`TARGET`].
#[derive(Debug, Clone)]
struct PreludeTopic {
    backend: LogBackend,
    partitions: u8,
}

fn prelude_topic() -> impl Strategy<Value = PreludeTopic> {
    (log_backend(), 0u8..4).prop_map(|(backend, partitions)| PreludeTopic {
        backend,
        partitions,
    })
}

/// Build a `count`-partition assignment with a single placeholder replica each.
/// Backend round-tripping is independent of the partition shape, so a minimal
/// assignment keeps the generator focused on the backend.
fn partitions(count: u8) -> Vec<Partition> {
    (0..count as u32)
        .map(|i| Partition {
            index: PartitionIndex(i),
            replicas: vec![NodeId::new("n0")],
            leader: None,
        })
        .collect()
}

fn create_command(name: &str, topic: &PreludeTopic) -> ClusterCommand {
    ClusterCommand::CreateTopic {
        name: name.to_string(),
        partitions: partitions(topic.partitions),
        backend: topic.backend,
    }
}

/// Build the committed command sequence: a member roster, the prelude creates,
/// and finally the target create carrying `target_backend`.
fn committed_commands(
    members: &[String],
    prelude: &[PreludeTopic],
    target_backend: LogBackend,
    target_partitions: u8,
) -> Vec<ClusterCommand> {
    let mut commands = Vec::new();
    // Flip every member to available, exercising a non-create command in the
    // arbitrary starting view (the backend must survive other command kinds).
    for m in members {
        commands.push(ClusterCommand::SetAvailability {
            node: NodeId::new(m.clone()),
            availability: NodeAvailability::Available,
        });
    }
    for (i, topic) in prelude.iter().enumerate() {
        commands.push(create_command(&format!("pre-{i}"), topic));
    }
    commands.push(create_command(
        TARGET,
        &PreludeTopic {
            backend: target_backend,
            partitions: target_partitions,
        },
    ));
    commands
}

/// Apply every command in `commands` to a fresh metadata seeded with `members`,
/// returning the resulting view. Seeding members up front lets the
/// `SetAvailability` commands match a real member (an unknown member is a no-op).
fn replay(members: &[String], commands: &[ClusterCommand]) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = members
        .iter()
        .map(|id| Member {
            id: NodeId::new(id.clone()),
            addr: format!("{id}:7001"),
            advertised_addr: format!("{id}:7001"),
            availability: NodeAvailability::Unavailable,
        })
        .collect();
    for command in commands {
        apply_command(&mut meta, command);
    }
    meta
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 2
    #[test]
    fn backend_round_trips_through_create_command(
        members in prop::collection::vec("[a-d]{1,3}", 0..4),
        prelude in prop::collection::vec(prelude_topic(), 0..6),
        target_backend in log_backend(),
        target_partitions in 0u8..4,
    ) {
        // The arbitrary starting view: everything committed before the target
        // create. Two independent views start from the identical state.
        let prelude_commands = committed_commands(&members, &prelude, target_backend, target_partitions);
        // Split into "before target" and the target command itself.
        let (start_commands, target_command) = prelude_commands
            .split_last()
            .map(|(last, head)| (head.to_vec(), last.clone()))
            .expect("sequence always ends with the target create");

        let view_a = replay(&members, &start_commands);
        let mut view_b = view_a.clone();
        let mut view_a = view_a;

        // Apply the same target command independently to each view (R3.2).
        apply_command(&mut view_a, &target_command);
        apply_command(&mut view_b, &target_command);

        // Each view records exactly one topic under TARGET, with a backend
        // equal to the command's backend (R3.1, 3.2).
        let topic_a = view_a.topics.get(TARGET).expect("target created on view a");
        let topic_b = view_b.topics.get(TARGET).expect("target created on view b");
        prop_assert_eq!(topic_a.backend, target_backend);
        prop_assert_eq!(topic_b.backend, target_backend);

        // Both independent views agree completely — the apply is deterministic,
        // so every node records the same backend (R3.2).
        prop_assert_eq!(&view_a, &view_b);

        // Replaying the full committed command sequence onto a fresh metadata
        // rebuilds the identical view, including the target's backend (R18.3).
        let replayed = replay(&members, &prelude_commands);
        let replayed_backend = replayed
            .topics
            .get(TARGET)
            .expect("target present after replay")
            .backend;
        prop_assert_eq!(replayed_backend, target_backend);
        prop_assert_eq!(&replayed, &view_a);
    }
}
