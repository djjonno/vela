//! Property test: a topic's log backend round-trips through the replicated
//! `CreateTopic` command and is recorded identically on every node.
//!
//! Feature: per-topic-log-durability, Property 2
//!
//! Property 2: for any backend value and any starting `ClusterMetadata`,
//! building a `CreateTopic` command carrying that backend and applying it via
//! [`apply_command`] on two independent metadata views records, on both views,
//! exactly one topic for the command's name whose backend equals the command's
//! backend — and re-applying the committed command log to a fresh view (a
//! metadata replay) rebuilds the same backend.
//!
//! This realizes the design's guarantee that the backend chosen at creation is
//! carried by the replicated command so that every node applying the committed
//! command stores the same backend on its `Topic` (Requirement 3.2), is
//! recorded exactly once per topic (Requirement 3.1), travels on the create
//! command (Requirement 2.3), and survives a metadata replay during recovery
//! (Requirement 18.3).
//!
//! Validates: Requirements 2.3, 3.1, 3.2, 18.3

use proptest::prelude::*;
use vela_core::{
    apply_command, ClusterCommand, ClusterMetadata, LogBackend, NodeId, Partition, PartitionIndex,
};

/// The name of the topic created by the command under test. It uses a prefix
/// distinct from the prior topics' (`existing-*`), so it never collides with a
/// topic in the arbitrary starting metadata and "exactly one" stays meaningful.
const TARGET_TOPIC: &str = "target-topic";

/// An arbitrary log backend: exactly one of the two closed values.
fn backend_strategy() -> impl Strategy<Value = LogBackend> {
    prop_oneof![Just(LogBackend::Durable), Just(LogBackend::InMemory)]
}

/// Build `n` trivial partitions (`0..n`) with a single placeholder replica;
/// `apply_command` stores the partitions verbatim, so their exact shape is
/// immaterial to the backend round-trip under test.
fn partitions(n: u32) -> Vec<Partition> {
    (0..n)
        .map(|i| Partition {
            index: PartitionIndex(i),
            replicas: vec![NodeId::new("a")],
            leader: None,
        })
        .collect()
}

/// An arbitrary sequence of prior `CreateTopic` commands with distinct names
/// (`existing-{i}`) and arbitrary backends. Re-applying these to an empty view
/// reaches an arbitrary starting `ClusterMetadata` purely through the committed
/// command log — exactly the state a metadata replay rebuilds.
fn prior_creates_strategy() -> impl Strategy<Value = Vec<ClusterCommand>> {
    prop::collection::vec((0u32..40, backend_strategy(), 0u32..3), 0..6).prop_map(|specs| {
        // Dedupe by index so prior topic names are distinct (a duplicate name
        // would merely overwrite, but distinct names keep the fixture clear).
        let mut seen = std::collections::BTreeSet::new();
        specs
            .into_iter()
            .filter(|(idx, _, _)| seen.insert(*idx))
            .map(|(idx, backend, part_count)| ClusterCommand::CreateTopic {
                name: format!("existing-{idx}"),
                partitions: partitions(part_count),
                backend,
            })
            .collect()
    })
}

/// Apply every command in `log` to a fresh `ClusterMetadata`, in order — the
/// metadata-replay path a node takes when rebuilding its catalogue from the
/// committed log.
fn replay(log: &[ClusterCommand]) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    for command in log {
        apply_command(&mut meta, command);
    }
    meta
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 2
    #[test]
    fn backend_round_trips_through_the_replicated_command(
        prior in prior_creates_strategy(),
        target_backend in backend_strategy(),
        target_partitions in 0u32..4,
    ) {
        // An arbitrary starting metadata, reached through the committed log.
        let starting = replay(&prior);

        // The command under test carries the chosen backend.
        let command = ClusterCommand::CreateTopic {
            name: TARGET_TOPIC.to_string(),
            partitions: partitions(target_partitions),
            backend: target_backend,
        };

        // Two independent views of the same starting metadata.
        let mut view_a = starting.clone();
        let mut view_b = starting.clone();
        apply_command(&mut view_a, &command);
        apply_command(&mut view_b, &command);

        // On each view, exactly one topic exists for the command's name and its
        // recorded backend equals the command's backend (Requirement 3.1, 3.2).
        for view in [&view_a, &view_b] {
            let matching = view
                .topics
                .values()
                .filter(|t| t.name == TARGET_TOPIC && t.backend == target_backend)
                .count();
            prop_assert_eq!(matching, 1, "exactly one topic must record the command's backend");

            let topic = view
                .topics
                .get(TARGET_TOPIC)
                .expect("the created topic must be present");
            prop_assert_eq!(topic.backend, target_backend);
        }

        // Both nodes recorded the identical backend (Requirement 3.2).
        prop_assert_eq!(
            view_a.topics[TARGET_TOPIC].backend,
            view_b.topics[TARGET_TOPIC].backend,
        );

        // A metadata replay of the committed command log rebuilds the same
        // backend (Requirement 18.3): re-applying every prior create plus the
        // target create to a fresh view yields the command's backend.
        let mut full_log = prior.clone();
        full_log.push(command);
        let replayed = replay(&full_log);
        prop_assert_eq!(replayed.topics[TARGET_TOPIC].backend, target_backend);
    }
}
