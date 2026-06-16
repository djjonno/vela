//! Property test for atomic topic deletion in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 13
//!
//! Property 13: Deleting topics atomically removes all partitions. For any
//! existing topic, a delete operation results in either all of the topic's
//! partitions being removed from metadata or none of them — even when a fault
//! is injected mid-operation; a partial removal never persists.
//!
//! Two facets are exercised per case:
//!
//! * Normal deletion — deleting one topic removes the topic and *every* one of
//!   its partitions in a single step, while every other topic (and all of its
//!   partitions) is left completely untouched. Because a topic owns its
//!   partitions inside its `Topic` value, the single `BTreeMap` removal that
//!   drops the topic drops all its partitions together; no partial remnant can
//!   be observed afterwards.
//!
//! * Fault injected mid-operation — `delete_topic_with` runs the per-partition
//!   teardown hook for *all* partitions before performing the single metadata
//!   removal. Injecting a fault (a panic) partway through that teardown
//!   therefore happens strictly before the removal, so the topic and all of its
//!   partitions must remain fully present: the all-or-nothing guarantee holds
//!   and no partial state ever persists.
//!
//! Validates: Requirements 3.1

use std::panic::AssertUnwindSafe;
use std::sync::Once;

use proptest::prelude::*;
use vela_core::{ClusterMetadata, LogBackend, Member, NodeAvailability, NodeId};

/// The replication factor used throughout this test. The cluster is built with
/// exactly this many available members so creation is never rejected for
/// insufficient nodes — keeping the property focused on deletion atomicity.
const REPLICATION_FACTOR: usize = 3;

/// Build a cluster of `n` available members named `node-0..node-{n-1}`.
fn cluster(n: usize) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = (0..n)
        .map(|i| Member {
            id: NodeId::new(format!("node-{i}")),
            addr: format!("node-{i}:7001"),
            availability: NodeAvailability::Available,
        })
        .collect();
    meta
}

/// Silence panic output for the duration of the test binary so the deliberately
/// injected mid-operation faults do not clutter test output. Installed once.
fn silence_injected_fault_panics() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        std::panic::set_hook(Box::new(|_info| {}));
    });
}

/// Generate the partition counts for a set of topics: between 2 and 5 topics
/// (so there is always at least one "other" topic to check is untouched), each
/// with a varied partition count in `1..=64`. Topic `i` is named `topic-{i}`,
/// which is valid and unique by construction.
fn topic_partition_counts_strategy() -> impl Strategy<Value = Vec<u32>> {
    proptest::collection::vec(1u32..=64, 2..=5)
}

/// Create the topics described by `counts` into a fresh cluster and return the
/// resulting metadata together with the topic names.
fn build_metadata_with_topics(counts: &[u32]) -> (ClusterMetadata, Vec<String>) {
    let mut meta = cluster(REPLICATION_FACTOR);
    let names: Vec<String> = (0..counts.len()).map(|i| format!("topic-{i}")).collect();
    for (name, &count) in names.iter().zip(counts.iter()) {
        meta.create_topic(name, count, REPLICATION_FACTOR, LogBackend::Durable)
            .expect("valid topic creation must succeed");
    }
    (meta, names)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 13
    #[test]
    fn deleting_a_topic_atomically_removes_all_its_partitions(
        counts in topic_partition_counts_strategy(),
        delete_seed in any::<proptest::sample::Index>(),
        fault_seed in any::<proptest::sample::Index>(),
    ) {
        silence_injected_fault_panics();

        // A baseline cluster with every topic created. This is the "all present"
        // reference state both facets are measured against.
        let (before, names) = build_metadata_with_topics(&counts);
        let delete_idx = delete_seed.index(names.len());
        let target = &names[delete_idx];
        let target_partitions = before.topics[target].partitions.len();

        // ---- Facet 1: a normal delete removes the whole topic atomically. ----
        let mut meta = before.clone();

        // The teardown hook is handed every partition of the topic exactly once,
        // before the single metadata removal happens. Recording the indices lets
        // us confirm the operation tore down the *complete* partition set (not a
        // partial subset) prior to removal.
        let mut visited: Vec<u32> = Vec::new();
        meta.delete_topic_with(target, |p| visited.push(p.index.0))
            .expect("deleting an existing topic must succeed");

        // Every partition of the topic was handed to the teardown hook: 0..N-1.
        visited.sort_unstable();
        let expected_indices: Vec<u32> = (0..target_partitions as u32).collect();
        prop_assert_eq!(&visited, &expected_indices);

        // The topic — and therefore *all* of its partitions — is gone. There is
        // no key left in the map, so no partition of it can remain (no partial
        // remnant persists).
        prop_assert!(!meta.topics.contains_key(target));

        // Every other topic is left completely untouched: same partitions, same
        // replicas, same leaders. Deletion of one topic removes nothing else.
        for (i, name) in names.iter().enumerate() {
            if i == delete_idx {
                continue;
            }
            prop_assert_eq!(
                meta.topics.get(name),
                before.topics.get(name),
                "untargeted topic {} must be unchanged",
                name
            );
        }

        // The all-or-nothing change bumped the epoch exactly once.
        prop_assert_eq!(meta.epoch, before.epoch + 1);

        // ---- Facet 2: a fault injected mid-operation removes nothing. ----
        // Re-start from the full "all present" state and inject a panic partway
        // through the per-partition teardown (strictly before the metadata
        // removal). Because removal only happens after the whole teardown loop,
        // the topic and all of its partitions must survive intact: either all
        // partitions are removed or none — and here, none.
        let mut faulted = before.clone();
        // Fault on partition number `1..=N` so it lands during teardown, before
        // the single removal step that would drop the topic.
        let fault_after = fault_seed.index(target_partitions) + 1;

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut seen = 0usize;
            faulted
                .delete_topic_with(target, |_p| {
                    seen += 1;
                    if seen == fault_after {
                        panic!("injected fault mid-deletion");
                    }
                })
                // Unreachable: the panic aborts before this Result is produced.
                .expect("unreachable: faulted before completion");
        }));

        // The injected fault did abort the operation mid-flight.
        prop_assert!(result.is_err(), "the injected fault must unwind the delete");

        // No partial removal persisted: the topic is still fully present with
        // all of its partitions, identical to the baseline, and the epoch did
        // not advance. The deletion was atomic — all or nothing — and nothing
        // happened.
        prop_assert_eq!(
            faulted.topics.get(target),
            before.topics.get(target),
            "a fault before removal must leave the topic and all partitions intact"
        );
        prop_assert_eq!(faulted.topics.len(), before.topics.len());
        prop_assert_eq!(faulted.epoch, before.epoch);
    }
}
