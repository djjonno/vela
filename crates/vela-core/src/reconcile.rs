//! Pure partition-reconcile planning (Requirement 6).
//!
//! After a committed `ClusterCommand` is applied to a node's served
//! [`ClusterMetadata`], the node's running partition replicas must be brought
//! into line with the new catalogue: a replica is started for every partition
//! this node now replicates and stopped for every partition that is gone or no
//! longer assigned to it. That alignment is **reconciliation**, and computing
//! *what* to start and stop is the pure diff [`plan_reconcile`].
//!
//! This planner performs no I/O and consults no runtime state beyond its three
//! arguments, so it is a deterministic function of `(metadata, running,
//! self_id)`. The server crate wraps it in a `tokio` task that applies the diff
//! (opening durable logs, registering peers, spawning/stopping driver tasks);
//! the deterministic simulation harness reuses the *same* planner to drive
//! per-node partition-replica spawning, so both share one diff function and
//! cannot diverge. The reserved metadata group `("__meta", 0)` is never started
//! or stopped here (Requirement 6.6); it is driven separately.

use std::collections::HashSet;

use crate::metadata::METADATA_GROUP_TOPIC;
use crate::model::{ClusterMetadata, Partition};

/// The set of replica changes one reconciliation pass should make.
///
/// Computed purely from a served catalogue, the running-replica set, and a
/// node's identity by [`plan_reconcile`], so the diff logic is testable without
/// a runtime, a real driver, or any I/O.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcilePlan {
    /// Partitions to start a replica for: `desired \ running` (Requirement 6.1).
    /// Each carries its full [`Partition`] so the spawn can register the
    /// partition's replica peers (Requirement 6.4).
    pub spawn: Vec<(String, Partition)>,
    /// `(topic, partition)` keys whose replica must stop: `running \ desired`
    /// (Requirement 6.2).
    pub stop: Vec<(String, u32)>,
}

/// Compute the reconcile diff for `self_id` against `metadata` and the `running`
/// replica set.
///
/// `desired = {(topic, p.index) : self_id in p.replicas}` over the served
/// catalogue (Requirement 6.5), excluding the reserved `__meta` group. The plan
/// then starts exactly `desired \ running`, stops exactly `running \ desired`,
/// and leaves the intersection untouched (Requirement 6.1, 6.2, 6.3). The
/// `("__meta", 0)` group is excluded from both sides, so it is never started or
/// stopped (Requirement 6.6).
pub fn plan_reconcile(
    metadata: &ClusterMetadata,
    running: &HashSet<(String, u32)>,
    self_id: &str,
) -> ReconcilePlan {
    // desired: the partitions whose replica set contains this node, with the
    // metadata group explicitly excluded (Requirement 6.5, 6.6). Track the keys
    // for the stop diff and carry each Partition for the spawn (Requirement
    // 6.4).
    let mut desired_keys: HashSet<(String, u32)> = HashSet::new();
    let mut spawn: Vec<(String, Partition)> = Vec::new();
    for topic in metadata.topics.values() {
        if topic.name == METADATA_GROUP_TOPIC {
            continue;
        }
        for partition in &topic.partitions {
            if partition.replicas.iter().any(|r| r.as_str() == self_id) {
                let key = (topic.name.clone(), partition.index.0);
                desired_keys.insert(key.clone());
                // Start a replica only for a partition not already running
                // (Requirement 6.1); an already-running one is left untouched
                // (Requirement 6.3).
                if !running.contains(&key) {
                    spawn.push((topic.name.clone(), partition.clone()));
                }
            }
        }
    }

    // stop: every running replica no longer desired, never the metadata group
    // (Requirement 6.2, 6.6).
    let mut stop: Vec<(String, u32)> = running
        .iter()
        .filter(|key| key.0 != METADATA_GROUP_TOPIC && !desired_keys.contains(*key))
        .cloned()
        .collect();
    // Deterministic order so a pass is reproducible regardless of HashSet
    // iteration order.
    stop.sort();

    ReconcilePlan { spawn, stop }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use proptest::prelude::*;

    use crate::model::{LogBackend, NodeId, PartitionIndex, Topic, TopicState};

    /// A partition with the given index replicated by `replicas`.
    fn partition(index: u32, replicas: &[&str]) -> Partition {
        Partition {
            index: PartitionIndex(index),
            replicas: replicas.iter().map(|r| NodeId::new(*r)).collect(),
            leader: None,
        }
    }

    /// A catalogue holding one topic with the given partitions.
    fn catalogue_with(topic: &str, partitions: Vec<Partition>) -> ClusterMetadata {
        let mut metadata = ClusterMetadata::new();
        metadata.topics.insert(
            topic.to_string(),
            Topic {
                name: topic.to_string(),
                partitions,
                state: TopicState::Active,
                backend: LogBackend::Durable,
            },
        );
        metadata
    }

    fn running_set(keys: &[(&str, u32)]) -> HashSet<(String, u32)> {
        keys.iter().map(|(t, p)| (t.to_string(), *p)).collect()
    }

    #[test]
    fn spawns_assigned_partitions_not_already_running() {
        // Two partitions assigned to node-a; one already running. Only the
        // missing one is spawned (Requirement 6.1), the running one is left
        // untouched (Requirement 6.3).
        let metadata = catalogue_with(
            "orders",
            vec![partition(0, &["node-a"]), partition(1, &["node-a"])],
        );
        let running = running_set(&[("orders", 0)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert_eq!(plan.spawn.len(), 1);
        assert_eq!(plan.spawn[0].0, "orders");
        assert_eq!(plan.spawn[0].1.index, PartitionIndex(1));
        assert!(plan.stop.is_empty());
    }

    #[test]
    fn does_not_spawn_partitions_not_assigned_to_self() {
        // The partition's replica set does not contain this node, so nothing is
        // started for it (Requirement 6.5).
        let metadata = catalogue_with("orders", vec![partition(0, &["node-b", "node-c"])]);
        let running = running_set(&[]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert!(plan.spawn.is_empty());
        assert!(plan.stop.is_empty());
    }

    #[test]
    fn stops_running_partitions_no_longer_desired() {
        // A replica is running for a partition absent from the served catalogue,
        // and another whose replica set no longer contains this node; both stop
        // (Requirement 6.2).
        let metadata = catalogue_with("orders", vec![partition(0, &["node-a"])]);
        let running = running_set(&[("orders", 0), ("orders", 1), ("ghost", 0)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert!(plan.spawn.is_empty());
        assert_eq!(
            plan.stop,
            vec![("ghost".to_string(), 0), ("orders".to_string(), 1)]
        );
    }

    #[test]
    fn never_starts_or_stops_the_metadata_group() {
        // The running metadata group is never stopped even though it is not a
        // client topic in the catalogue, and a `__meta` topic in the catalogue
        // is never started (Requirement 6.6).
        let mut metadata = catalogue_with("orders", vec![partition(0, &["node-a"])]);
        metadata.topics.insert(
            METADATA_GROUP_TOPIC.to_string(),
            Topic {
                name: METADATA_GROUP_TOPIC.to_string(),
                partitions: vec![partition(0, &["node-a"])],
                state: TopicState::Active,
                backend: LogBackend::Durable,
            },
        );
        let running = running_set(&[(METADATA_GROUP_TOPIC, 0), ("orders", 0)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert!(
            plan.spawn.iter().all(|(t, _)| t != METADATA_GROUP_TOPIC),
            "the metadata group is never spawned by the reconciler"
        );
        assert!(
            plan.stop.iter().all(|(t, _)| t != METADATA_GROUP_TOPIC),
            "the metadata group is never stopped by the reconciler"
        );
    }

    #[test]
    fn intersection_is_left_untouched() {
        // Every desired partition is already running and nothing else is: an
        // empty plan (Requirement 6.3).
        let metadata = catalogue_with(
            "orders",
            vec![partition(0, &["node-a"]), partition(1, &["node-a"])],
        );
        let running = running_set(&[("orders", 0), ("orders", 1)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert_eq!(plan, ReconcilePlan::default());
    }

    /// The pool of node identities replica sets and `self_id` are drawn from.
    const NODES: &[&str] = &["node-a", "node-b", "node-c", "node-d"];
    /// The pool of topic names. Includes the metadata group so the generated
    /// catalogue and running set both exercise the metadata-group exclusion.
    const TOPIC_NAMES: &[&str] = &["orders", "events", "logs", METADATA_GROUP_TOPIC];
    /// Exclusive upper bound on generated partition indices (kept small so
    /// catalogues and running sets overlap and cases stay fast).
    const PART_INDEX_MAX: u32 = 4;

    /// A distinct, possibly-empty replica set drawn from [`NODES`].
    fn replicas_strategy() -> impl Strategy<Value = Vec<NodeId>> {
        prop::collection::hash_set(0usize..NODES.len(), 0..=NODES.len())
            .prop_map(|idxs| idxs.into_iter().map(|i| NodeId::new(NODES[i])).collect())
    }

    /// An arbitrary catalogue: up to four topics drawn from [`TOPIC_NAMES`],
    /// each with up to four partitions keyed by distinct index. Using maps keeps
    /// topic names and per-topic partition indices unique, so each desired key
    /// is generated at most once.
    fn catalogue_strategy() -> impl Strategy<Value = ClusterMetadata> {
        let partitions =
            prop::collection::hash_map(0u32..PART_INDEX_MAX, replicas_strategy(), 0..=4);
        prop::collection::hash_map(
            prop::sample::select(TOPIC_NAMES.to_vec()),
            partitions,
            0..=4,
        )
        .prop_map(build_catalogue)
    }

    /// Build a catalogue from generated `(topic name -> index -> replicas)` data.
    fn build_catalogue(raw: HashMap<&'static str, HashMap<u32, Vec<NodeId>>>) -> ClusterMetadata {
        let mut metadata = ClusterMetadata::new();
        for (name, parts) in raw {
            let partitions = parts
                .into_iter()
                .map(|(index, replicas)| Partition {
                    index: PartitionIndex(index),
                    replicas,
                    leader: None,
                })
                .collect();
            metadata.topics.insert(
                name.to_string(),
                Topic {
                    name: name.to_string(),
                    partitions,
                    state: TopicState::Active,
                    backend: LogBackend::Durable,
                },
            );
        }
        metadata
    }

    /// An arbitrary running-replica set drawn from the same topic/index space as
    /// the catalogue (so it overlaps desired), including possible `__meta` keys.
    fn running_strategy() -> impl Strategy<Value = HashSet<(String, u32)>> {
        prop::collection::hash_set(
            (
                prop::sample::select(TOPIC_NAMES.to_vec()),
                0u32..PART_INDEX_MAX,
            ),
            0..=8,
        )
        .prop_map(|set| set.into_iter().map(|(t, p)| (t.to_string(), p)).collect())
    }

    /// The partitions assigned to `self_id` in `metadata`, excluding the
    /// reserved metadata group — the `desired` set recomputed independently of
    /// [`plan_reconcile`] for the property below.
    fn desired_keys(metadata: &ClusterMetadata, self_id: &str) -> HashSet<(String, u32)> {
        let mut desired = HashSet::new();
        for topic in metadata.topics.values() {
            if topic.name == METADATA_GROUP_TOPIC {
                continue;
            }
            for partition in &topic.partitions {
                if partition.replicas.iter().any(|r| r.as_str() == self_id) {
                    desired.insert((topic.name.clone(), partition.index.0));
                }
            }
        }
        desired
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Property 2: Reconciler diff correctness.
        ///
        /// Over random catalogues, running-replica sets, and node identities, the
        /// plan starts exactly `desired \ running`, stops exactly `running \
        /// desired`, leaves the intersection untouched, and never starts or
        /// stops the `("__meta", 0)` group.
        ///
        /// **Validates: Requirements 6.1, 6.2, 6.3, 6.5, 6.6**
        #[test]
        fn reconciler_diff_matches_desired_running_set_difference(
            metadata in catalogue_strategy(),
            running in running_strategy(),
            self_id in prop::sample::select(NODES.to_vec()),
        ) {
            let plan = plan_reconcile(&metadata, &running, self_id);

            let desired = desired_keys(&metadata, self_id);

            // The keys the plan would start, as a set (the generators make these
            // unique, so the count must match too).
            let spawn_keys: HashSet<(String, u32)> = plan
                .spawn
                .iter()
                .map(|(t, p)| (t.clone(), p.index.0))
                .collect();
            prop_assert_eq!(
                spawn_keys.len(),
                plan.spawn.len(),
                "spawn set must not contain duplicate keys"
            );

            // spawn == desired \ running (Requirement 6.1, 6.5).
            let expected_spawn: HashSet<(String, u32)> =
                desired.difference(&running).cloned().collect();
            prop_assert_eq!(&spawn_keys, &expected_spawn);

            // stop == running \ desired, excluding the metadata group, sorted
            // (Requirement 6.2, 6.6).
            let mut expected_stop: Vec<(String, u32)> = running
                .iter()
                .filter(|key| key.0 != METADATA_GROUP_TOPIC && !desired.contains(*key))
                .cloned()
                .collect();
            expected_stop.sort();
            prop_assert_eq!(&plan.stop, &expected_stop);

            // The intersection is left untouched: neither started nor stopped
            // (Requirement 6.3).
            for key in desired.intersection(&running) {
                prop_assert!(
                    !spawn_keys.contains(key),
                    "already-running desired partition {:?} must not be spawned",
                    key
                );
                prop_assert!(
                    !plan.stop.contains(key),
                    "already-running desired partition {:?} must not be stopped",
                    key
                );
            }

            // The metadata group is never started or stopped (Requirement 6.6).
            prop_assert!(
                plan.spawn.iter().all(|(t, _)| t != METADATA_GROUP_TOPIC),
                "the metadata group is never spawned"
            );
            prop_assert!(
                plan.stop.iter().all(|(t, _)| t != METADATA_GROUP_TOPIC),
                "the metadata group is never stopped"
            );

            // Each spawned entry carries the exact Partition from the catalogue,
            // so the spawn can register that partition's replica peers.
            for (topic, partition) in &plan.spawn {
                let cataloged = metadata
                    .topics
                    .get(topic)
                    .expect("a spawned topic must exist in the catalogue");
                prop_assert!(
                    cataloged.partitions.iter().any(|p| p == partition),
                    "spawned partition {:?} must match the catalogue entry",
                    partition.index
                );
            }
        }
    }
}
