//! Property test for one Raft group per partition in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 26
//!
//! Property 26: Exactly one Raft group exists per partition. For any set of
//! created topics/partitions, the [`RaftGroupFleet`] holds exactly one Raft
//! group per `(topic, partition)` key: the total group count equals the number
//! of distinct partitions, each distinct key is present exactly once, and an
//! attempt to create a second group for a key already present is rejected with
//! [`FleetError::GroupExists`] without changing the fleet.
//!
//! This is the node-local realization of Requirement 7.1 — "instantiate exactly
//! one independent Raft_Group for each Partition of each Topic, such that the
//! count of Raft_Groups equals the total count of Partitions across all Topics."
//! The fleet is keyed by `(topic, partition)`, so distinctness of partitions is
//! what bounds the group count; this test exercises that across many shapes of
//! topic sets.
//!
//! Validates: Requirements 7.1

use std::collections::BTreeMap;

use proptest::prelude::*;
use vela_core::{FleetError, GroupKey, PartitionIndex, RaftGroupFleet};
use vela_raft::NodeId as RaftNodeId;

/// Generate a topic name: a short, lowercase identifier. Names are deduplicated
/// into a map before use, so the only thing that matters here is producing a
/// realistic spread of distinct topic names across iterations.
fn topic_name_strategy() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-z][a-z0-9_-]{0,7}").expect("valid regex")
}

/// Generate a set of topics, each paired with a partition count in `1..=8`.
///
/// Returns a `BTreeMap<topic, partition_count>` so topic names are distinct (a
/// later duplicate name simply overwrites the earlier count). This yields a set
/// of `(topic, partition)` keys with no duplicates, of varied topic counts and
/// per-topic partition counts.
fn topic_set_strategy() -> impl Strategy<Value = BTreeMap<String, u32>> {
    prop::collection::vec((topic_name_strategy(), 1u32..=8), 1..=6).prop_map(|pairs| {
        let mut topics = BTreeMap::new();
        for (name, count) in pairs {
            topics.insert(name, count);
        }
        topics
    })
}

/// Expand a topic→partition-count map into the full set of distinct partition
/// keys: `(topic, 0), (topic, 1), ..., (topic, count-1)` for each topic.
fn partition_keys(topics: &BTreeMap<String, u32>) -> Vec<GroupKey> {
    topics
        .iter()
        .flat_map(|(name, &count)| (0..count).map(move |p| (name.clone(), PartitionIndex(p))))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 26
    #[test]
    fn exactly_one_raft_group_exists_per_partition(topics in topic_set_strategy()) {
        let keys = partition_keys(&topics);
        // The keys are distinct by construction (distinct topics × distinct
        // partition indices), so their count is the number of distinct
        // partitions the fleet should end up hosting.
        let distinct_partition_count = keys.len();

        let mut fleet = RaftGroupFleet::new();

        // Create exactly one Raft group per partition. Each create succeeds and
        // bumps the count by one, so the running count tracks the number of
        // partitions instantiated so far (Requirement 7.1).
        for (i, key) in keys.iter().enumerate() {
            fleet
                .create_group(key.clone(), RaftNodeId(i as u64), Vec::new())
                .expect("creating a group for a fresh partition must succeed");
            prop_assert_eq!(fleet.group_count(), i + 1);
        }

        // The total group count equals the number of distinct partitions: one
        // Raft group each, no more, no fewer (Requirement 7.1).
        prop_assert_eq!(fleet.group_count(), distinct_partition_count);

        // Every distinct partition is hosted exactly once: it is present, and a
        // second create for the same key is rejected with GroupExists and leaves
        // the fleet's group count unchanged — so there can never be two groups
        // for one partition (Requirement 7.1).
        for key in &keys {
            prop_assert!(fleet.contains(key));

            let count_before = fleet.group_count();
            let (topic, partition) = key;
            let duplicate = fleet.create_group(key.clone(), RaftNodeId(999), Vec::new());
            prop_assert_eq!(
                duplicate,
                Err(FleetError::GroupExists {
                    topic: topic.clone(),
                    partition: partition.0,
                })
            );
            // The rejected duplicate did not add a second group.
            prop_assert_eq!(fleet.group_count(), count_before);
        }

        // The count is still exactly the number of distinct partitions after all
        // the duplicate attempts: rejections never grew the fleet.
        prop_assert_eq!(fleet.group_count(), distinct_partition_count);
    }
}
