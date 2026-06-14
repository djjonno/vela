//! Property test for invalid topic-creation input rejection in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 9
//!
//! Property 9: Invalid topic-creation inputs are rejected without side effects.
//! For any topic name outside 1–255 characters or containing characters outside
//! `[A-Za-z0-9_-]`, *or* any partition count outside `1..=10000`, *or* any
//! cluster with fewer member nodes than the replication factor, topic creation
//! is rejected and the cluster metadata is left unchanged.
//!
//! Validates: Requirements 2.5, 2.6, 2.7

use proptest::prelude::*;
use vela_core::{ClusterMetadata, Member, NodeAvailability, NodeId};

/// The bounds the production code enforces, restated here so the generators can
/// deliberately produce values that fall *outside* them.
const MAX_NAME_LEN: usize = 255;
const MAX_PARTITIONS: u32 = 10_000;

/// Build cluster metadata with `available` available members and `unavailable`
/// unavailable members, named distinctly so they are genuinely distinct nodes.
fn cluster(available: usize, unavailable: usize) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    let mut members = Vec::with_capacity(available + unavailable);
    for i in 0..available {
        members.push(Member {
            id: NodeId::new(format!("avail-{i}")),
            addr: format!("avail-{i}:7001"),
            availability: NodeAvailability::Available,
        });
    }
    for i in 0..unavailable {
        members.push(Member {
            id: NodeId::new(format!("down-{i}")),
            addr: format!("down-{i}:7001"),
            availability: NodeAvailability::Unavailable,
        });
    }
    meta.members = members;
    meta
}

/// A topic-creation request that violates at least one validation rule, along
/// with the cluster it is issued against. The three variants mix the three
/// rejection categories so a single property exercises all of Requirements 2.5,
/// 2.6, and 2.7.
#[derive(Debug, Clone)]
struct InvalidRequest {
    meta: ClusterMetadata,
    name: String,
    partition_count: u32,
    replication_factor: usize,
}

/// Generate a name that is *invalid*: empty, longer than 255 characters, or
/// containing a character outside `[A-Za-z0-9_-]` (Requirement 2.6).
fn invalid_name_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Empty name (shorter than 1 character).
        Just(String::new()),
        // Too long: 256..=320 characters, all otherwise-valid.
        (256usize..=320).prop_map(|len| "a".repeat(len)),
        // Valid length but contains a disallowed character. We splice a bad
        // char into an otherwise-valid base so only the character rule fails.
        (
            prop::collection::vec(
                prop::sample::select(b"abcdefghijklmnopqrstuvwxyz0123456789-_".to_vec()),
                0..32,
            ),
            prop::sample::select(vec![' ', '!', '.', '/', '@', '#', '\n', 'é', 'λ']),
            0usize..=1,
        )
            .prop_map(|(good, bad, pos)| {
                let mut s: String = good.into_iter().map(|b| b as char).collect();
                let at = pos.min(s.len());
                s.insert(at, bad);
                s
            }),
    ]
}

/// Generate a partition count that is *invalid*: 0 or greater than 10,000
/// (Requirement 2.5).
fn invalid_partition_count_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0u32), (MAX_PARTITIONS + 1)..=1_000_000u32]
}

/// A valid topic name: 1–255 characters of `[A-Za-z0-9_-]`.
fn valid_name_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop::sample::select(
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".to_vec(),
        ),
        1..=MAX_NAME_LEN,
    )
    .prop_map(|bytes| bytes.into_iter().map(|b| b as char).collect())
}

/// A valid partition count in `1..=10000`.
fn valid_partition_count_strategy() -> impl Strategy<Value = u32> {
    1u32..=MAX_PARTITIONS
}

/// Generate an invalid request that mixes all three rejection categories:
/// invalid name, invalid partition count, and insufficient available nodes.
/// Each variant violates its targeted rule while keeping the other inputs valid
/// so the targeted rule is genuinely the cause of rejection.
fn invalid_request_strategy() -> impl Strategy<Value = InvalidRequest> {
    prop_oneof![
        // Category A — invalid name (Requirement 2.6). Cluster has enough
        // available nodes and the partition count is valid.
        (
            invalid_name_strategy(),
            valid_partition_count_strategy(),
            1usize..=4,
        )
            .prop_map(|(name, partition_count, rf)| InvalidRequest {
                meta: cluster(rf, 0),
                name,
                partition_count,
                replication_factor: rf,
            }),
        // Category B — invalid partition count (Requirement 2.5). Name is valid
        // and the cluster has enough available nodes.
        (
            valid_name_strategy(),
            invalid_partition_count_strategy(),
            1usize..=4,
        )
            .prop_map(|(name, partition_count, rf)| InvalidRequest {
                meta: cluster(rf, 0),
                name,
                partition_count,
                replication_factor: rf,
            }),
        // Category C — insufficient available nodes (Requirement 2.7). Name and
        // partition count are valid; the cluster has fewer *available* members
        // than the replication factor (with some unavailable members mixed in
        // to confirm they do not count toward it).
        (
            valid_name_strategy(),
            valid_partition_count_strategy(),
            1usize..=6,
        )
            .prop_flat_map(|(name, partition_count, rf)| {
                // Available members strictly fewer than the replication factor.
                (Just(name), Just(partition_count), Just(rf), 0usize..rf).prop_map(
                    |(name, partition_count, rf, available)| InvalidRequest {
                        meta: cluster(available, 2),
                        name,
                        partition_count,
                        replication_factor: rf,
                    },
                )
            }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 9
    #[test]
    fn invalid_inputs_are_rejected_without_side_effects(req in invalid_request_strategy()) {
        let mut meta = req.meta.clone();
        let before = meta.clone();

        let result = meta.create_topic(&req.name, req.partition_count, req.replication_factor);

        // The request must be rejected (Requirements 2.5, 2.6, 2.7).
        prop_assert!(
            result.is_err(),
            "expected rejection for name={:?}, partitions={}, rf={}, available_members={}",
            req.name,
            req.partition_count,
            req.replication_factor,
            before
                .members
                .iter()
                .filter(|m| matches!(m.availability, NodeAvailability::Available))
                .count(),
        );

        // The metadata must be completely unchanged on rejection.
        prop_assert_eq!(meta, before, "metadata must be unchanged on rejection");
    }
}
