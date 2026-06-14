//! Property test for the cluster availability state set in `vela-core`.
//!
//! Feature: vela-streaming-platform, Property 36
//!
//! Property 36: Node availability is always exactly one of two states. Every
//! member's availability is always exactly `Available` or `Unavailable`;
//! setting availability transitions between exactly these two states and never
//! any other. The `NodeAvailability` enum has exactly two variants, so this
//! test enumerates them exhaustively and asserts that every known member always
//! reports one of them, and that a `set_availability` to a given state results
//! in exactly that state.
//!
//! Validates: Requirements 9.3

use proptest::prelude::*;
use vela_core::{ClusterMetadata, Member, NodeAvailability, NodeId};

/// The two — and only two — availability states. `NodeAvailability` is a
/// two-state enum (Requirement 9.3); listing both variants here lets the test
/// assert exhaustively that an observed state is one of exactly these and
/// nothing else.
const BOTH_STATES: [NodeAvailability; 2] =
    [NodeAvailability::Available, NodeAvailability::Unavailable];

/// Generate one of the exactly-two availability states.
fn availability_strategy() -> impl Strategy<Value = NodeAvailability> {
    prop_oneof![
        Just(NodeAvailability::Available),
        Just(NodeAvailability::Unavailable),
    ]
}

/// Build a cluster of `n` members named `node-0..node-{n-1}`, each starting in
/// the supplied initial availability state.
fn cluster(initial: &[NodeAvailability]) -> ClusterMetadata {
    let mut meta = ClusterMetadata::new();
    meta.members = initial
        .iter()
        .enumerate()
        .map(|(i, &availability)| Member {
            id: NodeId::new(format!("node-{i}")),
            addr: format!("node-{i}:7001"),
            availability,
        })
        .collect();
    meta
}

/// Assert every member of `meta` reports an availability that is exactly one of
/// the two `NodeAvailability` states — never absent, never anything else.
fn assert_every_member_is_two_state(meta: &ClusterMetadata) -> Result<(), TestCaseError> {
    for member in &meta.members {
        let observed = meta.availability(&member.id);
        // A known member always has a state (never `None`).
        prop_assert_eq!(observed, Some(member.availability));
        // And that state is exactly one of the two enum variants.
        let state = observed.expect("a known member always has a state");
        prop_assert!(
            BOTH_STATES.contains(&state),
            "availability {:?} is not one of the two valid states",
            state
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 36
    #[test]
    fn node_availability_is_always_exactly_one_of_two_states(
        // Initial availability for each of 1..=8 members.
        initial in proptest::collection::vec(availability_strategy(), 1..=8),
        // A sequence of (member-index, new-state) operations to apply. The
        // index is taken modulo the member count so it always targets a real
        // member; both states appear because the generator draws from both.
        ops in proptest::collection::vec(
            (any::<usize>(), availability_strategy()),
            0..=64,
        ),
    ) {
        let mut meta = cluster(&initial);
        let member_count = meta.members.len();

        // Invariant holds from the start: every member is in exactly one of the
        // two states (Requirement 9.3).
        assert_every_member_is_two_state(&meta)?;

        for (raw_index, new_state) in ops {
            let idx = raw_index % member_count;
            let target = meta.members[idx].id.clone();

            // Setting an existing member's availability always succeeds and
            // reports that a member was updated.
            prop_assert!(meta.set_availability(&target, new_state));

            // The set transitions to exactly the requested state — no other.
            prop_assert_eq!(meta.availability(&target), Some(new_state));

            // After every operation the whole cluster still holds the
            // two-state invariant for every member.
            assert_every_member_is_two_state(&meta)?;
        }
    }
}
