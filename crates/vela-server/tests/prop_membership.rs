//! Property-based test for the membership availability state machine.
//!
//! Feature: vela-streaming-platform, Property 37
//!
//! Property 37: Three consecutive missed heartbeats mark a node unavailable,
//! and recovery restores it.
//!
//! *For any* member node, after three consecutive missed 1-second heartbeat
//! intervals (simulated clock) the node is marked unavailable, and a subsequent
//! successful response from a node currently marked unavailable marks it
//! available again.
//!
//! **Validates: Requirements 9.4, 9.5**
//!
//! The heartbeat loop's clock is simulated here by driving a generated sequence
//! of discrete heartbeat outcomes (miss or success) directly into
//! [`MembershipState`] — each event stands for one elapsed 1-second interval —
//! rather than spinning a real timer. A reference model tracks the expected
//! consecutive-miss streak and availability independently of the implementation
//! and is compared against the actual state after every event.

use proptest::prelude::*;

use vela_core::NodeAvailability;
use vela_server::membership::{MembershipState, MISSED_HEARTBEATS_THRESHOLD};

/// One simulated heartbeat outcome on the (simulated) 1-second cadence.
#[derive(Debug, Clone, Copy)]
enum Heartbeat {
    /// The heartbeat failed (connection error, timeout, or error reply).
    Miss,
    /// The heartbeat succeeded.
    Success,
}

/// An independent reference model of the availability rule (Requirements 9.4,
/// 9.5). It encodes the spec directly: a node flips to unavailable on the edge
/// of the Nth consecutive miss, and a success from an unavailable node restores
/// it and clears the streak.
struct Model {
    consecutive_misses: u32,
    availability: NodeAvailability,
    threshold: u32,
}

impl Model {
    fn new(threshold: u32) -> Self {
        Self {
            consecutive_misses: 0,
            availability: NodeAvailability::Available,
            threshold,
        }
    }

    /// Apply one heartbeat outcome, returning the availability transition the
    /// implementation is expected to report (`Some` only on the edge).
    fn apply(&mut self, hb: Heartbeat) -> Option<NodeAvailability> {
        match hb {
            Heartbeat::Miss => {
                self.consecutive_misses = self.consecutive_misses.saturating_add(1);
                // Unavailable exactly when there have been >= threshold
                // consecutive misses since the last success, and only on the
                // edge (not already unavailable).
                if self.consecutive_misses >= self.threshold
                    && self.availability != NodeAvailability::Unavailable
                {
                    self.availability = NodeAvailability::Unavailable;
                    Some(NodeAvailability::Unavailable)
                } else {
                    None
                }
            }
            Heartbeat::Success => {
                // A success resets the streak ...
                self.consecutive_misses = 0;
                // ... and restores availability only when recovering from an
                // unavailable node.
                if self.availability != NodeAvailability::Available {
                    self.availability = NodeAvailability::Available;
                    Some(NodeAvailability::Available)
                } else {
                    None
                }
            }
        }
    }
}

prop_compose! {
    /// A non-empty sequence of heartbeat outcomes biased toward misses so runs
    /// regularly reach the unavailable threshold and then recover.
    fn heartbeat_sequence()(
        events in prop::collection::vec(
            prop_oneof![
                3 => Just(Heartbeat::Miss),
                2 => Just(Heartbeat::Success),
            ],
            1..50usize,
        )
    ) -> Vec<Heartbeat> {
        events
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Feature: vela-streaming-platform, Property 37
    ///
    /// Drive a generated sequence of miss/success heartbeats through
    /// `MembershipState` and assert it matches the independent reference model
    /// after every event: the node becomes `Unavailable` exactly on the edge of
    /// the third consecutive miss, a success from an unavailable node restores
    /// `Available` and resets the streak, and the returned transition fires only
    /// on those edges.
    #[test]
    fn three_misses_mark_unavailable_and_success_restores(events in heartbeat_sequence()) {
        let threshold = MISSED_HEARTBEATS_THRESHOLD;
        let mut state = MembershipState::new();
        let mut model = Model::new(threshold);

        // A fresh peer starts available with no recorded misses.
        prop_assert_eq!(state.availability(), NodeAvailability::Available);
        prop_assert_eq!(state.consecutive_misses(), 0);

        for (step, hb) in events.into_iter().enumerate() {
            let expected_transition = model.apply(hb);
            let actual_transition = match hb {
                Heartbeat::Miss => state.record_miss(),
                Heartbeat::Success => state.record_success(),
            };

            // The transition edge (Some/None and which state) matches the model.
            prop_assert_eq!(
                actual_transition,
                expected_transition,
                "transition mismatch at step {} ({:?})",
                step,
                hb
            );
            // The resulting availability and miss streak match the model.
            prop_assert_eq!(
                state.availability(),
                model.availability,
                "availability mismatch at step {} ({:?})",
                step,
                hb
            );
            prop_assert_eq!(
                state.consecutive_misses(),
                model.consecutive_misses,
                "miss-count mismatch at step {} ({:?})",
                step,
                hb
            );

            // Cross-check the core invariants of Property 37 directly against
            // the streak, independent of the model's bookkeeping.
            if state.consecutive_misses() >= threshold {
                prop_assert_eq!(
                    state.availability(),
                    NodeAvailability::Unavailable,
                    "a streak of >= {} misses must be unavailable (step {})",
                    threshold,
                    step
                );
            }
            if matches!(hb, Heartbeat::Success) {
                // A success always clears the streak and leaves the node
                // available (Requirement 9.5).
                prop_assert_eq!(state.consecutive_misses(), 0);
                prop_assert_eq!(state.availability(), NodeAvailability::Available);
            }
        }
    }
}
