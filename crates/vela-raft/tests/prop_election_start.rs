//! Property test for idle-follower election start (Property 22).
//!
//! Feature: vela-streaming-platform, Property 22
//!
//! An idle follower becomes a candidate for the next term: when a follower's
//! election timeout elapses with no contact from a leader, it transitions to
//! Candidate, increments its term by exactly 1, votes for itself, and
//! broadcasts a `RequestVote` carrying the new term to every other node in its
//! Raft group.
//!
//! Validates: Requirements 7.2, 7.3, 7.5

use std::collections::BTreeSet;

use proptest::prelude::*;

use vela_raft::sim::{SimCluster, StepOutcome};
use vela_raft::{NodeId, RaftMessage, Role, TimerKind, ELECTION_TIMEOUT_BASE};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 22
    #[test]
    fn idle_follower_becomes_candidate_for_next_term(
        // Clusters of two or more: in a single-node group the self-vote is
        // already a majority, so the node becomes Leader rather than Candidate.
        node_count in 2u64..=7,
        seed in any::<u64>(),
        target_pick in any::<u64>(),
    ) {
        let mut sim = SimCluster::new(node_count, seed);
        let target = NodeId(target_pick % node_count);

        // Precondition: every node starts as an idle follower at term 0.
        prop_assert_eq!(sim.role(target), Some(Role::Follower));
        let prior_term = sim.node(target).expect("target exists").current_term();

        // Arm only the target's election timer, with no leader contact, then
        // deliver exactly that timeout.
        sim.arm(target, TimerKind::Election, ELECTION_TIMEOUT_BASE);

        let outcome = sim.step();
        let sends = match outcome {
            StepOutcome::Timer { node, kind, output } => {
                prop_assert_eq!(node, target);
                prop_assert_eq!(kind, TimerKind::Election);
                output.sends
            }
            other => {
                return Err(TestCaseError::fail(format!(
                    "expected the armed election timer to fire, got {other:?}"
                )));
            }
        };

        // It transitioned to Candidate (Requirement 7.2).
        prop_assert_eq!(sim.role(target), Some(Role::Candidate));

        // It incremented its term by exactly 1 (Requirements 7.2, 7.5).
        let new_term = sim.node(target).expect("target exists").current_term();
        prop_assert_eq!(new_term, prior_term + 1);

        // It voted for itself (Requirement 7.2).
        prop_assert_eq!(sim.node(target).expect("target exists").voted_for(), Some(target));

        // It broadcast a RequestVote to every other node in the group, each
        // carrying the new term and its own identity (Requirement 7.3).
        let peers: BTreeSet<NodeId> = (0..node_count)
            .map(NodeId)
            .filter(|&id| id != target)
            .collect();

        let mut request_vote_recipients: BTreeSet<NodeId> = BTreeSet::new();
        for (to, msg) in &sends {
            if let RaftMessage::RequestVote(rv) = msg {
                prop_assert_eq!(rv.term, new_term);
                prop_assert_eq!(rv.candidate_id, target);
                // No node solicits a vote from itself.
                prop_assert_ne!(*to, target);
                request_vote_recipients.insert(*to);
            }
        }

        prop_assert_eq!(request_vote_recipients, peers);
    }
}
