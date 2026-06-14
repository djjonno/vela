//! Property test for single-leader-per-term election in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 25
//!
//! Property 25: A majority of same-term votes elects exactly one leader per
//! term. For any Raft group and term, a candidate that collects votes from a
//! strict majority of the group within that term becomes leader, and no term
//! ever has more than one leader.
//!
//! The test drives real elections over the deterministic [`SimCluster`] harness
//! across varied seeds and cluster sizes. It arms one or more nodes' election
//! timers, runs the cluster to completion under a step budget, and continuously
//! checks the core safety invariant after every delivered event: no two distinct
//! nodes are ever leader in the same term. When exactly one node is armed (a
//! clean, uncontested election) it additionally asserts that exactly one leader
//! emerges.
//!
//! Validates: Requirements 7.4, 7.10

use std::collections::HashMap;

use proptest::prelude::*;
use vela_raft::sim::{SimCluster, StepOutcome};
use vela_raft::{NodeId, Role, TimerKind, ELECTION_TIMEOUT_BASE};

/// Record the leader (if any) currently believed by each replica for its term,
/// folding the observation into `seen`, a term -> leader map accumulated across
/// the whole run.
///
/// Returns `Err(message)` if a term is observed with two *distinct* leaders,
/// which would violate Requirement 7.10 (at most one leader per term).
fn observe_leaders(
    sim: &SimCluster,
    node_count: u64,
    seen: &mut HashMap<u64, NodeId>,
) -> Result<(), String> {
    for i in 0..node_count {
        let id = NodeId(i);
        if sim.role(id) == Some(Role::Leader) {
            let term = sim.node(id).expect("node exists").current_term();
            match seen.get(&term) {
                Some(&prev) if prev != id => {
                    return Err(format!("term {term} had two leaders: {prev:?} and {id:?}"));
                }
                _ => {
                    seen.insert(term, id);
                }
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 25
    #[test]
    fn majority_votes_elect_exactly_one_leader_per_term(
        seed in any::<u64>(),
        // Exercise both an odd-small and a larger cluster size.
        five_nodes in any::<bool>(),
        // Bitmask choosing which nodes start an election; forced non-empty below.
        arm_mask in any::<u32>(),
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let mut sim = SimCluster::new(node_count, seed);

        // Decide which nodes to arm. Guarantee at least one armed node so an
        // election actually starts; otherwise fall back to arming node 0.
        let mut armed: Vec<NodeId> = (0..node_count)
            .filter(|&i| (arm_mask >> i) & 1 == 1)
            .map(NodeId)
            .collect();
        if armed.is_empty() {
            armed.push(NodeId(0));
        }
        let single_candidate = armed.len() == 1;

        for &node in &armed {
            sim.arm(node, TimerKind::Election, ELECTION_TIMEOUT_BASE);
        }

        // Run to completion under a generous step budget. Heartbeats keep a
        // healthy cluster perpetually busy, so a bound is required; contended
        // elections may need several rounds (each at a higher term) to resolve.
        let mut seen: HashMap<u64, NodeId> = HashMap::new();
        let mut ever_leader = false;

        let budget = 4000;
        for _ in 0..budget {
            // Check the invariant after each event, including the initial state.
            if let Err(msg) = observe_leaders(&sim, node_count, &mut seen) {
                prop_assert!(false, "{}", msg);
            }
            if sim.leader().is_some() {
                ever_leader = true;
            }
            if matches!(sim.step(), StepOutcome::Idle) {
                break;
            }
        }
        // One final observation after the loop's last step.
        if let Err(msg) = observe_leaders(&sim, node_count, &mut seen) {
            prop_assert!(false, "{}", msg);
        }
        if sim.leader().is_some() {
            ever_leader = true;
        }

        // Safety: never more than one leader for any single term. By
        // construction `seen` only ever held one leader per term; assert it is
        // internally consistent (every recorded leader still maps uniquely).
        let mut leaders_by_term: HashMap<u64, NodeId> = HashMap::new();
        for (&term, &leader) in &seen {
            if let Some(&prev) = leaders_by_term.get(&term) {
                prop_assert_eq!(prev, leader, "two leaders recorded for term {}", term);
            }
            leaders_by_term.insert(term, leader);
        }

        // Liveness for the clean case: a single uncontested candidate must win
        // its election outright (self-vote plus follower grants form a majority).
        if single_candidate {
            prop_assert!(
                ever_leader,
                "a single-candidate election should elect exactly one leader \
                 (node_count={node_count}, seed={seed})"
            );
        }
    }
}
