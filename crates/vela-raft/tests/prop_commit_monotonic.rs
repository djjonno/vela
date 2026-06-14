//! Property test for commit-index monotonicity (Property 32).
//!
//! Feature: vela-streaming-platform, Property 32
//!
//! Across any execution of a Raft group — including executions where messages
//! are reordered, duplicated, or dropped — no node's commit index ever
//! decreases. A committed prefix is durable: once a node has learned that an
//! entry is committed, no later event (a stale reply, a duplicated
//! AppendEntries, a delayed heartbeat) may walk that knowledge backward.
//!
//! The test drives a `SimCluster` through a randomized event schedule under
//! varied cluster sizes and network faults, interleaving client proposals, and
//! asserts after *every* delivered event that each node's `commit_index` is
//! greater than or equal to the value it last held. `CommitIndex` is
//! `Option<u64>`, whose derived ordering places `None` (uncommitted) below every
//! `Some(_)`, so the single comparison `current >= previous` captures
//! monotonicity for both the "first commit" and "advance" cases.
//!
//! Validates: Requirements 8.7

use std::time::Duration;

use proptest::prelude::*;

use vela_raft::sim::SimCluster;
use vela_raft::{
    CommitIndex, EntryPayload, NodeId, PayloadKind, Role, TimerKind, ELECTION_TIMEOUT_BASE,
};

/// Assert that no node's commit index has decreased since `last` was recorded,
/// then update `last` to the freshly observed values.
///
/// `last[i]` holds the highest commit index previously seen at `NodeId(i)`.
/// Because `CommitIndex = Option<u64>` orders `None < Some(_)`, the comparison
/// `current >= prev` rejects any backward step: `Some(k) -> None`,
/// `Some(k) -> Some(j < k)`, all caught.
fn check_monotonic(sim: &SimCluster, last: &mut [CommitIndex]) -> Result<(), TestCaseError> {
    for (i, prev) in last.iter_mut().enumerate() {
        let current = sim
            .node(NodeId(i as u64))
            .expect("node exists for its index")
            .commit_index();
        prop_assert!(
            current >= *prev,
            "commit index regressed on NodeId({i}): {prev:?} -> {current:?}",
        );
        *prev = current;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 32
    #[test]
    fn commit_index_never_decreases_on_any_node(
        node_count in 1u64..=5,
        seed in any::<u64>(),
        reorder in any::<bool>(),
        // Modest duplication and drop rates: high enough to exercise stale and
        // repeated deliveries, low enough that a leader can still emerge so the
        // commit index actually moves and the invariant is meaningfully tested.
        duplicate_pct in 0u64..=30,
        drop_pct in 0u64..=20,
        proposal_count in 0usize..=12,
        propose_every in 3usize..=9,
    ) {
        let mut sim = SimCluster::new(node_count, seed);

        // Configure the network faults named by the property statement.
        sim.set_latency(Duration::from_millis(1));
        sim.set_reorder(reorder, Duration::from_millis(8));
        sim.set_duplicate_probability(duplicate_pct as f64 / 100.0);
        sim.set_drop_probability(drop_pct as f64 / 100.0);

        // Per-node high-water mark of the commit index. Everyone starts
        // uncommitted (`None`), which is the floor of the ordering.
        let mut last: Vec<CommitIndex> = vec![None; node_count as usize];
        prop_assert!(last.iter().all(Option::is_none));

        // Arm every node's election timer so an election is contested under the
        // configured faults; the seeded jitter staggers them.
        for id in 0..node_count {
            sim.arm(NodeId(id), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        }

        // Drive a randomized schedule of delivered events, interleaving client
        // proposals to whichever node currently believes itself leader.
        let mut proposals_made = 0usize;
        const MAX_STEPS: usize = 1500;
        for step_idx in 0..MAX_STEPS {
            // Periodically inject a proposal at the current leader (if any).
            if proposals_made < proposal_count && step_idx % propose_every == 0 {
                if let Some(leader) = sim.leader() {
                    let payload = EntryPayload::new(
                        PayloadKind::Record,
                        (proposals_made as u64).to_le_bytes().to_vec(),
                    );
                    // A proposal drives the leader (it may commit immediately in
                    // a single-node group), so re-check the invariant after it.
                    sim.propose(leader, payload);
                    proposals_made += 1;
                    check_monotonic(&sim, &mut last)?;
                }
            }

            match sim.step() {
                vela_raft::sim::StepOutcome::Idle => {
                    // Nothing left to deliver and no more proposals to make:
                    // the schedule is exhausted, so we can stop early.
                    if proposals_made >= proposal_count {
                        break;
                    }
                    // Otherwise keep looping so the next proposal can be injected
                    // once a leader exists; advance time to let timers progress.
                    sim.advance(ELECTION_TIMEOUT_BASE);
                }
                _ => {
                    // After every delivered event, the invariant must hold.
                    check_monotonic(&sim, &mut last)?;
                }
            }
        }

        // Sanity: under low-fault, multi-node runs a leader should normally
        // emerge. This is not the property under test (monotonicity holds even
        // with no leader), so it is only a soft expectation we assert loosely:
        // at minimum, the run must have left every observed commit index in a
        // non-decreasing final state, which `check_monotonic` already enforced.
        if node_count > 1 && drop_pct == 0 && duplicate_pct == 0 {
            // With a clean network and multiple nodes, exactly one node may lead.
            let leaders = (0..node_count)
                .filter(|&id| sim.role(NodeId(id)) == Some(Role::Leader))
                .count();
            prop_assert!(leaders <= 1, "a clean run elected {leaders} leaders");
        }
    }
}
