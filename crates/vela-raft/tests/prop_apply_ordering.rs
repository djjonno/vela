//! Property test for state-machine apply ordering (Property 33).
//!
//! Feature: vela-streaming-platform, Property 33
//!
//! Property 33: The state machine applies committed entries once, in order,
//! with no gaps. Newly committed log entries are surfaced for application in
//! strictly ascending index order, each exactly once, with no skipped index —
//! the sequence a node surfaces is `0, 1, 2, ...` up to its highest applied
//! index, with no duplicate and no hole (Raft paper §5.3; Requirement 8.8).
//!
//! The harness records, per node, every entry that node surfaced via
//! `RaftOutput.committed`, in delivery order (`SimCluster::committed`). Because
//! a real leader is never quiescent — heartbeats fire forever — and because the
//! run is deliberately stressed with message **reorder** and **duplication**
//! after a leader emerges, the same commit can be observed under repeated and
//! out-of-order deliveries. The state machine must nonetheless surface each
//! committed entry for application exactly once and strictly in index order.
//!
//! Over varied seeds and cluster sizes the test elects a single clean leader,
//! proposes a varied number of records, turns on reorder + duplication to
//! stress the apply path's idempotency, drives replication, and then for every
//! node asserts that the sequence of surfaced entry indices is exactly the
//! contiguous run `0..len` — which simultaneously proves it is strictly
//! ascending, starts at 0, has no gap, and contains no duplicate. It also
//! confirms the run is non-vacuous: the leader surfaces every proposed entry.
//!
//! Validates: Requirements 8.8

use std::time::Duration;

use proptest::prelude::*;

use vela_raft::sim::SimCluster;
use vela_raft::{EntryPayload, NodeId, PayloadKind, Role, TimerKind, ELECTION_TIMEOUT_BASE};

/// Step the cluster until some node believes itself leader, or until `budget`
/// steps are exhausted. Returns the leader's id, if one emerged.
fn elect_leader(sim: &mut SimCluster, budget: usize) -> Option<NodeId> {
    for _ in 0..budget {
        if let Some(leader) = sim.leader() {
            return Some(leader);
        }
        sim.step();
    }
    sim.leader()
}

/// Assert that the entries `node` surfaced for application form the contiguous
/// run `0, 1, ..., len-1`.
///
/// A single equality per position captures the whole property at once:
///
/// - **strictly ascending** — positions increase, so indices must too;
/// - **starts at 0 / no skipped index** — position `i` must carry index `i`,
///   so the first is 0 and there is never a hole;
/// - **exactly once / no duplicate** — a duplicate would repeat an index and
///   break the `index == i` equality at the next position.
fn check_apply_sequence(sim: &SimCluster, node: NodeId) -> Result<(), TestCaseError> {
    let surfaced = sim.committed(node);
    for (i, entry) in surfaced.iter().enumerate() {
        prop_assert_eq!(
            entry.index,
            i as u64,
            "node {:?} surfaced entry index {} at apply position {} \
             (expected a contiguous 0-based run); full sequence: {:?}",
            node,
            entry.index,
            i,
            surfaced.iter().map(|e| e.index).collect::<Vec<_>>()
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 33
    #[test]
    fn committed_entries_apply_once_in_order_without_gaps(
        seed in any::<u64>(),
        // Exercise both a 3-node and a 5-node group.
        five_nodes in any::<bool>(),
        // Vary how many records are proposed to the leader.
        num_entries in 1u8..=20,
        // Optionally stress the apply path with out-of-order deliveries.
        reorder in any::<bool>(),
        // Optionally stress idempotency with duplicated deliveries.
        duplicate in any::<bool>(),
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let mut sim = SimCluster::new(node_count, seed);

        // Arm a single node so it times out first and wins a clean election on a
        // quiet network; heartbeats then keep it leader for the rest of the run.
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = elect_leader(&mut sim, 400)
            .expect("a single armed candidate should win a clean election");
        prop_assert_eq!(sim.role(leader), Some(Role::Leader));

        // Now turn on the faults that stress the apply path. No drops: a drop
        // could unseat the lone leader, whereas reorder + duplication keep a
        // majority reachable while forcing repeated and out-of-order deliveries
        // of the very AppendEntries/replies that drive commits.
        sim.set_latency(Duration::from_millis(1));
        sim.set_reorder(reorder, Duration::from_millis(8));
        if duplicate {
            sim.set_duplicate_probability(0.5);
        }

        // Propose the records on the leader.
        for k in 0..num_entries {
            sim.propose(leader, EntryPayload::new(PayloadKind::Record, vec![k]));
        }

        // Drive replication. A faulted cluster is never quiescent (heartbeats
        // and duplicates keep firing), so run a generous budget and stop early
        // once the leader has surfaced every proposed entry for application. The
        // apply-ordering invariant is checked after *every* delivered event so
        // any transient out-of-order or duplicate apply is caught immediately.
        let expected_applied = u64::from(num_entries);
        for _ in 0..8000 {
            if sim.committed(leader).len() as u64 == expected_applied {
                // Verify once more after reaching the target, then stop.
                check_apply_sequence(&sim, leader)?;
                break;
            }
            sim.step();
            for id in 0..node_count {
                check_apply_sequence(&sim, NodeId(id))?;
            }
        }

        // Every node's surfaced sequence is a contiguous 0-based run (the core
        // property: in order, once each, no gaps).
        for id in 0..node_count {
            check_apply_sequence(&sim, NodeId(id))?;
        }

        // Non-vacuity: the leader actually applied every proposed entry, so the
        // ordering checks above ran against a fully populated sequence rather
        // than an empty one.
        prop_assert_eq!(
            sim.committed(leader).len() as u64,
            expected_applied,
            "leader {:?} surfaced {} entries, expected {} \
             (node_count={}, seed={}, reorder={}, duplicate={})",
            leader,
            sim.committed(leader).len(),
            expected_applied,
            node_count,
            seed,
            reorder,
            duplicate
        );
    }
}
