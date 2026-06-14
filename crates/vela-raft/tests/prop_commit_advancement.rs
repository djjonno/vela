//! Property test for commit-index advancement in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 31
//!
//! Property 31: Commit index advances exactly to the highest
//! majority-replicated current-term entry. For any leader and replication
//! state, the leader advances its commit index to an entry's index once that
//! entry — created in the leader's current term — is replicated on a strict
//! majority of the group, and never advances past an entry that is not yet on a
//! majority (Raft paper §5.4.2).
//!
//! The test drives real replication over the deterministic [`SimCluster`]
//! harness across varied seeds, cluster sizes, and proposal counts. It elects a
//! single clean leader, proposes a varied number of records, lets replication
//! settle, then asserts the leader's commit index equals the *independently
//! computed* highest log index that is both of the current term and present on a
//! majority of replicas. It additionally exercises a minority partition: with at
//! most a minority of followers severed, a majority is still reachable, so the
//! commit index must still advance to cover every proposed entry.
//!
//! Validates: Requirements 8.5, 8.6

use proptest::prelude::*;
use vela_raft::sim::SimCluster;
use vela_raft::{
    EntryPayload, LogStorage, NodeId, PayloadKind, Role, TimerKind, ELECTION_TIMEOUT_BASE,
};

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

/// Independently compute the highest log index that is both of term `term` and
/// replicated on a strict majority of the `node_count` replicas — i.e. the
/// commit index the leader is *allowed* to reach under Raft's commit rule.
///
/// Returns `None` when no current-term entry has reached a majority yet.
fn highest_majority_current_term_index(
    sim: &SimCluster,
    node_count: u64,
    term: u64,
) -> Option<u64> {
    let majority = node_count / 2 + 1;
    // The leader's last index bounds the search; any index beyond it cannot be
    // replicated anywhere.
    let leader = sim.leader()?;
    let last = sim.node(leader)?.log().last_index()?;

    let mut highest = None;
    for idx in 0..=last {
        let replicas = (0..node_count)
            .filter(|&i| {
                sim.node(NodeId(i))
                    .and_then(|n| n.log().entry(idx))
                    .map(|e| e.term == term)
                    .unwrap_or(false)
            })
            .count() as u64;
        if replicas >= majority {
            highest = Some(idx);
        }
    }
    highest
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 31
    #[test]
    fn commit_index_reaches_highest_majority_current_term_entry(
        seed in any::<u64>(),
        // Exercise both a 3-node and a 5-node group.
        five_nodes in any::<bool>(),
        // Vary how many records are proposed to the leader.
        num_entries in 1u8..=20,
        // Optionally sever a minority of followers; a majority stays reachable.
        partition_minority in any::<bool>(),
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let mut sim = SimCluster::new(node_count, seed);

        // Arm a single node so it times out first and wins a clean election;
        // heartbeats then keep it leader for the rest of the run.
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = elect_leader(&mut sim, 400)
            .expect("a single armed candidate should win a clean election");
        prop_assert_eq!(sim.role(leader), Some(Role::Leader));

        let term = sim.node(leader).expect("leader exists").current_term();

        // Optionally partition a minority of followers away from the whole
        // cluster. A 3-node group can lose 1 (majority 2 remains); a 5-node
        // group can lose 2 (majority 3 remains). The leader is never severed.
        if partition_minority {
            let minority = (node_count - 1) / 2; // 1 for n=3, 2 for n=5
            let followers: Vec<NodeId> = (0..node_count)
                .map(NodeId)
                .filter(|&id| id != leader)
                .take(minority as usize)
                .collect();
            for &f in &followers {
                for i in 0..node_count {
                    let other = NodeId(i);
                    if other != f {
                        sim.partition(f, other);
                    }
                }
            }
        }

        // Propose the records on the leader.
        for k in 0..num_entries {
            sim.propose(leader, EntryPayload::new(PayloadKind::Record, vec![k]));
        }

        // Let replication settle. A healthy cluster is never quiescent
        // (heartbeats fire forever), so run a generous step budget and stop
        // early once the commit index covers every proposed entry.
        let expected_last = u64::from(num_entries) - 1;
        for _ in 0..6000 {
            if sim.node(leader).expect("leader exists").commit_index() == Some(expected_last) {
                break;
            }
            sim.step();
        }

        let commit = sim.node(leader).expect("leader exists").commit_index();

        // The commit index equals the independently computed highest index that
        // is current-term and majority-replicated (R8.5, R8.6).
        let allowed = highest_majority_current_term_index(&sim, node_count, term);
        prop_assert_eq!(
            commit,
            allowed,
            "commit index {:?} != highest majority current-term index {:?} \
             (node_count={}, seed={}, entries={}, partition={})",
            commit, allowed, node_count, seed, num_entries, partition_minority
        );

        // With a majority always reachable, every proposed current-term entry
        // must have committed, so the commit index reaches the last index.
        prop_assert_eq!(
            commit,
            Some(expected_last),
            "commit index should advance to the last proposed entry {} \
             (node_count={}, seed={}, entries={}, partition={})",
            expected_last, node_count, seed, num_entries, partition_minority
        );

        // The commit index never runs past the leader's own log (R8.6).
        let leader_last = sim.node(leader).expect("leader exists").log().last_index();
        prop_assert!(
            commit <= leader_last,
            "commit index {:?} exceeds leader last index {:?}",
            commit, leader_last
        );
    }
}
