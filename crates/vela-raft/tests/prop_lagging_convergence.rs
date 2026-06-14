//! Property test for lagging-follower convergence in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 34
//!
//! Property 34: A lagging follower converges to the leader's log. A follower
//! that has fallen behind — because it was partitioned away while the rest of
//! the group committed new entries — eventually has its log brought into full
//! agreement with the leader's once it can communicate again. The leader
//! achieves this by sending the missing entries (heartbeats carry entries from
//! each peer's `next_index`), backing up `next_index` on rejection until the
//! logs match and then shipping the suffix (Raft paper §5.3).
//!
//! The test drives real replication over the deterministic [`SimCluster`]
//! harness across varied seeds, cluster sizes, and proposal counts. It elects a
//! single clean leader, severs ONE follower from every other node, proposes a
//! batch of records that the remaining majority commits, then heals the
//! partition and keeps stepping. It asserts the previously-lagging follower's
//! log converges to match the leader's exactly — identical last index and an
//! identical entry (index, term, payload) at every position.
//!
//! Validates: Requirements 8.9

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

/// Returns `true` when `follower`'s log is identical to `leader`'s: the same
/// last index and an equal [`LogEntry`] (index, term, payload) at every index.
fn logs_agree(sim: &SimCluster, leader: NodeId, follower: NodeId) -> bool {
    let leader_node = match sim.node(leader) {
        Some(n) => n,
        None => return false,
    };
    let follower_node = match sim.node(follower) {
        Some(n) => n,
        None => return false,
    };

    if leader_node.log().last_index() != follower_node.log().last_index() {
        return false;
    }

    match leader_node.log().last_index() {
        None => true, // both empty
        Some(last) => (0..=last).all(|idx| {
            match (leader_node.log().entry(idx), follower_node.log().entry(idx)) {
                (Some(le), Some(fe)) => le == fe,
                _ => false,
            }
        }),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 34
    #[test]
    fn lagging_follower_converges_to_leader_log(
        seed in any::<u64>(),
        // Exercise both a 3-node and a 5-node group.
        five_nodes in any::<bool>(),
        // Vary how many records are committed while one follower is severed.
        num_entries in 1u8..=15,
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let mut sim = SimCluster::new(node_count, seed);

        // Arm a single node so it times out first and wins a clean election;
        // heartbeats then keep it leader while the other nodes are reachable.
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = elect_leader(&mut sim, 400)
            .expect("a single armed candidate should win a clean election");
        prop_assert_eq!(sim.role(leader), Some(Role::Leader));

        // Pick a follower (never the leader) to fall behind.
        let lagging = (0..node_count)
            .map(NodeId)
            .find(|&id| id != leader)
            .expect("a 3+ node group always has a follower besides the leader");

        // Sever the lagging follower from every other node. The remaining
        // nodes still form a majority (2 of 3, or 4 of 5), so the leader can
        // keep committing without it.
        for i in 0..node_count {
            let other = NodeId(i);
            if other != lagging {
                sim.partition(lagging, other);
            }
        }

        // Propose the records on the leader and let the reachable majority
        // commit them. The severed follower receives none of these.
        let expected_last = u64::from(num_entries) - 1;
        for k in 0..num_entries {
            sim.propose(leader, EntryPayload::new(PayloadKind::Record, vec![k]));
        }
        for _ in 0..6000 {
            if sim.node(leader).expect("leader exists").commit_index() == Some(expected_last) {
                break;
            }
            sim.step();
        }
        prop_assert_eq!(
            sim.node(leader).expect("leader exists").commit_index(),
            Some(expected_last),
            "the reachable majority should commit every proposed entry while \
             the follower is severed (node_count={}, seed={}, entries={})",
            node_count, seed, num_entries
        );

        // The severed follower is genuinely behind before we heal it.
        prop_assert!(
            sim.node(lagging).expect("follower exists").log().last_index()
                < Some(expected_last),
            "the severed follower should lag the leader before healing"
        );

        // Heal the partition: the follower can communicate again.
        for i in 0..node_count {
            let other = NodeId(i);
            if other != lagging {
                sim.heal(lagging, other);
            }
        }

        // Keep stepping. The leader brings the lagging follower into agreement
        // by backing up its `next_index` and shipping the missing entries. A
        // generous budget absorbs any single election disruption the rejoining
        // node may cause (it can never win — its log is not up to date — so an
        // up-to-date node leads again and finishes the catch-up).
        let mut converged = false;
        for _ in 0..40_000 {
            if let Some(current_leader) = sim.leader() {
                if logs_agree(&sim, current_leader, lagging) {
                    converged = true;
                    break;
                }
            }
            sim.step();
        }

        prop_assert!(
            converged,
            "lagging follower did not converge to the leader's log \
             (node_count={}, seed={}, entries={})",
            node_count, seed, num_entries
        );

        // Final, explicit agreement check against whoever leads at the end.
        let final_leader = sim.leader().expect("a leader should exist after healing");
        let leader_last = sim.node(final_leader).expect("leader exists").log().last_index();
        let lagging_last = sim.node(lagging).expect("follower exists").log().last_index();
        prop_assert_eq!(
            lagging_last,
            leader_last,
            "converged follower last index {:?} != leader last index {:?}",
            lagging_last, leader_last
        );
        prop_assert!(
            logs_agree(&sim, final_leader, lagging),
            "follower log diverges from leader log after convergence \
             (node_count={}, seed={}, entries={})",
            node_count, seed, num_entries
        );
    }
}
