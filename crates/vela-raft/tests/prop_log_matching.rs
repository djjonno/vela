//! Property test for the Log Matching Property in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 35
//!
//! Property 35: Log Matching holds across the group. For *any* two Raft nodes
//! in a group, if their logs each contain an entry with the same index and the
//! same term, then their logs are identical in every entry up to and including
//! that index (Raft paper §5.3). This is the structural invariant that
//! `(prev_log_index, prev_log_term)` consistency-checking on every
//! `AppendEntries` is designed to preserve, and it must hold continuously —
//! mid-replication and under network faults — not merely once a cluster has
//! quiesced.
//!
//! The test drives real replication over the deterministic [`SimCluster`]
//! harness across varied seeds, cluster sizes, and interleaved proposal counts,
//! while injecting message reorder, duplication, a modest drop rate, and an
//! optional transient minority partition. After every proposal round, and again
//! after the run settles, it inspects *every* ordered pair of nodes: at each
//! index where both logs hold an entry of equal term it asserts the two logs'
//! `0..=index` prefixes are byte-for-byte identical (same index, term, and
//! payload at every position).
//!
//! Validates: Requirements 8.10

use std::time::Duration;

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

/// Assert the Log Matching Property across every ordered pair of nodes.
///
/// For each pair `(a, b)` and each index `idx` at which both logs hold an entry
/// of the *same term*, the two logs must be identical at every index
/// `0..=idx`. Because Raft logs are gap-free from index 0, comparing the
/// `read(0, idx)` prefixes (each a `Vec<LogEntry>` carrying index + term +
/// payload) is exactly the prefix-equality the property demands.
fn assert_log_matching(sim: &SimCluster, node_count: u64, ctx: &str) -> Result<(), TestCaseError> {
    for a in 0..node_count {
        for b in (a + 1)..node_count {
            let log_a = sim.node(NodeId(a)).expect("node a exists").log();
            let log_b = sim.node(NodeId(b)).expect("node b exists").log();

            // Only indices present in both logs can match; cap at the shorter.
            let max_common = match (log_a.last_index(), log_b.last_index()) {
                (Some(x), Some(y)) => x.min(y),
                _ => continue,
            };

            for idx in 0..=max_common {
                let term_a = log_a.entry(idx).map(|e| e.term);
                let term_b = log_b.entry(idx).map(|e| e.term);
                // Same index + same term => prefixes must be identical.
                if term_a.is_some() && term_a == term_b {
                    let prefix_a = log_a.read(0, idx);
                    let prefix_b = log_b.read(0, idx);
                    prop_assert_eq!(
                        prefix_a,
                        prefix_b,
                        "Log Matching violated between node {} and node {}: \
                         entries match at index {} (term {:?}) but their 0..={} \
                         prefixes differ; {}",
                        a,
                        b,
                        idx,
                        term_a,
                        idx,
                        ctx
                    );
                }
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 35
    #[test]
    fn log_matching_holds_across_the_group(
        seed in any::<u64>(),
        // Exercise both a 3-node and a 5-node group.
        five_nodes in any::<bool>(),
        // Vary how many records are proposed to the leader.
        num_entries in 1u8..=20,
        // Network faults exercised during the replication phase.
        reorder in any::<bool>(),
        duplicate in any::<bool>(),
        drop in any::<bool>(),
        // Optionally sever a single minority follower, then heal it.
        transient_partition in any::<bool>(),
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let mut sim = SimCluster::new(node_count, seed);

        // Arm a single node so it times out first and wins a clean election
        // before any faults are injected; heartbeats then keep it leader.
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = elect_leader(&mut sim, 400)
            .expect("a single armed candidate should win a clean election");
        prop_assert_eq!(sim.role(leader), Some(Role::Leader));

        // Log Matching holds trivially at the outset.
        assert_log_matching(&sim, node_count, "after election")?;

        // Inject the requested faults for the replication phase.
        if reorder {
            sim.set_reorder(true, Duration::from_millis(8));
        }
        if duplicate {
            sim.set_duplicate_probability(0.25);
        }
        if drop {
            // A modest drop rate stresses retry/backoff without starving the
            // majority of all progress.
            sim.set_drop_probability(0.1);
        }

        // Optionally sever a single minority follower from the rest of the
        // group. A majority always remains connected, so replication still
        // makes progress and the severed node must later converge once healed.
        let severed: Option<NodeId> = if transient_partition {
            (0..node_count).map(NodeId).find(|&id| id != leader)
        } else {
            None
        };
        if let Some(f) = severed {
            for i in 0..node_count {
                let other = NodeId(i);
                if other != f {
                    sim.partition(f, other);
                }
            }
        }

        // Interleave proposals with stepping so entries are appended while
        // replication, faults, and the optional partition are all in play.
        for k in 0..num_entries {
            sim.propose(leader, EntryPayload::new(PayloadKind::Record, vec![k]));
            for _ in 0..40 {
                sim.step();
            }
            // The invariant must hold continuously, even mid-replication.
            assert_log_matching(&sim, node_count, "during interleaved replication")?;
        }

        // Heal the transient partition so the lagging follower can catch up.
        if let Some(f) = severed {
            for i in 0..node_count {
                let other = NodeId(i);
                if other != f {
                    sim.heal(f, other);
                }
            }
        }

        // Let replication settle. A healthy cluster is never quiescent
        // (heartbeats fire forever), so run a generous bounded budget.
        for _ in 0..6000 {
            sim.step();
        }

        // The Log Matching Property holds across every pair of nodes (R8.10).
        assert_log_matching(
            &sim,
            node_count,
            &format!(
                "final (seed={seed}, n={node_count}, entries={num_entries}, \
                 reorder={reorder}, duplicate={duplicate}, drop={drop}, \
                 transient_partition={transient_partition})"
            ),
        )?;
    }
}
