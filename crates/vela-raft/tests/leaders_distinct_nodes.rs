//! Example test: leaders of different partitions may reside on different nodes.
//!
//! Feature: vela-streaming-platform
//!
//! Requirement 7.11: WHERE Raft_Groups belong to different Partitions, THE Vela
//! SHALL allow their Leaders to reside on different Nodes.
//!
//! Each [`SimCluster`] models a single partition's Raft group. Vela runs one
//! such group per partition, independently, so there is no shared state forcing
//! every partition's leader onto the same node. This test demonstrates that
//! freedom directly: it drives two independent groups and shows their elected
//! leaders land on two distinct nodes.
//!
//! The example is deterministic rather than probabilistic. In each group it arms
//! exactly one node's election timer; a single uncontested candidate wins its
//! election outright (its self-vote plus follower grants form a majority), so we
//! can steer group A's leadership to one node and group B's to another and then
//! assert the two leaders differ.
//!
//! Validates: Requirements 7.11

use vela_raft::sim::{SimCluster, StepOutcome};
use vela_raft::{NodeId, Role, TimerKind, ELECTION_TIMEOUT_BASE};

/// Build a fresh three-node group, arm `candidate`'s election timer, and run the
/// simulation until some node becomes leader (or the step budget is exhausted).
///
/// Returns the elected leader's id. Panics if no leader emerges, which would
/// itself be a failure of the single-candidate liveness guarantee.
fn elect_leader_on(seed: u64, candidate: NodeId) -> NodeId {
    let mut sim = SimCluster::new(3, seed);
    sim.arm(candidate, TimerKind::Election, ELECTION_TIMEOUT_BASE);

    // A healthy cluster heartbeats forever, so bound the run. A clean,
    // single-candidate election resolves well within this budget.
    let budget = 2000;
    for _ in 0..budget {
        if let Some(leader) = sim.leader() {
            return leader;
        }
        if matches!(sim.step(), StepOutcome::Idle) {
            break;
        }
    }
    sim.leader()
        .expect("a single uncontested candidate should win its election")
}

/// Two independent partition groups can elect leaders on different nodes.
///
/// Group A is steered to elect its leader on `NodeId(0)`; group B on `NodeId(1)`.
/// Because the groups are wholly independent `SimCluster`s, nothing pins both
/// leaders to the same node, and the two elected leaders are distinct.
#[test]
fn leaders_of_independent_partitions_can_differ() {
    // Two independent Raft groups, one per partition.
    let group_a_leader = elect_leader_on(1, NodeId(0));
    let group_b_leader = elect_leader_on(2, NodeId(1));

    // The candidate we armed in each group is the one that won.
    assert_eq!(group_a_leader, NodeId(0), "group A leader should be node 0");
    assert_eq!(group_b_leader, NodeId(1), "group B leader should be node 1");

    // The crux of Requirement 7.11: different partitions' leaders reside on
    // different nodes.
    assert_ne!(
        group_a_leader, group_b_leader,
        "leaders of independent partitions should be allowed to reside on \
         different nodes"
    );
}

/// Independent groups also remain free to elect leaders on the *same* node.
///
/// "Allow ... to reside on different Nodes" (7.11) is permissive, not a mandate:
/// the per-partition design must not force leaders apart any more than it forces
/// them together. Steering both groups to the same candidate confirms colocation
/// is equally possible, so leadership placement is genuinely unconstrained across
/// partitions.
#[test]
fn independent_partitions_may_also_share_a_leader_node() {
    let group_a_leader = elect_leader_on(3, NodeId(2));
    let group_b_leader = elect_leader_on(4, NodeId(2));

    assert_eq!(group_a_leader, NodeId(2));
    assert_eq!(group_b_leader, NodeId(2));
    assert_eq!(
        group_a_leader, group_b_leader,
        "independent partitions may colocate their leaders on one node"
    );
}

/// Each group elects exactly one leader, and that leader genuinely holds the
/// `Leader` role in its own group — guarding against a leader "leaking" across
/// the independent simulations.
#[test]
fn each_group_has_its_own_single_leader() {
    let mut group_a = SimCluster::new(3, 1);
    let mut group_b = SimCluster::new(3, 2);
    group_a.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
    group_b.arm(NodeId(1), TimerKind::Election, ELECTION_TIMEOUT_BASE);

    for sim in [&mut group_a, &mut group_b] {
        for _ in 0..2000 {
            if sim.leader().is_some() {
                break;
            }
            if matches!(sim.step(), StepOutcome::Idle) {
                break;
            }
        }
    }

    let a_leader = group_a.leader().expect("group A elects a leader");
    let b_leader = group_b.leader().expect("group B elects a leader");

    assert_eq!(group_a.role(a_leader), Some(Role::Leader));
    assert_eq!(group_b.role(b_leader), Some(Role::Leader));
    assert_ne!(a_leader, b_leader);
}
