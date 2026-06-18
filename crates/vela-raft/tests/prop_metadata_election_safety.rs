//! Simulation test for metadata-group election safety.
//!
//! Feature: cross-node-metadata-propagation, Property 5
//!
//! Property 5 (One metadata leader per term): for any execution of the metadata
//! group, at most one leader is elected in any single term, and a vote is
//! granted only to an at-least-as-up-to-date candidate.
//!
//! The metadata group (`__meta` / partition 0) is an *ordinary* `vela-raft`
//! group whose voter set is exactly the statically configured node set — every
//! node is a voter (requirements.md decision 3, Requirement 1.2). That is
//! precisely the topology [`SimCluster::new`] builds: `N` replicas where each
//! node's peer set is every other node. The metadata group therefore adds no
//! new consensus code, so this test reuses the deterministic Raft simulation
//! harness directly, instantiated with a metadata-group-shaped voter set, and
//! asserts the two consensus-safety invariants the metadata catalogue inherits
//! from `vela-raft`:
//!
//! 1. **At most one leader per term** (Raft §5.2): no two distinct replicas are
//!    ever leader in the same term.
//! 2. **Election restriction** (Raft §5.4.1): a replica grants its vote only to
//!    a candidate whose log is at least as up-to-date as its own, compared by
//!    last log term then last log index.
//!
//! To exercise the restriction non-trivially the simulation replicates real
//! metadata commands (`PayloadKind::Cluster` entries — the catalogue mutations
//! the metadata group agrees) through the group while injecting network faults,
//! so replicas' logs diverge and stale candidates genuinely arise. Both
//! invariants are checked after every delivered event across many seeds, cluster
//! sizes, and fault patterns.
//!
//! Validates: Requirements 2.2, 2.3

use std::collections::HashMap;

use proptest::prelude::*;
use vela_raft::sim::{SimCluster, StepOutcome};
use vela_raft::{
    EntryPayload, LogStorage, NodeId, PayloadKind, RaftMessage, Role, TimerKind,
    ELECTION_TIMEOUT_BASE,
};

/// A candidate's claimed log currency for a `(candidate, term)` election round:
/// the `(last_log_term, last_log_index)` it advertised in its `RequestVote`.
type ClaimMap = HashMap<(NodeId, u64), (Option<u64>, Option<u64>)>;

/// Lexicographic log-currency key matching the implementation and Raft §5.4.1:
/// compare by `(last_term, last_index)` with an empty log as the lowest
/// possible value (`term` defaults to 0, a missing index sorts below index 0).
fn log_key(term: Option<u64>, index: Option<u64>) -> (u64, i128) {
    (term.unwrap_or(0), index.map_or(-1i128, |i| i as i128))
}

/// Fold the leader (if any) each replica currently believes itself to be for
/// its term into `seen`, a term -> leader map accumulated across the run.
///
/// Returns `Err` if a single term is ever observed with two *distinct* leaders,
/// violating "at most one metadata leader per term" (Requirement 2.2, §5.2).
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

/// Inspect the effects of one delivered event for vote activity.
///
/// Every `RequestVote` a candidate broadcasts records its claimed log currency
/// for `(candidate, term)`. Every *granted* `RequestVoteReply` is then checked
/// against the voter's own log at the moment it voted: the candidate's claimed
/// log must be at least as up-to-date as the voter's, or the election
/// restriction (Requirement 2.3, §5.4.1) is violated.
///
/// A `RequestVoteReply` is addressed back to the candidate it answers, so the
/// reply's destination identifies the candidate and the reply's term identifies
/// the term the vote was cast in; the voter is the node that produced the reply.
/// Granting a vote never mutates the log, so the voter's log inspected right
/// after the step equals the log the decision was made against.
fn check_vote_grants(
    sim: &SimCluster,
    outcome: &StepOutcome,
    claims: &mut ClaimMap,
) -> Result<(), String> {
    let (voter, output) = match outcome {
        StepOutcome::Idle => return Ok(()),
        StepOutcome::Timer { node, output, .. } => (*node, output),
        StepOutcome::Message { to, output, .. } => (*to, output),
    };

    for (dest, msg) in &output.sends {
        match msg {
            RaftMessage::RequestVote(rv) => {
                claims.insert(
                    (rv.candidate_id, rv.term),
                    (rv.last_log_term, rv.last_log_index),
                );
            }
            RaftMessage::RequestVoteReply(reply) if reply.vote_granted => {
                let node = sim.node(voter).expect("voter exists");
                let voter_index = node.log().last_index();
                let voter_term = voter_index.and_then(|i| node.log().term_at(i));
                // `dest` is the candidate this granted reply answers.
                if let Some(&(cand_term, cand_index)) = claims.get(&(*dest, reply.term)) {
                    if log_key(cand_term, cand_index) < log_key(voter_term, voter_index) {
                        return Err(format!(
                            "voter {voter:?} granted its term-{} vote to candidate {:?} whose \
                             claimed log ({cand_term:?}, {cand_index:?}) is less up-to-date than \
                             the voter's own ({voter_term:?}, {voter_index:?})",
                            reply.term, *dest
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    // Feature: cross-node-metadata-propagation, Property 5
    #[test]
    fn metadata_group_elects_one_leader_per_term_and_honours_the_restriction(
        seed in any::<u64>(),
        // The metadata voter set is the whole cluster; exercise odd sizes 3 and 5.
        five_nodes in any::<bool>(),
        // Bitmask choosing which voters start an election; forced non-empty below.
        arm_mask in any::<u32>(),
        // 0 = clean network, 1 = reordering, 2 = reordering + drops (induces log
        // divergence and higher-term re-elections, exercising the restriction).
        chaos in 0u8..3,
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let mut sim = SimCluster::new(node_count, seed);

        match chaos {
            1 => sim.set_reorder(true, std::time::Duration::from_millis(8)),
            2 => {
                sim.set_reorder(true, std::time::Duration::from_millis(8));
                sim.set_drop_probability(0.1);
            }
            _ => {}
        }

        // Arm at least one voter so an election actually starts.
        let mut armed: Vec<NodeId> = (0..node_count)
            .filter(|&i| (arm_mask >> i) & 1 == 1)
            .map(NodeId)
            .collect();
        if armed.is_empty() {
            armed.push(NodeId(0));
        }
        // A clean, single-candidate election must elect exactly one leader.
        let expect_clean_win = chaos == 0 && armed.len() == 1;

        for &node in &armed {
            sim.arm(node, TimerKind::Election, ELECTION_TIMEOUT_BASE);
        }

        let mut seen: HashMap<u64, NodeId> = HashMap::new();
        let mut claims: ClaimMap = HashMap::new();
        let mut ever_leader = false;

        // Heartbeats keep a healthy group perpetually busy, so a step budget is
        // required; contended elections may need several higher-term rounds.
        let budget = 3000;
        for i in 0..budget {
            // Safety invariant 1: at most one leader per term (checked before
            // and, via the loop, after every delivered event).
            if let Err(msg) = observe_leaders(&sim, node_count, &mut seen) {
                prop_assert!(false, "{}", msg);
            }
            if sim.leader().is_some() {
                ever_leader = true;
            }

            // Periodically replicate a real metadata command through the group's
            // current leader, so logs grow and (under faults) diverge.
            if i % 40 == 0 {
                if let Some(leader) = sim.leader() {
                    let payload =
                        EntryPayload::new(PayloadKind::Cluster, vec![(i & 0xff) as u8]);
                    let _ = sim.propose(leader, payload);
                }
            }

            let outcome = sim.step();
            // Safety invariant 2: votes granted only to up-to-date candidates.
            if let Err(msg) = check_vote_grants(&sim, &outcome, &mut claims) {
                prop_assert!(false, "{}", msg);
            }
            if matches!(outcome, StepOutcome::Idle) {
                break;
            }
        }

        // Final observation after the loop's last delivered event.
        if let Err(msg) = observe_leaders(&sim, node_count, &mut seen) {
            prop_assert!(false, "{}", msg);
        }
        if sim.leader().is_some() {
            ever_leader = true;
        }

        // Liveness sanity for the clean uncontested case: a lone candidate with a
        // reachable majority of voters must win its election outright.
        if expect_clean_win {
            prop_assert!(
                ever_leader,
                "a single-candidate metadata election should elect exactly one leader \
                 (node_count={node_count}, seed={seed})"
            );
        }
    }
}
