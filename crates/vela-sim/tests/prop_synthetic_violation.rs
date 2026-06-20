#![cfg(feature = "sim")]
// Feature: deterministic-simulation-testing, Property: synthetic safety-violation meta-test (Requirement 10.6)
//! Synthetic safety-violation meta-test for `vela-sim` (Requirement 10.6).
//!
//! This is a **positive control** for the [`RaftSafetyChecker`]: it deliberately
//! drives the simulated cluster into a state that breaches a Raft safety
//! property and asserts the checker *detects* it — ending the observation with a
//! failing [`Violation`] that names the right [`PropertyId`] and the exact
//! detection [`VirtualInstant`] supplied to the check (Requirement 10.6, 2.3).
//!
//! Without this control the sibling property tests
//! (`prop_election_safety`, `prop_state_machine_safety`, …) would be *vacuous*:
//! a checker that never returns `Err` would pass every one of them. Here we
//! manufacture a known breach and require the checker to flag it, so those tests
//! are proven to have teeth.
//!
//! ## The injected fault: a same-term double leader (Election Safety, Raft §5.2)
//!
//! Election Safety is the cleanest breach to force deterministically through the
//! **public** integration API ([`SimulatedCluster::step_replica`]) without
//! relying on a real consensus bug (which correct code will never produce):
//!
//! 1. Drive `node-0`'s `__meta/0` replica through an election: a
//!    `Tick(Election)` makes it a candidate in term 1 (self-vote), then a single
//!    crafted granted [`RequestVoteReply`] reaches the 2-of-3 majority and
//!    promotes it to leader of term 1.
//! 2. Drive `node-1` the *same* way. Because the harness delivers messages only
//!    where we tell it to, `node-1` never saw `node-0`'s vote request, so it too
//!    times out into term 1, self-votes, and is promoted by a crafted granted
//!    reply.
//!
//! Both replicas now believe they are leader of `__meta/0` **in the same term 1**
//! — exactly the condition Election Safety forbids. `observe` accumulates the
//! per-`(group, term)` leader and flags the second, distinct leader as a
//! [`PropertyId::ElectionSafety`] [`Violation`] stamped with the instant passed
//! to it.
//!
//! The whole construction is a pure function of the (default) configuration and
//! the fixed inputs we feed — no seed-derived randomness affects the outcome —
//! so the meta-test is fully deterministic and never flakes.
//!
//! Validates: Requirements 10.6

use vela_core::metadata_group_key;
use vela_raft::{NodeId as RaftNodeId, RaftInput, RaftMessage, RequestVoteReply, Role, TimerKind};
use vela_sim::checker::{PropertyId, RaftSafetyChecker};
use vela_sim::cluster::SimulatedCluster;
use vela_sim::scenario::RunConfig;
use vela_sim::scheduler::VirtualInstant;

/// The term every manufactured leader claims. A fresh replica is at term 0, so a
/// single election `Tick` advances it to term 1; driving two replicas the same
/// way lands both in this one term — the same-term clash Election Safety forbids.
const FORCED_TERM: u64 = 1;

/// Drive the `__meta/0` replica on the node at `index` to *believe* it is leader
/// of [`FORCED_TERM`], using only the public [`SimulatedCluster::step_replica`]
/// entry point.
///
/// Steps the replica twice at `now`: a `Tick(Election)` (fresh term-0 replica →
/// term-1 candidate with its own self-vote) followed by one granted
/// [`RequestVoteReply`] for term 1 — enough to reach the 2-of-3 majority of the
/// cluster-wide metadata group and be promoted to leader. Asserts the promotion
/// actually happened, so a change in the election rules can never let this
/// helper silently fail to manufacture the breach.
fn force_meta_leader(cluster: &mut SimulatedCluster, index: usize, now: VirtualInstant) {
    let meta = metadata_group_key();

    // 1. Time out into an election: term 0 -> candidate in term 1 (self-vote).
    cluster.step_replica(index, &meta, now, RaftInput::Tick(TimerKind::Election));

    // 2. A single granted vote tips the candidate over the 2-of-3 majority.
    cluster.step_replica(
        index,
        &meta,
        now,
        RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
            term: FORCED_TERM,
            vote_granted: true,
            // Any peer distinct from the candidate (`RaftNodeId(index)`); the
            // sim maps node index `i` to `RaftNodeId(i)`.
            voter: RaftNodeId(if index == 0 { 1 } else { 0 }),
        })),
    );

    let node = cluster.node(index).expect("node index within range");
    let controller = node.controller().expect("a running node has a controller");
    assert_eq!(
        controller.role(),
        Some(Role::Leader),
        "node {index} must be leader of __meta/0 after a granted majority vote",
    );
    assert_eq!(
        controller
            .meta_replica()
            .expect("the metadata replica exists")
            .raft()
            .current_term(),
        FORCED_TERM,
        "the manufactured leader must hold the forced term",
    );
}

/// Forcing two distinct replicas to lead the metadata group in the same term is
/// a known Election Safety breach (Raft §5.2); the checker MUST detect it and
/// end the observation with a failing `Violation` that names
/// `PropertyId::ElectionSafety` and the exact detection instant
/// (Requirement 10.6).
#[test]
fn synthetic_double_leader_is_detected_as_an_election_safety_violation() {
    // Default config: a 3-node cluster with the metadata group replicated on
    // every node, so a 2-vote majority elects a metadata leader.
    let mut cluster =
        SimulatedCluster::new(RunConfig::default()).expect("default config builds a cluster");
    let mut checker = RaftSafetyChecker::new();

    let setup = VirtualInstant::ORIGIN;
    // The instant we will hand to the *failing* observation; the violation must
    // be stamped with exactly this instant, distinct from the setup instant so
    // the assertion genuinely pins down `violation.at`.
    let detect = VirtualInstant::from_nanos(123_456);

    // A freshly assembled cluster has no leaders: the checker must be clean,
    // proving the breach below is what trips it (not a perpetually-failing
    // checker).
    checker
        .observe(&cluster, setup)
        .expect("a fresh cluster has no leaders and must observe clean");

    // Manufacture the first metadata leader of term 1 and confirm the checker is
    // still clean — one leader per term is legal.
    force_meta_leader(&mut cluster, 0, setup);
    checker
        .observe(&cluster, setup)
        .expect("a single metadata leader in term 1 is legal and must observe clean");

    // Manufacture a *second*, distinct leader of the same group in the same
    // term: the Election Safety breach.
    force_meta_leader(&mut cluster, 1, setup);

    // (a) The observation ends failing.
    let violation = checker
        .observe(&cluster, detect)
        .expect_err("two leaders of __meta/0 in term 1 must fail Election Safety");

    // (b) It names the right property and the detection instant we supplied.
    assert_eq!(
        violation.property,
        PropertyId::ElectionSafety,
        "the synthetic breach must be attributed to Election Safety: {}",
        violation.detail,
    );
    assert_eq!(
        violation.at, detect,
        "the violation must be stamped with the instant passed to the detecting observation",
    );
}
