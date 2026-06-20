// Feature: vela-streaming-platform, Property 24
//
//! Property 24: A higher term always forces step-down.
//!
//! **Validates: Requirements 7.9** — "IF a Raft_Node receives any RPC carrying a
//! Term greater than its current Term, THEN THE Raft_Node SHALL set its current
//! Term to the greater Term and transition to Follower."
//!
//! The property drives a [`RaftNode`] into a varied starting role (Follower,
//! Candidate, or Leader) at a varied starting term, then delivers a single
//! message — one of all four [`RaftMessage`] variants — carrying a strictly
//! greater term. After that one [`RaftNode::step`], the node must be a Follower
//! whose current term equals the message's term, regardless of the role it held
//! or the message variant that carried the higher term.

use std::time::{Duration, Instant};

use proptest::prelude::*;

use vela_log::InMemoryLog;
use vela_raft::{
    AppendEntries, AppendEntriesReply, Clock, NodeId, RaftInput, RaftMessage, RaftNode,
    RequestVote, RequestVoteReply, Role, TimerKind,
};

/// A trivial [`Clock`] for direct, single-node `step` tests: logical time never
/// advances on its own and every armed timer is simply recorded. The election
/// and step-down logic under test never reads the clock's instant, so a fixed
/// reference time is sufficient and keeps the test fully deterministic.
#[derive(Default)]
struct TestClock {
    armed: Vec<TimerKind>,
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn arm(&mut self, kind: TimerKind, _dur: Duration) {
        self.armed.push(kind);
    }
}

/// The role a freshly built node should be driven into before the higher-term
/// message is delivered.
#[derive(Debug, Clone, Copy)]
enum StartRole {
    Follower,
    Candidate,
    Leader,
}

/// Build a node (id `NodeId(0)` in a three-node group) sitting in `role` at
/// `term`, using only the public `step` API to reach that state.
///
/// - **Follower** at term `t`: a fresh node is already a term-0 follower; for
///   `t > 0` a same-/higher-term heartbeat raises it to term `t` while leaving
///   it a follower.
/// - **Candidate** at term `t` (`t >= 1`): each election tick starts (or
///   restarts) an election, bumping the term by one; with two peers the lone
///   self-vote never reaches the majority of two, so the node stays a candidate.
/// - **Leader** at term `t` (`t >= 1`): drive to candidate at term `t`, then
///   feed one granted vote reply — that second vote is a majority of three, so
///   the candidate is promoted to leader without changing the term.
fn build_node(role: StartRole, term: u64) -> RaftNode<InMemoryLog> {
    let mut node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], InMemoryLog::new());
    let mut clock = TestClock::default();

    match role {
        StartRole::Follower => {
            if term > 0 {
                node.step(
                    RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                        term,
                        leader_id: NodeId(1),
                        prev_log_index: None,
                        prev_log_term: None,
                        entries: Vec::new(),
                        leader_commit: None,
                    })),
                    &mut clock,
                );
            }
        }
        StartRole::Candidate => {
            // `term` election ticks land the node at exactly `current_term == term`.
            for _ in 0..term {
                node.step(RaftInput::Tick(TimerKind::Election), &mut clock);
            }
        }
        StartRole::Leader => {
            for _ in 0..term {
                node.step(RaftInput::Tick(TimerKind::Election), &mut clock);
            }
            // One peer's granted vote completes a majority of three → leader.
            node.step(
                RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                    term,
                    vote_granted: true,
                    voter: NodeId(1),
                })),
                &mut clock,
            );
        }
    }

    node
}

/// Construct one of the four message variants, all carrying `term`. `flag`
/// supplies the boolean payload for the reply variants (`vote_granted` /
/// `success`); the variant index selects which message is built.
fn message_with_term(variant: u8, term: u64, flag: bool) -> RaftMessage {
    match variant {
        0 => RaftMessage::RequestVote(RequestVote {
            term,
            candidate_id: NodeId(1),
            last_log_index: None,
            last_log_term: None,
        }),
        1 => RaftMessage::RequestVoteReply(RequestVoteReply {
            term,
            vote_granted: flag,
            voter: NodeId(1),
        }),
        2 => RaftMessage::AppendEntries(AppendEntries {
            term,
            leader_id: NodeId(1),
            prev_log_index: None,
            prev_log_term: None,
            entries: Vec::new(),
            leader_commit: None,
        }),
        _ => RaftMessage::AppendEntriesReply(AppendEntriesReply {
            from: NodeId(1),
            term,
            success: flag,
            conflict_index: None,
            match_index: None,
        }),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property 24: any RPC or reply whose term exceeds the node's current term
    /// drives it to Follower at that term, from any starting role and for every
    /// message variant.
    #[test]
    fn higher_term_always_forces_step_down(
        role_sel in 0u8..3,
        start_term in 0u64..50,
        delta in 1u64..100,
        msg_variant in 0u8..4,
        flag in any::<bool>(),
    ) {
        let role = match role_sel {
            0 => StartRole::Follower,
            1 => StartRole::Candidate,
            _ => StartRole::Leader,
        };

        // Candidate/Leader are only reachable at term >= 1 via the step API.
        let start_term = match role {
            StartRole::Follower => start_term,
            StartRole::Candidate | StartRole::Leader => start_term.max(1),
        };

        let mut node = build_node(role, start_term);

        // Sanity-check the constructed starting state matches the generator.
        let expected_role = match role {
            StartRole::Follower => Role::Follower,
            StartRole::Candidate => Role::Candidate,
            StartRole::Leader => Role::Leader,
        };
        prop_assert_eq!(node.role(), expected_role);
        prop_assert_eq!(node.current_term(), start_term);

        // Deliver a single message carrying a strictly greater term.
        let higher_term = start_term + delta;
        let mut clock = TestClock::default();
        node.step(
            RaftInput::Message(message_with_term(msg_variant, higher_term, flag)),
            &mut clock,
        );

        // The node must have adopted the greater term and reverted to Follower.
        prop_assert_eq!(node.role(), Role::Follower);
        prop_assert_eq!(node.current_term(), higher_term);
    }
}
