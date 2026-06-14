//! Property test for Raft vote decisions in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 23
//!
//! Property 23: Vote decision follows term, log-currency, and single-vote
//! rules. For any node state and RequestVote RPC, the node grants its vote —
//! and adopts the RPC's term — if and only if the RPC term is greater than or
//! equal to its current term, the candidate's log is at least as up to date as
//! its own, and it has not already voted for another candidate in that term;
//! otherwise it denies the vote and responds with its current term.
//!
//! Validates: Requirements 7.7, 7.8

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogStorage, PayloadKind};
use vela_raft::{
    AppendEntries, Clock, NodeId, RaftInput, RaftMessage, RaftNode, RaftOutput, RequestVote,
    RequestVoteReply, TimerKind,
};

/// A trivial [`Clock`] for direct `step` tests: time never advances on its own
/// and armed timers are simply discarded. Vote decisions depend only on term,
/// log currency, and prior-vote state, not on wall-clock time.
#[derive(Default)]
struct TestClock;

impl Clock for TestClock {
    fn now(&self) -> Instant {
        // A fixed reference instant is sufficient; this test never reads it.
        Instant::now()
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// Pull the single [`RequestVoteReply`] out of a step's outbound messages.
/// Every `RequestVote` must produce exactly one reply (granted or denied).
fn expect_vote_reply(out: &RaftOutput) -> RequestVoteReply {
    out.sends
        .iter()
        .find_map(|(_, m)| match m {
            RaftMessage::RequestVoteReply(r) => Some(r.clone()),
            _ => None,
        })
        .expect("a RequestVote must yield a RequestVoteReply")
}

/// Lexicographic log-currency key matching the implementation: compare by
/// `(last_term, last_index)` with an empty log as the lowest possible value
/// (`term` defaults to 0, a missing index sorts below index 0).
fn log_key(term: Option<u64>, index: Option<u64>) -> (u64, i128) {
    (term.unwrap_or(0), index.map_or(-1i128, |i| i as i128))
}

/// One generated scenario: how to set up the node's prior state and the
/// RequestVote RPC to feed it.
#[derive(Debug, Clone)]
struct Scenario {
    /// Number of entries in the node's own log (last index = len - 1).
    node_log_len: u64,
    /// Term stamped on the node's last log entry (only relevant when non-empty).
    node_last_term: u64,
    /// Term the node is raised to before the RequestVote under test.
    base_term: u64,
    /// Prior-vote state in `base_term`: 0 = none, 1 = voted for a *different*
    /// candidate, 2 = already voted for *this* candidate.
    prior_vote: u8,
    /// The candidate id on the RequestVote under test.
    candidate: u64,
    /// Length of the candidate's claimed log (0 = empty → None/None).
    cand_log_len: u64,
    /// Term of the candidate's claimed last entry (only when non-empty).
    cand_last_term: u64,
}

fn scenario_strategy() -> impl Strategy<Value = Scenario> {
    (
        0u64..6, // node_log_len
        0u64..6, // node_last_term
        1u64..8, // base_term (>=1 so the raise is unambiguous)
        0u8..3,  // prior_vote selector
        1u64..4, // candidate id in {1,2,3}
        0u64..7, // cand_log_len
        0u64..8, // cand_last_term
    )
        .prop_map(
            |(
                node_log_len,
                node_last_term,
                base_term,
                prior_vote,
                candidate,
                cand_log_len,
                cand_last_term,
            )| Scenario {
                node_log_len,
                node_last_term,
                base_term,
                prior_vote,
                candidate,
                cand_log_len,
                cand_last_term,
            },
        )
}

/// Build a node whose own log has `len` entries, the last carrying `last_term`.
/// Only the final entry's term/index participates in the log-currency check,
/// so stamping every entry with `last_term` is sufficient and faithful.
fn node_with_log(len: u64, last_term: u64) -> RaftNode<InMemoryLog> {
    let mut log = InMemoryLog::new();
    for _ in 0..len {
        log.append(
            EntryPayload::new(PayloadKind::Record, Vec::new()),
            last_term,
        )
        .expect("append to in-memory log");
    }
    RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], log)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 23
    #[test]
    fn vote_decision_follows_term_log_currency_and_single_vote_rules(sc in scenario_strategy()) {
        let mut node = node_with_log(sc.node_log_len, sc.node_last_term);
        let mut clock = TestClock;

        // The candidate id under test, and a distinct id used to seed a
        // "voted for someone else" prior state.
        let candidate = NodeId(sc.candidate);
        let other_candidate = NodeId(sc.candidate + 100);

        // --- Establish the node's prior term and prior-vote state ---------
        match sc.prior_vote {
            // No prior vote this term: raise the term with a heartbeat-style
            // AppendEntries, which steps the node up to `base_term` and leaves
            // `voted_for` clear.
            0 => {
                node.step(
                    RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                        term: sc.base_term,
                        leader_id: NodeId(1),
                        prev_log_index: None,
                        prev_log_term: None,
                        entries: Vec::new(),
                        leader_commit: None,
                    })),
                    &mut clock,
                );
            }
            // A prior vote in `base_term`. Feed a guaranteed-grantable
            // RequestVote (maximal log) from the chosen prior candidate so the
            // node records `voted_for` for that node in `base_term`.
            _ => {
                let prior_candidate = if sc.prior_vote == 2 { candidate } else { other_candidate };
                node.step(
                    RaftInput::Message(RaftMessage::RequestVote(RequestVote {
                        term: sc.base_term,
                        candidate_id: prior_candidate,
                        last_log_index: Some(u64::MAX),
                        last_log_term: Some(u64::MAX),
                    })),
                    &mut clock,
                );
                prop_assert_eq!(node.voted_for(), Some(prior_candidate));
            }
        }

        // Snapshot the node's *actual* pre-RPC state, so the expectation is
        // computed against ground truth rather than setup assumptions.
        let pre_term = node.current_term();
        let pre_voted_for = node.voted_for();
        let node_last_index = node.log().last_index();
        let node_last_term = node_last_index.and_then(|i| node.log().term_at(i));

        // --- The RequestVote RPC under test -------------------------------
        let (cand_last_index, cand_last_term) = if sc.cand_log_len == 0 {
            (None, None)
        } else {
            (Some(sc.cand_log_len - 1), Some(sc.cand_last_term))
        };
        let rv = RequestVote {
            term: sc.cand_last_term, // reuse the generated value as the RPC term
            candidate_id: candidate,
            last_log_index: cand_last_index,
            last_log_term: cand_last_term,
        };
        let rv_term = rv.term;

        // --- Compute the expected decision, mirroring the rules -----------
        // A strictly higher RPC term forces step-down *before* the decision:
        // the node adopts the term and clears any prior vote (R7.9 interplay).
        let (eff_term, eff_voted_for) = if rv_term > pre_term {
            (rv_term, None)
        } else {
            (pre_term, pre_voted_for)
        };

        let term_valid = rv_term >= eff_term;
        let up_to_date =
            log_key(cand_last_term, cand_last_index) >= log_key(node_last_term, node_last_index);
        let not_yet_voted = eff_voted_for.is_none() || eff_voted_for == Some(candidate);
        let expect_grant = term_valid && up_to_date && not_yet_voted;

        // The reply always carries the node's (possibly just-adopted) term.
        let expect_reply_term = eff_term;
        // Post-decision vote: the granted candidate, else whatever stood after
        // any step-down (None if the term was raised, else the prior vote).
        let expect_post_voted_for = if expect_grant { Some(candidate) } else { eff_voted_for };

        // --- Drive the node and assert ------------------------------------
        let out = node.step(RaftInput::Message(RaftMessage::RequestVote(rv)), &mut clock);
        let reply = expect_vote_reply(&out);

        prop_assert_eq!(
            reply.vote_granted,
            expect_grant,
            "grant decision mismatch: term_valid={}, up_to_date={}, not_yet_voted={}",
            term_valid,
            up_to_date,
            not_yet_voted
        );
        prop_assert_eq!(reply.term, expect_reply_term, "reply must carry current term");
        prop_assert_eq!(node.current_term(), eff_term, "node term after decision");
        prop_assert_eq!(node.voted_for(), expect_post_voted_for, "node vote after decision");
        // The reply is addressed back to the soliciting candidate.
        prop_assert!(
            out.sends.iter().any(|(to, m)| *to == candidate
                && matches!(m, RaftMessage::RequestVoteReply(_))),
            "reply must be sent to the candidate"
        );
    }
}
