//! Property test for follower `AppendEntries` accept/reject in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 29
//!
//! Property 29: Followers accept matching and reject conflicting AppendEntries.
//! A follower appends the conveyed entries and acknowledges (`success = true`)
//! when the RPC's preceding entry matches its own log at the same index and
//! term (or when the batch begins at the head of the log, so there is no
//! preceding entry); otherwise it rejects the RPC (`success = false`), appends
//! nothing, and returns a conflict hint so the leader can retry with earlier
//! entries.
//!
//! Validates: Requirements 8.2, 8.3

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogEntry, LogStorage, PayloadKind};
use vela_raft::{
    AppendEntries, AppendEntriesReply, Clock, NodeId, RaftInput, RaftMessage, RaftNode, RaftOutput,
    TimerKind,
};

/// A trivial [`Clock`] for direct `step` tests: time never advances on its own
/// and armed timers are discarded. Follower accept/reject depends only on the
/// log-matching check, not on wall-clock time.
#[derive(Default)]
struct TestClock;

impl Clock for TestClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// Pull the single [`AppendEntriesReply`] a follower sends back to the leader.
fn expect_append_reply(out: &RaftOutput) -> AppendEntriesReply {
    out.sends
        .iter()
        .find_map(|(_, m)| match m {
            RaftMessage::AppendEntriesReply(r) => Some(r.clone()),
            _ => None,
        })
        .expect("an AppendEntries must yield an AppendEntriesReply")
}

/// Dump the follower's stored `(index, term)` pairs so a reject can be checked
/// to leave the log entirely unchanged.
fn dump(node: &RaftNode<InMemoryLog>) -> Vec<(u64, u64)> {
    match node.log().last_index() {
        None => Vec::new(),
        Some(last) => node
            .log()
            .read(0, last)
            .into_iter()
            .map(|e| (e.index, e.term))
            .collect(),
    }
}

/// Seed a follower whose log carries one entry per element of `terms`, indexed
/// 0..terms.len(). The node stays a term-0 follower in a 3-node group.
fn follower_with_log(terms: &[u64]) -> RaftNode<InMemoryLog> {
    let mut log = InMemoryLog::new();
    for &term in terms {
        log.append(EntryPayload::new(PayloadKind::Record, Vec::new()), term)
            .expect("append to in-memory log");
    }
    RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], log)
}

/// One generated scenario.
#[derive(Debug, Clone)]
struct Scenario {
    /// Terms of the follower's seeded log entries (length = log length).
    log_terms: Vec<u64>,
    /// The RPC's `prev_log_index`: `None` (batch at head) or `Some(index)`.
    prev_index: Option<u64>,
    /// Bias toward generating a matching preceding entry.
    want_match: bool,
    /// A candidate term used to force a mismatch (or stand in when the chosen
    /// preceding index is out of range).
    other_term: u64,
    /// Number of entries the RPC conveys (0 = heartbeat).
    num_entries: usize,
    /// Extra term headroom so the leader's term is plausibly current.
    ae_extra: u64,
}

fn scenario_strategy() -> impl Strategy<Value = Scenario> {
    (
        prop::collection::vec(1u64..4, 0..6), // log_terms
        prop::option::of(0u64..7),            // prev_index
        any::<bool>(),                        // want_match
        1u64..6,                              // other_term
        0usize..4,                            // num_entries
        0u64..3,                              // ae_extra
    )
        .prop_map(
            |(log_terms, prev_index, want_match, other_term, num_entries, ae_extra)| Scenario {
                log_terms,
                prev_index,
                want_match,
                other_term,
                num_entries,
                ae_extra,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 29
    #[test]
    fn followers_accept_matching_and_reject_conflicting_append_entries(sc in scenario_strategy()) {
        let mut node = follower_with_log(&sc.log_terms);
        let mut clock = TestClock;

        // A plausible current leader term: at least 1 and at least the highest
        // term in the follower's log, so the RPC is never stale (the follower
        // starts at term 0). This isolates the log-matching check.
        let max_log_term = sc.log_terms.iter().copied().max().unwrap_or(0);
        let ae_term = max_log_term.max(1) + sc.ae_extra;

        // Resolve the preceding-entry coordinates, preserving the leader's
        // invariant that `prev_log_term` is `Some` exactly when
        // `prev_log_index` is `Some`.
        let (prev_log_index, prev_log_term) = match sc.prev_index {
            None => (None, None),
            Some(p) => {
                let actual = node.log().term_at(p);
                let term = if sc.want_match {
                    // Match when the index is in range; otherwise any Some(..)
                    // is necessarily a mismatch against an absent entry.
                    actual.unwrap_or(sc.other_term)
                } else {
                    // Force a term different from whatever is stored (or absent).
                    let mut t = sc.other_term;
                    if Some(t) == actual {
                        t += 1;
                    }
                    t
                };
                (Some(p), Some(term))
            }
        };

        // Build a contiguous batch beginning right after the preceding entry,
        // all stamped with the leader's term (as a real leader would).
        let prev_base = prev_log_index.map_or(0, |p| p + 1);
        let entries: Vec<LogEntry> = (0..sc.num_entries as u64)
            .map(|i| LogEntry {
                index: prev_base + i,
                term: ae_term,
                payload: EntryPayload::new(PayloadKind::Record, vec![(prev_base + i) as u8]),
            })
            .collect();

        // Expected decision per Requirements 8.2/8.3: a head-of-log batch is
        // always consistent; otherwise the entry at `prev_log_index` must match
        // in term.
        let expected_match = match prev_log_index {
            None => true,
            Some(p) => node.log().term_at(p) == prev_log_term,
        };

        let before = dump(&node);

        let out = node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: ae_term,
                leader_id: NodeId(1),
                prev_log_index,
                prev_log_term,
                entries: entries.clone(),
                leader_commit: None,
            })),
            &mut clock,
        );

        let reply = expect_append_reply(&out);

        // The success flag follows the log-matching decision exactly.
        prop_assert_eq!(
            reply.success,
            expected_match,
            "success must follow the preceding-entry match: prev={:?}/{:?}",
            prev_log_index,
            prev_log_term
        );
        // The reply is addressed back to the leader and identifies this follower.
        prop_assert_eq!(reply.from, NodeId(0), "reply must identify the follower");
        prop_assert!(
            out.sends.iter().any(|(to, m)| *to == NodeId(1)
                && matches!(m, RaftMessage::AppendEntriesReply(_))),
            "reply must be sent to the leader"
        );

        if expected_match {
            // Accept: every conveyed entry is now present in the follower's log
            // at its index and term, and the ack reports the matched index.
            for e in &entries {
                prop_assert_eq!(
                    node.log().term_at(e.index),
                    Some(e.term),
                    "accepted entry {} must be present with its term",
                    e.index
                );
            }
            let expected_match_index = match entries.last() {
                Some(last) => Some(last.index),
                None => prev_log_index,
            };
            prop_assert_eq!(
                reply.match_index,
                expected_match_index,
                "ack must report the highest agreeing index"
            );
            prop_assert_eq!(reply.conflict_index, None, "an accept carries no conflict hint");
        } else {
            // Reject: nothing is appended (the log is byte-for-byte unchanged)
            // and a conflict hint is returned so the leader can back up.
            prop_assert_eq!(dump(&node), before, "a rejected RPC must not change the log");
            prop_assert_eq!(reply.match_index, None, "a reject reports no match index");
            prop_assert!(
                reply.conflict_index.is_some(),
                "a reject must return a conflict hint for the leader to retry"
            );
        }
    }
}
