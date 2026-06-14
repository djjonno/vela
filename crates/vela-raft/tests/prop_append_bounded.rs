//! Property test for bounded `AppendEntries` carrying the correct preceding
//! entry in `vela-raft`.
//!
//! Feature: vela-streaming-platform, Property 28
//!
//! Property 28: AppendEntries is bounded and carries the correct preceding
//! entry. Every `AppendEntries` a leader emits carries at most
//! [`MAX_ENTRIES_PER_APPEND`] (256) entries, and the `(prev_log_index,
//! prev_log_term)` it carries correctly identify the entry immediately
//! preceding the conveyed entries in the leader's log — that is, `prev_log_index`
//! is one less than the index of the first conveyed entry (or `None` when that
//! entry sits at index 0), and `prev_log_term` is the term the leader's log
//! holds at that preceding index.
//!
//! The test drives a real leader over the deterministic [`SimCluster`] harness.
//! It elects a single, uncontested leader (one armed node, no network faults, so
//! leadership is stable on `NodeId(0)`), then feeds it a varied backlog of
//! proposals — including a burst larger than the 256-entry cap before any
//! acknowledgments are delivered, which forces maximally sized batches, followed
//! by acknowledgment delivery (advancing `next_index`) and a second burst, which
//! forces batches that begin partway through the log and therefore carry a real
//! preceding entry. Every `AppendEntries` emitted across all of this — from
//! `propose` effects and from stepped heartbeats/retries — is captured and
//! checked against the invariant. The leader's log is append-only and never
//! reverts, so the term it holds at any preceding index is stable between
//! emission and the final check.
//!
//! Validates: Requirements 8.1

use proptest::prelude::*;
use vela_raft::sim::{SimCluster, StepOutcome};
use vela_raft::{
    AppendEntries, EntryPayload, LogStorage, NodeId, PayloadKind, RaftMessage, RaftOutput,
    TimerKind, ELECTION_TIMEOUT_BASE, MAX_ENTRIES_PER_APPEND,
};

/// Step the cluster until some replica believes itself leader, or the step
/// budget is exhausted. Returns the leader's id if one emerged.
fn elect_leader(sim: &mut SimCluster, budget: usize) -> Option<NodeId> {
    for _ in 0..budget {
        if let Some(leader) = sim.leader() {
            return Some(leader);
        }
        if matches!(sim.step(), StepOutcome::Idle) {
            break;
        }
    }
    sim.leader()
}

/// Extract the [`RaftOutput`] a single step produced, if it delivered an event.
fn step_output(outcome: StepOutcome) -> Option<RaftOutput> {
    match outcome {
        StepOutcome::Timer { output, .. } | StepOutcome::Message { output, .. } => Some(output),
        StepOutcome::Idle => None,
    }
}

/// Pull every `AppendEntries` RPC out of a step/propose effect into `sink`.
fn collect_append_entries(out: &RaftOutput, sink: &mut Vec<AppendEntries>) {
    for (_to, msg) in &out.sends {
        if let RaftMessage::AppendEntries(ae) = msg {
            sink.push(ae.clone());
        }
    }
}

/// A small, uniquely tagged record payload for the `seq`-th proposal.
fn record_payload(seq: u64) -> EntryPayload {
    EntryPayload::new(PayloadKind::Record, seq.to_le_bytes().to_vec())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: vela-streaming-platform, Property 28
    #[test]
    fn append_entries_is_bounded_and_carries_correct_preceding_entry(
        seed in any::<u64>(),
        // Exercise both a small-odd and a larger cluster.
        five_nodes in any::<bool>(),
        // First burst is always larger than the 256 cap so a maximally sized
        // batch is guaranteed to be emitted while next_index is still at 0.
        burst1 in 260u64..=420,
        // Second burst (after acks advance next_index) produces batches that
        // begin partway through the log, carrying a real preceding entry.
        burst2 in 1u64..=300,
        // How aggressively to drain the network between/after the bursts.
        step_chunk in 5usize..=40,
    ) {
        let node_count: u64 = if five_nodes { 5 } else { 3 };
        let mut sim = SimCluster::new(node_count, seed);

        // One armed node, no faults: a clean, uncontested election that leaves
        // NodeId(0) the stable leader for the whole run.
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = elect_leader(&mut sim, 2000);
        prop_assert!(
            leader.is_some(),
            "a single-candidate election should elect a leader (node_count={node_count}, seed={seed})"
        );
        let leader = leader.expect("leader elected above");
        prop_assert_eq!(leader, NodeId(0), "the only armed node should win the election");

        let mut seq: u64 = 0;
        let mut emitted: Vec<AppendEntries> = Vec::new();

        // Phase A — burst past the cap WITHOUT delivering acks. next_index stays
        // at 0, so each send reads from the head of the log and the batch grows
        // until it saturates at exactly the 256-entry cap.
        for _ in 0..burst1 {
            if let Some(out) = sim.propose(leader, record_payload(seq)) {
                collect_append_entries(&out, &mut emitted);
            }
            seq += 1;
        }

        // Phase B — drain the network so followers ack and the leader advances
        // next_index past the replicated prefix.
        for _ in 0..(step_chunk * 4) {
            match step_output(sim.step()) {
                Some(out) => collect_append_entries(&out, &mut emitted),
                None => break,
            }
        }

        // Phase C — a second burst now begins partway through the log, so the
        // emitted batches carry a non-None preceding entry.
        let leader = sim.leader().unwrap_or(leader);
        for _ in 0..burst2 {
            if let Some(out) = sim.propose(leader, record_payload(seq)) {
                collect_append_entries(&out, &mut emitted);
            }
            seq += 1;
        }

        // Phase D — flush remaining traffic, capturing heartbeats and retries.
        for _ in 0..(step_chunk * 8) {
            match step_output(sim.step()) {
                Some(out) => collect_append_entries(&out, &mut emitted),
                None => break,
            }
        }

        // The leader's log is append-only and never reverts, so the term it
        // holds at any preceding index is stable between emission and now.
        let leader_node = sim.node(NodeId(0)).expect("leader node exists");
        let log = leader_node.log();

        let mut max_batch: u64 = 0;
        for ae in &emitted {
            // All AppendEntries are emitted by the (stable) leader.
            prop_assert_eq!(
                ae.leader_id,
                NodeId(0),
                "AppendEntries must be emitted by the leader"
            );

            let len = ae.entries.len() as u64;
            max_batch = max_batch.max(len);

            // Bounded: at most 256 entries per RPC (Requirement 8.1).
            prop_assert!(
                len <= MAX_ENTRIES_PER_APPEND,
                "AppendEntries carried {} entries, exceeding the {} cap",
                len,
                MAX_ENTRIES_PER_APPEND
            );

            match ae.entries.first() {
                Some(first) => {
                    // The conveyed entries are contiguous and ascending from the
                    // first entry's index.
                    for (offset, entry) in ae.entries.iter().enumerate() {
                        prop_assert_eq!(
                            entry.index,
                            first.index + offset as u64,
                            "conveyed entries must be contiguous and ascending"
                        );
                    }

                    // prev_log_index is one before the first conveyed entry, or
                    // None when that entry sits at index 0.
                    let expected_prev_index =
                        if first.index == 0 { None } else { Some(first.index - 1) };
                    prop_assert_eq!(
                        ae.prev_log_index,
                        expected_prev_index,
                        "prev_log_index must immediately precede the first conveyed entry"
                    );

                    // prev_log_term is exactly the term the leader's log holds at
                    // the preceding index (None when there is no preceding entry).
                    let expected_prev_term =
                        expected_prev_index.and_then(|p| log.term_at(p));
                    prop_assert_eq!(
                        ae.prev_log_term,
                        expected_prev_term,
                        "prev_log_term must match the leader's term at prev_log_index"
                    );
                }
                None => {
                    // A heartbeat (empty batch) is trivially bounded; its
                    // preceding-entry coordinates must still be self-consistent
                    // with the leader's log.
                    prop_assert_eq!(
                        ae.prev_log_term,
                        ae.prev_log_index.and_then(|p| log.term_at(p)),
                        "heartbeat prev_log_term must match the leader's term at prev_log_index"
                    );
                }
            }
        }

        // The run must actually exercise replication, and the 256-entry cap must
        // bind: the over-cap first burst guarantees a maximally sized batch.
        prop_assert!(!emitted.is_empty(), "no AppendEntries were captured");
        prop_assert_eq!(
            max_batch,
            MAX_ENTRIES_PER_APPEND,
            "the over-cap burst should have produced a maximally sized batch"
        );
    }
}
