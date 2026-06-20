#![cfg(feature = "sim")]
//! Property test for the [`SimNetwork`] fault model.
//!
//! Feature: deterministic-simulation-testing, Property 4: Network faults behave
//! exactly as configured
//!
//! Property 4: every fault the [`Sim_Network`](vela_sim::network) is configured
//! with has exactly the effect the requirements prescribe, and no other. A
//! single seed-driven test exercises each configured behaviour over randomized
//! intensities, node selections, partition shapes, and send sequences:
//!
//! - **Base latency (5.1)** and **bounded reorder (5.2)**: every delivered copy
//!   lands at `now + base_latency` when reordering is off, and within
//!   `[now + base_latency, now + base_latency + max_reorder)` when it is on.
//! - **Drop (5.3)**: a drop probability of `1.0` delivers nothing; `0.0` drops
//!   nothing.
//! - **Duplicate (5.4)**: a duplication probability of `1.0` delivers exactly
//!   two copies; `0.0` delivers exactly one.
//! - **Directed cut (5.5)**: a directed cut blocks only its `from -> to`
//!   direction; the reverse still delivers.
//! - **Symmetric / asymmetric partition (5.5, 5.6)**: a symmetric partition
//!   blocks both cross-side directions while same-side traffic flows; an
//!   asymmetric partition blocks only the configured direction.
//! - **Heal (5.7)**: a heal restores delivery for messages sent at or after the
//!   heal while a pre-heal message keeps its cut fate.
//! - **Crashed node (6.2)**: a crashed node is cut in both directions until it
//!   is restarted.
//! - **Determinism**: the same seed and configuration reproduce an identical
//!   delivery trace.
//!
//! All randomness flows through the seeded `network` stream, so the whole test
//! is a pure function of its generated seed (Requirement 5.8).
//!
//! Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 6.2

use std::collections::HashMap;

use proptest::prelude::*;

use vela_core::{GroupKey, NodeId, PartitionIndex};
use vela_raft::{NodeId as RaftNodeId, RaftMessage, RequestVoteReply, Transport};
use vela_sim::network::{Envelope, SimNetwork};
use vela_sim::rng::SeedStreams;
use vela_sim::scenario::FaultIntensities;
use vela_sim::scheduler::{HealId, VirtualInstant};

/// The numeric peer id every single-peer transport handle resolves.
const PEER: RaftNodeId = RaftNodeId(1);

/// A node id from the fixed four-node pool the cut/partition/crash blocks draw
/// distinct members from.
fn node_n(i: usize) -> NodeId {
    NodeId::new(format!("node-{i}"))
}

/// The single Raft group every handle in the test belongs to.
fn group() -> GroupKey {
    ("orders".to_string(), PartitionIndex(0))
}

/// A simple, id-free Raft message carrying `term` as a tag so a delivered copy
/// can be matched back to the send that produced it.
fn msg(term: u64) -> RaftMessage {
    RaftMessage::RequestVoteReply(RequestVoteReply {
        term,
        vote_granted: true,
        voter: RaftNodeId(0),
    })
}

/// The `term` tag of a delivered envelope's payload.
fn term_of(env: &Envelope) -> u64 {
    match &env.msg {
        RaftMessage::RequestVoteReply(r) => r.term,
        other => unreachable!("unexpected payload {other:?}"),
    }
}

/// A single-peer replica-set map routing [`PEER`] to `target`, so a handle
/// minted with sender `s` sends `s -> target` on `send(PEER, ..)`.
fn peers_to(target: &NodeId) -> HashMap<RaftNodeId, NodeId> {
    HashMap::from([(PEER, target.clone())])
}

/// Build a network over the run seed's `network` stream with `faults`.
fn net(faults: &FaultIntensities, seed: u64) -> SimNetwork {
    SimNetwork::new(faults, SeedStreams::new(seed).network)
}

/// A fault set with the given base latency / reorder bound and **no**
/// probabilistic fault, so cut, partition, crash, and heal semantics are
/// observed without a stray drop/duplicate/reorder perturbing the counts.
fn clean(base_latency_nanos: u64, max_reorder_nanos: u64) -> FaultIntensities {
    FaultIntensities {
        base_latency_nanos,
        reorder_prob: 0.0,
        max_reorder_nanos,
        drop_prob: 0.0,
        duplicate_prob: 0.0,
        ..FaultIntensities::default()
    }
}

/// Decode a permutation of `[0, 1, 2, 3]` from `code` in `0..24` (a Lehmer
/// code), giving four mutually distinct pool indices for the cut/partition
/// blocks from a single generated integer.
fn permutation4(mut code: u32) -> [usize; 4] {
    let mut items = vec![0usize, 1, 2, 3];
    let mut out = [0usize; 4];
    for slot in &mut out {
        let radix = items.len() as u32;
        let pick = (code % radix) as usize;
        code /= radix;
        *slot = items.remove(pick);
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Property 4: network faults behave exactly as configured.
    #[test]
    fn network_faults_behave_exactly_as_configured(
        seed in any::<u64>(),
        now_nanos in 0u64..=10_000,
        base_latency in 0u64..=5_000,
        max_reorder in 1u64..=5_000,
        perm_code in 0u32..24,
        drop_p in 0.0f64..=1.0,
        dup_p in 0.0f64..=1.0,
        reorder_p in 0.0f64..=1.0,
        sends in prop::collection::vec((0usize..4, any::<u64>()), 1..=24),
    ) {
        let now = VirtualInstant::from_nanos(now_nanos);
        let [a, b, c, d] = permutation4(perm_code).map(node_n);

        // ---- 5.1 base latency, reorder disabled: exact delivery instant ----
        {
            let faults = clean(base_latency, max_reorder);
            let net = net(&faults, seed);
            let tx = net.transport(a.clone(), group(), peers_to(&b));
            net.set_now(now);
            for (_, term) in &sends {
                tx.send(PEER, msg(*term));
            }
            let pending = net.drain_pending();
            prop_assert_eq!(pending.len(), sends.len(), "no fault but latency: one copy each");
            for (at, _) in &pending {
                prop_assert_eq!(
                    *at,
                    VirtualInstant::from_nanos(now_nanos + base_latency),
                    "5.1: every copy is delivered at exactly now + base_latency"
                );
            }
            prop_assert_eq!(net.dropped(), 0);
            prop_assert_eq!(net.duplicated(), 0);
            prop_assert_eq!(net.cut_dropped(), 0);
        }

        // ---- 5.2 reorder enabled: every copy within [base, base+max_reorder) ----
        {
            let faults = FaultIntensities {
                reorder_prob: 1.0,
                ..clean(base_latency, max_reorder)
            };
            let net = net(&faults, seed);
            let tx = net.transport(a.clone(), group(), peers_to(&b));
            net.set_now(now);
            for (_, term) in &sends {
                tx.send(PEER, msg(*term));
            }
            let pending = net.drain_pending();
            prop_assert_eq!(pending.len(), sends.len(), "reorder adds delay, never drops/dups");
            let lo = now_nanos + base_latency;
            let hi = lo + max_reorder;
            for (at, _) in &pending {
                let n = at.as_nanos();
                prop_assert!(n >= lo, "5.2: copy delivered before base latency: {} < {}", n, lo);
                prop_assert!(n < hi, "5.2: reorder delay exceeded its bound: {} >= {}", n, hi);
            }
        }

        // ---- 5.3 drop probability boundaries ----
        {
            let faults = FaultIntensities { drop_prob: 1.0, ..clean(base_latency, max_reorder) };
            let net = net(&faults, seed);
            let tx = net.transport(a.clone(), group(), peers_to(&b));
            net.set_now(now);
            for (_, term) in &sends {
                tx.send(PEER, msg(*term));
            }
            prop_assert_eq!(net.pending_len(), 0, "5.3: drop_prob 1.0 delivers nothing");
            prop_assert_eq!(net.dropped(), sends.len() as u64);
            prop_assert_eq!(net.delivered(), 0);
        }
        {
            let net = net(&clean(base_latency, max_reorder), seed);
            let tx = net.transport(a.clone(), group(), peers_to(&b));
            net.set_now(now);
            for (_, term) in &sends {
                tx.send(PEER, msg(*term));
            }
            prop_assert_eq!(net.pending_len(), sends.len(), "5.3: drop_prob 0.0 drops nothing");
            prop_assert_eq!(net.dropped(), 0);
        }

        // ---- 5.4 duplicate probability boundaries ----
        {
            let faults = FaultIntensities { duplicate_prob: 1.0, ..clean(base_latency, max_reorder) };
            let net = net(&faults, seed);
            let tx = net.transport(a.clone(), group(), peers_to(&b));
            net.set_now(now);
            for (_, term) in &sends {
                tx.send(PEER, msg(*term));
            }
            prop_assert_eq!(net.pending_len(), sends.len() * 2, "5.4: duplicate_prob 1.0 delivers two copies");
            prop_assert_eq!(net.duplicated(), sends.len() as u64);
        }
        {
            let net = net(&clean(base_latency, max_reorder), seed);
            let tx = net.transport(a.clone(), group(), peers_to(&b));
            net.set_now(now);
            for (_, term) in &sends {
                tx.send(PEER, msg(*term));
            }
            prop_assert_eq!(net.pending_len(), sends.len(), "5.4: duplicate_prob 0.0 delivers one copy");
            prop_assert_eq!(net.duplicated(), 0);
        }

        // ---- 5.5 directed cut blocks only the from -> to direction ----
        {
            let net = net(&clean(base_latency, max_reorder), seed);
            let fwd = net.transport(a.clone(), group(), peers_to(&b));
            let rev = net.transport(b.clone(), group(), peers_to(&a));
            net.install_cut(HealId(1), a.clone(), b.clone());
            net.set_now(now);
            fwd.send(PEER, msg(1)); // a -> b: cut
            rev.send(PEER, msg(2)); // b -> a: still delivered
            let pending = net.drain_pending();
            prop_assert_eq!(pending.len(), 1, "5.5: only the reverse direction survives a directed cut");
            prop_assert_eq!(&pending[0].1.from, &b);
            prop_assert_eq!(&pending[0].1.to, &a);
            prop_assert_eq!(net.cut_dropped(), 1);
            prop_assert_eq!(net.dropped(), 0, "a cut is not a probabilistic drop");
        }

        // ---- 5.5 symmetric partition blocks both cross-side directions ----
        {
            // Sides {a, c} | {b, d}: cross pairs are cut both ways; same-side flows.
            let net = net(&clean(base_latency, max_reorder), seed);
            net.install_partition(
                HealId(2),
                [a.clone(), c.clone()],
                [b.clone(), d.clone()],
            );
            net.set_now(now);

            let a_to_b = net.transport(a.clone(), group(), peers_to(&b));
            let b_to_a = net.transport(b.clone(), group(), peers_to(&a));
            let a_to_c = net.transport(a.clone(), group(), peers_to(&c));
            let b_to_d = net.transport(b.clone(), group(), peers_to(&d));

            a_to_b.send(PEER, msg(1)); // cross: cut
            b_to_a.send(PEER, msg(2)); // cross (reverse): cut
            a_to_c.send(PEER, msg(3)); // same side {a,c}: delivered
            b_to_d.send(PEER, msg(4)); // same side {b,d}: delivered

            let pending = net.drain_pending();
            prop_assert_eq!(pending.len(), 2, "5.5: both same-side messages survive, both cross-side cut");
            let delivered_terms: Vec<u64> = pending.iter().map(|(_, e)| term_of(e)).collect();
            prop_assert!(delivered_terms.contains(&3) && delivered_terms.contains(&4));
            prop_assert_eq!(net.cut_dropped(), 2);
        }

        // ---- 5.6 asymmetric partition blocks only the configured direction ----
        {
            let net = net(&clean(base_latency, max_reorder), seed);
            // Block {a, c} -> {b, d} only.
            net.install_asymmetric_partition(
                HealId(3),
                [a.clone(), c.clone()],
                [b.clone(), d.clone()],
            );
            net.set_now(now);

            let a_to_b = net.transport(a.clone(), group(), peers_to(&b));
            let b_to_a = net.transport(b.clone(), group(), peers_to(&a));
            a_to_b.send(PEER, msg(1)); // blocked direction
            b_to_a.send(PEER, msg(2)); // open direction

            let pending = net.drain_pending();
            prop_assert_eq!(pending.len(), 1, "5.6: only the reverse (open) direction delivers");
            prop_assert_eq!(&pending[0].1.from, &b);
            prop_assert_eq!(&pending[0].1.to, &a);
            prop_assert_eq!(term_of(&pending[0].1), 2);
            prop_assert_eq!(net.cut_dropped(), 1);
        }

        // ---- 5.7 heal restores delivery for messages sent at or after the heal ----
        {
            let net = net(&clean(base_latency, max_reorder), seed);
            let tx = net.transport(a.clone(), group(), peers_to(&b));
            net.install_cut(HealId(4), a.clone(), b.clone());
            net.set_now(now);
            tx.send(PEER, msg(1)); // pre-heal: cut, stays cut
            prop_assert!(net.heal(HealId(4)), "heal removes the installed cut");
            prop_assert!(!net.heal(HealId(4)), "healing again is a no-op");
            tx.send(PEER, msg(2)); // post-heal: delivered
            let pending = net.drain_pending();
            prop_assert_eq!(pending.len(), 1, "5.7: only the post-heal message is delivered");
            prop_assert_eq!(term_of(&pending[0].1), 2);
            prop_assert_eq!(net.cut_dropped(), 1, "the pre-heal message kept its cut fate");
        }

        // ---- 6.2 a crashed node is cut both ways until restart ----
        {
            let net = net(&clean(base_latency, max_reorder), seed);
            let a_to_b = net.transport(a.clone(), group(), peers_to(&b));
            let b_to_a = net.transport(b.clone(), group(), peers_to(&a));
            net.crash_node(b.clone());
            net.set_now(now);
            a_to_b.send(PEER, msg(1)); // to crashed node: cut
            b_to_a.send(PEER, msg(2)); // from crashed node: cut
            prop_assert_eq!(net.pending_len(), 0, "6.2: no traffic to or from a crashed node");
            prop_assert_eq!(net.cut_dropped(), 2);

            prop_assert!(net.restart_node(&b), "the node was crashed");
            prop_assert!(!net.restart_node(&b), "restarting a live node is a no-op");
            a_to_b.send(PEER, msg(3)); // delivered after restart
            b_to_a.send(PEER, msg(4)); // delivered after restart
            prop_assert_eq!(net.drain_pending().len(), 2, "6.2: both directions resume after restart");
        }

        // ---- determinism: same seed + config => identical delivery trace ----
        {
            let faults = FaultIntensities {
                base_latency_nanos: base_latency,
                reorder_prob: reorder_p,
                max_reorder_nanos: max_reorder,
                drop_prob: drop_p,
                duplicate_prob: dup_p,
                ..FaultIntensities::default()
            };

            // A trace pairs each surviving copy's delivery instant with its
            // routed destination and message tag, capturing drop/dup/reorder
            // outcomes together.
            let trace = |s: u64| -> Vec<(u64, String, u64)> {
                let net = net(&faults, s);
                net.set_now(now);
                for (dest_idx, term) in &sends {
                    let dest = node_n(*dest_idx);
                    let tx = net.transport(a.clone(), group(), peers_to(&dest));
                    tx.send(PEER, msg(*term));
                }
                net.drain_pending()
                    .into_iter()
                    .map(|(at, env)| (at.as_nanos(), env.to.as_str().to_string(), term_of(&env)))
                    .collect()
            };

            prop_assert_eq!(
                trace(seed),
                trace(seed),
                "5.8: the same seed and config must reproduce an identical delivery trace"
            );
        }
    }
}
