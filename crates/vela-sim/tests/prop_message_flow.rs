#![cfg(feature = "sim")]
//! Property test that every inter-node Raft message flows through the
//! `Sim_Network`.
//!
//! Feature: deterministic-simulation-testing, Property 7: All inter-node
//! messages flow through the Sim_Network
//!
//! Requirement 3.3 demands that *every* inter-node message — `RequestVote`,
//! `AppendEntries`, and their replies, for both partition groups and the
//! metadata group — is routed through the [`SimNetwork`]. This test asserts that
//! structural guarantee at its root: the **only** egress a replica has for
//! messages is [`RaftOutput::sends`], the vector
//! [`RaftNode::step`](vela_raft::RaftNode::step) returns. `RaftNode` never calls
//! a [`Transport`] itself — it has no transport handle — so the harness's
//! contract is simply to dispatch every `out.sends` entry through that replica's
//! [`SimTransport`]. If that contract holds, no message can bypass the bus.
//!
//! The check is operational and exact. We build a small Raft group over the
//! production [`RaftNode`] + the in-memory [`SimBackend`], hand each replica a
//! [`SimTransport`] minted from one shared [`SimNetwork`], drive the group with
//! a seed-derived script of inputs (election / heartbeat ticks and proposals),
//! and dispatch *every* emitted send through its handle — counting each one.
//! Delivered messages are fed back in as `RaftInput::Message` so replies
//! (`RequestVoteReply`, `AppendEntriesReply`) and follow-on `AppendEntries` are
//! exercised too, and they in turn must also flow through the bus.
//!
//! Over a **healthy** network (no faults configured beyond base latency, no cuts
//! installed) the bus's own accounting must therefore balance exactly against
//! what the harness dispatched:
//!
//! - [`SimNetwork::delivered`] == the number of `out.sends` entries dispatched —
//!   every emitted message was buffered by the bus, and none was lost at the
//!   transport (a send to an unknown peer would silently vanish and break this
//!   equality), so no message bypasses or escapes the `Sim_Network`;
//! - [`SimNetwork::dropped`] / [`cut_dropped`](SimNetwork::cut_dropped) /
//!   [`duplicated`](SimNetwork::duplicated) are all zero — a healthy network
//!   neither drops, cuts, nor duplicates, so `delivered` accounts for the whole
//!   message population with nothing unaccounted for.
//!
//! The whole test drives only public APIs, is single-threaded, and draws all
//! randomness from the seeded streams, so it is fully deterministic and never
//! flakes.
//!
//! Validates: Requirements 3.3

use std::collections::HashMap;

use proptest::prelude::*;

use vela_core::{GroupKey, NodeId, PartitionIndex};
use vela_raft::{
    EntryPayload, NodeId as RaftNodeId, PayloadKind, RaftInput, RaftNode, TimerKind, Transport,
};
use vela_sim::clock::SimClock;
use vela_sim::network::SimNetwork;
use vela_sim::rng::SeedStreams;
use vela_sim::scenario::FaultIntensities;
use vela_sim::scheduler::VirtualInstant;
use vela_sim::storage::SimBackend;

/// A safety cap on the total number of dispatched sends, so a pathological
/// cascade cannot loop unboundedly. The `delivered == dispatched` invariant
/// holds at *every* instant (each dispatched send is buffered immediately), so
/// stopping early never weakens the assertion — it only bounds the work.
const DISPATCH_CAP: u64 = 100_000;

/// The single Raft group every replica in the test belongs to. A partition
/// group and the metadata group are indistinguishable to the transport (both
/// are just a [`GroupKey`]), so one group suffices to exercise the routing.
fn group() -> GroupKey {
    ("orders".to_string(), PartitionIndex(0))
}

/// Domain (string) id for the replica with numeric raft id `raft_id`.
fn dom(raft_id: u64) -> NodeId {
    NodeId::new(format!("node-{raft_id}"))
}

/// Step replica `idx` with `input`, then dispatch every message the step
/// emitted through that replica's [`SimTransport`], counting each one.
///
/// This is the harness's entire egress contract: a replica's only output of
/// messages is `out.sends`, and the harness routes each one through the bus.
fn drive(
    nodes: &mut [RaftNode<SimBackend>],
    transports: &[vela_sim::network::SimTransport],
    clock: &mut SimClock,
    idx: usize,
    input: RaftInput,
    dispatched: &mut u64,
) {
    let raft_id = (idx as u64) + 1;
    clock.set_active(dom(raft_id), group());
    let out = nodes[idx].step(input, clock);
    // Drain any timers the step armed; this test does not schedule them, it only
    // needs the `Clock` seam satisfied so `step` can arm.
    let _ = clock.drain_armed();
    for (to, msg) in out.sends {
        *dispatched += 1;
        transports[idx].send(to, msg);
    }
}

/// Deliver every buffered message back into its target replica as a
/// `RaftInput::Message`, dispatching the replies/follow-ons it produces, until
/// the bus quiesces or the dispatch cap is hit.
///
/// Because the group's timers never auto-fire here (the test supplies all ticks
/// explicitly), each delivery produces only a bounded set of follow-on messages,
/// so the loop terminates well before the cap under a healthy network.
fn settle(
    nodes: &mut [RaftNode<SimBackend>],
    transports: &[vela_sim::network::SimTransport],
    clock: &mut SimClock,
    net: &SimNetwork,
    dispatched: &mut u64,
) {
    while net.pending_len() > 0 && *dispatched < DISPATCH_CAP {
        net.set_now(VirtualInstant::ORIGIN);
        let pending = net.drain_pending();
        if pending.is_empty() {
            break;
        }
        for (_at, env) in pending {
            let idx = (env.to_raft.0 - 1) as usize;
            drive(
                nodes,
                transports,
                clock,
                idx,
                RaftInput::Message(env.msg),
                dispatched,
            );
        }
    }
}

proptest! {
    // At least 100 cases (property-test requirement); 256 is proptest's default
    // and covers a broad seed / group-size / script space while staying fast.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property 7: All inter-node messages flow through the Sim_Network
    /// (Requirement 3.3).
    ///
    /// Generators:
    /// - `seed`: the full 64-bit run-seed space (selects election jitter and
    ///   keeps the run reproducible).
    /// - `node_count`: 2 or 3 replicas — enough for real `RequestVote` /
    ///   `AppendEntries` traffic and replies between distinct nodes.
    /// - `script`: a bounded sequence of `(node-selector, action)` pairs;
    ///   `action` is 0 = election tick, 1 = heartbeat tick, 2 = propose, so the
    ///   group is driven through elections, heartbeats, and replication.
    #[test]
    fn all_messages_flow_through_the_network(
        seed in any::<u64>(),
        node_count in 2u64..=3,
        script in prop::collection::vec((any::<u8>(), 0u8..3u8), 1..=48),
    ) {
        let n = node_count;
        let g = group();

        // A healthy network: only base latency, no drop / reorder / duplicate /
        // partition. The whole point is that under no faults the bus accounts
        // for exactly the messages the replicas emit, with nothing lost.
        let net = SimNetwork::new(&FaultIntensities::default(), SeedStreams::new(seed).network);

        // The full `raft id -> domain id` map for the group. A handle resolves a
        // numeric destination through this map; including every id (the sender's
        // own included, though Raft never sends to itself) guarantees no emitted
        // send is dropped at the transport for an unknown peer.
        let peers_map: HashMap<RaftNodeId, NodeId> =
            (1..=n).map(|i| (RaftNodeId(i), dom(i))).collect();

        // Build the replicas over the production `RaftNode` + the in-memory
        // `SimBackend`, each with its real peer set (every other id), and mint a
        // `SimTransport` per replica over the one shared bus.
        let mut nodes: Vec<RaftNode<SimBackend>> = Vec::with_capacity(n as usize);
        let mut transports: Vec<vela_sim::network::SimTransport> = Vec::with_capacity(n as usize);
        for i in 1..=n {
            let peers: Vec<RaftNodeId> = (1..=n).filter(|&j| j != i).map(RaftNodeId).collect();
            nodes.push(RaftNode::new(RaftNodeId(i), peers, SimBackend::in_memory()));
            transports.push(net.transport(dom(i), g.clone(), peers_map.clone()));
        }

        let mut clock = SimClock::new(SeedStreams::new(seed).election);
        net.set_now(VirtualInstant::ORIGIN);

        // Total number of `out.sends` entries the harness dispatched through the
        // transports — the message population the bus must fully account for.
        let mut dispatched: u64 = 0;

        for (sel, action) in script {
            if dispatched >= DISPATCH_CAP {
                break;
            }
            let idx = (sel as usize) % (n as usize);
            let input = match action {
                0 => RaftInput::Tick(TimerKind::Election),
                1 => RaftInput::Tick(TimerKind::Heartbeat),
                _ => RaftInput::Propose(EntryPayload::new(PayloadKind::Record, vec![sel])),
            };
            drive(&mut nodes, &transports, &mut clock, idx, input, &mut dispatched);
            // Deliver the resulting traffic (and its replies) so reply message
            // types flow through the bus too.
            settle(&mut nodes, &transports, &mut clock, &net, &mut dispatched);
        }

        // The core structural guarantee: the bus buffered exactly the messages
        // the replicas emitted — none bypassed the `Sim_Network`, and none was
        // lost at the transport. Over a healthy network there are no drops, cuts,
        // or duplicates, so `delivered` equals the whole dispatched population.
        prop_assert_eq!(
            net.delivered(),
            dispatched,
            "every emitted inter-node message must be accounted for by the bus"
        );
        prop_assert_eq!(net.dropped(), 0, "a healthy network drops nothing");
        prop_assert_eq!(net.cut_dropped(), 0, "no cut is installed");
        prop_assert_eq!(net.duplicated(), 0, "a healthy network duplicates nothing");
        // Nothing should be left unaccounted for: every buffered delivery was
        // drained and fed back, so the bus is quiescent (unless the safety cap
        // intervened, which still preserves the equality above).
        prop_assert!(
            net.pending_len() == 0 || dispatched >= DISPATCH_CAP,
            "all buffered deliveries must be drained when below the dispatch cap"
        );
    }
}
