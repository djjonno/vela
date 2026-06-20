//! `Sim_Network`: the deterministic in-memory `Transport` seam with fault
//! injection.
//!
//! This module generalizes the single-group `vela_raft::sim::Bus` /
//! `InMemoryTransport` pair to the multi-node, multi-group cluster. Every
//! replica in the [`Simulated_Cluster`] is handed a per-`(node, group)`
//! [`SimTransport`] handle; all inter-node Raft traffic — `RequestVote`,
//! `AppendEntries`, and their replies, for both partition groups and `__meta/0`
//! — flows through the one shared [`SimNetwork`] bus behind those handles
//! (Requirement 3.3).
//!
//! # Why per-`(node, group)` handles
//!
//! The production [`Transport::send`] signature is `send(to, msg)` — it carries
//! neither the *sender* nor the *group* the message belongs to. The production
//! `GrpcTransport` solves this by stamping each handle with its `(topic,
//! partition)` and `self_id`; this harness does exactly the same: a
//! [`SimTransport`] is stamped with its sender [`NodeId`] and its [`GroupKey`]
//! at construction, so the bus learns both without changing the trait
//! (mirroring how `vela_raft::sim::InMemoryTransport` stamps `from`).
//!
//! The `to` argument is a numeric [`vela_raft::NodeId`] (the id Raft keys its
//! peer state by); the bus addresses nodes by their domain [`vela_core::NodeId`].
//! Each handle therefore also carries the group's `raft id -> domain id` map (a
//! fixed per-group [`Replica_Set`] mapping the cluster builds) and resolves the
//! destination back to the bus's addressing on every send.
//!
//! # Determinism and dispatch model
//!
//! `RaftNode::step` returns its outbound messages in `RaftOutput.sends` rather
//! than calling [`Transport`] directly, so — exactly as production and
//! `SimCluster` do — the runtime dispatches those sends through the handles
//! *after* a step returns, never inside it. On each [`SimTransport::send`] the
//! bus consults the seeded **network** RNG stream and the current
//! [`FaultIntensities`] to decide the message's fate, then buffers the resulting
//! deliveries. The runtime drains them with [`SimNetwork::drain_pending`] and
//! schedules each as an [`Event::MessageDeliver`] at its computed instant; the
//! scheduler's `(deliver_at, tie_break)` total order then fixes delivery order
//! deterministically (Requirement 5.8).
//!
//! Faults applied here (Requirements 5.1–5.4):
//!
//! - **Base latency** — every delivered copy has the configured one-way latency
//!   added to its delivery instant.
//! - **Reorder** — when enabled, a per-message seed-derived extra delay within a
//!   bound is added, so a later-sent message can be delivered first.
//! - **Drop** — with the configured seed-derived probability a message is
//!   dropped and never buffered.
//! - **Duplicate** — with the configured seed-derived probability one extra copy
//!   is buffered in addition to the original.
//!
//! Network *partitions* are layered on **ahead** of the fault model above
//! (Requirements 5.5–5.7, 6.2): before any drop/reorder/duplicate decision,
//! [`enqueue`] checks whether `(from -> to)` is cut. A cut is **deterministic,
//! not probabilistic** — it is installed by the fault schedule (a later task),
//! never drawn from the network RNG — so a cut message is dropped without
//! touching the RNG, leaving every downstream seed-derived decision for non-cut
//! messages exactly as it was. A message is cut when:
//!
//! - a **directed cut set** blocks its `(from, to)` direction (Requirement 5.5);
//! - it straddles a **partition** — symmetric (both directions) or asymmetric
//!   (one direction only, Requirement 5.6); or
//! - either endpoint is a **crashed node**, which is cut in both directions until
//!   it restarts (Requirement 6.2).
//!
//! **Heal** (Requirement 5.7) removes a partition or directed cut by its
//! [`HealId`]. Because the cut is applied at *send* time (in [`enqueue`]), heal
//! semantics fall out for free: a message enqueued after the cut is removed is
//! simply delivered, while messages already enqueued before the heal keep their
//! fate — i.e. a heal restores delivery for messages **sent at or after** the
//! heal. Healing only lifts the cut; any still-configured drop/delay/reorder/
//! duplication fault continues to apply to those post-heal messages, since they
//! flow through the unchanged drop/reorder/duplicate path once the cut check
//! passes.
//!
//! [`Simulated_Cluster`]: crate
//! [`Replica_Set`]: crate
//! [`enqueue`]: Bus::enqueue
//! [`Event::MessageDeliver`]: crate::scheduler::Event::MessageDeliver

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use vela_core::{GroupKey, NodeId};
use vela_raft::{NodeId as RaftNodeId, RaftMessage, Transport};

use crate::rng::SplitMix64;
use crate::scenario::FaultIntensities;
use crate::scheduler::{HealId, VirtualDuration, VirtualInstant};

/// A routed Raft message in flight on the [`Sim_Network`].
///
/// This is the canonical message envelope for the harness: it carries the
/// routing stamp the runtime needs to deliver the message to the right replica
/// **and** the `vela_raft::RaftMessage` payload to feed that replica. The
/// scheduler's [`Event::MessageDeliver`] wraps one of these; the bus produces
/// them on [`SimTransport::send`] and the runtime schedules them.
///
/// [`Sim_Network`]: crate
/// [`Event::MessageDeliver`]: crate::scheduler::Event::MessageDeliver
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The sending node, in the domain (string) addressing the bus uses. Used
    /// for partition cut-set checks (a later task) and diagnostics.
    pub from: NodeId,
    /// The receiving node, in domain addressing — the replica the runtime
    /// delivers this message to (paired with [`group`](Self::group)).
    pub to: NodeId,
    /// The recipient's numeric [`vela_raft::NodeId`] within [`group`](Self::group),
    /// i.e. the `to` originally passed to [`Transport::send`]. Carried so the
    /// delivery target is unambiguous even though the runtime routes by the
    /// domain `(to, group)` pair.
    pub to_raft: RaftNodeId,
    /// The Raft group the message belongs to (a partition group or `__meta/0`).
    pub group: GroupKey,
    /// The Raft message payload to feed to the recipient replica.
    pub msg: RaftMessage,
}

/// The network-relevant slice of [`FaultIntensities`], in the scheduler's
/// logical-time units.
///
/// Holding only the fields the bus consults keeps fault decisions readable and
/// decouples the bus from the unrelated (crash / storage / skew) intensities.
#[derive(Debug, Clone, Copy)]
struct NetworkConfig {
    /// Base one-way latency added to every delivered copy (Requirement 5.1).
    base_latency: VirtualDuration,
    /// Probability a delivered message receives extra reorder delay
    /// (Requirement 5.2).
    reorder_prob: f64,
    /// Upper bound on the extra reorder delay when applied (Requirement 5.2).
    max_reorder: VirtualDuration,
    /// Probability a message is dropped and never delivered (Requirement 5.3).
    drop_prob: f64,
    /// Probability a delivered message is duplicated with one extra copy
    /// (Requirement 5.4).
    duplicate_prob: f64,
}

impl NetworkConfig {
    /// Project the network-relevant fields out of the run's [`FaultIntensities`].
    fn from_faults(faults: &FaultIntensities) -> Self {
        Self {
            base_latency: VirtualDuration::from_nanos(faults.base_latency_nanos),
            reorder_prob: faults.reorder_prob,
            max_reorder: VirtualDuration::from_nanos(faults.max_reorder_nanos),
            drop_prob: faults.drop_prob,
            duplicate_prob: faults.duplicate_prob,
        }
    }
}

/// The direction(s) a [`Cut::Partition`] severs across its two sides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionDirection {
    /// Block delivery in both directions across the split (a symmetric
    /// partition, Requirement 5.5).
    Symmetric,
    /// Block only `side_a -> side_b`; the reverse direction still delivers (an
    /// asymmetric partition, Requirement 5.6).
    AToB,
}

/// One installed network cut, keyed by a [`HealId`] so the runtime can lift it
/// with a later heal (Requirement 5.7).
///
/// Cuts are deterministic — installed by the fault schedule, never drawn from
/// the network RNG — so consulting them on [`Bus::enqueue`] consumes no
/// randomness and leaves every seed-derived decision untouched.
#[derive(Debug, Clone)]
enum Cut {
    /// A directed cut set: blocks exactly the `from -> to` direction
    /// (Requirement 5.5).
    Directed {
        /// The sending node whose traffic is blocked.
        from: NodeId,
        /// The receiving node the traffic is blocked toward.
        to: NodeId,
    },
    /// A partition between two disjoint sides, severing cross-side delivery in
    /// one or both directions (Requirements 5.5, 5.6).
    Partition {
        /// One side of the split.
        side_a: BTreeSet<NodeId>,
        /// The other side of the split.
        side_b: BTreeSet<NodeId>,
        /// Which cross-side direction(s) are blocked.
        direction: PartitionDirection,
    },
}

impl Cut {
    /// Whether this cut blocks a `from -> to` delivery.
    ///
    /// Order-independent (a pure membership test), so it is insensitive to the
    /// iteration order of the bus's cut map — keeping the cut outcome a
    /// deterministic function of the installed faults alone.
    fn blocks(&self, from: &NodeId, to: &NodeId) -> bool {
        match self {
            Cut::Directed { from: f, to: t } => from == f && to == t,
            Cut::Partition {
                side_a,
                side_b,
                direction,
            } => {
                let a_to_b = side_a.contains(from) && side_b.contains(to);
                match direction {
                    PartitionDirection::Symmetric => {
                        a_to_b || (side_b.contains(from) && side_a.contains(to))
                    }
                    PartitionDirection::AToB => a_to_b,
                }
            }
        }
    }
}

/// A message copy buffered by the bus, awaiting the runtime to schedule it.
#[derive(Debug, Clone)]
struct PendingDelivery {
    /// The logical instant at which the copy is to be delivered.
    at: VirtualInstant,
    /// The routed message.
    envelope: Envelope,
}

/// The shared message bus behind every [`SimTransport`] handle.
///
/// Holds the buffered deliveries, the current logical instant (mirrored from the
/// scheduler before each dispatch), the network fault configuration, the seeded
/// **network** RNG stream, and diagnostic counters. Interior mutability lets the
/// `&self` [`Transport::send`] mutate it through an [`Rc<RefCell<Bus>>`].
#[derive(Debug)]
struct Bus {
    /// Current logical instant; the runtime mirrors the scheduler's `now` here
    /// before draining each step's sends so deliveries are scheduled relative to
    /// the event being processed.
    now: VirtualInstant,
    /// Network fault configuration for the run.
    config: NetworkConfig,
    /// Seeded `network` RNG stream — the sole source of every drop / reorder /
    /// duplicate decision, so all of them are a deterministic function of the
    /// run seed (Requirement 5.8).
    rng: SplitMix64,
    /// Buffered deliveries the runtime has not yet drained.
    pending: Vec<PendingDelivery>,
    /// Installed network cuts, keyed by [`HealId`] so a later heal can lift a
    /// specific one (Requirements 5.5–5.7). Iteration is only ever used for an
    /// order-independent "any cut blocks this message?" test.
    cuts: HashMap<HealId, Cut>,
    /// Nodes currently crashed; each is cut in both directions until it restarts
    /// (Requirement 6.2).
    crashed: BTreeSet<NodeId>,
    /// Count of messages dropped (by the drop probability). Diagnostics only.
    dropped: u64,
    /// Count of messages dropped because `(from -> to)` was cut by a partition
    /// or a crashed endpoint. Distinct from [`dropped`](Self::dropped): a cut is
    /// deterministic and consumes no RNG. Diagnostics only.
    cut_dropped: u64,
    /// Count of duplicate copies produced (by the duplication probability).
    duplicated: u64,
    /// Count of message copies buffered for delivery (primaries + duplicates).
    delivered: u64,
}

impl Bus {
    /// Create a bus over `network_rng` with the network fault `config`, anchored
    /// at logical origin.
    fn new(config: NetworkConfig, network_rng: SplitMix64) -> Self {
        Self {
            now: VirtualInstant::ORIGIN,
            config,
            rng: network_rng,
            pending: Vec::new(),
            cuts: HashMap::new(),
            crashed: BTreeSet::new(),
            dropped: 0,
            cut_dropped: 0,
            duplicated: 0,
            delivered: 0,
        }
    }

    /// The extra reorder delay for one copy: zero unless reordering is enabled
    /// and this copy is selected, in which case a seed-derived delay in
    /// `0..max_reorder` (Requirement 5.2).
    ///
    /// Draws from the RNG **only** when `reorder_prob > 0`, so a run with
    /// reordering disabled consumes no reorder randomness — keeping RNG
    /// consumption (and thus every downstream decision) stable across configs.
    fn extra_reorder_delay(&mut self) -> VirtualDuration {
        if self.config.reorder_prob > 0.0 && self.rng.next_f64() < self.config.reorder_prob {
            // `next_below(0)` is 0, so a zero bound is safe (no delay).
            VirtualDuration::from_nanos(self.rng.next_below(self.config.max_reorder.as_nanos()))
        } else {
            VirtualDuration::ZERO
        }
    }

    /// Buffer one copy of `msg` for delivery, applying base latency plus any
    /// reorder delay to compute its delivery instant.
    fn push_one(&mut self, envelope: Envelope) {
        let delay = self
            .config
            .base_latency
            .saturating_add(self.extra_reorder_delay());
        let at = self.now.saturating_add(delay);
        self.pending.push(PendingDelivery { at, envelope });
        self.delivered += 1;
    }

    /// Whether `(from -> to)` is currently cut — by a crashed endpoint (cut in
    /// both directions, Requirement 6.2) or by any installed partition / directed
    /// cut set (Requirements 5.5, 5.6).
    ///
    /// This is deterministic and RNG-free: it never consults the network RNG, so
    /// the drop/reorder/duplicate decisions for non-cut messages are unaffected.
    fn is_cut(&self, from: &NodeId, to: &NodeId) -> bool {
        if self.crashed.contains(from) || self.crashed.contains(to) {
            return true;
        }
        self.cuts.values().any(|cut| cut.blocks(from, to))
    }

    /// Accept a message from a handle, applying the partition cut set first and
    /// then the drop and duplication faults before buffering it for delivery.
    ///
    /// Decision order is fixed for determinism: the **cut** check first (a
    /// deterministic, RNG-free gate, Requirements 5.5/5.6/6.2), then drop, then
    /// the primary copy (which may draw a reorder delay), then the duplication
    /// decision (whose extra copy may draw its own reorder delay). Because the
    /// cut check precedes — and never touches — the RNG, every seed-derived
    /// decision for a non-cut message is identical to a run with no cuts
    /// installed.
    fn enqueue(&mut self, envelope: Envelope) {
        if self.is_cut(&envelope.from, &envelope.to) {
            self.cut_dropped += 1;
            return;
        }
        if self.config.drop_prob > 0.0 && self.rng.next_f64() < self.config.drop_prob {
            self.dropped += 1;
            return;
        }
        self.push_one(envelope.clone());
        if self.config.duplicate_prob > 0.0 && self.rng.next_f64() < self.config.duplicate_prob {
            self.duplicated += 1;
            self.push_one(envelope);
        }
    }
}

/// A per-`(node, group)` [`Transport`] handle over a shared [`Bus`].
///
/// Each replica in a group owns one of these, stamped with the replica's domain
/// [`NodeId`] and its [`GroupKey`] (the production `Transport::send` signature
/// carries neither). It also holds the group's `raft id -> domain id` map so it
/// can resolve the numeric destination [`Transport::send`] is given back to the
/// bus's domain addressing.
#[derive(Debug, Clone)]
pub struct SimTransport {
    /// The domain identity of the node this handle sends on behalf of.
    from: NodeId,
    /// The Raft group this handle belongs to.
    group: GroupKey,
    /// The group's fixed `raft id -> domain id` map (its [`Replica_Set`]),
    /// used to resolve the numeric destination of a [`Transport::send`].
    ///
    /// [`Replica_Set`]: crate
    peers: HashMap<RaftNodeId, NodeId>,
    /// The shared message bus.
    bus: Rc<RefCell<Bus>>,
}

impl Transport for SimTransport {
    /// Route `msg` toward the peer identified by the numeric `to` within this
    /// handle's group.
    ///
    /// Resolves `to` to a domain [`NodeId`] via the group's replica-set map and
    /// hands the stamped [`Envelope`] to the bus, which applies the configured
    /// faults. A `to` not in the group's map is silently ignored: Raft only ever
    /// sends to peers in its own group, all of which the map covers, so this is
    /// purely defensive.
    fn send(&self, to: RaftNodeId, msg: RaftMessage) {
        let Some(to_core) = self.peers.get(&to).cloned() else {
            return;
        };
        self.bus.borrow_mut().enqueue(Envelope {
            from: self.from.clone(),
            to: to_core,
            to_raft: to,
            group: self.group.clone(),
            msg,
        });
    }
}

/// The deterministic in-memory message bus shared by every replica's
/// [`SimTransport`] handle.
///
/// Construct one per [`Simulation_Run`] from the run's [`FaultIntensities`] and
/// its `network` RNG stream, then mint a [`SimTransport`] for each replica with
/// [`transport`](Self::transport). Around each replica step the runtime mirrors
/// the scheduler instant with [`set_now`](Self::set_now), dispatches the
/// replica's sends through its handle, and drains the resulting deliveries with
/// [`drain_pending`](Self::drain_pending) to schedule them.
///
/// [`Simulation_Run`]: crate
#[derive(Debug, Clone)]
pub struct SimNetwork {
    bus: Rc<RefCell<Bus>>,
}

impl SimNetwork {
    /// Create a network bus over the run's `network` RNG stream, configured by
    /// the network-relevant fields of `faults`.
    #[must_use]
    pub fn new(faults: &FaultIntensities, network_rng: SplitMix64) -> Self {
        Self {
            bus: Rc::new(RefCell::new(Bus::new(
                NetworkConfig::from_faults(faults),
                network_rng,
            ))),
        }
    }

    /// Mint a [`SimTransport`] for a replica identified by domain id `from` in
    /// `group`, resolving destinations through the group's `raft id -> domain
    /// id` map `peers` (its fixed [`Replica_Set`]).
    ///
    /// [`Replica_Set`]: crate
    #[must_use]
    pub fn transport(
        &self,
        from: NodeId,
        group: GroupKey,
        peers: HashMap<RaftNodeId, NodeId>,
    ) -> SimTransport {
        SimTransport {
            from,
            group,
            peers,
            bus: Rc::clone(&self.bus),
        }
    }

    /// Mirror the scheduler's current logical instant onto the bus so deliveries
    /// sent next are scheduled relative to it.
    pub fn set_now(&self, now: VirtualInstant) {
        self.bus.borrow_mut().now = now;
    }

    /// Install a **directed cut set** blocking the `from -> to` direction,
    /// keyed by `id` for a later [`heal`](Self::heal) (Requirement 5.5).
    ///
    /// Only `from -> to` is severed; `to -> from` is unaffected. Messages
    /// enqueued after this call whose `(from, to)` matches are dropped before any
    /// RNG-driven fault decision. Re-using an existing `id` replaces its cut.
    pub fn install_cut(&self, id: HealId, from: NodeId, to: NodeId) {
        self.bus
            .borrow_mut()
            .cuts
            .insert(id, Cut::Directed { from, to });
    }

    /// Install a **symmetric partition** splitting the cluster into `side_a` and
    /// `side_b`, blocking every cross-side delivery in **both** directions, keyed
    /// by `id` for a later [`heal`](Self::heal) (Requirement 5.5).
    ///
    /// Deliveries within a side, and any node in neither side, are unaffected.
    /// Re-using an existing `id` replaces its cut.
    pub fn install_partition<A, B>(&self, id: HealId, side_a: A, side_b: B)
    where
        A: IntoIterator<Item = NodeId>,
        B: IntoIterator<Item = NodeId>,
    {
        self.bus.borrow_mut().cuts.insert(
            id,
            Cut::Partition {
                side_a: side_a.into_iter().collect(),
                side_b: side_b.into_iter().collect(),
                direction: PartitionDirection::Symmetric,
            },
        );
    }

    /// Install an **asymmetric partition** blocking delivery only in the
    /// `from_side -> to_side` direction, keyed by `id` for a later
    /// [`heal`](Self::heal) (Requirement 5.6).
    ///
    /// The reverse direction (`to_side -> from_side`) continues to deliver
    /// (subject to the other configured faults). Re-using an existing `id`
    /// replaces its cut.
    pub fn install_asymmetric_partition<F, T>(&self, id: HealId, from_side: F, to_side: T)
    where
        F: IntoIterator<Item = NodeId>,
        T: IntoIterator<Item = NodeId>,
    {
        self.bus.borrow_mut().cuts.insert(
            id,
            Cut::Partition {
                side_a: from_side.into_iter().collect(),
                side_b: to_side.into_iter().collect(),
                direction: PartitionDirection::AToB,
            },
        );
    }

    /// Heal (remove) the partition or directed cut installed under `id`,
    /// returning whether a cut was present (Requirement 5.7).
    ///
    /// Heal restores delivery only for messages **sent at or after** this call:
    /// the cut is applied at send time, so a message enqueued after the heal is
    /// delivered, while messages already buffered keep their fate. Any
    /// still-configured drop/delay/reorder/duplication fault continues to apply
    /// to post-heal messages — they flow through the unchanged fault path once
    /// the (now-removed) cut no longer blocks them.
    pub fn heal(&self, id: HealId) -> bool {
        self.bus.borrow_mut().cuts.remove(&id).is_some()
    }

    /// Mark `node` crashed, cutting it off in **both** directions until it is
    /// restarted (Requirement 6.2): no message to or from it is delivered.
    pub fn crash_node(&self, node: NodeId) {
        self.bus.borrow_mut().crashed.insert(node);
    }

    /// Restart `node`, lifting its crashed-node cut so it can send and receive
    /// again, returning whether it had been crashed.
    ///
    /// As with [`heal`](Self::heal), delivery resumes only for messages sent at
    /// or after the restart; the other configured faults still apply to them.
    pub fn restart_node(&self, node: &NodeId) -> bool {
        self.bus.borrow_mut().crashed.remove(node)
    }

    /// Drain every buffered delivery, returning `(deliver_at, envelope)` pairs in
    /// deterministic buffering order for the runtime to schedule as
    /// [`Event::MessageDeliver`] events.
    ///
    /// The order returned is the order copies were buffered (primary before its
    /// duplicate); final delivery order is fixed by the scheduler's
    /// `(deliver_at, tie_break)` total order, not by this order.
    ///
    /// [`Event::MessageDeliver`]: crate::scheduler::Event::MessageDeliver
    pub fn drain_pending(&self) -> Vec<(VirtualInstant, Envelope)> {
        self.bus
            .borrow_mut()
            .pending
            .drain(..)
            .map(|d| (d.at, d.envelope))
            .collect()
    }

    /// Number of buffered deliveries not yet drained.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.bus.borrow().pending.len()
    }

    /// Total messages dropped by the drop probability (diagnostics).
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.bus.borrow().dropped
    }

    /// Total messages dropped because `(from -> to)` was cut by a partition,
    /// directed cut set, or crashed endpoint (diagnostics). Distinct from
    /// [`dropped`](Self::dropped): cuts are deterministic and consume no RNG.
    #[must_use]
    pub fn cut_dropped(&self) -> u64 {
        self.bus.borrow().cut_dropped
    }

    /// Total duplicate copies produced by the duplication probability
    /// (diagnostics).
    #[must_use]
    pub fn duplicated(&self) -> u64 {
        self.bus.borrow().duplicated
    }

    /// Total message copies buffered for delivery, primaries plus duplicates
    /// (diagnostics).
    #[must_use]
    pub fn delivered(&self) -> u64 {
        self.bus.borrow().delivered
    }

    /// Whether the network currently cuts delivery from `from` to `to` — by a
    /// crashed endpoint (cut in both directions, Requirement 6.2) or by any
    /// installed partition / directed cut set (Requirements 5.5, 5.6).
    ///
    /// A read-only window onto the bus's cut state, exposing the internal
    /// [`Bus::is_cut`] gate so a reachability-aware observer (the
    /// [`LivenessChecker`](crate::checker::liveness::LivenessChecker)) can ask
    /// whether two nodes can currently exchange messages. Two running nodes
    /// `a` and `b` are *mutually reachable* iff neither `is_cut(a, b)` nor
    /// `is_cut(b, a)` holds.
    ///
    /// This is the same deterministic, RNG-free test the bus applies at send
    /// time, so it consults no randomness and does not perturb any seed-derived
    /// fault decision.
    #[must_use]
    pub fn is_cut(&self, from: &NodeId, to: &NodeId) -> bool {
        self.bus.borrow().is_cut(from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::SeedStreams;
    use vela_raft::RequestVoteReply;

    use std::collections::HashMap;

    // Domain ids used across the tests.
    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    fn group() -> GroupKey {
        ("orders".to_string(), vela_core::PartitionIndex(0))
    }

    /// A simple, id-free Raft message usable as a payload in tests.
    fn msg(term: u64) -> RaftMessage {
        RaftMessage::RequestVoteReply(RequestVoteReply {
            term,
            vote_granted: true,
            voter: RaftNodeId(0),
        })
    }

    /// A two-peer replica-set map: raft ids 1 and 2 map to node-b and node-c.
    fn peers() -> HashMap<RaftNodeId, NodeId> {
        HashMap::from([
            (RaftNodeId(1), node("node-b")),
            (RaftNodeId(2), node("node-c")),
        ])
    }

    /// Build a network whose only active behaviour is the given fault tweak.
    fn net_with(faults: FaultIntensities, seed: u64) -> SimNetwork {
        SimNetwork::new(&faults, SeedStreams::new(seed).network)
    }

    /// A healthy-network intensity set (only base latency), as the default
    /// scenario uses.
    fn healthy() -> FaultIntensities {
        FaultIntensities::default()
    }

    /// Requirement 5.1: every delivered message has the base one-way latency
    /// added to its delivery instant.
    #[test]
    fn base_latency_is_added_to_every_message() {
        let faults = healthy();
        let net = net_with(faults, 1);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::from_nanos(1_000));
        tx.send(RaftNodeId(1), msg(7));

        let pending = net.drain_pending();
        assert_eq!(
            pending.len(),
            1,
            "exactly one copy with no fault but latency"
        );
        let (at, env) = &pending[0];
        assert_eq!(
            *at,
            VirtualInstant::from_nanos(1_000 + faults.base_latency_nanos),
            "delivery instant must be now + base latency"
        );
        assert_eq!(env.from, node("node-a"));
        assert_eq!(env.to, node("node-b"));
        assert_eq!(env.to_raft, RaftNodeId(1));
        assert_eq!(env.group, group());
        assert_eq!(env.msg, msg(7));
        assert_eq!(net.delivered(), 1);
        assert_eq!(net.dropped(), 0);
        assert_eq!(net.duplicated(), 0);
    }

    /// The transport stamps the handle's own `(node, group)` onto every message
    /// and resolves the numeric destination through the replica-set map.
    #[test]
    fn transport_stamps_sender_group_and_resolves_destination() {
        let net = net_with(healthy(), 2);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::ORIGIN);
        tx.send(RaftNodeId(2), msg(3));

        let pending = net.drain_pending();
        let (_, env) = &pending[0];
        assert_eq!(env.from, node("node-a"), "stamped with the handle's sender");
        assert_eq!(env.group, group(), "stamped with the handle's group");
        assert_eq!(env.to, node("node-c"), "raft id 2 resolves to node-c");
        assert_eq!(env.to_raft, RaftNodeId(2));
    }

    /// A destination not in the group's replica-set map is ignored (defensive;
    /// Raft never sends outside its group).
    #[test]
    fn unknown_destination_is_not_delivered() {
        let net = net_with(healthy(), 3);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::ORIGIN);
        tx.send(RaftNodeId(999), msg(1));

        assert_eq!(net.pending_len(), 0);
        assert_eq!(net.delivered(), 0);
        assert!(net.drain_pending().is_empty());
    }

    /// Requirement 5.3: with a drop probability of 1.0 every message is dropped
    /// and never delivered.
    #[test]
    fn certain_drop_never_delivers() {
        let faults = FaultIntensities {
            drop_prob: 1.0,
            ..healthy()
        };
        let net = net_with(faults, 4);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::ORIGIN);
        for _ in 0..16 {
            tx.send(RaftNodeId(1), msg(1));
        }

        assert_eq!(net.pending_len(), 0, "no message survives a 1.0 drop prob");
        assert_eq!(net.dropped(), 16);
        assert_eq!(net.delivered(), 0);
    }

    /// Requirement 5.3: a drop probability of 0.0 never drops.
    #[test]
    fn zero_drop_delivers_everything() {
        let net = net_with(healthy(), 5);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::ORIGIN);
        for _ in 0..16 {
            tx.send(RaftNodeId(1), msg(1));
        }

        assert_eq!(net.pending_len(), 16);
        assert_eq!(net.dropped(), 0);
        assert_eq!(net.delivered(), 16);
    }

    /// Requirement 5.4: with a duplication probability of 1.0 every message is
    /// delivered twice (the original plus one extra copy).
    #[test]
    fn certain_duplication_delivers_two_copies() {
        let faults = FaultIntensities {
            duplicate_prob: 1.0,
            ..healthy()
        };
        let net = net_with(faults, 6);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::ORIGIN);
        tx.send(RaftNodeId(1), msg(2));

        let pending = net.drain_pending();
        assert_eq!(pending.len(), 2, "original plus one duplicate");
        assert!(pending.iter().all(|(_, e)| e.msg == msg(2)));
        assert_eq!(net.duplicated(), 1);
        assert_eq!(net.delivered(), 2);
    }

    /// Requirement 5.4: a duplication probability of 0.0 never duplicates.
    #[test]
    fn zero_duplication_delivers_one_copy() {
        let net = net_with(healthy(), 7);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::ORIGIN);
        tx.send(RaftNodeId(1), msg(2));

        assert_eq!(net.pending_len(), 1);
        assert_eq!(net.duplicated(), 0);
        assert_eq!(net.delivered(), 1);
    }

    /// Requirement 5.2: when reordering is enabled every extra delay stays within
    /// the configured bound, and (for this seed) at least one message receives a
    /// non-zero extra delay — so delivery instants can differ from send order.
    #[test]
    fn reorder_delay_is_bounded_and_can_perturb_order() {
        let faults = FaultIntensities {
            reorder_prob: 1.0,
            max_reorder_nanos: 5_000,
            ..healthy()
        };
        let base = faults.base_latency_nanos;
        let net = net_with(faults, 8);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::ORIGIN);
        let count = 64;
        for _ in 0..count {
            tx.send(RaftNodeId(1), msg(1));
        }

        let pending = net.drain_pending();
        assert_eq!(pending.len() as u64, count);

        let mut saw_extra = false;
        for (at, _) in &pending {
            let nanos = at.as_nanos();
            assert!(nanos >= base, "below base latency: {nanos}");
            assert!(
                nanos < base + 5_000,
                "extra reorder delay exceeded the bound: {nanos}"
            );
            if nanos > base {
                saw_extra = true;
            }
        }
        assert!(
            saw_extra,
            "with reorder_prob 1.0 some message should receive a non-zero extra delay"
        );

        // The buffered instants are not all identical: ordering by deliver_at
        // would differ from the send order, which is the point of reordering.
        let instants: Vec<u64> = pending.iter().map(|(at, _)| at.as_nanos()).collect();
        let first = instants[0];
        assert!(
            instants.iter().any(|&n| n != first),
            "reorder should produce differing delivery instants"
        );
    }

    /// Requirement 5.8: every fault decision is a deterministic function of the
    /// seed — two buses built from the same seed produce identical deliveries for
    /// the same sends, and a different seed differs.
    #[test]
    fn fault_decisions_are_deterministic_for_a_seed() {
        let faults = FaultIntensities {
            drop_prob: 0.3,
            duplicate_prob: 0.3,
            reorder_prob: 0.5,
            max_reorder_nanos: 4_000,
            ..healthy()
        };

        let trace = |seed: u64| -> Vec<(u64, u64)> {
            let net = net_with(faults, seed);
            let tx = net.transport(node("node-a"), group(), peers());
            net.set_now(VirtualInstant::from_nanos(10));
            for term in 0..64 {
                tx.send(RaftNodeId(1), msg(term));
            }
            // Pair each surviving copy's delivery instant with its message term,
            // capturing drop/dup/reorder outcomes together.
            net.drain_pending()
                .into_iter()
                .map(|(at, env)| {
                    let term = match env.msg {
                        RaftMessage::RequestVoteReply(r) => r.term,
                        _ => unreachable!(),
                    };
                    (at.as_nanos(), term)
                })
                .collect()
        };

        let a = trace(0xABCD);
        let b = trace(0xABCD);
        assert_eq!(a, b, "same seed must reproduce identical fault decisions");

        let c = trace(0x1234);
        assert_ne!(
            a, c,
            "a different seed should produce different fault decisions"
        );
    }

    /// `set_now` anchors delivery instants: a message sent at a later instant is
    /// delivered later by exactly the same base latency.
    #[test]
    fn delivery_instant_tracks_set_now() {
        let faults = healthy();
        let base = faults.base_latency_nanos;
        let net = net_with(faults, 9);
        let tx = net.transport(node("node-a"), group(), peers());

        net.set_now(VirtualInstant::from_nanos(100));
        tx.send(RaftNodeId(1), msg(1));
        net.set_now(VirtualInstant::from_nanos(500));
        tx.send(RaftNodeId(1), msg(2));

        let pending = net.drain_pending();
        assert_eq!(pending[0].0, VirtualInstant::from_nanos(100 + base));
        assert_eq!(pending[1].0, VirtualInstant::from_nanos(500 + base));
    }

    /// A single-peer replica-set map routing raft id 1 to `target`, so a handle
    /// minted with sender `s` sends `s -> target` on `send(RaftNodeId(1), ..)`.
    fn peers_to(target: &str) -> HashMap<RaftNodeId, NodeId> {
        HashMap::from([(RaftNodeId(1), node(target))])
    }

    /// Requirement 5.5: a directed cut set blocks only the `from -> to`
    /// direction; the reverse direction still delivers.
    #[test]
    fn directed_cut_blocks_one_direction_only() {
        let net = net_with(healthy(), 10);
        let a_to_b = net.transport(node("node-a"), group(), peers_to("node-b"));
        let b_to_a = net.transport(node("node-b"), group(), peers_to("node-a"));

        net.install_cut(HealId(1), node("node-a"), node("node-b"));
        net.set_now(VirtualInstant::ORIGIN);

        a_to_b.send(RaftNodeId(1), msg(1)); // node-a -> node-b: cut
        b_to_a.send(RaftNodeId(1), msg(2)); // node-b -> node-a: still delivered

        let pending = net.drain_pending();
        assert_eq!(pending.len(), 1, "only the reverse direction survives");
        assert_eq!(pending[0].1.from, node("node-b"));
        assert_eq!(pending[0].1.to, node("node-a"));
        assert_eq!(net.cut_dropped(), 1);
        assert_eq!(net.dropped(), 0, "a cut is not a probabilistic drop");
    }

    /// Requirement 5.5: a symmetric partition blocks every cross-side delivery in
    /// both directions, while same-side delivery is unaffected.
    #[test]
    fn symmetric_partition_blocks_both_directions_across_the_split() {
        let net = net_with(healthy(), 11);
        // Split {a, b} | {c, d}.
        net.install_partition(
            HealId(7),
            [node("node-a"), node("node-b")],
            [node("node-c"), node("node-d")],
        );
        net.set_now(VirtualInstant::ORIGIN);

        let a_to_c = net.transport(node("node-a"), group(), peers_to("node-c"));
        let c_to_a = net.transport(node("node-c"), group(), peers_to("node-a"));
        let a_to_b = net.transport(node("node-a"), group(), peers_to("node-b"));

        a_to_c.send(RaftNodeId(1), msg(1)); // cross-side: cut
        c_to_a.send(RaftNodeId(1), msg(2)); // cross-side (reverse): cut
        a_to_b.send(RaftNodeId(1), msg(3)); // same side: delivered

        let pending = net.drain_pending();
        assert_eq!(pending.len(), 1, "only the same-side message survives");
        assert_eq!(pending[0].1.to, node("node-b"));
        assert_eq!(net.cut_dropped(), 2);
    }

    /// Requirement 5.6: an asymmetric partition blocks only the specified
    /// direction; the reverse direction continues to deliver.
    #[test]
    fn asymmetric_partition_blocks_only_the_specified_direction() {
        let net = net_with(healthy(), 12);
        // Block {a} -> {b} only.
        net.install_asymmetric_partition(HealId(3), [node("node-a")], [node("node-b")]);
        net.set_now(VirtualInstant::ORIGIN);

        let a_to_b = net.transport(node("node-a"), group(), peers_to("node-b"));
        let b_to_a = net.transport(node("node-b"), group(), peers_to("node-a"));

        a_to_b.send(RaftNodeId(1), msg(1)); // blocked direction
        b_to_a.send(RaftNodeId(1), msg(2)); // open direction

        let pending = net.drain_pending();
        assert_eq!(pending.len(), 1, "reverse direction still delivers");
        assert_eq!(pending[0].1.from, node("node-b"));
        assert_eq!(pending[0].1.to, node("node-a"));
        assert_eq!(net.cut_dropped(), 1);
    }

    /// Requirement 5.7: a heal restores delivery for messages sent at or after
    /// the heal; a message sent before the heal keeps its (cut) fate.
    #[test]
    fn heal_restores_delivery_for_messages_sent_at_or_after() {
        let net = net_with(healthy(), 13);
        let tx = net.transport(node("node-a"), group(), peers_to("node-b"));

        net.install_cut(HealId(9), node("node-a"), node("node-b"));
        net.set_now(VirtualInstant::ORIGIN);
        tx.send(RaftNodeId(1), msg(1)); // sent before heal: cut, stays cut

        assert!(net.heal(HealId(9)), "heal removes the installed cut");
        assert!(!net.heal(HealId(9)), "healing again is a no-op");

        tx.send(RaftNodeId(1), msg(2)); // sent after heal: delivered

        let pending = net.drain_pending();
        assert_eq!(pending.len(), 1, "only the post-heal message is delivered");
        assert_eq!(pending[0].1.msg, msg(2));
        assert_eq!(
            net.cut_dropped(),
            1,
            "the pre-heal message kept its cut fate"
        );
    }

    /// Requirement 5.7: a heal restores delivery *only* — other configured faults
    /// (here a certain drop) keep applying to post-heal messages.
    #[test]
    fn heal_leaves_other_configured_faults_in_effect() {
        let faults = FaultIntensities {
            drop_prob: 1.0,
            ..healthy()
        };
        let net = net_with(faults, 14);
        let tx = net.transport(node("node-a"), group(), peers_to("node-b"));

        net.install_cut(HealId(2), node("node-a"), node("node-b"));
        net.set_now(VirtualInstant::ORIGIN);
        tx.send(RaftNodeId(1), msg(1)); // cut (no RNG consumed)

        net.heal(HealId(2));
        tx.send(RaftNodeId(1), msg(2)); // passes cut check, then drop_prob 1.0 drops

        assert_eq!(net.pending_len(), 0, "drop fault still applies post-heal");
        assert_eq!(net.cut_dropped(), 1);
        assert_eq!(net.dropped(), 1);
        assert_eq!(net.delivered(), 0);
    }

    /// Requirement 6.2: a crashed node is cut in both directions until restart,
    /// after which delivery resumes.
    #[test]
    fn crashed_node_is_cut_both_ways_until_restart() {
        let net = net_with(healthy(), 15);
        let a_to_b = net.transport(node("node-a"), group(), peers_to("node-b"));
        let b_to_a = net.transport(node("node-b"), group(), peers_to("node-a"));

        net.crash_node(node("node-b"));
        net.set_now(VirtualInstant::ORIGIN);

        a_to_b.send(RaftNodeId(1), msg(1)); // to crashed node: cut
        b_to_a.send(RaftNodeId(1), msg(2)); // from crashed node: cut
        assert_eq!(net.pending_len(), 0, "no traffic to or from a crashed node");
        assert_eq!(net.cut_dropped(), 2);

        assert!(net.restart_node(&node("node-b")), "node-b was crashed");
        assert!(
            !net.restart_node(&node("node-b")),
            "restarting a live node is a no-op"
        );
        a_to_b.send(RaftNodeId(1), msg(3)); // delivered after restart
        b_to_a.send(RaftNodeId(1), msg(4)); // delivered after restart

        let pending = net.drain_pending();
        assert_eq!(pending.len(), 2, "both directions deliver after restart");
    }

    /// A cut is deterministic and RNG-free: installing a cut that never matches
    /// the sent messages leaves every seed-derived fault decision identical to a
    /// run with no cut installed (the cut check must not consume the RNG).
    #[test]
    fn cut_check_consumes_no_rng() {
        let faults = FaultIntensities {
            drop_prob: 0.3,
            duplicate_prob: 0.3,
            reorder_prob: 0.5,
            max_reorder_nanos: 4_000,
            ..healthy()
        };

        let trace = |install_unmatched_cut: bool| -> Vec<(u64, u64)> {
            let net = net_with(faults, 0xFEED);
            if install_unmatched_cut {
                // Cuts that never match `node-a -> node-b`: a different
                // direction and an unrelated crashed node.
                net.install_cut(HealId(1), node("node-a"), node("node-c"));
                net.crash_node(node("node-z"));
            }
            let tx = net.transport(node("node-a"), group(), peers_to("node-b"));
            net.set_now(VirtualInstant::from_nanos(10));
            for term in 0..64 {
                tx.send(RaftNodeId(1), msg(term));
            }
            net.drain_pending()
                .into_iter()
                .map(|(at, env)| {
                    let term = match env.msg {
                        RaftMessage::RequestVoteReply(r) => r.term,
                        _ => unreachable!(),
                    };
                    (at.as_nanos(), term)
                })
                .collect()
        };

        assert_eq!(
            trace(false),
            trace(true),
            "an unmatched cut must not perturb any seed-derived decision"
        );
    }
}
