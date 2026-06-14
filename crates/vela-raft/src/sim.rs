//! Deterministic simulation harness for `vela-raft`.
//!
//! Consensus is notoriously hard to test with real timers and real networks:
//! elections hinge on randomized timeouts and replication on message timing,
//! reorder, loss, and partitions. This module provides a fully deterministic,
//! single-threaded environment so those behaviours can be driven reproducibly
//! and asserted by `proptest` (Requirement 1.4, 7.2).
//!
//! Three pieces make up the harness:
//!
//! - [`ManualClock`] — a [`Clock`] whose time only moves when the simulation
//!   advances it, and whose seeded PRNG supplies election-timeout randomness so
//!   every run with the same seed is identical.
//! - [`InMemoryTransport`] — a per-node [`Transport`] handle over a shared
//!   message bus that can reorder, delay, drop, duplicate, and partition
//!   traffic on demand.
//! - [`SimCluster`] — `N` [`RaftNode`]s wired to one clock and one bus, with a
//!   [`SimCluster::step`] that advances logical time to the next scheduled event
//!   (a timer firing or a message arriving) and delivers exactly that one event.
//!
//! The harness deliberately avoids any third-party dependency: randomness comes
//! from a small in-house [`SplitMix64`] generator, keeping the consensus crate's
//! dependency surface minimal while remaining bit-for-bit reproducible.
//!
//! Because [`RaftNode::step`] returns its outbound messages in
//! [`RaftOutput::sends`] rather than calling [`Transport`] directly, the harness
//! dispatches those sends through each node's [`InMemoryTransport`] *after* the
//! step returns. The [`Transport`] seam is therefore exercised exactly as the
//! real server crate will exercise it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use vela_log::InMemoryLog;

use crate::{
    Clock, NodeId, RaftInput, RaftMessage, RaftNode, RaftOutput, Role, TimerKind, Transport,
};

/// A tiny, fast, deterministic pseudo-random generator (SplitMix64).
///
/// Used for election-timeout jitter and for network fault decisions. It is not
/// cryptographically secure — it only needs to be reproducible from a seed so
/// simulated runs are identical across machines and invocations.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Create a generator from `seed`.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Return the next 64-bit value and advance the internal state.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Return a value uniformly in `0..n` (returns 0 when `n == 0`).
    pub fn next_below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }

    /// Return a value uniformly in `[0.0, 1.0)` with 53 bits of precision.
    pub fn next_f64(&mut self) -> f64 {
        // 2^53 distinct values; matches f64 mantissa width.
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }
}

/// Apply `[base, 2*base)` random jitter to a duration using `rng`.
///
/// This realizes Raft's randomized election timeout: a node arms an `Election`
/// timer with the base timeout and the clock spreads the actual firing over the
/// `[base, 2*base)` window (Requirement 7.2). With `base = 150 ms` the window is
/// the prescribed 150–300 ms range.
fn jitter(rng: &mut SplitMix64, base: Duration) -> Duration {
    let base_nanos = base.as_nanos().min(u64::MAX as u128) as u64;
    if base_nanos == 0 {
        return base;
    }
    base + Duration::from_nanos(rng.next_below(base_nanos))
}

/// A timer scheduled by a node, awaiting its firing instant.
#[derive(Debug, Clone)]
struct ScheduledTimer {
    /// The node that armed the timer.
    node: NodeId,
    /// Which timer kind this is (election or heartbeat).
    kind: TimerKind,
    /// The logical instant at which it fires.
    fire_at: Instant,
    /// Tie-breaker for deterministic ordering of equal instants.
    seq: u64,
}

/// A [`Clock`] whose time advances only when the simulation moves it.
///
/// `ManualClock` records every armed timer together with the node that armed it
/// (the simulation sets the "active" node around each step), and applies seeded
/// randomness to election timers so elections are reproducible. Heartbeat timers
/// fire at exactly `now + dur`; election timers fire somewhere in
/// `now + [dur, 2*dur)` (Requirement 7.2, 7.6).
///
/// Re-arming a timer of a kind that is already pending for the same node
/// replaces the pending one, matching how a follower resets its election
/// timeout whenever it hears from the leader.
#[derive(Debug)]
pub struct ManualClock {
    /// Current logical instant.
    now: Instant,
    /// Seeded PRNG for election-timeout randomness.
    rng: SplitMix64,
    /// The node currently being stepped; armed timers are attributed to it.
    active: Option<NodeId>,
    /// All pending timers across all nodes.
    timers: Vec<ScheduledTimer>,
    /// Monotonic sequence for deterministic tie-breaking.
    seq: u64,
}

impl ManualClock {
    /// Create a clock seeded with `seed`, starting at the current instant.
    ///
    /// The absolute starting instant is arbitrary; only durations and the
    /// relative ordering of events are meaningful, and both are fully
    /// determined by `seed` and the sequence of simulated operations.
    pub fn new(seed: u64) -> Self {
        Self {
            now: Instant::now(),
            rng: SplitMix64::new(seed),
            active: None,
            timers: Vec::new(),
            seq: 0,
        }
    }

    /// The current logical instant.
    pub fn now(&self) -> Instant {
        self.now
    }

    /// Set (or clear) the node that subsequently armed timers belong to.
    ///
    /// The simulation calls this with `Some(node)` immediately before stepping
    /// that node and `None` afterwards, so [`Clock::arm`] can attribute timers
    /// without changing the trait's signature.
    pub fn set_active(&mut self, node: Option<NodeId>) {
        self.active = node;
    }

    /// Number of timers currently pending.
    pub fn pending_timers(&self) -> usize {
        self.timers.len()
    }

    /// The earliest instant at which any pending timer fires.
    pub fn earliest_fire(&self) -> Option<Instant> {
        // `Instant` has a total order, so `min` only returns `None` when empty.
        self.timers.iter().map(|t| t.fire_at).min()
    }

    /// Remove and return the earliest-firing pending timer, breaking ties by
    /// arming order.
    fn take_earliest(&mut self) -> Option<ScheduledTimer> {
        let idx = self
            .timers
            .iter()
            .enumerate()
            .min_by_key(|(_, t)| (t.fire_at, t.seq))
            .map(|(i, _)| i)?;
        Some(self.timers.swap_remove(idx))
    }

    /// Advance logical time forward to `instant` if it is in the future.
    fn advance_to(&mut self, instant: Instant) {
        if instant > self.now {
            self.now = instant;
        }
    }

    /// Advance logical time by `dur` (explicit time advance).
    pub fn advance(&mut self, dur: Duration) {
        self.now += dur;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.now
    }

    fn arm(&mut self, kind: TimerKind, dur: Duration) {
        let node = self
            .active
            .expect("ManualClock::arm called with no active node; call set_active first");
        // Election timeouts are randomized; heartbeats are exact (R7.2, R7.6).
        let delay = match kind {
            TimerKind::Election => jitter(&mut self.rng, dur),
            TimerKind::Heartbeat => dur,
        };
        // Re-arming a pending timer of the same kind for this node resets it.
        self.timers.retain(|t| !(t.node == node && t.kind == kind));
        let seq = self.seq;
        self.seq += 1;
        self.timers.push(ScheduledTimer {
            node,
            kind,
            fire_at: self.now + delay,
            seq,
        });
    }
}

/// A message in flight on the [`InMemoryTransport`] bus.
#[derive(Debug, Clone)]
struct InFlight {
    /// Sending node.
    from: NodeId,
    /// Receiving node.
    to: NodeId,
    /// The message payload.
    msg: RaftMessage,
    /// Logical instant at which it is delivered.
    deliver_at: Instant,
    /// Tie-breaker for deterministic ordering of equal instants.
    seq: u64,
}

/// The shared message bus behind every [`InMemoryTransport`] handle.
///
/// Holds the in-flight queue, the fault configuration, and a seeded PRNG for
/// fault decisions. Each [`InMemoryTransport`] is a thin per-node handle that
/// forwards to one `Bus` so the whole cluster shares a single network.
#[derive(Debug)]
struct Bus {
    /// Messages awaiting delivery.
    queue: Vec<InFlight>,
    /// Current logical instant, mirrored from the clock at dispatch time.
    now: Instant,
    /// Base one-way latency added to every message.
    latency: Duration,
    /// When set, an extra random delay up to `reorder_jitter` is added per
    /// message, which can reorder deliveries relative to send order.
    reorder: bool,
    /// Upper bound on the extra random delay applied when `reorder` is set.
    reorder_jitter: Duration,
    /// Probability in `[0.0, 1.0]` that any given message is dropped.
    drop_probability: f64,
    /// Probability in `[0.0, 1.0]` that a delivered message is also duplicated.
    duplicate_probability: f64,
    /// Directed links that are severed; `(from, to)` present means blocked.
    partitions: HashSet<(NodeId, NodeId)>,
    /// Seeded PRNG for drop/duplicate/reorder decisions.
    rng: SplitMix64,
    /// Monotonic sequence for deterministic tie-breaking.
    seq: u64,
    /// Count of messages dropped (by drop probability or partition).
    dropped: u64,
    /// Count of messages duplicated.
    duplicated: u64,
}

impl Bus {
    /// Create a bus seeded with `seed`, anchored to logical instant `now`.
    fn new(seed: u64, now: Instant) -> Self {
        Self {
            queue: Vec::new(),
            now,
            latency: Duration::from_millis(1),
            reorder: false,
            reorder_jitter: Duration::from_millis(5),
            drop_probability: 0.0,
            duplicate_probability: 0.0,
            partitions: HashSet::new(),
            rng: SplitMix64::new(seed),
            seq: 0,
            dropped: 0,
            duplicated: 0,
        }
    }

    /// Whether the directed link `from -> to` is currently partitioned.
    fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.partitions.contains(&(from, to))
    }

    /// Enqueue one message copy with the configured latency and reorder jitter.
    fn push_one(&mut self, from: NodeId, to: NodeId, msg: RaftMessage) {
        let mut delay = self.latency;
        if self.reorder {
            let bound = self.reorder_jitter.as_nanos().min(u64::MAX as u128) as u64;
            if bound > 0 {
                delay += Duration::from_nanos(self.rng.next_below(bound));
            }
        }
        let seq = self.seq;
        self.seq += 1;
        self.queue.push(InFlight {
            from,
            to,
            msg,
            deliver_at: self.now + delay,
            seq,
        });
    }

    /// Accept a message from a node, applying drop, partition, and duplication
    /// faults before queueing it for later delivery.
    fn enqueue(&mut self, from: NodeId, to: NodeId, msg: RaftMessage) {
        if self.is_partitioned(from, to) {
            self.dropped += 1;
            return;
        }
        if self.drop_probability > 0.0 && self.rng.next_f64() < self.drop_probability {
            self.dropped += 1;
            return;
        }
        self.push_one(from, to, msg.clone());
        if self.duplicate_probability > 0.0 && self.rng.next_f64() < self.duplicate_probability {
            self.duplicated += 1;
            self.push_one(from, to, msg);
        }
    }

    /// The earliest instant at which any queued message is delivered.
    fn earliest_deliver(&self) -> Option<Instant> {
        self.queue.iter().map(|m| m.deliver_at).min()
    }

    /// Remove and return the earliest-deliverable message, breaking ties by
    /// enqueue order.
    fn take_earliest(&mut self) -> Option<InFlight> {
        let idx = self
            .queue
            .iter()
            .enumerate()
            .min_by_key(|(_, m)| (m.deliver_at, m.seq))
            .map(|(i, _)| i)?;
        Some(self.queue.swap_remove(idx))
    }
}

/// A per-node [`Transport`] handle over a shared [`Bus`].
///
/// Each node in a [`SimCluster`] owns one of these, bound to its own
/// [`NodeId`], because [`Transport::send`] does not carry the sender's identity.
/// `send` forwards `(from, to, msg)` to the shared bus, where fault injection is
/// applied. Interior mutability ([`RefCell`]) is used because `send` takes
/// `&self`.
#[derive(Debug, Clone)]
pub struct InMemoryTransport {
    /// Identity of the node this handle sends on behalf of.
    from: NodeId,
    /// The shared message bus.
    bus: Rc<RefCell<Bus>>,
}

impl Transport for InMemoryTransport {
    fn send(&self, to: NodeId, msg: RaftMessage) {
        self.bus.borrow_mut().enqueue(self.from, to, msg);
    }
}

/// The result of a single [`SimCluster::step`].
#[derive(Debug)]
pub enum StepOutcome {
    /// No timer was pending and no message was in flight; the simulation is
    /// quiescent.
    Idle,
    /// A timer fired and was delivered to `node`, producing `output`.
    Timer {
        /// The node whose timer fired.
        node: NodeId,
        /// Which timer fired.
        kind: TimerKind,
        /// The effects the node produced in response.
        output: RaftOutput,
    },
    /// A message was delivered from `from` to `to`, producing `output`.
    Message {
        /// The sending node.
        from: NodeId,
        /// The receiving node.
        to: NodeId,
        /// The effects the receiving node produced in response.
        output: RaftOutput,
    },
}

/// `N` Raft replicas wired to one [`ManualClock`] and one shared bus.
///
/// A `SimCluster` models a single partition's Raft group entirely in memory and
/// in one thread. [`SimCluster::step`] is a discrete-event step: it finds the
/// next scheduled event — the earliest timer firing or the earliest message
/// arrival — advances logical time to it, and delivers exactly that one event to
/// the relevant node. Outbound messages the node produces are injected back onto
/// the bus (subject to the configured faults) for delivery on later steps.
///
/// Network behaviour is controlled with [`SimCluster::set_latency`],
/// [`SimCluster::set_reorder`], [`SimCluster::set_drop_probability`],
/// [`SimCluster::set_duplicate_probability`], and
/// [`SimCluster::partition`]/[`SimCluster::heal`].
pub struct SimCluster {
    /// The replicas, indexed in `NodeId(0)..NodeId(n)` order.
    nodes: Vec<RaftNode<InMemoryLog>>,
    /// Per-node transport handles, parallel to `nodes`.
    endpoints: Vec<InMemoryTransport>,
    /// The shared, manually advanced clock.
    clock: ManualClock,
    /// The shared message bus.
    bus: Rc<RefCell<Bus>>,
    /// Entries that became committed at each node, in delivery order.
    committed: HashMap<NodeId, Vec<vela_log::LogEntry>>,
}

impl SimCluster {
    /// Build a cluster of `node_count` replicas seeded with `seed`.
    ///
    /// Nodes are given ids `NodeId(0)..NodeId(node_count)`; each node's peer set
    /// is every other node. Every node starts as a follower with a fresh
    /// in-memory log. The clock and bus derive their PRNGs from `seed` (the bus
    /// uses a decorrelated derivative) so the whole run is reproducible.
    ///
    /// # Panics
    /// Panics if `node_count` is zero.
    pub fn new(node_count: u64, seed: u64) -> Self {
        assert!(node_count > 0, "a cluster needs at least one node");

        let clock = ManualClock::new(seed);
        // Decorrelate the bus PRNG from the clock PRNG so fault decisions and
        // election jitter do not march in lock-step.
        let bus = Rc::new(RefCell::new(Bus::new(
            seed.wrapping_add(0xD1B5_4A32_D192_ED03),
            clock.now(),
        )));

        let ids: Vec<NodeId> = (0..node_count).map(NodeId).collect();
        let mut nodes = Vec::with_capacity(ids.len());
        let mut endpoints = Vec::with_capacity(ids.len());
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            nodes.push(RaftNode::new(id, peers, InMemoryLog::new()));
            endpoints.push(InMemoryTransport {
                from: id,
                bus: Rc::clone(&bus),
            });
        }

        Self {
            nodes,
            endpoints,
            clock,
            bus,
            committed: HashMap::new(),
        }
    }

    /// Number of replicas in the cluster.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the cluster has no replicas (never true via [`SimCluster::new`]).
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The current logical instant.
    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    /// Shared, read-only access to a replica by id.
    pub fn node(&self, id: NodeId) -> Option<&RaftNode<InMemoryLog>> {
        self.index_of(id).map(|i| &self.nodes[i])
    }

    /// The role currently held by a replica, if it exists.
    pub fn role(&self, id: NodeId) -> Option<Role> {
        self.node(id).map(RaftNode::role)
    }

    /// The id of a replica that currently believes itself leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|n| n.role() == Role::Leader)
            .map(RaftNode::id)
    }

    /// Entries that have become committed at `id` so far, in delivery order.
    pub fn committed(&self, id: NodeId) -> &[vela_log::LogEntry] {
        self.committed.get(&id).map_or(&[], Vec::as_slice)
    }

    /// Total messages dropped by the bus (drop probability or partition).
    pub fn dropped_count(&self) -> u64 {
        self.bus.borrow().dropped
    }

    /// Total messages duplicated by the bus.
    pub fn duplicated_count(&self) -> u64 {
        self.bus.borrow().duplicated
    }

    /// Number of messages still in flight on the bus.
    pub fn in_flight(&self) -> usize {
        self.bus.borrow().queue.len()
    }

    // ---- network fault controls -------------------------------------------

    /// Set the base one-way message latency.
    pub fn set_latency(&mut self, latency: Duration) {
        self.bus.borrow_mut().latency = latency;
    }

    /// Enable or disable random per-message reorder jitter, bounding the extra
    /// random delay by `max_jitter` when enabled.
    pub fn set_reorder(&mut self, enabled: bool, max_jitter: Duration) {
        let mut bus = self.bus.borrow_mut();
        bus.reorder = enabled;
        bus.reorder_jitter = max_jitter;
    }

    /// Set the probability in `[0.0, 1.0]` that a sent message is dropped.
    pub fn set_drop_probability(&mut self, p: f64) {
        self.bus.borrow_mut().drop_probability = p.clamp(0.0, 1.0);
    }

    /// Set the probability in `[0.0, 1.0]` that a sent message is duplicated.
    pub fn set_duplicate_probability(&mut self, p: f64) {
        self.bus.borrow_mut().duplicate_probability = p.clamp(0.0, 1.0);
    }

    /// Sever communication between `a` and `b` in both directions.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        let mut bus = self.bus.borrow_mut();
        bus.partitions.insert((a, b));
        bus.partitions.insert((b, a));
    }

    /// Restore communication between `a` and `b` in both directions.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        let mut bus = self.bus.borrow_mut();
        bus.partitions.remove(&(a, b));
        bus.partitions.remove(&(b, a));
    }

    // ---- manual controls (useful before election logic exists) ------------

    /// Arm a timer on `node` directly, as if the node had armed it during a
    /// step. Election timers receive the usual randomized jitter.
    ///
    /// This is primarily a test affordance for the period before the election
    /// state machine drives its own timers.
    pub fn arm(&mut self, node: NodeId, kind: TimerKind, dur: Duration) {
        self.clock.set_active(Some(node));
        self.clock.arm(kind, dur);
        self.clock.set_active(None);
    }

    /// Inject a message from `from` to `to` onto the bus, subject to the
    /// configured faults.
    pub fn send(&mut self, from: NodeId, to: NodeId, msg: RaftMessage) {
        if let Some(idx) = self.index_of(from) {
            self.bus.borrow_mut().now = self.clock.now();
            self.endpoints[idx].send(to, msg);
        }
    }

    /// Deliver a client proposal to `node` and dispatch any resulting effects.
    pub fn propose(&mut self, node: NodeId, payload: crate::EntryPayload) -> Option<RaftOutput> {
        let idx = self.index_of(node)?;
        Some(self.drive(idx, node, RaftInput::Propose(payload)))
    }

    /// Advance logical time by `dur` without delivering any event.
    pub fn advance(&mut self, dur: Duration) {
        self.clock.advance(dur);
        self.bus.borrow_mut().now = self.clock.now();
    }

    // ---- the core discrete-event step --------------------------------------

    /// Advance to and deliver the single next scheduled event.
    ///
    /// Compares the earliest pending timer against the earliest in-flight
    /// message, advances logical time to whichever comes first (timers win
    /// exact ties for determinism), delivers it to the owning node, and injects
    /// that node's outbound messages back onto the bus. Returns [`StepOutcome`]
    /// describing what happened, or [`StepOutcome::Idle`] when nothing is
    /// scheduled.
    pub fn step(&mut self) -> StepOutcome {
        let next_timer = self.clock.earliest_fire();
        let next_msg = self.bus.borrow().earliest_deliver();

        let fire_timer = match (next_timer, next_msg) {
            (None, None) => return StepOutcome::Idle,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            // Timers win exact ties so ordering is fully deterministic.
            (Some(t), Some(m)) => t <= m,
        };

        if fire_timer {
            let timer = self
                .clock
                .take_earliest()
                .expect("earliest_fire reported a pending timer");
            self.clock.advance_to(timer.fire_at);
            let idx = self
                .index_of(timer.node)
                .expect("timer armed by a known node");
            let output = self.drive(idx, timer.node, RaftInput::Tick(timer.kind));
            StepOutcome::Timer {
                node: timer.node,
                kind: timer.kind,
                output,
            }
        } else {
            let msg = self
                .bus
                .borrow_mut()
                .take_earliest()
                .expect("earliest_deliver reported an in-flight message");
            self.clock.advance_to(msg.deliver_at);
            let idx = self
                .index_of(msg.to)
                .expect("message addressed to a known node");
            let output = self.drive(idx, msg.to, RaftInput::Message(msg.msg.clone()));
            StepOutcome::Message {
                from: msg.from,
                to: msg.to,
                output,
            }
        }
    }

    /// Run the simulation until it is quiescent or `max_steps` is reached.
    ///
    /// Returns the number of non-idle steps performed. A bound is required
    /// because faults like duplication can keep a cluster busy; callers pick a
    /// budget appropriate to their scenario.
    pub fn run_until_idle(&mut self, max_steps: usize) -> usize {
        let mut performed = 0;
        for _ in 0..max_steps {
            match self.step() {
                StepOutcome::Idle => break,
                _ => performed += 1,
            }
        }
        performed
    }

    // ---- internals ---------------------------------------------------------

    /// Position of `id` within `nodes`/`endpoints`.
    fn index_of(&self, id: NodeId) -> Option<usize> {
        self.nodes.iter().position(|n| n.id() == id)
    }

    /// Step node `idx` (identity `node`) with `input`, then dispatch its
    /// outbound messages and record any newly committed entries.
    fn drive(&mut self, idx: usize, node: NodeId, input: RaftInput) -> RaftOutput {
        // Keep the bus clock aligned so freshly sent messages are scheduled
        // relative to the current event time.
        self.bus.borrow_mut().now = self.clock.now();

        self.clock.set_active(Some(node));
        // `self.nodes` and `self.clock` are disjoint fields, so both may be
        // borrowed mutably at once.
        let output = self.nodes[idx].step(input, &mut self.clock);
        self.clock.set_active(None);

        for (to, msg) in &output.sends {
            self.endpoints[idx].send(*to, msg.clone());
        }
        if !output.committed.is_empty() {
            self.committed
                .entry(node)
                .or_default()
                .extend(output.committed.iter().cloned());
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_cluster_starts_with_all_followers() {
        let sim = SimCluster::new(3, 1);
        assert_eq!(sim.len(), 3);
        assert!(!sim.is_empty());
        for i in 0..3 {
            assert_eq!(sim.role(NodeId(i)), Some(Role::Follower));
        }
        assert_eq!(sim.leader(), None);
    }

    #[test]
    fn step_on_quiescent_cluster_is_idle() {
        let mut sim = SimCluster::new(3, 7);
        assert!(matches!(sim.step(), StepOutcome::Idle));
    }

    #[test]
    fn armed_election_timer_fires_within_the_randomized_window() {
        let mut sim = SimCluster::new(3, 42);
        let start = sim.now();
        sim.arm(NodeId(0), TimerKind::Election, Duration::from_millis(150));
        assert_eq!(sim.in_flight(), 0);

        match sim.step() {
            StepOutcome::Timer { node, kind, .. } => {
                assert_eq!(node, NodeId(0));
                assert_eq!(kind, TimerKind::Election);
            }
            other => panic!("expected a timer firing, got {other:?}"),
        }

        // Election jitter must land in [150, 300) ms (Requirement 7.2).
        let elapsed = sim.now().duration_since(start);
        assert!(
            elapsed >= Duration::from_millis(150) && elapsed < Duration::from_millis(300),
            "election timeout {elapsed:?} outside [150ms, 300ms)"
        );
        // Firing the timeout drove the node into an election: it is now a
        // candidate that has broadcast RequestVote to both peers and re-armed
        // its own election timer (Requirements 7.2, 7.3).
        assert_eq!(sim.role(NodeId(0)), Some(Role::Candidate));
        assert_eq!(sim.in_flight(), 2);
    }

    #[test]
    fn heartbeat_timer_fires_at_exact_interval() {
        let mut sim = SimCluster::new(3, 5);
        let start = sim.now();
        sim.arm(NodeId(1), TimerKind::Heartbeat, Duration::from_millis(50));
        match sim.step() {
            StepOutcome::Timer { node, kind, .. } => {
                assert_eq!(node, NodeId(1));
                assert_eq!(kind, TimerKind::Heartbeat);
            }
            other => panic!("expected a heartbeat firing, got {other:?}"),
        }
        assert_eq!(sim.now().duration_since(start), Duration::from_millis(50));
    }

    #[test]
    fn re_arming_resets_the_pending_timer() {
        let mut sim = SimCluster::new(3, 9);
        sim.arm(NodeId(0), TimerKind::Heartbeat, Duration::from_millis(50));
        sim.arm(NodeId(0), TimerKind::Heartbeat, Duration::from_millis(50));
        // The second arm replaced the first, leaving exactly one pending timer.
        assert_eq!(sim.clock.pending_timers(), 1);
    }

    #[test]
    fn injected_message_is_delivered_after_latency() {
        let mut sim = SimCluster::new(3, 3);
        let start = sim.now();
        sim.set_latency(Duration::from_millis(2));
        let msg = RaftMessage::AppendEntriesReply(crate::AppendEntriesReply {
            from: NodeId(0),
            term: 1,
            success: true,
            conflict_index: None,
            match_index: None,
        });
        sim.send(NodeId(0), NodeId(1), msg);
        assert_eq!(sim.in_flight(), 1);

        match sim.step() {
            StepOutcome::Message { from, to, .. } => {
                assert_eq!(from, NodeId(0));
                assert_eq!(to, NodeId(1));
            }
            other => panic!("expected a message delivery, got {other:?}"),
        }
        assert_eq!(sim.now().duration_since(start), Duration::from_millis(2));
        assert_eq!(sim.in_flight(), 0);
    }

    #[test]
    fn dropped_messages_are_never_delivered() {
        let mut sim = SimCluster::new(3, 11);
        sim.set_drop_probability(1.0);
        sim.send(
            NodeId(0),
            NodeId(1),
            RaftMessage::RequestVoteReply(crate::RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
        );
        assert_eq!(sim.in_flight(), 0);
        assert_eq!(sim.dropped_count(), 1);
        assert!(matches!(sim.step(), StepOutcome::Idle));
    }

    #[test]
    fn partitioned_links_block_delivery_both_ways() {
        let mut sim = SimCluster::new(3, 13);
        sim.partition(NodeId(0), NodeId(1));
        let vote = RaftMessage::RequestVoteReply(crate::RequestVoteReply {
            term: 1,
            vote_granted: false,
        });
        sim.send(NodeId(0), NodeId(1), vote.clone());
        sim.send(NodeId(1), NodeId(0), vote.clone());
        assert_eq!(sim.in_flight(), 0);
        assert_eq!(sim.dropped_count(), 2);

        // Healing the link restores delivery.
        sim.heal(NodeId(0), NodeId(1));
        sim.send(NodeId(0), NodeId(1), vote);
        assert_eq!(sim.in_flight(), 1);
    }

    #[test]
    fn duplication_enqueues_a_second_copy() {
        let mut sim = SimCluster::new(3, 17);
        sim.set_duplicate_probability(1.0);
        sim.send(
            NodeId(0),
            NodeId(1),
            RaftMessage::AppendEntriesReply(crate::AppendEntriesReply {
                from: NodeId(0),
                term: 1,
                success: true,
                conflict_index: None,
                match_index: None,
            }),
        );
        assert_eq!(sim.in_flight(), 2);
        assert_eq!(sim.duplicated_count(), 1);
    }

    #[test]
    fn same_seed_yields_identical_election_jitter() {
        let timeout = |seed: u64| {
            let mut sim = SimCluster::new(3, seed);
            let start = sim.now();
            sim.arm(NodeId(0), TimerKind::Election, Duration::from_millis(150));
            sim.step();
            sim.now().duration_since(start)
        };
        // Identical seeds are reproducible; different seeds (very likely) differ.
        assert_eq!(timeout(42), timeout(42));
        assert_ne!(timeout(1), timeout(2));
    }

    #[test]
    fn run_until_idle_respects_its_step_budget() {
        let mut sim = SimCluster::new(3, 23);
        sim.arm(NodeId(0), TimerKind::Election, Duration::from_millis(150));
        sim.arm(NodeId(1), TimerKind::Election, Duration::from_millis(150));
        // Once elections start, a healthy cluster keeps itself busy with vote
        // RPCs and heartbeats, so it never quiesces; run_until_idle therefore
        // stops at its step budget rather than running forever.
        assert_eq!(sim.run_until_idle(16), 16);
    }
}
