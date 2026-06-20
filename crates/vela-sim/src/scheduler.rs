//! Discrete-event scheduler: the global event queue and the step loop.
//!
//! The scheduler is the heart of the harness. It generalizes the single-group
//! `vela_raft::sim::SimCluster::step` model to the multi-node, multi-group
//! cluster: a single ordered collection of pending [`Event`]s plus the current
//! logical instant, advanced one event at a time.
//!
//! Three properties of this module are what make an entire [`Simulation_Run`] a
//! pure function of its seed:
//!
//! 1. **Logical time only.** [`VirtualInstant`] is a plain `u64` of nanoseconds
//!    from an arbitrary origin. It is never derived from, compared against, or
//!    seeded by the wall clock (unlike `ManualClock`, which anchors its origin
//!    at `Instant::now()`). Time advances only when [`Scheduler::step`] moves it
//!    forward to the next event (Requirement 4.1).
//! 2. **Forward-only progress.** Each step advances `now` to the earliest
//!    pending event and never backward, so no event is ever processed before an
//!    earlier-scheduled one (Requirement 4.4).
//! 3. **Deterministic tie-breaking.** Events scheduled at the same instant are
//!    ordered by a seed-derived [`TieBreak`] key drawn from the `tiebreak` RNG
//!    stream, plus a monotonic sequence number that makes the order a strict
//!    total order even when two keys collide (Requirements 1.5, 1.1).
//!
//! Dispatch — feeding an event to the right replica, routing its sends, applying
//! its commits — is *not* performed here; it belongs to the `SimRuntime` step
//! loop (a later task). This module only owns the queue, the clock, and the
//! budget that bounds the run (Requirement 4.6). Keeping dispatch out keeps the
//! scheduler small and independently testable.
//!
//! [`Simulation_Run`]: crate

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use vela_core::{GroupKey, NodeId};
use vela_raft::TimerKind;

use crate::network::Envelope;
use crate::rng::SplitMix64;
use crate::scenario::Budget;

/// A logical instant, in nanoseconds from an arbitrary origin.
///
/// This is a pure logical counter, **never** read from or compared against the
/// wall clock (Requirement 4.1). The origin ([`VirtualInstant::ORIGIN`]) is
/// simply `0`; only differences and relative ordering carry meaning. Using a
/// `u64` rather than a `std::time::Instant` removes even the residual real-time
/// touch that anchoring an origin at `Instant::now()` would introduce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct VirtualInstant(u64);

impl VirtualInstant {
    /// The origin of logical time, `0` nanoseconds.
    pub const ORIGIN: VirtualInstant = VirtualInstant(0);

    /// Construct an instant `nanos` logical nanoseconds after the origin.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// The number of logical nanoseconds since the origin.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// The instant `dur` after `self`, saturating at [`u64::MAX`] rather than
    /// overflowing. Saturation keeps arithmetic total and panic-free; the budget
    /// ends any run long before the logical clock could approach `u64::MAX`.
    #[must_use]
    pub const fn saturating_add(self, dur: VirtualDuration) -> Self {
        Self(self.0.saturating_add(dur.0))
    }

    /// The non-negative [`VirtualDuration`] from `earlier` to `self`, saturating
    /// at zero if `earlier` is actually later (which the forward-only step loop
    /// never produces).
    #[must_use]
    pub const fn duration_since(self, earlier: VirtualInstant) -> VirtualDuration {
        VirtualDuration(self.0.saturating_sub(earlier.0))
    }
}

/// A span of logical time, in nanoseconds.
///
/// The same unit as [`VirtualInstant`]: a logical count, never a wall-clock
/// span. Used for timer delays, network latencies, and budget arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct VirtualDuration(u64);

impl VirtualDuration {
    /// The zero duration.
    pub const ZERO: VirtualDuration = VirtualDuration(0);

    /// A duration of `nanos` logical nanoseconds.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// A duration of `millis` logical milliseconds.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    /// A duration of `secs` logical seconds.
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    /// The number of logical nanoseconds in this duration.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// The sum of two durations, saturating at [`u64::MAX`].
    #[must_use]
    pub const fn saturating_add(self, other: VirtualDuration) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

/// A client operation to issue against the cluster.
///
/// Carries only the operation's 0-based position within the generated
/// [`Workload`](crate::workload::Workload); the heavyweight arguments live in
/// the workload itself. When dispatched, the runtime resolves the concrete
/// [`ClientOperation`](crate::workload::ClientOperation) via
/// [`Workload::op`](crate::workload::Workload::op) and issues it against the
/// routed leader (see
/// [`SimRuntime::run`](crate::runtime::SimRuntime::run)).
#[derive(Debug, Clone)]
pub struct ClientOp {
    /// This operation's 0-based position within the generated workload.
    pub seq: u64,
}

/// A fault to apply to the cluster at a scheduled instant.
///
/// The run orchestration ([`SimRuntime::run`](crate::runtime::SimRuntime::run))
/// derives a deterministic [`Fault_Schedule`](crate) from the `faults` RNG
/// stream and schedules each fault as an [`Event::FaultApply`]. When dispatched,
/// each variant routes to a cluster / network API:
///
/// - [`Crash`](Self::Crash) / [`Restart`](Self::Restart) →
///   [`SimulatedCluster::crash_nodes`](crate::cluster::SimulatedCluster::crash_nodes)
///   / [`restart_nodes`](crate::cluster::SimulatedCluster::restart_nodes)
///   (Requirement 6.1, 6.3). A `Restart` is the "heal" of a crash.
/// - [`Partition`](Self::Partition) →
///   [`SimNetwork::install_partition`](crate::network::SimNetwork::install_partition);
///   the paired [`Event::FaultHeal`] carrying the same [`HealId`] lifts it
///   (Requirement 5.5, 5.7).
///
/// The schedule is kept deliberately conservative — only ever a strict minority
/// is crashed or isolated at once, and it is restarted / healed before the next
/// fault — so a majority of every group always survives (Requirement 6.5, 6.6).
#[derive(Debug, Clone)]
pub enum Fault {
    /// Crash the nodes at these indices: drop their volatile consensus state and
    /// cut them off the network until restarted (Requirement 6.1, 6.2).
    Crash {
        /// The node indices to crash.
        nodes: Vec<usize>,
    },
    /// Restart the crashed nodes at these indices through the durable-recovery
    /// path, rejoining them to the cluster (Requirement 6.3, 6.4).
    Restart {
        /// The node indices to restart.
        nodes: Vec<usize>,
    },
    /// Install a network partition splitting `side_a` from `side_b`, lifted by a
    /// later [`Event::FaultHeal`] carrying `id` (Requirement 5.5, 5.7).
    Partition {
        /// The id a later [`Event::FaultHeal`] lifts this partition under.
        id: HealId,
        /// One side of the split.
        side_a: Vec<NodeId>,
        /// The other side of the split.
        side_b: Vec<NodeId>,
    },
}

/// Identifies a transient network fault so a later [`Event::FaultHeal`] can
/// remove it.
///
/// The [`Sim_Network`](crate::network) keys each installed partition / directed
/// cut set by a `HealId`; [`crate::network::SimNetwork::heal`] lifts the cut
/// under a given id (Requirement 5.7). The fault schedule that assigns these ids
/// and pairs each apply with its heal lands in a later task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HealId(pub u64);

/// A single discrete simulation occurrence with a logical occurrence time.
///
/// Variants mirror the design's event taxonomy. The scheduler treats every
/// variant uniformly — it only orders and hands them out; the runtime's dispatch
/// interprets each kind.
#[derive(Debug, Clone)]
pub enum Event {
    /// An armed timer of `kind` fired for `group` on `node`.
    ///
    /// `generation` supersedes re-armed timers: when a replica re-arms a timer
    /// of the same `(node, group, kind)`, the generation is bumped, and a fired
    /// `TimerFire` whose generation is stale is dropped by the clock. This gives
    /// election-timer reset without an explicit cancellation path (mirroring
    /// `TimerClock::is_current`). The clock and its `generation` semantics land
    /// in a later task; this variant only defines the shape it will enqueue.
    TimerFire {
        /// The node whose replica armed the timer.
        node: NodeId,
        /// The Raft group the timer belongs to.
        group: GroupKey,
        /// Whether this is an election or heartbeat timer.
        kind: TimerKind,
        /// The arming generation; a stale generation is dropped when it fires.
        generation: u64,
    },
    /// A routed Raft message is delivered to its recipient.
    MessageDeliver(Envelope),
    /// A client operation is issued against the cluster.
    ClientOp(ClientOp),
    /// A fault is applied (crash, partition, skew, storage-fault arm).
    FaultApply(Fault),
    /// A previously applied transient network fault is healed.
    FaultHeal(HealId),
}

/// The seed-derived ordering key that breaks ties between events scheduled at
/// the same [`VirtualInstant`].
///
/// `key` is drawn from the `tiebreak` RNG stream when the event is scheduled, so
/// the relative order of simultaneous events is a deterministic function of the
/// run seed (Requirement 1.5). `seq` is a monotonic counter assigned at schedule
/// time; it makes the order a strict total order even in the (vanishingly rare)
/// case of two equal `key`s, so the heap order is never ambiguous (Requirement
/// 1.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieBreak {
    key: u64,
    seq: u64,
}

impl Ord for TieBreak {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key).then(self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for TieBreak {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An event paired with the instant it is due and its tie-break key.
///
/// Ordering compares `(at, tie_break)` only — never the [`Event`] payload, which
/// need not be orderable. Because `tie_break.seq` is unique per scheduled event,
/// no two distinct `Scheduled` values ever compare equal, so the queue imposes a
/// strict, deterministic total order on all pending events.
#[derive(Debug, Clone)]
pub struct Scheduled {
    /// The logical instant at which the event is due.
    pub at: VirtualInstant,
    /// The seed-derived tie-break key for events sharing `at`.
    pub tie_break: TieBreak,
    /// The event to process.
    pub event: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.tie_break == other.tie_break
    }
}

impl Eq for Scheduled {}

impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at
            .cmp(&other.at)
            .then_with(|| self.tie_break.cmp(&other.tie_break))
    }
}

impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Why the step loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    /// No events remain in the queue. The cluster is quiescent — for a healthy
    /// cluster this rarely happens, since elections and heartbeats keep
    /// re-arming timers, but it is the natural end when nothing is left to do.
    Quiescent,
    /// The configured maximum number of events ([`Budget::max_events`]) has been
    /// processed (Requirement 4.6).
    EventBudget,
    /// Logical time has reached the configured maximum
    /// ([`Budget::max_virtual_nanos`]) (Requirement 4.6).
    VirtualTimeBudget,
}

/// The result of one [`Scheduler::step`].
#[derive(Debug, Clone)]
pub enum Step {
    /// An event was popped and `now` advanced to its instant; the caller should
    /// process it (which may schedule follow-on events) before stepping again.
    Event(Scheduled),
    /// The run is over for the given reason; no event was popped.
    Done(EndReason),
}

/// The discrete-event scheduler: a min-ordered queue of pending events, the
/// current logical instant, and the budget that bounds the run.
///
/// # Usage
///
/// The owner (the `SimRuntime`) seeds initial events with [`schedule`], then
/// drives the run by looping [`step`]:
///
/// ```
/// use vela_sim::rng::SeedStreams;
/// use vela_sim::scenario::Budget;
/// use vela_sim::scheduler::{ClientOp, Event, Scheduler, Step, VirtualInstant};
///
/// let streams = SeedStreams::new(0xC0FFEE);
/// let budget = Budget { max_events: 2, max_virtual_nanos: u64::MAX };
/// let mut sched = Scheduler::new(budget, streams.tiebreak);
///
/// sched.schedule(VirtualInstant::from_nanos(10), Event::ClientOp(ClientOp { seq: 0 }));
/// sched.schedule(VirtualInstant::from_nanos(5), Event::ClientOp(ClientOp { seq: 1 }));
///
/// // Earliest-first: the event at t=5 is delivered before the one at t=10.
/// match sched.step() {
///     Step::Event(s) => assert_eq!(s.at, VirtualInstant::from_nanos(5)),
///     Step::Done(_) => unreachable!(),
/// }
/// ```
///
/// [`schedule`]: Scheduler::schedule
/// [`step`]: Scheduler::step
#[derive(Debug)]
pub struct Scheduler {
    /// The current logical instant; advances forward only.
    now: VirtualInstant,
    /// Pending events, min-ordered by `(at, tie_break)` via [`Reverse`].
    queue: BinaryHeap<Reverse<Scheduled>>,
    /// Monotonic sequence assigned to each scheduled event for a total order.
    next_seq: u64,
    /// Count of events handed out by [`Scheduler::step`].
    events_processed: u64,
    /// The bound that ends the run.
    budget: Budget,
    /// Seed-derived stream supplying tie-break keys for equal instants.
    tiebreak: SplitMix64,
}

impl Scheduler {
    /// Create an empty scheduler bounded by `budget`, drawing tie-break keys
    /// from `tiebreak` (the run's `tiebreak` RNG stream).
    ///
    /// The clock starts at [`VirtualInstant::ORIGIN`] and no events are pending.
    #[must_use]
    pub fn new(budget: Budget, tiebreak: SplitMix64) -> Self {
        Self {
            now: VirtualInstant::ORIGIN,
            queue: BinaryHeap::new(),
            next_seq: 0,
            events_processed: 0,
            budget,
            tiebreak,
        }
    }

    /// The current logical instant.
    #[must_use]
    pub fn now(&self) -> VirtualInstant {
        self.now
    }

    /// The number of events handed out so far by [`step`](Self::step).
    #[must_use]
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// The number of events still pending in the queue.
    #[must_use]
    pub fn pending_events(&self) -> usize {
        self.queue.len()
    }

    /// Whether the queue is empty.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.queue.is_empty()
    }

    /// The instant of the earliest pending event, if any.
    #[must_use]
    pub fn next_instant(&self) -> Option<VirtualInstant> {
        self.queue.peek().map(|Reverse(s)| s.at)
    }

    /// Schedule `event` to occur at `at`.
    ///
    /// `at` is clamped forward to the current instant: an event can never be
    /// inserted earlier than `now`, which preserves the forward-only invariant
    /// of [`step`](Self::step) (Requirement 4.4). In normal use callers schedule
    /// relative to `now` (`now + delay`), so the clamp is a guard rail rather
    /// than something hit in practice.
    ///
    /// The tie-break key is drawn from the `tiebreak` RNG stream here, so the
    /// order of events sharing an instant is fixed by the seed and the
    /// (deterministic) order in which they were scheduled (Requirement 1.5).
    pub fn schedule(&mut self, at: VirtualInstant, event: Event) {
        let at = if at < self.now { self.now } else { at };
        let tie_break = TieBreak {
            key: self.tiebreak.next_u64(),
            seq: self.next_seq,
        };
        self.next_seq += 1;
        self.queue.push(Reverse(Scheduled {
            at,
            tie_break,
            event,
        }));
    }

    /// Advance the run by one event.
    ///
    /// Returns [`Step::Done`] without popping when the budget has already been
    /// reached, so the bounding event — the one whose processing reached the
    /// budget — is delivered and processed, and the *next* call ends the run
    /// (Requirement 4.6). Otherwise pops the earliest pending event, advances
    /// `now` to its instant (forward only — Requirement 4.4), counts it, and
    /// returns it as [`Step::Event`] for the caller to dispatch. An empty queue
    /// ends the run as [`EndReason::Quiescent`].
    pub fn step(&mut self) -> Step {
        // Budget is checked *before* popping, so the event that reached the
        // bound on the previous step was returned and processed, and the run
        // ends here — after that bounding event (Requirement 4.6).
        if self.events_processed >= self.budget.max_events {
            return Step::Done(EndReason::EventBudget);
        }
        if self.now.as_nanos() >= self.budget.max_virtual_nanos {
            return Step::Done(EndReason::VirtualTimeBudget);
        }

        match self.queue.pop() {
            None => Step::Done(EndReason::Quiescent),
            Some(Reverse(scheduled)) => {
                // The min-heap guarantees `scheduled.at >= now` (nothing earlier
                // remains and `schedule` clamps the past to `now`), so this only
                // ever moves time forward.
                self.now = scheduled.at;
                self.events_processed += 1;
                Step::Event(scheduled)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::SeedStreams;

    /// A budget that never bounds a test that simply drains its own events.
    fn unbounded() -> Budget {
        Budget {
            max_events: u64::MAX,
            max_virtual_nanos: u64::MAX,
        }
    }

    fn client_op(seq: u64) -> Event {
        Event::ClientOp(ClientOp { seq })
    }

    /// Convenience: pull the next `Step::Event` or panic.
    fn expect_event(sched: &mut Scheduler) -> Scheduled {
        match sched.step() {
            Step::Event(s) => s,
            Step::Done(reason) => panic!("expected an event, got Done({reason:?})"),
        }
    }

    /// Requirement 4.4: events are delivered earliest-instant first and `now`
    /// only ever moves forward.
    #[test]
    fn events_are_delivered_in_instant_order() {
        let streams = SeedStreams::new(1);
        let mut sched = Scheduler::new(unbounded(), streams.tiebreak);

        // Schedule out of order.
        sched.schedule(VirtualInstant::from_nanos(30), client_op(0));
        sched.schedule(VirtualInstant::from_nanos(10), client_op(1));
        sched.schedule(VirtualInstant::from_nanos(20), client_op(2));

        let mut last = VirtualInstant::ORIGIN;
        let mut seen = Vec::new();
        loop {
            match sched.step() {
                Step::Event(s) => {
                    assert!(s.at >= last, "time moved backward: {:?} < {:?}", s.at, last);
                    last = s.at;
                    assert_eq!(sched.now(), s.at, "now must equal the delivered instant");
                    seen.push(s.at.as_nanos());
                }
                Step::Done(EndReason::Quiescent) => break,
                Step::Done(other) => panic!("unexpected end: {other:?}"),
            }
        }
        assert_eq!(seen, vec![10, 20, 30]);
    }

    /// Requirement 4.4: an event scheduled in the past is clamped to `now`, so
    /// it is never processed before an earlier-scheduled event already past.
    #[test]
    fn scheduling_in_the_past_is_clamped_to_now() {
        let streams = SeedStreams::new(2);
        let mut sched = Scheduler::new(unbounded(), streams.tiebreak);

        sched.schedule(VirtualInstant::from_nanos(100), client_op(0));
        let first = expect_event(&mut sched);
        assert_eq!(first.at, VirtualInstant::from_nanos(100));
        assert_eq!(sched.now(), VirtualInstant::from_nanos(100));

        // Now schedule "in the past" (t=10 < now=100): it must clamp to now.
        sched.schedule(VirtualInstant::from_nanos(10), client_op(1));
        let second = expect_event(&mut sched);
        assert_eq!(
            second.at,
            VirtualInstant::from_nanos(100),
            "a past instant must be clamped forward to now"
        );
        assert!(sched.now() >= first.at, "time must not move backward");
    }

    /// Requirements 1.5, 1.1: events at the same instant are ordered by a
    /// seed-derived tie-break, and the ordering is identical across two
    /// schedulers built from the same seed.
    #[test]
    fn equal_instant_tie_break_is_deterministic_for_a_seed() {
        let order_for_seed = |seed: u64| {
            let streams = SeedStreams::new(seed);
            let mut sched = Scheduler::new(unbounded(), streams.tiebreak);
            // Ten events, all at the same instant, scheduled in seq order.
            for seq in 0..10 {
                sched.schedule(VirtualInstant::from_nanos(42), client_op(seq));
            }
            let mut order = Vec::new();
            while let Step::Event(s) = sched.step() {
                match s.event {
                    Event::ClientOp(op) => order.push(op.seq),
                    other => panic!("unexpected event {other:?}"),
                }
            }
            order
        };

        let a = order_for_seed(0xABCD);
        let b = order_for_seed(0xABCD);
        assert_eq!(a, b, "same seed must give identical tie-break order");

        // The tie-break actually reorders (it is not just insertion order); for
        // this seed at least one event is out of its scheduled seq position.
        let insertion: Vec<u64> = (0..10).collect();
        assert_ne!(
            a, insertion,
            "seed-derived tie-break should reorder simultaneous events"
        );

        // And it is a permutation of all scheduled events (nothing lost/dup'd).
        let mut sorted = a.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, insertion);
    }

    /// Requirement 4.6: a run bounded by `max_events` ends *after* processing
    /// exactly that many events.
    #[test]
    fn event_budget_ends_after_processing_the_bounding_event() {
        let streams = SeedStreams::new(3);
        let budget = Budget {
            max_events: 3,
            max_virtual_nanos: u64::MAX,
        };
        let mut sched = Scheduler::new(budget, streams.tiebreak);
        for i in 0..10 {
            sched.schedule(VirtualInstant::from_nanos(i + 1), client_op(i));
        }

        let mut processed = 0;
        loop {
            match sched.step() {
                Step::Event(_) => processed += 1,
                Step::Done(EndReason::EventBudget) => break,
                Step::Done(other) => panic!("expected EventBudget, got {other:?}"),
            }
        }
        assert_eq!(processed, 3, "must process exactly max_events events");
        assert_eq!(sched.events_processed(), 3);
        assert!(sched.pending_events() > 0, "remaining events stay queued");
    }

    /// Requirement 4.6: a run bounded by `max_virtual_nanos` processes the event
    /// that reaches the bound and then ends.
    #[test]
    fn virtual_time_budget_ends_after_the_bounding_event() {
        let streams = SeedStreams::new(4);
        let budget = Budget {
            max_events: u64::MAX,
            max_virtual_nanos: 50,
        };
        let mut sched = Scheduler::new(budget, streams.tiebreak);
        sched.schedule(VirtualInstant::from_nanos(20), client_op(0));
        sched.schedule(VirtualInstant::from_nanos(50), client_op(1)); // reaches bound
        sched.schedule(VirtualInstant::from_nanos(80), client_op(2)); // beyond bound

        let mut reached = Vec::new();
        loop {
            match sched.step() {
                Step::Event(s) => reached.push(s.at.as_nanos()),
                Step::Done(EndReason::VirtualTimeBudget) => break,
                Step::Done(other) => panic!("expected VirtualTimeBudget, got {other:?}"),
            }
        }
        // The event at t=50 reaches the bound and is processed; t=80 is not.
        assert_eq!(reached, vec![20, 50]);
        assert_eq!(sched.now(), VirtualInstant::from_nanos(50));
    }

    /// An empty scheduler ends immediately as quiescent.
    #[test]
    fn empty_scheduler_is_quiescent() {
        let streams = SeedStreams::new(5);
        let mut sched = Scheduler::new(unbounded(), streams.tiebreak);
        assert!(sched.is_idle());
        assert_eq!(sched.next_instant(), None);
        assert!(matches!(sched.step(), Step::Done(EndReason::Quiescent)));
    }

    /// `VirtualInstant` / `VirtualDuration` arithmetic is saturating and the
    /// units line up (ms/secs convert to the expected nanos).
    #[test]
    fn virtual_time_arithmetic_is_saturating_and_consistent() {
        let t = VirtualInstant::from_nanos(1_000);
        assert_eq!(
            t.saturating_add(VirtualDuration::from_nanos(500))
                .as_nanos(),
            1_500
        );
        assert_eq!(VirtualDuration::from_millis(1).as_nanos(), 1_000_000);
        assert_eq!(VirtualDuration::from_secs(1).as_nanos(), 1_000_000_000);

        // Saturating add never overflows.
        let max = VirtualInstant::from_nanos(u64::MAX);
        assert_eq!(
            max.saturating_add(VirtualDuration::from_nanos(1)),
            VirtualInstant::from_nanos(u64::MAX)
        );

        // duration_since is the forward difference, saturating at zero.
        assert_eq!(
            VirtualInstant::from_nanos(30)
                .duration_since(VirtualInstant::from_nanos(10))
                .as_nanos(),
            20
        );
        assert_eq!(
            VirtualInstant::from_nanos(10).duration_since(VirtualInstant::from_nanos(30)),
            VirtualDuration::ZERO
        );
    }
}
