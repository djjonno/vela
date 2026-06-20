//! `SimClock`: the deterministic virtual-time [`Clock`] seam.
//!
//! `SimClock` implements `vela_raft::Clock` for the harness, generalizing
//! `vela_raft::sim::ManualClock` from one partition's group to the multi-node,
//! multi-group cluster. It is the bridge between the consensus core (which arms
//! abstract timers through the `Clock` trait) and the discrete-event
//! [`Scheduler`](crate::scheduler::Scheduler) (which owns the global, logical
//! event timeline).
//!
//! # Ownership model
//!
//! `Clock::arm(&mut self, …)` has access only to `self`, so `SimClock` cannot
//! reach into the [`Scheduler`] to enqueue a `TimerFire` directly (and must not
//! — the scheduler owns the seed-derived tie-break stream that orders events).
//! Instead `SimClock` *records* each armed timer into a `pending` buffer that
//! the runtime [`drains`](SimClock::drain_armed) after the step and schedules
//! onto the timeline. This keeps the two concerns cleanly separated: `SimClock`
//! decides *when (in logical time)* a timer fires and *which generation* it
//! carries; the `Scheduler` decides *in what order* simultaneous events run.
//!
//! Three behaviours mirror production exactly so the harness exercises the same
//! timer semantics the live server has:
//!
//! 1. **Jitter (Requirement 4.2).** An `Election` timer fires at `now + dur +
//!    jitter`, with `jitter` drawn from the seeded `election` RNG stream within
//!    `[0, dur)` — a `[dur, 2*dur)` window, i.e. 150–300 ms for the 150 ms base
//!    — exactly as `ManualClock`/`TimerClock` do.
//! 2. **Exact heartbeat (Requirement 4.3).** A `Heartbeat` timer fires at
//!    exactly `now + dur`, with no randomization.
//! 3. **Generations (mirrors `TimerClock::is_current`).** Re-arming a timer of
//!    the same `(node, group, kind)` bumps a per-key generation counter and
//!    stamps the new `TimerFire` with it; a fired `TimerFire` whose generation
//!    is stale is dropped via [`SimClock::is_current`], giving election-timer
//!    reset without an explicit cancellation path.
//!
//! # `now()` and the wall clock
//!
//! The `Clock` trait requires `fn now(&self) -> std::time::Instant`, but a pure
//! logical clock has no `Instant` to hand back. `SimClock` follows the **same
//! approach `ManualClock` uses**: it captures a single arbitrary `origin`
//! `Instant` at construction and returns `origin + logical_now`. This is the
//! *only* touch of wall time in the harness, and it is **not outcome-affecting**:
//!
//! - The Raft consensus core never calls `clock.now()` for any decision — timers
//!   are armed via `arm` and fire back as `RaftInput::Tick`s; `now()` exists only
//!   to satisfy the trait and for relative measurement by callers that want it.
//! - All event ordering, timer firing, and budget arithmetic run off the
//!   logical [`VirtualInstant`] timeline, never off `now()`.
//!
//! So the captured `origin` shifts every `now()` reading by the same constant
//! and can never change which events fire, in what order, or with what result.
//! The logical [`VirtualInstant`] held by the [`Scheduler`] remains the single
//! source of truth; `SimClock::now()` is a derived, shifted view of it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use vela_core::{GroupKey, NodeId};
use vela_raft::{Clock, TimerKind};

use crate::rng::SplitMix64;
use crate::scenario::FaultIntensities;
use crate::scheduler::{Event, VirtualDuration, VirtualInstant};

/// Identifies one logical timer: a `kind` timer for `group` on `node`.
type TimerId = (NodeId, GroupKey, TimerKind);

/// A bounded per-node affine view of logical time, `view(t) = offset + t *
/// rate`, modeling [`Clock_Skew`](crate) (Requirement 4.5).
///
/// A skewed node perceives time through this transform: its clock is offset by a
/// constant and ticks at a rate near (but not exactly) `1.0`. When the node arms
/// a timer for a duration `dur` *in its own view*, the timer must fire at the
/// **true** logical instant at which the node's view has advanced by `dur`.
///
/// Inverting the affine view, that true instant is `now + dur / rate`: the
/// constant `offset` cancels for a *relative* delay (it only shifts absolute
/// readings), so only `rate` perturbs timer firing. `rate` is kept strictly
/// positive and bounded near `1.0`, and `offset` bounded, so the skew can never
/// approach reading real wall-clock time — it only stretches or compresses the
/// node's relative sense of elapsed time. The global event queue stays ordered
/// by true [`VirtualInstant`], so determinism is preserved.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SkewModel {
    /// Constant offset added to the node's view, in logical nanoseconds. Cancels
    /// for relative timer delays; retained for fidelity to the affine model.
    offset_nanos: f64,
    /// Multiplicative rate of the node's view relative to true time. `> 1.0`
    /// means the node's clock runs fast (timers fire *sooner* in true time);
    /// `< 1.0` means it runs slow (timers fire *later*).
    rate: f64,
}

impl SkewModel {
    /// The smallest rate the model allows, guarding against a zero or negative
    /// rate (which would invert or stall time). Far below any bound a sane
    /// [`FaultIntensities::max_clock_skew_rate`] would configure.
    const MIN_RATE: f64 = 1.0e-3;

    /// Construct a skew with the given `offset_nanos` and a view `rate`.
    ///
    /// `rate` is clamped to be strictly positive ([`MIN_RATE`](Self::MIN_RATE)
    /// or greater) so the affine view is always invertible and forward-going.
    #[must_use]
    pub fn new(offset_nanos: f64, rate: f64) -> Self {
        Self {
            offset_nanos,
            rate: rate.max(Self::MIN_RATE),
        }
    }

    /// An identity skew (`offset = 0`, `rate = 1.0`): the node's view equals true
    /// time. Equivalent to having no skew at all.
    #[must_use]
    pub fn identity() -> Self {
        Self::new(0.0, 1.0)
    }

    /// Sample a bounded skew for a node from `faults` and the `faults` RNG
    /// `stream`.
    ///
    /// The offset is drawn uniformly within `±max_clock_skew_nanos` and the rate
    /// within `1.0 ± max_clock_skew_rate`, so both stay within the configured
    /// bounds (Requirement 4.5). With the healthy-cluster defaults (both bounds
    /// zero) this yields [`SkewModel::identity`]. Selecting *which* nodes are
    /// skewed is wired by a later task; this provides the bounded draw.
    #[must_use]
    pub fn sample(faults: &FaultIntensities, stream: &mut SplitMix64) -> Self {
        let offset_nanos = if faults.max_clock_skew_nanos == 0 {
            0.0
        } else {
            // Uniform in [-max, +max].
            let span = faults.max_clock_skew_nanos;
            let drawn = stream.next_below(span.saturating_mul(2).saturating_add(1));
            drawn as f64 - span as f64
        };
        let rate = if faults.max_clock_skew_rate == 0.0 {
            1.0
        } else {
            // Uniform in [1 - max_rate, 1 + max_rate] via a fixed-resolution draw.
            const RESOLUTION: u64 = 1_000_000;
            let frac = stream.next_below(RESOLUTION + 1) as f64 / RESOLUTION as f64;
            let delta = (frac * 2.0 - 1.0) * faults.max_clock_skew_rate;
            1.0 + delta
        };
        Self::new(offset_nanos, rate)
    }

    /// The node's view of true logical time `t`, `offset + t * rate`.
    #[must_use]
    fn view(&self, true_nanos: f64) -> f64 {
        self.offset_nanos + true_nanos * self.rate
    }

    /// The true logical time whose view is `view_nanos` (the inverse of
    /// [`view`](Self::view)).
    #[must_use]
    fn unview(&self, view_nanos: f64) -> f64 {
        (view_nanos - self.offset_nanos) / self.rate
    }

    /// The true [`VirtualInstant`] at which a timer armed at true instant `now`
    /// for a node-view `delay` fires.
    ///
    /// Computed through the affine view and its inverse: the node fires when its
    /// view has advanced from `view(now)` by `delay`, i.e. at true time
    /// `unview(view(now) + delay)` = `now + delay / rate`. Clamped to never
    /// precede `now` and saturated into the `u64` logical range.
    #[must_use]
    fn firing_instant(&self, now: VirtualInstant, delay: VirtualDuration) -> VirtualInstant {
        let now_nanos = now.as_nanos() as f64;
        let view_fire = self.view(now_nanos) + delay.as_nanos() as f64;
        let true_fire = self.unview(view_fire).max(now_nanos);
        VirtualInstant::from_nanos(nanos_from_f64(true_fire))
    }
}

/// Clamp and round a logical-nanosecond `f64` into the `u64` range, saturating
/// rather than wrapping or panicking on out-of-range values.
fn nanos_from_f64(x: f64) -> u64 {
    if x <= 0.0 {
        0
    } else if x >= u64::MAX as f64 {
        u64::MAX
    } else {
        x.round() as u64
    }
}

/// A timer the consensus core armed through [`SimClock::arm`], awaiting
/// scheduling onto the discrete-event timeline.
///
/// The runtime [`drains`](SimClock::drain_armed) these after stepping a replica
/// and schedules each as an [`Event::TimerFire`] at [`at`](Self::at). The
/// `generation` it carries is checked against [`SimClock::is_current`] when the
/// event fires, so a timer superseded by a later re-arm is dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArmedTimer {
    /// The true logical instant at which the timer fires (skew already applied).
    pub at: VirtualInstant,
    /// The node whose replica armed the timer.
    pub node: NodeId,
    /// The Raft group the timer belongs to.
    pub group: GroupKey,
    /// Whether this is an election or heartbeat timer.
    pub kind: TimerKind,
    /// The arming generation; a stale generation is dropped when it fires.
    pub generation: u64,
}

impl ArmedTimer {
    /// The [`Event::TimerFire`] this armed timer becomes when scheduled.
    #[must_use]
    pub fn to_event(&self) -> Event {
        Event::TimerFire {
            node: self.node.clone(),
            group: self.group.clone(),
            kind: self.kind,
            generation: self.generation,
        }
    }
}

/// The deterministic virtual-time [`Clock`] for the whole simulated cluster.
///
/// One `SimClock` serves every `(node, group)` replica in a run. Before stepping
/// a replica the runtime calls [`set_now`](Self::set_now) with the scheduler's
/// current instant and [`set_active`](Self::set_active) with that replica's
/// identity, so timers the replica arms during the step are attributed to it and
/// scheduled relative to the correct logical instant. After the step the runtime
/// [`drains`](Self::drain_armed) the armed timers onto the timeline.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use vela_core::{NodeId, model::PartitionIndex};
/// use vela_raft::{Clock, TimerKind, HEARTBEAT_INTERVAL};
/// use vela_sim::clock::SimClock;
/// use vela_sim::rng::SeedStreams;
/// use vela_sim::scheduler::VirtualInstant;
///
/// let streams = SeedStreams::new(0xF00D_u64);
/// let mut clock = SimClock::new(streams.election);
/// let node = NodeId::new("node-a");
/// let group = ("orders".to_string(), PartitionIndex(0));
///
/// clock.set_now(VirtualInstant::from_nanos(1_000));
/// clock.set_active(node.clone(), group.clone());
/// clock.arm(TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
///
/// let armed = clock.drain_armed();
/// assert_eq!(armed.len(), 1);
/// // Heartbeats fire at exactly now + interval (no jitter).
/// assert_eq!(armed[0].at, VirtualInstant::from_nanos(1_000 + 50_000_000));
/// ```
#[derive(Debug)]
pub struct SimClock {
    /// Arbitrary wall-clock anchor for [`now`](Self::now); never outcome-
    /// affecting (see the module-level note on `now()` and the wall clock).
    origin: Instant,
    /// Current logical instant, mirrored from the scheduler before each step.
    now: VirtualInstant,
    /// The replica currently being stepped; armed timers are attributed to it.
    active: Option<(NodeId, GroupKey)>,
    /// Seeded PRNG supplying election-timeout jitter (the run's `election`
    /// stream). Heartbeats never draw from it.
    election: SplitMix64,
    /// Latest arming generation per `(node, group, kind)` timer.
    generations: HashMap<TimerId, u64>,
    /// Per-node clock skew, if configured. Absent means the node's view equals
    /// true logical time.
    skews: HashMap<NodeId, SkewModel>,
    /// Timers armed since the last [`drain_armed`](Self::drain_armed), awaiting
    /// scheduling onto the timeline.
    pending: Vec<ArmedTimer>,
}

impl SimClock {
    /// Create a clock drawing election jitter from `election` (the run's
    /// `election` RNG stream).
    ///
    /// Logical time starts at [`VirtualInstant::ORIGIN`](crate::scheduler::VirtualInstant::ORIGIN);
    /// no replica is active, no skew is configured, and no timers are pending.
    /// The wall-clock `origin` anchoring [`now`](Self::now) is captured here and
    /// is never outcome-affecting (see the module docs).
    #[must_use]
    pub fn new(election: SplitMix64) -> Self {
        Self {
            origin: Instant::now(),
            now: VirtualInstant::ORIGIN,
            active: None,
            election,
            generations: HashMap::new(),
            skews: HashMap::new(),
            pending: Vec::new(),
        }
    }

    /// Mirror the scheduler's current logical instant into the clock.
    ///
    /// The runtime calls this with `scheduler.now()` immediately before stepping
    /// a replica, so timers armed during that step fire relative to the correct
    /// instant (`now + delay`).
    pub fn set_now(&mut self, now: VirtualInstant) {
        self.now = now;
    }

    /// The current logical instant the clock arms timers relative to.
    #[must_use]
    pub fn virtual_now(&self) -> VirtualInstant {
        self.now
    }

    /// Attribute subsequently armed timers to the replica `(node, group)`.
    ///
    /// The runtime calls this immediately before stepping that replica
    /// (mirroring `ManualClock::set_active`), so [`arm`](Clock::arm) — whose
    /// trait signature carries no node — can attribute the timer correctly.
    pub fn set_active(&mut self, node: NodeId, group: GroupKey) {
        self.active = Some((node, group));
    }

    /// Clear the active replica, so a stray [`arm`](Clock::arm) with no active
    /// replica is caught rather than silently misattributed.
    pub fn clear_active(&mut self) {
        self.active = None;
    }

    /// Configure (or replace) the [`Clock_Skew`](crate) applied to `node`'s view
    /// of time.
    ///
    /// Provided so a later task can mark a seed-selected subset of nodes as
    /// skewed; until then every node runs unskewed. An [`SkewModel::identity`]
    /// is accepted and behaves as no skew.
    pub fn set_skew(&mut self, node: NodeId, skew: SkewModel) {
        self.skews.insert(node, skew);
    }

    /// Remove any [`Clock_Skew`](crate) configured for `node`, restoring its
    /// view to true logical time.
    pub fn clear_skew(&mut self, node: &NodeId) {
        self.skews.remove(node);
    }

    /// Whether `generation` is the latest arming of the `kind` timer for `group`
    /// on `node`.
    ///
    /// The runtime checks this when a [`Event::TimerFire`] is dispatched and
    /// drops the tick if it is stale, so a timer superseded by a later re-arm
    /// never reaches the replica (mirroring `TimerClock::is_current`). A timer
    /// that was never armed has implicit generation `0`.
    #[must_use]
    pub fn is_current(
        &self,
        node: &NodeId,
        group: &GroupKey,
        kind: TimerKind,
        generation: u64,
    ) -> bool {
        let key = (node.clone(), group.clone(), kind);
        self.generations.get(&key).copied().unwrap_or(0) == generation
    }

    /// Remove and return every timer armed since the last call.
    ///
    /// The runtime calls this after stepping a replica and schedules each
    /// returned [`ArmedTimer`] onto the discrete-event timeline as an
    /// [`Event::TimerFire`] at its [`at`](ArmedTimer::at) instant.
    #[must_use]
    pub fn drain_armed(&mut self) -> Vec<ArmedTimer> {
        std::mem::take(&mut self.pending)
    }

    /// The number of timers armed but not yet drained. Diagnostic helper.
    #[must_use]
    pub fn pending_armed(&self) -> usize {
        self.pending.len()
    }

    /// Bump and return the next generation for the `(node, group, kind)` timer.
    ///
    /// Generations start at `0` (the implicit value for a never-armed timer) and
    /// the first arm returns `1`, exactly as `TimerClock::next_generation` does.
    fn bump_generation(&mut self, node: &NodeId, group: &GroupKey, kind: TimerKind) -> u64 {
        let key = (node.clone(), group.clone(), kind);
        let slot = self.generations.entry(key).or_insert(0);
        *slot += 1;
        *slot
    }

    /// The node-view delay for a timer of `kind` armed for `dur`.
    ///
    /// `Election` adds seed-derived jitter in `[0, dur)` (yielding a `[dur,
    /// 2*dur)` firing window — 150–300 ms for the 150 ms base, Requirement 4.2);
    /// `Heartbeat` is the exact `dur` (Requirement 4.3). This mirrors
    /// `ManualClock`'s `jitter` and `TimerClock::delay_for` precisely, drawing
    /// from the same kind of `SplitMix64` `next_below` call.
    fn delay_for(&mut self, kind: TimerKind, dur: Duration) -> VirtualDuration {
        let base_nanos = dur.as_nanos().min(u64::MAX as u128) as u64;
        match kind {
            TimerKind::Heartbeat => VirtualDuration::from_nanos(base_nanos),
            TimerKind::Election => {
                let jitter = if base_nanos == 0 {
                    0
                } else {
                    self.election.next_below(base_nanos)
                };
                VirtualDuration::from_nanos(base_nanos.saturating_add(jitter))
            }
        }
    }
}

impl Clock for SimClock {
    /// The current instant as an `Instant`, derived as `origin + logical_now`.
    ///
    /// Not outcome-affecting: the Raft core never reads this for a decision, and
    /// the constant `origin` shift cannot change event ordering or results (see
    /// the module-level note on `now()` and the wall clock). `checked_add`
    /// saturates at `origin` rather than panicking if the logical clock is
    /// pushed to an extreme in a test.
    fn now(&self) -> Instant {
        self.origin
            .checked_add(Duration::from_nanos(self.now.as_nanos()))
            .unwrap_or(self.origin)
    }

    /// Arm a `kind` timer for the active replica, to fire after `dur` (in the
    /// node's view of time).
    ///
    /// Computes the node-view [`delay_for`](Self::delay_for) (with election
    /// jitter), bumps the `(node, group, kind)` generation, applies any
    /// per-node [`SkewModel`] to translate the node-view delay into a true
    /// firing [`VirtualInstant`], and records the result for the runtime to
    /// schedule. Panics only if no replica is active — a runtime wiring bug,
    /// matching `ManualClock`'s contract.
    fn arm(&mut self, kind: TimerKind, dur: Duration) {
        let (node, group) = self
            .active
            .clone()
            .expect("SimClock::arm called with no active replica; call set_active first");

        // Always draw election jitter (when applicable) before applying skew, so
        // RNG consumption is independent of whether the node is skewed.
        let delay = self.delay_for(kind, dur);
        let generation = self.bump_generation(&node, &group, kind);

        let at = match self.skews.get(&node) {
            Some(skew) => skew.firing_instant(self.now, delay),
            None => self.now.saturating_add(delay),
        };

        self.pending.push(ArmedTimer {
            at,
            node,
            group,
            kind,
            generation,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::SeedStreams;
    use vela_core::model::PartitionIndex;
    use vela_raft::{ELECTION_TIMEOUT_BASE, HEARTBEAT_INTERVAL};

    const ELECTION_BASE_NANOS: u64 = 150_000_000;
    const HEARTBEAT_NANOS: u64 = 50_000_000;

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn group(topic: &str, p: u32) -> GroupKey {
        (topic.to_string(), PartitionIndex(p))
    }

    fn clock(seed: u64) -> SimClock {
        SimClock::new(SeedStreams::new(seed).election)
    }

    /// Arm one timer for `(node, group)` at logical `now` and return the single
    /// resulting armed timer.
    fn arm_one(
        clock: &mut SimClock,
        now: VirtualInstant,
        n: &NodeId,
        g: &GroupKey,
        kind: TimerKind,
        dur: Duration,
    ) -> ArmedTimer {
        clock.set_now(now);
        clock.set_active(n.clone(), g.clone());
        clock.arm(kind, dur);
        let mut armed = clock.drain_armed();
        assert_eq!(armed.len(), 1, "exactly one timer should be armed");
        armed.pop().unwrap()
    }

    /// Requirement 4.3: a heartbeat fires at exactly `now + interval`, with no
    /// jitter, regardless of seed.
    #[test]
    fn heartbeat_fires_at_exact_interval() {
        for seed in [1u64, 7, 42, 9999] {
            let mut c = clock(seed);
            let n = node("node-a");
            let g = group("orders", 0);
            let armed = arm_one(
                &mut c,
                VirtualInstant::from_nanos(2_000),
                &n,
                &g,
                TimerKind::Heartbeat,
                HEARTBEAT_INTERVAL,
            );
            assert_eq!(
                armed.at,
                VirtualInstant::from_nanos(2_000 + HEARTBEAT_NANOS),
                "heartbeat must fire at exactly now + interval"
            );
            assert_eq!(armed.kind, TimerKind::Heartbeat);
        }
    }

    /// Requirement 4.2: an election timer fires within `[base, 2*base)` =
    /// 150–300 ms measured from its arming instant, for every seed.
    #[test]
    fn election_fires_within_the_jitter_window() {
        let now = VirtualInstant::from_nanos(1_000);
        for seed in 0..200u64 {
            let mut c = clock(seed);
            let n = node("node-a");
            let g = group("orders", 0);
            let armed = arm_one(
                &mut c,
                now,
                &n,
                &g,
                TimerKind::Election,
                ELECTION_TIMEOUT_BASE,
            );
            let delay = armed.at.as_nanos() - now.as_nanos();
            assert!(
                (ELECTION_BASE_NANOS..2 * ELECTION_BASE_NANOS).contains(&delay),
                "seed {seed}: election delay {delay} outside [150ms, 300ms)"
            );
        }
    }

    /// Requirements 4.2 & determinism: the same seed yields the same election
    /// jitter sequence; the heartbeat never consumes the election stream.
    #[test]
    fn election_jitter_is_deterministic_and_heartbeat_does_not_consume_rng() {
        let n = node("node-a");
        let g = group("orders", 0);

        let draw_three = |seed: u64| {
            let mut c = clock(seed);
            let mut delays = Vec::new();
            for i in 0..3 {
                let now = VirtualInstant::from_nanos(i * 1_000_000_000);
                let armed = arm_one(
                    &mut c,
                    now,
                    &n,
                    &g,
                    TimerKind::Election,
                    ELECTION_TIMEOUT_BASE,
                );
                delays.push(armed.at.as_nanos() - now.as_nanos());
            }
            delays
        };
        assert_eq!(draw_three(123), draw_three(123), "same seed, same jitter");

        // Interleaving a heartbeat must not perturb the election jitter stream:
        // the election delays match whether or not a heartbeat was armed between.
        let mut c = clock(123);
        let now0 = VirtualInstant::from_nanos(0);
        let e0 = arm_one(
            &mut c,
            now0,
            &n,
            &g,
            TimerKind::Election,
            ELECTION_TIMEOUT_BASE,
        );
        let _hb = arm_one(
            &mut c,
            VirtualInstant::from_nanos(10),
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        let e1 = arm_one(
            &mut c,
            VirtualInstant::from_nanos(20),
            &n,
            &g,
            TimerKind::Election,
            ELECTION_TIMEOUT_BASE,
        );
        let expected = draw_three(123);
        assert_eq!(e0.at.as_nanos(), expected[0]);
        assert_eq!(e1.at.as_nanos() - 20, expected[1]);
    }

    /// Mirrors `TimerClock::is_current`: re-arming a timer bumps its generation,
    /// the new generation is current, and the prior one is stale; generations
    /// are tracked independently per `(node, group, kind)`.
    #[test]
    fn re_arming_bumps_generation_and_supersedes() {
        let mut c = clock(5);
        let n = node("node-a");
        let g = group("orders", 0);

        // A never-armed timer has implicit generation 0.
        assert!(c.is_current(&n, &g, TimerKind::Election, 0));
        assert!(!c.is_current(&n, &g, TimerKind::Election, 1));

        let first = arm_one(
            &mut c,
            VirtualInstant::from_nanos(0),
            &n,
            &g,
            TimerKind::Election,
            ELECTION_TIMEOUT_BASE,
        );
        assert_eq!(first.generation, 1);
        assert!(c.is_current(&n, &g, TimerKind::Election, 1));

        let second = arm_one(
            &mut c,
            VirtualInstant::from_nanos(0),
            &n,
            &g,
            TimerKind::Election,
            ELECTION_TIMEOUT_BASE,
        );
        assert_eq!(second.generation, 2);
        // The first arming is now stale; only the latest is current.
        assert!(!c.is_current(&n, &g, TimerKind::Election, 1));
        assert!(c.is_current(&n, &g, TimerKind::Election, 2));

        // Heartbeat and a different group track separate generations.
        let hb = arm_one(
            &mut c,
            VirtualInstant::from_nanos(0),
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        assert_eq!(hb.generation, 1, "heartbeat generation is independent");
        let other = arm_one(
            &mut c,
            VirtualInstant::from_nanos(0),
            &n,
            &group("orders", 1),
            TimerKind::Election,
            ELECTION_TIMEOUT_BASE,
        );
        assert_eq!(other.generation, 1, "a different group is independent");
        // The original election generation is untouched by the others.
        assert!(c.is_current(&n, &g, TimerKind::Election, 2));
    }

    /// Requirement 4.5: a fast clock (`rate > 1.0`) fires a heartbeat *sooner*
    /// in true time, and a slow clock (`rate < 1.0`) *later*; an unskewed node
    /// and an identity skew both fire at exactly the nominal instant.
    #[test]
    fn skew_rate_scales_firing_in_true_time() {
        let n = node("node-a");
        let g = group("orders", 0);
        let now = VirtualInstant::from_nanos(0);

        // Baseline: no skew → exactly now + interval.
        let mut c = clock(1);
        let base = arm_one(
            &mut c,
            now,
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        assert_eq!(base.at.as_nanos(), HEARTBEAT_NANOS);

        // Identity skew behaves as no skew.
        let mut c = clock(1);
        c.set_skew(n.clone(), SkewModel::identity());
        let id = arm_one(
            &mut c,
            now,
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        assert_eq!(id.at.as_nanos(), HEARTBEAT_NANOS);

        // Fast clock (rate 1.25): true firing = interval / 1.25 < interval.
        let mut c = clock(1);
        c.set_skew(n.clone(), SkewModel::new(0.0, 1.25));
        let fast = arm_one(
            &mut c,
            now,
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        assert_eq!(
            fast.at.as_nanos(),
            (HEARTBEAT_NANOS as f64 / 1.25).round() as u64
        );
        assert!(fast.at.as_nanos() < HEARTBEAT_NANOS);

        // Slow clock (rate 0.8): true firing = interval / 0.8 > interval.
        let mut c = clock(1);
        c.set_skew(n.clone(), SkewModel::new(0.0, 0.8));
        let slow = arm_one(
            &mut c,
            now,
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        assert_eq!(
            slow.at.as_nanos(),
            (HEARTBEAT_NANOS as f64 / 0.8).round() as u64
        );
        assert!(slow.at.as_nanos() > HEARTBEAT_NANOS);
    }

    /// Requirement 4.5: the constant offset cancels for a *relative* timer delay
    /// — only the rate perturbs firing — and clearing skew restores true time.
    #[test]
    fn skew_offset_cancels_for_relative_delay_and_clear_restores() {
        let n = node("node-a");
        let g = group("orders", 0);
        let now = VirtualInstant::from_nanos(5_000_000);

        // A large offset with unit rate must not change the relative firing.
        let mut c = clock(1);
        c.set_skew(n.clone(), SkewModel::new(1_000_000_000.0, 1.0));
        let armed = arm_one(
            &mut c,
            now,
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        assert_eq!(
            armed.at.as_nanos(),
            now.as_nanos() + HEARTBEAT_NANOS,
            "offset must cancel for a relative delay"
        );

        // Clearing skew leaves the node on true time.
        c.clear_skew(&n);
        let after = arm_one(
            &mut c,
            now,
            &n,
            &g,
            TimerKind::Heartbeat,
            HEARTBEAT_INTERVAL,
        );
        assert_eq!(after.at.as_nanos(), now.as_nanos() + HEARTBEAT_NANOS);
    }

    /// `SkewModel::sample` stays within the configured bounds and collapses to
    /// identity under the healthy-cluster defaults.
    #[test]
    fn sampled_skew_respects_bounds() {
        // Healthy defaults → identity.
        let mut stream = SeedStreams::new(0).faults;
        let healthy = SkewModel::sample(&FaultIntensities::default(), &mut stream);
        assert_eq!(healthy, SkewModel::identity());

        // Bounded draws stay within [±offset] and [1 ± rate].
        let faults = FaultIntensities {
            max_clock_skew_nanos: 1_000,
            max_clock_skew_rate: 0.05,
            ..FaultIntensities::default()
        };
        let mut stream = SeedStreams::new(99).faults;
        for _ in 0..1_000 {
            let s = SkewModel::sample(&faults, &mut stream);
            assert!(
                s.offset_nanos.abs() <= 1_000.0,
                "offset {} out of bounds",
                s.offset_nanos
            );
            assert!(
                (0.95..=1.05).contains(&s.rate),
                "rate {} outside [0.95, 1.05]",
                s.rate
            );
        }
    }

    /// `now()` advances with logical time as a constant shift of the origin and
    /// is monotonic as the logical clock moves forward.
    #[test]
    fn now_tracks_logical_time_monotonically() {
        let mut c = clock(1);
        let t0 = c.now();
        c.set_now(VirtualInstant::from_nanos(1_000_000));
        let t1 = c.now();
        c.set_now(VirtualInstant::from_nanos(2_000_000));
        let t2 = c.now();

        assert!(t1 > t0, "now must advance with logical time");
        assert!(t2 > t1);
        // The shift equals the logical delta exactly.
        assert_eq!(t1.duration_since(t0), Duration::from_nanos(1_000_000));
        assert_eq!(t2.duration_since(t1), Duration::from_nanos(1_000_000));
    }

    /// `to_event` reproduces the armed timer's routing stamp and generation.
    #[test]
    fn armed_timer_becomes_a_timer_fire_event() {
        let mut c = clock(1);
        let n = node("node-a");
        let g = group("orders", 2);
        let armed = arm_one(
            &mut c,
            VirtualInstant::from_nanos(0),
            &n,
            &g,
            TimerKind::Election,
            ELECTION_TIMEOUT_BASE,
        );
        match armed.to_event() {
            Event::TimerFire {
                node,
                group,
                kind,
                generation,
            } => {
                assert_eq!(node, n);
                assert_eq!(group, g);
                assert_eq!(kind, TimerKind::Election);
                assert_eq!(generation, armed.generation);
            }
            other => panic!("expected TimerFire, got {other:?}"),
        }
    }

    /// `arm` with no active replica is a wiring bug and panics.
    #[test]
    #[should_panic(expected = "no active replica")]
    fn arm_without_active_replica_panics() {
        let mut c = clock(1);
        c.set_now(VirtualInstant::from_nanos(0));
        c.arm(TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
    }
}
