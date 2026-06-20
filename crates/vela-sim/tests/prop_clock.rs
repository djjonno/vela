#![cfg(feature = "sim")]
//! Property test for the `SimClock` virtual-time timer semantics.
//!
//! Feature: deterministic-simulation-testing, Property 3: Virtual-time timer
//! semantics
//!
//! Property 3 asserts the three timer guarantees the harness's [`SimClock`] must
//! uphold so elections, heartbeats, and clock-skew behave exactly as production
//! and free of real-time flakiness:
//!
//! - **Heartbeat exactness (Requirement 4.3).** A heartbeat timer armed for
//!   `dur` fires at *exactly* `now + dur` — no randomization — for any seed and
//!   any arming instant.
//! - **Election jitter window (Requirement 4.2).** An election timer armed for
//!   the base timeout fires within `[base, 2*base)` (150–300 ms for the 150 ms
//!   base) measured from its arming instant, for any seed.
//! - **Bounded clock skew (Requirement 4.5).** A per-node bounded affine view
//!   `view(t) = offset + t * rate` only perturbs a node's *relative* timer
//!   firing: the constant offset cancels for a relative delay, an identity skew
//!   behaves as no skew, a faster rate fires no later than a slower one, and a
//!   skew sampled within the configured bounds keeps the firing instant within
//!   the affine-transformed bound.
//!
//! A fourth, cross-cutting check covers determinism (Requirement 4.2 / the
//! reproducibility constraint): the same seed armed with the same sequence of
//! timers yields byte-for-byte identical armed instants and generations.
//!
//! The whole test drives only the public clock API and is fully deterministic
//! (no wall-clock reads, no randomness outside the seeded streams), so it never
//! flakes.
//!
//! Validates: Requirements 4.2, 4.3, 4.5

use std::time::Duration;

use proptest::prelude::*;

use vela_core::model::PartitionIndex;
use vela_core::{GroupKey, NodeId};
use vela_raft::{Clock, TimerKind, ELECTION_TIMEOUT_BASE, HEARTBEAT_INTERVAL};
use vela_sim::clock::{ArmedTimer, SimClock, SkewModel};
use vela_sim::rng::SeedStreams;
use vela_sim::scenario::FaultIntensities;
use vela_sim::scheduler::VirtualInstant;

/// A node id for the timers under test.
fn node(id: &str) -> NodeId {
    NodeId::new(id)
}

/// A `(topic, partition)` group key for the timers under test.
fn group(topic: &str, partition: u32) -> GroupKey {
    (topic.to_string(), PartitionIndex(partition))
}

/// A fresh clock drawing election jitter from `seed`'s `election` stream.
fn clock_for(seed: u64) -> SimClock {
    SimClock::new(SeedStreams::new(seed).election)
}

/// Arm exactly one `kind` timer for `(n, g)` at logical `now` and return it.
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

/// The election base timeout in logical nanoseconds (150 ms).
fn election_base_nanos() -> u64 {
    ELECTION_TIMEOUT_BASE.as_nanos() as u64
}

/// The heartbeat interval in logical nanoseconds (50 ms).
fn heartbeat_nanos() -> u64 {
    HEARTBEAT_INTERVAL.as_nanos() as u64
}

proptest! {
    // At least 100 cases (Property-test requirement); 256 is proptest's default
    // and keeps the run fast while covering a broad seed / instant / skew space.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property 3: Virtual-time timer semantics (Requirements 4.2, 4.3, 4.5).
    ///
    /// Generators:
    /// - `seed`: the full 64-bit run-seed space (selects election jitter).
    /// - `now_nanos`: an arbitrary arming instant on the logical timeline.
    /// - `max_skew_nanos` / `max_rate`: bounded skew intensities, with
    ///   `max_rate < 0.5` so the view rate stays well clear of zero.
    /// - `r_slow` / `r_gap`: two strictly-ordered positive rates (`r_fast =
    ///   r_slow + r_gap > r_slow`) for the fast-vs-slow ordering check.
    #[test]
    fn virtual_time_timer_semantics(
        seed in any::<u64>(),
        now_nanos in 0u64..2_000_000_000,
        max_skew_nanos in 0u64..2_000_000,
        max_rate in 0.0f64..0.49,
        r_slow in 0.25f64..0.95,
        r_gap in 0.05f64..0.75,
    ) {
        let n = node("node-a");
        let g = group("orders", 0);
        let now = VirtualInstant::from_nanos(now_nanos);
        let hb_nanos = heartbeat_nanos();
        let base = election_base_nanos();

        // (a) Heartbeat exactness (Requirement 4.3): fires at exactly now + dur,
        // with no jitter, regardless of seed or arming instant.
        {
            let mut c = clock_for(seed);
            let hb = arm_one(&mut c, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
            prop_assert_eq!(
                hb.at,
                VirtualInstant::from_nanos(now_nanos + hb_nanos),
                "heartbeat must fire at exactly now + interval"
            );
            prop_assert_eq!(hb.kind, TimerKind::Heartbeat);
        }

        // (b) Election jitter window (Requirement 4.2): fires within
        // [base, 2*base) = 150-300 ms measured from the arming instant.
        {
            let mut c = clock_for(seed);
            let el = arm_one(&mut c, now, &n, &g, TimerKind::Election, ELECTION_TIMEOUT_BASE);
            let delay = el.at.as_nanos() - now_nanos;
            prop_assert!(
                (base..2 * base).contains(&delay),
                "election delay {} outside [{}, {})",
                delay,
                base,
                2 * base
            );
            prop_assert_eq!(el.kind, TimerKind::Election);
        }

        // (c) Determinism: the same seed armed with the same sequence of timers
        // yields byte-for-byte identical armed timers (instants + generations).
        {
            let sequence = |seed: u64| -> Vec<ArmedTimer> {
                let mut c = clock_for(seed);
                let mut out = Vec::new();
                for i in 0..5u64 {
                    let t = VirtualInstant::from_nanos(now_nanos + i * 1_000);
                    out.push(arm_one(&mut c, t, &n, &g, TimerKind::Election, ELECTION_TIMEOUT_BASE));
                    out.push(arm_one(&mut c, t, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL));
                }
                out
            };
            prop_assert_eq!(
                sequence(seed),
                sequence(seed),
                "same seed must yield identical armed timers"
            );
        }

        // (d) Bounded clock skew (Requirement 4.5).

        // Identity skew behaves exactly as no skew — for both timer kinds.
        {
            let mut plain = clock_for(seed);
            let plain_hb = arm_one(&mut plain, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
            let mut id = clock_for(seed);
            id.set_skew(n.clone(), SkewModel::identity());
            let id_hb = arm_one(&mut id, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
            prop_assert_eq!(plain_hb.at, id_hb.at, "identity skew must equal no skew (heartbeat)");

            let mut plain_e = clock_for(seed);
            let plain_el = arm_one(&mut plain_e, now, &n, &g, TimerKind::Election, ELECTION_TIMEOUT_BASE);
            let mut id_e = clock_for(seed);
            id_e.set_skew(n.clone(), SkewModel::identity());
            let id_el = arm_one(&mut id_e, now, &n, &g, TimerKind::Election, ELECTION_TIMEOUT_BASE);
            prop_assert_eq!(plain_el.at, id_el.at, "identity skew must equal no skew (election)");
        }

        // A pure offset (rate 1.0) cancels for a relative timer delay, and
        // clearing the skew restores true logical time.
        {
            let mut c = clock_for(seed);
            c.set_skew(n.clone(), SkewModel::new(1_000_000_000.0, 1.0));
            let off_hb = arm_one(&mut c, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
            prop_assert_eq!(
                off_hb.at,
                VirtualInstant::from_nanos(now_nanos + hb_nanos),
                "a constant offset must cancel for a relative delay"
            );

            c.clear_skew(&n);
            let cleared = arm_one(&mut c, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
            prop_assert_eq!(
                cleared.at,
                VirtualInstant::from_nanos(now_nanos + hb_nanos),
                "clearing skew must restore true logical time"
            );
        }

        // A faster rate fires no later than a slower rate (true time compresses
        // as the node's clock speeds up).
        {
            let r_fast = r_slow + r_gap;
            let mut fast = clock_for(seed);
            fast.set_skew(n.clone(), SkewModel::new(0.0, r_fast));
            let fast_hb = arm_one(&mut fast, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);

            let mut slow = clock_for(seed);
            slow.set_skew(n.clone(), SkewModel::new(0.0, r_slow));
            let slow_hb = arm_one(&mut slow, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);

            prop_assert!(
                fast_hb.at <= slow_hb.at,
                "fast rate {} fired later ({:?}) than slow rate {} ({:?})",
                r_fast,
                fast_hb.at,
                r_slow,
                slow_hb.at
            );
        }

        // A skew sampled within the configured bounds keeps a heartbeat's firing
        // delay within the affine-transformed window HB/rate, rate in
        // [1-max_rate, 1+max_rate]. The offset cancels, so only the rate bounds
        // the relative delay.
        {
            let faults = FaultIntensities {
                max_clock_skew_nanos: max_skew_nanos,
                max_clock_skew_rate: max_rate,
                ..FaultIntensities::default()
            };
            let mut fault_stream = SeedStreams::new(seed).faults;
            let skew = SkewModel::sample(&faults, &mut fault_stream);

            let mut c = clock_for(seed);
            c.set_skew(n.clone(), skew);
            let hb = arm_one(&mut c, now, &n, &g, TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
            let delay = hb.at.as_nanos() - now_nanos;

            let hb_f = hb_nanos as f64;
            // Lowest delay at the fastest allowed rate, highest at the slowest.
            let lower = (hb_f / (1.0 + max_rate)).floor() as u64;
            let upper = (hb_f / (1.0 - max_rate)).ceil() as u64;
            // ±1 slack absorbs the final rounding into integer nanoseconds.
            prop_assert!(
                delay + 1 >= lower && delay <= upper + 1,
                "sampled-skew delay {} outside affine bound [{}, {}] (max_rate {})",
                delay,
                lower,
                upper,
                max_rate
            );
        }
    }
}
