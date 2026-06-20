#![cfg(feature = "sim")]
//! Property test for the discrete-event scheduler's ordering and termination.
//!
//! Feature: deterministic-simulation-testing, Property 2: Event ordering and
//! bounded termination
//!
//! Property 2 ties together the two scheduler invariants that make a
//! `Simulation_Run` both reproducible and finite:
//!
//! - **Event ordering (Requirement 4.4).** Across the whole run, the scheduler
//!   advances virtual time to the next scheduled event and delivers it, so no
//!   event is ever processed before an earlier-scheduled one: the sequence of
//!   delivered instants is monotonically non-decreasing, `now()` after each step
//!   equals the just-delivered instant, and `now()` never moves backward.
//! - **Bounded termination (Requirement 4.6).** A run is bounded by a maximum
//!   number of events *or* a maximum virtual-time duration, and it ends *after*
//!   processing the event that reaches the bound. The number of events delivered
//!   before the run ends never exceeds `max_events`; an `EventBudget` end means
//!   exactly `max_events` were processed; a `VirtualTimeBudget` end means the
//!   bounding (final) delivered event is the first to reach `max_virtual_nanos`,
//!   with every earlier delivery strictly below it; and a drained queue ends the
//!   run as `Quiescent`.
//!
//! The test drives the real [`Scheduler`] through its public API only, over a
//! seed-derived `tiebreak` stream from [`SeedStreams`], with a randomized set of
//! scheduled events (covering every [`Event`] variant) and a randomized
//! [`Budget`] whose ranges sometimes bound the run and sometimes let the queue
//! drain.
//!
//! Note on the `VirtualTimeBudget` boundary: the scheduler checks the bound
//! *before* popping, so the event that crosses the bound is delivered and the
//! *next* step ends the run (the run ends "after processing the event that
//! reaches the bound", Requirement 4.6). The final delivered instant is
//! therefore `>= max_virtual_nanos`, and a still-pending event is `>=` that
//! instant (it may equal the bound when several events share it), not strictly
//! greater.
//!
//! Validates: Requirements 4.4, 4.6

use proptest::prelude::*;
use vela_core::{NodeId, PartitionIndex};
use vela_raft::{NodeId as RaftNodeId, RaftMessage, RequestVoteReply, TimerKind};
use vela_sim::network::Envelope;
use vela_sim::rng::SeedStreams;
use vela_sim::scenario::Budget;
use vela_sim::scheduler::{
    ClientOp, EndReason, Event, Fault, HealId, Scheduler, Step, VirtualInstant,
};

/// Build a scheduler event from a small selector and an index, exercising every
/// [`Event`] variant. The scheduler does not interpret the payload — it only
/// orders and hands events out — so the payload's only job here is to cover all
/// variants and confirm the queue is payload-agnostic.
fn make_event(selector: u8, i: u64) -> Event {
    let group = ("topic".to_string(), PartitionIndex((i % 4) as u32));
    match selector % 5 {
        0 => Event::TimerFire {
            node: NodeId::new(format!("node-{}", i % 3)),
            group,
            kind: if i % 2 == 0 {
                TimerKind::Election
            } else {
                TimerKind::Heartbeat
            },
            generation: i,
        },
        1 => Event::MessageDeliver(Envelope {
            from: NodeId::new(format!("node-{}", i % 3)),
            to: NodeId::new(format!("node-{}", (i + 1) % 3)),
            to_raft: RaftNodeId(i),
            group,
            msg: RaftMessage::RequestVoteReply(RequestVoteReply {
                term: i,
                vote_granted: i % 2 == 0,
                voter: RaftNodeId(i % 3),
            }),
        }),
        2 => Event::ClientOp(ClientOp { seq: i }),
        3 => Event::FaultApply(Fault::Crash {
            nodes: vec![(i % 3) as usize],
        }),
        _ => Event::FaultHeal(HealId(i)),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: deterministic-simulation-testing, Property 2: Event ordering and
    // bounded termination
    #[test]
    fn event_ordering_and_bounded_termination(
        seed in any::<u64>(),
        // (instant, event-selector) pairs. Instants span a range wide enough
        // that the virtual-time budget below sometimes bounds the run; the count
        // spans a range wide enough that the event budget sometimes bounds it.
        ops in prop::collection::vec((0u64..=2_000, any::<u8>()), 0..=50),
        // Event budget: ranges from below the op count (binds) to above it
        // (drains). Includes 0 to cover the immediate-stop boundary.
        max_events in 0u64..=60,
        // Virtual-time budget: ranges from below the max instant (binds) to
        // above it (drains). Includes 0 to cover the immediate-stop boundary.
        max_virtual_nanos in 0u64..=2_500,
    ) {
        let streams = SeedStreams::new(seed);
        let budget = Budget { max_events, max_virtual_nanos };
        let mut sched = Scheduler::new(budget, streams.tiebreak);

        // Schedule all events up front (now is still ORIGIN, so nothing is
        // clamped) and remember every instant we asked to schedule.
        for (i, (at, selector)) in ops.iter().enumerate() {
            sched.schedule(
                VirtualInstant::from_nanos(*at),
                make_event(*selector, i as u64),
            );
        }

        // Drive the run to its end, asserting ordering on every delivered event.
        let mut delivered: Vec<u64> = Vec::new();
        let mut last_instant = VirtualInstant::ORIGIN;
        let mut prev_now = VirtualInstant::ORIGIN;
        let end_reason = loop {
            match sched.step() {
                Step::Event(scheduled) => {
                    // Requirement 4.4: instants are delivered non-decreasing.
                    prop_assert!(
                        scheduled.at >= last_instant,
                        "instant moved backward: {:?} < {:?}",
                        scheduled.at,
                        last_instant,
                    );
                    // Requirement 4.4: now() equals the delivered instant and
                    // never decreases.
                    prop_assert_eq!(sched.now(), scheduled.at);
                    prop_assert!(sched.now() >= prev_now);

                    // Requirement 4.6: deliveries never exceed the event budget.
                    delivered.push(scheduled.at.as_nanos());
                    prop_assert!(delivered.len() as u64 <= max_events);

                    last_instant = scheduled.at;
                    prev_now = sched.now();
                }
                Step::Done(reason) => break reason,
            }
        };

        // Independent of how it ended: the delivered instants are sorted
        // non-decreasing, and the count respects the event budget.
        prop_assert!(delivered.windows(2).all(|w| w[0] <= w[1]));
        prop_assert!(delivered.len() as u64 <= max_events);
        prop_assert_eq!(sched.events_processed(), delivered.len() as u64);

        let next_pending = sched.next_instant();

        match end_reason {
            // Requirement 4.6: an event-budget end processes exactly max_events
            // events (the bound is checked before the next pop, after the
            // bounding event was delivered).
            EndReason::EventBudget => {
                prop_assert_eq!(delivered.len() as u64, max_events);
            }
            // Requirement 4.6: a virtual-time-budget end processes the event that
            // reaches the bound and then stops. The event budget did not fire
            // first, so strictly fewer than max_events were delivered.
            EndReason::VirtualTimeBudget => {
                prop_assert!((delivered.len() as u64) < max_events);
                match delivered.last() {
                    Some(&final_at) => {
                        // The bounding (final) delivered event is the first to
                        // reach the bound; every earlier delivery is below it.
                        prop_assert!(final_at >= max_virtual_nanos);
                        prop_assert_eq!(sched.now().as_nanos(), final_at);
                        for &earlier in &delivered[..delivered.len() - 1] {
                            prop_assert!(earlier < max_virtual_nanos);
                        }
                    }
                    // No event delivered: only possible when the bound is 0, so
                    // now (ORIGIN) already reaches it on the first step.
                    None => prop_assert_eq!(max_virtual_nanos, 0),
                }
                // A still-pending event is at or after the final instant (which
                // is at or after the bound); it need not strictly exceed it.
                if let Some(np) = next_pending {
                    prop_assert!(np.as_nanos() >= sched.now().as_nanos());
                    prop_assert!(np.as_nanos() >= max_virtual_nanos);
                }
            }
            // The queue drained before either bound: neither budget had been
            // reached when the pop came up empty.
            EndReason::Quiescent => {
                prop_assert_eq!(sched.pending_events(), 0);
                prop_assert!(next_pending.is_none());
                prop_assert!((delivered.len() as u64) < max_events);
                prop_assert!(sched.now().as_nanos() < max_virtual_nanos);
            }
        }
    }
}
