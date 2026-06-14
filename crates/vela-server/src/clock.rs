//! Real-clock timer source for the partition drivers.
//!
//! `vela-raft` injects time through the [`Clock`] trait so the consensus core
//! stays deterministic and simulatable. In the server, the live implementation
//! [`TimerClock`] turns each `arm` into a `tokio` sleep that, when it elapses,
//! posts a [`DriverCommand::Tick`](crate::driver::DriverCommand::Tick) onto the
//! owning partition driver's queue. Delivering timer events as messages on the
//! same queue is exactly the *Runtime Model* the design prescribes: the driver
//! sees ticks interleaved with RPCs and proposals in one ordered stream.
//!
//! ## Reset semantics via generations
//!
//! Raft re-arms its election timer whenever it hears from a valid leader or
//! grants a vote, which must cancel the previously-armed timeout. Rather than
//! track and abort `tokio` sleeps, each `arm` bumps a per-kind **generation**
//! counter and the spawned sleep carries the generation it was armed with. When
//! the tick arrives, the driver asks [`TimerClock::is_current`] whether that
//! generation is still the latest for its kind; a tick from a superseded arming
//! is silently dropped. This yields correct timer-reset behaviour without any
//! cancellation machinery.
//!
//! ## Election randomization
//!
//! Raft arms the election timer with a fixed base; the requirement's randomized
//! 150–300 ms window (Requirement 7.2) is produced here by adding a jitter in
//! `[0, base)` to election timers (heartbeats fire at their exact interval).
//! The jitter is drawn from a cheap process-time source so no RNG dependency is
//! needed; election spreading only needs approximate independence between
//! replicas.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use vela_raft::{Clock, TimerKind};

use crate::driver::{DriverCommand, DriverHandle};

/// A [`Clock`] backed by `tokio` timers that posts ticks to a driver queue.
pub struct TimerClock {
    /// Sender onto the owning driver's command queue.
    tx: DriverHandle,
    /// Latest arming generation of the election timer.
    election_generation: u64,
    /// Latest arming generation of the heartbeat timer.
    heartbeat_generation: u64,
}

impl TimerClock {
    /// Create a clock that delivers ticks onto `tx`.
    pub fn new(tx: DriverHandle) -> Self {
        Self {
            tx,
            election_generation: 0,
            heartbeat_generation: 0,
        }
    }

    /// Whether `generation` is the latest arming of `kind`. The driver uses this
    /// to drop ticks from a since-reset timer.
    pub fn is_current(&self, kind: TimerKind, generation: u64) -> bool {
        generation
            == match kind {
                TimerKind::Election => self.election_generation,
                TimerKind::Heartbeat => self.heartbeat_generation,
            }
    }

    /// Bump and return the next generation for `kind`.
    fn next_generation(&mut self, kind: TimerKind) -> u64 {
        let slot = match kind {
            TimerKind::Election => &mut self.election_generation,
            TimerKind::Heartbeat => &mut self.heartbeat_generation,
        };
        *slot += 1;
        *slot
    }

    /// The actual delay to wait for a timer of `kind` armed for `base`.
    ///
    /// Election timers add a jitter in `[0, base)` to spread the randomized
    /// 150–300 ms window across replicas (Requirement 7.2); heartbeats fire at
    /// exactly their interval (Requirement 7.6).
    fn delay_for(kind: TimerKind, base: Duration) -> Duration {
        match kind {
            TimerKind::Election => base + election_jitter(base),
            TimerKind::Heartbeat => base,
        }
    }
}

impl Clock for TimerClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn arm(&mut self, kind: TimerKind, dur: Duration) {
        let generation = self.next_generation(kind);
        let delay = Self::delay_for(kind, dur);
        let tx = self.tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            // If the driver has stopped, the send fails harmlessly.
            let _ = tx.send(DriverCommand::Tick { kind, generation });
        });
    }
}

/// A jitter in `[0, base)` drawn from a cheap process-time source.
///
/// Election randomness only needs rough independence between replicas to avoid
/// repeated split votes, so a sub-microsecond reading of the wall clock is a
/// sufficient entropy source without pulling in an RNG crate.
fn election_jitter(base: Duration) -> Duration {
    let base_nanos = base.as_nanos().max(1);
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u128)
        .unwrap_or(0);
    Duration::from_nanos((entropy % base_nanos) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn arming_bumps_the_generation_per_kind() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut clock = TimerClock::new(tx);

        // Each kind tracks its own generation independently.
        assert!(!clock.is_current(TimerKind::Election, 1));
        let g1 = clock.next_generation(TimerKind::Election);
        assert_eq!(g1, 1);
        assert!(clock.is_current(TimerKind::Election, 1));

        let g2 = clock.next_generation(TimerKind::Election);
        assert_eq!(g2, 2);
        // The previous generation is now stale.
        assert!(!clock.is_current(TimerKind::Election, 1));
        assert!(clock.is_current(TimerKind::Election, 2));

        // Heartbeat generations are separate.
        assert!(clock.is_current(TimerKind::Heartbeat, 0));
        assert_eq!(clock.next_generation(TimerKind::Heartbeat), 1);
        assert!(clock.is_current(TimerKind::Heartbeat, 1));
    }

    #[test]
    fn heartbeat_delay_is_exact_and_election_delay_is_within_window() {
        let base = Duration::from_millis(150);
        assert_eq!(
            TimerClock::delay_for(TimerKind::Heartbeat, base),
            base,
            "heartbeats fire at their exact interval"
        );
        for _ in 0..1000 {
            let delay = TimerClock::delay_for(TimerKind::Election, base);
            assert!(
                delay >= base && delay < base * 2,
                "election delay {delay:?} must lie in [base, 2*base)"
            );
        }
    }

    #[tokio::test]
    async fn armed_election_timer_delivers_a_tick() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut clock = TimerClock::new(tx);
        clock.arm(TimerKind::Heartbeat, Duration::from_millis(5));

        let command = rx.recv().await.expect("a tick must be delivered");
        match command {
            DriverCommand::Tick { kind, generation } => {
                assert_eq!(kind, TimerKind::Heartbeat);
                assert_eq!(generation, 1);
                assert!(clock.is_current(kind, generation));
            }
            _ => panic!("expected a Tick command"),
        }
    }
}
