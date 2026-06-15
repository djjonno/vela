//! Monotonic clock seam for pacing the `Periodic` sync policy.
//!
//! The `Periodic` [`SyncPolicy`](super::config::SyncPolicy) forces buffered
//! Record_Frames to stable storage at most `interval_ms` apart **while the log
//! is actively servicing mutating operations** (Requirement 4.2). That pacing
//! is operation-driven — there is no background thread — so the WAL needs a way
//! to read wall-clock time, and tests need a way to make that time
//! deterministic.
//!
//! This module provides exactly that seam, mirroring the [`fs`](super::fs) seam:
//!
//! - [`Clock`] — a minimal trait returning a monotonic millisecond reading.
//! - [`RealClock`] — the production clock, backed by [`std::time::Instant`].
//! - [`test_clock::TestClock`] — a manually-advanceable clock for tests.
//!
//! `DurableWal` is generic over the clock (`DurableWal<F, C: Clock = RealClock>`)
//! so production carries a zero-cost [`RealClock`] while a cadence test injects a
//! [`TestClock`](test_clock::TestClock) through `open_with_clock`. The default
//! type parameter keeps every existing `DurableWal<F>` reference compiling
//! unchanged.
//!
//! Only **differences** between readings are meaningful; the epoch is
//! unspecified. The WAL records the time of its last force and compares a later
//! reading against it, so the clock need not be anchored to any absolute time.

// `allow(dead_code)`: `RealClock` and `Clock` are exercised by the `Periodic`
// policy branches in `mod.rs` (always compiled), and `TestClock` is test-only.
// The `Clone`/`Debug` derives and the unused-on-some-builds constructors carry
// no caller on every build configuration, so this scopes the lint the same way
// the sibling seams (`fs.rs`, `manifest.rs`) do.
#![allow(dead_code)]

use std::time::Instant;

/// A monotonic millisecond clock the WAL reads to pace `Periodic` forces.
///
/// Implementations need only provide a non-decreasing reading whose
/// *differences* measure elapsed wall-clock time; the absolute value and epoch
/// are unspecified. The trait is object-safe-irrelevant here — `DurableWal`
/// carries the concrete clock as a type parameter, with no dynamic dispatch.
pub(crate) trait Clock {
    /// The current monotonic time, in milliseconds. Only differences between
    /// two readings are meaningful.
    fn now_millis(&self) -> u64;
}

/// The production [`Clock`], backed by a monotonic [`std::time::Instant`].
///
/// Each clock captures an [`Instant`] at construction and reports milliseconds
/// elapsed since then, which is monotonic and immune to wall-clock adjustments.
/// A `DurableWal` only ever compares readings from its own clock, so the
/// per-instance epoch is sufficient.
#[derive(Debug, Clone)]
pub struct RealClock {
    /// The instant this clock was created; readings are elapsed time from here.
    start: Instant,
}

impl RealClock {
    /// Construct a real clock anchored at the current instant.
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RealClock {
    fn now_millis(&self) -> u64 {
        // Saturating on the `u128 -> u64` narrowing is a non-concern: it would
        // require a single log instance to run for ~585 million years.
        self.start.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
pub(crate) mod test_clock {
    //! A manually-advanceable [`Clock`] for deterministic cadence tests.

    use super::Clock;
    use std::cell::Cell;
    use std::rc::Rc;

    /// A [`Clock`] whose time is set by the test, not the wall clock.
    ///
    /// The time lives behind a shared `Rc<Cell<_>>`, so a test holds one clone
    /// to advance time while the `DurableWal` holds another to read it — the
    /// two observe the same value. `DurableWal` is single-writer and need not be
    /// `Sync`, so the non-atomic `Rc<Cell<_>>` is the natural choice (matching
    /// the `Cell`-based poison flag in `mod.rs`).
    #[derive(Debug, Clone, Default)]
    pub(crate) struct TestClock {
        /// Shared current time in milliseconds; starts at `0`.
        now: Rc<Cell<u64>>,
    }

    impl TestClock {
        /// Construct a clock reading `0` milliseconds.
        pub(crate) fn new() -> Self {
            Self {
                now: Rc::new(Cell::new(0)),
            }
        }

        /// Advance the clock by `millis` milliseconds.
        pub(crate) fn advance(&self, millis: u64) {
            self.now.set(self.now.get() + millis);
        }
    }

    impl Clock for TestClock {
        fn now_millis(&self) -> u64 {
            self.now.get()
        }
    }
}
