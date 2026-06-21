//! Deterministic seams for the long-running `produce`/`consume` loops.
//!
//! Per the steering's "traits at crate seams" guidance, the Producer REPL
//! ([`crate::produce`], task 8.2) and the continuous Consumer
//! ([`crate::consume`], task 9.1) never touch wall-clock time, real stdin, or
//! real OS signals directly. Each external dependency is reached through a
//! trait so the loops can be driven on a paused virtual clock with scripted
//! input and a triggerable interrupt, making their timing and termination
//! behavior fully deterministic in tests (Requirements 6.x, 7.1, 7.2, 9.x,
//! 11.x).
//!
//! Three seams are defined here, each with the production implementation the
//! binary wires up:
//!
//! - [`Clock`] — `now`/`sleep`, over [`tokio::time`] ([`TokioClock`]); tests use
//!   `tokio::time::pause()`/`start_paused` virtual time.
//! - [`LineSource`] — an async line reader, over `BufReader<Stdin>::lines()`
//!   ([`StdinLines`]); tests feed a scripted sequence ending in `None` (EOF).
//! - [`Signal`] — a future that resolves on interrupt, over
//!   [`tokio::signal::ctrl_c`] ([`CtrlC`]); tests trigger it on demand.
//!
//! The seams mirror the cross-seam pattern already used in `cli.rs`'s tests
//! (`#[tonic::async_trait]` impls of the `VelaClient` service), so `vela-ctl`
//! gains no new dependency: `tonic::async_trait` is reused for the async trait
//! methods.

// The seams are defined ahead of their consumers: the Producer REPL
// (`crate::produce`, task 8.2) and the continuous Consumer (`crate::consume`,
// task 9.1) wire these up. Until those modules land, the production impls are
// unreferenced from `main`, so allow `dead_code` for this module only.
#![allow(dead_code)]

use std::io;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader, Lines, Stdin};
use tokio::time::Instant;

/// A source of time for the long-running loops.
///
/// All `Polling_Interval`, retry-backoff, `Metadata_TTL`, and discovery-timeout
/// waits go through this seam rather than calling [`tokio::time`] directly, so
/// tests advance a paused virtual clock instead of sleeping for real
/// (Requirements 9.2, 9.5, 11.2). [`now`](Clock::now) returns a
/// [`tokio::time::Instant`] so elapsed-time measurements honour the same virtual
/// clock that [`sleep`](Clock::sleep) waits on.
#[tonic::async_trait]
pub trait Clock: Send + Sync {
    /// The current instant, on the same timeline as [`sleep`](Clock::sleep).
    fn now(&self) -> Instant;

    /// Wait for `dur` to elapse on this clock's timeline.
    async fn sleep(&self, dur: Duration);
}

/// An async source of input lines for the Producer REPL.
///
/// The REPL awaits each line through this seam so a `select!` over it and a
/// [`Signal`] keeps the prompt responsive to Ctrl+C while blocked on input
/// (Requirement 7.2), and tests can script a finite sequence of lines that ends
/// in `None` to exercise the EOF-exit path (Requirement 6.6).
#[tonic::async_trait]
pub trait LineSource: Send {
    /// Read the next line, with its trailing newline stripped.
    ///
    /// Returns `Ok(Some(line))` for each line, `Ok(None)` at end-of-input, and
    /// `Err` if the underlying stream fails.
    async fn next_line(&mut self) -> io::Result<Option<String>>;
}

/// An interrupt signal for terminating a long-running loop.
///
/// [`interrupted`](Signal::interrupted) resolves when the operator asks to stop
/// (Ctrl+C in production). The loops `select!` over their work and this future
/// so an interrupt stops them promptly (Requirements 7.1, 11.1).
#[tonic::async_trait]
pub trait Signal: Send + Sync {
    /// Resolve once an interrupt has been received.
    async fn interrupted(&self);
}

/// Production [`Clock`] backed by [`tokio::time`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioClock;

#[tonic::async_trait]
impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep(&self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

/// Production [`LineSource`] reading lines from standard input.
///
/// Wraps `tokio::io::stdin()` in a [`BufReader`] and reads through its
/// [`Lines`] adapter, so the REPL reads real operator input one line at a time.
pub struct StdinLines {
    lines: Lines<BufReader<Stdin>>,
}

impl StdinLines {
    /// Build a [`StdinLines`] over the process's standard input.
    #[must_use]
    pub fn new() -> Self {
        Self {
            lines: BufReader::new(tokio::io::stdin()).lines(),
        }
    }
}

impl Default for StdinLines {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl LineSource for StdinLines {
    async fn next_line(&mut self) -> io::Result<Option<String>> {
        self.lines.next_line().await
    }
}

/// Production [`Signal`] backed by [`tokio::signal::ctrl_c`].
#[derive(Debug, Clone, Copy, Default)]
pub struct CtrlC;

#[tonic::async_trait]
impl Signal for CtrlC {
    async fn interrupted(&self) {
        // A failure to install the handler should not wedge the loop; treat it
        // as "no interrupt will arrive" by leaving the future pending forever so
        // the loop keeps running until its other branch (EOF/work) completes.
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::Arc;

    use tokio::sync::Notify;

    /// A scripted [`LineSource`] that yields its queued lines then `None`,
    /// standing in for stdin in tests (matches the design's
    /// `VecDeque<String>` test impl).
    struct ScriptedLines {
        lines: VecDeque<String>,
    }

    #[tonic::async_trait]
    impl LineSource for ScriptedLines {
        async fn next_line(&mut self) -> io::Result<Option<String>> {
            Ok(self.lines.pop_front())
        }
    }

    /// A triggerable [`Signal`] whose `interrupted` future resolves once
    /// [`Notify::notify_one`] has been called.
    #[derive(Clone, Default)]
    struct TriggerSignal {
        notify: Arc<Notify>,
    }

    #[tonic::async_trait]
    impl Signal for TriggerSignal {
        async fn interrupted(&self) {
            self.notify.notified().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tokio_clock_sleep_advances_virtual_time() {
        let clock = TokioClock;
        let start = clock.now();
        clock.sleep(Duration::from_millis(500)).await;
        // Under a paused clock, sleeping advances virtual time by exactly the
        // requested duration with no real waiting.
        assert_eq!(
            clock.now().duration_since(start),
            Duration::from_millis(500)
        );
    }

    #[tokio::test]
    async fn scripted_line_source_yields_lines_then_eof() {
        let mut lines = ScriptedLines {
            lines: VecDeque::from(vec!["a".to_string(), "b".to_string()]),
        };
        assert_eq!(lines.next_line().await.unwrap(), Some("a".to_string()));
        assert_eq!(lines.next_line().await.unwrap(), Some("b".to_string()));
        // Exhausting the script reports EOF, the REPL's zero-exit trigger.
        assert_eq!(lines.next_line().await.unwrap(), None);
    }

    #[tokio::test]
    async fn trigger_signal_resolves_only_after_notify() {
        let signal = TriggerSignal::default();

        // Before triggering, the interrupt future stays pending.
        tokio::select! {
            biased;
            () = signal.interrupted() => panic!("interrupt fired before notify"),
            () = tokio::task::yield_now() => {}
        }

        signal.notify.notify_one();
        // After triggering, the future resolves.
        signal.interrupted().await;
    }
}
