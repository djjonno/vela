//! The Producer REPL (`Produce_Repl`).
//!
//! [`run_repl`] turns the one-shot `produce` command into a long-running,
//! interactive prompt: it shows `> `, reads a line, produces it as a record,
//! prints the committed offset, and loops until the input stream ends or an
//! interrupt arrives (Requirements 6.1–6.6, 7.1, 7.2).
//!
//! The loop never touches stdin or OS signals directly. Input arrives through
//! the [`LineSource`] seam and termination through the [`Signal`] seam (both in
//! [`crate::seams`]), and the prompt/offset output is written to any
//! [`std::io::Write`]. The loop `select!`s over the next line and the interrupt
//! so the prompt stays responsive to Ctrl+C while it is blocked waiting for
//! input (Requirement 7.2), and a triggered interrupt stops reading and returns
//! at once (Requirement 7.1).
//!
//! Errors from a single `produce` are reported and the session continues —
//! a transient cluster error must not end an interactive session
//! (Requirement 6.5). End-of-input returns `Ok(())`, which the binary maps to a
//! zero exit status (Requirement 6.6).

// `run_repl` is wired into the `produce` command by task 10.2; until then it is
// unreferenced from `main`, so allow `dead_code` for this module only (mirroring
// `crate::seams`).
#![allow(dead_code)]

use std::io::{self, Write};

use vela_client::Producer;

use crate::cli::CtlError;
use crate::seams::{LineSource, Signal};

/// Run the Producer REPL until end-of-input or an interrupt.
///
/// Each iteration writes the `> ` prompt, then waits — via `tokio::select!` over
/// the [`LineSource`] and the [`Signal`] — for either the next line or an
/// interrupt (Requirement 6.1, 7.2). On a line, the value (keyed by `key` when
/// present, so every line of a keyed session uses that key — Requirement 6.4) is
/// produced and its committed offset printed before a fresh prompt
/// (Requirement 6.2, 6.3). A `produce` error is printed and the loop continues
/// without terminating (Requirement 6.5). End-of-input returns `Ok(())` for a
/// zero exit (Requirement 6.6); an interrupt stops reading and returns
/// (Requirement 7.1).
///
/// Returns [`CtlError`] only when writing the prompt/offset to `out` or reading
/// the next line fails at the I/O layer; ordinary produce failures do not end
/// the session.
pub async fn run_repl(
    producer: Producer,
    topic: String,
    key: Option<Vec<u8>>,
    mut lines: impl LineSource,
    signal: impl Signal,
    out: &mut impl Write,
) -> Result<(), CtlError> {
    loop {
        // Show the prompt and flush so it is visible before we block on input
        // (Requirement 6.1).
        write!(out, "> ").map_err(io_error)?;
        out.flush().map_err(io_error)?;

        // Wait for the next line or an interrupt. `biased` polls the interrupt
        // first so Ctrl+C wins promptly over a line that arrives simultaneously,
        // keeping the prompt responsive while blocked on input (Requirement 7.2).
        let next = tokio::select! {
            biased;
            () = signal.interrupted() => return Ok(()), // Requirement 7.1
            read = lines.next_line() => read,
        };

        match next {
            // A line: produce it and report the committed offset, then loop back
            // to a fresh prompt (Requirement 6.2, 6.3).
            Ok(Some(line)) => {
                match producer
                    .produce(&topic, key.as_deref(), line.into_bytes())
                    .await
                {
                    Ok(offset) => {
                        writeln!(out, "produced to topic '{topic}' at offset {offset}")
                            .map_err(io_error)?;
                    }
                    // A produce failure is reported but never ends the session
                    // (Requirement 6.5).
                    Err(err) => {
                        writeln!(out, "vela-ctl: {err}").map_err(io_error)?;
                    }
                }
            }
            // End-of-input: the stream closed, so exit with a zero status
            // (Requirement 6.6).
            Ok(None) => return Ok(()),
            // A read failure on the input stream ends the session as an I/O
            // (connection) error.
            Err(err) => return Err(io_error(err)),
        }
    }
}

/// Map a local I/O failure (prompt write or input read) to a [`CtlError`].
///
/// These are failures of the REPL's own terminal I/O rather than a cluster
/// rejection, so they are reported as a connection-class error.
fn io_error(err: io::Error) -> CtlError {
    CtlError::Connection(format!("repl I/O error: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tokio::sync::Notify;
    use vela_client::ClientCore;

    /// A scripted [`LineSource`] that yields its queued lines then `None` (EOF).
    struct ScriptedLines {
        lines: VecDeque<String>,
    }

    #[tonic::async_trait]
    impl LineSource for ScriptedLines {
        async fn next_line(&mut self) -> io::Result<Option<String>> {
            Ok(self.lines.pop_front())
        }
    }

    /// A scripted [`LineSource`] that also records how many times `next_line`
    /// was called, so a test can assert the REPL stopped reading early when an
    /// interrupt fires (Requirement 7.1).
    struct CountingLines {
        lines: VecDeque<String>,
        reads: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl LineSource for CountingLines {
        async fn next_line(&mut self) -> io::Result<Option<String>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.lines.pop_front())
        }
    }

    /// A [`Signal`] that never fires, so the loop runs to end-of-input.
    struct NeverSignal;

    #[tonic::async_trait]
    impl Signal for NeverSignal {
        async fn interrupted(&self) {
            std::future::pending::<()>().await;
        }
    }

    /// A triggerable [`Signal`] whose `interrupted` future resolves once
    /// [`Notify::notify_one`] has been called. Arming it *before* `run_repl`
    /// makes the loop's `biased` `select!` take the interrupt branch on its first
    /// poll, deterministically and with no real waiting (Requirement 7.1, 7.2).
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

    /// A producer over a core that is never dialed (used by the EOF-only path,
    /// where `produce` is never called).
    fn producer() -> Producer {
        Producer::new(Arc::new(ClientCore::new([(
            "node-a".to_string(),
            "http://node-a:50051".to_string(),
        )])))
    }

    /// A producer over a core with no bootstrap nodes, so every `produce`
    /// resolves to [`ClientError::NoNodes`](vela_client::ClientError) instantly —
    /// without touching the network or any clock. This keeps the prompt/EOF
    /// behavior under test deterministic while still exercising a real `produce`
    /// call per scripted line (whose failure the REPL reports and survives,
    /// Requirement 6.5).
    fn offline_producer() -> Producer {
        Producer::new(Arc::new(ClientCore::new(std::iter::empty())))
    }

    /// Compile-sanity: with no input, the REPL prints a single prompt and exits
    /// zero on EOF without producing anything (Requirement 6.1, 6.6). The full
    /// terminal/interrupt behavior is covered by task 8.4's tests.
    #[tokio::test]
    async fn empty_input_prints_prompt_and_exits_zero() {
        let mut out = Vec::<u8>::new();
        let result = run_repl(
            producer(),
            "orders".to_string(),
            None,
            ScriptedLines {
                lines: VecDeque::new(),
            },
            NeverSignal,
            &mut out,
        )
        .await;

        assert!(matches!(result, Ok(())), "EOF should exit zero: {result:?}");
        assert_eq!(String::from_utf8(out).expect("utf8 output"), "> ");
    }

    /// With several scripted lines followed by EOF, the REPL prompts once per
    /// line plus a final prompt and then exits zero on end-of-input
    /// (Requirement 6.1, 6.6). Each line drives a real `produce` whose failure is
    /// reported without ending the session (Requirement 6.5), so the loop keeps
    /// running to EOF regardless of produce outcome.
    #[tokio::test]
    async fn prompt_per_line_then_eof_exits_zero() {
        let scripted = vec!["one".to_string(), "two".to_string(), "three".to_string()];
        let mut out = Vec::<u8>::new();
        let result = run_repl(
            offline_producer(),
            "orders".to_string(),
            None,
            ScriptedLines {
                lines: VecDeque::from(scripted.clone()),
            },
            NeverSignal,
            &mut out,
        )
        .await;

        assert!(matches!(result, Ok(())), "EOF should exit zero: {result:?}");

        let output = String::from_utf8(out).expect("utf8 output");
        // One `> ` prompt is shown before each line is read, plus a final prompt
        // before the EOF read returns the session (Requirement 6.1, 6.6).
        assert_eq!(
            output.matches("> ").count(),
            scripted.len() + 1,
            "expected one prompt per line plus a final prompt, got: {output:?}",
        );
    }

    /// An interrupt that is already armed when the REPL begins waiting wins the
    /// loop's `biased` `select!`, so the REPL returns `Ok(())` without consuming
    /// any of the still-available scripted input (Requirement 7.1, 7.2).
    #[tokio::test]
    async fn interrupt_stops_reading_mid_session() {
        let reads = Arc::new(AtomicUsize::new(0));
        let signal = TriggerSignal::default();
        // Arm the interrupt before the loop starts: the prompt is written, then
        // the `select!` polls the interrupt branch first and takes it at once
        // (Requirement 7.2), so reading never begins.
        signal.notify.notify_one();

        let mut out = Vec::<u8>::new();
        let result = run_repl(
            producer(),
            "orders".to_string(),
            None,
            CountingLines {
                lines: VecDeque::from(vec![
                    "unread-a".to_string(),
                    "unread-b".to_string(),
                    "unread-c".to_string(),
                ]),
                reads: Arc::clone(&reads),
            },
            signal,
            &mut out,
        )
        .await;

        assert!(
            matches!(result, Ok(())),
            "an interrupt should stop the session with a zero exit: {result:?}",
        );
        // The interrupt won before any line was read, so the scripted input is
        // untouched — the REPL stopped reading rather than draining the input
        // (Requirement 7.1).
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "interrupt must stop reading before consuming any input",
        );
        // The prompt was still shown before the loop blocked on the interrupt
        // (Requirement 6.1), and nothing was produced.
        assert_eq!(String::from_utf8(out).expect("utf8 output"), "> ");
    }
}
