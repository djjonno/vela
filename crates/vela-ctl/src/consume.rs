//! The continuous consumer (`Consume_Loop`).
//!
//! [`run_consume`] turns the one-shot `consume` command into a long-running,
//! multi-partition reader. It discovers a topic's partitions, then spawns **one
//! independent poll task per partition** so a stuck, slow, or leaderless
//! partition can never stall or starve the others (Requirement 10.5). Each task
//! owns its partition's `Next_Offset` in process memory only — the consumer is a
//! standalone, non-committing reader that neither persists offsets nor joins a
//! consumer group (Requirement 8.8). Tasks report each record over an
//! [`mpsc`](tokio::sync::mpsc) channel to a single printer (the [`run_consume`]
//! body itself), which emits `partition, offset, value` so offsets stay
//! distinguishable across partitions (Requirement 9.6).
//!
//! All waits — the empty-poll `Polling_Interval` (Requirement 9.2, 9.5), the
//! zero-partition discovery retry, and the retryable-error backoff — go through
//! the injected [`Clock`] seam, so the loop's timing is fully deterministic on a
//! paused virtual clock in tests. Termination is driven by the [`Signal`] seam:
//! an interrupt cancels every poll task and the printer and returns promptly
//! (Requirement 11.1, 11.2). Cancellation is broadcast to the tasks through a
//! [`watch`](tokio::sync::watch) channel that every task `select!`s against, so
//! a task waiting on the clock or blocked mid-poll stops at once.
//!
//! # Leader resolution and resilience
//!
//! Per-partition reads go through [`Consumer::consume`], which dispatches to the
//! partition's believed leader and transparently re-resolves on a `NotLeader`
//! redirect or a transport failure (the leader cache is seeded from the
//! cluster's `Member_Address_Map` on first resolution). When an error still
//! surfaces — a partition with no elected leader
//! ([`ClientError::NoLeader`](vela_client::ClientError::NoLeader)), or a leader
//! that stayed unreachable for the whole retry budget — the task waits a
//! `Polling_Interval` and re-attempts rather than exiting, so a transient
//! leadership change never ends the session (Requirement 10.1, 10.2, 10.3,
//! 10.4).
//!
//! # Start offset (`Offset_Reset`)
//!
//! Each partition's initial `Next_Offset` comes from the [`OffsetReset`]
//! selector (Requirement 8.6, 8.7):
//!
//! - [`OffsetReset::Earliest`] → offset `0`, reading the partition from the
//!   beginning of its committed log.
//! - [`OffsetReset::Latest`] (the default) → the partition's latest committed
//!   offset, so only records produced after the session starts are read.
//!
//! There is no dedicated "latest committed offset" affordance in the
//! client-facing contract, and `ConsumeResponse.next_offset` is defined as
//! `request.offset + records_returned` (see `vela-server`'s consume handler), so
//! a single zero/one-max probe cannot report the end of the log. Following Open
//! Decision 2's default direction ("`latest` = the `next_offset` returned by a
//! probe poll"), the latest offset is therefore found by a *bounded probe that
//! drains to the end*: successive committed reads, discarding records, until a
//! poll returns empty — at which point `next_offset` is the latest committed
//! offset. The probe is non-committing like every other read and is cancellable
//! by an interrupt.

// `run_consume` is wired into the `consume` command by task 10.2; until then it
// is unreferenced from `main`, so allow `dead_code` for this module only
// (mirroring `crate::seams` and `crate::produce`).
#![allow(dead_code)]

use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use vela_client::{ClientError, Consumer, VelaClient};

use crate::cli::CtlError;
use crate::seams::{Clock, Signal};

/// How long to keep retrying partition discovery for a topic that exists but
/// reports zero partitions before giving up with a no-partitions error
/// (Requirement 8.5).
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on the in-flight record channel between the per-partition poll tasks
/// and the single printer. A bounded channel applies natural backpressure if
/// the printer falls behind a fast producer without growing unboundedly.
const RECORD_CHANNEL_CAPACITY: usize = 256;

/// The consumer's start-position selector (`Offset_Reset`).
///
/// Chooses where each partition's initial `Next_Offset` begins
/// (Requirement 8.6, 8.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OffsetReset {
    /// Start at each partition's latest committed offset, so only records
    /// produced after the session starts are read (the default,
    /// Requirement 8.6).
    #[default]
    Latest,
    /// Start at offset `0`, reading each partition from the beginning of its
    /// committed log (Requirement 8.7).
    Earliest,
}

/// A record handed from a per-partition poll task to the printer.
///
/// Carries the partition it came from so the printer can render `partition,
/// offset, value` and keep offsets distinguishable across partitions
/// (Requirement 9.6).
#[derive(Debug, Clone)]
struct PolledRecord {
    /// The partition the record was read from.
    partition: u32,
    /// The record's committed offset within that partition.
    offset: u64,
    /// The record's value bytes (rendered lossily as UTF-8 on output).
    value: Vec<u8>,
}

/// Run the continuous consumer until end-of-stream or an interrupt.
///
/// Discovers the topic's partitions (Requirement 8.1) — all of them, or only the
/// single `partition` supplied (Requirement 8.4) — spawns one independent poll
/// task per partition (Requirement 10.5), and prints every record each task
/// reports as `partition, offset, value` (Requirement 9.6). Each task seeds its
/// `Next_Offset` from `offset_reset`, then polls its partition's leader at
/// `next_offset`, waiting `poll_interval` between empty polls (Requirement 9.1,
/// 9.2) and recovering from retryable leadership errors rather than exiting
/// (Requirement 10.1–10.4). An interrupt from `signal` cancels every task and
/// the printer and returns `Ok(())` (Requirement 11.1, 11.2).
///
/// Returns a [`CtlError`] only for a terminal discovery failure — the topic does
/// not exist, reports no partitions before the discovery timeout
/// (Requirement 8.5), or the cluster is unreachable — or if writing a record to
/// `out` fails at the I/O layer.
// The argument list is the command's full dependency-injected surface (client,
// target, the two `Offset_Reset`/`Polling_Interval` knobs, and the three
// deterministic seams clock/signal/out); grouping them into a struct would only
// move the same fields behind one more indirection.
#[allow(clippy::too_many_arguments)]
pub async fn run_consume(
    client: VelaClient,
    topic: String,
    partition: Option<u32>,
    offset_reset: OffsetReset,
    poll_interval: Duration,
    clock: Arc<dyn Clock>,
    signal: impl Signal,
    out: &mut impl Write,
) -> Result<(), CtlError> {
    // 1. Determine the partition set. A supplied partition runs only that one
    //    (Requirement 8.4); otherwise discover the whole topic, retrying a
    //    zero-partition topic until a partition appears or the discovery timeout
    //    elapses (Requirement 8.1, 8.5). Discovery is interruptible.
    let partitions = match partition {
        Some(p) => vec![p],
        None => match discover_partitions(&client, &topic, poll_interval, clock.as_ref(), &signal)
            .await?
        {
            Some(partitions) => partitions,
            // Interrupted before consuming started: a clean, zero-status exit.
            None => return Ok(()),
        },
    };

    // 2. The record channel (tasks → printer) and the cancellation broadcast
    //    (printer/interrupt → tasks).
    let (records_tx, mut records_rx) = mpsc::channel::<PolledRecord>(RECORD_CHANNEL_CAPACITY);
    let (cancel_tx, cancel_rx) = watch::channel(false);

    // 3. Spawn one independent poll task per partition (Requirement 10.5). Each
    //    owns its `Next_Offset` and a sender clone; the printer keeps the
    //    receiver.
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(partitions.len());
    for p in partitions {
        let task = poll_partition(
            client.consumer(),
            topic.clone(),
            p,
            offset_reset,
            poll_interval,
            Arc::clone(&clock),
            records_tx.clone(),
            cancel_rx.clone(),
        );
        handles.push(tokio::spawn(task));
    }
    // Drop the template sender so `recv` can observe "all tasks ended" (every
    // task has exited and dropped its clone) as a `None`.
    drop(records_tx);

    // 4. Printer loop: render each reported record, staying responsive to an
    //    interrupt the whole time (Requirement 9.6, 11.1, 11.2). `biased` polls
    //    the interrupt first so Ctrl+C wins promptly over a record that arrives
    //    simultaneously.
    loop {
        tokio::select! {
            biased;
            () = signal.interrupted() => break,
            received = records_rx.recv() => match received {
                Some(record) => print_record(out, &record)?,
                // All poll tasks have ended and dropped their senders.
                None => break,
            }
        }
    }

    // 5. Cancel every task and wait for them to wind down, so the process does
    //    not exit while poll tasks are still running (Requirement 11.1). Sending
    //    is idempotent — the printer may have already broken on a closed channel.
    let _ = cancel_tx.send(true);
    for handle in handles {
        let _ = handle.await;
    }
    Ok(())
}

/// Discover a topic's partitions, retrying a zero-partition topic until one
/// appears or the discovery timeout elapses (Requirement 8.1, 8.5).
///
/// Returns `Ok(Some(partitions))` with `0..partition_count` once the topic
/// reports at least one partition, `Ok(None)` if an interrupt arrived before a
/// partition was found (a clean exit), or a [`CtlError`] for a topic that does
/// not exist, a cluster that is unreachable, or a zero-partition topic that
/// never gained a partition within [`DISCOVERY_TIMEOUT`]. All waits use the
/// injected `clock`, and the wait between retries is interruptible
/// (Requirement 11.2).
async fn discover_partitions(
    client: &VelaClient,
    topic: &str,
    poll_interval: Duration,
    clock: &dyn Clock,
    signal: &impl Signal,
) -> Result<Option<Vec<u32>>, CtlError> {
    let admin = client.admin();
    let start = clock.now();
    loop {
        // Race discovery against an interrupt so Ctrl+C stops a topic that is
        // slow to gain partitions.
        let described = tokio::select! {
            biased;
            () = signal.interrupted() => return Ok(None),
            result = admin.describe_topic(topic) => result,
        };

        match described {
            Ok(info) if info.partition_count > 0 => {
                return Ok(Some((0..info.partition_count).collect()));
            }
            // The topic exists but reports no partitions yet. Keep retrying on
            // the poll interval until one appears or the timeout elapses
            // (Requirement 8.5).
            Ok(_) => {
                if clock.now().duration_since(start) >= DISCOVERY_TIMEOUT {
                    return Err(CtlError::Cluster(ClientError::NoPartitions {
                        topic: topic.to_string(),
                    }));
                }
                tokio::select! {
                    biased;
                    () = signal.interrupted() => return Ok(None),
                    () = clock.sleep(poll_interval) => {}
                }
            }
            // A missing topic or an unreachable cluster is terminal: surface it
            // and let the binary exit non-zero (Requirement 1.4).
            Err(err) => return Err(classify_client_error(err)),
        }
    }
}

/// Continuously poll a single partition, reporting each record to the printer.
///
/// Seeds `Next_Offset` from `offset_reset` (Requirement 8.6, 8.7), then loops:
/// read committed records from the partition's leader at `next_offset`; on a
/// non-empty batch advance `next_offset` to the returned `next_offset`
/// (Requirement 9.4), forward each record, and poll again immediately to drain
/// any backlog; on an empty poll wait `poll_interval` and re-poll
/// (Requirement 9.1, 9.2, 9.3); on any error (a re-resolved-but-still-failing
/// leader, or a partition with no elected leader) wait `poll_interval` and retry
/// rather than exiting (Requirement 10.1–10.4). Every wait and the read itself
/// are `select!`ed against the cancellation channel, so an interrupt stops the
/// task at once (Requirement 11.1, 11.2). The held `Next_Offset` lives only here
/// in memory (Requirement 8.8).
// Each parameter is an independent input the spawned task owns (its consumer,
// target partition, the two start-offset/interval knobs, the clock, and the two
// channel endpoints); they have no natural grouping into a shared config type.
#[allow(clippy::too_many_arguments)]
async fn poll_partition(
    consumer: Consumer,
    topic: String,
    partition: u32,
    offset_reset: OffsetReset,
    poll_interval: Duration,
    clock: Arc<dyn Clock>,
    records: mpsc::Sender<PolledRecord>,
    mut cancel: watch::Receiver<bool>,
) {
    // Establish the start offset; a cancel during start-offset discovery ends
    // the task cleanly.
    let mut next_offset = match initial_offset(
        &consumer,
        &topic,
        partition,
        offset_reset,
        poll_interval,
        clock.as_ref(),
        &mut cancel,
    )
    .await
    {
        Some(offset) => offset,
        None => return,
    };

    // Tracks whether the current consecutive-failure streak has already been
    // reported, so a persistently failing partition warns once rather than on
    // every poll. Reset by any successful poll.
    let mut reported_error = false;

    loop {
        // Race the read against cancellation so an interrupt interrupts a poll
        // in flight (Requirement 11.1).
        let outcome = tokio::select! {
            biased;
            () = wait_cancel(&mut cancel) => return,
            result = consumer.consume(&topic, partition, next_offset, None) => result,
        };

        match outcome {
            // Records were returned: advance the offset (Requirement 9.4),
            // forward each one, and loop straight back to drain any remainder.
            Ok(batch) if !batch.records.is_empty() => {
                // A successful poll clears the failure streak, so a later error
                // warns afresh rather than staying silent.
                reported_error = false;
                next_offset = batch.next_offset;
                for consumed in batch.records {
                    let value = consumed
                        .record
                        .map(|record| record.value)
                        .unwrap_or_default();
                    let polled = PolledRecord {
                        partition,
                        offset: consumed.offset,
                        value,
                    };
                    // The printer has gone away (interrupt/shutdown): stop.
                    if records.send(polled).await.is_err() {
                        return;
                    }
                }
            }
            // Empty poll: wait the polling interval, then re-poll. Late records
            // arrive on a subsequent poll (Requirement 9.2, 9.3).
            Ok(_) => {
                reported_error = false;
                if wait_interval(clock.as_ref(), poll_interval, &mut cancel).await {
                    return;
                }
            }
            // Any error — a leader that stayed unreachable through the dispatch
            // retry budget, or a partition with no elected leader — is retryable
            // for a continuous consumer: wait and try again rather than exiting
            // (Requirement 10.1, 10.2, 10.3, 10.4). Warn once per failure streak
            // (on `stderr`, so it never corrupts the record stream on `stdout`)
            // so a persistently unreachable partition is visible to the operator
            // instead of silently producing nothing.
            Err(err) => {
                if !reported_error {
                    eprintln!("vela-ctl: partition {partition}: {err}; retrying");
                    reported_error = true;
                }
                if wait_interval(clock.as_ref(), poll_interval, &mut cancel).await {
                    return;
                }
            }
        }
    }
}

/// Resolve a partition's initial `Next_Offset` from the `Offset_Reset` selector.
///
/// `earliest` is offset `0` (Requirement 8.7); `latest` is the partition's
/// latest committed offset, found by [`probe_latest_offset`] (Requirement 8.6).
/// Returns `None` if an interrupt arrived while probing.
async fn initial_offset(
    consumer: &Consumer,
    topic: &str,
    partition: u32,
    offset_reset: OffsetReset,
    poll_interval: Duration,
    clock: &dyn Clock,
    cancel: &mut watch::Receiver<bool>,
) -> Option<u64> {
    match offset_reset {
        OffsetReset::Earliest => Some(0),
        OffsetReset::Latest => {
            probe_latest_offset(consumer, topic, partition, poll_interval, clock, cancel).await
        }
    }
}

/// Find a partition's latest committed offset by draining to the end of its
/// committed log (Open Decision 2's default direction; see the module docs).
///
/// Issues successive committed reads, discarding the records, advancing through
/// the returned `next_offset` until a poll returns empty — at which point
/// `next_offset` is the latest committed offset and so the offset of the next
/// record to be produced. The probe is non-committing, and a retryable read
/// error waits `poll_interval` and retries. Returns `None` if an interrupt
/// arrived first.
async fn probe_latest_offset(
    consumer: &Consumer,
    topic: &str,
    partition: u32,
    poll_interval: Duration,
    clock: &dyn Clock,
    cancel: &mut watch::Receiver<bool>,
) -> Option<u64> {
    let mut offset = 0;
    loop {
        let outcome = tokio::select! {
            biased;
            () = wait_cancel(cancel) => return None,
            result = consumer.consume(topic, partition, offset, None) => result,
        };

        match outcome {
            // End of the committed log reached: `next_offset` is the latest
            // committed offset (on an empty read it equals `offset`).
            Ok(batch) if batch.records.is_empty() => return Some(batch.next_offset),
            // More records remain; skip past them and keep draining.
            Ok(batch) => offset = batch.next_offset,
            // Retryable read error: wait and retry the probe.
            Err(_) => {
                if wait_interval(clock, poll_interval, cancel).await {
                    return None;
                }
            }
        }
    }
}

/// Render one record as `partition, offset, value` (Requirement 9.6).
///
/// The value bytes are rendered with [`String::from_utf8_lossy`] so arbitrary
/// payloads print readably without failing the session.
fn print_record(out: &mut impl Write, record: &PolledRecord) -> Result<(), CtlError> {
    let value = String::from_utf8_lossy(&record.value);
    writeln!(
        out,
        "partition {} offset {} value {}",
        record.partition, record.offset, value
    )
    .map_err(io_error)?;
    // Flush every record so it reaches the operator immediately. The continuous
    // consumer never exits on its own, and stdout is block-buffered when it is
    // not a TTY (piped, redirected, or captured), so without an explicit flush a
    // low-volume stream would sit in the buffer indefinitely and appear to print
    // nothing (Requirement 9.6).
    out.flush().map_err(io_error)
}

/// Wait until cancellation is signalled.
///
/// Resolves once the cancellation flag is set to `true`, or immediately if the
/// sender has been dropped (treated as cancellation so a task never hangs).
async fn wait_cancel(cancel: &mut watch::Receiver<bool>) {
    // `borrow_and_update` marks the current value seen so the next `changed`
    // awaits the *next* update rather than returning immediately.
    while !*cancel.borrow_and_update() {
        if cancel.changed().await.is_err() {
            return;
        }
    }
}

/// Wait `dur` on the injected clock, returning early if cancelled.
///
/// Returns `true` if cancellation interrupted the wait (the caller should stop),
/// or `false` if the full interval elapsed (the caller should continue). All
/// inter-poll waits go through this so they stay interruptible (Requirement 9.2,
/// 11.2) and deterministic on a paused clock.
async fn wait_interval(
    clock: &dyn Clock,
    dur: Duration,
    cancel: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        () = wait_cancel(cancel) => true,
        () = clock.sleep(dur) => false,
    }
}

/// Sort a [`ClientError`] from discovery into a [`CtlError`].
///
/// A transport-level `Unavailable` status means the cluster could not be reached
/// (a connection failure); every other error — including
/// [`ClientError::TopicNotFound`] and [`ClientError::NoPartitions`] — is a
/// cluster-side rejection. Mirrors the binary's own classification so the exit
/// status is consistent.
fn classify_client_error(err: ClientError) -> CtlError {
    if let ClientError::Rpc(status) = &err {
        if status.code() == tonic::Code::Unavailable {
            return CtlError::Connection(status.message().to_string());
        }
    }
    CtlError::Cluster(err)
}

/// Map a local output-write failure to a [`CtlError`].
///
/// A failure writing a record to `out` is the printer's own terminal I/O
/// failing rather than a cluster rejection, so it is reported as a
/// connection-class error.
fn io_error(err: io::Error) -> CtlError {
    CtlError::Connection(format!("consume output error: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::sync::Notify;
    use tokio::time::Instant;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
    use vela_proto::v1::{
        ConsumeRequest, ConsumeResponse, ConsumedRecord, CreateTopicRequest, CreateTopicResponse,
        DeleteTopicRequest, DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse,
        DescribeTopicRequest, DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse,
        ListTopicsRequest, ListTopicsResponse, LogBackend, PartitionInfo, ProduceBatchRequest,
        ProduceBatchResponse, ProduceRequest, ProduceResponse, Record, TopicInfo,
    };

    use crate::seams::TokioClock;

    /// A [`Signal`] pre-armed to fire immediately, so a loop that races against
    /// it stops at once.
    #[derive(Clone, Default)]
    struct FiredSignal;

    #[tonic::async_trait]
    impl Signal for FiredSignal {
        async fn interrupted(&self) {
            // Already interrupted: resolve immediately.
        }
    }

    /// A triggerable [`Signal`] whose `interrupted` future resolves once a
    /// permit has been stored with [`Notify::notify_one`].
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

    fn client() -> VelaClient {
        VelaClient::new([("node-a".to_string(), "http://127.0.0.1:1".to_string())])
    }

    /// The printer renders a record as `partition, offset, value`, keeping the
    /// partition visible so offsets stay distinguishable across partitions
    /// (Requirement 9.6).
    #[test]
    fn print_record_renders_partition_offset_and_value() {
        let mut out = Vec::<u8>::new();
        print_record(
            &mut out,
            &PolledRecord {
                partition: 3,
                offset: 42,
                value: b"hello".to_vec(),
            },
        )
        .expect("write to an in-memory buffer succeeds");
        assert_eq!(
            String::from_utf8(out).expect("utf8 output"),
            "partition 3 offset 42 value hello\n"
        );
    }

    /// A non-UTF-8 value is rendered lossily rather than failing the session.
    #[test]
    fn print_record_renders_non_utf8_value_lossily() {
        let mut out = Vec::<u8>::new();
        print_record(
            &mut out,
            &PolledRecord {
                partition: 0,
                offset: 0,
                value: vec![0xff, 0xfe],
            },
        )
        .expect("write succeeds");
        let rendered = String::from_utf8(out).expect("utf8 output");
        assert!(rendered.starts_with("partition 0 offset 0 value "));
        assert!(rendered.ends_with('\n'));
    }

    /// A transport `Unavailable` from discovery is a connection failure; any
    /// other client error is a cluster rejection.
    #[test]
    fn classify_client_error_distinguishes_connection_from_cluster() {
        let unavailable = ClientError::Rpc(Box::new(tonic::Status::unavailable("dial failed")));
        assert!(matches!(
            classify_client_error(unavailable),
            CtlError::Connection(_)
        ));

        let no_partitions = ClientError::NoPartitions {
            topic: "orders".to_string(),
        };
        assert!(matches!(
            classify_client_error(no_partitions),
            CtlError::Cluster(ClientError::NoPartitions { .. })
        ));
    }

    /// An interrupt that is already pending returns a clean zero-status exit
    /// before any record is read, even with a single supplied partition and an
    /// unreachable cluster (Requirement 11.1).
    #[tokio::test(flavor = "multi_thread")]
    async fn immediate_interrupt_exits_cleanly() {
        let mut out = Vec::<u8>::new();
        let result = run_consume(
            client(),
            "orders".to_string(),
            Some(0),
            OffsetReset::Earliest,
            Duration::from_millis(500),
            Arc::new(TokioClock),
            FiredSignal,
            &mut out,
        )
        .await;

        assert!(matches!(result, Ok(())), "interrupt exits zero: {result:?}");
    }

    /// Cancellation broadcast through the `watch` channel resolves a waiting
    /// task's [`wait_cancel`]; the helper also resolves if the sender is dropped.
    #[tokio::test]
    async fn wait_cancel_resolves_on_signal_and_on_drop() {
        let (tx, rx) = watch::channel(false);
        let mut rx_signalled = rx.clone();
        tx.send(true).expect("send cancel");
        // A set flag resolves immediately.
        wait_cancel(&mut rx_signalled).await;

        // A dropped sender also resolves (so a task never hangs).
        let mut rx_dropped = rx;
        drop(tx);
        wait_cancel(&mut rx_dropped).await;
    }

    /// A triggered interrupt stops discovery of a never-appearing topic without
    /// waiting out the discovery timeout (Requirement 11.2).
    #[tokio::test(flavor = "multi_thread")]
    async fn interrupt_stops_discovery_wait() {
        let signal = TriggerSignal::default();
        signal.notify.notify_one();
        let mut out = Vec::<u8>::new();
        // Single-partition mode skips discovery, so use all-partition mode to
        // reach the discovery path; the pre-armed interrupt wins the race.
        let result = run_consume(
            client(),
            "orders".to_string(),
            None,
            OffsetReset::Earliest,
            Duration::from_millis(500),
            Arc::new(TokioClock),
            signal,
            &mut out,
        )
        .await;
        assert!(matches!(result, Ok(())), "interrupt exits zero: {result:?}");
    }

    // -- Edge-case harness (task 9.3) --------------------------------------
    //
    // The edge cases below drive the real [`run_consume`] against an in-process
    // fake `VelaClient` gRPC server (the harness established by `cli.rs`'s
    // `example_tests` and `vela-client`'s consumer/admin routing tests). The
    // long-running loop is made deterministic through the injected seams: a
    // controllable [`Clock`] gates or advances every wait, a shared in-memory
    // writer lets a test observe rendered output, and a [`TriggerSignal`]
    // delivers the interrupt on demand.

    /// The node id the fake server names as the partition leader, registered to
    /// its own address so leader resolution resolves to the live server.
    const LEADER_ID: &str = "node-leader";

    /// An in-process fake of the client-facing `VelaClient` service for one
    /// node, backed by an in-memory committed log shared across partitions.
    ///
    /// `consume` returns every record from the requested offset to the end of
    /// the log (so the `latest` probe drains cleanly to the end) and records the
    /// `(partition, offset)` of every read, letting a test assert exactly which
    /// partitions and offsets were polled. `find_leader` always names
    /// [`LEADER_ID`]; `describe_topic` reports `partition_count`. The mutating
    /// RPCs (`produce`/`create`/`delete`) count their calls so a test can prove
    /// the standalone consumer never invokes them (Requirement 8.8).
    #[derive(Clone)]
    struct FakeConsumeNode {
        /// Partitions reported by `describe_topic` discovery.
        partition_count: u32,
        /// The committed log: index `i` holds the value at offset `i`. Shared
        /// across every partition for test simplicity.
        log: Arc<Vec<Vec<u8>>>,
        /// Every `(partition, offset)` a `consume` RPC requested, in order.
        consumed: Arc<Mutex<Vec<(u32, u64)>>>,
        consume_calls: Arc<AtomicU32>,
        describe_calls: Arc<AtomicU32>,
        produce_calls: Arc<AtomicU32>,
        create_calls: Arc<AtomicU32>,
        delete_calls: Arc<AtomicU32>,
    }

    impl FakeConsumeNode {
        /// A node serving `partition_count` partitions over the committed `log`.
        fn new(partition_count: u32, log: Vec<Vec<u8>>) -> Self {
            Self {
                partition_count,
                log: Arc::new(log),
                consumed: Arc::new(Mutex::new(Vec::new())),
                consume_calls: Arc::new(AtomicU32::new(0)),
                describe_calls: Arc::new(AtomicU32::new(0)),
                produce_calls: Arc::new(AtomicU32::new(0)),
                create_calls: Arc::new(AtomicU32::new(0)),
                delete_calls: Arc::new(AtomicU32::new(0)),
            }
        }

        /// The ordered `(partition, offset)` of every `consume` RPC served.
        fn consumed(&self) -> Vec<(u32, u64)> {
            self.consumed
                .lock()
                .expect("consumed mutex poisoned")
                .clone()
        }

        fn consume_calls(&self) -> u32 {
            self.consume_calls.load(Ordering::SeqCst)
        }

        fn describe_calls(&self) -> u32 {
            self.describe_calls.load(Ordering::SeqCst)
        }

        fn produce_calls(&self) -> u32 {
            self.produce_calls.load(Ordering::SeqCst)
        }

        fn create_calls(&self) -> u32 {
            self.create_calls.load(Ordering::SeqCst)
        }

        fn delete_calls(&self) -> u32 {
            self.delete_calls.load(Ordering::SeqCst)
        }
    }

    #[tonic::async_trait]
    impl VelaClientService for FakeConsumeNode {
        async fn consume(
            &self,
            request: Request<ConsumeRequest>,
        ) -> std::result::Result<Response<ConsumeResponse>, Status> {
            let request = request.into_inner();
            self.consume_calls.fetch_add(1, Ordering::SeqCst);
            self.consumed
                .lock()
                .expect("consumed mutex poisoned")
                .push((request.partition, request.offset));
            let start = request.offset as usize;
            let records: Vec<ConsumedRecord> = self
                .log
                .iter()
                .enumerate()
                .skip(start)
                .map(|(i, value)| ConsumedRecord {
                    offset: i as u64,
                    record: Some(Record {
                        key: None,
                        value: value.clone(),
                    }),
                })
                .collect();
            // Mirror the server's `next_offset = request.offset + records_returned`
            // contract so the `latest` probe terminates at the end of the log.
            let next_offset = request.offset + records.len() as u64;
            Ok(Response::new(ConsumeResponse {
                records,
                next_offset,
            }))
        }

        async fn find_leader(
            &self,
            _request: Request<FindLeaderRequest>,
        ) -> std::result::Result<Response<FindLeaderResponse>, Status> {
            Ok(Response::new(FindLeaderResponse {
                leader: Some(LEADER_ID.to_string()),
            }))
        }

        async fn describe_topic(
            &self,
            request: Request<DescribeTopicRequest>,
        ) -> std::result::Result<Response<DescribeTopicResponse>, Status> {
            self.describe_calls.fetch_add(1, Ordering::SeqCst);
            let name = request.into_inner().name;
            let partitions = (0..self.partition_count)
                .map(|index| PartitionInfo {
                    index,
                    replicas: vec![LEADER_ID.to_string()],
                    leader: Some(LEADER_ID.to_string()),
                })
                .collect();
            Ok(Response::new(DescribeTopicResponse {
                topic: Some(TopicInfo {
                    name,
                    partition_count: self.partition_count,
                    partitions,
                    log_backend: LogBackend::Durable as i32,
                }),
            }))
        }

        async fn describe_cluster(
            &self,
            _request: Request<DescribeClusterRequest>,
        ) -> std::result::Result<Response<DescribeClusterResponse>, Status> {
            // No member addresses: the client falls back to its id=url registry,
            // which already maps LEADER_ID to this server.
            Ok(Response::new(DescribeClusterResponse {
                members: vec![],
                epoch: 0,
            }))
        }

        async fn produce(
            &self,
            _request: Request<ProduceRequest>,
        ) -> std::result::Result<Response<ProduceResponse>, Status> {
            self.produce_calls.fetch_add(1, Ordering::SeqCst);
            Err(Status::unimplemented("produce is not used by the consumer"))
        }

        async fn produce_batch(
            &self,
            _request: Request<ProduceBatchRequest>,
        ) -> std::result::Result<Response<ProduceBatchResponse>, Status> {
            Err(Status::unimplemented(
                "produce_batch is not used by the consumer",
            ))
        }

        async fn create_topic(
            &self,
            _request: Request<CreateTopicRequest>,
        ) -> std::result::Result<Response<CreateTopicResponse>, Status> {
            self.create_calls.fetch_add(1, Ordering::SeqCst);
            Err(Status::unimplemented(
                "create_topic is not used by the consumer",
            ))
        }

        async fn delete_topic(
            &self,
            _request: Request<DeleteTopicRequest>,
        ) -> std::result::Result<Response<DeleteTopicResponse>, Status> {
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            Err(Status::unimplemented(
                "delete_topic is not used by the consumer",
            ))
        }

        async fn list_topics(
            &self,
            _request: Request<ListTopicsRequest>,
        ) -> std::result::Result<Response<ListTopicsResponse>, Status> {
            Err(Status::unimplemented(
                "list_topics is not used by the consumer",
            ))
        }
    }

    /// Bind a fake node on an OS-chosen localhost port and serve it on a
    /// background task. The listener is bound before returning, so the endpoint
    /// is already accepting connections — no startup race.
    async fn serve_consume(node: FakeConsumeNode) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        let service = VelaClientServer::new(node);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("fake server serves");
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Build a client whose only node is the fake server, registered under
    /// [`LEADER_ID`] so `find_leader` answers resolve to its address.
    fn client_for(addr: String) -> VelaClient {
        VelaClient::new([(LEADER_ID.to_string(), addr)])
    }

    /// An in-memory [`Write`] target the consumer renders into, readable by the
    /// test while the spawned loop keeps writing.
    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("output mutex poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// The text rendered into a shared output buffer so far.
    fn output_text(out: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(out.lock().expect("output mutex poisoned").clone()).expect("utf8 output")
    }

    /// Whether the rendered output contains `needle` yet.
    fn output_contains(out: &Arc<Mutex<Vec<u8>>>, needle: &str) -> bool {
        output_text(out).contains(needle)
    }

    /// Poll `predicate` until it holds, yielding between checks.
    ///
    /// Used to await a deterministic milestone the loop reaches over loopback
    /// (records rendered, a poll served, a wait entered) without coupling to
    /// wall-clock timing. Panics if the milestone is never reached, so a hung
    /// loop fails the test rather than blocking forever.
    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..2_000 {
            if predicate() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("condition was not met within the test timeout");
    }

    /// A [`Clock`] whose `sleep` advances virtual time instantly, so a
    /// time-bounded loop (the zero-partition discovery timeout) terminates with
    /// no real waiting. `now()` advances by exactly the slept duration so the
    /// elapsed-time bound still progresses.
    struct InstantClock {
        base: Instant,
        elapsed: Mutex<Duration>,
    }

    impl InstantClock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                elapsed: Mutex::new(Duration::ZERO),
            }
        }
    }

    #[tonic::async_trait]
    impl Clock for InstantClock {
        fn now(&self) -> Instant {
            self.base + *self.elapsed.lock().expect("clock mutex poisoned")
        }

        async fn sleep(&self, dur: Duration) {
            *self.elapsed.lock().expect("clock mutex poisoned") += dur;
        }
    }

    /// A [`Clock`] whose every `sleep` blocks until the test releases it,
    /// turning each inter-poll wait into an observable, controllable gate.
    ///
    /// `sleeps` counts how many waits have been entered (so a test can detect
    /// the loop parked in a wait), and each [`Notify::notify_one`] on `release`
    /// lets exactly one wait complete (so a test can advance the clock by one
    /// interval to release the next poll). A wait that is never released stays
    /// blocked, standing in for "the interval has not elapsed yet".
    #[derive(Default)]
    struct GateClock {
        sleeps: AtomicU32,
        release: Notify,
    }

    #[tonic::async_trait]
    impl Clock for GateClock {
        fn now(&self) -> Instant {
            Instant::now()
        }

        async fn sleep(&self, _dur: Duration) {
            self.sleeps.fetch_add(1, Ordering::SeqCst);
            self.release.notified().await;
        }
    }

    /// A single supplied partition is the only one read: only that partition is
    /// ever polled, and the topic's other partitions are never discovered
    /// (Requirement 8.4).
    #[tokio::test(flavor = "multi_thread")]
    async fn single_partition_mode_reads_only_the_supplied_partition() {
        // A 3-partition topic, but the operator asks for partition 1 only.
        let node = FakeConsumeNode::new(3, vec![b"a".to_vec(), b"b".to_vec()]);
        let addr = serve_consume(node.clone()).await;
        let client = client_for(addr);

        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let signal = TriggerSignal::default();
        let clock = Arc::new(GateClock::default());

        let task = {
            let output = Arc::clone(&output);
            let signal = signal.clone();
            let clock = Arc::clone(&clock);
            tokio::spawn(async move {
                let mut writer = SharedWriter(output);
                run_consume(
                    client,
                    "orders".to_string(),
                    Some(1),
                    OffsetReset::Earliest,
                    Duration::from_millis(500),
                    clock,
                    signal,
                    &mut writer,
                )
                .await
            })
        };

        // Wait until both records of partition 1 have been rendered.
        wait_until(|| output_contains(&output, "offset 1")).await;
        signal.notify.notify_one();
        let result = task.await.expect("consume task joins");
        assert!(matches!(result, Ok(())), "interrupt exits zero: {result:?}");

        // Every read targeted partition 1 — the other partitions were never
        // polled — and discovery was skipped entirely in single-partition mode.
        let consumed = node.consumed();
        assert!(!consumed.is_empty(), "the supplied partition was read");
        assert!(
            consumed.iter().all(|&(partition, _)| partition == 1),
            "only the supplied partition is read, got {consumed:?}",
        );
        assert_eq!(
            node.describe_calls(),
            0,
            "single-partition mode skips topic discovery",
        );
        // The records are rendered against the supplied partition (Req 9.6).
        let text = output_text(&output);
        assert!(
            text.contains("partition 1 offset 0 value a"),
            "got {text:?}"
        );
        assert!(
            text.contains("partition 1 offset 1 value b"),
            "got {text:?}"
        );
    }

    /// A topic that exists but reports zero partitions retries discovery until
    /// the discovery timeout elapses, then fails with `NoPartitions`
    /// (Requirement 8.5). The virtual clock advances past the timeout instantly.
    #[tokio::test(flavor = "multi_thread")]
    async fn zero_partition_topic_times_out_with_no_partitions() {
        let node = FakeConsumeNode::new(0, vec![]);
        let addr = serve_consume(node.clone()).await;
        let client = client_for(addr);

        let clock = Arc::new(InstantClock::new());
        // A never-triggered signal: discovery runs to its timeout rather than
        // being interrupted.
        let signal = TriggerSignal::default();
        let mut out = Vec::<u8>::new();

        let result = run_consume(
            client,
            "orders".to_string(),
            None,
            OffsetReset::Earliest,
            Duration::from_millis(500),
            clock,
            signal,
            &mut out,
        )
        .await;

        assert!(
            matches!(
                result,
                Err(CtlError::Cluster(ClientError::NoPartitions { ref topic })) if topic == "orders"
            ),
            "zero partitions past the discovery timeout is NoPartitions, got {result:?}",
        );
        assert!(
            node.describe_calls() >= 2,
            "discovery retried describe_topic before timing out, saw {}",
            node.describe_calls(),
        );
    }

    /// `earliest` starts each partition at offset 0, reading the committed log
    /// from the beginning (Requirement 8.7).
    #[tokio::test(flavor = "multi_thread")]
    async fn earliest_offset_reset_reads_from_the_beginning() {
        let node = FakeConsumeNode::new(1, vec![b"r0".to_vec(), b"r1".to_vec(), b"r2".to_vec()]);
        let addr = serve_consume(node.clone()).await;
        let client = client_for(addr);

        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let signal = TriggerSignal::default();
        let clock = Arc::new(GateClock::default());

        let task = {
            let output = Arc::clone(&output);
            let signal = signal.clone();
            let clock = Arc::clone(&clock);
            tokio::spawn(async move {
                let mut writer = SharedWriter(output);
                run_consume(
                    client,
                    "orders".to_string(),
                    Some(0),
                    OffsetReset::Earliest,
                    Duration::from_millis(500),
                    clock,
                    signal,
                    &mut writer,
                )
                .await
            })
        };

        wait_until(|| output_contains(&output, "offset 2")).await;
        signal.notify.notify_one();
        let result = task.await.expect("consume task joins");
        assert!(matches!(result, Ok(())), "interrupt exits zero: {result:?}");

        // Every committed record from offset 0 onward was delivered, in order.
        let text = output_text(&output);
        assert!(
            text.contains("partition 0 offset 0 value r0"),
            "got {text:?}"
        );
        assert!(
            text.contains("partition 0 offset 1 value r1"),
            "got {text:?}"
        );
        assert!(
            text.contains("partition 0 offset 2 value r2"),
            "got {text:?}"
        );
        // The very first read started at offset 0 (the beginning of the log).
        assert_eq!(
            node.consumed().first().copied(),
            Some((0, 0)),
            "earliest's first read is at offset 0",
        );
    }

    /// `latest` starts each partition at the end of the committed log: the
    /// pre-existing records are drained by the probe and never delivered, and
    /// the steady-state poll begins at the end offset (Requirement 8.6).
    #[tokio::test(flavor = "multi_thread")]
    async fn latest_offset_reset_starts_at_the_end_of_the_log() {
        let node = FakeConsumeNode::new(1, vec![b"r0".to_vec(), b"r1".to_vec(), b"r2".to_vec()]);
        let addr = serve_consume(node.clone()).await;
        let client = client_for(addr);

        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let signal = TriggerSignal::default();
        let clock = Arc::new(GateClock::default());

        let task = {
            let output = Arc::clone(&output);
            let signal = signal.clone();
            let clock = Arc::clone(&clock);
            tokio::spawn(async move {
                let mut writer = SharedWriter(output);
                run_consume(
                    client,
                    "orders".to_string(),
                    Some(0),
                    OffsetReset::Latest,
                    Duration::from_millis(500),
                    clock,
                    signal,
                    &mut writer,
                )
                .await
            })
        };

        // Wait until the probe has drained to the end and the steady-state poll
        // has parked in its first interval wait.
        wait_until(|| clock.sleeps.load(Ordering::SeqCst) >= 1).await;
        signal.notify.notify_one();
        let result = task.await.expect("consume task joins");
        assert!(matches!(result, Ok(())), "interrupt exits zero: {result:?}");

        // None of the three pre-existing records were delivered: `latest` skips
        // everything committed before the session started.
        assert_eq!(
            output_text(&output),
            "",
            "latest delivers no pre-existing records",
        );
        // The steady-state poll begins at the end of the log (offset 3), never
        // re-reading the drained records.
        assert_eq!(
            node.consumed().last().copied(),
            Some((0, 3)),
            "the steady-state poll starts at the end offset, got {:?}",
            node.consumed(),
        );
    }

    /// The consumer is standalone and non-committing: it never calls a mutating
    /// RPC, holds offsets in memory only, and so a fresh run restarts at the
    /// beginning and re-delivers every record (Requirement 8.8).
    #[tokio::test(flavor = "multi_thread")]
    async fn consumer_is_non_committing_and_resets_offsets_on_a_fresh_run() {
        let node = FakeConsumeNode::new(1, vec![b"x".to_vec(), b"y".to_vec()]);
        let addr = serve_consume(node.clone()).await;

        // Run a session twice, each over a brand-new client/consumer, asserting
        // both read from the start and deliver every record.
        for run in 0..2 {
            let client = client_for(addr.clone());
            let output = Arc::new(Mutex::new(Vec::<u8>::new()));
            let signal = TriggerSignal::default();
            let clock = Arc::new(GateClock::default());

            let task = {
                let output = Arc::clone(&output);
                let signal = signal.clone();
                let clock = Arc::clone(&clock);
                tokio::spawn(async move {
                    let mut writer = SharedWriter(output);
                    run_consume(
                        client,
                        "orders".to_string(),
                        Some(0),
                        OffsetReset::Earliest,
                        Duration::from_millis(500),
                        clock,
                        signal,
                        &mut writer,
                    )
                    .await
                })
            };

            wait_until(|| output_contains(&output, "offset 1")).await;
            signal.notify.notify_one();
            let result = task.await.expect("consume task joins");
            assert!(
                matches!(result, Ok(())),
                "run {run} interrupt exits zero: {result:?}",
            );
            let text = output_text(&output);
            assert!(
                text.contains("partition 0 offset 0 value x"),
                "run {run} re-delivers offset 0, got {text:?}",
            );
            assert!(
                text.contains("partition 0 offset 1 value y"),
                "run {run} re-delivers offset 1, got {text:?}",
            );
        }

        // No mutating RPC was ever called — there is no commit RPC, and the
        // consumer never produces or alters topics (Requirement 8.8).
        assert_eq!(node.produce_calls(), 0, "the consumer never produces");
        assert_eq!(node.create_calls(), 0, "the consumer never creates topics");
        assert_eq!(node.delete_calls(), 0, "the consumer never deletes topics");
        // Each fresh run restarted at offset 0, proving offsets are in-memory
        // only and never persisted across runs.
        let zero_reads = node
            .consumed()
            .iter()
            .filter(|&&(partition, offset)| partition == 0 && offset == 0)
            .count();
        assert!(
            zero_reads >= 2,
            "each fresh run restarts at offset 0, got {:?}",
            node.consumed(),
        );
    }

    /// An empty poll waits the polling interval before re-polling: no second
    /// poll happens until the clock advances by one interval (Requirement 9.2).
    #[tokio::test(flavor = "multi_thread")]
    async fn empty_poll_waits_the_interval_before_repolling() {
        // An empty log, so every poll returns no records.
        let node = FakeConsumeNode::new(1, vec![]);
        let addr = serve_consume(node.clone()).await;
        let client = client_for(addr);

        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let signal = TriggerSignal::default();
        let clock = Arc::new(GateClock::default());

        let task = {
            let signal = signal.clone();
            let clock = Arc::clone(&clock);
            tokio::spawn(async move {
                let mut writer = SharedWriter(output);
                run_consume(
                    client,
                    "orders".to_string(),
                    Some(0),
                    OffsetReset::Earliest,
                    Duration::from_millis(500),
                    clock,
                    signal,
                    &mut writer,
                )
                .await
            })
        };

        // The first poll returns empty and the loop parks in the interval wait.
        wait_until(|| clock.sleeps.load(Ordering::SeqCst) >= 1).await;
        assert_eq!(
            node.consume_calls(),
            1,
            "exactly one poll before the interval has elapsed",
        );
        // Give the loop a chance to (incorrectly) re-poll: while the interval is
        // unelapsed it must not.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            node.consume_calls(),
            1,
            "no re-poll until the polling interval elapses",
        );

        // Advancing the clock by one interval releases exactly one more poll.
        clock.release.notify_one();
        wait_until(|| node.consume_calls() >= 2).await;

        signal.notify.notify_one();
        let result = task.await.expect("consume task joins");
        assert!(matches!(result, Ok(())), "interrupt exits zero: {result:?}");
    }

    /// An interrupt delivered while the loop is waiting between polls stops the
    /// session promptly and exits with `Ok` (Requirement 11.1, 11.2).
    #[tokio::test(flavor = "multi_thread")]
    async fn interrupt_during_interval_wait_exits_promptly() {
        let node = FakeConsumeNode::new(1, vec![]);
        let addr = serve_consume(node.clone()).await;
        let client = client_for(addr);

        let output = Arc::new(Mutex::new(Vec::<u8>::new()));
        let signal = TriggerSignal::default();
        // The gate is never released, so the loop stays parked in the inter-poll
        // wait until the interrupt arrives.
        let clock = Arc::new(GateClock::default());

        let task = {
            let signal = signal.clone();
            let clock = Arc::clone(&clock);
            tokio::spawn(async move {
                let mut writer = SharedWriter(output);
                run_consume(
                    client,
                    "orders".to_string(),
                    Some(0),
                    OffsetReset::Earliest,
                    Duration::from_millis(500),
                    clock,
                    signal,
                    &mut writer,
                )
                .await
            })
        };

        // Wait until the loop has polled once (empty) and parked in the wait.
        wait_until(|| clock.sleeps.load(Ordering::SeqCst) >= 1).await;

        // Interrupting mid-wait stops the session promptly with a zero status.
        signal.notify.notify_one();
        let result = task.await.expect("consume task joins");
        assert!(
            matches!(result, Ok(())),
            "interrupt while waiting exits zero: {result:?}",
        );
    }
}
