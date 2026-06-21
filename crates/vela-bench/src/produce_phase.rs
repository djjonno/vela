//! The Producer_Phase: warmup, concurrency, and the Measurement_Window.
//!
//! The Producer_Phase drives the configured Workload into the target topic
//! through a [`ProduceSink`] seam and reports how much measured work was
//! acknowledged and over what wall-clock window (Requirements 1.1, 1.2, 1.4,
//! 4.4, 10.1, 10.4).
//!
//! The phase is structured as:
//!
//! 1. **Warmup** — issue exactly `warmup` produce operations first; they all
//!    complete *before* the Measurement_Window opens (Requirement 10.1). A
//!    warmup failure aborts the phase without ever opening the window, mapping
//!    to `FailureReason::WarmupFailed` (Requirement 10.6).
//! 2. **Measured** — open the window at the first measured produce invocation
//!    (Requirement 1.4), then issue the remaining `record_count - warmup`
//!    records keeping up to `producer_concurrency` requests in flight via
//!    [`futures::stream::StreamExt::buffer_unordered`] (Requirement 4.4). A
//!    record is counted only after it returns `Ok(offset)` and only when it is
//!    a measured (non-warmup) record (Requirement 1.2). The window closes at
//!    the acknowledgment of the last measured record (Requirement 1.4).
//!
//! Any `Err` from a produce stops further produces and aborts the phase,
//! carrying the topic, partition, and cause so the harness can build
//! `FailureReason::ProduceError` (Requirements 3.7, 9.1).
//!
//! ## The [`ProduceSink`] seam
//!
//! The real produce call is `client.producer().produce(topic, key, value)`.
//! To keep this phase unit-testable without a live cluster, the phase depends
//! only on the [`ProduceSink`] trait; the real `VelaClient` adapter is wired in
//! by the run harness (`run.rs`), and tests drive a fake sink. The pure
//! [`measured_set`] helper — the warmup/measured selection rule — is exposed
//! independently so it can be property-tested in isolation.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::StreamExt;
use thiserror::Error;

use crate::outcome::{FailureReason, Phase};
use crate::params::WorkloadParameters;
use crate::workload::{key_for, payload_for};

/// The set of positions counted toward Produce_Throughput: `[warmup, total)`.
///
/// The first `warmup` positions (`0..warmup`) are warmup operations excluded
/// from the Measurement_Window; the returned range is exactly the measured
/// positions, so its length is `total - warmup` (Requirements 1.2, 10.1, 10.4).
/// When `warmup == 0` the range is `0..total` — every position, including
/// position 0, is measured (Requirement 10.4).
///
/// This is a pure function over `(total, warmup)`. `warmup` is clamped to
/// `total` so an out-of-range `warmup >= total` yields an empty range rather
/// than an invalid one; the harness rejects that configuration earlier via
/// [`WorkloadParameters::validate`].
#[must_use]
pub fn measured_set(total: u64, warmup: u64) -> std::ops::Range<u64> {
    let start = warmup.min(total);
    start..total
}

/// A failed produce operation, carrying the location and cause needed to build
/// `FailureReason::ProduceError` (Requirements 3.7, 9.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceFailure {
    /// The target topic.
    pub topic: String,
    /// The target partition.
    pub partition: u32,
    /// The underlying error cause.
    pub cause: String,
}

/// The producer-side seam the Producer_Phase drives.
///
/// A `produce` either acknowledges the record — returning its committed
/// `offset` — or fails with a [`ProduceFailure`]. The real implementation wraps
/// `client.producer().produce(...)`; tests provide a fake. `position` is the
/// record's 0-based position so a sink can correlate calls; `key` and `value`
/// are the deterministic payload bytes from [`key_for`] / [`payload_for`].
#[async_trait]
pub trait ProduceSink {
    /// Produce the record at `position`, returning its committed offset on
    /// acknowledgment or a [`ProduceFailure`] on an unresolved error.
    async fn produce(
        &self,
        position: u64,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<u64, ProduceFailure>;
}

/// The measured result of a completed Producer_Phase.
///
/// `acked_count` and `acked_value_bytes` count only measured (non-warmup)
/// Acknowledged_Records, the figures fed to `metrics::throughput` so
/// Produce_Throughput reflects the measured subset only (Requirement 1.2).
///
/// `total_acked_count` and `total_acked_value_bytes` count **every**
/// Acknowledged_Record — warmup *and* measured — because the glossary defines an
/// Acknowledged_Record as any produced record that received a committed offset,
/// which includes the warmup records. These totals are what the harness uses to
/// drive the Consumer_Phase stop condition and data-integrity verification, and
/// to populate the report's acknowledged-record count and total payload bytes
/// (Requirements 2.2, 5.1, 6.1). With `warmup == 0` the totals equal the
/// measured figures.
///
/// `window` is the wall-clock interval from the first measured produce
/// invocation to the last measured acknowledgment (Requirement 1.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerPhaseResult {
    /// Number of measured Acknowledged_Records (excludes warmup), for throughput.
    pub acked_count: u64,
    /// Sum of the value byte lengths of the measured Acknowledged_Records.
    pub acked_value_bytes: u64,
    /// Total Acknowledged_Records, warmup included (`warmup acked + measured`).
    pub total_acked_count: u64,
    /// Total value bytes of all Acknowledged_Records, warmup included.
    pub total_acked_value_bytes: u64,
    /// The Producer_Phase Measurement_Window duration.
    pub window: Duration,
}

/// Why a Producer_Phase aborted.
///
/// A phase-local error the harness maps to a [`FailureReason`] via
/// [`ProducerPhaseError::into_failure_reason`]. A warmup failure aborts before
/// the window opens (Requirement 10.6); a measured produce failure aborts the
/// run (Requirements 3.7, 9.1).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProducerPhaseError {
    /// A warmup produce failed before the Measurement_Window opened.
    #[error("warmup produce failed: {cause}")]
    Warmup {
        /// The underlying error cause.
        cause: String,
    },
    /// A measured produce surfaced an error the client retry path did not
    /// resolve.
    #[error("produce failed on topic `{topic}` partition {partition}: {cause}")]
    Produce {
        /// The target topic.
        topic: String,
        /// The target partition.
        partition: u32,
        /// The underlying error cause.
        cause: String,
    },
}

impl ProducerPhaseError {
    /// Map this phase-local error to the report-level [`FailureReason`].
    #[must_use]
    pub fn into_failure_reason(self) -> FailureReason {
        match self {
            ProducerPhaseError::Warmup { cause } => FailureReason::WarmupFailed {
                phase: Phase::Produce,
                cause,
            },
            ProducerPhaseError::Produce {
                topic,
                partition,
                cause,
            } => FailureReason::ProduceError {
                topic,
                partition,
                cause,
            },
        }
    }
}

/// Run the Producer_Phase against `sink` for the given Workload_Parameters.
///
/// Issues exactly `params.warmup` warmup produces first, awaiting their
/// completion before opening the Measurement_Window (Requirement 10.1); a
/// warmup failure aborts with [`ProducerPhaseError::Warmup`] without opening the
/// window (Requirement 10.6). It then opens the window and issues the measured
/// records (`measured_set(record_count, warmup)`) keeping up to
/// `producer_concurrency` requests in flight (Requirement 4.4), counting a
/// record only after `Ok(offset)` (Requirement 1.2) and closing the window at
/// the last measured acknowledgment (Requirement 1.4). Any measured produce
/// error stops further produces and aborts with
/// [`ProducerPhaseError::Produce`] (Requirements 3.7, 9.1).
pub async fn run_producer_phase<S>(
    sink: &S,
    params: &WorkloadParameters,
) -> Result<ProducerPhaseResult, ProducerPhaseError>
where
    S: ProduceSink + ?Sized,
{
    let concurrency = (params.producer_concurrency.max(1)) as usize;

    // --- Warmup: exactly `warmup` produces, all completing before the window
    // opens (Requirement 10.1). A failure aborts without opening the window
    // (Requirement 10.6). Warmup records that are acknowledged still received a
    // committed offset, so they count toward the *total* Acknowledged_Record
    // figures (glossary), even though they never enter the measured window.
    let mut warmup_acked_count: u64 = 0;
    let mut warmup_acked_value_bytes: u64 = 0;
    if params.warmup > 0 {
        let mut warmup_stream = futures::stream::iter(0..params.warmup)
            .map(|position| {
                let key = key_for(position, params.key_mode);
                let value = payload_for(position, params.value_size);
                let value_len = value.len() as u64;
                async move {
                    sink.produce(position, key, value)
                        .await
                        .map(|_offset| value_len)
                }
            })
            .buffer_unordered(concurrency);

        while let Some(result) = warmup_stream.next().await {
            match result {
                // A warmup ack got a committed offset: count it toward the total.
                Ok(value_len) => {
                    warmup_acked_count += 1;
                    warmup_acked_value_bytes += value_len;
                }
                Err(failure) => {
                    return Err(ProducerPhaseError::Warmup {
                        cause: failure.cause,
                    });
                }
            }
        }
    }

    // --- Measured: open the window at the first measured produce invocation
    // (Requirement 1.4) and drive the remaining records with bounded
    // concurrency (Requirement 4.4).
    let measured = measured_set(params.record_count, params.warmup);

    let mut acked_count: u64 = 0;
    let mut acked_value_bytes: u64 = 0;

    let window_open = Instant::now();
    let mut last_ack = window_open;

    let mut measured_stream = futures::stream::iter(measured)
        .map(|position| {
            let key = key_for(position, params.key_mode);
            let value = payload_for(position, params.value_size);
            let value_len = value.len() as u64;
            async move {
                sink.produce(position, key, value)
                    .await
                    .map(|_offset| value_len)
            }
        })
        .buffer_unordered(concurrency);

    while let Some(result) = measured_stream.next().await {
        match result {
            // Count a record only after it becomes an Acknowledged_Record, and
            // close the window at the last such acknowledgment (Req 1.2, 1.4).
            Ok(value_len) => {
                acked_count += 1;
                acked_value_bytes += value_len;
                last_ack = Instant::now();
            }
            // Stop further produces and abort (Requirements 3.7, 9.1). Dropping
            // the stream cancels in-flight and not-yet-started produces.
            Err(failure) => {
                return Err(ProducerPhaseError::Produce {
                    topic: failure.topic,
                    partition: failure.partition,
                    cause: failure.cause,
                });
            }
        }
    }

    Ok(ProducerPhaseResult {
        acked_count,
        acked_value_bytes,
        // The total Acknowledged_Record figures include the warmup acks
        // (glossary): warmup + measured.
        total_acked_count: warmup_acked_count + acked_count,
        total_acked_value_bytes: warmup_acked_value_bytes + acked_value_bytes,
        window: last_ack.saturating_duration_since(window_open),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{KeyMode, WorkloadParameters};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    // ----- measured_set (Requirements 1.2, 10.1, 10.4) ---------------------

    #[test]
    fn measured_set_with_zero_warmup_is_every_position() {
        assert_eq!(measured_set(10, 0), 0..10);
        assert_eq!(measured_set(1, 0), 0..1);
    }

    #[test]
    fn measured_set_excludes_the_warmup_prefix() {
        assert_eq!(measured_set(10, 3), 3..10);
        let set = measured_set(10, 3);
        assert_eq!(set.clone().count() as u64, 10 - 3);
        // No warmup position is measured.
        assert!(set.clone().all(|p| p >= 3));
        // Position 0 (a warmup op) is excluded.
        assert!(!set.contains(&0));
    }

    #[test]
    fn measured_set_count_is_total_minus_warmup() {
        for (total, warmup) in [(1u64, 0u64), (5, 4), (100, 1), (1000, 250)] {
            assert_eq!(measured_set(total, warmup).count() as u64, total - warmup);
        }
    }

    #[test]
    fn measured_set_clamps_out_of_range_warmup_to_empty() {
        assert!(measured_set(5, 5).is_empty());
        assert!(measured_set(5, 9).is_empty());
    }

    // ----- A fake ProduceSink for the happy-path test ----------------------

    /// A sink that acknowledges every produce, recording how many times it was
    /// invoked so the test can confirm warmup produces are issued too.
    #[derive(Default)]
    struct CountingSink {
        invocations: AtomicU64,
    }

    #[async_trait]
    impl ProduceSink for CountingSink {
        async fn produce(
            &self,
            position: u64,
            _key: Option<Vec<u8>>,
            _value: Vec<u8>,
        ) -> Result<u64, ProduceFailure> {
            self.invocations.fetch_add(1, Ordering::SeqCst);
            // Echo the position back as the committed offset.
            Ok(position)
        }
    }

    fn params() -> WorkloadParameters {
        WorkloadParameters {
            record_count: 20,
            value_size: 32,
            key_mode: KeyMode::Keyed,
            partition_count: 4,
            producer_concurrency: 4,
            topic: "vela-bench".to_string(),
            warmup: 5,
            time_budget: Duration::from_secs(60),
            startup_budget: Duration::from_secs(60),
            floor_produce_rps: None,
            floor_consume_rps: None,
        }
    }

    #[tokio::test]
    async fn happy_path_counts_only_measured_records() {
        let p = params();
        let sink = CountingSink::default();

        let result = run_producer_phase(&sink, &p).await.expect("phase succeeds");

        // Measured acks exclude the warmup prefix (Requirement 1.2, 10.1).
        let measured = p.record_count - p.warmup;
        assert_eq!(result.acked_count, measured);
        assert_eq!(result.acked_value_bytes, measured * p.value_size as u64);

        // The total Acknowledged_Record figures include the warmup acks
        // (glossary): every produced record received a committed offset, so the
        // total is the full record count and its full payload volume.
        assert_eq!(result.total_acked_count, p.record_count);
        assert_eq!(
            result.total_acked_value_bytes,
            p.record_count * p.value_size as u64
        );

        // Every record — warmup and measured — was actually produced.
        assert_eq!(
            sink.invocations.load(Ordering::SeqCst),
            p.record_count,
            "warmup produces must be issued before the measured window"
        );

        // The window is a real, finite interval.
        assert!(result.window <= Duration::from_secs(60));
    }

    #[tokio::test]
    async fn happy_path_with_zero_warmup_measures_every_record() {
        let mut p = params();
        p.warmup = 0;
        let sink = CountingSink::default();

        let result = run_producer_phase(&sink, &p).await.expect("phase succeeds");

        assert_eq!(result.acked_count, p.record_count);
        // With no warmup, the totals equal the measured figures.
        assert_eq!(result.total_acked_count, result.acked_count);
        assert_eq!(result.total_acked_value_bytes, result.acked_value_bytes);
        assert_eq!(result.total_acked_count, p.record_count);
        assert_eq!(sink.invocations.load(Ordering::SeqCst), p.record_count);
    }
}
