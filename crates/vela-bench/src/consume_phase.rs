//! The Consumer_Phase: per-partition reads, warmup, and the Measurement_Window.
//!
//! The Consumer_Phase reads the produced Workload back through a
//! [`ConsumeSource`] seam, counting how much measured work was read and over
//! what wall-clock window, and collecting every consumed value so the harness
//! can run data-integrity verification (Requirements 2.1, 2.2, 2.3, 2.5, 9.2,
//! 10.2).
//!
//! The phase walks the target topic's partitions `0..partition_count` and, for
//! each partition, consumes from **offset 0**, advancing by the returned
//! `next_offset`, until that partition is drained (Requirement 2.1). It
//! continues across partitions until the **total number of records read —
//! including warmup reads — equals `acked_count`** (the Acknowledged_Record
//! count from the Producer_Phase, Requirement 2.2).
//!
//! Warmup and measurement mirror the Producer_Phase semantics:
//!
//! 1. **Warmup** — the first `warmup` reads of the phase are warmup reads,
//!    excluded from the Measurement_Window (Requirement 10.2). They are still
//!    real records, so they are collected for integrity verification and count
//!    toward the `acked_count` stop condition (Requirement 2.2).
//! 2. **Measured** — a read is counted toward Consume_Throughput only after the
//!    record is delivered by the source and only once warmup has completed
//!    (Requirement 2.3). The Measurement_Window opens at the **first measured
//!    consume invocation** and closes at the **receipt of the last measured
//!    record** (Requirement 2.5).
//!
//! Any `Err` from a consume stops further consumes and aborts the phase,
//! carrying the topic, partition, and cause so the harness can build
//! `FailureReason::ConsumeError` (Requirements 3.7, 9.2).
//!
//! ## The [`ConsumeSource`] seam
//!
//! The real consume call is
//! `client.consumer().consume(topic, partition, offset, Option<u32> max)`
//! returning a `ConsumeOutcome { records, next_offset }`. To keep this phase
//! unit-testable without a live cluster, the phase depends only on the
//! [`ConsumeSource`] trait — mirroring the Producer_Phase's `ProduceSink` seam.
//! The real `VelaClient` adapter is wired in by the run harness (`run.rs`), and
//! tests drive a fake source.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use thiserror::Error;

use crate::outcome::{FailureReason, Phase};
use crate::params::WorkloadParameters;

/// Upper bound on the number of records requested in a single consume call,
/// matching the client's accepted `max` range (`1..=10_000`).
const MAX_BATCH: u64 = 10_000;

/// A batch of consumed records and the offset to request next.
///
/// Mirrors the client's `ConsumeOutcome`: `records` are the value payloads of
/// the committed records delivered by this consume (in ascending offset order),
/// and `next_offset` is the offset the phase requests next to continue reading
/// the same partition. An empty `records` signals the partition is drained at
/// `offset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedBatch {
    /// The value bytes of the records delivered by this consume call.
    pub records: Vec<Vec<u8>>,
    /// The offset to request next to continue reading this partition.
    pub next_offset: u64,
}

/// A failed consume operation, carrying the location and cause needed to build
/// `FailureReason::ConsumeError` (Requirements 3.7, 9.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeFailure {
    /// The target topic.
    pub topic: String,
    /// The target partition.
    pub partition: u32,
    /// The underlying error cause.
    pub cause: String,
}

/// The consumer-side seam the Consumer_Phase drives.
///
/// A `consume` either delivers a [`ConsumedBatch`] for `(partition, offset)` or
/// fails with a [`ConsumeFailure`]. The real implementation wraps
/// `client.consumer().consume(topic, partition, offset, max)`; tests provide a
/// fake. `max` bounds the number of records the call may return (`None` accepts
/// the server default).
#[async_trait]
pub trait ConsumeSource {
    /// Consume up to `max` records from `partition` starting at `offset`,
    /// returning the delivered batch or a [`ConsumeFailure`] on an unresolved
    /// error.
    async fn consume(
        &self,
        partition: u32,
        offset: u64,
        max: Option<u32>,
    ) -> Result<ConsumedBatch, ConsumeFailure>;
}

/// The measured result of a completed Consumer_Phase.
///
/// `read_count` and `read_value_bytes` count only measured (non-warmup) records
/// (Requirement 2.3); `window` is the wall-clock interval from the first
/// measured consume invocation to the receipt of the last measured record
/// (Requirement 2.5). `consumed` carries the value bytes of **every** record
/// read — warmup included — so the harness can run `verify::verify_consumed`
/// against the full Workload (Requirements 2.2, 5.1, 5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPhaseResult {
    /// Number of measured records read (excludes warmup reads).
    pub read_count: u64,
    /// Sum of the value byte lengths of the measured records read.
    pub read_value_bytes: u64,
    /// The Consumer_Phase Measurement_Window duration.
    pub window: Duration,
    /// The value bytes of every record read, for data-integrity verification.
    pub consumed: Vec<Vec<u8>>,
}

/// Why a Consumer_Phase aborted.
///
/// A phase-local error the harness maps to a [`FailureReason`] via
/// [`ConsumerPhaseError::into_failure_reason`]. A warmup failure aborts before
/// the window opens (Requirement 10.6); a measured consume failure aborts the
/// run (Requirements 3.7, 9.2).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConsumerPhaseError {
    /// A warmup consume failed before the Measurement_Window opened.
    #[error("warmup consume failed: {cause}")]
    Warmup {
        /// The underlying error cause.
        cause: String,
    },
    /// A consume surfaced an error the client retry path did not resolve.
    #[error("consume failed on topic `{topic}` partition {partition}: {cause}")]
    Consume {
        /// The target topic.
        topic: String,
        /// The target partition.
        partition: u32,
        /// The underlying error cause.
        cause: String,
    },
}

impl ConsumerPhaseError {
    /// Map this phase-local error to the report-level [`FailureReason`].
    #[must_use]
    pub fn into_failure_reason(self) -> FailureReason {
        match self {
            ConsumerPhaseError::Warmup { cause } => FailureReason::WarmupFailed {
                phase: Phase::Consume,
                cause,
            },
            ConsumerPhaseError::Consume {
                topic,
                partition,
                cause,
            } => FailureReason::ConsumeError {
                topic,
                partition,
                cause,
            },
        }
    }
}

/// Run the Consumer_Phase against `source` for the given Workload_Parameters,
/// reading back `acked_count` records produced by the Producer_Phase.
///
/// Walks partitions `0..params.partition_count`, consuming each from offset 0
/// and advancing by the returned `next_offset` until the partition is drained
/// (Requirement 2.1), continuing across partitions until the total records read
/// — warmup included — equals `acked_count` (Requirement 2.2). The first
/// `params.warmup` reads are warmup reads excluded from the Measurement_Window
/// (Requirement 10.2); a measured read is counted only after delivery and after
/// warmup completes (Requirement 2.3). The window opens at the first measured
/// consume invocation and closes at the receipt of the last measured record
/// (Requirement 2.5). Every consumed value — warmup included — is collected for
/// integrity verification. Any consume error stops further consumes and aborts
/// with [`ConsumerPhaseError::Consume`] (Requirements 3.7, 9.2).
pub async fn run_consumer_phase<S>(
    source: &S,
    params: &WorkloadParameters,
    acked_count: u64,
) -> Result<ConsumerPhaseResult, ConsumerPhaseError>
where
    S: ConsumeSource + ?Sized,
{
    let warmup = params.warmup;

    let mut total_read: u64 = 0;
    let mut read_count: u64 = 0;
    let mut read_value_bytes: u64 = 0;
    let mut consumed: Vec<Vec<u8>> = Vec::new();

    // The Measurement_Window opens at the first measured consume invocation and
    // closes at the receipt of the last measured record (Requirement 2.5). Both
    // remain `None` until at least one measured record is read.
    let mut window_open: Option<Instant> = None;
    let mut last_measured: Option<Instant> = None;

    'partitions: for partition in 0..params.partition_count {
        let mut offset: u64 = 0;

        loop {
            if total_read >= acked_count {
                break 'partitions;
            }

            // Bound each request to what is still needed (Requirement 2.2), so a
            // source never has to over-deliver past `acked_count`.
            let remaining = acked_count - total_read;
            let max = Some(remaining.min(MAX_BATCH) as u32);

            // Timestamp the invocation before the call so it can become the
            // window open if this call delivers the first measured record.
            let invoked_at = Instant::now();
            let batch = source.consume(partition, offset, max).await.map_err(
                |ConsumeFailure {
                     topic,
                     partition,
                     cause,
                 }| {
                    // A warmup read failing before the window opens is a warmup
                    // failure (Requirement 10.6); otherwise it aborts the run
                    // (Requirements 3.7, 9.2).
                    if total_read < warmup {
                        ConsumerPhaseError::Warmup { cause }
                    } else {
                        ConsumerPhaseError::Consume {
                            topic,
                            partition,
                            cause,
                        }
                    }
                },
            )?;
            let received_at = Instant::now();

            // An empty batch means this partition is drained; move to the next.
            if batch.records.is_empty() {
                break;
            }

            for value in batch.records {
                if total_read >= acked_count {
                    break;
                }

                // A read is measured once warmup has completed (Requirement
                // 2.3). Warmup reads are still collected and counted toward the
                // `acked_count` stop condition (Requirement 2.2).
                if total_read >= warmup {
                    if window_open.is_none() {
                        window_open = Some(invoked_at);
                    }
                    read_count += 1;
                    read_value_bytes += value.len() as u64;
                    last_measured = Some(received_at);
                }

                total_read += 1;
                consumed.push(value);
            }

            offset = batch.next_offset;
        }
    }

    let window = match (window_open, last_measured) {
        (Some(open), Some(close)) => close.saturating_duration_since(open),
        _ => Duration::ZERO,
    };

    Ok(ConsumerPhaseResult {
        read_count,
        read_value_bytes,
        window,
        consumed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{KeyMode, WorkloadParameters};
    use crate::workload::payload_for;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A fake [`ConsumeSource`] backed by a per-partition list of value
    /// payloads. It honours `offset`/`next_offset` paging and caps each batch
    /// to `batch_size` (and to the requested `max`), recording its call count.
    struct FakeSource {
        partitions: Vec<Vec<Vec<u8>>>,
        batch_size: usize,
        calls: AtomicU64,
    }

    impl FakeSource {
        fn new(partitions: Vec<Vec<Vec<u8>>>, batch_size: usize) -> Self {
            Self {
                partitions,
                batch_size,
                calls: AtomicU64::new(0),
            }
        }
    }

    #[async_trait]
    impl ConsumeSource for FakeSource {
        async fn consume(
            &self,
            partition: u32,
            offset: u64,
            max: Option<u32>,
        ) -> Result<ConsumedBatch, ConsumeFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let part = &self.partitions[partition as usize];
            let start = offset as usize;
            if start >= part.len() {
                return Ok(ConsumedBatch {
                    records: Vec::new(),
                    next_offset: offset,
                });
            }
            let cap = max
                .map(|m| m as usize)
                .unwrap_or(self.batch_size)
                .min(self.batch_size);
            let end = (start + cap).min(part.len());
            Ok(ConsumedBatch {
                records: part[start..end].to_vec(),
                next_offset: end as u64,
            })
        }
    }

    fn params(partition_count: u32, warmup: u64) -> WorkloadParameters {
        WorkloadParameters {
            record_count: 100,
            value_size: 16,
            key_mode: KeyMode::Keyless,
            partition_count,
            producer_concurrency: 4,
            topic: "vela-bench".to_string(),
            warmup,
            time_budget: Duration::from_secs(60),
            startup_budget: Duration::from_secs(60),
            floor_produce_rps: None,
            floor_consume_rps: None,
        }
    }

    /// Split `count` payloads of `size` round-robin across `partition_count`
    /// partitions, returning the per-partition value lists.
    fn split(count: u64, size: usize, partition_count: u32) -> Vec<Vec<Vec<u8>>> {
        let mut parts: Vec<Vec<Vec<u8>>> = vec![Vec::new(); partition_count as usize];
        for position in 0..count {
            let p = (position % u64::from(partition_count)) as usize;
            parts[p].push(payload_for(position, size));
        }
        parts
    }

    #[tokio::test]
    async fn reads_all_partitions_until_acked_count_with_warmup_excluded() {
        let acked = 10u64;
        let p = params(2, 3);
        let source = FakeSource::new(split(acked, p.value_size, p.partition_count), 8);

        let result = run_consumer_phase(&source, &p, acked)
            .await
            .expect("phase succeeds");

        // Every produced record is read back, warmup included (Req 2.2).
        assert_eq!(result.consumed.len() as u64, acked);

        // Measured reads exclude the warmup prefix (Req 2.3, 10.2).
        let measured = acked - p.warmup;
        assert_eq!(result.read_count, measured);
        assert_eq!(result.read_value_bytes, measured * p.value_size as u64);

        // The window is a real, finite interval.
        assert!(result.window <= Duration::from_secs(60));
    }

    #[tokio::test]
    async fn zero_warmup_measures_every_record() {
        let acked = 12u64;
        let p = params(3, 0);
        let source = FakeSource::new(split(acked, p.value_size, p.partition_count), 8);

        let result = run_consumer_phase(&source, &p, acked)
            .await
            .expect("phase succeeds");

        assert_eq!(result.read_count, acked);
        assert_eq!(result.read_value_bytes, acked * p.value_size as u64);
        assert_eq!(result.consumed.len() as u64, acked);
    }

    #[tokio::test]
    async fn advances_by_next_offset_across_multiple_batches() {
        // A batch_size of 2 forces several paged consume calls per partition,
        // exercising the `next_offset` advance.
        let acked = 10u64;
        let p = params(2, 0);
        let source = FakeSource::new(split(acked, p.value_size, p.partition_count), 2);

        let result = run_consumer_phase(&source, &p, acked)
            .await
            .expect("phase succeeds");

        assert_eq!(result.consumed.len() as u64, acked);
        assert_eq!(result.read_count, acked);
        // 5 records per partition at 2 per batch => 3 data calls + 1 drain call
        // each if it had to drain, but the phase stops at `acked_count`, so it
        // makes at least the data calls needed to gather all records.
        assert!(source.calls.load(Ordering::SeqCst) >= 6);
    }

    #[tokio::test]
    async fn zero_acked_count_reads_nothing() {
        let p = params(2, 0);
        let source = FakeSource::new(split(0, p.value_size, p.partition_count), 8);

        let result = run_consumer_phase(&source, &p, 0)
            .await
            .expect("phase succeeds");

        assert_eq!(result.read_count, 0);
        assert_eq!(result.read_value_bytes, 0);
        assert!(result.consumed.is_empty());
        assert_eq!(result.window, Duration::ZERO);
        // No consume call is issued when nothing is expected.
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn collected_values_reconstruct_the_workload_for_verification() {
        // The collected values (warmup included) must equal the produced set so
        // `verify::verify_consumed` can confirm integrity (Req 2.2, 5.1, 5.2).
        let acked = 9u64;
        let p = params(3, 2);
        let source = FakeSource::new(split(acked, p.value_size, p.partition_count), 4);

        let result = run_consumer_phase(&source, &p, acked)
            .await
            .expect("phase succeeds");

        assert_eq!(
            crate::verify::verify_consumed(acked, p.value_size, result.consumed.iter()),
            Ok(())
        );
    }
}
