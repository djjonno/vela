//! Produce error mapping and short-circuit behavior for the Producer_Phase.
//!
//! These integration tests drive [`run_producer_phase`] against fake
//! [`ProduceSink`]s to confirm that a measured produce error aborts the phase
//! with [`ProducerPhaseError::Produce`] carrying the failing topic, partition,
//! and cause, that it maps to [`FailureReason::ProduceError`] with the same
//! fields, and that the error stops further produces rather than draining every
//! record (Requirement 9.1).

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use vela_bench::outcome::FailureReason;
use vela_bench::params::WorkloadParameters;
use vela_bench::produce_phase::{
    run_producer_phase, ProduceFailure, ProduceSink, ProducerPhaseError,
};

/// A fake sink that acknowledges the first `fail_at_call - 1` produces and then
/// fails every produce from the `fail_at_call`-th onward with a fixed
/// [`ProduceFailure`]. It records how many times `produce` was invoked so a
/// test can assert the phase short-circuits.
struct FailingSink {
    /// The 1-based produce-call number at (and after) which the sink errors.
    fail_at_call: u64,
    topic: String,
    partition: u32,
    cause: String,
    invocations: AtomicU64,
}

impl FailingSink {
    /// A sink that fails on its very first produce.
    fn always(topic: &str, partition: u32, cause: &str) -> Self {
        Self::after(1, topic, partition, cause)
    }

    /// A sink that succeeds for the first `fail_at_call - 1` produces, then
    /// fails on the `fail_at_call`-th and every produce after it.
    fn after(fail_at_call: u64, topic: &str, partition: u32, cause: &str) -> Self {
        Self {
            fail_at_call,
            topic: topic.to_string(),
            partition,
            cause: cause.to_string(),
            invocations: AtomicU64::new(0),
        }
    }

    fn invocations(&self) -> u64 {
        self.invocations.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ProduceSink for FailingSink {
    async fn produce(
        &self,
        position: u64,
        _key: Option<Vec<u8>>,
        _value: Vec<u8>,
    ) -> Result<u64, ProduceFailure> {
        // 1-based count of this invocation.
        let call = self.invocations.fetch_add(1, Ordering::SeqCst) + 1;
        if call >= self.fail_at_call {
            Err(ProduceFailure {
                topic: self.topic.clone(),
                partition: self.partition,
                cause: self.cause.clone(),
            })
        } else {
            Ok(position)
        }
    }
}

/// Small, valid parameters: a measured-only run (no warmup) large enough that a
/// short-circuit is unambiguous relative to the concurrency bound.
fn params() -> WorkloadParameters {
    WorkloadParameters {
        record_count: 100,
        producer_concurrency: 4,
        warmup: 0,
        value_size: 32,
        ..WorkloadParameters::default()
    }
}

#[tokio::test]
async fn measured_produce_error_maps_to_produce_error_with_location_and_cause() {
    let p = params();
    let sink = FailingSink::always("vela-bench", 2, "not leader");

    let err = run_producer_phase(&sink, &p)
        .await
        .expect_err("a produce error must abort the phase");

    // The phase-local error carries the failing topic/partition/cause verbatim.
    assert_eq!(
        err,
        ProducerPhaseError::Produce {
            topic: "vela-bench".to_string(),
            partition: 2,
            cause: "not leader".to_string(),
        }
    );

    // ...and maps to the report-level FailureReason with the same fields
    // (Requirement 9.1).
    assert_eq!(
        err.into_failure_reason(),
        FailureReason::ProduceError {
            topic: "vela-bench".to_string(),
            partition: 2,
            cause: "not leader".to_string(),
        }
    );
}

#[tokio::test]
async fn produce_error_stops_further_produces() {
    let p = params();
    // Fail on the 3rd produce. With buffer_unordered(concurrency) some already
    // buffered calls may run, so we bound by invocations < record_count rather
    // than asserting an exact count.
    let sink = FailingSink::after(3, "vela-bench", 1, "transport");

    let err = run_producer_phase(&sink, &p)
        .await
        .expect_err("the failing produce must abort the phase");

    assert!(matches!(err, ProducerPhaseError::Produce { .. }));

    let invocations = sink.invocations();
    assert!(
        invocations < p.record_count,
        "phase must stop producing on error: {invocations} invocations should be < record_count {}",
        p.record_count
    );
}

#[tokio::test]
async fn warmup_produce_error_maps_to_warmup_failed() {
    // A failure during the warmup prefix aborts before the window opens and
    // maps to WarmupFailed rather than ProduceError (Requirement 10.6).
    let mut p = params();
    p.warmup = 10;
    let sink = FailingSink::always("vela-bench", 0, "rejected");

    let err = run_producer_phase(&sink, &p)
        .await
        .expect_err("a warmup produce error must abort the phase");

    assert_eq!(
        err,
        ProducerPhaseError::Warmup {
            cause: "rejected".to_string(),
        }
    );

    assert_eq!(
        err.into_failure_reason(),
        FailureReason::WarmupFailed {
            phase: vela_bench::outcome::Phase::Produce,
            cause: "rejected".to_string(),
        }
    );
}
