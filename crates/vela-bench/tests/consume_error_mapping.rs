//! Consumer_Phase consume-error mapping (Requirement 9.2).
//!
//! A measured consume that fails must abort the phase with
//! [`ConsumerPhaseError::Consume`], carrying the failed operation's topic,
//! partition, and cause, and that phase-local error must map onto
//! [`FailureReason::ConsumeError`] with the same fields. The error must also
//! stop further consumes: once a consume fails, the phase issues no more
//! consume calls.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use vela_bench::consume_phase::{
    run_consumer_phase, ConsumeFailure, ConsumeSource, ConsumedBatch, ConsumerPhaseError,
};
use vela_bench::outcome::FailureReason;
use vela_bench::params::WorkloadParameters;

/// A fake [`ConsumeSource`] that always fails with a fixed
/// [`ConsumeFailure`], counting how many times it was invoked.
struct AlwaysError {
    topic: String,
    partition: u32,
    cause: String,
    calls: AtomicU64,
}

// Implemented without the `async_trait` attribute macro (a non-dev dependency
// unavailable to this integration-test crate) by writing the desugared,
// boxed-future signature `async_trait` would generate.
impl ConsumeSource for AlwaysError {
    fn consume<'life0, 'async_trait>(
        &'life0 self,
        _partition: u32,
        _offset: u64,
        _max: Option<u32>,
    ) -> Pin<Box<dyn Future<Output = Result<ConsumedBatch, ConsumeFailure>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ConsumeFailure {
                topic: self.topic.clone(),
                partition: self.partition,
                cause: self.cause.clone(),
            })
        })
    }
}

/// A fake [`ConsumeSource`] that delivers a non-empty batch on its first
/// invocation and then fails on its second, counting every invocation. Used to
/// prove the phase stops consuming once a consume fails.
struct ErrorOnSecondCall {
    topic: String,
    partition: u32,
    cause: String,
    calls: AtomicU64,
}

impl ConsumeSource for ErrorOnSecondCall {
    fn consume<'life0, 'async_trait>(
        &'life0 self,
        _partition: u32,
        _offset: u64,
        _max: Option<u32>,
    ) -> Pin<Box<dyn Future<Output = Result<ConsumedBatch, ConsumeFailure>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 1 {
                // Deliver a batch so the phase has reason to consume again.
                Ok(ConsumedBatch {
                    records: vec![vec![0u8; 4], vec![1u8; 4], vec![2u8; 4], vec![3u8; 4]],
                    next_offset: 4,
                })
            } else {
                Err(ConsumeFailure {
                    topic: self.topic.clone(),
                    partition: self.partition,
                    cause: self.cause.clone(),
                })
            }
        })
    }
}

/// Build a small, valid keyless configuration with `warmup == 0`, so any
/// consume failure is a measured consume error rather than a warmup failure.
fn params() -> WorkloadParameters {
    WorkloadParameters {
        partition_count: 4,
        warmup: 0,
        ..WorkloadParameters::default()
    }
}

#[tokio::test]
async fn consume_error_maps_to_failure_reason_with_topic_partition_cause() {
    let topic = "vela-bench-errors".to_string();
    let partition = 2u32;
    let cause = "transport closed".to_string();

    let source = AlwaysError {
        topic: topic.clone(),
        partition,
        cause: cause.clone(),
        calls: AtomicU64::new(0),
    };

    // An `acked_count` large enough that the phase would keep consuming if the
    // source were not failing.
    let acked_count = 1_000u64;

    let err = run_consumer_phase(&source, &params(), acked_count)
        .await
        .expect_err("a failing consume aborts the phase");

    // The phase-local error carries the failed operation's location and cause
    // (Requirement 9.2).
    assert_eq!(
        err,
        ConsumerPhaseError::Consume {
            topic: topic.clone(),
            partition,
            cause: cause.clone(),
        }
    );

    // ...and maps onto `FailureReason::ConsumeError` with the same fields.
    assert_eq!(
        err.into_failure_reason(),
        FailureReason::ConsumeError {
            topic,
            partition,
            cause,
        }
    );
}

#[tokio::test]
async fn consume_error_stops_further_consumes() {
    let source = ErrorOnSecondCall {
        topic: "vela-bench".to_string(),
        partition: 0,
        cause: "broken pipe".to_string(),
        calls: AtomicU64::new(0),
    };

    // Large enough that, absent the error, the phase would issue many consumes.
    let acked_count = 1_000u64;

    let err = run_consumer_phase(&source, &params(), acked_count)
        .await
        .expect_err("the second consume fails, aborting the phase");

    assert!(matches!(err, ConsumerPhaseError::Consume { .. }));

    // The phase aborted at the failing (second) consume: exactly two consume
    // calls were issued, with no further consumes after the error despite
    // `acked_count` being far from satisfied.
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
}
