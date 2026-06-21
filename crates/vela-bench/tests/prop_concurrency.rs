// Feature: throughput-benchmark, Property 9: In-flight produce requests never exceed the configured concurrency
//
// The Producer_Phase keeps up to `producer_concurrency` produce requests in
// flight via `buffer_unordered` (Requirement 4.4). This property drives
// `run_producer_phase` against an instrumented fake [`ProduceSink`] that
// records the maximum number of simultaneously in-flight `produce` calls. For
// any record count `n` and concurrency `c`, the observed maximum must never
// exceed `c`, and every one of the `n` records must still be produced and
// acknowledged.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use proptest::prelude::*;

use vela_bench::params::{KeyMode, WorkloadParameters};
use vela_bench::produce_phase::{
    run_producer_phase, ProduceFailure, ProduceSink, ProducerPhaseResult,
};

/// An instrumented [`ProduceSink`] that observes produce-call concurrency.
///
/// On entry to `produce` it increments a live-in-flight counter and folds that
/// value into an observed maximum; it then yields several times so that
/// concurrently-issued produces actually coexist (forcing `buffer_unordered`
/// to fill up to its limit) before decrementing the counter and acknowledging.
/// A separate counter records the total number of invocations.
#[derive(Default)]
struct ConcurrencyProbe {
    /// Produces currently in flight.
    in_flight: AtomicUsize,
    /// The largest `in_flight` value ever observed.
    max_in_flight: AtomicUsize,
    /// Total `produce` invocations.
    invocations: AtomicU64,
}

// Implemented without the `async_trait` attribute macro (a non-dev dependency
// unavailable to this integration-test crate) by writing the desugared,
// boxed-future signature `async_trait` would generate.
impl ProduceSink for ConcurrencyProbe {
    fn produce<'life0, 'async_trait>(
        &'life0 self,
        position: u64,
        _key: Option<Vec<u8>>,
        _value: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<u64, ProduceFailure>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            // Record this produce as in flight and update the observed maximum.
            let live = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(live, Ordering::SeqCst);

            // Yield repeatedly so co-scheduled produces overlap: control returns
            // to the driving stream, which polls and starts further produces up
            // to the concurrency limit before any of these complete.
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }

            // This produce is no longer in flight; acknowledge it.
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.invocations.fetch_add(1, Ordering::SeqCst);
            Ok(position)
        })
    }
}

/// Build keyless Workload_Parameters with `warmup == 0` (so the measured set is
/// every record) and small, valid budgets that keep each case fast.
fn params(record_count: u64, producer_concurrency: u32) -> WorkloadParameters {
    WorkloadParameters {
        record_count,
        value_size: 8,
        key_mode: KeyMode::Keyless,
        partition_count: 1,
        producer_concurrency,
        topic: "vela-bench".to_string(),
        warmup: 0,
        time_budget: Duration::from_secs(60),
        startup_budget: Duration::from_secs(60),
        floor_produce_rps: None,
        floor_consume_rps: None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 100,
        max_shrink_iters: 100,
        ..ProptestConfig::default()
    })]

    /// Property 9: in-flight produces never exceed `producer_concurrency`, and
    /// all `record_count` records are produced and acknowledged.
    ///
    /// **Validates: Requirements 4.4**
    #[test]
    fn in_flight_produces_never_exceed_concurrency(
        record_count in 1u64..=200,
        producer_concurrency in 1u32..=16,
    ) {
        let p = params(record_count, producer_concurrency);

        // A fresh current-thread runtime per case keeps the async test cheap
        // and deterministic; `buffer_unordered` drives the bounded set of
        // produce futures cooperatively within it.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");

        let probe = ConcurrencyProbe::default();

        let result: ProducerPhaseResult = runtime
            .block_on(run_producer_phase(&probe, &p))
            .expect("producer phase succeeds against the always-acking probe");

        let max_in_flight = probe.max_in_flight.load(Ordering::SeqCst);
        let invocations = probe.invocations.load(Ordering::SeqCst);

        // The bound: never more than `producer_concurrency` produces at once.
        prop_assert!(
            max_in_flight <= producer_concurrency as usize,
            "observed {} in-flight produces, exceeding the concurrency bound {}",
            max_in_flight,
            producer_concurrency
        );

        // Every record was actually produced and acknowledged.
        prop_assert_eq!(invocations, record_count);
        prop_assert_eq!(result.acked_count, record_count);
    }
}
