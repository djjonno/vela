// Feature: throughput-benchmark, Property 2: Throughput is correct for any positive window and rejects a zero window
//!
//! Property test for [`vela_bench::metrics::throughput`].
//!
//! Property 2: Throughput is correct for any positive window and rejects a zero
//! window. *For any* record/byte counts and *any* window with a strictly
//! positive duration, [`throughput`] returns `Ok` whose `records_per_sec` and
//! `bytes_per_sec` equal `count / window.as_secs_f64()` (within a relative float
//! tolerance) and are both finite — never NaN or infinite. *For any* counts over
//! a zero-length window (`Duration::ZERO`), it returns `Err(ZeroWindow)` rather
//! than producing an undefined value.
//!
//! Validates: Requirements 1.3, 1.5, 2.4

use std::time::Duration;

use proptest::prelude::*;
use vela_bench::metrics::{throughput, Throughput, ZeroWindow};

/// Assert two rates agree within a relative tolerance, guarding tiny magnitudes
/// with an absolute floor so the comparison stays meaningful near zero.
fn approx_eq(actual: f64, expected: f64) {
    let tolerance = 1e-9 * expected.abs().max(1.0);
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected}, got {actual} (tolerance {tolerance})"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// A strictly positive window yields finite, correctly-divided rates.
    #[test]
    fn positive_window_computes_finite_correct_rates(
        records in any::<u64>(),
        bytes in any::<u64>(),
        secs in 0u64..=86_400,
        nanos in 0u32..1_000_000_000,
    ) {
        // Build a window guaranteed to be strictly positive (total nanos >= 1).
        let mut window = Duration::new(secs, nanos);
        if window.is_zero() {
            window = Duration::from_nanos(1);
        }
        prop_assert!(!window.is_zero());

        let result = throughput(records, bytes, window);
        let Throughput {
            records_per_sec,
            bytes_per_sec,
        } = result.expect("positive window must yield Ok");

        let window_secs = window.as_secs_f64();
        approx_eq(records_per_sec, records as f64 / window_secs);
        approx_eq(bytes_per_sec, bytes as f64 / window_secs);

        prop_assert!(records_per_sec.is_finite());
        prop_assert!(bytes_per_sec.is_finite());
    }

    /// A zero-length window is rejected with `ZeroWindow`, never undefined.
    #[test]
    fn zero_window_is_rejected(
        records in any::<u64>(),
        bytes in any::<u64>(),
    ) {
        prop_assert_eq!(
            throughput(records, bytes, Duration::ZERO),
            Err(ZeroWindow)
        );
    }
}
