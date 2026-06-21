//! Throughput arithmetic and Measurement_Window math.
//!
//! Implemented in task 4.1.
//!
//! A [`Throughput`] pairs the records-per-second and bytes-per-second figures
//! computed for one phase's Measurement_Window. [`throughput`] performs the
//! division, guarding against a zero-length window so the benchmark never
//! reports an undefined (NaN/infinite) value (Requirement 1.5).

use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Records-per-second and bytes-per-second over one phase's Measurement_Window.
///
/// Carried on the Benchmark_Report (which serializes it), so it derives
/// `serde` (de)serialization. Both figures are finite for any window with a
/// positive duration.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Throughput {
    /// Records processed per second over the Measurement_Window.
    pub records_per_sec: f64,
    /// Payload bytes processed per second over the Measurement_Window.
    pub bytes_per_sec: f64,
}

/// The Measurement_Window duration was zero, so a throughput rate is undefined.
///
/// Surfaced rather than dividing by zero (which would yield NaN or infinity),
/// satisfying Requirement 1.5 / 2.4: never report an undefined throughput.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("measurement window duration is zero; throughput is undefined")]
pub struct ZeroWindow;

/// Compute [`Throughput`] over `window` for `records` records and `bytes` bytes.
///
/// For a positive `window`, returns `Ok` with `records / secs` and
/// `bytes / secs`, where `secs == window.as_secs_f64()`; both figures are
/// finite. For `window == Duration::ZERO`, returns `Err(ZeroWindow)` rather
/// than dividing by zero (Requirements 1.3, 1.5, 2.4).
pub fn throughput(records: u64, bytes: u64, window: Duration) -> Result<Throughput, ZeroWindow> {
    if window.is_zero() {
        return Err(ZeroWindow);
    }
    let secs = window.as_secs_f64();
    Ok(Throughput {
        records_per_sec: records as f64 / secs,
        bytes_per_sec: bytes as f64 / secs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_window_is_rejected() {
        assert_eq!(throughput(100, 4096, Duration::ZERO), Err(ZeroWindow));
    }

    #[test]
    fn computes_rates_for_one_second_window() {
        let t = throughput(100, 4096, Duration::from_secs(1)).expect("positive window");
        assert_eq!(t.records_per_sec, 100.0);
        assert_eq!(t.bytes_per_sec, 4096.0);
    }

    #[test]
    fn computes_rates_for_subsecond_window() {
        let t = throughput(50, 500, Duration::from_millis(500)).expect("positive window");
        assert_eq!(t.records_per_sec, 100.0);
        assert_eq!(t.bytes_per_sec, 1000.0);
    }

    #[test]
    fn rates_are_finite_for_positive_window() {
        let t = throughput(1, 1, Duration::from_nanos(1)).expect("positive window");
        assert!(t.records_per_sec.is_finite());
        assert!(t.bytes_per_sec.is_finite());
    }

    #[test]
    fn zero_counts_over_positive_window_are_zero_rates() {
        let t = throughput(0, 0, Duration::from_secs(5)).expect("positive window");
        assert_eq!(t.records_per_sec, 0.0);
        assert_eq!(t.bytes_per_sec, 0.0);
    }
}
