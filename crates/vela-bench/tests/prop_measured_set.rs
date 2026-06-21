// Feature: throughput-benchmark, Property 1: Measured set excludes exactly the warmup operations
//!
//! Property 1: Measured set excludes exactly the warmup operations.
//!
//! For a phase with total operation count `total` (`>= 1`) and warmup count
//! `warmup` in `0..total`, the measured set produced by
//! [`vela_bench::produce_phase::measured_set`] is exactly the positions
//! `[warmup, total)`: the measured count equals `total - warmup`, no warmup
//! position (`< warmup`) is ever measured, and when `warmup == 0` every
//! position — including position 0 — is measured. This is the warmup/measured
//! selection rule that keeps warmup operations out of both Measurement_Windows
//! (Requirements 1.2, 2.3, 10.1, 10.2, 10.4).
//!
//! The generators constrain inputs to the quantified space: `total` in
//! `1..=100_000` and, via `prop_flat_map`, `warmup` bounded by `0..total` so
//! every `(total, warmup)` pair is in range. Assertions are arithmetic over the
//! returned range (start/end, count, containment) rather than iterating large
//! ranges, so the property stays fast even at the top of the `total` range.
//!
//! Validates: Requirements 1.2, 2.3, 10.1, 10.2, 10.4

use proptest::prelude::*;
use vela_bench::produce_phase::measured_set;

/// Generate an in-range `(total, warmup)` pair: `total in 1..=100_000` and
/// `warmup in 0..total`. `prop_flat_map` bounds `warmup` by the chosen `total`
/// so the pair is always valid (`warmup < total`).
fn total_and_warmup() -> impl Strategy<Value = (u64, u64)> {
    (1u64..=100_000).prop_flat_map(|total| (Just(total), 0u64..total))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The measured set is exactly `warmup..total` for any in-range
    /// `(total, warmup)` (Requirements 1.2, 2.3, 10.1, 10.2, 10.4).
    #[test]
    fn measured_set_excludes_exactly_the_warmup_operations((total, warmup) in total_and_warmup()) {
        let set = measured_set(total, warmup);

        // The measured set is exactly the positions `[warmup, total)`.
        prop_assert_eq!(set.clone(), warmup..total);

        // The range bounds are precisely warmup (inclusive) and total
        // (exclusive).
        prop_assert_eq!(set.start, warmup);
        prop_assert_eq!(set.end, total);

        // Measured count is exactly `total - warmup`.
        prop_assert_eq!(set.clone().count() as u64, total - warmup);

        // No warmup position (`< warmup`) is measured: the range starts at or
        // above `warmup`, and it does not contain the boundary just below it.
        prop_assert!(set.start >= warmup);
        if warmup > 0 {
            prop_assert!(!set.contains(&(warmup - 1)));
            // Position 0 is a warmup operation and must be excluded.
            prop_assert!(!set.contains(&0));
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// With `warmup == 0` the measured set is `0..total` and includes position
    /// 0 — every operation is measured (Requirement 10.4).
    #[test]
    fn measured_set_with_zero_warmup_measures_every_position(total in 1u64..=100_000) {
        let set = measured_set(total, 0);

        prop_assert_eq!(set.clone(), 0..total);
        prop_assert_eq!(set.start, 0);
        prop_assert_eq!(set.end, total);
        prop_assert_eq!(set.clone().count() as u64, total);
        // Position 0 is measured when there is no warmup.
        prop_assert!(set.contains(&0));
    }
}
