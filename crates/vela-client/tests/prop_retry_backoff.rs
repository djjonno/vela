//! Property test for the retry-policy backoff schedule in `vela-client`.
//!
//! Feature: ctl-client-routing-and-repl, Property 10
//!
//! Property 10: Retry backoff bounds and monotonicity. For any retry budget and
//! any attempt index `n`, the backoff `RetryBudget::backoff(n)` is exactly
//! `min(base * 2^n, cap)`, is non-decreasing in `n`, and never exceeds the cap.
//! For the default budget the schedule is `min(100ms * 2^n, 2s)` (Requirement
//! 3.4): it starts at 100ms, doubles each attempt, and clamps at the 2s cap.
//!
//! The generators constrain inputs to the space the property quantifies over: a
//! `base` and `cap` in realistic millisecond ranges with `base <= cap` (the
//! invariant a sensible budget satisfies), and an attempt index spanning small
//! values (where doubling is observable) through `u32::MAX` (where the
//! `base * 2^n` product overflows and must saturate to the cap rather than
//! panic). Monotonicity is checked across adjacent attempts; the closed-form
//! `min(base * 2^n, cap)` is checked directly for the attempts where the product
//! does not overflow.
//!
//! Validates: Requirements 3.4, 3.5, 4.3, 4.4

use std::time::Duration;

use proptest::prelude::*;
use vela_client::RetryBudget;

/// Generate a `(base, cap)` pair in milliseconds with `base <= cap`. A budget is
/// only sensible when the first backoff does not already exceed the cap, so the
/// generator derives `cap` as `base + extra` to maintain that invariant while
/// still covering `base == cap`.
fn base_cap_ms_strategy() -> impl Strategy<Value = (u64, u64)> {
    (1u64..=2_000, 0u64..=10_000).prop_map(|(base, extra)| (base, base + extra))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 10
    #[test]
    fn backoff_is_min_base_pow2_cap_non_decreasing_and_capped(
        (base_ms, cap_ms) in base_cap_ms_strategy(),
        attempt in 0u32..=u32::MAX,
    ) {
        let base = Duration::from_millis(base_ms);
        let cap = Duration::from_millis(cap_ms);
        // `total` does not affect `backoff`; any value is fine here.
        let budget = RetryBudget::new(Duration::from_secs(5), base, cap);

        let backoff = budget.backoff(attempt);

        // Never exceeds the cap (Requirement 3.4).
        prop_assert!(
            backoff <= cap,
            "backoff {backoff:?} exceeds cap {cap:?} at attempt {attempt}"
        );

        // Equals the closed form `min(base * 2^n, cap)` wherever the product does
        // not overflow; where it overflows the result saturates to the cap, which
        // the bound above already asserts.
        if let Some(scaled) = 2u32
            .checked_pow(attempt)
            .and_then(|factor| base.checked_mul(factor))
        {
            prop_assert_eq!(backoff, scaled.min(cap));
        } else {
            prop_assert_eq!(backoff, cap);
        }

        // Non-decreasing in `attempt`: the next backoff is never smaller. (Add
        // saturates so the comparison is safe at `u32::MAX`.)
        let next = budget.backoff(attempt.saturating_add(1));
        prop_assert!(
            next >= backoff,
            "backoff decreased from {backoff:?} (attempt {attempt}) to {next:?}"
        );
    }

    // Feature: ctl-client-routing-and-repl, Property 10
    #[test]
    fn default_budget_starts_at_base_and_clamps_at_cap(attempt in 0u32..=u32::MAX) {
        // The default schedule is `min(100ms * 2^n, 2s)` (Requirement 3.4).
        let budget = RetryBudget::default();
        let backoff = budget.backoff(attempt);

        prop_assert!(backoff >= Duration::from_millis(100));
        prop_assert!(backoff <= Duration::from_secs(2));

        if let Some(scaled) = 2u32
            .checked_pow(attempt)
            .and_then(|factor| Duration::from_millis(100).checked_mul(factor))
        {
            prop_assert_eq!(backoff, scaled.min(Duration::from_secs(2)));
        } else {
            prop_assert_eq!(backoff, Duration::from_secs(2));
        }
    }
}
