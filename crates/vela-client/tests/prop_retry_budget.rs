//! Property test for the retry-policy time budget in `vela-client`.
//!
//! Feature: ctl-client-routing-and-repl, Property 11
//!
//! Property 11: Retry budget total-time bound and termination. For any retry
//! budget, `RetryBudget::may_retry(elapsed)` is true exactly while
//! `elapsed < total` and false once `elapsed >= total`, so a retry may only
//! *start* within the budget (Requirement 3.5, 4.4). A dispatch loop that
//! accumulates elapsed time by the per-attempt `backoff` therefore terminates:
//! because every backoff is at least the (non-zero) base, elapsed strictly
//! increases each iteration and crosses `total` in a bounded number of steps,
//! after which `may_retry` is false and the loop stops with a
//! no-leader-after-retries outcome rather than retrying forever (Requirement
//! 3.4, 4.3).
//!
//! The generators constrain inputs to the quantified space: a sensible budget
//! (`base <= cap`, non-zero base, positive total) and an arbitrary `elapsed`
//! around the `total` boundary to exercise the exclusive `< total` predicate at,
//! below, and above the limit. The termination check simulates the real loop on
//! a virtual elapsed clock (no sleeping) and asserts it both halts and never
//! starts an attempt outside the budget.
//!
//! Validates: Requirements 3.4, 3.5, 4.3, 4.4

use std::time::Duration;

use proptest::prelude::*;
use vela_client::RetryBudget;

/// Generate a sensible budget: non-zero `base`, `cap >= base`, and a positive
/// `total`. These are the invariants a real budget satisfies and the space over
/// which termination must hold.
fn budget_strategy() -> impl Strategy<Value = RetryBudget> {
    (1u64..=2_000, 0u64..=10_000, 1u64..=10_000).prop_map(|(base_ms, cap_extra, total_ms)| {
        let base = Duration::from_millis(base_ms);
        let cap = Duration::from_millis(base_ms + cap_extra);
        RetryBudget::new(Duration::from_millis(total_ms), base, cap)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 11
    #[test]
    fn may_retry_is_true_exactly_below_total(
        budget in budget_strategy(),
        elapsed_ms in 0u64..=20_000,
    ) {
        let elapsed = Duration::from_millis(elapsed_ms);
        // The predicate is the strict `elapsed < total` (Requirement 3.5, 4.4):
        // at exactly `total` the budget is exhausted and no further retry begins.
        prop_assert_eq!(budget.may_retry(elapsed), elapsed < budget.total());
    }

    // Feature: ctl-client-routing-and-repl, Property 11
    #[test]
    fn retry_loop_terminates_within_budget(budget in budget_strategy()) {
        // Simulate the dispatch loop on a virtual elapsed clock: every attempt
        // that `may_retry` allows waits one backoff before the next, accumulating
        // elapsed time. The loop must terminate (no infinite retrying) and must
        // never start an attempt once the budget is exhausted.
        let mut elapsed = Duration::ZERO;
        let mut attempt: u32 = 0;
        // A generous ceiling: with the smallest base (1ms) and largest total
        // (10s) the loop can take at most ~10_000 steps, so 100_000 proves it is
        // bounded without ever being reached.
        let max_iterations = 100_000u32;

        while budget.may_retry(elapsed) {
            // A retry only ever starts while within the budget.
            prop_assert!(elapsed < budget.total());

            let backoff = budget.backoff(attempt);
            // Each backoff is at least the non-zero base, so elapsed strictly
            // increases — this is what guarantees termination.
            prop_assert!(backoff >= budget.base());
            prop_assert!(!backoff.is_zero());

            elapsed = elapsed.saturating_add(backoff);
            attempt += 1;

            prop_assert!(
                attempt < max_iterations,
                "retry loop did not terminate within {max_iterations} iterations"
            );
        }

        // The loop stopped because the budget was exhausted: the next attempt is
        // refused, which is where dispatch returns no-leader-after-retries.
        prop_assert!(!budget.may_retry(elapsed));
        prop_assert!(elapsed >= budget.total());
    }
}
