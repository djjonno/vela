//! Time-bounded exponential-backoff retry policy.
//!
//! A single produce, consume, or topic-admin request may be retried several
//! times while the client follows `NotLeader` redirects and re-resolves leaders
//! after transport failures (Requirement 3.2, 3.3, 4.1, 4.2). Rather than bound
//! those retries by a fixed *count*, the client bounds them by a total
//! elapsed-time [`Retry_Budget`](RetryBudget) (Requirement 3.4, 4.3):
//!
//! - Retries continue only while the time elapsed since the first attempt is
//!   below [`total`](RetryBudget::DEFAULT_TOTAL) (default 5 seconds). Once the
//!   budget is exhausted the dispatch loop stops and returns a
//!   no-leader-after-retries error (Requirement 3.5, 4.4).
//! - Before each retry the client waits an exponential backoff that starts at
//!   [`base`](RetryBudget::DEFAULT_BASE) (default 100 ms) and doubles each
//!   attempt up to a [`cap`](RetryBudget::DEFAULT_CAP) (default 2 seconds)
//!   (Requirement 3.4).
//!
//! [`RetryBudget`] is a pure, allocation-free helper: it computes backoffs and
//! the budget predicate from values supplied by the caller and never reads a
//! clock or sleeps itself, so its bounds are deterministically unit- and
//! property-testable. The dispatch loop owns the [`Clock`] that measures elapsed
//! time and performs the sleeps.

use std::time::Duration;

/// A time-bounded exponential-backoff policy for a single request's retries.
///
/// `backoff(attempt)` yields the wait before the `attempt`-th retry (0-based),
/// and `may_retry(elapsed)` reports whether another retry may start given the
/// time elapsed since the first attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryBudget {
    /// Total elapsed-time budget across all retries of one request.
    total: Duration,
    /// The first backoff, doubled on each subsequent retry.
    base: Duration,
    /// The maximum any single backoff may reach.
    cap: Duration,
}

impl RetryBudget {
    /// Default total elapsed-time budget: 5 seconds (Requirement 3.4).
    pub const DEFAULT_TOTAL: Duration = Duration::from_secs(5);
    /// Default first backoff: 100 milliseconds (Requirement 3.4).
    pub const DEFAULT_BASE: Duration = Duration::from_millis(100);
    /// Default backoff cap: 2 seconds (Requirement 3.4).
    pub const DEFAULT_CAP: Duration = Duration::from_secs(2);

    /// Create a budget with explicit `total`, `base`, and `cap` values.
    pub fn new(total: Duration, base: Duration, cap: Duration) -> Self {
        Self { total, base, cap }
    }

    /// The total elapsed-time budget across all retries.
    pub fn total(&self) -> Duration {
        self.total
    }

    /// The first backoff (doubled on each subsequent retry up to the cap).
    pub fn base(&self) -> Duration {
        self.base
    }

    /// The maximum any single backoff may reach.
    pub fn cap(&self) -> Duration {
        self.cap
    }

    /// The backoff to wait before the `attempt`-th retry (0-based):
    /// `min(base * 2^attempt, cap)` (Requirement 3.4).
    ///
    /// Backoff is non-decreasing in `attempt` and never exceeds `cap`. The
    /// `base * 2^attempt` computation is guarded against overflow: once the
    /// doubling factor or the product would overflow, the result saturates to
    /// `cap` (which it would have reached for any realistic `attempt` anyway),
    /// so a large `attempt` can never panic.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let scaled = 2u32
            .checked_pow(attempt)
            .and_then(|factor| self.base.checked_mul(factor));
        match scaled {
            Some(backoff) => backoff.min(self.cap),
            // `2^attempt` or the product overflowed — far past the cap.
            None => self.cap,
        }
    }

    /// Whether another retry may start given `elapsed` time since the first
    /// attempt: `elapsed < total` (Requirement 3.5, 4.4).
    ///
    /// At exactly `total` the budget is exhausted and no further retry begins.
    pub fn may_retry(&self, elapsed: Duration) -> bool {
        elapsed < self.total
    }
}

impl Default for RetryBudget {
    /// The default budget: 5 s total, 100 ms base, 2 s cap (Requirement 3.4).
    fn default() -> Self {
        Self::new(Self::DEFAULT_TOTAL, Self::DEFAULT_BASE, Self::DEFAULT_CAP)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_spec() {
        // Requirement 3.4: total 5s, base 100ms, cap 2s.
        let budget = RetryBudget::default();
        assert_eq!(budget.total(), Duration::from_secs(5));
        assert_eq!(budget.base(), Duration::from_millis(100));
        assert_eq!(budget.cap(), Duration::from_secs(2));
    }

    #[test]
    fn first_backoff_is_the_base() {
        // Requirement 3.4: backoff begins at 100ms (attempt 0 → base * 2^0).
        let budget = RetryBudget::default();
        assert_eq!(budget.backoff(0), Duration::from_millis(100));
    }

    #[test]
    fn backoff_doubles_until_the_cap() {
        // Requirement 3.4: doubles each retry, capped at 2s.
        let budget = RetryBudget::default();
        assert_eq!(budget.backoff(1), Duration::from_millis(200));
        assert_eq!(budget.backoff(2), Duration::from_millis(400));
        assert_eq!(budget.backoff(3), Duration::from_millis(800));
        assert_eq!(budget.backoff(4), Duration::from_millis(1600));
        // 100ms * 2^5 = 3200ms, clamped to the 2s cap.
        assert_eq!(budget.backoff(5), Duration::from_secs(2));
        assert_eq!(budget.backoff(6), Duration::from_secs(2));
    }

    #[test]
    fn backoff_saturates_to_cap_on_overflow() {
        // A very large attempt must never panic: the doubling factor / product
        // overflows and the backoff saturates to the cap.
        let budget = RetryBudget::default();
        assert_eq!(budget.backoff(u32::MAX), Duration::from_secs(2));
        assert_eq!(budget.backoff(64), Duration::from_secs(2));
    }

    #[test]
    fn may_retry_is_exclusive_at_the_total() {
        // Requirement 3.5, 4.4: retries stop once elapsed reaches the total.
        let budget = RetryBudget::default();
        assert!(budget.may_retry(Duration::ZERO));
        assert!(budget.may_retry(Duration::from_millis(4_999)));
        // Just below the total (by the smallest representable margin) a retry
        // may still start: this pins the predicate boundary as `< total`, so a
        // mutation to `<=` here would be caught by the exact-total case below.
        assert!(budget.may_retry(Duration::from_secs(5) - Duration::from_nanos(1)));
        // At exactly the total the budget is exhausted (exclusive bound).
        assert!(!budget.may_retry(Duration::from_secs(5)));
        // Just past the total the budget remains exhausted; this rules out a
        // mutation that flips the comparison to `>`/`>=`.
        assert!(!budget.may_retry(Duration::from_secs(5) + Duration::from_nanos(1)));
        assert!(!budget.may_retry(Duration::from_secs(6)));
    }

    #[test]
    fn may_retry_honors_a_custom_total() {
        // The predicate must read the budget's own `total`, not a hardcoded 5s:
        // with a 1s total, 999ms still allows a retry but 1s does not.
        let budget = RetryBudget::new(
            Duration::from_secs(1),
            RetryBudget::DEFAULT_BASE,
            RetryBudget::DEFAULT_CAP,
        );
        assert!(budget.may_retry(Duration::from_millis(999)));
        assert!(!budget.may_retry(Duration::from_secs(1)));
        // A value below the default 5s total but at/above this custom total must
        // be rejected — proving the default is not silently substituted.
        assert!(!budget.may_retry(Duration::from_secs(2)));
    }

    #[test]
    fn backoff_uses_base_and_cap_fields() {
        // Distinct, non-default base and cap so a mutation that swaps base/cap
        // (or substitutes `total`) produces a different, detectable sequence.
        let budget = RetryBudget::new(
            Duration::from_secs(30),
            Duration::from_millis(50),
            Duration::from_millis(150),
        );
        // attempt 0 → base, before any doubling.
        assert_eq!(budget.backoff(0), Duration::from_millis(50));
        // 50ms * 2 = 100ms, still under the 150ms cap.
        assert_eq!(budget.backoff(1), Duration::from_millis(100));
        // 50ms * 4 = 200ms, clamped to the 150ms cap (not the 30s total).
        assert_eq!(budget.backoff(2), Duration::from_millis(150));
        assert_eq!(budget.backoff(3), Duration::from_millis(150));
    }

    #[test]
    fn backoff_is_non_decreasing_up_to_the_cap() {
        // Doubling must never produce a smaller wait than the previous attempt,
        // and every value stays within [base, cap].
        let budget = RetryBudget::default();
        let mut previous = budget.backoff(0);
        assert_eq!(previous, budget.base());
        for attempt in 1..=8 {
            let current = budget.backoff(attempt);
            assert!(
                current >= previous,
                "backoff({attempt}) = {current:?} regressed below {previous:?}",
            );
            assert!(current >= budget.base());
            assert!(current <= budget.cap());
            previous = current;
        }
    }

    #[test]
    fn accessors_return_the_constructed_values() {
        // `new` must store each argument in its matching field (no swaps), and
        // the accessors must return them verbatim.
        let total = Duration::from_secs(7);
        let base = Duration::from_millis(25);
        let cap = Duration::from_millis(900);
        let budget = RetryBudget::new(total, base, cap);
        assert_eq!(budget.total(), total);
        assert_eq!(budget.base(), base);
        assert_eq!(budget.cap(), cap);
    }
}
