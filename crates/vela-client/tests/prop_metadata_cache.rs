//! Property test for metadata-cache freshness in `vela-client`.
//!
//! Feature: ctl-client-routing-and-repl, Property 12: Metadata cache freshness
//! and refresh idempotence — for any cached topic entry with learn time `t0` and
//! any later time `t1`, the cache treats the entry as fresh (reused, no
//! `DescribeTopic`) exactly when `t1 - t0 < Metadata_TTL` and as stale (triggering
//! a single refresh) exactly when `t1 - t0 >= Metadata_TTL`; consequently any
//! number of routing operations within one TTL window issues at most one
//! `DescribeTopic` for that topic.
//!
//! The freshness predicate is exercised purely through
//! [`MetadataCache::get_fresh`] with caller-supplied `now` [`Instant`]s, so the
//! TTL boundary is deterministic. Elapsed durations are generated to span below,
//! at, and above the TTL, pinning the strict-`<` boundary exactly. Refresh
//! idempotence is checked by modelling routing operations: a miss stands for a
//! `DescribeTopic`/`Metadata_Refresh` that re-learns the entry, and the property
//! asserts that across a sequence of operations whose times all fall inside one
//! TTL window the model performs at most one refresh.
//!
//! Validates: Requirements 1.3, 1.5

use std::time::{Duration, Instant};

use proptest::prelude::*;
use vela_client::{MetadataCache, TopicMeta};

/// A minimal cached entry learned at `learned_at`. Partition count and leaders do
/// not affect freshness — only `learned_at` is compared against the TTL — so a
/// fixed shape keeps the generators focused on the time dimension the property
/// quantifies over.
fn meta(learned_at: Instant) -> TopicMeta {
    TopicMeta {
        partition_count: 1,
        leaders: vec![None],
        learned_at,
    }
}

/// Generate a TTL paired with a list of operation offsets that all fall strictly
/// inside the `[0, ttl)` TTL window. The offsets are dependent on the TTL so the
/// "within one TTL window" precondition of the idempotence property holds by
/// construction.
fn ttl_and_window_offsets() -> impl Strategy<Value = (u64, Vec<u64>)> {
    (1u64..=120_000).prop_flat_map(|ttl_ms| {
        prop::collection::vec(0u64..ttl_ms, 1..=32).prop_map(move |offsets| (ttl_ms, offsets))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 12
    //
    // Freshness boundary: an entry learned at `t0` is fresh at `t0 + elapsed`
    // exactly when `elapsed < TTL`. The elapsed range spans below, at, and above
    // the TTL, so the strict-`<` boundary (age `== TTL` is stale, Requirement 1.5;
    // age `< TTL` is reused, Requirement 1.3) is pinned exactly.
    #[test]
    fn fresh_iff_elapsed_below_ttl(
        ttl_ms in 1u64..=120_000,
        elapsed_ms in 0u64..=240_000,
    ) {
        let cache = MetadataCache::new(Duration::from_millis(ttl_ms));
        let t0 = Instant::now();
        cache.put("orders", meta(t0));

        let now = t0 + Duration::from_millis(elapsed_ms);
        let is_fresh = cache.get_fresh("orders", now).is_some();

        prop_assert_eq!(
            is_fresh,
            elapsed_ms < ttl_ms,
            "elapsed {}ms vs ttl {}ms: get_fresh returned fresh={}",
            elapsed_ms,
            ttl_ms,
            is_fresh
        );
    }

    // Feature: ctl-client-routing-and-repl, Property 12
    //
    // Refresh idempotence within one TTL window: model a sequence of routing
    // operations, each occurring at a time inside the same TTL window. A miss
    // models a `DescribeTopic`/`Metadata_Refresh` that re-learns the entry; a hit
    // models reuse of the cached metadata. Starting from an empty cache, exactly
    // one operation can miss (the first learn) and every subsequent operation in
    // the window reuses it, so the topic is described at most once per TTL window.
    #[test]
    fn at_most_one_refresh_per_ttl_window(
        (ttl_ms, offsets) in ttl_and_window_offsets(),
    ) {
        let cache = MetadataCache::new(Duration::from_millis(ttl_ms));
        let t0 = Instant::now();

        // Count the modelled `DescribeTopic` refreshes across all operations.
        let mut describes = 0u32;
        for off in &offsets {
            let now = t0 + Duration::from_millis(*off);
            if cache.get_fresh("orders", now).is_none() {
                // A miss forces a Metadata_Refresh, which re-learns the entry at
                // the current time.
                describes += 1;
                cache.put("orders", meta(now));
            }
        }

        // Across any number of operations within a single TTL window the topic is
        // refreshed at most once (Requirement 1.3). With at least one operation it
        // is refreshed exactly once: the initial learn (Requirement 1.5 only fires
        // once the window is exited, which never happens here).
        prop_assert!(
            describes <= 1,
            "expected at most one DescribeTopic within a TTL window, got {} for ttl {}ms and offsets {:?}",
            describes,
            ttl_ms,
            offsets
        );
        prop_assert_eq!(describes, 1, "the first operation in the window must learn the entry exactly once");
    }
}
