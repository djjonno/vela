//! TTL-governed cache of topic metadata.
//!
//! Before routing a produce or consume request the client needs a topic's
//! partition count and, for consume, each partition's leader (Requirement 1.1,
//! 1.2). Re-fetching that with `DescribeTopic` on every request is wasteful, so
//! [`MetadataCache`] caches it per topic together with the time it was learned.
//!
//! Each entry carries a [`Metadata_TTL`](MetadataCache::DEFAULT_TTL): while a
//! cached entry's age is within the TTL it is reused (Requirement 1.3); once its
//! age reaches the TTL it is treated as stale and the next routing operation
//! must perform a `Metadata_Refresh` (Requirement 1.5). A stale-routing failure
//! (a routed partition out of range, or leader resolution failing) drops the
//! entry via [`MetadataCache::invalidate`] so the next attempt refreshes
//! (Requirement 1.6). The default TTL is 30 seconds (Requirement 1.7).
//!
//! Freshness is evaluated against a caller-supplied `now: Instant` rather than
//! reading the clock internally, so the TTL boundary is deterministic in tests.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Cached partition-and-leader metadata for one topic.
///
/// `leaders[i]` is the believed leader node id of partition `i`, or `None` when
/// that partition's leader is not (yet) known. `learned_at` stamps when the
/// entry was learned, so its age can be compared against the cache's TTL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMeta {
    /// The topic's partition count (`Partition_Count`).
    pub partition_count: u32,
    /// Per-partition believed leader node id, indexed by partition.
    pub leaders: Vec<Option<String>>,
    /// When this entry was learned, used to evaluate freshness against the TTL.
    pub learned_at: Instant,
}

/// A thread-safe, TTL-governed map from topic name to its cached [`TopicMeta`].
#[derive(Debug)]
pub struct MetadataCache {
    entries: Mutex<HashMap<String, TopicMeta>>,
    ttl: Duration,
}

impl MetadataCache {
    /// The default `Metadata_TTL` applied when none is supplied: 30 seconds
    /// (Requirement 1.7).
    pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

    /// Create an empty cache with an explicit TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Create an empty cache with the default 30-second TTL (Requirement 1.7).
    pub fn with_default_ttl() -> Self {
        Self::new(Self::DEFAULT_TTL)
    }

    /// The TTL governing entry freshness.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Return the cached metadata for `topic` if it is still fresh at `now`.
    ///
    /// Returns `None` when the topic is absent or when its entry's age has
    /// reached the TTL (age `>= ttl` is stale — Requirement 1.5); otherwise
    /// returns a clone of the fresh entry so the caller may reuse it without
    /// re-fetching (Requirement 1.3). `now` is supplied by the caller so the
    /// freshness boundary is deterministic.
    pub fn get_fresh(&self, topic: &str, now: Instant) -> Option<TopicMeta> {
        let entries = self.lock();
        let meta = entries.get(topic)?;
        // `saturating_duration_since` yields zero if `now` precedes `learned_at`
        // (clock skew / paused-time tests), which is correctly treated as fresh.
        if now.saturating_duration_since(meta.learned_at) < self.ttl {
            Some(meta.clone())
        } else {
            None
        }
    }

    /// Store `meta` for `topic`, replacing any previous entry.
    pub fn put(&self, topic: &str, meta: TopicMeta) {
        self.lock().insert(topic.to_string(), meta);
    }

    /// Drop any cached metadata for `topic`, forcing a refresh on the next
    /// routing operation (Requirement 1.6).
    ///
    /// Returns the entry that was removed, if any.
    pub fn invalidate(&self, topic: &str) -> Option<TopicMeta> {
        self.lock().remove(topic)
    }

    /// Number of cached topics (primarily for tests).
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, TopicMeta>> {
        self.entries.lock().expect("metadata cache mutex poisoned")
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::with_default_ttl()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(partition_count: u32, learned_at: Instant) -> TopicMeta {
        TopicMeta {
            partition_count,
            leaders: vec![None; partition_count as usize],
            learned_at,
        }
    }

    #[test]
    fn default_ttl_is_thirty_seconds() {
        // Requirement 1.7: default Metadata_TTL of 30 seconds.
        assert_eq!(MetadataCache::DEFAULT_TTL, Duration::from_secs(30));
        assert_eq!(MetadataCache::default().ttl(), Duration::from_secs(30));
    }

    #[test]
    fn miss_when_absent() {
        let cache = MetadataCache::with_default_ttl();
        assert!(cache.is_empty());
        assert_eq!(cache.get_fresh("orders", Instant::now()), None);
    }

    #[test]
    fn hit_within_ttl() {
        // Requirement 1.3: a cached entry within the TTL is reused.
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put("orders", meta(8, t0));

        // A populated cache is non-empty (pins `is_empty` against an always-true
        // mutant, complementing the empty-cache assertions elsewhere).
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        let fresh = cache
            .get_fresh("orders", t0 + Duration::from_secs(29))
            .expect("entry is still fresh");
        assert_eq!(fresh.partition_count, 8);
    }

    #[test]
    fn stale_at_exactly_the_ttl_boundary() {
        // Requirement 1.5: age >= TTL is stale (the boundary itself is stale).
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put("orders", meta(8, t0));

        assert_eq!(
            cache.get_fresh("orders", t0 + Duration::from_secs(30)),
            None
        );
        // Just before the boundary it is still fresh.
        assert!(cache
            .get_fresh("orders", t0 + Duration::from_millis(29_999))
            .is_some());
    }

    #[test]
    fn boundary_is_exact_to_the_nanosecond() {
        // The freshness predicate is `age < ttl`: one nanosecond before the TTL
        // the entry is still fresh, and at exactly the TTL it is stale. Using
        // adjacent instants pins the comparison operator precisely so a mutant
        // flipping `<` to `<=` (or `>`) cannot survive (Requirement 1.3, 1.5).
        let ttl = Duration::from_millis(10);
        let cache = MetadataCache::new(ttl);
        let t0 = Instant::now();
        cache.put("orders", meta(2, t0));

        let one_nano_before = t0 + ttl - Duration::from_nanos(1);
        assert!(
            cache.get_fresh("orders", one_nano_before).is_some(),
            "one nanosecond before the TTL the entry must be fresh"
        );
        assert_eq!(
            cache.get_fresh("orders", t0 + ttl),
            None,
            "at exactly the TTL the entry must be stale"
        );
    }

    #[test]
    fn stale_past_the_ttl() {
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put("orders", meta(8, t0));
        assert_eq!(
            cache.get_fresh("orders", t0 + Duration::from_secs(60)),
            None
        );
    }

    #[test]
    fn now_before_learned_at_is_treated_as_fresh() {
        // Clock skew / paused-time: `now` earlier than `learned_at` saturates to
        // a zero age, which is within any TTL.
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put("orders", meta(4, t0 + Duration::from_secs(5)));
        assert!(cache.get_fresh("orders", t0).is_some());
    }

    #[test]
    fn invalidate_forces_a_miss() {
        // Requirement 1.6: invalidate drops the entry so the next op refreshes.
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put("orders", meta(8, t0));

        let removed = cache.invalidate("orders").expect("entry was present");
        assert_eq!(removed.partition_count, 8);
        assert_eq!(cache.get_fresh("orders", t0), None);
        assert!(cache.is_empty());
    }

    #[test]
    fn invalidate_is_a_noop_when_absent() {
        let cache = MetadataCache::with_default_ttl();
        assert_eq!(cache.invalidate("orders"), None);
        assert!(cache.is_empty());
    }

    #[test]
    fn put_replaces_a_previous_entry() {
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put("orders", meta(4, t0));
        cache.put("orders", meta(16, t0));
        assert_eq!(cache.len(), 1);
        let fresh = cache.get_fresh("orders", t0).expect("fresh");
        assert_eq!(fresh.partition_count, 16);
    }

    #[test]
    fn entries_are_keyed_by_topic() {
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put("orders", meta(8, t0));
        cache.put("events", meta(2, t0));
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.get_fresh("orders", t0).map(|m| m.partition_count),
            Some(8)
        );
        assert_eq!(
            cache.get_fresh("events", t0).map(|m| m.partition_count),
            Some(2)
        );
        assert_eq!(cache.get_fresh("missing", t0), None);
    }

    #[test]
    fn leaders_are_preserved_round_trip() {
        let cache = MetadataCache::new(Duration::from_secs(30));
        let t0 = Instant::now();
        cache.put(
            "orders",
            TopicMeta {
                partition_count: 3,
                leaders: vec![Some("node-a".to_string()), None, Some("node-c".to_string())],
                learned_at: t0,
            },
        );
        let fresh = cache.get_fresh("orders", t0).expect("fresh");
        assert_eq!(
            fresh.leaders,
            vec![Some("node-a".to_string()), None, Some("node-c".to_string())]
        );
    }
}
