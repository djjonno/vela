//! Cache of believed partition leaders.
//!
//! The client caches the leader location of each `(topic, partition)` so that
//! repeated requests skip a `FindLeader` round-trip and go straight to the node
//! it last believed to be the leader (Requirement 11.1). Entries are populated
//! from `FindLeader` responses (resolved to a node address) and from cluster
//! metadata.
//!
//! The cache stores the leader's **address** (a dialable endpoint), already
//! resolved from the node id, so dispatch can build a channel without a second
//! lookup. When a request is redirected or fails because the believed leader is
//! stale, [`LeaderCache::invalidate`] drops the entry so the next request
//! re-resolves the leader — the seam the redirection-retry logic (task 16.2)
//! builds on.

use std::collections::HashMap;
use std::sync::Mutex;

/// The cache key: a topic name and partition index.
type PartitionKey = (String, u32);

/// A thread-safe map from `(topic, partition)` to the believed leader address.
#[derive(Debug, Default)]
pub struct LeaderCache {
    leaders: Mutex<HashMap<PartitionKey, String>>,
}

impl LeaderCache {
    /// Create an empty leader cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `addr` as the believed leader address for `(topic, partition)`,
    /// replacing any previous entry.
    pub fn insert(&self, topic: &str, partition: u32, addr: impl Into<String>) {
        let mut leaders = self.lock();
        leaders.insert((topic.to_string(), partition), addr.into());
    }

    /// Return the believed leader address for `(topic, partition)`, if cached.
    pub fn get(&self, topic: &str, partition: u32) -> Option<String> {
        let leaders = self.lock();
        leaders.get(&(topic.to_string(), partition)).cloned()
    }

    /// Drop any cached leader for `(topic, partition)`.
    ///
    /// Returns the address that was removed, if there was one. Called when a
    /// request reveals the cached leader is stale (a `NotLeader` redirect or a
    /// transport failure) so the next request re-resolves the leader.
    pub fn invalidate(&self, topic: &str, partition: u32) -> Option<String> {
        let mut leaders = self.lock();
        leaders.remove(&(topic.to_string(), partition))
    }

    /// Number of cached leader entries (primarily for tests).
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PartitionKey, String>> {
        self.leaders.lock().expect("leader cache mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_misses_on_empty_cache() {
        let cache = LeaderCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.get("orders", 0), None);
    }

    #[test]
    fn insert_then_lookup_returns_the_address() {
        let cache = LeaderCache::new();
        cache.insert("orders", 3, "http://node-b:50051");
        assert_eq!(
            cache.get("orders", 3).as_deref(),
            Some("http://node-b:50051")
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn entries_are_keyed_by_topic_and_partition() {
        let cache = LeaderCache::new();
        cache.insert("orders", 0, "http://node-a:50051");
        cache.insert("orders", 1, "http://node-b:50051");
        cache.insert("events", 0, "http://node-c:50051");

        assert_eq!(
            cache.get("orders", 0).as_deref(),
            Some("http://node-a:50051")
        );
        assert_eq!(
            cache.get("orders", 1).as_deref(),
            Some("http://node-b:50051")
        );
        assert_eq!(
            cache.get("events", 0).as_deref(),
            Some("http://node-c:50051")
        );
        // A partition that was never inserted is absent.
        assert_eq!(cache.get("events", 1), None);
    }

    #[test]
    fn insert_replaces_a_stale_leader() {
        let cache = LeaderCache::new();
        cache.insert("orders", 0, "http://node-a:50051");
        cache.insert("orders", 0, "http://node-b:50051");
        assert_eq!(
            cache.get("orders", 0).as_deref(),
            Some("http://node-b:50051")
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn invalidate_removes_and_returns_the_entry() {
        let cache = LeaderCache::new();
        cache.insert("orders", 0, "http://node-a:50051");

        let removed = cache.invalidate("orders", 0);
        assert_eq!(removed.as_deref(), Some("http://node-a:50051"));
        assert_eq!(cache.get("orders", 0), None);
        assert!(cache.is_empty());
    }

    #[test]
    fn invalidate_is_a_noop_when_absent() {
        let cache = LeaderCache::new();
        assert_eq!(cache.invalidate("orders", 0), None);
        assert!(cache.is_empty());
    }

    #[test]
    fn invalidate_only_drops_the_targeted_partition() {
        let cache = LeaderCache::new();
        cache.insert("orders", 0, "http://node-a:50051");
        cache.insert("orders", 1, "http://node-b:50051");

        cache.invalidate("orders", 0);
        assert_eq!(cache.get("orders", 0), None);
        assert_eq!(
            cache.get("orders", 1).as_deref(),
            Some("http://node-b:50051")
        );
    }
}
