//! Connection management and node-address resolution.
//!
//! Two small pieces back the client's dispatch:
//!
//! - [`ConnectionManager`] caches one lazily-connected tonic [`Channel`] per node
//!   address. Channels are cheap to clone and multiplex concurrent RPCs, so the
//!   client keeps one per node and reuses it for every partition that node leads.
//! - [`NodeRegistry`] maps a node **id** (what `FindLeader` returns) to its
//!   **address** (what we dial). It is seeded from the bootstrap node list and
//!   can be extended as metadata is learned.
//!
//! Channels are built with [`Endpoint::connect_lazy`], so constructing the client
//! and registering nodes performs no network I/O — the connection is established
//! on first use. This keeps unit construction offline.

use std::collections::HashMap;
use std::sync::Mutex;

use tonic::transport::{Channel, Endpoint};

use crate::error::{ClientError, Result};

/// Caches one lazily-connected [`Channel`] per node address.
#[derive(Debug, Default)]
pub struct ConnectionManager {
    channels: Mutex<HashMap<String, Channel>>,
}

impl ConnectionManager {
    /// Create an empty connection manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a channel for `addr`, building and caching one if necessary.
    ///
    /// The channel is created lazily and clones share the same underlying
    /// connection pool, so this never blocks on a network round-trip. An address
    /// that cannot be parsed into an endpoint URI yields
    /// [`ClientError::InvalidAddress`].
    pub fn channel(&self, addr: &str) -> Result<Channel> {
        // Normalize to a dialable URL. A configured `id=url` Endpoint already
        // carries a scheme (`http://host:port`), but an address learned from the
        // server's `Member_Address_Map` (and a peer's `FindLeader` hint) is a
        // bare transport address (`host:port`), which `Endpoint::from_shared`
        // rejects for lack of a scheme. Prepend `http://` when no scheme is
        // present so both forms dial the same node, and key the channel cache on
        // the normalized URL so the two forms share one connection.
        let url = normalize_addr(addr);
        let mut channels = self.lock();
        if let Some(channel) = channels.get(&url) {
            return Ok(channel.clone());
        }
        let endpoint =
            Endpoint::from_shared(url.clone()).map_err(|source| ClientError::InvalidAddress {
                addr: addr.to_string(),
                source,
            })?;
        let channel = endpoint.connect_lazy();
        channels.insert(url, channel.clone());
        Ok(channel)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Channel>> {
        self.channels
            .lock()
            .expect("connection manager mutex poisoned")
    }
}

/// Ensure `addr` is a dialable URL by prepending `http://` when it carries no
/// `http`/`https` scheme.
///
/// Configured `id=url` Endpoints already include a scheme and pass through
/// unchanged; bare `host:port` transport addresses (from the server's
/// `Member_Address_Map` or a `FindLeader` leader hint) gain the default
/// `http://` scheme so they can be parsed into a tonic [`Endpoint`].
///
/// Exposed within the crate so leader resolution can de-duplicate its probe set
/// by each node's dialable form, treating a configured `http://host:port` and a
/// discovered bare `host:port` as the same node.
pub(crate) fn normalize_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Maps node ids to their transport addresses.
///
/// `FindLeader` identifies a leader by node id; to dial it the client must
/// resolve that id to an address. The registry is seeded from the bootstrap node
/// list and may grow as the client learns of more nodes.
#[derive(Debug, Default)]
pub struct NodeRegistry {
    addrs: Mutex<HashMap<String, String>>,
}

impl NodeRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry from `(node_id, addr)` pairs.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        let registry = Self::new();
        for (id, addr) in pairs {
            registry.insert(id, addr);
        }
        registry
    }

    /// Record (or update) the address for `node_id`.
    pub fn insert(&self, node_id: impl Into<String>, addr: impl Into<String>) {
        let mut addrs = self.lock();
        addrs.insert(node_id.into(), addr.into());
    }

    /// Record the address for `node_id` only if it is not already known,
    /// returning `true` when the entry was added.
    ///
    /// Used to seed addresses learned from the server's `Member_Address_Map`
    /// without clobbering an operator-supplied `id=url` Endpoint. The configured
    /// endpoints are authoritative: only the operator knows how their client
    /// reaches each node (e.g. through published ports / NAT), whereas the
    /// cluster's self-reported member addresses are its *internal* view (a bind
    /// address like `0.0.0.0:7001`, or a peer hostname only resolvable inside the
    /// cluster network). Discovery therefore *fills gaps* — ids the operator did
    /// not supply — rather than overriding a working configured address.
    pub fn insert_if_absent(&self, node_id: impl Into<String>, addr: impl Into<String>) -> bool {
        use std::collections::hash_map::Entry;
        let mut addrs = self.lock();
        match addrs.entry(node_id.into()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(slot) => {
                slot.insert(addr.into());
                true
            }
        }
    }

    /// Resolve a node id to its address, if known.
    pub fn addr_of(&self, node_id: &str) -> Option<String> {
        let addrs = self.lock();
        addrs.get(node_id).cloned()
    }

    /// A snapshot of every known node address, in arbitrary order.
    ///
    /// Used by leader resolution to probe not just the configured bootstrap
    /// endpoints but every member the client has learned of (via cluster
    /// discovery), so a partition's leader can be found even when no bootstrap
    /// node hosts that partition's replica. Order is unspecified (the backing
    /// map is unordered); callers must not depend on it.
    pub fn addrs(&self) -> Vec<String> {
        let addrs = self.lock();
        addrs.values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, String>> {
        self.addrs.lock().expect("node registry mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_is_cached_and_reused_per_address() {
        let manager = ConnectionManager::new();
        // connect_lazy does no network I/O but needs a tokio runtime to hold the
        // connection pool's reactor handle.
        let first = manager.channel("http://node-a:50051").expect("valid uri");
        let second = manager.channel("http://node-a:50051").expect("valid uri");
        // Both calls return a (cloned) channel for the same address; only one
        // entry is cached.
        drop((first, second));
        assert_eq!(manager.channels.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn distinct_addresses_get_distinct_channels() {
        let manager = ConnectionManager::new();
        let _ = manager.channel("http://node-a:50051").expect("valid uri");
        let _ = manager.channel("http://node-b:50051").expect("valid uri");
        assert_eq!(manager.channels.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn schemeless_and_id_url_forms_share_one_channel() {
        // A bare `host:port` (as learned from the server's Member_Address_Map)
        // and the equivalent `http://host:port` (a configured id=url Endpoint)
        // normalize to the same URL, so they dial the same node and share a
        // single cached channel rather than failing to parse or duplicating.
        let manager = ConnectionManager::new();
        let bare = manager
            .channel("node-a:50051")
            .expect("bare host:port dials");
        let with_scheme = manager
            .channel("http://node-a:50051")
            .expect("id=url form dials");
        drop((bare, with_scheme));
        assert_eq!(manager.channels.lock().unwrap().len(), 1);
    }

    #[test]
    fn invalid_address_is_rejected() {
        let manager = ConnectionManager::new();
        let err = manager.channel("not a uri").unwrap_err();
        assert!(matches!(err, ClientError::InvalidAddress { .. }));
    }

    #[test]
    fn registry_resolves_known_ids_and_misses_unknown() {
        let registry = NodeRegistry::from_pairs([
            ("node-a".to_string(), "http://node-a:50051".to_string()),
            ("node-b".to_string(), "http://node-b:50051".to_string()),
        ]);
        assert_eq!(
            registry.addr_of("node-a").as_deref(),
            Some("http://node-a:50051")
        );
        assert_eq!(
            registry.addr_of("node-b").as_deref(),
            Some("http://node-b:50051")
        );
        assert_eq!(registry.addr_of("node-c"), None);
    }

    #[test]
    fn registry_insert_updates_address() {
        let registry = NodeRegistry::new();
        registry.insert("node-a", "http://old:50051");
        registry.insert("node-a", "http://new:50051");
        assert_eq!(
            registry.addr_of("node-a").as_deref(),
            Some("http://new:50051")
        );
    }
}
