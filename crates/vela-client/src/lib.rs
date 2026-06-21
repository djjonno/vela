//! `vela-client` — the client library.
//!
//! Provides the three client roles — [`Producer`], [`Consumer`], and
//! [`AdminClient`] — and owns client-to-leader routing. Depends inward on
//! [`vela_proto`] only; it deliberately does **not** depend on `vela-core`, so it
//! reimplements a small client-side [`PartitionRouter`] rather than reusing
//! core's.
//!
//! # Leader routing (this milestone, task 16.1)
//!
//! Each partition request is directed to the node the client *believes* leads
//! that partition (Requirement 11.1):
//!
//! - A [`LeaderCache`] maps `(topic, partition)` to the believed leader address.
//!   On a miss, the client calls `FindLeader`, resolves the returned node id to
//!   an address via a [`NodeRegistry`] seeded from the bootstrap node list, and
//!   caches the result.
//! - A [`ConnectionManager`] keeps one lazily-connected channel per node address.
//! - The [`Producer`] wraps the [`PartitionRouter`] so a record's partition is
//!   resolved *before* dispatch (Requirement 4.1, 4.2).
//!
//! # Leader-redirection and transport-failure retry (task 4.1)
//!
//! [`ClientCore::dispatch`] runs each attempt against the believed leader and
//! [`classify`](crate::ClientCore)s the outcome: it re-resolves the leader on a
//! `NotLeader` redirect or a transport failure, refreshes stale topic metadata
//! on a stale-routing error, and surfaces non-retryable application errors
//! immediately (Requirement 3.2, 3.3, 3.6, 1.6). Retries are bounded by a
//! time-based [`RetryBudget`] with exponential backoff (Requirement 3.4); once
//! the budget is exhausted dispatch returns
//! [`ClientError::NoLeaderAfterRetries`] (Requirement 3.5). [`Producer`] and
//! [`Consumer`] both dispatch through it, so a produce or consume that lands on
//! a non-leader — or a stale/unreachable leader — is transparently retried
//! against the re-resolved leader. All backoff waits and metadata-TTL
//! comparisons are measured against an injected [`Clock`], so the timing is
//! deterministic under a paused tokio runtime in tests.
//!
//! # Example
//!
//! ```no_run
//! # async fn run() -> Result<(), vela_client::ClientError> {
//! use vela_client::{LogBackend, VelaClient};
//!
//! // Bootstrap with (node_id, address) pairs.
//! let client = VelaClient::new([
//!     ("node-a".to_string(), "http://127.0.0.1:50051".to_string()),
//!     ("node-b".to_string(), "http://127.0.0.1:50052".to_string()),
//! ]);
//!
//! client.admin().create_topic("orders", 8, LogBackend::Durable).await?;
//! let offset = client.producer().produce("orders", Some(b"user-42"), b"hello".to_vec()).await?;
//! let batch = client.consumer().consume("orders", 0, 0, None).await?;
//! # let _ = (offset, batch);
//! # Ok(())
//! # }
//! ```

mod admin;
mod connection;
mod consumer;
mod core;
mod error;
mod leader_cache;
mod metadata_cache;
mod producer;
mod retry;
mod router;

use std::sync::Arc;

pub use admin::{AdminClient, LogBackend};
pub use connection::{ConnectionManager, NodeRegistry};
pub use consumer::{ConsumeOutcome, Consumer};
pub use core::{
    resolve_leader, ClientConfig, ClientCore, Clock, LeaderProbe, LeaderResolution, TokioClock,
};
pub use error::{ClientError, Result};
pub use leader_cache::LeaderCache;
pub use metadata_cache::{MetadataCache, TopicMeta};
pub use producer::Producer;
pub use retry::RetryBudget;
pub use router::{KeylessStrategy, PartitionRouter, RouteError};

/// Entry point to the Vela client library.
///
/// Holds the shared [`ClientCore`] (connection pool, leader cache, router, and
/// node registry) and hands out the three client roles. Cloning a [`VelaClient`]
/// is cheap — all roles share one core, so they share one cache and one set of
/// connections.
#[derive(Debug, Clone)]
pub struct VelaClient {
    core: Arc<ClientCore>,
}

impl VelaClient {
    /// Build a client from `(node_id, address)` bootstrap pairs.
    ///
    /// The addresses seed both the node registry (so `FindLeader` results can be
    /// resolved to dialable addresses) and the bootstrap set used for topic-admin
    /// calls and the initial `FindLeader`. Construction performs no network I/O;
    /// connections are established lazily on first use.
    pub fn new(nodes: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            core: Arc::new(ClientCore::new(nodes)),
        }
    }

    /// Build a client from `(node_id, address)` bootstrap pairs with an explicit
    /// [`ClientConfig`].
    ///
    /// Identical to [`VelaClient::new`] except the shared [`ClientCore`] is built
    /// with `config`, so the metadata-cache TTL (Requirement 1.7) and the
    /// router's keyless strategy (Requirement 5.2, 5.6) honor the supplied
    /// settings. Passing [`ClientConfig::default`] is equivalent to
    /// [`VelaClient::new`].
    pub fn with_config(
        nodes: impl IntoIterator<Item = (String, String)>,
        config: ClientConfig,
    ) -> Self {
        Self {
            core: Arc::new(ClientCore::with_config(nodes, config)),
        }
    }

    /// A [`Producer`] sharing this client's core.
    pub fn producer(&self) -> Producer {
        Producer::new(Arc::clone(&self.core))
    }

    /// A [`Consumer`] sharing this client's core.
    pub fn consumer(&self) -> Consumer {
        Consumer::new(Arc::clone(&self.core))
    }

    /// An [`AdminClient`] sharing this client's core.
    pub fn admin(&self) -> AdminClient {
        AdminClient::new(Arc::clone(&self.core))
    }

    /// The shared client core, for advanced use and tests.
    pub fn core(&self) -> &Arc<ClientCore> {
        &self.core
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_share_one_core() {
        let client = VelaClient::new([("node-a".to_string(), "http://node-a:50051".to_string())]);
        // Seed a leader via one role's view of the shared cache...
        client
            .core()
            .leaders()
            .insert("orders", 0, "http://node-a:50051");
        // ...and observe it through the shared core. All roles are backed by the
        // same Arc<ClientCore>.
        assert_eq!(
            client.core().leaders().get("orders", 0).as_deref(),
            Some("http://node-a:50051")
        );
        let _ = (client.producer(), client.consumer(), client.admin());
    }

    #[test]
    fn with_config_builds_a_configured_core() {
        // `VelaClient::with_config` must propagate the metadata TTL and keyless
        // strategy through to the shared core (Requirement 1.7, 5.2, 5.6).
        use std::time::Duration;

        let config = ClientConfig {
            metadata_ttl: Duration::from_secs(7),
            keyless: KeylessStrategy::Sticky { run_length: 16 },
        };
        let client = VelaClient::with_config(
            [("node-a".to_string(), "http://node-a:50051".to_string())],
            config,
        );
        assert_eq!(client.core().metadata().ttl(), Duration::from_secs(7));
        assert_eq!(
            client.core().router().keyless_strategy(),
            KeylessStrategy::Sticky { run_length: 16 }
        );
    }
}
