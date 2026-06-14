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
//! # Leader-redirection retry (task 16.2)
//!
//! On a `NotLeader` response, the client re-resolves the leader and retries —
//! waiting at least [`RETRY_DELAY_MS`] ms before each retry and following at
//! most [`MAX_RETRIES`] redirects before returning
//! [`ClientError::NoLeaderAfterRetries`] (Requirement 11.2–11.4).
//! [`ClientCore::dispatch`] owns this loop: it invalidates/updates the
//! [`LeaderCache`] from the redirect's leader hint (resolved via the
//! [`NodeRegistry`], or re-fetched with `FindLeader` when the hint is absent or
//! unknown) and re-runs the per-attempt operation. [`Producer`] and [`Consumer`]
//! both dispatch through it, so a produce or consume that lands on a non-leader
//! is transparently redirected to the real leader. Non-redirect errors propagate
//! immediately.
//!
//! # Example
//!
//! ```no_run
//! # async fn run() -> Result<(), vela_client::ClientError> {
//! use vela_client::VelaClient;
//!
//! // Bootstrap with (node_id, address) pairs.
//! let client = VelaClient::new([
//!     ("node-a".to_string(), "http://127.0.0.1:50051".to_string()),
//!     ("node-b".to_string(), "http://127.0.0.1:50052".to_string()),
//! ]);
//!
//! client.admin().create_topic("orders", 8).await?;
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
mod producer;
mod router;

use std::sync::Arc;

pub use admin::AdminClient;
pub use connection::{ConnectionManager, NodeRegistry};
pub use consumer::{ConsumeOutcome, Consumer};
pub use core::{ClientCore, MAX_RETRIES, RETRY_DELAY_MS};
pub use error::{ClientError, Result};
pub use leader_cache::LeaderCache;
pub use producer::Producer;
pub use router::PartitionRouter;

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
}
