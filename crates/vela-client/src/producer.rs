//! The produce client.
//!
//! [`Producer::produce`] wraps partition routing so the partition is resolved
//! *before* dispatch (Requirement 4.1, 4.2): it looks up the topic's partition
//! count, asks the client-side [`PartitionRouter`](crate::router::PartitionRouter)
//! for the target partition, resolves that partition's believed leader, and
//! sends the `Produce` RPC straight to it (Requirement 11.1).

use std::sync::Arc;

use vela_proto::v1::{ProduceRequest, Record};

use crate::core::ClientCore;
use crate::error::Result;

/// Produces records to topics, routing each record to a partition leader.
#[derive(Debug, Clone)]
pub struct Producer {
    core: Arc<ClientCore>,
}

impl Producer {
    /// Create a producer over a shared client core.
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }

    /// Produce `value` to `topic`, optionally keyed by `key`.
    ///
    /// Routing happens client-side before dispatch:
    /// 1. resolve the topic's partition count (cached after first lookup);
    /// 2. route `(topic, key)` to a partition — a non-empty key maps
    ///    deterministically, a `None`/empty key round-robins (Requirement 4.1,
    ///    4.2);
    /// 3. send `Produce` to that partition's believed leader, retrying through
    ///    [`ClientCore::dispatch`] if the leader redirects us with `NotLeader`
    ///    (Requirement 11.1–11.4).
    ///
    /// Returns the committed 0-based offset assigned by the leader
    /// (Requirement 4.4).
    pub async fn produce(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64> {
        let partition_count = self.core.partition_count(topic).await?;
        let partition = self.core.router().resolve(topic, key, partition_count);

        // Captured by the per-attempt closure; cloned on each attempt so a
        // redirect can re-send the identical request to the new leader.
        let core = Arc::clone(&self.core);
        let topic_owned = topic.to_string();
        let key_owned = key.map(<[u8]>::to_vec);
        let value = value.into();

        self.core
            .dispatch(topic, partition, move |addr| {
                let core = Arc::clone(&core);
                let topic = topic_owned.clone();
                let key = key_owned.clone();
                let value = value.clone();
                async move {
                    let mut client = core.client_for(&addr)?;
                    let response = client
                        .produce(ProduceRequest {
                            topic,
                            partition,
                            record: Some(Record { key, value }),
                        })
                        .await?
                        .into_inner();
                    Ok(response.offset)
                }
            })
            .await
    }

    /// The partition this producer would route `(topic, key)` to, given a known
    /// `partition_count`. Exposed for callers and tests that want to inspect
    /// routing without dispatching.
    pub fn route(&self, topic: &str, key: Option<&[u8]>, partition_count: u32) -> u32 {
        self.core.router().resolve(topic, key, partition_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer() -> Producer {
        Producer::new(Arc::new(ClientCore::new([(
            "node-a".to_string(),
            "http://node-a:50051".to_string(),
        )])))
    }

    #[test]
    fn keyed_routing_is_deterministic_through_the_producer() {
        let p = producer();
        let first = p.route("orders", Some(b"user-1"), 8);
        for _ in 0..50 {
            assert_eq!(p.route("orders", Some(b"user-1"), 8), first);
        }
    }

    #[test]
    fn keyless_routing_covers_partitions_through_the_producer() {
        let p = producer();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4 {
            seen.insert(p.route("orders", None, 4));
        }
        let expected: std::collections::HashSet<u32> = (0..4u32).collect();
        assert_eq!(seen, expected);
    }
}
