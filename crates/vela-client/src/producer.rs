//! The produce client.
//!
//! [`Producer::produce`] wraps partition routing so the partition is resolved
//! *before* dispatch (Requirement 4.1, 4.2): it looks up the topic's partition
//! count, asks the client-side [`PartitionRouter`](crate::router::PartitionRouter)
//! for the target partition, resolves that partition's believed leader, and
//! sends the `Produce` RPC straight to it (Requirement 11.1).
//!
//! Routing also guards the zero-partition case (Requirement 1.8, 1.9, 5.3): a
//! topic that exists but reports no partitions yet is not a hard failure. The
//! producer invalidates the topic's cached metadata and re-fetches its partition
//! count on a fixed retry interval until a partition appears or a discovery
//! timeout elapses, surfacing [`ClientError::NoPartitions`] only on timeout.

use std::sync::Arc;
use std::time::Duration;

use vela_proto::v1::{ProduceRequest, Record};

use crate::core::ClientCore;
use crate::error::{ClientError, Result};
use crate::retry::RetryBudget;
use crate::router::RouteError;

/// Interval between zero-partition discovery retries. Mirrors the retry budget's
/// base backoff so the producer re-checks for newly-created partitions at the
/// same cadence the dispatch engine retries (Requirement 1.8).
const DISCOVERY_INTERVAL: Duration = RetryBudget::DEFAULT_BASE;

/// Ceiling on the zero-partition discovery wait before the producer gives up and
/// surfaces [`ClientError::NoPartitions`]. Mirrors the retry budget's total
/// elapsed-time budget (Requirement 1.8, 8.5).
const DISCOVERY_TIMEOUT: Duration = RetryBudget::DEFAULT_TOTAL;

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
        // Resolve the target partition, applying the zero-partition discovery
        // retry before routing (Requirement 1.8, 1.9, 5.3).
        let partition = self.resolve_partition(topic, key).await?;

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

    /// Resolve `(topic, key)` to a partition, applying the zero-partition
    /// discovery retry (Requirement 1.8, 1.9, 5.3).
    ///
    /// On the common path the topic's `partition_count` is non-zero and the
    /// canonical partitioner resolves a partition immediately, with no waiting.
    /// When the topic exists but reports zero partitions the router rejects the
    /// record ([`RouteError::ZeroPartitions`]) rather than computing a partition
    /// against a zero count (Requirement 1.9, 5.3); instead of failing outright,
    /// the producer invalidates the topic's cached metadata so the next lookup
    /// performs a `Metadata_Refresh`, then re-checks the partition count on the
    /// [`DISCOVERY_INTERVAL`] until a partition appears or [`DISCOVERY_TIMEOUT`]
    /// elapses — returning [`ClientError::NoPartitions`] on timeout
    /// (Requirement 1.8, 8.5).
    ///
    /// The discovery waits go through `tokio::time` directly because
    /// [`ClientCore`] exposes no public clock accessor; under a paused tokio
    /// runtime they consume only virtual time, so the loop is deterministically
    /// testable.
    async fn resolve_partition(&self, topic: &str, key: Option<&[u8]>) -> Result<u32> {
        let start = tokio::time::Instant::now();
        loop {
            let partition_count = self.core.partition_count(topic).await?;
            match self.core.router().resolve(topic, key, partition_count) {
                // Normal path: a non-zero count routes to a partition at once.
                Ok(partition) => return Ok(partition),
                // The topic exists but has no partitions yet. Re-discover until
                // one appears or the discovery window is spent (Requirement 1.8).
                Err(RouteError::ZeroPartitions { .. }) => {
                    if start.elapsed() >= DISCOVERY_TIMEOUT {
                        return Err(ClientError::NoPartitions {
                            topic: topic.to_string(),
                        });
                    }
                    // Drop the cached zero-partition metadata so the next
                    // `partition_count` re-fetches it (Requirement 1.5, 1.6),
                    // then wait the retry interval before re-checking.
                    self.core.metadata().invalidate(topic);
                    tokio::time::sleep(DISCOVERY_INTERVAL).await;
                }
            }
        }
    }

    /// The partition this producer would route `(topic, key)` to, given a known
    /// `partition_count`. Exposed for callers and tests that want to inspect
    /// routing without dispatching. Returns [`RouteError`] when
    /// `partition_count == 0` (Requirement 1.9, 5.3).
    pub fn route(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        partition_count: u32,
    ) -> std::result::Result<u32, RouteError> {
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
        let first = p
            .route("orders", Some(b"user-1"), 8)
            .expect("non-zero count");
        for _ in 0..50 {
            assert_eq!(
                p.route("orders", Some(b"user-1"), 8)
                    .expect("non-zero count"),
                first
            );
        }
    }

    #[test]
    fn keyless_routing_covers_partitions_through_the_producer() {
        let p = producer();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4 {
            seen.insert(p.route("orders", None, 4).expect("non-zero count"));
        }
        let expected: std::collections::HashSet<u32> = (0..4u32).collect();
        assert_eq!(seen, expected);
    }

    // --- Zero-partition discovery retry (task 4.2, Requirement 1.8) -------

    use crate::metadata_cache::TopicMeta;

    /// Topic metadata reporting `partition_count` partitions, stamped fresh
    /// against the (paused) tokio clock so [`ClientCore::partition_count`] serves
    /// it from cache without a network round-trip.
    fn topic_meta(partition_count: u32) -> TopicMeta {
        TopicMeta {
            partition_count,
            leaders: vec![None; partition_count as usize],
            learned_at: tokio::time::Instant::now().into_std(),
        }
    }

    /// A zero-partition topic that never gains a partition surfaces
    /// [`ClientError::NoPartitions`] once the discovery timeout elapses
    /// (Requirement 1.8, 8.5).
    ///
    /// A background task emulates the cluster, keeping the metadata cache
    /// populated with a fresh zero-partition entry at a cadence finer than the
    /// producer's discovery interval, so each re-check the producer makes after
    /// invalidating still reads a zero count (never falling through to a network
    /// fetch). The producer must give up and report no partitions rather than
    /// loop forever.
    #[tokio::test(start_paused = true)]
    async fn zero_partition_topic_times_out_with_no_partitions() {
        let p = producer();
        let core = Arc::clone(&p.core);
        // Seed a fresh zero-partition entry so the first lookup sees the topic
        // exists but has no partitions yet.
        core.metadata().put("orders", topic_meta(0));

        let server = {
            let core = Arc::clone(&core);
            async move {
                loop {
                    core.metadata().put("orders", topic_meta(0));
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        };

        let result = tokio::select! {
            res = p.resolve_partition("orders", None) => res,
            _ = server => unreachable!("the server emulation runs until the producer gives up"),
        };

        assert!(
            matches!(result, Err(ClientError::NoPartitions { topic }) if topic == "orders"),
            "a topic that never gains a partition must time out with NoPartitions",
        );
    }

    /// A topic that starts with zero partitions but gains them mid-discovery
    /// resolves successfully once a partition appears, instead of failing
    /// (Requirement 1.8).
    ///
    /// The emulated cluster reports zero partitions for the first 250 ms, then
    /// four partitions thereafter. The producer's discovery loop must pick up the
    /// new count on a subsequent re-check and route the record within range.
    #[tokio::test(start_paused = true)]
    async fn zero_partition_topic_then_gains_a_partition_routes() {
        let p = producer();
        let core = Arc::clone(&p.core);
        // Seed a fresh zero-partition entry for the first lookup.
        core.metadata().put("orders", topic_meta(0));

        let started = tokio::time::Instant::now();
        let server = {
            let core = Arc::clone(&core);
            async move {
                loop {
                    let count = if started.elapsed() >= Duration::from_millis(250) {
                        4
                    } else {
                        0
                    };
                    core.metadata().put("orders", topic_meta(count));
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        };

        let partition = tokio::select! {
            res = p.resolve_partition("orders", Some(b"user-1")) => {
                res.expect("routing succeeds once a partition appears")
            }
            _ = server => unreachable!("the server emulation runs until the producer resolves"),
        };

        assert!(
            partition < 4,
            "the record must route within the discovered partition range, got {partition}",
        );
    }
}
