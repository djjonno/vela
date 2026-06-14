//! Shared client core.
//!
//! [`ClientCore`] holds the state common to [`Producer`], [`Consumer`], and
//! [`AdminClient`]: the connection pool, the node-id→address registry, the
//! believed-leader cache, the partition router, and a small topic-metadata
//! cache (partition counts). The three public client roles each hold an
//! `Arc<ClientCore>` so they share one cache and one connection pool.
//!
//! This is also where the **leader-routing seam** lives. [`ClientCore::leader_addr`]
//! returns the believed leader address for a partition, resolving it via
//! `FindLeader` on a cache miss. [`ClientCore::dispatch`] runs a single attempt
//! against that believed leader and, on a `NotLeader` redirect, re-resolves the
//! leader and retries — waiting at least [`RETRY_DELAY_MS`] ms before each retry
//! and following at most [`MAX_RETRIES`] redirects before returning
//! [`ClientError::NoLeaderAfterRetries`] (Requirement 11.2–11.4). Non-redirect
//! errors propagate immediately.
//!
//! [`Producer`]: crate::Producer
//! [`Consumer`]: crate::Consumer
//! [`AdminClient`]: crate::AdminClient

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use prost::Message as _;
use tonic::transport::Channel;
use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_proto::v1::{self, FindLeaderRequest};

use crate::connection::{ConnectionManager, NodeRegistry};
use crate::error::{ClientError, Result};
use crate::leader_cache::LeaderCache;
use crate::router::PartitionRouter;

/// Minimum delay the client waits before each redirection retry (Requirement
/// 11.3). The wait is a *floor*: re-resolving the leader may add to it.
pub const RETRY_DELAY_MS: u64 = 100;

/// Maximum number of `NotLeader` redirections the client follows for a single
/// request before giving up and returning [`ClientError::NoLeaderAfterRetries`]
/// (Requirement 11.4).
pub const MAX_RETRIES: u32 = 5;

/// The decision the retry loop makes after one dispatch attempt.
///
/// Factored out as a pure function ([`plan_retry`]) of "did the attempt produce
/// a `NotLeader` redirect?" and "how many redirects have we already followed?"
/// so the retry/backoff bookkeeping is unit-testable without a live server.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RetryPlan {
    /// The attempt did not redirect; surface its result (success or error) to
    /// the caller unchanged.
    Surface,
    /// A `NotLeader` redirect: wait the mandatory delay, re-resolve to the
    /// hinted leader (`Some(node_id)`) or via `FindLeader` (`None`), and retry.
    RetryAfterDelay {
        /// The leader node-id hint carried by the redirect, if any.
        hint: Option<String>,
    },
    /// The redirect budget is exhausted; return the no-leader-found error
    /// (Requirement 11.4).
    GiveUp,
}

/// Decide the next step after a dispatch attempt.
///
/// `redirect` is `Some(hint)` when the attempt failed with a `NotLeader`
/// redirect (carrying an optional leader node-id `hint`), and `None` for any
/// other outcome — success or a non-redirect error — which is surfaced as-is.
/// `redirects_done` is the number of redirects already followed for this
/// request. Once that reaches [`MAX_RETRIES`], a further redirect yields
/// [`RetryPlan::GiveUp`] (Requirement 11.3, 11.4).
pub(crate) fn plan_retry(redirect: Option<Option<String>>, redirects_done: u32) -> RetryPlan {
    match redirect {
        None => RetryPlan::Surface,
        Some(_) if redirects_done >= MAX_RETRIES => RetryPlan::GiveUp,
        Some(hint) => RetryPlan::RetryAfterDelay { hint },
    }
}

/// Inspect a client error and, if it is a `NotLeader` redirect, return its
/// optional leader node-id hint.
///
/// Returns `None` for any non-redirect error (which the retry layer propagates
/// unchanged), and `Some(hint)` when the error is an RPC failure whose details
/// decode to a [`v1::VelaError`] classified [`v1::ErrorCode::NotLeader`]. The
/// inner `hint` is the leader node id the server suggested, when known.
pub(crate) fn not_leader_hint(error: &ClientError) -> Option<Option<String>> {
    let ClientError::Rpc(status) = error else {
        return None;
    };
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    let vela_error = v1::VelaError::decode(details).ok()?;
    if vela_error.code == v1::ErrorCode::NotLeader as i32 {
        Some(vela_error.leader)
    } else {
        None
    }
}

/// State shared by every client role.
#[derive(Debug)]
pub struct ClientCore {
    /// Lazily-connected channels keyed by node address.
    connections: ConnectionManager,
    /// node id → address resolution, seeded from the bootstrap list.
    registry: NodeRegistry,
    /// Believed leader address per `(topic, partition)`.
    leaders: LeaderCache,
    /// Client-side `(topic, key) → partition` resolver.
    router: PartitionRouter,
    /// Cached partition counts per topic, populated via `DescribeTopic`.
    topic_partitions: Mutex<HashMap<String, u32>>,
    /// Bootstrap node addresses, used for admin calls and the initial
    /// `FindLeader` before any leader is cached.
    bootstrap: Vec<String>,
}

impl ClientCore {
    /// Build a core from `(node_id, addr)` bootstrap pairs.
    ///
    /// The addresses double as the registry seed (so `FindLeader` results can be
    /// resolved to addresses) and as the bootstrap set for admin / initial
    /// `FindLeader` calls.
    pub fn new(nodes: impl IntoIterator<Item = (String, String)>) -> Self {
        let pairs: Vec<(String, String)> = nodes.into_iter().collect();
        let bootstrap = pairs.iter().map(|(_, addr)| addr.clone()).collect();
        Self {
            connections: ConnectionManager::new(),
            registry: NodeRegistry::from_pairs(pairs),
            leaders: LeaderCache::new(),
            router: PartitionRouter::new(),
            topic_partitions: Mutex::new(HashMap::new()),
            bootstrap,
        }
    }

    /// The client-side partition router.
    pub fn router(&self) -> &PartitionRouter {
        &self.router
    }

    /// The believed-leader cache.
    pub fn leaders(&self) -> &LeaderCache {
        &self.leaders
    }

    /// The node-id→address registry.
    pub fn registry(&self) -> &NodeRegistry {
        &self.registry
    }

    /// Build a `VelaClient` gRPC stub for an explicit node address.
    pub fn client_for(&self, addr: &str) -> Result<VelaClientClient<Channel>> {
        Ok(VelaClientClient::new(self.connections.channel(addr)?))
    }

    /// Build a `VelaClient` stub against a bootstrap node (round-robin-free:
    /// the first configured node). Used for topic-admin RPCs and the initial
    /// `FindLeader`, which any node can serve or forward.
    pub fn bootstrap_client(&self) -> Result<VelaClientClient<Channel>> {
        let addr = self.bootstrap.first().ok_or(ClientError::NoNodes)?;
        self.client_for(addr)
    }

    /// Resolve the believed leader address for `(topic, partition)`.
    ///
    /// Returns the cached address on a hit; on a miss, calls `FindLeader`,
    /// resolves the returned node id to an address, caches it, and returns it
    /// (Requirement 11.1). The result is the node the client *believes* leads the
    /// partition; verifying that belief (and re-resolving on a `NotLeader`
    /// redirect) is the retry layer's job (task 16.2).
    pub async fn leader_addr(&self, topic: &str, partition: u32) -> Result<String> {
        if let Some(addr) = self.leaders.get(topic, partition) {
            return Ok(addr);
        }
        self.refresh_leader(topic, partition).await
    }

    /// Force a `FindLeader` lookup for `(topic, partition)`, updating the cache.
    ///
    /// Exposed so the retry layer can re-resolve after invalidating a stale
    /// entry.
    pub async fn refresh_leader(&self, topic: &str, partition: u32) -> Result<String> {
        let mut client = self.bootstrap_client()?;
        let response = client
            .find_leader(FindLeaderRequest {
                topic: topic.to_string(),
                partition,
            })
            .await?
            .into_inner();

        let node = response.leader.ok_or_else(|| ClientError::NoLeader {
            topic: topic.to_string(),
            partition,
        })?;

        let addr = self
            .registry
            .addr_of(&node)
            .ok_or_else(|| ClientError::UnknownNode {
                node,
                topic: topic.to_string(),
                partition,
            })?;

        self.leaders.insert(topic, partition, addr.clone());
        Ok(addr)
    }

    /// Run `operation` against the partition's believed leader, retrying on a
    /// `NotLeader` redirect (Requirement 11.2–11.4).
    ///
    /// The flow:
    /// 1. resolve the believed leader address (cache hit, or `FindLeader`);
    /// 2. call `operation(addr)` once;
    /// 3. if it succeeds — or fails with anything other than a `NotLeader`
    ///    redirect — return that result unchanged (non-redirect errors propagate
    ///    immediately);
    /// 4. on a `NotLeader` redirect, wait at least [`RETRY_DELAY_MS`] ms, then
    ///    re-resolve the leader from the redirect's hint (or via `FindLeader`)
    ///    and retry, up to [`MAX_RETRIES`] times;
    /// 5. once the redirect budget is exhausted, return
    ///    [`ClientError::NoLeaderAfterRetries`].
    ///
    /// `operation` is an `async` closure of the leader address; it is invoked
    /// once per attempt, so it must be cheaply re-runnable (clone its inputs).
    /// This is the seam [`Producer`](crate::Producer) and
    /// [`Consumer`](crate::Consumer) dispatch through so a request that lands on
    /// a non-leader is transparently redirected to the real leader.
    pub async fn dispatch<T, F, Fut>(&self, topic: &str, partition: u32, operation: F) -> Result<T>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut addr = self.leader_addr(topic, partition).await?;
        let mut redirects_done = 0u32;

        loop {
            let attempt = operation(addr.clone()).await;
            let redirect = match &attempt {
                Err(error) => not_leader_hint(error),
                Ok(_) => None,
            };

            match plan_retry(redirect, redirects_done) {
                RetryPlan::Surface => return attempt,
                RetryPlan::GiveUp => {
                    return Err(ClientError::NoLeaderAfterRetries {
                        topic: topic.to_string(),
                        partition,
                    });
                }
                RetryPlan::RetryAfterDelay { hint } => {
                    redirects_done += 1;
                    // Wait the mandatory floor before retrying (Requirement 11.3).
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    addr = self.resolve_redirect(topic, partition, hint).await?;
                }
            }
        }
    }

    /// Resolve the leader address to retry against after a `NotLeader` redirect,
    /// updating the leader cache.
    ///
    /// If the redirect carried a node-id `hint` that resolves via the registry,
    /// the cache is updated to that address and it is returned without any
    /// network call. Otherwise (no hint, or an unresolvable one) the stale entry
    /// is invalidated and the leader is re-resolved via `FindLeader`.
    async fn resolve_redirect(
        &self,
        topic: &str,
        partition: u32,
        hint: Option<String>,
    ) -> Result<String> {
        if let Some(node) = hint {
            if let Some(addr) = self.registry.addr_of(&node) {
                self.leaders.insert(topic, partition, addr.clone());
                return Ok(addr);
            }
        }
        // No usable hint: drop the stale belief and ask the cluster afresh.
        self.leaders.invalidate(topic, partition);
        self.refresh_leader(topic, partition).await
    }

    /// Resolve a topic's partition count, caching it after the first lookup.
    ///
    /// The producer needs the partition count to route a key; it is learned from
    /// `DescribeTopic` and cached so subsequent produces skip the round-trip.
    pub async fn partition_count(&self, topic: &str) -> Result<u32> {
        if let Some(count) = self
            .topic_partitions
            .lock()
            .expect("topic metadata mutex poisoned")
            .get(topic)
            .copied()
        {
            return Ok(count);
        }

        let mut client = self.bootstrap_client()?;
        let response = client
            .describe_topic(vela_proto::v1::DescribeTopicRequest {
                name: topic.to_string(),
            })
            .await?
            .into_inner();

        let count = response
            .topic
            .ok_or_else(|| ClientError::MalformedResponse(format!("DescribeTopic({topic})")))?
            .partition_count;

        self.topic_partitions
            .lock()
            .expect("topic metadata mutex poisoned")
            .insert(topic.to_string(), count);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core() -> ClientCore {
        ClientCore::new([
            ("node-a".to_string(), "http://node-a:50051".to_string()),
            ("node-b".to_string(), "http://node-b:50051".to_string()),
        ])
    }

    #[tokio::test]
    async fn leader_addr_is_a_cache_hit_when_preseeded() {
        let core = core();
        core.leaders().insert("orders", 2, "http://node-b:50051");
        // A cached entry is returned without any network call, so this resolves
        // even with no server running.
        let addr = core.leader_addr("orders", 2).await.expect("cache hit");
        assert_eq!(addr, "http://node-b:50051");
    }

    #[test]
    fn no_bootstrap_nodes_is_an_error() {
        let core = ClientCore::new(std::iter::empty());
        let err = core.bootstrap_client().unwrap_err();
        assert!(matches!(err, ClientError::NoNodes));
    }

    #[test]
    fn registry_is_seeded_from_bootstrap_pairs() {
        let core = core();
        assert_eq!(
            core.registry().addr_of("node-a").as_deref(),
            Some("http://node-a:50051")
        );
        assert_eq!(
            core.registry().addr_of("node-b").as_deref(),
            Some("http://node-b:50051")
        );
    }

    // --- Redirection retry decision logic (task 16.2) --------------------

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use proptest::prelude::*;

    /// A `NotLeader` redirect status carrying an optional leader node-id hint,
    /// shaped exactly as the server emits it: a `VelaError` encoded into the
    /// status details.
    fn not_leader_status(hint: Option<&str>) -> tonic::Status {
        let vela_error = v1::VelaError {
            code: v1::ErrorCode::NotLeader as i32,
            message: "not the leader for this partition".to_string(),
            leader: hint.map(str::to_string),
        };
        let details = prost::bytes::Bytes::from(vela_error.encode_to_vec());
        tonic::Status::with_details(tonic::Code::FailedPrecondition, "not leader", details)
    }

    fn not_leader_error(hint: Option<&str>) -> ClientError {
        ClientError::Rpc(Box::new(not_leader_status(hint)))
    }

    #[test]
    fn retry_consts_match_requirements() {
        // Requirement 11.3 (>=100 ms before each retry) and 11.4 (<=5 retries).
        assert_eq!(RETRY_DELAY_MS, 100);
        assert_eq!(MAX_RETRIES, 5);
    }

    #[test]
    fn plan_retry_surfaces_a_non_redirect() {
        // A non-`NotLeader` outcome (success or other error) is surfaced as-is.
        assert_eq!(plan_retry(None, 0), RetryPlan::Surface);
        assert_eq!(plan_retry(None, MAX_RETRIES), RetryPlan::Surface);
    }

    #[test]
    fn plan_retry_retries_within_budget() {
        for redirects_done in 0..MAX_RETRIES {
            assert_eq!(
                plan_retry(Some(Some("node-b".to_string())), redirects_done),
                RetryPlan::RetryAfterDelay {
                    hint: Some("node-b".to_string())
                }
            );
        }
    }

    #[test]
    fn plan_retry_gives_up_at_the_budget() {
        // Once MAX_RETRIES redirects have been followed, a further redirect stops.
        assert_eq!(
            plan_retry(Some(Some("node-b".to_string())), MAX_RETRIES),
            RetryPlan::GiveUp
        );
        assert_eq!(plan_retry(Some(None), MAX_RETRIES + 1), RetryPlan::GiveUp);
    }

    /// Drive [`plan_retry`] over a stream of identical attempt outcomes,
    /// counting the redirects followed and returning the terminal plan — the
    /// retry/backoff bookkeeping with no I/O or clock.
    fn simulate(redirects_each_attempt: bool) -> (u32, RetryPlan) {
        let mut redirects_done = 0u32;
        loop {
            let redirect = redirects_each_attempt.then_some(None);
            match plan_retry(redirect, redirects_done) {
                RetryPlan::RetryAfterDelay { .. } => redirects_done += 1,
                terminal => return (redirects_done, terminal),
            }
        }
    }

    #[test]
    fn endless_redirects_give_up_after_exactly_max_retries() {
        // A leader that never settles: the client follows exactly MAX_RETRIES
        // redirects, then returns the no-leader plan (Requirement 11.4).
        assert_eq!(simulate(true), (MAX_RETRIES, RetryPlan::GiveUp));
    }

    #[test]
    fn a_non_redirect_stops_immediately_with_no_retries() {
        assert_eq!(simulate(false), (0, RetryPlan::Surface));
    }

    #[test]
    fn not_leader_hint_extracts_a_present_hint() {
        assert_eq!(
            not_leader_hint(&not_leader_error(Some("node-b"))),
            Some(Some("node-b".to_string()))
        );
    }

    #[test]
    fn not_leader_hint_recognizes_a_hintless_redirect() {
        // A `NotLeader` with no leader yet known is still a redirect (re-resolve
        // via FindLeader), distinct from "not a redirect".
        assert_eq!(not_leader_hint(&not_leader_error(None)), Some(None));
    }

    #[test]
    fn not_leader_hint_ignores_non_redirect_errors() {
        // A different error code in the details is not a redirect.
        let other = v1::VelaError {
            code: v1::ErrorCode::TopicNotFound as i32,
            message: "no such topic".to_string(),
            leader: None,
        };
        let details = prost::bytes::Bytes::from(other.encode_to_vec());
        let status = tonic::Status::with_details(tonic::Code::NotFound, "missing", details);
        assert_eq!(not_leader_hint(&ClientError::Rpc(Box::new(status))), None);

        // A status with no typed details is not a redirect either.
        let bare = tonic::Status::new(tonic::Code::Internal, "boom");
        assert_eq!(not_leader_hint(&ClientError::Rpc(Box::new(bare))), None);

        // A non-RPC client error is never a redirect.
        assert_eq!(not_leader_hint(&ClientError::NoNodes), None);
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_returns_the_first_success_without_retrying() {
        let core = core();
        core.leaders().insert("orders", 0, "http://node-a:50051");
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = Arc::clone(&calls);

        let result: Result<u64> = core
            .dispatch("orders", 0, move |_addr| {
                let calls = Arc::clone(&calls_in);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(7)
                }
            })
            .await;

        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_propagates_a_non_redirect_error_immediately() {
        let core = core();
        core.leaders().insert("orders", 0, "http://node-a:50051");
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = Arc::clone(&calls);

        let result: Result<u64> = core
            .dispatch("orders", 0, move |_addr| {
                let calls = Arc::clone(&calls_in);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(ClientError::Rpc(Box::new(tonic::Status::new(
                        tonic::Code::Internal,
                        "boom",
                    ))))
                }
            })
            .await;

        assert!(matches!(result, Err(ClientError::Rpc(_))));
        // No retry: a non-redirect error stops on the first attempt.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_follows_a_hint_then_succeeds() {
        let core = core();
        core.leaders().insert("orders", 0, "http://node-a:50051");
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = Arc::clone(&calls);

        let result: Result<u64> = core
            .dispatch("orders", 0, move |addr| {
                let calls = Arc::clone(&calls_in);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if addr == "http://node-a:50051" {
                        // node-a is stale; redirect to node-b.
                        Err(not_leader_error(Some("node-b")))
                    } else {
                        Ok(99)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 99);
        // Exactly one redirect was followed.
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        // The cache now points at the redirect target, resolved via the registry.
        assert_eq!(
            core.leaders().get("orders", 0).as_deref(),
            Some("http://node-b:50051")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_gives_up_after_max_retries() {
        let core = core();
        core.leaders().insert("orders", 0, "http://node-a:50051");
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = Arc::clone(&calls);

        // Every attempt redirects (to a resolvable node), so the client never
        // lands on a usable leader and exhausts its budget.
        let result: Result<u64> = core
            .dispatch("orders", 0, move |_addr| {
                let calls = Arc::clone(&calls_in);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(not_leader_error(Some("node-b")))
                }
            })
            .await;

        assert!(matches!(
            result,
            Err(ClientError::NoLeaderAfterRetries {
                ref topic,
                partition: 0,
            }) if topic == "orders"
        ));
        // Initial attempt + MAX_RETRIES redirected attempts.
        assert_eq!(calls.load(Ordering::SeqCst), 1 + MAX_RETRIES);
    }

    // --- Redirection retry behavior, over arbitrary redirect runs (task 16.3) -

    /// Run one `dispatch` whose operation redirects (`NotLeader`) on its first
    /// `k` attempts and then succeeds, on a paused virtual clock so the retry
    /// delays cost no wall-clock time and elapsed time is exactly the sum of the
    /// waits. The redirect hint always names `node-b`, which the registry can
    /// resolve, so re-resolution never makes a `FindLeader` network call.
    ///
    /// Returns `(result, attempts, elapsed)`: the dispatch outcome, the number
    /// of times the operation was invoked, and the virtual time that elapsed.
    fn run_redirect_dispatch(k: u32) -> (Result<u64>, u32, Duration) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("build paused current-thread runtime");

        rt.block_on(async move {
            let core = core();
            core.leaders().insert("orders", 0, "http://node-a:50051");
            let calls = Arc::new(AtomicU32::new(0));
            let calls_in = Arc::clone(&calls);

            let start = tokio::time::Instant::now();
            let result: Result<u64> = core
                .dispatch("orders", 0, move |_addr| {
                    let calls = Arc::clone(&calls_in);
                    async move {
                        // `fetch_add` returns the count *before* this attempt:
                        // attempts 0..k redirect, attempt k (and later) succeed.
                        let prior = calls.fetch_add(1, Ordering::SeqCst);
                        if prior < k {
                            // Redirect to a registry-resolvable node so the retry
                            // path updates the cache without any network call.
                            Err(not_leader_error(Some("node-b")))
                        } else {
                            Ok(7u64)
                        }
                    }
                })
                .await;
            let elapsed = start.elapsed();
            (result, calls.load(Ordering::SeqCst), elapsed)
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: vela-streaming-platform, Property 38
        //
        // Property 38: Client retries redirections with a minimum delay and a
        // bounded count. Over any run of `NotLeader` redirects, the client waits
        // at least RETRY_DELAY_MS (>=100 ms) before each retry and follows at
        // most MAX_RETRIES redirects before returning a no-leader error.
        //
        // **Validates: Requirements 11.3, 11.4**
        #[test]
        fn redirection_retries_have_a_minimum_delay_and_bounded_count(
            // Cover both regimes and the boundary: runs that settle within the
            // retry budget (k <= MAX_RETRIES) and runs that never settle
            // (k > MAX_RETRIES), forcing give-up.
            k in 0u32..=(MAX_RETRIES + 3),
        ) {
            let (result, attempts, elapsed) = run_redirect_dispatch(k);

            // The client follows at most MAX_RETRIES redirects; once the budget
            // is exhausted it stops, so it never makes more than 1 + MAX_RETRIES
            // attempts regardless of how long redirects persist (Requirement 11.4).
            let retries_followed = k.min(MAX_RETRIES);
            prop_assert!(attempts <= 1 + MAX_RETRIES);
            prop_assert_eq!(attempts, retries_followed + 1);

            if k <= MAX_RETRIES {
                // A run that settles within budget eventually succeeds, after
                // exactly `k` redirects (k + 1 attempts).
                prop_assert_eq!(result.as_ref().ok().copied(), Some(7u64));
            } else {
                // A run that never settles gives up after exactly MAX_RETRIES
                // redirects and returns the no-leader error (Requirement 11.4).
                let gave_up = matches!(
                    result,
                    Err(ClientError::NoLeaderAfterRetries { ref topic, partition: 0 })
                        if topic == "orders"
                );
                prop_assert!(gave_up, "expected NoLeaderAfterRetries for orders/0");
            }

            // Minimum delay: each of the `retries_followed` retries waits at
            // least RETRY_DELAY_MS, so the total virtual time elapsed is at least
            // that many 100 ms floors (Requirement 11.3). On the paused clock the
            // operation itself consumes no time, so this is the delay alone.
            let min_total_delay = Duration::from_millis(RETRY_DELAY_MS) * retries_followed;
            prop_assert!(
                elapsed >= min_total_delay,
                "elapsed {elapsed:?} should be >= {min_total_delay:?} for {retries_followed} retries",
            );
            // The per-retry floor itself satisfies the >=100 ms requirement.
            prop_assert!(RETRY_DELAY_MS >= 100);
        }
    }
}
