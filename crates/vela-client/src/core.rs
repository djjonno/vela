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
//! against that believed leader and classifies the outcome ([`classify`]): it
//! re-resolves the leader on a `NotLeader` redirect or a transport failure,
//! refreshes stale topic metadata on a stale-routing error, and surfaces
//! non-retryable application errors immediately (Requirement 3.2, 3.3, 3.6,
//! 1.6). Retries are bounded by a time-based [`RetryBudget`] with exponential
//! backoff (Requirement 3.4); once the budget is exhausted dispatch returns
//! [`ClientError::NoLeaderAfterRetries`] (Requirement 3.5). All backoff waits
//! and metadata-TTL comparisons are measured against an injected [`Clock`], so
//! the timing is deterministic under a paused tokio runtime in tests.
//!
//! [`Producer`]: crate::Producer
//! [`Consumer`]: crate::Consumer
//! [`AdminClient`]: crate::AdminClient

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tonic::transport::Channel;
use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_proto::v1::{self, FindLeaderRequest};

use crate::connection::{normalize_addr, ConnectionManager, NodeRegistry};
use crate::error::{ClientError, Result};
use crate::leader_cache::LeaderCache;
use crate::metadata_cache::{MetadataCache, TopicMeta};
use crate::retry::RetryBudget;
use crate::router::{KeylessStrategy, PartitionRouter};

/// A clock seam over `tokio::time`, injected into [`ClientCore`] so the
/// dispatch/retry engine's backoff waits and the metadata cache's TTL are
/// measured against a clock the tests control.
///
/// The production [`TokioClock`] reads `tokio::time::Instant::now()` and sleeps
/// with `tokio::time::sleep`, so a test running on a paused tokio runtime
/// (`#[tokio::test(start_paused = true)]`) drives backoff deterministically with
/// no real wall-clock delay.
pub trait Clock: std::fmt::Debug + Send + Sync {
    /// The current instant on this clock.
    fn now(&self) -> tokio::time::Instant;
    /// A future that completes once `duration` has elapsed on this clock.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// The production [`Clock`], backed by real `tokio::time`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// One node's answer to a `FindLeader` probe during multi-node leader
/// resolution.
///
/// Leader resolution asks each configured node in turn (see
/// [`ClientCore::refresh_leader`]); this classifies what each answered so the
/// decision of which outcome to surface is a pure fold ([`resolve_leader`]),
/// testable without a live server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderProbe {
    /// The node named the partition's live leader (a node id). A replica — even
    /// a follower — answers with the leader it knows (Requirement 8.2).
    Leader(String),
    /// The node was reachable and knows the partition but reports no current
    /// leader: it does not host the partition's replica (so it has no live
    /// leader to report, only the cluster does), or an election is in progress.
    NoLeader,
    /// The probe did not yield a usable answer — the node was unreachable or
    /// rejected the request (e.g. it has not yet applied the topic's creation).
    Failed,
}

/// The decision reached by folding the per-node [`LeaderProbe`]s gathered across
/// the configured nodes, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderResolution {
    /// A node named the live leader; resolve and dial this node id.
    Found(String),
    /// At least one node was reachable and knew the partition, but none named a
    /// current leader — the partition has no elected leader right now.
    NoLeaderElected,
    /// No node yielded a usable answer; surface the underlying failure.
    AllFailed,
}

/// Decide leader resolution from the `probes` gathered across the configured
/// nodes, in order.
///
/// The first node to name a leader wins (Requirement 8.2 — any replica that
/// knows the live leader resolves it, regardless of which node leads the
/// partition or which endpoint was listed first). If none names a leader but at
/// least one node was reachable and knew the partition, the partition simply has
/// no elected leader. If no node yielded a usable answer, the caller surfaces the
/// underlying transport/RPC failure.
pub fn resolve_leader<'a>(probes: impl IntoIterator<Item = &'a LeaderProbe>) -> LeaderResolution {
    let mut any_reachable = false;
    for probe in probes {
        match probe {
            LeaderProbe::Leader(node) => return LeaderResolution::Found(node.clone()),
            LeaderProbe::NoLeader => any_reachable = true,
            LeaderProbe::Failed => {}
        }
    }
    if any_reachable {
        LeaderResolution::NoLeaderElected
    } else {
        LeaderResolution::AllFailed
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

/// The richer per-attempt classification the generalized dispatch/retry engine
/// branches on (design §5, "Unified dispatch/retry engine").
///
/// Where [`not_leader_hint`] only recognizes a `NotLeader` redirect, this sorts
/// *every* attempt outcome into one of five buckets so the retry loop can react:
/// surface a success, re-resolve the leader (`NotLeader`/`Transport`), refresh
/// stale topic metadata (`StaleRouting`), or surface a non-retryable
/// application/config error unchanged (`Fatal`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AttemptOutcome {
    /// The attempt succeeded; surface the `Ok` result to the caller. Produced by
    /// the dispatch loop on an `Ok`; [`classify`] (which sorts *errors*) never
    /// returns it.
    #[allow(dead_code)]
    Success,
    /// A `NotLeader` redirect carrying an optional leader node-id `hint`: update
    /// the believed leader from the hint (or re-resolve via `FindLeader`) and
    /// retry (Requirement 3.2).
    NotLeader {
        /// The leader node-id the server suggested, when known.
        hint: Option<String>,
    },
    /// A transport/connection failure (a `tonic::Status` with
    /// `code() == Unavailable`, including a bare connection error): invalidate
    /// the partition's believed leader and re-resolve via `FindLeader` before
    /// retrying (Requirement 3.3, 10.2).
    Transport,
    /// A routing failure attributable to stale topic metadata — a partition
    /// reported unavailable, or no leader elected for a cached topic: invalidate
    /// the topic's metadata so the next attempt performs a refresh, then retry
    /// (Requirement 1.6).
    StaleRouting,
    /// A non-retryable application or configuration error (validation,
    /// topic/partition-not-found, payload-too-large, unknown node, no bootstrap
    /// nodes, ...): surface immediately without retrying (Requirement 3.6).
    Fatal,
}

/// Sort a client error into the [`AttemptOutcome`] the dispatch/retry engine
/// reacts to (design §5 mapping table).
///
/// Pure — a function of the error alone — so the retry policy is unit- and
/// property-testable without a live server. The dispatch loop maps an `Ok`
/// result to [`AttemptOutcome::Success`] itself; `classify` only sorts errors,
/// so it never returns `Success`.
pub(crate) fn classify(error: &ClientError) -> AttemptOutcome {
    // A `NotLeader` redirect (a typed `VelaError` in the status details) is the
    // existing fast path; reuse the same extractor the redirect retry uses so
    // the hint is decoded identically (Requirement 3.2).
    if let Some(hint) = not_leader_hint(error) {
        return AttemptOutcome::NotLeader { hint };
    }

    match error {
        // The cluster reports no elected leader for a partition we routed to:
        // treat as stale routing so the topic's metadata is refreshed before the
        // next attempt (Requirement 1.6).
        ClientError::NoLeader { .. } => AttemptOutcome::StaleRouting,

        // Transport- or application-level failures arrive as a gRPC status.
        ClientError::Rpc(status) => classify_rpc(status),

        // Configuration / terminal client errors are non-retryable: retrying
        // cannot make them succeed, so surface them immediately (Requirement 3.6).
        ClientError::NoNodes
        | ClientError::InvalidAddress { .. }
        | ClientError::UnknownNode { .. }
        | ClientError::NoLeaderAfterRetries { .. }
        | ClientError::MalformedResponse(_)
        | ClientError::InvalidBackend { .. }
        | ClientError::TopicNotFound { .. }
        | ClientError::NoPartitions { .. } => AttemptOutcome::Fatal,
    }
}

/// Classify a gRPC status into an [`AttemptOutcome`].
///
/// `NotLeader` is handled by [`not_leader_hint`] before this is reached, so this
/// covers the remaining cases. A typed [`v1::VelaError`] classification (when the
/// server attached one) takes precedence over the transport code: a
/// `PARTITION_UNAVAILABLE` is stale routing even though it travels on an
/// `Unavailable` status, and the non-retryable application codes are `Fatal`. A
/// status without a recognized typed error falls back to its transport code: a
/// bare `Unavailable` (a connection failure) is retryable `Transport`; anything
/// else is surfaced as `Fatal`.
fn classify_rpc(status: &tonic::Status) -> AttemptOutcome {
    if let Some(code) = decode_error_code(status) {
        if code == v1::ErrorCode::PartitionUnavailable as i32 {
            return AttemptOutcome::StaleRouting;
        }
        if code == v1::ErrorCode::Validation as i32
            || code == v1::ErrorCode::TopicNotFound as i32
            || code == v1::ErrorCode::PartitionNotFound as i32
            || code == v1::ErrorCode::PayloadTooLarge as i32
        {
            return AttemptOutcome::Fatal;
        }
    }

    if status.code() == tonic::Code::Unavailable {
        AttemptOutcome::Transport
    } else {
        AttemptOutcome::Fatal
    }
}

/// Decode the typed [`v1::VelaError`] classification code from a status's
/// details, if one is present and well-formed.
fn decode_error_code(status: &tonic::Status) -> Option<i32> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    Some(v1::VelaError::decode(details).ok()?.code)
}

/// How [`ClientCore::dispatch_admin`] should route a topic-admin request and
/// react to its failures (design §6, "AdminClient — routed through the engine").
///
/// Admin requests are **not** partition-scoped, so they target a configured
/// (bootstrap) node rather than a partition's believed leader. The two variants
/// capture the two admin retry policies the requirements call for:
///
/// - [`Mutating`](Self::Mutating) — `create`/`delete` (Requirement 4.1–4.4):
///   redirect to the hinted `Metadata_Leader` on a `NotLeader` response, and
///   re-resolve (try another configured node) on a transport failure, both
///   under the shared [`RetryBudget`]. When the budget is exhausted without
///   reaching the leader, dispatch returns [`ClientError::NoLeaderAfterRetries`]
///   (Requirement 4.4) tagged with the carried `topic`.
/// - [`ReadOnly`](Self::ReadOnly) — `list`/`describe` (Requirement 4.5, 4.6):
///   any configured node can serve or forward the request, so it is retried
///   **only** on a transport failure (re-resolving to another configured node)
///   and is **never** redirected on a `NotLeader` response. When the budget is
///   exhausted the last underlying error is surfaced unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdminRouting {
    /// A topic-mutating admin command, tagged with the topic it targets so an
    /// exhausted retry budget can report which topic failed.
    Mutating {
        /// The topic the `create`/`delete` targets.
        topic: String,
    },
    /// A read-only admin command (`list`/`describe`): transport-only retry, no
    /// `NotLeader` redirection.
    ReadOnly,
}

/// Tunable configuration for a [`ClientCore`] (and the [`VelaClient`] wrapping
/// it).
///
/// Captures the two operator-facing client settings the CLI flags drive:
///
/// - `metadata_ttl`: the maximum age a cached topic-metadata entry may reach
///   before the next routing operation refreshes it, governing the
///   [`MetadataCache`] (`--metadata-ttl`, Requirement 1.7).
/// - `keyless`: the [`KeylessStrategy`] the [`PartitionRouter`] applies to
///   no-key / empty-key records (`--keyless`, Requirement 5.2, 5.6).
///
/// [`ClientConfig::default`] reproduces the historical behavior exactly — a
/// 30-second metadata TTL ([`MetadataCache::DEFAULT_TTL`]) and per-record
/// round-robin keyless routing — so [`ClientCore::new`]/[`ClientCore::with_clock`]
/// (which use the default) are unchanged.
///
/// [`VelaClient`]: crate::VelaClient
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// TTL governing topic-metadata cache freshness (Requirement 1.7).
    pub metadata_ttl: Duration,
    /// Keyless routing strategy for no-key / empty-key records
    /// (Requirement 5.2, 5.6).
    pub keyless: KeylessStrategy,
}

impl Default for ClientConfig {
    /// The historical defaults: a 30-second metadata TTL and per-record
    /// round-robin keyless routing.
    fn default() -> Self {
        Self {
            metadata_ttl: MetadataCache::DEFAULT_TTL,
            keyless: KeylessStrategy::default(),
        }
    }
}

/// Choose the address to seed for a discovered cluster member, by precedence.
///
/// Prefers the member's client-reachable `advertised_addr` (Req 6.1); falls
/// back to the bind `addr` when no advertised address is set — which is also
/// the path for an older server that does not populate the field at all
/// (Req 6.2, 7.1); and returns `None` when both are empty so the member is
/// skipped (Req 6.4). Pure — a function of the member alone — so the precedence
/// is directly unit- and property-testable.
///
/// `connection::normalize_addr` prepends `http://` to a schemeless `host:port`
/// when the address is later dialed, so no scheme handling is needed here.
pub(crate) fn seed_address(member: &v1::Member) -> Option<&str> {
    if !member.advertised_addr.is_empty() {
        Some(&member.advertised_addr)
    } else if !member.addr.is_empty() {
        Some(&member.addr)
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
    /// TTL-governed cache of each topic's partition count and per-partition
    /// leaders, populated via `DescribeTopic` (Requirement 1.1–1.7).
    metadata: MetadataCache,
    /// Time-bounded exponential-backoff policy governing a single request's
    /// redirect / transport retries (Requirement 3.4, 3.5).
    retry: RetryBudget,
    /// Clock seam measuring backoff waits and metadata freshness — the real
    /// [`TokioClock`] in production, a paused clock under test.
    clock: Arc<dyn Clock>,
    /// Bootstrap node addresses, used for admin calls and the initial
    /// `FindLeader` before any leader is cached.
    bootstrap: Vec<String>,
    /// Guards the lazy, one-time `DescribeCluster` cluster-discovery step so the
    /// registry is seeded from the server's `Member_Address_Map` at most once,
    /// on the first leader resolution (Requirement 13.1, 13.2). The seeding is
    /// best-effort: a failed or empty `DescribeCluster` leaves the `id=url`
    /// fallback registry in place (Requirement 13.5).
    discovery: tokio::sync::OnceCell<()>,
}

impl ClientCore {
    /// Build a core from `(node_id, addr)` bootstrap pairs, using the real
    /// [`TokioClock`].
    ///
    /// The addresses double as the registry seed (so `FindLeader` results can be
    /// resolved to addresses) and as the bootstrap set for admin / initial
    /// `FindLeader` calls.
    pub fn new(nodes: impl IntoIterator<Item = (String, String)>) -> Self {
        Self::with_clock(nodes, Arc::new(TokioClock))
    }

    /// Build a core from `(node_id, addr)` bootstrap pairs with an explicit
    /// [`ClientConfig`], using the real [`TokioClock`].
    ///
    /// The metadata cache is built with `config.metadata_ttl` (Requirement 1.7)
    /// and the partition router with `config.keyless` (Requirement 5.2, 5.6).
    /// Passing [`ClientConfig::default`] is identical to [`ClientCore::new`].
    pub fn with_config(
        nodes: impl IntoIterator<Item = (String, String)>,
        config: ClientConfig,
    ) -> Self {
        Self::with_config_and_clock(nodes, config, Arc::new(TokioClock))
    }

    /// Build a core from `(node_id, addr)` bootstrap pairs with an injected
    /// [`Clock`].
    ///
    /// Identical to [`ClientCore::new`] but lets tests supply a clock they
    /// control so the dispatch/retry backoff and metadata TTL are deterministic.
    pub fn with_clock(
        nodes: impl IntoIterator<Item = (String, String)>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::with_config_and_clock(nodes, ClientConfig::default(), clock)
    }

    /// Build a core from `(node_id, addr)` bootstrap pairs with both an explicit
    /// [`ClientConfig`] and an injected [`Clock`].
    ///
    /// The most general constructor: the metadata cache is built with
    /// `config.metadata_ttl` via [`MetadataCache::new`] (Requirement 1.7) and the
    /// router with `config.keyless` via [`PartitionRouter::with_strategy`]
    /// (Requirement 5.2, 5.6), while the injected `clock` keeps backoff and
    /// metadata-freshness timing deterministic under test. The addresses double
    /// as the registry seed (so `FindLeader` results can be resolved to
    /// addresses) and as the bootstrap set for admin / initial `FindLeader`
    /// calls.
    pub fn with_config_and_clock(
        nodes: impl IntoIterator<Item = (String, String)>,
        config: ClientConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let pairs: Vec<(String, String)> = nodes.into_iter().collect();
        let bootstrap = pairs.iter().map(|(_, addr)| addr.clone()).collect();
        Self {
            connections: ConnectionManager::new(),
            registry: NodeRegistry::from_pairs(pairs),
            leaders: LeaderCache::new(),
            router: PartitionRouter::with_strategy(config.keyless),
            metadata: MetadataCache::new(config.metadata_ttl),
            retry: RetryBudget::default(),
            clock,
            bootstrap,
            discovery: tokio::sync::OnceCell::new(),
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

    /// The TTL-governed topic-metadata cache.
    pub fn metadata(&self) -> &MetadataCache {
        &self.metadata
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
    /// Asks **every node the client knows of in turn** — the configured
    /// bootstrap endpoints first, then any further members learned via
    /// `DescribeCluster` discovery — until one names the partition's live
    /// leader, then resolves that node id to an address, caches it, and returns
    /// it (Requirement 8.2, 11.1). Trying every known node — rather than only
    /// the bootstrap endpoints — is what lets resolution succeed no matter which
    /// node leads the partition or which endpoint was listed first: a node that
    /// does not host the partition's replica answers "no leader" (Req 8.2), and
    /// the lookup falls through to the next until a replica (leader or follower)
    /// reports the live leader. In particular it lets a client seeded with a
    /// single endpoint resolve a partition whose replicas all live on *other*,
    /// discovered nodes.
    ///
    /// When no node names a leader, the outcome distinguishes a partition with
    /// no elected leader yet ([`ClientError::NoLeader`]) from a cluster none of
    /// whose nodes could be reached or which rejected the probe (the last
    /// transport/RPC error is surfaced, so an all-unreachable cluster is
    /// reported as a connection failure). Exposed so the retry layer can
    /// re-resolve after invalidating a stale entry.
    pub async fn refresh_leader(&self, topic: &str, partition: u32) -> Result<String> {
        // Lazily seed the registry from the cluster's Member_Address_Map on the
        // first leader resolution, so a leader node id `FindLeader` returns can
        // be dialed without an `id=url` Endpoint (Requirement 13.1, 13.2).
        self.ensure_cluster_discovered().await;

        // Probe every node the client knows of — the operator-supplied bootstrap
        // endpoints first (authoritative), then any further members learned via
        // `DescribeCluster` discovery — so resolution succeeds even when the
        // single bootstrap node does not host this partition's replica and so
        // cannot name its leader. Without the discovered members, a client
        // seeded with one endpoint could never resolve a partition that endpoint
        // does not replicate (Requirement 8.2, 13.1, 13.2).
        let probe_addrs = self.leader_probe_addrs();

        let mut probes: Vec<LeaderProbe> = Vec::with_capacity(probe_addrs.len());
        // Retained so an all-failed resolution surfaces the real error (e.g. a
        // transport `Unavailable`, which the CLI classifies as a connection
        // failure) rather than a synthesized one.
        let mut last_error: Option<ClientError> = None;

        for addr in &probe_addrs {
            let mut client = match self.client_for(addr) {
                Ok(client) => client,
                Err(err) => {
                    last_error = Some(err);
                    probes.push(LeaderProbe::Failed);
                    continue;
                }
            };
            match client
                .find_leader(FindLeaderRequest {
                    topic: topic.to_string(),
                    partition,
                })
                .await
            {
                Ok(response) => match response.into_inner().leader {
                    Some(node) => probes.push(LeaderProbe::Leader(node)),
                    None => probes.push(LeaderProbe::NoLeader),
                },
                Err(status) => {
                    last_error = Some(ClientError::from(status));
                    probes.push(LeaderProbe::Failed);
                }
            }
        }

        match resolve_leader(&probes) {
            LeaderResolution::Found(node) => {
                let addr =
                    self.registry
                        .addr_of(&node)
                        .ok_or_else(|| ClientError::UnknownNode {
                            node,
                            topic: topic.to_string(),
                            partition,
                        })?;
                self.leaders.insert(topic, partition, addr.clone());
                Ok(addr)
            }
            // At least one node was reachable and knew the partition, but none
            // named a leader: the partition has no elected leader right now.
            LeaderResolution::NoLeaderElected => Err(ClientError::NoLeader {
                topic: topic.to_string(),
                partition,
            }),
            // No node yielded a usable answer: surface the underlying failure
            // (or "no bootstrap nodes" if none were configured).
            LeaderResolution::AllFailed => Err(last_error.unwrap_or(ClientError::NoNodes)),
        }
    }

    /// The ordered, de-duplicated set of node addresses to probe for a
    /// partition leader.
    ///
    /// The configured bootstrap endpoints come first — they are authoritative
    /// and known-reachable from the client — followed by any further member
    /// addresses learned via `DescribeCluster` discovery. De-duplication is by
    /// each address's dialable (normalized) form, so a node listed under two
    /// spellings (a configured `http://host:port` Endpoint and the same node's
    /// discovered bare `host:port`) is probed once, preferring the configured
    /// spelling. Probing the discovered members — not just the bootstrap list —
    /// is what lets a client seeded with a single endpoint resolve a partition
    /// whose replicas all live on *other* nodes (Requirement 8.2).
    fn leader_probe_addrs(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut ordered = Vec::new();
        for addr in self.bootstrap.iter().cloned().chain(self.registry.addrs()) {
            if seen.insert(normalize_addr(&addr)) {
                ordered.push(addr);
            }
        }
        ordered
    }

    /// Lazily seed the node registry from the cluster's `Member_Address_Map`,
    /// running the `DescribeCluster` discovery step **at most once** for the
    /// lifetime of this core (Requirement 13.1, 13.2, 13.5).
    ///
    /// The [`tokio::sync::OnceCell`] guard ensures the call is attempted only
    /// once even when several partitions resolve their leaders concurrently:
    /// the first caller runs the discovery and any others await its result. The
    /// step is best-effort, so the guard is considered satisfied whether the
    /// `DescribeCluster` succeeds, fails, or returns no members — a later leader
    /// resolution never re-attempts it.
    async fn ensure_cluster_discovered(&self) {
        self.discovery
            .get_or_init(|| self.seed_registry_from_cluster())
            .await;
    }

    /// Call `DescribeCluster` against a bootstrap node and seed the
    /// [`NodeRegistry`] with members the operator did **not** already configure,
    /// so a leader id learned from `FindLeader` resolves to an address even when
    /// no `id=url` Endpoint was supplied for it (Requirement 13.1, 13.2).
    ///
    /// Discovery only *fills gaps*: an `id=url` Endpoint the operator supplied is
    /// authoritative and is never overridden ([`NodeRegistry::insert_if_absent`]).
    /// Only the operator knows the address through which their client can reach
    /// each node (e.g. a published port behind NAT / Docker port mapping),
    /// whereas a member's self-reported address is the cluster's *internal* view
    /// — a bind address such as `0.0.0.0:7001`, or a peer hostname resolvable
    /// only inside the cluster network — which may be undialable from the client.
    /// Members with an empty address are skipped. On any failure — no bootstrap
    /// node, a `DescribeCluster` error (e.g. an older server without the RPC), or
    /// an empty member set — the existing registry is left untouched, so the
    /// client proceeds with just the `id=url` registry (Requirement 13.5). An
    /// unmapped resolved leader id still surfaces as
    /// [`ClientError::UnknownNode`] from [`refresh_leader`](Self::refresh_leader)
    /// (Requirement 13.4, 13.6).
    async fn seed_registry_from_cluster(&self) {
        let Ok(mut client) = self.bootstrap_client() else {
            return;
        };
        let members = match client
            .describe_cluster(vela_proto::v1::DescribeClusterRequest {})
            .await
        {
            Ok(response) => response.into_inner().members,
            Err(_) => return,
        };
        for member in members {
            // Fill the gap only — never clobber an operator-supplied `id=url`
            // address, which is the one known to be reachable from here.
            if let Some(addr) = seed_address(&member) {
                let addr = addr.to_string();
                self.registry.insert_if_absent(member.id, addr);
            }
        }
    }

    /// Run `operation` against the partition's believed leader, retrying within a
    /// time-bounded [`RetryBudget`] (Requirement 3.1–3.6, 1.6).
    ///
    /// The flow:
    /// 1. resolve the believed leader address (cache hit, or `FindLeader`);
    /// 2. call `operation(addr)` once;
    /// 3. on `Ok`, return it; on `Err`, [`classify`] the failure:
    ///    - `Fatal` (validation, not-found, payload-too-large, …): surface
    ///      immediately without retrying (Requirement 3.6);
    ///    - `NotLeader { hint }`: re-resolve from the hint (or via `FindLeader`)
    ///      (Requirement 3.2);
    ///    - `Transport`: invalidate the cached leader and re-resolve via
    ///      `FindLeader` (Requirement 3.3);
    ///    - `StaleRouting`: invalidate the topic's metadata (so the next
    ///      `partition_count` refreshes) and re-resolve the leader
    ///      (Requirement 1.6);
    /// 4. before each retry, stop if the [`RetryBudget`]'s elapsed-time budget is
    ///    exhausted — returning [`ClientError::NoLeaderAfterRetries`]
    ///    (Requirement 3.5) — otherwise wait the budget's exponential backoff
    ///    (Requirement 3.4) and retry.
    ///
    /// `operation` is an `async` closure of the leader address; it is invoked
    /// once per attempt, so it must be cheaply re-runnable (clone its inputs).
    /// This is the seam [`Producer`](crate::Producer) and
    /// [`Consumer`](crate::Consumer) dispatch through so a request that lands on
    /// a non-leader — or a stale/unreachable leader — is transparently retried
    /// against the re-resolved leader.
    pub async fn dispatch<T, F, Fut>(&self, topic: &str, partition: u32, operation: F) -> Result<T>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut addr = self.leader_addr(topic, partition).await?;
        let start = self.clock.now();
        let mut attempt = 0u32;

        loop {
            let error = match operation(addr.clone()).await {
                // `AttemptOutcome::Success`: surface the result unchanged.
                Ok(value) => return Ok(value),
                Err(error) => error,
            };

            // Sort the failure into a reaction; a `Fatal` error is non-retryable
            // and surfaces immediately (Requirement 3.6).
            let reaction = match classify(&error) {
                AttemptOutcome::Fatal => return Err(error),
                // `classify` sorts only errors, so `Success` cannot occur here
                // (an `Ok` already returned above).
                AttemptOutcome::Success => unreachable!("classify never yields Success"),
                retryable => retryable,
            };

            // Stop once the request's time budget is exhausted (Requirement 3.5).
            let elapsed = self.clock.now().saturating_duration_since(start);
            if !self.retry.may_retry(elapsed) {
                return Err(ClientError::NoLeaderAfterRetries {
                    topic: topic.to_string(),
                    partition,
                });
            }

            // Wait the exponential backoff before re-resolving (Requirement 3.4).
            self.clock.sleep(self.retry.backoff(attempt)).await;
            attempt += 1;

            // React to the specific failure to set up the next attempt's target.
            addr = match reaction {
                // Update from the hint (registry), else invalidate + `FindLeader`.
                AttemptOutcome::NotLeader { hint } => {
                    self.resolve_redirect(topic, partition, hint).await?
                }
                // The believed leader is unreachable: drop it and re-resolve
                // (Requirement 3.3, 10.2).
                AttemptOutcome::Transport => {
                    self.leaders.invalidate(topic, partition);
                    self.refresh_leader(topic, partition).await?
                }
                // Stale topic metadata: invalidate it so the next
                // `partition_count` refreshes, drop the believed leader, and
                // re-resolve (Requirement 1.6).
                AttemptOutcome::StaleRouting => {
                    self.metadata.invalidate(topic);
                    self.leaders.invalidate(topic, partition);
                    self.refresh_leader(topic, partition).await?
                }
                AttemptOutcome::Success | AttemptOutcome::Fatal => {
                    unreachable!("Success/Fatal handled above")
                }
            };
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

    /// Run a topic-admin `operation` against a configured node, retrying within
    /// the shared time-bounded [`RetryBudget`] (Requirement 4.1–4.6, 12.3–12.5).
    ///
    /// This is the admin sibling of [`dispatch`](Self::dispatch). Admin requests
    /// are not partition-scoped, so the target is a configured (bootstrap) node
    /// rather than a partition's believed leader; the first attempt goes to the
    /// first configured node. The reaction to a failure depends on `routing`:
    ///
    /// - [`AdminRouting::Mutating`] (`create`/`delete`):
    ///   - `NotLeader { hint }`: redirect to the hinted `Metadata_Leader` —
    ///     resolved via the [`NodeRegistry`] when the hint is present and known,
    ///     otherwise fall back to another configured node — and retry
    ///     (Requirement 4.1, 12.3, 12.4, 12.5);
    ///   - `Transport`: re-resolve to another configured node and retry
    ///     (Requirement 4.2);
    ///   - `StaleRouting`: likewise re-resolve to another configured node (this
    ///     does not normally arise for admin requests, but is treated as a
    ///     transient, retryable failure);
    ///   - on an exhausted budget, return [`ClientError::NoLeaderAfterRetries`]
    ///     tagged with the routed `topic` (Requirement 4.4).
    /// - [`AdminRouting::ReadOnly`] (`list`/`describe`):
    ///   - `Transport`: re-resolve to another configured node and retry
    ///     (Requirement 4.6);
    ///   - any other failure (including `NotLeader`) is surfaced immediately —
    ///     a read-only request is never redirected (Requirement 4.5);
    ///   - on an exhausted budget, the last underlying error is surfaced.
    ///
    /// A `Fatal` classification always surfaces immediately (Requirement 3.6).
    /// The redirect/​re-resolve steps are registry lookups only, so following a
    /// hint costs no extra network round-trip. Every retry waits the budget's
    /// exponential backoff against the injected [`Clock`], so the timing is
    /// deterministic under a paused tokio runtime in tests.
    ///
    /// `operation` is invoked once per attempt with the target node address, so
    /// it must be cheaply re-runnable (clone its inputs), exactly as for
    /// [`dispatch`](Self::dispatch).
    pub(crate) async fn dispatch_admin<T, F, Fut>(
        &self,
        routing: AdminRouting,
        operation: F,
    ) -> Result<T>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        // The index into `bootstrap` of the node the current attempt targets;
        // re-resolution rotates to the next configured node.
        let mut index = 0usize;
        let mut addr = self.bootstrap.first().ok_or(ClientError::NoNodes)?.clone();
        let start = self.clock.now();
        let mut attempt = 0u32;

        /// The target the next attempt should use, decided from the failure.
        enum NextTarget {
            /// Redirect to the hinted metadata-leader node id (or fall back).
            Redirect(Option<String>),
            /// Re-resolve to another configured node.
            ReResolve,
        }

        loop {
            let error = match operation(addr.clone()).await {
                // `AttemptOutcome::Success`: surface the result unchanged.
                Ok(value) => return Ok(value),
                Err(error) => error,
            };

            // Decide how to react, or surface the error immediately. A read-only
            // request retries *only* on transport failure and is never
            // redirected on `NotLeader` (Requirement 4.5, 4.6); a mutating
            // request redirects on `NotLeader` and re-resolves on transport /
            // stale-routing failures (Requirement 4.1, 4.2).
            let next = match (&routing, classify(&error)) {
                // Non-retryable application/config error: surface as-is (Req 3.6).
                (_, AttemptOutcome::Fatal) => return Err(error),
                // `classify` sorts only errors, so `Success` cannot occur here.
                (_, AttemptOutcome::Success) => {
                    unreachable!("classify never yields Success")
                }
                // Read-only: a transport failure re-resolves; anything else
                // (a `NotLeader` redirect or stale routing) is surfaced.
                (AdminRouting::ReadOnly, AttemptOutcome::Transport) => NextTarget::ReResolve,
                (AdminRouting::ReadOnly, _) => return Err(error),
                // Mutating: follow a `NotLeader` redirect to the metadata leader.
                (AdminRouting::Mutating { .. }, AttemptOutcome::NotLeader { hint }) => {
                    NextTarget::Redirect(hint)
                }
                // Mutating: a transport or stale-routing failure re-resolves.
                (
                    AdminRouting::Mutating { .. },
                    AttemptOutcome::Transport | AttemptOutcome::StaleRouting,
                ) => NextTarget::ReResolve,
            };

            // Stop once the request's time budget is exhausted (Requirement 4.4).
            let elapsed = self.clock.now().saturating_duration_since(start);
            if !self.retry.may_retry(elapsed) {
                return match &routing {
                    AdminRouting::Mutating { topic } => Err(ClientError::NoLeaderAfterRetries {
                        topic: topic.clone(),
                        partition: 0,
                    }),
                    // A read-only request surfaces the last transport error.
                    AdminRouting::ReadOnly => Err(error),
                };
            }

            // Wait the exponential backoff before re-targeting (Requirement 4.3).
            self.clock.sleep(self.retry.backoff(attempt)).await;
            attempt += 1;

            addr = match next {
                NextTarget::Redirect(hint) => self.resolve_admin_target(hint, &mut index)?,
                NextTarget::ReResolve => self.next_bootstrap(&mut index)?,
            };
        }
    }

    /// Resolve the node address to retry an admin request against after a
    /// `NotLeader` redirect.
    ///
    /// If the redirect carried a metadata-leader node-id `hint` the registry can
    /// resolve, that address is used (no network call). Otherwise — no hint, or
    /// an unresolvable one — the request falls back to another configured node
    /// (Requirement 4.1, 12.4, 12.5).
    fn resolve_admin_target(&self, hint: Option<String>, index: &mut usize) -> Result<String> {
        if let Some(node) = hint {
            if let Some(addr) = self.registry.addr_of(&node) {
                return Ok(addr);
            }
        }
        self.next_bootstrap(index)
    }

    /// Advance to the next configured (bootstrap) node, wrapping around, and
    /// return its address.
    ///
    /// Used to re-resolve an admin request's target after a transport failure or
    /// an unusable redirect hint. Errors with [`ClientError::NoNodes`] when no
    /// nodes are configured.
    fn next_bootstrap(&self, index: &mut usize) -> Result<String> {
        if self.bootstrap.is_empty() {
            return Err(ClientError::NoNodes);
        }
        *index = (*index + 1) % self.bootstrap.len();
        Ok(self.bootstrap[*index].clone())
    }

    /// Resolve a topic's partition count, reusing fresh cached metadata.
    ///
    /// The producer needs the partition count to route a key. While the topic's
    /// cached metadata is fresh (within the [`MetadataCache`] TTL) the cached
    /// count is reused (Requirement 1.3); when it is absent or stale the count is
    /// re-learned via `DescribeTopic`, which also re-seeds the leader cache
    /// (Requirement 1.5). A topic the server does not know surfaces as
    /// [`ClientError::TopicNotFound`] (Requirement 1.4).
    pub async fn partition_count(&self, topic: &str) -> Result<u32> {
        let now = self.clock.now().into_std();
        if let Some(meta) = self.metadata.get_fresh(topic, now) {
            return Ok(meta.partition_count);
        }
        Ok(self.refresh_metadata(topic).await?.partition_count)
    }

    /// Re-fetch `topic`'s partition-and-leader metadata via `DescribeTopic`,
    /// cache it, and re-seed the [`LeaderCache`] from any leaders it learned.
    ///
    /// This is the `Metadata_Refresh` (Requirement 1.5): it replaces the cached
    /// `Partition_Count` and re-learns each partition's leader, recording the
    /// dialable address of any learned leader the registry can resolve so a
    /// later `dispatch` skips a `FindLeader` round-trip. A `DescribeTopic`
    /// response with no topic payload is treated as topic-not-found
    /// (Requirement 1.4).
    async fn refresh_metadata(&self, topic: &str) -> Result<TopicMeta> {
        let mut client = self.bootstrap_client()?;
        let response = client
            .describe_topic(vela_proto::v1::DescribeTopicRequest {
                name: topic.to_string(),
            })
            .await?
            .into_inner();

        let info = response.topic.ok_or_else(|| ClientError::TopicNotFound {
            topic: topic.to_string(),
        })?;

        let mut leaders = vec![None; info.partition_count as usize];
        for partition in &info.partitions {
            let index = partition.index as usize;
            if index < leaders.len() {
                leaders[index].clone_from(&partition.leader);
            }
            // Re-seed the leader cache from a learned leader we can dial, so the
            // next dispatch goes straight to it.
            if let Some(node) = &partition.leader {
                if let Some(addr) = self.registry.addr_of(node) {
                    self.leaders.insert(topic, partition.index, addr);
                }
            }
        }

        let meta = TopicMeta {
            partition_count: info.partition_count,
            leaders,
            learned_at: self.clock.now().into_std(),
        };
        self.metadata.put(topic, meta.clone());
        Ok(meta)
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
    fn leader_probe_addrs_lists_bootstrap_first_then_discovered_members() {
        // Leader resolution probes the configured bootstrap endpoints first
        // (authoritative, known-reachable), then any further members the client
        // learned via discovery — so a partition whose replicas all live on
        // *other*, discovered nodes can still be resolved from a single
        // bootstrap endpoint (Requirement 8.2).
        let core = ClientCore::new([("node-a".to_string(), "http://node-a:50051".to_string())]);
        // Simulate `DescribeCluster` discovery seeding two more members the
        // operator did not configure (bare `host:port`, as carried in metadata).
        core.registry().insert_if_absent("node-b", "node-b:7001");
        core.registry().insert_if_absent("node-c", "node-c:7001");

        let probes = core.leader_probe_addrs();

        // The bootstrap endpoint is probed first.
        assert_eq!(
            probes.first().map(String::as_str),
            Some("http://node-a:50051")
        );
        // Every discovered member is included so a leader hosted only on one of
        // them is reachable.
        assert!(probes.iter().any(|a| a == "node-b:7001"));
        assert!(probes.iter().any(|a| a == "node-c:7001"));
        assert_eq!(probes.len(), 3);
    }

    #[test]
    fn leader_probe_addrs_dedupes_a_node_listed_under_two_spellings() {
        // The bootstrap endpoint `http://node-a:50051` and the same node's
        // discovered bare `node-a:50051` normalize to one dialable URL, so the
        // node is probed once (preferring the configured spelling) rather than
        // twice.
        let core = ClientCore::new([("node-a".to_string(), "http://node-a:50051".to_string())]);
        // A discovered entry for the SAME node under its schemeless spelling.
        core.registry().insert("node-a-alias", "node-a:50051");
        core.registry().insert_if_absent("node-b", "node-b:7001");

        let probes = core.leader_probe_addrs();

        assert_eq!(
            probes.iter().filter(|a| a.contains("node-a:50051")).count(),
            1,
            "the node reachable under two spellings is probed once: {probes:?}"
        );
        // The configured (scheme-carrying) spelling is the one kept.
        assert!(probes.iter().any(|a| a == "http://node-a:50051"));
        assert!(probes.iter().any(|a| a == "node-b:7001"));
        assert_eq!(probes.len(), 2);
    }

    #[test]
    fn default_config_matches_historical_behavior() {
        // `ClientCore::new` uses the default config, which must reproduce the
        // historical 30 s metadata TTL and round-robin keyless routing.
        let core = core();
        assert_eq!(core.metadata().ttl(), MetadataCache::DEFAULT_TTL);
        assert_eq!(
            core.router().keyless_strategy(),
            KeylessStrategy::RoundRobin
        );
    }

    #[test]
    fn with_config_applies_metadata_ttl_and_keyless_strategy() {
        // A configured TTL is reflected by the metadata cache and the chosen
        // keyless strategy by the router (Requirement 1.7, 5.2, 5.6).
        let config = ClientConfig {
            metadata_ttl: Duration::from_secs(5),
            keyless: KeylessStrategy::Sticky { run_length: 16 },
        };
        let core = ClientCore::with_config(
            [("node-a".to_string(), "http://node-a:50051".to_string())],
            config,
        );
        assert_eq!(core.metadata().ttl(), Duration::from_secs(5));
        assert_eq!(
            core.router().keyless_strategy(),
            KeylessStrategy::Sticky { run_length: 16 }
        );
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

    #[tokio::test(start_paused = true)]
    async fn cluster_discovery_falls_back_to_the_id_url_registry_when_unreachable() {
        // With no server answering `DescribeCluster`, the best-effort discovery
        // step swallows the transport failure and leaves the `id=url` fallback
        // registry in place — degraded but functional (Requirement 13.5).
        let core = core();
        core.ensure_cluster_discovered().await;
        assert_eq!(
            core.registry().addr_of("node-a").as_deref(),
            Some("http://node-a:50051")
        );
        // The guard is satisfied even though the call failed, so a later leader
        // resolution never re-attempts discovery (at-most-once — Req 13.1).
        assert!(core.discovery.initialized());
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

    // --- Multi-node leader resolution fold (resolve_leader) --------------

    #[test]
    fn resolve_leader_returns_the_first_named_leader() {
        // The first node to name a leader wins, regardless of later answers.
        let probes = [
            LeaderProbe::Leader("node-a".to_string()),
            LeaderProbe::Leader("node-b".to_string()),
        ];
        assert_eq!(
            resolve_leader(&probes),
            LeaderResolution::Found("node-a".to_string())
        );
    }

    #[test]
    fn resolve_leader_falls_through_a_no_leader_node() {
        // A node that does not host the partition answers `NoLeader`; resolution
        // falls through to the replica that names the live leader (the bug fix:
        // the first endpoint need not host the partition — Req 8.2).
        let probes = [
            LeaderProbe::NoLeader,
            LeaderProbe::Leader("node-d".to_string()),
        ];
        assert_eq!(
            resolve_leader(&probes),
            LeaderResolution::Found("node-d".to_string())
        );
    }

    #[test]
    fn resolve_leader_falls_through_a_failed_node() {
        // An unreachable / rejecting node is skipped; a later node still resolves.
        let probes = [
            LeaderProbe::Failed,
            LeaderProbe::Leader("node-c".to_string()),
        ];
        assert_eq!(
            resolve_leader(&probes),
            LeaderResolution::Found("node-c".to_string())
        );
    }

    #[test]
    fn resolve_leader_reports_no_leader_when_some_reachable_node_has_none() {
        // Every node that knew the partition reported no current leader, so the
        // partition has no elected leader right now (an election in progress).
        let probes = [LeaderProbe::NoLeader, LeaderProbe::NoLeader];
        assert_eq!(resolve_leader(&probes), LeaderResolution::NoLeaderElected);
    }

    #[test]
    fn resolve_leader_treats_a_reachable_no_leader_as_authoritative_over_failures() {
        // A reachable "no leader" mixed with unreachable nodes still means the
        // partition exists but has no current leader (not an all-failed cluster).
        let probes = [LeaderProbe::Failed, LeaderProbe::NoLeader];
        assert_eq!(resolve_leader(&probes), LeaderResolution::NoLeaderElected);
    }

    #[test]
    fn resolve_leader_reports_all_failed_when_no_node_answers_usefully() {
        // No node was reachable / none knew the partition: the caller surfaces
        // the underlying transport/RPC error.
        let probes = [LeaderProbe::Failed, LeaderProbe::Failed];
        assert_eq!(resolve_leader(&probes), LeaderResolution::AllFailed);
        // An empty probe set (no configured nodes) is likewise all-failed.
        assert_eq!(resolve_leader(&[]), LeaderResolution::AllFailed);
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

    /// The number of retries the default [`RetryBudget`] permits before its
    /// elapsed-time budget is exhausted, mirroring the dispatch loop's schedule:
    /// a retry is allowed while the time elapsed *before* it is within budget,
    /// after which that retry's backoff is added to the running elapsed total.
    fn retries_allowed_by_default_budget() -> u32 {
        let budget = RetryBudget::default();
        let mut elapsed = Duration::ZERO;
        let mut allowed = 0u32;
        while budget.may_retry(elapsed) {
            elapsed += budget.backoff(allowed);
            allowed += 1;
        }
        allowed
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_gives_up_when_the_retry_budget_is_exhausted() {
        let core = core();
        core.leaders().insert("orders", 0, "http://node-a:50051");
        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = Arc::clone(&calls);

        // Every attempt redirects (to a resolvable node), so the client never
        // lands on a usable leader and eventually exhausts its time budget
        // (Requirement 3.5).
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
        // Initial attempt + every retry the budget permitted.
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1 + retries_allowed_by_default_budget()
        );
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

        // Feature: ctl-client-routing-and-repl, Property 11
        //
        // Property 11 (retry budget total-time bound and termination): over any
        // run of `NotLeader` redirects, the client retries only while the
        // elapsed time is within the `RetryBudget`, waiting the budget's
        // exponential backoff before each retry; once the budget is exhausted it
        // stops and returns a no-leader-after-retries error. A run that settles
        // within the budget succeeds instead (Requirement 3.4, 3.5).
        //
        // **Validates: Requirements 3.4, 3.5**
        #[test]
        fn redirection_retries_are_bounded_by_the_time_budget(
            // Cover both regimes and the boundary: runs that settle within the
            // budget and runs that never settle (forcing give-up).
            k in 0u32..=(retries_allowed_by_default_budget() + 4),
        ) {
            let (result, attempts, elapsed) = run_redirect_dispatch(k);
            let budget = RetryBudget::default();
            let allowed = retries_allowed_by_default_budget();
            let retries_followed = k.min(allowed);

            // At least one attempt always runs, and the loop never exceeds the
            // initial attempt plus the retries the budget permits (Req 3.5).
            prop_assert!(attempts >= 1);
            prop_assert!(attempts <= 1 + allowed);
            prop_assert_eq!(attempts, retries_followed + 1);

            // Elapsed virtual time is bounded by the budget plus one capped
            // backoff (the final permitted retry can push just past `total`).
            prop_assert!(elapsed <= budget.total() + budget.cap());

            if k <= allowed {
                // A run that settles within budget succeeds after `k` redirects.
                prop_assert_eq!(result.as_ref().ok().copied(), Some(7u64));
            } else {
                // A run that never settles gives up once the budget is exhausted
                // (Requirement 3.5), having genuinely spent the whole budget.
                let gave_up = matches!(
                    result,
                    Err(ClientError::NoLeaderAfterRetries { ref topic, partition: 0 })
                        if topic == "orders"
                );
                prop_assert!(gave_up, "expected NoLeaderAfterRetries for orders/0");
                prop_assert!(elapsed >= budget.total());
            }
        }
    }

    // --- Attempt outcome classification (`classify`, task 2.4) -----------

    /// A `VelaError`-bearing status for an arbitrary classification `code`,
    /// shaped exactly as the server emits it (the typed error encoded into the
    /// status details). `transport` is the gRPC code the status travels on.
    fn vela_error_status(code: v1::ErrorCode, transport: tonic::Code) -> tonic::Status {
        let vela_error = v1::VelaError {
            code: code as i32,
            message: format!("{code:?}"),
            leader: None,
        };
        let details = prost::bytes::Bytes::from(vela_error.encode_to_vec());
        tonic::Status::with_details(transport, "typed error", details)
    }

    fn rpc(status: tonic::Status) -> ClientError {
        ClientError::Rpc(Box::new(status))
    }

    #[test]
    fn classify_sorts_a_not_leader_redirect_with_its_hint() {
        // NotLeader branch: the hint is decoded identically to `not_leader_hint`.
        assert_eq!(
            classify(&not_leader_error(Some("node-b"))),
            AttemptOutcome::NotLeader {
                hint: Some("node-b".to_string())
            }
        );
        // A hintless redirect is still a `NotLeader` (re-resolve via FindLeader).
        assert_eq!(
            classify(&not_leader_error(None)),
            AttemptOutcome::NotLeader { hint: None }
        );
    }

    #[test]
    fn classify_sorts_a_bare_unavailable_as_transport() {
        // Transport branch: a connection failure surfaces as `Unavailable` with
        // no typed details, so it is retryable transport (Requirement 3.3).
        let status = tonic::Status::new(tonic::Code::Unavailable, "connection refused");
        assert_eq!(classify(&rpc(status)), AttemptOutcome::Transport);
    }

    #[test]
    fn classify_sorts_partition_unavailable_and_no_leader_as_stale_routing() {
        // StaleRouting branch: a `PARTITION_UNAVAILABLE` is stale routing even
        // though it travels on an `Unavailable` status — the typed code wins
        // over the transport code (Requirement 1.6).
        assert_eq!(
            classify(&rpc(vela_error_status(
                v1::ErrorCode::PartitionUnavailable,
                tonic::Code::Unavailable
            ))),
            AttemptOutcome::StaleRouting
        );
        // A client-side "no leader elected" on a routed partition is likewise
        // stale routing: refresh the topic's metadata, then retry.
        assert_eq!(
            classify(&ClientError::NoLeader {
                topic: "orders".to_string(),
                partition: 0,
            }),
            AttemptOutcome::StaleRouting
        );
    }

    #[test]
    fn classify_sorts_non_retryable_application_codes_as_fatal() {
        // Fatal branch: each non-retryable `VelaError` code surfaces without a
        // retry (Requirement 3.6). Cover the exact codes from the mapping table.
        for code in [
            v1::ErrorCode::Validation,
            v1::ErrorCode::TopicNotFound,
            v1::ErrorCode::PartitionNotFound,
            v1::ErrorCode::PayloadTooLarge,
        ] {
            // The transport code is incidental here; the typed code decides.
            assert_eq!(
                classify(&rpc(vela_error_status(
                    code,
                    tonic::Code::FailedPrecondition
                ))),
                AttemptOutcome::Fatal,
                "{code:?} should classify as Fatal",
            );
        }

        // An RPC status with no typed details and a non-`Unavailable` code is
        // also surfaced rather than retried.
        assert_eq!(
            classify(&rpc(tonic::Status::new(tonic::Code::Internal, "boom"))),
            AttemptOutcome::Fatal
        );

        // Configuration / terminal client errors are non-retryable too.
        assert_eq!(classify(&ClientError::NoNodes), AttemptOutcome::Fatal);
        assert_eq!(
            classify(&ClientError::TopicNotFound {
                topic: "orders".to_string()
            }),
            AttemptOutcome::Fatal
        );
        assert_eq!(
            classify(&ClientError::NoPartitions {
                topic: "orders".to_string()
            }),
            AttemptOutcome::Fatal
        );
        assert_eq!(
            classify(&ClientError::UnknownNode {
                node: "node-z".to_string(),
                topic: "orders".to_string(),
                partition: 0,
            }),
            AttemptOutcome::Fatal
        );
    }

    #[test]
    fn classify_typed_fatal_code_wins_over_an_unavailable_transport() {
        // Typed-code-vs-transport-code precedence (design §5): a non-retryable
        // typed `VelaError` decides the outcome even when it rides on an
        // `Unavailable` status. This pins each individual code comparison in the
        // Fatal branch: carrying the code on `Unavailable` means the transport
        // fallback would yield `Transport`, so any single code that fails to
        // match flips the result — distinguishing the intended `Fatal` from the
        // masked outcome the catch-all would otherwise produce (Requirement 3.6,
        // 3.3).
        for code in [
            v1::ErrorCode::Validation,
            v1::ErrorCode::TopicNotFound,
            v1::ErrorCode::PartitionNotFound,
            v1::ErrorCode::PayloadTooLarge,
        ] {
            assert_eq!(
                classify(&rpc(vela_error_status(code, tonic::Code::Unavailable))),
                AttemptOutcome::Fatal,
                "{code:?} on an Unavailable transport must stay Fatal, not fall \
                 through to the transport-code Transport classification",
            );
        }
    }

    #[test]
    fn classify_never_returns_success_and_success_is_the_ok_outcome() {
        // Success branch: it is reserved for an `Ok` result (assigned by the
        // dispatch loop), distinct from every error classification — `classify`
        // itself never yields it.
        let errors = [
            not_leader_error(Some("node-b")),
            rpc(tonic::Status::new(tonic::Code::Unavailable, "down")),
            ClientError::NoLeader {
                topic: "orders".to_string(),
                partition: 0,
            },
            ClientError::NoNodes,
        ];
        for error in &errors {
            assert_ne!(classify(error), AttemptOutcome::Success);
        }
    }
}

#[cfg(test)]
mod registry_seeding_tests {
    //! Client registry-seeding tests (task 7.5).
    //!
    //! These drive [`ClientCore::refresh_leader`] against an in-process fake
    //! `VelaClient` server to prove how the node-id→address registry is seeded
    //! and consulted when resolving a leader returned by `FindLeader`
    //! (Requirement 13.1–13.4, 13.6). The fake server answers two RPCs:
    //!
    //! - `DescribeCluster` with a configured `Member_Address_Map`, the
    //!   **primary** seeding source (Requirement 13.1, 13.2); and
    //! - `FindLeader` with a chosen leader node id, so the resolved id can be
    //!   observed mapping to an address end-to-end.
    //!
    //! Lazy cluster discovery ([`ClientCore::ensure_cluster_discovered`]) runs on
    //! the first `refresh_leader`, seeding the registry from the member map
    //! before the leader id is resolved. The four cases cover the seeding matrix:
    //! a member-mapped id resolves; an `id=url` bootstrap pair fills a gap for an
    //! id absent from the map (Requirement 13.3); an empty member set leaves only
    //! the `id=url` fallback (Requirement 13.3, 13.5); and an id present in
    //! neither source surfaces [`ClientError::UnknownNode`] (Requirement 13.4,
    //! 13.6).
    //!
    //! The client is built with [`ClientCore::with_clock`] and an instant
    //! [`VirtualClock`] (mirroring `admin.rs`'s `routing_tests` and
    //! `tests/prop_dispatch_reresolution.rs`), so any retry backoff costs no real
    //! wall-clock time while the fake gRPC server runs on a multi-thread runtime.

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};
    use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
    use vela_proto::v1::{
        ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse,
        DeleteTopicRequest, DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse,
        DescribeTopicRequest, DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse,
        ListTopicsRequest, ListTopicsResponse, Member, ProduceRequest, ProduceResponse,
    };

    use super::{ClientCore, Clock};
    use crate::error::ClientError;

    /// A clock whose `sleep` advances virtual time instantly, so any dispatch /
    /// retry backoff costs no real wall-clock time. Mirrors the `VirtualClock`
    /// in `admin.rs`'s `routing_tests` and `tests/prop_dispatch_reresolution.rs`:
    /// `now()` advances by exactly the slept duration so the elapsed-time bound
    /// still progresses, while the returned future is ready immediately.
    #[derive(Debug)]
    struct VirtualClock {
        base: tokio::time::Instant,
        elapsed: Mutex<Duration>,
    }

    impl VirtualClock {
        fn new() -> Self {
            Self {
                base: tokio::time::Instant::now(),
                elapsed: Mutex::new(Duration::ZERO),
            }
        }
    }

    impl Clock for VirtualClock {
        fn now(&self) -> tokio::time::Instant {
            self.base + *self.elapsed.lock().expect("virtual clock mutex poisoned")
        }

        fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            *self.elapsed.lock().expect("virtual clock mutex poisoned") += duration;
            Box::pin(std::future::ready(()))
        }
    }

    /// An in-process fake of the client-facing `VelaClient` service that answers
    /// `DescribeCluster` with a configured member set (the `Member_Address_Map`)
    /// and `FindLeader` with a chosen leader node id, so registry seeding and
    /// leader resolution can be observed end-to-end. Every other RPC is unused.
    #[derive(Clone)]
    struct FakeClusterNode {
        /// The members `DescribeCluster` reports — the primary seeding source.
        members: Vec<Member>,
        /// The leader id `FindLeader` names for any partition.
        leader: Option<String>,
    }

    impl FakeClusterNode {
        fn new(members: Vec<Member>, leader: Option<&str>) -> Self {
            Self {
                members,
                leader: leader.map(str::to_string),
            }
        }
    }

    #[tonic::async_trait]
    impl VelaClientService for FakeClusterNode {
        async fn describe_cluster(
            &self,
            _request: Request<DescribeClusterRequest>,
        ) -> Result<Response<DescribeClusterResponse>, Status> {
            Ok(Response::new(DescribeClusterResponse {
                members: self.members.clone(),
                epoch: 0,
            }))
        }

        async fn find_leader(
            &self,
            _request: Request<FindLeaderRequest>,
        ) -> Result<Response<FindLeaderResponse>, Status> {
            Ok(Response::new(FindLeaderResponse {
                leader: self.leader.clone(),
            }))
        }

        // The remaining client RPCs are not exercised by these seeding tests.
        async fn produce(
            &self,
            _request: Request<ProduceRequest>,
        ) -> Result<Response<ProduceResponse>, Status> {
            Err(Status::unimplemented("produce is not exercised here"))
        }

        async fn consume(
            &self,
            _request: Request<ConsumeRequest>,
        ) -> Result<Response<ConsumeResponse>, Status> {
            Err(Status::unimplemented("consume is not exercised here"))
        }

        async fn create_topic(
            &self,
            _request: Request<CreateTopicRequest>,
        ) -> Result<Response<CreateTopicResponse>, Status> {
            Err(Status::unimplemented("create_topic is not exercised here"))
        }

        async fn delete_topic(
            &self,
            _request: Request<DeleteTopicRequest>,
        ) -> Result<Response<DeleteTopicResponse>, Status> {
            Err(Status::unimplemented("delete_topic is not exercised here"))
        }

        async fn list_topics(
            &self,
            _request: Request<ListTopicsRequest>,
        ) -> Result<Response<ListTopicsResponse>, Status> {
            Err(Status::unimplemented("list_topics is not exercised here"))
        }

        async fn describe_topic(
            &self,
            _request: Request<DescribeTopicRequest>,
        ) -> Result<Response<DescribeTopicResponse>, Status> {
            Err(Status::unimplemented(
                "describe_topic is not exercised here",
            ))
        }
    }

    /// A cluster member with the given id and address (availability defaulted —
    /// it plays no part in node-id→address resolution).
    fn member(id: &str, addr: &str) -> Member {
        Member {
            id: id.to_string(),
            addr: addr.to_string(),
            ..Default::default()
        }
    }

    /// Bind a fake cluster node on an OS-chosen localhost port and serve it on a
    /// background task. The listener is bound before returning, so the endpoint
    /// is already accepting connections — no startup race.
    async fn serve(node: FakeClusterNode) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        let service = VelaClientServer::new(node);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("fake server serves");
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Build a core over the given bootstrap `(id, addr)` pairs and an instant
    /// [`VirtualClock`].
    fn core_over(nodes: impl IntoIterator<Item = (String, String)>) -> ClientCore {
        ClientCore::with_clock(nodes, Arc::new(VirtualClock::new()))
    }

    /// A leader id learned from `FindLeader` resolves to its member address when
    /// that id is present in the `DescribeCluster` `Member_Address_Map`: the
    /// registry is seeded primarily from the member set (Requirement 13.1,
    /// 13.2). The leader id is **not** a bootstrap pair, so it can only resolve
    /// via the member map.
    #[tokio::test(flavor = "multi_thread")]
    async fn member_map_ids_resolve_to_their_addresses() {
        // The fake reports `node-x` as a member with its own address and names
        // it the leader; `node-x` is absent from the bootstrap pairs.
        let node = FakeClusterNode::new(
            vec![member("node-x", "http://node-x:50051")],
            Some("node-x"),
        );
        let server_addr = serve(node).await;
        let core = core_over([("seed-node".to_string(), server_addr)]);

        let addr = core
            .refresh_leader("orders", 0)
            .await
            .expect("the member-mapped leader id resolves to its address");

        assert_eq!(addr, "http://node-x:50051");
        // The map seeded the registry, so the id resolves even though it was
        // never supplied as an `id=url` Endpoint (Requirement 13.2).
        assert_eq!(
            core.registry().addr_of("node-x").as_deref(),
            Some("http://node-x:50051")
        );
    }

    /// An id absent from the `DescribeCluster` member set still resolves when it
    /// was supplied as an `id=url` bootstrap pair: the `id=url` registry fills
    /// the gap the member map leaves (the fallback — Requirement 13.3).
    #[tokio::test(flavor = "multi_thread")]
    async fn id_url_bootstrap_pairs_fill_gaps_left_by_the_member_map() {
        // The member map covers `node-x` only; the leader `node-y` is not in it
        // but is supplied as an `id=url` Endpoint.
        let node = FakeClusterNode::new(
            vec![member("node-x", "http://node-x:50051")],
            Some("node-y"),
        );
        let server_addr = serve(node).await;
        let core = core_over([
            ("seed-node".to_string(), server_addr),
            ("node-y".to_string(), "http://node-y:50051".to_string()),
        ]);

        let addr = core
            .refresh_leader("orders", 0)
            .await
            .expect("a leader absent from the member map resolves via its id=url pair");

        assert_eq!(addr, "http://node-y:50051");
    }

    /// When `DescribeCluster` returns no members, resolution falls back entirely
    /// to the `id=url` registry: the client proceeds degraded but functional with
    /// just the configured Endpoints (Requirement 13.3, 13.5).
    #[tokio::test(flavor = "multi_thread")]
    async fn empty_member_set_falls_back_to_the_id_url_registry() {
        // An older server (or one with no membership yet) reports no members; the
        // leader `node-z` resolves only because it was supplied as an `id=url`
        // Endpoint.
        let node = FakeClusterNode::new(Vec::new(), Some("node-z"));
        let server_addr = serve(node).await;
        let core = core_over([
            ("seed-node".to_string(), server_addr),
            ("node-z".to_string(), "http://node-z:50051".to_string()),
        ]);

        let addr = core
            .refresh_leader("orders", 0)
            .await
            .expect("with no member map the id=url Endpoint resolves the leader");

        assert_eq!(addr, "http://node-z:50051");
        // No member map means no member-sourced registry entries were added.
        assert_eq!(core.registry().addr_of("node-x"), None);
    }

    /// A leader id present in neither the `Member_Address_Map` nor a configured
    /// `id=url` Endpoint cannot be mapped to an address, so resolution surfaces
    /// [`ClientError::UnknownNode`] identifying the unresolved id (Requirement
    /// 13.4, 13.6).
    #[tokio::test(flavor = "multi_thread")]
    async fn unmapped_leader_id_is_an_unknown_node() {
        // The member map covers `node-x`; the bootstrap covers `seed-node`; the
        // leader `ghost-node` is in neither, so it is unresolvable.
        let node = FakeClusterNode::new(
            vec![member("node-x", "http://node-x:50051")],
            Some("ghost-node"),
        );
        let server_addr = serve(node).await;
        let core = core_over([("seed-node".to_string(), server_addr)]);

        let err = core
            .refresh_leader("orders", 0)
            .await
            .expect_err("an unmapped leader id cannot be resolved to an address");

        assert!(
            matches!(
                &err,
                ClientError::UnknownNode { node, topic, partition }
                    if node == "ghost-node" && topic == "orders" && *partition == 0
            ),
            "expected UnknownNode for ghost-node/orders/0, got {err:?}",
        );
    }

    /// [`seed_address`](super::seed_address) selects by precedence: a non-empty
    /// `advertised_addr` wins (Req 6.1); otherwise a non-empty bind `addr` is
    /// used — the older-server / no-advertised path (Req 6.2, 7.1); and a member
    /// with both empty is skipped (Req 6.4).
    #[test]
    fn seed_address_prefers_advertised_then_addr_then_none() {
        // Advertised address wins even when a bind address is also present.
        let both = Member {
            id: "n".to_string(),
            addr: "10.0.0.1:7001".to_string(),
            advertised_addr: "127.0.0.1:7002".to_string(),
            ..Default::default()
        };
        assert_eq!(super::seed_address(&both), Some("127.0.0.1:7002"));

        // No advertised address: fall back to the bind address (also the path
        // for an older server that never sets the field).
        let addr_only = Member {
            id: "n".to_string(),
            addr: "10.0.0.1:7001".to_string(),
            advertised_addr: String::new(),
            ..Default::default()
        };
        assert_eq!(super::seed_address(&addr_only), Some("10.0.0.1:7001"));

        // Both empty: the member has no usable address, so it is skipped.
        let neither = Member {
            id: "n".to_string(),
            addr: String::new(),
            advertised_addr: String::new(),
            ..Default::default()
        };
        assert_eq!(super::seed_address(&neither), None);
    }

    /// Gap-filling seeding never clobbers an operator-supplied `id=url`
    /// Endpoint: when a discovered member's id is already present in the
    /// registry, [`NodeRegistry::insert_if_absent`] leaves the configured
    /// address untouched and reports no insertion; only an absent id is added
    /// (Req 6.3). This mirrors what `seed_registry_from_cluster` does after
    /// selecting an address via `seed_address`.
    #[test]
    fn preseeded_id_url_is_left_unchanged_by_discovery() {
        let registry = crate::NodeRegistry::from_pairs([(
            "node-a".to_string(),
            "http://node-a:50051".to_string(),
        )]);

        // node-a is already configured: a discovered (internal/advertised)
        // address must not override the authoritative operator endpoint.
        let added = registry.insert_if_absent("node-a", "10.0.0.1:7001");
        assert!(!added, "an already-present id must not be re-inserted");
        assert_eq!(
            registry.addr_of("node-a").as_deref(),
            Some("http://node-a:50051"),
            "the configured id=url address must survive discovery",
        );

        // node-b was not configured: discovery fills the gap.
        let added = registry.insert_if_absent("node-b", "127.0.0.1:7002");
        assert!(added, "an absent id must be added by discovery");
        assert_eq!(
            registry.addr_of("node-b").as_deref(),
            Some("127.0.0.1:7002"),
        );
    }
}

#[cfg(test)]
mod prop_seed_precedence {
    //! Property test for client registry-seeding precedence in `vela-client`.
    //!
    //! Feature: advertised-listeners, Property 5: Client seeding precedence is
    //! advertised, then addr, then skip.
    //!
    //! For any wire [`v1::Member`], the address selected for registry seeding is
    //! the `advertised_addr` when it is non-empty (Req 6.1); otherwise the `addr`
    //! when it is non-empty — which is also the path for an older server that
    //! never populates the advertised field (Req 6.2, 7.1); otherwise no address
    //! is selected and the member is skipped (Req 6.4).
    //!
    //! [`seed_address`](super::seed_address) is `pub(crate)`, so it is not
    //! reachable from an external integration test under `tests/`. This property
    //! therefore lives in-crate, beside the selector it exercises, and asserts
    //! the precedence directly across a generator that ranges over every
    //! empty/non-empty combination of the two address fields.
    //!
    //! Validates: Requirements 6.1, 6.2, 6.4, 7.1

    use proptest::prelude::*;
    use vela_proto::v1::Member;

    use super::seed_address;

    /// Either an empty string (the proto3 default a field carries when unset, and
    /// the value an older server leaves on `advertised_addr`) or an arbitrary
    /// non-empty address-like token. Covering both halves for each field exercises
    /// all four `(advertised, addr)` empty/non-empty quadrants.
    fn maybe_empty_addr() -> impl Strategy<Value = String> {
        prop_oneof![Just(String::new()), "[a-zA-Z0-9.:_/-]{1,32}"]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: advertised-listeners, Property 5
        #[test]
        fn seed_address_follows_advertised_then_addr_then_skip(
            id in "[a-zA-Z0-9_-]{0,16}",
            advertised_addr in maybe_empty_addr(),
            addr in maybe_empty_addr(),
        ) {
            let member = Member {
                id,
                addr: addr.clone(),
                advertised_addr: advertised_addr.clone(),
                ..Default::default()
            };

            let selected = seed_address(&member);

            if !advertised_addr.is_empty() {
                // The advertised address wins whenever it is non-empty, even when
                // a bind address is also present (Req 6.1).
                prop_assert_eq!(selected, Some(advertised_addr.as_str()));
            } else if !addr.is_empty() {
                // No advertised address: fall back to the bind address — also the
                // older-server path (Req 6.2, 7.1).
                prop_assert_eq!(selected, Some(addr.as_str()));
            } else {
                // Both empty: the member has no usable address, so it is skipped
                // (Req 6.4).
                prop_assert_eq!(selected, None);
            }
        }
    }
}
