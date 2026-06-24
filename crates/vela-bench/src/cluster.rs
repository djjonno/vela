//! The `Cluster` seam and the in-process Cluster_Under_Test.
//!
//! The benchmark reaches the Cluster_Under_Test only through the [`Cluster`]
//! trait (design Decision 2 / Requirement 3.2): it exposes the bootstrap
//! `(node_id, address)` pairs a [`VelaClient`](vela_client::VelaClient) is
//! seeded with, a readiness check bounded by the startup budget, and a
//! shutdown. Keeping the topology behind a trait lets the harness logic be
//! tested against a fake cluster and leaves room to plug in a multi-node or
//! external cluster later without touching the phase logic.
//!
//! [`InProcessCluster`] is the concrete implementation: a single node started
//! inside the benchmark process via [`vela_server::serve`] on a [`tokio`] task,
//! bound to an ephemeral localhost port, with an empty peer list and
//! `replication_factor = 1` so each partition's Raft group can elect itself
//! leader and commit on its own (Requirement 9.3). The construction and
//! readiness pattern mirrors `crates/vela-server/tests/cross_node_produce_consume.rs`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_server::{serve, CliArgs, Config};

use crate::BenchError;

/// Interval between readiness probes while waiting for the Cluster_Under_Test
/// to start serving (Requirement 9.3). Short enough that a freshly bound
/// listener is observed promptly, long enough not to busy-spin.
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Monotonic counter making each in-process cluster's node id and data
/// directory unique within a process, so concurrent benchmark runs (and the
/// crate's own tests) never collide.
static CLUSTER_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The Cluster_Under_Test the benchmark drives, behind a trait so the harness
/// is testable and the topology is swappable (Requirement 3.2).
///
/// The benchmark connects to the cluster's bootstrap address(es) with a
/// `VelaClient`, exactly as a real client would, then drives produce/consume
/// through that client. The trait exposes only what the harness needs: the
/// bootstrap seed, a readiness gate, and teardown.
#[async_trait]
pub trait Cluster {
    /// Bootstrap `(node_id, address)` pairs to seed a
    /// [`VelaClient`](vela_client::VelaClient).
    ///
    /// Each address is client-dialable (an `http://host:port` URL), so the
    /// pairs can be handed straight to `VelaClient::new`.
    fn bootstrap(&self) -> Vec<(String, String)>;

    /// Resolve once the cluster can serve requests, or error with
    /// [`BenchError::ClusterNotReady`] when `budget` elapses first
    /// (Requirement 9.3).
    async fn await_ready(&self, budget: Duration) -> Result<(), BenchError>;

    /// Tear the cluster down at the end of a run.
    async fn shutdown(self) -> Result<(), BenchError>;
}

/// An in-process single-node Cluster_Under_Test.
///
/// Started via [`vela_server::serve`] on a background [`tokio`] task bound to an
/// ephemeral localhost port (design Decision 2). The spawned task's
/// [`JoinHandle`] is retained so [`shutdown`](Cluster::shutdown) can abort the
/// server, and the per-run data directory is retained so it can be cleaned up.
#[derive(Debug)]
pub struct InProcessCluster {
    /// This node's stable cluster identity, used as the bootstrap node id.
    node_id: String,
    /// The address the in-process server's gRPC listener is bound on.
    addr: SocketAddr,
    /// Handle to the background `serve` task, aborted on shutdown.
    handle: JoinHandle<()>,
}

impl InProcessCluster {
    /// Start an in-process single-node cluster.
    ///
    /// Binds an ephemeral localhost port, builds a validated single-node
    /// [`Config`] through the same `Config::from_cli` path the daemon uses
    /// (empty peers, `replication_factor = 1`), and spawns
    /// [`vela_server::serve`] on a background task. Construction performs no
    /// readiness wait — call [`await_ready`](Cluster::await_ready) before
    /// driving traffic.
    ///
    /// Returns [`BenchError::ClusterStartup`] if the port cannot be reserved or
    /// the single-node configuration fails to validate.
    pub async fn start() -> Result<Self, BenchError> {
        let n = CLUSTER_COUNTER.fetch_add(1, Ordering::Relaxed);
        let addr = free_addr()?;
        let node_id = format!("vela-bench-{}-{n}", process::id());
        let data_dir = unique_data_dir(n);

        // Build the single-node config through the daemon's own CLI path so the
        // node is validated exactly as `velad` would validate it. An empty peer
        // list with `replication_factor = 1` makes every partition a one-replica
        // Raft group that elects itself leader and commits locally.
        let config = Config::from_cli(CliArgs {
            node_id: Some(node_id.clone()),
            listen_addr: Some(addr.to_string()),
            advertised_addr: None,
            peers: Vec::new(),
            replication_factor: Some("1".to_string()),
            data_dir: Some(data_dir.to_string_lossy().into_owned()),
        })
        .map_err(|error| BenchError::ClusterStartup {
            detail: format!("invalid single-node configuration: {error}"),
        })?;

        // `serve` binds the gRPC listener before it would signal readiness, so a
        // subsequent connection probe reflects a listener actually accepting
        // connections. An early return from `serve` is a genuine failure; log it
        // so a run that never becomes ready surfaces the cause.
        let handle = tokio::spawn(async move {
            if let Err(error) = serve(config).await {
                tracing::error!(%error, "in-process Cluster_Under_Test server task exited");
            }
        });

        Ok(Self {
            node_id,
            addr,
            handle,
        })
    }

    /// The single bootstrap address as a client-dialable `http://host:port` URL.
    fn bootstrap_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

#[async_trait]
impl Cluster for InProcessCluster {
    fn bootstrap(&self) -> Vec<(String, String)> {
        vec![(self.node_id.clone(), self.bootstrap_url())]
    }

    /// Poll the bootstrap node on a fixed interval until a connection succeeds
    /// or `budget` elapses (Requirement 9.3).
    ///
    /// A successful TCP/HTTP2 connection to the freshly bound listener is the
    /// cheap readiness signal: it costs no application work and avoids the
    /// client's own multi-second request retry budget, so the loop honors the
    /// fixed poll interval. At least one probe is attempted before the deadline
    /// is consulted, so a budget that has already nominally elapsed still gives
    /// the cluster one chance to answer.
    async fn await_ready(&self, budget: Duration) -> Result<(), BenchError> {
        let url = self.bootstrap_url();
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            if VelaClientClient::connect(url.clone()).await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BenchError::ClusterNotReady {
                    budget_secs: budget.as_secs(),
                });
            }
            tokio::time::sleep(READINESS_POLL_INTERVAL).await;
        }
    }

    /// Abort the background `serve` task and tear the cluster down.
    ///
    /// Aborting only cancels the top-level `serve` task; the per-partition Raft
    /// driver tasks it spawned keep running briefly and may still read their
    /// durable logs by path as they wind down. Removing the data directory here
    /// would therefore race those still-live readers and trip the WAL's
    /// fail-stop on a vanished segment file. So the per-run data directory —
    /// a unique path under the system temp dir — is intentionally left in place
    /// for the OS temp reaper to reclaim, exactly as the `vela-server`
    /// integration tests do for the same reason.
    async fn shutdown(self) -> Result<(), BenchError> {
        self.handle.abort();
        Ok(())
    }
}

/// A live, externally-managed Cluster_Under_Test the benchmark connects to via
/// caller-supplied endpoints (the `--endpoints` flag).
///
/// Unlike [`InProcessCluster`], the benchmark neither starts nor stops this
/// cluster — it only seeds a [`VelaClient`](vela_client::VelaClient) with the
/// given bootstrap endpoints and drives produce/consume traffic against the
/// already-running deployment (e.g. the docker-compose cluster). The client
/// then discovers the full membership and per-partition leaders itself via the
/// cluster's `DescribeCluster`/`FindLeader` RPCs, so the bootstrap node-id
/// labels are placeholders that real ids learned from discovery supersede.
/// [`shutdown`](Cluster::shutdown) is therefore a no-op: the benchmark must not
/// tear down a cluster it did not start.
#[derive(Debug, Clone)]
pub struct ExternalCluster {
    /// Bootstrap `(node_id, url)` pairs seeded into the client, one per supplied
    /// endpoint. Each `url` is a client-dialable `http://host:port`.
    bootstrap: Vec<(String, String)>,
}

impl ExternalCluster {
    /// Build a cluster handle from caller-supplied `endpoints`.
    ///
    /// Each endpoint is `host:port`, `http://host:port`, or `id@addr` (the `id`
    /// half is an optional bootstrap node-id label; without it the normalized
    /// URL doubles as the label). A bare `host:port` is normalized to an
    /// `http://` URL so it can be handed straight to `VelaClient::new`. Returns
    /// [`BenchError::ClusterStartup`] when `endpoints` is empty or an entry is
    /// blank.
    pub fn new(endpoints: &[String]) -> Result<Self, BenchError> {
        if endpoints.is_empty() {
            return Err(BenchError::ClusterStartup {
                detail: "no --endpoints supplied for an external cluster".to_string(),
            });
        }
        let mut bootstrap = Vec::with_capacity(endpoints.len());
        for raw in endpoints {
            let raw = raw.trim();
            if raw.is_empty() {
                return Err(BenchError::ClusterStartup {
                    detail: "an --endpoints entry was empty".to_string(),
                });
            }
            // Optional `id@addr` form: an explicit label before `@`, otherwise
            // the normalized URL doubles as the bootstrap label.
            let (label, addr) = match raw.split_once('@') {
                Some((id, addr)) if !id.is_empty() && !addr.is_empty() => {
                    (Some(id.to_string()), addr)
                }
                _ => (None, raw),
            };
            let url = normalize_url(addr);
            let id = label.unwrap_or_else(|| url.clone());
            bootstrap.push((id, url));
        }
        Ok(Self { bootstrap })
    }
}

#[async_trait]
impl Cluster for ExternalCluster {
    fn bootstrap(&self) -> Vec<(String, String)> {
        self.bootstrap.clone()
    }

    /// Resolve once any supplied endpoint accepts a connection, or error with
    /// [`BenchError::ClusterNotReady`] when `budget` elapses (Requirement 9.3).
    ///
    /// Probing connectivity to a single reachable endpoint is enough: the
    /// client seeds from every endpoint and discovers the rest of the cluster
    /// from there. At least one full pass over the endpoints is attempted
    /// before the deadline is consulted.
    async fn await_ready(&self, budget: Duration) -> Result<(), BenchError> {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            for (_id, url) in &self.bootstrap {
                if VelaClientClient::connect(url.clone()).await.is_ok() {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BenchError::ClusterNotReady {
                    budget_secs: budget.as_secs(),
                });
            }
            tokio::time::sleep(READINESS_POLL_INTERVAL).await;
        }
    }

    /// A no-op: the external cluster is managed by the operator, not the
    /// benchmark, so a run must never stop it.
    async fn shutdown(self) -> Result<(), BenchError> {
        Ok(())
    }
}

/// Normalize a bootstrap address into a client-dialable URL: an address that
/// already carries an `http`/`https` scheme is used verbatim, otherwise a bare
/// `host:port` is prefixed with `http://`.
fn normalize_url(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address the in-process server should bind.
///
/// There is a small race between releasing the port and `serve` re-binding it,
/// but on localhost this is reliable in practice — the same pattern the
/// cross-node server integration test uses.
fn free_addr() -> Result<SocketAddr, BenchError> {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| BenchError::ClusterStartup {
            detail: format!("could not reserve an ephemeral localhost port: {error}"),
        })?;
    let addr = listener
        .local_addr()
        .map_err(|error| BenchError::ClusterStartup {
            detail: format!("could not read the reserved local address: {error}"),
        })?;
    drop(listener);
    Ok(addr)
}

/// A unique, writable data directory under the system temp directory for this
/// node. Each node needs its own root so its durable partition logs do not
/// collide with another node's (or another run's).
fn unique_data_dir(n: u64) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("vela-bench-cut-{}-{n}-{nanos}", process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A started in-process cluster exposes exactly one bootstrap pair whose
    /// address is a client-dialable `http://` URL, becomes ready promptly, and
    /// tears down cleanly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn starts_serves_and_shuts_down() {
        let cluster = InProcessCluster::start()
            .await
            .expect("the in-process cluster starts");

        let bootstrap = cluster.bootstrap();
        assert_eq!(
            bootstrap.len(),
            1,
            "a single-node cluster has one bootstrap"
        );
        let (node_id, addr) = &bootstrap[0];
        assert!(!node_id.is_empty(), "the bootstrap node id is populated");
        assert!(
            addr.starts_with("http://127.0.0.1:"),
            "the bootstrap address is a dialable localhost URL, got {addr}"
        );

        cluster
            .await_ready(Duration::from_secs(30))
            .await
            .expect("the freshly bound listener becomes ready within the budget");

        cluster.shutdown().await.expect("the cluster shuts down");
    }

    // ----- ExternalCluster (the `--endpoints` target) ----------------------

    #[test]
    fn external_cluster_normalizes_bare_and_schemed_endpoints() {
        let cluster = ExternalCluster::new(&[
            "127.0.0.1:7001".to_string(),
            "http://127.0.0.1:7002".to_string(),
        ])
        .expect("valid endpoints build a cluster");

        let bootstrap = cluster.bootstrap();
        // A bare host:port is prefixed with http://; a schemed URL is kept as-is.
        // With no explicit id label, the normalized URL doubles as the id.
        assert_eq!(
            bootstrap,
            vec![
                (
                    "http://127.0.0.1:7001".to_string(),
                    "http://127.0.0.1:7001".to_string()
                ),
                (
                    "http://127.0.0.1:7002".to_string(),
                    "http://127.0.0.1:7002".to_string()
                ),
            ]
        );
    }

    #[test]
    fn external_cluster_honors_explicit_id_label() {
        let cluster = ExternalCluster::new(&["node1@127.0.0.1:7001".to_string()])
            .expect("an id@addr endpoint builds a cluster");
        assert_eq!(
            cluster.bootstrap(),
            vec![("node1".to_string(), "http://127.0.0.1:7001".to_string())]
        );
    }

    #[test]
    fn external_cluster_rejects_empty_endpoint_list() {
        let error = ExternalCluster::new(&[]).expect_err("no endpoints is an error");
        assert!(matches!(error, BenchError::ClusterStartup { .. }));
    }

    #[test]
    fn external_cluster_rejects_a_blank_endpoint() {
        let error = ExternalCluster::new(&["   ".to_string()])
            .expect_err("a blank endpoint entry is an error");
        assert!(matches!(error, BenchError::ClusterStartup { .. }));
    }

    /// An external cluster's shutdown is a no-op: the benchmark must not tear
    /// down a deployment it did not start. (`shutdown` consumes `self`, so this
    /// also confirms it returns `Ok`.)
    #[tokio::test]
    async fn external_cluster_shutdown_is_a_no_op() {
        let cluster =
            ExternalCluster::new(&["127.0.0.1:7001".to_string()]).expect("valid endpoint");
        cluster.shutdown().await.expect("shutdown is a no-op Ok");
    }
}
