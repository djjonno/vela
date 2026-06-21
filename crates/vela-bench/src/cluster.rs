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
    /// The unique data directory backing this node's durable partition logs;
    /// removed on shutdown (best effort).
    data_dir: PathBuf,
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
            data_dir,
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

    /// Abort the background `serve` task and remove the node's data directory.
    ///
    /// Aborting the task drops the bound listener and tears the server down;
    /// directory removal is best effort (a failure to clean up a temp directory
    /// is not a benchmark failure).
    async fn shutdown(self) -> Result<(), BenchError> {
        self.handle.abort();
        // Best-effort, non-blocking-enough cleanup of a small temp directory;
        // a synchronous `std::fs` call avoids pulling in tokio's `fs` feature
        // for what is a fire-and-forget teardown step.
        let _ = std::fs::remove_dir_all(&self.data_dir);
        Ok(())
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
}
