//! `vela-server` — the node daemon library.
//!
//! The only crate that wires networking (tonic gRPC) to the core. Hosts the
//! per-partition driver tasks, implements the `VelaClient` / `VelaPeer`
//! services, runs membership, and is built into the `velad` binary.
//!
//! This task (14.1) implements [configuration parsing](config) and the
//! structured-logging entry points the `velad` binary uses on startup. The
//! gRPC listener bind and the partition driver tasks are wired up in later
//! tasks (14.2+).

pub mod config;
pub mod convert;

mod clock;
mod driver;
pub mod membership;
mod node;
mod paths;
mod reconciler;
mod registry;
mod service;
mod transport;

use std::net::SocketAddr;

use vela_proto::v1::vela_client_server::VelaClientServer;
use vela_proto::v1::vela_peer_server::VelaPeerServer;

use crate::node::NodeShared;
use crate::service::{VelaClientService, VelaPeerService};

pub use config::{CliArgs, Config, ConfigError, Peer};

/// Initialize the global `tracing` subscriber used for structured logs.
///
/// Honours the `RUST_LOG` environment variable, defaulting to `info`. Uses a
/// non-panicking initializer so tests (which install their own subscribers) and
/// repeated calls are harmless.
pub fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // `try_init` returns `Err` if a global subscriber is already set; that is
    // fine and intentionally ignored.
    let _ = fmt().with_env_filter(filter).try_init();
}

/// Load and validate node configuration, logging a structured error on failure.
///
/// On an invalid or missing required value this emits a `tracing` error entry
/// describing the problem and returns the [`ConfigError`]; the binary maps that
/// to a non-zero exit (Requirement 15.2).
pub fn load_config(args: CliArgs) -> Result<Config, ConfigError> {
    match Config::from_cli(args) {
        Ok(config) => {
            tracing::info!(
                node_id = %config.node_id.as_str(),
                listen_addr = %config.listen_addr,
                peer_count = config.peers.len(),
                replication_factor = config.replication_factor,
                "loaded node configuration"
            );
            Ok(config)
        }
        Err(error) => {
            tracing::error!(%error, "invalid node configuration; refusing to start");
            Err(error)
        }
    }
}

/// Emit the readiness log indicating the node is ready to serve requests
/// (Requirement 15.3).
///
/// Called once the gRPC listener has successfully bound (Requirement 15.1).
pub fn log_ready(config: &Config) {
    tracing::info!(
        node_id = %config.node_id.as_str(),
        listen_addr = %config.listen_addr,
        "velad is ready to serve requests"
    );
}

/// A boxed, thread-safe error returned by the server runtime.
pub type ServeError = Box<dyn std::error::Error + Send + Sync>;

/// Bind the gRPC listener and serve the `VelaClient` and `VelaPeer` services
/// until the process is terminated (Requirement 15.1, 12.2, 12.3).
///
/// The listener is bound on `config.listen_addr` *before* readiness is signalled
/// so the [`log_ready`] entry truly reflects a node that can accept connections
/// (Requirement 15.1, 15.3). Both services share one [`NodeShared`] view of the
/// node, so admin operations, partition drivers, and peer RPCs all act on the
/// same state.
pub async fn serve(config: Config) -> Result<(), ServeError> {
    let addr: SocketAddr = config.listen_addr;
    let node = NodeShared::new(&config)?;

    let client = VelaClientServer::new(VelaClientService::new(node.clone()));
    let peer = VelaPeerServer::new(VelaPeerService::new(node.clone()));

    // Start the membership subsystem: connect to each configured peer, send
    // 1 s heartbeats, and track availability transitions (Requirement 9.1, 9.2,
    // 9.4, 9.5).
    membership::spawn_membership(node, config.peers.clone());

    // Bind first so readiness reflects a listener that is actually accepting
    // connections (Requirement 15.1).
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)?;

    log_ready(&config);

    tonic::transport::Server::builder()
        .add_service(client)
        .add_service(peer)
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    /// A captured `tracing` event: its level and the rendered `message` field.
    #[derive(Clone, Debug)]
    struct Captured {
        level: Level,
        message: String,
    }

    /// Minimal in-memory layer that records the level and message of each event.
    #[derive(Clone, Default)]
    struct CaptureLayer {
        events: Arc<Mutex<Vec<Captured>>>,
    }

    struct MessageVisitor(String);

    impl Visit for MessageVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
            }
        }
    }

    impl<S: Subscriber> Layer<S> for CaptureLayer {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = MessageVisitor(String::new());
            event.record(&mut visitor);
            self.events.lock().unwrap().push(Captured {
                level: *event.metadata().level(),
                message: visitor.0,
            });
        }
    }

    fn valid_args() -> CliArgs {
        CliArgs {
            node_id: Some("node-a".to_string()),
            listen_addr: Some("127.0.0.1:7001".to_string()),
            advertised_addr: None,
            peers: vec!["node-b:7001".to_string()],
            replication_factor: Some("3".to_string()),
            data_dir: Some("/var/lib/vela".to_string()),
        }
    }

    #[test]
    fn config_error_emits_structured_error_log() {
        let layer = CaptureLayer::default();
        let events = layer.events.clone();
        let subscriber = tracing_subscriber::registry().with(layer);

        let result = tracing::subscriber::with_default(subscriber, || {
            let mut args = valid_args();
            args.node_id = None;
            load_config(args)
        });

        assert!(result.is_err(), "missing node_id must fail to load");
        let events = events.lock().unwrap();
        let error = events
            .iter()
            .find(|e| e.level == Level::ERROR)
            .expect("an ERROR event must be emitted for invalid config");
        assert!(
            error.message.contains("invalid node configuration"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn readiness_emits_info_log() {
        let layer = CaptureLayer::default();
        let events = layer.events.clone();
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let config = Config::from_cli(valid_args()).expect("valid config");
            log_ready(&config);
        });

        let events = events.lock().unwrap();
        let ready = events
            .iter()
            .find(|e| e.level == Level::INFO && e.message.contains("ready to serve"))
            .expect("a readiness INFO event must be emitted");
        assert_eq!(ready.level, Level::INFO);
    }
}
