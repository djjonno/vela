//! Example tests for the node's lifecycle structured logs (task 14.7).
//!
//! These cover two of the lifecycle-logging acceptance criteria for the server
//! daemon through the crate's public surface:
//!
//! - a configuration error emits a structured ERROR log *and* `load_config`
//!   returns `Err` — the `velad` binary maps that `Err` to a non-zero exit
//!   (Requirement 15.2);
//! - a successful readiness emits a structured INFO log indicating the node is
//!   ready to serve requests (Requirement 15.3).
//!
//! The per-partition Raft role-transition log (Requirement 15.4) is exercised
//! by a unit test inside `vela-server`'s `driver` module, where the partition
//! driver internals that emit it are reachable.
//!
//! Each assertion installs a small in-memory `tracing` capture layer for the
//! duration of the logging call and inspects what was emitted.

use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

use vela_server::{load_config, log_ready, CliArgs, Config, ConfigError};

/// A captured `tracing` event: its level and the rendered `message` field.
#[derive(Clone, Debug)]
struct Captured {
    level: Level,
    message: String,
}

/// Minimal in-memory layer recording the level and message of each event.
#[derive(Clone, Default)]
struct CaptureLayer {
    events: Arc<Mutex<Vec<Captured>>>,
}

/// Visitor that extracts only the formatted `message` field of an event.
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

/// Run `f` with a fresh capture layer installed as the thread-local default
/// subscriber, returning everything it logged.
fn capture_logs(f: impl FnOnce()) -> Vec<Captured> {
    let layer = CaptureLayer::default();
    let events = layer.events.clone();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, f);
    let captured = events.lock().unwrap().clone();
    captured
}

/// A valid set of CLI args; individual tests mutate one field to make it
/// invalid.
fn valid_args() -> CliArgs {
    CliArgs {
        node_id: Some("node-a".to_string()),
        listen_addr: Some("127.0.0.1:7001".to_string()),
        peers: vec!["node-b:7001".to_string()],
        replication_factor: Some("3".to_string()),
        data_dir: Some("/var/lib/vela".to_string()),
    }
}

/// Requirement 15.2: an invalid or missing required configuration value emits a
/// structured ERROR log and `load_config` returns `Err` (the binary maps that
/// to a non-zero exit).
#[test]
fn config_error_emits_error_log_and_load_fails() {
    let mut result: Result<(), ConfigError> = Ok(());
    let logs = capture_logs(|| {
        let mut args = valid_args();
        // Drop a required value to force a configuration error.
        args.node_id = None;
        result = load_config(args).map(|_| ());
    });

    assert!(
        result.is_err(),
        "missing node_id must make load_config return Err (binary exits non-zero)"
    );

    let error = logs
        .iter()
        .find(|e| e.level == Level::ERROR)
        .expect("an ERROR log must be emitted for an invalid configuration");
    assert!(
        error.message.contains("invalid node configuration"),
        "unexpected error message: {}",
        error.message
    );
}

/// Requirement 15.3: a successful readiness emits a structured INFO log
/// indicating the node is ready to serve requests.
#[test]
fn readiness_emits_info_log() {
    let config = Config::from_cli(valid_args()).expect("valid configuration must load");
    let logs = capture_logs(|| log_ready(&config));

    let ready = logs
        .iter()
        .find(|e| e.level == Level::INFO && e.message.contains("ready to serve"))
        .expect("a readiness INFO log must be emitted");
    assert_eq!(ready.level, Level::INFO);
}
