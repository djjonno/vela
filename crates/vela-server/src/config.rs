//! `velad` node configuration.
//!
//! Configuration is supplied through CLI flags or environment variables and
//! validated into a [`Config`] before the daemon starts (Requirement 14.4,
//! 15.1). The fields mirror the design's *Node Startup, Config, and Local
//! Cluster* section: `node_id`, `listen_addr`, `peers` (list), and
//! `replication_factor`.
//!
//! Parsing is deliberately split in two:
//!
//! 1. [`CliArgs`] uses `clap` only to *collect* raw values from the command
//!    line and the environment. Every field is optional/untyped so `clap` never
//!    hard-exits on a missing-required or malformed-value error.
//! 2. [`Config::from_cli`] performs the real validation and returns a typed
//!    [`ConfigError`]. This lets the binary log a structured `tracing` entry and
//!    exit non-zero on any configuration problem (Requirement 15.2) rather than
//!    letting `clap` print to stderr and `exit(2)` out from under us.

use std::net::SocketAddr;

use clap::Parser;
use thiserror::Error;
use vela_core::NodeId;

/// The minimum permitted replication factor. A partition's Raft group needs at
/// least one replica, so a factor below 1 is rejected.
pub const MIN_REPLICATION_FACTOR: u32 = 1;

/// Raw, unvalidated `velad` arguments collected from the CLI and environment.
///
/// All fields are optional strings so that `clap` performs no value validation
/// of its own; required-ness and format checks live in [`Config::from_cli`] so
/// that failures surface as a structured log and a non-zero exit
/// (Requirement 15.2).
#[derive(Debug, Clone, Default, Parser)]
#[command(name = "velad", about = "Vela node daemon", version)]
pub struct CliArgs {
    /// Stable identity of this node (e.g. `node-a`).
    #[arg(long, env = "VELA_NODE_ID")]
    pub node_id: Option<String>,

    /// Address the gRPC listener binds on (e.g. `0.0.0.0:7001`).
    #[arg(long, env = "VELA_LISTEN_ADDR")]
    pub listen_addr: Option<String>,

    /// Peer node addresses (`host:port`), comma-separated or repeated.
    #[arg(long = "peers", env = "VELA_PEERS", value_delimiter = ',', num_args = 0..)]
    pub peers: Vec<String>,

    /// Number of replicas per partition (Raft group size).
    #[arg(long, env = "VELA_REPLICATION_FACTOR")]
    pub replication_factor: Option<String>,
}

/// A validated `velad` configuration, ready for the daemon to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Stable identity of this node within the cluster.
    pub node_id: NodeId,
    /// The socket address the gRPC listener binds on.
    pub listen_addr: SocketAddr,
    /// Addresses (`host:port`) of the configured peer nodes. May be empty for a
    /// single-node cluster.
    pub peers: Vec<String>,
    /// Number of replicas assigned to each partition's Raft group.
    pub replication_factor: u32,
}

/// A configuration value that is missing, malformed, or out of range.
///
/// Each variant maps to a structured `tracing` error and a non-zero exit at the
/// binary boundary (Requirement 15.2).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigError {
    /// A required configuration value was not supplied via flag or environment.
    #[error("missing required configuration value: {0}")]
    MissingRequired(&'static str),

    /// `listen_addr` was supplied but is not a valid `host:port` socket address.
    #[error("invalid listen address '{value}': {reason}")]
    InvalidListenAddr {
        /// The offending raw value.
        value: String,
        /// A human-readable reason the value was rejected.
        reason: String,
    },

    /// `replication_factor` was supplied but is not a valid integer >= 1.
    #[error("invalid replication factor '{value}': {reason}")]
    InvalidReplicationFactor {
        /// The offending raw value.
        value: String,
        /// A human-readable reason the value was rejected.
        reason: String,
    },

    /// A peer entry was empty after trimming surrounding whitespace.
    #[error("invalid peer address: peer entries must not be empty")]
    EmptyPeer,
}

impl Config {
    /// Parse and validate configuration from the process's CLI arguments and
    /// environment.
    ///
    /// `clap` handles `--help`/`--version` and genuinely malformed *usage*
    /// (unknown flags) by printing to stderr and exiting, as is conventional.
    /// Missing-required and malformed *values* are returned as a
    /// [`ConfigError`] so the caller can log and exit deliberately.
    pub fn parse() -> Result<Self, ConfigError> {
        Self::from_cli(CliArgs::parse())
    }

    /// Validate already-collected [`CliArgs`] into a [`Config`].
    ///
    /// Returns the first [`ConfigError`] encountered; on error the caller must
    /// not start the node.
    pub fn from_cli(args: CliArgs) -> Result<Self, ConfigError> {
        let node_id = trimmed_required(args.node_id.as_deref(), "node_id")?;

        let listen_raw = require(args.listen_addr.as_deref(), "listen_addr")?;
        let listen_addr =
            listen_raw
                .parse::<SocketAddr>()
                .map_err(|err| ConfigError::InvalidListenAddr {
                    value: listen_raw.to_string(),
                    reason: err.to_string(),
                })?;

        let rf_raw = require(args.replication_factor.as_deref(), "replication_factor")?;
        let replication_factor = parse_replication_factor(rf_raw)?;

        let peers = normalize_peers(args.peers)?;

        Ok(Config {
            node_id: NodeId::new(node_id),
            listen_addr,
            peers,
            replication_factor,
        })
    }
}

/// Return the trimmed value if present and non-empty, else `MissingRequired`.
fn require<'a>(value: Option<&'a str>, field: &'static str) -> Result<&'a str, ConfigError> {
    match value.map(str::trim) {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(ConfigError::MissingRequired(field)),
    }
}

/// Like [`require`] but returns an owned `String` for storage.
fn trimmed_required(value: Option<&str>, field: &'static str) -> Result<String, ConfigError> {
    require(value, field).map(str::to_string)
}

/// Parse and range-check the replication factor.
fn parse_replication_factor(raw: &str) -> Result<u32, ConfigError> {
    let factor = raw
        .parse::<u32>()
        .map_err(|err| ConfigError::InvalidReplicationFactor {
            value: raw.to_string(),
            reason: err.to_string(),
        })?;
    if factor < MIN_REPLICATION_FACTOR {
        return Err(ConfigError::InvalidReplicationFactor {
            value: raw.to_string(),
            reason: format!("must be at least {MIN_REPLICATION_FACTOR}"),
        });
    }
    Ok(factor)
}

/// Trim each peer entry and reject any that is empty after trimming.
fn normalize_peers(peers: Vec<String>) -> Result<Vec<String>, ConfigError> {
    let mut out = Vec::with_capacity(peers.len());
    for peer in peers {
        let trimmed = peer.trim();
        if trimmed.is_empty() {
            return Err(ConfigError::EmptyPeer);
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build [`CliArgs`] for the common valid case, overridable per field.
    fn cli(
        node_id: Option<&str>,
        listen_addr: Option<&str>,
        peers: &[&str],
        replication_factor: Option<&str>,
    ) -> CliArgs {
        CliArgs {
            node_id: node_id.map(str::to_string),
            listen_addr: listen_addr.map(str::to_string),
            peers: peers.iter().map(|p| p.to_string()).collect(),
            replication_factor: replication_factor.map(str::to_string),
        }
    }

    #[test]
    fn valid_config_is_parsed() {
        let config = Config::from_cli(cli(
            Some("node-a"),
            Some("127.0.0.1:7001"),
            &["node-b:7001", "node-c:7001"],
            Some("3"),
        ))
        .expect("valid config must parse");

        assert_eq!(config.node_id, NodeId::new("node-a"));
        assert_eq!(config.listen_addr, "127.0.0.1:7001".parse().unwrap());
        assert_eq!(config.peers, vec!["node-b:7001", "node-c:7001"]);
        assert_eq!(config.replication_factor, 3);
    }

    #[test]
    fn empty_peer_list_is_valid_for_single_node() {
        let config = Config::from_cli(cli(Some("solo"), Some("0.0.0.0:7001"), &[], Some("1")))
            .expect("single-node config must parse");
        assert!(config.peers.is_empty());
        assert_eq!(config.replication_factor, MIN_REPLICATION_FACTOR);
    }

    #[test]
    fn missing_node_id_is_rejected() {
        let err = Config::from_cli(cli(None, Some("127.0.0.1:7001"), &[], Some("1")))
            .expect_err("missing node_id must error");
        assert_eq!(err, ConfigError::MissingRequired("node_id"));
    }

    #[test]
    fn blank_node_id_is_treated_as_missing() {
        let err = Config::from_cli(cli(Some("   "), Some("127.0.0.1:7001"), &[], Some("1")))
            .expect_err("blank node_id must error");
        assert_eq!(err, ConfigError::MissingRequired("node_id"));
    }

    #[test]
    fn missing_listen_addr_is_rejected() {
        let err = Config::from_cli(cli(Some("n"), None, &[], Some("1")))
            .expect_err("missing listen_addr must error");
        assert_eq!(err, ConfigError::MissingRequired("listen_addr"));
    }

    #[test]
    fn invalid_listen_addr_is_rejected() {
        let err = Config::from_cli(cli(Some("n"), Some("not-an-addr"), &[], Some("1")))
            .expect_err("invalid listen_addr must error");
        match err {
            ConfigError::InvalidListenAddr { value, .. } => assert_eq!(value, "not-an-addr"),
            other => panic!("expected InvalidListenAddr, got {other:?}"),
        }
    }

    #[test]
    fn missing_replication_factor_is_rejected() {
        let err = Config::from_cli(cli(Some("n"), Some("127.0.0.1:7001"), &[], None))
            .expect_err("missing replication_factor must error");
        assert_eq!(err, ConfigError::MissingRequired("replication_factor"));
    }

    #[test]
    fn non_numeric_replication_factor_is_rejected() {
        let err = Config::from_cli(cli(Some("n"), Some("127.0.0.1:7001"), &[], Some("three")))
            .expect_err("non-numeric replication_factor must error");
        match err {
            ConfigError::InvalidReplicationFactor { value, .. } => assert_eq!(value, "three"),
            other => panic!("expected InvalidReplicationFactor, got {other:?}"),
        }
    }

    #[test]
    fn zero_replication_factor_is_rejected() {
        let err = Config::from_cli(cli(Some("n"), Some("127.0.0.1:7001"), &[], Some("0")))
            .expect_err("zero replication_factor must error");
        match err {
            ConfigError::InvalidReplicationFactor { value, reason } => {
                assert_eq!(value, "0");
                assert!(reason.contains("at least"));
            }
            other => panic!("expected InvalidReplicationFactor, got {other:?}"),
        }
    }

    #[test]
    fn peer_entries_are_trimmed() {
        let config = Config::from_cli(cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &["  node-b:7001  ", "node-c:7001"],
            Some("2"),
        ))
        .expect("config with padded peers must parse");
        assert_eq!(config.peers, vec!["node-b:7001", "node-c:7001"]);
    }

    #[test]
    fn empty_peer_entry_is_rejected() {
        let err = Config::from_cli(cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &["node-b:7001", "   "],
            Some("2"),
        ))
        .expect_err("empty peer entry must error");
        assert_eq!(err, ConfigError::EmptyPeer);
    }

    #[test]
    fn clap_splits_comma_separated_peers_from_argv() {
        let args = CliArgs::try_parse_from([
            "velad",
            "--node-id",
            "node-a",
            "--listen-addr",
            "127.0.0.1:7001",
            "--peers",
            "node-b:7001,node-c:7001",
            "--replication-factor",
            "3",
        ])
        .expect("well-formed argv must parse");

        let config = Config::from_cli(args).expect("argv config must validate");
        assert_eq!(config.peers, vec!["node-b:7001", "node-c:7001"]);
        assert_eq!(config.node_id, NodeId::new("node-a"));
        assert_eq!(config.replication_factor, 3);
    }
}
