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
use std::path::PathBuf;

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

    /// Client-reachable address advertised to clients (e.g. `127.0.0.1:7001`).
    /// Defaults to `listen_addr` when unset or blank after trimming
    /// (Requirement 1.1, 1.2, 1.3).
    #[arg(long, env = "VELA_ADVERTISED_ADDR")]
    pub advertised_addr: Option<String>,

    /// Peer nodes as `id@host:port` (e.g. `node2@node2:7001`), comma-separated
    /// or repeated. A bare `host:port` is accepted and uses the address as the
    /// peer id (single-node/local fallback).
    #[arg(long = "peers", env = "VELA_PEERS", value_delimiter = ',', num_args = 0..)]
    pub peers: Vec<String>,

    /// Number of replicas per partition (Raft group size).
    #[arg(long, env = "VELA_REPLICATION_FACTOR")]
    pub replication_factor: Option<String>,

    /// Root directory under which durable partition logs store their segments.
    #[arg(long, env = "VELA_DATA_DIR")]
    pub data_dir: Option<String>,
}

/// A configured peer node: its stable cluster identity and transport address.
///
/// Peers are configured as `id@host:port` (e.g. `node2@node2:7001`) so a node
/// knows each peer by the *same* stable id that peer uses for itself. That
/// shared identity is what lets every node derive a consistent numeric
/// [`raft_node_id`](crate::registry::raft_node_id) for a given node, so Raft
/// votes/appends and the leader reported by `FindLeader` line up across the
/// cluster. A bare `host:port` entry (no `id@`) is accepted for the
/// single-node/local case and falls back to using the address as the id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    /// The peer's stable cluster node id (matches its own `VELA_NODE_ID`).
    pub id: String,
    /// The peer's transport address (`host:port`) — its Listen_Address, used
    /// for server-to-server dialing.
    pub addr: String,
    /// The peer's client-reachable advertised address (`host:port`). Equals
    /// [`addr`](Self::addr) when no advertised half is supplied in the peer
    /// entry. Client-facing metadata only; never used for inter-node dialing.
    pub advertised_addr: String,
}

/// A validated `velad` configuration, ready for the daemon to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Stable identity of this node within the cluster.
    pub node_id: NodeId,
    /// The socket address the gRPC listener binds on.
    pub listen_addr: SocketAddr,
    /// The resolved client-reachable advertised address (`host:port`). Equals
    /// `listen_addr.to_string()` when `--advertised-addr` / `VELA_ADVERTISED_ADDR`
    /// is unset or blank after trimming (Requirement 1.2, 1.5).
    pub advertised_addr: String,
    /// The configured peer nodes (id + `host:port`). May be empty for a
    /// single-node cluster.
    pub peers: Vec<Peer>,
    /// Number of replicas assigned to each partition's Raft group.
    pub replication_factor: u32,
    /// Root directory under which all Durable partition logs hosted on this
    /// node store their segments (Requirement 6.1, 6.3).
    pub data_dir: PathBuf,
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

        // Resolve the advertised address: a non-empty trimmed flag/env value is
        // recorded verbatim with no format validation (any non-empty value,
        // including a `0.0.0.0` wildcard host or a bare hostname, is accepted —
        // Req 1.1, 1.3, 1.4); otherwise it defaults to the listen address so
        // zero-config deployments are unchanged (Req 1.2).
        let advertised_addr = match args.advertised_addr.as_deref().map(str::trim) {
            Some(v) if !v.is_empty() => v.to_string(),
            _ => listen_addr.to_string(),
        };

        let rf_raw = require(args.replication_factor.as_deref(), "replication_factor")?;
        let replication_factor = parse_replication_factor(rf_raw)?;

        let peers = normalize_peers(args.peers)?;

        // `VELA_DATA_DIR` is required at startup (assumption A1): because
        // `durable` is the default backend, any node may be asked to host a
        // durable partition, so the directory is validated like the other
        // required values and the node fails fast when it is absent
        // (Requirement 6.1, 6.2).
        let data_dir = PathBuf::from(require(args.data_dir.as_deref(), "data_dir")?);

        Ok(Config {
            node_id: NodeId::new(node_id),
            listen_addr,
            advertised_addr,
            peers,
            replication_factor,
            data_dir,
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

/// Parse each peer entry into a [`Peer`] and reject any that is empty after
/// trimming.
///
/// An entry follows the grammar `id@listen[@advertised]` (or a bare
/// `host:port`, where the address doubles as both the id and the listen
/// address, for the single-node/local fallback). After splitting `(id, rest)`
/// on the first `@`, `rest` is split once more on `@` into
/// `(listen, advertised)`; when no second `@` is present the advertised address
/// defaults to the listen address (D2). Each half is trimmed; an entry that is
/// empty, or whose `id`, `listen`, or explicitly-supplied `advertised` half is
/// empty (e.g. `@addr`, `id@`, `id@listen@`, or a lone `@`), is rejected with
/// [`ConfigError::EmptyPeer`]. Every legacy `id@listen` and bare `host:port`
/// entry parses unchanged with `advertised = listen`.
fn normalize_peers(peers: Vec<String>) -> Result<Vec<Peer>, ConfigError> {
    let mut out = Vec::with_capacity(peers.len());
    for peer in peers {
        let trimmed = peer.trim();
        if trimmed.is_empty() {
            return Err(ConfigError::EmptyPeer);
        }
        let (id, rest) = match trimmed.split_once('@') {
            Some((id, rest)) => (id.trim(), rest.trim()),
            None => (trimmed, trimmed),
        };
        // Split the listen/advertised halves; absent second `@` defaults the
        // advertised address to the listen address (D2).
        let (listen, advertised) = match rest.split_once('@') {
            Some((listen, advertised)) => (listen.trim(), advertised.trim()),
            None => (rest, rest),
        };
        if id.is_empty() || listen.is_empty() || advertised.is_empty() {
            return Err(ConfigError::EmptyPeer);
        }
        out.push(Peer {
            id: id.to_string(),
            addr: listen.to_string(),
            advertised_addr: advertised.to_string(),
        });
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
        data_dir: Option<&str>,
    ) -> CliArgs {
        CliArgs {
            node_id: node_id.map(str::to_string),
            listen_addr: listen_addr.map(str::to_string),
            advertised_addr: None,
            peers: peers.iter().map(|p| p.to_string()).collect(),
            replication_factor: replication_factor.map(str::to_string),
            data_dir: data_dir.map(str::to_string),
        }
    }

    #[test]
    fn valid_config_is_parsed() {
        let config = Config::from_cli(cli(
            Some("node-a"),
            Some("127.0.0.1:7001"),
            &["node-b:7001", "node-c:7001"],
            Some("3"),
            Some("/var/lib/vela"),
        ))
        .expect("valid config must parse");

        assert_eq!(config.node_id, NodeId::new("node-a"));
        assert_eq!(config.listen_addr, "127.0.0.1:7001".parse().unwrap());
        assert_eq!(
            config.peers,
            vec![
                Peer {
                    id: "node-b:7001".to_string(),
                    addr: "node-b:7001".to_string(),
                    advertised_addr: "node-b:7001".to_string()
                },
                Peer {
                    id: "node-c:7001".to_string(),
                    addr: "node-c:7001".to_string(),
                    advertised_addr: "node-c:7001".to_string()
                },
            ]
        );
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.data_dir, PathBuf::from("/var/lib/vela"));
    }

    #[test]
    fn empty_peer_list_is_valid_for_single_node() {
        let config = Config::from_cli(cli(
            Some("solo"),
            Some("0.0.0.0:7001"),
            &[],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect("single-node config must parse");
        assert!(config.peers.is_empty());
        assert_eq!(config.replication_factor, MIN_REPLICATION_FACTOR);
    }

    #[test]
    fn missing_node_id_is_rejected() {
        let err = Config::from_cli(cli(
            None,
            Some("127.0.0.1:7001"),
            &[],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect_err("missing node_id must error");
        assert_eq!(err, ConfigError::MissingRequired("node_id"));
    }

    #[test]
    fn blank_node_id_is_treated_as_missing() {
        let err = Config::from_cli(cli(
            Some("   "),
            Some("127.0.0.1:7001"),
            &[],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect_err("blank node_id must error");
        assert_eq!(err, ConfigError::MissingRequired("node_id"));
    }

    #[test]
    fn missing_listen_addr_is_rejected() {
        let err = Config::from_cli(cli(Some("n"), None, &[], Some("1"), Some("/var/lib/vela")))
            .expect_err("missing listen_addr must error");
        assert_eq!(err, ConfigError::MissingRequired("listen_addr"));
    }

    #[test]
    fn invalid_listen_addr_is_rejected() {
        let err = Config::from_cli(cli(
            Some("n"),
            Some("not-an-addr"),
            &[],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect_err("invalid listen_addr must error");
        match err {
            ConfigError::InvalidListenAddr { value, .. } => assert_eq!(value, "not-an-addr"),
            other => panic!("expected InvalidListenAddr, got {other:?}"),
        }
    }

    #[test]
    fn missing_replication_factor_is_rejected() {
        let err = Config::from_cli(cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &[],
            None,
            Some("/var/lib/vela"),
        ))
        .expect_err("missing replication_factor must error");
        assert_eq!(err, ConfigError::MissingRequired("replication_factor"));
    }

    #[test]
    fn non_numeric_replication_factor_is_rejected() {
        let err = Config::from_cli(cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &[],
            Some("three"),
            Some("/var/lib/vela"),
        ))
        .expect_err("non-numeric replication_factor must error");
        match err {
            ConfigError::InvalidReplicationFactor { value, .. } => assert_eq!(value, "three"),
            other => panic!("expected InvalidReplicationFactor, got {other:?}"),
        }
    }

    #[test]
    fn zero_replication_factor_is_rejected() {
        let err = Config::from_cli(cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &[],
            Some("0"),
            Some("/var/lib/vela"),
        ))
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
            Some("/var/lib/vela"),
        ))
        .expect("config with padded peers must parse");
        assert_eq!(
            config.peers,
            vec![
                Peer {
                    id: "node-b:7001".to_string(),
                    addr: "node-b:7001".to_string(),
                    advertised_addr: "node-b:7001".to_string()
                },
                Peer {
                    id: "node-c:7001".to_string(),
                    addr: "node-c:7001".to_string(),
                    advertised_addr: "node-c:7001".to_string()
                },
            ]
        );
    }

    #[test]
    fn empty_peer_entry_is_rejected() {
        let err = Config::from_cli(cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &["node-b:7001", "   "],
            Some("2"),
            Some("/var/lib/vela"),
        ))
        .expect_err("empty peer entry must error");
        assert_eq!(err, ConfigError::EmptyPeer);
    }

    #[test]
    fn peer_with_explicit_id_is_split() {
        // `id@host:port` records the peer's real cluster id distinct from its
        // address, so identity lines up across the cluster.
        let config = Config::from_cli(cli(
            Some("node1"),
            Some("0.0.0.0:7001"),
            &["node2@node2:7001", "node3@node3:7001"],
            Some("3"),
            Some("/var/lib/vela"),
        ))
        .expect("config with id@addr peers must parse");
        assert_eq!(
            config.peers,
            vec![
                Peer {
                    id: "node2".to_string(),
                    addr: "node2:7001".to_string(),
                    advertised_addr: "node2:7001".to_string()
                },
                Peer {
                    id: "node3".to_string(),
                    addr: "node3:7001".to_string(),
                    advertised_addr: "node3:7001".to_string()
                },
            ]
        );
    }

    #[test]
    fn peer_id_and_addr_halves_are_trimmed() {
        let config = Config::from_cli(cli(
            Some("node1"),
            Some("0.0.0.0:7001"),
            &["  node2  @  node2:7001  "],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect("config with padded id@addr must parse");
        assert_eq!(
            config.peers,
            vec![Peer {
                id: "node2".to_string(),
                addr: "node2:7001".to_string(),
                advertised_addr: "node2:7001".to_string()
            }]
        );
    }

    #[test]
    fn peer_with_empty_id_or_addr_half_is_rejected() {
        for bad in ["@node2:7001", "node2@", "@", "  @  "] {
            let err = Config::from_cli(cli(
                Some("node1"),
                Some("0.0.0.0:7001"),
                &[bad],
                Some("1"),
                Some("/var/lib/vela"),
            ))
            .expect_err("a half-empty id@addr peer must error");
            assert_eq!(err, ConfigError::EmptyPeer, "input {bad:?}");
        }
    }

    #[test]
    fn data_dir_is_read_from_env_value() {
        // `VELA_DATA_DIR` is collected onto `CliArgs::data_dir` (via the
        // `env = "VELA_DATA_DIR"` binding) and validated into `Config::data_dir`
        // (Requirement 6.1, 6.3).
        let config = Config::from_cli(cli(
            Some("node-a"),
            Some("127.0.0.1:7001"),
            &[],
            Some("1"),
            Some("/srv/vela/data"),
        ))
        .expect("config with a data_dir must parse");
        assert_eq!(config.data_dir, PathBuf::from("/srv/vela/data"));
    }

    #[test]
    fn missing_data_dir_is_rejected() {
        // An absent `VELA_DATA_DIR` fails fast with a structured configuration
        // error, which the binary maps to a non-zero exit (Requirement 6.2).
        let err = Config::from_cli(cli(Some("n"), Some("127.0.0.1:7001"), &[], Some("1"), None))
            .expect_err("missing data_dir must error");
        assert_eq!(err, ConfigError::MissingRequired("data_dir"));
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
            "--data-dir",
            "/var/lib/vela",
        ])
        .expect("well-formed argv must parse");

        let config = Config::from_cli(args).expect("argv config must validate");
        assert_eq!(
            config.peers,
            vec![
                Peer {
                    id: "node-b:7001".to_string(),
                    addr: "node-b:7001".to_string(),
                    advertised_addr: "node-b:7001".to_string()
                },
                Peer {
                    id: "node-c:7001".to_string(),
                    addr: "node-c:7001".to_string(),
                    advertised_addr: "node-c:7001".to_string()
                },
            ]
        );
        assert_eq!(config.node_id, NodeId::new("node-a"));
        assert_eq!(config.replication_factor, 3);
    }

    // ---- advertised address resolution (advertised-listeners 1.x) ---------

    #[test]
    fn advertised_addr_value_is_recorded() {
        // A non-empty `--advertised-addr` / `VELA_ADVERTISED_ADDR` value is
        // recorded verbatim as the node's Advertised_Address (Req 1.1). clap
        // binds both the flag and the env var onto `CliArgs::advertised_addr`, so
        // exercising that field covers both sources (mirroring
        // `data_dir_is_read_from_env_value`).
        let mut args = cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &[],
            Some("1"),
            Some("/var/lib/vela"),
        );
        args.advertised_addr = Some("203.0.113.5:9001".to_string());
        let config = Config::from_cli(args).expect("config with an advertised addr must parse");
        assert_eq!(config.advertised_addr, "203.0.113.5:9001");
    }

    #[test]
    fn absent_advertised_addr_defaults_to_listen() {
        // Req 1.2: an absent advertised value defaults to the Listen_Address.
        let config = Config::from_cli(cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &[],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect("config without an advertised addr must parse");
        assert_eq!(config.advertised_addr, "127.0.0.1:7001");
        assert_eq!(config.advertised_addr, config.listen_addr.to_string());
    }

    #[test]
    fn blank_advertised_addr_defaults_to_listen() {
        // Req 1.2: a value that is empty after trimming falls back to the listen
        // address rather than being recorded or rejected.
        for blank in ["", "   ", "\t \n"] {
            let mut args = cli(
                Some("n"),
                Some("127.0.0.1:7001"),
                &[],
                Some("1"),
                Some("/var/lib/vela"),
            );
            args.advertised_addr = Some(blank.to_string());
            let config =
                Config::from_cli(args).expect("a blank advertised addr must default, not error");
            assert_eq!(config.advertised_addr, "127.0.0.1:7001", "input {blank:?}");
        }
    }

    #[test]
    fn advertised_addr_surrounding_whitespace_is_trimmed() {
        // Req 1.3: surrounding whitespace is trimmed before the value is recorded.
        let mut args = cli(
            Some("n"),
            Some("127.0.0.1:7001"),
            &[],
            Some("1"),
            Some("/var/lib/vela"),
        );
        args.advertised_addr = Some("  198.51.100.7:7002  ".to_string());
        let config = Config::from_cli(args).expect("a padded advertised addr must parse");
        assert_eq!(config.advertised_addr, "198.51.100.7:7002");
    }

    #[test]
    fn any_non_empty_advertised_addr_is_accepted() {
        // Req 1.4: any non-empty value is accepted without format validation,
        // including a `0.0.0.0` wildcard host, a bare hostname, and a host:port.
        for value in [
            "0.0.0.0:7001",
            "0.0.0.0",
            "node2",
            "node2:7001",
            "example.com",
        ] {
            let mut args = cli(
                Some("n"),
                Some("127.0.0.1:7001"),
                &[],
                Some("1"),
                Some("/var/lib/vela"),
            );
            args.advertised_addr = Some(value.to_string());
            let config = Config::from_cli(args)
                .unwrap_or_else(|err| panic!("advertised {value:?} must be accepted, got {err:?}"));
            assert_eq!(config.advertised_addr, value);
        }
    }

    // ---- peer grammar `id@listen[@advertised]` (advertised-listeners D2) ---

    #[test]
    fn peer_with_advertised_half_parses_all_three_fields() {
        // `id@listen@advertised` records the peer's id, its bind/listen address,
        // and its client-reachable advertised address as three distinct fields.
        let config = Config::from_cli(cli(
            Some("node1"),
            Some("0.0.0.0:7001"),
            &["node2@node2:7001@127.0.0.1:7002"],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect("an id@listen@advertised peer must parse");
        assert_eq!(
            config.peers,
            vec![Peer {
                id: "node2".to_string(),
                addr: "node2:7001".to_string(),
                advertised_addr: "127.0.0.1:7002".to_string(),
            }]
        );
    }

    #[test]
    fn peer_advertised_half_is_trimmed() {
        let config = Config::from_cli(cli(
            Some("node1"),
            Some("0.0.0.0:7001"),
            &["  node2  @  node2:7001  @  127.0.0.1:7002  "],
            Some("1"),
            Some("/var/lib/vela"),
        ))
        .expect("a padded id@listen@advertised peer must parse");
        assert_eq!(
            config.peers,
            vec![Peer {
                id: "node2".to_string(),
                addr: "node2:7001".to_string(),
                advertised_addr: "127.0.0.1:7002".to_string(),
            }]
        );
    }

    #[test]
    fn peer_with_empty_advertised_half_is_rejected() {
        // An explicitly-empty advertised half (`id@listen@`, or whitespace-only)
        // is rejected with the existing `EmptyPeer`, symmetric with the empty
        // listen half (design D2).
        for bad in ["node2@node2:7001@", "node2@node2:7001@   "] {
            let err = Config::from_cli(cli(
                Some("node1"),
                Some("0.0.0.0:7001"),
                &[bad],
                Some("1"),
                Some("/var/lib/vela"),
            ))
            .expect_err("an empty advertised half must error");
            assert_eq!(err, ConfigError::EmptyPeer, "input {bad:?}");
        }
    }
}
