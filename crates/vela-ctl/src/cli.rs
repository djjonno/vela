//! The `vela-ctl` command-line surface.
//!
//! Defines the [`clap`] argument model ([`Cli`] + [`Command`]) and the
//! [`run`] dispatcher that drives the [`vela_client`] API. The four operator
//! (topic-admin) commands map one-to-one onto [`AdminClient`] calls:
//!
//! - `create <name> --partitions N` → [`create_topic`] (Requirement 13.1)
//! - `delete <name>` → [`delete_topic`] (Requirement 13.2)
//! - `list` → [`list_topics`] (Requirement 13.3)
//! - `describe <name>` → [`describe_topic`] (Requirement 13.4)
//!
//! A fifth admin command exposes cluster membership:
//!
//! - `describe-cluster` (alias `cluster`) → [`describe_cluster`] — the
//!   `Member_Address_Map` (each node's id, address, availability) plus the
//!   metadata epoch (Requirement 12.6, 12.7, 12.8)
//!
//! Two data-plane commands drive the produce/consume client roles:
//!
//! - `produce <name> [--key K] [--value V]` → the interactive Producer REPL
//!   ([`crate::produce::run_repl`], Requirement 6.x). When `--value` is supplied
//!   the command instead produces that single record and exits (a
//!   backward-compatible one-shot); when it is omitted the REPL reads lines from
//!   stdin and produces each until end-of-input or Ctrl+C.
//! - `consume <name> [--partition P] [--offset-reset R] [--poll-interval MS]` →
//!   the continuous Consumer loop ([`crate::consume::run_consume`], Requirements
//!   8–11), which discovers a topic's partitions and polls each leader until
//!   Ctrl+C.
//!
//! The four topic-admin calls are each wrapped in a 5 s [`tokio::time::timeout`]:
//! if the cluster cannot be reached in time the command reports a connection
//! error and exits non-zero (Requirement 13.6); any error the cluster returns is
//! likewise reported with a non-zero exit (Requirement 13.7). The long-running
//! produce/consume loops are not time-bounded — they run until the operator
//! ends the session — and classify their own failures internally. A command
//! that completes returns `Ok(())`, which the caller turns into exit status zero
//! (Requirement 13.5).
//!
//! [`AdminClient`]: vela_client::AdminClient
//! [`create_topic`]: vela_client::AdminClient::create_topic
//! [`delete_topic`]: vela_client::AdminClient::delete_topic
//! [`list_topics`]: vela_client::AdminClient::list_topics
//! [`describe_topic`]: vela_client::AdminClient::describe_topic
//! [`describe_cluster`]: vela_client::AdminClient::describe_cluster

use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use vela_client::{ClientConfig, ClientError, KeylessStrategy, LogBackend, VelaClient};

use crate::consume::{run_consume, OffsetReset};
use crate::produce::run_repl;
use crate::seams::{CtrlC, StdinLines, TokioClock};

/// Default endpoint contacted when `--endpoints`/`VELA_ADDR` is not supplied.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:50051";

/// Run length applied to the sticky keyless partitioner when `--keyless sticky`
/// is selected without an explicit run length: a run of this many consecutive
/// keyless records is assigned to one partition before rotating (Requirement
/// 5.6).
const DEFAULT_STICKY_RUN_LENGTH: u32 = 16;

/// Upper bound on how long any single admin call may take before it is reported
/// as a connection failure (Requirement 13.6).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Exit code for a cluster-returned (request) error (Requirement 13.7).
const EXIT_CLUSTER_ERROR: u8 = 1;
/// Exit code for a connection failure (Requirement 13.6).
const EXIT_CONNECTION_ERROR: u8 = 2;

/// The `vela-ctl` command line.
#[derive(Debug, Parser)]
#[command(
    name = "vela-ctl",
    version,
    about = "Control tool for administering a Vela cluster"
)]
pub struct Cli {
    /// Cluster endpoint(s) to contact. Repeat the flag or comma-separate several
    /// addresses; the client treats them as bootstrap nodes and routes admin
    /// calls to whichever it can reach. To produce/consume, prefix each entry
    /// with its cluster node id as `id=url` (e.g.
    /// `node1=http://127.0.0.1:7001`) so partition leaders can be dialed by id.
    #[arg(
        long = "endpoints",
        visible_alias = "addr",
        global = true,
        env = "VELA_ADDR",
        value_delimiter = ',',
        default_value = DEFAULT_ENDPOINT,
        value_name = "URL"
    )]
    pub endpoints: Vec<String>,

    /// Maximum age, in seconds, a cached topic-metadata entry may reach before
    /// the client refreshes it on the next produce/consume routing operation
    /// (Requirement 1.7). Defaults to 30 seconds; rejected at parse time if not
    /// a non-negative integer.
    #[arg(
        long = "metadata-ttl",
        global = true,
        default_value = "30",
        value_name = "SECS",
        value_parser = parse_metadata_ttl
    )]
    pub metadata_ttl: Duration,

    /// The operation to perform.
    #[command(subcommand)]
    pub command: Command,
}

/// The operator commands exposed by `vela-ctl`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create a topic with a name and partition count (Requirement 13.1).
    Create {
        /// Topic name.
        name: String,
        /// Number of partitions to create the topic with.
        #[arg(long, short = 'p', value_name = "N")]
        partitions: u32,
        /// Log storage backend for the topic: `durable` (the default) or
        /// `in-memory` (per-topic-log-durability Requirements 1.1–1.3). Any
        /// other value is rejected at parse time, before any request is sent.
        #[arg(
            long,
            value_name = "BACKEND",
            default_value = "durable",
            value_parser = parse_backend
        )]
        backend: LogBackend,
    },
    /// Delete a topic (Requirement 13.2).
    Delete {
        /// Topic name.
        name: String,
    },
    /// List all topics with their partition counts (Requirement 13.3).
    List,
    /// Describe a topic's partitions and the node leading each (Requirement 13.4).
    Describe {
        /// Topic name.
        name: String,
    },
    /// Describe the cluster membership: each node's id, address, and
    /// availability, plus the metadata epoch (Requirement 12.6, 12.7, 12.8).
    #[command(name = "describe-cluster", visible_alias = "cluster")]
    DescribeCluster,
    /// Produce records to a topic (Requirement 6).
    ///
    /// With `--value` the command produces that single record and exits (a
    /// backward-compatible one-shot). Without `--value` it opens an interactive
    /// REPL that produces each line read from stdin until end-of-input or Ctrl+C
    /// (Requirement 6.x, 7.x). The partition is resolved client-side: a `--key`
    /// maps deterministically to a partition, while an absent key spreads
    /// records across partitions (Requirement 5.1, 5.2).
    Produce {
        /// Topic name.
        name: String,
        /// Optional record key. A non-empty key routes deterministically to a
        /// partition; omit it to spread records across partitions. In a REPL
        /// session the key (when supplied) is applied to every produced line
        /// (Requirement 6.4).
        #[arg(long, short = 'k', value_name = "KEY")]
        key: Option<String>,
        /// Optional record value (payload). When supplied, a single record is
        /// produced and the command exits (one-shot, backward compatible). When
        /// omitted, `produce` opens the interactive REPL described above
        /// (Requirement 6.x).
        #[arg(long, short = 'v', value_name = "VALUE")]
        value: Option<String>,
        /// Keyless routing strategy for records produced without a `--key`:
        /// `round-robin` (the default) spreads each keyless record across the
        /// partitions in cyclic order, while `sticky` assigns a run of
        /// consecutive keyless records to one partition before rotating
        /// (Requirement 5.2, 5.6). Any other value is rejected at parse time.
        #[arg(
            long = "keyless",
            value_name = "STRATEGY",
            default_value = "round-robin",
            value_parser = parse_keyless
        )]
        keyless: KeylessStrategy,
    },
    /// Consume committed records from a topic (Requirement 8).
    ///
    /// Runs the continuous, multi-partition consumer: it discovers the topic's
    /// partitions and polls each partition's leader, printing every record until
    /// Ctrl+C (Requirements 8–11). Supply `--partition` to read a single
    /// partition instead of the whole topic (Requirement 8.4).
    Consume {
        /// Topic name.
        name: String,
        /// Partition index to read from. Omit to consume every partition of the
        /// topic (Requirement 8.4).
        #[arg(long, short = 'p', value_name = "INDEX")]
        partition: Option<u32>,
        /// Deprecated for the continuous consumer: the start offset is now
        /// derived from `--offset-reset` and each partition is polled to the end
        /// of its log, so this flag no longer applies and is ignored. Retained on
        /// the command surface for backward-compatible parsing only.
        #[arg(long, short = 'o', value_name = "OFFSET", default_value_t = 0)]
        offset: u64,
        /// Deprecated for the continuous consumer: the loop polls each partition
        /// continuously rather than reading a bounded batch, so this flag no
        /// longer applies and is ignored. Retained for backward-compatible
        /// parsing only.
        #[arg(long, short = 'm', value_name = "N")]
        max: Option<u32>,
        /// Where each partition begins reading: `latest` (the default) reads only
        /// records produced after the session starts, while `earliest` reads from
        /// the beginning of each partition's log (Requirement 8.6, 8.7). Any
        /// other value is rejected at parse time.
        #[arg(
            long = "offset-reset",
            value_name = "RESET",
            default_value = "latest",
            value_parser = parse_offset_reset
        )]
        offset_reset: OffsetReset,
        /// Interval, in milliseconds, to wait before re-polling a partition that
        /// returned no new records (Requirement 9.5). Defaults to 500ms; rejected
        /// at parse time if not a non-negative integer.
        #[arg(
            long = "poll-interval",
            value_name = "MS",
            default_value = "500",
            value_parser = parse_poll_interval
        )]
        poll_interval: Duration,
    },
}

impl Cli {
    /// Parse `vela-ctl`'s arguments from the process environment.
    ///
    /// On a parse error or `--help`/`--version`, `clap` prints and exits the
    /// process directly with its own status, so this only returns on success.
    pub fn parse_args() -> Self {
        <Self as Parser>::parse()
    }
}

/// Parse a `--backend` flag value into the client [`LogBackend`].
///
/// Delegates to the client's own parser so the CLI accepts exactly the two
/// values the client API does — `durable` and `in-memory` — and rejects
/// anything else at parse time, before any request is sent
/// (per-topic-log-durability Requirement 1.3).
fn parse_backend(value: &str) -> Result<LogBackend, ClientError> {
    value.parse()
}

/// Parse a `--keyless` flag value into a [`KeylessStrategy`].
///
/// Accepts exactly `round-robin` (per-record rotation, the default) and
/// `sticky` (a run of [`DEFAULT_STICKY_RUN_LENGTH`] consecutive keyless records
/// per partition before rotating); any other value is rejected at parse time,
/// before any request is sent, mirroring [`parse_backend`] (Requirement 5.2,
/// 5.6).
fn parse_keyless(value: &str) -> Result<KeylessStrategy, String> {
    match value {
        "round-robin" => Ok(KeylessStrategy::RoundRobin),
        "sticky" => Ok(KeylessStrategy::Sticky {
            run_length: DEFAULT_STICKY_RUN_LENGTH,
        }),
        other => Err(format!(
            "invalid keyless strategy '{other}' (expected 'round-robin' or 'sticky')"
        )),
    }
}

/// Parse an `--offset-reset` flag value into an [`OffsetReset`].
///
/// Accepts exactly `latest` (the default) and `earliest`; any other value is
/// rejected at parse time, before any request is sent (Requirement 8.6, 8.7).
fn parse_offset_reset(value: &str) -> Result<OffsetReset, String> {
    match value {
        "latest" => Ok(OffsetReset::Latest),
        "earliest" => Ok(OffsetReset::Earliest),
        other => Err(format!(
            "invalid offset reset '{other}' (expected 'latest' or 'earliest')"
        )),
    }
}

/// Parse a `--poll-interval` flag value (milliseconds) into a [`Duration`].
///
/// Rejects any value that is not a non-negative integer at parse time, before
/// any request is sent (Requirement 9.5).
fn parse_poll_interval(value: &str) -> Result<Duration, String> {
    value
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| {
            format!("invalid poll interval '{value}' (expected milliseconds as an integer)")
        })
}

/// Parse a `--metadata-ttl` flag value (seconds) into a [`Duration`].
///
/// Rejects any value that is not a non-negative integer at parse time, before
/// any request is sent (Requirement 1.7).
fn parse_metadata_ttl(value: &str) -> Result<Duration, String> {
    value
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| format!("invalid metadata TTL '{value}' (expected seconds as an integer)"))
}

/// A human-readable label for a topic's wire backend value.
///
/// Decodes the `TopicInfo.log_backend` wire field into the client enum; an
/// unspecified or unrecognized value is reported as `unspecified`.
fn backend_label(log_backend: i32) -> String {
    LogBackend::from_wire(log_backend)
        .map(|backend| backend.to_string())
        .unwrap_or_else(|| "unspecified".to_string())
}

/// A human-readable label for a member's wire availability value.
///
/// Decodes the `Member.availability` wire field; an unspecified or unrecognized
/// value is reported as `unknown`.
fn availability_label(availability: i32) -> &'static str {
    match vela_proto::v1::NodeAvailability::try_from(availability) {
        Ok(vela_proto::v1::NodeAvailability::Available) => "available",
        Ok(vela_proto::v1::NodeAvailability::Unavailable) => "unavailable",
        Ok(vela_proto::v1::NodeAvailability::Unspecified) | Err(_) => "unknown",
    }
}

/// A failure that ends the process with a non-zero status.
///
/// The two variants map to the two failure modes the CLI must distinguish:
/// reaching the cluster (a connection error, Requirement 13.6) versus a request
/// the cluster actively rejected (Requirement 13.7).
#[derive(Debug)]
pub enum CtlError {
    /// The cluster could not be reached (timed out or transport unavailable).
    Connection(String),
    /// The cluster returned an error for the request.
    Cluster(ClientError),
}

/// Turn a command result into a process exit status.
///
/// Success is status zero (Requirement 13.5); each failure is reported to stderr
/// and yields a distinct non-zero status (Requirements 13.6, 13.7).
pub fn report(result: Result<(), CtlError>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(CtlError::Connection(msg)) => {
            eprintln!("vela-ctl: connection error: {msg}");
            ExitCode::from(EXIT_CONNECTION_ERROR)
        }
        Err(CtlError::Cluster(err)) => {
            eprintln!("vela-ctl: {err}");
            ExitCode::from(EXIT_CLUSTER_ERROR)
        }
    }
}

/// Execute a parsed [`Cli`] against a cluster.
///
/// Builds a [`VelaClient`] from the endpoint list and dispatches the chosen
/// command. The four topic-admin commands print a human-readable outcome on
/// success; `produce`/`consume` hand off to the long-running REPL/poll loops.
///
/// The client core is configured from the parsed flags via [`ClientConfig`]: the
/// `--metadata-ttl` governs the shared metadata cache for every command
/// (Requirement 1.7), and the `produce` `--keyless` strategy is applied to the
/// router (Requirement 5.2, 5.6). For non-`produce` commands the keyless setting
/// is irrelevant, so the default round-robin strategy is used.
pub async fn run(cli: Cli) -> Result<(), CtlError> {
    // The metadata TTL applies to the shared core for every command; the keyless
    // strategy is only meaningful for `produce`, so other commands use the
    // default. The core is built once per `run`, so the strategy is read from
    // the command before dispatch (Requirement 1.7, 5.2, 5.6).
    let keyless = match &cli.command {
        Command::Produce { keyless, .. } => *keyless,
        _ => KeylessStrategy::default(),
    };
    let config = ClientConfig {
        metadata_ttl: cli.metadata_ttl,
        keyless,
    };

    let client = VelaClient::with_config(derive_nodes(&cli.endpoints), config);
    let admin = client.admin();

    match cli.command {
        Command::Create {
            name,
            partitions,
            backend,
        } => {
            let topic = with_timeout(admin.create_topic(&name, partitions, backend)).await?;
            println!(
                "created topic '{}' with {} partition(s) ({} backend)",
                topic.name,
                topic.partition_count,
                backend_label(topic.log_backend)
            );
        }
        Command::Delete { name } => {
            with_timeout(admin.delete_topic(&name)).await?;
            println!("deleted topic '{name}'");
        }
        Command::List => {
            let topics = with_timeout(admin.list_topics()).await?;
            if topics.is_empty() {
                println!("no topics");
            } else {
                for topic in topics {
                    println!("{}\t{} partition(s)", topic.name, topic.partition_count);
                }
            }
        }
        Command::Describe { name } => {
            let topic = with_timeout(admin.describe_topic(&name)).await?;
            println!(
                "topic '{}' ({} partition(s), {} backend)",
                topic.name,
                topic.partition_count,
                backend_label(topic.log_backend)
            );
            for partition in topic.partitions {
                let leader = partition.leader.as_deref().unwrap_or("<no leader>");
                println!("  partition {} leader {}", partition.index, leader);
            }
        }
        Command::DescribeCluster => {
            let cluster = with_timeout(admin.describe_cluster()).await?;
            println!(
                "cluster: {} member(s) (epoch {})",
                cluster.members.len(),
                cluster.epoch
            );
            for member in cluster.members {
                // Surface both the internal/bind address and the
                // client-reachable advertised address (advertised-listeners):
                // the advertised address is the one a client dials, and may
                // differ from the bind address under port mapping / NAT. An
                // older server omits the advertised field, so fall back to the
                // bind address, which it mirrors.
                let advertised = if member.advertised_addr.is_empty() {
                    member.addr.as_str()
                } else {
                    member.advertised_addr.as_str()
                };
                println!(
                    "  {}\t{}\t(advertised {})\t{}",
                    member.id,
                    member.addr,
                    advertised,
                    availability_label(member.availability)
                );
            }
        }
        // `keyless` was read above and applied to the configured router, so it
        // is not needed again here.
        Command::Produce {
            name, key, value, ..
        } => {
            let producer = client.producer();
            let key_bytes = key.map(String::into_bytes);
            match value {
                // Backward-compatible one-shot: `--value V` produces a single
                // record and exits, time-bounded like the admin calls.
                Some(value) => {
                    let offset = with_timeout(producer.produce(
                        &name,
                        key_bytes.as_deref(),
                        value.into_bytes(),
                    ))
                    .await?;
                    println!("produced to topic '{name}' at offset {offset}");
                }
                // No `--value`: open the interactive Producer REPL over stdin and
                // Ctrl+C, producing each entered line until end-of-input or an
                // interrupt (Requirement 6.x, 7.x). The REPL runs unbounded and
                // classifies its own I/O failures.
                None => {
                    let mut out = std::io::stdout();
                    run_repl(
                        producer,
                        name,
                        key_bytes,
                        StdinLines::new(),
                        CtrlC,
                        &mut out,
                    )
                    .await?;
                }
            }
        }
        // `offset`/`max` are ignored here — the continuous loop derives each
        // partition's start offset from `offset_reset` and polls to the end of
        // the log (see their flag docs).
        Command::Consume {
            name,
            partition,
            offset_reset,
            poll_interval,
            ..
        } => {
            // Run the continuous, multi-partition consumer until Ctrl+C, using
            // the production seams: the real wall clock, OS-signal interrupt, and
            // process stdout (Requirements 8–11). `run_consume` owns its own
            // error classification, so it is not wrapped in `with_timeout`.
            let mut out = std::io::stdout();
            run_consume(
                client,
                name,
                partition,
                offset_reset,
                poll_interval,
                Arc::new(TokioClock),
                CtrlC,
                &mut out,
            )
            .await?;
        }
    }
    Ok(())
}

/// Pair each endpoint with the node id used to resolve partition leaders.
///
/// Produce/consume resolve a partition's leader by its *cluster* node id (the
/// `VELA_NODE_ID` each node is configured with) via the client's node registry,
/// so that id must be known to dial the leader. Each `--endpoints` entry may
/// therefore carry an explicit id as `id=url` (e.g. `node1=http://127.0.0.1:7001`).
///
/// An entry without an `id=` prefix is a bare address: it still seeds the
/// bootstrap set used for admin calls and the initial `FindLeader`, but is given
/// a synthetic positional `node-{i}` id that will not match a real leader id —
/// fine for topic admin, insufficient for produce/consume to a partition led by
/// that node. Supply `id=url` entries (typically all cluster nodes) to use the
/// data-plane commands.
fn derive_nodes(endpoints: &[String]) -> Vec<(String, String)> {
    endpoints
        .iter()
        .enumerate()
        .map(|(i, entry)| match entry.split_once('=') {
            Some((id, addr)) if !id.is_empty() => (id.to_string(), addr.to_string()),
            _ => (format!("node-{i}"), entry.clone()),
        })
        .collect()
}

/// Run `op` under the [`CONNECT_TIMEOUT`], classifying every failure.
///
/// A timeout becomes a [`CtlError::Connection`] (Requirement 13.6). A returned
/// error is classified by [`classify`]: a transport-`Unavailable` status is a
/// connection error, anything else is a [`CtlError::Cluster`] rejection
/// (Requirement 13.7).
async fn with_timeout<F, T>(op: F) -> Result<T, CtlError>
where
    F: Future<Output = Result<T, ClientError>>,
{
    match tokio::time::timeout(CONNECT_TIMEOUT, op).await {
        Err(_elapsed) => Err(CtlError::Connection(format!(
            "no node reachable within {}s",
            CONNECT_TIMEOUT.as_secs()
        ))),
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(classify(err)),
    }
}

/// Sort a [`ClientError`] into a connection failure or a cluster rejection.
///
/// - [`ClientError::UnknownNode`]: a resolved leader node id that maps to no
///   address — from neither the server's `Member_Address_Map` nor a configured
///   `id=url` endpoint — is a fail-fast connection error reporting the
///   unresolved node id and that an `id=url` endpoint is required for it
///   (Requirement 13.4, 13.6).
/// - A transport-level `Unavailable` status means the client never reached a
///   node (e.g. connection refused), so it is reported as a connection error
///   (Requirement 13.6).
/// - Every other error — including [`ClientError::TopicNotFound`]
///   (Requirement 1.4), [`ClientError::NoPartitions`] (Requirement 1.8, 8.5),
///   and [`ClientError::NoLeaderAfterRetries`] (Requirement 4.4) — is a request
///   the cluster, or the client's routing layer, rejected, and is reported as a
///   cluster error (Requirement 13.7). All of these exit non-zero.
fn classify(err: ClientError) -> CtlError {
    match err {
        // A resolved leader id with no known address: fail fast with a clear,
        // actionable message (Requirement 13.4, 13.6).
        ClientError::UnknownNode {
            node,
            topic,
            partition,
        } => CtlError::Connection(format!(
            "leader node `{node}` for {topic}/{partition} has no known address; \
             an `id=url` endpoint is required for node {node}"
        )),
        // A transport `Unavailable` means no node was reached (Requirement 13.6).
        ClientError::Rpc(status) if status.code() == tonic::Code::Unavailable => {
            CtlError::Connection(status.message().to_string())
        }
        // Topic-not-found, no-partitions, no-leader-after-retries, and every
        // other error are cluster-side rejections reported non-zero
        // (Requirement 1.4, 1.8, 4.4, 8.5, 13.7).
        other => CtlError::Cluster(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse argv (program name prepended) the way the binary would.
    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        let mut argv = vec!["vela-ctl"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
    }

    #[test]
    fn create_parses_name_and_partition_count() {
        let cli = parse(&["create", "orders", "--partitions", "8"]).expect("valid");
        match cli.command {
            Command::Create {
                name, partitions, ..
            } => {
                assert_eq!(name, "orders");
                assert_eq!(partitions, 8);
            }
            other => panic!("expected create, got {other:?}"),
        }
    }

    #[test]
    fn create_accepts_short_partition_flag() {
        let cli = parse(&["create", "orders", "-p", "3"]).expect("valid");
        assert!(matches!(cli.command, Command::Create { partitions: 3, .. }));
    }

    #[test]
    fn create_defaults_backend_to_durable() {
        // Omitting `--backend` selects the durable default
        // (per-topic-log-durability Requirement 1.2).
        let cli = parse(&["create", "orders", "-p", "1"]).expect("valid");
        assert!(matches!(
            cli.command,
            Command::Create {
                backend: LogBackend::Durable,
                ..
            }
        ));
    }

    #[test]
    fn create_accepts_both_backend_values() {
        let durable = parse(&["create", "orders", "-p", "1", "--backend", "durable"])
            .expect("valid durable backend");
        assert!(matches!(
            durable.command,
            Command::Create {
                backend: LogBackend::Durable,
                ..
            }
        ));

        let in_memory = parse(&["create", "orders", "-p", "1", "--backend", "in-memory"])
            .expect("valid in-memory backend");
        assert!(matches!(
            in_memory.command,
            Command::Create {
                backend: LogBackend::InMemory,
                ..
            }
        ));
    }

    #[test]
    fn create_rejects_an_unknown_backend() {
        // An unrecognized backend is rejected at parse time, before any request
        // is sent (per-topic-log-durability Requirement 1.3).
        assert!(parse(&["create", "orders", "-p", "1", "--backend", "bogus"]).is_err());
    }

    #[test]
    fn create_requires_partition_count() {
        // Missing `--partitions` is a parse error, not a default.
        assert!(parse(&["create", "orders"]).is_err());
    }

    #[test]
    fn create_rejects_non_numeric_partition_count() {
        assert!(parse(&["create", "orders", "--partitions", "lots"]).is_err());
    }

    #[test]
    fn delete_parses_name() {
        let cli = parse(&["delete", "orders"]).expect("valid");
        assert!(matches!(cli.command, Command::Delete { name } if name == "orders"));
    }

    #[test]
    fn list_parses_with_no_args() {
        let cli = parse(&["list"]).expect("valid");
        assert!(matches!(cli.command, Command::List));
    }

    #[test]
    fn describe_parses_name() {
        let cli = parse(&["describe", "orders"]).expect("valid");
        assert!(matches!(cli.command, Command::Describe { name } if name == "orders"));
    }

    #[test]
    fn produce_parses_name_key_and_value() {
        let cli = parse(&["produce", "orders", "--key", "user-1", "--value", "hi"]).expect("valid");
        match cli.command {
            Command::Produce {
                name, key, value, ..
            } => {
                assert_eq!(name, "orders");
                assert_eq!(key.as_deref(), Some("user-1"));
                assert_eq!(value.as_deref(), Some("hi"));
            }
            other => panic!("expected produce, got {other:?}"),
        }
    }

    #[test]
    fn produce_key_is_optional() {
        let cli = parse(&["produce", "orders", "--value", "hi"]).expect("valid");
        assert!(matches!(cli.command, Command::Produce { key: None, .. }));
    }

    #[test]
    fn produce_value_is_optional_and_opens_the_repl() {
        // Omitting `--value` is no longer a parse error: it selects the
        // interactive REPL (value resolves to `None`), reading lines from stdin
        // (Requirement 6.x).
        let cli = parse(&["produce", "orders"]).expect("valid");
        assert!(matches!(cli.command, Command::Produce { value: None, .. }));
    }

    #[test]
    fn consume_parses_partition_offset_and_max() {
        let cli = parse(&[
            "consume", "orders", "-p", "2", "--offset", "10", "--max", "50",
        ])
        .expect("valid");
        match cli.command {
            Command::Consume {
                name,
                partition,
                offset,
                max,
                ..
            } => {
                assert_eq!(name, "orders");
                assert_eq!(partition, Some(2));
                assert_eq!(offset, 10);
                assert_eq!(max, Some(50));
            }
            other => panic!("expected consume, got {other:?}"),
        }
    }

    #[test]
    fn consume_defaults_offset_to_zero_and_max_to_none() {
        let cli = parse(&["consume", "orders", "--partition", "0"]).expect("valid");
        assert!(matches!(
            cli.command,
            Command::Consume {
                offset: 0,
                max: None,
                ..
            }
        ));
    }

    #[test]
    fn consume_partition_is_optional() {
        // Omitting `--partition` consumes every partition of the topic, so it is
        // no longer a parse error and resolves to `None` (Requirement 8.4).
        let cli = parse(&["consume", "orders"]).expect("valid");
        assert!(matches!(
            cli.command,
            Command::Consume {
                partition: None,
                ..
            }
        ));
    }

    #[test]
    fn produce_defaults_keyless_to_round_robin() {
        // Omitting `--keyless` selects the round-robin default (Requirement 5.2).
        let cli = parse(&["produce", "orders", "--value", "hi"]).expect("valid");
        assert!(matches!(
            cli.command,
            Command::Produce {
                keyless: KeylessStrategy::RoundRobin,
                ..
            }
        ));
    }

    #[test]
    fn produce_accepts_both_keyless_strategies() {
        let round_robin = parse(&[
            "produce",
            "orders",
            "--value",
            "hi",
            "--keyless",
            "round-robin",
        ])
        .expect("valid round-robin strategy");
        assert!(matches!(
            round_robin.command,
            Command::Produce {
                keyless: KeylessStrategy::RoundRobin,
                ..
            }
        ));

        let sticky = parse(&["produce", "orders", "--value", "hi", "--keyless", "sticky"])
            .expect("valid sticky strategy");
        assert!(matches!(
            sticky.command,
            Command::Produce {
                keyless: KeylessStrategy::Sticky { .. },
                ..
            }
        ));
    }

    #[test]
    fn produce_rejects_an_unknown_keyless_strategy() {
        // An unrecognized strategy is rejected at parse time (Requirement 5.2).
        assert!(parse(&["produce", "orders", "--value", "hi", "--keyless", "bogus"]).is_err());
    }

    #[test]
    fn consume_defaults_offset_reset_to_latest() {
        // Omitting `--offset-reset` selects the latest default (Requirement 8.6).
        let cli = parse(&["consume", "orders"]).expect("valid");
        assert!(matches!(
            cli.command,
            Command::Consume {
                offset_reset: OffsetReset::Latest,
                ..
            }
        ));
    }

    #[test]
    fn consume_accepts_both_offset_resets() {
        let latest =
            parse(&["consume", "orders", "--offset-reset", "latest"]).expect("valid latest");
        assert!(matches!(
            latest.command,
            Command::Consume {
                offset_reset: OffsetReset::Latest,
                ..
            }
        ));

        let earliest =
            parse(&["consume", "orders", "--offset-reset", "earliest"]).expect("valid earliest");
        assert!(matches!(
            earliest.command,
            Command::Consume {
                offset_reset: OffsetReset::Earliest,
                ..
            }
        ));
    }

    #[test]
    fn consume_rejects_an_unknown_offset_reset() {
        assert!(parse(&["consume", "orders", "--offset-reset", "bogus"]).is_err());
    }

    #[test]
    fn consume_defaults_poll_interval_to_500ms() {
        // Omitting `--poll-interval` applies the 500ms default (Requirement 9.5).
        let cli = parse(&["consume", "orders"]).expect("valid");
        assert!(matches!(
            cli.command,
            Command::Consume { poll_interval, .. } if poll_interval == Duration::from_millis(500)
        ));
    }

    #[test]
    fn consume_parses_poll_interval_milliseconds() {
        let cli = parse(&["consume", "orders", "--poll-interval", "250"]).expect("valid");
        assert!(matches!(
            cli.command,
            Command::Consume { poll_interval, .. } if poll_interval == Duration::from_millis(250)
        ));
    }

    #[test]
    fn consume_rejects_a_non_numeric_poll_interval() {
        assert!(parse(&["consume", "orders", "--poll-interval", "soon"]).is_err());
    }

    #[test]
    fn metadata_ttl_defaults_to_thirty_seconds() {
        // Omitting `--metadata-ttl` applies the 30s default (Requirement 1.7).
        let cli = parse(&["list"]).expect("valid");
        assert_eq!(cli.metadata_ttl, Duration::from_secs(30));
    }

    #[test]
    fn metadata_ttl_parses_seconds_and_is_global() {
        // The flag is global, so it is accepted after the subcommand too.
        let cli = parse(&["list", "--metadata-ttl", "5"]).expect("valid");
        assert_eq!(cli.metadata_ttl, Duration::from_secs(5));
    }

    #[test]
    fn metadata_ttl_rejects_a_non_numeric_value() {
        assert!(parse(&["--metadata-ttl", "soon", "list"]).is_err());
    }

    #[test]
    fn a_subcommand_is_required() {
        assert!(parse(&[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_rejected() {
        assert!(parse(&["frobnicate"]).is_err());
    }

    #[test]
    fn endpoints_default_to_local_node() {
        let cli = parse(&["list"]).expect("valid");
        assert_eq!(cli.endpoints, vec![DEFAULT_ENDPOINT.to_string()]);
    }

    #[test]
    fn endpoints_accept_repeated_flags() {
        let cli = parse(&[
            "--endpoints",
            "http://a:1",
            "--endpoints",
            "http://b:2",
            "list",
        ])
        .expect("valid");
        assert_eq!(cli.endpoints, vec!["http://a:1", "http://b:2"]);
    }

    #[test]
    fn endpoints_accept_comma_separated_values() {
        let cli = parse(&["--endpoints", "http://a:1,http://b:2", "list"]).expect("valid");
        assert_eq!(cli.endpoints, vec!["http://a:1", "http://b:2"]);
    }

    #[test]
    fn addr_alias_sets_endpoints() {
        let cli = parse(&["--addr", "http://a:1", "list"]).expect("valid");
        assert_eq!(cli.endpoints, vec!["http://a:1"]);
    }

    #[test]
    fn endpoints_flag_is_global_after_subcommand() {
        let cli = parse(&["list", "--endpoints", "http://a:1"]).expect("valid");
        assert_eq!(cli.endpoints, vec!["http://a:1"]);
    }

    #[test]
    fn derive_nodes_pairs_each_address_with_a_stable_id() {
        let nodes = derive_nodes(&["http://a:1".to_string(), "http://b:2".to_string()]);
        assert_eq!(
            nodes,
            vec![
                ("node-0".to_string(), "http://a:1".to_string()),
                ("node-1".to_string(), "http://b:2".to_string()),
            ]
        );
    }

    #[test]
    fn derive_nodes_honors_explicit_node_ids() {
        // An `id=url` entry seeds the registry with the real cluster node id, so
        // partition leaders returned by FindLeader can be resolved to addresses.
        let nodes = derive_nodes(&[
            "node1=http://127.0.0.1:7001".to_string(),
            "node2=http://127.0.0.1:7002".to_string(),
        ]);
        assert_eq!(
            nodes,
            vec![
                ("node1".to_string(), "http://127.0.0.1:7001".to_string()),
                ("node2".to_string(), "http://127.0.0.1:7002".to_string()),
            ]
        );
    }

    #[test]
    fn derive_nodes_mixes_explicit_and_synthetic_ids() {
        // A bare address keeps its positional synthetic id; an `id=url` entry
        // keeps its explicit id. The synthetic id uses the entry's index.
        let nodes = derive_nodes(&[
            "http://a:1".to_string(),
            "node2=http://127.0.0.1:7002".to_string(),
        ]);
        assert_eq!(
            nodes,
            vec![
                ("node-0".to_string(), "http://a:1".to_string()),
                ("node2".to_string(), "http://127.0.0.1:7002".to_string()),
            ]
        );
    }

    #[test]
    fn timeout_maps_to_connection_error() {
        // A future that never resolves trips the timeout and is reported as a
        // connection failure (Requirement 13.6).
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        rt.block_on(async {
            tokio::time::pause();
            let op = async {
                std::future::pending::<()>().await;
                Ok::<(), ClientError>(())
            };
            let fut = with_timeout(op);
            tokio::pin!(fut);
            // Advance past the timeout deterministically.
            tokio::time::advance(CONNECT_TIMEOUT + Duration::from_secs(1)).await;
            let result = fut.await;
            assert!(matches!(result, Err(CtlError::Connection(_))));
        });
    }

    #[test]
    fn unavailable_status_is_a_connection_error() {
        let err = ClientError::Rpc(Box::new(tonic::Status::unavailable("dial failed")));
        assert!(matches!(classify(err), CtlError::Connection(_)));
    }

    #[test]
    fn application_status_is_a_cluster_error() {
        let err = ClientError::Rpc(Box::new(tonic::Status::not_found("no such topic")));
        assert!(matches!(classify(err), CtlError::Cluster(_)));
    }

    #[test]
    fn routing_error_is_a_cluster_error() {
        let err = ClientError::NoNodes;
        assert!(matches!(classify(err), CtlError::Cluster(_)));
    }

    #[test]
    fn topic_not_found_is_a_cluster_error() {
        // A topic the cluster reports absent is a request-level rejection, not a
        // connection failure (Requirement 1.4).
        let err = ClientError::TopicNotFound {
            topic: "orders".to_string(),
        };
        assert!(matches!(classify(err), CtlError::Cluster(_)));
    }

    #[test]
    fn no_partitions_is_a_cluster_error() {
        // A topic that never reported a partition before the discovery timeout is
        // a cluster-side rejection reported non-zero (Requirement 1.8, 8.5).
        let err = ClientError::NoPartitions {
            topic: "orders".to_string(),
        };
        assert!(matches!(classify(err), CtlError::Cluster(_)));
    }

    #[test]
    fn no_leader_after_retries_is_a_cluster_error() {
        // Exhausting the redirect retry budget without reaching a leader is a
        // cluster-side rejection, not a connection failure (Requirement 4.4).
        let err = ClientError::NoLeaderAfterRetries {
            topic: "orders".to_string(),
            partition: 3,
        };
        assert!(matches!(classify(err), CtlError::Cluster(_)));
    }

    #[test]
    fn unknown_node_is_a_connection_error_naming_the_node_and_id_url() {
        // A resolved leader id with no known address is a fail-fast connection
        // error whose message names the unresolved node id and points at the
        // `id=url` endpoint fallback (Requirement 13.4, 13.6).
        let err = ClientError::UnknownNode {
            node: "node7".to_string(),
            topic: "orders".to_string(),
            partition: 2,
        };
        match classify(err) {
            CtlError::Connection(msg) => {
                assert!(
                    msg.contains("node7"),
                    "message should name the unresolved node id: {msg}"
                );
                assert!(
                    msg.contains("id=url"),
                    "message should point at the `id=url` fallback: {msg}"
                );
            }
            other => panic!("expected a connection error, got {other:?}"),
        }
    }

    #[test]
    fn report_maps_outcomes_to_exit_codes() {
        // Smoke-check the mapping is wired; ExitCode is opaque, so we only assert
        // the call is total and does not panic for each variant.
        let _ = report(Ok(()));
        let _ = report(Err(CtlError::Connection("x".into())));
        let _ = report(Err(CtlError::Cluster(ClientError::NoNodes)));
    }
}

#[cfg(test)]
mod example_tests {
    //! End-to-end example tests for the four operator commands and the two
    //! failure modes, driven against an in-process fake `VelaClient` gRPC server
    //! (Requirements 13.1–13.7).
    //!
    //! Each test stands up a [`FakeAdmin`] tonic service on an OS-chosen
    //! localhost port, points a [`Cli`] at it, and runs [`run`]. We assert on the
    //! [`CtlError`] variant [`run`] yields — which is exactly what determines the
    //! process exit status — rather than the opaque [`ExitCode`] from [`report`]:
    //!
    //! - `Ok(())` → exit 0 (Requirement 13.5),
    //! - [`CtlError::Connection`] → non-zero (Requirement 13.6),
    //! - [`CtlError::Cluster`] → non-zero (Requirement 13.7).

    use super::{run, Cli, Command, CtlError};

    use std::sync::{Arc, Mutex};

    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    use vela_client::{LogBackend as ClientBackend, VelaClient};
    use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
    use vela_proto::v1::{
        ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse,
        DeleteTopicRequest, DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse,
        DescribeTopicRequest, DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse,
        ListTopicsRequest, ListTopicsResponse, LogBackend, PartitionInfo, ProduceBatchRequest,
        ProduceBatchResponse, ProduceRequest, ProduceResponse, TopicInfo,
    };

    /// An in-process fake of the client-facing `VelaClient` service.
    ///
    /// In the default (accepting) mode every admin RPC returns a canned success
    /// shaped like a real cluster response, so the CLI's output-formatting paths
    /// run to completion (Requirements 13.1–13.4). In `reject` mode every admin
    /// RPC returns an application error `Status`, standing in for a cluster that
    /// rejects the request (Requirement 13.7).
    ///
    /// `captured_backend` records the `log_backend` carried by the most recent
    /// `CreateTopic` request, so tests can assert exactly which backend the
    /// client sent on the wire (per-topic-log-durability Requirements 1.1, 1.2).
    #[derive(Clone)]
    struct FakeAdmin {
        reject: bool,
        captured_backend: Arc<Mutex<Option<i32>>>,
    }

    impl FakeAdmin {
        /// A canned topic with two partitions — one led, one mid-election — so
        /// `describe` exercises both the leader and `<no leader>` branches.
        fn canned_topic(name: &str) -> TopicInfo {
            TopicInfo {
                name: name.to_string(),
                partition_count: 2,
                partitions: vec![
                    PartitionInfo {
                        index: 0,
                        replicas: vec!["node-0".to_string(), "node-1".to_string()],
                        leader: Some("node-0".to_string()),
                    },
                    PartitionInfo {
                        index: 1,
                        replicas: vec!["node-0".to_string(), "node-1".to_string()],
                        leader: None,
                    },
                ],
                log_backend: LogBackend::Durable as i32,
            }
        }

        /// The canned cluster rejection used in `reject` mode. A non-`Unavailable`
        /// status, so the CLI classifies it as a cluster error (Requirement 13.7)
        /// rather than a connection failure.
        fn rejection() -> Status {
            Status::not_found("no such topic")
        }
    }

    #[tonic::async_trait]
    impl VelaClientService for FakeAdmin {
        async fn create_topic(
            &self,
            request: Request<CreateTopicRequest>,
        ) -> Result<Response<CreateTopicResponse>, Status> {
            if self.reject {
                return Err(Self::rejection());
            }
            let request = request.into_inner();
            // Record the backend the client sent, and echo it back on the topic
            // so the description reflects exactly what was requested.
            *self
                .captured_backend
                .lock()
                .expect("captured-backend mutex poisoned") = Some(request.log_backend);
            let mut topic = Self::canned_topic(&request.name);
            topic.log_backend = request.log_backend;
            Ok(Response::new(CreateTopicResponse { topic: Some(topic) }))
        }

        async fn delete_topic(
            &self,
            _request: Request<DeleteTopicRequest>,
        ) -> Result<Response<DeleteTopicResponse>, Status> {
            if self.reject {
                return Err(Self::rejection());
            }
            Ok(Response::new(DeleteTopicResponse {}))
        }

        async fn list_topics(
            &self,
            _request: Request<ListTopicsRequest>,
        ) -> Result<Response<ListTopicsResponse>, Status> {
            if self.reject {
                return Err(Self::rejection());
            }
            Ok(Response::new(ListTopicsResponse {
                topics: vec![Self::canned_topic("orders"), Self::canned_topic("events")],
            }))
        }

        async fn describe_topic(
            &self,
            request: Request<DescribeTopicRequest>,
        ) -> Result<Response<DescribeTopicResponse>, Status> {
            if self.reject {
                return Err(Self::rejection());
            }
            let name = request.into_inner().name;
            Ok(Response::new(DescribeTopicResponse {
                topic: Some(Self::canned_topic(&name)),
            }))
        }

        async fn find_leader(
            &self,
            _request: Request<FindLeaderRequest>,
        ) -> Result<Response<FindLeaderResponse>, Status> {
            Ok(Response::new(FindLeaderResponse {
                leader: Some("node-0".to_string()),
            }))
        }

        // The admin CLI does not seed its registry from `DescribeCluster`; return
        // an empty membership so the generated service trait is satisfied (the
        // RPC was added to `VelaClient` in task 7.1).
        async fn describe_cluster(
            &self,
            _request: Request<DescribeClusterRequest>,
        ) -> Result<Response<DescribeClusterResponse>, Status> {
            Ok(Response::new(DescribeClusterResponse {
                members: vec![],
                epoch: 0,
            }))
        }

        // Produce/Consume are not exercised by the admin CLI; stub them out.
        async fn produce(
            &self,
            _request: Request<ProduceRequest>,
        ) -> Result<Response<ProduceResponse>, Status> {
            Err(Status::unimplemented("produce is not used by vela-ctl"))
        }

        async fn produce_batch(
            &self,
            _request: Request<ProduceBatchRequest>,
        ) -> Result<Response<ProduceBatchResponse>, Status> {
            Err(Status::unimplemented(
                "produce_batch is not used by vela-ctl",
            ))
        }

        async fn consume(
            &self,
            _request: Request<ConsumeRequest>,
        ) -> Result<Response<ConsumeResponse>, Status> {
            Err(Status::unimplemented("consume is not used by vela-ctl"))
        }
    }

    /// Bind a fake server on an OS-chosen localhost port and start serving it on
    /// a background task. The `TcpListener` is bound *before* we return, so the
    /// returned endpoint URL is already accepting connections — the CLI never
    /// races server startup.
    async fn serve(fake: FakeAdmin) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        let service = VelaClientServer::new(fake);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("fake server serves");
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Serve an accepting-or-rejecting fake whose captured backend is discarded.
    async fn spawn_fake(reject: bool) -> String {
        serve(FakeAdmin {
            reject,
            captured_backend: Arc::new(Mutex::new(None)),
        })
        .await
    }

    /// Serve an accepting fake and return both its endpoint and the handle that
    /// captures the `log_backend` carried by `CreateTopic` requests, so a test
    /// can assert exactly which backend the client sent.
    async fn spawn_capturing() -> (String, Arc<Mutex<Option<i32>>>) {
        let captured = Arc::new(Mutex::new(None));
        let endpoint = serve(FakeAdmin {
            reject: false,
            captured_backend: Arc::clone(&captured),
        })
        .await;
        (endpoint, captured)
    }

    /// Build a [`Cli`] aimed at a single endpoint.
    fn cli(endpoint: String, command: Command) -> Cli {
        Cli {
            endpoints: vec![endpoint],
            metadata_ttl: std::time::Duration::from_secs(30),
            command,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_succeeds_and_exits_zero() {
        // Requirement 13.1 (create) + 13.5 (success → Ok → exit 0).
        let endpoint = spawn_fake(false).await;
        let result = run(cli(
            endpoint,
            Command::Create {
                name: "orders".to_string(),
                partitions: 4,
                backend: ClientBackend::Durable,
            },
        ))
        .await;
        assert!(
            matches!(result, Ok(())),
            "create should succeed: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_sends_the_durable_backend_on_the_wire() {
        // A durable create carries the durable backend value to the server
        // (per-topic-log-durability Requirement 1.1, 1.2).
        let (endpoint, captured) = spawn_capturing().await;
        run(cli(
            endpoint,
            Command::Create {
                name: "orders".to_string(),
                partitions: 1,
                backend: ClientBackend::Durable,
            },
        ))
        .await
        .expect("create should succeed");
        assert_eq!(
            *captured.lock().expect("captured-backend mutex poisoned"),
            Some(LogBackend::Durable as i32),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_sends_the_in_memory_backend_on_the_wire() {
        // A specified in-memory backend is carried as LOG_BACKEND_IN_MEMORY
        // (per-topic-log-durability Requirement 1.1).
        let (endpoint, captured) = spawn_capturing().await;
        run(cli(
            endpoint,
            Command::Create {
                name: "orders".to_string(),
                partitions: 1,
                backend: ClientBackend::InMemory,
            },
        ))
        .await
        .expect("create should succeed");
        assert_eq!(
            *captured.lock().expect("captured-backend mutex poisoned"),
            Some(LogBackend::InMemory as i32),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn describe_reports_the_topic_backend() {
        // `describe_topic` surfaces the topic's backend in usable form
        // (per-topic-log-durability Requirement 1.4).
        let endpoint = spawn_fake(false).await;
        let client = VelaClient::new([("node-0".to_string(), endpoint)]);
        let info = client
            .admin()
            .describe_topic("orders")
            .await
            .expect("describe should succeed");
        assert_eq!(
            ClientBackend::from_wire(info.log_backend),
            Some(ClientBackend::Durable),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_succeeds_and_exits_zero() {
        // Requirement 13.2 (delete) + 13.5.
        let endpoint = spawn_fake(false).await;
        let result = run(cli(
            endpoint,
            Command::Delete {
                name: "orders".to_string(),
            },
        ))
        .await;
        assert!(
            matches!(result, Ok(())),
            "delete should succeed: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_succeeds_and_exits_zero() {
        // Requirement 13.3 (list) + 13.5. The fake returns two topics, so the
        // non-empty list-formatting branch runs.
        let endpoint = spawn_fake(false).await;
        let result = run(cli(endpoint, Command::List)).await;
        assert!(matches!(result, Ok(())), "list should succeed: {result:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn describe_succeeds_and_exits_zero() {
        // Requirement 13.4 (describe) + 13.5. The canned topic has a led and an
        // unled partition, exercising both leader-formatting branches.
        let endpoint = spawn_fake(false).await;
        let result = run(cli(
            endpoint,
            Command::Describe {
                name: "orders".to_string(),
            },
        ))
        .await;
        assert!(
            matches!(result, Ok(())),
            "describe should succeed: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_failure_exits_non_zero() {
        // Requirement 13.6: a node that cannot be reached is a connection error
        // (which `report` maps to a non-zero exit). Reserve a port, then release
        // it so nothing is listening — the dial is refused immediately.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);

        let result = run(cli(format!("http://127.0.0.1:{port}"), Command::List)).await;
        assert!(
            matches!(result, Err(CtlError::Connection(_))),
            "unreachable node should be a connection error: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cluster_rejection_exits_non_zero() {
        // Requirement 13.7: an error the cluster returns is a cluster error
        // (which `report` maps to a non-zero exit), distinct from a connection
        // failure.
        let endpoint = spawn_fake(true).await;
        let result = run(cli(
            endpoint,
            Command::Delete {
                name: "missing".to_string(),
            },
        ))
        .await;
        assert!(
            matches!(result, Err(CtlError::Cluster(_))),
            "a cluster-returned error should be a cluster error: {result:?}"
        );
    }
}
