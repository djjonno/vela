//! The `vela-ctl` command-line surface.
//!
//! Defines the [`clap`] argument model ([`Cli`] + [`Command`]) and the
//! [`run`] dispatcher that drives the [`vela_client`] admin API. The four
//! operator commands map one-to-one onto [`AdminClient`] calls:
//!
//! - `create <name> --partitions N` → [`create_topic`] (Requirement 13.1)
//! - `delete <name>` → [`delete_topic`] (Requirement 13.2)
//! - `list` → [`list_topics`] (Requirement 13.3)
//! - `describe <name>` → [`describe_topic`] (Requirement 13.4)
//!
//! Every call is wrapped in a 5 s [`tokio::time::timeout`]: if the cluster cannot
//! be reached in time the command reports a connection error and exits non-zero
//! (Requirement 13.6); any error the cluster returns is likewise reported with a
//! non-zero exit (Requirement 13.7). A command that completes returns
//! `Ok(())`, which the caller turns into exit status zero (Requirement 13.5).
//!
//! [`AdminClient`]: vela_client::AdminClient
//! [`create_topic`]: vela_client::AdminClient::create_topic
//! [`delete_topic`]: vela_client::AdminClient::delete_topic
//! [`list_topics`]: vela_client::AdminClient::list_topics
//! [`describe_topic`]: vela_client::AdminClient::describe_topic

use std::future::Future;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use vela_client::{ClientError, VelaClient};

/// Default endpoint contacted when `--endpoints`/`VELA_ADDR` is not supplied.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:50051";

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
    /// calls to whichever it can reach.
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
/// command, printing a human-readable outcome on success.
pub async fn run(cli: Cli) -> Result<(), CtlError> {
    let client = VelaClient::new(derive_nodes(&cli.endpoints));
    let admin = client.admin();

    match cli.command {
        Command::Create { name, partitions } => {
            let topic = with_timeout(admin.create_topic(&name, partitions)).await?;
            println!(
                "created topic '{}' with {} partition(s)",
                topic.name, topic.partition_count
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
                "topic '{}' ({} partition(s))",
                topic.name, topic.partition_count
            );
            for partition in topic.partitions {
                let leader = partition.leader.as_deref().unwrap_or("<no leader>");
                println!("  partition {} leader {}", partition.index, leader);
            }
        }
    }
    Ok(())
}

/// Pair each endpoint address with a synthetic node id.
///
/// [`VelaClient::new`] takes `(node_id, address)` bootstrap pairs, but an
/// operator only supplies addresses. The ids are arbitrary handles the client
/// uses internally, so we derive stable `node-{i}` labels positionally.
fn derive_nodes(endpoints: &[String]) -> Vec<(String, String)> {
    endpoints
        .iter()
        .enumerate()
        .map(|(i, addr)| (format!("node-{i}"), addr.clone()))
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
/// A transport-level `Unavailable` status means the client never reached a node
/// (e.g. connection refused), so it is reported as a connection error
/// (Requirement 13.6). Every other error is a request the cluster — or the
/// client's routing layer — rejected (Requirement 13.7).
fn classify(err: ClientError) -> CtlError {
    if let ClientError::Rpc(status) = &err {
        if status.code() == tonic::Code::Unavailable {
            return CtlError::Connection(status.message().to_string());
        }
    }
    CtlError::Cluster(err)
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
            Command::Create { name, partitions } => {
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

    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    use vela_proto::v1::vela_client_server::{VelaClient, VelaClientServer};
    use vela_proto::v1::{
        ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse,
        DeleteTopicRequest, DeleteTopicResponse, DescribeTopicRequest, DescribeTopicResponse,
        FindLeaderRequest, FindLeaderResponse, ListTopicsRequest, ListTopicsResponse,
        PartitionInfo, ProduceRequest, ProduceResponse, TopicInfo,
    };

    /// An in-process fake of the client-facing `VelaClient` service.
    ///
    /// In the default (accepting) mode every admin RPC returns a canned success
    /// shaped like a real cluster response, so the CLI's output-formatting paths
    /// run to completion (Requirements 13.1–13.4). In `reject` mode every admin
    /// RPC returns an application error `Status`, standing in for a cluster that
    /// rejects the request (Requirement 13.7).
    #[derive(Clone)]
    struct FakeAdmin {
        reject: bool,
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
    impl VelaClient for FakeAdmin {
        async fn create_topic(
            &self,
            request: Request<CreateTopicRequest>,
        ) -> Result<Response<CreateTopicResponse>, Status> {
            if self.reject {
                return Err(Self::rejection());
            }
            let name = request.into_inner().name;
            Ok(Response::new(CreateTopicResponse {
                topic: Some(Self::canned_topic(&name)),
            }))
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

        // Produce/Consume are not exercised by the admin CLI; stub them out.
        async fn produce(
            &self,
            _request: Request<ProduceRequest>,
        ) -> Result<Response<ProduceResponse>, Status> {
            Err(Status::unimplemented("produce is not used by vela-ctl"))
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
    async fn spawn_fake(reject: bool) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        let service = VelaClientServer::new(FakeAdmin { reject });
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("fake server serves");
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Build a [`Cli`] aimed at a single endpoint.
    fn cli(endpoint: String, command: Command) -> Cli {
        Cli {
            endpoints: vec![endpoint],
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
            },
        ))
        .await;
        assert!(
            matches!(result, Ok(())),
            "create should succeed: {result:?}"
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
