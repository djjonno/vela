//! The topic-administration client.
//!
//! [`AdminClient`] exposes the whole-topic operations of the `VelaClient`
//! service: create, delete, list, and describe topics (Requirement 13.1–13.4).
//! These are not partition-scoped, so they are sent to a bootstrap node, which
//! serves or forwards them to the metadata group as needed.
//!
//! # Selecting a log backend (per-topic-log-durability Requirement 1)
//!
//! A topic declares its log storage backend at creation time. The client
//! exposes the choice as the typed [`LogBackend`] enum — exactly two values,
//! [`LogBackend::Durable`] (the default) and [`LogBackend::InMemory`] — so an
//! out-of-range value is unrepresentable and rejected by the type system before
//! any request is sent (Requirement 1.3). [`AdminClient::create_topic`] maps the
//! chosen backend onto the `CreateTopicRequest` wire field (Requirement 1.1,
//! 1.2), and [`AdminClient::describe_topic`] reports the backend a topic was
//! created with (Requirement 1.4); [`LogBackend::from_wire`] decodes the wire
//! value into the client enum.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use vela_proto::v1::{
    CreateTopicRequest, DeleteTopicRequest, DescribeClusterRequest, DescribeClusterResponse,
    DescribeTopicRequest, ListTopicsRequest, LogBackend as WireLogBackend, TopicInfo,
};

use crate::core::{AdminRouting, ClientCore};
use crate::error::{ClientError, Result};

/// A topic's log storage backend, as selected through the client API.
///
/// Exactly two values are accepted (Requirement 1.3); [`Durable`](Self::Durable)
/// is the default so a caller that does not choose gets durability
/// (Requirement 1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogBackend {
    /// Persist the partition log to durable storage (the default).
    #[default]
    Durable,
    /// Keep the partition log in volatile memory only.
    InMemory,
}

impl LogBackend {
    /// The proto wire value (`CreateTopicRequest.log_backend`) for this backend.
    ///
    /// Always one of the two concrete `LogBackend` wire values — never the
    /// `UNSPECIFIED` sentinel — so the server records exactly the chosen backend
    /// (Requirement 1.1).
    pub fn to_wire(self) -> i32 {
        match self {
            LogBackend::Durable => WireLogBackend::Durable as i32,
            LogBackend::InMemory => WireLogBackend::InMemory as i32,
        }
    }

    /// Decode a proto wire value into the client backend, or `None` if the value
    /// is the unspecified sentinel or an unrecognized integer.
    ///
    /// Used to surface a described topic's backend in a usable form
    /// (Requirement 1.4).
    pub fn from_wire(value: i32) -> Option<Self> {
        match WireLogBackend::try_from(value) {
            Ok(WireLogBackend::Durable) => Some(LogBackend::Durable),
            Ok(WireLogBackend::InMemory) => Some(LogBackend::InMemory),
            Ok(WireLogBackend::Unspecified) | Err(_) => None,
        }
    }

    /// This backend's canonical lower-case name (`durable` / `in-memory`).
    pub fn as_str(self) -> &'static str {
        match self {
            LogBackend::Durable => "durable",
            LogBackend::InMemory => "in-memory",
        }
    }
}

impl fmt::Display for LogBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LogBackend {
    type Err = ClientError;

    /// Parse `durable` or `in-memory`, rejecting any other value as
    /// [`ClientError::InvalidBackend`] *before* a request is sent
    /// (Requirement 1.3).
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "durable" => Ok(LogBackend::Durable),
            "in-memory" => Ok(LogBackend::InMemory),
            other => Err(ClientError::InvalidBackend {
                value: other.to_string(),
            }),
        }
    }
}

/// Creates, deletes, lists, and describes topics.
#[derive(Debug, Clone)]
pub struct AdminClient {
    core: Arc<ClientCore>,
}

impl AdminClient {
    /// Create an admin client over a shared client core.
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }

    /// Create a topic `name` with `partitions` partitions and the given log
    /// `backend` (Requirement 13.1; per-topic-log-durability Requirement 1.1).
    ///
    /// The `backend` is a typed [`LogBackend`], so only the two valid values are
    /// representable (Requirement 1.3); pass [`LogBackend::Durable`] (its
    /// `Default`) for the durable-by-default behavior (Requirement 1.2). Returns
    /// the created topic's metadata.
    pub async fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        backend: LogBackend,
    ) -> Result<TopicInfo> {
        // A `create` mutates topic metadata, so it routes through the
        // metadata-leader dispatch: send to a configured node, redirect to the
        // hinted Metadata_Leader on `NotLeader`, re-resolve on transport failure
        // (Requirement 4.1–4.4, 12.3).
        let core = Arc::clone(&self.core);
        let response = self
            .core
            .dispatch_admin(
                AdminRouting::Mutating {
                    topic: name.to_string(),
                },
                move |addr| {
                    let core = Arc::clone(&core);
                    let name = name.to_string();
                    async move {
                        let mut client = core.client_for(&addr)?;
                        let response = client
                            .create_topic(CreateTopicRequest {
                                name,
                                partitions,
                                log_backend: backend.to_wire(),
                            })
                            .await?
                            .into_inner();
                        Ok(response)
                    }
                },
            )
            .await?;
        response
            .topic
            .ok_or_else(|| ClientError::MalformedResponse(format!("CreateTopic({name})")))
    }

    /// Delete the topic `name` (Requirement 13.2).
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        // A `delete` mutates topic metadata, so it routes through the same
        // metadata-leader dispatch as `create` (Requirement 4.1–4.4, 12.3).
        let core = Arc::clone(&self.core);
        self.core
            .dispatch_admin(
                AdminRouting::Mutating {
                    topic: name.to_string(),
                },
                move |addr| {
                    let core = Arc::clone(&core);
                    let name = name.to_string();
                    async move {
                        let mut client = core.client_for(&addr)?;
                        client.delete_topic(DeleteTopicRequest { name }).await?;
                        Ok(())
                    }
                },
            )
            .await
    }

    /// List all topics known to cluster metadata, with their partition counts
    /// (Requirement 13.3).
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        // `list` is read-only: any configured node can serve or forward it, so
        // it retries only on a transport failure and is never redirected on a
        // `NotLeader` response (Requirement 4.5, 4.6).
        let core = Arc::clone(&self.core);
        self.core
            .dispatch_admin(AdminRouting::ReadOnly, move |addr| {
                let core = Arc::clone(&core);
                async move {
                    let mut client = core.client_for(&addr)?;
                    let response = client.list_topics(ListTopicsRequest {}).await?.into_inner();
                    Ok(response.topics)
                }
            })
            .await
    }

    /// Describe a single topic's partitions, current leaders, and log backend
    /// (Requirement 13.4; per-topic-log-durability Requirement 1.4).
    ///
    /// The returned [`TopicInfo`] carries the topic's backend on its
    /// `log_backend` field; decode it with [`LogBackend::from_wire`].
    pub async fn describe_topic(&self, name: &str) -> Result<TopicInfo> {
        // `describe` is read-only: transport-only retry, no `NotLeader`
        // redirection (Requirement 4.5, 4.6).
        let core = Arc::clone(&self.core);
        let response = self
            .core
            .dispatch_admin(AdminRouting::ReadOnly, move |addr| {
                let core = Arc::clone(&core);
                let name = name.to_string();
                async move {
                    let mut client = core.client_for(&addr)?;
                    let response = client
                        .describe_topic(DescribeTopicRequest { name })
                        .await?
                        .into_inner();
                    Ok(response)
                }
            })
            .await?;
        response
            .topic
            .ok_or_else(|| ClientError::MalformedResponse(format!("DescribeTopic({name})")))
    }

    /// Describe the current cluster membership: each known member's node id,
    /// transport address, and availability, plus the metadata epoch the view was
    /// observed at (Requirement 12.6, 12.7, 12.8).
    ///
    /// This is the `Member_Address_Map` a programmatic client seeds its node
    /// registry from to resolve a leader node id to a dialable address. Like
    /// `list`/`describe`, it is read-only: any configured node can serve or
    /// forward it, so it retries only on a transport failure and is never
    /// redirected on a `NotLeader` response (Requirement 4.5, 4.6).
    pub async fn describe_cluster(&self) -> Result<DescribeClusterResponse> {
        let core = Arc::clone(&self.core);
        self.core
            .dispatch_admin(AdminRouting::ReadOnly, move |addr| {
                let core = Arc::clone(&core);
                async move {
                    let mut client = core.client_for(&addr)?;
                    let response = client
                        .describe_cluster(DescribeClusterRequest {})
                        .await?
                        .into_inner();
                    Ok(response)
                }
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_backend_is_durable() {
        // A caller that does not choose a backend gets durability by default
        // (Requirement 1.2).
        assert_eq!(LogBackend::default(), LogBackend::Durable);
    }

    #[test]
    fn backend_maps_to_the_concrete_wire_value() {
        // The chosen backend always maps to a concrete wire value, never the
        // unspecified sentinel (Requirement 1.1).
        assert_eq!(
            LogBackend::Durable.to_wire(),
            WireLogBackend::Durable as i32
        );
        assert_eq!(
            LogBackend::InMemory.to_wire(),
            WireLogBackend::InMemory as i32
        );
        assert_ne!(
            LogBackend::Durable.to_wire(),
            WireLogBackend::Unspecified as i32
        );
    }

    #[test]
    fn from_wire_decodes_known_backends_only() {
        // The two concrete backends decode; the unspecified sentinel and any
        // unrecognized integer do not (Requirement 1.4).
        assert_eq!(
            LogBackend::from_wire(WireLogBackend::Durable as i32),
            Some(LogBackend::Durable)
        );
        assert_eq!(
            LogBackend::from_wire(WireLogBackend::InMemory as i32),
            Some(LogBackend::InMemory)
        );
        assert_eq!(
            LogBackend::from_wire(WireLogBackend::Unspecified as i32),
            None
        );
        assert_eq!(LogBackend::from_wire(99), None);
    }

    #[test]
    fn parsing_accepts_exactly_the_two_backends() {
        assert_eq!(
            "durable".parse::<LogBackend>().unwrap(),
            LogBackend::Durable
        );
        assert_eq!(
            "in-memory".parse::<LogBackend>().unwrap(),
            LogBackend::InMemory
        );
    }

    #[test]
    fn parsing_rejects_an_invalid_backend_before_sending() {
        // An unrecognized value is rejected at parse time — before any
        // `CreateTopic` request is built or sent (Requirement 1.3).
        let err = "bogus".parse::<LogBackend>().unwrap_err();
        assert!(
            matches!(err, ClientError::InvalidBackend { ref value } if value == "bogus"),
            "expected InvalidBackend, got {err:?}"
        );
    }

    #[test]
    fn display_round_trips_through_parse() {
        for backend in [LogBackend::Durable, LogBackend::InMemory] {
            assert_eq!(backend.to_string().parse::<LogBackend>().unwrap(), backend);
        }
    }
}

#[cfg(test)]
mod routing_tests {
    //! Admin routing tests (task 5.2).
    //!
    //! These drive the real [`AdminClient`] methods against in-process fake
    //! `VelaClient` servers (the harness established by `consumer.rs`'s tests and
    //! `tests/prop_dispatch_reresolution.rs`), proving the two admin routing
    //! policies [`AdminClient`] dispatches through [`ClientCore::dispatch_admin`]:
    //!
    //! - **Mutating** (`create`/`delete`): a `NotLeader` response redirects to the
    //!   hinted metadata leader, which then serves the request (Requirement 4.1,
    //!   12.3); a transport failure re-resolves to another configured node
    //!   (Requirement 4.2).
    //! - **Read-only** (`list`/`describe`): a `NotLeader` response is **never**
    //!   redirected and surfaces as an error (Requirement 4.5), but a transport
    //!   failure **is** retried against another configured node (Requirement 4.6).
    //!
    //! The client is built with [`ClientCore::with_clock`] and an instant
    //! [`VirtualClock`] so the retry backoff (Requirement 4.3) costs no real
    //! wall-clock time while the fake gRPC servers run on a multi-thread runtime.

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use prost::Message as _;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};
    use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
    use vela_proto::v1::{
        self, ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse,
        DeleteTopicRequest, DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse,
        DescribeTopicRequest, DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse,
        ListTopicsRequest, ListTopicsResponse, ProduceRequest, ProduceResponse, TopicInfo,
    };

    use super::{AdminClient, LogBackend};
    use crate::core::{ClientCore, Clock};
    use crate::error::ClientError;

    /// A clock whose `sleep` advances virtual time instantly, so the admin
    /// dispatch backoff (Requirement 4.3) costs no real wall-clock time. Mirrors
    /// the `VirtualClock` in `tests/prop_dispatch_reresolution.rs`: `now()`
    /// advances by exactly the slept duration so the [`RetryBudget`] elapsed-time
    /// bound still progresses, while the returned future is ready immediately.
    ///
    /// [`RetryBudget`]: crate::RetryBudget
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

    /// How a fake node answers whichever admin RPC it receives.
    #[derive(Clone)]
    enum AdminScript {
        /// Serve the request successfully — a node that owns (or forwards to) the
        /// metadata leader.
        Succeed,
        /// Reject with a `NotLeader` redirect carrying `hint`, shaped exactly as
        /// the server emits it (a typed `VelaError` in the status details).
        NotLeader { hint: Option<String> },
        /// Reject with a transport `Unavailable`, standing in for an unreachable
        /// or failing node.
        Unavailable,
    }

    /// An in-process fake of the client-facing `VelaClient` service representing
    /// one node, answering every topic-admin RPC per `script` and counting how
    /// many admin RPCs reached it so a test can assert which node served a
    /// request. The non-admin RPCs are unused by these tests.
    #[derive(Clone)]
    struct FakeAdminNode {
        script: AdminScript,
        calls: Arc<AtomicU32>,
    }

    impl FakeAdminNode {
        fn new(script: AdminScript) -> Self {
            Self {
                script,
                calls: Arc::new(AtomicU32::new(0)),
            }
        }

        /// The number of admin RPCs this node has served.
        fn calls(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }

        /// Record an admin RPC and decide the scripted error, if any. `Ok(())`
        /// means the caller should build a success response.
        // Test-only helper mirroring the tonic service signature; boxing the
        // `Status` here would force `?`-conversion churn at every call site in
        // the fake service for no production benefit.
        #[allow(clippy::result_large_err)]
        fn record(&self) -> Result<(), Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &self.script {
                AdminScript::Succeed => Ok(()),
                AdminScript::NotLeader { hint } => Err(not_leader_status(hint.as_deref())),
                AdminScript::Unavailable => Err(Status::unavailable("connection refused")),
            }
        }
    }

    /// A `TopicInfo` payload for a successful create/describe response.
    fn topic_info(name: &str) -> TopicInfo {
        TopicInfo {
            name: name.to_string(),
            partition_count: 1,
            ..Default::default()
        }
    }

    /// A `NotLeader` redirect status shaped exactly as the server emits it: a
    /// typed [`v1::VelaError`] (code `NotLeader`, optional leader hint) encoded
    /// into the status details, so the client's `classify`/`not_leader_hint`
    /// decode it identically.
    fn not_leader_status(hint: Option<&str>) -> Status {
        let vela_error = v1::VelaError {
            code: v1::ErrorCode::NotLeader as i32,
            message: "not the metadata leader".to_string(),
            leader: hint.map(str::to_string),
        };
        let details = prost::bytes::Bytes::from(vela_error.encode_to_vec());
        Status::with_details(tonic::Code::FailedPrecondition, "not leader", details)
    }

    #[tonic::async_trait]
    impl VelaClientService for FakeAdminNode {
        async fn create_topic(
            &self,
            _request: Request<CreateTopicRequest>,
        ) -> Result<Response<CreateTopicResponse>, Status> {
            self.record()?;
            Ok(Response::new(CreateTopicResponse {
                topic: Some(topic_info("orders")),
            }))
        }

        async fn delete_topic(
            &self,
            _request: Request<DeleteTopicRequest>,
        ) -> Result<Response<DeleteTopicResponse>, Status> {
            self.record()?;
            Ok(Response::new(DeleteTopicResponse {}))
        }

        async fn list_topics(
            &self,
            _request: Request<ListTopicsRequest>,
        ) -> Result<Response<ListTopicsResponse>, Status> {
            self.record()?;
            Ok(Response::new(ListTopicsResponse {
                topics: vec![topic_info("orders")],
            }))
        }

        async fn describe_topic(
            &self,
            _request: Request<DescribeTopicRequest>,
        ) -> Result<Response<DescribeTopicResponse>, Status> {
            self.record()?;
            Ok(Response::new(DescribeTopicResponse {
                topic: Some(topic_info("orders")),
            }))
        }

        // The remaining client RPCs are not exercised by these admin tests.
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

        async fn find_leader(
            &self,
            _request: Request<FindLeaderRequest>,
        ) -> Result<Response<FindLeaderResponse>, Status> {
            Err(Status::unimplemented("find_leader is not exercised here"))
        }

        async fn describe_cluster(
            &self,
            _request: Request<DescribeClusterRequest>,
        ) -> Result<Response<DescribeClusterResponse>, Status> {
            Err(Status::unimplemented(
                "describe_cluster is not exercised here",
            ))
        }
    }

    /// Bind a fake node on an OS-chosen localhost port and serve it on a
    /// background task. The listener is bound before returning, so the endpoint
    /// is already accepting connections — no startup race.
    async fn serve(node: FakeAdminNode) -> String {
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

    /// Build an admin client over two fake nodes (node-a first, so it is the
    /// bootstrap target the first attempt is directed at) and an instant
    /// [`VirtualClock`].
    fn admin_over(node_a_addr: &str, node_b_addr: &str) -> AdminClient {
        let core = ClientCore::with_clock(
            [
                ("node-a".to_string(), node_a_addr.to_string()),
                ("node-b".to_string(), node_b_addr.to_string()),
            ],
            Arc::new(VirtualClock::new()),
        );
        AdminClient::new(Arc::new(core))
    }

    /// A `NotLeader` response to `create` redirects to the hinted metadata leader
    /// (`node-b`), which then serves the create (Requirement 4.1, 12.3).
    #[tokio::test(flavor = "multi_thread")]
    async fn create_redirects_to_metadata_leader_then_succeeds() {
        let node_a = FakeAdminNode::new(AdminScript::NotLeader {
            hint: Some("node-b".to_string()),
        });
        let node_b = FakeAdminNode::new(AdminScript::Succeed);
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let admin = admin_over(&a_addr, &b_addr);
        let topic = admin
            .create_topic("orders", 1, LogBackend::Durable)
            .await
            .expect("the redirect is followed and the metadata leader serves the create");

        assert_eq!(topic.name, "orders");
        assert_eq!(
            node_a.calls(),
            1,
            "the first (non-leader) node was tried once"
        );
        assert_eq!(
            node_b.calls(),
            1,
            "the create was redirected to and served by the hinted metadata leader",
        );
    }

    /// A `NotLeader` response to `delete` redirects to the hinted metadata leader
    /// (`node-b`), which then serves the delete (Requirement 4.1, 12.3).
    #[tokio::test(flavor = "multi_thread")]
    async fn delete_redirects_to_metadata_leader_then_succeeds() {
        let node_a = FakeAdminNode::new(AdminScript::NotLeader {
            hint: Some("node-b".to_string()),
        });
        let node_b = FakeAdminNode::new(AdminScript::Succeed);
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let admin = admin_over(&a_addr, &b_addr);
        admin
            .delete_topic("orders")
            .await
            .expect("the redirect is followed and the metadata leader serves the delete");

        assert_eq!(
            node_a.calls(),
            1,
            "the first (non-leader) node was tried once"
        );
        assert_eq!(
            node_b.calls(),
            1,
            "the delete was redirected to and served by the hinted metadata leader",
        );
    }

    /// A transport failure on `create` re-resolves to another configured node,
    /// which serves the create (Requirement 4.2).
    #[tokio::test(flavor = "multi_thread")]
    async fn create_transport_failure_reresolves_to_another_node() {
        let node_a = FakeAdminNode::new(AdminScript::Unavailable);
        let node_b = FakeAdminNode::new(AdminScript::Succeed);
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let admin = admin_over(&a_addr, &b_addr);
        let topic = admin
            .create_topic("orders", 1, LogBackend::Durable)
            .await
            .expect("a transport failure re-resolves to another node that serves the create");

        assert_eq!(topic.name, "orders");
        assert_eq!(
            node_a.calls(),
            1,
            "the first node was tried once before re-resolving",
        );
        assert_eq!(
            node_b.calls(),
            1,
            "the create was re-resolved to and served by the next configured node",
        );
    }

    /// A read-only `list` is **never** redirected on a `NotLeader` response: it
    /// surfaces the error rather than following the hint (Requirement 4.5).
    #[tokio::test(flavor = "multi_thread")]
    async fn list_does_not_redirect_on_not_leader() {
        let node_a = FakeAdminNode::new(AdminScript::NotLeader {
            hint: Some("node-b".to_string()),
        });
        let node_b = FakeAdminNode::new(AdminScript::Succeed);
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let admin = admin_over(&a_addr, &b_addr);
        let err = admin
            .list_topics()
            .await
            .expect_err("a read-only request is not redirected on NotLeader");

        assert!(
            matches!(err, ClientError::Rpc(_)),
            "the NotLeader response surfaces unchanged, got {err:?}",
        );
        assert_eq!(
            node_a.calls(),
            1,
            "the contacted node was tried exactly once"
        );
        assert_eq!(
            node_b.calls(),
            0,
            "the hinted leader is never contacted for a read-only request (Req 4.5)",
        );
    }

    /// A read-only `describe` is **never** redirected on a `NotLeader` response
    /// either: it surfaces the error rather than following the hint
    /// (Requirement 4.5).
    #[tokio::test(flavor = "multi_thread")]
    async fn describe_does_not_redirect_on_not_leader() {
        let node_a = FakeAdminNode::new(AdminScript::NotLeader {
            hint: Some("node-b".to_string()),
        });
        let node_b = FakeAdminNode::new(AdminScript::Succeed);
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let admin = admin_over(&a_addr, &b_addr);
        let err = admin
            .describe_topic("orders")
            .await
            .expect_err("a read-only request is not redirected on NotLeader");

        assert!(
            matches!(err, ClientError::Rpc(_)),
            "the NotLeader response surfaces unchanged, got {err:?}",
        );
        assert_eq!(
            node_a.calls(),
            1,
            "the contacted node was tried exactly once"
        );
        assert_eq!(
            node_b.calls(),
            0,
            "the hinted leader is never contacted for a read-only request (Req 4.5)",
        );
    }

    /// A read-only `list` **is** retried against another configured node on a
    /// transport failure, which then serves it (Requirement 4.6).
    #[tokio::test(flavor = "multi_thread")]
    async fn list_retries_on_transport_failure() {
        let node_a = FakeAdminNode::new(AdminScript::Unavailable);
        let node_b = FakeAdminNode::new(AdminScript::Succeed);
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let admin = admin_over(&a_addr, &b_addr);
        let topics = admin
            .list_topics()
            .await
            .expect("a transport failure re-resolves to another node that serves the list");

        assert_eq!(topics.len(), 1);
        assert_eq!(
            node_a.calls(),
            1,
            "the first node was tried once before re-resolving",
        );
        assert_eq!(
            node_b.calls(),
            1,
            "the list was re-resolved to and served by the next configured node (Req 4.6)",
        );
    }

    /// A read-only `describe` **is** retried against another configured node on a
    /// transport failure, which then serves it (Requirement 4.6).
    #[tokio::test(flavor = "multi_thread")]
    async fn describe_retries_on_transport_failure() {
        let node_a = FakeAdminNode::new(AdminScript::Unavailable);
        let node_b = FakeAdminNode::new(AdminScript::Succeed);
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let admin = admin_over(&a_addr, &b_addr);
        let topic = admin
            .describe_topic("orders")
            .await
            .expect("a transport failure re-resolves to another node that serves the describe");

        assert_eq!(topic.name, "orders");
        assert_eq!(
            node_a.calls(),
            1,
            "the first node was tried once before re-resolving",
        );
        assert_eq!(
            node_b.calls(),
            1,
            "the describe was re-resolved to and served by the next configured node (Req 4.6)",
        );
    }
}
