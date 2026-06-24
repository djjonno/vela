//! The consume client.
//!
//! [`Consumer::consume`] reads committed records from an explicit
//! `(topic, partition)`, directing the request to that partition's believed
//! leader (Requirement 3.1). Unlike produce, consume targets a caller-chosen
//! partition, so there is no routing step — only leader resolution.
//!
//! Consume flows through the generalized [`ClientCore::dispatch`] engine, so it
//! inherits the full routing/retry behavior for free: a `NotLeader` redirect
//! re-resolves the leader (from the hint or via `FindLeader`) and retries
//! (Requirement 3.2, 10.1); a transport/connection failure invalidates the
//! cached leader, re-resolves via `FindLeader`, and retries (Requirement 3.3,
//! 10.2); a stale-routing error refreshes the topic's metadata before retrying
//! (Requirement 1.6). A partition with no elected leader surfaces as
//! [`ClientError::NoLeader`] — a partition-unavailable result kept distinct from
//! a transport failure (Requirement 2.3, 10.3) so a continuous consumer can
//! wait and re-attempt resolution rather than treating it as a dead connection.
//!
//! [`ClientCore::dispatch`]: crate::ClientCore::dispatch
//! [`ClientError::NoLeader`]: crate::ClientError::NoLeader

use std::sync::Arc;

use vela_proto::v1::{ConsumeRequest, ConsumedRecord};

use crate::core::ClientCore;
use crate::error::Result;

/// Reads committed records from topic partitions via their leaders.
#[derive(Debug, Clone)]
pub struct Consumer {
    core: Arc<ClientCore>,
}

impl Consumer {
    /// Create a consumer over a shared client core.
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }

    /// Consume up to `max` committed records from `(topic, partition)` starting
    /// at `offset`.
    ///
    /// The request is sent to the partition's believed leader through
    /// [`ClientCore::dispatch`], which transparently re-resolves and retries on a
    /// `NotLeader` redirect or a transport failure and refreshes stale metadata,
    /// all within a time-bounded retry budget (Requirement 3.1–3.3, 10.1, 10.2).
    /// A partition with no elected leader surfaces as [`ClientError::NoLeader`],
    /// distinct from a transport failure (Requirement 2.3, 10.3). `max` is `None`
    /// to accept the server default (500), or a bound in `1..=10000`. Returns the
    /// committed records in ascending offset order together with the next offset
    /// to request.
    ///
    /// [`ClientCore::dispatch`]: crate::ClientCore::dispatch
    /// [`ClientError::NoLeader`]: crate::ClientError::NoLeader
    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max: Option<u32>,
    ) -> Result<ConsumeOutcome> {
        // Captured by the per-attempt closure; cloned on each attempt so a
        // redirect can re-send the identical request to the new leader.
        let core = Arc::clone(&self.core);
        let topic_owned = topic.to_string();

        self.core
            .dispatch(topic, partition, move |addr| {
                let core = Arc::clone(&core);
                let topic = topic_owned.clone();
                async move {
                    let mut client = core.client_for(&addr)?;
                    let response = client
                        .consume(ConsumeRequest {
                            topic,
                            partition,
                            offset,
                            max_count: max,
                        })
                        .await?
                        .into_inner();
                    Ok(ConsumeOutcome {
                        records: response.records,
                        next_offset: response.next_offset,
                    })
                }
            })
            .await
    }
}

/// The result of a [`Consumer::consume`] call: the committed records returned
/// and the next offset the consumer should request to continue.
#[derive(Debug, Clone)]
pub struct ConsumeOutcome {
    /// Committed records in ascending offset order.
    pub records: Vec<ConsumedRecord>,
    /// The offset to request next to continue reading.
    pub next_offset: u64,
}

#[cfg(test)]
mod tests {
    //! Consumer routing tests (task 4.3).
    //!
    //! These drive the real [`Consumer::consume`] against an in-process fake
    //! `VelaClient` server (the established harness from `vela-ctl`'s example
    //! tests and `tests/leader_directed.rs`), scripting per-node `consume` and
    //! `find_leader` answers and counting attempts with an `AtomicU32`. They
    //! prove the three behaviors consume inherits by flowing through
    //! [`ClientCore::dispatch`]:
    //!
    //! - a `NotLeader` redirect re-resolves and the next attempt targets the new
    //!   leader (Requirement 3.2, 10.1);
    //! - a partition with no elected leader surfaces as
    //!   [`ClientError::NoLeader`], distinct from a transport failure
    //!   (Requirement 2.3, 10.3);
    //! - a transport failure invalidates the cached leader, re-resolves via
    //!   `FindLeader`, and the retry targets the re-resolved leader
    //!   (Requirement 3.3, 10.1, 10.2).

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use prost::Message as _;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};
    use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
    use vela_proto::v1::{
        self, ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse,
        DeleteTopicRequest, DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse,
        DescribeTopicRequest, DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse,
        ListTopicsRequest, ListTopicsResponse, ProduceBatchRequest, ProduceBatchResponse,
        ProduceRequest, ProduceResponse,
    };

    use super::Consumer;
    use crate::core::ClientCore;
    use crate::error::ClientError;

    /// How a fake node answers a `consume` RPC.
    #[derive(Clone)]
    enum ConsumeScript {
        /// Serve an (empty) batch and report `next_offset` — a healthy leader.
        Succeed { next_offset: u64 },
        /// Reject with a `NotLeader` redirect carrying `hint`, as the server
        /// emits it (a typed `VelaError` in the status details).
        NotLeader { hint: Option<String> },
        /// Reject with a transport `Unavailable`, standing in for an unreachable
        /// or failing leader.
        Unavailable,
    }

    /// An in-process fake of the client-facing `VelaClient` service representing
    /// one node. `find_leader` reports `find_leader` (a node id, or `None` for
    /// "no elected leader"); `consume` follows `script`; `consume_calls` counts
    /// how many `consume` RPCs reached this node so a test can assert which node
    /// an attempt was directed to.
    #[derive(Clone)]
    struct FakeNode {
        find_leader: Option<String>,
        script: ConsumeScript,
        consume_calls: Arc<AtomicU32>,
    }

    impl FakeNode {
        fn new(find_leader: Option<&str>, script: ConsumeScript) -> Self {
            Self {
                find_leader: find_leader.map(str::to_string),
                script,
                consume_calls: Arc::new(AtomicU32::new(0)),
            }
        }

        /// The number of `consume` RPCs this node has served.
        fn consume_calls(&self) -> u32 {
            self.consume_calls.load(Ordering::SeqCst)
        }
    }

    /// A `NotLeader` redirect status shaped exactly as the server emits it: a
    /// typed [`v1::VelaError`] (code `NotLeader`, optional leader hint) encoded
    /// into the status details, so the client's `classify`/`not_leader_hint`
    /// decode it identically.
    fn not_leader_status(hint: Option<&str>) -> Status {
        let vela_error = v1::VelaError {
            code: v1::ErrorCode::NotLeader as i32,
            message: "not the leader for this partition".to_string(),
            leader: hint.map(str::to_string),
        };
        let details = prost::bytes::Bytes::from(vela_error.encode_to_vec());
        Status::with_details(tonic::Code::FailedPrecondition, "not leader", details)
    }

    #[tonic::async_trait]
    impl VelaClientService for FakeNode {
        async fn consume(
            &self,
            _request: Request<ConsumeRequest>,
        ) -> Result<Response<ConsumeResponse>, Status> {
            self.consume_calls.fetch_add(1, Ordering::SeqCst);
            match &self.script {
                ConsumeScript::Succeed { next_offset } => Ok(Response::new(ConsumeResponse {
                    records: vec![],
                    next_offset: *next_offset,
                })),
                ConsumeScript::NotLeader { hint } => Err(not_leader_status(hint.as_deref())),
                ConsumeScript::Unavailable => Err(Status::unavailable("connection refused")),
            }
        }

        async fn find_leader(
            &self,
            _request: Request<FindLeaderRequest>,
        ) -> Result<Response<FindLeaderResponse>, Status> {
            Ok(Response::new(FindLeaderResponse {
                leader: self.find_leader.clone(),
            }))
        }

        // The remaining client RPCs are not exercised by these consume tests.
        async fn produce(
            &self,
            _request: Request<ProduceRequest>,
        ) -> Result<Response<ProduceResponse>, Status> {
            Err(Status::unimplemented("produce is not exercised here"))
        }

        async fn produce_batch(
            &self,
            _request: Request<ProduceBatchRequest>,
        ) -> Result<Response<ProduceBatchResponse>, Status> {
            Err(Status::unimplemented("produce_batch is not exercised here"))
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
    async fn serve(node: FakeNode) -> String {
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

    /// A `NotLeader` redirect on consume re-resolves the leader from the hint and
    /// the next attempt is directed to the new leader (Requirement 3.2, 10.1).
    #[tokio::test(flavor = "multi_thread")]
    async fn consume_redirect_targets_the_reresolved_leader() {
        // node-a believes it leads but redirects to node-b; node-b serves.
        let node_a = FakeNode::new(
            Some("node-a"),
            ConsumeScript::NotLeader {
                hint: Some("node-b".to_string()),
            },
        );
        let node_b = FakeNode::new(Some("node-b"), ConsumeScript::Succeed { next_offset: 5 });
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let core = Arc::new(ClientCore::new([
            ("node-a".to_string(), a_addr.clone()),
            ("node-b".to_string(), b_addr.clone()),
        ]));
        // Believe node-a leads orders/0, so the first attempt lands on the stale
        // leader and must be redirected.
        core.leaders().insert("orders", 0, a_addr.as_str());
        let consumer = Consumer::new(Arc::clone(&core));

        let outcome = consumer
            .consume("orders", 0, 0, None)
            .await
            .expect("redirect is followed and the new leader serves the read");

        assert_eq!(outcome.next_offset, 5);
        assert_eq!(node_a.consume_calls(), 1, "the stale leader was tried once");
        assert_eq!(
            node_b.consume_calls(),
            1,
            "the retry was directed to the re-resolved leader (Req 3.2, 10.1)",
        );
        // The cache now points at the redirect target, resolved via the registry.
        assert_eq!(
            core.leaders().get("orders", 0).as_deref(),
            Some(b_addr.as_str()),
        );
    }

    /// When every reachable node reports no elected leader, consume surfaces a
    /// partition-unavailable [`ClientError::NoLeader`] — never a transport
    /// failure (Requirement 2.3, 10.3).
    #[tokio::test(flavor = "multi_thread")]
    async fn consume_partition_unavailable_is_distinct_from_transport() {
        // The node is reachable and knows the partition but reports no leader.
        let node = FakeNode::new(None, ConsumeScript::Succeed { next_offset: 0 });
        let addr = serve(node.clone()).await;

        let core = Arc::new(ClientCore::new([("node-a".to_string(), addr)]));
        let consumer = Consumer::new(Arc::clone(&core));

        let err = consumer
            .consume("orders", 0, 0, None)
            .await
            .expect_err("no elected leader yields an error");

        assert!(
            matches!(err, ClientError::NoLeader { ref topic, partition: 0 } if topic == "orders"),
            "expected a partition-unavailable NoLeader, got {err:?}",
        );
        // The distinction the continuous consumer relies on: this is *not* a
        // transport failure (Requirement 2.3, 10.3).
        assert!(
            !matches!(err, ClientError::Rpc(_)),
            "partition-unavailable must be distinct from a transport failure",
        );
        // No read was attempted: resolution failed before any consume RPC.
        assert_eq!(node.consume_calls(), 0);
    }

    /// The contrast to the above: a leader that cannot be reached surfaces a
    /// transport failure, not a partition-unavailable result — confirming the
    /// two outcomes are distinct error variants (Requirement 2.3).
    #[tokio::test(flavor = "multi_thread")]
    async fn consume_transport_failure_is_distinct_from_partition_unavailable() {
        // No server is listening on this port, so leader resolution fails at the
        // transport layer.
        let core = Arc::new(ClientCore::new([(
            "node-a".to_string(),
            "http://127.0.0.1:1".to_string(),
        )]));
        let consumer = Consumer::new(Arc::clone(&core));

        let err = consumer
            .consume("orders", 0, 0, None)
            .await
            .expect_err("an unreachable cluster yields an error");

        assert!(
            matches!(err, ClientError::Rpc(_)),
            "expected a transport failure, got {err:?}",
        );
        assert!(
            !matches!(err, ClientError::NoLeader { .. }),
            "a transport failure must be distinct from partition-unavailable",
        );
    }

    /// A transport failure to the believed leader invalidates the cached leader,
    /// re-resolves via `FindLeader`, and the retry targets the re-resolved leader
    /// (Requirement 3.3, 10.1, 10.2).
    #[tokio::test(flavor = "multi_thread")]
    async fn consume_transport_failure_triggers_reresolution() {
        // node-a's consume fails at the transport layer, but it knows node-b now
        // leads the partition; node-b serves the read.
        let node_a = FakeNode::new(Some("node-b"), ConsumeScript::Unavailable);
        let node_b = FakeNode::new(Some("node-b"), ConsumeScript::Succeed { next_offset: 9 });
        let a_addr = serve(node_a.clone()).await;
        let b_addr = serve(node_b.clone()).await;

        let core = Arc::new(ClientCore::new([
            ("node-a".to_string(), a_addr.clone()),
            ("node-b".to_string(), b_addr.clone()),
        ]));
        // Believe node-a leads orders/0, so the first attempt fails transport and
        // forces a re-resolution.
        core.leaders().insert("orders", 0, a_addr.as_str());
        let consumer = Consumer::new(Arc::clone(&core));

        let outcome = consumer
            .consume("orders", 0, 0, None)
            .await
            .expect("re-resolves past the transport failure and reads from the new leader");

        assert_eq!(outcome.next_offset, 9);
        assert_eq!(
            node_a.consume_calls(),
            1,
            "the believed leader was tried once before re-resolution",
        );
        assert_eq!(
            node_b.consume_calls(),
            1,
            "the retry targeted the re-resolved leader (Req 3.3, 10.2)",
        );
        // The cache was invalidated and re-pointed at the re-resolved leader.
        assert_eq!(
            core.leaders().get("orders", 0).as_deref(),
            Some(b_addr.as_str()),
        );
    }
}
