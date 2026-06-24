//! Integration test: single-bootstrap leader resolution via cluster discovery.
//!
//! Feature: advertised-listeners (and ctl-client-routing-and-repl Req 8.2, 13.x)
//!
//! Exercises the exact scenario a host client hits against a port-mapped
//! cluster: the client is seeded with a **single** bootstrap endpoint, and the
//! partition whose leader it wants has all its replicas on *other* nodes — so
//! the bootstrap node cannot name the leader. Resolution must still succeed by:
//!
//! 1. discovering the rest of the cluster from the bootstrap node's
//!    `DescribeCluster`, seeding each member's **advertised** (client-reachable)
//!    address into the registry (advertised-listeners Req 6.1); then
//! 2. probing not just the bootstrap endpoint but every discovered member for
//!    the partition leader, so a leader hosted only on a discovered node is
//!    found (Req 8.2).
//!
//! Two in-process fake `VelaClient` servers stand in for a cluster:
//!
//! - `node1` (the sole bootstrap endpoint) hosts no replica of the partition,
//!   so its `FindLeader` answers `None` (no leader). Its `DescribeCluster`
//!   advertises `node2` at `node2`'s reachable address — the only place the
//!   client can learn how to reach `node2`.
//! - `node2` leads the partition: its `FindLeader` answers `Some("node2")`.
//!
//! The test asserts `refresh_leader` returns `node2`'s advertised address, that
//! `node2` (a discovered, non-bootstrap node) was actually probed, and that the
//! result resolves through the registry seeded by discovery. This guards the
//! behavior end-to-end: without union-probing the discovered members, this
//! resolution would fail with "no leader", exactly as a stale single-endpoint
//! client once did.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use vela_client::ClientCore;
use vela_proto::v1;
use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
use vela_proto::v1::{
    ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse, DeleteTopicRequest,
    DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse, DescribeTopicRequest,
    DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse, ListTopicsRequest,
    ListTopicsResponse, Member, ProduceBatchRequest, ProduceBatchResponse, ProduceRequest,
    ProduceResponse,
};

/// An in-process fake of one node's client-facing `VelaClient` service.
///
/// Answers only the two RPCs leader resolution drives — `FindLeader` (with a
/// configurable leader, counting probes) and `DescribeCluster` (with a
/// configurable member list) — and leaves the rest unimplemented.
#[derive(Clone)]
struct FakeNode {
    /// The leader this node names for any `FindLeader` (`None` => no leader, as
    /// a node that hosts no replica of the partition answers).
    leader: Option<String>,
    /// The membership this node returns from `DescribeCluster`.
    members: Vec<Member>,
    /// How many `FindLeader` probes reached this node.
    find_leader_calls: Arc<AtomicU32>,
}

impl FakeNode {
    fn new(leader: Option<&str>, members: Vec<Member>) -> Self {
        Self {
            leader: leader.map(str::to_string),
            members,
            find_leader_calls: Arc::new(AtomicU32::new(0)),
        }
    }

    fn find_leader_calls(&self) -> u32 {
        self.find_leader_calls.load(Ordering::SeqCst)
    }
}

#[tonic::async_trait]
impl VelaClientService for FakeNode {
    async fn find_leader(
        &self,
        _request: Request<FindLeaderRequest>,
    ) -> Result<Response<FindLeaderResponse>, Status> {
        self.find_leader_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(FindLeaderResponse {
            leader: self.leader.clone(),
        }))
    }

    async fn describe_cluster(
        &self,
        _request: Request<DescribeClusterRequest>,
    ) -> Result<Response<DescribeClusterResponse>, Status> {
        Ok(Response::new(DescribeClusterResponse {
            members: self.members.clone(),
            epoch: 1,
        }))
    }

    async fn produce(
        &self,
        _request: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        Err(Status::unimplemented("produce is not used by this test"))
    }

    async fn produce_batch(
        &self,
        _request: Request<ProduceBatchRequest>,
    ) -> Result<Response<ProduceBatchResponse>, Status> {
        Err(Status::unimplemented(
            "produce_batch is not used by this test",
        ))
    }

    async fn consume(
        &self,
        _request: Request<ConsumeRequest>,
    ) -> Result<Response<ConsumeResponse>, Status> {
        Err(Status::unimplemented("consume is not used by this test"))
    }

    async fn create_topic(
        &self,
        _request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        Err(Status::unimplemented(
            "create_topic is not used by this test",
        ))
    }

    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> Result<Response<DeleteTopicResponse>, Status> {
        Err(Status::unimplemented(
            "delete_topic is not used by this test",
        ))
    }

    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        Err(Status::unimplemented(
            "list_topics is not used by this test",
        ))
    }

    async fn describe_topic(
        &self,
        _request: Request<DescribeTopicRequest>,
    ) -> Result<Response<DescribeTopicResponse>, Status> {
        Err(Status::unimplemented(
            "describe_topic is not used by this test",
        ))
    }
}

/// Bind `node` on an OS-chosen localhost port and serve it on a background task.
/// The listener is bound before returning, so the endpoint is already accepting
/// connections — the client never races server startup.
async fn serve(node: FakeNode) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(VelaClientServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("fake server serves");
    });
    format!("http://127.0.0.1:{port}")
}

/// A client seeded with a single bootstrap endpoint resolves a partition whose
/// leader lives on a different, discovered node — by learning that node's
/// advertised address via `DescribeCluster` and then probing it for the leader.
#[tokio::test(flavor = "multi_thread")]
async fn single_bootstrap_resolves_leader_on_a_discovered_node() {
    // node2 leads the partition. Bind it first so we know its reachable address
    // to advertise from node1's DescribeCluster.
    let node2 = FakeNode::new(Some("node2"), Vec::new());
    let node2_addr = serve(node2.clone()).await;

    // node1 is the sole bootstrap endpoint: it hosts no replica of the
    // partition (FindLeader => None) but advertises node2's reachable address in
    // its DescribeCluster membership, mirroring a bind/advertised split (the
    // bind address is unset here; only the advertised address is dialable).
    let node1 = FakeNode::new(
        None,
        vec![
            Member {
                id: "node1".to_string(),
                addr: "node1:7001".to_string(),
                advertised_addr: String::new(),
                availability: v1::NodeAvailability::Available as i32,
            },
            Member {
                id: "node2".to_string(),
                addr: "node2:7001".to_string(),
                advertised_addr: node2_addr.clone(),
                availability: v1::NodeAvailability::Available as i32,
            },
        ],
    );
    let node1_addr = serve(node1.clone()).await;

    // Seed the client with ONLY node1 — the single published bootstrap endpoint.
    let core = ClientCore::new([("node1".to_string(), node1_addr.clone())]);

    let resolved = core
        .refresh_leader("orders", 1)
        .await
        .expect("leader resolves via discovery even though the bootstrap node hosts no replica");

    // The resolved leader is node2, dialable at its advertised address — the one
    // discovery seeded into the registry (advertised-listeners Req 6.1).
    assert_eq!(
        resolved, node2_addr,
        "the resolved address must be node2's discovered advertised address",
    );
    assert_eq!(
        core.registry().addr_of("node2").as_deref(),
        Some(node2_addr.as_str()),
        "discovery should have seeded node2's advertised address into the registry",
    );

    // The bootstrap node was probed and answered "no leader"; resolution then
    // fell through to the discovered node, which named the leader. Probing the
    // discovered, non-bootstrap node is the behavior under test (Req 8.2).
    assert_eq!(
        node1.find_leader_calls(),
        1,
        "the bootstrap node is probed once and answers no leader",
    );
    assert_eq!(
        node2.find_leader_calls(),
        1,
        "the discovered (non-bootstrap) node is probed and names the leader",
    );
}
