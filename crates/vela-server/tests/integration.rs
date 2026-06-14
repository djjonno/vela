//! End-to-end integration tests for the `vela-server` node daemon.
//!
//! These exercise a real [`serve`](vela_server::serve) instance over `tokio`
//! and `tonic` — binding an actual gRPC listener on a localhost port and
//! driving it through the generated `VelaClient` / `VelaPeer` clients — to
//! confirm the behaviours task 14.8 covers:
//!
//! - **Listener bind (Requirement 15.1):** `serve` binds its configured address
//!   on startup and both gRPC services become reachable (a `CreateTopic` then
//!   `ListTopics` round-trip on a single-node `rf=1` cluster succeeds, and the
//!   peer service answers `Heartbeat`).
//! - **Peer connection / discovery (Requirement 9.1, 9.2, 14.3):** two server
//!   instances configured as peers each answer `Heartbeat` over `VelaPeer`,
//!   demonstrating the server-to-server transport every node's membership loop
//!   relies on to discover and track its peers; a node pointed at a dead peer
//!   keeps serving while its membership loop retries the connection in the
//!   background (the 5 s connect timeout / 1 s retry never blocks request
//!   serving).
//! - **Metadata propagation (Requirement 2.8, 3.5, 3.6):** `SyncMetadata`
//!   adopts a fresher metadata snapshot and acks the applied epoch — the
//!   server-side half of the within-5 s propagation path — and refuses to move
//!   backward to a staler epoch.
//!
//! ## Scope notes
//!
//! The current server seeds each node as the sole member of its own cluster and
//! exposes no client RPC for reading the membership table, so cross-node
//! *discovery* is asserted at the reachability level (each peer answers a direct
//! `Heartbeat`) rather than by inspecting one node's view of another's
//! availability. Likewise, metadata **partial-failure reporting** (identifying
//! the exact laggard nodes that did not ack a delete within the deadline,
//! Requirement 3.6) is logic that lives in `vela-core`'s
//! `MetadataController::confirm_delete_propagation` and is unit-tested there; a
//! full multi-node delete-with-laggards round-trip is impractical against the
//! single-node-seeded server, so here we assert the server-side `SyncMetadata`
//! ack path that propagation is built on. These deferrals are intentional and
//! match what the current server supports.

use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_proto::v1::vela_peer_client::VelaPeerClient;

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address the server should bind.
///
/// There is a small window between releasing the port here and `serve`
/// re-binding it, but on localhost in a test this is reliable and keeps the
/// test from needing `serve` to report back the port it chose.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated single-field [`Config`] from raw values via the same
/// path the daemon uses, so tests exercise real configuration parsing.
fn config(node_id: &str, addr: SocketAddr, peers: &[&str], rf: u32) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        peers: peers.iter().map(|p| p.to_string()).collect(),
        replication_factor: Some(rf.to_string()),
    })
    .expect("valid test configuration")
}

/// Spawn a node serving `config` on a background task. The task runs until the
/// test's runtime is torn down.
fn spawn_server(config: Config) {
    tokio::spawn(async move {
        // `serve` only returns on a bind or transport error; in a passing test
        // it runs until the runtime shuts down, so a returned error is a real
        // failure worth surfacing.
        if let Err(error) = serve(config).await {
            panic!("server exited unexpectedly: {error}");
        }
    });
}

/// Connect a `VelaClient` to `addr`, retrying until the listener is accepting
/// connections (it is bound from a freshly spawned task) or a bounded number of
/// attempts elapse.
async fn connect_client(addr: SocketAddr) -> VelaClientClient<Channel> {
    let url = format!("http://{addr}");
    for _ in 0..100 {
        if let Ok(client) = VelaClientClient::connect(url.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("VelaClient at {addr} did not become reachable");
}

/// Connect a `VelaPeer` to `addr` with the same bounded retry as
/// [`connect_client`].
async fn connect_peer(addr: SocketAddr) -> VelaPeerClient<Channel> {
    let url = format!("http://{addr}");
    for _ in 0..100 {
        if let Ok(client) = VelaPeerClient::connect(url.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("VelaPeer at {addr} did not become reachable");
}

/// Requirement 15.1 — the gRPC listener binds on startup and **both** services
/// are reachable: a `CreateTopic` → `ListTopics` → `DescribeTopic` round-trip on
/// a single-node `rf=1` cluster succeeds (proving `VelaClient` is served and the
/// listener bound), and `Heartbeat` answers on the same address (proving
/// `VelaPeer` is served too).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_binds_and_serves_both_services_on_startup() {
    let addr = free_addr();
    spawn_server(config("node-a", addr, &[], 1));

    let mut client = connect_client(addr).await;

    // A fresh node has no topics.
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds against a bound listener")
        .into_inner()
        .topics;
    assert!(topics.is_empty(), "a fresh node lists no topics");

    // Create a topic, then read it back two ways.
    let created = client
        .create_topic(v1::CreateTopicRequest {
            name: "orders".to_string(),
            partitions: 1,
        })
        .await
        .expect("create_topic succeeds")
        .into_inner()
        .topic
        .expect("created topic is returned");
    assert_eq!(created.name, "orders");
    assert_eq!(created.partition_count, 1);

    let listed = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "orders");

    let described = client
        .describe_topic(v1::DescribeTopicRequest {
            name: "orders".to_string(),
        })
        .await
        .expect("describe_topic succeeds")
        .into_inner()
        .topic
        .expect("described topic is returned");
    // A single-node cluster assigns the sole node as the partition leader.
    assert_eq!(described.partitions.len(), 1);
    assert_eq!(described.partitions[0].leader.as_deref(), Some("node-a"));

    // The peer service is served on the same listener and identifies the node.
    let mut peer = connect_peer(addr).await;
    let reply = peer
        .heartbeat(v1::HeartbeatRequest {
            node_id: "tester".to_string(),
        })
        .await
        .expect("heartbeat succeeds against the peer service")
        .into_inner();
    assert_eq!(reply.node_id, "node-a");
}

/// Requirement 5.x / 15.1 — the consume path is reachable end-to-end against a
/// bound listener: consuming from a freshly created (empty) partition returns a
/// successful, empty result. This needs no elected Raft leader to be observed
/// (the committed log is simply empty), so it stays deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_from_empty_partition_returns_empty_result() {
    let addr = free_addr();
    spawn_server(config("node-a", addr, &[], 1));

    let mut client = connect_client(addr).await;
    client
        .create_topic(v1::CreateTopicRequest {
            name: "events".to_string(),
            partitions: 1,
        })
        .await
        .expect("create_topic succeeds");

    let response = client
        .consume(v1::ConsumeRequest {
            topic: "events".to_string(),
            partition: 0,
            offset: 0,
            max_count: None,
        })
        .await
        .expect("consume succeeds on an empty partition")
        .into_inner();
    assert!(
        response.records.is_empty(),
        "an empty partition yields no records"
    );
    assert_eq!(response.next_offset, 0);
}

/// Requirement 9.1, 9.2, 14.3 — two nodes configured as peers each bind their
/// peer listener and answer `Heartbeat`, demonstrating the server-to-server
/// transport that membership and discovery ride on. Each node's membership loop
/// dials the other (the heartbeat substrate), and the direct heartbeats here
/// confirm both endpoints are reachable and self-identify correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peers_reach_each_others_heartbeat_endpoint() {
    let addr_a = free_addr();
    let addr_b = free_addr();

    // Each node is configured with the other as a peer, so both membership
    // loops attempt the cross-node connection on startup (Requirement 9.1).
    spawn_server(config("node-a", addr_a, &[&addr_b.to_string()], 1));
    spawn_server(config("node-b", addr_b, &[&addr_a.to_string()], 1));

    let mut peer_a = connect_peer(addr_a).await;
    let mut peer_b = connect_peer(addr_b).await;

    let reply_a = peer_a
        .heartbeat(v1::HeartbeatRequest {
            node_id: "node-b".to_string(),
        })
        .await
        .expect("node-a answers heartbeats")
        .into_inner();
    assert_eq!(reply_a.node_id, "node-a");

    let reply_b = peer_b
        .heartbeat(v1::HeartbeatRequest {
            node_id: "node-a".to_string(),
        })
        .await
        .expect("node-b answers heartbeats")
        .into_inner();
    assert_eq!(reply_b.node_id, "node-b");

    // Both nodes still serve client traffic while their membership loops run.
    let mut client_a = connect_client(addr_a).await;
    client_a
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("node-a serves clients alongside membership");
}

/// Requirement 9.1, 9.2 — a node configured with an unreachable peer does not
/// crash or block: its membership loop bounds each connection attempt by the
/// 5 s timeout and retries every 1 s in the background, while the node keeps
/// answering client requests. The peer address is a reserved-for-tests port
/// that nothing is listening on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_with_dead_peer_keeps_serving() {
    let addr = free_addr();
    // A free (unbound) address stands in for a dead peer: the membership loop
    // will fail to connect and retry without ever blocking request serving.
    let dead_peer = free_addr();
    spawn_server(config("node-a", addr, &[&dead_peer.to_string()], 1));

    let mut client = connect_client(addr).await;

    // The node serves a full admin round-trip despite the unreachable peer.
    client
        .create_topic(v1::CreateTopicRequest {
            name: "orders".to_string(),
            partitions: 1,
        })
        .await
        .expect("create_topic succeeds despite a dead peer");
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds despite a dead peer")
        .into_inner()
        .topics;
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "orders");
}

/// Requirement 2.8, 3.5, 3.6 — `SyncMetadata` is the server-side half of metadata
/// propagation: pushing a fresher snapshot (higher `epoch`) causes the node to
/// adopt it and ack the applied epoch, and the adopted topics become visible to
/// clients (adoption well within the 5 s propagation window). A subsequent push
/// carrying a *staler* epoch is not adopted, and the ack still reports the
/// fresher epoch the node holds — propagation only ever moves forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_metadata_adopts_fresher_epoch_and_acks() {
    let addr = free_addr();
    spawn_server(config("node-a", addr, &[], 1));

    let mut peer = connect_peer(addr).await;
    let mut client = connect_client(addr).await;

    // Push a fresher snapshot (epoch 5) carrying a propagated topic.
    let fresh = v1::ClusterMetadata {
        members: vec![v1::Member {
            id: "node-a".to_string(),
            addr: addr.to_string(),
            availability: v1::NodeAvailability::Available as i32,
        }],
        topics: vec![v1::TopicInfo {
            name: "propagated".to_string(),
            partition_count: 1,
            partitions: vec![v1::PartitionInfo {
                index: 0,
                replicas: vec!["node-a".to_string()],
                leader: Some("node-a".to_string()),
            }],
        }],
        epoch: 5,
    };
    let ack = peer
        .sync_metadata(v1::SyncMetadataRequest {
            metadata: Some(fresh),
        })
        .await
        .expect("sync_metadata succeeds")
        .into_inner();
    assert_eq!(ack.epoch, 5, "the node acks the freshly applied epoch");

    // The adopted metadata is now visible to clients.
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics;
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "propagated");

    // A staler snapshot (epoch 3) must not be adopted, and the ack reports the
    // fresher epoch the node still holds.
    let stale = v1::ClusterMetadata {
        members: Vec::new(),
        topics: vec![v1::TopicInfo {
            name: "should-not-appear".to_string(),
            partition_count: 1,
            partitions: vec![v1::PartitionInfo {
                index: 0,
                replicas: vec!["node-a".to_string()],
                leader: Some("node-a".to_string()),
            }],
        }],
        epoch: 3,
    };
    let ack = peer
        .sync_metadata(v1::SyncMetadataRequest {
            metadata: Some(stale),
        })
        .await
        .expect("sync_metadata succeeds")
        .into_inner();
    assert_eq!(
        ack.epoch, 5,
        "a staler push does not move the epoch backward"
    );

    // The stale topic was not adopted; the fresher view is retained.
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics;
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "propagated");
}
