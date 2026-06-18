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
//! - **Metadata `SyncMetadata` reserved no-op (Requirement 1.3):**
//!   `SyncMetadata` is now off the commit path — cluster metadata is agreed
//!   solely through the dedicated `__meta/0` Raft group and reaches every node
//!   via `AppendEntries`. A `SyncMetadata` push therefore does NOT adopt the
//!   incoming snapshot; the RPC still answers, reporting the node's own current
//!   epoch read-only.
//!
//! ## Scope notes
//!
//! The current server seeds each node as the sole member of its own cluster and
//! exposes no client RPC for reading the membership table, so cross-node
//! *discovery* is asserted at the reachability level (each peer answers a direct
//! `Heartbeat`) rather than by inspecting one node's view of another's
//! availability. Metadata agreement and cross-node propagation are exercised by
//! the dedicated `__meta/0` Raft group's integration tests rather than here;
//! this file asserts only that the legacy `SyncMetadata` push no longer adopts.

use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_proto::v1::vela_peer_client::VelaPeerClient;

/// Monotonic counter making per-test data directories unique within a process.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, writable data directory under the system temp directory for one
/// test node.
///
/// Topics default to the Durable backend, so creating a topic opens a real
/// `DurableWal` under the node's data directory; rooting that beneath a unique
/// temp directory lets the tests exercise the real on-disk path without
/// depending on a fixed, possibly-unwritable location. The directory is created
/// lazily by the WAL; cleanup is left to the OS temp reaper because the spawned
/// server task outlives the test and holds the directory open.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("vela-server-it-{}-{n}-{nanos}", process::id()))
        .to_string_lossy()
        .into_owned()
}

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
        data_dir: Some(unique_data_dir()),
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

/// Issue `CreateTopic`, retrying while `__meta/0` has not yet elected a leader.
///
/// With the inline `BootstrapClock` bootstrap removed, even a single-node
/// metadata group elects through the normal asynchronous election path, so a
/// `CreateTopic` proposal is rejected with a "no metadata leader available"
/// status until that election fires (Requirement 4.2). Retrying within a
/// bounded window waits out the randomized election timeout and returns the
/// first committed create.
async fn create_topic_awaiting_metadata_leader(
    client: &mut VelaClientClient<Channel>,
    topic: &str,
    partitions: u32,
) -> v1::TopicInfo {
    let mut last_status = None;
    for _ in 0..100 {
        match client
            .create_topic(v1::CreateTopicRequest {
                name: topic.to_string(),
                partitions,
                log_backend: v1::LogBackend::Unspecified as i32,
            })
            .await
        {
            Ok(response) => {
                return response
                    .into_inner()
                    .topic
                    .expect("a committed create returns the applied topic");
            }
            Err(status) => {
                last_status = Some(status);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "metadata group did not self-elect a leader to commit CreateTopic within the bounded \
         window (last error: {last_status:?})"
    );
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

    // Create a topic, then read it back two ways. The 1-voter `__meta/0` group
    // self-elects asynchronously, so retry the create until that election fires.
    let created = create_topic_awaiting_metadata_leader(&mut client, "orders", 1).await;
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
/// successful, empty result once the partition has an elected leader. Consume
/// routes by the partition's live Raft-elected leader (Requirement 8.1), so the
/// single-node partition is given a moment to self-elect first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_from_empty_partition_returns_empty_result() {
    let addr = free_addr();
    spawn_server(config("node-a", addr, &[], 1));

    let mut client = connect_client(addr).await;
    create_topic_awaiting_metadata_leader(&mut client, "events", 1).await;

    // Wait for the single-node partition to self-elect, since consume now
    // redirects/awaits the live leader rather than serving a leaderless read.
    for _ in 0..100 {
        let leader = client
            .find_leader(v1::FindLeaderRequest {
                topic: "events".to_string(),
                partition: 0,
            })
            .await
            .expect("find_leader RPC succeeds")
            .into_inner()
            .leader;
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

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

    // The node keeps serving client requests despite the unreachable peer: a
    // read that needs no metadata-group commit returns promptly rather than
    // blocking on the dead peer. Topic admin now commits through the dedicated
    // metadata Raft group, which cannot reach a majority while a voter is dead
    // (correct Raft behaviour, Req 2.5), so this liveness test asserts read
    // serving rather than a create.
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds despite a dead peer")
        .into_inner()
        .topics;
    assert!(topics.is_empty(), "a fresh node lists no topics");
}

/// Requirement 1.3 — `SyncMetadata` is a **reserved no-op** off the commit path.
/// Cluster metadata is now agreed solely through the dedicated `__meta/0` Raft
/// group and reaches every node via `AppendEntries`, so a `SyncMetadata` push —
/// even one carrying a fresher `epoch` and new topics — does NOT adopt the
/// incoming snapshot into the served catalogue. The RPC still answers (keeping
/// the gRPC contract valid), reporting the node's own current epoch read-only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_metadata_is_a_reserved_no_op_and_does_not_adopt() {
    let addr = free_addr();
    spawn_server(config("node-a", addr, &[], 1));

    let mut peer = connect_peer(addr).await;
    let mut client = connect_client(addr).await;

    // Push a snapshot (epoch 5) carrying a topic that, under the old bespoke
    // propagation protocol, would have been adopted into the served view.
    let pushed = v1::ClusterMetadata {
        members: vec![v1::Member {
            id: "node-a".to_string(),
            addr: addr.to_string(),
            availability: v1::NodeAvailability::Available as i32,
        }],
        topics: vec![v1::TopicInfo {
            name: "should-not-appear".to_string(),
            partition_count: 1,
            partitions: vec![v1::PartitionInfo {
                index: 0,
                replicas: vec!["node-a".to_string()],
                leader: Some("node-a".to_string()),
            }],
            log_backend: v1::LogBackend::Durable as i32,
        }],
        epoch: 5,
    };
    let ack = peer
        .sync_metadata(v1::SyncMetadataRequest {
            metadata: Some(pushed),
        })
        .await
        .expect("sync_metadata still answers")
        .into_inner();
    // The reply carries the node's own current epoch (0 on a fresh node), not
    // the pushed epoch — the snapshot was not adopted.
    assert_eq!(
        ack.epoch, 0,
        "sync_metadata reports the node's own epoch, never adopting the push"
    );

    // The pushed topic is NOT visible to clients: the served catalogue is
    // unchanged because `SyncMetadata` no longer adopts (Req 1.3).
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics succeeds")
        .into_inner()
        .topics;
    assert!(
        topics.is_empty(),
        "a reserved-no-op SyncMetadata must not adopt the pushed snapshot"
    );
}
