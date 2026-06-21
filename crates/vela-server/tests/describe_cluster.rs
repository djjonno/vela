//! Integration tests for the client-facing `DescribeCluster` RPC (task 7.4).
//!
//! `DescribeCluster` is the backward-compatible addition that exposes the
//! Member_Address_Map — each known member's node id and transport address — to
//! programmatic clients so they can seed a node registry (node id -> address)
//! for leader-directed routing without an `id=url` Endpoint (Requirement 12.7,
//! 12.8). The handler builds its `DescribeClusterResponse { members, epoch }`
//! from the served [`ClusterMetadata`] view — the same catalogue every other
//! client RPC reads — mapping each member (id + addr + availability) onto the
//! wire and reporting the served view's applied-change epoch.
//!
//! These tests drive a real [`serve`](vela_server::serve) instance over `tokio`
//! and `tonic` (binding an actual gRPC listener and calling the generated
//! `VelaClient`), matching the harness established in `integration.rs` /
//! `cluster_smoke.rs`, and assert:
//!
//! - **Membership set (Requirement 12.7, 12.8):** a node configured with a peer
//!   seeds that peer into its served membership at startup, so `DescribeCluster`
//!   returns each member's id and transport address (and availability) plus the
//!   metadata epoch — exactly what a client needs to resolve a leader node id to
//!   an address.
//! - **Backward-compatible "no extra members" path (Requirement 12.6, 12.7):** a
//!   peerless single node returns only itself as a member with no error, and the
//!   pre-existing client RPCs (`ListTopics`, `CreateTopic`, `DescribeTopic`)
//!   still behave exactly as before — the new RPC is purely additive and leaves
//!   existing client behavior unchanged.
//!
//! ## Scope note
//!
//! A node always seeds itself as the sole available member of its own cluster at
//! startup (`NodeShared::new`) and grows that set as configured peers are
//! registered (`crate::membership::register_peers`). Through the public `serve`
//! path the served membership therefore always contains at least this node, so
//! the closest realizable form of the "empty / older membership" path is a
//! peerless node reporting a single member; that path is asserted to leave the
//! existing client RPCs unchanged.

use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;

/// Monotonic counter making per-test data directories unique within a process.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, writable data directory under the system temp directory for one
/// test node. Mirrors the helper in `integration.rs`: topics default to the
/// Durable backend, so a created topic opens a real WAL beneath this root; the
/// directory is created lazily and cleanup is left to the OS temp reaper because
/// the spawned server task outlives the test.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!(
            "vela-server-describe-{}-{n}-{nanos}",
            process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address the server should bind. There is a small race
/// between releasing the port and `serve` re-binding it, but on localhost in a
/// test this is reliable.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated [`Config`] through the same CLI path the daemon uses, so
/// the test exercises real configuration parsing.
fn config(node_id: &str, addr: SocketAddr, peers: &[&str], rf: u32) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        advertised_addr: None,
        peers: peers.iter().map(|p| p.to_string()).collect(),
        replication_factor: Some(rf.to_string()),
        data_dir: Some(unique_data_dir()),
    })
    .expect("valid test configuration")
}

/// Like [`config`] but with an explicit advertised address, for the
/// advertised-listeners scenarios. The node's own advertised address is set via
/// `--advertised-addr`; peers carry their advertised address through the
/// `id@listen@advertised` grammar.
fn config_with_advertised(
    node_id: &str,
    addr: SocketAddr,
    advertised: &str,
    peers: &[&str],
    rf: u32,
) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        advertised_addr: Some(advertised.to_string()),
        peers: peers.iter().map(|p| p.to_string()).collect(),
        replication_factor: Some(rf.to_string()),
        data_dir: Some(unique_data_dir()),
    })
    .expect("valid advertised test configuration")
}

/// Spawn a node serving `config` on a background task. The task runs until the
/// test runtime is torn down; an early return from `serve` is a real failure.
fn spawn_server(config: Config) {
    tokio::spawn(async move {
        if let Err(error) = serve(config).await {
            panic!("server exited unexpectedly: {error}");
        }
    });
}

/// Connect a `VelaClient` to `addr`, retrying until the freshly spawned listener
/// accepts connections or a bounded number of attempts elapse.
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

/// Issue `CreateTopic`, retrying while `__meta/0` has not yet elected a leader.
///
/// The single-voter metadata group self-elects through the normal asynchronous
/// election path, so a `CreateTopic` proposal is rejected with a "no metadata
/// leader available" status until that election fires (Requirement 4.2).
/// Retrying within a bounded window waits out the randomized election timeout
/// and returns the first committed create.
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

/// Locate the member with `id` in a `DescribeCluster` reply, failing the test
/// with a clear message if it is absent.
fn member<'a>(members: &'a [v1::Member], id: &str) -> &'a v1::Member {
    members
        .iter()
        .find(|m| m.id == id)
        .unwrap_or_else(|| panic!("DescribeCluster must report member {id:?}, got {members:?}"))
}

/// Requirement 12.7, 12.8 — a node configured with a peer reports the full
/// membership set through `DescribeCluster`: every known member's node id and
/// transport address (and availability), plus the metadata epoch. Two nodes are
/// brought up as each other's peer so both heartbeat loops succeed and the
/// membership stays available; querying node-a then returns both node-a (its own
/// listen address) and node-b (the configured peer address), which is exactly
/// the Member_Address_Map a client seeds its node registry from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_returns_each_member_id_and_addr() {
    let addr_a = free_addr();
    let addr_b = free_addr();

    // Configure each node with the other as an explicit `id@addr` peer, so the
    // peer's member entry records its stable cluster id (node-a / node-b)
    // distinct from its address.
    let peer_b = format!("node-b@{addr_b}");
    let peer_a = format!("node-a@{addr_a}");
    spawn_server(config("node-a", addr_a, &[peer_b.as_str()], 1));
    spawn_server(config("node-b", addr_b, &[peer_a.as_str()], 1));

    let mut client = connect_client(addr_a).await;

    let response = client
        .describe_cluster(v1::DescribeClusterRequest {})
        .await
        .expect("describe_cluster succeeds against a bound listener")
        .into_inner();

    // Both the node itself and its configured peer are reported, each with the
    // id and transport address a client needs to resolve a leader node id.
    assert_eq!(
        response.members.len(),
        2,
        "the node and its one configured peer are both members, got {:?}",
        response.members
    );

    let node_a = member(&response.members, "node-a");
    assert_eq!(
        node_a.addr,
        addr_a.to_string(),
        "node-a is reported at its own listen address"
    );
    assert_eq!(
        node_a.availability,
        v1::NodeAvailability::Available as i32,
        "the local node is available"
    );

    let node_b = member(&response.members, "node-b");
    assert_eq!(
        node_b.addr,
        addr_b.to_string(),
        "node-b is reported at its configured peer address"
    );
    assert_eq!(
        node_b.availability,
        v1::NodeAvailability::Available as i32,
        "a reachable peer is available"
    );

    // Every reported member carries a non-empty id and address — the contract a
    // programmatic client relies on to map any leader node id to an address
    // (Requirement 12.8).
    for m in &response.members {
        assert!(!m.id.is_empty(), "every member carries a node id");
        assert!(
            !m.addr.is_empty(),
            "every member carries a transport address"
        );
    }

    // The reply carries the served view's applied-change epoch. With no topic
    // changes and no availability transitions (both peers stay reachable), a
    // fresh node's membership sits at epoch 0.
    assert_eq!(
        response.epoch, 0,
        "an unchanged, freshly-seeded membership is observed at epoch 0"
    );
}

/// Requirement 12.6, 12.7 — `DescribeCluster` is a purely additive,
/// backward-compatible RPC. A peerless single node reports only itself as a
/// member (the minimal membership reachable through `serve`) with no error, and
/// the pre-existing client RPCs continue to behave exactly as before: listing is
/// empty on a fresh node, and a create -> list -> describe topic round-trip
/// succeeds unchanged. This proves adding the membership endpoint leaves
/// existing client behavior unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_is_additive_and_leaves_existing_behavior_unchanged() {
    let addr = free_addr();
    spawn_server(config("node-a", addr, &[], 1));

    let mut client = connect_client(addr).await;

    // The new endpoint answers without error on a peerless node, reporting just
    // the node itself — the older "no extra members" path is well-formed.
    let response = client
        .describe_cluster(v1::DescribeClusterRequest {})
        .await
        .expect("describe_cluster succeeds on a peerless node")
        .into_inner();
    assert_eq!(
        response.members.len(),
        1,
        "a peerless node reports only itself, got {:?}",
        response.members
    );
    let only = &response.members[0];
    assert_eq!(only.id, "node-a", "the sole member is this node");
    assert_eq!(
        only.addr,
        addr.to_string(),
        "the sole member is reported at its listen address"
    );
    assert_eq!(
        response.epoch, 0,
        "a fresh membership is observed at epoch 0"
    );

    // Existing client behavior is unchanged: a fresh node lists no topics, and a
    // create -> list -> describe round-trip works exactly as before the RPC was
    // added.
    let topics = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics still succeeds")
        .into_inner()
        .topics;
    assert!(topics.is_empty(), "a fresh node lists no topics");

    let created = create_topic_awaiting_metadata_leader(&mut client, "orders", 1).await;
    assert_eq!(created.name, "orders");
    assert_eq!(created.partition_count, 1);

    let listed = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics still succeeds after a create")
        .into_inner()
        .topics;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "orders");

    let described = client
        .describe_topic(v1::DescribeTopicRequest {
            name: "orders".to_string(),
        })
        .await
        .expect("describe_topic still succeeds")
        .into_inner()
        .topic
        .expect("described topic is returned");
    assert_eq!(described.partitions.len(), 1);
    assert_eq!(described.partitions[0].leader.as_deref(), Some("node-a"));

    // The additive RPC did not disturb the membership view: it still reports the
    // single node after the topic round-trip.
    let after = client
        .describe_cluster(v1::DescribeClusterRequest {})
        .await
        .expect("describe_cluster still succeeds after topic admin")
        .into_inner();
    assert_eq!(
        after.members.len(),
        1,
        "membership is unaffected by topic admin, got {:?}",
        after.members
    );
    assert_eq!(after.members[0].id, "node-a");
}

/// advertised-listeners Requirement 4.2 — `DescribeCluster` reports each
/// member's *advertised* address (distinct from its bind address). Two nodes are
/// brought up as each other's peer; each is configured with its own advertised
/// address via `--advertised-addr`, and learns its peer's advertised address
/// through the `id@listen@advertised` peer grammar (the peer's real bind address
/// is used for dialing, so heartbeats keep both available). Querying node-a then
/// returns node-a's and node-b's configured advertised addresses on the
/// `advertised_addr` field, while the `addr` field still carries each member's
/// bind address — the two are reported distinctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_reports_each_members_advertised_address() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let adv_a = "203.0.113.1:9001";
    let adv_b = "203.0.113.2:9002";

    // Each peer entry carries the peer's real bind address (for dialing) and its
    // client-facing advertised address (reported, never dialed).
    let peer_b = format!("node-b@{addr_b}@{adv_b}");
    let peer_a = format!("node-a@{addr_a}@{adv_a}");
    spawn_server(config_with_advertised(
        "node-a",
        addr_a,
        adv_a,
        &[peer_b.as_str()],
        1,
    ));
    spawn_server(config_with_advertised(
        "node-b",
        addr_b,
        adv_b,
        &[peer_a.as_str()],
        1,
    ));

    let mut client = connect_client(addr_a).await;
    let response = client
        .describe_cluster(v1::DescribeClusterRequest {})
        .await
        .expect("describe_cluster succeeds against a bound listener")
        .into_inner();

    let node_a = member(&response.members, "node-a");
    let node_b = member(&response.members, "node-b");

    // Each member reports its configured advertised address.
    assert_eq!(
        node_a.advertised_addr, adv_a,
        "node-a reports its own configured advertised address"
    );
    assert_eq!(
        node_b.advertised_addr, adv_b,
        "node-b reports its peer-configured advertised address"
    );

    // The bind address is still reported on `addr`, distinct from advertised.
    assert_eq!(node_a.addr, addr_a.to_string());
    assert_eq!(node_b.addr, addr_b.to_string());
    assert_ne!(node_a.addr, node_a.advertised_addr);
    assert_ne!(node_b.addr, node_b.advertised_addr);
}

/// advertised-listeners Requirement 4.3 — two nodes that have applied the same
/// metadata epoch report the same advertised address for a given member.
/// Operators configure each node's view consistently (node-b's self advertised
/// address equals what node-a is told for node-b via the peer grammar, and vice
/// versa), so querying node-a and node-b at the same epoch yields identical
/// advertised addresses per member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_nodes_report_the_same_advertised_address_for_a_member() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let adv_a = "203.0.113.1:9001";
    let adv_b = "203.0.113.2:9002";

    let peer_b = format!("node-b@{addr_b}@{adv_b}");
    let peer_a = format!("node-a@{addr_a}@{adv_a}");
    spawn_server(config_with_advertised(
        "node-a",
        addr_a,
        adv_a,
        &[peer_b.as_str()],
        1,
    ));
    spawn_server(config_with_advertised(
        "node-b",
        addr_b,
        adv_b,
        &[peer_a.as_str()],
        1,
    ));

    let mut client_a = connect_client(addr_a).await;
    let mut client_b = connect_client(addr_b).await;

    let from_a = client_a
        .describe_cluster(v1::DescribeClusterRequest {})
        .await
        .expect("describe_cluster against node-a succeeds")
        .into_inner();
    let from_b = client_b
        .describe_cluster(v1::DescribeClusterRequest {})
        .await
        .expect("describe_cluster against node-b succeeds")
        .into_inner();

    // Both views are observed at the same (fresh, unchanged) epoch.
    assert_eq!(
        from_a.epoch, 0,
        "node-a's freshly-seeded view sits at epoch 0"
    );
    assert_eq!(
        from_b.epoch, 0,
        "node-b's freshly-seeded view sits at epoch 0"
    );
    assert_eq!(from_a.epoch, from_b.epoch);

    // For each member, both nodes report the same advertised address.
    for id in ["node-a", "node-b"] {
        assert_eq!(
            member(&from_a.members, id).advertised_addr,
            member(&from_b.members, id).advertised_addr,
            "both nodes report the same advertised address for {id}"
        );
    }

    // And those agreed addresses are exactly the operator-configured values.
    assert_eq!(member(&from_a.members, "node-a").advertised_addr, adv_a);
    assert_eq!(member(&from_a.members, "node-b").advertised_addr, adv_b);
}
