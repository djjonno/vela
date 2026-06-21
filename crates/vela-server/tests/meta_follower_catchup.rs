//! Metadata follower catch-up integration test (task 9.5).
//!
//! This is the multi-node realization of Raft log replication's "the leader
//! retries `AppendEntries` to any follower whose log lags until that follower's
//! metadata log matches the leader's, so a follower that missed entries
//! eventually converges" (Requirement 3.6, Raft §5.3) and the recovery clause
//! "WHEN a recovered Metadata_Replica's log lags the Metadata_Leader, THE
//! Metadata_Leader SHALL bring it up to date through `AppendEntries` … after
//! which the recovered Node's served catalogue matches the committed log"
//! (Requirement 9.4).
//!
//! The scenario follows the design's *Follower catch-up* integration sketch:
//! stand up a 3-node cluster but start only **two** of its nodes — a metadata
//! majority (2 of 3) — and create several topics against that majority while the
//! third node is held down. Each `CreateTopic` is agreed solely through the
//! dedicated `("__meta", 0)` metadata Raft group: the two running voters form a
//! quorum, so the entries commit even though the third voter is absent. The
//! third node is then started and must, with no manual push:
//!
//! - **converge** — the metadata leader replicates the committed metadata log to
//!   the late node over `AppendEntries` (retrying until it catches up), so the
//!   late node's served catalogue — built by applying that committed log in
//!   order — eventually shows **every** topic created while it was down,
//!   including topics it never originated (Requirement 3.6, 9.4); and
//! - **reconcile** — applying those committed `CreateTopic` commands drives the
//!   off-loop reconciler, which starts a partition driver on the late node for
//!   every partition whose replica set contains it (Requirement 6.1). With its
//!   driver running, the late node answers `FindLeader` with the partition's
//!   live Raft-elected leader, proving the driver was spawned and joined the
//!   partition's group.
//!
//! ## Why the late node is in the replica sets
//!
//! Replica assignment draws from the **available** cluster members at create
//! time (Requirement 2.7, 9.6). The third node is a configured member, so on the
//! leader it starts out available and is assigned a replica of each new
//! partition; its heartbeat only later marks it unavailable. The topics are
//! therefore created promptly, while the held-down node is still an assignable
//! member, so its replica sets genuinely contain it and there is something for
//! it to reconcile when it joins. The test asserts that assignment up front (the
//! create response lists the node) so a missed window fails loudly rather than
//! silently weakening the catch-up assertion.
//!
//! ## Bounded waits, no unbounded blocking
//!
//! Catch-up happens asynchronously over real election/heartbeat timers, and a
//! late voter joining can briefly perturb the metadata election (its log is
//! behind, so it cannot win — Raft §5.4.1 — but it can force a re-election that
//! a fully-logged voter wins). Every wait here is therefore a bounded poll with
//! short sleeps: a genuinely stuck node fails the test promptly instead of
//! hanging. The patterns (`free_addr`, `unique_data_dir`, `spawn_server`,
//! `connect_client`) mirror `integration.rs` and `cluster_smoke.rs`.

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
/// test node. Each node's metadata group opens a durable `__meta` WAL beneath
/// it, so the directories must be distinct per node. Cleanup is left to the OS
/// temp reaper because the spawned server task outlives the test and holds the
/// directory open.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("vela-server-catchup-{}-{n}-{nanos}", process::id()))
        .to_string_lossy()
        .into_owned()
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener, returning the address the server should bind. Reserving all three
/// node addresses up front lets every node be configured with its peers' real
/// addresses before any node — including the late one — actually starts.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated [`Config`] through the same CLI path the daemon uses, with
/// an explicit `id@host:port` peer list so every node agrees on the cluster's
/// stable identities — and therefore on the metadata group's voter set.
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

/// Spawn a node serving `config` on a background task. The task runs until the
/// test's runtime is torn down; an early return from `serve` is a real failure.
fn spawn_server(config: Config) {
    tokio::spawn(async move {
        if let Err(error) = serve(config).await {
            panic!("server exited unexpectedly: {error}");
        }
    });
}

/// Connect a `VelaClient` to `addr`, retrying until the freshly spawned listener
/// accepts connections or a bounded budget elapses.
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

/// Create an in-memory topic against the 2-node metadata majority, returning the
/// applied [`v1::TopicInfo`].
///
/// `CreateTopic` commits only on the metadata leader and redirects a non-leader
/// with `NotLeader`; the leader is whichever of the two running voters won the
/// metadata election, so we try each client in turn and retry on any error
/// (`NotLeader`, "no metadata leader available" before the election settles, or
/// a transient connect race) until one commits. The wait is bounded so a cluster
/// that never elects a metadata leader fails the test rather than hanging.
async fn create_on_majority(
    clients: &mut [VelaClientClient<Channel>],
    name: &str,
    partitions: u32,
) -> v1::TopicInfo {
    let request = v1::CreateTopicRequest {
        name: name.to_string(),
        partitions,
        // In-memory keeps the test lean: the metadata group still uses its
        // durable `__meta` WAL, but client partitions write nothing to disk.
        log_backend: v1::LogBackend::InMemory as i32,
    };
    // This is the coldest start in the test: a *fresh* metadata election must
    // settle AND the first `ClusterCommand` must commit, all while one of the
    // three voters (node-c) is down and its failed vote/heartbeat connects add
    // scheduling churn. On a loaded 2-core CI runner — every test binary plus
    // several 3-node in-process clusters competing for the same cores — that can
    // take well over 5 s, so the budget matches the sibling cross-node test's
    // cold-election budget (`discover_metadata_leader`, 20 s) rather than the
    // tighter post-election convergence budgets elsewhere in this file. A
    // genuinely stuck cluster still fails the test instead of hanging.
    for _ in 0..800 {
        for client in clients.iter_mut() {
            if let Ok(response) = client.create_topic(request.clone()).await {
                return response
                    .into_inner()
                    .topic
                    .expect("a committed create returns the applied topic");
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "topic '{name}' did not commit on the 2-node metadata majority within the bounded window"
    );
}

/// The sorted topic names currently in `client`'s served catalogue.
async fn listed_topic_names(client: &mut VelaClientClient<Channel>) -> Vec<String> {
    let mut names: Vec<String> = client
        .list_topics(v1::ListTopicsRequest {})
        .await
        .expect("list_topics RPC succeeds")
        .into_inner()
        .topics
        .into_iter()
        .map(|t| t.name)
        .collect();
    names.sort();
    names
}

/// Poll `client`'s catalogue until it lists exactly `expected` (sorted) or the
/// bounded budget is exhausted. Catch-up is asynchronous (and may wait out a
/// brief metadata re-election when the late voter joins), so the budget is
/// generous while staying bounded.
async fn await_catalogue(client: &mut VelaClientClient<Channel>, expected: &[&str]) {
    let mut want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    want.sort();

    for _ in 0..500 {
        if listed_topic_names(client).await == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let have = listed_topic_names(client).await;
    panic!("the late node did not converge to the full catalogue: have {have:?}, want {want:?}");
}

/// Poll `FindLeader` for `(topic, partition)` on `client` until a live leader is
/// reported, returning it.
///
/// On the late node `FindLeader` answers with a leader only once the node hosts
/// the partition's driver (the reconciler started it) **and** that driver has
/// learned the partition's Raft-elected leader — so a non-`None` answer is
/// direct evidence the node reconciled and joined the partition's group.
async fn await_partition_leader(
    client: &mut VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
) -> String {
    for _ in 0..500 {
        let leader = client
            .find_leader(v1::FindLeaderRequest {
                topic: topic.to_string(),
                partition,
            })
            .await
            .expect("find_leader RPC succeeds")
            .into_inner()
            .leader;
        if let Some(leader) = leader {
            return leader;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "the late node did not host/elect a leader for {topic}/{partition} within the bounded window"
    );
}

/// Requirements 3.6, 9.4 — a node held down while topics are created on the
/// metadata majority is brought up to date by the metadata leader over
/// `AppendEntries` once it joins: it converges to the full committed catalogue
/// (including topics it never originated) and reconciles, starting a partition
/// driver for every partition whose replica set contains it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_joining_node_catches_up_via_append_entries_and_reconciles() {
    // Reserve all three addresses up front so every node can be configured with
    // its peers' real addresses before the late node starts.
    let addr_a = free_addr();
    let addr_b = free_addr();
    let addr_c = free_addr();

    let peer_a = format!("node-a@{addr_a}");
    let peer_b = format!("node-b@{addr_b}");
    let peer_c = format!("node-c@{addr_c}");

    // Start only node-a and node-b: a 2-of-3 metadata majority. node-c is a
    // configured voter (so the metadata group is a 3-voter set) but is held down
    // for now. rf=3 means each partition's replica set is all three nodes, so
    // the late node-c will have drivers to reconcile when it joins.
    spawn_server(config("node-a", addr_a, &[&peer_b, &peer_c], 3));
    spawn_server(config("node-b", addr_b, &[&peer_a, &peer_c], 3));

    let mut majority = vec![connect_client(addr_a).await, connect_client(addr_b).await];

    // Create several topics on the 2-node majority while node-c is down. They
    // commit through the metadata Raft group's elected leader even though one
    // voter (node-c) is absent — 2 of 3 is a quorum (Requirement 3.2).
    let topics = ["orders", "events", "logs"];
    for name in topics {
        let topic = create_on_majority(&mut majority, name, 1).await;
        assert_eq!(topic.name, name);
        assert_eq!(topic.partition_count, 1);
        // The held-down node was still an assignable member at create time, so
        // its replica set contains it — there is something for it to reconcile
        // when it joins. A missed window fails here, loudly.
        let replicas = &topic.partitions[0].replicas;
        assert!(
            replicas.iter().any(|r| r == "node-c"),
            "the held-down node-c must be assigned a replica of '{name}' (replicas: {replicas:?})"
        );
    }

    // The majority itself holds the full catalogue (sanity: the creates really
    // committed on the running voters).
    await_catalogue(&mut majority[0], &topics).await;

    // ---- Bring up the late node-c and let it catch up with no manual push. --
    spawn_server(config("node-c", addr_c, &[&peer_a, &peer_b], 3));
    let mut client_c = connect_client(addr_c).await;

    // Converge: the metadata leader replicates the committed log to node-c over
    // AppendEntries, and node-c's served catalogue — rebuilt by applying that
    // log in order — eventually shows every topic, including ones it never
    // originated (Requirement 3.6, 9.4).
    await_catalogue(&mut client_c, &topics).await;

    // Reconcile: applying those committed creates started a partition driver on
    // node-c for each partition whose replica set contains it, so node-c now
    // answers FindLeader with the partition's live Raft-elected leader
    // (Requirement 6.1). A node-c describe also confirms the converged catalogue
    // records node-c as a replica.
    for name in topics {
        let described = client_c
            .describe_topic(v1::DescribeTopicRequest {
                name: name.to_string(),
            })
            .await
            .expect("describe_topic succeeds on the converged late node")
            .into_inner()
            .topic
            .expect("the converged catalogue describes the topic");
        assert!(
            described.partitions[0]
                .replicas
                .iter()
                .any(|r| r == "node-c"),
            "node-c's converged catalogue must record it as a replica of '{name}'"
        );

        let leader = await_partition_leader(&mut client_c, name, 0).await;
        assert!(
            !leader.is_empty(),
            "node-c must report a live leader for '{name}'/0 once its driver joins the group"
        );
    }
}
