//! Single-node cold-restart recovery for the `vela-server` node daemon
//! (task 15).
//!
//! This is the end-to-end realization of the durable bootstrap: a node creates
//! a Durable topic and an In-Memory topic, produces and commits records to the
//! durable partition, is then fully torn down (releasing every WAL's exclusive
//! directory lock), and is **restarted on the same data directory**. The
//! restarted node must:
//!
//! - recover the topic catalogue, including each topic's recorded backend, from
//!   the durable `__meta` Raft group (Requirement 18.1, 18.3);
//! - reopen the durable topic's partition on its existing segments and return
//!   the previously committed records at their original offsets (Requirement
//!   14.1, 18.2); and
//! - start the in-memory topic empty — its catalogue entry survives, but its
//!   data does not (Requirement 11.4, 13.2).
//!
//! ## Releasing WAL locks across the restart
//!
//! A `DurableWal` holds an exclusive lock on its data directory for its
//! lifetime, so the second incarnation can only reopen the same paths once the
//! first incarnation's WALs are dropped. The metadata group's WAL is owned by
//! `NodeShared`; each durable partition's WAL is owned by its driver task. The
//! test runs the first incarnation inside its own `tokio` runtime and **drops
//! that runtime** before restarting: dropping the runtime drops every spawned
//! task (the gRPC server, the membership loops, and every partition driver) and
//! the `NodeShared` they share, which in turn drops every WAL and releases its
//! lock. The second incarnation then opens cleanly on the same directory.
//!
//! The temp-directory pattern mirrors `vela-core`'s
//! `prop_restart_preserves_committed_records.rs`: a uniquely named directory
//! under [`std::env::temp_dir`], recursively removed when the guard drops (after
//! both incarnations have released it).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::runtime::Runtime;
use tonic::transport::Channel;

use vela_server::{serve, CliArgs, Config};

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;

/// Monotonic counter making temp-dir names unique within a single process.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// An owned temporary directory recursively removed when dropped.
///
/// Cleanup is best-effort so a removal failure never masks an assertion. The
/// guard outlives both server incarnations, so it is removed only after every
/// WAL under it has been dropped and its lock released.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the unix epoch")
            .as_nanos();
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "vela-server-cold-restart-{tag}-{}-{unique}-{nanos}",
            process::id()
        );
        Self {
            path: std::env::temp_dir().join(name),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Reserve a free localhost port by binding (then dropping) an ephemeral
/// listener. There is a small race before `serve` re-binds, but on localhost in
/// a test this is reliable.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Build a validated [`Config`] rooted at `data_dir`, through the same CLI path
/// the daemon uses. Both incarnations share `data_dir`; only the listen address
/// differs so the restarted node can rebind a fresh port.
fn config(node_id: &str, addr: SocketAddr, data_dir: &Path) -> Config {
    Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(addr.to_string()),
        peers: Vec::new(),
        replication_factor: Some("1".to_string()),
        data_dir: Some(data_dir.to_string_lossy().into_owned()),
    })
    .expect("valid test configuration")
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

/// Poll `FindLeader` until `(topic, partition)` reports an elected leader or the
/// bounded budget is exhausted. Election on the real clock fires in the 150–300
/// ms window, so ~1 s of bounded retries covers it without hanging.
async fn await_leader(client: &mut VelaClientClient<Channel>, topic: &str, partition: u32) {
    for _ in 0..100 {
        let leader = client
            .find_leader(v1::FindLeaderRequest {
                topic: topic.to_string(),
                partition,
            })
            .await
            .expect("find_leader RPC succeeds")
            .into_inner()
            .leader;
        if leader.is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("partition {topic}/{partition} did not elect a leader within the bounded window");
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
    log_backend: i32,
) -> v1::TopicInfo {
    let mut last_status = None;
    for _ in 0..100 {
        match client
            .create_topic(v1::CreateTopicRequest {
                name: topic.to_string(),
                partitions,
                log_backend,
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

/// Produce `value` to `(topic, partition)`, retrying briefly to absorb the
/// instant between a leader being reported and the produce path observing it,
/// and return the committed offset.
async fn produce(
    client: &mut VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
    value: &[u8],
) -> u64 {
    for _ in 0..100 {
        match client
            .produce(v1::ProduceRequest {
                topic: topic.to_string(),
                partition,
                record: Some(v1::Record {
                    key: None,
                    value: value.to_vec(),
                }),
            })
            .await
        {
            Ok(response) => return response.into_inner().offset,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("produce to {topic}/{partition} did not commit within the bounded window");
}

/// Spawn a node serving `config` on a background task within the current
/// runtime. The task runs until its runtime is dropped; an early `serve` return
/// is a real failure (including a failed durable bootstrap).
fn spawn_server(config: Config) {
    tokio::spawn(async move {
        if let Err(error) = serve(config).await {
            panic!("server exited unexpectedly: {error}");
        }
    });
}

/// A multi-threaded runtime for one server incarnation. Dropping it tears the
/// incarnation down completely, releasing every WAL lock it held.
fn incarnation_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build a tokio runtime for one incarnation")
}

/// Requirements 11.4, 14.1, 18.1, 18.2, 18.3 — a single-node cold restart on a
/// reattached data directory recovers the catalogue (including backends),
/// reopens the durable topic on its existing segments and returns the
/// previously committed records at their original offsets, and starts the
/// in-memory topic empty.
#[test]
fn cold_restart_recovers_catalogue_durable_records_and_empty_in_memory() {
    let data = TempDir::new("recover");
    let values: [&[u8]; 3] = [b"v0", b"v1", b"v2"];

    // ---- First incarnation: create both topics, commit durable records. ----
    {
        let rt = incarnation_runtime();
        rt.block_on(async {
            let addr = free_addr();
            spawn_server(config("node-a", addr, data.path()));
            let mut client = connect_client(addr).await;

            // A Durable topic (the default backend) and an In-Memory topic.
            // The 1-voter `__meta/0` group self-elects asynchronously, so retry
            // the create until that metadata election fires.
            let durable = create_topic_awaiting_metadata_leader(
                &mut client,
                "orders",
                1,
                v1::LogBackend::Durable as i32,
            )
            .await;
            assert_eq!(durable.log_backend, v1::LogBackend::Durable as i32);

            let in_memory = create_topic_awaiting_metadata_leader(
                &mut client,
                "events",
                1,
                v1::LogBackend::InMemory as i32,
            )
            .await;
            assert_eq!(in_memory.log_backend, v1::LogBackend::InMemory as i32);

            // Commit three records to the durable partition; offsets are gap-free
            // and 0-based.
            await_leader(&mut client, "orders", 0).await;
            for (i, value) in values.iter().enumerate() {
                assert_eq!(produce(&mut client, "orders", 0, value).await, i as u64);
            }

            // Put a record on the in-memory partition too, so the post-restart
            // "starts empty" assertion is meaningful (its data must NOT survive).
            await_leader(&mut client, "events", 0).await;
            assert_eq!(produce(&mut client, "events", 0, b"ephemeral").await, 0);
        });
        // Drop the incarnation: every spawned task (server, drivers) and the
        // shared NodeShared drop, releasing every WAL's directory lock.
        drop(rt);
    }

    // ---- Second incarnation: cold restart on the SAME data directory. ------
    {
        let rt = incarnation_runtime();
        rt.block_on(async {
            let addr = free_addr();
            spawn_server(config("node-a", addr, data.path()));
            let mut client = connect_client(addr).await;

            // The catalogue is recovered: both topics, with their recorded
            // backends (Requirement 18.1, 18.3).
            let mut topics = client
                .list_topics(v1::ListTopicsRequest {})
                .await
                .expect("list_topics succeeds after restart")
                .into_inner()
                .topics;
            topics.sort_by(|a, b| a.name.cmp(&b.name));
            assert_eq!(topics.len(), 2, "both topics are recovered");
            assert_eq!(topics[0].name, "events");
            assert_eq!(
                topics[0].log_backend,
                v1::LogBackend::InMemory as i32,
                "the in-memory backend is recovered"
            );
            assert_eq!(topics[1].name, "orders");
            assert_eq!(
                topics[1].log_backend,
                v1::LogBackend::Durable as i32,
                "the durable backend is recovered"
            );

            // The durable partition reopened on its existing segments and
            // returns every previously committed record at its original offset
            // (Requirement 14.1, 18.2). Consume routes by the live Raft-elected
            // leader, so wait for the recovered partition to re-elect before
            // reading (Requirement 8.1).
            await_leader(&mut client, "orders", 0).await;
            let consumed = client
                .consume(v1::ConsumeRequest {
                    topic: "orders".to_string(),
                    partition: 0,
                    offset: 0,
                    max_count: None,
                })
                .await
                .expect("consume from the recovered durable partition succeeds")
                .into_inner();
            assert_eq!(consumed.records.len(), values.len());
            assert_eq!(consumed.next_offset, values.len() as u64);
            for (i, value) in values.iter().enumerate() {
                assert_eq!(consumed.records[i].offset, i as u64);
                let record = consumed.records[i]
                    .record
                    .as_ref()
                    .expect("recovered record carries a payload");
                assert_eq!(record.value, *value, "record at offset {i} round-trips");
            }

            // The in-memory topic survives in the catalogue but starts empty:
            // its committed records did not survive the restart (Requirement
            // 11.4, 13.2). Consume routes by the live leader, so wait for this
            // partition to elect after restart before reading (Requirement 8.1).
            await_leader(&mut client, "events", 0).await;
            let empty = client
                .consume(v1::ConsumeRequest {
                    topic: "events".to_string(),
                    partition: 0,
                    offset: 0,
                    max_count: None,
                })
                .await
                .expect("consume from the recovered in-memory partition succeeds")
                .into_inner();
            assert!(
                empty.records.is_empty(),
                "the in-memory topic starts empty after a restart"
            );
            assert_eq!(empty.next_offset, 0);
        });
        drop(rt);
    }
}
