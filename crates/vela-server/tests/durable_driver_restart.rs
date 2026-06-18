//! Single-node durable partition-driver restart for the `vela-server` node
//! daemon (task 18).
//!
//! This complements [`cold_restart`](../cold_restart.rs), which asserts the
//! catalogue + per-partition data recovery half of a cold restart. Here the
//! focus is Requirement 14.2 and 14.3 specifically: after a durable partition's
//! driver is torn down and recovered on the same data directory, the partition
//! must **resume serving both produce and consume** — not merely return the
//! records committed before the restart.
//!
//! The test therefore:
//!
//! - commits records to a durable partition, then fully tears the node down
//!   (releasing every WAL's exclusive directory lock);
//! - restarts the node on the **same** data directory;
//! - consumes and asserts every previously committed record is returned at its
//!   original offset (Requirement 14.1);
//! - **produces again** after recovery and asserts the new record is accepted at
//!   the next offset — i.e. the recovered high-water offset does not regress and
//!   the partition serves produce again (Requirement 14.2, 14.3); and
//! - consumes the full, combined sequence to confirm consume service resumed
//!   over both the recovered and the newly produced records (Requirement 14.2).
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
//! `prop_restart_preserves_committed_records.rs` and `cold_restart.rs`: a
//! uniquely named directory under [`std::env::temp_dir`], recursively removed
//! when the guard drops (after both incarnations have released it).

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
            "vela-server-durable-driver-restart-{tag}-{}-{unique}-{nanos}",
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
/// ms window, so ~2 s of bounded retries covers it without hanging.
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
/// bounded window waits out the randomized election timeout.
async fn create_topic_awaiting_metadata_leader(
    client: &mut VelaClientClient<Channel>,
    topic: &str,
    partitions: u32,
    log_backend: i32,
) {
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
            Ok(_) => return,
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

/// Consume from `(topic, partition)` starting at `offset`, returning the
/// decoded `(offset, value)` pairs and the reported `next_offset`.
async fn consume_all(
    client: &mut VelaClientClient<Channel>,
    topic: &str,
    partition: u32,
    offset: u64,
) -> (Vec<(u64, Vec<u8>)>, u64) {
    let response = client
        .consume(v1::ConsumeRequest {
            topic: topic.to_string(),
            partition,
            offset,
            max_count: None,
        })
        .await
        .expect("consume RPC succeeds")
        .into_inner();
    let records = response
        .records
        .into_iter()
        .map(|r| {
            let value = r.record.expect("consumed record carries a payload").value;
            (r.offset, value)
        })
        .collect();
    (records, response.next_offset)
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

/// Requirements 14.1, 14.2, 14.3 — a durable partition recovered on a reattached
/// data directory returns its previously committed records at their original
/// offsets **and** resumes serving both produce and consume: a record produced
/// after recovery is accepted at the next (non-regressing) offset, and a
/// subsequent consume returns the full combined sequence in order.
#[test]
fn durable_partition_resumes_produce_and_consume_after_restart() {
    let data = TempDir::new("resume");
    let before: [&[u8]; 3] = [b"r0", b"r1", b"r2"];
    let after: &[u8] = b"r3-after-restart";

    // ---- First incarnation: create a durable topic, commit three records. ----
    {
        let rt = incarnation_runtime();
        rt.block_on(async {
            let addr = free_addr();
            spawn_server(config("node-a", addr, data.path()));
            let mut client = connect_client(addr).await;

            create_topic_awaiting_metadata_leader(
                &mut client,
                "orders",
                1,
                v1::LogBackend::Durable as i32,
            )
            .await;

            await_leader(&mut client, "orders", 0).await;
            for (i, value) in before.iter().enumerate() {
                assert_eq!(produce(&mut client, "orders", 0, value).await, i as u64);
            }
        });
        // Drop the incarnation: every spawned task (server, partition driver)
        // and the shared NodeShared drop, releasing every WAL's directory lock.
        drop(rt);
    }

    // ---- Second incarnation: recover the durable driver on the SAME dir. ----
    {
        let rt = incarnation_runtime();
        rt.block_on(async {
            let addr = free_addr();
            spawn_server(config("node-a", addr, data.path()));
            let mut client = connect_client(addr).await;

            // (14.1) Every previously committed record is returned at its
            // original offset after recovery. Consume routes by the live
            // Raft-elected leader, so wait for the recovered partition to
            // re-elect before reading (Requirement 8.1).
            await_leader(&mut client, "orders", 0).await;
            let (recovered, next) = consume_all(&mut client, "orders", 0, 0).await;
            assert_eq!(recovered.len(), before.len());
            assert_eq!(next, before.len() as u64);
            for (i, value) in before.iter().enumerate() {
                assert_eq!(recovered[i].0, i as u64, "offset {i} preserved");
                assert_eq!(&recovered[i].1, value, "record at offset {i} round-trips");
            }

            // (14.2, 14.3) The partition resumes serving produce: a record
            // produced after recovery is accepted at the next offset, which is
            // exactly the pre-restart high-water offset (no regression).
            await_leader(&mut client, "orders", 0).await;
            let new_offset = produce(&mut client, "orders", 0, after).await;
            assert_eq!(
                new_offset,
                before.len() as u64,
                "the recovered partition assigns the next, non-regressing offset"
            );

            // (14.2) Consume resumes over the full combined sequence: the three
            // recovered records followed by the newly produced one, in order.
            let (combined, next) = consume_all(&mut client, "orders", 0, 0).await;
            assert_eq!(combined.len(), before.len() + 1);
            assert_eq!(next, before.len() as u64 + 1);
            for (i, value) in before.iter().enumerate() {
                assert_eq!(combined[i].0, i as u64);
                assert_eq!(&combined[i].1, value);
            }
            assert_eq!(combined[before.len()].0, before.len() as u64);
            assert_eq!(&combined[before.len()].1, after);
        });
        drop(rt);
    }
}
