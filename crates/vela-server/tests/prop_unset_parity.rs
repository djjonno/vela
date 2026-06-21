//! Property test for unconfigured-advertised parity in `vela-server`.
//!
//! Feature: advertised-listeners, Property 7: An unconfigured advertised address
//! is indistinguishable from the prior version. For any configuration with no
//! advertised address supplied, the serialized self member has
//! `addr == advertised_addr == listen_addr.to_string()` — both wire fields carry
//! the listen address, so the node behaves identically to the version before the
//! advertised field existed.
//!
//! This spans config resolution (no advertised supplied defaults to the listen
//! address), the self-member construction at startup, and the `member_to_proto`
//! serialization. As with the other membership integration tests, the serialized
//! self member is observed through the public `serve` + `DescribeCluster` path
//! (the handler serializes each served member via `member_to_proto`), with a
//! fresh node — and a fresh listen address — per generated node id. Each case
//! runs on its own short-lived `tokio` runtime that is dropped before the next.
//!
//! Validates: Requirements 7.3

use std::net::SocketAddr;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proptest::prelude::*;
use tonic::transport::Channel;

use vela_proto::v1;
use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_server::{serve, CliArgs, Config};

/// Process-unique counter making per-case data directories distinct.
static DATA_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique, writable data directory under the system temp directory for one
/// test node. Each case uses a fresh directory so per-case WAL locks never
/// collide; cleanup is left to the OS temp reaper.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!(
            "vela-prop-unset-parity-{}-{n}-{nanos}",
            process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

/// Reserve a free localhost port by binding then dropping an ephemeral listener.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

/// Connect a `VelaClient` to `addr`, retrying until the listener is reachable.
async fn connect_client(addr: SocketAddr) -> VelaClientClient<Channel> {
    let url = format!("http://{addr}");
    for _ in 0..200 {
        if let Ok(client) = VelaClientClient::connect(url.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("VelaClient at {addr} did not become reachable");
}

/// Bring up a single peerless node with **no** advertised address configured and
/// return its reported self member.
async fn self_member_unconfigured(node_id: &str, listen: SocketAddr) -> v1::Member {
    let config = Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(listen.to_string()),
        advertised_addr: None,
        peers: Vec::new(),
        replication_factor: Some("1".to_string()),
        data_dir: Some(unique_data_dir()),
    })
    .expect("valid test configuration");

    let server = tokio::spawn(async move {
        let _ = serve(config).await;
    });

    let mut client = connect_client(listen).await;
    let response = client
        .describe_cluster(v1::DescribeClusterRequest {})
        .await
        .expect("describe_cluster succeeds against a bound listener")
        .into_inner();
    server.abort();

    response
        .members
        .into_iter()
        .find(|m| m.id == node_id)
        .expect("the node reports itself as a member")
}

/// Build a short-lived multi-threaded runtime for one case.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build a tokio runtime for the case")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: advertised-listeners, Property 7
    #[test]
    fn unconfigured_advertised_matches_listen_on_both_fields(
        node_id in proptest::string::string_regex("[A-Za-z0-9_-]{1,24}").expect("valid regex"),
    ) {
        let rt = runtime();
        let listen = free_addr();
        let member = rt.block_on(async { self_member_unconfigured(&node_id, listen).await });
        drop(rt);

        // With no advertised address configured, both wire fields carry the
        // listen address, so the node is indistinguishable from the pre-feature
        // version (Req 7.3).
        let listen = listen.to_string();
        prop_assert_eq!(&member.addr, &listen);
        prop_assert_eq!(&member.advertised_addr, &listen);
        prop_assert_eq!(&member.addr, &member.advertised_addr);
    }
}
