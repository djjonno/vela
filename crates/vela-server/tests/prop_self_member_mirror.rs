//! Property test for the self-member mapping in `vela-server`.
//!
//! Feature: advertised-listeners, Property 3: Self member mirrors configuration.
//! For any resolved `Config`, the self `Member` built at startup has
//! `addr == config.listen_addr.to_string()` and
//! `advertised_addr == config.advertised_addr`.
//!
//! The self member is private node state, so — like the crate's other
//! membership integration tests — the property observes it through the public
//! `serve` + `DescribeCluster` path: a real node is brought up for each resolved
//! configuration, and the self member it reports back is asserted to mirror the
//! configuration's listen and advertised addresses. The advertised input is
//! drawn from the absent / blank / padded / arbitrary shapes so the resolved
//! `config.advertised_addr` (the value the self member must mirror) varies across
//! the whole resolution space. Each case runs on its own short-lived `tokio`
//! runtime that is dropped before the next, so the node's background tasks and
//! its metadata WAL lock are released between cases.
//!
//! Validates: Requirements 2.1, 2.2

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
/// test node. Cleanup is left to the OS temp reaper; each case uses a fresh
/// directory so per-case WAL locks never collide.
fn unique_data_dir() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();
    let n = DATA_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!(
            "vela-prop-self-member-{}-{n}-{nanos}",
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

/// Connect a `VelaClient` to `addr`, retrying until the freshly spawned listener
/// accepts connections.
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

/// Bring up a single peerless node for `(node_id, listen, advertised)`, query
/// `DescribeCluster`, and return its reported self member together with the
/// configuration's resolved advertised address.
async fn self_member(
    node_id: &str,
    listen: SocketAddr,
    advertised: Option<String>,
) -> (v1::Member, String) {
    let config = Config::from_cli(CliArgs {
        node_id: Some(node_id.to_string()),
        listen_addr: Some(listen.to_string()),
        advertised_addr: advertised,
        peers: Vec::new(),
        replication_factor: Some("1".to_string()),
        data_dir: Some(unique_data_dir()),
    })
    .expect("valid test configuration");
    let resolved_advertised = config.advertised_addr.clone();

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

    let member = response
        .members
        .into_iter()
        .find(|m| m.id == node_id)
        .expect("the node reports itself as a member");
    (member, resolved_advertised)
}

/// Generate an advertised input spanning absent / blank / padded / arbitrary.
fn advertised_input_strategy() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        Just(None),
        proptest::string::string_regex("[ \\t]{0,4}")
            .expect("valid regex")
            .prop_map(Some),
        proptest::string::string_regex("[ \\t]{0,2}[A-Za-z0-9_.:-]{1,32}[ \\t]{0,2}")
            .expect("valid regex")
            .prop_map(Some),
    ]
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

    // Feature: advertised-listeners, Property 3
    #[test]
    fn self_member_mirrors_config(
        node_id in proptest::string::string_regex("[A-Za-z0-9_-]{1,24}").expect("valid regex"),
        advertised in advertised_input_strategy(),
    ) {
        let rt = runtime();
        let listen = free_addr();
        let (member, resolved_advertised) =
            rt.block_on(async { self_member(&node_id, listen, advertised).await });
        // Drop the runtime to cancel the node's background tasks and release its
        // metadata WAL lock before the next case.
        drop(rt);

        // The self member mirrors configuration: its bind address is the listen
        // address (Req 2.2) and its advertised address is the resolved
        // configuration value (Req 2.1).
        prop_assert_eq!(member.addr, listen.to_string());
        prop_assert_eq!(member.advertised_addr, resolved_advertised);
    }
}
