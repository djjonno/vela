//! Property test for leader re-resolution in `vela-client`'s dispatch engine.
//!
//! Feature: ctl-client-routing-and-repl, Property 8
//!
//! Property 8: Leader re-resolution on both NotLeader and transport failure. For
//! any dispatch attempt that fails with either a `NotLeader` response or a
//! transport/connection failure, the engine re-resolves the partition's leader
//! before the next attempt — from the redirect's leader hint when present and
//! resolvable, and otherwise by invalidating the cached leader and re-resolving
//! via `FindLeader` — and the subsequent attempt is directed at the re-resolved
//! address rather than the failed one (Requirement 3.2, 3.3, 10.1, 10.2).
//!
//! The re-resolution logic is internal to [`ClientCore::dispatch`], so this
//! exercises its *observable* behaviour through the public dispatch seam every
//! partition request flows through. Each case seeds the believed leader for a
//! partition to a deliberately *wrong* address ([`FAILED_ADDR`]), then dispatches
//! an operation that records every address it is handed and fails the **first**
//! attempt with the generated failure kind, succeeding afterwards:
//!
//! - `NotLeader { hint: "node-b" }` — a typed `VelaError` redirect naming a
//!   registry-resolvable node. The engine resolves the hint to that node's
//!   address through the registry, with no network call (Requirement 3.2, 10.1).
//! - a bare transport `Unavailable` — a connection failure to the believed
//!   leader. The engine invalidates the cached leader and re-resolves via
//!   `FindLeader`, which an in-process fake `VelaClient` server answers with
//!   `node-b` (Requirement 3.3, 10.2).
//!
//! In both cases the re-resolved leader is `node-b`, whose registered address is
//! the fake server's bound address. The property asserts the second attempt is
//! directed at that re-resolved address — different from the failed address it
//! started with — for every generated failure kind, topic, and partition.
//!
//! Dispatch is driven on an injected [`VirtualClock`] whose `sleep` advances
//! virtual time instantly, so the retry backoff (Requirement 3.4) costs no real
//! wall-clock time across the ≥100 cases while still bounding the budget.
//!
//! Validates: Requirements 3.2, 3.3, 10.1, 10.2

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use proptest::prelude::*;
use prost::Message as _;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use vela_client::{ClientCore, ClientError, Clock, Result};
use vela_proto::v1;
use vela_proto::v1::vela_client_server::{VelaClient as VelaClientService, VelaClientServer};
use vela_proto::v1::{
    ConsumeRequest, ConsumeResponse, CreateTopicRequest, CreateTopicResponse, DeleteTopicRequest,
    DeleteTopicResponse, DescribeClusterRequest, DescribeClusterResponse, DescribeTopicRequest,
    DescribeTopicResponse, FindLeaderRequest, FindLeaderResponse, ListTopicsRequest,
    ListTopicsResponse, ProduceRequest, ProduceResponse,
};

/// The believed-but-wrong leader address each partition is seeded with — the
/// address the first attempt is (incorrectly) directed at and which
/// re-resolution must move away from. A loopback port that is never dialled (the
/// recording operation returns its failure without connecting), kept distinct
/// from the fake server's OS-chosen port.
const FAILED_ADDR: &str = "http://127.0.0.1:1";

/// The node id the fake server names as the re-resolved leader, and the id the
/// `NotLeader` redirect hint carries. Resolves to the fake server's address via
/// the client registry.
const RESOLVED_NODE: &str = "node-b";

/// Which failure the first dispatch attempt returns — the two re-resolution
/// triggers Property 8 quantifies over.
#[derive(Debug, Clone, Copy)]
enum FailureKind {
    /// A `NotLeader` redirect carrying a registry-resolvable leader hint
    /// (Requirement 3.2, 10.1).
    NotLeader,
    /// A transport/connection failure to the believed leader (Requirement 3.3,
    /// 10.2).
    Transport,
}

fn failure_kind_strategy() -> impl Strategy<Value = FailureKind> {
    prop_oneof![Just(FailureKind::NotLeader), Just(FailureKind::Transport)]
}

/// Build the failure the first attempt returns, shaped exactly as it reaches the
/// dispatch loop.
///
/// `NotLeader` is a typed [`v1::VelaError`] encoded into a status's details (as
/// the server emits it on the wire); the transport code it travels on is
/// incidental because the typed `NotLeader` code is what the engine reads.
/// `Transport` is a bare `Unavailable` status with no typed error — a connection
/// failure.
fn first_failure(kind: FailureKind) -> ClientError {
    match kind {
        FailureKind::NotLeader => {
            let vela_error = v1::VelaError {
                code: v1::ErrorCode::NotLeader as i32,
                message: "not the leader for this partition".to_string(),
                leader: Some(RESOLVED_NODE.to_string()),
            };
            let details = prost::bytes::Bytes::from(vela_error.encode_to_vec());
            ClientError::Rpc(Box::new(tonic::Status::with_details(
                tonic::Code::FailedPrecondition,
                "not leader",
                details,
            )))
        }
        FailureKind::Transport => {
            ClientError::Rpc(Box::new(tonic::Status::unavailable("dial failed")))
        }
    }
}

/// A clock whose `sleep` advances virtual time instantly, so the dispatch
/// backoff (Requirement 3.4) costs no real wall-clock time.
///
/// Injected into [`ClientCore::with_clock`] in place of the production
/// `TokioClock`. `now()` advances by exactly the slept duration, so the
/// [`RetryBudget`](vela_client::RetryBudget) elapsed-time bound still progresses
/// (a never-succeeding loop would terminate), while the returned future is ready
/// immediately — letting the fake gRPC server run on an ordinary multi-thread
/// runtime rather than a paused one.
#[derive(Debug)]
struct VirtualClock {
    base: tokio::time::Instant,
    elapsed: Mutex<Duration>,
}

impl VirtualClock {
    fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
            elapsed: Mutex::new(Duration::ZERO),
        }
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> tokio::time::Instant {
        self.base + *self.elapsed.lock().expect("virtual clock mutex poisoned")
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        *self.elapsed.lock().expect("virtual clock mutex poisoned") += duration;
        Box::pin(std::future::ready(()))
    }
}

/// An in-process fake of the client-facing `VelaClient` service that answers
/// `FindLeader` with [`RESOLVED_NODE`], standing in for the cluster the
/// transport re-resolution path probes. Every other RPC is unused by this test.
#[derive(Clone, Default)]
struct FindLeaderServer;

#[tonic::async_trait]
impl VelaClientService for FindLeaderServer {
    async fn find_leader(
        &self,
        _request: Request<FindLeaderRequest>,
    ) -> std::result::Result<Response<FindLeaderResponse>, Status> {
        Ok(Response::new(FindLeaderResponse {
            leader: Some(RESOLVED_NODE.to_string()),
        }))
    }

    async fn produce(
        &self,
        _request: Request<ProduceRequest>,
    ) -> std::result::Result<Response<ProduceResponse>, Status> {
        Err(Status::unimplemented("produce is not used by this test"))
    }

    async fn consume(
        &self,
        _request: Request<ConsumeRequest>,
    ) -> std::result::Result<Response<ConsumeResponse>, Status> {
        Err(Status::unimplemented("consume is not used by this test"))
    }

    async fn create_topic(
        &self,
        _request: Request<CreateTopicRequest>,
    ) -> std::result::Result<Response<CreateTopicResponse>, Status> {
        Err(Status::unimplemented(
            "create_topic is not used by this test",
        ))
    }

    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> std::result::Result<Response<DeleteTopicResponse>, Status> {
        Err(Status::unimplemented(
            "delete_topic is not used by this test",
        ))
    }

    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> std::result::Result<Response<ListTopicsResponse>, Status> {
        Err(Status::unimplemented(
            "list_topics is not used by this test",
        ))
    }

    async fn describe_topic(
        &self,
        _request: Request<DescribeTopicRequest>,
    ) -> std::result::Result<Response<DescribeTopicResponse>, Status> {
        Err(Status::unimplemented(
            "describe_topic is not used by this test",
        ))
    }

    async fn describe_cluster(
        &self,
        _request: Request<DescribeClusterRequest>,
    ) -> std::result::Result<Response<DescribeClusterResponse>, Status> {
        Err(Status::unimplemented(
            "describe_cluster is not used by this test",
        ))
    }
}

/// A shared multi-thread runtime hosting one fake `FindLeader` server, bound on
/// an OS-chosen localhost port. Built once and reused across every proptest case
/// so the ≥100 cases share a single server rather than binding a fresh port
/// each. `node_b_addr` is the server's URL — the address `RESOLVED_NODE` is
/// registered to, i.e. the re-resolved leader's dialable address.
struct Harness {
    rt: tokio::runtime::Runtime,
    node_b_addr: String,
}

fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build multi-thread runtime");
        // Bind the listener before returning so the URL is already accepting
        // connections — the client never races server startup. The spawned
        // server task keeps running on the runtime's worker threads after this
        // `block_on` returns.
        let node_b_addr = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind ephemeral port");
            let port = listener.local_addr().expect("local addr").port();
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(VelaClientServer::new(FindLeaderServer))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .expect("fake server serves");
            });
            format!("http://127.0.0.1:{port}")
        });
        Harness { rt, node_b_addr }
    })
}

/// Run one dispatch whose first attempt fails with `kind`, on a fresh core wired
/// to the shared fake server and an instant [`VirtualClock`].
///
/// Returns the ordered addresses the operation was directed at and whether the
/// dispatch ultimately succeeded. `seen[0]` is the failed (seeded) address;
/// `seen[1]` (when present) is the address the engine re-resolved to.
fn run_case(kind: FailureKind, topic: &str, partition: u32) -> (Vec<String>, bool) {
    let harness = harness();
    harness.rt.block_on(async {
        // Register `node-b` at the fake server's address (so both the redirect
        // hint and a `FindLeader` answer resolve there); seed the believed
        // leader to the wrong address the first attempt will be directed at.
        let core = ClientCore::with_clock(
            [(RESOLVED_NODE.to_string(), harness.node_b_addr.clone())],
            Arc::new(VirtualClock::new()),
        );
        core.leaders().insert(topic, partition, FAILED_ADDR);

        let calls = Arc::new(AtomicU32::new(0));
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let calls_in = Arc::clone(&calls);
        let seen_in = Arc::clone(&seen);

        let result: Result<()> = core
            .dispatch(topic, partition, move |addr| {
                let calls = Arc::clone(&calls_in);
                let seen = Arc::clone(&seen_in);
                async move {
                    let attempt = calls.fetch_add(1, Ordering::SeqCst);
                    seen.lock().expect("seen mutex poisoned").push(addr);
                    if attempt == 0 {
                        Err(first_failure(kind))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;

        let directed = seen.lock().expect("seen mutex poisoned").clone();
        (directed, result.is_ok())
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: ctl-client-routing-and-repl, Property 8
    #[test]
    fn retry_targets_the_reresolved_leader_not_the_failed_one(
        kind in failure_kind_strategy(),
        topic in "[a-z]{1,8}",
        partition in 0u32..8,
    ) {
        let resolved_addr = &harness().node_b_addr;
        let (seen, ok) = run_case(kind, &topic, partition);

        // The retry against the re-resolved leader succeeds, so dispatch returns
        // `Ok` after exactly two attempts: the failed one, then the re-resolved.
        prop_assert!(ok, "dispatch should succeed once retried against the re-resolved leader");
        prop_assert_eq!(
            seen.len(),
            2,
            "expected one failed attempt then one successful retry, got {:?}",
            seen,
        );

        // The first attempt was directed at the believed-but-wrong leader.
        prop_assert_eq!(
            seen[0].as_str(),
            FAILED_ADDR,
            "the first attempt targets the seeded (failed) leader address",
        );

        // The core property (Requirement 3.2, 3.3, 10.1, 10.2): the retry is
        // directed at the re-resolved leader — `node-b`'s address — and *not*
        // the address that just failed, for both the `NotLeader` (hint via
        // registry) and `Transport` (`FindLeader`) re-resolution paths.
        prop_assert_ne!(
            seen[1].as_str(),
            seen[0].as_str(),
            "the retry must not reuse the failed address ({:?})", kind,
        );
        prop_assert_eq!(
            seen[1].as_str(),
            resolved_addr.as_str(),
            "the retry must target the re-resolved leader's address ({:?})", kind,
        );
    }
}
