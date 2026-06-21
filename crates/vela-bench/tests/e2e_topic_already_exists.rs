//! Topic-already-exists end-to-end integration test (task 16.2).
//!
//! This is a real end-to-end test: it stands up an in-process
//! Cluster_Under_Test, **pre-creates** the target topic through a real
//! `vela-client`, then drives one Benchmark_Run against that same cluster and
//! asserts the run refuses to measure against a pre-populated topic
//! (Requirement 3.4).
//!
//! Per the design's Benchmark_Run lifecycle, the harness pre-checks the target
//! topic via `describe_topic` *before* the Producer_Phase: a topic that already
//! exists aborts the run with `FailureReason::TopicAlreadyExists` and never
//! enters producing. So this test proves three things at once: the precheck
//! runs against the real cluster, it fires when the topic genuinely exists, and
//! the failure short-circuits before any record is acknowledged.
//!
//! ## Ordering note (`run_with_cluster` consumes the cluster)
//!
//! [`run_with_cluster`] takes the [`InProcessCluster`] by value, so the
//! pre-existing topic must be created *before* the cluster is moved into it.
//! [`Cluster::bootstrap`] returns owned `(node_id, address)` `String`s, so we
//! capture the bootstrap pairs and await readiness first, build a `VelaClient`
//! from those owned addresses to create the topic, and only then hand the
//! cluster to `run_with_cluster`.

use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use vela_bench::cluster::{Cluster, InProcessCluster};
use vela_bench::outcome::{FailureReason, Outcome};
use vela_bench::params::{KeyMode, WorkloadParameters};
use vela_bench::run::run_with_cluster;
use vela_client::{LogBackend, VelaClient};

/// Makes each test topic name unique within a process so repeated runs (and
/// concurrent tests) never collide on a real cluster.
static TOPIC_NONCE: AtomicU64 = AtomicU64::new(0);

/// A unique, valid (`1..=255` char) target topic name for one Benchmark_Run.
fn unique_topic() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = TOPIC_NONCE.fetch_add(1, Ordering::Relaxed);
    format!("vela-bench-exists-{}-{n}-{nanos}", process::id())
}

/// A small, valid Workload that would otherwise produce and consume quickly —
/// the run must fail at the pre-existing-topic precheck before any of it runs.
fn params(topic: String) -> WorkloadParameters {
    WorkloadParameters {
        record_count: 16,
        value_size: 16,
        key_mode: KeyMode::Keyless,
        partition_count: 2,
        producer_concurrency: 4,
        topic,
        warmup: 0,
        time_budget: Duration::from_secs(60),
        startup_budget: Duration::from_secs(30),
        floor_produce_rps: None,
        floor_consume_rps: None,
    }
}

/// Requirement 3.4 — a target topic that already exists at the start of a
/// Benchmark_Run aborts the run with a failing Outcome and a descriptive
/// `TopicAlreadyExists` reason, *before any producing*, rather than measuring
/// against a pre-populated topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_existing_topic_fails_before_producing() {
    let params = params(unique_topic());

    // Stand up a real in-process Cluster_Under_Test.
    let cluster = InProcessCluster::start()
        .await
        .expect("the in-process cluster starts");

    // Capture the bootstrap pairs (owned Strings) and await readiness *before*
    // the cluster is moved into `run_with_cluster`, which consumes it.
    let bootstrap = cluster.bootstrap();
    cluster
        .await_ready(Duration::from_secs(30))
        .await
        .expect("the cluster becomes ready within the budget");

    // Pre-create the target topic through the real client, exactly as a prior
    // run (or an external client) would have, so the benchmark's precheck finds
    // it already present.
    let client = VelaClient::new(bootstrap);
    client
        .admin()
        .create_topic(&params.topic, params.partition_count, LogBackend::default())
        .await
        .expect("the target topic is pre-created on the cluster");

    // Drive the Benchmark_Run against the same cluster. The harness pre-checks
    // the topic via `describe_topic` and must abort before the Producer_Phase.
    let report = run_with_cluster(cluster, params.clone(), None, None).await;

    // The Outcome is a failure carrying the typed `TopicAlreadyExists` reason
    // naming the target topic (Requirement 3.4).
    assert!(
        matches!(report.outcome, Outcome::Failed { .. }),
        "a pre-existing topic must yield a failing Outcome, got {:?}",
        report.outcome
    );
    assert_eq!(
        report.failure_reason,
        Some(FailureReason::TopicAlreadyExists {
            topic: params.topic.clone(),
        }),
        "the failure reason names the pre-existing target topic",
    );

    // The run never entered the Producer_Phase: no throughput is presented and
    // nothing was acknowledged (failed before producing).
    assert!(
        report.produce_throughput.is_none(),
        "no Produce_Throughput is presented when the run fails before producing",
    );
    assert!(
        report.consume_throughput.is_none(),
        "no Consume_Throughput is presented when the run fails before producing",
    );
    assert_eq!(
        report.acknowledged_records, 0,
        "no records are acknowledged when the run fails before producing",
    );
}
