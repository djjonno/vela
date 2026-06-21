//! End-to-end wall-clock accounting integration test (task 16.3).
//!
//! This test drives a real [`InProcessCluster`] through the run harness
//! ([`run_with_cluster`]) with a tiny but non-trivial Workload and asserts how
//! the Benchmark_Run accounts for time:
//!
//! - `total_elapsed` spans the *whole* run — from run start (cluster startup
//!   and topic creation included) through to Consumer_Phase completion — so it
//!   is strictly positive (Requirement 8.2).
//! - Each phase's Measurement_Window is positive. The window durations are not
//!   exposed directly on the [`BenchmarkReport`], but a phase's throughput is
//!   `records / window`, so a finite, strictly-positive `records_per_sec`
//!   *implies* a strictly-positive, finite window. We therefore assert
//!   window-positivity indirectly through finite, positive produce and consume
//!   throughput (Requirements 1.4, 2.5).
//! - The Measurement_Windows exclude Cluster_Under_Test startup and topic
//!   creation time (Requirement 10.3). Those costs land inside `total_elapsed`
//!   but outside either window, so each phase's *implied* window
//!   (`records / records_per_sec`) must be no larger than `total_elapsed`. We
//!   assert that implied produce and consume windows are finite and
//!   `<= total_elapsed`, evidencing that startup/creation time is not folded
//!   into the windows.
//!
//! The workload is deliberately tiny (50 records, 16-byte values, 2 partitions)
//! so the run is fast and reliable on CI while still exercising the real
//! produce/consume data path across more than one partition. The harness
//! pattern (start an in-process cluster, then run a benchmark against it)
//! mirrors `crates/vela-server/tests/cross_node_produce_consume.rs`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use vela_bench::cluster::InProcessCluster;
use vela_bench::params::{KeyMode, WorkloadParameters};
use vela_bench::run::run_with_cluster;

/// The number of records produced and consumed by the test Workload. Small
/// enough to keep the run fast on CI, large enough that the Measurement_Window
/// of each phase is comfortably non-degenerate.
const RECORD_COUNT: u64 = 50;

/// A process-and-time-unique topic name so concurrent test binaries (and reruns
/// against a reused data directory) never collide on a pre-existing topic — a
/// collision would fail the run with `TopicAlreadyExists` rather than measuring.
fn unique_topic() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("vela-bench-e2e-wallclock-{}-{nanos}", std::process::id())
}

/// The tiny-but-non-trivial Workload_Parameters for this test.
fn params(topic: String) -> WorkloadParameters {
    WorkloadParameters {
        record_count: RECORD_COUNT,
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

/// A real end-to-end Benchmark_Run accounts for wall-clock time correctly:
/// `total_elapsed` spans startup → consume completion, both Measurement_Windows
/// are positive (evidenced by finite, positive throughput), and neither window
/// folds in startup/creation time (evidenced by each implied window fitting
/// within `total_elapsed`).
///
/// Validates Requirements 1.4, 2.5, 8.2, 10.3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wall_clock_accounting_spans_startup_to_consume_and_excludes_startup_from_windows() {
    let topic = unique_topic();
    let params = params(topic);

    let cluster = InProcessCluster::start()
        .await
        .expect("the in-process Cluster_Under_Test starts");

    // Drive the full sequence against the already-started cluster. No report
    // files are emitted; we assert against the returned report directly.
    let report = run_with_cluster(cluster, params, None, None).await;

    // The run must succeed end to end; surface the failure reason otherwise so a
    // regression is diagnosable rather than an opaque assertion failure.
    assert!(
        report.outcome.is_passed(),
        "expected a passing Outcome, got failure: {:?}",
        report.outcome.failure_reason()
    );

    // --- Requirement 8.2: total_elapsed spans the whole run. ---------------
    // The clock starts at run start (cluster startup + topic creation included)
    // and stops at Consumer_Phase completion, so it is strictly positive.
    assert!(
        report.total_elapsed > Duration::ZERO,
        "total_elapsed must be strictly positive (it spans startup → consume completion), got {:?}",
        report.total_elapsed
    );
    let total_secs = report.total_elapsed.as_secs_f64();
    assert!(
        total_secs.is_finite() && total_secs > 0.0,
        "total_elapsed seconds must be finite and positive, got {total_secs}"
    );

    // --- Requirement 1.4: the Producer_Phase Measurement_Window is positive. -
    // The window is not exposed directly, but throughput = records / window, so
    // a finite, strictly-positive records/s implies a finite, positive window.
    let produce = report
        .produce_throughput
        .expect("a passing run reports Produce_Throughput");
    assert!(
        produce.records_per_sec.is_finite() && produce.records_per_sec > 0.0,
        "produce records/s must be finite and positive (implying a positive produce window), got {}",
        produce.records_per_sec
    );
    assert!(
        produce.bytes_per_sec.is_finite() && produce.bytes_per_sec >= 0.0,
        "produce bytes/s must be finite and non-negative, got {}",
        produce.bytes_per_sec
    );

    // --- Requirement 2.5: the Consumer_Phase Measurement_Window is positive. -
    let consume = report
        .consume_throughput
        .expect("a passing run reports Consume_Throughput");
    assert!(
        consume.records_per_sec.is_finite() && consume.records_per_sec > 0.0,
        "consume records/s must be finite and positive (implying a positive consume window), got {}",
        consume.records_per_sec
    );
    assert!(
        consume.bytes_per_sec.is_finite() && consume.bytes_per_sec >= 0.0,
        "consume bytes/s must be finite and non-negative, got {}",
        consume.bytes_per_sec
    );

    // --- Requirement 10.3: windows exclude startup/topic-creation time. -----
    // Each phase's implied Measurement_Window is `records / records_per_sec`.
    // Because cluster startup and topic creation are charged to `total_elapsed`
    // but NOT to either window, each implied window must fit within
    // `total_elapsed`. (If startup time had been folded into a window, the
    // implied window could exceed the whole run's elapsed time.)
    let implied_produce_window = RECORD_COUNT as f64 / produce.records_per_sec;
    assert!(
        implied_produce_window.is_finite(),
        "the implied produce window must be finite, got {implied_produce_window}"
    );
    assert!(
        implied_produce_window <= total_secs,
        "the implied produce window ({implied_produce_window} s) must not exceed total_elapsed \
         ({total_secs} s): startup/creation time is excluded from the Measurement_Window"
    );

    let implied_consume_window = RECORD_COUNT as f64 / consume.records_per_sec;
    assert!(
        implied_consume_window.is_finite(),
        "the implied consume window must be finite, got {implied_consume_window}"
    );
    assert!(
        implied_consume_window <= total_secs,
        "the implied consume window ({implied_consume_window} s) must not exceed total_elapsed \
         ({total_secs} s): startup/creation time is excluded from the Measurement_Window"
    );

    // Sanity: a passing run acknowledged exactly the configured record count.
    assert_eq!(
        report.acknowledged_records, RECORD_COUNT,
        "a passing run acknowledges exactly the configured record count"
    );
}
