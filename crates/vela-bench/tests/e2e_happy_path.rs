//! End-to-end happy-path integration test for the Throughput_Benchmark (task 16.1).
//!
//! This is a real end-to-end test: it stands up an actual in-process Vela
//! cluster ([`InProcessCluster`]) and drives one full Benchmark_Run through the
//! crate's harness ([`run_with_cluster`]) against a tiny workload, exactly as
//! the binary would for a real run. It proves the create → produce → consume →
//! verify sequence completes and reaches [`Outcome::Passed`] end to end, against
//! a multi-partition topic, through the real `vela-client` Producer/Consumer
//! APIs and the per-partition Raft groups of a started cluster.
//!
//! The harness ([`run_with_cluster`]) awaits cluster readiness internally
//! (within the configured startup budget) before driving any traffic, so the
//! test simply starts the cluster and hands it to the harness. A `Passed`
//! Outcome already establishes the data-integrity guarantees the run verifies
//! internally: the Consumer_Phase reads each partition from offset 0, the total
//! number of records read equals the Acknowledged_Record count, and every read
//! payload matches the deterministic payload produced for its position
//! (Requirements 5.1, 5.2). This test additionally asserts the run's reported
//! signals are consistent with that passing run.
//!
//! Requirements: 1.1, 2.1, 2.2, 3.1, 3.2, 3.3.
//!
//! The workload is deliberately tiny (a small record count, a small value size,
//! a few partitions) so the test is fast and reliable in CI while still
//! exercising the full multi-partition path. The topic name carries the process
//! id and a high-resolution timestamp so concurrent or repeated test runs never
//! collide on a pre-existing topic (which the harness would reject).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use vela_bench::cluster::InProcessCluster;
use vela_bench::outcome::Outcome;
use vela_bench::params::{KeyMode, WorkloadParameters};
use vela_bench::run::run_with_cluster;

/// A unique topic name for this run, embedding the process id and a
/// high-resolution timestamp so repeated or concurrent runs of this test never
/// target an already-existing topic (the harness aborts on a pre-existing
/// topic, Requirement 3.4).
fn unique_topic() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("vela-bench-e2e-{}-{nanos}", std::process::id())
}

/// A tiny, fast, multi-partition Workload_Parameters for the happy-path run.
///
/// `value_size = 16` is comfortably `>= 8` so each payload embeds its record
/// position (the integrity check reads it back), and `partition_count = 3`
/// (`>= 2`) exercises the per-partition consume path across more than one
/// partition. Warmup is zero so every produced record is measured and verified.
fn tiny_params() -> WorkloadParameters {
    WorkloadParameters {
        record_count: 50,
        value_size: 16,
        key_mode: KeyMode::Keyless,
        partition_count: 3,
        producer_concurrency: 4,
        topic: unique_topic(),
        warmup: 0,
        time_budget: Duration::from_secs(60),
        startup_budget: Duration::from_secs(30),
        floor_produce_rps: None,
        floor_consume_rps: None,
    }
}

/// Requirements 1.1, 2.1, 2.2, 3.1, 3.2, 3.3 — against a real in-process
/// cluster, a tiny create → produce → consume → verify Benchmark_Run completes
/// and reaches `Passed`, with the reported signals consistent with that run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_happy_path_run_passes_against_real_cluster() {
    let params = tiny_params();

    // Start a real, single-node in-process Vela cluster (Requirement 3.2). The
    // harness awaits its readiness internally before driving any traffic.
    let cluster = InProcessCluster::start()
        .await
        .expect("the in-process cluster starts");

    // Drive one full Benchmark_Run end to end through the real `vela-client`
    // Producer/Consumer APIs: create the multi-partition topic, produce, consume
    // per partition from offset 0, and verify integrity (Requirements 3.1, 3.3).
    let report = run_with_cluster(cluster, params.clone(), None, None).await;

    // A `Passed` Outcome means every gating condition held: the topic was
    // created, produce and consume completed, and the run's internal integrity
    // verification confirmed the records read equal the records acked and each
    // payload matches its position (Requirements 1.1, 2.1, 2.2, 5.1, 5.2). On a
    // failure, surface the reason so the integration failure is debuggable.
    if let Outcome::Failed { reason } = &report.outcome {
        panic!("end-to-end happy-path run failed: {reason:?}");
    }
    assert_eq!(
        report.outcome,
        Outcome::Passed,
        "the tiny end-to-end run passes against a real cluster"
    );

    // Every produced record was acknowledged (Requirement 1.1): the
    // Acknowledged_Record count equals the configured record count. Because the
    // run passed, the Consumer_Phase read exactly this many records back, each
    // partition from offset 0, with total read == acked (Requirements 2.1, 2.2).
    assert_eq!(
        report.acknowledged_records, params.record_count,
        "every produced record was acknowledged"
    );

    // Both phases completed, so both throughput figures are present (a phase
    // that did not complete would carry `None`).
    assert!(
        report.produce_throughput.is_some(),
        "a completed Producer_Phase reports a Produce_Throughput"
    );
    assert!(
        report.consume_throughput.is_some(),
        "a completed Consumer_Phase reports a Consume_Throughput"
    );

    // The reported payload volume matches the produced workload exactly:
    // record_count records of value_size bytes each.
    assert_eq!(
        report.total_payload_bytes,
        params.record_count * params.value_size as u64,
        "total payload bytes equals record_count * value_size"
    );
}
