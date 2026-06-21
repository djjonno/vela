//! End-to-end warmup-exclusion integration test for the Throughput_Benchmark
//! (task 16.4).
//!
//! This is a real end-to-end test: it stands up an actual in-process Vela
//! cluster ([`InProcessCluster`]) and drives one full Benchmark_Run through the
//! crate's harness ([`run_with_cluster`]) with a warmup count greater than
//! zero, exactly as the binary would for a real run. It proves that, with
//! `warmup > 0`, the create → produce → consume → verify sequence still
//! completes and reaches [`Outcome::Passed`]: the warmup operations are
//! executed and then excluded from both Measurement_Windows, the measured
//! operations run, and every produced record is still acknowledged and read
//! back intact (Requirements 10.1, 10.2).
//!
//! Requirements: 10.1, 10.2.
//!
//! ## What is observable
//!
//! The [`BenchmarkReport`](vela_bench::report::BenchmarkReport) exposes the
//! Acknowledged_Record count and the two throughput figures, but not the raw
//! measured record count or the Measurement_Window durations. So warmup
//! exclusion is asserted via two complementary angles:
//!
//! 1. **End to end** — with `warmup > 0` the run still `Passed`: all
//!    `record_count` records are produced and acknowledged
//!    (`acknowledged_records == record_count`), the Consumer_Phase reads every
//!    acknowledged record back, the run's internal integrity verification holds,
//!    and both `produce_throughput` / `consume_throughput` are present, positive,
//!    and finite (computed over the measured subset, not the warmup prefix).
//! 2. **The measured-set rule directly** — [`measured_set`] is the pure
//!    warmup/measured selection rule both phases use to decide which positions
//!    fall inside the Measurement_Window. Asserting
//!    `measured_set(record_count, warmup) == warmup..record_count` (length
//!    `record_count - warmup`) directly evidences that exactly the first
//!    `warmup` operations are excluded from the measured set (Requirements 10.1,
//!    10.2).
//!
//! The workload is deliberately tiny so the test is fast and reliable in CI
//! while still exercising the warmup-then-measured path across more than one
//! partition. The topic name carries the process id and a high-resolution
//! timestamp so concurrent or repeated test runs never collide on a
//! pre-existing topic (which the harness would reject).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use vela_bench::cluster::InProcessCluster;
use vela_bench::outcome::Outcome;
use vela_bench::params::{KeyMode, WorkloadParameters};
use vela_bench::produce_phase::measured_set;
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
    format!("vela-bench-warmup-{}-{nanos}", std::process::id())
}

/// A tiny, fast, multi-partition Workload_Parameters with a non-zero warmup.
///
/// `warmup = 10` is strictly less than `record_count = 50`, so the
/// configuration validates and `record_count - warmup = 40` operations are
/// measured in each phase. `value_size = 16` is `>= 8` so each payload embeds
/// its record position (the integrity check reads it back), and
/// `partition_count = 2` (`>= 2`) exercises the per-partition path across more
/// than one partition.
fn warmup_params() -> WorkloadParameters {
    WorkloadParameters {
        record_count: 50,
        value_size: 16,
        key_mode: KeyMode::Keyless,
        partition_count: 2,
        producer_concurrency: 4,
        topic: unique_topic(),
        warmup: 10,
        time_budget: Duration::from_secs(60),
        startup_budget: Duration::from_secs(30),
        floor_produce_rps: None,
        floor_consume_rps: None,
    }
}

/// Requirements 10.1, 10.2 — with `warmup > 0`, a real end-to-end Benchmark_Run
/// still passes (warmup operations execute then are excluded from both
/// Measurement_Windows), and the measured-set rule excludes exactly the warmup
/// prefix from both phases' Measurement_Windows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_warmup_is_excluded_from_both_measurement_windows() {
    let params = warmup_params();

    // The measured-set rule both phases use to decide which positions fall
    // inside the Measurement_Window. With `warmup > 0`, exactly the first
    // `warmup` operations (`0..warmup`) are excluded; the measured set is
    // `warmup..record_count` and has length `record_count - warmup`
    // (Requirements 10.1, 10.2).
    let measured = measured_set(params.record_count, params.warmup);
    assert_eq!(
        measured,
        params.warmup..params.record_count,
        "the measured set excludes exactly the warmup prefix from each phase's window"
    );
    assert_eq!(
        measured.clone().count() as u64,
        params.record_count - params.warmup,
        "the measured set has length record_count - warmup"
    );
    // No warmup position is measured; the first measured position is `warmup`.
    assert!(
        measured.clone().all(|position| position >= params.warmup),
        "no warmup position falls inside the Measurement_Window"
    );
    assert!(
        !measured.contains(&0),
        "position 0 is a warmup operation and is excluded from the window"
    );

    // Start a real, single-node in-process Vela cluster (Requirement 3.2). The
    // harness awaits its readiness internally before driving any traffic.
    let cluster = InProcessCluster::start()
        .await
        .expect("the in-process cluster starts");

    // Drive one full Benchmark_Run end to end with `warmup > 0`: the warmup
    // produces/consumes run first and are excluded from each phase's window,
    // then the measured operations run, then integrity is verified.
    let report = run_with_cluster(cluster, params.clone(), None, None).await;

    // A `Passed` Outcome means the warmup operations completed and were excluded,
    // the measured operations ran, and the run's internal integrity verification
    // confirmed every acknowledged record was read back intact. On a failure,
    // surface the reason so the integration failure is debuggable.
    if let Outcome::Failed { reason } = &report.outcome {
        panic!("end-to-end warmup-exclusion run failed: {reason:?}");
    }
    assert_eq!(
        report.outcome,
        Outcome::Passed,
        "the warmup run passes against a real cluster"
    );

    // Acknowledged_Records counts every acked record (warmup included), so it
    // equals the configured record count even though only the measured subset
    // is counted toward throughput (Requirement 10.1). Because the run passed,
    // the Consumer_Phase read exactly this many records back.
    assert_eq!(
        report.acknowledged_records, params.record_count,
        "all record_count records are produced and acknowledged with warmup > 0"
    );

    // Both phases completed, so both throughput figures are present and reflect
    // the measured subset over its Measurement_Window: positive and finite
    // (never NaN/infinite, and never a misleading zero).
    let produce = report
        .produce_throughput
        .expect("a completed Producer_Phase reports a Produce_Throughput");
    let consume = report
        .consume_throughput
        .expect("a completed Consumer_Phase reports a Consume_Throughput");

    for (label, t) in [("produce", produce), ("consume", consume)] {
        assert!(
            t.records_per_sec.is_finite() && t.records_per_sec > 0.0,
            "{label} records/s is positive and finite, got {}",
            t.records_per_sec
        );
        assert!(
            t.bytes_per_sec.is_finite() && t.bytes_per_sec > 0.0,
            "{label} bytes/s is positive and finite, got {}",
            t.bytes_per_sec
        );
    }
}
