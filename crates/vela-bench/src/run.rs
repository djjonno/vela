//! The harness that sequences a single Benchmark_Run and owns the time budget.
//!
//! [`run`] is the crate's top-level entry point: it stands up an in-process
//! Cluster_Under_Test, drives one Benchmark_Run end to end, assembles the single
//! [`BenchmarkReport`] every output renders from, emits the JSON / stdout / HTML
//! outputs, and returns the report so the binary (`main.rs`) can map its
//! [`Outcome`] to a process exit code. [`run_with_cluster`] is the same harness
//! over a caller-supplied [`Cluster`], so the sequencing is exercised against an
//! already-started cluster (the integration tests in task 16.x drive a real
//! [`InProcessCluster`] through it).
//!
//! ## Sequencing (design "Benchmark_Run lifecycle")
//!
//! 1. **Validate** the Workload_Parameters before any side effect — no cluster
//!    start, no topic creation (Requirements 3.5, 4.5, 4.6, 10.5). An invalid
//!    parameter yields `FailureReason::InvalidParameter` immediately.
//! 2. **Start the overall time-budget clock** at run start, *inclusive* of
//!    cluster startup and topic creation (Requirement 8.2).
//! 3. **Start the cluster** and [`await_ready`](Cluster::await_ready) within the
//!    startup budget (Requirement 9.3 → `ClusterNotReady`).
//! 4. **Pre-check** the target topic via `describe_topic`: if it already exists,
//!    abort with `TopicAlreadyExists` rather than measuring against a
//!    pre-populated topic (Requirement 3.4).
//! 5. **Create** the topic with the configured partition count
//!    (Requirements 3.3, 3.6 → `TopicCreationFailed`).
//! 6. **Producer_Phase** then **Consumer_Phase**, each through the real
//!    `vela-client` Producer / Consumer APIs behind the
//!    [`ProduceSink`] / [`ConsumeSource`] seams.
//! 7. **Verify** that the consumed records reconstruct the produced Workload
//!    (Requirements 5.1, 5.2, 5.5).
//! 8. **Assemble** the [`BenchmarkReport`] and the [`Outcome`] and emit the three
//!    outputs (Requirement 6.x).
//!
//! ## Time budget (Requirement 8.2, 8.3, 5.4)
//!
//! The cluster-ready → topic-create → produce → consume → verify portion runs
//! inside a single [`tokio::time::timeout`] bounded by the per-run time budget.
//! Cluster startup and topic creation are *inside* this overall budget
//! (Requirement 8.2) but are naturally *excluded* from both Measurement_Windows,
//! because each phase opens its own window at its first measured operation
//! (Requirement 10.3). On elapse the run terminates with
//! `TimeBudgetExceeded { budget, read, expected }`, retaining the counts
//! recorded so far (Requirement 5.4).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use vela_client::{LogBackend, VelaClient};
use vela_proto::v1::vela_client_client::VelaClientClient;
use vela_proto::v1::FindLeaderRequest;

use crate::cluster::{Cluster, InProcessCluster};
use crate::consume_phase::{run_consumer_phase, ConsumeFailure, ConsumeSource, ConsumedBatch};
use crate::html::write_html_file;
use crate::metrics::{throughput, Throughput};
use crate::outcome::{determine_outcome, FailureReason, Phase};
use crate::params::WorkloadParameters;
use crate::produce_phase::{run_producer_phase, ProduceFailure, ProduceSink};
use crate::report::BenchmarkReport;
use crate::verify::{verify_consumed, VerificationError};
use crate::BenchError;

/// Run one Benchmark_Run against a freshly started in-process Cluster_Under_Test.
///
/// Validates `params` before any side effect, starts the overall time-budget
/// clock, stands up an [`InProcessCluster`], and drives the full sequence,
/// returning the single [`BenchmarkReport`] (also emitted as JSON to
/// `report_json` when given, as a stdout summary, and as HTML to `report_html`
/// when given). The returned report's [`Outcome`] is what `main.rs` maps to a
/// process exit code (Requirement 7.3).
pub async fn run(
    params: WorkloadParameters,
    report_json: Option<PathBuf>,
    report_html: Option<PathBuf>,
) -> BenchmarkReport {
    // Reject an out-of-range parameter before starting a cluster or creating a
    // topic — no side effects (Requirements 3.5, 4.5, 4.6, 10.5).
    if let Some(report) = invalid_params_report(&params) {
        emit_outputs(&report, report_json.as_deref(), report_html.as_deref());
        return report;
    }

    // Start the overall time-budget clock at run start, inclusive of cluster
    // startup and topic creation (Requirement 8.2).
    let start = Instant::now();

    match InProcessCluster::start().await {
        Ok(cluster) => run_started(cluster, params, start, report_json, report_html).await,
        Err(error) => {
            // The cluster could not even be started (port reservation / config):
            // surface it as a not-ready failure against the startup budget.
            let reason = bench_error_to_reason(&error, &params);
            let report = assemble_report(params, PhaseData::failed(reason), start.elapsed());
            emit_outputs(&report, report_json.as_deref(), report_html.as_deref());
            report
        }
    }
}

/// Run one Benchmark_Run against a caller-supplied, already-started [`Cluster`].
///
/// Identical sequencing to [`run`], but the [`Cluster`] is provided by the
/// caller (the integration tests drive a real [`InProcessCluster`]; unit tests
/// drive a fake). Validation still happens first; the overall time-budget clock
/// starts immediately after a successful validation.
pub async fn run_with_cluster<C: Cluster>(
    cluster: C,
    params: WorkloadParameters,
    report_json: Option<PathBuf>,
    report_html: Option<PathBuf>,
) -> BenchmarkReport {
    if let Some(report) = invalid_params_report(&params) {
        // Tear the supplied cluster down even though we never drove it, so the
        // caller is not left owning a live cluster.
        let _ = cluster.shutdown().await;
        emit_outputs(&report, report_json.as_deref(), report_html.as_deref());
        return report;
    }

    let start = Instant::now();
    run_started(cluster, params, start, report_json, report_html).await
}

/// Drive the time-bounded portion of the run against a started `cluster`, then
/// assemble and emit the report.
///
/// The cluster-ready → create → produce → consume → verify sequence runs inside
/// a single [`tokio::time::timeout`] bounded by the per-run time budget; on
/// elapse the run fails with `TimeBudgetExceeded`, retaining the counts recorded
/// so far (Requirement 5.4, 8.3). The cluster is shut down (best effort)
/// regardless of outcome.
async fn run_started<C: Cluster>(
    cluster: C,
    params: WorkloadParameters,
    start: Instant,
    report_json: Option<PathBuf>,
    report_html: Option<PathBuf>,
) -> BenchmarkReport {
    let progress = Arc::new(Mutex::new(RunProgress::new(params.record_count)));

    let data = match tokio::time::timeout(
        params.time_budget,
        run_phases(&cluster, &params, Arc::clone(&progress)),
    )
    .await
    {
        Ok(data) => data,
        // The overall budget elapsed mid-run: terminate retaining the counts
        // recorded so far (Requirement 5.4, 8.3).
        Err(_elapsed) => timeout_data(&progress, params.time_budget),
    };

    // `total_elapsed` spans run start (cluster startup included) to here — the
    // completion of the run (Requirement 8.2).
    let total_elapsed = start.elapsed();

    // Tear the cluster down regardless of outcome (best effort).
    let _ = cluster.shutdown().await;

    let report = assemble_report(params, data, total_elapsed);
    emit_outputs(&report, report_json.as_deref(), report_html.as_deref());
    report
}

/// The measured signals gathered while sequencing a run, fed to
/// [`assemble_report`].
///
/// `prior_failure` carries any failure detected before floor gating (an
/// operation error, an unready cluster, a zero window, an integrity violation,
/// or an exceeded time budget). The two throughput figures are `None` when their
/// phase did not complete, so the report renders them as explicitly absent
/// rather than a measured zero (Requirement 6.5).
#[derive(Debug, Default)]
struct PhaseData {
    prior_failure: Option<FailureReason>,
    produce_throughput: Option<Throughput>,
    consume_throughput: Option<Throughput>,
    acknowledged_records: u64,
    total_payload_bytes: u64,
}

impl PhaseData {
    /// A `PhaseData` carrying only a pre-phase failure (no measured throughput).
    fn failed(reason: FailureReason) -> Self {
        Self {
            prior_failure: Some(reason),
            ..Self::default()
        }
    }
}

/// Counts retained as the run progresses, so a time-budget elapse can report the
/// records read and the records expected even though the in-flight run future is
/// cancelled (Requirement 5.4).
#[derive(Debug, Default)]
struct RunProgress {
    /// Acknowledged_Records counted by the Producer_Phase.
    acked: u64,
    /// Payload bytes acknowledged by the Producer_Phase.
    acked_bytes: u64,
    /// Records read so far by the Consumer_Phase (warmup included).
    read: u64,
    /// Records expected to be read: `record_count` until the Producer_Phase
    /// completes, then the Acknowledged_Record count.
    expected: u64,
}

impl RunProgress {
    fn new(record_count: u64) -> Self {
        Self {
            expected: record_count,
            ..Self::default()
        }
    }
}

/// Build the [`PhaseData`] for a time-budget elapse, retaining the counts
/// recorded so far (Requirement 5.4, 8.3).
fn timeout_data(progress: &Mutex<RunProgress>, budget: Duration) -> PhaseData {
    let progress = progress.lock().expect("run progress mutex poisoned");
    PhaseData {
        prior_failure: Some(FailureReason::TimeBudgetExceeded {
            budget_secs: budget.as_secs(),
            read: progress.read,
            expected: progress.expected,
        }),
        produce_throughput: None,
        consume_throughput: None,
        acknowledged_records: progress.acked,
        total_payload_bytes: progress.acked_bytes,
    }
}

/// Drive the cluster-ready → create → produce → consume → verify sequence,
/// returning the gathered [`PhaseData`].
///
/// Every recoverable failure is mapped to a typed [`FailureReason`] carried on
/// the returned `PhaseData`; the function itself does not error. `progress` is
/// updated at phase boundaries so a concurrent time-budget elapse can retain the
/// recorded counts (Requirement 5.4).
async fn run_phases<C: Cluster>(
    cluster: &C,
    params: &WorkloadParameters,
    progress: Arc<Mutex<RunProgress>>,
) -> PhaseData {
    // --- Cluster readiness (Requirement 9.3). ------------------------------
    if let Err(error) = cluster.await_ready(params.startup_budget).await {
        return PhaseData::failed(bench_error_to_reason(&error, params));
    }

    // Connect a client to the cluster's bootstrap address(es), exactly as a real
    // client would (Requirement 3.1, 3.2).
    let client = VelaClient::new(cluster.bootstrap());
    let admin = client.admin();

    // --- Pre-existing-topic pre-check (Requirement 3.4). -------------------
    // A successful describe means the topic already exists; refuse to measure
    // against a pre-populated topic. Any error is treated as "does not exist",
    // so the run proceeds to create it (a genuine transport failure then
    // surfaces as a creation failure below).
    if admin.describe_topic(&params.topic).await.is_ok() {
        return PhaseData::failed(FailureReason::TopicAlreadyExists {
            topic: params.topic.clone(),
        });
    }

    // --- Topic creation (Requirements 3.3, 3.6). ---------------------------
    if let Err(error) = admin
        .create_topic(&params.topic, params.partition_count, LogBackend::default())
        .await
    {
        return PhaseData::failed(FailureReason::TopicCreationFailed {
            topic: params.topic.clone(),
            cause: error.to_string(),
        });
    }

    // --- Leader-election readiness (Requirements 8.2, 10.3). ---------------
    // Right after `create_topic` the per-partition Raft groups have not yet
    // elected leaders, so the first produce would get `NoLeader` and (with the
    // client surfacing a resolution-time `NoLeader` immediately) fail fast. Wait
    // until every partition has an elected leader before opening the
    // Producer_Phase. This setup time is inside the overall time budget but is
    // naturally excluded from both Measurement_Windows, because each phase opens
    // its own window at its first measured operation (Requirements 8.2, 10.3).
    if let Err(reason) = await_partition_leaders(cluster, params).await {
        return PhaseData::failed(reason);
    }

    // --- Producer_Phase (Requirements 1.x, 3.7, 4.4, 9.1, 10.1). -----------
    let produce_sink = VelaProduceSink::new(client.clone(), params.topic.clone());
    let producer_result = match run_producer_phase(&produce_sink, params).await {
        Ok(result) => result,
        Err(error) => return PhaseData::failed(error.into_failure_reason()),
    };
    // Measured figures (warmup excluded) drive Produce_Throughput only
    // (Requirement 1.2); the total figures (warmup included) are the
    // Acknowledged_Record count the consumer reads back, verification checks, and
    // the report presents (glossary; Requirements 2.2, 5.1, 6.1).
    let acked = producer_result.acked_count;
    let acked_bytes = producer_result.acked_value_bytes;
    let total_acked = producer_result.total_acked_count;
    let total_acked_bytes = producer_result.total_acked_value_bytes;
    {
        let mut p = progress.lock().expect("run progress mutex poisoned");
        p.acked = total_acked;
        p.acked_bytes = total_acked_bytes;
        // Once produced, the consumer must read exactly the (total) acknowledged
        // count, so a time-budget elapse reports the right expected count.
        p.expected = total_acked;
    }

    // Produce_Throughput, or a zero-window failure rather than an undefined rate
    // (Requirements 1.3, 1.5).
    let produce_throughput = match throughput(acked, acked_bytes, producer_result.window) {
        Ok(t) => Some(t),
        Err(_zero) => {
            return PhaseData {
                prior_failure: Some(FailureReason::ZeroMeasurementWindow {
                    phase: Phase::Produce,
                }),
                produce_throughput: None,
                consume_throughput: None,
                acknowledged_records: total_acked,
                total_payload_bytes: total_acked_bytes,
            };
        }
    };

    // --- Consumer_Phase (Requirements 2.x, 3.7, 9.2, 10.2). ----------------
    // The consumer reads back the full Acknowledged_Record count (warmup
    // included), so it is driven by the total acked count (Requirement 2.2).
    let consume_source = VelaConsumeSource::new(client.clone(), params.topic.clone());
    let consumer_result = match run_consumer_phase(&consume_source, params, total_acked).await {
        Ok(result) => result,
        Err(error) => {
            return PhaseData {
                prior_failure: Some(error.into_failure_reason()),
                produce_throughput,
                consume_throughput: None,
                acknowledged_records: total_acked,
                total_payload_bytes: total_acked_bytes,
            };
        }
    };
    {
        let mut p = progress.lock().expect("run progress mutex poisoned");
        p.read = consumer_result.consumed.len() as u64;
    }

    // Consume_Throughput, or a zero-window failure (Requirements 2.4, 1.5).
    let consume_throughput = match throughput(
        consumer_result.read_count,
        consumer_result.read_value_bytes,
        consumer_result.window,
    ) {
        Ok(t) => Some(t),
        Err(_zero) => {
            return PhaseData {
                prior_failure: Some(FailureReason::ZeroMeasurementWindow {
                    phase: Phase::Consume,
                }),
                produce_throughput,
                consume_throughput: None,
                acknowledged_records: total_acked,
                total_payload_bytes: total_acked_bytes,
            };
        }
    };

    // --- Data-integrity verification (Requirements 5.1, 5.2, 5.5). ---------
    // The full Acknowledged_Record set (warmup included) must be read back and
    // match, so verification expects the total acked count (Requirement 5.1).
    let prior_failure = match verify_consumed(
        total_acked,
        params.value_size,
        consumer_result.consumed.iter(),
    ) {
        Ok(()) => None,
        Err(error) => Some(verification_failure_reason(error)),
    };

    PhaseData {
        prior_failure,
        produce_throughput,
        consume_throughput,
        acknowledged_records: total_acked,
        total_payload_bytes: total_acked_bytes,
    }
}

/// Interval between leader-election readiness probes after topic creation.
const LEADER_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Block until every partition `[0, partition_count)` of the target topic has an
/// elected leader, or fail when the readiness budget elapses (Requirements 8.2,
/// 10.3).
///
/// Right after `create_topic` the per-partition Raft groups have not yet elected
/// leaders. Producing into a leaderless partition would surface a resolution-time
/// `NoLeader` that the client (by design) returns immediately, failing the run.
/// This wait closes that election window *before* the Producer_Phase opens its
/// Measurement_Window, so it is charged to the overall time budget but excluded
/// from throughput (Requirement 10.3).
///
/// The high-level [`VelaClient`] does not expose a per-partition leadership
/// query, so this probes the cluster's first bootstrap node directly through the
/// raw `FindLeader` RPC — the same call the cross-node server integration test
/// uses — treating a response carrying `Some(leader)` as "elected". The wait is
/// bounded by `params.startup_budget`; on elapse without all leaders it maps to
/// [`FailureReason::ClusterNotReady`]. Single-node election is fast, so in
/// practice this returns almost immediately.
async fn await_partition_leaders<C: Cluster>(
    cluster: &C,
    params: &WorkloadParameters,
) -> Result<(), FailureReason> {
    let bootstrap = cluster.bootstrap();
    let addr = match bootstrap.first() {
        Some((_, addr)) => addr.clone(),
        None => {
            return Err(FailureReason::ClusterNotReady {
                budget_secs: params.startup_budget.as_secs(),
            });
        }
    };

    let deadline = tokio::time::Instant::now() + params.startup_budget;
    let mut remaining: Vec<u32> = (0..params.partition_count).collect();

    loop {
        // A fresh stub each pass keeps the probe simple; the channel is cheap and
        // the connection is reused for the per-partition lookups within a pass.
        if let Ok(mut client) = VelaClientClient::connect(addr.clone()).await {
            let mut still_pending = Vec::new();
            for partition in remaining {
                let elected = client
                    .find_leader(FindLeaderRequest {
                        topic: params.topic.clone(),
                        partition,
                    })
                    .await
                    .map(|response| response.into_inner().leader.is_some())
                    .unwrap_or(false);
                if !elected {
                    still_pending.push(partition);
                }
            }
            remaining = still_pending;
            if remaining.is_empty() {
                return Ok(());
            }
        }

        // Consult the deadline after at least one probe pass, so a budget that
        // has nominally elapsed still gives the cluster one chance to answer.
        if tokio::time::Instant::now() >= deadline {
            return Err(FailureReason::ClusterNotReady {
                budget_secs: params.startup_budget.as_secs(),
            });
        }
        tokio::time::sleep(LEADER_POLL_INTERVAL).await;
    }
}

/// Assemble the single [`BenchmarkReport`] from the gathered [`PhaseData`].
///
/// The [`Outcome`] is determined by [`determine_outcome`]: a prior failure wins
/// outright, otherwise the configured throughput floors are applied to the
/// measured rates (Requirements 5.3, 8.3, 9.4, 11.1, 11.2, 11.3). The
/// report's `failure_reason` mirrors the Outcome's reason so the JSON field, the
/// stdout summary, and the HTML_Report present the same failure.
fn assemble_report(
    params: WorkloadParameters,
    data: PhaseData,
    total_elapsed: Duration,
) -> BenchmarkReport {
    let produce_rps = data.produce_throughput.map(|t| t.records_per_sec);
    let consume_rps = data.consume_throughput.map(|t| t.records_per_sec);

    let outcome = determine_outcome(
        data.prior_failure,
        produce_rps,
        consume_rps,
        params.floor_produce_rps,
        params.floor_consume_rps,
    );
    let failure_reason = outcome.failure_reason().cloned();

    BenchmarkReport {
        params,
        outcome,
        produce_throughput: data.produce_throughput,
        consume_throughput: data.consume_throughput,
        acknowledged_records: data.acknowledged_records,
        total_payload_bytes: data.total_payload_bytes,
        total_elapsed,
        failure_reason,
    }
}

/// Build the failing [`BenchmarkReport`] for an invalid configuration, or `None`
/// when `params` validate. Pure: performs no side effects (Requirements 3.5,
/// 4.5, 4.6, 10.5).
fn invalid_params_report(params: &WorkloadParameters) -> Option<BenchmarkReport> {
    match params.validate() {
        Ok(()) => None,
        Err(error) => {
            let reason = FailureReason::InvalidParameter {
                name: error.name,
                detail: error.detail,
            };
            Some(assemble_report(
                params.clone(),
                PhaseData::failed(reason),
                Duration::ZERO,
            ))
        }
    }
}

/// Map a [`VerificationError`] to the report-level [`FailureReason`].
fn verification_failure_reason(error: VerificationError) -> FailureReason {
    match error {
        VerificationError::CountMismatch { read, expected } => {
            FailureReason::IntegrityCountMismatch { read, expected }
        }
        VerificationError::PayloadMismatch { position } => {
            FailureReason::IntegrityPayloadMismatch { position }
        }
    }
}

/// Map a low-level [`BenchError`] surfaced while standing up the cluster to a
/// report-level [`FailureReason`].
///
/// A failure to reserve a port or validate the config, and a startup-budget
/// timeout, both manifest to the operator as "the cluster did not become ready",
/// so both map to `ClusterNotReady` against the configured startup budget
/// (Requirement 9.3).
fn bench_error_to_reason(error: &BenchError, params: &WorkloadParameters) -> FailureReason {
    match error {
        BenchError::ClusterNotReady { budget_secs } => FailureReason::ClusterNotReady {
            budget_secs: *budget_secs,
        },
        BenchError::ClusterStartup { .. } => FailureReason::ClusterNotReady {
            budget_secs: params.startup_budget.as_secs(),
        },
        BenchError::InvalidParameter { name, detail } => FailureReason::InvalidParameter {
            name: name.clone(),
            detail: detail.clone(),
        },
        BenchError::Operation {
            operation,
            topic,
            partition,
            cause,
        } => {
            // Defensive: the harness drives operations through the phase seams,
            // which produce their own typed reasons, so this is not expected to
            // be reached. Map it to a produce/consume error by operation kind.
            if operation == "consume" {
                FailureReason::ConsumeError {
                    topic: topic.clone(),
                    partition: *partition,
                    cause: cause.clone(),
                }
            } else {
                FailureReason::ProduceError {
                    topic: topic.clone(),
                    partition: *partition,
                    cause: cause.clone(),
                }
            }
        }
    }
}

/// Emit the three coordinated outputs from the single [`BenchmarkReport`]
/// (Requirement 6.1): the machine-readable JSON artifact (when a path is given),
/// the human-readable stdout summary (always), and the self-contained
/// HTML_Report (when a path is given). A failure to write an output file is
/// logged but does not change the [`Outcome`] already recorded in the report.
fn emit_outputs(report: &BenchmarkReport, report_json: Option<&Path>, report_html: Option<&Path>) {
    if let Some(path) = report_json {
        if let Err(error) = report.write_json_file(path) {
            tracing::error!(%error, path = %path.display(), "failed to write JSON Benchmark_Report");
        }
    }

    if let Err(error) = report.print_summary() {
        tracing::error!(%error, "failed to write stdout summary");
    }

    if let Some(path) = report_html {
        if let Err(error) = write_html_file(report, path) {
            tracing::error!(%error, path = %path.display(), "failed to write HTML_Report");
        }
    }
}

// ---------------------------------------------------------------------------
// Real `VelaClient` adapters for the phase seams.
// ---------------------------------------------------------------------------

/// The real [`ProduceSink`] adapter: produces each record through the
/// `vela-client` Producer API (Requirement 3.1).
///
/// Wraps a [`VelaClient`] and the target topic. The client owns partition
/// routing, leader redirection, and the bounded retry budget, so a `Result::Err`
/// returned from `produce` is an *unresolved* operation error the benchmark
/// surfaces as a [`ProduceFailure`] (Requirements 3.7, 9.1). The client does not
/// expose the partition a keyed/keyless record routed to, so the failure carries
/// a best-effort partition of `0`.
#[derive(Debug, Clone)]
pub struct VelaProduceSink {
    client: VelaClient,
    topic: String,
}

impl VelaProduceSink {
    /// Wrap `client` and `topic` as a [`ProduceSink`].
    pub fn new(client: VelaClient, topic: String) -> Self {
        Self { client, topic }
    }
}

#[async_trait]
impl ProduceSink for VelaProduceSink {
    async fn produce(
        &self,
        _position: u64,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<u64, ProduceFailure> {
        self.client
            .producer()
            .produce(&self.topic, key.as_deref(), value)
            .await
            .map_err(|error| ProduceFailure {
                topic: self.topic.clone(),
                // The client resolves the partition internally and does not
                // surface it on error; report a best-effort sentinel.
                partition: 0,
                cause: error.to_string(),
            })
    }
}

/// The real [`ConsumeSource`] adapter: consumes records through the
/// `vela-client` Consumer API (Requirement 3.1).
///
/// Wraps a [`VelaClient`] and the target topic. Each consume maps the returned
/// `ConsumeOutcome` to a [`ConsumedBatch`], extracting each consumed record's
/// value bytes (a record with no payload yields empty bytes). A `Result::Err` is
/// an unresolved consume error surfaced as a [`ConsumeFailure`]
/// (Requirements 3.7, 9.2), carrying the partition the read targeted.
#[derive(Debug, Clone)]
pub struct VelaConsumeSource {
    client: VelaClient,
    topic: String,
}

impl VelaConsumeSource {
    /// Wrap `client` and `topic` as a [`ConsumeSource`].
    pub fn new(client: VelaClient, topic: String) -> Self {
        Self { client, topic }
    }
}

#[async_trait]
impl ConsumeSource for VelaConsumeSource {
    async fn consume(
        &self,
        partition: u32,
        offset: u64,
        max: Option<u32>,
    ) -> Result<ConsumedBatch, ConsumeFailure> {
        match self
            .client
            .consumer()
            .consume(&self.topic, partition, offset, max)
            .await
        {
            Ok(outcome) => {
                let records = outcome
                    .records
                    .into_iter()
                    .map(|consumed| {
                        consumed
                            .record
                            .map(|record| record.value)
                            .unwrap_or_default()
                    })
                    .collect();
                Ok(ConsumedBatch {
                    records,
                    next_offset: outcome.next_offset,
                })
            }
            Err(error) => Err(ConsumeFailure {
                topic: self.topic.clone(),
                partition,
                cause: error.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Throughput;
    use crate::outcome::Outcome;
    use crate::params::{KeyMode, WorkloadParameters};

    /// A fake [`Cluster`] for exercising the sequencing paths that do not need a
    /// live client: validation rejection, cluster-readiness failure, and
    /// shutdown. `ready` controls whether [`await_ready`](Cluster::await_ready)
    /// succeeds.
    struct FakeCluster {
        ready: bool,
    }

    #[async_trait]
    impl Cluster for FakeCluster {
        fn bootstrap(&self) -> Vec<(String, String)> {
            vec![("node-a".to_string(), "http://127.0.0.1:0".to_string())]
        }

        async fn await_ready(&self, budget: Duration) -> Result<(), BenchError> {
            if self.ready {
                Ok(())
            } else {
                Err(BenchError::ClusterNotReady {
                    budget_secs: budget.as_secs(),
                })
            }
        }

        async fn shutdown(self) -> Result<(), BenchError> {
            Ok(())
        }
    }

    fn small_params() -> WorkloadParameters {
        WorkloadParameters {
            record_count: 10,
            value_size: 16,
            key_mode: KeyMode::Keyless,
            partition_count: 2,
            producer_concurrency: 4,
            topic: "vela-bench-test".to_string(),
            warmup: 0,
            time_budget: Duration::from_secs(60),
            startup_budget: Duration::from_secs(60),
            floor_produce_rps: None,
            floor_consume_rps: None,
        }
    }

    fn measured() -> PhaseData {
        PhaseData {
            prior_failure: None,
            produce_throughput: Some(Throughput {
                records_per_sec: 500.0,
                bytes_per_sec: 8_000.0,
            }),
            consume_throughput: Some(Throughput {
                records_per_sec: 400.0,
                bytes_per_sec: 6_400.0,
            }),
            acknowledged_records: 100,
            total_payload_bytes: 1_600,
        }
    }

    // ----- invalid_params_report (Requirements 3.5, 4.5, 4.6, 10.5) --------

    #[test]
    fn valid_params_produce_no_early_report() {
        assert!(invalid_params_report(&small_params()).is_none());
    }

    #[test]
    fn invalid_params_report_names_the_offending_field() {
        let mut params = small_params();
        params.record_count = 0;
        let report = invalid_params_report(&params).expect("invalid params yield a report");
        assert!(!report.outcome.is_passed());
        match report.failure_reason {
            Some(FailureReason::InvalidParameter { name, .. }) => assert_eq!(name, "record_count"),
            other => panic!("expected InvalidParameter, got {other:?}"),
        }
        // No throughput is presented for a rejected configuration.
        assert!(report.produce_throughput.is_none());
        assert!(report.consume_throughput.is_none());
    }

    // ----- assemble_report (Requirements 6.1, 9.4, 11.1, 11.2) -------------

    #[test]
    fn assemble_report_passes_with_no_failure_and_no_floor() {
        let report = assemble_report(small_params(), measured(), Duration::from_millis(250));
        assert_eq!(report.outcome, Outcome::Passed);
        assert!(report.failure_reason.is_none());
        assert!(report.produce_throughput.is_some());
        assert!(report.consume_throughput.is_some());
        assert_eq!(report.total_elapsed, Duration::from_millis(250));
    }

    #[test]
    fn assemble_report_applies_produce_floor() {
        let mut params = small_params();
        params.floor_produce_rps = Some(1_000.0);
        let report = assemble_report(params, measured(), Duration::from_millis(10));
        // The measured 500 rps is below the 1000 rps floor (Requirement 11.1).
        match report.outcome.failure_reason() {
            Some(FailureReason::FloorBreachProduce {
                measured_rps,
                floor_rps,
            }) => {
                assert_eq!(*measured_rps, 500.0);
                assert_eq!(*floor_rps, 1_000.0);
            }
            other => panic!("expected FloorBreachProduce, got {other:?}"),
        }
        // The measured throughput is still reported even on a floor breach.
        assert!(report.produce_throughput.is_some());
    }

    #[test]
    fn assemble_report_prior_failure_wins_over_floor() {
        let mut data = measured();
        data.prior_failure = Some(FailureReason::IntegrityCountMismatch {
            read: 9,
            expected: 10,
        });
        let mut params = small_params();
        params.floor_produce_rps = Some(1_000.0);
        let report = assemble_report(params, data, Duration::from_millis(10));
        assert_eq!(
            report.failure_reason,
            Some(FailureReason::IntegrityCountMismatch {
                read: 9,
                expected: 10,
            })
        );
    }

    // ----- timeout_data (Requirements 5.4, 8.3) ----------------------------

    #[test]
    fn timeout_data_retains_recorded_counts() {
        let progress = Mutex::new(RunProgress {
            acked: 100,
            acked_bytes: 3_200,
            read: 40,
            expected: 100,
        });
        let data = timeout_data(&progress, Duration::from_secs(120));
        assert_eq!(
            data.prior_failure,
            Some(FailureReason::TimeBudgetExceeded {
                budget_secs: 120,
                read: 40,
                expected: 100,
            })
        );
        assert_eq!(data.acknowledged_records, 100);
        assert_eq!(data.total_payload_bytes, 3_200);
        assert!(data.produce_throughput.is_none());
        assert!(data.consume_throughput.is_none());
    }

    // ----- bench_error_to_reason (Requirement 9.3) -------------------------

    #[test]
    fn cluster_not_ready_maps_to_failure_reason() {
        let reason = bench_error_to_reason(
            &BenchError::ClusterNotReady { budget_secs: 60 },
            &small_params(),
        );
        assert_eq!(reason, FailureReason::ClusterNotReady { budget_secs: 60 });
    }

    #[test]
    fn cluster_startup_maps_to_not_ready_against_startup_budget() {
        let mut params = small_params();
        params.startup_budget = Duration::from_secs(45);
        let reason = bench_error_to_reason(
            &BenchError::ClusterStartup {
                detail: "port".to_string(),
            },
            &params,
        );
        assert_eq!(reason, FailureReason::ClusterNotReady { budget_secs: 45 });
    }

    // ----- verification_failure_reason (Requirements 5.1, 5.2, 5.5) --------

    #[test]
    fn verification_errors_map_to_failure_reasons() {
        assert_eq!(
            verification_failure_reason(VerificationError::CountMismatch {
                read: 9,
                expected: 10,
            }),
            FailureReason::IntegrityCountMismatch {
                read: 9,
                expected: 10,
            }
        );
        assert_eq!(
            verification_failure_reason(VerificationError::PayloadMismatch { position: 3 }),
            FailureReason::IntegrityPayloadMismatch { position: 3 }
        );
    }

    // ----- end-to-end sequencing failure paths via a fake cluster ----------

    /// An invalid configuration is rejected before the cluster is driven, and
    /// the supplied cluster is still shut down (Requirements 4.5, 4.6).
    #[tokio::test]
    async fn run_with_cluster_rejects_invalid_params_before_driving_cluster() {
        let mut params = small_params();
        params.partition_count = 0;
        let report = run_with_cluster(FakeCluster { ready: true }, params, None, None).await;
        match report.failure_reason {
            Some(FailureReason::InvalidParameter { name, .. }) => {
                assert_eq!(name, "partition_count")
            }
            other => panic!("expected InvalidParameter, got {other:?}"),
        }
    }

    /// A cluster that never becomes ready fails the run with `ClusterNotReady`
    /// against the startup budget, before any topic work (Requirement 9.3).
    #[tokio::test]
    async fn run_with_cluster_fails_when_cluster_never_ready() {
        let mut params = small_params();
        params.startup_budget = Duration::from_secs(3);
        let report = run_with_cluster(FakeCluster { ready: false }, params, None, None).await;
        assert!(!report.outcome.is_passed());
        assert_eq!(
            report.failure_reason,
            Some(FailureReason::ClusterNotReady { budget_secs: 3 })
        );
        assert!(report.produce_throughput.is_none());
        assert!(report.consume_throughput.is_none());
    }
}
