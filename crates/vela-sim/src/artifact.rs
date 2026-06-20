//! `FailureArtifact`: CI-collectable failure output and run summaries.
//!
//! When a [`Simulation_Run`] ends, the harness needs to hand a developer (and
//! CI) everything required to understand — and *replay* — what happened. This
//! module produces two human-readable outputs and writes them to a path the CI
//! workflow can collect as a build artifact (Requirement 13.4):
//!
//! - A [`RunSummary`] is produced **regardless of [`Outcome`]** (pass, fail, or
//!   invalid): the seed, the [`ScenarioParameters`], the outcome, and a
//!   structured tally of the recorded [`History`]. It is the always-on record of
//!   what a run did.
//! - A [`FailureArtifact`] is produced **only on a failing [`Outcome`]**
//!   (Requirement 13.1, 13.2, 13.5). It carries the seed, the parameters, the
//!   violated [`PropertyId`], the logical detection [`VirtualInstant`], the
//!   checker's structured `detail` (which already names the affected group /
//!   term / replicas, so no re-run is needed to obtain them — Requirement 13.5),
//!   and the full recorded [`History`].
//!
//! # Replay: a run is a pure function of its seed
//!
//! A Vela DST run is a pure function of `(seed, params)` (Requirement 1.3): the
//! whole harness is single-threaded and every random decision is seed-derived,
//! so **re-running [`SimRuntime::run`] with the recorded `(seed, params)`
//! reproduces the identical failing [`Outcome`] and the same violated property**
//! (Requirement 2.2). The artifact therefore does *not* need to serialize an
//! event-by-event trace to be replayable: the `(seed, params)` pair **is** the
//! replayable trace. The recorded [`History`] is included on top of that
//! (Requirement 13.2) so the failure can be read and reasoned about *without* a
//! re-run — it is the structured summary of what the clients observed.
//!
//! # Where output is written
//!
//! Output is written under a CI-collectable directory, defaulting to
//! [`DEFAULT_ARTIFACT_DIR`] (`target/dst-artifacts/`, which the CI workflow
//! uploads) and overridable through the [`ARTIFACT_DIR_ENV`]
//! (`VELA_DST_ARTIFACT_DIR`) environment variable so a developer can redirect it
//! locally. Filenames are deterministic and incorporate the seed
//! ([`RunSummary::filename`], [`FailureArtifact::filename`]) so a given run's
//! files are predictable and a re-run overwrites rather than accumulates.
//!
//! Serialization is a small hand-rolled key/value text format rather than a
//! `serde` dependency (`vela-sim` deliberately has no `serde` dependency): the
//! output is for humans and CI logs, not machine round-tripping.
//!
//! # Wiring
//!
//! The constructors ([`RunSummary::from_outcome`], [`FailureArtifact::from_outcome`])
//! are pure and independently testable. [`persist_run`] is the side-effecting
//! entry point that writes the summary always and the artifact on failure;
//! [`SimRuntime::run`] invokes it **only when [`ARTIFACT_DIR_ENV`] is set**, so
//! the many unit/property tests that call `run` repeatedly do not spam the
//! filesystem. CI sets the variable; local `cargo test` runs do not, unless a
//! developer opts in.
//!
//! [`Simulation_Run`]: crate
//! [`SimRuntime::run`]: crate::runtime::SimRuntime::run

use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::checker::PropertyId;
use crate::history::{History, OpArgs, OpResponse, RecordedOp};
use crate::runtime::Outcome;
use crate::scenario::ScenarioParameters;
use crate::scheduler::VirtualInstant;

/// The default CI-collectable output directory, relative to the workspace root.
///
/// The CI workflow uploads this directory on a failing run (Requirement 13.4,
/// 14.4). It lives under `target/` so it is git-ignored and cleaned by
/// `cargo clean`.
pub const DEFAULT_ARTIFACT_DIR: &str = "target/dst-artifacts";

/// The environment variable that overrides [`DEFAULT_ARTIFACT_DIR`].
///
/// Setting it also *enables* artifact writing from [`SimRuntime::run`]: when it
/// is unset, `run` performs no filesystem writes (so repeated test runs do not
/// spam the disk); when it is set, `run` writes the summary (always) and the
/// failure artifact (on failure) under the configured directory.
///
/// [`SimRuntime::run`]: crate::runtime::SimRuntime::run
pub const ARTIFACT_DIR_ENV: &str = "VELA_DST_ARTIFACT_DIR";

/// A structured tally of a run's recorded [`History`], by response kind.
///
/// This is the "structured summary of the run" the artifact and the run summary
/// carry: rather than re-dumping every byte, it counts how the clients' issued
/// operations resolved (Requirement 13.1's diagnostics, Requirement 13.4's run
/// summary). Every recorded operation falls into exactly one bucket, so
/// [`total_ops`](Self::total_ops) equals the sum of the per-kind counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HistoryStats {
    /// The total number of recorded client operations.
    pub total_ops: usize,
    /// Successful produces ([`OpResponse::ProduceOk`]).
    pub produce_ok: usize,
    /// Successful consumes ([`OpResponse::ConsumeOk`]).
    pub consume_ok: usize,
    /// Successful topic creates ([`OpResponse::CreateTopicOk`]).
    pub create_topic_ok: usize,
    /// Successful topic deletes ([`OpResponse::DeleteTopicOk`]).
    pub delete_topic_ok: usize,
    /// Recorded leader redirects ([`OpResponse::Redirect`]).
    pub redirects: usize,
    /// Recorded unresolved-redirection responses
    /// ([`OpResponse::UnresolvedRedirection`]).
    pub unresolved_redirections: usize,
    /// Recorded no-leader responses ([`OpResponse::NoLeader`]).
    pub no_leader: usize,
    /// Recorded surfaced storage I/O errors ([`OpResponse::IoError`]).
    pub io_errors: usize,
    /// Any other recorded rejection ([`OpResponse::Error`]).
    pub other_errors: usize,
}

impl HistoryStats {
    /// Tally a [`History`] into per-response-kind counts.
    #[must_use]
    pub fn from_history(history: &History) -> Self {
        let mut stats = HistoryStats {
            total_ops: history.len(),
            ..HistoryStats::default()
        };
        for op in history.iter() {
            match &op.response {
                OpResponse::ProduceOk { .. } => stats.produce_ok += 1,
                OpResponse::ConsumeOk { .. } => stats.consume_ok += 1,
                OpResponse::CreateTopicOk { .. } => stats.create_topic_ok += 1,
                OpResponse::DeleteTopicOk { .. } => stats.delete_topic_ok += 1,
                OpResponse::Redirect { .. } => stats.redirects += 1,
                OpResponse::UnresolvedRedirection => stats.unresolved_redirections += 1,
                OpResponse::NoLeader => stats.no_leader += 1,
                OpResponse::IoError { .. } => stats.io_errors += 1,
                OpResponse::Error { .. } => stats.other_errors += 1,
            }
        }
        stats
    }

    /// Render the tally as indented `key = value` lines (no trailing newline).
    fn render_into(&self, out: &mut String) {
        let _ = writeln!(out, "  total_ops              = {}", self.total_ops);
        let _ = writeln!(out, "  produce_ok             = {}", self.produce_ok);
        let _ = writeln!(out, "  consume_ok             = {}", self.consume_ok);
        let _ = writeln!(out, "  create_topic_ok        = {}", self.create_topic_ok);
        let _ = writeln!(out, "  delete_topic_ok        = {}", self.delete_topic_ok);
        let _ = writeln!(out, "  redirects              = {}", self.redirects);
        let _ = writeln!(
            out,
            "  unresolved_redirection = {}",
            self.unresolved_redirections
        );
        let _ = writeln!(out, "  no_leader              = {}", self.no_leader);
        let _ = writeln!(out, "  io_errors              = {}", self.io_errors);
        let _ = write!(out, "  other_errors           = {}", self.other_errors);
    }
}

/// The outcome of a run as a compact, stable label for the summary file.
fn outcome_label(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Passed => "PASSED",
        Outcome::Failed { .. } => "FAILED",
        Outcome::Invalid { .. } => "INVALID",
    }
}

/// Render a [`ScenarioParameters`] set as indented `key = value` lines (no
/// trailing newline). Shared by the summary and the failure artifact so the two
/// describe the scenario identically.
fn render_params_into(params: &ScenarioParameters, out: &mut String) {
    let f = &params.faults;
    let _ = writeln!(out, "  node_count             = {}", params.node_count);
    let _ = writeln!(
        out,
        "  replication_factor     = {}",
        params.replication_factor
    );
    let _ = writeln!(out, "  partition_count        = {}", params.partition_count);
    let _ = writeln!(out, "  workload_size          = {}", params.workload_size);
    let _ = writeln!(
        out,
        "  budget.max_events      = {}",
        params.budget.max_events
    );
    let _ = writeln!(
        out,
        "  budget.max_virtual_ns  = {}",
        params.budget.max_virtual_nanos
    );
    let _ = writeln!(out, "  faults.base_latency_ns = {}", f.base_latency_nanos);
    let _ = writeln!(out, "  faults.reorder_prob    = {}", f.reorder_prob);
    let _ = writeln!(out, "  faults.max_reorder_ns  = {}", f.max_reorder_nanos);
    let _ = writeln!(out, "  faults.drop_prob       = {}", f.drop_prob);
    let _ = writeln!(out, "  faults.duplicate_prob  = {}", f.duplicate_prob);
    let _ = writeln!(out, "  faults.partition_prob  = {}", f.partition_prob);
    let _ = writeln!(out, "  faults.crash_prob      = {}", f.crash_prob);
    let _ = writeln!(out, "  faults.max_skew_ns     = {}", f.max_clock_skew_nanos);
    let _ = writeln!(out, "  faults.max_skew_rate   = {}", f.max_clock_skew_rate);
    let _ = writeln!(out, "  faults.torn_write_prob = {}", f.torn_write_prob);
    let _ = write!(out, "  faults.io_error_prob   = {}", f.io_error_prob);
}

/// The always-written record of a single [`Simulation_Run`], produced for every
/// [`Outcome`] (Requirement 13.4).
///
/// Carries the seed and [`ScenarioParameters`] (so the run can be replayed,
/// Requirement 2.1), the outcome (and, when it failed, the violated property and
/// detection instant — Requirement 2.3), and a [`HistoryStats`] tally of what
/// the clients observed.
///
/// [`Simulation_Run`]: crate
#[derive(Debug, Clone, PartialEq)]
pub struct RunSummary {
    /// The 64-bit run seed.
    pub seed: u64,
    /// The scenario parameters the run used.
    pub params: ScenarioParameters,
    /// The violated property, when the run failed.
    pub property: Option<PropertyId>,
    /// The logical instant a violation was detected, when the run failed.
    pub detected_at: Option<VirtualInstant>,
    /// A human-readable detail for a failing or invalid run.
    pub detail: Option<String>,
    /// A stable label for the outcome (`PASSED` / `FAILED` / `INVALID`).
    pub outcome: &'static str,
    /// A tally of the recorded [`History`].
    pub history: HistoryStats,
}

impl RunSummary {
    /// Build a run summary from a run's `(seed, params)`, its [`Outcome`], and the
    /// recorded [`History`]. Pure and side-effect-free.
    #[must_use]
    pub fn from_outcome(
        seed: u64,
        params: ScenarioParameters,
        outcome: &Outcome,
        history: &History,
    ) -> Self {
        let (property, detected_at, detail) = match outcome {
            Outcome::Passed => (None, None, None),
            Outcome::Failed {
                property,
                at,
                detail,
            } => (Some(*property), Some(*at), Some(detail.clone())),
            Outcome::Invalid { detail } => (None, None, Some(detail.clone())),
        };
        Self {
            seed,
            params,
            property,
            detected_at,
            detail,
            outcome: outcome_label(outcome),
            history: HistoryStats::from_history(history),
        }
    }

    /// The deterministic filename for this summary, incorporating the seed.
    #[must_use]
    pub fn filename(&self) -> String {
        format!("dst-run-{:016x}.summary.txt", self.seed)
    }

    /// Render the summary as a human-readable text document.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# Vela DST run summary");
        let _ = writeln!(out, "seed     = {} (0x{:016x})", self.seed, self.seed);
        let _ = writeln!(out, "outcome  = {}", self.outcome);
        if let Some(property) = self.property {
            let _ = writeln!(out, "property = {property}");
        }
        if let Some(at) = self.detected_at {
            let _ = writeln!(out, "detected_at_nanos = {}", at.as_nanos());
        }
        if let Some(detail) = &self.detail {
            let _ = writeln!(out, "detail   = {detail}");
        }
        let _ = writeln!(out, "\n## scenario parameters");
        render_params_into(&self.params, &mut out);
        let _ = writeln!(out, "\n## history");
        self.history.render_into(&mut out);
        out.push('\n');
        out
    }
}

/// A complete, replayable record of a failing [`Simulation_Run`] (Requirement
/// 13.1, 13.2, 13.5).
///
/// Built from a failing [`Outcome`] via [`from_outcome`](Self::from_outcome).
/// Contains:
///
/// - the **seed** and **[`ScenarioParameters`]** — the replayable trace itself
///   (re-running [`SimRuntime::run`] with them reproduces the failure;
///   Requirement 2.1, 2.2),
/// - the violated **[`PropertyId`]** and the detection **[`VirtualInstant`]**
///   (Requirement 2.3, 13.1),
/// - the checker's **`detail`** — structured diagnostics naming the affected
///   group / term / replicas, captured at detection time so no re-run is needed
///   to obtain them (Requirement 13.5), and
/// - the full recorded **[`History`]** (Requirement 13.2).
///
/// [`SimRuntime::run`]: crate::runtime::SimRuntime::run
#[derive(Debug, Clone, PartialEq)]
pub struct FailureArtifact {
    /// The 64-bit run seed; with [`params`](Self::params), the replayable trace.
    pub seed: u64,
    /// The scenario parameters; with [`seed`](Self::seed), the replayable trace.
    pub params: ScenarioParameters,
    /// The violated correctness property.
    pub property: PropertyId,
    /// The logical instant the violation was detected.
    pub detected_at: VirtualInstant,
    /// Structured diagnostics naming the affected group / term / replicas,
    /// captured without requiring a re-run (Requirement 13.5).
    pub detail: String,
    /// The full recorded client [`History`] leading to the violation.
    pub history: History,
}

impl FailureArtifact {
    /// Build a [`FailureArtifact`] from a run's `(seed, params)`, its
    /// [`Outcome`], and the recorded [`History`].
    ///
    /// Returns `Some` only when `outcome` is [`Outcome::Failed`]; a passing or
    /// invalid run has no failure artifact (it still gets a [`RunSummary`]).
    /// Pure and side-effect-free.
    #[must_use]
    pub fn from_outcome(
        seed: u64,
        params: ScenarioParameters,
        outcome: &Outcome,
        history: &History,
    ) -> Option<Self> {
        match outcome {
            Outcome::Failed {
                property,
                at,
                detail,
            } => Some(Self {
                seed,
                params,
                property: *property,
                detected_at: *at,
                detail: detail.clone(),
                history: history.clone(),
            }),
            Outcome::Passed | Outcome::Invalid { .. } => None,
        }
    }

    /// The deterministic filename for this artifact, incorporating the seed.
    #[must_use]
    pub fn filename(&self) -> String {
        format!("dst-failure-{:016x}.txt", self.seed)
    }

    /// Render the artifact as a human-readable text document: the failure
    /// header, replay instructions, the scenario parameters, and the full
    /// recorded history.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# Vela DST failure artifact");
        let _ = writeln!(
            out,
            "seed              = {} (0x{:016x})",
            self.seed, self.seed
        );
        let _ = writeln!(out, "violated_property = {}", self.property);
        let _ = writeln!(out, "detected_at_nanos = {}", self.detected_at.as_nanos());
        let _ = writeln!(out, "detail            = {}", self.detail);

        let _ = writeln!(out, "\n## replay");
        let _ = writeln!(
            out,
            "A Vela DST run is a pure function of (seed, params): re-running"
        );
        let _ = writeln!(
            out,
            "SimRuntime::run(RunConfig {{ seed, params }}) with the seed and"
        );
        let _ = writeln!(
            out,
            "parameters above reproduces this exact failing Outcome and the same"
        );
        let _ = writeln!(
            out,
            "violated property. No separate event-by-event trace is required; the"
        );
        let _ = writeln!(
            out,
            "recorded History below is included so the failure can be read without"
        );
        let _ = writeln!(out, "a re-run.");

        let _ = writeln!(out, "\n## scenario parameters");
        render_params_into(&self.params, &mut out);

        let _ = writeln!(out, "\n## history ({} ops)", self.history.len());
        HistoryStats::from_history(&self.history).render_into(&mut out);
        out.push('\n');
        if self.history.is_empty() {
            let _ = writeln!(out, "\n  (no client operations were recorded)");
        } else {
            let _ = writeln!(out, "\n## recorded operations");
            for (i, op) in self.history.iter().enumerate() {
                let _ = writeln!(out, "  [{i}] {}", render_op(op));
            }
        }
        out
    }

    /// Write this artifact to `dir`, creating the directory if needed, and
    /// return the path written. The filename is [`filename`](Self::filename).
    ///
    /// # Errors
    ///
    /// Returns any [`io::Error`] from creating the directory or writing the file.
    pub fn write_to_dir(&self, dir: &Path) -> io::Result<PathBuf> {
        fs::create_dir_all(dir)?;
        let path = dir.join(self.filename());
        fs::write(&path, self.render())?;
        Ok(path)
    }
}

/// Render one recorded operation as a single compact line for the artifact's
/// history dump. Byte payloads are summarized by length to keep the output
/// readable and bounded.
fn render_op(op: &RecordedOp) -> String {
    let args = match &op.args {
        OpArgs::CreateTopic {
            topic,
            partitions,
            replication_factor,
        } => format!("create topic={topic} partitions={partitions} rf={replication_factor}"),
        OpArgs::DeleteTopic { topic } => format!("delete topic={topic}"),
        OpArgs::Produce {
            topic,
            partition,
            key,
            value,
        } => format!(
            "produce topic={topic} p={} key_len={} value_len={}",
            partition.0,
            key.as_ref().map_or(0, Vec::len),
            value.len()
        ),
        OpArgs::Consume {
            topic,
            partition,
            start_offset,
            max_records,
        } => format!(
            "consume topic={topic} p={} start={start_offset} max={max_records}",
            partition.0
        ),
    };
    let resp = match &op.response {
        OpResponse::ProduceOk {
            partition, offset, ..
        } => format!("ProduceOk p={} offset={offset}", partition.0),
        OpResponse::ConsumeOk {
            partition,
            start_offset,
            records,
            ..
        } => format!(
            "ConsumeOk p={} start={start_offset} records={}",
            partition.0,
            records.len()
        ),
        OpResponse::CreateTopicOk { topic } => format!("CreateTopicOk topic={topic}"),
        OpResponse::DeleteTopicOk { topic } => format!("DeleteTopicOk topic={topic}"),
        OpResponse::Redirect { leader } => format!("Redirect leader={}", leader.0),
        OpResponse::UnresolvedRedirection => "UnresolvedRedirection".to_string(),
        OpResponse::NoLeader => "NoLeader".to_string(),
        OpResponse::IoError { message } => format!("IoError({message})"),
        OpResponse::Error { message } => format!("Error({message})"),
    };
    format!(
        "t[{}..{}] {} -> {}",
        op.invoked_at.as_nanos(),
        op.responded_at.as_nanos(),
        args,
        resp
    )
}

/// Resolve the CI-collectable output directory: [`ARTIFACT_DIR_ENV`] if set and
/// non-empty, otherwise [`DEFAULT_ARTIFACT_DIR`].
#[must_use]
pub fn resolve_artifact_dir() -> PathBuf {
    match std::env::var(ARTIFACT_DIR_ENV) {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => PathBuf::from(DEFAULT_ARTIFACT_DIR),
    }
}

/// Persist a run's output to the resolved CI-collectable directory: the
/// [`RunSummary`] is written **regardless of [`Outcome`]**, and the
/// [`FailureArtifact`] is written **only on a failing outcome** (Requirement
/// 13.4).
///
/// The directory is [`resolve_artifact_dir`]. Returns the paths written (the
/// summary path always; the artifact path when the run failed).
///
/// # Errors
///
/// Returns any [`io::Error`] from creating the directory or writing a file.
pub fn persist_run(
    seed: u64,
    params: ScenarioParameters,
    outcome: &Outcome,
    history: &History,
) -> io::Result<(PathBuf, Option<PathBuf>)> {
    let dir = resolve_artifact_dir();
    fs::create_dir_all(&dir)?;

    let summary = RunSummary::from_outcome(seed, params, outcome, history);
    let summary_path = dir.join(summary.filename());
    fs::write(&summary_path, summary.render())?;

    let artifact_path = match FailureArtifact::from_outcome(seed, params, outcome, history) {
        Some(artifact) => Some(artifact.write_to_dir(&dir)?),
        None => None,
    };
    Ok((summary_path, artifact_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::OpArgs;
    use crate::scheduler::VirtualInstant;
    use vela_core::PartitionIndex;

    fn t(nanos: u64) -> VirtualInstant {
        VirtualInstant::from_nanos(nanos)
    }

    fn produce_args(topic: &str, partition: u32, value: &[u8]) -> OpArgs {
        OpArgs::Produce {
            topic: topic.to_string(),
            partition: PartitionIndex(partition),
            key: None,
            value: value.to_vec(),
        }
    }

    /// A history with one successful produce and one no-leader response, used to
    /// exercise the tally and the artifact's history dump.
    fn sample_history() -> History {
        let mut history = History::new();
        history.record_produce_success(produce_args("orders", 1, b"hello"), t(10), t(20), 0);
        history.record_failure(
            produce_args("orders", 1, b"again"),
            t(30),
            t(40),
            OpResponse::NoLeader,
        );
        history
    }

    fn failing_outcome() -> Outcome {
        Outcome::Failed {
            property: PropertyId::ElectionSafety,
            at: t(1_234_567),
            detail: "group=orders/0 term=4 has two leaders: node-0 and node-2".to_string(),
        }
    }

    /// Requirement 2.1, 2.3, 13.1, 13.5: a failing outcome yields a
    /// `FailureArtifact` naming the violated property, the detection instant, the
    /// seed and params, and a non-empty diagnostic detail.
    #[test]
    fn failing_outcome_yields_artifact_with_property_instant_seed_params_and_detail() {
        let seed = 0xABCD_1234_5678_9ABC;
        let params = ScenarioParameters::default();
        let history = sample_history();
        let outcome = failing_outcome();

        let artifact = FailureArtifact::from_outcome(seed, params, &outcome, &history)
            .expect("a failing outcome must yield a FailureArtifact");

        assert_eq!(artifact.seed, seed);
        assert_eq!(artifact.params, params);
        assert_eq!(artifact.property, PropertyId::ElectionSafety);
        assert_eq!(artifact.detected_at, t(1_234_567));
        assert!(
            !artifact.detail.is_empty(),
            "the artifact must carry a non-empty structured diagnostic"
        );
        // The recorded History is captured for replay-free reading (13.2).
        assert_eq!(artifact.history, history);

        // The rendered document names the property, the instant, the seed, and
        // the diagnostic, and documents the replay path.
        let rendered = artifact.render();
        assert!(rendered.contains("Election Safety"));
        assert!(rendered.contains("1234567"));
        assert!(rendered.contains(&format!("0x{seed:016x}")));
        assert!(rendered.contains("two leaders"));
        assert!(rendered.contains("SimRuntime::run"));
    }

    /// A passing or invalid outcome has no failure artifact.
    #[test]
    fn passing_and_invalid_outcomes_have_no_artifact() {
        let params = ScenarioParameters::default();
        let history = History::new();
        assert!(
            FailureArtifact::from_outcome(1, params, &Outcome::Passed, &history).is_none(),
            "a passing run has no failure artifact"
        );
        let invalid = Outcome::Invalid {
            detail: "replication factor 4 exceeds node count 3".to_string(),
        };
        assert!(
            FailureArtifact::from_outcome(1, params, &invalid, &history).is_none(),
            "an invalid run has no failure artifact"
        );
    }

    /// Requirement 13.4: a run summary is produced for a passing outcome too
    /// (regardless of outcome), tallying the recorded history.
    #[test]
    fn run_summary_is_produced_for_a_passing_outcome() {
        let seed = 7;
        let params = ScenarioParameters::default();
        let history = sample_history();

        let summary = RunSummary::from_outcome(seed, params, &Outcome::Passed, &history);

        assert_eq!(summary.seed, seed);
        assert_eq!(summary.outcome, "PASSED");
        assert_eq!(summary.property, None);
        assert_eq!(summary.detected_at, None);
        assert_eq!(summary.history.total_ops, 2);
        assert_eq!(summary.history.produce_ok, 1);
        assert_eq!(summary.history.no_leader, 1);

        let rendered = summary.render();
        assert!(rendered.contains("outcome  = PASSED"));
        assert!(rendered.contains("total_ops              = 2"));
        // A passing summary names no violated property.
        assert!(!rendered.contains("property ="));
    }

    /// Requirement 13.4: a failing run summary records the violated property and
    /// the detection instant.
    #[test]
    fn run_summary_for_a_failing_outcome_records_property_and_instant() {
        let summary = RunSummary::from_outcome(
            3,
            ScenarioParameters::default(),
            &failing_outcome(),
            &sample_history(),
        );

        assert_eq!(summary.outcome, "FAILED");
        assert_eq!(summary.property, Some(PropertyId::ElectionSafety));
        assert_eq!(summary.detected_at, Some(t(1_234_567)));

        let rendered = summary.render();
        assert!(rendered.contains("outcome  = FAILED"));
        assert!(rendered.contains("property = Election Safety"));
        assert!(rendered.contains("detected_at_nanos = 1234567"));
    }

    /// The history tally buckets every response kind, and the buckets sum to the
    /// total.
    #[test]
    fn history_stats_tally_every_response_kind() {
        let mut history = History::new();
        history.record_produce_success(produce_args("t", 0, b"v"), t(1), t(2), 0);
        history.record_consume_success(
            OpArgs::Consume {
                topic: "t".to_string(),
                partition: PartitionIndex(0),
                start_offset: 0,
                max_records: 1,
            },
            t(3),
            t(4),
            Vec::new(),
        );
        history.record_redirect(
            produce_args("t", 0, b"v"),
            t(5),
            t(6),
            vela_core::NodeId("node-1".to_string()),
        );
        history.record_failure(produce_args("t", 0, b"v"), t(7), t(8), OpResponse::NoLeader);
        history.record_failure(
            produce_args("t", 0, b"v"),
            t(9),
            t(10),
            OpResponse::IoError {
                message: "disk".to_string(),
            },
        );

        let stats = HistoryStats::from_history(&history);
        assert_eq!(stats.total_ops, 5);
        assert_eq!(stats.produce_ok, 1);
        assert_eq!(stats.consume_ok, 1);
        assert_eq!(stats.redirects, 1);
        assert_eq!(stats.no_leader, 1);
        assert_eq!(stats.io_errors, 1);

        let sum = stats.produce_ok
            + stats.consume_ok
            + stats.create_topic_ok
            + stats.delete_topic_ok
            + stats.redirects
            + stats.unresolved_redirections
            + stats.no_leader
            + stats.io_errors
            + stats.other_errors;
        assert_eq!(sum, stats.total_ops, "every op falls in exactly one bucket");
    }

    /// `write_to_dir` creates the directory and writes a seed-named file whose
    /// contents match `render`.
    #[test]
    fn write_to_dir_writes_a_seed_named_file() {
        let seed = 0x0102_0304_0506_0708;
        let artifact = FailureArtifact::from_outcome(
            seed,
            ScenarioParameters::default(),
            &failing_outcome(),
            &sample_history(),
        )
        .expect("failing outcome yields an artifact");

        // A unique temp directory so the test is hermetic and parallel-safe.
        let dir = std::env::temp_dir().join(format!(
            "vela-dst-artifact-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);

        let path = artifact.write_to_dir(&dir).expect("write must succeed");
        assert_eq!(path, dir.join(format!("dst-failure-{seed:016x}.txt")));
        let written = fs::read_to_string(&path).expect("file must be readable");
        assert_eq!(written, artifact.render());
        assert!(written.contains("Election Safety"));

        fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// The default directory is used when the env override is unset/empty; a set
    /// override wins. (Mutates a process-global env var, so kept in one test.)
    #[test]
    fn resolve_artifact_dir_honors_the_env_override() {
        // Saved and restored so the test does not perturb others.
        let saved = std::env::var_os(ARTIFACT_DIR_ENV);

        std::env::remove_var(ARTIFACT_DIR_ENV);
        assert_eq!(resolve_artifact_dir(), PathBuf::from(DEFAULT_ARTIFACT_DIR));

        std::env::set_var(ARTIFACT_DIR_ENV, "/tmp/custom-dst-dir");
        assert_eq!(resolve_artifact_dir(), PathBuf::from("/tmp/custom-dst-dir"));

        match saved {
            Some(v) => std::env::set_var(ARTIFACT_DIR_ENV, v),
            None => std::env::remove_var(ARTIFACT_DIR_ENV),
        }
    }
}
