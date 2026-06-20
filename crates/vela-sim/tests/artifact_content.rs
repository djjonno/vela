#![cfg(feature = "sim")]
//! Example/integration tests for failure-artifact content (task 21.3).
//!
//! Feature: deterministic-simulation-testing — when a [`Simulation_Run`] ends
//! with a failing [`Outcome`], the harness must hand a developer (and CI)
//! everything needed to understand *and replay* the failure: the seed and
//! [`ScenarioParameters`], the violated [`PropertyId`] and the logical detection
//! [`VirtualInstant`], the recorded [`History`] (the replay-free diagnostic
//! trace), and the structured `detail` naming the affected group / term /
//! replicas (Requirements 2.1, 2.3, 13.1, 13.2, 13.5).
//!
//! Where the unit tests inside `artifact.rs` exercise the constructors against
//! the crate-private helpers, these are *example-style* tests that drive the
//! public [`vela_sim::artifact`] API from **outside** the crate: they assemble a
//! representative failing run by hand (a failing [`Outcome::Failed`] plus a
//! [`History`] of a successful produce, a successful consume, and an expected
//! failure response), build the [`FailureArtifact`], and assert its structured
//! fields *and* its rendered document carry the replayable, diagnosable record
//! the requirements demand. The distinctive seed / parameter / offset values
//! used below let the rendered-text assertions match real content rather than
//! incidental substrings.
//!
//! Validates: Requirements 2.1, 2.3, 13.1, 13.2, 13.5
//!
//! [`Simulation_Run`]: vela_sim
//! [`Outcome`]: vela_sim::runtime::Outcome

use std::ffi::OsString;
use std::fs;

use vela_sim::artifact::{
    persist_run, resolve_artifact_dir, FailureArtifact, RunSummary, ARTIFACT_DIR_ENV,
};
use vela_sim::checker::PropertyId;
use vela_sim::history::{History, OpArgs, OpResponse};
use vela_sim::runtime::Outcome;
use vela_sim::scenario::ScenarioParameters;
use vela_sim::scheduler::VirtualInstant;

use vela_core::{PartitionIndex, Record};

/// A distinctive seed whose `{:016x}` form (`deadbeefcafef00d`) and decimal form
/// are unlikely to collide with any incidental substring in the rendered output,
/// so a rendered-text match proves the seed itself is reported.
const SEED: u64 = 0xDEAD_BEEF_CAFE_F00D;

/// A distinctive committed offset for the successful produce; `offset=4242` is a
/// recognizable needle in the rendered history dump.
const PRODUCE_OFFSET: u64 = 4242;

/// A distinctive detection instant in nanoseconds; `987654321` is a recognizable
/// needle proving the logical detection instant is rendered (Requirement 2.3).
const DETECTED_AT_NANOS: u64 = 987_654_321;

/// A distinctive workload size baked into the scenario parameters; `13579`
/// appears in the rendered parameter block, proving the parameters (not just the
/// seed) are reported for replay (Requirement 2.1).
const WORKLOAD_SIZE: usize = 13_579;

fn t(nanos: u64) -> VirtualInstant {
    VirtualInstant::from_nanos(nanos)
}

/// Scenario parameters carrying a few distinctive values so the rendered
/// parameter block can be matched on real content.
fn sample_params() -> ScenarioParameters {
    ScenarioParameters {
        node_count: 5,
        replication_factor: 3,
        partition_count: 7,
        workload_size: WORKLOAD_SIZE,
        ..ScenarioParameters::default()
    }
}

fn produce_args(topic: &str, partition: u32, value: &[u8]) -> OpArgs {
    OpArgs::Produce {
        topic: topic.to_string(),
        partition: PartitionIndex(partition),
        key: None,
        value: value.to_vec(),
    }
}

fn consume_args(topic: &str, partition: u32, start_offset: u64, max_records: u32) -> OpArgs {
    OpArgs::Consume {
        topic: topic.to_string(),
        partition: PartitionIndex(partition),
        start_offset,
        max_records,
    }
}

/// A representative recorded run: a successful produce (with an assigned offset),
/// a successful consume (returning one ordered record), and an expected failure
/// response (a no-leader rejection recorded rather than discarded). This is the
/// "replayable trace / diagnostics" the artifact embeds (Requirement 13.2).
fn sample_history() -> History {
    let mut history = History::new();
    history.record_produce_success(
        produce_args("orders", 0, b"event-payload"),
        t(100),
        t(200),
        PRODUCE_OFFSET,
    );
    history.record_consume_success(
        consume_args("orders", 0, PRODUCE_OFFSET, 16),
        t(300),
        t(400),
        vec![Record {
            key: None,
            value: b"event-payload".to_vec(),
        }],
    );
    history.record_failure(
        produce_args("orders", 0, b"after-leader-loss"),
        t(500),
        t(600),
        OpResponse::NoLeader,
    );
    history
}

/// A representative failing outcome: an Election-Safety breach whose `detail`
/// names the affected group, term, and the two conflicting replicas — the
/// structured diagnostics captured at detection time (Requirement 13.5).
fn failing_outcome() -> Outcome {
    Outcome::Failed {
        property: PropertyId::ElectionSafety,
        at: t(DETECTED_AT_NANOS),
        detail: "group=orders/0 term=4 has two leaders: node-0 and node-2".to_string(),
    }
}

/// Requirements 2.1, 2.3, 13.1, 13.2, 13.5: a failing outcome yields a
/// `FailureArtifact` whose structured fields capture the seed, the scenario
/// parameters, the violated property, the detection instant, the structured
/// diagnostic detail, and the full recorded history.
#[test]
fn failing_outcome_yields_structured_replayable_artifact() {
    let params = sample_params();
    let history = sample_history();
    let outcome = failing_outcome();

    let artifact = FailureArtifact::from_outcome(SEED, params, &outcome, &history)
        .expect("a failing Outcome must yield a FailureArtifact");

    // Replayable: the seed and the exact scenario parameters are carried, so a
    // re-run of SimRuntime::run(RunConfig { seed, params }) reproduces it (2.1).
    assert_eq!(artifact.seed, SEED);
    assert_eq!(artifact.params, params);

    // The violated property and the logical detection instant (2.3, 13.1).
    assert_eq!(artifact.property, PropertyId::ElectionSafety);
    assert_eq!(artifact.detected_at, t(DETECTED_AT_NANOS));

    // The structured diagnostics naming group / term / replicas (13.5).
    assert!(
        artifact.detail.contains("term=4")
            && artifact.detail.contains("node-0")
            && artifact.detail.contains("node-2"),
        "the artifact detail must name the affected group/term/replicas, got: {}",
        artifact.detail
    );

    // The full recorded History is embedded verbatim for replay-free reading
    // (13.2): the produce, the consume, and the recorded failure all survive.
    assert_eq!(artifact.history, history);
    assert_eq!(artifact.history.len(), 3);
}

/// Requirements 2.1, 2.3, 13.1, 13.2, 13.5: the artifact's *rendered* document
/// surfaces the same record — the seed, parameters, property, instant, history,
/// diagnostics — and documents the replay path so the file is self-describing.
#[test]
fn rendered_artifact_reports_seed_params_property_instant_history_and_replay() {
    let artifact =
        FailureArtifact::from_outcome(SEED, sample_params(), &failing_outcome(), &sample_history())
            .expect("a failing Outcome must yield a FailureArtifact");

    let rendered = artifact.render();

    // Seed reported (2.1) — the distinctive hex form proves it is the seed.
    assert!(
        rendered.contains(&format!("0x{SEED:016x}")),
        "rendered artifact must report the seed in hex; got:\n{rendered}"
    );

    // Scenario parameters reported (2.1): the parameter section plus a
    // distinctive value (the workload size) prove the params travel with it.
    assert!(rendered.contains("## scenario parameters"));
    assert!(
        rendered.contains(&WORKLOAD_SIZE.to_string()),
        "rendered artifact must report the scenario parameters (workload_size); got:\n{rendered}"
    );

    // Violated property and detection instant (2.3, 13.1).
    assert!(
        rendered.contains("Election Safety"),
        "rendered artifact must name the violated property; got:\n{rendered}"
    );
    assert!(
        rendered.contains(&DETECTED_AT_NANOS.to_string()),
        "rendered artifact must report the detection instant; got:\n{rendered}"
    );

    // Structured diagnostics naming group/term/replicas (13.5).
    assert!(
        rendered.contains("orders")
            && rendered.contains("term=4")
            && rendered.contains("node-0")
            && rendered.contains("node-2"),
        "rendered artifact must carry the group/term/replica diagnostic; got:\n{rendered}"
    );

    // The recorded History is rendered (13.2): the recorded operations section,
    // the produce's assigned offset, and the expected no-leader response.
    assert!(rendered.contains("## recorded operations"));
    assert!(
        rendered.contains(&format!("offset={PRODUCE_OFFSET}")),
        "rendered history must show the produce's committed offset; got:\n{rendered}"
    );
    assert!(
        rendered.contains("ConsumeOk"),
        "rendered history must show the successful consume; got:\n{rendered}"
    );
    assert!(
        rendered.contains("NoLeader"),
        "rendered history must show the recorded failure response; got:\n{rendered}"
    );

    // The replay path is documented: the artifact explains a run is a pure
    // function of (seed, params) and points at re-running SimRuntime::run.
    assert!(
        rendered.contains("## replay") && rendered.contains("SimRuntime::run"),
        "rendered artifact must document how to replay from seed + params; got:\n{rendered}"
    );
}

/// A passing (and an invalid) outcome has no failure artifact — only failing
/// runs produce one. Confirms the public `from_outcome` contract from outside
/// the crate.
#[test]
fn non_failing_outcomes_produce_no_artifact() {
    let params = sample_params();
    let history = sample_history();

    assert!(
        FailureArtifact::from_outcome(SEED, params, &Outcome::Passed, &history).is_none(),
        "a passing run has no failure artifact"
    );
    assert!(
        FailureArtifact::from_outcome(
            SEED,
            params,
            &Outcome::Invalid {
                detail: "replication factor exceeds node count".to_string(),
            },
            &history,
        )
        .is_none(),
        "an invalid run has no failure artifact"
    );
}

/// A [`RunSummary`] is produced **regardless of outcome**, including for a
/// passing run (Requirement 13.4 underpins the always-on record): it carries the
/// seed, a `PASSED` label, no violated property, and a faithful tally of the
/// recorded history.
#[test]
fn run_summary_is_produced_for_a_passing_outcome() {
    let summary =
        RunSummary::from_outcome(SEED, sample_params(), &Outcome::Passed, &sample_history());

    assert_eq!(summary.seed, SEED);
    assert_eq!(summary.outcome, "PASSED");
    assert_eq!(summary.property, None);
    assert_eq!(summary.detected_at, None);
    // The history tally reflects the three recorded ops.
    assert_eq!(summary.history.total_ops, 3);
    assert_eq!(summary.history.produce_ok, 1);
    assert_eq!(summary.history.consume_ok, 1);
    assert_eq!(summary.history.no_leader, 1);

    let rendered = summary.render();
    assert!(rendered.contains("outcome  = PASSED"));
    // A passing summary names no violated property.
    assert!(!rendered.contains("property ="));
}

/// `write_to_dir` writes a seed-named file whose contents are exactly
/// `render()`. Hermetic: a unique temp directory keyed by pid + thread, cleaned
/// up at the end, and no environment variables touched.
#[test]
fn write_to_dir_writes_a_seed_named_file_matching_render() {
    let artifact =
        FailureArtifact::from_outcome(SEED, sample_params(), &failing_outcome(), &sample_history())
            .expect("a failing Outcome must yield a FailureArtifact");

    let dir = std::env::temp_dir().join(format!(
        "vela-dst-artifact-content-write-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = fs::remove_dir_all(&dir);

    let path = artifact
        .write_to_dir(&dir)
        .expect("writing the artifact to a fresh temp dir must succeed");

    // The filename incorporates the seed and lives in the requested directory.
    assert_eq!(path.file_name().unwrap(), artifact.filename().as_str());
    assert_eq!(path.parent().unwrap(), dir);
    assert!(
        artifact.filename().contains(&format!("{SEED:016x}")),
        "the artifact filename must incorporate the seed"
    );

    // The on-disk contents are exactly the rendered document.
    let written = fs::read_to_string(&path).expect("the artifact file must be readable");
    assert_eq!(written, artifact.render());

    let _ = fs::remove_dir_all(&dir);
}

/// `persist_run` writes the always-on summary (regardless of outcome) and, on a
/// failing outcome, the full artifact, both to the resolved directory — which
/// honors [`ARTIFACT_DIR_ENV`]. Their on-disk contents match the freshly
/// rendered documents.
///
/// This is the **only** test in this binary that touches [`ARTIFACT_DIR_ENV`].
/// It saves the variable's prior value, points it at a unique temp directory,
/// performs the run, then restores the variable, so no sibling test observes the
/// override. (Each `tests/*.rs` file is its own process, so this cannot leak to
/// other files either.)
#[test]
fn persist_run_writes_summary_always_and_artifact_on_failure() {
    let params = sample_params();
    let history = sample_history();
    let outcome = failing_outcome();

    let dir = std::env::temp_dir().join(format!(
        "vela-dst-artifact-content-persist-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = fs::remove_dir_all(&dir);

    // Save and override the artifact-dir environment variable, restoring it no
    // matter how the assertions below resolve.
    let saved: Option<OsString> = std::env::var_os(ARTIFACT_DIR_ENV);
    std::env::set_var(ARTIFACT_DIR_ENV, &dir);

    // With the override set, the resolved directory is exactly our temp dir.
    assert_eq!(resolve_artifact_dir(), dir);

    let result = persist_run(SEED, params, &outcome, &history);

    // Restore the environment immediately after the side-effecting call.
    match saved {
        Some(value) => std::env::set_var(ARTIFACT_DIR_ENV, value),
        None => std::env::remove_var(ARTIFACT_DIR_ENV),
    }

    let (summary_path, artifact_path) =
        result.expect("persist_run must succeed writing to a fresh temp dir");

    // The summary is always written and matches a freshly rendered summary.
    let expected_summary = RunSummary::from_outcome(SEED, params, &outcome, &history).render();
    let summary_on_disk =
        fs::read_to_string(&summary_path).expect("the summary file must be readable");
    assert_eq!(summary_on_disk, expected_summary);

    // On a failing outcome, the full artifact is also written and matches render.
    let artifact_path = artifact_path.expect("a failing run must also write a FailureArtifact");
    let expected_artifact = FailureArtifact::from_outcome(SEED, params, &outcome, &history)
        .expect("failing outcome yields an artifact")
        .render();
    let artifact_on_disk =
        fs::read_to_string(&artifact_path).expect("the artifact file must be readable");
    assert_eq!(artifact_on_disk, expected_artifact);

    let _ = fs::remove_dir_all(&dir);
}
