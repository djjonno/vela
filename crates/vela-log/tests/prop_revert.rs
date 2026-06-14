//! Property test for revert semantics on the append-only log.
//!
//! Feature: vela-streaming-platform, Property 5
//!
//! **Property 5: Revert truncates the uncommitted suffix and protects committed
//! entries.** For any log and any index `idx`, `revert(idx)` with
//! `idx >= commit_index` removes exactly the entries with index greater than
//! `idx` and keeps the rest; `revert(idx)` with `idx < commit_index` is rejected,
//! leaving the stored entries and commit index unchanged.
//!
//! Validates: Requirements 6.10, 6.11.
//!
//! The commit index is modelled as [`Option<u64>`] where `None` is the
//! uncommitted state "preceding index 0" (Requirement 6.7). A `None` baseline is
//! never a lower bound, so every index satisfies `idx >= commit_index` and revert
//! is always permitted on an uncommitted log.

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogError, LogStorage, PayloadKind};

/// A generated revert scenario: how many entries the log holds, an optional
/// baseline commit to establish `commit_index`, and the index to revert to.
#[derive(Debug, Clone)]
struct Scenario {
    /// Per-entry terms; its length is the number of entries in the log (0..=20).
    terms: Vec<u64>,
    /// Optional baseline commit applied before the attempt, establishing a
    /// non-`None` `commit_index`. `None` leaves the log uncommitted.
    baseline_commit: Option<u64>,
    /// The index passed to the `revert` call under test. Deliberately ranges
    /// below, within, and above the stored range to exercise both branches.
    target: u64,
}

/// Strategy producing scenarios spanning empty/non-empty logs, committed and
/// uncommitted baselines, and below / within / above-range revert targets.
fn scenario_strategy() -> impl Strategy<Value = Scenario> {
    (0u64..=20)
        .prop_flat_map(|entry_count| {
            // Terms for each stored entry.
            let terms = proptest::collection::vec(1u64..=8, entry_count as usize);

            // Baseline commit: `None`, or some index within the stored range.
            let baseline = if entry_count == 0 {
                Just(None).boxed()
            } else {
                prop_oneof![
                    1 => Just(None),
                    3 => (0u64..entry_count).prop_map(Some),
                ]
                .boxed()
            };

            // Target spans a little past the end (and occasionally far past) so
            // the "above last index is a no-op truncate" path and the
            // "below commit is rejected" path are both exercised alongside
            // in-range reverts.
            let target = prop_oneof![
                9 => 0u64..(entry_count + 4),
                1 => 1_000u64..2_000,
            ];

            (terms, baseline, target)
        })
        .prop_map(|(terms, baseline_commit, target)| Scenario {
            terms,
            baseline_commit,
            target,
        })
}

/// Build a log for the scenario, applying the baseline commit if any.
fn build_log(scenario: &Scenario) -> InMemoryLog {
    let mut log = InMemoryLog::new();
    for (i, term) in scenario.terms.iter().enumerate() {
        let payload = EntryPayload::new(PayloadKind::Record, vec![i as u8]);
        log.append(payload, *term).expect("append never fails");
    }
    if let Some(baseline) = scenario.baseline_commit {
        log.commit(baseline)
            .expect("baseline commit is within bounds by construction");
    }
    log
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn revert_truncates_uncommitted_suffix_and_protects_committed(scenario in scenario_strategy()) {
        let mut log = build_log(&scenario);

        // Capture the pre-call state so we can assert "unchanged" on rejection
        // and compute the exact expected survivors on success.
        let commit_before = log.commit_index();
        let last_before = log.last_index();
        let entries_before = log.read(0, u64::MAX);

        let idx = scenario.target;

        // Reference predicate derived straight from Requirements 6.10 / 6.11.
        // Only a `Some(commit)` can make `idx` "below the commit index"; a `None`
        // baseline never does.
        let below_commit = matches!(commit_before, Some(commit) if idx < commit);

        let result = log.revert(idx);

        if below_commit {
            // 6.11: a revert below the commit index is rejected with
            // RevertBelowCommit and leaves both entries and commit unchanged.
            prop_assert_eq!(
                result,
                Err(LogError::RevertBelowCommit {
                    requested: idx,
                    commit: commit_before,
                }),
                "expected revert({}) below commit {:?} to be rejected",
                idx,
                commit_before
            );
            prop_assert_eq!(
                log.commit_index(),
                commit_before,
                "a rejected revert must leave the commit index unchanged"
            );
            prop_assert_eq!(
                log.read(0, u64::MAX),
                entries_before,
                "a rejected revert must leave stored entries unchanged"
            );
        } else {
            // 6.10: a revert at or above the commit index removes exactly the
            // entries with index greater than `idx`, keeping the rest.
            prop_assert!(
                result.is_ok(),
                "expected revert({idx}) to succeed (commit_before={commit_before:?}, \
                 last_before={last_before:?}), got {result:?}"
            );

            // The expected survivors are precisely those entries whose index is
            // <= idx; every entry with index > idx must be gone.
            let expected: Vec<_> = entries_before
                .iter()
                .filter(|entry| entry.index <= idx)
                .cloned()
                .collect();
            prop_assert_eq!(
                log.read(0, u64::MAX),
                expected,
                "revert must keep exactly the entries with index <= {}",
                idx
            );

            // No surviving entry may have an index greater than the revert point.
            if let Some(new_last) = log.last_index() {
                prop_assert!(
                    new_last <= idx,
                    "no entry with index > {} may survive (new last_index={})",
                    idx,
                    new_last
                );
            }

            // Revert never touches the commit index (6.10 only removes entries).
            prop_assert_eq!(
                log.commit_index(),
                commit_before,
                "revert must not change the commit index"
            );
        }
    }
}
