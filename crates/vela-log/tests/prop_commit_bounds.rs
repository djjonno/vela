//! Property test for commit bounds on the append-only log.
//!
//! Feature: vela-streaming-platform, Property 4
//!
//! **Property 4: Commit advances only within valid bounds and never backward.**
//! For any log and any index `idx`, `commit(idx)` advances the commit index when
//! `current_commit <= idx <= last_index`, and otherwise (`idx < current_commit`
//! or `idx > last_index`) is rejected, leaving the commit index and stored
//! entries unchanged.
//!
//! Validates: Requirements 6.8, 6.9.
//!
//! The commit index is modelled as [`Option<u64>`] where `None` is the
//! uncommitted state "preceding index 0" (Requirement 6.7); a `None` baseline is
//! never a lower bound, so any index within the stored range is a valid advance
//! from it.

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogError, LogStorage, PayloadKind};

/// A generated commit scenario: how many entries the log holds, an optional
/// baseline commit to establish `current_commit`, and the index to attempt to
/// commit.
#[derive(Debug, Clone)]
struct Scenario {
    /// Per-entry terms; its length is the number of entries in the log (0..=20).
    terms: Vec<u64>,
    /// Optional baseline commit applied before the attempt, establishing a
    /// non-`None` `current_commit`. `None` leaves the log uncommitted.
    baseline_commit: Option<u64>,
    /// The index passed to the `commit` call under test. Deliberately ranges
    /// below, within, and above the stored range to exercise both branches.
    target: u64,
}

/// Strategy producing scenarios spanning empty/non-empty logs, committed and
/// uncommitted baselines, and in-range / out-of-range targets.
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
            // both the "above last index" and "below current commit" rejection
            // paths are exercised alongside valid advances.
            let target = prop_oneof![
                9 => 0u64..(entry_count + 4),
                1 => 1_000u64..2_000,
            ];

            (Just(entry_count), terms, baseline, target)
        })
        .prop_map(|(_entry_count, terms, baseline_commit, target)| Scenario {
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
    fn commit_advances_only_within_bounds_and_never_backward(scenario in scenario_strategy()) {
        let mut log = build_log(&scenario);

        // Capture the pre-call state so we can assert "unchanged" on rejection.
        let commit_before = log.commit_index();
        let last_index = log.last_index();
        let entries_before = log.read(0, u64::MAX);

        let idx = scenario.target;

        // Reference predicate derived straight from Requirements 6.8 / 6.9.
        // A `None` baseline (uncommitted, "preceding index 0") is never a lower
        // bound, so only a `Some(current)` can make an index "backward".
        let above_last = match last_index {
            None => true, // empty log: any commit is out of bounds.
            Some(highest) => idx > highest,
        };
        let below_current = matches!(commit_before, Some(current) if idx < current);
        let expected_valid = !above_last && !below_current;

        let result = log.commit(idx);

        if expected_valid {
            // 6.8: a valid commit succeeds and advances the index to `idx`.
            prop_assert!(
                result.is_ok(),
                "expected commit({idx}) to succeed (commit_before={commit_before:?}, \
                 last_index={last_index:?}), got {result:?}"
            );
            prop_assert_eq!(
                log.commit_index(),
                Some(idx),
                "commit index must advance to the requested index"
            );
        } else {
            // 6.9: an out-of-bounds commit is rejected with CommitOutOfBounds and
            // leaves the commit index unchanged. `LogError` no longer derives
            // `PartialEq` (its durable `Io` variant wraps a non-`PartialEq`
            // `std::io::Error`), so match on the variant and assert its fields.
            match &result {
                Err(LogError::CommitOutOfBounds {
                    requested,
                    current,
                    last,
                }) => {
                    prop_assert_eq!(*requested, idx);
                    prop_assert_eq!(*current, commit_before);
                    prop_assert_eq!(*last, last_index);
                }
                other => prop_assert!(
                    false,
                    "expected commit({}) to be rejected as out of bounds with \
                     CommitOutOfBounds, got {:?}",
                    idx,
                    other
                ),
            }
            prop_assert_eq!(
                log.commit_index(),
                commit_before,
                "a rejected commit must leave the commit index unchanged"
            );
        }

        // Either way, stored entries are never mutated by a commit (6.8, 6.9).
        prop_assert_eq!(
            log.read(0, u64::MAX),
            entries_before,
            "commit must never alter stored entries"
        );
    }
}
