//! Property test for snapshots of the append-only log.
//!
//! Feature: vela-streaming-platform, Property 6
//!
//! **Property 6: Snapshot reflects exactly the committed prefix** — for any
//! log, a snapshot represents the committed entries up to and including the
//! commit index (and nothing uncommitted); on a log with no commit, the
//! snapshot is empty.
//!
//! **Validates: Requirements 6.7, 6.12**

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogStorage, PayloadKind};

/// A generated snapshot scenario: the per-entry terms (whose length is the
/// number of entries in the log) and an optional commit to apply before taking
/// the snapshot.
#[derive(Debug, Clone)]
struct Scenario {
    /// Per-entry terms; its length is the number of entries in the log (0..=40).
    terms: Vec<u64>,
    /// Optional commit applied before snapshotting, establishing a non-`None`
    /// commit index. `None` leaves the log uncommitted (Requirement 6.7).
    commit: Option<u64>,
}

/// Strategy producing scenarios spanning empty/non-empty logs and both the
/// uncommitted (`None`) and committed states.
fn scenario_strategy() -> impl Strategy<Value = Scenario> {
    proptest::collection::vec(1u64..=8, 0..=40).prop_flat_map(|terms| {
        let len = terms.len() as u64;
        // Commit: `None` (uncommitted), or some valid index within the stored
        // range. An empty log can only be uncommitted.
        let commit = if len == 0 {
            Just(None).boxed()
        } else {
            prop_oneof![
                1 => Just(None),
                4 => (0u64..len).prop_map(Some),
            ]
            .boxed()
        };
        (Just(terms), commit).prop_map(|(terms, commit)| Scenario { terms, commit })
    })
}

/// Build a log for the scenario, applying the commit if any. Each entry's
/// payload bytes encode its index so snapshot entries can be tied back to the
/// positions they were appended at.
fn build_log(scenario: &Scenario) -> InMemoryLog {
    let mut log = InMemoryLog::new();
    for (i, &term) in scenario.terms.iter().enumerate() {
        let payload = EntryPayload::new(PayloadKind::Record, (i as u64).to_le_bytes().to_vec());
        let assigned = log.append(payload, term).expect("append should succeed");
        assert_eq!(assigned, i as u64, "append must assign the next index");
    }
    if let Some(commit) = scenario.commit {
        log.commit(commit)
            .expect("commit is within bounds by construction");
    }
    log
}

proptest! {
    // Minimum 100 iterations; 256 cases gives ample coverage of the commit space.
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn snapshot_reflects_exactly_the_committed_prefix(scenario in scenario_strategy()) {
        let log = build_log(&scenario);
        let commit_index = log.commit_index();
        let snapshot = log.snapshot();

        // The snapshot always reports the log's own commit index (R6.12).
        prop_assert_eq!(
            snapshot.commit_index,
            commit_index,
            "snapshot must carry the log's commit index"
        );

        match commit_index {
            None => {
                // 6.7: with no commit, the snapshot is empty.
                prop_assert!(
                    snapshot.entries.is_empty(),
                    "an uncommitted log must produce an empty snapshot, got {} entries",
                    snapshot.entries.len()
                );
            }
            Some(committed) => {
                // 6.12: the snapshot is exactly the committed prefix 0..=committed.
                let expected_indices: Vec<u64> = (0..=committed).collect();
                let got_indices: Vec<u64> =
                    snapshot.entries.iter().map(|e| e.index).collect();
                prop_assert_eq!(
                    &got_indices,
                    &expected_indices,
                    "snapshot must contain exactly indices 0..={}",
                    committed
                );

                // Nothing uncommitted leaks in: the last snapshot index is the
                // commit index, never beyond it.
                prop_assert_eq!(
                    snapshot.entries.last().map(|e| e.index),
                    Some(committed),
                    "snapshot must not include any entry past the commit index"
                );

                // Each snapshot entry equals the stored entry at its index, so
                // the prefix is a faithful copy rather than a fabrication.
                for entry in &snapshot.entries {
                    prop_assert_eq!(
                        Some(entry.clone()),
                        log.entry(entry.index),
                        "snapshot entry must equal the stored entry at its index"
                    );
                }
            }
        }
    }
}
