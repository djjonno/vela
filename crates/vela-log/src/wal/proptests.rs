//! Property tests for [`DurableWal`](super::DurableWal) that need the
//! crate-internal filesystem seam.
//!
//! These live inside the `wal` module (rather than in the crate's `tests/`
//! directory) because the in-memory, fault-injecting
//! [`MemFileSystem`](super::fs::fault::MemFileSystem) is `#[cfg(test)]` and
//! crate-internal, so it is unreachable from an external integration test. A
//! `#[cfg(test)]` submodule can drive a `DurableWal` over that filesystem,
//! drop it, and reopen on the same backing store to exercise the crash/restart
//! shape recovery requires.
//!
//! ## Recovery round-trip under `Always` (task 15.1)
//!
//! ### Property: recovery round-trip preserves observable state
//!
//! *For any* sequence of `append` / `append_entries` / `commit` / `revert` (and
//! occasional `compaction`) operations applied to a `DurableWal` under the
//! [`Always`](super::SyncPolicy::Always) sync policy, when the WAL is forced
//! and reopened on the same filesystem, the reopened WAL reports identical
//! retained entries, `last_index`, `log_start_index`, and `commit_index` as the
//! original WAL held after those operations.
//!
//! **Validates: Requirements 5.3, 12.3**
//!
//! Requirement 5.3: "FOR ALL sequences of append, append_entries, commit, and
//! revert operations applied to a Durable_WAL whose buffered writes are forced
//! to stable storage, WHEN a new Durable_WAL is opened on the same
//! Data_Directory, THE reopened Durable_WAL SHALL report the same retained
//! entries, Last_Index, Log_Start_Index, and Commit_Index as the original
//! Durable_WAL held after those operations (recovery round-trip property)."

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::fs::fault::MemFileSystem;
use super::{DurableWal, WalConfig};
use crate::{EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage, PayloadKind};

/// Data directory used for every generated case; each case gets a fresh
/// [`MemFileSystem`], so the fixed path never collides across cases.
const DIR: &str = "/wal";

/// One generated operation. Raw selector fields (`sel`) are reduced modulo the
/// valid range *at apply time* against the live WAL state, so every generated
/// `commit` / `revert` / `compaction` targets an in-bounds index and every
/// `append_entries` batch is a legal, contiguous continuation or uncommitted
/// overwrite — building up a real, valid WAL state rather than relying on
/// rejected operations.
#[derive(Debug, Clone)]
enum Op {
    /// A single `append` of a fresh entry at `last_index + 1`.
    Append {
        term: u64,
        kind: u8,
        len: usize,
        fill: u8,
    },
    /// A contiguous `append_entries` batch. The first index is chosen within
    /// `[lo, next_index]`, where `lo` is `commit_index + 1` (so the batch never
    /// overwrites a committed entry) or `log_start_index` when nothing is
    /// committed; a start below `next_index` therefore overwrites the
    /// uncommitted suffix, and `next_index` extends the log.
    AppendEntries {
        count: usize,
        term: u64,
        kind: u8,
        len: usize,
        sel: u64,
    },
    /// Advance the commit index to a target in `[current_commit, last_index]`.
    Commit { sel: u64 },
    /// Revert to a target in `[max(commit_index, log_start_index), last_index]`,
    /// dropping the uncommitted suffix above it.
    Revert { sel: u64 },
    /// Compact to a retained point in `[log_start_index, commit_index]`,
    /// exercising the `log_start_index` round-trip (only when something is
    /// committed).
    Compaction { sel: u64 },
}

/// Map a small selector to a [`PayloadKind`], so generated entries span all
/// three kinds.
fn kind_of(sel: u8) -> PayloadKind {
    match sel % 3 {
        0 => PayloadKind::Record,
        1 => PayloadKind::Cluster,
        _ => PayloadKind::Noop,
    }
}

/// Build a payload of `len` bytes all equal to `fill`, tagged by `kind_sel`.
fn make_payload(kind_sel: u8, len: usize, fill: u8) -> EntryPayload {
    EntryPayload::new(kind_of(kind_sel), vec![fill; len])
}

/// Strategy for a single operation, weighted toward the two append forms so the
/// log grows enough for commit/revert/compaction to have material to act on.
fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u64..6, 0u8..3, 0usize..8, any::<u8>())
            .prop_map(|(term, kind, len, fill)| Op::Append { term, kind, len, fill }),
        3 => (1usize..5, 0u64..6, 0u8..3, 0usize..6, any::<u64>())
            .prop_map(|(count, term, kind, len, sel)| Op::AppendEntries { count, term, kind, len, sel }),
        2 => any::<u64>().prop_map(|sel| Op::Commit { sel }),
        1 => any::<u64>().prop_map(|sel| Op::Revert { sel }),
        1 => any::<u64>().prop_map(|sel| Op::Compaction { sel }),
    ]
}

/// Apply one operation to `wal`, deriving in-bounds arguments from the live
/// state. Operations that legitimately have nothing to do (e.g. a commit on an
/// empty log) are skipped; every operation that does run is constructed to be
/// valid, so no error is expected under these generators.
fn apply(wal: &mut DurableWal<MemFileSystem>, op: &Op) {
    match op {
        Op::Append {
            term,
            kind,
            len,
            fill,
        } => {
            wal.append(make_payload(*kind, *len, *fill), *term)
                .expect("append under Always should succeed without faults");
        }
        Op::AppendEntries {
            count,
            term,
            kind,
            len,
            sel,
        } => {
            let log_start = wal.log_start_index();
            let next = wal.last_index().map_or(log_start, |last| last + 1);
            // Never start at or below the commit index (that would be a
            // commit-conflict overwrite); otherwise start as low as the
            // retained range allows so overwrites of the uncommitted suffix are
            // exercised alongside plain extension.
            let lo = match wal.commit_index() {
                Some(commit) => commit + 1,
                None => log_start,
            };
            // `lo <= next` always: commit_index <= last_index, so
            // commit_index + 1 <= last_index + 1 == next; on an empty log
            // lo == log_start == next.
            let span = next - lo + 1;
            let start = lo + (sel % span);
            let entries: Vec<LogEntry> = (0..*count)
                .map(|offset| {
                    let index = start + offset as u64;
                    LogEntry {
                        index,
                        term: *term,
                        payload: make_payload(*kind, *len, (index as u8).wrapping_add(*term as u8)),
                    }
                })
                .collect();
            wal.append_entries(&entries)
                .expect("a contiguous, non-committed batch should be accepted");
        }
        Op::Commit { sel } => {
            if let Some(last) = wal.last_index() {
                let lo = wal.commit_index().unwrap_or(0);
                let span = last - lo + 1;
                let target = lo + (sel % span);
                wal.commit(target)
                    .expect("a target within [commit, last_index] should commit");
            }
        }
        Op::Revert { sel } => {
            if let Some(last) = wal.last_index() {
                let log_start = wal.log_start_index();
                let lo = wal.commit_index().unwrap_or(log_start);
                let span = last - lo + 1;
                let target = lo + (sel % span);
                wal.revert(target)
                    .expect("a target at/above the commit index should revert");
            }
        }
        Op::Compaction { sel } => {
            // Compaction needs a commit to bound the retained point; a retained
            // point in [log_start, commit] is always in bounds, and one equal to
            // log_start is a no-op.
            if let Some(commit) = wal.commit_index() {
                let log_start = wal.log_start_index();
                let span = commit - log_start + 1;
                let point = log_start + (sel % span);
                wal.compaction(point)
                    .expect("a retained point within [log_start, commit] is valid");
            }
        }
    }
}

/// The observable state a reopened WAL must reproduce exactly.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    last_index: Option<u64>,
    commit_index: Option<u64>,
    log_start_index: u64,
    entries: Vec<LogEntry>,
}

/// Capture the full observable state of `wal`: the bounds plus every retained
/// entry (read back from disk in ascending index order).
fn observe(wal: &DurableWal<MemFileSystem>) -> Observed {
    let log_start_index = wal.log_start_index();
    let last_index = wal.last_index();
    let entries = match last_index {
        Some(last) => wal.read(log_start_index, last),
        None => Vec::new(),
    };
    Observed {
        last_index,
        commit_index: wal.commit_index(),
        log_start_index,
        entries,
    }
}

proptest! {
    // At least 256 cases, per task 15.1 / Requirement 12.3.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Apply a random op sequence under `Always`, force, drop, and reopen on the
    /// same in-memory filesystem; the reopened WAL must report identical
    /// retained entries, `last_index`, `log_start_index`, and `commit_index`.
    #[test]
    fn recovery_roundtrip_under_always(
        // A small segment size exercises rollover and multi-segment layouts;
        // the low end occasionally forces the oversized-frame-own-segment path.
        segment_size in 16u64..256,
        ops in prop::collection::vec(op_strategy(), 0..40),
    ) {
        let fs = MemFileSystem::new();
        // `Always` is the default sync policy, the only consensus-safe one and
        // the only policy that guarantees an exact recovery round-trip.
        let cfg = WalConfig::new(DIR).with_segment_size(segment_size);

        let before = {
            let mut wal = DurableWal::open_with(cfg, fs.clone())
                .expect("open on a fresh filesystem should succeed");
            for op in &ops {
                apply(&mut wal, op);
            }
            // Under `Always` every successful op is already durable; flushing is
            // harmless and additionally exercises the flush path.
            wal.flush().expect("flush under Always should succeed");
            observe(&wal)
        }; // dropping `wal` releases the exclusive directory lock

        // Reopen on the same backing store with the same configuration.
        let reopen_cfg = WalConfig::new(DIR).with_segment_size(segment_size);
        let reopened = DurableWal::open_with(reopen_cfg, fs.clone())
            .expect("reopen on the same filesystem should succeed");
        let after = observe(&reopened);

        prop_assert_eq!(after.last_index, before.last_index, "last_index mismatch");
        prop_assert_eq!(after.commit_index, before.commit_index, "commit_index mismatch");
        prop_assert_eq!(
            after.log_start_index,
            before.log_start_index,
            "log_start_index mismatch"
        );
        prop_assert_eq!(after.entries, before.entries, "retained entries mismatch");
    }
}

// ===========================================================================
// Task 15.2: DurableWal vs InMemoryLog drop-in equivalence (no compaction)
// ===========================================================================
//
// ### Property: DurableWal is a drop-in equivalent of InMemoryLog
//
// *For any* sequence of `append` / `append_entries` / `commit` / `revert` /
// `flush` operations (performing **no** compaction) applied identically to a
// `DurableWal` under the [`Always`](super::SyncPolicy::Always) sync policy and
// to an `InMemoryLog`, every operation returns an equal result — the assigned
// index or the `Ok`/`Err`(variant) outcome — and after every operation both
// logs report equal observable state: `last_index`, `commit_index`, `term_at`,
// `entry`, `read`, and `snapshot`. `flush` is an always-successful no-op on
// `InMemoryLog`.
//
// **Validates: Requirements 1.2, 12.6**
//
// Requirement 1.2: "WHEN a sequence of `LogStorage` operations is applied to the
// Durable_WAL and the identical sequence is applied to an `InMemoryLog`, and no
// Compaction is performed, THE Durable_WAL SHALL return, for every operation in
// the sequence, a result equal to the result returned by the `InMemoryLog`,
// where equality covers both the operation's direct return value ... and the
// observable state subsequently reported by last_index, commit_index, term_at,
// entry, read, and snapshot."
//
// Inputs are built **once per operation** and fed unchanged to both logs, so any
// divergence is a real behavioral difference rather than an artifact of
// differing inputs. The `commit` / `revert` / `append_entries` targets are
// derived from the (identical) shared state with a raw selector that deliberately
// reaches *outside* the valid range, so the error paths
// (`CommitOutOfBounds`, `RevertBelowCommit`, `NonContiguousEntries`) are
// exercised and both implementations must reject identically — the whole point
// of drop-in fidelity.

/// One generated operation for the equivalence test. Unlike the recovery test's
/// [`Op`], this set excludes compaction (R1.2 equivalence holds only when no
/// compaction is performed) and adds an explicit [`Flush`](EqOp::Flush).
#[derive(Debug, Clone)]
enum EqOp {
    /// A single `append` of a fresh entry; both logs assign the same index.
    Append {
        term: u64,
        kind: u8,
        len: usize,
        fill: u8,
    },
    /// A `count`-entry, internally-contiguous batch whose first index is chosen
    /// from a window `[0, next_index + 2]` so it spans commit-conflict
    /// overwrites, valid suffix overwrites, plain extension, and gaps beyond the
    /// log — exercising both the `Ok` and `Err(NonContiguousEntries)` paths.
    AppendEntries {
        count: usize,
        term: u64,
        kind: u8,
        len: usize,
        start_sel: u64,
    },
    /// `commit` to a target in `[0, last_index + 1]`, spanning below-commit,
    /// in-range, and above-last so both valid commits and `CommitOutOfBounds`
    /// rejections are exercised.
    Commit { sel: u64 },
    /// `revert` to a target in `[0, last_index + 1]`, spanning below-commit
    /// (rejected), in-range (drops the uncommitted suffix), and above-last
    /// (no-op).
    Revert { sel: u64 },
    /// `flush`: forces the DurableWal and is a successful no-op on InMemoryLog.
    Flush,
}

/// Strategy for a single equivalence operation, weighted toward the append forms
/// so the log grows enough for commit/revert to act on real material.
fn eq_op_strategy() -> impl Strategy<Value = EqOp> {
    prop_oneof![
        4 => (0u64..6, 0u8..3, 0usize..8, any::<u8>())
            .prop_map(|(term, kind, len, fill)| EqOp::Append { term, kind, len, fill }),
        3 => (1usize..5, 0u64..6, 0u8..3, 0usize..6, any::<u64>())
            .prop_map(|(count, term, kind, len, start_sel)| EqOp::AppendEntries {
                count,
                term,
                kind,
                len,
                start_sel,
            }),
        2 => any::<u64>().prop_map(|sel| EqOp::Commit { sel }),
        2 => any::<u64>().prop_map(|sel| EqOp::Revert { sel }),
        1 => Just(EqOp::Flush),
    ]
}

/// Map a [`LogError`] to its variant name so two `Result`s can be compared for
/// equality even though `LogError` no longer derives `PartialEq` (it carries a
/// `std::io::Error`). For drop-in equivalence both logs must produce the same
/// `Ok`/`Err` outcome *and*, on rejection, the same error variant.
fn err_name(err: &LogError) -> &'static str {
    match err {
        LogError::CommitOutOfBounds { .. } => "CommitOutOfBounds",
        LogError::RevertBelowCommit { .. } => "RevertBelowCommit",
        LogError::NonContiguousEntries => "NonContiguousEntries",
        LogError::Io { .. } => "Io",
        LogError::Corruption { .. } => "Corruption",
        LogError::CompactionOutOfBounds { .. } => "CompactionOutOfBounds",
        LogError::Config { .. } => "Config",
    }
}

/// Reduce an index-returning result to an equatable summary (`Ok(index)` or
/// `Err(variant_name)`).
fn summary_index(result: Result<u64, LogError>) -> Result<u64, &'static str> {
    result.map_err(|err| err_name(&err))
}

/// Reduce a unit-returning result to an equatable summary (`Ok(())` or
/// `Err(variant_name)`).
fn summary_unit(result: Result<(), LogError>) -> Result<(), &'static str> {
    result.map_err(|err| err_name(&err))
}

/// Apply one operation to **both** logs with identical inputs and assert their
/// direct return values are equal.
fn apply_eq(
    dur: &mut DurableWal<MemFileSystem>,
    mem: &mut InMemoryLog,
    op: &EqOp,
) -> Result<(), TestCaseError> {
    match op {
        EqOp::Append {
            term,
            kind,
            len,
            fill,
        } => {
            let payload = make_payload(*kind, *len, *fill);
            let dur_result = dur.append(payload.clone(), *term);
            let mem_result = mem.append(payload, *term);
            prop_assert_eq!(
                summary_index(dur_result),
                summary_index(mem_result),
                "append return value mismatch"
            );
        }
        EqOp::AppendEntries {
            count,
            term,
            kind,
            len,
            start_sel,
        } => {
            // Derive the batch start from the shared state (identical for both
            // logs unless they have already diverged, which a prior assertion
            // would have caught). The window `[0, next + 2]` reaches past the
            // log end so gap rejections are exercised alongside valid batches.
            let next = mem.last_index().map_or(0, |last| last + 1);
            let start = start_sel % (next + 3);
            let entries: Vec<LogEntry> = (0..*count)
                .map(|offset| {
                    let index = start + offset as u64;
                    LogEntry {
                        index,
                        term: *term,
                        payload: make_payload(*kind, *len, (index as u8).wrapping_add(*term as u8)),
                    }
                })
                .collect();
            // The very same slice is handed to both logs.
            let dur_result = dur.append_entries(&entries);
            let mem_result = mem.append_entries(&entries);
            prop_assert_eq!(
                summary_unit(dur_result),
                summary_unit(mem_result),
                "append_entries return value mismatch"
            );
        }
        EqOp::Commit { sel } => {
            // `[0, last_index + 1]` spans below-commit, in-range, and above-last
            // (and `{0, 1}` on an empty log, both rejected).
            let bound = mem.last_index().map_or(0, |last| last + 1);
            let target = sel % (bound + 2);
            let dur_result = dur.commit(target);
            let mem_result = mem.commit(target);
            prop_assert_eq!(
                summary_unit(dur_result),
                summary_unit(mem_result),
                "commit return value mismatch"
            );
        }
        EqOp::Revert { sel } => {
            let bound = mem.last_index().map_or(0, |last| last + 1);
            let target = sel % (bound + 2);
            let dur_result = dur.revert(target);
            let mem_result = mem.revert(target);
            prop_assert_eq!(
                summary_unit(dur_result),
                summary_unit(mem_result),
                "revert return value mismatch"
            );
        }
        EqOp::Flush => {
            let dur_result = dur.flush();
            let mem_result = mem.flush();
            prop_assert_eq!(
                summary_unit(dur_result),
                summary_unit(mem_result),
                "flush return value mismatch"
            );
        }
    }
    Ok(())
}

/// Assert the two logs expose equal observable state: `last_index`,
/// `commit_index`, `snapshot`, and `term_at`/`entry` swept across the retained
/// range plus a margin above `last_index` (to confirm both report `None`
/// out of range).
fn assert_equivalent(
    dur: &DurableWal<MemFileSystem>,
    mem: &InMemoryLog,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(dur.last_index(), mem.last_index(), "last_index mismatch");
    prop_assert_eq!(
        dur.commit_index(),
        mem.commit_index(),
        "commit_index mismatch"
    );
    // `Snapshot` derives `PartialEq`, so this compares commit index plus every
    // committed entry's index, term, and payload bytes.
    prop_assert_eq!(dur.snapshot(), mem.snapshot(), "snapshot mismatch");

    // Sweep one index past the end so the out-of-range `None` agreement is
    // checked on both sides.
    let hi = mem.last_index().map_or(0, |last| last + 2);
    for i in 0..=hi {
        prop_assert_eq!(dur.term_at(i), mem.term_at(i), "term_at({}) mismatch", i);
        // `LogEntry` derives `PartialEq`: compares index, term, and payload.
        prop_assert_eq!(dur.entry(i), mem.entry(i), "entry({}) mismatch", i);
    }
    Ok(())
}

proptest! {
    // At least 256 cases, per task 15.2 / Requirement 12.6.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Apply identical op sequences (no compaction) to a `DurableWal` under
    /// `Always` and an `InMemoryLog`; assert equal per-op return values and
    /// equal observable state after every op, plus equal `read` results over a
    /// set of generated ranges.
    #[test]
    fn durable_matches_in_memory_without_compaction(
        // A small segment size exercises rollover / multi-segment layouts on the
        // DurableWal side; InMemoryLog is unaffected by it.
        segment_size in 16u64..256,
        ops in prop::collection::vec(eq_op_strategy(), 0..40),
        // Raw read ranges (including `start > end` and ranges beyond the log) fed
        // identically to both logs.
        read_probes in prop::collection::vec((0u64..24, 0u64..24), 0..6),
    ) {
        let fs = MemFileSystem::new();
        // `Always` is the default sync policy and the only consensus-safe one;
        // it is also what makes the DurableWal observably equivalent here.
        let cfg = WalConfig::new(DIR).with_segment_size(segment_size);

        let mut dur = DurableWal::open_with(cfg, fs)
            .expect("open on a fresh filesystem should succeed");
        let mut mem = InMemoryLog::new();

        // Equivalent from the empty state, before any operation.
        assert_equivalent(&dur, &mem)?;

        for op in &ops {
            apply_eq(&mut dur, &mut mem, op)?;
            assert_equivalent(&dur, &mem)?;
        }

        // `read` equivalence over arbitrary ranges, including degenerate ones.
        for (start, end) in &read_probes {
            prop_assert_eq!(
                dur.read(*start, *end),
                mem.read(*start, *end),
                "read({}, {}) mismatch",
                start,
                end
            );
        }
    }
}

// ===========================================================================
// Task 2.2: durable hard-state round-trip (Feature: per-topic-log-durability,
// Property 10)
// ===========================================================================
//
// ### Property: the last persisted hard state is restored byte-for-byte
//
// *For any* non-empty sequence of persisted `(current_term, voted_for)` values
// applied in order to a `DurableWal` under the
// [`Always`](super::SyncPolicy::Always) sync policy, when the WAL is dropped and
// reopened on the same filesystem, the reopened WAL's
// [`hard_state`](LogStorage::hard_state) equals the **last** persisted
// `HardState` — the prior persists are superseded, and the surviving value
// round-trips exactly (term and vote alike).
//
// **Validates: Requirements 10.3**
//
// Requirement 10.3: "FOR ALL sequences of term advances and vote grants applied
// to a Durable replica whose Raft_Hard_State was persisted, WHEN the replica is
// restarted on the same data, THE restored `current_term` and `voted_for` SHALL
// equal the values the replica held immediately before the restart."
//
// `Always` is the consensus-safe policy (the `DurableWal` default) and the only
// one this feature constructs for a log that backs consensus, so the test
// matches that convention. The values are unconstrained `u64` terms and
// arbitrary `Option<u64>` votes, so the generator sweeps the full hard-state
// input space rather than a sanitized subset.

/// Strategy for one persisted hard state: an arbitrary term and an arbitrary
/// optional vote, spanning both the voted (`Some`) and not-yet-voted (`None`)
/// cases across the whole `u64` range.
fn hard_state_strategy() -> impl Strategy<Value = HardState> {
    (any::<u64>(), proptest::option::of(any::<u64>())).prop_map(|(current_term, voted_for)| {
        HardState {
            current_term,
            voted_for,
        }
    })
}

proptest! {
    // At least 256 cases, matching this crate's existing proptest config and
    // comfortably above the project's 100-iteration minimum.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Persist a non-empty sequence of hard-state values under `Always`, drop
    /// and reopen on the same in-memory filesystem, and assert the reopened WAL
    /// restores exactly the last value persisted.
    #[test]
    fn durable_hard_state_round_trip(
        states in prop::collection::vec(hard_state_strategy(), 1..40),
    ) {
        // The sequence is non-empty by construction, so a last value exists.
        // `HardState` is `Copy`, so this is the value the WAL held immediately
        // before the simulated restart.
        let expected = *states.last().expect("sequence is non-empty by construction");

        let fs = MemFileSystem::new();
        {
            // `Always` is the default sync policy and the only consensus-safe
            // one; each persist is durable before it returns.
            let mut wal = DurableWal::open_with(WalConfig::new(DIR), fs.clone())
                .expect("open on a fresh filesystem should succeed");
            for state in &states {
                wal.persist_hard_state(*state)
                    .expect("persist_hard_state under Always should succeed without faults");
            }
        } // dropping `wal` releases the exclusive directory lock

        // Reopen on the same backing store: the recovered hard state must equal
        // the last value persisted, superseding every earlier persist.
        let reopened = DurableWal::open_with(WalConfig::new(DIR), fs.clone())
            .expect("reopen on the same filesystem should succeed");

        prop_assert_eq!(
            reopened.hard_state(),
            Some(expected),
            "reopened hard state must equal the last persisted value"
        );
    }
}
