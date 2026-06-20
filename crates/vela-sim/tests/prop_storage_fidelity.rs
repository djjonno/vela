#![cfg(feature = "sim")]
//! Property test: Sim_Storage model fidelity and the durability boundary.
//!
//! Feature: deterministic-simulation-testing, Property 5: Storage model
//! fidelity and durability boundary
//!
//! Property 5 asserts the four behaviours Sim_Storage must exhibit so that a
//! simulated replica's log is indistinguishable from a production durable
//! replica's log:
//!
//! - **Fidelity (Requirement 7.1).** With no Storage_Fault, Sim_Storage's
//!   durable backend (the real [`DurableWal`] over a deterministic in-memory
//!   [`FaultFileSystem`], driven through the [`LogStorage`] seam) returns, for
//!   every operation in a sequence, a result equal to a reference
//!   [`InMemoryLog`]'s result, and reports equal observable state
//!   (`last_index`, `commit_index`, `term_at`, `entry`, `read`, `snapshot`)
//!   after every operation. Because Sim_Storage *is* the production WAL, this is
//!   equivalence to the production durable log by construction.
//! - **Crash durability boundary (Requirement 7.2, and 7.6).** Under
//!   [`SyncPolicy::Always`] every acknowledged append/commit and the persisted
//!   [`HardState`] is forced to stable storage before it returns, so a
//!   [`crash`](SimStorageHandle::crash) (drop the un-fsynced tail) followed by a
//!   reopen recovers *exactly* the acknowledged state — every record and the
//!   hard state survive, nothing is fabricated.
//! - **Torn tail (Requirement 7.3).** A torn trailing write recovers by
//!   discarding the torn tail down to the last intact record: the reopen
//!   succeeds (never errors or panics) and never grows the log beyond what was
//!   written; nothing acknowledged is lost.
//! - **I/O error (Requirement 7.4).** An armed I/O-error fault surfaces through
//!   the [`LogStorage`] `Result` (or fail-stop on a reopen) at the next matching
//!   operation — never a silent success.
//!
//! All four are exercised within a **single** `proptest` case so the property is
//! one indivisible statement about a Sim_Storage replica. Every random choice
//! (the op sequence, the persisted hard state, the torn-tail byte count, and the
//! armed I/O-error kind) is a deterministic function of the generated inputs, so
//! a failing case replays from its seed.
//!
//! The detailed torn-tail / armed-I/O-error edge cases are covered separately by
//! task 9.4; here they are part of Property 5's coverage at the property level.
//!
//! Validates: Requirements 7.1, 7.2, 7.3, 7.4

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use vela_core::{GroupKey, NodeId, PartitionIndex};
use vela_log::{
    EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage, PayloadKind, SyncPolicy,
};
use vela_sim::rng::SplitMix64;
use vela_sim::storage::{IoFaultKind, SimBackend, SimStorageHandle};

/// Build a `(topic, partition)` group key.
fn group(topic: &str, partition: u32) -> GroupKey {
    (topic.to_string(), PartitionIndex(partition))
}

/// Map a small selector to a [`PayloadKind`] so generated entries span all
/// three kinds the log carries.
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

/// One generated [`LogStorage`] operation applied identically to both logs.
///
/// `commit` / `revert` / `append_entries` carry raw selectors that are reduced
/// against the (shared) live state at apply time, deliberately reaching just
/// past the valid range so both the accepting and the rejecting paths
/// (`CommitOutOfBounds`, `RevertBelowCommit`, `NonContiguousEntries`) are
/// exercised — fidelity must hold for rejections too.
#[derive(Debug, Clone)]
enum Op {
    /// A single `append` of a fresh entry; both logs assign the same index.
    Append {
        term: u64,
        kind: u8,
        len: usize,
        fill: u8,
    },
    /// A `count`-entry, internally-contiguous batch whose first index is chosen
    /// from a window reaching past the log end, so it spans commit-conflict
    /// overwrites, valid suffix overwrites, plain extension, and gaps.
    AppendEntries {
        count: usize,
        term: u64,
        kind: u8,
        len: usize,
        start_sel: u64,
    },
    /// `commit` to a target spanning below-commit, in-range, and above-last.
    Commit { sel: u64 },
    /// `revert` to a target spanning below-commit, in-range, and above-last.
    Revert { sel: u64 },
    /// `flush`: forces the durable WAL, an always-successful no-op on the
    /// in-memory reference.
    Flush,
}

/// Strategy for a single operation, weighted toward the append forms so the log
/// grows enough for commit/revert to act on real material.
fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u64..6, 0u8..3, 0usize..8, any::<u8>())
            .prop_map(|(term, kind, len, fill)| Op::Append { term, kind, len, fill }),
        3 => (1usize..5, 0u64..6, 0u8..3, 0usize..6, any::<u64>())
            .prop_map(|(count, term, kind, len, start_sel)| Op::AppendEntries {
                count,
                term,
                kind,
                len,
                start_sel,
            }),
        2 => any::<u64>().prop_map(|sel| Op::Commit { sel }),
        2 => any::<u64>().prop_map(|sel| Op::Revert { sel }),
        1 => Just(Op::Flush),
    ]
}

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

/// Map a [`LogError`] to its variant name so two `Result`s can be compared for
/// equality even though `LogError` does not derive `PartialEq` (it carries a
/// `std::io::Error`). Fidelity requires the same `Ok`/`Err` outcome *and*, on
/// rejection, the same error variant.
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

/// Reduce an index-returning result to an equatable summary.
fn summary_index(result: Result<u64, LogError>) -> Result<u64, &'static str> {
    result.map_err(|err| err_name(&err))
}

/// Reduce a unit-returning result to an equatable summary.
fn summary_unit(result: Result<(), LogError>) -> Result<(), &'static str> {
    result.map_err(|err| err_name(&err))
}

/// Apply one operation to **both** logs with identical inputs derived from the
/// shared state, asserting their direct return values are equal.
fn apply_eq(dur: &mut SimBackend, mem: &mut InMemoryLog, op: &Op) -> Result<(), TestCaseError> {
    match op {
        Op::Append {
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
        Op::AppendEntries {
            count,
            term,
            kind,
            len,
            start_sel,
        } => {
            // Window `[0, next + 2]` reaches past the log end so gap rejections
            // are exercised alongside valid batches; identical for both logs
            // unless they have already diverged (a prior assertion would catch).
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
            let dur_result = dur.append_entries(&entries);
            let mem_result = mem.append_entries(&entries);
            prop_assert_eq!(
                summary_unit(dur_result),
                summary_unit(mem_result),
                "append_entries return value mismatch"
            );
        }
        Op::Commit { sel } => {
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
        Op::Revert { sel } => {
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
        Op::Flush => {
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

/// Assert the durable backend and the in-memory reference expose equal
/// observable state. `hard_state` is intentionally excluded: the durable WAL
/// always reports `Some(..)` while `InMemoryLog` reports `None` by design, so
/// hard-state fidelity is asserted via the durability round-trip instead.
fn assert_equivalent(dur: &SimBackend, mem: &InMemoryLog) -> Result<(), TestCaseError> {
    prop_assert_eq!(dur.last_index(), mem.last_index(), "last_index mismatch");
    prop_assert_eq!(
        dur.commit_index(),
        mem.commit_index(),
        "commit_index mismatch"
    );
    // `Snapshot` derives `PartialEq`: compares commit index plus every committed
    // entry's index, term, and payload bytes.
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

/// The full retained state of a backend: bounds plus every retained entry
/// (read back in ascending index order). Under `Always` every appended entry is
/// durable, so this is exactly what a crash must preserve.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    last_index: Option<u64>,
    commit_index: Option<u64>,
    entries: Vec<LogEntry>,
}

/// Capture the full observable state of a backend.
fn observe(backend: &SimBackend) -> Observed {
    let last_index = backend.last_index();
    let entries = match last_index {
        Some(last) => backend.read(0, last),
        None => Vec::new(),
    };
    Observed {
        last_index,
        commit_index: backend.commit_index(),
        entries,
    }
}

/// Open (or reopen) a durable backend, failing the case (rather than panicking)
/// if the open errors when it must succeed.
fn open(handle: &SimStorageHandle, context: &str) -> Result<SimBackend, TestCaseError> {
    handle
        .open()
        .map_err(|e| TestCaseError::fail(format!("{context}: open failed: {e:?}")))
}

proptest! {
    // Comfortably above the project's 100-iteration minimum for property tests.
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Feature: deterministic-simulation-testing, Property 5: Storage model
    /// fidelity and durability boundary.
    #[test]
    fn storage_model_fidelity_and_durability_boundary(
        ops in prop::collection::vec(op_strategy(), 0..32),
        read_probes in prop::collection::vec((0u64..24, 0u64..24), 0..6),
        hs in hard_state_strategy(),
        // Number of uncommitted appends standing in front of the torn tail.
        torn_pre in 0u64..5,
        // Drives the seed-derived torn-tail byte count and the armed I/O kind.
        seed in any::<u64>(),
    ) {
        // ================================================================
        // Requirement 7.1 — fidelity: the durable backend matches a bare
        // InMemoryLog operation-for-operation, with no Storage_Fault.
        // ================================================================
        let fidelity = SimStorageHandle::new(&NodeId::new("node-fidelity"), &group("orders", 0));
        // Sim_Storage uses the consensus-safe Always policy; the handle's config
        // is the production durable WAL config, so fidelity is by construction.
        prop_assert_eq!(fidelity.config().sync_policy, SyncPolicy::Always);

        let mut dur = open(&fidelity, "fidelity")?;
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

        // ================================================================
        // Requirement 7.2 / 7.6 — crash durability boundary: under Always,
        // a crash + reopen preserves exactly the acknowledged state (every
        // record and the persisted hard state); nothing is lost or fabricated.
        // ================================================================
        dur.persist_hard_state(hs)
            .map_err(|e| TestCaseError::fail(format!("persist_hard_state failed: {e:?}")))?;
        let acknowledged = observe(&dur);
        let acknowledged_hs = dur.hard_state();

        // The live backend must be dropped before crash()/reopen so the
        // data-directory lock is released.
        drop(dur);
        fidelity.crash();
        let recovered = open(&fidelity, "post-crash reopen")?;

        prop_assert_eq!(
            observe(&recovered),
            acknowledged,
            "a crash under Always must preserve exactly the acknowledged records"
        );
        prop_assert_eq!(
            recovered.hard_state(),
            acknowledged_hs,
            "a crash under Always must preserve the persisted hard state"
        );

        // ================================================================
        // Requirement 7.3 — torn tail: a torn trailing write recovers by
        // discarding the torn tail down to the last intact record. With only
        // uncommitted appends in front of it, the reopen must succeed cleanly,
        // commit nothing, and never grow the log beyond what was written.
        // ================================================================
        let torn = SimStorageHandle::new(&NodeId::new("node-torn"), &group("orders", 1));
        {
            let mut backend = open(&torn, "torn setup")?;
            for i in 0..torn_pre {
                backend
                    .append(make_payload(0, 1, i as u8), 1)
                    .map_err(|e| TestCaseError::fail(format!("torn append failed: {e:?}")))?;
            }
        } // drop releases the lock so the reopen can re-acquire it

        // A seed-derived byte count in 1..=256 tears (at least part of) the
        // trailing write. `tear_last_write` truncates the most-recently-written
        // file by this many bytes; it is a no-op when nothing was written.
        let mut rng = SplitMix64::new(seed);
        let torn_bytes = rng.next_below(256) + 1;
        torn.arm_torn_tail(torn_bytes);

        let torn_recovered = torn
            .open()
            .map_err(|e| TestCaseError::fail(format!("a torn tail must recover, not error: {e:?}")))?;
        prop_assert_eq!(
            torn_recovered.commit_index(),
            None,
            "no record was committed, so a torn tail must leave nothing committed"
        );
        let recovered_count = torn_recovered.last_index().map_or(0, |last| last + 1);
        prop_assert!(
            recovered_count <= torn_pre,
            "a torn tail must discard the trailing write, never grow the log: \
             recovered {recovered_count} > written {torn_pre}"
        );

        // ================================================================
        // Requirement 7.4 — I/O error: an armed I/O-error fault surfaces
        // through the LogStorage Result (or fail-stop on reopen) at the next
        // matching operation, never as a silent success.
        // ================================================================
        let io = SimStorageHandle::new(&NodeId::new("node-io"), &group("orders", 2));
        match rng.next_below(3) {
            // A write/fsync fault surfaces on the next append (which, under
            // Always, forces the manifest write and fsync).
            0 => {
                let mut backend = open(&io, "io-write")?;
                io.arm_io_error(IoFaultKind::Write);
                let result = backend.append(make_payload(0, 1, 0), 1);
                prop_assert!(
                    matches!(result, Err(LogError::Io { .. })),
                    "an armed write fault must surface as LogError::Io, got {result:?}"
                );
            }
            1 => {
                let mut backend = open(&io, "io-fsync")?;
                io.arm_io_error(IoFaultKind::Fsync);
                let result = backend.append(make_payload(0, 1, 0), 1);
                prop_assert!(
                    matches!(result, Err(LogError::Io { .. })),
                    "an armed fsync fault must surface as LogError::Io, got {result:?}"
                );
            }
            // A read fault fails-stop on the recovery read path of the reopen.
            _ => {
                {
                    let mut backend = open(&io, "io-read setup")?;
                    backend
                        .append(make_payload(0, 1, 0), 1)
                        .map_err(|e| TestCaseError::fail(format!("io-read append: {e:?}")))?;
                    backend
                        .commit(0)
                        .map_err(|e| TestCaseError::fail(format!("io-read commit: {e:?}")))?;
                }
                io.arm_io_error(IoFaultKind::Read);
                let result = io.open();
                prop_assert!(
                    matches!(result, Err(LogError::Io { .. })),
                    "an armed read fault must fail-stop on reopen, got {:?}",
                    result.map(|_| "Ok(backend)")
                );
            }
        }
    }
}
