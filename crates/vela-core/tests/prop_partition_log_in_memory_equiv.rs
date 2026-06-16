//! Property test: an in-memory [`PartitionLog`] is observably identical to a
//! bare [`InMemoryLog`].
//!
//! Feature: per-topic-log-durability, Property 1
//!
//! Property 1: for any sequence of [`LogStorage`] operations, a
//! `PartitionLog::InMemory(InMemoryLog)` returns, for every operation, a result
//! equal to a bare `InMemoryLog`'s result and reports equal observable state
//! (`last_index`, `commit_index`, `term_at`, `entry`, `read`, `snapshot`).
//!
//! This is the domain-layer realization of Requirements 4.3, 4.4, and 13.1:
//! `PartitionLog` applies each operation to the backend it holds and returns
//! that backend's result unchanged (4.3); the in-memory variant therefore
//! matches `InMemoryLog` operation-for-operation (4.4); and so an in-memory
//! topic behaves exactly as one backed directly by `InMemoryLog` (13.1).
//!
//! The test generates a random sequence of operations — appends, batch appends
//! (contiguous and gapped), commits, reverts, and reads — and applies each
//! identically to both a `PartitionLog::InMemory(InMemoryLog::new())` and a
//! bare `InMemoryLog`, asserting equal per-operation results and equal
//! observable state after every step.
//!
//! Validates: Requirements 4.3, 4.4, 13.1

use proptest::prelude::*;
use vela_core::PartitionLog;
use vela_log::{EntryPayload, HardState, InMemoryLog, LogEntry, LogStorage, PayloadKind};

/// One `LogStorage` operation to apply to both logs.
#[derive(Debug, Clone)]
enum Op {
    /// `append(payload, term)`.
    Append {
        kind: PayloadKind,
        byte: u8,
        term: u64,
    },
    /// `append_entries(batch)` over a batch starting at `start` with `count`
    /// sequentially-indexed entries (sometimes contiguous, sometimes a gap).
    AppendEntries { start: u64, count: u8, term: u64 },
    /// `commit(index)`.
    Commit(u64),
    /// `revert(index)`.
    Revert(u64),
    /// `flush()`.
    Flush,
    /// `persist_hard_state(state)`.
    PersistHardState {
        current_term: u64,
        voted_for: Option<u64>,
    },
    /// `read(start, end)`.
    Read(u64, u64),
}

fn payload_kind() -> impl Strategy<Value = PayloadKind> {
    prop_oneof![
        Just(PayloadKind::Record),
        Just(PayloadKind::Cluster),
        Just(PayloadKind::Noop),
    ]
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Appends are weighted up so the log actually grows and the
        // commit/revert/read operations have material to act on.
        4 => (payload_kind(), any::<u8>(), 0u64..4).prop_map(|(kind, byte, term)| Op::Append {
            kind,
            byte,
            term,
        }),
        2 => (0u64..8, 0u8..4, 0u64..4).prop_map(|(start, count, term)| Op::AppendEntries {
            start,
            count,
            term,
        }),
        2 => (0u64..8).prop_map(Op::Commit),
        2 => (0u64..8).prop_map(Op::Revert),
        1 => Just(Op::Flush),
        1 => (0u64..4, proptest::option::of(0u64..4))
            .prop_map(|(current_term, voted_for)| Op::PersistHardState { current_term, voted_for }),
        2 => (0u64..8, 0u64..8).prop_map(|(s, e)| Op::Read(s, e)),
    ]
}

/// Build a `count`-long batch of sequentially-indexed entries beginning at
/// `start`, all carrying `term`. Used to exercise `append_entries` with both
/// contiguous batches and gaps.
fn batch(start: u64, count: u8, term: u64) -> Vec<LogEntry> {
    (0..count as u64)
        .map(|offset| LogEntry {
            index: start + offset,
            term,
            payload: EntryPayload::new(PayloadKind::Record, vec![offset as u8]),
        })
        .collect()
}

/// Apply one operation to both logs and assert the returned results match.
/// `LogError` is not `PartialEq`, so results are compared by their `Debug`
/// rendering, which is identical for identical errors.
fn apply_and_compare(
    op: &Op,
    part: &mut PartitionLog,
    base: &mut InMemoryLog,
) -> Result<(), TestCaseError> {
    match op {
        Op::Append { kind, byte, term } => {
            let p = part.append(EntryPayload::new(*kind, vec![*byte]), *term);
            let b = base.append(EntryPayload::new(*kind, vec![*byte]), *term);
            prop_assert_eq!(format!("{p:?}"), format!("{b:?}"));
        }
        Op::AppendEntries { start, count, term } => {
            let entries = batch(*start, *count, *term);
            let p = part.append_entries(&entries);
            let b = base.append_entries(&entries);
            prop_assert_eq!(format!("{p:?}"), format!("{b:?}"));
        }
        Op::Commit(index) => {
            let p = part.commit(*index);
            let b = base.commit(*index);
            prop_assert_eq!(format!("{p:?}"), format!("{b:?}"));
        }
        Op::Revert(index) => {
            let p = part.revert(*index);
            let b = base.revert(*index);
            prop_assert_eq!(format!("{p:?}"), format!("{b:?}"));
        }
        Op::Flush => {
            let p = part.flush();
            let b = base.flush();
            prop_assert_eq!(format!("{p:?}"), format!("{b:?}"));
        }
        Op::PersistHardState {
            current_term,
            voted_for,
        } => {
            let state = HardState {
                current_term: *current_term,
                voted_for: *voted_for,
            };
            let p = part.persist_hard_state(state);
            let b = base.persist_hard_state(state);
            prop_assert_eq!(format!("{p:?}"), format!("{b:?}"));
        }
        Op::Read(start, end) => {
            prop_assert_eq!(part.read(*start, *end), base.read(*start, *end));
        }
    }
    Ok(())
}

/// Assert every observable read accessor agrees between the two logs.
fn compare_observable_state(part: &PartitionLog, base: &InMemoryLog) -> Result<(), TestCaseError> {
    prop_assert_eq!(part.last_index(), base.last_index());
    prop_assert_eq!(part.commit_index(), base.commit_index());
    prop_assert_eq!(part.snapshot(), base.snapshot());
    prop_assert_eq!(part.hard_state(), base.hard_state());

    // A full read must agree, plus per-index `entry`/`term_at` across a band
    // that extends just past the highest stored index so absent positions
    // (which must both return `None`) are covered too.
    prop_assert_eq!(part.read(0, u64::MAX), base.read(0, u64::MAX));
    let probe_hi = base.last_index().map_or(2, |last| last + 2);
    for index in 0..=probe_hi {
        prop_assert_eq!(part.entry(index), base.entry(index));
        prop_assert_eq!(part.term_at(index), base.term_at(index));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: per-topic-log-durability, Property 1
    #[test]
    fn in_memory_partition_log_matches_bare_in_memory_log(
        ops in prop::collection::vec(op_strategy(), 0..200),
    ) {
        let mut part = PartitionLog::InMemory(InMemoryLog::new());
        let mut base = InMemoryLog::new();

        // Equal before any operation.
        compare_observable_state(&part, &base)?;

        for op in &ops {
            apply_and_compare(op, &mut part, &mut base)?;
            compare_observable_state(&part, &base)?;
        }
    }
}
