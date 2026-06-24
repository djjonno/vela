// Feature: batched-produce, Property 2: A committed batch assigns a contiguous offset range from the captured base
//!
//! Property 2: applying a committed entry to a partition's [`StateMachine`]
//! assigns dense, gap-free, 0-based offsets in commit order, and a batch entry
//! takes a *contiguous* run captured from the offset the partition held before
//! the apply.
//!
//! Concretely, with the state machine's record count `base = next_offset()`
//! captured immediately before an apply:
//!
//! - a single `Record` entry returns [`AppliedOffsets::One`]`(base)` and
//!   advances `next_offset()` by exactly 1;
//! - a `RecordBatch` entry of `N >= 1` returns
//!   [`AppliedOffsets::Range`]` { base, count: N }`, occupies the offsets
//!   `base, base+1, ..., base+N-1`, and advances `next_offset()` by exactly N.
//!
//! Consequently, for *any* interleaving of single-record and batch entries on
//! one partition: each committed batch occupies a contiguous offset run with no
//! other record's offset falling inside that run, the records read back in
//! ascending offset order increase by exactly 1 with no gaps, and a one-record
//! batch lands on the very offset a single produce would (Requirement 4.5).
//!
//! ## Model
//!
//! The test drives a real [`StateMachine`] with a generated sequence of
//! operations and mirrors the expected dense offset stream with a running
//! counter plus an `owner` vector that tags each committed offset with the
//! 0-based index of the operation that produced it. After replaying the
//! sequence it asserts: every returned [`AppliedOffsets`] matches the captured
//! base/count; `next_offset()` advanced by exactly the operation's record count
//! at each step; the full `read` is gap-free, ascending, and value-for-value
//! equal to the model; and for every batch the `owner` run over its
//! `[base, base+count)` range is exactly that batch (so no foreign record sits
//! inside it) and the offsets bracketing the run belong to other operations.
//!
//! Validates: Requirements 1.3, 2.1, 2.3, 2.4, 2.5, 4.5

use proptest::prelude::*;

use vela_core::{encode_record_batch, AppliedOffsets, Record, StateMachine};
use vela_log::{EntryPayload, LogEntry, PayloadKind};

/// One produced operation on a single partition: either a single record
/// (carrying its value bytes) or a batch of `N >= 1` records (carrying their
/// value bytes in order). Values are kept short so the offset bookkeeping —
/// not the byte volume — is what the many iterations exercise.
#[derive(Debug, Clone)]
enum Op {
    Single(Vec<u8>),
    Batch(Vec<Vec<u8>>),
}

/// A `Record` log entry carrying `value` as its payload bytes, matching the
/// single-record produce path the state machine assigns one offset to.
fn single_entry(value: &[u8]) -> LogEntry {
    LogEntry {
        index: 0,
        term: 1,
        payload: EntryPayload::new(PayloadKind::Record, value.to_vec()),
    }
}

/// A `RecordBatch` log entry carrying `values` as the length-delimited
/// concatenation the state machine decodes on apply (keys are unpersisted, so
/// records are built with `None` keys, matching the production batch path).
fn batch_entry(values: &[Vec<u8>]) -> LogEntry {
    let records: Vec<Record> = values
        .iter()
        .map(|v| Record::new(None, v.clone()))
        .collect();
    LogEntry {
        index: 0,
        term: 1,
        payload: EntryPayload::new(PayloadKind::RecordBatch, encode_record_batch(&records)),
    }
}

/// An arbitrary operation: a single record with a short value, or a batch of
/// `1..=8` short-valued records. Batch `N` is kept small so a generated
/// sequence stays cheap while still covering the `N == 1` boundary (where a
/// batch must behave like a single produce) and multi-record runs.
fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        prop::collection::vec(any::<u8>(), 0..=8).prop_map(Op::Single),
        prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=8), 1..=8).prop_map(Op::Batch),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: batched-produce, Property 2: A committed batch assigns a
    // contiguous offset range from the captured base.
    //
    // For any interleaving of single-record and batch operations applied in
    // commit order to one partition, each apply assigns the next dense offsets
    // from the captured `base = next_offset()`: a single takes `One(base)` and
    // advances by 1; a batch of N takes `Range { base, count: N }` and advances
    // by exactly N. The records read back increase by exactly 1, gap-free and
    // ascending, and each batch occupies a contiguous run no other record's
    // offset falls inside (Requirements 1.3, 2.1, 2.4, 2.5, 4.5).
    #[test]
    fn interleaved_singles_and_batches_assign_contiguous_dense_offsets(
        ops in prop::collection::vec(op_strategy(), 0..=24),
    ) {
        let mut sm = StateMachine::new();
        // The model: the expected dense value stream, and `owner[offset]` = the
        // 0-based index of the operation that produced the record at `offset`.
        let mut expected_values: Vec<Vec<u8>> = Vec::new();
        let mut owner: Vec<usize> = Vec::new();
        // Each generated batch's captured base and record count, tagged with its
        // operation index, checked for contiguity after the full replay.
        let mut batches: Vec<(u64, u32, usize)> = Vec::new();

        for (op_index, op) in ops.iter().enumerate() {
            // The base is the partition's record count captured BEFORE the apply
            // (Requirement 2.4). `next_offset()` and `len()` must agree.
            let base = sm.next_offset();
            prop_assert_eq!(base, sm.len() as u64);

            match op {
                Op::Single(value) => {
                    let applied = sm.apply(&single_entry(value));
                    // A single record takes exactly the captured base offset.
                    prop_assert_eq!(applied, Some(AppliedOffsets::One(base)));
                    // next_offset advances by exactly 1 (Requirement 4.5).
                    prop_assert_eq!(sm.next_offset(), base + 1);

                    expected_values.push(value.clone());
                    owner.push(op_index);
                }
                Op::Batch(values) => {
                    let n = values.len() as u32;
                    let applied = sm.apply(&batch_entry(values));
                    // A batch takes the contiguous range base..base+N from the
                    // captured base (Requirement 1.3, 2.1, 2.4).
                    prop_assert_eq!(
                        applied,
                        Some(AppliedOffsets::Range { base, count: n })
                    );
                    // next_offset advances by exactly N (Requirement 2.4, 4.5).
                    prop_assert_eq!(sm.next_offset(), base + n as u64);

                    for value in values {
                        expected_values.push(value.clone());
                        owner.push(op_index);
                    }
                    batches.push((base, n, op_index));
                }
            }
        }

        // The committed records read back as one gap-free, ascending sequence:
        // offset i holds the i-th produced value, offsets increase by exactly 1
        // in commit order regardless of single vs. batch origin (Requirement
        // 2.5, 4.5).
        let read = sm.read(0, expected_values.len() + 8);
        prop_assert_eq!(read.len(), expected_values.len());
        for (i, rec) in read.iter().enumerate() {
            prop_assert_eq!(rec.offset, i as u64);
            prop_assert_eq!(&rec.value, &expected_values[i]);
        }

        // Every committed batch occupies a contiguous run with no other record's
        // offset inside it: each offset in [base, base+count) is owned by exactly
        // that batch, and the offsets bracketing the run (if present) belong to a
        // different operation (Requirement 2.1, 2.5).
        for (base, count, op_index) in batches {
            for off in base..base + count as u64 {
                prop_assert_eq!(owner[off as usize], op_index);
            }
            if base > 0 {
                prop_assert_ne!(owner[(base - 1) as usize], op_index);
            }
            let after = (base + count as u64) as usize;
            if after < owner.len() {
                prop_assert_ne!(owner[after], op_index);
            }
        }
    }

    // A focused restatement of the core claim: from any prior length `base`
    // (established by `base` single produces), a single batch of N >= 1 takes
    // exactly `Range { base, count: N }`, occupies base..base+N contiguously,
    // and advances next_offset by exactly N (Requirement 1.3, 2.4).
    #[test]
    fn batch_takes_contiguous_range_from_captured_prior_length(
        base in 0usize..=64,
        values in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=8), 1..=8),
    ) {
        let mut sm = StateMachine::new();
        for i in 0..base {
            sm.apply(&single_entry(&[i as u8]));
        }
        prop_assert_eq!(sm.next_offset(), base as u64);

        let n = values.len() as u32;
        let applied = sm.apply(&batch_entry(&values));
        prop_assert_eq!(applied, Some(AppliedOffsets::Range { base: base as u64, count: n }));
        prop_assert_eq!(sm.next_offset(), base as u64 + n as u64);

        // The N records occupy base..base+N contiguously, value-for-value.
        let read = sm.read(base as u64, values.len() + 8);
        prop_assert_eq!(read.len(), values.len());
        for (i, rec) in read.iter().enumerate() {
            prop_assert_eq!(rec.offset, base as u64 + i as u64);
            prop_assert_eq!(&rec.value, &values[i]);
        }
    }
}
