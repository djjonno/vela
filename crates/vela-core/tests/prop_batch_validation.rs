// Feature: batched-produce, Property 3: Batch validation accepts in-bounds batches and rejects out-of-bounds with the correct reason
//!
//! Property 3: [`validate_batch`] is a pure, total range check over a candidate
//! batch. It returns `Ok(())` **if and only if** the batch is non-empty, every
//! record's combined key+value size is at most the per-record limit
//! ([`MAX_RECORD_BYTES`], 1 MiB), the record count is at most
//! [`MAX_BATCH_RECORDS`], and the batch's total encoded size is at most
//! [`MAX_BATCH_BYTES`]. Otherwise it returns the specific rejection reason —
//! [`BatchRejection::Empty`], [`BatchRejection::RecordTooLarge`] naming the
//! 0-based offending record and its submitted combined size,
//! [`BatchRejection::TooManyRecords`] reporting the `max` and `submitted`
//! count, or [`BatchRejection::TooLarge`] reporting the `max` and `submitted`
//! encoded bytes — and, being a pure check, appends nothing and leaves all
//! state unchanged (Requirements 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6).
//!
//! ## Encoded-size model
//!
//! A batch is carried as one `RecordBatch` entry whose bytes are a
//! length-delimited concatenation of the records' **value** bytes: per record a
//! fixed 4-byte `u32` length prefix plus the value bytes (keys are not
//! persisted). The [`MAX_BATCH_BYTES`] check is therefore against
//! `sum(4 + value.len())`, which [`encoded_size`] mirrors so the test's
//! expectations match the production framing exactly.
//!
//! ## Validation precedence
//!
//! Checks run in a fixed order so a batch failing several limits reports a
//! single predictable reason: `Empty` -> `RecordTooLarge` (first offending
//! record) -> `TooManyRecords` -> `TooLarge`. The targeted boundary tests below
//! each isolate one limit so the reported variant is deterministic, and the
//! broad small-batch test compares against an independent [`model`] of the same
//! precedence.
//!
//! ## Cost control
//!
//! [`MAX_BATCH_BYTES`] is 16 MiB and [`MAX_BATCH_RECORDS`] is 10,000, so the
//! boundary cases are constructed directly with exactly-sized records rather
//! than drawn from a maximal random space: the per-record boundary straddles
//! 1 MiB on a single record amid small ones, the count boundary uses
//! empty-value records, and the byte boundary uses a fixed run of 1 MiB records
//! plus one tuned tail record. The cheap in-range space (small batches) is
//! swept broadly against the model.
//!
//! Validates: Requirements 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6

use proptest::prelude::*;

use vela_core::{
    validate_batch, BatchRejection, Record, MAX_BATCH_BYTES, MAX_BATCH_RECORDS, MAX_RECORD_BYTES,
};

/// The fixed-width `u32` length prefix that frames each record's value in an
/// encoded batch payload — the single source of truth the production codec and
/// [`validate_batch`] share. Mirrored here so the test's encoded-size
/// expectations match the bytes the entry will occupy.
const FRAME_HEADER_BYTES: usize = 4;

/// The encoded size, in bytes, of `records` under the length-delimited framing
/// the `MAX_BATCH_BYTES` limit is checked against: per record a
/// [`FRAME_HEADER_BYTES`] prefix plus its value bytes (keys unpersisted). Uses
/// saturating arithmetic to mirror the production check, which cannot overflow
/// a `usize` for any representable batch.
fn encoded_size(records: &[Record]) -> usize {
    records.iter().fold(0usize, |acc, record| {
        acc.saturating_add(FRAME_HEADER_BYTES)
            .saturating_add(record.value.len())
    })
}

/// An independent model of [`validate_batch`]'s biconditional and precedence,
/// derived directly from the acceptance criteria (Requirements 3.1–3.5). The
/// broad small-batch test asserts `validate_batch` agrees with this model.
fn model(records: &[Record]) -> Result<(), BatchRejection> {
    if records.is_empty() {
        return Err(BatchRejection::Empty);
    }
    for (index, record) in records.iter().enumerate() {
        let size = record.key.as_ref().map_or(0, |k| k.len()) + record.value.len();
        if size > MAX_RECORD_BYTES {
            return Err(BatchRejection::RecordTooLarge { index, size });
        }
    }
    let submitted = records.len();
    if submitted > MAX_BATCH_RECORDS {
        return Err(BatchRejection::TooManyRecords {
            max: MAX_BATCH_RECORDS,
            submitted,
        });
    }
    let encoded = encoded_size(records);
    if encoded > MAX_BATCH_BYTES {
        return Err(BatchRejection::TooLarge {
            max: MAX_BATCH_BYTES,
            submitted: encoded,
        });
    }
    Ok(())
}

/// A small record: an optional short key and a short value, both well within
/// every limit. Used to fill batches cheaply around the interesting boundary.
fn small_record() -> impl Strategy<Value = Record> {
    (
        proptest::option::of(prop::collection::vec(any::<u8>(), 0..=32)),
        prop::collection::vec(any::<u8>(), 0..=32),
    )
        .prop_map(|(key, value)| Record::new(key.filter(|k| !k.is_empty()), value))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: batched-produce, Property 3: Batch validation accepts in-bounds
    // batches and rejects out-of-bounds with the correct reason.
    //
    // The cheap in-range space: small batches (including the empty batch) whose
    // counts and encoded sizes are far below the count/byte ceilings and whose
    // records are far below the per-record ceiling. `validate_batch` must agree
    // with the independent model on every such batch — covering `Empty` and the
    // accepting `Ok(())` case — and must leave its borrowed input unchanged.
    #[test]
    fn matches_model_on_small_batches_with_no_side_effects(
        records in prop::collection::vec(small_record(), 0..=64),
    ) {
        let before = records.clone();
        prop_assert_eq!(validate_batch(&records), model(&records));
        // `validate_batch` borrows `&[Record]`; an unchanged input shows the
        // check is side-effect-free (Requirement 3.6).
        prop_assert_eq!(&records, &before);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // The per-record boundary (Requirement 3.2): one record straddles the 1 MiB
    // combined key+value limit at a known position `idx` amid in-bounds records,
    // so the first offending record is exactly `idx`. Over the limit yields
    // `RecordTooLarge { index: idx, size }` with the exact combined size; at or
    // under the limit yields no size rejection (here, `Ok(())`).
    #[test]
    fn per_record_limit_reports_first_offender_with_index_and_size(
        prefix_len in 0usize..=4,
        suffix_len in 0usize..=4,
        delta in -64i64..=64i64,
        key_frac in 0u32..=1000,
    ) {
        // Combined key+value size straddling 1 MiB for the candidate record.
        let total = (MAX_RECORD_BYTES as i64 + delta) as usize;
        let key_len = (total as u64 * key_frac as u64 / 1000) as usize;
        let value_len = total - key_len;
        let key = if key_len == 0 {
            None
        } else {
            Some(vec![1u8; key_len])
        };
        let candidate = Record::new(key, vec![2u8; value_len]);

        // Surround the candidate with tiny, in-bounds records. The candidate
        // sits at index `prefix_len`, and every record before it is in-bounds,
        // so it is the first offender if it is oversized.
        let mut records: Vec<Record> = (0..prefix_len)
            .map(|_| Record::new(None, b"ok".to_vec()))
            .collect();
        records.push(candidate);
        records.extend((0..suffix_len).map(|_| Record::new(None, b"ok".to_vec())));

        let result = validate_batch(&records);

        if total > MAX_RECORD_BYTES {
            prop_assert_eq!(
                result,
                Err(BatchRejection::RecordTooLarge {
                    index: prefix_len,
                    size: total,
                }),
                "an oversized record must be rejected by its 0-based index and combined size"
            );
        } else {
            // At or under the per-record limit, with a small count and small
            // encoded size, the batch is accepted.
            prop_assert_eq!(
                result,
                Ok(()),
                "a batch whose records, count, and bytes are all in bounds is accepted"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // The record-count boundary (Requirement 3.3): counts straddle
    // `MAX_BATCH_RECORDS` using empty-value records, so no record is oversized
    // and the encoded size (4 bytes per record) stays far under the byte limit.
    // Over the count limit yields `TooManyRecords { max, submitted }`; at or
    // under it yields `Ok(())`.
    #[test]
    fn count_limit_reports_max_and_submitted(
        offset in -8i64..=8i64,
    ) {
        let count = (MAX_BATCH_RECORDS as i64 + offset) as usize;
        let records: Vec<Record> = (0..count).map(|_| Record::new(None, Vec::new())).collect();

        // Sanity: empty values keep this batch's encoded size trivially under
        // the byte ceiling, so the count check is the only one that can fire.
        prop_assert!(encoded_size(&records) <= MAX_BATCH_BYTES);

        let result = validate_batch(&records);

        if count > MAX_BATCH_RECORDS {
            prop_assert_eq!(
                result,
                Err(BatchRejection::TooManyRecords {
                    max: MAX_BATCH_RECORDS,
                    submitted: count,
                }),
                "a batch over the record-count limit reports max and submitted"
            );
        } else {
            prop_assert_eq!(
                result,
                Ok(()),
                "a batch at or under the record-count limit is accepted"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // The total-bytes boundary (Requirement 3.4): a fixed run of 1 MiB records
    // plus one tuned tail record makes the encoded size straddle
    // `MAX_BATCH_BYTES` while every record stays within the per-record limit and
    // the count (16) stays well under the count limit — so the byte check is the
    // only one that can fire. Over the byte limit yields
    // `TooLarge { max, submitted }` with the exact encoded size.
    #[test]
    fn total_bytes_limit_reports_max_and_submitted(
        delta in -64i64..=64i64,
    ) {
        // 15 full 1 MiB records contribute 15 * (4 + 1 MiB) encoded bytes; the
        // tail's length is tuned so the grand total is MAX_BATCH_BYTES + delta.
        const FULL: usize = 15;
        let full_encoded = FULL * (FRAME_HEADER_BYTES + MAX_RECORD_BYTES);
        // tail_encoded = (MAX_BATCH_BYTES + delta) - full_encoded
        // tail_len = tail_encoded - FRAME_HEADER_BYTES
        let tail_len = ((MAX_BATCH_BYTES as i64 + delta)
            - full_encoded as i64
            - FRAME_HEADER_BYTES as i64) as usize;
        // The tail stays within the per-record limit, so it never trips the
        // per-record check — only the aggregate byte check can fire.
        prop_assert!(tail_len <= MAX_RECORD_BYTES);

        let mut records: Vec<Record> = (0..FULL)
            .map(|_| Record::new(None, vec![0u8; MAX_RECORD_BYTES]))
            .collect();
        records.push(Record::new(None, vec![0u8; tail_len]));

        let encoded = encoded_size(&records);
        // By construction the encoded size is exactly MAX_BATCH_BYTES + delta.
        prop_assert_eq!(encoded, (MAX_BATCH_BYTES as i64 + delta) as usize);
        prop_assert!(records.len() <= MAX_BATCH_RECORDS);

        let result = validate_batch(&records);

        if encoded > MAX_BATCH_BYTES {
            prop_assert_eq!(
                result,
                Err(BatchRejection::TooLarge {
                    max: MAX_BATCH_BYTES,
                    submitted: encoded,
                }),
                "a batch over the byte limit reports max and the exact submitted encoded size"
            );
        } else {
            prop_assert_eq!(
                result,
                Ok(()),
                "a batch at or under the byte limit (records and count in bounds) is accepted"
            );
        }
    }

    // The empty batch is rejected as `Empty` before any other check
    // (Requirement 2.2, 3.5). A single deterministic case, kept in the proptest
    // block for cohesion.
    #[test]
    fn empty_batch_is_rejected_as_empty(_unit in Just(())) {
        prop_assert_eq!(validate_batch(&[]), Err(BatchRejection::Empty));
    }
}
