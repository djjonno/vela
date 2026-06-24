// Feature: batched-produce, Property 1: Batch payload round-trips through encode/decode
//!
//! Property 1: a batch's record values survive a full encode/decode cycle
//! intact. The batch payload codec is the pair
//! [`encode_record_batch`]/[`decode_record_batch`]: the encoder frames each
//! record's **value** bytes (keys are not persisted, matching the single-record
//! produce path) as a length-delimited concatenation — a 4-byte little-endian
//! `u32` length prefix followed by the value bytes — and the decoder recovers
//! the ordered value frames.
//!
//! For *any* ordered list of records,
//! `decode_record_batch(encode_record_batch(records))` yields exactly the
//! sequence of **value** bytes the records carried, in the same order — so a
//! batch appended as one `RecordBatch` entry reproduces its records' values
//! verbatim and in order. Because keys are not persisted, the round-trip equals
//! the input **value** sequence (`record.value`), not the keys.
//!
//! Validates: Requirements 1.2, 10.2

use proptest::prelude::*;

use vela_core::{decode_record_batch, encode_record_batch, Record};

/// An arbitrary record: an optional key (including `None` and empty) and a
/// value (including empty). Sizes are kept modest so the codec round-trip stays
/// fast across many iterations; the property holds for any value bytes,
/// regardless of length. Keys are generated to exercise the codec's invariant
/// that they are never persisted — the round-trip must equal values only.
fn arbitrary_record() -> impl Strategy<Value = Record> {
    (
        proptest::option::of(prop::collection::vec(any::<u8>(), 0..=64)),
        prop::collection::vec(any::<u8>(), 0..=128),
    )
        .prop_map(|(key, value)| Record::new(key, value))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: batched-produce, Property 1: Batch payload round-trips through
    // encode/decode.
    //
    // For any ordered list of records, decoding the encoded batch yields the
    // input VALUE sequence exactly and in order. Keys (including None and
    // empty) and empty values are all exercised by the generator; the result
    // depends only on the values, in order (Requirements 1.2, 10.2).
    #[test]
    fn decode_of_encode_equals_input_value_sequence(
        records in prop::collection::vec(arbitrary_record(), 0..=64),
    ) {
        let expected: Vec<Vec<u8>> = records.iter().map(|r| r.value.clone()).collect();
        let decoded = decode_record_batch(&encode_record_batch(&records));
        prop_assert_eq!(decoded, expected);
    }
}
