//! Data-integrity verification: count and per-position payload checks.
//!
//! After the Consumer_Phase completes, the benchmark must confirm that the
//! records read back exactly reconstruct the produced Workload, so a reported
//! throughput reflects real work rather than a silently broken path
//! (Requirement 5.1, 5.2, 5.5). [`verify_consumed`] is the pure check that
//! performs that confirmation over the consumed record values.
//!
//! Two checks are made:
//!
//! 1. **Count** — the number of records read must equal the number of
//!    Acknowledged_Records from the Producer_Phase. A read count that differs
//!    in either direction (under-read or over-read) is a [`CountMismatch`],
//!    retaining both recorded counts (Requirement 5.1, 5.5).
//! 2. **Per-position payload** — each read record's value must equal the
//!    deterministic payload generated for its position (Requirement 5.2). How a
//!    record is mapped back to a position depends on `value_size`:
//!    - When `value_size >= 8` the position is recoverable from the payload
//!      prefix ([`crate::workload::position_of`]), so each consumed record is
//!      mapped directly to its expected payload and compared byte-for-byte,
//!      regardless of the partition/offset order it arrived in. A single
//!      corrupted byte is detected as a [`PayloadMismatch`] reporting the
//!      affected position.
//!    - When `value_size < 8` the position cannot be embedded, so verification
//!      falls back to a multiset comparison: the bag of consumed payloads must
//!      equal the bag of expected payloads for positions `[0, expected_count)`.
//!      A discrepancy in payload content is reported as a [`PayloadMismatch`]
//!      with a best-effort recovered position.
//!
//! The error type is local to this module on purpose: it is a small, pure value
//! that `run.rs` maps to the report's `FailureReason::IntegrityCountMismatch` /
//! `FailureReason::IntegrityPayloadMismatch`, keeping `verify` free of any
//! dependency on the outcome model.

use std::collections::HashMap;

use thiserror::Error;

use crate::workload::{payload_for, position_of};

/// The number of leading payload bytes that embed the record position.
const POSITION_PREFIX_LEN: usize = core::mem::size_of::<u64>();

/// A data-integrity violation found while verifying the consumed Workload.
///
/// A pure value produced by [`verify_consumed`]. `run.rs` maps it to the
/// Benchmark_Report's typed failure reason
/// (`FailureReason::IntegrityCountMismatch` / `IntegrityPayloadMismatch`); both
/// variants retain the information needed to surface the violation in the
/// report, the stdout summary, and the HTML_Report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum VerificationError {
    /// The number of records read did not equal the Acknowledged_Record count.
    ///
    /// Covers both under-read (`read < expected`) and over-read
    /// (`read > expected`); both recorded counts are retained
    /// (Requirement 5.1, 5.5).
    #[error("records read ({read}) does not equal acknowledged records ({expected})")]
    CountMismatch {
        /// The number of records actually read during the Consumer_Phase.
        read: u64,
        /// The number of Acknowledged_Records expected from the Producer_Phase.
        expected: u64,
    },

    /// A read record's payload did not equal the payload expected for its
    /// position (Requirement 5.2, 5.5).
    #[error("payload mismatch at record position {position}")]
    PayloadMismatch {
        /// The 0-based position of the affected record.
        position: u64,
    },
}

/// Verify that `consumed` exactly reconstructs the produced Workload.
///
/// `expected_count` is the number of Acknowledged_Records produced,
/// `value_size` is the configured record value size, and `consumed` yields the
/// value bytes of every record read during the Consumer_Phase (in any order).
///
/// Returns `Ok(())` when the read count equals `expected_count` and every
/// payload matches its expected value; otherwise returns the
/// [`VerificationError`] describing the first violation found.
///
/// The check adapts to `value_size`:
///
/// - `value_size >= 8`: each record's payload carries its position in the
///   leading 8 bytes, so the position is recovered and the payload compared
///   directly. The count is checked after all records are inspected.
/// - `value_size < 8` (including `0`): the position cannot be embedded, so a
///   multiset comparison is used — the bag of consumed payloads must equal the
///   bag of `payload_for(p, value_size)` for `p in [0, expected_count)`.
///
/// This function is pure: it reads only its arguments and performs no I/O.
pub fn verify_consumed<I, V>(
    expected_count: u64,
    value_size: usize,
    consumed: I,
) -> Result<(), VerificationError>
where
    I: IntoIterator<Item = V>,
    V: AsRef<[u8]>,
{
    if value_size >= POSITION_PREFIX_LEN {
        verify_by_embedded_position(expected_count, value_size, consumed)
    } else {
        verify_by_multiset(expected_count, value_size, consumed)
    }
}

/// `value_size >= 8` path: recover each record's embedded position and compare
/// its payload directly, then confirm the total count.
fn verify_by_embedded_position<I, V>(
    expected_count: u64,
    value_size: usize,
    consumed: I,
) -> Result<(), VerificationError>
where
    I: IntoIterator<Item = V>,
    V: AsRef<[u8]>,
{
    let mut read: u64 = 0;
    for value in consumed {
        let value = value.as_ref();
        read += 1;
        match position_of(value, value_size) {
            // A record carrying a position outside the produced range was never
            // produced for this Workload: report it as a payload mismatch at the
            // offending position.
            Some(position) if position >= expected_count => {
                return Err(VerificationError::PayloadMismatch { position });
            }
            Some(position) => {
                if payload_for(position, value_size).as_slice() != value {
                    return Err(VerificationError::PayloadMismatch { position });
                }
            }
            // value_size >= 8 yet the position could not be recovered: the value
            // is malformed (e.g. truncated). Report it at its ordinal index.
            None => {
                return Err(VerificationError::PayloadMismatch { position: read - 1 });
            }
        }
    }

    if read != expected_count {
        return Err(VerificationError::CountMismatch {
            read,
            expected: expected_count,
        });
    }

    Ok(())
}

/// `value_size < 8` path: compare the multiset of consumed payloads against the
/// multiset of expected payloads for positions `[0, expected_count)`.
fn verify_by_multiset<I, V>(
    expected_count: u64,
    value_size: usize,
    consumed: I,
) -> Result<(), VerificationError>
where
    I: IntoIterator<Item = V>,
    V: AsRef<[u8]>,
{
    let mut actual: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut read: u64 = 0;
    for value in consumed {
        read += 1;
        *actual.entry(value.as_ref().to_vec()).or_insert(0) += 1;
    }

    if read != expected_count {
        return Err(VerificationError::CountMismatch {
            read,
            expected: expected_count,
        });
    }

    let mut expected: HashMap<Vec<u8>, u64> = HashMap::new();
    for position in 0..expected_count {
        *expected
            .entry(payload_for(position, value_size))
            .or_insert(0) += 1;
    }

    if actual == expected {
        return Ok(());
    }

    // The bags differ despite equal totals: locate an offending payload and
    // report a best-effort position recovered from its (truncated) prefix.
    for (payload, &actual_count) in &actual {
        if expected.get(payload).copied().unwrap_or(0) != actual_count {
            return Err(VerificationError::PayloadMismatch {
                position: recover_truncated_position(payload),
            });
        }
    }
    for (payload, &expected_n) in &expected {
        if actual.get(payload).copied().unwrap_or(0) != expected_n {
            return Err(VerificationError::PayloadMismatch {
                position: recover_truncated_position(payload),
            });
        }
    }

    // Unreachable: unequal maps with equal totals must differ on some payload.
    Ok(())
}

/// Best-effort position recovery for the `value_size < 8` path.
///
/// `payload_for` embeds the low `value_size` bytes of the position, so decoding
/// the leading bytes as a zero-extended little-endian `u64` recovers the
/// position modulo `256^value_size` — enough to point an operator at the
/// affected record. An empty payload (`value_size == 0`) recovers `0`.
fn recover_truncated_position(payload: &[u8]) -> u64 {
    let mut bytes = [0u8; POSITION_PREFIX_LEN];
    let take = payload.len().min(POSITION_PREFIX_LEN);
    bytes[..take].copy_from_slice(&payload[..take]);
    u64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a correct, in-order consumed set for `count` records of `size`.
    fn workload(count: u64, size: usize) -> Vec<Vec<u8>> {
        (0..count).map(|p| payload_for(p, size)).collect()
    }

    // ---- value_size >= 8 (embedded-position) path -----------------------

    #[test]
    fn correct_workload_verifies() {
        assert_eq!(verify_consumed(1000, 16, workload(1000, 16).iter()), Ok(()));
    }

    #[test]
    fn correct_workload_verifies_in_any_order() {
        // Order independence: the verifier maps each record by its embedded
        // position, so a reversed delivery order still passes.
        let mut records = workload(500, 32);
        records.reverse();
        assert_eq!(verify_consumed(500, 32, records.iter()), Ok(()));
    }

    #[test]
    fn corrupted_byte_fails_reporting_its_position() {
        let mut records = workload(100, 16);
        // Corrupt a fill byte (offset >= 8) of position 42 so the embedded
        // position still recovers as 42 but the payload no longer matches.
        records[42][12] ^= 0xFF;
        assert_eq!(
            verify_consumed(100, 16, records.iter()),
            Err(VerificationError::PayloadMismatch { position: 42 })
        );
    }

    #[test]
    fn extra_record_fails_with_count_mismatch() {
        let mut records = workload(50, 16);
        // A duplicate of an in-range record keeps every payload valid, so the
        // sole violation is the inflated count (over-read).
        records.push(payload_for(0, 16));
        assert_eq!(
            verify_consumed(50, 16, records.iter()),
            Err(VerificationError::CountMismatch {
                read: 51,
                expected: 50,
            })
        );
    }

    #[test]
    fn missing_record_fails_with_count_mismatch() {
        let mut records = workload(50, 16);
        records.pop();
        assert_eq!(
            verify_consumed(50, 16, records.iter()),
            Err(VerificationError::CountMismatch {
                read: 49,
                expected: 50,
            })
        );
    }

    #[test]
    fn out_of_range_position_fails_as_payload_mismatch() {
        // A record carrying a position beyond the produced range was never
        // produced for this Workload.
        let records = [payload_for(0, 16), payload_for(7, 16)];
        assert_eq!(
            verify_consumed(1, 16, records.iter()),
            Err(VerificationError::PayloadMismatch { position: 7 })
        );
    }

    #[test]
    fn truncated_record_fails_as_payload_mismatch() {
        let records = [vec![1u8, 2, 3]]; // shorter than 8 bytes
        assert_eq!(
            verify_consumed(1, 16, records.iter()),
            Err(VerificationError::PayloadMismatch { position: 0 })
        );
    }

    // ---- value_size < 8 (multiset) path ---------------------------------

    #[test]
    fn small_payload_workload_verifies_in_any_order() {
        let mut records = workload(300, 4);
        records.reverse();
        assert_eq!(verify_consumed(300, 4, records.iter()), Ok(()));
    }

    #[test]
    fn small_payload_corruption_fails_as_payload_mismatch() {
        let mut records = workload(10, 4);
        // Replace position 3's payload with one no position in [0,10) produces.
        records[3] = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let err = verify_consumed(10, 4, records.iter()).unwrap_err();
        assert!(matches!(err, VerificationError::PayloadMismatch { .. }));
    }

    #[test]
    fn small_payload_extra_record_fails_with_count_mismatch() {
        let mut records = workload(10, 4);
        records.push(payload_for(0, 4));
        assert_eq!(
            verify_consumed(10, 4, records.iter()),
            Err(VerificationError::CountMismatch {
                read: 11,
                expected: 10,
            })
        );
    }

    // ---- value_size == 0 (empty payload) --------------------------------

    #[test]
    fn empty_payload_workload_verifies_on_count() {
        let records = workload(25, 0);
        assert_eq!(verify_consumed(25, 0, records.iter()), Ok(()));
    }

    #[test]
    fn empty_payload_count_mismatch_is_detected() {
        let records = workload(25, 0);
        assert_eq!(
            verify_consumed(24, 0, records.iter()),
            Err(VerificationError::CountMismatch {
                read: 25,
                expected: 24,
            })
        );
    }

    // ---- empty expectation ----------------------------------------------

    #[test]
    fn zero_expected_with_no_records_verifies() {
        let empty: Vec<Vec<u8>> = Vec::new();
        assert_eq!(verify_consumed(0, 16, empty.iter()), Ok(()));
    }
}
