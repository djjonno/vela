// Feature: throughput-benchmark, Property 3: Payload generation is deterministic, verifiable, and tamper-evident
//
// Property 3: Payload generation is deterministic, verifiable, and
// tamper-evident.
//
// `payload_for` is referentially transparent and returns a vector of length
// exactly `value_size`. When `value_size >= 8` the embedded position round-trips
// through `position_of` (`position_of(payload_for(p, s), s) == Some(p)`). A
// correctly-read Workload verifies; a single corrupted fill byte at position `p`
// is detected as a `PayloadMismatch { position: p }`, and an extra in-range
// record is detected as a `CountMismatch { read, expected }`.
//
// These properties exercise the pure workload/verification surface directly:
// `vela_bench::workload::{payload_for, position_of}` and
// `vela_bench::verify::{verify_consumed, VerificationError}`.
//
// Validates: Requirements 5.1, 5.2, 5.5, 5.6

use proptest::prelude::*;

use vela_bench::verify::{verify_consumed, VerificationError};
use vela_bench::workload::{payload_for, position_of};

/// The minimum `value_size` that can embed a full little-endian `u64` position,
/// matching `core::mem::size_of::<u64>()` used by the code under test.
const POSITION_PREFIX_LEN: usize = core::mem::size_of::<u64>();

/// Upper bound on generated `value_size`. Bounded so each generated payload is a
/// small allocation and the suite stays fast while still spanning the `s < 8`,
/// `s == 8`, and `s > 8` regimes.
const MAX_VALUE_SIZE: usize = 4096;

/// Build a correct, in-order consumed Workload of `count` records at `size`,
/// exactly as the Producer_Phase would have generated them.
fn workload(count: u64, size: usize) -> Vec<Vec<u8>> {
    (0..count).map(|p| payload_for(p, size)).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: throughput-benchmark, Property 3
    //
    // Determinism + exact length: `payload_for` depends only on its arguments,
    // so two calls with the same `(position, value_size)` are byte-identical,
    // and the result is always exactly `value_size` bytes (Requirement 5.6).
    #[test]
    fn payload_is_deterministic_and_exact_length(
        position in any::<u64>(),
        value_size in 0usize..=MAX_VALUE_SIZE,
    ) {
        let first = payload_for(position, value_size);
        let second = payload_for(position, value_size);

        prop_assert_eq!(&first, &second, "payload_for must be referentially transparent");
        prop_assert_eq!(first.len(), value_size, "payload length must equal value_size");
    }

    // Feature: throughput-benchmark, Property 3
    //
    // Round-trip: when `value_size >= 8` the embedded position is recoverable,
    // so `position_of(payload_for(p, s), s) == Some(p)` for every position
    // (Requirement 5.2, 5.6).
    #[test]
    fn embedded_position_round_trips_for_large_payloads(
        position in any::<u64>(),
        value_size in POSITION_PREFIX_LEN..=MAX_VALUE_SIZE,
    ) {
        let payload = payload_for(position, value_size);
        prop_assert_eq!(position_of(&payload, value_size), Some(position));
    }

    // Feature: throughput-benchmark, Property 3
    //
    // A correctly-read Workload verifies. The `value_size` range spans both the
    // embedded-position path (`s >= 8`) and the multiset fallback (`s < 8`,
    // including `0`), so the success guarantee holds across both verification
    // strategies (Requirement 5.1, 5.2).
    #[test]
    fn correct_workload_verifies(
        count in 1u64..=200,
        value_size in 0usize..=64,
    ) {
        let consumed = workload(count, value_size);
        prop_assert_eq!(verify_consumed(count, value_size, &consumed), Ok(()));
    }

    // Feature: throughput-benchmark, Property 3
    //
    // Tamper — single corrupted byte. For `value_size >= 8`, flipping a fill
    // byte (offset >= 8) of the record at position `target` leaves the embedded
    // position recoverable as `target` but breaks the payload, so the verifier
    // reports exactly `PayloadMismatch { position: target }` (Requirement 5.2,
    // 5.5).
    #[test]
    fn single_corrupted_byte_is_detected_at_its_position(
        count in 1u64..=200,
        // value_size >= 9 guarantees at least one fill byte at offset >= 8.
        value_size in (POSITION_PREFIX_LEN + 1)..=64,
        target_seed in any::<u64>(),
        offset_seed in any::<usize>(),
    ) {
        let target = target_seed % count;
        // Choose a fill-byte offset in [8, value_size).
        let fill_len = value_size - POSITION_PREFIX_LEN;
        let offset = POSITION_PREFIX_LEN + (offset_seed % fill_len);

        let mut consumed = workload(count, value_size);
        consumed[target as usize][offset] ^= 0xFF;

        prop_assert_eq!(
            verify_consumed(count, value_size, &consumed),
            Err(VerificationError::PayloadMismatch { position: target }),
        );
    }

    // Feature: throughput-benchmark, Property 3
    //
    // Tamper — extra record. Pushing an in-range duplicate keeps every payload
    // valid, so the only violation is the inflated count: the verifier reports
    // `CountMismatch { read: count + 1, expected: count }` (Requirement 5.1,
    // 5.5).
    #[test]
    fn extra_record_is_detected_as_count_mismatch(
        count in 1u64..=200,
        value_size in 0usize..=64,
    ) {
        let mut consumed = workload(count, value_size);
        // A duplicate of an in-range record stays a valid payload, isolating the
        // count violation from any payload violation.
        consumed.push(payload_for(0, value_size));

        prop_assert_eq!(
            verify_consumed(count, value_size, &consumed),
            Err(VerificationError::CountMismatch {
                read: count + 1,
                expected: count,
            }),
        );
    }
}
