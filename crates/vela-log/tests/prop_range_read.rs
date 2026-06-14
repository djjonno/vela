//! Property test for range reads on the append-only log.
//!
//! Feature: vela-streaming-platform, Property 3
//!
//! **Property 3: Range read is ascending and gap-omitting** — for any log and
//! any range `start..=end`, `read` returns the stored entries whose indices
//! fall within the range in ascending index order, omitting absent indices;
//! and for any range where `start > end` it returns zero entries without error.
//!
//! **Validates: Requirements 6.5, 6.6**

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogStorage, PayloadKind};

/// Build an [`InMemoryLog`] by appending `terms.len()` entries, one per term.
///
/// Each entry's payload bytes encode its index so a returned entry can be tied
/// back to the position it was appended at.
fn build_log(terms: &[u64]) -> InMemoryLog {
    let mut log = InMemoryLog::new();
    for (i, &term) in terms.iter().enumerate() {
        let payload = EntryPayload::new(PayloadKind::Record, (i as u64).to_le_bytes().to_vec());
        let assigned = log.append(payload, term).expect("append should succeed");
        assert_eq!(assigned, i as u64, "append must assign the next index");
    }
    log
}

proptest! {
    // Minimum 100 iterations; 256 cases gives ample coverage of the range space.
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn range_read_is_ascending_and_gap_omitting(
        // 0..=40 entries, each with an arbitrary term.
        terms in proptest::collection::vec(0u64..8, 0..=40),
        // Bounds deliberately reach past the log so out-of-range indices are
        // exercised, and `start` can exceed `end` to cover Requirement 6.6.
        start in 0u64..50,
        end in 0u64..50,
    ) {
        let log = build_log(&terms);
        let len = terms.len() as u64;

        let result = log.read(start, end);

        if start > end {
            // Requirement 6.6: an inverted range yields zero entries, no error.
            prop_assert!(
                result.is_empty(),
                "start ({start}) > end ({end}) must return zero entries, got {} ",
                result.len()
            );
        } else {
            // Requirement 6.5: the result is exactly the stored entries whose
            // indices fall in `start..=end`, in ascending index order, with any
            // index outside the stored range [0, len) omitted.
            let expected_indices: Vec<u64> = (start..=end).filter(|&i| i < len).collect();
            let got_indices: Vec<u64> = result.iter().map(|e| e.index).collect();

            prop_assert_eq!(
                &got_indices,
                &expected_indices,
                "read({}, {}) over a log of len {} must return the in-range \
                 stored indices ascending",
                start,
                end,
                len
            );

            // Strictly ascending with no duplicates.
            for pair in got_indices.windows(2) {
                prop_assert!(
                    pair[0] < pair[1],
                    "indices must be strictly ascending: {} then {}",
                    pair[0],
                    pair[1]
                );
            }

            // Each returned entry equals the stored entry at that index.
            for entry in &result {
                prop_assert!(entry.index < len, "returned index {} is in range", entry.index);
                prop_assert_eq!(
                    Some(entry.clone()),
                    log.entry(entry.index),
                    "returned entry must equal the stored entry at its index"
                );
            }
        }
    }
}
