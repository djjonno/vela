//! Property test for the append/read round-trip of the partition log.
//!
//! Feature: vela-streaming-platform, Property 2
//!
//! ### Property 2: Append/read round-trip preserves entries
//!
//! *For any* sequence of appended entries with no intervening revert, reading the
//! full index range returns exactly those entries in the same order.
//!
//! **Validates: Requirements 6.13**
//!
//! Requirement 6.13: "FOR ALL Log_Entries appended and then read back over their
//! full index range without an intervening revert, THE Log SHALL return entries
//! equal to those appended in the same order."

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogEntry, LogStorage, PayloadKind};

/// Strategy for a single payload-kind tag.
fn payload_kind() -> impl Strategy<Value = PayloadKind> {
    prop_oneof![
        Just(PayloadKind::Record),
        Just(PayloadKind::Cluster),
        Just(PayloadKind::Noop),
    ]
}

/// Strategy for one to-be-appended entry: a `(kind, bytes, term)` triple.
///
/// Bytes are bounded in length and terms span a wide range so the generated
/// sequence exercises varied payloads and non-monotonic terms.
fn appendable() -> impl Strategy<Value = (PayloadKind, Vec<u8>, u64)> {
    (
        payload_kind(),
        prop::collection::vec(any::<u8>(), 0..32),
        0u64..1_000,
    )
}

proptest! {
    // Minimum 100 iterations (Property 2).
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Appending a sequence of entries (no intervening revert) and then reading the
    /// full index range returns exactly those entries, in append order.
    #[test]
    fn append_read_roundtrip_preserves_entries(
        items in prop::collection::vec(appendable(), 0..50)
    ) {
        let mut log = InMemoryLog::new();

        // Append every generated entry, recording what we expect to read back.
        // The append API assigns indices itself, so we mirror that here: the
        // first entry lands at index 0 and each subsequent at the next index.
        let mut expected: Vec<LogEntry> = Vec::with_capacity(items.len());
        for (kind, bytes, term) in &items {
            let payload = EntryPayload::new(*kind, bytes.clone());
            let assigned = log.append(payload.clone(), *term).unwrap();
            prop_assert_eq!(assigned, expected.len() as u64);
            expected.push(LogEntry {
                index: assigned,
                term: *term,
                payload,
            });
        }

        // Read the full index range. On an empty log there is nothing to read and
        // the full range read must be empty; otherwise read `0..=last_index`.
        let read_back = match log.last_index() {
            None => {
                prop_assert!(expected.is_empty());
                log.read(0, 0)
            }
            Some(last) => log.read(0, last),
        };

        // The round-trip must return exactly the appended entries, same order.
        prop_assert_eq!(read_back, expected);
    }
}
