//! Property test for sequential index assignment in `vela-log`.
//!
//! Feature: vela-streaming-platform, Property 1
//!
//! Property 1: Append assigns the next sequential index. For any sequence of
//! appends to a log (empty or not), each append stores the entry at index 0 for
//! the first entry and at exactly `highest_index + 1` thereafter, and returns
//! that assigned index.
//!
//! Validates: Requirements 6.3, 6.4

use proptest::prelude::*;
use vela_log::{EntryPayload, InMemoryLog, LogStorage, PayloadKind};

/// Map a small tag selector to a [`PayloadKind`] so the generator explores all
/// payload variants without depending on the enum's internal ordering.
fn kind_for(tag: u8) -> PayloadKind {
    match tag % 3 {
        0 => PayloadKind::Record,
        1 => PayloadKind::Cluster,
        _ => PayloadKind::Noop,
    }
}

/// Generate a sequence of appends, each a `(kind tag, payload bytes, term)`
/// tuple. The sequence may be empty (exercising the empty-log boundary) and
/// ranges up to a modest length to keep iterations fast while still covering
/// many appends.
fn appends_strategy() -> impl Strategy<Value = Vec<(u8, Vec<u8>, u64)>> {
    prop::collection::vec(
        (
            any::<u8>(),
            prop::collection::vec(any::<u8>(), 0..16),
            any::<u64>(),
        ),
        0..64,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 1
    #[test]
    fn append_assigns_next_sequential_index(appends in appends_strategy()) {
        let mut log = InMemoryLog::new();

        for (i, (tag, bytes, term)) in appends.into_iter().enumerate() {
            let expected_index = i as u64;

            // Before the append, `last_index` reflects the count of entries so
            // far: `None` for the first append, otherwise `expected_index - 1`.
            let prior_last = log.last_index();
            if expected_index == 0 {
                prop_assert_eq!(prior_last, None);
            } else {
                prop_assert_eq!(prior_last, Some(expected_index - 1));
            }

            let kind = kind_for(tag);
            let payload = EntryPayload::new(kind, bytes.clone());
            let assigned = log.append(payload.clone(), term).unwrap();

            // The returned index is the next sequential index: 0 for the first
            // entry, `highest_index + 1` thereafter (Requirements 6.3, 6.4).
            prop_assert_eq!(assigned, expected_index);

            // The entry is stored at exactly that index, and `last_index`
            // advances to it.
            prop_assert_eq!(log.last_index(), Some(expected_index));
            let stored = log.entry(expected_index).expect("entry stored at index");
            prop_assert_eq!(stored.index, expected_index);
            prop_assert_eq!(stored.term, term);
            prop_assert_eq!(&stored.payload, &payload);
        }
    }
}
