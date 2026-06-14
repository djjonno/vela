//! Unit/example and smoke tests for append-only log edge cases.
//!
//! Feature: vela-streaming-platform, Task 3.9
//!
//! These are example/unit/smoke tests (not property tests). They cover:
//! - the initial commit index of a fresh log is the uncommitted state `None`
//!   (Requirement 6.7),
//! - a `read` whose `start` exceeds its `end` returns zero entries without an
//!   error (Requirement 6.6), and
//! - a structural smoke check that the `LogStorage` trait exists and that
//!   `InMemoryLog` implements it (Requirements 6.1, 6.2).

use vela_log::{CommitIndex, EntryPayload, InMemoryLog, LogStorage, PayloadKind};

/// Build a record payload whose single byte encodes `byte`.
fn payload(byte: u8) -> EntryPayload {
    EntryPayload::new(PayloadKind::Record, vec![byte])
}

/// Smoke check (Requirements 6.1, 6.2): consensus depends on the `LogStorage`
/// trait, not a concrete type. This generic function compiles only because the
/// trait exists and exposes the methods used; calling it with an `InMemoryLog`
/// proves the in-memory implementation satisfies the trait bound.
///
/// It exercises the trait purely through the `T: LogStorage` bound — never
/// through `InMemoryLog`'s inherent API — so it is a genuine trait-level check.
fn exercise_via_trait<T: LogStorage>(log: &mut T) -> CommitIndex {
    let assigned = log.append(payload(7), 1).expect("append should succeed");
    assert_eq!(assigned, 0, "first append on an empty log is index 0");
    log.commit(0)
        .expect("commit of the only entry should succeed");
    log.commit_index()
}

#[test]
fn log_storage_trait_is_implemented_by_in_memory_log() {
    // If `InMemoryLog` did not implement `LogStorage`, this would not compile.
    let mut log = InMemoryLog::new();
    let committed = exercise_via_trait(&mut log);
    assert_eq!(
        committed,
        Some(0),
        "driving InMemoryLog through the LogStorage trait must work end to end"
    );
}

#[test]
fn fresh_log_reports_uncommitted_commit_index() {
    // Requirement 6.7: before any commit, the commit index is the uncommitted
    // state preceding index 0, represented as `None`.
    let log = InMemoryLog::new();
    assert_eq!(log.commit_index(), None);
    assert_eq!(log.last_index(), None);

    // Appending entries without committing must not advance the commit index.
    let mut log = InMemoryLog::new();
    for i in 0..3 {
        log.append(payload(i), 1).expect("append should succeed");
    }
    assert_eq!(
        log.commit_index(),
        None,
        "appending without committing leaves the log uncommitted"
    );

    // The snapshot of an uncommitted log is empty and carries `None`.
    let snapshot = log.snapshot();
    assert_eq!(snapshot.commit_index, None);
    assert!(snapshot.entries.is_empty());
}

#[test]
fn read_with_start_greater_than_end_returns_empty_without_error() {
    // Requirement 6.6: an inverted range yields zero entries and is not an
    // error. `read` returns a `Vec`, so "no error" is expressed by an empty
    // result rather than a panic or `Result::Err`.
    let mut log = InMemoryLog::new();
    for i in 0..5 {
        log.append(payload(i), 1).expect("append should succeed");
    }

    // Just-inverted range.
    assert!(log.read(3, 2).is_empty());
    // Inverted range entirely within the stored span.
    assert!(log.read(4, 0).is_empty());
    // Inverted range with bounds past the end of the log.
    assert!(log.read(100, 1).is_empty());

    // An inverted range on an empty log is likewise empty.
    let empty = InMemoryLog::new();
    assert!(empty.read(1, 0).is_empty());
}
