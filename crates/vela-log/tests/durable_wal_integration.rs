//! End-to-end integration tests for [`DurableWal`] against the **real
//! filesystem**.
//!
//! Unlike the crate's unit tests (which drive the in-memory fault filesystem
//! through internal seams), these tests exercise only the public API surface
//! re-exported from the crate root —
//! `vela_log::{DurableWal, WalConfig, SyncPolicy, LogStorage, LogEntry,
//! EntryPayload, PayloadKind, LogError, Snapshot}` — and a genuine on-disk
//! Data_Directory, so they validate that `DurableWal::open` works end-to-end
//! over `std::fs` (Requirements 12.1, 12.2, 12.5).
//!
//! Each test creates a process-unique temporary directory under
//! [`std::env::temp_dir`] and removes it on completion via a [`TempDir`] guard.
//! The guard's `Drop` runs after the locally-owned `DurableWal` values have been
//! dropped, so the exclusive directory lock is released before cleanup.

use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use vela_log::{
    DurableWal, EntryPayload, LogEntry, LogStorage, PayloadKind, Snapshot, SyncPolicy, WalConfig,
};

/// Monotonic counter making temp-dir names unique within a single process even
/// when two tests start within the same nanosecond.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// An owned temporary directory that is recursively removed when dropped.
///
/// Cleanup is best-effort: a failure to remove the directory (for example
/// because a stray lock is still held) must not mask a test assertion, so the
/// error is ignored.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a uniquely-named directory under the system temp directory.
    ///
    /// The name combines the process id, a per-process atomic counter, and the
    /// current nanosecond timestamp so that concurrent test binaries and
    /// repeated runs never collide.
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the unix epoch")
            .as_nanos();
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("vela-log-it-{tag}-{}-{unique}-{nanos}", process::id());
        let path = std::env::temp_dir().join(name);
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // Best-effort: ignore errors so cleanup never overrides a real failure.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Build a `Record` payload from raw bytes.
fn record(bytes: &[u8]) -> EntryPayload {
    EntryPayload::new(PayloadKind::Record, bytes.to_vec())
}

/// open → produce (append several entries) → commit → drop → reopen → read.
///
/// Asserts that after a clean reopen the recovered log reports the same
/// entries, `last_index`, and `commit_index`, and that the payload-bearing
/// reads (`entry`, `read`, `snapshot`) return the original data from disk
/// (Requirements 5.1, 5.3, 9.1, 12.1).
#[test]
fn open_produce_reopen_read_round_trips_through_real_fs() {
    let tmp = TempDir::new("produce-reopen");
    let cfg = WalConfig::new(tmp.path());

    // The exact entries we expect to recover, built once so the original and
    // the reopened log can be compared against the same source of truth.
    let expected: Vec<LogEntry> = (0..5u64)
        .map(|i| LogEntry {
            index: i,
            term: 1 + i / 2, // a couple of distinct terms across the batch
            payload: record(&[i as u8, (i * 7) as u8]),
        })
        .collect();

    // --- produce: append entries on a fresh directory, then commit a prefix.
    {
        let mut wal = DurableWal::open(cfg.clone()).expect("open on a fresh dir should succeed");
        for entry in &expected {
            let assigned = wal
                .append(entry.payload.clone(), entry.term)
                .expect("append under the Always policy should persist");
            assert_eq!(assigned, entry.index, "append must assign Last_Index + 1");
        }
        assert_eq!(wal.last_index(), Some(4));

        wal.commit(3).expect("commit within bounds should succeed");
        assert_eq!(wal.commit_index(), Some(3));
        // `wal` is dropped here, releasing the directory lock before reopen.
    }

    // --- reopen: a new DurableWal on the same directory must recover state.
    let reopened = DurableWal::open(cfg).expect("reopen of an existing dir should succeed");

    assert_eq!(
        reopened.last_index(),
        Some(4),
        "Last_Index must survive reopen"
    );
    assert_eq!(
        reopened.commit_index(),
        Some(3),
        "Commit_Index must survive reopen"
    );
    assert_eq!(reopened.log_start_index(), 0, "no compaction means start 0");

    // Every entry is readable from disk with identical index/term/payload.
    for entry in &expected {
        assert_eq!(
            reopened.entry(entry.index).as_ref(),
            Some(entry),
            "entry {} must be recovered byte-for-byte",
            entry.index
        );
        assert_eq!(reopened.term_at(entry.index), Some(entry.term));
    }

    // A full range read returns the entries in ascending order.
    assert_eq!(reopened.read(0, 4), expected, "range read must match");

    // The snapshot is exactly the committed prefix 0..=3.
    let snapshot = reopened.snapshot();
    assert_eq!(
        snapshot,
        Snapshot {
            commit_index: Some(3),
            entries: expected[..=3].to_vec(),
        },
        "snapshot must be the retained committed prefix"
    );
}

/// append → commit → compaction(point) → drop → reopen.
///
/// Asserts that after compacting away a committed prefix and reopening, the
/// `Log_Start_Index` is preserved, the discarded entries are gone (and reads
/// below the line return nothing without error), and every retained entry is
/// intact with its original index/term/payload (Requirements 7.2, 7.6, 7.7,
/// 8.1, 8.3, 8.4, 12.1).
#[test]
fn compaction_then_reopen_preserves_log_start_and_retained_entries() {
    let tmp = TempDir::new("compaction-reopen");
    // A small segment size forces several segments so compaction can reclaim
    // whole below-the-line segments rather than only updating the start index.
    let cfg = WalConfig::new(tmp.path())
        .with_segment_size(64)
        .with_sync_policy(SyncPolicy::Always);

    let total = 10u64;
    let retained_point = 6u64;

    let all: Vec<LogEntry> = (0..total)
        .map(|i| LogEntry {
            index: i,
            term: 1,
            payload: record(&[0xA0 | (i as u8)]),
        })
        .collect();

    // --- produce, commit everything, then compact away [0, retained_point).
    {
        let mut wal = DurableWal::open(cfg.clone()).expect("open on a fresh dir should succeed");
        for entry in &all {
            wal.append(entry.payload.clone(), entry.term)
                .expect("append should persist");
        }
        wal.commit(total - 1)
            .expect("commit of full log should succeed");

        wal.compaction(retained_point)
            .expect("compaction of a committed prefix should succeed");

        assert_eq!(
            wal.log_start_index(),
            retained_point,
            "compaction must advance Log_Start_Index to the retained point"
        );
        // Discarded indices are gone immediately, before any reopen.
        assert_eq!(wal.entry(0), None);
        assert_eq!(wal.entry(retained_point - 1), None);
        // `wal` is dropped here, releasing the directory lock before reopen.
    }

    // --- reopen: the advanced Log_Start_Index and retained entries persist.
    let reopened = DurableWal::open(cfg).expect("reopen after compaction should succeed");

    assert_eq!(
        reopened.log_start_index(),
        retained_point,
        "Log_Start_Index must survive reopen (R7.7)"
    );
    assert_eq!(
        reopened.last_index(),
        Some(total - 1),
        "Last_Index is unchanged by compaction (R7.6)"
    );
    assert_eq!(
        reopened.commit_index(),
        Some(total - 1),
        "Commit_Index is unchanged by compaction (R7.6)"
    );

    // Discarded entries below the line return None / empty, never an error.
    for i in 0..retained_point {
        assert_eq!(reopened.entry(i), None, "discarded index {i} must be gone");
        assert_eq!(reopened.term_at(i), None);
    }
    // A read range entirely below the line is empty (R8.4).
    assert!(reopened.read(0, retained_point - 1).is_empty());

    // Retained entries are intact, byte-for-byte, in ascending order.
    let expected_retained = &all[retained_point as usize..];
    for entry in expected_retained {
        assert_eq!(
            reopened.entry(entry.index).as_ref(),
            Some(entry),
            "retained index {} must be intact after reopen",
            entry.index
        );
    }
    assert_eq!(
        reopened.read(retained_point, total - 1),
        expected_retained.to_vec(),
        "range read of the retained tail must match"
    );

    // A read clamped across the line returns only the retained portion (R8.3).
    assert_eq!(
        reopened.read(0, total - 1),
        expected_retained.to_vec(),
        "a read spanning the line returns only retained entries"
    );
}
