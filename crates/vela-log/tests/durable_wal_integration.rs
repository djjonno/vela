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

/// Build a `RecordBatch` payload from raw bytes. The bytes are opaque to
/// `vela-log` (a length-delimited concatenation produced by `vela-core`); the
/// log must carry and recover them exactly like any other payload.
fn record_batch(bytes: &[u8]) -> EntryPayload {
    EntryPayload::new(PayloadKind::RecordBatch, bytes.to_vec())
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

/// append(RecordBatch) → read back, in a single open.
///
/// Asserts that a `RecordBatch` entry round-trips through the live WAL: the
/// assigned index follows the append contract and the entry read back carries
/// the same `PayloadKind::RecordBatch` tag and the exact opaque bytes that were
/// appended, alongside an ordinary `Record` entry so the two kinds coexist
/// (batched-produce Requirement 2.1).
#[test]
fn record_batch_entry_round_trips_through_real_fs() {
    let tmp = TempDir::new("record-batch-round-trip");
    let cfg = WalConfig::new(tmp.path());

    // A length-delimited-style opaque payload: vela-log never interprets it.
    let batch_bytes: Vec<u8> = (0..64u8).collect();

    let mut wal = DurableWal::open(cfg).expect("open on a fresh dir should succeed");

    // A plain Record first, then the RecordBatch, so the new kind is exercised
    // interleaved with the existing one.
    let record_index = wal
        .append(record(&[1, 2, 3]), 1)
        .expect("append of a Record should persist");
    assert_eq!(record_index, 0, "first append takes index 0");

    let batch_index = wal
        .append(record_batch(&batch_bytes), 1)
        .expect("append of a RecordBatch should persist");
    assert_eq!(batch_index, 1, "append must assign Last_Index + 1");
    assert_eq!(wal.last_index(), Some(1));

    // Read the batch entry back and assert kind and bytes round-trip exactly.
    let read_back = wal
        .entry(batch_index)
        .expect("the RecordBatch entry must be readable");
    assert_eq!(
        read_back.payload.kind,
        PayloadKind::RecordBatch,
        "the RecordBatch kind tag must round-trip"
    );
    assert_eq!(
        read_back.payload.bytes, batch_bytes,
        "the opaque RecordBatch bytes must round-trip byte-for-byte"
    );
    assert_eq!(read_back.index, batch_index);
    assert_eq!(read_back.term, 1);

    // The unrelated Record entry is unaffected by the new kind.
    let plain = wal
        .entry(record_index)
        .expect("the Record entry is present");
    assert_eq!(plain.payload.kind, PayloadKind::Record);
    assert_eq!(plain.payload.bytes, vec![1, 2, 3]);
}

/// append(RecordBatch) → commit → drop → reopen → read.
///
/// Asserts that the recovery/replay path preserves a `RecordBatch` entry: after
/// a clean reopen, the recovered segment reports the same `PayloadKind` tag and
/// opaque bytes, the same `last_index`/`commit_index`, and the snapshot's
/// committed prefix carries the batch entry intact (batched-produce
/// Requirements 2.1, 7.3).
#[test]
fn record_batch_entry_survives_reopen_recovery() {
    let tmp = TempDir::new("record-batch-recovery");
    let cfg = WalConfig::new(tmp.path());

    // Mixed kinds across the log so recovery must preserve each entry's tag.
    let expected: Vec<LogEntry> = vec![
        LogEntry {
            index: 0,
            term: 1,
            payload: record(&[0xAA, 0xBB]),
        },
        LogEntry {
            index: 1,
            term: 1,
            payload: record_batch(&(0..40u8).collect::<Vec<u8>>()),
        },
        LogEntry {
            index: 2,
            term: 2,
            payload: record_batch(&[]), // an empty-bytes batch must also recover
        },
        LogEntry {
            index: 3,
            term: 2,
            payload: record(&[0xCC]),
        },
    ];

    // --- produce: append the mixed entries on a fresh dir, then commit all.
    {
        let mut wal = DurableWal::open(cfg.clone()).expect("open on a fresh dir should succeed");
        for entry in &expected {
            let assigned = wal
                .append(entry.payload.clone(), entry.term)
                .expect("append under the Always policy should persist");
            assert_eq!(assigned, entry.index, "append must assign Last_Index + 1");
        }
        wal.commit(3).expect("commit within bounds should succeed");
        // `wal` is dropped here, releasing the directory lock before reopen.
    }

    // --- reopen: recovery must replay every entry with its kind and bytes.
    let reopened = DurableWal::open(cfg).expect("reopen of an existing dir should succeed");

    assert_eq!(
        reopened.last_index(),
        Some(3),
        "Last_Index must survive reopen"
    );
    assert_eq!(
        reopened.commit_index(),
        Some(3),
        "Commit_Index must survive reopen"
    );

    for entry in &expected {
        let recovered = reopened
            .entry(entry.index)
            .unwrap_or_else(|| panic!("entry {} must be recovered", entry.index));
        assert_eq!(
            recovered.payload.kind, entry.payload.kind,
            "kind of entry {} must be preserved across recovery",
            entry.index
        );
        assert_eq!(
            recovered.payload.bytes, entry.payload.bytes,
            "bytes of entry {} must be preserved byte-for-byte across recovery",
            entry.index
        );
        assert_eq!(
            &recovered, entry,
            "entry {} must recover intact",
            entry.index
        );
    }

    // The full range read returns every entry in ascending order.
    assert_eq!(
        reopened.read(0, 3),
        expected,
        "range read must match after recovery"
    );

    // The snapshot's committed prefix carries the RecordBatch entries intact.
    let snapshot = reopened.snapshot();
    assert_eq!(
        snapshot,
        Snapshot {
            commit_index: Some(3),
            entries: expected.clone(),
        },
        "snapshot must preserve the RecordBatch entries in the committed prefix"
    );
}
