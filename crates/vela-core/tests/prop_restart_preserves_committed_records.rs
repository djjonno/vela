//! Property test for durable restart recovery of a `PartitionReplica`.
//!
//! Feature: per-topic-log-durability, Property 13
//!
//! Property 13: committing a sequence of records on a `PartitionReplica` backed
//! by `PartitionLog::Durable`, then dropping the replica and reopening a fresh
//! durable backend on the same data directory and recovering, restores the
//! identical committed records at identical offsets. The recovered Raft
//! `commit_index` equals the recovered log's commit index, the committed prefix
//! is re-applied exactly once in ascending offset order, and the high-water
//! offset never regresses across the restart.
//!
//! This is the `vela-core` realization of Requirements 11.1, 11.2, 11.3 (a
//! restarted durable replica initializes `commit_index` from the recovered log
//! and re-applies the committed prefix to the state machine exactly once in
//! ascending order, so committed records reappear at the same offsets) and of
//! Requirements 14.1, 14.3 (a durable topic's committed records survive a
//! restart and the committed offset never regresses).
//!
//! The test drives a real end-to-end restart: it opens a genuine `DurableWal`
//! under [`std::env::temp_dir`] with the consensus-safe `SyncPolicy::Always`,
//! commits records through a single-node Raft group (the lone replica is its
//! own majority, so each proposal commits immediately and deterministically),
//! drops the replica to release the directory lock, then reopens the WAL on the
//! same path and recovers via [`PartitionReplica::recover`].
//!
//! Case count: because each case performs real `fsync` I/O (every `Always`
//! append and commit forces to stable storage) the proptest case count is held
//! at the project minimum of 100 with deliberately small record batches, which
//! keeps the suite fast while still exercising a wide range of committed
//! sequences end to end.
//!
//! Validates: Requirements 11.1, 11.2, 11.3, 14.1, 14.3

use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proptest::prelude::*;
use vela_core::{PartitionLog, PartitionReplica};
use vela_log::{DurableWal, EntryPayload, LogStorage, PayloadKind, SyncPolicy, WalConfig};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, Role, TimerKind};

/// Monotonic counter making temp-dir names unique within a single process even
/// when two cases start within the same nanosecond.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// An owned temporary directory recursively removed when dropped.
///
/// Cleanup is best-effort: a failure to remove the directory must not mask a
/// test assertion, so the error is ignored. The guard is dropped only after the
/// locally-owned `PartitionReplica`/`DurableWal` values, so the exclusive
/// directory lock is released before cleanup.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a uniquely-named directory under the system temp directory,
    /// combining the process id, a per-process atomic counter, and the current
    /// nanosecond timestamp so concurrent binaries and repeated runs never
    /// collide.
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the unix epoch")
            .as_nanos();
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("vela-core-it-{tag}-{}-{unique}-{nanos}", process::id());
        Self {
            path: std::env::temp_dir().join(name),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A minimal [`Clock`] that never advances on its own; arming a timer is a
/// no-op. The test drives consensus with explicit [`RaftInput`]s, so no real
/// timing is needed and runs stay deterministic.
struct TestClock {
    now: Instant,
}

impl TestClock {
    fn new() -> Self {
        Self {
            now: Instant::now(),
        }
    }
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.now
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

/// Open a durable WAL on `dir` with the consensus-safe `Always` policy.
fn open_durable(dir: &Path) -> PartitionLog {
    let wal = DurableWal::open(WalConfig::new(dir).with_sync_policy(SyncPolicy::Always))
        .expect("opening a durable WAL on the data directory should succeed");
    PartitionLog::Durable(wal)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: per-topic-log-durability, Property 13
    #[test]
    fn restart_preserves_committed_records_at_identical_offsets(
        // A handful of records per case, each with arbitrary opaque value bytes.
        // Both bounds are kept small because every committed record triggers a
        // real fsync under the Always policy.
        values in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..16),
            1..12,
        ),
    ) {
        let tmp = TempDir::new("restart-recover");
        let n = values.len();

        // Captured immediately before the simulated restart.
        let pre_records: Vec<(u64, Vec<u8>)>;
        let pre_commit_index;
        let pre_high_water;

        // --- run: commit the record sequence on a fresh durable replica. ----
        {
            let mut clock = TestClock::new();
            let mut replica =
                PartitionReplica::with_log(RaftNodeId(0), Vec::new(), open_durable(tmp.path()));

            // A single-node group: the lone self-vote is a majority, so the
            // election makes the replica leader and each proposal commits in
            // the same step.
            replica.step(RaftInput::Tick(TimerKind::Election), &mut clock);
            prop_assert_eq!(replica.role(), Role::Leader);

            for value in &values {
                replica.step(
                    RaftInput::Propose(EntryPayload::new(PayloadKind::Record, value.clone())),
                    &mut clock,
                );
            }

            // Each record committed and was assigned its gap-free 0-based
            // offset by the state machine.
            let read_back = replica.read(0, n);
            prop_assert_eq!(read_back.len(), n);
            pre_records = read_back
                .into_iter()
                .map(|r| (r.offset, r.value))
                .collect();
            pre_commit_index = replica.raft().commit_index();
            pre_high_water = replica.high_water_mark();

            // The captured records are exactly offsets 0..n with the produced
            // values, in order (sanity check on the pre-restart state).
            let expected: Vec<(u64, Vec<u8>)> = values
                .iter()
                .enumerate()
                .map(|(i, v)| (i as u64, v.clone()))
                .collect();
            prop_assert_eq!(&pre_records, &expected);
            prop_assert_eq!(pre_high_water, Some((n as u64) - 1));

            // `replica` (and its DurableWal) drop here, releasing the lock.
        }

        // --- restart: reopen the same data directory and recover. -----------
        let recovered = PartitionReplica::recover(
            RaftNodeId(0),
            Vec::new(),
            open_durable(tmp.path()),
        );

        // 11.1: the recovered Raft commit_index is initialized from the
        // recovered log's commit index and equals the pre-restart value.
        prop_assert_eq!(recovered.raft().commit_index(), pre_commit_index);
        prop_assert_eq!(
            recovered.raft().log().commit_index(),
            pre_commit_index,
            "recovered commit_index must equal the durable log's commit index"
        );

        // 11.2 / 11.3: every committed record is re-applied exactly once, in
        // ascending offset order, so the recovered records are identical to the
        // pre-restart records at identical offsets.
        let post_records: Vec<(u64, Vec<u8>)> = recovered
            .read(0, n)
            .into_iter()
            .map(|r| (r.offset, r.value))
            .collect();
        prop_assert_eq!(&post_records, &pre_records);

        // Exactly-once ascending re-application: no record was dropped or
        // duplicated, and the offsets are the contiguous prefix 0..n.
        prop_assert_eq!(post_records.len(), n);
        for (i, (offset, _)) in post_records.iter().enumerate() {
            prop_assert_eq!(*offset, i as u64);
        }

        // 14.1 / 14.3: the high-water offset is preserved and never regresses
        // across the restart.
        prop_assert_eq!(recovered.high_water_mark(), pre_high_water);
        prop_assert!(recovered.high_water_mark() >= pre_high_water);
    }
}
