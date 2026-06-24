// Feature: batched-produce, Property 6: A batch is made durable with a single sync, fewer than N for N > 1
//!
//! Property 6: appending a batch to a durable log amortizes its durability cost
//! over all of the batch's records. The durable backend
//! ([`vela_log::DurableWal`]) runs under the only consensus-safe policy,
//! [`SyncPolicy::Always`], where every `append` forces the new tail to stable
//! storage before acknowledging. A batch of `N` records is carried as **one**
//! `RecordBatch` log entry, so it is a single `append` and therefore a single
//! force — exactly as cheap, in syncs, as appending one single-record entry —
//! whereas producing the same `N` records one at a time costs `N` separate
//! `append`s and `N` separate forces.
//!
//! The force count is observed deterministically through the `sim` fault
//! filesystem ([`vela_log::sim::FaultFileSystem`]), an in-memory
//! [`FileSystem`](vela_log::sim::FileSystem) that tallies every `sync_all`,
//! `sync_data`, and `sync_dir` it performs and exposes the total via
//! `fsync_count()`. Each measurement opens a fresh `DurableWal` over its own
//! fresh fault filesystem (so the segment-creation forces of the *first* append
//! are paid identically by every arm), records `fsync_count()` before and after
//! the append(s), and compares the deltas.
//!
//! For *any* batch of `N > 1` records the test asserts:
//!
//! - **one sync covers the whole batch** — appending the batch performs the
//!   same number of forces as appending a single record, and at least one force
//!   (Requirement 7.1);
//! - **fewer than N single produces** — the batch's force count is strictly
//!   less than the force count of `N` separate single-record appends
//!   (Requirement 7.2);
//! - **one entry, committing as one unit** — the batch advances the log's last
//!   index by exactly 1, i.e. it is a single replicated log entry rather than
//!   `N` independent ones (Requirement 7.3).
//!
//! Validates: Requirements 7.1, 7.2, 7.3
#![cfg(feature = "sim")]

use proptest::prelude::*;

use vela_core::{encode_record_batch, Record, SimWalClock};
use vela_log::sim::FaultFileSystem;
use vela_log::{DurableWal, EntryPayload, LogStorage, PayloadKind, SyncPolicy, WalConfig};

/// Open a fresh durable WAL over the injected fault filesystem `fs` at `dir`
/// with the consensus-safe `Always` policy — the policy under which every
/// `append` forces the tail to stable storage (the source of Property 6).
fn open_wal(fs: FaultFileSystem, dir: &str) -> DurableWal<FaultFileSystem, SimWalClock> {
    DurableWal::open_with_clock(
        WalConfig::new(dir).with_sync_policy(SyncPolicy::Always),
        fs,
        SimWalClock::new(),
    )
    .expect("opening a sim DurableWal under SyncPolicy::Always should succeed")
}

/// A `RecordBatch` log entry carrying `n` records' values as the
/// length-delimited concatenation the durable log stores opaquely — exactly the
/// one entry the batch-produce path appends (keys are unpersisted, so records
/// carry `None` keys, matching production).
fn batch_payload(n: usize) -> EntryPayload {
    let records: Vec<Record> = (0..n)
        .map(|i| Record::new(None, vec![i as u8; 4]))
        .collect();
    EntryPayload::new(PayloadKind::RecordBatch, encode_record_batch(&records))
}

/// A single-`Record` log entry carrying one short value, matching the
/// single-record produce path the durable log assigns one offset to.
fn single_payload(seed: u8) -> EntryPayload {
    EntryPayload::new(PayloadKind::Record, vec![seed; 4])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: batched-produce, Property 6: A batch is made durable with a
    // single sync, fewer than N for N > 1.
    //
    // For any N > 1: a batch of N records appended as one RecordBatch entry is
    // forced exactly as many times as a single-record append (one sync covers
    // the whole batch, Requirement 7.1), strictly fewer times than N separate
    // single-record appends (Requirement 7.2), and lands as exactly one log
    // entry committing as one unit (Requirement 7.3).
    #[test]
    fn batch_of_n_is_durable_in_one_sync_fewer_than_n_singles(n in 2usize..=64) {
        // Arm 1: one batch append of N records over its own fresh fault FS.
        let batch_fs = FaultFileSystem::default();
        let mut batch_wal = open_wal(batch_fs.clone(), "/sim/batch-durability/batch");
        let before_batch = batch_fs.fsync_count();
        let batch_index = batch_wal
            .append(batch_payload(n), 1)
            .expect("appending a RecordBatch entry under Always should succeed");
        let batch_forces = batch_fs.fsync_count() - before_batch;

        // Arm 2: one single-record append over its own fresh fault FS.
        let single_fs = FaultFileSystem::default();
        let mut single_wal = open_wal(single_fs.clone(), "/sim/batch-durability/single");
        let before_single = single_fs.fsync_count();
        single_wal
            .append(single_payload(0), 1)
            .expect("appending one Record entry under Always should succeed");
        let single_forces = single_fs.fsync_count() - before_single;

        // Arm 3: N separate single-record appends over its own fresh fault FS.
        let many_fs = FaultFileSystem::default();
        let mut many_wal = open_wal(many_fs.clone(), "/sim/batch-durability/many");
        let before_many = many_fs.fsync_count();
        for i in 0..n {
            many_wal
                .append(single_payload(i as u8), 1)
                .expect("appending one of N Record entries under Always should succeed");
        }
        let many_forces = many_fs.fsync_count() - before_many;

        // Requirement 7.1: a single durability sync covers the entire batch —
        // appending the batch forces exactly as many times as appending one
        // single record (both being the first append on a fresh log, they pay
        // the same segment-creation cost), and at least one force happened.
        prop_assert!(batch_forces >= 1, "the batch append must force at least once");
        prop_assert_eq!(
            batch_forces,
            single_forces,
            "a batch of {} forced {} times; one single record forced {} times — \
             a batch must be made durable with a single record's worth of syncs",
            n,
            batch_forces,
            single_forces
        );

        // Requirement 7.2: the batch is made durable with strictly fewer syncs
        // than producing the same N records as N separate single-record appends,
        // for every N > 1.
        prop_assert!(
            batch_forces < many_forces,
            "a batch of {} forced {} times, which is not strictly fewer than the \
             {} forces {} separate single-record appends performed",
            n,
            batch_forces,
            many_forces,
            n
        );

        // Requirement 7.3: the batch is one log entry committing as one unit —
        // the append advances the log's last index by exactly 1 (from empty),
        // rather than appending N independent entries.
        prop_assert_eq!(batch_index, 0, "first append must take index 0");
        prop_assert_eq!(
            batch_wal.last_index(),
            Some(0),
            "a batch of {} records must be a single log entry (last index 0)",
            n
        );

        // And the N single-record arm did append N independent entries — the
        // contrast that makes the durability win meaningful.
        prop_assert_eq!(
            many_wal.last_index(),
            Some(n as u64 - 1),
            "N single-record appends must produce N independent log entries"
        );
    }
}
