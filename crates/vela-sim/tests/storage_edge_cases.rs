#![cfg(feature = "sim")]
//! Edge-case tests for Sim_Storage torn-tail and armed I/O-error faults
//! (Requirements 7.3, 7.4), exercised end-to-end through the **public**
//! `vela_sim::storage` API over the real `DurableWal` under
//! `SyncPolicy::Always`.
//!
//! These complement the unit tests living beside the implementation in
//! `src/storage.rs`: those cover the lower-level single-operation cases, while
//! these drive concrete multi-record scenarios that pin the two requirements'
//! observable contract:
//!
//! - **Torn tail (Requirement 7.3):** an explicitly armed torn-tail makes
//!   recovery discard the torn trailing write down to the last intact record —
//!   every committed/acknowledged record survives and the torn trailing record
//!   is gone. Recovery succeeds (it never errors or panics on a torn tail).
//! - **Armed I/O error (Requirement 7.4):** an I/O fault selected from the
//!   `storage` RNG stream surfaces as [`LogError::Io`] through the
//!   [`LogStorage`] result type at the seed-derived operation (or the reopen,
//!   for the read kind) — it never silently succeeds — and the returned
//!   [`StorageFaultPlan`] names exactly the armed kind.
//!
//! The torn-tail scenarios are built so the *trailing write's* manifest
//! acknowledgement is the slot torn away while the last *acknowledged* extent
//! survives in the prior slot, which is what drives recovery back to the last
//! intact record (see `tear_amount` below). The assertions are exact so a
//! mutated recovery/arming path is caught.

use vela_sim::rng::SeedStreams;
use vela_sim::scenario::FaultIntensities;
use vela_sim::storage::{IoFaultKind, SimStorageHandle, StorageFaultPlan};

use vela_core::{GroupKey, NodeId, PartitionIndex};
use vela_log::{EntryPayload, LogError, LogStorage, PayloadKind};

/// A stable node id for the single replica these tests drive.
fn node() -> NodeId {
    NodeId::new("node-a")
}

/// A stable `(topic, partition)` group key.
fn group() -> GroupKey {
    ("orders".to_string(), PartitionIndex(0))
}

/// A single-byte `Record` payload, so each appended entry is distinguishable.
fn record(byte: u8) -> EntryPayload {
    EntryPayload::new(PayloadKind::Record, vec![byte])
}

/// How many trailing bytes to tear from the backing disk.
///
/// The manifest is a two-slot, fixed-offset, 128-byte-per-slot file; tearing
/// the disk's trailing bytes damages the slot at the file's end (slot 1) while
/// leaving the front slot (slot 0) intact. The torn-tail scenarios below issue
/// an even number of durable operations, so the *newest* acknowledgement lands
/// in slot 1 (the file end) and the prior acknowledged extent survives in
/// slot 0 — exactly the shape in which recovery must fall back to the last
/// intact record. `64` removes enough of slot 1 to invalidate it without
/// reaching slot 0; it is also the harness's `MAX_TORN_BYTES` magnitude.
const TEAR_BYTES: u64 = 64;

// ---------------------------------------------------------------------------
// Torn tail (Requirement 7.3)
// ---------------------------------------------------------------------------

/// An armed torn-tail discards the torn trailing write while every committed
/// record survives, and recovery succeeds rather than erroring or panicking.
///
/// Sequence (four durable ops → newest acknowledgement in the file-end slot):
/// append r0, append r1, commit(1), append r2 (uncommitted). Tearing the
/// trailing write removes the acknowledgement of r2, so recovery falls back to
/// the committed extent {r0, r1}.
#[test]
fn armed_torn_tail_recovers_to_the_last_intact_committed_record() {
    let handle = SimStorageHandle::new(&node(), &group());

    {
        let mut backend = handle.open().unwrap();
        assert_eq!(backend.append(record(10), 1).unwrap(), 0);
        assert_eq!(backend.append(record(11), 1).unwrap(), 1);
        backend.commit(1).unwrap();
        // The trailing, unacknowledged-by-commit write whose tail we tear.
        assert_eq!(backend.append(record(12), 1).unwrap(), 2);
        // Drop the live backend so the data-directory lock is free for reopen.
    }

    handle.arm_torn_tail(TEAR_BYTES);

    let recovered = handle
        .open()
        .expect("reopen after a torn tail must recover, not error");

    // The torn trailing record (index 2) is discarded; the committed prefix
    // {r0, r1} survives intact with its commit index.
    assert_eq!(
        recovered.last_index(),
        Some(1),
        "recovery must discard the torn trailing write down to the last intact record"
    );
    assert_eq!(recovered.commit_index(), Some(1));
    assert_eq!(recovered.entry(0).unwrap().payload, record(10));
    assert_eq!(recovered.entry(1).unwrap().payload, record(11));
    assert_eq!(
        recovered.entry(2),
        None,
        "the torn trailing record must not be recovered"
    );
}

/// Control: the identical sequence with no torn-tail armed recovers every
/// record, so the discard in the test above is attributable to the tear and
/// not to the reopen path itself.
#[test]
fn unfaulted_reopen_recovers_every_record() {
    let handle = SimStorageHandle::new(&node(), &group());

    {
        let mut backend = handle.open().unwrap();
        backend.append(record(10), 1).unwrap();
        backend.append(record(11), 1).unwrap();
        backend.commit(1).unwrap();
        backend.append(record(12), 1).unwrap();
    }

    let recovered = handle.open().unwrap();

    assert_eq!(recovered.last_index(), Some(2));
    assert_eq!(recovered.commit_index(), Some(1));
    assert_eq!(recovered.entry(0).unwrap().payload, record(10));
    assert_eq!(recovered.entry(1).unwrap().payload, record(11));
    assert_eq!(recovered.entry(2).unwrap().payload, record(12));
}

/// A torn tail with no committed records recovers to the last intact record:
/// the trailing write is dropped and the prior acknowledged record survives.
///
/// Sequence (two durable ops → newest acknowledgement in the file-end slot):
/// append r0, append r1 (both uncommitted). Tearing the trailing write removes
/// r1's acknowledgement, so recovery falls back to the intact r0.
#[test]
fn armed_torn_tail_recovers_to_the_last_intact_uncommitted_record() {
    let handle = SimStorageHandle::new(&node(), &group());

    {
        let mut backend = handle.open().unwrap();
        backend.append(record(20), 3).unwrap();
        backend.append(record(21), 3).unwrap();
    }

    handle.arm_torn_tail(TEAR_BYTES);

    let recovered = handle
        .open()
        .expect("reopen after a torn tail must recover, not error");

    assert_eq!(
        recovered.last_index(),
        Some(0),
        "recovery must keep the last intact record and drop the torn trailing one"
    );
    assert_eq!(
        recovered.commit_index(),
        None,
        "nothing was committed, so the recovered log has no commit index"
    );
    assert_eq!(recovered.entry(0).unwrap().payload, record(20));
    assert_eq!(recovered.entry(1), None, "the torn trailing record is gone");
}

// ---------------------------------------------------------------------------
// Armed I/O error at a seed-derived operation (Requirement 7.4)
// ---------------------------------------------------------------------------

/// `FaultIntensities` that force an I/O error (and never a torn write) so the
/// seed-derived selection always arms exactly one I/O fault.
fn io_error_only() -> FaultIntensities {
    FaultIntensities {
        torn_write_prob: 0.0,
        io_error_prob: 1.0,
        ..FaultIntensities::default()
    }
}

/// Derive the I/O-fault kind the `storage` stream selects for `seed` under
/// [`io_error_only`], on a throwaway handle so the real assertion arms a fresh
/// disk. Asserts the plan carries an I/O fault and no torn tail.
fn seed_derived_io_kind(seed: u64) -> IoFaultKind {
    let probe = SimStorageHandle::new(&node(), &group());
    let mut stream = SeedStreams::new(seed).storage;
    let plan = probe.arm_seed_derived_faults(&mut stream, &io_error_only());
    assert_eq!(
        plan.torn_tail, None,
        "torn writes are disabled, so no torn tail may be armed"
    );
    plan.io_fault
        .expect("an I/O fault must be armed at io_error_prob = 1.0")
}

/// Drive the operation that the armed `kind` must fail on, asserting the error
/// surfaces as [`LogError::Io`] through the result type (never a silent
/// success) and that the seed-derived [`StorageFaultPlan`] names exactly that
/// kind.
fn assert_seed_derived_io_error_surfaces(seed: u64, expected: IoFaultKind) {
    let handle = SimStorageHandle::new(&node(), &group());

    // Pre-populate durable state so the manifest exists; the read-kind fault
    // only surfaces on a recovery read, and a healthy prefix makes the
    // write/fsync kinds fail on a clearly-subsequent operation.
    {
        let mut backend = handle.open().unwrap();
        backend.append(record(1), 1).unwrap();
        backend.commit(0).unwrap();
    }

    let mut stream = SeedStreams::new(seed).storage;
    let plan: StorageFaultPlan = handle.arm_seed_derived_faults(&mut stream, &io_error_only());

    // The plan reports exactly the armed kind and nothing else.
    assert_eq!(plan.torn_tail, None);
    assert_eq!(
        plan.io_fault,
        Some(expected),
        "the seed-derived plan must name the armed I/O fault kind"
    );

    match expected {
        // A read fault fails-stop on the recovery read path: the reopen errors.
        IoFaultKind::Read => match handle.open() {
            Ok(_) => panic!("reopen must surface the armed read I/O error, not succeed"),
            Err(err) => assert!(
                matches!(err, LogError::Io { .. }),
                "expected LogError::Io from the recovery read, got {err:?}"
            ),
        },
        // A write/fsync fault surfaces on the next append's manifest force; the
        // reopen itself (a read path) succeeds.
        IoFaultKind::Write | IoFaultKind::Fsync => {
            let mut backend = handle
                .open()
                .expect("reopen reads only, so it must succeed for a write/fsync fault");
            let err = backend
                .append(record(2), 1)
                .expect_err("the armed write/fsync fault must surface, not silently succeed");
            assert!(
                matches!(err, LogError::Io { .. }),
                "expected LogError::Io from the armed operation, got {err:?}"
            );
        }
    }
}

/// For each I/O-fault kind, a seed that selects it deterministically arms the
/// fault, and the matching operation surfaces it as [`LogError::Io`] through
/// the result type — never a silent success (Requirement 7.4).
///
/// Searching seeds until every kind is covered also exercises the seed-derived
/// selection path itself: which kind is armed is a pure function of the
/// `storage` stream.
#[test]
fn seed_derived_io_error_surfaces_for_every_kind() {
    // Indexed by kind: fsync, write, read.
    let mut covered = [false; 3];
    let mut checked = [false; 3];

    for seed in 0u64..4096 {
        let kind = seed_derived_io_kind(seed);
        let idx = match kind {
            IoFaultKind::Fsync => 0,
            IoFaultKind::Write => 1,
            IoFaultKind::Read => 2,
        };
        covered[idx] = true;
        if !checked[idx] {
            checked[idx] = true;
            assert_seed_derived_io_error_surfaces(seed, kind);
        }
        if checked.iter().all(|&c| c) {
            break;
        }
    }

    assert!(
        covered.iter().all(|&c| c),
        "expected the storage stream to select every I/O-fault kind across the searched seeds"
    );
}

/// The seed-derived selection is reproducible: the same seed arms the same
/// I/O-fault kind every time, so a failing run replays identically
/// (Requirement 7.5 underpinning 7.4).
#[test]
fn seed_derived_io_kind_is_reproducible() {
    for seed in [0u64, 1, 7, 42, 0xABCD_1234, u64::MAX] {
        assert_eq!(
            seed_derived_io_kind(seed),
            seed_derived_io_kind(seed),
            "the same seed must select the same I/O-fault kind"
        );
    }
}
