//! Sim_Storage: the `LogStorage` seam over the real `DurableWal` and a
//! deterministic in-memory fault filesystem.
//!
//! Sim_Storage gives a simulated replica durable-WAL fidelity (Requirement 7.1)
//! by running the **production** [`DurableWal`] over a deterministic in-memory
//! [`FaultFileSystem`] rather than re-implementing a log model: the WAL's own
//! framing, manifest, recovery, torn-tail classification, and `Always` sync
//! semantics are therefore exactly the production ones. An [`InMemoryLog`]
//! variant provides in-memory-topic parity for topics that opt out of
//! durability.
//!
//! The three pieces this module provides:
//!
//! - [`SimWalClock`] — the WAL [`Clock`](vela_log::sim::Clock) seam, reading
//!   logical time only and never the wall clock. Defined once in `vela-core`
//!   (under its `sim` feature) and re-exported here, so the **same** clock type
//!   flows through both this crate's [`SimBackend`] and the production
//!   [`vela_core::PartitionLog::Sim`] backend the cluster builds.
//! - [`SimBackend`] — the closed-backend [`LogStorage`] a replica's `RaftNode`
//!   is built over (`Durable` over the fault filesystem, or `InMemory`).
//! - [`SimStorageHandle`] — owns one replica's backing disk and mints durable
//!   backends over it, modelling a Node_Crash (drop the un-fsynced tail) and a
//!   restart (reopen the same disk to run the real recovery path). It mints both
//!   a [`SimBackend`] (for storage-level tests) and a
//!   [`vela_core::PartitionLog`] `Sim` backend (for the cluster's production
//!   replicas) over the one shared disk.
//!
//! Seed-derived torn-tail / I/O-error fault *arming* is driven through
//! [`SimStorageHandle::arm_seed_derived_faults`], which consults the run's
//! `storage` RNG stream and [`FaultIntensities`] to decide — deterministically —
//! whether to tear this replica's trailing write and/or surface an I/O error on
//! its next storage operation. The lower-level [`arm_torn_tail`] /
//! [`arm_io_error`] entry points act directly on the shared [`FaultFileSystem`]
//! for callers that have already made the selection.
//!
//! [`arm_torn_tail`]: SimStorageHandle::arm_torn_tail
//! [`arm_io_error`]: SimStorageHandle::arm_io_error

use std::path::{Path, PathBuf};

use vela_core::{GroupKey, NodeId, PartitionLog};
use vela_log::sim::FaultFileSystem;
use vela_log::{
    CommitIndex, DurableWal, EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage,
    Snapshot, SyncPolicy, WalConfig,
};

use crate::rng::SplitMix64;
use crate::scenario::FaultIntensities;

/// The logical WAL [`Clock`](vela_log::sim::Clock) for Sim_Storage, reading
/// logical time only — never the wall clock.
///
/// Re-exported from `vela-core` (where it is defined under the `sim` feature)
/// so a single clock type is shared by this crate's [`SimBackend`] and the
/// production [`vela_core::PartitionLog::Sim`] backend the cluster builds —
/// avoiding two incompatible clock types at the
/// [`SimStorageHandle::open`] / [`SimStorageHandle::open_partition_log`] seam.
pub use vela_core::SimWalClock;

/// File name of the WAL manifest within a replica's Data_Directory.
///
/// This mirrors `vela_log`'s internal `MANIFEST_FILE_NAME` (`wal.manifest`),
/// which is not part of the public `sim` surface. Sim_Storage already hard-codes
/// the on-disk layout in [`data_dir_for`]; naming the manifest here lets a
/// path-scoped I/O-error fault target the one file the WAL writes, fsyncs, and
/// reads on every durable operation under [`SyncPolicy::Always`], so an armed
/// failure reliably surfaces on the next matching operation. Kept in sync with
/// the WAL: if `vela_log` renames the manifest, this constant moves with it.
const WAL_MANIFEST_FILE_NAME: &str = "wal.manifest";

/// Upper bound on the number of trailing bytes a seed-derived torn-tail fault
/// drops from a replica's most recent write (see
/// [`SimStorageHandle::arm_seed_derived_faults`]).
///
/// The exact count models *how much* of the trailing write failed to reach
/// stable storage; recovery applies the WAL's own torn-vs-interior
/// classification regardless of the count, discarding the torn tail down to the
/// last intact record. A small bound keeps the torn region within the trailing
/// write rather than reaching into earlier, acknowledged data.
const MAX_TORN_BYTES: u64 = 64;

/// The concrete [`LogStorage`] backend a simulated replica's `RaftNode` is built
/// over.
///
/// Mirrors the production `PartitionLog` closed-backend model: a durable-topic
/// replica is backed by the real [`DurableWal`] over a deterministic in-memory
/// [`FaultFileSystem`] (durable-topic fidelity — the WAL's framing, manifest,
/// recovery, and `Always` sync run unchanged, Requirement 7.1), and an
/// in-memory-topic replica is backed by the production [`InMemoryLog`]
/// (in-memory-topic parity). [`LogStorage`] is implemented by dispatching every
/// trait operation to whichever backend is held and returning its result
/// unchanged, so a `RaftNode<SimBackend>` behaves observably like one over the
/// matching production backend.
#[allow(clippy::large_enum_variant)]
pub enum SimBackend {
    /// The durable WAL backend over the deterministic in-memory fault disk.
    Durable(DurableWal<FaultFileSystem, SimWalClock>),
    /// The volatile in-memory backend.
    InMemory(InMemoryLog),
}

impl SimBackend {
    /// Construct an empty in-memory backend (in-memory-topic parity).
    pub fn in_memory() -> Self {
        SimBackend::InMemory(InMemoryLog::new())
    }
}

/// Dispatch a [`LogStorage`] method to whichever backend the [`SimBackend`]
/// holds, forwarding the arguments and returning the backend's result unchanged.
macro_rules! dispatch {
    ($self:ident, $method:ident $(, $arg:expr)*) => {
        match $self {
            SimBackend::Durable(backend) => backend.$method($($arg),*),
            SimBackend::InMemory(backend) => backend.$method($($arg),*),
        }
    };
}

impl LogStorage for SimBackend {
    fn append(&mut self, payload: EntryPayload, term: u64) -> Result<u64, LogError> {
        dispatch!(self, append, payload, term)
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> Result<(), LogError> {
        dispatch!(self, append_entries, entries)
    }

    fn read(&self, start: u64, end: u64) -> Vec<LogEntry> {
        dispatch!(self, read, start, end)
    }

    fn entry(&self, index: u64) -> Option<LogEntry> {
        dispatch!(self, entry, index)
    }

    fn last_index(&self) -> Option<u64> {
        dispatch!(self, last_index)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        dispatch!(self, term_at, index)
    }

    fn commit_index(&self) -> CommitIndex {
        dispatch!(self, commit_index)
    }

    fn commit(&mut self, index: u64) -> Result<(), LogError> {
        dispatch!(self, commit, index)
    }

    fn revert(&mut self, index: u64) -> Result<(), LogError> {
        dispatch!(self, revert, index)
    }

    fn snapshot(&self) -> Snapshot {
        dispatch!(self, snapshot)
    }

    fn flush(&mut self) -> Result<(), LogError> {
        dispatch!(self, flush)
    }

    fn persist_hard_state(&mut self, hard_state: HardState) -> Result<(), LogError> {
        dispatch!(self, persist_hard_state, hard_state)
    }

    fn hard_state(&self) -> Option<HardState> {
        dispatch!(self, hard_state)
    }
}

/// The deterministic data directory for `(node, group)` on the in-memory disk.
///
/// The layout is a pure function of the node id, topic name, and partition
/// index — `/sim/<node>/<topic>/<partition>` — so a crash and a restart reopen
/// exactly the same files (Requirement 6.3, 6.4). It never consults the real
/// filesystem; the path names entries in the in-memory [`FaultFileSystem`].
pub fn data_dir_for(node: &NodeId, group: &GroupKey) -> PathBuf {
    let (topic, partition) = group;
    Path::new("/sim")
        .join(node.as_str())
        .join(topic)
        .join(partition.0.to_string())
}

/// Which low-level filesystem failure an armed storage I/O fault injects.
///
/// All three target the WAL manifest, the one file written, fsynced, and read on
/// every durable operation under [`SyncPolicy::Always`], so whichever is armed
/// surfaces as a [`LogError::Io`] through the [`LogStorage`] `Result` (or
/// fail-stop on a reopen) at the next matching operation — never a silent
/// success (Requirement 7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoFaultKind {
    /// Fail the next fsync of the manifest. Under [`SyncPolicy::Always`] the
    /// next `append`/`commit` forces the manifest, so its durability step
    /// returns [`LogError::Io`].
    Fsync,
    /// Fail the next write to the manifest. The next `append`/`commit`'s
    /// manifest slot write returns [`LogError::Io`].
    Write,
    /// Fail reads of the manifest. The next reopen's recovery read fails-stop,
    /// so [`SimStorageHandle::open`] returns [`LogError::Io`].
    Read,
}

/// The seed-derived storage faults [`SimStorageHandle::arm_seed_derived_faults`]
/// armed for one replica, returned so a run can record (and reproduce) exactly
/// what was selected from the `storage` stream.
///
/// An all-`None` plan means no storage fault was selected for the replica —
/// either because the corresponding intensity was zero or because the seeded
/// draw declined it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StorageFaultPlan {
    /// `Some(bytes)` when a torn-tail fault was armed, dropping `bytes` trailing
    /// bytes from the replica's most recent write (Requirement 7.3).
    pub torn_tail: Option<u64>,
    /// `Some(kind)` when an I/O-error fault was armed on the replica's next
    /// matching storage operation (Requirement 7.4).
    pub io_fault: Option<IoFaultKind>,
}

impl StorageFaultPlan {
    /// Whether any storage fault was armed for the replica.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.torn_tail.is_none() && self.io_fault.is_none()
    }
}

/// Owns one durable replica's backing disk (a shared [`FaultFileSystem`]) plus
/// the WAL configuration for its `(node, group)` data directory, and mints
/// durable [`SimBackend`]s over that disk.
///
/// The *live* WAL handle lives inside the replica's `RaftNode`, not here,
/// because `RaftNode<S>` owns its [`LogStorage`] by value. The handle therefore
/// plays two roles: it is the backing disk (shared, clonable byte store) and the
/// open/reopen factory:
///
/// - [`open`](SimStorageHandle::open) builds a fresh `DurableWal` over the
///   shared disk. It is used for both the initial open and a restart-reopen; the
///   latter runs the WAL's real recovery path, restoring `current_term`, the
///   vote, and the committed prefix from the surviving durable bytes
///   (Requirement 6.3).
/// - [`crash`](SimStorageHandle::crash) drops the un-fsynced tail of every file
///   on the shared disk ([`FaultFileSystem::crash`]). The caller MUST drop the
///   live [`SimBackend`] first, which releases the data-directory lock so a
///   later [`open`](SimStorageHandle::open) can re-lock and recover. Under
///   [`SyncPolicy::Always`] every acknowledged append/commit was fsynced before
///   it returned, so the durable bytes — and thus every Acknowledged_Record and
///   the persisted [`HardState`] — survive the crash (Requirements 7.2, 7.6).
///
/// The handle clones the `FaultFileSystem`, so [`fault_fs`](SimStorageHandle::fault_fs)
/// exposes the shared disk for the seed-derived storage-fault arming a later
/// task wires up.
pub struct SimStorageHandle {
    /// The shared in-memory disk; clones share one backing byte store, so a WAL
    /// opened over a clone sees the bytes a crash leaves behind.
    fault_fs: FaultFileSystem,
    /// The WAL configuration (data directory + `Always` sync) for this group.
    config: WalConfig,
    /// The logical WAL clock handed to each opened `DurableWal`.
    clock: SimWalClock,
}

impl SimStorageHandle {
    /// Build a handle for `(node, group)` over a fresh in-memory disk, using the
    /// deterministic [`data_dir_for`] layout and the consensus-safe
    /// [`SyncPolicy::Always`].
    pub fn new(node: &NodeId, group: &GroupKey) -> Self {
        Self::with_fs(node, group, FaultFileSystem::default())
    }

    /// As [`new`](Self::new) but over an existing (shared) [`FaultFileSystem`],
    /// for callers that pre-arm faults on, or share one disk across, the handle.
    /// Always uses [`SyncPolicy::Always`].
    pub fn with_fs(node: &NodeId, group: &GroupKey, fault_fs: FaultFileSystem) -> Self {
        let config = WalConfig::new(data_dir_for(node, group)).with_sync_policy(SyncPolicy::Always);
        Self {
            fault_fs,
            config,
            clock: SimWalClock::new(),
        }
    }

    /// The shared backing disk, exposed so callers can inspect it or arm
    /// storage faults directly; [`arm_seed_derived_faults`](Self::arm_seed_derived_faults)
    /// is the seed-driven entry point built over it.
    pub fn fault_fs(&self) -> &FaultFileSystem {
        &self.fault_fs
    }

    /// The logical WAL clock this handle hands to every opened `DurableWal`.
    pub fn clock(&self) -> &SimWalClock {
        &self.clock
    }

    /// The WAL configuration (data directory + sync policy) for this group.
    pub fn config(&self) -> &WalConfig {
        &self.config
    }

    /// Open (or reopen) a durable backend over the shared disk.
    ///
    /// Used both for the initial open and for a restart-reopen. Reopening after
    /// a [`crash`](Self::crash) runs the WAL's real open/recovery path over the
    /// surviving durable bytes, recovering the term, vote, and committed prefix.
    /// The caller must have dropped any previously-opened [`SimBackend`] for this
    /// handle first, so the data-directory lock is free to re-acquire.
    pub fn open(&self) -> Result<SimBackend, LogError> {
        let wal = DurableWal::open_with_clock(
            self.config.clone(),
            self.fault_fs.clone(),
            self.clock.clone(),
        )?;
        Ok(SimBackend::Durable(wal))
    }

    /// Open (or reopen) the shared disk as a production
    /// [`vela_core::PartitionLog`] `Sim` backend.
    ///
    /// This is the seam the [`SimulatedCluster`](crate::cluster::SimulatedCluster)
    /// builds the **production** [`PartitionReplica`](vela_core::PartitionReplica)
    /// / [`MetadataController`](vela_core::MetadataController) over: it opens the
    /// real [`DurableWal`] over this handle's deterministic in-memory disk —
    /// exactly as [`open`](Self::open) does for the storage-level
    /// [`SimBackend`] — and wraps it via [`PartitionLog::sim`], so the cluster's
    /// replicas run the same WAL/recovery machinery as a real durable replica
    /// while every byte of I/O stays injectable and reproducible.
    ///
    /// As with [`open`](Self::open), reopening after a [`crash`](Self::crash)
    /// runs the WAL's real recovery path over the surviving durable bytes; the
    /// caller must have dropped any previously-opened backend for this handle
    /// first so the data-directory lock is free to re-acquire.
    pub fn open_partition_log(&self) -> Result<PartitionLog, LogError> {
        let wal = DurableWal::open_with_clock(
            self.config.clone(),
            self.fault_fs.clone(),
            self.clock.clone(),
        )?;
        Ok(PartitionLog::sim(wal))
    }

    /// Model a Node_Crash on this disk: drop the un-fsynced tail of every file,
    /// retaining exactly the bytes forced to stable storage (Requirement 7.2).
    ///
    /// The caller MUST drop the live [`SimBackend`] before calling this (and
    /// before reopening) so the data-directory lock is released. Under
    /// [`SyncPolicy::Always`] nothing acknowledged is ever un-fsynced, so this
    /// preserves every Acknowledged_Record and the persisted [`HardState`]
    /// (Requirement 7.6); it only discards writes that had not yet been forced.
    pub fn crash(&self) {
        self.fault_fs.crash();
    }

    /// The WAL manifest path within this replica's Data_Directory — the target
    /// every path-scoped I/O-error fault is armed against.
    fn manifest_path(&self) -> PathBuf {
        self.config.data_dir.join(WAL_MANIFEST_FILE_NAME)
    }

    /// Arm a torn-tail storage fault: drop the final `bytes_dropped` bytes of
    /// this replica's most recent write (Requirement 7.3).
    ///
    /// Models a trailing write that did not fully reach stable storage. On the
    /// next reopen the WAL's own torn-vs-interior classification discards the
    /// torn tail down to the last intact record — this method only arms the
    /// condition on the backing disk; it does not reimplement recovery. Tearing
    /// is a no-op when there is no recorded last write (e.g. immediately after a
    /// [`crash`](Self::crash), which clears the marker).
    pub fn arm_torn_tail(&self, bytes_dropped: u64) {
        self.fault_fs.tear_last_write(bytes_dropped);
    }

    /// Arm an I/O-error storage fault of `kind` on this replica's manifest, so
    /// the next matching storage operation surfaces a [`LogError::Io`] through
    /// the [`LogStorage`] `Result` (or fail-stop on a reopen) rather than
    /// succeeding silently (Requirement 7.4).
    pub fn arm_io_error(&self, kind: IoFaultKind) {
        let manifest = self.manifest_path();
        match kind {
            IoFaultKind::Fsync => self.fault_fs.arm_fsync_failure_for(&manifest),
            IoFaultKind::Write => self.fault_fs.arm_write_failure_for(&manifest),
            IoFaultKind::Read => self.fault_fs.arm_read_failure_for(&manifest),
        }
    }

    /// Select and arm this replica's storage faults from the run's `storage` RNG
    /// stream and the configured [`FaultIntensities`], returning the
    /// [`StorageFaultPlan`] that was armed (Requirement 7.5).
    ///
    /// The selection is a deterministic function of the `storage` stream alone:
    /// the caller advances one shared stream across every replica, so *which*
    /// nodes/operations a Storage_Fault hits is fixed by the run seed. Draw order
    /// is fixed (torn-tail, then I/O error, then the I/O-error kind) and a class
    /// is consulted **only** when its intensity is non-zero, so a run that
    /// disables a fault class draws no randomness for it and leaves every
    /// downstream decision stable across configurations — mirroring the
    /// `Sim_Network` bus convention.
    ///
    /// - A torn-tail is armed with probability `faults.torn_write_prob`,
    ///   tearing `1..=`[`MAX_TORN_BYTES`] trailing bytes of the most recent write
    ///   (Requirement 7.3).
    /// - An I/O error is armed with probability `faults.io_error_prob`; its
    ///   [`IoFaultKind`] (fsync / write / read) is itself drawn from the stream
    ///   (Requirement 7.4).
    pub fn arm_seed_derived_faults(
        &self,
        storage: &mut SplitMix64,
        faults: &FaultIntensities,
    ) -> StorageFaultPlan {
        let mut plan = StorageFaultPlan::default();

        if faults.torn_write_prob > 0.0 && storage.next_f64() < faults.torn_write_prob {
            // `next_below(MAX_TORN_BYTES)` is `0..MAX_TORN_BYTES`; +1 yields
            // `1..=MAX_TORN_BYTES` so an armed torn-tail always drops at least
            // one byte and so is never a no-op.
            let bytes = storage.next_below(MAX_TORN_BYTES) + 1;
            self.arm_torn_tail(bytes);
            plan.torn_tail = Some(bytes);
        }

        if faults.io_error_prob > 0.0 && storage.next_f64() < faults.io_error_prob {
            let kind = match storage.next_below(3) {
                0 => IoFaultKind::Fsync,
                1 => IoFaultKind::Write,
                _ => IoFaultKind::Read,
            };
            self.arm_io_error(kind);
            plan.io_fault = Some(kind);
        }

        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::SeedStreams;
    use vela_core::PartitionIndex;
    use vela_log::PayloadKind;

    fn group(topic: &str, partition: u32) -> GroupKey {
        (topic.to_string(), PartitionIndex(partition))
    }

    fn record(byte: u8) -> EntryPayload {
        EntryPayload::new(PayloadKind::Record, vec![byte])
    }

    #[test]
    fn data_dir_is_deterministic_per_node_and_group() {
        let node = NodeId::new("node-a");
        let g = group("orders", 2);
        // Stable, pure function of (node, topic, partition).
        assert_eq!(data_dir_for(&node, &g), data_dir_for(&node, &g));
        assert_eq!(
            data_dir_for(&node, &g),
            PathBuf::from("/sim/node-a/orders/2")
        );
        // Distinct groups and nodes map to distinct directories.
        assert_ne!(
            data_dir_for(&node, &g),
            data_dir_for(&node, &group("orders", 3))
        );
        assert_ne!(
            data_dir_for(&node, &g),
            data_dir_for(&NodeId::new("node-b"), &g)
        );
    }

    #[test]
    fn in_memory_backend_round_trips_like_a_log() {
        let mut backend = SimBackend::in_memory();
        assert_eq!(backend.append(record(0), 1).unwrap(), 0);
        assert_eq!(backend.append(record(1), 1).unwrap(), 1);
        backend.commit(1).unwrap();
        assert_eq!(backend.last_index(), Some(1));
        assert_eq!(backend.commit_index(), Some(1));
        assert_eq!(backend.entry(1).unwrap().payload, record(1));
    }

    #[test]
    fn durable_backend_appends_commits_and_recovers_on_reopen() {
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));

        // Open, append + commit two records, then drop the live backend so the
        // data-directory lock is released for the reopen.
        {
            let mut backend = handle.open().unwrap();
            assert_eq!(backend.append(record(10), 1).unwrap(), 0);
            assert_eq!(backend.append(record(11), 1).unwrap(), 1);
            backend.commit(1).unwrap();
            backend
                .persist_hard_state(HardState {
                    current_term: 7,
                    voted_for: Some(3),
                })
                .unwrap();
            assert_eq!(backend.last_index(), Some(1));
        }

        // Reopen over the same disk: the WAL's real recovery path restores the
        // committed prefix and the persisted hard state.
        let recovered = handle.open().unwrap();
        assert_eq!(recovered.last_index(), Some(1));
        assert_eq!(recovered.commit_index(), Some(1));
        assert_eq!(recovered.entry(0).unwrap().payload, record(10));
        assert_eq!(recovered.entry(1).unwrap().payload, record(11));
        assert_eq!(
            recovered.hard_state(),
            Some(HardState {
                current_term: 7,
                voted_for: Some(3),
            })
        );
    }

    #[test]
    fn crash_preserves_fsynced_records_under_always() {
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));

        // Under SyncPolicy::Always every acknowledged append/commit is fsynced,
        // so a crash (drop the un-fsynced tail) discards nothing acknowledged.
        {
            let mut backend = handle.open().unwrap();
            backend.append(record(20), 2).unwrap();
            backend.append(record(21), 2).unwrap();
            backend.commit(1).unwrap();
        }

        handle.crash();

        let recovered = handle.open().unwrap();
        assert_eq!(recovered.last_index(), Some(1));
        assert_eq!(recovered.commit_index(), Some(1));
        assert_eq!(recovered.entry(0).unwrap().payload, record(20));
        assert_eq!(recovered.entry(1).unwrap().payload, record(21));
    }

    /// Intensities with torn-tail and I/O-error probabilities forced on, so the
    /// seed-derived selection always arms a fault. Other fields stay healthy.
    fn storage_faults(torn: f64, io: f64) -> FaultIntensities {
        FaultIntensities {
            torn_write_prob: torn,
            io_error_prob: io,
            ..FaultIntensities::default()
        }
    }

    #[test]
    fn seed_derived_selection_is_reproducible_for_a_seed() {
        // Requirement 7.5: which fault is selected is a pure function of the
        // `storage` stream, so the same seed reproduces the same plan.
        let faults = storage_faults(0.5, 0.5);

        let plan_a = {
            let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
            let mut rng = SeedStreams::new(0xABCD_1234).storage;
            handle.arm_seed_derived_faults(&mut rng, &faults)
        };
        let plan_b = {
            let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
            let mut rng = SeedStreams::new(0xABCD_1234).storage;
            handle.arm_seed_derived_faults(&mut rng, &faults)
        };

        assert_eq!(plan_a, plan_b);
    }

    #[test]
    fn healthy_intensities_arm_no_faults_and_draw_no_randomness() {
        // With both storage probabilities at zero, nothing is armed and the
        // stream is left untouched, so disabling storage faults cannot perturb
        // any other seed-derived decision.
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
        let faults = FaultIntensities::default();

        let mut rng = SeedStreams::new(7).storage;
        let mut witness = SeedStreams::new(7).storage;

        let plan = handle.arm_seed_derived_faults(&mut rng, &faults);

        assert!(plan.is_empty());
        assert_eq!(
            rng.next_u64(),
            witness.next_u64(),
            "no randomness must be drawn when both storage intensities are zero"
        );
    }

    #[test]
    fn certain_intensities_arm_both_fault_classes() {
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
        let faults = storage_faults(1.0, 1.0);

        let mut rng = SeedStreams::new(99).storage;
        let plan = handle.arm_seed_derived_faults(&mut rng, &faults);

        let bytes = plan.torn_tail.expect("torn-tail armed at probability 1.0");
        assert!(
            (1..=MAX_TORN_BYTES).contains(&bytes),
            "torn-tail byte count {bytes} must be in 1..={MAX_TORN_BYTES}"
        );
        assert!(
            plan.io_fault.is_some(),
            "an I/O error must be armed at probability 1.0"
        );
    }

    #[test]
    fn armed_write_io_error_surfaces_through_result() {
        // Requirement 7.4: an armed write failure surfaces as LogError::Io
        // through the LogStorage Result rather than succeeding silently.
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
        let mut backend = handle.open().unwrap();

        handle.arm_io_error(IoFaultKind::Write);

        // Under Always the append forces the manifest write, which fails.
        let err = backend.append(record(1), 1).unwrap_err();
        assert!(matches!(err, LogError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn armed_fsync_io_error_surfaces_through_result() {
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
        let mut backend = handle.open().unwrap();

        handle.arm_io_error(IoFaultKind::Fsync);

        let err = backend.append(record(1), 1).unwrap_err();
        assert!(matches!(err, LogError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn armed_read_io_error_fails_stop_on_reopen() {
        // Requirement 7.4: an armed read failure fails-stop on the recovery
        // read path, surfacing through the open() Result.
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
        {
            let mut backend = handle.open().unwrap();
            backend.append(record(1), 1).unwrap();
            backend.commit(0).unwrap();
        }

        handle.arm_io_error(IoFaultKind::Read);

        match handle.open() {
            Ok(_) => panic!("reopen must fail when manifest reads fail"),
            Err(err) => assert!(matches!(err, LogError::Io { .. }), "got {err:?}"),
        }
    }

    #[test]
    fn armed_torn_tail_discards_the_unacknowledged_trailing_write() {
        // Requirement 7.3: tearing the trailing write makes recovery discard it
        // as a torn tail rather than fabricating data or panicking. A single
        // un-committed append's manifest acknowledgement is torn away, so the
        // record is dropped and the reopen recovers an empty log.
        let handle = SimStorageHandle::new(&NodeId::new("node-a"), &group("orders", 0));
        {
            let mut backend = handle.open().unwrap();
            backend.append(record(1), 1).unwrap();
        }

        // Drop well past the trailing write so its acknowledgement cannot decode.
        handle.arm_torn_tail(256);

        let recovered = handle
            .open()
            .expect("reopen after a torn tail must recover, not error");
        assert_eq!(
            recovered.last_index(),
            None,
            "the unacknowledged torn-tail record must be discarded"
        );
    }
}
