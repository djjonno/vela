//! The single concrete [`LogStorage`] type injected into a partition replica.
//!
//! Vela makes log durability a per-topic property, but consensus
//! ([`vela_raft::RaftNode`]) takes its log by injection over the
//! [`LogStorage`] seam and stays generic over a single concrete type. The
//! backend set is closed — exactly two backends, [`Durable`](PartitionLog::Durable)
//! and [`InMemory`](PartitionLog::InMemory) — so a two-variant `enum` models it
//! exactly while keeping `RaftNode<PartitionLog>` a concrete, monomorphised type
//! with zero-cost (non-virtual) calls, unlike a `Box<dyn LogStorage>`.
//!
//! [`PartitionLog`] implements [`LogStorage`] by dispatching every trait
//! operation to whichever backend it holds and returning that backend's result
//! unchanged (Requirement 4). The [`InMemory`](PartitionLog::InMemory) variant
//! therefore behaves observably identically to a bare [`InMemoryLog`] for any
//! operation sequence (Requirement 4.4, 13.1); the
//! [`Durable`](PartitionLog::Durable) variant carries a [`DurableWal`] over the
//! real filesystem and clock (`DurableWal<RealFileSystem, RealClock>`).

use vela_log::{
    CommitIndex, DurableWal, EntryPayload, HardState, InMemoryLog, LogEntry, LogError, LogStorage,
    Snapshot,
};

#[cfg(feature = "sim")]
use vela_log::sim::FaultFileSystem;

use crate::model::LogBackend;

#[cfg(feature = "sim")]
pub use sim_support::SimWalClock;

/// Deterministic-simulation support for [`PartitionLog`], gated behind the
/// non-default `sim` feature so production builds never see it.
///
/// Defines the one logical WAL [`Clock`](vela_log::sim::Clock) the harness uses
/// for *every* simulated replica — including the `PartitionLog::Sim` backend and
/// `vela-sim`'s own `SimBackend`, which re-exports this type so a single clock
/// type flows through both. Reading logical time only (never the wall clock)
/// keeps a run a pure function of its seed.
#[cfg(feature = "sim")]
mod sim_support {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use vela_log::sim::Clock as WalClock;

    /// The logical WAL [`Clock`](vela_log::sim::Clock) for the simulation
    /// harness, reading logical milliseconds only — never the wall clock.
    ///
    /// A [`DurableWal`](vela_log::DurableWal) consults its clock solely to pace
    /// the `Periodic` sync policy; the harness always uses
    /// [`SyncPolicy::Always`](vela_log::SyncPolicy), under which the clock value
    /// can never affect an Outcome. `SimWalClock` nonetheless honours the
    /// determinism constraint structurally: it returns a reading from a shared
    /// [`Arc<AtomicU64>`] that only the harness advances and never touches
    /// `std::time`. A fresh clock reads `0`.
    ///
    /// `SimWalClock` is `Send + Sync` because the reading lives behind an
    /// `Arc<AtomicU64>` (rather than an `Rc<Cell<_>>`). This matters beyond the
    /// harness: `SimWalClock` is a field of `PartitionLog::Sim`, and `Send` is
    /// an auto-trait computed over *every* enum variant, so a `!Send` clock
    /// would make the whole [`PartitionLog`](super::PartitionLog) `!Send` under
    /// `--all-features` and break the production `vela-server`, which stores
    /// partition logs behind a `Sync` shared state. The harness is
    /// single-threaded, so reading and writing with [`Ordering::Relaxed`] is
    /// behaviourally identical to the old non-atomic cell and stays
    /// deterministic — the atomics exist purely to make the type thread-safe.
    ///
    /// The reading lives behind a shared `Arc<AtomicU64>` so a clone handed to a
    /// `DurableWal` observes the same logical time another clone is set to: a
    /// clone shares the same `Arc`, exactly as the old `Rc<Cell>` clone shared
    /// its cell.
    #[derive(Debug)]
    pub struct SimWalClock {
        /// Shared logical time in milliseconds; only the harness advances it.
        now: Arc<AtomicU64>,
    }

    impl Clone for SimWalClock {
        /// Clone the handle, sharing the same underlying `Arc<AtomicU64>` so the
        /// clone observes — and can set — the same logical time as the original.
        fn clone(&self) -> Self {
            Self {
                now: Arc::clone(&self.now),
            }
        }
    }

    impl Default for SimWalClock {
        /// A fresh clock reading logical `0` milliseconds.
        fn default() -> Self {
            Self {
                now: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl SimWalClock {
        /// Construct a clock reading logical `0` milliseconds.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Set the logical millisecond reading the WAL would observe.
        ///
        /// Only the harness calls this, when wiring the WAL clock to the
        /// Virtual_Clock; under `SyncPolicy::Always` the value is never read for
        /// timing, so this exists purely to keep the seam logical-time-only.
        pub fn set_logical_millis(&self, millis: u64) {
            self.now.store(millis, Ordering::Relaxed);
        }
    }

    impl WalClock for SimWalClock {
        fn now_millis(&self) -> u64 {
            self.now.load(Ordering::Relaxed)
        }
    }
}

/// Which [`PartitionLog`] variant the server should construct for a partition
/// replica, named independently of a live backend instance.
///
/// Constructing a real [`PartitionLog::Durable`] needs a path and configuration
/// the server owns (where the segments live, the sync policy), so the
/// spawn-selection helper [`PartitionLog::select`] does not build a log; it maps
/// a topic's recorded [`LogBackend`] onto this discriminant, and the server then
/// constructs the matching variant for every replica of the topic. Separating
/// the *decision* (this enum) from the *construction* (server concern) keeps the
/// selection a pure, total, testable function (Requirement 3.4, 5.1, 5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionLogKind {
    /// Construct the durable backend ([`PartitionLog::Durable`]).
    Durable,
    /// Construct the in-memory backend ([`PartitionLog::InMemory`]).
    InMemory,
}

/// The one concrete [`LogStorage`] injected into a partition replica. Holds
/// exactly one backend and dispatches every trait operation to it, returning
/// the backend's result unchanged (Requirement 4.1, 4.2, 4.3).
///
/// The variants differ substantially in size (a [`DurableWal`] carries its
/// configuration, open segments, and index state), but a `PartitionLog` is a
/// single long-lived value per partition replica, so the unused capacity of an
/// [`InMemory`](PartitionLog::InMemory) value is immaterial. Keeping the backend
/// inline (rather than boxing) preserves the closed two-variant model and the
/// zero-cost, non-virtual dispatch that motivates an `enum` over a trait object.
#[allow(clippy::large_enum_variant)]
pub enum PartitionLog {
    /// The durable Write-Ahead-Log backend (`DurableWal<RealFileSystem, RealClock>`).
    Durable(DurableWal),
    /// The volatile in-memory backend.
    InMemory(InMemoryLog),
    /// The deterministic-simulation backend: the real [`DurableWal`] running
    /// over `vela-log`'s in-memory fault filesystem and the logical
    /// [`SimWalClock`], gated behind the non-default `sim` feature.
    ///
    /// This is how the `vela-sim` harness drives the **production**
    /// [`PartitionReplica`](crate::PartitionReplica) /
    /// [`MetadataController`](crate::MetadataController) over a deterministic
    /// disk: the WAL's framing, manifest, recovery, torn-tail classification,
    /// and `Always` sync run unchanged, so a `RaftNode<PartitionLog>` behaves
    /// observably like one over the real durable backend while every byte of
    /// I/O is injectable and reproducible. Absent from production builds.
    #[cfg(feature = "sim")]
    Sim(DurableWal<FaultFileSystem, SimWalClock>),
}

/// Dispatch a [`LogStorage`] method to whichever backend the [`PartitionLog`]
/// holds, forwarding the arguments and returning the backend's result unchanged.
macro_rules! dispatch {
    ($self:ident, $method:ident $(, $arg:expr)*) => {
        match $self {
            PartitionLog::Durable(backend) => backend.$method($($arg),*),
            PartitionLog::InMemory(backend) => backend.$method($($arg),*),
            #[cfg(feature = "sim")]
            PartitionLog::Sim(backend) => backend.$method($($arg),*),
        }
    };
}

impl PartitionLog {
    /// The spawn-selection helper: map a topic's recorded [`LogBackend`] to the
    /// [`PartitionLogKind`] the server should construct for every replica of
    /// that topic (Requirement 3.4, 5.1, 5.4).
    ///
    /// A topic recorded [`LogBackend::Durable`] selects
    /// [`PartitionLogKind::Durable`] and a topic recorded
    /// [`LogBackend::InMemory`] selects [`PartitionLogKind::InMemory`]. The
    /// mapping is total and pure — it builds no log, since constructing the
    /// durable variant needs a path and sync policy owned by the server — so the
    /// selection decision can be tested in isolation from filesystem I/O.
    pub fn select(backend: LogBackend) -> PartitionLogKind {
        match backend {
            LogBackend::Durable => PartitionLogKind::Durable,
            LogBackend::InMemory => PartitionLogKind::InMemory,
        }
    }

    /// Wrap an already-opened simulation [`DurableWal`] (over the fault
    /// filesystem and the logical [`SimWalClock`]) as a [`PartitionLog::Sim`].
    ///
    /// Gated behind the non-default `sim` feature. This is the seam the
    /// `vela-sim` harness injects through: it opens the WAL over its
    /// deterministic in-memory disk and hands the result here, so the
    /// production replica is built over the exact same WAL machinery as a real
    /// durable replica while every byte of I/O stays injectable.
    #[cfg(feature = "sim")]
    #[must_use]
    pub fn sim(wal: DurableWal<FaultFileSystem, SimWalClock>) -> Self {
        PartitionLog::Sim(wal)
    }
}

impl LogStorage for PartitionLog {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_maps_durable_backend_to_durable_kind() {
        assert_eq!(
            PartitionLog::select(LogBackend::Durable),
            PartitionLogKind::Durable
        );
    }

    #[test]
    fn select_maps_in_memory_backend_to_in_memory_kind() {
        assert_eq!(
            PartitionLog::select(LogBackend::InMemory),
            PartitionLogKind::InMemory
        );
    }

    #[test]
    fn select_is_total_over_the_default_backend() {
        // The default backend is Durable (Requirement 1.2), so the helper picks
        // the durable variant for a topic created without an explicit backend.
        assert_eq!(
            PartitionLog::select(LogBackend::default()),
            PartitionLogKind::Durable
        );
    }
}
