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

use crate::model::LogBackend;

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
}

/// Dispatch a [`LogStorage`] method to whichever backend the [`PartitionLog`]
/// holds, forwarding the arguments and returning the backend's result unchanged.
macro_rules! dispatch {
    ($self:ident, $method:ident $(, $arg:expr)*) => {
        match $self {
            PartitionLog::Durable(backend) => backend.$method($($arg),*),
            PartitionLog::InMemory(backend) => backend.$method($($arg),*),
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
