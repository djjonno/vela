//! `vela-log` — the append-only, ordered log for a single partition.
//!
//! This crate is innermost in the dependency graph: it knows nothing about
//! topics, gRPC, Raft, or the server. The log lives behind the [`LogStorage`]
//! trait so an in-memory implementation can be swapped for a durable one later
//! without touching consensus.
//!
//! Indices are **0-based**. The first entry appended to an empty log is stored
//! at index 0, and each subsequent entry is stored at `highest_index + 1`
//! (Requirements 6.3, 6.4). The commit position is an [`Option<u64>`]
//! ([`CommitIndex`]) where `None` represents the uncommitted state preceding
//! index 0 (Requirement 6.7).

use thiserror::Error;

mod wal;

pub use wal::{DurableWal, RealClock, RealFileSystem, SyncPolicy, WalConfig};

/// The kind of payload carried by a [`LogEntry`].
///
/// `vela-log` stays free of domain types: a payload is opaque bytes plus this
/// small tag describing how a higher layer (`vela-core`) should decode them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PayloadKind {
    /// A produced event record.
    Record,
    /// A cluster-metadata command (topic create/delete, availability changes).
    Cluster,
    /// A leader's no-op entry, written on election (extended Raft paper §8).
    Noop,
}

/// The opaque payload of a [`LogEntry`]: a [`PayloadKind`] tag plus raw bytes.
///
/// Keeping the payload opaque is what lets `vela-log` avoid depending on any
/// other Vela crate; encoding and decoding of the bytes happens in `vela-core`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryPayload {
    /// Tag describing how `bytes` should be interpreted by higher layers.
    pub kind: PayloadKind,
    /// Opaque, already-encoded payload bytes.
    pub bytes: Vec<u8>,
}

impl EntryPayload {
    /// Construct a payload from a tag and raw bytes.
    pub fn new(kind: PayloadKind, bytes: Vec<u8>) -> Self {
        Self { kind, bytes }
    }
}

/// A single element of a partition log.
///
/// Carries its own 0-based `index` and the Raft `term` in which it was created,
/// alongside the opaque [`EntryPayload`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// 0-based position of this entry within the log.
    pub index: u64,
    /// Raft term in which this entry was created.
    pub term: u64,
    /// Opaque payload (record, cluster command, or no-op).
    pub payload: EntryPayload,
}

/// Commit position of a log.
///
/// `None` means "uncommitted state preceding index 0" — i.e. nothing has been
/// committed yet (Requirement 6.7). `Some(i)` means entries `0..=i` are
/// committed.
pub type CommitIndex = Option<u64>;

/// A representation of the committed log state up to the commit index
/// (Requirement 6.12).
///
/// Contains the commit position and the committed prefix of entries. On a log
/// with no commit, `commit_index` is `None` and `entries` is empty
/// (Requirement 6.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The commit index this snapshot reflects.
    pub commit_index: CommitIndex,
    /// The committed entries, indices `0..=commit_index`, in ascending order.
    pub entries: Vec<LogEntry>,
}

/// A Raft replica's persistent hard state: the consensus state that must
/// survive a restart for safety.
///
/// Raft requires that `current_term` and `voted_for` be durable: a node that
/// forgets its vote or term after a restart can grant a second vote in a term
/// or regress its term, violating the at-most-one-vote-per-term and
/// monotonic-term guarantees. For a durable [`LogStorage`] this state is
/// persisted and restored via [`persist_hard_state`](LogStorage::persist_hard_state)
/// and [`hard_state`](LogStorage::hard_state); for a volatile log such as
/// [`InMemoryLog`] it stays in memory only, consistent with the backend.
///
/// `voted_for` is `None` when the replica has not voted in `current_term`. The
/// [`Default`] is term `0` with no vote, matching a fresh log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    /// The replica's current Raft term.
    pub current_term: u64,
    /// The candidate this replica voted for in `current_term`, or `None` if it
    /// has not voted in that term.
    pub voted_for: Option<u64>,
}

/// Errors returned by [`LogStorage`] operations.
///
/// Does not derive `PartialEq`/`Eq`: the [`Io`](LogError::Io) variant carries a
/// [`std::io::Error`], which is not `PartialEq`. Tests match on variants (e.g.
/// with `matches!`) rather than comparing error values for equality.
#[derive(Debug, Error)]
pub enum LogError {
    /// A commit was requested with an index below the current commit index or
    /// above the highest stored index (Requirement 6.9).
    #[error(
        "commit index {requested} out of bounds (commit_index={current:?}, last_index={last:?})"
    )]
    CommitOutOfBounds {
        /// The index that was requested for commit.
        requested: u64,
        /// The commit index at the time of the request.
        current: CommitIndex,
        /// The highest stored index at the time of the request.
        last: Option<u64>,
    },

    /// A revert was requested with an index below the current commit index,
    /// which would discard committed entries (Requirement 6.11).
    #[error("cannot revert to index {requested} below commit index {commit:?}")]
    RevertBelowCommit {
        /// The index that was requested for revert.
        requested: u64,
        /// The commit index at the time of the request.
        commit: CommitIndex,
    },

    /// `append_entries` was given entries that do not form a valid, contiguous
    /// continuation of the log.
    #[error("append_entries received non-contiguous or out-of-order entries")]
    NonContiguousEntries,

    /// An I/O operation against the durable log's backing storage failed. Used
    /// only by the durable `LogStorage` implementation (Requirement 10.1).
    /// `op` names the in-progress operation for diagnosis (Requirement 10.2).
    #[error("durable log I/O failure during {op}: {source}")]
    Io {
        /// Name of the operation that was in progress when the I/O failed.
        op: &'static str,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// A persisted record frame failed its integrity check during a read or
    /// recovery (Requirement 10.1). `index` is the absolute index expected at
    /// the corrupt position, not the corrupt frame's own index field.
    #[error("log corruption at index {index}: {detail}")]
    Corruption {
        /// The absolute index expected at the position where corruption was
        /// detected.
        index: u64,
        /// A short, static description of the corruption.
        detail: &'static str,
    },

    /// Compaction was requested with a retained point that would discard an
    /// uncommitted entry or the entry at the commit index (Requirement 10.1).
    #[error(
        "compaction retained point {requested} out of bounds \
         (commit_index={commit:?}, log_start={log_start})"
    )]
    CompactionOutOfBounds {
        /// The retained point that was requested.
        requested: u64,
        /// The commit index at the time of the request.
        commit: CommitIndex,
        /// The log start index at the time of the request.
        log_start: u64,
    },

    /// The durable log was opened with an invalid configuration
    /// (Requirement 10.1). Kept distinct from [`Io`](LogError::Io) so that
    /// configuration validation is not conflated with real I/O failures.
    #[error("invalid WAL configuration: {detail}")]
    Config {
        /// A description of what was invalid about the configuration.
        detail: String,
    },
}

/// The storage seam behind which a partition's append-only log lives.
///
/// Consensus depends on this trait rather than a concrete implementation
/// (Requirement 6.1), so the in-memory log used in this milestone can be
/// replaced by a durable implementation later without touching `vela-raft`.
///
/// All indices are 0-based. See the module docs for index and commit
/// conventions.
pub trait LogStorage {
    /// Append one entry at `highest_index + 1` (or 0 if the log is empty) and
    /// return the assigned index (Requirements 6.3, 6.4).
    fn append(&mut self, payload: EntryPayload, term: u64) -> Result<u64, LogError>;

    /// Append entries that already carry their `index` and `term`, used by
    /// replication and by revert-then-append reconciliation.
    fn append_entries(&mut self, entries: &[LogEntry]) -> Result<(), LogError>;

    /// Inclusive range read in ascending index order.
    ///
    /// Returns the stored entries whose indices fall within `start..=end`,
    /// omitting any absent index; returns empty (not an error) when
    /// `start > end` (Requirements 6.5, 6.6).
    fn read(&self, start: u64, end: u64) -> Vec<LogEntry>;

    /// Single-entry lookup; `None` if no entry is stored at `index`.
    fn entry(&self, index: u64) -> Option<LogEntry>;

    /// The highest stored index, or `None` if the log is empty.
    fn last_index(&self) -> Option<u64>;

    /// The term of the entry at `index`, or `None` if absent.
    fn term_at(&self, index: u64) -> Option<u64>;

    /// The current commit index (Requirement 6.7).
    fn commit_index(&self) -> CommitIndex;

    /// Advance the commit index to `index` when
    /// `current_commit <= index <= last_index`; otherwise reject and leave
    /// state unchanged (Requirements 6.8, 6.9).
    fn commit(&mut self, index: u64) -> Result<(), LogError>;

    /// Remove all entries with index greater than `index`.
    ///
    /// Rejected when `index < commit_index`, protecting committed entries
    /// (Requirements 6.10, 6.11).
    fn revert(&mut self, index: u64) -> Result<(), LogError>;

    /// Produce a [`Snapshot`] of the committed log state up to the commit
    /// index (Requirement 6.12).
    fn snapshot(&self) -> Snapshot;

    /// Force all buffered record frames to stable storage (Requirement 4.8).
    ///
    /// Additive seam for a future group-commit-before-acknowledge path. The
    /// default is a successful no-op, which is correct for non-durable
    /// implementations such as [`InMemoryLog`] that never buffer writes
    /// (Requirement 4.9); the durable implementation overrides it to fsync and
    /// returns [`LogError::Io`] if the force fails.
    fn flush(&mut self) -> Result<(), LogError> {
        Ok(())
    }

    /// Persist the Raft [`HardState`] (`current_term`, `voted_for`) to stable
    /// storage (Requirements 9.3, 10.1, 10.2).
    ///
    /// Additive seam mirroring [`flush`](LogStorage::flush): the default is a
    /// successful no-op, which is correct for volatile implementations such as
    /// [`InMemoryLog`] whose hard state stays in memory only. A durable
    /// implementation overrides it to write the state durably before returning
    /// and returns [`LogError::Io`] if the persist fails.
    fn persist_hard_state(&mut self, _hard_state: HardState) -> Result<(), LogError> {
        Ok(())
    }

    /// Return the persisted Raft [`HardState`] recovered at open, or `None`
    /// when this log does not persist hard state (Requirements 10.1, 10.2).
    ///
    /// Additive seam mirroring [`flush`](LogStorage::flush): the default is
    /// `None`, correct for volatile implementations such as [`InMemoryLog`]
    /// that keep no durable hard state. A durable implementation overrides it
    /// to return the recovered `(current_term, voted_for)`; a fresh durable log
    /// reports [`HardState::default`].
    fn hard_state(&self) -> Option<HardState> {
        None
    }
}

/// In-memory implementation of [`LogStorage`] for this milestone
/// (Requirement 6.2).
///
/// Entries are stored densely in a [`Vec`] whose position is the entry's
/// 0-based index: the invariant `entries[i].index == i as u64` holds at all
/// times. Because the log is append-only and contiguous, [`last_index`] is
/// simply `len - 1` and there are never gaps in the stored range.
///
/// [`last_index`]: LogStorage::last_index
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InMemoryLog {
    /// Dense, contiguous entries; `entries[i].index == i`.
    entries: Vec<LogEntry>,
    /// Commit position; `None` before any commit (Requirement 6.7).
    commit_index: CommitIndex,
}

impl InMemoryLog {
    /// Create an empty log with no committed entries (Requirement 6.7).
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            commit_index: None,
        }
    }
}

impl LogStorage for InMemoryLog {
    fn append(&mut self, payload: EntryPayload, term: u64) -> Result<u64, LogError> {
        // Next index is the current length: 0 on an empty log, otherwise
        // exactly `highest_index + 1` (Requirements 6.3, 6.4).
        let index = self.entries.len() as u64;
        self.entries.push(LogEntry {
            index,
            term,
            payload,
        });
        Ok(index)
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> Result<(), LogError> {
        if entries.is_empty() {
            return Ok(());
        }

        // The incoming batch must itself be contiguous and ascending.
        for pair in entries.windows(2) {
            if pair[1].index != pair[0].index + 1 {
                return Err(LogError::NonContiguousEntries);
            }
        }

        let start = entries[0].index;
        let len = self.entries.len() as u64;

        // The batch must connect to the existing log without leaving a gap:
        // it may extend at `len` or overwrite an uncommitted suffix, but it
        // may not start beyond the end of the log.
        if start > len {
            return Err(LogError::NonContiguousEntries);
        }

        // Overwriting a committed entry would violate log safety; reject it.
        if let Some(committed) = self.commit_index {
            if start <= committed {
                return Err(LogError::NonContiguousEntries);
            }
        }

        // Drop any conflicting uncommitted suffix, then append the batch. This
        // keeps the dense `entries[i].index == i` invariant intact.
        self.entries.truncate(start as usize);
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn read(&self, start: u64, end: u64) -> Vec<LogEntry> {
        // A start past the end is not an error: it yields no entries (R6.6).
        if start > end {
            return Vec::new();
        }
        let len = self.entries.len() as u64;
        if start >= len {
            return Vec::new();
        }
        // Clamp the inclusive upper bound to the highest stored index. Because
        // storage is contiguous, the resulting slice already omits any index
        // outside the stored range (Requirement 6.5).
        let hi = end.min(len - 1);
        self.entries[start as usize..=hi as usize].to_vec()
    }

    fn entry(&self, index: u64) -> Option<LogEntry> {
        self.entries.get(index as usize).cloned()
    }

    fn last_index(&self) -> Option<u64> {
        let len = self.entries.len() as u64;
        if len == 0 {
            None
        } else {
            Some(len - 1)
        }
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        self.entries.get(index as usize).map(|entry| entry.term)
    }

    fn commit_index(&self) -> CommitIndex {
        self.commit_index
    }

    fn commit(&mut self, index: u64) -> Result<(), LogError> {
        let last = self.last_index();

        // Reject indices above the highest stored entry (or any commit on an
        // empty log) and indices below the current commit (Requirement 6.9).
        let above_last = match last {
            None => true,
            Some(highest) => index > highest,
        };
        let below_current = matches!(self.commit_index, Some(current) if index < current);

        if above_last || below_current {
            return Err(LogError::CommitOutOfBounds {
                requested: index,
                current: self.commit_index,
                last,
            });
        }

        // Advance (monotonically; equal index is an idempotent no-op) (R6.8).
        self.commit_index = Some(index);
        Ok(())
    }

    fn revert(&mut self, index: u64) -> Result<(), LogError> {
        // Reverting below the commit index would discard committed entries
        // (Requirement 6.11).
        if matches!(self.commit_index, Some(committed) if index < committed) {
            return Err(LogError::RevertBelowCommit {
                requested: index,
                commit: self.commit_index,
            });
        }

        // Remove every entry with an index greater than `index`, keeping
        // `0..=index` (Requirement 6.10). `truncate` is a no-op when the log is
        // already shorter, so a high `index` simply removes nothing.
        let keep = (index as usize).saturating_add(1);
        self.entries.truncate(keep);
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        // The snapshot is exactly the committed prefix; empty when nothing has
        // been committed (Requirements 6.7, 6.12).
        let entries = match self.commit_index {
            None => Vec::new(),
            Some(committed) => self.entries[..=(committed as usize)].to_vec(),
        };
        Snapshot {
            commit_index: self.commit_index,
            entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(byte: u8) -> EntryPayload {
        EntryPayload::new(PayloadKind::Record, vec![byte])
    }

    #[test]
    fn new_log_is_empty_and_uncommitted() {
        let log = InMemoryLog::new();
        assert_eq!(log.last_index(), None);
        assert_eq!(log.commit_index(), None);
        assert_eq!(
            log.snapshot(),
            Snapshot {
                commit_index: None,
                entries: Vec::new()
            }
        );
    }

    #[test]
    fn append_assigns_sequential_zero_based_indices() {
        let mut log = InMemoryLog::new();
        assert_eq!(log.append(payload(0), 1).unwrap(), 0);
        assert_eq!(log.append(payload(1), 1).unwrap(), 1);
        assert_eq!(log.append(payload(2), 2).unwrap(), 2);
        assert_eq!(log.last_index(), Some(2));
        assert_eq!(log.term_at(2), Some(2));
    }

    #[test]
    fn read_is_ascending_and_clamped_empty_when_start_after_end() {
        let mut log = InMemoryLog::new();
        for i in 0..5 {
            log.append(payload(i), 1).unwrap();
        }
        let got: Vec<u64> = log.read(1, 3).into_iter().map(|e| e.index).collect();
        assert_eq!(got, vec![1, 2, 3]);
        // end beyond last index clamps to the stored range.
        let got: Vec<u64> = log.read(3, 100).into_iter().map(|e| e.index).collect();
        assert_eq!(got, vec![3, 4]);
        // start > end yields empty, not an error.
        assert!(log.read(4, 2).is_empty());
    }

    #[test]
    fn commit_advances_within_bounds_and_rejects_otherwise() {
        let mut log = InMemoryLog::new();
        // Cannot commit on an empty log.
        assert!(log.commit(0).is_err());
        for i in 0..3 {
            log.append(payload(i), 1).unwrap();
        }
        log.commit(1).unwrap();
        assert_eq!(log.commit_index(), Some(1));
        // Equal index is idempotent.
        log.commit(1).unwrap();
        // Below current commit is rejected and leaves state unchanged.
        assert!(log.commit(0).is_err());
        assert_eq!(log.commit_index(), Some(1));
        // Above last index is rejected.
        assert!(log.commit(3).is_err());
        assert_eq!(log.commit_index(), Some(1));
    }

    #[test]
    fn revert_removes_suffix_but_protects_committed_entries() {
        let mut log = InMemoryLog::new();
        for i in 0..5 {
            log.append(payload(i), 1).unwrap();
        }
        log.commit(2).unwrap();
        // Revert below the commit index is rejected.
        assert!(log.revert(1).is_err());
        assert_eq!(log.last_index(), Some(4));
        // Revert at/above commit removes the uncommitted suffix.
        log.revert(2).unwrap();
        assert_eq!(log.last_index(), Some(2));
        assert_eq!(log.commit_index(), Some(2));
    }

    #[test]
    fn snapshot_reflects_committed_prefix() {
        let mut log = InMemoryLog::new();
        for i in 0..4 {
            log.append(payload(i), 1).unwrap();
        }
        log.commit(1).unwrap();
        let snap = log.snapshot();
        assert_eq!(snap.commit_index, Some(1));
        let indices: Vec<u64> = snap.entries.iter().map(|e| e.index).collect();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn append_entries_extends_and_overwrites_uncommitted_suffix() {
        let mut log = InMemoryLog::new();
        let batch = vec![
            LogEntry {
                index: 0,
                term: 1,
                payload: payload(0),
            },
            LogEntry {
                index: 1,
                term: 1,
                payload: payload(1),
            },
        ];
        log.append_entries(&batch).unwrap();
        assert_eq!(log.last_index(), Some(1));

        // Overwrite the uncommitted suffix from index 1.
        let overwrite = vec![LogEntry {
            index: 1,
            term: 2,
            payload: payload(9),
        }];
        log.append_entries(&overwrite).unwrap();
        assert_eq!(log.term_at(1), Some(2));

        // A gap (start beyond len) is rejected.
        let gap = vec![LogEntry {
            index: 5,
            term: 2,
            payload: payload(5),
        }];
        assert!(matches!(
            log.append_entries(&gap),
            Err(LogError::NonContiguousEntries)
        ));

        // Overwriting a committed entry is rejected.
        log.commit(1).unwrap();
        let clobber = vec![LogEntry {
            index: 1,
            term: 3,
            payload: payload(7),
        }];
        assert!(matches!(
            log.append_entries(&clobber),
            Err(LogError::NonContiguousEntries)
        ));
    }
}
