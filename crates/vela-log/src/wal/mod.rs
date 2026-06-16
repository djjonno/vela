//! Durable on-disk Write-Ahead Log (`DurableWal`) for `vela-log`.
//!
//! This module tree adds a durable [`LogStorage`](crate::LogStorage)
//! implementation alongside the in-memory one, plus an explicitly-triggered log
//! compaction operation. Nothing here depends on another Vela crate; the WAL is
//! internal to `vela-log`.
//!
//! The implementation follows a Kafka-style storage model: an **in-memory
//! index** holding per-entry metadata (term + on-disk location), with
//! **payload bytes living only in segment files** and read back on demand.
//! Durability is provided by checksummed record framing, segment files, a
//! crash-atomic double-buffered manifest recording the durably-acknowledged
//! extent, and a startup recovery/replay pass.
//!
//! The submodules are:
//!
//! - [`config`] — `WalConfig`, `SyncPolicy`, and validation.
//! - [`frame`] — record-frame encode/decode and CRC32C.
//! - [`segment`] — segment file create/append/read/seal, naming, rollover.
//! - [`index`] — in-memory index and absolute-index mapping.
//! - [`manifest`] — double-buffered, CRC'd durable metadata.
//! - [`recovery`] — open-time replay and torn-tail vs interior classification.
//! - [`fs`] — filesystem seam for real I/O and deterministic fault injection.
//! - [`error`] — WAL-specific error helpers over [`crate::LogError`].
//!
//! # State assembled here (task 8)
//!
//! [`DurableWal`] coordinates the four pieces the submodules provide — the
//! in-memory [`LogIndex`], the [`SegmentSet`] of on-disk files, the durable
//! [`Manifest`], and the [`SyncPolicy`] — behind the single-writer
//! `LogStorage` contract. This task wires up the open-empty path, `append`
//! with the `Always` persist-before-acknowledge guarantee, `commit`'s durable
//! advance, and the memory-served queries (`last_index`, `term_at`,
//! `commit_index`), plus a minimal replay sufficient to reopen a cleanly-synced
//! log. The remaining trait methods are stubbed for their owning tasks (see the
//! `LogStorage` impl).

// The generic [`FileSystem`]/[`Clock`] seam *traits* are internal to `wal` (per
// the design) and deliberately not re-exported, yet `DurableWal` — which is
// generic over those seams — is re-exported from the crate root. The concrete
// production defaults ([`RealFileSystem`], [`RealClock`]) *are* re-exported so
// the public `DurableWal::open` return type is nameable downstream, but the
// trait bounds themselves stay private, which is what `private_bounds`/
// `private_interfaces` flag. Allowing the two lints module-wide keeps the seam
// traits private while still exposing a usable `DurableWal`; the only public
// entry points remain `open`/`open_with` and the `LogStorage` trait.
#![allow(private_interfaces, private_bounds)]

mod clock;
mod config;
mod error;
mod frame;
mod fs;
mod index;
mod manifest;
#[cfg(test)]
mod proptests;
mod recovery;
mod segment;

pub use clock::RealClock;
pub use config::{SyncPolicy, WalConfig};
pub use fs::RealFileSystem;

use std::cell::Cell;
use std::io;

use clock::Clock;
use frame::{decode, encode, FrameDecode};
use fs::{FileSystem, WalFile};
use index::{IndexEntry, LogIndex};
use manifest::{Manifest, ManifestState};
use segment::{segment_file_name, FrameLocation, RestoreTail, SegmentSet};

use crate::{CommitIndex, EntryPayload, HardState, LogEntry, LogError, LogStorage, Snapshot};

/// A durable, on-disk [`LogStorage`](crate::LogStorage) for one partition log.
///
/// `DurableWal` keeps an **in-memory index** ([`LogIndex`]) of every retained
/// entry's term and on-disk frame location, while payload bytes live only in
/// the [`SegmentSet`]'s segment files and are read back on demand. The durable
/// [`Manifest`] records the acknowledged extent, the commit index, and the log
/// start index crash-atomically. Under the [`Always`](SyncPolicy::Always)
/// policy the WAL forces a record frame (and, for a freshly created segment,
/// the parent directory) and then the manifest's acknowledged extent to stable
/// storage before an `append`/`commit` returns, delivering the
/// persist-before-acknowledge guarantee consensus requires.
///
/// The type is generic over the [`FileSystem`] seam so production uses
/// [`RealFileSystem`] (the default) while tests drive it with an in-memory,
/// fault-injecting filesystem; construct the former via [`open`](DurableWal::open)
/// and the latter via [`open_with`](DurableWal::open_with).
///
/// `DurableWal` is single-writer-per-partition: mutating operations take
/// `&mut self` and reads take `&self`, so the type system serializes writes
/// against reads (no concurrent-reader-during-write support is built).
///
/// A second type parameter carries the [`Clock`] seam the `Periodic` sync
/// policy reads to pace its forces (Requirement 4.2). It defaults to the
/// monotonic [`RealClock`], so every reference written as `DurableWal<F>`
/// continues to compile unchanged; a cadence test injects a deterministic clock
/// via [`open_with_clock`](DurableWal::open_with_clock).
pub struct DurableWal<F: FileSystem = RealFileSystem, C: Clock = RealClock> {
    /// Validated configuration (data directory, segment size, sync policy).
    cfg: WalConfig,
    /// The filesystem seam this log performs all I/O through.
    fs: F,
    /// The monotonic clock used to pace `Periodic` forces; never read under the
    /// `Always` or `Never` policies.
    clock: C,
    /// Exclusive Data_Directory lock; held for the lifetime of the log and
    /// released when the guard drops. Read only via its `Drop`.
    #[allow(dead_code)]
    dir_lock: F::Lock,
    /// In-memory index: per-entry term + on-disk frame location, no payloads.
    index: LogIndex,
    /// Commit position mirrored in memory; the durable copy lives in the
    /// manifest. `None` before anything is committed.
    commit_index: Option<u64>,
    /// The ordered set of on-disk segment files backing the log.
    segments: SegmentSet<F>,
    /// The durable manifest (the requirements' `Frame_Metadata`).
    manifest: Manifest,
    /// In-memory mirror of the acknowledged extent's highest index, or `None`
    /// when nothing has been acknowledged durable.
    durable_last: Option<u64>,
    /// Base index of the segment holding `durable_last`'s frame.
    durable_segment: u64,
    /// Byte offset just past `durable_last`'s frame within its segment.
    durable_offset: u64,
    /// In-memory mirror of the persisted Raft hard-state term; the durable copy
    /// lives in the manifest. `0` for a fresh log (Requirements 9.2, 10.1).
    hs_current_term: u64,
    /// In-memory mirror of the persisted Raft vote; `None` when this replica has
    /// not voted in `hs_current_term`. The durable copy lives in the manifest
    /// (Requirements 9.1, 10.2).
    hs_voted_for: Option<u64>,
    /// Set when an unrecoverable read I/O fault is observed, after which the
    /// log has fail-stopped (panicked) on a read path. Held in a [`Cell`] so the
    /// `&self` reads (`entry`/`read`/`snapshot`) can record the poison before
    /// they fail-stop; `DurableWal` is single-writer-per-partition and is not
    /// required to be `Sync`, so the zero-cost, non-atomic [`Cell`] is the
    /// idiomatic choice over an `AtomicBool` here. Mutating operations also
    /// refuse to run once it is set.
    poisoned: Cell<bool>,
    /// Under the `Periodic` policy, the clock reading (`now_millis`) at which
    /// the last force completed, or `None` before any force. A force is due once
    /// the gap from this reading reaches the configured interval; the first
    /// mutating operation (with `None` here) always forces (Requirement 4.2).
    /// Unused under `Always`/`Never`.
    last_force_at: Option<u64>,
    /// Under the `Periodic` policy, set when a force deferred-or-issued during a
    /// mutating operation failed to reach stable storage. The **next** mutating
    /// call observes it, returns [`Io`](LogError::Io), and clears it, so a failed
    /// periodic force is never silently swallowed (Requirement 4.6). Unused
    /// under `Always`/`Never`.
    pending_force_error: bool,
}

impl DurableWal<RealFileSystem> {
    /// Open (or create) a durable log at `cfg.data_dir` using the real
    /// filesystem.
    ///
    /// Validates the configuration, creates the data directory, acquires its
    /// exclusive lock, and reconstructs in-memory state from the manifest and
    /// any existing segments. An empty or absent directory initializes an empty
    /// log (no retained entries, `log_start_index = 0`, `commit_index = None`)
    /// per Requirement 5.2.
    pub fn open(cfg: WalConfig) -> Result<Self, LogError> {
        Self::open_with(cfg, RealFileSystem::new())
    }
}

impl<F: FileSystem> DurableWal<F, RealClock> {
    /// Open a durable log against an explicit [`FileSystem`], for tests that
    /// drive the in-memory fault filesystem.
    ///
    /// Behaves exactly like [`open`](DurableWal::open) but over the supplied
    /// `fs`, so a `DurableWal` can be dropped and reopened on the same
    /// in-memory filesystem to exercise the reopen-after-append round trip. The
    /// production [`RealClock`] is used; inject a deterministic clock with
    /// [`open_with_clock`](DurableWal::open_with_clock) instead.
    pub fn open_with(cfg: WalConfig, fs: F) -> Result<Self, LogError> {
        Self::open_with_clock(cfg, fs, RealClock::new())
    }
}

impl<F: FileSystem, C: Clock> DurableWal<F, C> {
    /// Open a durable log against an explicit [`FileSystem`] **and** [`Clock`].
    ///
    /// This is the full constructor the other entry points delegate to; it lets
    /// a test inject a deterministic clock so the `Periodic` policy's force
    /// cadence can be driven by advancing time rather than sleeping
    /// (Requirement 4.2). Aside from the clock it is identical to
    /// [`open_with`](DurableWal::open_with): it validates the configuration,
    /// creates and locks the data directory, and reconstructs in-memory state
    /// from the manifest and any existing segments.
    pub(crate) fn open_with_clock(cfg: WalConfig, fs: F, clock: C) -> Result<Self, LogError> {
        // Validate, create the data directory, and take the exclusive lock.
        let dir_lock = cfg.prepare(&fs)?;

        // Read the best (highest-seq, intact) manifest slot, defaulting an
        // absent/torn manifest to the empty state.
        let (manifest, state) =
            Manifest::open(&fs, &cfg.data_dir).map_err(|source| LogError::Io {
                op: "open manifest",
                source,
            })?;
        let state = state.unwrap_or_default();

        // Capture the recovered Raft hard state from the best manifest slot
        // before `replay` consumes the rest of `state`. A fresh log defaults to
        // term 0 with no vote, so `hard_state()` reports `HardState::default()`
        // (Requirements 10.1, 10.2).
        let hs_current_term = state.hs_current_term;
        let hs_voted_for = state.hs_voted_for;

        // Reconstruct the in-memory index, commit index, and acknowledged
        // extent from the segments on disk, discriminating a recoverable torn
        // tail from interior corruption and applying policy-scoped shortfall
        // handling. This read-only pass leaves the segments unchanged, so an
        // interior-corruption or shortfall failure returns before any segment
        // is modified (Requirements 5, 6).
        let recovered = recovery::replay(&fs, &cfg, &state)?;

        // Reconstruct the full segment set from disk so the active segment is
        // the real segment holding the recovered tail (truncated past the last
        // acknowledged frame), the earlier segments are sealed, and any torn
        // orphan beyond the tail is removed. A subsequent append therefore
        // continues in the correct active segment, and a torn tail is physically
        // dropped (Requirements 5.1, 6.2).
        let tail = recovered.durable_last.map(|_| RestoreTail {
            segment: recovered.durable_segment,
            offset_end: recovered.durable_offset,
        });
        let segments = SegmentSet::restore(
            &fs,
            cfg.data_dir.clone(),
            cfg.segment_size,
            recovered.index.log_start_index(),
            tail,
        )
        .map_err(|source| LogError::Io {
            op: "restore segments",
            source,
        })?;

        Ok(Self {
            cfg,
            fs,
            clock,
            dir_lock,
            index: recovered.index,
            commit_index: recovered.commit_index,
            segments,
            manifest,
            durable_last: recovered.durable_last,
            durable_segment: recovered.durable_segment,
            durable_offset: recovered.durable_offset,
            hs_current_term,
            hs_voted_for,
            poisoned: Cell::new(false),
            last_force_at: None,
            pending_force_error: false,
        })
    }

    /// Build the durable [`ManifestState`] to persist, with `commit_index` set
    /// to `commit` and every other field taken from the current in-memory
    /// mirror.
    fn manifest_state(&self, commit: Option<u64>) -> ManifestState {
        ManifestState {
            log_start_index: self.index.log_start_index(),
            commit_index: commit,
            durable_last: self.durable_last,
            durable_segment: self.durable_segment,
            durable_offset: self.durable_offset,
            hs_current_term: self.hs_current_term,
            hs_voted_for: self.hs_voted_for,
        }
    }

    /// Force the active segment's data (and the parent directory when a new
    /// segment file was created) and advance the durably-acknowledged extent to
    /// `(last, segment, offset_past)` in the manifest, updating the in-memory
    /// mirror only once every force has succeeded.
    ///
    /// This is the persist step shared by the `Always` policy (where it runs on
    /// every `append`/`append_entries`) and a due `Periodic` force. On any
    /// failure it returns [`Io`](LogError::Io) **before** touching the mirror,
    /// so a caller that aborts on the error leaves reported state unchanged
    /// (Requirements 4.1, 4.5, 4.11).
    fn force_tail(
        &mut self,
        created_segment: bool,
        last: u64,
        segment: u64,
        offset_past: u64,
    ) -> Result<(), LogError> {
        self.segments.sync_active().map_err(|source| LogError::Io {
            op: "sync segment",
            source,
        })?;
        if created_segment {
            self.fs
                .sync_dir(&self.cfg.data_dir)
                .map_err(|source| LogError::Io {
                    op: "sync directory",
                    source,
                })?;
        }
        let state = ManifestState {
            log_start_index: self.index.log_start_index(),
            commit_index: self.commit_index,
            durable_last: Some(last),
            durable_segment: segment,
            durable_offset: offset_past,
            hs_current_term: self.hs_current_term,
            hs_voted_for: self.hs_voted_for,
        };
        self.manifest
            .write(&self.fs, &state)
            .map_err(|source| LogError::Io {
                op: "sync manifest",
                source,
            })?;
        self.durable_last = Some(last);
        self.durable_segment = segment;
        self.durable_offset = offset_past;
        Ok(())
    }

    /// Force buffered Record_Frames (the active segment) to stable storage and
    /// persist `commit` in the manifest, without advancing the acknowledged
    /// extent (no new frame was written).
    ///
    /// Used by a due `Periodic` `commit`: the commit invocation participates in
    /// the force cadence (Requirement 4.2), so it both forces the frames
    /// buffered by prior deferred operations and records the new commit index.
    fn force_commit(&mut self, commit: u64) -> Result<(), LogError> {
        self.segments.sync_active().map_err(|source| LogError::Io {
            op: "commit sync segment",
            source,
        })?;
        let state = self.manifest_state(Some(commit));
        self.manifest
            .write(&self.fs, &state)
            .map_err(|source| LogError::Io {
                op: "commit manifest",
                source,
            })?;
        Ok(())
    }

    /// Whether a `Periodic` force is due now (Requirement 4.2).
    ///
    /// The first mutating operation always forces (there is no prior force to
    /// pace against); afterwards a force is due once the monotonic gap since the
    /// last completed force reaches `interval_ms`. While the log is idle the gap
    /// may exceed the interval — the next mutating operation simply forces.
    fn periodic_force_due(&self, interval_ms: u64) -> bool {
        match self.last_force_at {
            None => true,
            Some(prev) => self.clock.now_millis().saturating_sub(prev) >= interval_ms,
        }
    }

    /// Surface a deferred `Periodic` force failure on this mutating call
    /// (Requirement 4.6).
    ///
    /// If a previous `Periodic` force failed, the failure was recorded rather
    /// than returned (the originating operation had no persist-before-acknowledge
    /// obligation under `Periodic`). The next mutating call observes it here,
    /// returns [`Io`](LogError::Io), and clears the flag so it surfaces exactly
    /// once.
    fn take_pending_force_error(&mut self, op: &'static str) -> Result<(), LogError> {
        if self.pending_force_error {
            self.pending_force_error = false;
            return Err(LogError::Io {
                op,
                source: io::Error::other("a prior periodic force failed to reach stable storage"),
            });
        }
        Ok(())
    }

    /// Apply the post-write durability step for an operation that advanced the
    /// log tail to `(last, segment, offset_past)`, honoring the sync policy.
    ///
    /// - `Always`: force immediately; a failure propagates as [`Io`](LogError::Io)
    ///   so the caller aborts with reported state unchanged (Requirement 4.1).
    /// - `Periodic`: force only when one is due (Requirement 4.2). A force
    ///   failure is recorded for the next call rather than returned, since
    ///   `Periodic` makes no persist-before-acknowledge promise (Requirement
    ///   4.6); the operation still completes.
    /// - `Never`: never force the frames (Requirement 4.3).
    fn persist_tail(
        &mut self,
        created_segment: bool,
        last: u64,
        segment: u64,
        offset_past: u64,
    ) -> Result<(), LogError> {
        match self.cfg.sync_policy {
            SyncPolicy::Always => self.force_tail(created_segment, last, segment, offset_past),
            SyncPolicy::Periodic { interval_ms } => {
                if self.periodic_force_due(interval_ms) {
                    match self.force_tail(created_segment, last, segment, offset_past) {
                        Ok(()) => self.last_force_at = Some(self.clock.now_millis()),
                        Err(_) => self.pending_force_error = true,
                    }
                }
                Ok(())
            }
            SyncPolicy::Never => Ok(()),
        }
    }

    /// Read and decode the frame at `location`, opening its segment file
    /// directly by base index.
    ///
    /// Resolving the segment file from the recorded [`FrameLocation`] (rather
    /// than through the [`SegmentSet`]) lets reads work uniformly for entries
    /// written this session and entries recovered on reopen, before task 12
    /// reconstructs the full segment set.
    fn load_frame(&self, location: FrameLocation) -> io::Result<LogEntry> {
        let path = self.cfg.data_dir.join(segment_file_name(location.segment));
        let file = self.fs.open_read(&path)?;
        let mut buf = vec![0u8; location.len as usize];
        file.read_exact_at(location.offset, &mut buf)?;
        match decode(&buf) {
            FrameDecode::Ok { entry, .. } => Ok(entry),
            FrameDecode::Incomplete | FrameDecode::Corrupt => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame at recorded location failed to decode",
            )),
        }
    }

    /// Read the payload-bearing entry at absolute `index` from disk, or
    /// **fail-stop** if the read fails.
    ///
    /// The trait's read signatures are infallible, so a disk read that fails —
    /// whether a filesystem I/O error or a decode failure (corruption) at a
    /// location the index recorded — must never be disguised as data absence
    /// (an empty/`None`/short result). Per the design's fail-stop policy
    /// (Property 5, Requirement 10.4) such a fault logs via [`tracing::error!`],
    /// marks the log poisoned, and panics. Both a decode failure (an
    /// `InvalidData` from [`load_frame`](Self::load_frame) — an I/O-class
    /// corruption at a recorded frame) and a genuine I/O error funnel through
    /// here, so neither is ever returned as absence. The shared callers
    /// ([`entry`](LogStorage::entry), [`read`](LogStorage::read),
    /// [`snapshot`](LogStorage::snapshot)) only invoke this for an index they
    /// have already confirmed is within the retained range.
    fn read_payload(&self, index: u64, location: FrameLocation) -> LogEntry {
        match self.load_frame(location) {
            Ok(entry) => entry,
            Err(err) => {
                // An I/O error (or corruption at a recorded location) on a read
                // is unrecoverable and must not look like an absent entry.
                tracing::error!(
                    index,
                    error = %err,
                    "durable WAL read failed; poisoning and failing stop"
                );
                self.poisoned.set(true);
                panic!("durable WAL read failed at index {index}: {err}");
            }
        }
    }

    /// The lowest retained absolute index (the Log_Start_Index): `0` for a log
    /// that has never been compacted, advancing as Compaction discards a
    /// prefix (Requirement 8 / Log_Start_Index).
    ///
    /// Entries below this index have been discarded and read back as absent
    /// ([`entry`](LogStorage::entry)/[`term_at`](LogStorage::term_at) return
    /// `None`); this accessor exposes that absolute-index base, which is part
    /// of the WAL's public behavior and survives reopen (Requirement 7.7).
    pub fn log_start_index(&self) -> u64 {
        self.index.log_start_index()
    }

    /// Discard a committed prefix of the log, advancing the Log_Start_Index to
    /// `retained_point` and reclaiming whole below-line segments (Requirement
    /// 7).
    ///
    /// Compaction is an explicitly-triggered operation, not part of the
    /// [`LogStorage`] trait: the WAL treats `retained_point` as authoritative
    /// and never consults replication progress — choosing a replication-safe
    /// point is the caller's responsibility (Requirement 7.1).
    ///
    /// Behavior:
    ///
    /// - **No-op** when `retained_point <= log_start_index`: nothing is
    ///   discarded and the Log_Start_Index does not regress, returning `Ok`
    ///   regardless of the commit index. This no-op case takes **precedence**
    ///   over the bounds rejection below, because it discards nothing
    ///   (Requirement 7.4 over 7.3).
    /// - **Rejected** with [`CompactionOutOfBounds`](LogError::CompactionOutOfBounds)
    ///   when a `retained_point` above the Log_Start_Index would discard an
    ///   uncommitted entry or the entry at the commit index — i.e. when the
    ///   commit index is `None`, or `retained_point > commit_index`. On
    ///   rejection the persisted log is left unchanged (Requirement 7.3). Since
    ///   a valid `retained_point` is `<= commit_index <= last_index`, it is
    ///   inherently within range.
    /// - **Compacted** otherwise: the advanced Log_Start_Index is persisted to
    ///   the manifest and fsynced **before** any segment is removed, so a crash
    ///   mid-compaction leaves the log recoverable with a Log_Start_Index no
    ///   less than its pre-compaction value and no retained entry lost; any
    ///   below-line segments orphaned by a crash before their removal are
    ///   ignored by recovery (Requirement 7.8). The in-memory index then drops
    ///   the discarded prefix, and whole below-line segments are deleted at
    ///   segment granularity — a segment straddling `retained_point` is kept,
    ///   its below-line frames skipped on read and recovery (Requirement 7.5).
    ///   Retained entries keep their identical index, term, and payload, and
    ///   `last_index`/`commit_index` are unchanged (Requirement 7.6); the
    ///   acknowledged extent (`durable_last`/segment/offset) is unchanged too,
    ///   since the tail did not move.
    ///
    /// On a manifest write failure the persisted log is left unchanged — the
    /// in-memory index is not advanced and no segment is deleted — and the
    /// failure is returned as [`Io`](LogError::Io).
    pub fn compaction(&mut self, retained_point: u64) -> Result<(), LogError> {
        if self.poisoned.get() {
            return Err(LogError::Io {
                op: "compaction",
                source: io::Error::other("log poisoned by a prior read failure"),
            });
        }

        // No-op when the retained point is at or below the current
        // Log_Start_Index: nothing is discarded and the start never regresses.
        // This precedes the bounds check, because a no-op discards nothing and
        // so cannot discard a committed/uncommitted entry (Requirement 7.4 over
        // 7.3).
        if retained_point <= self.index.log_start_index() {
            return Ok(());
        }

        // A valid compaction discards only indices at or below the commit index
        // while retaining the entry at the commit index, i.e.
        // `retained_point <= commit_index`. Reject a point that would discard an
        // uncommitted entry or the commit entry itself, including the case where
        // nothing is committed (Requirement 7.3). Since `commit_index` is always
        // `<= last_index`, a retained point passing this check is in range.
        let reject = match self.commit_index {
            None => true,
            Some(commit) => retained_point > commit,
        };
        if reject {
            return Err(LogError::CompactionOutOfBounds {
                requested: retained_point,
                commit: self.commit_index,
                log_start: self.index.log_start_index(),
            });
        }

        // Persist the advanced Log_Start_Index to stable storage *before*
        // removing any segment (Requirement 7.8: crash-safety ordering). The
        // acknowledged extent and commit index are carried unchanged — only the
        // log start advances. On failure nothing below runs, so the in-memory
        // index and segments are untouched and reported state is unchanged.
        let state = ManifestState {
            log_start_index: retained_point,
            commit_index: self.commit_index,
            durable_last: self.durable_last,
            durable_segment: self.durable_segment,
            durable_offset: self.durable_offset,
            hs_current_term: self.hs_current_term,
            hs_voted_for: self.hs_voted_for,
        };
        self.manifest
            .write(&self.fs, &state)
            .map_err(|source| LogError::Io {
                op: "compaction manifest",
                source,
            })?;

        // The advanced log start is durable: drop the in-memory index entries
        // below it (`last_index`/`commit_index` and retained entries are left
        // intact — Requirement 7.6) and reclaim whole below-line segments at
        // segment granularity (Requirement 7.5). A crash between the manifest
        // fsync and a delete leaves an orphan below-line segment that recovery
        // ignores (Requirement 7.8).
        self.index.compact_to(retained_point);
        self.segments
            .remove_below(&self.fs, retained_point)
            .map_err(|source| LogError::Io {
                op: "compaction remove segments",
                source,
            })?;

        Ok(())
    }
}

impl<F: FileSystem, C: Clock> LogStorage for DurableWal<F, C> {
    fn append(&mut self, payload: EntryPayload, term: u64) -> Result<u64, LogError> {
        if self.poisoned.get() {
            return Err(LogError::Io {
                op: "append",
                source: io::Error::other("log poisoned by a prior read failure"),
            });
        }

        // A `Periodic` force that failed during an earlier operation surfaces
        // here, before any work, as `Io` (Requirement 4.6).
        self.take_pending_force_error("append")?;

        // The next absolute index is `last_index + 1`, or `log_start_index` when
        // the log is empty — including immediately after compaction, so no index
        // below `log_start_index` is ever reused (Requirements 1.3, 8.5).
        let index = self.index.next_index();
        let entry = LogEntry {
            index,
            term,
            payload,
        };
        let frame = encode(&entry);

        // Write the frame to the active segment, rolling over as the placement
        // rules require.
        let outcome = self
            .segments
            .append(&self.fs, index, &frame)
            .map_err(|source| LogError::Io {
                op: "append frame",
                source,
            })?;
        let segment = outcome.location.segment;
        let offset_past = outcome.location.offset + outcome.location.len as u64;

        // Apply the policy's durability step. Under `Always` a force failure
        // propagates here, before the in-memory index is advanced, so reported
        // state is unchanged on failure (Requirements 4.1, 4.5, 10.3). Under
        // `Periodic` a due force runs (failure recorded for the next call); under
        // `Never` the frame is left in the OS unforced (Requirements 4.2, 4.3).
        self.persist_tail(outcome.created_segment, index, segment, offset_past)?;

        // Advance the in-memory index last. Under `Always` any durable-write
        // failure returned early above; under `Periodic`/`Never` the write to the
        // OS succeeded, so the operation completes and the entry is visible.
        self.index
            .push(IndexEntry::from_location(outcome.location, term));
        Ok(index)
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> Result<(), LogError> {
        // An empty batch is a success no-op, leaving the log unchanged
        // (Requirement 1.6).
        if entries.is_empty() {
            return Ok(());
        }
        if self.poisoned.get() {
            return Err(LogError::Io {
                op: "append_entries",
                source: io::Error::other("log poisoned by a prior read failure"),
            });
        }

        // A `Periodic` force that failed during an earlier operation surfaces
        // here, before any work, as `Io` (Requirement 4.6).
        self.take_pending_force_error("append_entries")?;

        // The incoming batch must itself be contiguous and ascending by index
        // (Requirement 1.4).
        for pair in entries.windows(2) {
            if pair[1].index != pair[0].index + 1 {
                return Err(LogError::NonContiguousEntries);
            }
        }

        let start = entries[0].index;
        let log_start = self.index.log_start_index();
        let last = self.index.last_index();
        // `next_index` is `last_index + 1`, or `log_start_index` on an empty
        // log — the one index past the end the batch may legally extend at.
        let next = self.index.next_index();

        // Reject a batch that would leave a gap (begins beyond the end of the
        // log) or that would reach into the compacted prefix below
        // `log_start_index`; neither can connect to the retained range
        // (Requirement 1.4).
        if start > next || start < log_start {
            return Err(LogError::NonContiguousEntries);
        }

        // Reject a batch that overwrites an entry at or below the commit index:
        // this `NonContiguousEntries` variant doubles as the commit-conflict
        // signal, preserving drop-in fidelity with `InMemoryLog`
        // (Requirement 1.4 note).
        if let Some(committed) = self.commit_index {
            if start <= committed {
                return Err(LogError::NonContiguousEntries);
            }
        }

        let always = self.cfg.sync_policy == SyncPolicy::Always;

        // An overwrite (the batch begins at or before the last retained index)
        // must first discard the conflicting uncommitted suffix at and above
        // `start`, both on disk and in the in-memory index, and reduce the
        // durably-acknowledged extent to `start - 1` (empty when
        // `start == log_start_index`). Reducing the extent durably *before*
        // destroying any frame data ensures a crash mid-overwrite never leaves
        // the manifest acknowledging a frame that has been removed.
        if matches!(last, Some(highest) if start <= highest) {
            let cut = self
                .index
                .location(start)
                .expect("start within the retained range during overwrite");
            let (reduced_last, reduced_segment, reduced_offset) = if start == log_start {
                (None, 0, 0)
            } else {
                let prev = self
                    .index
                    .location(start - 1)
                    .expect("start-1 within the retained range when start > log_start");
                (Some(start - 1), prev.segment, prev.offset + prev.len as u64)
            };

            if always {
                let state = ManifestState {
                    log_start_index: log_start,
                    commit_index: self.commit_index,
                    durable_last: reduced_last,
                    durable_segment: reduced_segment,
                    durable_offset: reduced_offset,
                    hs_current_term: self.hs_current_term,
                    hs_voted_for: self.hs_voted_for,
                };
                self.manifest
                    .write(&self.fs, &state)
                    .map_err(|source| LogError::Io {
                        op: "append_entries reduce manifest",
                        source,
                    })?;
                self.durable_last = reduced_last;
                self.durable_segment = reduced_segment;
                self.durable_offset = reduced_offset;
            } else {
                // Under `Periodic`/`Never` the acknowledged extent is not forced
                // ahead of the truncation (recovery recomputes the valid run and
                // ignores the recorded extent under these policies); still reduce
                // the in-memory mirror so it tracks the truncated tail rather
                // than pointing past it (Requirements 4.2, 4.3).
                self.durable_last = reduced_last;
                self.durable_segment = reduced_segment;
                self.durable_offset = reduced_offset;
            }

            // Drop the in-memory suffix first (so the index matches the reduced
            // extent), then truncate the on-disk frames across segments.
            self.index.truncate_from(start);
            self.segments
                .truncate_at(&self.fs, cut)
                .map_err(|source| LogError::Io {
                    op: "append_entries truncate segments",
                    source,
                })?;
        }

        // Write the batch frames in ascending index order, collecting the
        // freshly-assigned index entries. They are pushed into the in-memory
        // index only after the batch's durable writes succeed, so a mid-batch
        // failure never reports a partially-applied batch as success
        // (Requirement 10.3).
        let mut new_entries = Vec::with_capacity(entries.len());
        let mut created_segment = false;
        let mut last_segment = self.durable_segment;
        let mut last_offset = self.durable_offset;
        for entry in entries {
            let frame = encode(entry);
            let outcome = self
                .segments
                .append(&self.fs, entry.index, &frame)
                .map_err(|source| LogError::Io {
                    op: "append_entries frame",
                    source,
                })?;
            created_segment |= outcome.created_segment;
            last_segment = outcome.location.segment;
            last_offset = outcome.location.offset + outcome.location.len as u64;
            new_entries.push(IndexEntry::from_location(outcome.location, entry.term));
        }
        let new_last = entries[entries.len() - 1].index;

        // Apply the policy's durability step once for the whole batch: under
        // `Always` the frame(s), the parent directory if a new segment file was
        // created, and the manifest's advanced acknowledged extent are forced
        // before returning, and a failure propagates here before the in-memory
        // index is advanced (Requirements 4.1, 4.11). Under `Periodic` a due
        // force runs (failure recorded for the next call); under `Never` the
        // frames are left in the OS unforced (Requirements 4.2, 4.3).
        self.persist_tail(created_segment, new_last, last_segment, last_offset)?;

        // The batch is durable (or, under `Periodic`/`Never`, written to the
        // OS): commit the new entries to the in-memory index.
        for entry in new_entries {
            self.index.push(entry);
        }
        Ok(())
    }

    fn read(&self, start: u64, end: u64) -> Vec<LogEntry> {
        // `start > end` is an empty read, not an error (Requirement 8.4).
        if start > end {
            return Vec::new();
        }
        // Nothing retained → nothing to return.
        let last = match self.index.last_index() {
            Some(last) => last,
            None => return Vec::new(),
        };
        // Clamp the requested range to the retained range
        // `[log_start_index..=last_index]`. After Compaction this offset by
        // `log_start_index` is what keeps reads below the retained prefix empty
        // (Requirements 8.3, 8.4).
        let lo = start.max(self.index.log_start_index());
        let hi = end.min(last);
        if lo > hi {
            return Vec::new();
        }
        // The retained range is dense, so every index in `[lo, hi]` is present;
        // read each payload from disk in ascending order, failing stop on any
        // read error (Requirement 10.4).
        let mut entries = Vec::with_capacity((hi - lo + 1) as usize);
        for index in lo..=hi {
            let location = self
                .index
                .location(index)
                .expect("index within the retained range has a frame location");
            entries.push(self.read_payload(index, location));
        }
        entries
    }

    fn entry(&self, index: u64) -> Option<LogEntry> {
        // An index outside the retained range is `None` without error
        // (Requirement 8.1); a present index reads its payload from disk and
        // fails stop on a read error rather than masking it as absence
        // (Requirement 10.4).
        let location = self.index.location(index)?;
        Some(self.read_payload(index, location))
    }

    fn last_index(&self) -> Option<u64> {
        self.index.last_index()
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        self.index.term_at(index)
    }

    fn commit_index(&self) -> CommitIndex {
        self.commit_index
    }

    fn commit(&mut self, index: u64) -> Result<(), LogError> {
        if self.poisoned.get() {
            return Err(LogError::Io {
                op: "commit",
                source: io::Error::other("log poisoned by a prior read failure"),
            });
        }

        // A `Periodic` force that failed during an earlier operation surfaces
        // here, before any work, as `Io` (Requirement 4.6).
        self.take_pending_force_error("commit")?;

        let last = self.index.last_index();
        // Reject indices above the highest retained entry (or any commit on an
        // empty log) and indices below the current commit (Requirement 1.7).
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

        match self.cfg.sync_policy {
            // `Periodic`: the commit invocation participates in the force
            // cadence (Requirement 4.2). On a due force, buffered frames are
            // forced and the new commit is persisted; otherwise the commit is
            // deferred (it advances in memory and is persisted by a later
            // force). A due-force failure is recorded for the next call rather
            // than returned (Requirement 4.6).
            SyncPolicy::Periodic { interval_ms } => {
                if self.periodic_force_due(interval_ms) {
                    match self.force_commit(index) {
                        Ok(()) => self.last_force_at = Some(self.clock.now_millis()),
                        Err(_) => self.pending_force_error = true,
                    }
                }
            }
            // `Always` forces the advanced commit to stable storage before
            // returning (Requirement 4.4); `Never` does not force frame data but
            // still records the commit in the manifest (metadata, not a
            // Record_Frame) so it survives reopen. Both write the manifest; on
            // failure the commit index is left unchanged (Requirement 4.5).
            SyncPolicy::Always | SyncPolicy::Never => {
                let state = self.manifest_state(Some(index));
                self.manifest
                    .write(&self.fs, &state)
                    .map_err(|source| LogError::Io {
                        op: "commit manifest",
                        source,
                    })?;
            }
        }

        self.commit_index = Some(index);
        Ok(())
    }

    fn revert(&mut self, index: u64) -> Result<(), LogError> {
        if self.poisoned.get() {
            return Err(LogError::Io {
                op: "revert",
                source: io::Error::other("log poisoned by a prior read failure"),
            });
        }

        // Reverting below the current commit index would discard committed
        // entries (Requirements 1.8, 9.4). On rejection the persisted log,
        // `last_index`, and commit index are left unchanged.
        if matches!(self.commit_index, Some(committed) if index < committed) {
            return Err(LogError::RevertBelowCommit {
                requested: index,
                commit: self.commit_index,
            });
        }

        // Reverting below the log start index reaches into the compacted
        // prefix; every retained index below the commit index is committed, so
        // this is rejected for the same reason (Requirement 9.4). (In a
        // never-compacted log `log_start_index` is 0, so this guard only fires
        // once a prefix has been discarded.)
        if index < self.index.log_start_index() {
            return Err(LogError::RevertBelowCommit {
                requested: index,
                commit: self.commit_index,
            });
        }

        // Removal is only work when entries strictly above `index` exist. A
        // revert at or above `last_index` (including on an empty log) removes
        // nothing and is a success no-op, matching `InMemoryLog::revert`'s
        // truncate; no durable write is performed when nothing changes
        // (Requirement 1.7 base semantics).
        match self.index.last_index() {
            Some(last) if index < last => {}
            _ => return Ok(()),
        }

        // The first frame to drop is the one at `index + 1` (valid because
        // `index < last_index`); everything at and above it is removed. The
        // retained extent's new high-water mark is the frame at `index`.
        let cut = self
            .index
            .location(index + 1)
            .expect("index + 1 within the retained range when index < last_index");
        let retained = self
            .index
            .location(index)
            .expect("index within the retained range when index >= log_start_index");
        let reduced_last = Some(index);
        let reduced_segment = retained.segment;
        let reduced_offset = retained.offset + retained.len as u64;

        // Under `Always`, reduce the durably-acknowledged extent to `index` and
        // fsync the manifest *before* destroying any frame data, so a crash
        // mid-revert never leaves the manifest acknowledging a removed frame
        // (Requirement 9.6: extent-first ordering). On a manifest write failure
        // the in-memory index and segments are still intact, so reported state
        // is unchanged (Requirement 4.5).
        let always = self.cfg.sync_policy == SyncPolicy::Always;
        if always {
            let state = ManifestState {
                log_start_index: self.index.log_start_index(),
                commit_index: self.commit_index,
                durable_last: reduced_last,
                durable_segment: reduced_segment,
                durable_offset: reduced_offset,
                hs_current_term: self.hs_current_term,
                hs_voted_for: self.hs_voted_for,
            };
            self.manifest
                .write(&self.fs, &state)
                .map_err(|source| LogError::Io {
                    op: "revert manifest",
                    source,
                })?;
            self.durable_last = reduced_last;
            self.durable_segment = reduced_segment;
            self.durable_offset = reduced_offset;
        } else {
            // `revert` is not in the `Periodic` force cadence and only `Always`
            // must force the removal (Requirement 9.5), so under
            // `Periodic`/`Never` no manifest force is issued here; still reduce
            // the in-memory mirror so it tracks the truncated tail (recovery
            // recomputes the valid run under these policies and ignores the
            // recorded extent).
            self.durable_last = reduced_last;
            self.durable_segment = reduced_segment;
            self.durable_offset = reduced_offset;
        }

        // Drop the in-memory suffix first (so the index matches the reduced
        // extent), then truncate the on-disk frames across segments, removing
        // now-unreferenced tail segments and truncating the containing one.
        self.index.truncate_from(index + 1);
        self.segments
            .truncate_at(&self.fs, cut)
            .map_err(|source| LogError::Io {
                op: "revert truncate segments",
                source,
            })?;

        // Under `Always`, force the truncated segment to stable storage before
        // returning (Requirement 9.5).
        if always {
            self.segments.sync_active().map_err(|source| LogError::Io {
                op: "revert sync segment",
                source,
            })?;
        }

        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        // With nothing committed the snapshot is empty (Requirement 8.7);
        // otherwise it is the retained committed prefix
        // `log_start_index..=commit_index`, read from disk in ascending order
        // (Requirement 8.6). `read` already clamps to the retained range and
        // fails stop on a read error, and `commit_index <= last_index` always
        // holds, so it returns exactly that prefix.
        match self.commit_index {
            None => Snapshot {
                commit_index: None,
                entries: Vec::new(),
            },
            Some(commit) => Snapshot {
                commit_index: Some(commit),
                entries: self.read(self.index.log_start_index(), commit),
            },
        }
    }

    fn flush(&mut self) -> Result<(), LogError> {
        // Force the active segment's data and the manifest to stable storage,
        // leaving reported state unchanged (Requirement 4.10).
        self.segments.sync_active().map_err(|source| LogError::Io {
            op: "flush segment",
            source,
        })?;
        self.manifest
            .sync(&self.fs)
            .map_err(|source| LogError::Io {
                op: "flush manifest",
                source,
            })?;
        Ok(())
    }

    fn persist_hard_state(&mut self, hard_state: HardState) -> Result<(), LogError> {
        // A read fault has already fail-stopped the log; refuse further writes
        // rather than persist over a poisoned log.
        if self.poisoned.get() {
            return Err(LogError::Io {
                op: "persist_hard_state",
                source: io::Error::other("log poisoned by a prior read failure"),
            });
        }

        // Write a fresh manifest slot carrying the new hard state alongside the
        // unchanged extent/commit/log-start fields, reusing the double-buffered
        // alternate-slot + fsync path. `Manifest::write` fsyncs before it
        // returns, so the state is durable before this method returns — the
        // persist-before-return guarantee Raft depends on (Requirements 9.1,
        // 9.2). On failure the in-memory mirror is left unchanged, so the
        // reported `hard_state()` does not advance (Requirement 9.4).
        let state = ManifestState {
            log_start_index: self.index.log_start_index(),
            commit_index: self.commit_index,
            durable_last: self.durable_last,
            durable_segment: self.durable_segment,
            durable_offset: self.durable_offset,
            hs_current_term: hard_state.current_term,
            hs_voted_for: hard_state.voted_for,
        };
        self.manifest
            .write(&self.fs, &state)
            .map_err(|source| LogError::Io {
                op: "persist hard state manifest",
                source,
            })?;

        // Only mirror the persisted values once the fsync succeeded.
        self.hs_current_term = hard_state.current_term;
        self.hs_voted_for = hard_state.voted_for;
        Ok(())
    }

    fn hard_state(&self) -> Option<HardState> {
        // The durable WAL always reports a hard state: the value recovered from
        // the best manifest slot at open, or `HardState::default()` (term 0, no
        // vote) for a fresh log (Requirements 10.1, 10.2).
        Some(HardState {
            current_term: self.hs_current_term,
            voted_for: self.hs_voted_for,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::clock::test_clock::TestClock;
    use super::fs::fault::MemFileSystem;
    use super::fs::FileSystem;
    use super::*;
    use crate::{EntryPayload, LogError, LogStorage, PayloadKind};
    use std::path::Path;

    /// Data directory used by the in-memory filesystem tests.
    const DIR: &str = "/wal";

    /// A `Record` payload of three identical `tag` bytes, so a recovered entry
    /// can be matched back to the value it was appended with.
    fn payload(tag: u8) -> EntryPayload {
        EntryPayload::new(PayloadKind::Record, vec![tag, tag, tag])
    }

    /// A config with a deliberately tiny segment size so each appended frame
    /// rolls into its own segment, exercising multi-segment replay on reopen.
    fn small_cfg() -> WalConfig {
        WalConfig::new(DIR).with_segment_size(40)
    }

    /// Open a `DurableWal` over a clone of `fs` (clones share one backing store,
    /// so dropping and reopening sees the same persisted bytes).
    fn open(fs: &MemFileSystem) -> DurableWal<MemFileSystem> {
        DurableWal::open_with(small_cfg(), fs.clone()).expect("open should succeed")
    }

    // --- open: empty directory (R5.2) --------------------------------------

    #[test]
    fn open_empty_directory_initializes_empty_log() {
        let fs = MemFileSystem::new();
        let wal = open(&fs);
        assert_eq!(wal.last_index(), None);
        assert_eq!(wal.commit_index(), None);
        // Every query on an empty log is out of range, without error.
        assert_eq!(wal.term_at(0), None);
        assert_eq!(wal.entry(0), None);
    }

    #[test]
    fn open_creates_and_locks_the_data_directory() {
        let fs = MemFileSystem::new();
        let _wal = open(&fs);
        assert!(fs.exists(Path::new(DIR)));
    }

    // --- durable hard state: persist, reopen, restore (R9, R10) ------------

    #[test]
    fn fresh_log_reports_default_hard_state() {
        let fs = MemFileSystem::new();
        let wal = open(&fs);
        // A fresh durable log always reports a hard state, equal to the default
        // (term 0, no vote) — never `None` (Requirements 10.1, 10.2).
        assert_eq!(wal.hard_state(), Some(HardState::default()));
    }

    #[test]
    fn persist_hard_state_is_observable_in_memory() {
        let fs = MemFileSystem::new();
        let mut wal = open(&fs);
        let hs = HardState {
            current_term: 5,
            voted_for: Some(3),
        };
        wal.persist_hard_state(hs).unwrap();
        // The in-memory mirror reflects the persisted value immediately.
        assert_eq!(wal.hard_state(), Some(hs));
    }

    #[test]
    fn hard_state_restores_exact_value_after_reopen() {
        let fs = MemFileSystem::new();
        let hs = HardState {
            current_term: 9,
            voted_for: Some(2),
        };
        {
            let mut wal = open(&fs);
            wal.persist_hard_state(hs).unwrap();
        } // drop releases the directory lock; the manifest slot is durable.

        // Reopening the same filesystem restores the exact persisted value.
        let reopened = open(&fs);
        assert_eq!(reopened.hard_state(), Some(hs));
    }

    #[test]
    fn hard_state_restores_term_advance_without_a_vote_after_reopen() {
        let fs = MemFileSystem::new();
        let hs = HardState {
            current_term: 12,
            voted_for: None,
        };
        {
            let mut wal = open(&fs);
            wal.persist_hard_state(hs).unwrap();
        }

        let reopened = open(&fs);
        assert_eq!(reopened.hard_state(), Some(hs));
    }

    #[test]
    fn last_persisted_hard_state_wins_after_reopen() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open(&fs);
            // A sequence of persists: a vote, a term advance, then a new vote.
            wal.persist_hard_state(HardState {
                current_term: 1,
                voted_for: Some(1),
            })
            .unwrap();
            wal.persist_hard_state(HardState {
                current_term: 2,
                voted_for: None,
            })
            .unwrap();
            wal.persist_hard_state(HardState {
                current_term: 2,
                voted_for: Some(4),
            })
            .unwrap();
        }

        // Only the last persisted value survives the reopen.
        let reopened = open(&fs);
        assert_eq!(
            reopened.hard_state(),
            Some(HardState {
                current_term: 2,
                voted_for: Some(4),
            })
        );
    }

    #[test]
    fn hard_state_persists_alongside_committed_records() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open(&fs);
            wal.append(payload(0), 1).unwrap();
            wal.append(payload(1), 1).unwrap();
            wal.commit(1).unwrap();
            wal.persist_hard_state(HardState {
                current_term: 7,
                voted_for: Some(1),
            })
            .unwrap();
        }

        // Persisting hard state does not disturb the committed log extent, and
        // the hard state is recovered alongside it.
        let reopened = open(&fs);
        assert_eq!(reopened.last_index(), Some(1));
        assert_eq!(reopened.commit_index(), Some(1));
        assert_eq!(
            reopened.hard_state(),
            Some(HardState {
                current_term: 7,
                voted_for: Some(1),
            })
        );
    }

    // --- append: index assignment and disk round-trip (R1.3, R8.5) ---------

    #[test]
    fn append_assigns_sequential_zero_based_indices() {
        let fs = MemFileSystem::new();
        let mut wal = open(&fs);
        assert_eq!(wal.append(payload(0), 1).unwrap(), 0);
        assert_eq!(wal.append(payload(1), 1).unwrap(), 1);
        assert_eq!(wal.append(payload(2), 2).unwrap(), 2);
        assert_eq!(wal.last_index(), Some(2));
        assert_eq!(wal.term_at(0), Some(1));
        assert_eq!(wal.term_at(2), Some(2));
    }

    #[test]
    fn append_then_entry_reads_the_payload_back_from_disk() {
        let fs = MemFileSystem::new();
        let mut wal = open(&fs);
        wal.append(payload(7), 3).unwrap();
        let got = wal.entry(0).expect("entry present");
        assert_eq!(got.index, 0);
        assert_eq!(got.term, 3);
        assert_eq!(got.payload, payload(7));
    }

    #[test]
    fn term_at_and_entry_are_none_out_of_range() {
        let fs = MemFileSystem::new();
        let mut wal = open(&fs);
        wal.append(payload(0), 1).unwrap();
        assert_eq!(wal.term_at(1), None);
        assert_eq!(wal.entry(5), None);
    }

    // --- reopen-after-append round trip (R5.2, R4.1, R4.11) ----------------

    #[test]
    fn reopen_after_append_recovers_index_and_entries() {
        let fs = MemFileSystem::new();
        let terms = [1u64, 1, 2, 3, 5];
        {
            let mut wal = open(&fs);
            for (i, term) in terms.iter().enumerate() {
                assert_eq!(wal.append(payload(i as u8), *term).unwrap(), i as u64);
            }
            assert_eq!(wal.last_index(), Some(4));
        } // dropping the WAL releases the directory lock

        // Reopen on the same in-memory filesystem: the index and every payload
        // are reconstructed from the durably-acknowledged frames.
        let reopened = open(&fs);
        assert_eq!(reopened.last_index(), Some(4));
        for (i, term) in terms.iter().enumerate() {
            let index = i as u64;
            assert_eq!(reopened.term_at(index), Some(*term), "term at {index}");
            let entry = reopened.entry(index).expect("entry recoverable");
            assert_eq!(entry.index, index);
            assert_eq!(entry.term, *term);
            assert_eq!(entry.payload, payload(i as u8));
        }
        // Nothing beyond the recovered tail, and appends continue from there.
        assert_eq!(reopened.entry(5), None);
        let mut reopened = reopened;
        assert_eq!(reopened.append(payload(9), 8).unwrap(), 5);
        assert_eq!(reopened.last_index(), Some(5));
    }

    // --- commit durability across reopen (R4.4) ----------------------------

    #[test]
    fn commit_advances_within_bounds_and_persists_across_reopen() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open(&fs);
            for i in 0..3 {
                wal.append(payload(i), 1).unwrap();
            }
            // Out-of-bounds commit is rejected and leaves the commit unchanged.
            assert!(matches!(
                wal.commit(3),
                Err(LogError::CommitOutOfBounds { .. })
            ));
            assert_eq!(wal.commit_index(), None);
            wal.commit(1).unwrap();
            assert_eq!(wal.commit_index(), Some(1));
        }
        // The forced commit index survives the reopen (R9.1).
        let reopened = open(&fs);
        assert_eq!(reopened.commit_index(), Some(1));
        assert_eq!(reopened.last_index(), Some(2));
    }

    // --- config / lock at open (R11.5, R11.8) ------------------------------

    #[test]
    fn invalid_config_is_rejected_before_touching_the_filesystem() {
        let fs = MemFileSystem::new();
        let result = DurableWal::open_with(WalConfig::new(DIR).with_segment_size(0), fs.clone());
        assert!(matches!(result, Err(LogError::Config { .. })));
        // No partial initialization: the directory was never created.
        assert!(!fs.exists(Path::new(DIR)));
    }

    #[test]
    fn second_open_is_refused_while_the_directory_is_locked() {
        let fs = MemFileSystem::new();
        let _wal = open(&fs);
        let result = DurableWal::open_with(small_cfg(), fs.clone());
        assert!(matches!(result, Err(LogError::Io { op, .. }) if op == "lock data directory"));
    }

    // --- failure restores pre-op state (R10.3, R4.5) -----------------------

    #[test]
    fn append_failure_under_always_leaves_reported_state_unchanged() {
        let fs = MemFileSystem::new();
        let mut wal = open(&fs);
        // Fail the next fsync, which is the frame's forced flush during append.
        fs.arm_next_fsync_failure();
        let err = wal.append(payload(0), 1).unwrap_err();
        assert!(matches!(err, LogError::Io { .. }));
        // The in-memory index was not advanced: observable state is unchanged.
        assert_eq!(wal.last_index(), None);
        assert_eq!(wal.commit_index(), None);
        assert_eq!(wal.term_at(0), None);
    }

    #[test]
    fn reopen_after_a_failed_first_append_recovers_an_empty_log() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open(&fs);
            // The first append writes the frame bytes, then fails forcing them:
            // it returns `Io` before the manifest acknowledges anything, so the
            // frame on disk is unacknowledged.
            fs.arm_next_fsync_failure();
            assert!(matches!(
                wal.append(payload(0), 1),
                Err(LogError::Io { .. })
            ));
            assert_eq!(wal.last_index(), None);
        } // drop releases the directory lock

        // Reopen: under `Always` the unacknowledged frame is discarded, the
        // recovered log is empty, and its torn orphan segment is reclaimed so a
        // fresh append starts cleanly at index 0 (R10.3, R6.7).
        let mut reopened = open(&fs);
        assert_eq!(reopened.last_index(), None);
        assert_eq!(reopened.commit_index(), None);
        assert_eq!(reopened.append(payload(1), 2).unwrap(), 0);
        assert_eq!(reopened.last_index(), Some(0));
        assert_eq!(reopened.entry(0).unwrap().payload, payload(1));
    }

    // --- read: bounds and clamping (R8.3, R8.4) ----------------------------

    /// Append `count` entries whose payload tag and term both encode their
    /// index, so a read result can be matched back to the indices requested.
    fn open_with_entries(fs: &MemFileSystem, count: u8) -> DurableWal<MemFileSystem> {
        let mut wal = open(fs);
        for i in 0..count {
            assert_eq!(wal.append(payload(i), 1).unwrap(), i as u64);
        }
        wal
    }

    /// The indices carried by a slice of entries, for terse range assertions.
    fn indices(entries: &[LogEntry]) -> Vec<u64> {
        entries.iter().map(|e| e.index).collect()
    }

    #[test]
    fn read_on_empty_log_is_empty() {
        let fs = MemFileSystem::new();
        let wal = open(&fs);
        assert!(wal.read(0, 10).is_empty());
        assert!(wal.read(0, 0).is_empty());
    }

    #[test]
    fn read_with_start_after_end_is_empty() {
        let fs = MemFileSystem::new();
        let wal = open_with_entries(&fs, 5);
        // start > end yields empty, not an error (R8.4) — matching InMemoryLog.
        assert!(wal.read(4, 2).is_empty());
        assert!(wal.read(3, 0).is_empty());
    }

    #[test]
    fn read_full_range_returns_every_entry_in_ascending_order() {
        let fs = MemFileSystem::new();
        let wal = open_with_entries(&fs, 5); // indices 0..=4
        let got = wal.read(0, 4);
        assert_eq!(indices(&got), vec![0, 1, 2, 3, 4]);
        // Each payload was read back from disk, not just the index metadata.
        for (i, entry) in got.iter().enumerate() {
            assert_eq!(entry.payload, payload(i as u8));
        }
        // A range wider than the log clamps to the stored extent on both ends.
        assert_eq!(indices(&wal.read(0, 100)), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn read_partial_overlap_clamps_to_the_retained_range() {
        let fs = MemFileSystem::new();
        let wal = open_with_entries(&fs, 5); // indices 0..=4
                                             // Interior sub-range is returned exactly.
        assert_eq!(indices(&wal.read(1, 3)), vec![1, 2, 3]);
        // Upper bound beyond last_index clamps down to it (R8.3).
        assert_eq!(indices(&wal.read(3, 100)), vec![3, 4]);
        // A single-index range.
        assert_eq!(indices(&wal.read(2, 2)), vec![2]);
    }

    #[test]
    fn read_range_entirely_above_last_index_is_empty() {
        let fs = MemFileSystem::new();
        let wal = open_with_entries(&fs, 3); // indices 0..=2
        assert!(wal.read(3, 10).is_empty());
        assert!(wal.read(5, 5).is_empty());
    }

    // --- entry: present vs out-of-range (R8.1, R13.3) ----------------------

    #[test]
    fn entry_present_reads_payload_and_out_of_range_is_none() {
        let fs = MemFileSystem::new();
        let wal = open_with_entries(&fs, 3); // indices 0..=2
        let got = wal.entry(1).expect("index 1 retained");
        assert_eq!(got.index, 1);
        assert_eq!(got.payload, payload(1));
        // Above last_index is None without error (R8.1).
        assert_eq!(wal.entry(3), None);
        assert_eq!(wal.entry(u64::MAX), None);
    }

    // --- snapshot: None vs committed prefix (R8.6, R8.7) -------------------

    #[test]
    fn snapshot_is_empty_when_nothing_is_committed() {
        let fs = MemFileSystem::new();
        let wal = open_with_entries(&fs, 3);
        // commit_index None → empty snapshot with a None commit (R8.7).
        let snap = wal.snapshot();
        assert_eq!(snap.commit_index, None);
        assert!(snap.entries.is_empty());
    }

    #[test]
    fn snapshot_returns_the_committed_prefix_read_from_disk() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4
        wal.commit(2).unwrap();
        // Entries 0..=commit_index in ascending order (R8.6).
        let snap = wal.snapshot();
        assert_eq!(snap.commit_index, Some(2));
        assert_eq!(indices(&snap.entries), vec![0, 1, 2]);
        // Payloads round-trip from disk, not just the index metadata.
        for (i, entry) in snap.entries.iter().enumerate() {
            assert_eq!(entry.payload, payload(i as u8));
        }
    }

    // --- append_entries: reconciliation (R1.4, R1.5, R1.6, R4.1, R4.11) ----

    /// A batch of contiguous entries `[from, from + terms.len())` whose payload
    /// tag and term both encode their index, so a reopened/extended log can be
    /// matched back to exactly the entries written.
    fn batch(from: u64, terms: &[u64]) -> Vec<LogEntry> {
        terms
            .iter()
            .enumerate()
            .map(|(offset, &term)| {
                let index = from + offset as u64;
                LogEntry {
                    index,
                    term,
                    payload: payload(index as u8),
                }
            })
            .collect()
    }

    #[test]
    fn append_entries_empty_batch_is_a_noop() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 3); // indices 0..=2
        wal.append_entries(&[]).unwrap();
        // The log is untouched: same last index, same entries.
        assert_eq!(wal.last_index(), Some(2));
        assert_eq!(indices(&wal.read(0, 10)), vec![0, 1, 2]);
    }

    #[test]
    fn append_entries_extends_at_the_end_of_the_log() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 3); // indices 0..=2
        wal.append_entries(&batch(3, &[4, 4])).unwrap();
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.term_at(3), Some(4));
        assert_eq!(wal.term_at(4), Some(4));
        // Payloads round-trip from disk for the appended entries.
        assert_eq!(wal.entry(4).unwrap().payload, payload(4));
    }

    #[test]
    fn append_entries_on_empty_log_appends_from_index_zero() {
        let fs = MemFileSystem::new();
        let mut wal = open(&fs);
        wal.append_entries(&batch(0, &[1, 1, 2])).unwrap();
        assert_eq!(wal.last_index(), Some(2));
        assert_eq!(indices(&wal.read(0, 10)), vec![0, 1, 2]);
    }

    #[test]
    fn append_entries_overwrites_the_uncommitted_suffix() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4, terms all 1
                                                 // Overwrite from index 2 with a shorter, higher-term batch.
        wal.append_entries(&batch(2, &[7, 7])).unwrap();
        // The suffix at/above 2 is replaced; the new tail ends at 3.
        assert_eq!(wal.last_index(), Some(3));
        assert_eq!(wal.term_at(1), Some(1), "prefix below start is retained");
        assert_eq!(
            wal.term_at(2),
            Some(7),
            "overwritten entry has the new term"
        );
        assert_eq!(wal.term_at(3), Some(7));
        // The old index 4 is gone.
        assert_eq!(wal.entry(4), None);
        // The overwritten payloads read back from disk as the new values.
        assert_eq!(wal.entry(2).unwrap().payload, payload(2));
        assert_eq!(wal.entry(3).unwrap().payload, payload(3));
    }

    #[test]
    fn append_entries_rejects_a_gap_beyond_the_log() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 3); // indices 0..=2, next is 3
                                                 // Starting at 4 leaves a gap at 3.
        assert!(matches!(
            wal.append_entries(&batch(4, &[1])),
            Err(LogError::NonContiguousEntries)
        ));
        // The log is left unchanged.
        assert_eq!(wal.last_index(), Some(2));
        assert_eq!(indices(&wal.read(0, 10)), vec![0, 1, 2]);
    }

    #[test]
    fn append_entries_rejects_an_internally_noncontiguous_batch() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 2); // indices 0..=1
        let bad = vec![
            LogEntry {
                index: 2,
                term: 1,
                payload: payload(2),
            },
            // Skips index 3.
            LogEntry {
                index: 4,
                term: 1,
                payload: payload(4),
            },
        ];
        assert!(matches!(
            wal.append_entries(&bad),
            Err(LogError::NonContiguousEntries)
        ));
        assert_eq!(wal.last_index(), Some(1));
    }

    #[test]
    fn append_entries_rejects_overwrite_at_or_below_commit() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4
        wal.commit(2).unwrap();
        // Starting at the commit index (2) would overwrite a committed entry.
        assert!(matches!(
            wal.append_entries(&batch(2, &[9])),
            Err(LogError::NonContiguousEntries)
        ));
        // Starting below the commit index is likewise rejected.
        assert!(matches!(
            wal.append_entries(&batch(1, &[9])),
            Err(LogError::NonContiguousEntries)
        ));
        // The log and commit are left unchanged.
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.commit_index(), Some(2));
        // An overwrite strictly above the commit index is still allowed.
        wal.append_entries(&batch(3, &[8, 8])).unwrap();
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.term_at(3), Some(8));
        assert_eq!(wal.term_at(4), Some(8));
    }

    #[test]
    fn append_entries_overwrite_survives_reopen_dropping_old_entries() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open(&fs);
            // Five entries, each (with the tiny segment size) in its own segment.
            wal.append_entries(&batch(0, &[1, 1, 1, 1, 1])).unwrap();
            assert_eq!(wal.last_index(), Some(4));
            // Overwrite the suffix from index 2 with two higher-term entries.
            wal.append_entries(&batch(2, &[7, 7])).unwrap();
            assert_eq!(wal.last_index(), Some(3));
        } // drop releases the lock

        // Reopen: the overwritten (old) entries 2..=4 must NOT come back, and
        // the new 2..=3 must be exactly what was written (R1.5, R5.3).
        let reopened = open(&fs);
        assert_eq!(reopened.last_index(), Some(3));
        assert_eq!(reopened.term_at(0), Some(1));
        assert_eq!(reopened.term_at(1), Some(1));
        assert_eq!(reopened.term_at(2), Some(7), "new term recovered, not old");
        assert_eq!(reopened.term_at(3), Some(7));
        assert_eq!(reopened.entry(4), None, "old tail entry is gone");
        // Payloads round-trip from disk after recovery.
        for index in 0..=3u64 {
            assert_eq!(reopened.entry(index).unwrap().payload, payload(index as u8));
        }
    }

    #[test]
    fn append_entries_full_overwrite_from_log_start() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open(&fs);
            wal.append_entries(&batch(0, &[1, 1, 1])).unwrap();
            // Overwrite the entire log from index 0.
            wal.append_entries(&batch(0, &[9, 9])).unwrap();
            assert_eq!(wal.last_index(), Some(1));
            assert_eq!(wal.term_at(0), Some(9));
        }
        let reopened = open(&fs);
        assert_eq!(reopened.last_index(), Some(1));
        assert_eq!(reopened.term_at(0), Some(9));
        assert_eq!(reopened.term_at(1), Some(9));
        assert_eq!(reopened.entry(2), None);
    }

    // --- read fail-stop on an injected I/O error (R10.4, Property 5) -------

    #[test]
    fn read_io_error_fails_stop_instead_of_reporting_absence() {
        let fs = MemFileSystem::new();
        // One entry in segment 0; arm a read failure for that segment file.
        let wal = open_with_entries(&fs, 1);
        let segment_path = Path::new(DIR).join(super::segment_file_name(0));
        fs.arm_read_failure_for(&segment_path);

        // `entry` must fail-stop (panic), never return `None`, when the
        // underlying read errors (R10.4).
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wal.entry(0)));
        assert!(
            result.is_err(),
            "a read I/O error must fail-stop, not return"
        );
        // The log records the poison so a later mutating op refuses to run.
        assert!(wal.poisoned.get(), "a read fault must poison the log");
    }

    #[test]
    fn range_read_io_error_fails_stop() {
        let fs = MemFileSystem::new();
        let wal = open_with_entries(&fs, 1);
        let segment_path = Path::new(DIR).join(super::segment_file_name(0));
        fs.arm_read_failure_for(&segment_path);

        // `read` funnels through the same fail-stop path, so it must panic
        // rather than return an empty (or short) collection (R10.4).
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wal.read(0, 0)));
        assert!(result.is_err(), "a range read I/O error must fail-stop");
        assert!(wal.poisoned.get());
    }

    // --- revert: durability + reopen (R1.8, R9.2, R9.3, R9.4, R9.5, R9.6) --

    #[test]
    fn revert_below_commit_is_rejected_and_leaves_state_unchanged() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4
        wal.commit(2).unwrap();
        // Reverting below the commit index is rejected (R1.8 / R9.4).
        assert!(matches!(
            wal.revert(1),
            Err(LogError::RevertBelowCommit { requested: 1, .. })
        ));
        // Entries, last_index, and commit are all unchanged on rejection.
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.commit_index(), Some(2));
        assert_eq!(indices(&wal.read(0, 10)), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn revert_below_log_start_is_rejected() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4
                                                 // Model a compacted prefix by advancing the in-memory log start while
                                                 // leaving commit_index None, so the log-start guard is exercised in
                                                 // isolation from the commit guard (full Compaction lands in task 13).
        wal.index.compact_to(2); // retained 2..=4, log_start_index = 2
        assert!(matches!(
            wal.revert(1),
            Err(LogError::RevertBelowCommit { requested: 1, .. })
        ));
        // State unchanged: the retained range and (absent) commit are intact.
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.commit_index(), None);
    }

    #[test]
    fn revert_at_or_above_last_index_is_a_noop() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 3); // indices 0..=2
                                                 // At last_index there is nothing strictly above to remove.
        wal.revert(2).unwrap();
        assert_eq!(wal.last_index(), Some(2));
        // Above last_index is likewise a no-op (matching InMemoryLog's truncate).
        wal.revert(100).unwrap();
        assert_eq!(wal.last_index(), Some(2));
        assert_eq!(indices(&wal.read(0, 10)), vec![0, 1, 2]);
    }

    #[test]
    fn revert_on_empty_log_is_a_noop() {
        let fs = MemFileSystem::new();
        let mut wal = open(&fs);
        wal.revert(0).unwrap();
        wal.revert(5).unwrap();
        assert_eq!(wal.last_index(), None);
        assert_eq!(wal.commit_index(), None);
    }

    #[test]
    fn revert_then_append_continues_after_the_revert_point() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4
        wal.revert(2).unwrap(); // keep 0..=2
        assert_eq!(wal.last_index(), Some(2));
        // The next append takes index 3 (revert point + 1), reusing the
        // truncated segment file rather than reusing any dropped index.
        assert_eq!(wal.append(payload(9), 7).unwrap(), 3);
        assert_eq!(wal.last_index(), Some(3));
        assert_eq!(wal.term_at(3), Some(7));
        assert_eq!(wal.entry(3).unwrap().payload, payload(9));
        // The originally-appended index-3 payload is gone, replaced by the new.
        assert_eq!(wal.entry(4), None);
    }

    #[test]
    fn revert_removes_uncommitted_suffix_and_survives_reopen() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open(&fs);
            for i in 0..5u8 {
                assert_eq!(wal.append(payload(i), 1).unwrap(), i as u64);
            }
            wal.commit(1).unwrap();
            // Revert removes entries strictly above index 2, keeping 0..=2.
            wal.revert(2).unwrap();
            assert_eq!(wal.last_index(), Some(2));
            assert_eq!(wal.commit_index(), Some(1));
            // The reverted entries are gone immediately.
            assert_eq!(wal.entry(3), None);
            assert_eq!(wal.entry(4), None);
        } // drop releases the directory lock

        // Reopen on the same filesystem: the reverted entries must NOT return,
        // last_index is the revert point, and the retained entries read cleanly
        // from disk with no frame/metadata mismatch (R9.2, R9.3).
        let reopened = open(&fs);
        assert_eq!(reopened.last_index(), Some(2));
        assert_eq!(reopened.commit_index(), Some(1));
        for index in 0..=2u64 {
            let entry = reopened.entry(index).expect("retained entry recoverable");
            assert_eq!(entry.index, index);
            assert_eq!(entry.payload, payload(index as u8));
        }
        assert_eq!(reopened.entry(3), None, "reverted entry must not return");
        assert_eq!(reopened.entry(4), None);
        // Appends after reopen continue at the revert point + 1.
        let mut reopened = reopened;
        assert_eq!(reopened.append(payload(9), 4).unwrap(), 3);
        assert_eq!(reopened.last_index(), Some(3));
    }

    // --- compaction (R7, R8.5, R13.5) --------------------------------------

    /// A config with a segment large enough to hold every appended frame in a
    /// single segment, so compaction to an interior index leaves a *straddling*
    /// segment whose below-line frames stay on disk but are skipped.
    fn wide_cfg() -> WalConfig {
        WalConfig::new(DIR).with_segment_size(4096)
    }

    /// Open over `fs` with the wide (single-segment) config.
    fn open_wide(fs: &MemFileSystem) -> DurableWal<MemFileSystem> {
        DurableWal::open_with(wide_cfg(), fs.clone()).expect("open should succeed")
    }

    /// Path of the segment file with the given base index, for existence checks.
    fn segment_path(base: u64) -> std::path::PathBuf {
        Path::new(DIR).join(super::segment_file_name(base))
    }

    #[test]
    fn compaction_reclaims_whole_below_line_segments() {
        let fs = MemFileSystem::new();
        // Tiny segments: indices 0..=4 each land in their own segment 0..=4.
        let mut wal = open_with_entries(&fs, 5);
        wal.commit(4).unwrap();
        // Every segment file exists before compaction.
        for base in 0..=4u64 {
            assert!(fs.exists(&segment_path(base)), "segment {base} present");
        }

        // Retain from index 2: segments 0 and 1 are wholly below it and removed;
        // segment 2 holds the retained boundary frame and stays.
        wal.compaction(2).unwrap();
        assert_eq!(wal.log_start_index(), 2);

        // Whole below-line segment files are gone; the rest remain (R7.5).
        assert!(!fs.exists(&segment_path(0)), "segment 0 reclaimed");
        assert!(!fs.exists(&segment_path(1)), "segment 1 reclaimed");
        for base in 2..=4u64 {
            assert!(fs.exists(&segment_path(base)), "segment {base} retained");
        }

        // Retained entries still read back with their payloads; discarded ones
        // are absent without error (R7.6, R8.1).
        assert_eq!(wal.entry(0), None);
        assert_eq!(wal.entry(1), None);
        for index in 2..=4u64 {
            let entry = wal.entry(index).expect("retained entry readable");
            assert_eq!(entry.index, index);
            assert_eq!(entry.payload, payload(index as u8));
        }
        // last_index and commit are unchanged by compaction (R7.6).
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.commit_index(), Some(4));
    }

    #[test]
    fn compaction_partial_segment_skips_below_line_frames_on_read_and_reopen() {
        let fs = MemFileSystem::new();
        {
            // One wide segment holds indices 0..=4.
            let mut wal = open_wide(&fs);
            for i in 0..5u8 {
                assert_eq!(wal.append(payload(i), 1).unwrap(), i as u64);
            }
            wal.commit(4).unwrap();

            // Compact to an index *inside* the single (active) segment: the
            // segment file must stay, since it still holds retained frames.
            wal.compaction(2).unwrap();
            assert_eq!(wal.log_start_index(), 2);
            assert!(
                fs.exists(&segment_path(0)),
                "straddling segment is not removed"
            );
            // Below-line frames are skipped on read (R7.5); retained ones read.
            assert_eq!(wal.entry(0), None);
            assert_eq!(wal.entry(1), None);
            assert_eq!(wal.entry(2).unwrap().payload, payload(2));
            assert_eq!(wal.entry(4).unwrap().payload, payload(4));
        } // drop releases the directory lock

        // Reopen: log_start is preserved, the below-line frames physically left
        // inside the straddling segment are skipped on recovery, and the
        // retained range reads cleanly from disk (R7.5, R7.7).
        let reopened = DurableWal::open_with(wide_cfg(), fs.clone()).unwrap();
        assert_eq!(reopened.log_start_index(), 2);
        assert_eq!(reopened.last_index(), Some(4));
        assert_eq!(reopened.commit_index(), Some(4));
        assert_eq!(
            reopened.entry(0),
            None,
            "below-line frame skipped on reopen"
        );
        assert_eq!(reopened.entry(1), None);
        for index in 2..=4u64 {
            assert_eq!(reopened.entry(index).unwrap().payload, payload(index as u8));
        }
    }

    #[test]
    fn compaction_at_or_below_log_start_is_a_noop_even_with_no_commit() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4, commit None
                                                 // log_start is 0 and nothing is committed: retained_point 0 discards
                                                 // nothing, so the no-op rule wins over the bounds rejection (R7.4 > R7.3).
        wal.compaction(0).unwrap();
        assert_eq!(wal.log_start_index(), 0);
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.commit_index(), None);

        // Model a previously-compacted log (log_start = 2) while leaving commit
        // None, so a retained_point at or below log_start is still a no-op even
        // though a positive point would otherwise be rejected (R7.4 > R7.3).
        wal.index.compact_to(2);
        wal.compaction(1).unwrap(); // below log_start
        wal.compaction(2).unwrap(); // equal to log_start
        assert_eq!(wal.log_start_index(), 2);
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.commit_index(), None);
    }

    #[test]
    fn compaction_above_log_start_is_rejected_without_a_commit() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // commit None
        let err = wal.compaction(2).unwrap_err();
        assert!(matches!(
            err,
            LogError::CompactionOutOfBounds {
                requested: 2,
                commit: None,
                log_start: 0
            }
        ));
        // The log is left unchanged on rejection (R7.3).
        assert_eq!(wal.log_start_index(), 0);
        assert_eq!(wal.last_index(), Some(4));
        for base in 0..=4u64 {
            assert!(fs.exists(&segment_path(base)), "segment {base} untouched");
        }
    }

    #[test]
    fn compaction_past_the_commit_index_is_rejected() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // indices 0..=4
        wal.commit(2).unwrap();
        // A retained_point above the commit index would discard the commit entry
        // or an uncommitted entry: rejected, log unchanged (R7.3).
        for point in [3u64, 4] {
            let err = wal.compaction(point).unwrap_err();
            assert!(matches!(
                err,
                LogError::CompactionOutOfBounds { commit, log_start: 0, .. }
                    if commit == Some(2)
            ));
        }
        assert_eq!(wal.log_start_index(), 0);
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.commit_index(), Some(2));

        // Retaining exactly from the commit index keeps the commit entry and is
        // therefore valid (R7.2).
        wal.compaction(2).unwrap();
        assert_eq!(wal.log_start_index(), 2);
        assert_eq!(wal.entry(2).unwrap().payload, payload(2));
        assert_eq!(wal.commit_index(), Some(2));
    }

    #[test]
    fn compaction_reopened_log_start_is_preserved_with_retained_entries() {
        let fs = MemFileSystem::new();
        {
            let mut wal = open_with_entries(&fs, 5); // tiny segments 0..=4
            wal.commit(4).unwrap();
            wal.compaction(3).unwrap(); // retain 3..=4
            assert_eq!(wal.log_start_index(), 3);
        } // drop releases the directory lock

        // Reopen: log_start, last_index, and commit survive; discarded entries
        // do not return; retained entries keep identical index/term/payload
        // (R7.7).
        let reopened = open(&fs);
        assert_eq!(reopened.log_start_index(), 3);
        assert_eq!(reopened.last_index(), Some(4));
        assert_eq!(reopened.commit_index(), Some(4));
        for below in [0u64, 1, 2] {
            assert_eq!(reopened.entry(below), None, "{below} discarded");
            assert_eq!(reopened.term_at(below), None);
        }
        for index in 3..=4u64 {
            let entry = reopened.entry(index).expect("retained entry recoverable");
            assert_eq!(entry.index, index);
            assert_eq!(entry.term, 1);
            assert_eq!(entry.payload, payload(index as u8));
        }
        // Appends after reopen continue at last_index + 1, never reusing a
        // discarded index (R8.5).
        let mut reopened = reopened;
        assert_eq!(reopened.append(payload(9), 7).unwrap(), 5);
        assert_eq!(reopened.last_index(), Some(5));
    }

    #[test]
    fn compaction_orphan_low_segments_are_ignored_on_reopen() {
        let fs = MemFileSystem::new();
        // Simulate a crash *between* the manifest fsync and the segment
        // deletions (R7.8): advance the durable log_start in the manifest while
        // leaving every below-line segment file on disk.
        {
            let mut wal = open_with_entries(&fs, 5); // tiny segments 0..=4
            wal.commit(4).unwrap();
            let crashed = ManifestState {
                log_start_index: 2,
                commit_index: Some(4),
                durable_last: wal.durable_last,
                durable_segment: wal.durable_segment,
                durable_offset: wal.durable_offset,
                hs_current_term: wal.hs_current_term,
                hs_voted_for: wal.hs_voted_for,
            };
            wal.manifest.write(&fs, &crashed).unwrap();
            // The low segment files 0 and 1 were never deleted.
            assert!(fs.exists(&segment_path(0)));
            assert!(fs.exists(&segment_path(1)));
        } // drop releases the directory lock

        // Reopen: the advanced log_start wins, and the orphaned below-line
        // segments are ignored — their frames are never restored (R7.8).
        let reopened = open(&fs);
        assert_eq!(reopened.log_start_index(), 2);
        assert_eq!(reopened.last_index(), Some(4));
        assert_eq!(reopened.commit_index(), Some(4));
        assert_eq!(reopened.entry(0), None, "orphan low segment ignored");
        assert_eq!(reopened.entry(1), None);
        for index in 2..=4u64 {
            assert_eq!(reopened.entry(index).unwrap().payload, payload(index as u8));
        }
    }

    #[test]
    fn compaction_manifest_write_failure_leaves_the_log_unchanged() {
        let fs = MemFileSystem::new();
        let mut wal = open_with_entries(&fs, 5); // tiny segments 0..=4
        wal.commit(4).unwrap();
        // Fail the manifest fsync that persists the advanced log_start: the
        // compaction must abort before advancing the index or deleting segments.
        fs.arm_next_fsync_failure();
        let err = wal.compaction(2).unwrap_err();
        assert!(matches!(err, LogError::Io { .. }));
        // Nothing changed: log_start, entries, and every segment file are intact.
        assert_eq!(wal.log_start_index(), 0);
        assert_eq!(wal.last_index(), Some(4));
        assert_eq!(wal.entry(0).unwrap().payload, payload(0));
        for base in 0..=4u64 {
            assert!(fs.exists(&segment_path(base)), "segment {base} not deleted");
        }
    }

    // --- Periodic / Never sync policies and flush (R4.2, R4.3, R4.5, R4.6,
    //     R4.7, R4.10) -----------------------------------------------------

    /// A `Periodic` config with `interval_ms` and a segment large enough to hold
    /// every appended frame in one active segment (so a force covers them all).
    fn periodic_cfg(interval_ms: u64) -> WalConfig {
        WalConfig::new(DIR)
            .with_segment_size(4096)
            .with_sync_policy(SyncPolicy::Periodic { interval_ms })
    }

    /// A `Never` config with a tiny segment size (each frame in its own segment).
    fn never_cfg() -> WalConfig {
        WalConfig::new(DIR)
            .with_segment_size(40)
            .with_sync_policy(SyncPolicy::Never)
    }

    /// Open a `DurableWal` over `fs` with `cfg` and an injected [`TestClock`],
    /// returning both so the test can advance time while the log reads it.
    fn open_with_clock(
        fs: &MemFileSystem,
        cfg: WalConfig,
        clock: &TestClock,
    ) -> DurableWal<MemFileSystem, TestClock> {
        DurableWal::open_with_clock(cfg, fs.clone(), clock.clone()).expect("open should succeed")
    }

    // --- Periodic: operation-driven force cadence (R4.2) -------------------

    #[test]
    fn periodic_first_op_forces_then_defers_within_the_interval() {
        let fs = MemFileSystem::new();
        let clock = TestClock::new(); // t = 0
        let mut wal = open_with_clock(&fs, periodic_cfg(1000), &clock);

        // The first mutating op always forces (no prior force to pace against):
        // the acknowledged extent advances and the force time is recorded.
        wal.append(payload(0), 1).unwrap();
        assert_eq!(wal.durable_last, Some(0), "first op forces");
        assert_eq!(wal.last_force_at, Some(0));

        // Within the interval the force is deferred: the bytes are written to the
        // OS (the op succeeds and the entry is visible) but the extent does not
        // advance.
        clock.advance(500); // t = 500, gap 500 < 1000
        wal.append(payload(1), 1).unwrap();
        assert_eq!(wal.last_index(), Some(1), "entry is visible");
        assert_eq!(wal.durable_last, Some(0), "deferred: extent not advanced");

        clock.advance(400); // t = 900, gap 900 < 1000
        wal.append(payload(2), 1).unwrap();
        assert_eq!(wal.durable_last, Some(0), "still deferred");
        assert_eq!(wal.last_force_at, Some(0));
    }

    #[test]
    fn periodic_forces_again_once_the_interval_elapses() {
        let fs = MemFileSystem::new();
        let clock = TestClock::new();
        let mut wal = open_with_clock(&fs, periodic_cfg(1000), &clock);

        wal.append(payload(0), 1).unwrap(); // forces at t = 0
        clock.advance(500);
        wal.append(payload(1), 1).unwrap(); // deferred
        clock.advance(600); // t = 1100, gap from last force (0) is 1100 >= 1000
        wal.append(payload(2), 1).unwrap(); // forces

        assert_eq!(
            wal.durable_last,
            Some(2),
            "extent advances on the due force"
        );
        assert_eq!(wal.last_force_at, Some(1100));

        // The whole valid run is recoverable on reopen (under `Periodic`,
        // recovery keeps every valid frame regardless of the recorded extent).
        drop(wal);
        let reopened =
            DurableWal::open_with(periodic_cfg(1000), fs.clone()).expect("reopen should succeed");
        assert_eq!(reopened.last_index(), Some(2));
        for i in 0..=2u64 {
            assert_eq!(reopened.entry(i).unwrap().payload, payload(i as u8));
        }
    }

    #[test]
    fn periodic_commit_participates_in_the_force_cadence() {
        let fs = MemFileSystem::new();
        let clock = TestClock::new();
        let mut wal = open_with_clock(&fs, periodic_cfg(1000), &clock);

        wal.append(payload(0), 1).unwrap(); // forces at t = 0
        clock.advance(500);
        wal.append(payload(1), 1).unwrap(); // deferred
        clock.advance(600); // t = 1100, a force is due
                            // The commit invocation is part of the cadence: it forces the buffered
                            // frames and persists the commit.
        wal.commit(1).unwrap();
        assert_eq!(wal.last_force_at, Some(1100), "commit issued the due force");
        assert_eq!(wal.commit_index(), Some(1));

        // The forced commit survives reopen.
        drop(wal);
        let reopened =
            DurableWal::open_with(periodic_cfg(1000), fs.clone()).expect("reopen should succeed");
        assert_eq!(reopened.commit_index(), Some(1));
        assert_eq!(reopened.last_index(), Some(1));
    }

    // --- Periodic: a failed force surfaces on the next call (R4.6) ---------

    #[test]
    fn periodic_failed_force_surfaces_on_the_next_mutating_call() {
        let fs = MemFileSystem::new();
        let clock = TestClock::new();
        let mut wal = open_with_clock(&fs, periodic_cfg(1000), &clock);

        // Arm an fsync failure so the first op's (due) force fails. Under
        // `Periodic` the op still succeeds — the frame reached the OS — but the
        // failure is recorded, the extent does not advance, and the force time
        // is not updated (Requirement 4.6).
        fs.arm_next_fsync_failure();
        wal.append(payload(0), 1).unwrap();
        assert_eq!(wal.last_index(), Some(0), "the op still succeeds");
        assert_eq!(
            wal.durable_last, None,
            "the failed force did not advance the extent"
        );
        assert!(wal.pending_force_error, "the failure is recorded");
        assert_eq!(wal.last_force_at, None);

        // The next mutating call surfaces the recorded failure as `Io` and
        // clears it, doing no work of its own (the index is unchanged).
        let err = wal.append(payload(1), 1).unwrap_err();
        assert!(matches!(err, LogError::Io { .. }));
        assert!(
            !wal.pending_force_error,
            "the flag is cleared after surfacing"
        );
        assert_eq!(wal.last_index(), Some(0), "the surfacing call did no work");

        // A subsequent op proceeds normally and forces (no prior force recorded).
        wal.append(payload(1), 1).unwrap();
        assert_eq!(wal.last_index(), Some(1));
        assert_eq!(
            wal.durable_last,
            Some(1),
            "the retried force advances the extent"
        );
    }

    #[test]
    fn periodic_failed_force_surfaces_on_the_next_commit() {
        let fs = MemFileSystem::new();
        let clock = TestClock::new();
        let mut wal = open_with_clock(&fs, periodic_cfg(1000), &clock);

        fs.arm_next_fsync_failure();
        wal.append(payload(0), 1).unwrap(); // due force fails, recorded
        assert!(wal.pending_force_error);

        // `commit` is also a mutating call, so it surfaces the failure as `Io`
        // and leaves the commit unchanged.
        let err = wal.commit(0).unwrap_err();
        assert!(matches!(err, LogError::Io { .. }));
        assert_eq!(wal.commit_index(), None, "commit did not advance");
        assert!(!wal.pending_force_error);
    }

    // --- Never: writes go to the OS, never forced (R4.3) -------------------

    #[test]
    fn never_append_does_not_force_segment_data() {
        let fs = MemFileSystem::new();
        let mut wal = DurableWal::open_with(never_cfg(), fs.clone()).unwrap();

        // Arm a failure on every fsync of the first segment file. Under `Never`
        // the append performs no force, so the armed failure never fires and the
        // append succeeds.
        let seg0 = Path::new(DIR).join(super::segment_file_name(0));
        fs.arm_fsync_failure_for(&seg0);

        wal.append(payload(0), 1).unwrap();
        assert_eq!(wal.last_index(), Some(0));
        assert_eq!(
            wal.durable_last, None,
            "Never never advances the acknowledged extent"
        );
    }

    #[test]
    fn never_recovers_os_visible_data_and_committed_state_on_reopen() {
        let fs = MemFileSystem::new();
        {
            let mut wal = DurableWal::open_with(never_cfg(), fs.clone()).unwrap();
            for i in 0..3u8 {
                assert_eq!(wal.append(payload(i), 1).unwrap(), i as u64);
            }
            // `commit` records the commit in the manifest (metadata) so it
            // survives reopen, even though frame data is never force-flushed.
            wal.commit(1).unwrap();
            assert_eq!(wal.commit_index(), Some(1));
        } // drop releases the directory lock

        // Reopen: the OS-visible frames are recovered (the in-memory filesystem
        // persists every write) and the committed state survives.
        let reopened = DurableWal::open_with(never_cfg(), fs.clone()).unwrap();
        assert_eq!(reopened.last_index(), Some(2));
        assert_eq!(reopened.commit_index(), Some(1));
        for i in 0..3u64 {
            assert_eq!(reopened.entry(i).unwrap().payload, payload(i as u8));
        }
    }

    // --- flush forces buffered frames + manifest (R4.10) -------------------

    #[test]
    fn flush_forces_buffered_frames_and_leaves_reported_state_unchanged() {
        let fs = MemFileSystem::new();
        // `Never` so the appends themselves perform no force; all frames land in
        // one active segment (4096-byte segment), so `flush` forces them all.
        let cfg = WalConfig::new(DIR)
            .with_segment_size(4096)
            .with_sync_policy(SyncPolicy::Never);
        let mut wal = DurableWal::open_with(cfg, fs.clone()).unwrap();
        wal.append(payload(0), 1).unwrap();
        wal.append(payload(1), 1).unwrap();
        wal.commit(0).unwrap();

        let last_before = wal.last_index();
        let commit_before = wal.commit_index();
        wal.flush().expect("flush should force buffered frames");

        // `flush` does not change the reported retained entries, last index, or
        // commit index (Requirement 4.10).
        assert_eq!(wal.last_index(), last_before);
        assert_eq!(wal.commit_index(), commit_before);
        assert_eq!(indices(&wal.read(0, 10)), vec![0, 1]);
    }

    #[test]
    fn flush_maps_a_force_failure_to_io_and_leaves_state_unchanged() {
        let fs = MemFileSystem::new();
        let cfg = WalConfig::new(DIR)
            .with_segment_size(4096)
            .with_sync_policy(SyncPolicy::Never);
        let mut wal = DurableWal::open_with(cfg, fs.clone()).unwrap();
        wal.append(payload(0), 1).unwrap();
        wal.append(payload(1), 1).unwrap();

        // Fail the next fsync — the flush's active-segment force — and confirm
        // it surfaces as `Io` while leaving reported state unchanged.
        fs.arm_next_fsync_failure();
        let err = wal.flush().unwrap_err();
        assert!(matches!(err, LogError::Io { .. }));
        assert_eq!(wal.last_index(), Some(1));
        assert_eq!(wal.commit_index(), None);
        assert_eq!(indices(&wal.read(0, 10)), vec![0, 1]);
    }

    #[test]
    fn flush_makes_deferred_periodic_writes_durable() {
        let fs = MemFileSystem::new();
        let clock = TestClock::new();
        let mut wal = open_with_clock(&fs, periodic_cfg(1000), &clock);

        wal.append(payload(0), 1).unwrap(); // forces at t = 0
        clock.advance(100);
        wal.append(payload(1), 1).unwrap(); // deferred (gap 100 < 1000)
        assert_eq!(wal.durable_last, Some(0), "deferred before flush");

        // `flush` forces the buffered frame(s) and the manifest to stable
        // storage on demand, independent of the periodic cadence.
        wal.flush().expect("flush should succeed");

        // Reopen recovers the full valid run.
        drop(wal);
        let reopened =
            DurableWal::open_with(periodic_cfg(1000), fs.clone()).expect("reopen should succeed");
        assert_eq!(reopened.last_index(), Some(1));
        assert_eq!(reopened.entry(1).unwrap().payload, payload(1));
    }
}
