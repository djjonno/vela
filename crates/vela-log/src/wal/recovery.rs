//! Open-time recovery and replay.
//!
//! [`replay`] lists and orders the segment files by base index, skips frames
//! below `log_start_index` (a compacted prefix), sequentially validates the
//! frames, and rebuilds the in-memory index, `last_index`, `log_start_index`,
//! and `commit_index` (Requirement 5). It distinguishes a recoverable **torn
//! tail** (discard and succeed) from non-recoverable **interior corruption**
//! (a valid frame after a bad one → [`crate::LogError::Corruption`]), and
//! reconciles the recovered run against the manifest's durably-acknowledged
//! extent with policy-scoped handling of a shortfall (Requirement 6).
//!
//! # Torn tail vs interior corruption
//!
//! [`scan_segment`](super::segment::scan_segment) decodes a single segment's
//! frames in order and reports how it ended ([`ScanOutcome`]): cleanly, at an
//! incomplete (torn) frame, or at a corrupt (CRC-bad) frame. It **stops at the
//! first bad frame**, so a valid frame can only follow a bad one in a *later*
//! segment. `replay` therefore discriminates the two cases by looking across
//! the ordered segments: a bad frame is **interior corruption** iff any later
//! segment still holds a valid retained frame (R6.3); otherwise the bad frame
//! begins the **torn tail**, which is discarded down to the last valid frame
//! (R6.1, R6.2).
//!
//! # Acknowledged-extent reconciliation
//!
//! The manifest's acknowledged extent (`durable_last`) is the high-water mark
//! the WAL has promised is durable. Under [`Always`](super::config::SyncPolicy::Always):
//!
//! - the recovered run **must reach** `durable_last`; a shortfall below it is
//!   `Corruption`, never a silent truncation (R6.4); and
//! - any valid frame **beyond** `durable_last` is unacknowledged — a crash
//!   between a frame fsync and its covering manifest fsync, for which `append`
//!   never returned success — so it is discarded as a torn tail (Property 1,
//!   R6.7).
//!
//! Under [`Periodic`](super::config::SyncPolicy::Periodic) and
//! [`Never`](super::config::SyncPolicy::Never) the acknowledged extent is a
//! lagging hint, not a floor or a ceiling: the full valid run is kept and a
//! tail shortfall is accepted (R6.5). Interior corruption still fails under
//! every policy.

use super::config::SyncPolicy;
use super::fs::FileSystem;
use super::index::{IndexEntry, LogIndex};
use super::manifest::ManifestState;
use super::segment::{ordered_segments, scan_segment, FrameLocation, ScanOutcome};
use crate::{LogError, WalConfig};

/// The in-memory state reconstructed by [`replay`].
///
/// The `durable_*` fields mirror the recovered acknowledged extent — the
/// location of the last retained frame — which `DurableWal` keeps in memory and
/// which seeds [`SegmentSet::restore`](super::segment::SegmentSet::restore) so a
/// subsequent append continues in the correct active segment.
#[derive(Debug)]
pub(crate) struct Recovered {
    /// The rebuilt in-memory index.
    pub index: LogIndex,
    /// The commit index, clamped to be no greater than the last retained index
    /// (Requirement 5.4).
    pub commit_index: Option<u64>,
    /// Highest recovered (acknowledged) index, or `None` when nothing remains.
    pub durable_last: Option<u64>,
    /// Base index of the segment holding `durable_last`'s frame.
    pub durable_segment: u64,
    /// Byte offset just past `durable_last`'s frame within its segment.
    pub durable_offset: u64,
}

/// Reconstruct the in-memory log state from the segment files on disk
/// (Requirements 5, 6).
///
/// This is **read-only**: it scans the segments and classifies them, returning
/// [`LogError::Corruption`] for interior corruption or an acknowledged-extent
/// shortfall (under `Always`) and [`LogError::Io`] for a filesystem failure,
/// without modifying any segment. The physical truncation of a discarded torn
/// tail happens afterwards, only on success, when `DurableWal::open`
/// reconstructs the [`SegmentSet`](super::segment::SegmentSet).
pub(crate) fn replay<F: FileSystem>(
    fs: &F,
    cfg: &WalConfig,
    state: &ManifestState,
) -> Result<Recovered, LogError> {
    let log_start = state.log_start_index;
    let ack = state.durable_last;
    let always = cfg.sync_policy == SyncPolicy::Always;

    // Scan every segment once, in ascending base-index order (R5.1, R3.5). A
    // filesystem failure aborts recovery with `Io` and no partial init (R5.5).
    let ordered = ordered_segments(fs, &cfg.data_dir).map_err(|source| LogError::Io {
        op: "list segments",
        source,
    })?;
    let mut scans = Vec::with_capacity(ordered.len());
    for (base, path) in ordered {
        let scan = scan_segment(fs, &path).map_err(|source| LogError::Io {
            op: "scan segment",
            source,
        })?;
        scans.push((base, scan));
    }

    // Walk the scanned frames in order, building the contiguous valid run and
    // detecting interior corruption.
    let mut entries: Vec<IndexEntry> = Vec::new();
    let mut expected = log_start;
    let mut last_location: Option<FrameLocation> = None;

    'segments: for (si, (base, scan)) in scans.iter().enumerate() {
        let mut contributed = false;
        for framed in &scan.frames {
            let idx = framed.entry.index;
            // Skip a compacted prefix that still physically lives on disk inside
            // a retained or orphaned segment (Requirement 7.5).
            if idx < log_start {
                continue;
            }
            // A position mismatch is a hole in the retained run: corruption at
            // the position-EXPECTED index, not the frame's own index field
            // (Requirement 6.3).
            if idx != expected {
                return Err(LogError::Corruption {
                    index: expected,
                    detail: "non-contiguous frame during replay",
                });
            }
            let location = FrameLocation {
                segment: *base,
                offset: framed.offset,
                len: framed.len,
            };
            entries.push(IndexEntry::from_location(location, framed.entry.term));
            last_location = Some(location);
            expected += 1;
            contributed = true;
        }

        match scan.outcome {
            // A fully-consumed segment never breaks the run.
            ScanOutcome::Clean => continue,
            ScanOutcome::Incomplete { .. } | ScanOutcome::Corrupt { .. } => {
                // A torn segment that lies wholly within the discarded prefix
                // (below `log_start`, contributing no retained frame) is an
                // orphan and is ignored (Requirement 7.8).
                if !contributed && *base < log_start {
                    continue;
                }
                // Interior corruption iff any later segment still holds a valid
                // retained frame: `scan_segment` stops at the first bad frame,
                // so a "valid frame after" the break is detected across segments
                // (Requirement 6.3).
                let valid_after = scans[si + 1..]
                    .iter()
                    .any(|(_, s)| s.frames.iter().any(|f| f.entry.index >= log_start));
                if valid_after {
                    return Err(LogError::Corruption {
                        index: expected,
                        detail: "interior corruption: corrupt frame followed by a valid frame",
                    });
                }
                // No valid frame follows: this is the torn tail. Stop here and
                // discard the rest (Requirements 6.1, 6.2).
                break 'segments;
            }
        }
    }

    let mut recovered_last = (expected != log_start).then(|| expected - 1);

    // Reconcile the recovered run against the manifest's acknowledged extent.
    if always {
        match ack {
            Some(acked) => {
                // Never silently truncate below the acknowledged extent: if the
                // valid run cannot reach `ack`, that is a shortfall and recovery
                // fails rather than losing acknowledged data (Requirement 6.4).
                if recovered_last.is_none_or(|r| r < acked) {
                    return Err(LogError::Corruption {
                        index: acked,
                        detail: "recovered fewer frames than the acknowledged extent",
                    });
                }
                // Discard any valid-but-unacknowledged frames beyond `ack`: a
                // crash between a frame fsync and its covering manifest fsync
                // left a durable frame `append` never acknowledged, so it is
                // dropped as a torn tail (Property 1, Requirements 6.1, 6.7).
                if recovered_last.is_some_and(|r| r > acked) {
                    let keep = (acked - log_start) as usize + 1;
                    entries.truncate(keep);
                    recovered_last = Some(acked);
                    last_location = entries.last().map(IndexEntry::location);
                }
            }
            None => {
                // Nothing acknowledged: under `Always` every on-disk frame is an
                // unacknowledged (failed or torn) write, so none survives.
                entries.clear();
                recovered_last = None;
                last_location = None;
            }
        }
    }
    // Under `Periodic`/`Never` the acknowledged extent is a lagging hint, not a
    // floor or a ceiling: the full valid run is kept and a tail shortfall is
    // accepted (Requirement 6.5). Interior corruption already failed above,
    // under every policy.

    let index = LogIndex::from_entries(log_start, entries);

    // The recovered commit index can never exceed the last retained index
    // (Requirement 5.4); with nothing retained it collapses to `None`.
    let commit_index = state
        .commit_index
        .and_then(|commit| recovered_last.map(|last| commit.min(last)));

    let (durable_last, durable_segment, durable_offset) = match last_location {
        Some(loc) => (recovered_last, loc.segment, loc.offset + loc.len as u64),
        None => (None, 0, 0),
    };

    Ok(Recovered {
        index,
        commit_index,
        durable_last,
        durable_segment,
        durable_offset,
    })
}

#[cfg(test)]
mod tests {
    use super::super::config::{SyncPolicy, WalConfig};
    use super::super::fs::fault::MemFileSystem;
    use super::super::fs::{FileSystem, WalFile};
    use super::super::manifest::{Manifest, ManifestState};
    use super::super::segment::{ordered_segments, segment_file_name, SegmentSet};
    use super::*;
    use crate::{EntryPayload, LogEntry, PayloadKind};
    use std::path::{Path, PathBuf};

    /// Data directory used by the in-memory filesystem tests.
    const DIR: &str = "/wal";

    /// A `Record` payload of three identical `tag` bytes.
    fn payload(tag: u8) -> EntryPayload {
        EntryPayload::new(PayloadKind::Record, vec![tag, tag, tag])
    }

    /// A fresh in-memory filesystem with the data directory created.
    fn mem_fs() -> MemFileSystem {
        let fs = MemFileSystem::new();
        fs.create_dir_all(Path::new(DIR)).unwrap();
        fs
    }

    /// A config with a tiny segment size, so each appended frame rolls into its
    /// own segment and multi-segment replay is exercised.
    fn cfg(policy: SyncPolicy) -> WalConfig {
        WalConfig::new(DIR)
            .with_segment_size(40)
            .with_sync_policy(policy)
    }

    /// Write `terms.len()` frames (indices `0..terms.len()`) to disk through a
    /// fresh [`SegmentSet`], and acknowledge `ack` (the highest index recorded
    /// durable) plus `commit` in the manifest. Returns the per-frame on-disk
    /// `(segment, offset, len)` so a test can target a specific frame.
    fn seed(
        fs: &MemFileSystem,
        segment_size: u64,
        terms: &[u64],
        ack: Option<u64>,
        commit: Option<u64>,
    ) -> Vec<(u64, u64, u32)> {
        use super::super::frame::encode;
        let mut segments = SegmentSet::new(DIR, segment_size);
        let mut locations = Vec::new();
        let mut last = (0u64, 0u64, 0u32);
        for (i, &term) in terms.iter().enumerate() {
            let index = i as u64;
            let entry = LogEntry {
                index,
                term,
                payload: payload(index as u8),
            };
            let outcome = segments.append(fs, index, &encode(&entry)).unwrap();
            let loc = (
                outcome.location.segment,
                outcome.location.offset,
                outcome.location.len,
            );
            locations.push(loc);
            if Some(index) == ack {
                last = loc;
            }
        }
        segments.sync_active().unwrap();

        let (durable_segment, durable_offset) = match ack {
            Some(_) => (last.0, last.1 + last.2 as u64),
            None => (0, 0),
        };
        let (mut manifest, _) = Manifest::open(fs, Path::new(DIR)).unwrap();
        manifest
            .write(
                fs,
                &ManifestState {
                    log_start_index: 0,
                    commit_index: commit,
                    durable_last: ack,
                    durable_segment,
                    durable_offset,
                    ..ManifestState::default()
                },
            )
            .unwrap();
        locations
    }

    /// Run a full `replay` for `policy` against the directory's manifest.
    fn replay_dir(fs: &MemFileSystem, policy: SyncPolicy) -> Result<Recovered, LogError> {
        let (_, state) = Manifest::open(fs, Path::new(DIR)).unwrap();
        let state = state.unwrap_or_default();
        replay(fs, &cfg(policy), &state)
    }

    // --- empty / absent directory (R5.2) -----------------------------------

    #[test]
    fn replay_on_empty_directory_is_an_empty_log() {
        let fs = mem_fs();
        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        assert!(recovered.index.is_empty());
        assert_eq!(recovered.index.last_index(), None);
        assert_eq!(recovered.commit_index, None);
        assert_eq!(recovered.durable_last, None);
    }

    // --- clean reopen (R5.1, R5.3) -----------------------------------------

    #[test]
    fn replay_recovers_a_clean_multi_segment_log() {
        let fs = mem_fs();
        seed(&fs, 40, &[1, 1, 2, 3], Some(3), Some(2));
        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        assert_eq!(recovered.index.last_index(), Some(3));
        assert_eq!(recovered.index.log_start_index(), 0);
        assert_eq!(recovered.commit_index, Some(2));
        assert_eq!(recovered.durable_last, Some(3));
        for (idx, term) in [(0, 1), (1, 1), (2, 2), (3, 3)] {
            assert_eq!(recovered.index.term_at(idx), Some(term));
        }
    }

    #[test]
    fn replay_clamps_commit_to_the_last_recovered_index() {
        let fs = mem_fs();
        // Manifest claims commit 5 but only indices 0..=2 are acknowledged.
        seed(&fs, 40, &[1, 1, 1], Some(2), Some(5));
        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        assert_eq!(recovered.index.last_index(), Some(2));
        assert_eq!(
            recovered.commit_index,
            Some(2),
            "commit clamped to last (R5.4)"
        );
    }

    // --- torn tail recovered + truncated (R6.1, R6.2) ----------------------

    #[test]
    fn replay_discards_a_torn_tail_frame() {
        let fs = mem_fs();
        // Five frames written; only 0..=3 acknowledged. Tear the final frame
        // (index 4, the unacknowledged tail) so it cannot decode.
        let locs = seed(&fs, 40, &[1, 1, 1, 1, 1], Some(3), Some(1));
        let (seg, off, len) = locs[4];
        let path = PathBuf::from(DIR).join(segment_file_name(seg));
        fs.truncate_file(&path, off + len as u64 - 2);

        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        // The acknowledged run survives; the torn tail is gone.
        assert_eq!(recovered.index.last_index(), Some(3));
        assert_eq!(recovered.durable_last, Some(3));
        assert_eq!(recovered.commit_index, Some(1));
    }

    #[test]
    fn replay_discards_a_valid_but_unacknowledged_tail_under_always() {
        let fs = mem_fs();
        // Index 4 is a fully-valid frame on disk, but the manifest only
        // acknowledges 0..=3 (a crash between the frame fsync and the manifest
        // fsync). Under Always it is unacknowledged and must be discarded.
        seed(&fs, 40, &[1, 1, 1, 1, 1], Some(3), None);
        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        assert_eq!(
            recovered.index.last_index(),
            Some(3),
            "beyond-ack frame discarded"
        );
        assert_eq!(recovered.durable_last, Some(3));
    }

    #[test]
    fn replay_keeps_a_valid_unacknowledged_tail_under_never() {
        let fs = mem_fs();
        // Same on-disk shape, but Never does not promise persist-before-ack, so
        // the valid frames the OS flushed are kept.
        seed(&fs, 40, &[1, 1, 1, 1, 1], Some(3), None);
        let recovered = replay_dir(&fs, SyncPolicy::Never).unwrap();
        assert_eq!(
            recovered.index.last_index(),
            Some(4),
            "valid run kept under Never"
        );
    }

    // --- interior corruption (R6.3) ----------------------------------------

    #[test]
    fn replay_fails_on_interior_corruption_with_the_expected_index() {
        let fs = mem_fs();
        // Five acknowledged frames, each in its own segment. Corrupt the middle
        // one (index 2) leaving valid frames after it: interior corruption.
        let locs = seed(&fs, 40, &[1, 1, 1, 1, 1], Some(4), Some(2));
        let (seg, off, _len) = locs[2];
        let path = PathBuf::from(DIR).join(segment_file_name(seg));
        let mut bytes = fs.file_bytes(&path).unwrap();
        // Flip a payload byte (past the 4-byte len + 17-byte body header).
        bytes[off as usize + 4 + 17] ^= 0xFF;
        fs.open_read_write(&path)
            .unwrap()
            .write_at(0, &bytes)
            .unwrap();

        let err = replay_dir(&fs, SyncPolicy::Always).unwrap_err();
        assert!(
            matches!(err, LogError::Corruption { index: 2, .. }),
            "interior corruption reports the position-expected index, got {err:?}"
        );
    }

    #[test]
    fn replay_fails_on_interior_corruption_under_every_policy() {
        for policy in [
            SyncPolicy::Always,
            SyncPolicy::Never,
            SyncPolicy::periodic_default(),
        ] {
            let fs = mem_fs();
            let locs = seed(&fs, 40, &[1, 1, 1, 1], Some(3), Some(1));
            let (seg, off, _len) = locs[1];
            let path = PathBuf::from(DIR).join(segment_file_name(seg));
            let mut bytes = fs.file_bytes(&path).unwrap();
            bytes[off as usize + 4 + 17] ^= 0xFF;
            fs.open_read_write(&path)
                .unwrap()
                .write_at(0, &bytes)
                .unwrap();

            let err = replay_dir(&fs, policy).unwrap_err();
            assert!(
                matches!(err, LogError::Corruption { index: 1, .. }),
                "interior corruption must fail under {policy:?}, got {err:?}"
            );
        }
    }

    // --- non-contiguous (a hole) is corruption (R6.3) ----------------------

    #[test]
    fn replay_fails_when_the_first_retained_frame_is_missing() {
        let fs = mem_fs();
        // Acknowledge 0..=2, then tear index 0 entirely (truncate its segment to
        // empty) so the run begins at index 1 — a hole at the expected index 0
        // with valid frames after it.
        let locs = seed(&fs, 40, &[1, 1, 1], Some(2), Some(0));
        let (seg0, _, _) = locs[0];
        let path = PathBuf::from(DIR).join(segment_file_name(seg0));
        fs.truncate_file(&path, 0);

        let err = replay_dir(&fs, SyncPolicy::Always).unwrap_err();
        assert!(
            matches!(err, LogError::Corruption { index: 0, .. }),
            "a hole at index 0 is interior corruption, got {err:?}"
        );
    }

    // --- policy-scoped shortfall below the acknowledged extent (R6.4, R6.5) -

    #[test]
    fn replay_fails_on_a_shortfall_below_ack_under_always() {
        let fs = mem_fs();
        // Manifest acknowledges index 4, but only 0..=2 made it to disk (the
        // tail frames 3,4 are torn). Under Always this is a fatal shortfall.
        let locs = seed(&fs, 40, &[1, 1, 1, 1, 1], Some(4), Some(1));
        // Tear index 3's frame so the valid run stops at 2 — and remove index 4
        // so there is no valid frame after the torn one (a tail shortfall, not
        // interior corruption).
        let (seg3, off3, _) = locs[3];
        fs.truncate_file(&PathBuf::from(DIR).join(segment_file_name(seg3)), off3 + 2);
        let (seg4, _, _) = locs[4];
        fs.truncate_file(&PathBuf::from(DIR).join(segment_file_name(seg4)), 0);

        let err = replay_dir(&fs, SyncPolicy::Always).unwrap_err();
        assert!(
            matches!(err, LogError::Corruption { index: 4, .. }),
            "an Always shortfall reports the acknowledged extent, got {err:?}"
        );
    }

    #[test]
    fn replay_accepts_a_tail_shortfall_under_periodic_and_never() {
        for policy in [SyncPolicy::Never, SyncPolicy::periodic_default()] {
            let fs = mem_fs();
            let locs = seed(&fs, 40, &[1, 1, 1, 1, 1], Some(4), Some(1));
            let (seg3, off3, _) = locs[3];
            fs.truncate_file(&PathBuf::from(DIR).join(segment_file_name(seg3)), off3 + 2);
            let (seg4, _, _) = locs[4];
            fs.truncate_file(&PathBuf::from(DIR).join(segment_file_name(seg4)), 0);

            let recovered = replay_dir(&fs, policy).unwrap();
            assert_eq!(
                recovered.index.last_index(),
                Some(2),
                "{policy:?} truncates the tail shortfall and succeeds"
            );
        }
    }

    // --- torn-manifest fallback drives a prior acknowledged extent (R6.8) --

    #[test]
    fn replay_uses_the_prior_intact_manifest_slot_when_the_newest_is_torn() {
        let fs = mem_fs();
        // Acknowledge 0..=2 (slot 0), then 0..=3 (slot 1).
        let locs = seed(&fs, 40, &[1, 1, 1, 1], Some(2), Some(0));
        // A fourth frame and a newer manifest slot acknowledging it.
        let (seg3, off3, len3) = locs[3];
        {
            let (mut manifest, state) = Manifest::open(&fs, Path::new(DIR)).unwrap();
            let mut state = state.unwrap();
            state.durable_last = Some(3);
            state.durable_segment = seg3;
            state.durable_offset = off3 + len3 as u64;
            manifest.write(&fs, &state).unwrap();
        }
        // Tear the newest manifest slot (slot 1) so recovery falls back to the
        // prior slot that acknowledges only 0..=2. Truncating into slot 1 leaves
        // too few bytes for it to decode, so the highest valid slot is slot 0.
        use super::super::manifest::MANIFEST_FILE_NAME;
        let manifest_path = PathBuf::from(DIR).join(MANIFEST_FILE_NAME);
        let full = fs.file_size(&manifest_path).unwrap();
        // Drop almost all of slot 1 (the second half of the two-slot file).
        fs.truncate_file(&manifest_path, full / 2 + 2);

        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        // The fallback extent is 2, so the unacknowledged index 3 is discarded.
        assert_eq!(recovered.index.last_index(), Some(2));
        assert_eq!(recovered.durable_last, Some(2));
    }

    // --- orphan low segment below log_start is ignored (R7.5, R7.8) --------

    #[test]
    fn replay_skips_frames_below_log_start() {
        let fs = mem_fs();
        // Write 0..=4 into one large segment, then model a compacted log whose
        // retained range is 2..=4 by setting log_start_index = 2.
        use super::super::frame::encode;
        let mut segments = SegmentSet::new(DIR, 4096);
        let mut last = (0u64, 0u64, 0u32);
        for index in 0..5u64 {
            let entry = LogEntry {
                index,
                term: 1,
                payload: payload(index as u8),
            };
            let outcome = segments.append(&fs, index, &encode(&entry)).unwrap();
            last = (
                outcome.location.segment,
                outcome.location.offset,
                outcome.location.len,
            );
        }
        segments.sync_active().unwrap();

        let state = ManifestState {
            log_start_index: 2,
            commit_index: Some(3),
            durable_last: Some(4),
            durable_segment: last.0,
            durable_offset: last.1 + last.2 as u64,
            ..ManifestState::default()
        };
        let recovered = replay(&fs, &cfg(SyncPolicy::Always), &state).unwrap();
        // Indices below log_start are skipped; the retained range is 2..=4.
        assert_eq!(recovered.index.log_start_index(), 2);
        assert_eq!(recovered.index.last_index(), Some(4));
        assert_eq!(recovered.index.term_at(1), None, "below log_start skipped");
        assert_eq!(recovered.index.term_at(2), Some(1));
        assert_eq!(recovered.commit_index, Some(3));
    }

    // --- filesystem error during replay is reported (R5.5) -----------------

    #[test]
    fn replay_maps_a_scan_io_error_to_io() {
        let fs = mem_fs();
        let locs = seed(&fs, 40, &[1, 1, 1], Some(2), Some(1));
        // Fail reads of the first segment file so the scan errors.
        let (seg0, _, _) = locs[0];
        fs.arm_read_failure_for(&PathBuf::from(DIR).join(segment_file_name(seg0)));
        let err = replay_dir(&fs, SyncPolicy::Always).unwrap_err();
        assert!(matches!(err, LogError::Io { .. }), "got {err:?}");
    }

    // --- reconstruction puts appends back in the right active segment ------

    #[test]
    fn restore_after_replay_continues_in_the_correct_segment() {
        let fs = mem_fs();
        let locs = seed(&fs, 40, &[1, 1, 1, 1], Some(3), Some(1));
        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        let tail = recovered
            .durable_last
            .map(|_| super::super::segment::RestoreTail {
                segment: recovered.durable_segment,
                offset_end: recovered.durable_offset,
            });
        let segments =
            SegmentSet::restore(&fs, DIR, 40, recovered.index.log_start_index(), tail).unwrap();
        // The active segment is the one holding the recovered tail (index 3).
        assert_eq!(segments.active().map(|m| m.base_index), Some(locs[3].0));
        // The earlier frames are sealed segments, ascending by base index.
        let sealed: Vec<u64> = segments.sealed().iter().map(|m| m.base_index).collect();
        assert_eq!(sealed, vec![locs[0].0, locs[1].0, locs[2].0]);
    }

    #[test]
    fn restore_removes_a_torn_orphan_segment_above_the_tail() {
        let fs = mem_fs();
        // 0..=4 on disk, only 0..=3 acknowledged; index 4 is a torn orphan.
        let locs = seed(&fs, 40, &[1, 1, 1, 1, 1], Some(3), Some(1));
        let recovered = replay_dir(&fs, SyncPolicy::Always).unwrap();
        let tail = recovered
            .durable_last
            .map(|_| super::super::segment::RestoreTail {
                segment: recovered.durable_segment,
                offset_end: recovered.durable_offset,
            });
        SegmentSet::restore(&fs, DIR, 40, 0, tail).unwrap();
        // The orphan segment holding the discarded index 4 was removed from disk.
        let bases: Vec<u64> = ordered_segments(&fs, Path::new(DIR))
            .unwrap()
            .into_iter()
            .map(|(base, _)| base)
            .collect();
        assert!(!bases.contains(&locs[4].0), "torn orphan must be removed");
        assert!(
            bases.contains(&locs[3].0),
            "the recovered tail segment stays"
        );
    }
}
