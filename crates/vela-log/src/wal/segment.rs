//! Segment files.
//!
//! The Durable_WAL stores its Record_Frames as an ordered sequence of bounded
//! **segment files** in the Data_Directory (Requirement 3). This module owns
//! everything about those files: how they are named, how a frame is appended to
//! the active segment, when the active segment rolls over to a new one, how a
//! segment is sealed (made immutable), how its on-disk size is tracked, and how
//! a segment is scanned back into frames for recovery and reads.
//!
//! # Layout and naming
//!
//! Each segment is named by its **base index** — the absolute index of its
//! first frame — zero-padded to [`BASE_INDEX_WIDTH`] decimal digits with a
//! `.wal` extension (e.g. `00000000000000000042.wal` for base index 42). The
//! padding is wide enough for any `u64`, so **lexical order equals numeric
//! order**: a plain directory listing already sorts segments ascending by base
//! index (Requirements 3.5, 5.1). [`segment_file_name`] builds a name and
//! [`parse_base_index`] reads one back; [`ordered_segments`] lists a directory
//! and returns its segments sorted ascending.
//!
//! # Rollover rules (Requirement 3)
//!
//! [`SegmentSet::append`] places each frame according to four rules:
//!
//! - **First segment** (R3.2): when there is no active segment, a new one is
//!   created whose base index is the entry's index.
//! - **Rollover** (R3.3): when the active segment is **non-empty** and writing
//!   the frame would push the active segment past `segment_size`
//!   (`active.size + frame.len > segment_size`), the active segment is sealed
//!   and a new one is started at the entry's index.
//! - **Oversized frame** (R3.4): a single frame larger than `segment_size` is
//!   written as the **sole** frame of its own segment. This falls out of the
//!   rollover rule: the frame rolls into a fresh, empty segment, and because an
//!   empty active segment never triggers rollover, the oversized frame is
//!   written even though it exceeds `segment_size` — which prevents an infinite
//!   rollover loop.
//! - **No splitting / ascending order** (R3.1, R3.6): a frame is always written
//!   wholly within one segment, and because base indices are assigned from
//!   ascending entry indices, reading segments in base-index order yields
//!   entries in ascending index order.
//!
//! # Validated scan
//!
//! [`scan_segment`] reads a segment file and decodes its frames in order,
//! advancing by each frame's encoded length. It returns the decoded frames
//! (each with its byte offset and length, so the in-memory index can be rebuilt
//! without retaining payloads) plus a [`ScanOutcome`] classifying how the
//! segment ended: cleanly, at an incomplete (torn) frame, or at a corrupt one.
//! Recovery (task 12) combines the per-segment outcome with whether any valid
//! frame follows to distinguish a recoverable torn tail from fatal interior
//! corruption (Requirement 6).

// Most of this module is now live: `DurableWal` (tasks 8–11) drives `append`,
// `sync_active`, and `truncate_at`; recovery (task 12) consumes
// `ordered_segments`, `scan_segment`/`ScanOutcome`, `SegmentSet::restore`, and
// the segment metadata; and Compaction (task 13) drives `remove_below`, which
// reads the `sealed`/`active` accessors. The few remaining items without a
// non-test caller carry a narrowed, per-item `#[allow(dead_code)]`: the
// `SegmentSet` empty constructor (`new`) is exercised only by the unit tests,
// and the by-`SegmentSet` disk read (`read_entry`/`decode_one`) is exercised by
// tests while reads currently flow through `DurableWal`'s own frame loader.

use std::io;
use std::path::{Path, PathBuf};

use super::frame::{self, FrameDecode};
use super::fs::{FileSystem, WalFile};
use crate::LogEntry;

/// Decimal width to which a segment's base index is zero-padded in its file
/// name. `u64::MAX` is 20 decimal digits, so this width guarantees fixed-width
/// names whose lexical order equals their numeric order (Requirement 3.5).
const BASE_INDEX_WIDTH: usize = 20;

/// File extension shared by every segment file.
const SEGMENT_EXTENSION: &str = "wal";

/// The file name of the segment whose base index is `base_index`.
///
/// Zero-padded to [`BASE_INDEX_WIDTH`] digits with a `.wal` extension, so the
/// name sorts identically whether compared lexically or numerically
/// (Requirement 3.5).
pub(crate) fn segment_file_name(base_index: u64) -> String {
    format!("{base_index:0BASE_INDEX_WIDTH$}.{SEGMENT_EXTENSION}")
}

/// Parse a segment's base index out of its path, or `None` when `path` is not a
/// segment file.
///
/// A path is a segment file iff it has the `.wal` extension and a stem that
/// parses as a `u64`. This rejects unrelated files (including the directory
/// lock sentinel `.wal.lock`, whose extension is `lock`) so a directory listing
/// can be filtered to just segments (Requirements 3.5, 5.1).
pub(crate) fn parse_base_index(path: &Path) -> Option<u64> {
    if path.extension()?.to_str()? != SEGMENT_EXTENSION {
        return None;
    }
    path.file_stem()?.to_str()?.parse::<u64>().ok()
}

/// List `dir` and return its segment files as `(base_index, path)` pairs sorted
/// ascending by base index.
///
/// Non-segment files are filtered out. This is the canonical ordering used by
/// recovery to read segments in ascending index order (Requirements 3.5, 5.1).
pub(crate) fn ordered_segments<F: FileSystem>(
    fs: &F,
    dir: &Path,
) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut segments: Vec<(u64, PathBuf)> = fs
        .read_dir(dir)?
        .into_iter()
        .filter_map(|path| parse_base_index(&path).map(|base| (base, path)))
        .collect();
    segments.sort_by_key(|(base, _)| *base);
    Ok(segments)
}

/// Immutable metadata describing one segment file.
///
/// `size` tracks the segment's current on-disk byte length; it is the offset at
/// which the next appended frame begins. `sealed` marks a segment that will
/// receive no further frames (the active segment becomes sealed when it rolls
/// over). This mirrors the `SegmentMeta` the design assigns to `DurableWal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentMeta {
    /// Absolute index of this segment's first frame.
    pub base_index: u64,
    /// Path of the segment file within the Data_Directory.
    pub path: PathBuf,
    /// Whether the segment is sealed (immutable) and will take no more frames.
    pub sealed: bool,
    /// Current on-disk size in bytes; the offset of the next frame.
    pub size: u64,
}

/// The append-only active segment: a tracked [`SegmentMeta`] plus an open file
/// handle. New frames are written positionally at [`SegmentMeta::size`], which
/// is then advanced by the frame's byte length (Requirement 3).
///
/// No `derive(Debug)`: the handle type `F::File` is not bound by `Debug` at the
/// [`FileSystem`] seam, so a blanket derive would not apply across all `F`.
pub(crate) struct ActiveSegment<F: FileSystem> {
    /// Metadata for the open segment (`sealed` is always `false` here).
    meta: SegmentMeta,
    /// Open read/write handle for positional appends and reads.
    file: F::File,
}

impl<F: FileSystem> ActiveSegment<F> {
    /// Create a new, empty active segment with base index `base_index`.
    ///
    /// Opens (creating) the segment file and adopts its current on-disk size as
    /// the write offset — `0` for a fresh file, or the existing length when an
    /// already-populated active segment is reopened during recovery.
    fn create(fs: &F, dir: &Path, base_index: u64) -> io::Result<Self> {
        let path = dir.join(segment_file_name(base_index));
        let file = fs.open_read_write(&path)?;
        let size = file.size()?;
        Ok(Self {
            meta: SegmentMeta {
                base_index,
                path,
                sealed: false,
                size,
            },
            file,
        })
    }

    /// Append `frame` at the tracked end offset and return that offset.
    ///
    /// The write is positional (no implicit cursor), so the offset returned is
    /// exactly where the frame's bytes begin — what the in-memory index records
    /// for later reads. The tracked size advances by the frame length, keeping
    /// each frame wholly within this one segment (Requirement 3.1).
    fn append_frame(&mut self, frame: &[u8]) -> io::Result<u64> {
        let offset = self.meta.size;
        self.file.write_at(offset, frame)?;
        self.meta.size += frame.len() as u64;
        Ok(offset)
    }
}

/// The location of one frame on disk: which segment holds it, the byte offset
/// where it begins, and its encoded byte length.
///
/// This is exactly what the in-memory index stores per entry so the payload can
/// be read back from the segment on demand without retaining it in memory
/// (Requirement 13). `len` is the **whole frame** length (length field + body +
/// CRC), so a reader can fetch and decode the frame with a single positional
/// read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameLocation {
    /// Base index of the segment containing the frame.
    pub segment: u64,
    /// Byte offset of the frame within its segment.
    pub offset: u64,
    /// Total encoded byte length of the frame.
    pub len: u32,
}

/// The recovered log tail used to reconstruct a [`SegmentSet`] at open
/// ([`SegmentSet::restore`]).
///
/// `segment` is the base index of the segment holding the last retained frame
/// and `offset_end` is the byte offset just past that frame — exactly the
/// `(durable_segment, durable_offset)` acknowledged extent recovery rebuilds.
/// The segment becomes the active segment, truncated to `offset_end` so any
/// torn-tail bytes beyond the last acknowledged frame are dropped (Requirement
/// 6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestoreTail {
    /// Base index of the segment holding the last retained frame.
    pub segment: u64,
    /// Byte offset just past the last retained frame within that segment.
    pub offset_end: u64,
}

/// The result of appending one frame: where it landed, and whether a new
/// segment file was created to hold it.
///
/// `created_segment` lets the caller fsync the parent directory after a new
/// segment file appears under the `Always` policy, so a crash cannot lose a
/// newly created segment holding an acknowledged entry (Requirement 4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendOutcome {
    /// Where the frame was written.
    pub location: FrameLocation,
    /// Whether this append created a new segment file.
    pub created_segment: bool,
}

/// One frame recovered by a [`scan_segment`] pass: the decoded entry plus its
/// on-disk location within the segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScannedFrame {
    /// The decoded log entry.
    pub entry: LogEntry,
    /// Byte offset of the frame within the segment.
    pub offset: u64,
    /// Total encoded byte length of the frame.
    pub len: u32,
}

/// How a segment scan terminated.
///
/// A scan decodes frames until it either consumes the whole file
/// ([`Clean`](ScanOutcome::Clean)) or hits a frame it cannot accept. The two
/// failure cases mirror [`FrameDecode`] and are what recovery needs to tell a
/// recoverable torn tail from fatal interior corruption (Requirement 6): an
/// [`Incomplete`](ScanOutcome::Incomplete) trailing frame is a torn-write
/// candidate, while a [`Corrupt`](ScanOutcome::Corrupt) frame followed by any
/// valid frame is interior corruption. This module reports the classification;
/// recovery (task 12) decides which is fatal by looking across segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanOutcome {
    /// Every byte of the segment was consumed by valid frames.
    Clean,
    /// Decoding stopped at an incomplete (torn) frame beginning at `offset`.
    Incomplete {
        /// Offset at which the incomplete frame began.
        offset: u64,
    },
    /// Decoding stopped at a corrupt (CRC-bad or structurally invalid) frame at
    /// `offset`.
    Corrupt {
        /// Offset at which the corrupt frame began.
        offset: u64,
    },
}

/// The frames recovered from a segment together with how the scan ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentScan {
    /// The valid frames decoded, in ascending offset (and thus index) order.
    pub frames: Vec<ScannedFrame>,
    /// How the scan terminated.
    pub outcome: ScanOutcome,
}

/// Read `path` and decode its frames sequentially into a [`SegmentScan`].
///
/// Frames are decoded from the front of the file, advancing by each frame's
/// encoded length, until the file is fully consumed or a frame cannot be
/// accepted. The whole segment is read into memory once; segments are bounded
/// by `segment_size` (plus at most one oversized frame), so this is a single
/// bounded allocation per segment. Used by recovery and by any caller needing
/// the validated contents of a segment (Requirements 3, 6).
pub(crate) fn scan_segment<F: FileSystem>(fs: &F, path: &Path) -> io::Result<SegmentScan> {
    let file = fs.open_read(path)?;
    let size = file.size()?;
    let mut bytes = vec![0u8; size as usize];
    if size > 0 {
        file.read_exact_at(0, &mut bytes)?;
    }

    let mut frames = Vec::new();
    let mut offset = 0usize;
    loop {
        if offset == bytes.len() {
            return Ok(SegmentScan {
                frames,
                outcome: ScanOutcome::Clean,
            });
        }
        match frame::decode(&bytes[offset..]) {
            FrameDecode::Ok { entry, consumed } => {
                frames.push(ScannedFrame {
                    entry,
                    offset: offset as u64,
                    len: consumed as u32,
                });
                offset += consumed;
            }
            FrameDecode::Incomplete => {
                return Ok(SegmentScan {
                    frames,
                    outcome: ScanOutcome::Incomplete {
                        offset: offset as u64,
                    },
                });
            }
            FrameDecode::Corrupt => {
                return Ok(SegmentScan {
                    frames,
                    outcome: ScanOutcome::Corrupt {
                        offset: offset as u64,
                    },
                });
            }
        }
    }
}

/// The ordered set of segment files backing one partition log.
///
/// A `SegmentSet` owns the sealed segments (ascending by base index) and the
/// single open active segment, and enforces the placement rules of Requirement
/// 3 on every [`append`](SegmentSet::append). The filesystem seam is passed in
/// per call rather than stored, so the set holds only state that is genuinely
/// per-segment (metadata and the active file handle) and shares the caller's
/// `FileSystem`.
///
/// This is the abstraction `DurableWal` (task 8) builds on: it creates the
/// first segment for an entry, appends with rollover, reads payloads back by
/// `(segment, offset, len)` (task 9), and — via the sealed/active metadata
/// exposed here — truncates and removes segments during revert and compaction
/// (tasks 11, 13).
///
/// No `derive(Debug)`: it holds an [`ActiveSegment`], whose `F::File` handle is
/// not bound by `Debug` at the [`FileSystem`] seam.
pub(crate) struct SegmentSet<F: FileSystem> {
    /// Data_Directory holding the segment files.
    dir: PathBuf,
    /// Maximum segment size in bytes before rollover.
    segment_size: u64,
    /// Sealed (immutable) segments, ascending by base index.
    sealed: Vec<SegmentMeta>,
    /// The open active segment, if any.
    active: Option<ActiveSegment<F>>,
}

impl<F: FileSystem> SegmentSet<F> {
    /// Create an empty segment set rooted at `dir` with the given
    /// `segment_size`. No files are touched until the first
    /// [`append`](SegmentSet::append).
    #[allow(dead_code)] // empty-set constructor exercised by the unit tests
    pub(crate) fn new(dir: impl Into<PathBuf>, segment_size: u64) -> Self {
        Self {
            dir: dir.into(),
            segment_size,
            sealed: Vec::new(),
            active: None,
        }
    }

    /// Reconstruct the segment set from the segment files on disk after
    /// recovery (Requirements 5, 6).
    ///
    /// `tail` is the recovered log tail — the segment and end offset of the last
    /// retained frame — or `None` when recovery retained no frames. `log_start`
    /// is the recovered Log_Start_Index. The reconstruction:
    ///
    /// - makes the segment holding the recovered tail the **active** segment,
    ///   truncated to [`RestoreTail::offset_end`], physically dropping any
    ///   torn-tail bytes beyond the last acknowledged frame so they are never
    ///   restored on a subsequent recovery (Requirement 6.2);
    /// - reopens the segments **below** the tail (down to the directory's
    ///   contents) as **sealed**, adopting their on-disk size;
    /// - **removes** any segment file **above** the tail, which can hold only
    ///   discarded torn frames, so a subsequent append cannot collide with it.
    ///
    /// When `tail` is `None` the recovered log is empty: every segment file at
    /// or above `log_start` is such a torn orphan and is removed, while the
    /// compacted prefix below `log_start` (orphaned whole segments from a crash
    /// mid-compaction, Requirement 7.8) is left on disk and reopened as sealed.
    /// The on-disk segment files are authoritative — listed via
    /// [`ordered_segments`] — so this works whether the segments were written
    /// this session or are being recovered on reopen.
    pub(crate) fn restore(
        fs: &F,
        dir: impl Into<PathBuf>,
        segment_size: u64,
        log_start: u64,
        tail: Option<RestoreTail>,
    ) -> io::Result<Self> {
        let dir = dir.into();
        let mut sealed = Vec::new();

        for (base, path) in ordered_segments(fs, &dir)? {
            // The segment that holds the recovered tail becomes the active
            // segment, reconstructed after the loop.
            if matches!(tail, Some(tail) if base == tail.segment) {
                continue;
            }
            // A segment above the recovered tail (or, for an empty recovered
            // log, at or above `log_start`) holds only discarded torn frames; a
            // segment below it is retained and reopened as sealed.
            let above = match tail {
                Some(tail) => base > tail.segment,
                None => base >= log_start,
            };
            if above {
                fs.remove_file(&path)?;
            } else {
                let size = fs.open_read(&path)?.size()?;
                sealed.push(SegmentMeta {
                    base_index: base,
                    path,
                    sealed: true,
                    size,
                });
            }
        }

        let active = match tail {
            Some(tail) => {
                let path = dir.join(segment_file_name(tail.segment));
                let file = fs.open_read_write(&path)?;
                // Truncate to just past the last acknowledged frame, dropping any
                // torn-tail bytes within the active segment (Requirement 6.2).
                file.set_len(tail.offset_end)?;
                Some(ActiveSegment {
                    meta: SegmentMeta {
                        base_index: tail.segment,
                        path,
                        sealed: false,
                        size: tail.offset_end,
                    },
                    file,
                })
            }
            None => None,
        };

        Ok(Self {
            dir,
            segment_size,
            sealed,
            active,
        })
    }

    /// Metadata for the sealed segments, ascending by base index.
    pub(crate) fn sealed(&self) -> &[SegmentMeta] {
        &self.sealed
    }

    /// Metadata for the active segment, or `None` when the log is empty.
    pub(crate) fn active(&self) -> Option<&SegmentMeta> {
        self.active.as_ref().map(|active| &active.meta)
    }

    /// Remove every sealed segment that lies **wholly below** `retained_point`
    /// — one whose every frame has an absolute index `< retained_point` —
    /// deleting its file and dropping it from the sealed list. This is the
    /// segment-granularity reclaim behind Compaction (Requirement 7.5).
    ///
    /// A sealed segment's exclusive upper index bound is the base index of the
    /// segment that follows it (the next sealed segment, or the active
    /// segment). The segment is wholly below `retained_point` exactly when that
    /// successor's base index is `<= retained_point`, since every frame it
    /// holds then has an index strictly below `retained_point`. A segment whose
    /// index range *straddles* `retained_point` is kept: its retained frames
    /// stay, while the below-line frames remain physically on disk but are
    /// skipped on read and on recovery (Requirement 7.5). The active segment
    /// always holds the log tail — and therefore the retained commit entry the
    /// caller has bounds-checked `retained_point` against — so it is never
    /// removed here.
    ///
    /// Because segments are ascending by base index and `retained_point` only
    /// ever advances, the removable segments form a prefix of the sealed list;
    /// the scan stops at the first segment that is not wholly below the point.
    pub(crate) fn remove_below(&mut self, fs: &F, retained_point: u64) -> io::Result<()> {
        let active_base = self.active().map(|meta| meta.base_index);
        let bases: Vec<u64> = self.sealed().iter().map(|meta| meta.base_index).collect();

        let mut remove_count = 0usize;
        for i in 0..bases.len() {
            // The exclusive upper bound of sealed segment `i`'s index range is
            // its successor's base index: the next sealed segment, or — for the
            // last sealed segment — the active segment.
            let next_base = match bases.get(i + 1) {
                Some(&base) => base,
                None => match active_base {
                    Some(base) => base,
                    // No successor at all: cannot prove the segment is wholly
                    // below `retained_point`, so leave it in place.
                    None => break,
                },
            };
            if next_base <= retained_point {
                remove_count += 1;
            } else {
                // Ascending order: no later sealed segment can qualify either.
                break;
            }
        }

        for meta in self.sealed.drain(..remove_count) {
            fs.remove_file(&meta.path)?;
        }
        Ok(())
    }

    /// Decide whether appending a frame of `frame_len` bytes requires starting
    /// a new segment, given the current active segment size.
    ///
    /// Encodes Requirement 3 directly:
    /// - no active segment → new segment (R3.2);
    /// - active **non-empty** and the frame would exceed `segment_size` → new
    ///   segment (R3.3);
    /// - otherwise the frame fits in the active segment — including the case of
    ///   an oversized frame landing in an empty active segment, which is how an
    ///   oversized frame becomes the sole frame of its own segment without an
    ///   infinite rollover (R3.4).
    fn needs_new_segment(&self, frame_len: u64) -> bool {
        match &self.active {
            None => true,
            Some(active) => {
                active.meta.size != 0 && active.meta.size + frame_len > self.segment_size
            }
        }
    }

    /// Append an already-encoded `frame` for the entry at absolute index
    /// `entry_index`, rolling over to a new segment when the placement rules
    /// require it.
    ///
    /// When a new segment is needed, the current active segment (if any) is
    /// sealed and moved into the sealed list, and a fresh segment is created
    /// whose base index is `entry_index` — preserving ascending index order
    /// across segments (Requirement 3.6). The frame is then written wholly
    /// within the active segment (Requirement 3.1).
    pub(crate) fn append(
        &mut self,
        fs: &F,
        entry_index: u64,
        frame: &[u8],
    ) -> io::Result<AppendOutcome> {
        let created_segment = self.needs_new_segment(frame.len() as u64);

        if created_segment {
            // Seal the outgoing active segment before starting a new one.
            if let Some(active) = self.active.take() {
                let mut meta = active.meta;
                meta.sealed = true;
                self.sealed.push(meta);
            }
            self.active = Some(ActiveSegment::create(fs, &self.dir, entry_index)?);
        }

        let active = self
            .active
            .as_mut()
            .expect("active segment present after placement");
        let offset = active.append_frame(frame)?;

        Ok(AppendOutcome {
            location: FrameLocation {
                segment: active.meta.base_index,
                offset,
                len: frame.len() as u32,
            },
            created_segment,
        })
    }

    /// Discard the on-disk frame data at and after `cut`, leaving the segment
    /// that contained `cut` truncated to `cut.offset` and open as the active
    /// segment ready for the next [`append`](SegmentSet::append).
    ///
    /// This is the segment-level primitive behind `append_entries`'
    /// overwrite-of-the-uncommitted-suffix (task 10) and `revert` (task 11):
    /// given the [`FrameLocation`] of the first index being dropped, it removes
    /// every conflicting frame across segments. Because frames are written in
    /// ascending index order, every segment whose base index is **greater** than
    /// `cut.segment` holds only dropped indices and is removed from the
    /// directory (the segment-granularity reclaim shape of Requirement 7.5),
    /// while the segment `cut.segment` is truncated to `cut.offset` bytes —
    /// dropping `cut`'s frame and everything after it within that file — and
    /// reopened as the **unsealed** active segment so the next append continues
    /// immediately after the retained prefix. Segments below `cut.segment` are
    /// retained as sealed.
    ///
    /// The on-disk segment files are authoritative here (listed via
    /// [`ordered_segments`]), so this works whether the segments were written
    /// this session or recovered on reopen — the in-memory sealed/active state
    /// is rebuilt from disk to match. When `cut.offset` is `0` the containing
    /// segment is emptied but kept as the (empty) active segment, so the next
    /// append reuses its file rather than leaving a stray empty segment.
    pub(crate) fn truncate_at(&mut self, fs: &F, cut: FrameLocation) -> io::Result<()> {
        // Rebuild the sealed set from disk while removing every segment that
        // lies wholly above the cut (its frames are all being dropped).
        let mut sealed = Vec::new();
        for (base, path) in ordered_segments(fs, &self.dir)? {
            if base > cut.segment {
                fs.remove_file(&path)?;
            } else if base < cut.segment {
                let size = fs.open_read(&path)?.size()?;
                sealed.push(SegmentMeta {
                    base_index: base,
                    path,
                    sealed: true,
                    size,
                });
            }
            // `base == cut.segment` is the containing segment, handled below.
        }

        // Truncate the containing segment to the cut offset and adopt it as the
        // active segment so subsequent appends continue right after the prefix.
        let path = self.dir.join(segment_file_name(cut.segment));
        let file = fs.open_read_write(&path)?;
        file.set_len(cut.offset)?;

        self.sealed = sealed;
        self.active = Some(ActiveSegment {
            meta: SegmentMeta {
                base_index: cut.segment,
                path,
                sealed: false,
                size: cut.offset,
            },
            file,
        });
        Ok(())
    }

    /// Force the active segment's data to stable storage.
    ///
    /// A no-op when the log is empty. Frame durability under the `Always`
    /// policy is built on this (the caller additionally fsyncs the directory
    /// when a new segment file was created — see [`AppendOutcome`]).
    pub(crate) fn sync_active(&self) -> io::Result<()> {
        match &self.active {
            Some(active) => active.file.sync_data(),
            None => Ok(()),
        }
    }

    /// Read and decode the frame at `(segment, offset, len)`.
    ///
    /// Reads from the active segment's open handle when `segment` is the active
    /// base index, otherwise opens the sealed segment file for reading. The
    /// `len` bytes read are exactly one whole frame, which is decoded back into
    /// its [`LogEntry`]. A frame that fails to decode (which should not happen
    /// for a location the index recorded) surfaces as an I/O error; the
    /// fail-stop read policy lives one layer up in `DurableWal` (task 9).
    #[allow(dead_code)] // by-`SegmentSet` read exercised by tests; `DurableWal`
                        // currently reads through its own frame loader
    pub(crate) fn read_entry(
        &self,
        fs: &F,
        segment: u64,
        offset: u64,
        len: u32,
    ) -> io::Result<LogEntry> {
        let mut buf = vec![0u8; len as usize];

        if let Some(active) = &self.active {
            if active.meta.base_index == segment {
                active.file.read_exact_at(offset, &mut buf)?;
                return decode_one(&buf);
            }
        }

        let meta = self
            .sealed
            .iter()
            .find(|meta| meta.base_index == segment)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "read_entry: no segment with the given base index",
                )
            })?;
        let file = fs.open_read(&meta.path)?;
        file.read_exact_at(offset, &mut buf)?;
        decode_one(&buf)
    }
}

/// Decode exactly one frame from `buf`, mapping anything other than a complete,
/// valid frame to an I/O error. Used by the disk-backed read path, which reads a
/// frame whose location the index already recorded.
#[allow(dead_code)] // paired with `read_entry` above
fn decode_one(buf: &[u8]) -> io::Result<LogEntry> {
    match frame::decode(buf) {
        FrameDecode::Ok { entry, .. } => Ok(entry),
        FrameDecode::Incomplete | FrameDecode::Corrupt => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "read_entry: frame at recorded location failed to decode",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::super::fs::fault::MemFileSystem;
    use super::*;
    use crate::{EntryPayload, PayloadKind};
    use std::path::PathBuf;

    /// Data directory used by the in-memory filesystem tests.
    const DIR: &str = "/wal";

    /// Build a `LogEntry` with a payload of `payload_len` filler bytes.
    fn entry(index: u64, term: u64, payload_len: usize) -> LogEntry {
        LogEntry {
            index,
            term,
            payload: EntryPayload {
                kind: PayloadKind::Record,
                bytes: vec![0xAB; payload_len],
            },
        }
    }

    /// A fresh in-memory filesystem with the data directory created.
    fn mem_fs() -> MemFileSystem {
        let fs = MemFileSystem::new();
        fs.create_dir_all(Path::new(DIR)).unwrap();
        fs
    }

    /// Encoded byte length of a frame whose payload is `payload_len` bytes.
    fn frame_len(payload_len: usize) -> u64 {
        frame::encoded_len(payload_len) as u64
    }

    // --- name parsing & ordering (R3.5) ------------------------------------

    #[test]
    fn segment_file_name_is_zero_padded_to_twenty_digits() {
        assert_eq!(segment_file_name(42), "00000000000000000042.wal");
        assert_eq!(segment_file_name(0), "00000000000000000000.wal");
        assert_eq!(segment_file_name(u64::MAX), "18446744073709551615.wal");
    }

    #[test]
    fn base_index_round_trips_through_the_file_name() {
        for base in [0u64, 1, 42, 1_000_000, u64::MAX] {
            let name = segment_file_name(base);
            let path = PathBuf::from(DIR).join(&name);
            assert_eq!(parse_base_index(&path), Some(base), "round-trip {base}");
        }
    }

    #[test]
    fn parse_base_index_rejects_non_segment_paths() {
        // Wrong extension, including the directory lock sentinel `.wal.lock`.
        assert_eq!(parse_base_index(Path::new("/wal/.wal.lock")), None);
        assert_eq!(parse_base_index(Path::new("/wal/wal.manifest")), None);
        // Right extension but a non-numeric stem.
        assert_eq!(parse_base_index(Path::new("/wal/segment.wal")), None);
        // No extension at all.
        assert_eq!(
            parse_base_index(Path::new("/wal/00000000000000000001")),
            None
        );
    }

    #[test]
    fn zero_padding_makes_lexical_order_equal_numeric_order() {
        // The point of fixed-width zero-padding: sorting the *strings* yields
        // the same order as sorting the numbers (2 < 10 < 100), which a naive
        // unpadded name (e.g. "10.wal" < "2.wal") would get wrong.
        let mut names: Vec<String> = [100u64, 2, 10]
            .iter()
            .map(|b| segment_file_name(*b))
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                segment_file_name(2),
                segment_file_name(10),
                segment_file_name(100),
            ]
        );
    }

    #[test]
    fn ordered_segments_sorts_ascending_and_ignores_non_segments() {
        let fs = mem_fs();
        // Create files out of order, plus non-segment files that must be ignored.
        for base in [10u64, 0, 5] {
            fs.open_read_write(&PathBuf::from(DIR).join(segment_file_name(base)))
                .unwrap();
        }
        fs.open_read_write(&PathBuf::from(DIR).join("wal.manifest"))
            .unwrap();
        fs.open_read_write(&PathBuf::from(DIR).join(".wal.lock"))
            .unwrap();

        let bases: Vec<u64> = ordered_segments(&fs, Path::new(DIR))
            .unwrap()
            .into_iter()
            .map(|(base, _)| base)
            .collect();
        assert_eq!(bases, vec![0, 5, 10]);
    }

    // --- append, size tracking, and read round-trip (R3.1, R3.6) -----------

    #[test]
    fn append_tracks_offsets_and_size_and_reads_back() {
        let fs = mem_fs();
        // A segment large enough to hold every frame: no rollover here.
        let mut segments = SegmentSet::new(DIR, 4096);

        let e0 = entry(0, 1, 4);
        let e1 = entry(1, 1, 8);
        let f0 = frame::encode(&e0);
        let f1 = frame::encode(&e1);

        let a0 = segments.append(&fs, 0, &f0).unwrap();
        assert!(a0.created_segment, "first append creates the segment");
        assert_eq!(a0.location.segment, 0);
        assert_eq!(a0.location.offset, 0);
        assert_eq!(a0.location.len as u64, frame_len(4));

        let a1 = segments.append(&fs, 1, &f1).unwrap();
        assert!(!a1.created_segment, "second frame fits in the same segment");
        assert_eq!(a1.location.segment, 0);
        // The second frame begins exactly where the first ended.
        assert_eq!(a1.location.offset, frame_len(4));

        // On-disk size is the sum of both frame lengths.
        assert_eq!(segments.active().unwrap().size, frame_len(4) + frame_len(8));
        // And matches what the filesystem actually holds.
        let path = PathBuf::from(DIR).join(segment_file_name(0));
        assert_eq!(fs.file_size(&path), Some(frame_len(4) + frame_len(8)));

        // Both entries read back identically from their recorded locations.
        assert_eq!(
            segments
                .read_entry(
                    &fs,
                    a0.location.segment,
                    a0.location.offset,
                    a0.location.len
                )
                .unwrap(),
            e0
        );
        assert_eq!(
            segments
                .read_entry(
                    &fs,
                    a1.location.segment,
                    a1.location.offset,
                    a1.location.len
                )
                .unwrap(),
            e1
        );
    }

    // --- boundary rollover (R3.3) ------------------------------------------

    #[test]
    fn rollover_happens_only_when_the_frame_would_exceed_segment_size() {
        let fs = mem_fs();
        // Each empty-payload frame is `frame_len(0)` bytes; size the segment to
        // hold exactly two of them so the boundary is exercised precisely.
        let unit = frame_len(0);
        let mut segments = SegmentSet::new(DIR, unit * 2);

        // Frame 0: creates segment 0, size == unit.
        let a0 = segments
            .append(&fs, 0, &frame::encode(&entry(0, 1, 0)))
            .unwrap();
        assert!(a0.created_segment);
        assert_eq!(a0.location.segment, 0);

        // Frame 1: active.size + frame == 2*unit == segment_size, which is NOT
        // greater than segment_size, so it fits — no rollover at the boundary.
        let a1 = segments
            .append(&fs, 1, &frame::encode(&entry(1, 1, 0)))
            .unwrap();
        assert!(!a1.created_segment, "exact fit must not roll over");
        assert_eq!(a1.location.segment, 0);
        assert_eq!(segments.active().unwrap().size, unit * 2);

        // Frame 2: active.size + frame == 3*unit > segment_size, so it rolls
        // into a new segment whose base index is the entry's index (2).
        let a2 = segments
            .append(&fs, 2, &frame::encode(&entry(2, 1, 0)))
            .unwrap();
        assert!(a2.created_segment, "exceeding the size must roll over");
        assert_eq!(a2.location.segment, 2);
        assert_eq!(a2.location.offset, 0, "new segment starts at offset 0");

        // Segment 0 is sealed and holds frames 0 and 1; segment 2 is active.
        assert_eq!(segments.sealed().len(), 1);
        let sealed0 = &segments.sealed()[0];
        assert_eq!(sealed0.base_index, 0);
        assert!(sealed0.sealed);
        assert_eq!(sealed0.size, unit * 2);
        assert_eq!(segments.active().unwrap().base_index, 2);

        // Reading each segment back yields its entries in ascending index order.
        let scan0 = scan_segment(&fs, &PathBuf::from(DIR).join(segment_file_name(0))).unwrap();
        let indices0: Vec<u64> = scan0.frames.iter().map(|f| f.entry.index).collect();
        assert_eq!(indices0, vec![0, 1]);
        assert_eq!(scan0.outcome, ScanOutcome::Clean);

        let scan2 = scan_segment(&fs, &PathBuf::from(DIR).join(segment_file_name(2))).unwrap();
        let indices2: Vec<u64> = scan2.frames.iter().map(|f| f.entry.index).collect();
        assert_eq!(indices2, vec![2]);
    }

    // --- oversized frame becomes the sole frame of its own segment (R3.4) ---

    #[test]
    fn oversized_frame_is_written_as_the_sole_frame_of_its_segment() {
        let fs = mem_fs();
        // Segment size smaller than a single frame: every frame is "oversized".
        let segment_size = frame_len(0) - 1;
        let mut segments = SegmentSet::new(DIR, segment_size);

        // An oversized first frame is written anyway into its fresh (empty)
        // segment — the empty active segment never triggers rollover, so there
        // is no infinite loop.
        let big = frame::encode(&entry(0, 1, 100));
        assert!(frame_len(100) > segment_size);
        let a0 = segments.append(&fs, 0, &big).unwrap();
        assert!(a0.created_segment);
        assert_eq!(a0.location.segment, 0);
        assert_eq!(a0.location.offset, 0);

        // The next frame cannot share the (now non-empty, already-oversized)
        // segment, so it rolls into its own new segment.
        let next = frame::encode(&entry(1, 1, 100));
        let a1 = segments.append(&fs, 1, &next).unwrap();
        assert!(a1.created_segment);
        assert_eq!(a1.location.segment, 1);
        assert_eq!(a1.location.offset, 0);

        // Each segment holds exactly one (oversized) frame.
        let scan0 = scan_segment(&fs, &PathBuf::from(DIR).join(segment_file_name(0))).unwrap();
        assert_eq!(scan0.frames.len(), 1);
        assert_eq!(scan0.frames[0].entry.index, 0);
        assert_eq!(scan0.outcome, ScanOutcome::Clean);

        let scan1 = scan_segment(&fs, &PathBuf::from(DIR).join(segment_file_name(1))).unwrap();
        assert_eq!(scan1.frames.len(), 1);
        assert_eq!(scan1.frames[0].entry.index, 1);
    }

    // --- validated scan classification (R3, R6 groundwork) -----------------

    #[test]
    fn scan_of_empty_segment_is_clean_with_no_frames() {
        let fs = mem_fs();
        let path = PathBuf::from(DIR).join(segment_file_name(0));
        fs.open_read_write(&path).unwrap();
        let scan = scan_segment(&fs, &path).unwrap();
        assert!(scan.frames.is_empty());
        assert_eq!(scan.outcome, ScanOutcome::Clean);
    }

    #[test]
    fn scan_reports_incomplete_tail_when_last_frame_is_truncated() {
        let fs = mem_fs();
        let mut segments = SegmentSet::new(DIR, 4096);
        segments
            .append(&fs, 0, &frame::encode(&entry(0, 1, 4)))
            .unwrap();
        let a1 = segments
            .append(&fs, 1, &frame::encode(&entry(1, 1, 4)))
            .unwrap();

        // Drop the final two bytes of the second frame: a torn last write.
        let path = PathBuf::from(DIR).join(segment_file_name(0));
        let full = fs.file_size(&path).unwrap();
        fs.truncate_file(&path, full - 2);

        let scan = scan_segment(&fs, &path).unwrap();
        // The first frame survives; the torn tail is flagged at its offset.
        assert_eq!(scan.frames.len(), 1);
        assert_eq!(scan.frames[0].entry.index, 0);
        assert_eq!(
            scan.outcome,
            ScanOutcome::Incomplete {
                offset: a1.location.offset
            }
        );
    }

    #[test]
    fn scan_reports_corrupt_frame_at_its_offset() {
        let fs = mem_fs();
        let mut segments = SegmentSet::new(DIR, 4096);
        segments
            .append(&fs, 0, &frame::encode(&entry(0, 1, 4)))
            .unwrap();
        let a1 = segments
            .append(&fs, 1, &frame::encode(&entry(1, 1, 4)))
            .unwrap();
        segments
            .append(&fs, 2, &frame::encode(&entry(2, 1, 4)))
            .unwrap();

        // Corrupt a payload byte of the middle frame, leaving a valid frame
        // after it — the shape recovery treats as interior corruption.
        let path = PathBuf::from(DIR).join(segment_file_name(0));
        let mut bytes = fs.file_bytes(&path).unwrap();
        let payload_byte = a1.location.offset as usize + 4 + 17; // len + body header
        bytes[payload_byte] ^= 0xFF;
        // Rewrite the whole file with the corrupted bytes.
        let path2 = PathBuf::from(DIR).join("corrupt.wal");
        let file = fs.open_read_write(&path2).unwrap();
        file.write_at(0, &bytes).unwrap();

        let scan = scan_segment(&fs, &path2).unwrap();
        // Frame 0 decodes; the scan stops at the corrupt frame 1.
        assert_eq!(scan.frames.len(), 1);
        assert_eq!(scan.frames[0].entry.index, 0);
        assert_eq!(
            scan.outcome,
            ScanOutcome::Corrupt {
                offset: a1.location.offset
            }
        );
    }

    // --- read_entry from a sealed segment ----------------------------------

    #[test]
    fn read_entry_reads_from_sealed_and_active_segments() {
        let fs = mem_fs();
        let unit = frame_len(4);
        // Hold exactly one frame per segment so the first append seals quickly.
        let mut segments = SegmentSet::new(DIR, unit);

        let e0 = entry(0, 7, 4);
        let e1 = entry(1, 9, 4);
        let a0 = segments.append(&fs, 0, &frame::encode(&e0)).unwrap();
        let a1 = segments.append(&fs, 1, &frame::encode(&e1)).unwrap();

        // e0 now lives in a sealed segment, e1 in the active one.
        assert_eq!(segments.sealed().len(), 1);
        assert_eq!(
            segments
                .read_entry(
                    &fs,
                    a0.location.segment,
                    a0.location.offset,
                    a0.location.len
                )
                .unwrap(),
            e0
        );
        assert_eq!(
            segments
                .read_entry(
                    &fs,
                    a1.location.segment,
                    a1.location.offset,
                    a1.location.len
                )
                .unwrap(),
            e1
        );
    }

    #[test]
    fn read_entry_errors_for_an_unknown_segment() {
        let fs = mem_fs();
        let mut segments = SegmentSet::new(DIR, 4096);
        let a0 = segments
            .append(&fs, 0, &frame::encode(&entry(0, 1, 4)))
            .unwrap();
        // No segment has base index 99.
        let err = segments
            .read_entry(&fs, 99, a0.location.offset, a0.location.len)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
