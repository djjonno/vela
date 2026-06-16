//! Durable manifest (the requirements' `Frame_Metadata`).
//!
//! The manifest is the single source of truth for the durably-acknowledged
//! extent, the `commit_index`, and the `log_start_index` (Requirements 6.8,
//! 7.8, 9.1). It is one file, [`MANIFEST_FILE_NAME`], holding **two fixed-size
//! slots** (double-buffering) at fixed offsets — slot 0 at byte `0`, slot 1 at
//! byte [`SLOT_SIZE`].
//!
//! # Slot layout
//!
//! Each slot is [`SLOT_SIZE`] bytes; the first [`SLOT_USED`] carry the record
//! and its CRC, the remainder is zero padding so the two slots live at fixed
//! offsets regardless of contents:
//!
//! ```text
//! offset  size  field
//! 0       4     magic   u32 LE  ("VWAL")
//! 4       2     version u16 LE
//! 6       8     seq     u64 LE  (monotonic; the highest valid slot wins)
//! 14      8     log_start_index u64 LE
//! 22      1+8   commit          tag(u8) + u64 LE   (0 => None, 1 => Some)
//! 31      1+8   durable_last    tag(u8) + u64 LE   (0 => None, 1 => Some)
//! 40      8     durable_segment u64 LE  (base index of the segment holding durable_last)
//! 48      8     durable_offset  u64 LE  (byte offset just past durable_last's frame)
//! 56      8     hs_current_term u64 LE  (Raft hard state: persisted current term)
//! 64      1+8   hs_voted_for    tag(u8) + u64 LE   (0 => None, 1 => Some) Raft vote
//! 73      4     crc     u32 LE  (CRC32C over bytes [0 .. 73), i.e. all preceding fields)
//! 77..128       zero padding
//! ```
//!
//! # Crash-atomic double-buffering (Requirement 6.8)
//!
//! A [`Manifest::write`] writes the **other** slot (alternating), then fsyncs;
//! only then is the update durable. [`Manifest::open`] reads both slots,
//! discards any with a bad magic, an unknown version, or a failed CRC, and
//! selects the valid slot with the highest `seq`. A crash mid-update therefore
//! always leaves at least the previous intact slot, and recovery falls back to
//! it rather than trusting a torn newest slot. A directory with no manifest, or
//! one whose every slot is invalid, yields a defaulted/empty [`ManifestState`].

// `allow(dead_code)`: every item here is reachable only once `DurableWal` is
// assembled (tasks 8–13) and re-exported from `lib.rs` (task 16). `append`
// advances the acknowledged extent, `commit` updates `commit_index`, `revert`
// reduces the extent, and `compaction` advances `log_start_index`; `open` plus
// recovery (task 12) read the best slot back. Until those call sites land, the
// manifest has no non-test caller on the library target and would otherwise
// trip the `dead_code` lint under `-D warnings`; the unit tests below already
// exercise every path. The allow is narrowed as those tasks wire it in,
// mirroring how `frame.rs`/`fs.rs`/`segment.rs` scope the same allow.
#![allow(dead_code)]

use std::io;
use std::path::{Path, PathBuf};

use super::frame::Crc32c;
use super::fs::{FileSystem, WalFile};

/// File name of the manifest within the Data_Directory.
pub(crate) const MANIFEST_FILE_NAME: &str = "wal.manifest";

/// Magic tag identifying a manifest slot: the ASCII bytes `"VWAL"`, read little
/// endian. A slot whose first four bytes are not this (e.g. zero padding from an
/// unwritten slot) is treated as empty/invalid.
const MAGIC: u32 = u32::from_le_bytes(*b"VWAL");

/// On-disk slot format version. A slot carrying any other version is rejected
/// by [`decode_slot`], so a future format change cannot be misread as valid.
///
/// Bumped from `1` to `2` when the Raft hard state (`hs_current_term`,
/// `hs_voted_for`) was added to the slot: a slot written by the older format is
/// rejected on read and recovery falls back to the empty state rather than
/// misreading the shorter layout.
const VERSION: u16 = 2;

/// Number of double-buffering slots in the manifest file.
const SLOT_COUNT: u64 = 2;

/// Fixed byte size of one slot. Padded well beyond [`SLOT_USED`] so the slot
/// boundaries stay at fixed offsets and leave room for future fields without a
/// layout change.
const SLOT_SIZE: u64 = 128;

// Field offsets within a slot (see the module-level layout table).
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_SEQ: usize = 6;
const OFF_LOG_START: usize = 14;
const OFF_COMMIT_TAG: usize = 22;
const OFF_COMMIT_VAL: usize = 23;
const OFF_DLAST_TAG: usize = 31;
const OFF_DLAST_VAL: usize = 32;
const OFF_DSEG: usize = 40;
const OFF_DOFF: usize = 48;
const OFF_HS_TERM: usize = 56;
const OFF_HS_VOTE_TAG: usize = 64;
const OFF_HS_VOTE_VAL: usize = 65;
const OFF_CRC: usize = 73;

/// Number of bytes a slot actually uses: the fields plus the trailing CRC. The
/// rest of the slot (up to [`SLOT_SIZE`]) is zero padding.
const SLOT_USED: usize = OFF_CRC + 4;

/// The durable state recorded by the manifest — the requirements'
/// `Frame_Metadata`.
///
/// `durable_last`/`durable_segment`/`durable_offset` together describe the
/// **acknowledged extent**: the high-water mark of data the WAL has promised is
/// durable. `durable_segment` is the base index of the segment holding the last
/// acknowledged frame and `durable_offset` is the byte offset just past that
/// frame; both are meaningful only when `durable_last` is `Some`.
///
/// [`Default`] is the fresh/empty state: nothing retained below index 0, nothing
/// committed, and no acknowledged extent — exactly what a brand-new log reports
/// (Requirement 5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ManifestState {
    /// Lowest retained absolute index; advanced by compaction (Requirement 7).
    pub log_start_index: u64,
    /// Commit position, `None` before anything is committed (Requirement 9).
    pub commit_index: Option<u64>,
    /// Highest durably-acknowledged absolute index, or `None` when empty.
    pub durable_last: Option<u64>,
    /// Base index of the segment containing `durable_last`'s frame.
    pub durable_segment: u64,
    /// Byte offset just past `durable_last`'s frame within its segment.
    pub durable_offset: u64,
    /// Raft hard state: the replica's persisted `current_term` (Requirement
    /// 9.2, 10.1). `0` for a fresh log that has never persisted hard state.
    pub hs_current_term: u64,
    /// Raft hard state: the candidate this replica persisted a vote for in
    /// `hs_current_term`, or `None` when it has not voted in that term
    /// (Requirements 9.1, 10.2).
    pub hs_voted_for: Option<u64>,
}

/// Encode `(seq, state)` into a fixed [`SLOT_SIZE`]-byte slot buffer.
///
/// The CRC is computed over the populated prefix `[0 .. OFF_CRC)` and stored at
/// [`OFF_CRC`]; the trailing bytes are left as zero padding (Requirement 6.8).
fn encode_slot(seq: u64, state: &ManifestState) -> [u8; SLOT_SIZE as usize] {
    let mut buf = [0u8; SLOT_SIZE as usize];

    buf[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(&MAGIC.to_le_bytes());
    buf[OFF_VERSION..OFF_VERSION + 2].copy_from_slice(&VERSION.to_le_bytes());
    buf[OFF_SEQ..OFF_SEQ + 8].copy_from_slice(&seq.to_le_bytes());
    buf[OFF_LOG_START..OFF_LOG_START + 8].copy_from_slice(&state.log_start_index.to_le_bytes());
    put_option(&mut buf, OFF_COMMIT_TAG, OFF_COMMIT_VAL, state.commit_index);
    put_option(&mut buf, OFF_DLAST_TAG, OFF_DLAST_VAL, state.durable_last);
    buf[OFF_DSEG..OFF_DSEG + 8].copy_from_slice(&state.durable_segment.to_le_bytes());
    buf[OFF_DOFF..OFF_DOFF + 8].copy_from_slice(&state.durable_offset.to_le_bytes());
    buf[OFF_HS_TERM..OFF_HS_TERM + 8].copy_from_slice(&state.hs_current_term.to_le_bytes());
    put_option(
        &mut buf,
        OFF_HS_VOTE_TAG,
        OFF_HS_VOTE_VAL,
        state.hs_voted_for,
    );

    let crc = Crc32c::checksum(&buf[..OFF_CRC]);
    buf[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());

    buf
}

/// Decode a slot from the front of `buf`, returning `(seq, state)` only for a
/// structurally valid, checksum-verified slot.
///
/// Returns `None` — meaning "treat this slot as absent" — when the buffer is too
/// short, the magic or version does not match, an option tag is neither `0` nor
/// `1`, or the stored CRC does not equal the CRC recomputed over the preceding
/// bytes. This is what lets [`Manifest::open`] discard a torn newest slot and
/// fall back to the previous intact one (Requirement 6.8).
fn decode_slot(buf: &[u8]) -> Option<(u64, ManifestState)> {
    if buf.len() < SLOT_USED {
        return None;
    }

    if read_u32(buf, OFF_MAGIC) != MAGIC {
        return None;
    }
    if u16::from_le_bytes([buf[OFF_VERSION], buf[OFF_VERSION + 1]]) != VERSION {
        return None;
    }

    let stored_crc = read_u32(buf, OFF_CRC);
    if Crc32c::checksum(&buf[..OFF_CRC]) != stored_crc {
        return None;
    }

    let seq = read_u64(buf, OFF_SEQ);
    let commit_index = get_option(buf, OFF_COMMIT_TAG, OFF_COMMIT_VAL)?;
    let durable_last = get_option(buf, OFF_DLAST_TAG, OFF_DLAST_VAL)?;
    let hs_voted_for = get_option(buf, OFF_HS_VOTE_TAG, OFF_HS_VOTE_VAL)?;

    let state = ManifestState {
        log_start_index: read_u64(buf, OFF_LOG_START),
        commit_index,
        durable_last,
        durable_segment: read_u64(buf, OFF_DSEG),
        durable_offset: read_u64(buf, OFF_DOFF),
        hs_current_term: read_u64(buf, OFF_HS_TERM),
        hs_voted_for,
    };
    Some((seq, state))
}

/// Write an `Option<u64>` as a `tag(u8) + u64 LE` pair: tag `1` and the value
/// for `Some`, tag `0` (value left zero) for `None`.
fn put_option(buf: &mut [u8], tag_off: usize, val_off: usize, opt: Option<u64>) {
    if let Some(value) = opt {
        buf[tag_off] = 1;
        buf[val_off..val_off + 8].copy_from_slice(&value.to_le_bytes());
    } else {
        buf[tag_off] = 0;
    }
}

/// Read an `Option<u64>` written by [`put_option`]. A tag of `0` is `None`, `1`
/// is `Some(value)`, and any other tag is structurally invalid, yielding `None`
/// from the outer `?` so the whole slot is rejected.
fn get_option(buf: &[u8], tag_off: usize, val_off: usize) -> Option<Option<u64>> {
    match buf[tag_off] {
        0 => Some(None),
        1 => Some(Some(read_u64(buf, val_off))),
        _ => None,
    }
}

/// Read a little-endian `u32` at `off` (caller guarantees the bytes exist).
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().expect("4-byte u32 slice"))
}

/// Read a little-endian `u64` at `off` (caller guarantees the bytes exist).
fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().expect("8-byte u64 slice"))
}

/// The double-buffered manifest writer.
///
/// Holds the resolved manifest path and just enough state to keep alternation
/// and highest-`seq`-wins correct across writes and reopens: the `seq` of the
/// most recently durable slot, and which slot the next [`write`](Manifest::write)
/// should target. It is not generic over the filesystem — the [`FileSystem`] is
/// supplied per call — so `DurableWal` can hold a plain `Manifest`.
#[derive(Debug, Clone)]
pub(crate) struct Manifest {
    /// Absolute path of the manifest file.
    path: PathBuf,
    /// Sequence number of the most recently durable slot; the next write uses
    /// `seq + 1`.
    seq: u64,
    /// Index of the slot the next write targets (alternates `0`/`1`).
    next_slot: u64,
}

impl Manifest {
    /// Open the manifest in `dir`, returning the writer plus the best recovered
    /// [`ManifestState`], or `None` when the directory has no manifest or no
    /// intact slot (a fresh log).
    ///
    /// The next write targets the slot **other** than the winning one, so a
    /// reopened manifest keeps alternating and the latest state continues to win
    /// (Requirement 6.8).
    pub(crate) fn open<F: FileSystem>(
        fs: &F,
        dir: &Path,
    ) -> io::Result<(Self, Option<ManifestState>)> {
        let path = dir.join(MANIFEST_FILE_NAME);

        let best = if fs.exists(&path) {
            read_best_slot(fs, &path)?
        } else {
            None
        };

        let (seq, next_slot, state) = match best {
            // A valid slot was found: continue its `seq`, and write the other
            // slot next so the double-buffer keeps alternating.
            Some((seq, winning_slot, state)) => (seq, (winning_slot + 1) % SLOT_COUNT, Some(state)),
            // Fresh (or wholly invalid) manifest: start at slot 0 with seq 0, so
            // the first write becomes seq 1 in slot 0.
            None => (0, 0, None),
        };

        Ok((
            Self {
                path,
                seq,
                next_slot,
            },
            state,
        ))
    }

    /// Persist `state` to the alternate slot with a bumped `seq`, then force the
    /// manifest file to stable storage before returning (Requirement 6.8).
    ///
    /// On success the just-written slot becomes the durable winner and the next
    /// write flips to the other slot. The slot write extends the file as needed,
    /// so a fresh manifest is created on the first call.
    pub(crate) fn write<F: FileSystem>(&mut self, fs: &F, state: &ManifestState) -> io::Result<()> {
        let seq = self.seq + 1;
        let slot = self.next_slot;
        let bytes = encode_slot(seq, state);

        let file = fs.open_read_write(&self.path)?;
        file.write_at(slot * SLOT_SIZE, &bytes)?;
        file.sync_all()?;

        // Only advance our bookkeeping once the slot is durable.
        self.seq = seq;
        self.next_slot = (slot + 1) % SLOT_COUNT;
        Ok(())
    }

    /// Force the manifest file to stable storage without writing a new slot.
    ///
    /// Used by `flush` (task 14) and the `Always`/`Periodic` policies to fsync
    /// the manifest on demand. A manifest that has never been written has
    /// nothing to force, which is a success no-op rather than an error.
    pub(crate) fn sync<F: FileSystem>(&self, fs: &F) -> io::Result<()> {
        match fs.open_read(&self.path) {
            Ok(file) => file.sync_all(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}

/// Read both slots of the manifest at `path` and return the valid one with the
/// highest `seq`, as `(seq, winning_slot_index, state)`.
///
/// Reads the whole (small, fixed-size) file once, then decodes each slot from
/// its fixed offset. Slots that are too short, have a bad magic/version, or fail
/// their CRC are skipped; among the rest the highest `seq` wins. Returns `None`
/// when no slot is valid (Requirement 6.8).
fn read_best_slot<F: FileSystem>(
    fs: &F,
    path: &Path,
) -> io::Result<Option<(u64, u64, ManifestState)>> {
    let file = fs.open_read(path)?;
    let size = file.size()?;
    let mut bytes = vec![0u8; size as usize];
    if size > 0 {
        file.read_exact_at(0, &mut bytes)?;
    }

    let mut best: Option<(u64, u64, ManifestState)> = None;
    for slot in 0..SLOT_COUNT {
        let start = (slot * SLOT_SIZE) as usize;
        if start >= bytes.len() {
            break;
        }
        if let Some((seq, state)) = decode_slot(&bytes[start..]) {
            // Strictly greater so the first slot wins any (impossible) tie.
            if best.is_none_or(|(best_seq, _, _)| seq > best_seq) {
                best = Some((seq, slot, state));
            }
        }
    }
    Ok(best)
}

#[cfg(test)]
mod tests {
    use super::super::fs::fault::MemFileSystem;
    use super::super::fs::FileSystem;
    use super::*;
    use std::path::Path;

    /// Data directory used by the in-memory filesystem tests.
    const DIR: &str = "/wal";

    /// A fresh in-memory filesystem with the data directory created.
    fn mem_fs() -> MemFileSystem {
        let fs = MemFileSystem::new();
        fs.create_dir_all(Path::new(DIR)).unwrap();
        fs
    }

    /// The manifest file path within [`DIR`].
    fn manifest_path() -> PathBuf {
        PathBuf::from(DIR).join(MANIFEST_FILE_NAME)
    }

    /// A representative, fully-populated state for round-trip tests. Includes a
    /// non-default Raft hard state so the version-2 fields are exercised.
    fn sample_state() -> ManifestState {
        ManifestState {
            log_start_index: 4,
            commit_index: Some(9),
            durable_last: Some(12),
            durable_segment: 4,
            durable_offset: 512,
            hs_current_term: 7,
            hs_voted_for: Some(3),
        }
    }

    /// Decode slot `slot` directly from the manifest file's raw bytes, for
    /// asserting on the physical double-buffer layout.
    fn decode_file_slot(fs: &MemFileSystem, slot: u64) -> Option<(u64, ManifestState)> {
        let bytes = fs.file_bytes(&manifest_path())?;
        let start = (slot * SLOT_SIZE) as usize;
        if start >= bytes.len() {
            return None;
        }
        decode_slot(&bytes[start..])
    }

    // --- fresh / empty manifest --------------------------------------------

    #[test]
    fn open_on_empty_dir_yields_no_state() {
        let fs = mem_fs();
        let (manifest, state) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        // No manifest file exists yet: a fresh log, defaulting to slot 0/seq 0.
        assert_eq!(state, None);
        assert_eq!(manifest.seq, 0);
        assert_eq!(manifest.next_slot, 0);
        // Opening did not create the manifest file.
        assert!(fs.file_bytes(&manifest_path()).is_none());
    }

    #[test]
    fn default_state_is_the_empty_log() {
        // The fresh state a caller substitutes for `None` is the empty log.
        assert_eq!(
            ManifestState::default(),
            ManifestState {
                log_start_index: 0,
                commit_index: None,
                durable_last: None,
                durable_segment: 0,
                durable_offset: 0,
                hs_current_term: 0,
                hs_voted_for: None,
            }
        );
    }

    // --- write / read round-trip -------------------------------------------

    #[test]
    fn write_then_reopen_round_trips_state() {
        let fs = mem_fs();
        let state = sample_state();

        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &state).unwrap();

        // A new opener on the same directory recovers exactly what was written.
        let (reopened, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(state));
        // First write went to slot 0 (seq 1); the next write must target slot 1.
        assert_eq!(reopened.seq, 1);
        assert_eq!(reopened.next_slot, 1);
    }

    #[test]
    fn round_trips_none_options() {
        let fs = mem_fs();
        // `commit_index` and `durable_last` both absent must survive the trip.
        let state = ManifestState {
            log_start_index: 0,
            commit_index: None,
            durable_last: None,
            durable_segment: 0,
            durable_offset: 0,
            hs_current_term: 0,
            hs_voted_for: None,
        };

        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &state).unwrap();

        let (_, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(state));
    }

    // --- hard state (version-2 fields) round-trip --------------------------

    #[test]
    fn round_trips_hard_state_fields() {
        let fs = mem_fs();
        // A populated hard state (term + a vote) must survive encode/decode and
        // a reopen byte-for-byte, exercising the new version-2 slot fields.
        let state = ManifestState {
            hs_current_term: 42,
            hs_voted_for: Some(7),
            ..ManifestState::default()
        };

        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &state).unwrap();

        let (_, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(state));
        assert_eq!(recovered.unwrap().hs_current_term, 42);
        assert_eq!(recovered.unwrap().hs_voted_for, Some(7));
    }

    #[test]
    fn round_trips_hard_state_without_a_vote() {
        let fs = mem_fs();
        // A term advanced with no vote yet (`voted_for == None`) must round-trip
        // with its `None` tag preserved.
        let state = ManifestState {
            hs_current_term: 9,
            hs_voted_for: None,
            ..ManifestState::default()
        };

        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &state).unwrap();

        let (_, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(state));
    }

    #[test]
    fn slot_uses_the_bumped_version() {
        // The hard-state-bearing slot is written under version 2; a slot
        // claiming the older version 1 is rejected, so a pre-upgrade slot can
        // never be misread as the longer version-2 layout.
        assert_eq!(VERSION, 2);
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &sample_state()).unwrap();

        // The persisted slot carries the current version in its version field.
        let bytes = fs.file_bytes(&manifest_path()).unwrap();
        let stored = u16::from_le_bytes([bytes[OFF_VERSION], bytes[OFF_VERSION + 1]]);
        assert_eq!(stored, VERSION);
    }

    #[test]
    fn torn_newest_slot_falls_back_to_prior_hard_state() {
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();

        // Two slots with distinct hard states: the older grants a vote in
        // term 3, the newer advances to term 4.
        let older = ManifestState {
            hs_current_term: 3,
            hs_voted_for: Some(1),
            ..ManifestState::default()
        }; // seq 1 -> slot 0
        let newer = ManifestState {
            hs_current_term: 4,
            hs_voted_for: Some(2),
            ..ManifestState::default()
        }; // seq 2 -> slot 1
        manifest.write(&fs, &older).unwrap();
        manifest.write(&fs, &newer).unwrap();

        // Tear the newest slot (slot 1) so it no longer decodes.
        fs.truncate_file(&manifest_path(), SLOT_SIZE + OFF_CRC as u64);

        // Recovery falls back to the prior intact slot, restoring its exact
        // hard state rather than trusting the torn newest slot.
        let (_, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(older));
        assert_eq!(recovered.unwrap().hs_current_term, 3);
        assert_eq!(recovered.unwrap().hs_voted_for, Some(1));
    }

    // --- alternation across writes (double-buffering) ----------------------

    #[test]
    fn writes_alternate_slots_and_bump_seq() {
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();

        let a = ManifestState {
            log_start_index: 0,
            commit_index: Some(0),
            durable_last: Some(0),
            durable_segment: 0,
            durable_offset: 10,
            hs_current_term: 0,
            hs_voted_for: None,
        };
        let b = ManifestState {
            commit_index: Some(1),
            durable_last: Some(1),
            durable_offset: 20,
            ..a
        };
        let c = ManifestState {
            commit_index: Some(2),
            durable_last: Some(2),
            durable_offset: 30,
            ..a
        };

        manifest.write(&fs, &a).unwrap(); // seq 1 -> slot 0
        manifest.write(&fs, &b).unwrap(); // seq 2 -> slot 1
        manifest.write(&fs, &c).unwrap(); // seq 3 -> slot 0 (overwrites a)

        // Slot 0 now holds the newest (seq 3 / c); slot 1 holds seq 2 / b.
        assert_eq!(decode_file_slot(&fs, 0), Some((3, c)));
        assert_eq!(decode_file_slot(&fs, 1), Some((2, b)));

        // The highest seq wins on reopen.
        let (reopened, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(c));
        assert_eq!(reopened.seq, 3);
        // Winner is slot 0, so the next write must flip back to slot 1.
        assert_eq!(reopened.next_slot, 1);
    }

    // --- torn newest slot falls back to the prior intact slot (R6.8) -------

    #[test]
    fn truncated_newest_slot_falls_back_to_prior() {
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();

        let older = sample_state(); // seq 1 -> slot 0
        let newer = ManifestState {
            commit_index: Some(10),
            durable_last: Some(13),
            ..older
        }; // seq 2 -> slot 1
        manifest.write(&fs, &older).unwrap();
        manifest.write(&fs, &newer).unwrap();

        // Tear the newest slot (slot 1) by dropping its CRC and tail: cut the
        // file just short of slot 1's CRC so slot 1 no longer decodes.
        fs.truncate_file(&manifest_path(), SLOT_SIZE + OFF_CRC as u64);

        // Recovery falls back to the last intact slot (slot 0 / `older`).
        let (reopened, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(older));
        // The intact winner is slot 0 (seq 1); the next write targets slot 1.
        assert_eq!(reopened.seq, 1);
        assert_eq!(reopened.next_slot, 1);
    }

    #[test]
    fn corrupted_newest_slot_falls_back_to_prior() {
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();

        let older = sample_state(); // seq 1 -> slot 0
        let newer = ManifestState {
            commit_index: Some(11),
            ..older
        }; // seq 2 -> slot 1
        manifest.write(&fs, &older).unwrap();
        manifest.write(&fs, &newer).unwrap();

        // Flip a byte inside slot 1's `seq` field so its CRC no longer matches.
        flip_byte(&fs, SLOT_SIZE + OFF_SEQ as u64);

        // The CRC mismatch makes slot 1 invalid; slot 0 (`older`) wins.
        let (_, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(older));
    }

    // --- CRC rejects a corrupted sole slot ---------------------------------

    #[test]
    fn corrupting_the_only_slot_yields_fresh_state() {
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &sample_state()).unwrap(); // seq 1 -> slot 0

        // Corrupt a payload byte in slot 0; with no other valid slot, recovery
        // sees an empty manifest and reports a fresh log.
        flip_byte(&fs, OFF_LOG_START as u64);

        let (reopened, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, None);
        assert_eq!(reopened.seq, 0);
        assert_eq!(reopened.next_slot, 0);
    }

    #[test]
    fn unknown_version_is_rejected() {
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &sample_state()).unwrap();

        // Bump the stored version so the slot no longer matches `VERSION`. The
        // CRC still covers it, but the version gate rejects the slot first.
        let mut bytes = fs.file_bytes(&manifest_path()).unwrap();
        let bad_version = VERSION + 1;
        bytes[OFF_VERSION..OFF_VERSION + 2].copy_from_slice(&bad_version.to_le_bytes());
        let crc = Crc32c::checksum(&bytes[..OFF_CRC]);
        bytes[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
        write_all(&fs, &bytes);

        let (_, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, None);
    }

    // --- reopen continues seq so the latest state always wins --------------

    #[test]
    fn reopen_continues_seq_and_keeps_latest_winning() {
        let fs = mem_fs();

        let states = [
            ManifestState {
                commit_index: Some(0),
                durable_last: Some(0),
                ..ManifestState::default()
            },
            ManifestState {
                commit_index: Some(1),
                durable_last: Some(1),
                durable_offset: 64,
                ..ManifestState::default()
            },
            ManifestState {
                commit_index: Some(2),
                durable_last: Some(2),
                durable_offset: 128,
                ..ManifestState::default()
            },
        ];

        // Write each state through a freshly reopened manifest, proving the
        // writer picks up the correct `seq`/slot after every reopen.
        let mut expected_seq = 0;
        for state in &states {
            let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
            assert_eq!(manifest.seq, expected_seq);
            manifest.write(&fs, state).unwrap();
            expected_seq += 1;
            assert_eq!(manifest.seq, expected_seq);
        }

        // The final reopen recovers the last state with the highest seq.
        let (reopened, recovered) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        assert_eq!(recovered, Some(states[2]));
        assert_eq!(reopened.seq, 3);
    }

    // --- sync ---------------------------------------------------------------

    #[test]
    fn sync_is_a_noop_before_any_write() {
        let fs = mem_fs();
        let (manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        // Nothing has been written, so there is nothing to force: success.
        assert!(manifest.sync(&fs).is_ok());
        assert!(fs.file_bytes(&manifest_path()).is_none());
    }

    #[test]
    fn sync_after_write_succeeds() {
        let fs = mem_fs();
        let (mut manifest, _) = Manifest::open(&fs, Path::new(DIR)).unwrap();
        manifest.write(&fs, &sample_state()).unwrap();
        assert!(manifest.sync(&fs).is_ok());
    }

    // --- test helpers -------------------------------------------------------

    /// Flip the low bit of the byte at `offset` in the manifest file, so any
    /// CRC covering it no longer matches.
    fn flip_byte(fs: &MemFileSystem, offset: u64) {
        let mut bytes = fs.file_bytes(&manifest_path()).unwrap();
        bytes[offset as usize] ^= 0x01;
        write_all(fs, &bytes);
    }

    /// Overwrite the whole manifest file with `bytes`.
    fn write_all(fs: &MemFileSystem, bytes: &[u8]) {
        let file = fs.open_read_write(&manifest_path()).unwrap();
        file.write_at(0, bytes).unwrap();
    }
}
