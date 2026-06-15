//! In-memory index and absolute-index mapping.
//!
//! The Durable_WAL keeps an **in-memory index but not payload bytes**
//! (Requirement 13): for each retained Log_Entry it holds the entry's `term`
//! and the on-disk location of its Record_Frame (`segment`, `offset`, `len`),
//! while the payload bytes live only in the segment files and are read back on
//! demand. Memory therefore scales with the number of retained entries, not
//! with payload volume.
//!
//! [`LogIndex`] stores those per-entry [`IndexEntry`] records densely in a
//! `Vec` whose slot `0` corresponds to the absolute index
//! [`log_start_index`](LogIndex::log_start_index). This realizes the **absolute
//! (non-renumbering) index model** of Requirement 8: an entry's absolute index
//! never changes, so after Compaction advances `log_start_index` a lookup for
//! absolute index `i` must offset by the start — the naive "index == array
//! position" mapping is wrong once a prefix has been discarded.
//!
//! # Absolute-index mapping (R8, R13)
//!
//! For a non-empty index:
//!
//! - `last_index() = log_start_index + len - 1`
//! - the physical slot for absolute index `i` is `i - log_start_index`, valid
//!   only when `log_start_index <= i <= last_index`
//!
//! Lookups for an index **below** `log_start_index` (discarded by Compaction)
//! or **above** `last_index` (never appended) return `None` without erroring
//! (Requirements 8.1, 8.2): these queries are answered from memory and cannot
//! fail (Requirement 13.2).

// `allow(dead_code)`: this module defines the in-memory index that the rest of
// the WAL is built on, but its mutators and accessors have no non-test caller
// until `DurableWal` is assembled. `append`/`last_index`/`term_at` land in task
// 8, the disk-backed reads consume [`LogIndex::location`] in task 9,
// `append_entries`/`revert` use [`LogIndex::truncate_from`] in tasks 10–11,
// recovery rebuilds the index in task 12, and Compaction uses
// [`LogIndex::compact_to`] in task 13. Until those call sites land these items
// would otherwise trip the `dead_code` lint under `-D warnings`; the unit tests
// below already exercise every one. The allow is narrowed as the tasks wire the
// pieces in, mirroring how `frame.rs`, `fs.rs`, and `segment.rs` scope it.
#![allow(dead_code)]

use super::segment::FrameLocation;

/// One entry's in-memory index record: its Raft `term` plus the on-disk
/// location of its Record_Frame. It deliberately carries **no payload bytes**
/// (Requirements 13.1, 13.4) — the payload is read from the segment file via
/// the location on demand.
///
/// The three location fields (`segment`, `offset`, `len`) are exactly a
/// [`FrameLocation`]; [`from_location`](IndexEntry::from_location) and
/// [`location`](IndexEntry::location) convert between the two without
/// duplicating the layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    /// Raft term in which the entry was created.
    pub term: u64,
    /// Base index of the segment file holding the entry's frame.
    pub segment: u64,
    /// Byte offset of the frame within its segment.
    pub offset: u64,
    /// Total encoded byte length of the frame.
    pub len: u32,
}

impl IndexEntry {
    /// Build an [`IndexEntry`] from a frame `location` and the entry's `term`.
    pub(crate) fn from_location(location: FrameLocation, term: u64) -> Self {
        Self {
            term,
            segment: location.segment,
            offset: location.offset,
            len: location.len,
        }
    }

    /// The on-disk [`FrameLocation`] of this entry's Record_Frame.
    pub(crate) fn location(&self) -> FrameLocation {
        FrameLocation {
            segment: self.segment,
            offset: self.offset,
            len: self.len,
        }
    }
}

/// The dense in-memory index of one partition log.
///
/// Holds the retained entries' [`IndexEntry`] records in a `Vec` whose slot `0`
/// is the absolute index [`log_start_index`](LogIndex::log_start_index). All
/// index/term/location queries are served from memory and cannot fail
/// (Requirement 13.2); payload-bearing reads happen one layer up in
/// `DurableWal` using the [`FrameLocation`] this index returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LogIndex {
    /// Retained entries, dense and ascending; `entries[0]` is `log_start`.
    entries: Vec<IndexEntry>,
    /// Absolute index of `entries[0]`; the lowest retained index. `0` for a log
    /// that has never been compacted, increasing as Compaction discards a
    /// prefix (Requirement 8 / Log_Start_Index).
    log_start: u64,
}

impl LogIndex {
    /// An empty index whose first appended entry will take absolute index
    /// `log_start`. Use `0` for a fresh log; a larger value reconstructs a log
    /// that begins above `0` after Compaction.
    pub(crate) fn new(log_start: u64) -> Self {
        Self {
            entries: Vec::new(),
            log_start,
        }
    }

    /// Rebuild an index from already-ordered `entries` whose first element has
    /// absolute index `log_start`. Used by WAL_Recovery (task 12) to restore
    /// the in-memory index from the scanned segments.
    pub(crate) fn from_entries(log_start: u64, entries: Vec<IndexEntry>) -> Self {
        Self { entries, log_start }
    }

    /// Number of retained entries.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index holds no retained entries.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The lowest retained absolute index (Log_Start_Index). `0` until
    /// Compaction advances it.
    pub(crate) fn log_start_index(&self) -> u64 {
        self.log_start
    }

    /// The highest retained absolute index, or `None` when the index is empty
    /// (Requirement 8). Equals `log_start_index + len - 1` when non-empty.
    pub(crate) fn last_index(&self) -> Option<u64> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.log_start + self.entries.len() as u64 - 1)
        }
    }

    /// The index assigned to the next appended entry: `last_index + 1`, or
    /// `log_start_index` when the index is empty (including immediately after
    /// Compaction). This is the absolute index `append` assigns and never reuses
    /// an index below `log_start_index` (Requirement 8.5).
    pub(crate) fn next_index(&self) -> u64 {
        match self.last_index() {
            Some(last) => last + 1,
            None => self.log_start,
        }
    }

    /// The physical slot holding absolute index `abs_index`, or `None` when it
    /// lies below `log_start_index` or above `last_index`.
    ///
    /// This is the single place the `i - log_start_index` mapping (and its
    /// bounds) lives; every accessor below routes through it.
    fn slot(&self, abs_index: u64) -> Option<usize> {
        if abs_index < self.log_start {
            return None; // below the retained prefix (discarded by Compaction)
        }
        let slot = (abs_index - self.log_start) as usize;
        if slot < self.entries.len() {
            Some(slot)
        } else {
            None // at or above next_index: never appended
        }
    }

    /// The [`IndexEntry`] at absolute `abs_index`, or `None` when out of range
    /// (below `log_start_index` or above `last_index`) — Requirement 8.
    pub(crate) fn get(&self, abs_index: u64) -> Option<&IndexEntry> {
        self.slot(abs_index).map(|slot| &self.entries[slot])
    }

    /// The Raft term of the entry at absolute `abs_index`, or `None` when out of
    /// range (Requirement 8.2).
    pub(crate) fn term_at(&self, abs_index: u64) -> Option<u64> {
        self.get(abs_index).map(|entry| entry.term)
    }

    /// The on-disk [`FrameLocation`] of the entry at absolute `abs_index`, or
    /// `None` when out of range. The disk-backed reads (task 9) use this to map
    /// an absolute index to the frame to fetch (Requirement 13.3).
    pub(crate) fn location(&self, abs_index: u64) -> Option<FrameLocation> {
        self.get(abs_index).map(IndexEntry::location)
    }

    /// Append `entry` as the new last index. The caller assigns absolute indices
    /// via [`next_index`](LogIndex::next_index), so the dense `Vec` stays aligned
    /// with the absolute index model. Used by `append` (task 8).
    pub(crate) fn push(&mut self, entry: IndexEntry) {
        self.entries.push(entry);
    }

    /// Discard every retained entry whose absolute index is `>= abs_index`,
    /// keeping the prefix `[log_start_index, abs_index)`.
    ///
    /// `log_start_index` is unchanged. This is the overwrite/truncate primitive:
    /// `append_entries` calls it with the batch's first index to drop the
    /// conflicting suffix before writing (task 10), and `revert(i)` calls it with
    /// `i + 1` to keep `[log_start..=i]` (task 11).
    ///
    /// - `abs_index <= log_start_index` clears the index entirely (nothing in
    ///   `[log_start, abs_index)`), leaving `log_start_index` unchanged.
    /// - `abs_index > last_index` is a no-op (nothing to discard).
    pub(crate) fn truncate_from(&mut self, abs_index: u64) {
        if abs_index <= self.log_start {
            self.entries.clear();
            return;
        }
        let keep = (abs_index - self.log_start) as usize;
        if keep < self.entries.len() {
            self.entries.truncate(keep);
        }
    }

    /// Advance `log_start_index` to `new_log_start`, dropping every retained
    /// entry whose absolute index is below it (Compaction, task 13 / Requirement
    /// 13.5).
    ///
    /// - When `new_log_start <= log_start_index` this is a **no-op**, mirroring
    ///   the Compaction no-op rule (Requirement 7.4): nothing is discarded and
    ///   `log_start_index` does not regress.
    /// - Otherwise the `new_log_start - log_start_index` leading entries are
    ///   dropped and `log_start_index` becomes `new_log_start`. Callers
    ///   (`DurableWal::compaction`) bounds-check `new_log_start` against the
    ///   commit index first (Requirement 7.3), so in valid use
    ///   `new_log_start <= last_index + 1`; the count is clamped to the number of
    ///   entries defensively.
    pub(crate) fn compact_to(&mut self, new_log_start: u64) {
        if new_log_start <= self.log_start {
            return; // no-op: never regress (R7.4)
        }
        let drop = ((new_log_start - self.log_start) as usize).min(self.entries.len());
        self.entries.drain(..drop);
        self.log_start = new_log_start;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an [`IndexEntry`] with distinctive, recoverable field values so a
    /// test can assert it got back exactly the entry it stored.
    fn entry(term: u64, segment: u64, offset: u64, len: u32) -> IndexEntry {
        IndexEntry {
            term,
            segment,
            offset,
            len,
        }
    }

    /// Fill an index that starts at `log_start` with `count` entries whose term
    /// encodes their absolute index (`100 + abs`), so lookups can be checked.
    fn filled(log_start: u64, count: u64) -> LogIndex {
        let mut index = LogIndex::new(log_start);
        for n in 0..count {
            let abs = log_start + n;
            index.push(entry(100 + abs, abs, abs * 10, abs as u32));
        }
        index
    }

    // --- empty index -------------------------------------------------------

    #[test]
    fn empty_index_reports_no_entries_and_no_last_index() {
        let index = LogIndex::new(0);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.log_start_index(), 0);
        assert_eq!(index.last_index(), None);
        // The first appended entry takes index 0 on a fresh log.
        assert_eq!(index.next_index(), 0);
        // Every lookup on an empty index is out of range.
        assert_eq!(index.get(0), None);
        assert_eq!(index.term_at(0), None);
        assert_eq!(index.location(0), None);
    }

    #[test]
    fn default_is_an_empty_log_starting_at_zero() {
        let index = LogIndex::default();
        assert!(index.is_empty());
        assert_eq!(index.log_start_index(), 0);
        assert_eq!(index.next_index(), 0);
    }

    // --- mapping from absolute index to slot (R8) --------------------------

    #[test]
    fn last_index_is_log_start_plus_len_minus_one() {
        let index = filled(0, 3);
        assert_eq!(index.len(), 3);
        assert_eq!(index.last_index(), Some(2));
        assert_eq!(index.next_index(), 3);
    }

    #[test]
    fn get_maps_absolute_index_to_the_stored_entry() {
        let index = filled(0, 3);
        for abs in 0..3 {
            let got = index.get(abs).expect("in range");
            assert_eq!(got.term, 100 + abs, "term identifies absolute {abs}");
            assert_eq!(got.segment, abs);
            assert_eq!(got.offset, abs * 10);
            assert_eq!(got.len, abs as u32);
        }
    }

    #[test]
    fn term_at_and_location_agree_with_get() {
        let index = filled(0, 3);
        assert_eq!(index.term_at(1), Some(101));
        assert_eq!(
            index.location(1),
            Some(FrameLocation {
                segment: 1,
                offset: 10,
                len: 1,
            })
        );
    }

    #[test]
    fn lookup_above_last_index_is_none() {
        let index = filled(0, 3); // indices 0..=2
        assert_eq!(index.get(3), None);
        assert_eq!(index.term_at(3), None);
        assert_eq!(index.location(3), None);
        assert_eq!(index.get(u64::MAX), None);
    }

    // --- post-compaction: log_start_index > 0 (R8, R13.5) ------------------

    #[test]
    fn post_compaction_mapping_offsets_by_log_start() {
        // A log compacted so the retained range is absolute 5..=7.
        let index = filled(5, 3);
        assert_eq!(index.log_start_index(), 5);
        assert_eq!(index.len(), 3);
        assert_eq!(index.last_index(), Some(7));
        assert_eq!(index.next_index(), 8);

        // Indices below log_start were discarded: None, not an error, not slot 0.
        for below in [0u64, 1, 4] {
            assert_eq!(index.get(below), None, "{below} is below log_start");
            assert_eq!(index.term_at(below), None);
            assert_eq!(index.location(below), None);
        }

        // The retained range maps correctly (term encodes the absolute index).
        for abs in 5..=7 {
            assert_eq!(index.term_at(abs), Some(100 + abs), "retained {abs}");
        }

        // Above last_index is still out of range.
        assert_eq!(index.get(8), None);
    }

    #[test]
    fn boundary_indices_at_log_start_and_last_index_are_in_range() {
        let index = filled(5, 3); // 5..=7
        assert!(index.get(5).is_some(), "log_start is in range");
        assert!(index.get(7).is_some(), "last_index is in range");
        assert_eq!(index.get(4), None, "one below log_start is out");
        assert_eq!(index.get(8), None, "one above last_index is out");
    }

    // --- push / next_index --------------------------------------------------

    #[test]
    fn push_extends_the_last_index_by_one() {
        let mut index = LogIndex::new(0);
        index.push(entry(1, 0, 0, 4));
        assert_eq!(index.last_index(), Some(0));
        index.push(entry(1, 0, 4, 4));
        assert_eq!(index.last_index(), Some(1));
        assert_eq!(index.next_index(), 2);
    }

    // --- truncate_from ------------------------------------------------------

    #[test]
    fn truncate_from_keeps_the_prefix_below_the_index() {
        let mut index = filled(0, 5); // 0..=4
        index.truncate_from(3); // keep 0..=2
        assert_eq!(index.last_index(), Some(2));
        assert_eq!(index.get(2).map(|e| e.term), Some(102));
        assert_eq!(index.get(3), None);
    }

    #[test]
    fn truncate_from_at_or_below_log_start_clears_everything() {
        let mut index = filled(5, 3); // 5..=7
        index.truncate_from(5); // keep [5, 5) == nothing
        assert!(index.is_empty());
        // log_start is unchanged, so the next append resumes at 5.
        assert_eq!(index.log_start_index(), 5);
        assert_eq!(index.next_index(), 5);

        let mut index = filled(5, 3);
        index.truncate_from(2); // below log_start also clears
        assert!(index.is_empty());
        assert_eq!(index.log_start_index(), 5);
    }

    #[test]
    fn truncate_from_above_last_index_is_a_no_op() {
        let mut index = filled(0, 3); // 0..=2
        index.truncate_from(3); // == next_index: nothing to drop
        assert_eq!(index.last_index(), Some(2));
        index.truncate_from(100);
        assert_eq!(index.last_index(), Some(2));
    }

    // --- compact_to (R7.4 no-op, R13.5 drop prefix) ------------------------

    #[test]
    fn compact_to_drops_the_prefix_and_advances_log_start() {
        let mut index = filled(0, 5); // 0..=4
        index.compact_to(2); // retain 2..=4
        assert_eq!(index.log_start_index(), 2);
        assert_eq!(index.len(), 3);
        assert_eq!(index.last_index(), Some(4));
        // Discarded indices are gone; retained ones still map by absolute index.
        assert_eq!(index.get(1), None);
        assert_eq!(index.term_at(2), Some(102));
        assert_eq!(index.term_at(4), Some(104));
    }

    #[test]
    fn compact_to_at_or_below_log_start_is_a_no_op() {
        let mut index = filled(3, 3); // 3..=5
        index.compact_to(3); // equal: no-op
        assert_eq!(index.log_start_index(), 3);
        assert_eq!(index.len(), 3);
        index.compact_to(1); // below: never regress
        assert_eq!(index.log_start_index(), 3);
        assert_eq!(index.len(), 3);
    }

    #[test]
    fn compact_to_does_not_change_last_index_or_terms() {
        let mut index = filled(0, 5); // 0..=4
        let last_before = index.last_index();
        index.compact_to(3); // retain 3..=4
        assert_eq!(
            index.last_index(),
            last_before,
            "last_index unchanged (R7.6)"
        );
        assert_eq!(index.term_at(3), Some(103), "retained term unchanged");
        assert_eq!(index.term_at(4), Some(104));
    }

    // --- from_entries (recovery rebuild) -----------------------------------

    #[test]
    fn from_entries_rebuilds_an_index_at_a_given_log_start() {
        let entries = vec![entry(100, 0, 0, 4), entry(101, 0, 4, 4)];
        let index = LogIndex::from_entries(5, entries);
        assert_eq!(index.log_start_index(), 5);
        assert_eq!(index.last_index(), Some(6));
        assert_eq!(index.term_at(5), Some(100));
        assert_eq!(index.term_at(6), Some(101));
        assert_eq!(index.get(4), None);
        assert_eq!(index.get(7), None);
    }

    // --- IndexEntry <-> FrameLocation conversion ---------------------------

    #[test]
    fn index_entry_round_trips_through_a_frame_location() {
        let loc = FrameLocation {
            segment: 9,
            offset: 1234,
            len: 56,
        };
        let entry = IndexEntry::from_location(loc, 7);
        assert_eq!(entry.term, 7);
        assert_eq!(entry.location(), loc);
    }
}
