# Requirements Document

## Introduction

This feature adds a durable, on-disk Write-Ahead Log (WAL) implementation to the
`vela-log` crate, with support for log compaction. `vela-log` is the innermost
crate of the Vela workspace: an append-only, ordered, per-partition log that
today has only an in-memory implementation (`InMemoryLog`) behind the
`LogStorage` trait.

The goal is to provide a durable `LogStorage` implementation (the Durable_WAL)
that survives process restarts and crashes, so persistence can be added to Vela
**without modifying `vela-raft`** (consensus depends only on the `LogStorage`
trait). The Durable_WAL persists entries to segment files using checksummed
record framing, reconstructs its state on startup by replaying those files, and
reclaims space by compacting a committed prefix of the log.

The Durable_WAL MUST honor every existing `LogStorage` contract (append,
append_entries, read, entry, last_index, term_at, commit_index, commit, revert,
snapshot) so it is a drop-in replacement for `InMemoryLog`. Two behaviors extend
the existing surface: an explicitly triggered compaction operation, and an
absolute (non-renumbering) index model in which a compacted prefix becomes a gap
rather than being re-based.

Compaction requires the caller to choose a Retained_Point that is safe with
respect to replication progress. In a multi-node deployment, enabling Compaction
depends on a follower catch-up mechanism that is **out of scope for `vela-log`**
and MUST exist before Compaction is enabled: the Durable_WAL provides only the
mechanism to discard a committed prefix; it does not choose the Retained_Point
and does not know follower progress. Consequently, after Compaction `snapshot()`
returns only the retained committed prefix, so any consumer using it to catch up
a follower must account for entries below the Log_Start_Index being gone.

Only the `Always` Sync_Policy provides the persist-before-acknowledge guarantee
that consensus requires; `Periodic` and `Never` are permitted only for logs that
do not back consensus.

The Durable_WAL keeps an **in-memory index but not payload bytes**: for each
retained Log_Entry it holds the entry's term and the location and length of its
Record_Frame, while payload bytes live only in the Segment files and are read
from disk on demand. Memory therefore scales with the number of retained entries,
not with payload volume. Queries answerable from the index (`last_index`,
`term_at`, `commit_index`, and the existence and bounds of entries) are served
from memory and cannot fail; the payload-bearing reads (`entry`, `read`,
`snapshot`) fetch from disk and can encounter I/O errors. A later evolution toward
a sparse, memory-mapped index backed by the operating system page cache is
anticipated but out of scope here; these requirements do not mandate a specific
index density.

Adding `flush` to `LogStorage` grows the shared trait, but **additively and with a
default no-op implementation**, so existing `vela-raft` source compiles unchanged
and the "consensus depends only on the trait" property is preserved.

The Durable_WAL targets the **single-writer-per-partition** model that consensus
uses: write operations take `&mut self` and reads take `&self`, so the type system
already serializes writes against reads. It is not required to support concurrent
readers while a write is in progress, and the design need not build for that.

Design and implementation details (exact frame byte layout, segment naming, the
checksum algorithm, and buffering strategy) are deferred to the design phase. The
persistence **location** of the Commit_Index (Requirement 9) and the
Log_Start_Index (Requirement 7) — a manifest file versus in-log records — and the
Frame_Metadata mechanism (Requirement 6) are likewise deferred to design, as they
carry crash-consistency implications. These requirements define observable
behavior and guarantees only.

## Glossary

- **Vela**: The distributed event-streaming platform that hosts this feature.
- **vela-log**: The innermost crate providing the append-only, per-partition log
  behind the `LogStorage` trait. It depends on no other Vela crate.
- **LogStorage**: The existing trait that defines the log storage seam
  (append, append_entries, read, entry, last_index, term_at, commit_index,
  commit, revert, snapshot, flush). Consensus depends on this trait, not on a
  concrete implementation.
- **Flush**: An operation on the `LogStorage` trait that forces all buffered
  Record_Frames to stable storage. It is a successful no-op for non-durable
  implementations (such as `InMemoryLog`) and forces buffered writes to disk for
  the Durable_WAL. The seam exists so a future group-commit-before-acknowledge
  path can be added without changing the trait again; wiring `vela-raft` to call
  it is out of scope for this feature.
- **Retained_Point**: The Index supplied to Compaction at and above which
  Log_Entries are kept; all Indices below it are discarded. The Durable_WAL
  treats the Retained_Point as authoritative and does not consult replication
  progress — choosing a replication-safe Retained_Point is the caller's
  responsibility.
- **Frame_Metadata**: Durable per-Segment metadata (a segment footer or a
  manifest) recording how many Record_Frames, and/or up to which Index, were
  acknowledged as durable. WAL_Recovery uses it to distinguish a torn tail from
  interior corruption. The exact mechanism (footer vs. manifest) is deferred to
  the design phase.
- **Durable_WAL**: The on-disk implementation of `LogStorage` introduced by this
  feature. It persists log state to the Data_Directory and reconstructs that
  state after a restart or crash.
- **InMemoryLog**: The existing non-durable implementation of `LogStorage`,
  retained unchanged.
- **Log_Entry**: A single log element carrying a 0-based `index`, a Raft `term`,
  and an opaque `EntryPayload` (`PayloadKind` tag plus bytes).
- **Index**: The 0-based absolute position of a Log_Entry within a partition log.
  Indices are assigned once at append time and never change.
- **Log_Start_Index**: The lowest Index still retained by the Durable_WAL. It is
  0 for a log that has never been compacted, and increases as a prefix is
  discarded by Compaction. Indices below the Log_Start_Index are no longer
  stored.
- **Last_Index**: The highest stored Index, or absent when the log holds no
  retained entries.
- **Commit_Index**: The commit position of the log, of type `Option<u64>`;
  `None` means nothing has been committed (the state preceding Index 0).
- **Data_Directory**: The filesystem directory, supplied via Configuration, in
  which the Durable_WAL stores its Segment files for one partition log.
- **Segment**: A bounded on-disk file holding a contiguous range of
  Record_Frames. The log is stored as an ordered sequence of Segments; new
  Segments are created as the active Segment reaches the configured Segment_Size.
- **Segment_Size**: The configured maximum size, in bytes, of a single Segment
  before the Durable_WAL begins a new active Segment.
- **Record_Frame**: The on-disk encoding of one Log_Entry, consisting of a length
  field, the encoded entry fields (index, term, payload kind, payload bytes), and
  a Checksum over the framed content.
- **Checksum**: A CRC value stored in each Record_Frame and used to detect
  corruption or partial writes of that frame.
- **Sync_Policy**: The configured durability policy controlling when the
  Durable_WAL forces buffered writes to stable storage. One of: `Always`
  (force before each mutating operation returns), `Periodic` (force at a
  configured interval), or `Never` (rely on the operating system to flush).
- **WAL_Recovery**: The startup process that reads the Segments in the
  Data_Directory, validates Record_Frames, and reconstructs the in-memory index
  and Commit_Index of the Durable_WAL.
- **Torn_Write**: A Record_Frame that is incomplete or fails its Checksum because
  a crash interrupted the write before it was made durable. A Torn_Write is
  recoverable (discarded) only when it forms the tail run of the log with no valid
  Record_Frame following it; the same condition occurring before a valid frame is
  interior corruption.
- **Compaction**: The operation that discards a committed prefix of the log to
  reclaim space, advancing the Log_Start_Index while retaining all entries at and
  above a retained point.
- **LogError**: The typed error enum returned by `LogStorage` operations. This
  feature adds variants for I/O failures, corruption, out-of-bounds compaction,
  and invalid configuration (`Io`, `Corruption`, `CompactionOutOfBounds`,
  `Config`), while retaining the existing variants
  (`CommitOutOfBounds`, `RevertBelowCommit`, `NonContiguousEntries`).

## Requirements

### Requirement 1: Durable Drop-In LogStorage Implementation

**User Story:** As a Vela developer, I want a durable on-disk log that implements the existing `LogStorage` trait, so that consensus gains persistence without any change to `vela-raft`.

#### Acceptance Criteria

1. THE Durable_WAL SHALL implement the `LogStorage` trait, exposing the append, append_entries, read, entry, last_index, term_at, commit_index, commit, revert, snapshot, and flush operations.
2. WHEN a sequence of `LogStorage` operations is applied to the Durable_WAL and the identical sequence is applied to an `InMemoryLog`, and no Compaction is performed, THE Durable_WAL SHALL return, for every operation in the sequence, a result equal to the result returned by the `InMemoryLog`, where equality covers both the operation's direct return value (the assigned Index, the `Ok`/`LogError` outcome, or the returned read, entry, or snapshot value) and the observable state subsequently reported by last_index, commit_index, term_at, entry, read, and snapshot.
3. WHEN `append` is invoked on the Durable_WAL, THE Durable_WAL SHALL assign the new entry the Index `Last_Index + 1`, or Index 0 when the log holds no retained entries and has never been compacted, and SHALL return the assigned Index.
4. WHEN `append_entries` receives a non-empty batch that is non-contiguous, out of order, begins beyond `Last_Index + 1`, or overwrites an entry at or below the Commit_Index, THE Durable_WAL SHALL reject the batch with `LogError::NonContiguousEntries` and SHALL leave the persisted log unchanged. (Note: this variant doubles as the commit-conflict signal for the "overwrites an entry at or below the Commit_Index" case, preserving drop-in fidelity with `InMemoryLog` rather than indicating a contiguity error per se.)
5. WHEN `append_entries` receives a non-empty batch whose entries are contiguous and ascending by Index, whose first Index is no greater than `Last_Index + 1`, and whose first Index is above the Commit_Index, THE Durable_WAL SHALL discard any retained entries at or above the batch's first Index, persist the batch in ascending Index order, and return success.
6. WHEN `append_entries` receives an empty batch, THE Durable_WAL SHALL leave the persisted log unchanged and return success.
7. WHEN `commit` is invoked with an Index outside the range `current Commit_Index ..= Last_Index`, THE Durable_WAL SHALL reject the request with `LogError::CommitOutOfBounds` and SHALL leave the Commit_Index unchanged.
8. WHEN `revert` is invoked with an Index below the current Commit_Index, THE Durable_WAL SHALL reject the request with `LogError::RevertBelowCommit` and SHALL leave the persisted log unchanged.

### Requirement 2: On-Disk Record Framing and Checksums

**User Story:** As a platform operator, I want each persisted entry framed with a
checksum, so that corruption and partial writes can be detected on read and
recovery.

#### Acceptance Criteria

1. WHEN the Durable_WAL persists a Log_Entry, THE Durable_WAL SHALL write a Record_Frame containing the entry's Index, term, payload kind, payload bytes, and a Checksum computed over the framed content, where the framed content is the length field plus the encoded entry fields (Index, term, payload kind, payload bytes) and excludes the Checksum itself.
2. WHEN the Durable_WAL reads a complete Record_Frame, THE Durable_WAL SHALL recompute the Checksum over that Record_Frame's framed content and compare the recomputed Checksum for equality against the Checksum stored in that Record_Frame.
3. IF a Record_Frame's recomputed Checksum does not equal its stored Checksum, THEN THE Durable_WAL SHALL classify that Record_Frame as corrupt.
4. IF a Record_Frame is incomplete, holding fewer bytes than its length field declares or lacking a complete Checksum, THEN THE Durable_WAL SHALL classify that Record_Frame as corrupt.
5. FOR ALL Log_Entries persisted as Record_Frames and decoded back from those Record_Frames without corruption, including Log_Entries whose payload bytes are empty, THE Durable_WAL SHALL produce entries whose Index, term, payload kind, and payload bytes equal those of the original entries (round-trip property).
6. THE Record_Frame length field SHALL be wide enough to express the size of the largest permitted `EntryPayload`, so that no valid Log_Entry is unrepresentable on disk.

### Requirement 3: Segment-Based File Layout

**User Story:** As a platform operator, I want the log stored as bounded segment
files, so that the log can grow without unbounded single files and so that
compaction can reclaim whole segments.

#### Acceptance Criteria

1. THE Durable_WAL SHALL store its Record_Frames in the Data_Directory as an ordered sequence of one or more Segments, and SHALL store each Record_Frame entirely within a single Segment without splitting any Record_Frame across Segments.
2. WHEN no active Segment exists and the Durable_WAL persists a Log_Entry, THE Durable_WAL SHALL create the first active Segment for that entry.
3. WHEN the Durable_WAL persists a Log_Entry AND the active Segment is non-empty AND writing the entry's Record_Frame would cause the active Segment's on-disk size to exceed Segment_Size, THE Durable_WAL SHALL start a new active Segment for that entry.
4. WHEN a Log_Entry's Record_Frame is larger than Segment_Size, THE Durable_WAL SHALL store that Record_Frame as the sole Record_Frame of its own Segment.
5. THE Durable_WAL SHALL record, for each Segment, the range of Indices the Segment contains, such that WAL_Recovery can order the Segments by ascending Index.
6. WHEN the Durable_WAL appends a Log_Entry, THE Durable_WAL SHALL preserve ascending Index order across Segments so that reading Segments in order yields entries in ascending Index order.

### Requirement 4: Append Durability and Sync Policy

**User Story:** As a platform operator, I want control over how aggressively
writes are flushed to disk, so that I can trade durability against throughput.

#### Acceptance Criteria

1. WHERE the Sync_Policy is `Always`, WHEN `append` or `append_entries` returns successfully, THE Durable_WAL SHALL have forced the corresponding Record_Frames to stable storage before returning, and WHEN that operation created a new Segment file, THE Durable_WAL SHALL additionally have fsynced the parent Data_Directory before returning so that a crash cannot lose a newly created Segment holding an acknowledged entry.
2. WHERE the Sync_Policy is `Periodic` with a configured interval expressed in milliseconds, WHILE the Durable_WAL is actively servicing `append`, `append_entries`, or `commit` invocations, THE Durable_WAL SHALL force buffered Record_Frames to stable storage such that the elapsed wall-clock time between the completion of consecutive forces does not exceed the configured interval; WHILE the log is idle with no such invocations occurring, the interval MAY be exceeded until the next invocation. (The `Periodic` policy therefore does not require a background flush thread; this is acceptable because `Periodic` is permitted only for logs that do not back consensus.)
3. WHERE the Sync_Policy is `Never`, WHEN `append` or `append_entries` returns successfully, THE Durable_WAL SHALL have written the corresponding Record_Frames to the operating system without itself forcing them to stable storage.
4. WHEN `commit` advances the Commit_Index and the Sync_Policy is `Always`, THE Durable_WAL SHALL force the updated Commit_Index to stable storage before returning.
5. IF forcing buffered Record_Frames to stable storage fails during an `append`, `append_entries`, or `commit` invocation operating under a Sync_Policy that requires that force, THEN THE Durable_WAL SHALL return `LogError::Io` from that operation and SHALL leave its reported retained entries, Last_Index, and Commit_Index equal to their values immediately before the operation.
6. IF a `Periodic` force of buffered Record_Frames to stable storage fails, THEN THE Durable_WAL SHALL return `LogError::Io` from the next `append`, `append_entries`, or `commit` invocation and SHALL NOT report the affected Record_Frames as forced to stable storage.
7. THE Durable_WAL SHALL provide that only the `Always` Sync_Policy delivers the persist-before-acknowledge guarantee required by consensus; the `Periodic` and `Never` Sync_Policies SHALL be used only for logs that do not back consensus.
8. THE `LogStorage` trait SHALL provide a `flush` operation that forces all buffered Record_Frames to stable storage and returns `LogError::Io` if the force fails.
9. WHERE an implementation does not buffer writes or is non-durable, such as `InMemoryLog`, THE `flush` operation SHALL be a successful no-op provided by a default trait implementation.
10. WHEN `flush` is invoked on the Durable_WAL, THE Durable_WAL SHALL force all buffered Record_Frames to stable storage before returning, and IF the force fails THEN THE Durable_WAL SHALL return `LogError::Io` and SHALL leave its reported retained entries, Last_Index, and Commit_Index unchanged.
11. WHERE the Sync_Policy is `Always`, WHEN `append` or `append_entries` returns successfully, THE Durable_WAL SHALL have forced the Frame_Metadata that records the corresponding Record_Frames as acknowledged to stable storage before returning, so that no acknowledged Record_Frame is ever left uncovered by durable Frame_Metadata (this is what makes Requirement 6, criteria 1 and 6, hold). (Design-phase note: forcing both the Record_Frame and its covering Frame_Metadata can imply a second fsync per append; the design SHALL state whether Frame_Metadata is sealed into the frame stream itself or carried as a separately-fsynced durable tail pointer, since a per-append double fsync compounds the throughput concern.)

### Requirement 5: Crash Recovery and Replay

**User Story:** As a platform operator, I want the log to rebuild its exact state
after a restart, so that committed records survive process and node failures.

#### Acceptance Criteria

1. WHEN the Durable_WAL is opened on a Data_Directory that contains previously written Segments, THE WAL_Recovery SHALL read the Segments in ascending Index order and reconstruct the retained entries, the Last_Index, the Log_Start_Index, and the Commit_Index.
2. WHEN the Durable_WAL is opened on an empty or absent Data_Directory, THE Durable_WAL SHALL initialize an empty log with no retained entries, an absent Last_Index, a Log_Start_Index of 0, and a Commit_Index of `None`.
3. FOR ALL sequences of append, append_entries, commit, and revert operations applied to a Durable_WAL whose buffered writes are forced to stable storage, WHEN a new Durable_WAL is opened on the same Data_Directory, THE reopened Durable_WAL SHALL report the same retained entries, Last_Index, Log_Start_Index, and Commit_Index as the original Durable_WAL held after those operations (recovery round-trip property).
4. WHEN WAL_Recovery completes, THE Durable_WAL SHALL report a Commit_Index that is either `None` or a value no greater than its Last_Index, and in either case no less than the Commit_Index that was last forced to stable storage before the restart.
5. IF a filesystem operation fails while opening the Data_Directory or reading its Segments during WAL_Recovery, THEN THE Durable_WAL SHALL return `LogError::Io` describing the failed operation and SHALL NOT create a partially initialized log.

### Requirement 6: Torn-Tail and Interior Corruption Handling

**User Story:** As a platform operator, I want an interrupted write at the end of
the log to be recoverable while genuine mid-log corruption is reported, so that a
crash does not lose the whole log but silent data loss is never hidden.

#### Acceptance Criteria

1. IF WAL_Recovery encounters a contiguous tail run of one or more Torn_Writes, including any trailing bytes too short to form a complete Record_Frame, that is not yet covered by the durable Frame_Metadata and is discriminated by the absence of any valid Record_Frame following the run, THEN THE WAL_Recovery SHALL discard that tail run, treat the last preceding valid Record_Frame as the log tail, and complete recovery successfully.
2. WHEN WAL_Recovery discards a tail run of Torn_Writes, THE Durable_WAL SHALL set Last_Index to the Index of the last valid retained Record_Frame, or report no retained entries when no valid Record_Frame remains, and SHALL NOT restore the discarded Torn_Writes on any subsequent WAL_Recovery.
3. IF WAL_Recovery encounters a corrupt Record_Frame, whether checksum-corrupt or torn, that is followed by one or more valid Record_Frames, THEN THE WAL_Recovery SHALL fail with `LogError::Corruption` identifying the Index expected at that position rather than the corrupt frame's own Index field, and SHALL leave the Segments unchanged.
4. WHERE the Sync_Policy is `Always`, IF WAL_Recovery cannot account for as many valid Record_Frames as the durable Frame_Metadata records as acknowledged, THEN THE WAL_Recovery SHALL fail with `LogError::Corruption` and SHALL NOT silently truncate the log below the durably acknowledged extent.
5. WHERE the Sync_Policy is `Periodic` or `Never`, IF WAL_Recovery finds fewer valid Record_Frames at the tail than the durable Frame_Metadata records, THEN THE WAL_Recovery SHALL treat the shortfall as a torn tail, discard it down to the last valid Record_Frame, and complete recovery successfully, because these policies do not promise persist-before-acknowledge and such loss is expected; an interior corruption (a corrupt frame followed by a valid frame, per criterion 3) SHALL still fail with `LogError::Corruption` under every Sync_Policy.
6. WHEN WAL_Recovery finds no interior corruption and accounts for every Record_Frame the durable Frame_Metadata requires for the active Sync_Policy, with at most an optional tail run of Torn_Writes beyond that extent, THE WAL_Recovery SHALL complete successfully.
7. WHILE the Sync_Policy is `Always`, THE Durable_WAL SHALL ensure that any Record_Frame for which `append` or `append_entries` returned successfully is not discarded as a Torn_Write during a subsequent WAL_Recovery.
8. THE Durable_WAL SHALL update the durable Frame_Metadata crash-atomically and SHALL protect it with its own integrity check, such that a crash mid-update leaves WAL_Recovery able to fall back to the last intact Frame_Metadata; IF the Frame_Metadata is itself torn or fails its integrity check, THEN WAL_Recovery SHALL recover the acknowledged extent from the last intact Frame_Metadata rather than trusting the torn update.

### Requirement 7: Log Compaction

**User Story:** As a platform operator, I want to discard a committed prefix of
the log, so that disk usage stays bounded as the committed log grows.

#### Acceptance Criteria

1. THE Durable_WAL SHALL provide a Compaction operation that accepts a Retained_Point expressed as an Index and discards all Log_Entries with an Index below that Retained_Point while retaining all Log_Entries with an Index at or above it. THE Durable_WAL SHALL treat the supplied Retained_Point as authoritative and SHALL NOT consult replication or follower progress; choosing a replication-safe Retained_Point is the caller's responsibility.
2. WHEN Compaction is requested with a Retained_Point such that all discarded Indices are at or below the Commit_Index and the entry at the Commit_Index is retained, THE Durable_WAL SHALL discard the Record_Frames below the Retained_Point and SHALL set the Log_Start_Index to the Retained_Point.
3. IF Compaction is requested with a Retained_Point above the current Log_Start_Index that would discard an uncommitted entry or the entry at the Commit_Index, or with such a Retained_Point when the Commit_Index is `None`, THEN THE Durable_WAL SHALL reject the request with `LogError::CompactionOutOfBounds` and SHALL leave the persisted log unchanged.
4. WHEN Compaction is requested with a Retained_Point at or below the current Log_Start_Index, THE Durable_WAL SHALL complete successfully as a no-op and SHALL leave the Log_Start_Index and all persisted Record_Frames unchanged, regardless of the Commit_Index; this no-op case takes precedence over criterion 3, because it discards nothing.
5. WHEN Compaction discards all Record_Frames contained in a Segment, THE Durable_WAL SHALL remove that Segment from the Data_Directory. Compaction reclaims disk at Segment granularity: Record_Frames below the Log_Start_Index that fall within a Segment still holding retained entries remain physically on disk but are logically discarded, and SHALL be skipped on read and on WAL_Recovery.
6. WHEN Compaction completes, THE Durable_WAL SHALL retain every Log_Entry with an Index at or above the Log_Start_Index without altering that entry's Index, term, or payload, and SHALL leave the Last_Index and Commit_Index unchanged.
7. WHEN a Durable_WAL that has performed Compaction is reopened on the same Data_Directory, THE reopened Durable_WAL SHALL report the same Log_Start_Index as the original Durable_WAL held after Compaction and SHALL retain every Log_Entry held after Compaction with an identical Index, term, and payload.
8. WHEN Compaction discards a prefix, THE Durable_WAL SHALL persist the advanced Log_Start_Index to stable storage before removing any Segment, such that a crash during Compaction leaves the log recoverable with a Log_Start_Index no less than its pre-compaction value and no retained Log_Entry lost; any below-the-line Segments left orphaned by a crash mid-removal SHALL be ignored by WAL_Recovery.

### Requirement 8: Absolute Index Model After Compaction

**User Story:** As a Vela developer, I want indices to stay absolute after
compaction, so that consensus continues to address entries by their original
index and reads below the retained prefix are unambiguous.

#### Acceptance Criteria

1. WHEN `entry` is invoked with an Index below the Log_Start_Index or above the Last_Index, THE Durable_WAL SHALL return `None` without returning an error.
2. WHEN `term_at` is invoked with an Index below the Log_Start_Index or above the Last_Index, THE Durable_WAL SHALL return `None` without returning an error.
3. WHEN `read` is invoked with a range, THE Durable_WAL SHALL return only the retained entries whose Index is at or above the Log_Start_Index and within the requested range, in ascending Index order.
4. IF a `read` range falls entirely below the Log_Start_Index or contains no retained Index, THEN THE Durable_WAL SHALL return an empty collection without returning an error.
5. WHEN `append` assigns an Index after Compaction, THE Durable_WAL SHALL assign `Last_Index + 1` and SHALL NOT reuse any Index below the Log_Start_Index.
6. WHEN `snapshot` is invoked and the Commit_Index is not `None`, THE Durable_WAL SHALL return a `Snapshot` whose `commit_index` equals the current Commit_Index and whose entries are the retained entries with an Index in `Log_Start_Index..=Commit_Index`, in ascending Index order.
7. IF `snapshot` is invoked and the Commit_Index is `None`, THEN THE Durable_WAL SHALL return a `Snapshot` whose `commit_index` is `None` and whose entries are empty.

> **Implementer note:** After Compaction, an entry's Index no longer equals its
> position in any dense storage array; every lookup, range read, and snapshot
> bound must offset by the Log_Start_Index. The obvious "index == array position"
> implementation is incorrect once a prefix has been discarded.
>
> **Cross-reference:** Because `snapshot` (criterion 6) returns only the retained
> committed prefix, entries below the Log_Start_Index are gone. A consumer using
> `snapshot` to catch up a follower must account for this partiality — see the
> caller-responsibility note in the Introduction and Requirement 7, criterion 1.

### Requirement 9: Commit and Revert Durability

**User Story:** As a platform operator, I want commit and revert outcomes to
persist across restarts, so that the recovered log reflects the last durable
consensus decisions.

#### Acceptance Criteria

1. WHEN `commit` advances the Commit_Index and that write is forced to stable storage, WHEN a Durable_WAL is subsequently reopened on the same Data_Directory, THE reopened Durable_WAL SHALL report a Commit_Index equal to the advanced value.
2. WHEN `revert` removes entries with an Index strictly above the given retained Index and that removal is forced to stable storage, WHEN a Durable_WAL is subsequently reopened on the same Data_Directory, THE reopened Durable_WAL SHALL NOT restore the removed entries.
3. WHEN `revert` removes entries above a given Index, that removal is forced to stable storage, and no `append` or `append_entries` occurred after the revert and before reopen, WHEN a Durable_WAL is reopened on the same Data_Directory, THE reopened Durable_WAL SHALL report a Last_Index equal to that given Index.
4. IF `revert` is requested with an Index below the Log_Start_Index, THEN THE Durable_WAL SHALL reject the request with `LogError::RevertBelowCommit`, because every retained Index below the Commit_Index is committed, and SHALL leave its persisted entries, Last_Index, and Commit_Index unchanged.
5. WHERE the Sync_Policy is `Always`, WHEN `revert` returns successfully, THE Durable_WAL SHALL have forced the persisted removal to stable storage before returning.
6. WHEN `revert` removes a tail of entries, THE Durable_WAL SHALL reduce the durably acknowledged extent recorded in the Frame_Metadata to the retained Index and SHALL truncate or remove the now-unreferenced tail Record_Frames and Segments consistently with that extent, so that a subsequent WAL_Recovery neither restores the removed entries nor reports a frame/metadata mismatch (Requirement 6, criterion 4) for them.

### Requirement 10: I/O and Corruption Error Reporting

**User Story:** As a Vela developer, I want durability failures surfaced as typed
errors on the existing trait, so that consensus can react without depending on
implementation details.

#### Acceptance Criteria

1. THE Durable_WAL SHALL report I/O failures, corruption, out-of-bounds compaction, and invalid configuration through the `LogError` enum, adding the `Io`, `Corruption`, `CompactionOutOfBounds`, and `Config` variants while retaining the existing `CommitOutOfBounds`, `RevertBelowCommit`, and `NonContiguousEntries` variants.
2. IF a filesystem operation fails during `append`, `append_entries`, `commit`, `revert`, or Compaction, THEN THE Durable_WAL SHALL return `LogError::Io` that identifies which specific operation (append, append_entries, commit, revert, or Compaction) was in progress.
3. IF a mutating operation fails with `LogError::Io`, THEN THE Durable_WAL SHALL leave its reported retained entries, Last_Index, Log_Start_Index, and Commit_Index equal to their values immediately before the failed operation.
4. The read-path operations `entry`, `read`, and `snapshot` fetch payload bytes from disk and can therefore encounter filesystem errors, whereas `term_at`, `last_index`, and `commit_index` are served from the in-memory index and cannot. IF a filesystem operation fails while serving `entry`, `read`, or `snapshot`, THEN THE Durable_WAL SHALL treat it as an unrecoverable fault, SHALL surface the failure out-of-band through diagnostics, and SHALL NOT represent the I/O failure as the absence of data — it SHALL NOT return `None`, an empty range, or a short `Snapshot` that a caller could not distinguish from genuine absence. (Design-phase note: because the existing `LogStorage` read signatures are infallible, the exact fail-stop mechanism — partition shutdown or panic versus introducing a fallible read seam in a future trait revision — is selected during design; the binding requirement here is that an I/O error is never silently disguised as data absence.)
5. THE Durable_WAL SHALL define every `LogError` variant using `thiserror`, and each variant's `Display` message SHALL be non-empty and distinct from every other variant's message.

### Requirement 11: Configuration

**User Story:** As a platform operator, I want to configure where and how the log
persists data, so that I can place data appropriately and tune durability.

#### Acceptance Criteria

1. WHEN the Durable_WAL is opened, THE Durable_WAL SHALL accept a Configuration that specifies the Data_Directory and that optionally specifies the Segment_Size and the Sync_Policy.
2. WHERE the Configuration omits the Segment_Size, THE Durable_WAL SHALL apply a default Segment_Size of 67108864 bytes (64 mebibytes).
3. WHERE the Configuration omits the Sync_Policy, THE Durable_WAL SHALL apply the `Always` Sync_Policy.
4. IF the Configuration specifies a Data_Directory that cannot be created or opened for writing, THEN THE Durable_WAL SHALL return `LogError::Io` and SHALL NOT create a partially initialized log.
5. IF the Configuration specifies a Segment_Size of zero bytes, THEN THE Durable_WAL SHALL reject the Configuration with `LogError::Config` describing the invalid Segment_Size and SHALL NOT create a partially initialized log.
6. WHERE the Configuration selects the `Periodic` Sync_Policy and omits its interval, THE Durable_WAL SHALL apply a default interval of 1000 milliseconds.
7. IF the Configuration selects the `Periodic` Sync_Policy with an interval of zero milliseconds, THEN THE Durable_WAL SHALL reject the Configuration with `LogError::Config` describing the invalid interval and SHALL NOT create a partially initialized log.
8. WHEN the Durable_WAL is opened, THE Durable_WAL SHALL acquire exclusive access to the Data_Directory, and IF the Data_Directory is already held by another live Durable_WAL instance or process, THEN THE Durable_WAL SHALL fail to open with `LogError::Io` and SHALL NOT modify the Data_Directory.

### Requirement 12: Crate Boundaries and Testing Constraints

**User Story:** As a Vela developer, I want the durable log to respect the
project's crate boundaries and testing conventions, so that `vela-log` stays an
innermost, independently testable crate.

#### Acceptance Criteria

1. THE Durable_WAL SHALL reside in the `vela-log` crate and SHALL NOT declare a build, runtime, or test dependency on any crate whose name begins with `vela-` (any other Vela workspace crate).
2. THE Durable_WAL SHALL define its library error type (`LogError`) using `thiserror` and SHALL NOT declare a build, runtime, or test dependency on `anyhow`.
3. THE vela-log crate SHALL provide a `proptest`-based test that generates randomized sequences of `LogStorage` operations across at least 256 generated cases and SHALL report a test failure if the recovery round-trip property of Requirement 5.3 does not hold for any generated case.
4. THE vela-log crate SHALL provide a `proptest`-based test that generates randomized Log_Entry values across at least 256 generated cases and SHALL report a test failure if the framing round-trip property of Requirement 2.4 does not hold for any generated case.
5. WHEN the vela-log crate is built and tested at the workspace root, THE Durable_WAL SHALL compile and its tests SHALL complete with zero failures alongside the retained `InMemoryLog` and its existing tests.
6. THE `InMemoryLog` SHALL satisfy the `flush` operation as a successful no-op via the default trait implementation, and THE Durable_WAL SHALL override `flush` to force buffered Record_Frames to stable storage, such that the drop-in equivalence of Requirement 1.2 holds with `flush` treated as an always-successful no-op on `InMemoryLog`.

### Requirement 13: In-Memory Index and On-Disk Payloads

**User Story:** As a platform operator, I want the durable log's memory use to
scale with entry count rather than data volume, so that large topics stay
affordable to host.

#### Acceptance Criteria

1. THE Durable_WAL SHALL maintain an in-memory index that, for each retained Log_Entry, records the entry's term and the location and length of its Record_Frame within a Segment, and SHALL NOT own or retain that entry's payload bytes in the in-memory index after an `append` or read operation returns.
2. WHEN `term_at`, `last_index`, or `commit_index` is invoked, THE Durable_WAL SHALL answer from the in-memory index without reading a Segment file.
3. WHEN `entry`, `read`, or `snapshot` is invoked, THE Durable_WAL SHALL obtain the payload bytes from the Segment files rather than from a pinned in-heap payload copy, relying on the operating system page cache to keep hot Segments resident.
4. As a consequence of criteria 1 and 3, THE resident memory the Durable_WAL attributes to its in-memory index SHALL grow with the number of retained Log_Entries and SHALL NOT grow with the size of payload bytes.
5. WHEN Compaction advances the Log_Start_Index, THE Durable_WAL SHALL drop the in-memory index entries for the discarded Indices, so that index memory tracks only retained entries.
