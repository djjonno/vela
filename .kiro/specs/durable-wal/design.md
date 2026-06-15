# Design Document

## Overview

This design adds a durable, on-disk `LogStorage` implementation — `DurableWal` —
to the `vela-log` crate, plus an explicitly-triggered log compaction operation.
It realizes the 13 approved requirements while keeping `vela-raft` source
unchanged: consensus continues to depend only on the `LogStorage` trait, which
grows by exactly one additive, default-implemented method (`flush`).

The implementation follows the Kafka-style storage model fixed in the
requirements: an **in-memory index** holding per-entry metadata (term + on-disk
location), with **payload bytes living only in segment files** and read back on
demand. Memory therefore scales with entry count, not data volume. Durability is
provided by checksummed record framing, segment files, a crash-atomic
double-buffered manifest recording the durably-acknowledged extent, and a startup
recovery/replay pass that distinguishes a recoverable torn tail from
non-recoverable interior corruption.

### Goals

- A drop-in `LogStorage` whose observable behavior equals `InMemoryLog` for any
  operation sequence that performs no compaction (R1.2).
- Survive process restart and crash: committed, acknowledged entries are never
  lost under the `Always` sync policy (R5, R6, R9).
- Bounded disk via segment-granularity compaction with an absolute index model
  (R7, R8).
- Bounded memory via an index-only-in-RAM design (R13).
- Typed errors via `thiserror`; no `anyhow`; no dependency on any `vela-*` crate
  (R10, R12).

### Non-goals (explicitly out of scope)

- Choosing a replication-safe `Retained_Point` — the caller's responsibility
  (R7.1); the WAL never consults follower progress.
- A follower catch-up / InstallSnapshot mechanism (lives above `vela-log`).
- Wiring `vela-raft` to use `flush` for group commit (the seam is provided; the
  call site is future work).
- A sparse / memory-mapped index over the page cache (anticipated evolution; this
  design uses a dense in-memory index).
- Concurrent readers during a write (single-writer-per-partition; `&mut self`
  writes and `&self` reads are serialized by the type system).

## Architecture

`vela-log` keeps its existing public surface (`LogStorage`, `InMemoryLog`, the
domain types) and adds a `wal` module tree. Nothing in `wal` depends on another
Vela crate.

```
crates/vela-log/src/
├── lib.rs            # existing types + LogStorage trait (gains `flush` default);
│                     #   re-exports wal::{DurableWal, WalConfig, SyncPolicy}
└── wal/
    ├── mod.rs        # DurableWal: the LogStorage impl, owns index + segments + manifest
    ├── config.rs     # WalConfig, SyncPolicy, validation (R11)
    ├── frame.rs      # Record_Frame encode/decode + CRC32C (R2)
    ├── segment.rs    # Segment file: create/append/read/seal, naming, rollover (R3)
    ├── index.rs      # in-memory index (term + location), absolute-index mapping (R8, R13)
    ├── manifest.rs   # double-buffered, CRC'd durable state: acknowledged extent,
    │                 #   commit_index, log_start_index (R6.8, R7.8, R9)
    ├── recovery.rs   # open-time replay: scan segments, classify tail vs interior (R5, R6)
    ├── fs.rs         # thin filesystem seam (open/read/write/fsync/rename/lock) for
    │                 #   real I/O + deterministic fault injection in tests
    └── error.rs      # LogError additions (Io, Corruption, CompactionOutOfBounds, Config)
```

Dependency direction stays inward; `wal` is internal to `vela-log`.

## Components and Interfaces

### Component responsibilities

- **DurableWal** (`mod.rs`) — implements `LogStorage`; coordinates the index,
  the active segment, the manifest, and the sync policy. Holds the single-writer
  state and a `poisoned` flag for read fail-stop.
- **frame** — the on-disk encoding of one `LogEntry` and CRC computation.
- **segment** — one append-only file holding a contiguous index range; rollover
  and oversized-frame rules; sequential validated scan for recovery/reads.
- **index** — dense `Vec<IndexEntry>` offset by `log_start_index`; O(1) lookup,
  no payload bytes.
- **manifest** — the `Frame_Metadata` of the requirements: the durable
  acknowledged extent (last durable index + its segment/offset), `commit_index`,
  and `log_start_index`, written crash-atomically.
- **recovery** — rebuilds all in-memory state from disk at open.
- **fs** — wraps `std::fs` behind a trait so crash/torn-write/fsync-failure
  scenarios are deterministically testable.

## Data Models

### On-disk format

### Record_Frame (R2)

Each entry is encoded as a self-describing, CRC-protected frame:

```
offset  size    field
0       4       len      u32 LE   = byte length of BODY (= 17 + payload_len)
4       len     body              = index(8 LE) ++ term(8 LE) ++ kind(1) ++ payload_bytes
4+len   4       crc      u32 LE   = CRC32C over bytes [0 .. 4+len)  (len field + body)
```

- `kind`: `0=Record, 1=Cluster, 2=Noop` (mirrors `PayloadKind`).
- The CRC covers the **length field and the body**, excluding only the CRC itself
  (R2.1), so a corrupted length is itself detectable (it changes the bytes the CRC
  covers and shifts the trailing CRC position, both of which fail validation).
- **Incomplete frame** (R2.4): during a scan, if fewer than 4 bytes remain for
  `len`, or fewer than `len + 4` bytes remain for `body + crc`, the frame is
  classified corrupt (torn).
- **Max size** (R2.6): `len` is `u32`, so a body up to ~4 GiB − 1 is
  representable — far above any `EntryPayload` and above the default 64 MiB
  segment, satisfying the oversized-frame rule (R3.4).
- **CRC32C**: implemented in-house from a generated Castagnoli table (≈40 lines,
  no new dependency — matching the project's in-house `SplitMix64` precedent). A
  small `Crc32c` with `update(&[u8]) -> u32`. (Alternative: the `crc32fast` crate;
  rejected to keep `vela-log`'s dependency surface minimal.)

### Segment files (R3)

- One file per segment, named by its **base index**, zero-padded for lexical =
  numeric ordering: `00000000000000000042.wal` (base index 42).
- A segment holds a contiguous ascending run of frames; no frame spans two
  segments (R3.1). Recovery lists `*.wal`, parses the base index from the name,
  and orders ascending (R3.5, R5.1).
- **Rollover** (R3.3): on persist, if the active segment is non-empty **and**
  `active.size + frame.size > segment_size`, seal the active segment and start a
  new one whose base index is the new entry's index.
- **Oversized frame** (R3.4): if a single frame is larger than `segment_size` and
  the active segment is empty, it is written as the sole frame of its own segment
  (prevents an infinite rollover loop).

### Manifest = Frame_Metadata (R6.8, R7.8, R9)

A single file `wal.manifest` with **two fixed-size slots** (double-buffering):

```
slot {
  magic    u32   ("VWAL")
  version  u16
  seq      u64           # monotonically increasing; newest valid slot wins
  log_start_index u64
  commit   tag(u8)+u64   # None / Some(commit_index)
  durable_last    tag(u8)+u64   # None / Some(last acknowledged index)
  durable_segment u64           # segment base index containing durable_last
  durable_offset  u64           # byte offset just past durable_last's frame
  crc      u32           # CRC32C over the slot bytes preceding crc
}
```

- A manifest update writes the **other** slot, `fsync`s, then is considered
  committed. Recovery reads both slots, discards any with a bad magic/CRC, and
  picks the valid slot with the highest `seq`. A crash mid-update therefore always
  leaves at least the previous intact slot (R6.8) — crash-atomic with fallback.
- `durable_last/segment/offset` is the **acknowledged extent**: the high-water
  mark of data the WAL has promised is durable. Recovery trusts frames up to this
  point and treats anything beyond it as a torn-tail candidate.

This manifest, not segment footers, is the single source of truth for the
acknowledged extent, `commit_index`, and `log_start_index`. Choosing a manifest
over per-segment footers keeps `commit`/`revert`/`compaction` state updates
atomic and in one place, and avoids rewriting a sealed segment's footer.

### In-memory data structures

```rust
struct IndexEntry { term: u64, segment: u64, offset: u64, len: u32 } // no payload

struct DurableWal<F: FileSystem> {
    cfg: WalConfig,
    fs: F,
    dir_lock: DirLock,                 // exclusive Data_Directory lock (R11.8)
    index: Vec<IndexEntry>,            // index[i] => absolute index (log_start_index + i)
    log_start_index: u64,              // R8/R13: absolute base of retained range
    commit_index: Option<u64>,
    segments: Vec<SegmentMeta>,        // ordered; base_index, path, sealed, size
    active: Option<ActiveSegment>,     // open append handle + write buffer
    manifest: Manifest,                // two-slot writer, current seq
    durable_last: Option<u64>,         // acknowledged extent mirror
    poisoned: bool,                    // set on unrecoverable read I/O (fail-stop)
}
```

Absolute-index mapping (R8, R13):

- `last_index() = if index.is_empty() { None } else { Some(log_start_index + index.len()-1) }`
- physical slot for absolute `i` is `i - log_start_index`, valid only when
  `log_start_index <= i <= last_index`.
- `IndexEntry` never owns payload bytes (R13.1, R13.4); payloads are read from the
  segment file via `(segment, offset, len)` on demand.

## LogStorage trait change

Additive method with a default no-op so `vela-raft` and `InMemoryLog` are
untouched (R1.1, R4.8–R4.10, R12.6):

```rust
pub trait LogStorage {
    // ... existing methods unchanged ...
    /// Force buffered records to stable storage. Default: success no-op.
    fn flush(&mut self) -> Result<(), LogError> { Ok(()) }
}
```

`InMemoryLog` inherits the default; `DurableWal` overrides it to fsync.

### LogError additions (R10)

```rust
#[derive(Debug, Error)]
pub enum LogError {
    // existing
    #[error("commit index {requested} out of bounds (commit_index={current:?}, last_index={last:?})")]
    CommitOutOfBounds { requested: u64, current: CommitIndex, last: Option<u64> },
    #[error("cannot revert to index {requested} below commit index {commit:?}")]
    RevertBelowCommit { requested: u64, commit: CommitIndex },
    #[error("append_entries received non-contiguous or out-of-order entries")]
    NonContiguousEntries,
    // new
    #[error("durable log I/O failure during {op}: {source}")]
    Io { op: &'static str, #[source] source: std::io::Error },
    #[error("log corruption at index {index}: {detail}")]
    Corruption { index: u64, detail: &'static str },
    #[error("compaction retained point {requested} out of bounds (commit_index={commit:?}, log_start={log_start})")]
    CompactionOutOfBounds { requested: u64, commit: CommitIndex, log_start: u64 },
    #[error("invalid WAL configuration: {detail}")]
    Config { detail: String },
}
```

`Io` carries the in-progress operation name (R10.2); every variant has a distinct,
non-empty `Display` (R10.5). `Config` is distinct from `Io` so configuration
validation isn't conflated with real I/O (R11.5/R11.7).

> Note: `LogError` currently derives `PartialEq, Eq`. `std::io::Error` is not
> `PartialEq`. The design drops `PartialEq/Eq` from `LogError` and updates the
> existing `InMemoryLog` tests to match on variants (e.g. `matches!`) instead of
> `assert_eq!` on the error value. This is the one ripple into existing code and
> is contained to test assertions.

## Sync policy (R4)

```rust
pub enum SyncPolicy { Always, Periodic { interval_ms: u64 }, Never }
```

- **Always** (default, only consensus-safe — R4.7): on `append`/`append_entries`,
  write frame bytes, `fsync` the segment file; if a new segment file was created,
  `fsync` the parent directory (R4.1); then advance the manifest's acknowledged
  extent and `fsync` the manifest (R4.11) before returning. `commit` and `revert`
  likewise fsync the manifest before returning (R4.4, R9.5). This is the flagged
  double-fsync (data + manifest); `append_entries` amortizes it across a batch.
- **Periodic**: writes go to the OS; a force is issued lazily so that the
  wall-clock gap between forces during active operation does not exceed
  `interval_ms`; an idle log may exceed it (operation-driven, no background
  thread — R4.2). Default interval 1000 ms (R11.6).
- **Never**: write to the OS, never force (R4.3).
- A failed force returns `Io` and leaves reported state unchanged (R4.5); a failed
  periodic force surfaces on the next mutating call (R4.6).

## Key algorithms

### open(cfg) -> Result<DurableWal> (R5, R11)

```
validate(cfg):                                 # R11.5/R11.7
  if segment_size == 0 -> Err(Config)
  if Periodic{interval_ms==0} -> Err(Config)
create_dir_all(data_dir) or Err(Io)            # R11.4 (no partial init on failure)
dir_lock = try_exclusive_lock(data_dir)        # R11.8
  if held -> Err(Io)                            # do not modify dir
manifest = Manifest::read_best_slot(data_dir)? # highest-seq valid slot, or empty
if no segments and no manifest:                # R5.2
  init empty: index=[], log_start=0, commit=None, durable_last=None
else:
  recovery::replay(...)                        # below
```

### recovery::replay (R5, R6)

```
segs = list "*.wal", parse base index, sort ascending          # R5.1, R3.5
log_start = manifest.log_start_index (default 0)
commit    = manifest.commit
ack       = manifest.durable_last (acknowledged extent)
expected  = log_start                                          # next index expected
for seg in segs:
  skip seg entirely if it lies fully below log_start           # R7.5 partial-skip / orphan (R7.8)
  scan frames sequentially from seg start:
    skip frames with index < log_start (partially-retained seg) # R7.5
    decode frame:
      if incomplete/CRC-bad:
        if a valid frame appears later (interior)  -> Err(Corruption{index:expected}) # R6.3
        else  -> this begins the torn tail run -> stop scanning  # R6.1
      if frame.index != expected -> Err(Corruption{index:expected})
      push IndexEntry{term, segment, offset, len}; expected += 1
# reconcile against the acknowledged extent (Frame_Metadata):
recovered_last = expected - 1 (or None)
if policy == Always and recovered_last < ack:                  # R6.4
   Err(Corruption{index: ack})                                  # never silently truncate
if policy in {Periodic, Never} and recovered_last < ack:       # R6.5
   accept as torn-tail truncation (expected loss)
# torn tail beyond ack is discarded by stopping the scan        # R6.1/R6.2
last_index = recovered_last
commit = min(commit, last_index)                               # R5.4 invariant
```

`commit` after recovery is `None` or `<= last_index`, and `>= last forced
commit` (R5.4). A torn or CRC-bad manifest slot is ignored in favor of the last
intact slot (R6.8). Any filesystem error during replay returns `Io` with no
partial init (R5.5).

### append(payload, term) -> Result<u64> (R1.3, R3, R4)

```
fail if poisoned
index = last_index().map_or(log_start_index, |l| l+1)
        # = log_start_index when empty (incl. after compaction) -> R8.5
frame = encode(index, term, payload)
seg = active or create_first_segment(base=index)               # R3.2
if seg non-empty and seg.size + frame.len > segment_size:       # R3.3
   seal(seg); seg = create_segment(base=index); new_file=true
if frame.len > segment_size and seg empty: write as sole frame  # R3.4
write frame to seg
if policy==Always: fsync(seg); if new_file fsync(dir);          # R4.1
                   manifest.set_ack(index, seg, off); fsync(manifest) # R4.11
push IndexEntry; return index
# on any I/O failure: restore pre-op state, return Io           # R10.3
```

### append_entries(entries) (R1.4–R1.6)

```
if empty -> Ok                                                  # R1.6
verify entries contiguous & ascending; else NonContiguousEntries # R1.4
start = entries[0].index
if start > last_index+1 -> NonContiguousEntries                 # gap
if commit.is_some() and start <= commit -> NonContiguousEntries  # commit-conflict (R1.4 note)
# overwrite uncommitted suffix from `start`:
truncate index to start; truncate/seal segment data at start's offset;
reduce manifest ack to start-1 first, then write batch frames    # consistent with revert rule
append each entry frame (as in append, ascending)               # R1.5
Always: single fsync(seg)+dir(if new)+manifest at end of batch
```

### read / entry / term_at / snapshot (R8, R10.4, R13)

```
term_at(i):  in-range? index[i-log_start].term : None           # memory only, infallible (R8.2)
last_index(), commit_index(): from memory                       # infallible (R13.2)
entry(i):    if !in_range(i) -> None                            # R8.1
             read_payload(seg, off, len) from disk; decode      # R13.3
read(s,e):   clamp to [log_start..=last_index] ∩ [s..=e]; ascending; # R8.3
             empty if no overlap (incl. s>e, all below log_start)    # R8.4
snapshot():  commit None -> Snapshot{None, []}                  # R8.7
             else entries in log_start..=commit (read each)     # R8.6
# read_payload I/O error -> poison + fail-stop (see below)      # R10.4
```

### compaction(retained_point) (R7, R8)

```
if retained_point <= log_start_index: return Ok (no-op)         # R7.4 (precedence over R7.3)
if commit.is_none()
   or retained_point > commit                                   # would discard commit/uncommitted
   -> Err(CompactionOutOfBounds)                                 # R7.3
manifest.set_log_start(retained_point); fsync(manifest)         # R7.8 persist BEFORE deleting
drop index entries below retained_point; log_start = retained_point  # R13.5
delete segments whose entire range < retained_point             # R7.5 whole-segment reclaim
# frames below retained_point inside a still-retained segment stay on disk,
# skipped on read & recovery                                     # R7.5
# crash between manifest fsync and deletes: orphan low segments ignored on recovery (R7.8)
# retained entries keep identical index/term/payload; last_index, commit unchanged (R7.6)
```

### revert(index) (R9)

```
if commit.is_some() and index < commit -> RevertBelowCommit     # R1.8/R9.4
if index < log_start_index -> RevertBelowCommit                 # R9.4
reduce manifest ack to `index`; fsync(manifest)                 # R9.6 (extent first)
truncate in-memory index to keep 0..=index (relative)
truncate active segment / remove now-unreferenced tail segments  # R9.6
Always: fsync before return                                     # R9.5
# recovery now neither restores reverted entries nor sees a frame/metadata mismatch
```

### flush() (R4.8–R4.10)

```
DurableWal::flush: fsync active segment + manifest; map failure to Io,
                   leave reported state unchanged.
InMemoryLog: inherits default no-op Ok(()).
```

## Error Handling

Index/term/commit queries are answered from memory and cannot fail. The
payload-bearing reads (`entry`, `read`, `snapshot`) touch disk. Because the trait
read signatures are **infallible** (changing them would break the
consensus-facing seam), an I/O failure on a read is treated as an **unrecoverable
fault**:

1. log the error via `tracing::error!` (out-of-band surface),
2. set `self.poisoned = true`,
3. **fail-stop**: panic with a clear message rather than return `None` / an empty
   range / a short `Snapshot`.

This guarantees an I/O error is never disguised as data absence (R10.4). A future
trait revision could add a fallible read seam (`try_entry` etc.); that is noted as
the alternative and deliberately not done here to keep `vela-raft` untouched.

## Correctness Properties

### Property 1: Acknowledged data is never lost (Always)

Frame bytes are fsynced, and only then is the manifest acknowledged-extent
advanced and fsynced. A crash between the two leaves a durable frame the manifest
doesn't yet cover → recovery treats it as torn tail and discards it, which is safe
because `append` had not yet returned success for it (R4.11 ordering).

**Validates: Requirements 4.11, 5.3, 6.1, 6.7**

### Property 2: Torn tail is distinguished from interior corruption

The manifest's acknowledged extent plus per-frame CRC let recovery tell an
incomplete tail (no valid frame after, and ≤ ack under Always) from interior
corruption (a valid frame after a bad one, or a shortfall below ack under Always)
→ `Corruption`, never silent truncation (R6).

**Validates: Requirements 6.1, 6.3, 6.4, 6.5**

### Property 3: The manifest survives a torn update

Double-buffered slots + CRC + monotonic `seq` mean recovery always falls back to
the last intact slot when the newest update is torn (R6.8).

**Validates: Requirements 6.8**

### Property 4: Compaction never regresses or loses retained data

`log_start_index` is persisted before any segment delete; a crash mid-delete
leaves orphan below-line segments that recovery ignores; `log_start_index` never
regresses and no retained entry is lost (R7.8).

**Validates: Requirements 7.5, 7.8**

### Property 5: An I/O error is never observed as data absence

A read-path I/O failure poisons the WAL and fail-stops rather than returning
`None`, an empty range, or a short `Snapshot` (R10.4).

**Validates: Requirements 10.4**

### Property 6: Drop-in equivalence with InMemoryLog

For any operation sequence performing no compaction, `DurableWal` returns results
and exposes observable state equal to `InMemoryLog` (R1.2), with `flush` an
always-successful no-op on `InMemoryLog`.

**Validates: Requirements 1.2, 12.6**

## Testing Strategy

- **Unit (frame.rs):** encode/decode round-trip incl. empty payload; CRC detects
  single-bit flips, truncation, length corruption; incomplete-frame classification.
- **Unit (segment.rs):** rollover at boundary, oversized-frame-own-segment,
  ordering, name parsing.
- **Unit (recovery.rs) via the `fs` fault seam:** clean reopen; torn tail
  (truncated final frame) recovered; interior corruption → `Corruption`; torn
  manifest slot → fallback; orphan low segment after compaction ignored;
  Never/Periodic tail shortfall accepted vs Always shortfall → `Corruption`.
- **Unit (compaction/revert):** segment-granularity deletion, partial-segment
  skip, no-op precedence (R7.4 over R7.3), revert extent/segment consistency.
- **Config:** zero segment size / zero interval → `Config`; unwritable dir → `Io`,
  no partial init; second open of same dir → `Io` (lock).
- **proptest:**
  - *Framing round-trip* over ≥256 random `LogEntry` values (R2.4 / R12.4).
  - *Recovery round-trip*: random op sequences (append/append_entries/commit/
    revert), force, reopen, assert identical retained entries / last_index /
    log_start / commit over ≥256 cases (R5.3 / R12.3).
  - *Equivalence vs `InMemoryLog`*: same op sequence (no compaction) yields equal
    results and observable state (R1.2).
- **fs fault seam:** an internal `FileSystem` trait (real impl over `std::fs`;
  test impl that can truncate the last write, fail an fsync, or reorder) makes
  crash scenarios deterministic without real process kills — and keeps reads
  testable for the fail-stop path. This seam is internal to `wal`, not part of
  `LogStorage`.

## Requirements traceability

| Requirement | Realized by |
|---|---|
| R1 Drop-in trait | `DurableWal` impls `LogStorage`; `flush` default; equivalence proptest |
| R2 Framing/CRC | `frame.rs` layout + in-house CRC32C; incomplete=corrupt |
| R3 Segments | `segment.rs` naming, rollover, oversized rule, range recording |
| R4 Durability/sync | sync policy in `mod.rs`; dir fsync; manifest fsync (R4.11); `flush` |
| R5 Recovery | `recovery::replay`; empty-dir init; R5.4 commit invariant; Io on fail |
| R6 Torn vs interior | per-frame CRC + manifest extent; policy-scoped shortfall; manifest fallback |
| R7 Compaction | `compaction`: no-op precedence, bounds, segment reclaim, persist-before-delete |
| R8 Absolute index | dense index offset by `log_start`; read/entry/term_at/snapshot bounds |
| R9 Commit/revert durability | manifest commit/extent updates fsynced; revert extent consistency |
| R10 Errors | `LogError` Io/Corruption/CompactionOutOfBounds/Config; read fail-stop |
| R11 Config | `config.rs` validation + defaults; dir lock; no partial init |
| R12 Boundaries/tests | `wal` in `vela-log`, no `vela-*`/`anyhow` deps; proptests |
| R13 Index/payload split | `IndexEntry` w/o payload; memory vs disk read split; drop on compaction |

## Open implementation notes

- Dropping `PartialEq/Eq` from `LogError` (due to `io::Error`) requires touching
  the existing `InMemoryLog` unit tests to assert on variants. Contained to tests.
- The default `flush` lands on the trait in `lib.rs`; no other crate changes.
