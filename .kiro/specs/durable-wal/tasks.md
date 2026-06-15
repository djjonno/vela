# Implementation Plan

## Overview

This plan implements the durable on-disk WAL (`DurableWal`) and log compaction in
the `vela-log` crate, behind the existing `LogStorage` trait (which gains only an
additive, default `flush`). Work proceeds inside-out: framing and the filesystem
seam first, then segments, manifest, and the in-memory index, then the
`DurableWal` operations, then recovery and compaction, and finally the property
tests and quality gate. Each task is test-inclusive because the requirements
mandate specific proptests (R12.3, R12.4) and behavior-level guarantees.

## Tasks

- [x] 1. Crate scaffolding: trait change, error variants, module skeleton
  - Add the `wal` module tree (`wal/{mod,config,frame,segment,index,manifest,recovery,fs,error}.rs`) under `crates/vela-log/src/`, wired into `lib.rs`.
  - Add the additive `fn flush(&mut self) -> Result<(), LogError> { Ok(()) }` default to the `LogStorage` trait; confirm `InMemoryLog` inherits it unchanged.
  - Extend `LogError` with `Io { op, source }`, `Corruption { index, detail }`, `CompactionOutOfBounds { requested, commit, log_start }`, and `Config { detail }`; drop `PartialEq, Eq` from `LogError` and update existing `InMemoryLog` tests to match on variants (`matches!`) instead of `assert_eq!` on the error value.
  - Ensure the workspace still builds and existing `vela-log` tests pass.
  - _Requirements: 1.1, 4.8, 4.9, 10.1, 10.5, 12.1, 12.2_

- [x] 2. Record framing and CRC32C
  - [x] 2.1 Implement an in-house `Crc32c` (Castagnoli table) with `update(&[u8]) -> u32`.
    - _Requirements: 2.1_
  - [x] 2.2 Implement `frame` encode/decode for the `len(u32) | body(index,term,kind,payload) | crc(u32)` layout, with CRC over `len + body`; map `PayloadKind` to/from the `kind` byte.
    - Classify incomplete input (short `len`, or fewer than `len + 4` bytes) and CRC mismatch as corrupt.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6_
  - [x] 2.3 Unit tests: round-trip including empty payload; single-bit flip, truncation, and length-field corruption all detected; incomplete-frame classification.
    - _Requirements: 2.3, 2.4, 2.5_
  - [x] 2.4 Property test (proptest, ≥256 cases): framing round-trip over random `LogEntry` values.
    - _Requirements: 2.5, 12.4_

- [x] 3. Filesystem seam for real I/O and deterministic fault injection
  - Define an internal `FileSystem`/file-handle trait (open/read/write/fsync/fsync_dir/rename/remove/lock) with a `std::fs`-backed implementation.
  - Provide a test implementation that can truncate the last write (torn write), fail a specific `fsync`, and simulate a missing/locked directory.
  - Keep this seam internal to `wal` (not part of `LogStorage`).
  - _Requirements: 5.5, 6.1, 10.2, 10.3_

- [x] 4. Configuration, validation, and exclusive directory lock
  - Implement `WalConfig { data_dir, segment_size, sync_policy }` and `SyncPolicy { Always, Periodic { interval_ms }, Never }` with defaults (64 MiB, `Always`, 1000 ms).
  - Validate: zero `segment_size` → `Config`; `Periodic { interval_ms: 0 }` → `Config`; unwritable/uncreatable `data_dir` → `Io` with no partial init.
  - Acquire an exclusive lock on `data_dir` on open; if already held, fail with `Io` and do not modify the directory.
  - Unit tests for each validation/lock path.
  - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8_

- [x] 5. Segment files
  - Implement `segment`: base-index file naming (zero-padded `*.wal`), append a frame, sequential validated scan, seal, on-disk size tracking, and name parsing/ordering.
  - Enforce: each frame wholly within one segment; rollover when active is non-empty and the next frame would exceed `segment_size`; oversized frame becomes the sole frame of its own segment; preserve ascending index order across segments.
  - Unit tests: boundary rollover, oversized-frame-own-segment, ordering, name parse.
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 6. Durable manifest (Frame_Metadata)
  - Implement `manifest`: two fixed-size CRC'd slots holding `seq`, `log_start_index`, `commit_index`, and the acknowledged extent (`durable_last`, segment, offset).
  - Write alternates slots + fsync; read selects the highest-`seq` slot with valid magic/CRC; a torn newest slot falls back to the prior intact slot.
  - Unit tests: write/read round-trip; torn-slot fallback; empty (fresh) manifest.
  - _Requirements: 6.8, 7.8, 9.1_

- [x] 7. In-memory index and absolute-index mapping
  - Implement `index` as a dense `Vec<IndexEntry { term, segment, offset, len }>` offset by `log_start_index`; no payload bytes retained.
  - Provide `last_index`, physical-slot mapping, bounds checks for below-`log_start_index`/above-`last_index`.
  - Unit tests for mapping and bounds, including a post-compaction `log_start_index > 0`.
  - _Requirements: 8.1, 8.2, 13.1, 13.2, 13.4_

- [x] 8. DurableWal core: open-empty, append, in-memory queries, Always sync
  - Implement `DurableWal::open` for an empty/absent directory (init: empty index, `log_start_index = 0`, `commit_index = None`).
  - Implement `append`: index assignment (`last_index+1`, else `log_start_index`), framing, segment write/rollover, and under `Always` fsync frame + parent dir on new segment + manifest extent.
  - Implement `last_index`, `term_at`, `commit_index` from memory; restore pre-op state and return `Io` on write failure.
  - Unit tests including reopen-after-append (Always) round-trip via the fs seam.
  - _Requirements: 1.3, 4.1, 4.4, 4.11, 5.2, 8.5, 10.3, 13.2_

- [x] 9. Disk-backed reads with fail-stop
  - Implement `entry`, `read` (clamped to `[log_start_index..=last_index] ∩ [start..=end]`, ascending; empty when no overlap or `start > end`), and `snapshot` (`None` → empty; else `log_start_index..=commit_index`), reading payloads from segments.
  - On a read-path I/O error: log via `tracing::error!`, set `poisoned`, and fail-stop (panic) — never return `None`/empty/short `Snapshot` for an I/O failure.
  - Unit tests for bounds/clamping and a fault-injected read fail-stop.
  - _Requirements: 8.3, 8.4, 8.6, 8.7, 10.4, 13.3_

- [x] 10. append_entries reconciliation
  - Implement empty-batch no-op; contiguity/ordering validation; gap and commit-conflict rejection (`NonContiguousEntries`); overwrite of the uncommitted suffix (truncate index + segment data + reduce manifest extent, then write the batch, ascending).
  - Under `Always`, fsync once at end of batch (frame(s) + dir-if-new + manifest).
  - Unit tests for extend, overwrite-suffix, gap, empty, and commit-conflict cases.
  - _Requirements: 1.4, 1.5, 1.6, 4.1, 4.11_

- [x] 11. commit and revert with durability
  - Implement `commit` (advance within `current..=last_index`; reject out of bounds; under `Always` fsync manifest before return).
  - Implement `revert` (reject below `commit_index` or below `log_start_index` with `RevertBelowCommit`; reduce manifest acknowledged extent to the retained index, then truncate/remove unreferenced tail frames/segments consistently; under `Always` fsync before return).
  - Unit + reopen tests asserting commit persists and reverted entries neither return nor cause a frame/metadata mismatch.
  - _Requirements: 1.7, 1.8, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

- [x] 12. Crash recovery and replay
  - Implement `recovery::replay`: list/sort segments by base index, skip frames below `log_start_index`, sequentially validate frames, rebuild index + `last_index` + `log_start_index` + `commit_index`, and clamp `commit_index <= last_index`.
  - Torn tail run (no valid frame after, beyond the acknowledged extent) → discard and succeed; interior corruption (valid frame after a bad one) → `Corruption` with the position-expected index; under `Always` a shortfall below the acknowledged extent → `Corruption`; under `Periodic`/`Never` a tail shortfall → torn-tail truncation; filesystem error → `Io` with no partial init.
  - Unit tests via the fs seam for: clean reopen, torn tail, interior corruption, torn-manifest fallback, and policy-scoped shortfall behavior.
  - _Requirements: 5.1, 5.3, 5.4, 5.5, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8_

- [x] 13. Log compaction
  - Implement `compaction(retained_point)`: no-op success when `retained_point <= log_start_index` (precedence over the bounds rejection); reject (above `log_start_index`) points that discard an uncommitted/commit entry or when `commit_index` is `None` (`CompactionOutOfBounds`); persist advanced `log_start_index` to the manifest (fsync) before deleting any segment; delete only whole below-line segments; drop in-memory index entries for discarded indices; keep retained entries and `last_index`/`commit_index` unchanged.
  - Unit + reopen tests: segment-granularity reclaim, partial-segment skip-on-read/recovery, no-op precedence, crash-ordering (orphan low segment ignored), reopened `log_start_index` preserved.
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.5, 13.5_

- [x] 14. Periodic and Never sync policies and flush
  - Implement operation-driven `Periodic` forcing (bound the gap between forces during active operations; idle may exceed; surface a failed periodic force on the next mutating call) and `Never` (write to OS, never force).
  - Implement `DurableWal::flush` (fsync active segment + manifest; map failure to `Io`, leave state unchanged).
  - Unit tests for periodic forcing cadence (via injected clock), Never durability behavior, and flush success/failure.
  - _Requirements: 4.2, 4.3, 4.5, 4.6, 4.7, 4.10_

- [x] 15. Property tests: recovery round-trip and InMemoryLog equivalence
  - [x] 15.1 Proptest (≥256 cases): random op sequences (append/append_entries/commit/revert) under `Always`, force, reopen, assert identical retained entries / `last_index` / `log_start_index` / `commit_index`.
    - _Requirements: 5.3, 12.3_
  - [x] 15.2 Proptest: same op sequence (no compaction) against `DurableWal` and `InMemoryLog` yields equal return values and observable state, treating `flush` as a no-op on `InMemoryLog`.
    - _Requirements: 1.2, 12.6_

- [x] 16. Workspace integration and quality gate
  - Add `vela-log/tests/` integration tests covering open → produce → reopen → read and a compaction-then-reopen scenario.
  - Confirm `vela-log` declares no `vela-*` or `anyhow` dependency; CRC and any new deps are non-`vela` and pinned.
  - Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` at the workspace root; ensure zero failures alongside the retained `InMemoryLog`.
  - _Requirements: 12.1, 12.2, 12.5_

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1"] },
    { "wave": 2, "tasks": ["2", "3", "4", "6"] },
    { "wave": 3, "tasks": ["5"] },
    { "wave": 4, "tasks": ["7", "8"] },
    { "wave": 5, "tasks": ["9", "10", "11", "14"] },
    { "wave": 6, "tasks": ["12"] },
    { "wave": 7, "tasks": ["13"] },
    { "wave": 8, "tasks": ["15"] },
    { "wave": 9, "tasks": ["16"] }
  ]
}
```

```
1 (scaffolding)
├── 2 (framing + CRC)
│   └── 5 (segments) ──┐
├── 3 (fs seam) ───────┤
├── 4 (config + lock)  │
└── 6 (manifest) ──────┤
                       ├── 7 (in-memory index)
                       └── 8 (DurableWal core: open/append/Always)
                            ├── 9 (disk reads + fail-stop)
                            ├── 10 (append_entries)
                            ├── 11 (commit + revert)
                            │    └── 12 (recovery/replay)
                            │         └── 13 (compaction)
                            └── 14 (Periodic/Never + flush)
                                 └── 15 (property tests)
                                      └── 16 (integration + quality gate)
```

- Task 1 unblocks everything.
- Tasks 2, 3, 4, 6 are largely independent and can proceed in parallel after 1.
- Task 5 needs framing (2) and the fs seam (3); 7 and 8 need 5 and 6.
- Recovery (12) depends on segments, manifest, and the mutating ops (8–11).
- Compaction (13) builds on recovery semantics; property tests (15) need the full
  operation set; the quality gate (16) is last.

## Notes

- The only ripple into existing code is dropping `PartialEq/Eq` from `LogError`
  (because `std::io::Error` is not `PartialEq`) and updating `InMemoryLog`'s tests
  to match on variants. Done in task 1.
- `DurableWal` is single-writer-per-partition: `&mut self` writes, `&self` reads;
  no concurrent-reader-during-write support is built.
- CRC32C is implemented in-house (no new dependency), matching the project's
  in-house `SplitMix64` precedent; if a dependency is later preferred, `crc32fast`
  is the fallback.
- Only the `Always` sync policy is consensus-safe; `Periodic`/`Never` exist for
  non-consensus logs and are validated/tested accordingly.
- Crash scenarios (torn writes, fsync failure, torn manifest) are exercised
  deterministically through the internal `FileSystem` fault seam from task 3, so
  no real process kills are required in tests.
