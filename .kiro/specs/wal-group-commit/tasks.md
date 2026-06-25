# Implementation Plan: WAL Group Commit

## Overview

Bottom-up across the inward dependency edge `vela-log → vela-raft →
vela-server`, keeping the workspace green at every checkpoint. The single-record
and batch produce semantics, offset assignment, and crash-recovery guarantees
are preserved; only *when* the `fsync` happens and *which thread* it runs on
change. Property tests use `proptest` (≥100 iterations), live in each crate's
`tests/` directory, and are tagged `// Feature: wal-group-commit, Property {n}`.

## Tasks

- [x] 1. `vela-log`: Durable_Index + Grouped policy + group `flush`
  - [x] 1.1 Add `LogStorage::durable_index(&self) -> CommitIndex`
    - Default impl returns `last_index()`; `InMemoryLog` uses the default;
      `DurableWal` returns `durable_last`.
    - _Requirements: 1.2, 1.3_
  - [x] 1.2 Add `SyncPolicy::Grouped` and route it through config + `persist_tail`
    - `append`/`append_entries` under `Grouped` write frame bytes but never
      auto-force (like `Never`); validation/serialisation accept it.
    - _Requirements: 3.1_
  - [x] 1.3 Make `flush()` a true group force
    - Force every segment holding un-forced frames in `durable_last+1..=last_index`,
      advance `durable_last`/segment/offset to `last_index`, durably write the
      manifest extent; on failure return `Io` leaving the extent unchanged.
    - _Requirements: 1.4, 3.2_
  - [x] 1.4 Unit + property tests
    - Unit: Grouped append buffers; flush forces across a segment rollover;
      durable_index advances; flush-failure leaves the extent unchanged; reopen
      recovers exactly the flushed prefix.
    - **Property 1**: for any append/flush interleaving `durable_index <=
      last_index`, and after a successful `flush` `durable_index == last_index`.
    - _Requirements: 1.4, 1.5, 3.2_

- [x] 2. Checkpoint — `cargo test -p vela-log` green, fmt/clippy clean.

- [x] 3. `vela-raft`: gate commit/ack on Durable_Index
  - [x] 3.1 Add `RaftInput::Durable` and re-drive on durability advance
    - Leader re-runs `advance_commit`; follower re-emits the deferred ack to the
      current leader with `match_index = durable_index`.
    - _Requirements: 1.2, 1.3_
  - [x] 3.2 Gate leader commit on `durable_index`
    - `advance_commit` ceiling becomes `min(last_index, durable_index)`; keep the
      current-term + majority rule.
    - _Requirements: 1.3_
  - [x] 3.3 Gate follower ack on `durable_index`
    - Success `match_index = min(last_appended, durable_index)`; the post-flush
      `Durable` input produces the ack covering the newly-durable entries.
    - _Requirements: 1.2_
  - [x] 3.4 Property tests
    - **Property 2**: leader `commit_index <= durable_index` across arbitrary
      schedules.
    - **Property 3**: a follower's acked `match_index <= durable_index`.
    - **Property 4**: group-commit drive yields identical committed offsets and
      values to the per-append drive.
    - _Requirements: 1.2, 1.3, 3.3_

- [x] 4. Checkpoint — `cargo test -p vela-raft` green, fmt/clippy clean.

- [x] 5. `vela-server`: group-committing driver
  - [x] 5.1 Drain-and-batch the driver loop
    - `recv().await` then `try_recv` up to `MAX_DRAIN`; step each (buffered),
      buffer outbound sends and pending produces instead of dispatching inline.
    - _Requirements: 3.1_
  - [x] 5.2 Offloaded single flush per cycle + Durable re-drive
    - `block_in_place(|| replica.flush())`; on `Ok` step `RaftInput::Durable`,
      then dispatch buffered sends and resolve committed produces; on `Err`
      `revert(durable_index())`, fail affected produces, drop pending acks, log.
    - _Requirements: 1.1, 1.4, 2.1, 2.2, 2.3, 3.1_
  - [x] 5.3 Same offload for the metadata driver; remove the blocking offset read
    - `block_in_place` the `MetadataDriver` append/flush; replace
      `records_before`/`offset_at` disk read with the in-memory `StateMachine`
      offset.
    - _Requirements: 2.1, 2.2_
  - [x] 5.4 Switch durable partition + metadata WAL config to `Grouped`
    - `durable_wal_config` and the metadata WAL use `SyncPolicy::Grouped`; the
      driver owns forcing. (Durability now comes from the driver's flush, not
      per-append force.)
    - _Requirements: 1.1, 3.1_
  - [x] 5.5 Integration test
    - Concurrent batched produce across a 3-node in-process cluster commits all
      records with no spurious leadership change; consume returns them in order.
    - _Requirements: 1.1, 4.1_

- [x] 6. Checkpoint — `cargo test --workspace` green; fmt + `clippy -D warnings`
  clean.

- [x] 7. End-to-end validation against the docker cluster
  - Rebuild the image, `docker compose up -d`, run the reproducing bench
    command, confirm `PASSED` with a non-trivial produce throughput and no
    `NoLeader`.
  - _Requirements: 4.1_

## Notes

- Per-append `Always` durability remains available and unchanged for any caller
  that wants it; only the cluster's partition/metadata logs move to `Grouped`
  with driver-owned forcing.
- The safety invariant to preserve in review: no replica acks or counts an entry
  toward commit before its local `fsync`; commit = majority of durable stores.
