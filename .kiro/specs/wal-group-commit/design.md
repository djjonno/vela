# Design: WAL Group Commit

## Overview

The fix keeps the per-partition driver as the single writer it already is, but
turns it into a **group-committing writer**: it appends queued mutations to a
*buffered* WAL, forces them to disk with a single **offloaded** `fsync`, and
only then releases any acknowledgement. Consensus is made durability-aware via a
**Durable_Index**: a replica counts/acks an entry only once it is `fsync`ed
locally, so "committed" continues to mean "fsynced on a majority."

This is the smallest change that is *correct* at scale. We deliberately do not:

- run one OS thread per partition (does not scale to many partitions per node);
- split the `DurableWal` into separate appender/syncer owners sharing file
  handles (high-risk surgery); or
- wrap consensus state in an `Arc<Mutex>` writer (reintroduces locking on the
  hot path the design explicitly avoids).

Instead the existing single-writer task offloads only the blocking `fsync` via
`tokio::task::block_in_place`, which lets the runtime relocate other tasks to
sibling workers for the duration of the force.

## Layered changes (inward dependency order)

```
vela-log (durable seam)  ->  vela-raft (durable gating)  ->  vela-server (group-commit driver)
```

### 1. `vela-log` — buffered append + a real group `flush` + Durable_Index

The `DurableWal` already separates "write frame bytes" (buffered under
`Never`/`Periodic`) from "force" (`force_tail`/`flush`), and already tracks the
durable extent (`durable_last`/`durable_segment`/`durable_offset`). We build on
that.

- **`LogStorage::durable_index(&self) -> CommitIndex`** (new trait method).
  - `InMemoryLog`: returns `last_index()` — a volatile log treats an append as
    immediately "durable" in the only sense it can (it has no disk), so existing
    consensus behaviour over the in-memory log is unchanged.
  - `DurableWal`: returns `durable_last`.
  - Default impl returns `last_index()` so any other implementor is unchanged.

- **`SyncPolicy::Grouped`** (new variant) — append/`append_entries` write frame
  bytes to the OS but never auto-force; forcing is caller-driven via `flush`.
  We add a distinct variant (rather than reusing `Never`) so intent is explicit
  and config validation/serialisation stays meaningful. `Always`/`Periodic`/
  `Never` behaviour is untouched, so all existing WAL tests keep passing.

- **Strengthen `flush()`** so it is a true group force:
  1. `sync` **every** segment that holds un-forced frames between
     `durable_last+1` and `last_index` (not just the active segment), so a
     batch that rolled segments is fully durable.
  2. Advance the in-memory durable extent (`durable_last`/segment/offset) to
     `last_index`'s frame.
  3. Durably write the manifest with the advanced extent (so a reopen
     acknowledges it, consistent with recovery).
  4. On any force failure: return `LogError::Io`, leave `durable_last` and the
     manifest unchanged (the un-forced tail stays non-durable). `last_index` and
     `commit_index` (reported state) are unchanged either way (Req 1.4, 3.2).

- **Recovery is unchanged.** A `Grouped` log that crashed with a buffered,
  un-forced tail recovers exactly the `fsync`ed prefix (the existing torn-tail
  classification already does this), so Req 1.5 holds for free.

### 2. `vela-raft` — gate commit/ack on Durable_Index

Consensus logic is unchanged except that the leader's self-count and the
follower's ack are gated on the local Durable_Index, and a new input lets the
driver re-drive consensus when durability advances.

- **`RaftInput::Durable`** (new) — "my log's Durable_Index may have advanced."
  - Leader: re-run `advance_commit` (which now also gates on `durable_index`).
  - Follower/candidate: if there are newly-durable entries a leader is waiting
    on, emit a fresh `AppendEntries` ack carrying the new `match_index`
    (= Durable_Index). Modelled as: the driver, after a flush, asks the replica
    to produce the deferred ack for the most recent leader.

- **Leader `advance_commit`** — change the candidate-index ceiling from
  `last_index` to `min(last_index, durable_index)`, and keep the existing
  "current-term entry, majority of `match_index`" rule. So the leader never
  commits an index it has not itself `fsync`ed (Req 1.3).

- **Follower `handle_append_entries`** — split "append" from "ack":
  - Append the entries (buffered; no force here).
  - The success ack's `match_index` is `min(last_appended, durable_index)`.
    Because the follower has not yet flushed the just-appended entries, the
    *immediate* ack reports only the previously-durable extent; the post-flush
    `RaftInput::Durable` produces the ack that advances `match_index` to include
    the new entries (Req 1.2).
  - Log-matching/conflict/term rules are unchanged.

- **Hard state (term/vote)** still persists-before-return (`persist_hard_state`
  fsyncs). It is low-frequency (elections only), so it is offloaded at the
  driver via `block_in_place` rather than group-committed. The vote/term
  durability guarantee is unchanged.

This keeps the safety argument simple and textbook: **no node ever acknowledges
or counts an entry it has not durably stored, and commit = majority of durable
stores.** Group commit only changes *when* the force happens, never the gating.

### 3. `vela-server` — the group-committing driver

The driver loop (`PartitionDriver::run`) becomes:

1. `recv().await` one `DriverCommand`, then **drain** the rest of the ready
   queue with `try_recv` up to a bound (`MAX_DRAIN`, e.g. 1024) — this is the
   batch.
2. Apply each command via `replica.step(..)` (buffered appends; cheap, no
   `fsync`). Collect outputs but **defer** releasing any acknowledgement:
   - hold produce/`ProduceBatch` replies in `pending` as today;
   - hold follower acks and other outbound sends in a per-cycle buffer instead
     of dispatching them inside `after_step`.
3. If any command appended to the log, force once:
   `block_in_place(|| replica.flush())`.
   - On `Ok`: step `RaftInput::Durable` so the leader re-advances commit and the
     follower emits its now-durable ack; then dispatch all buffered sends and
     resolve any pending produces whose target is now committed.
   - On `Err(Io)`: `revert(durable_index())` to drop the non-durable tail,
     fail the affected pending produces with a typed error, drop the would-be
     acks (do not send), and log. Recovery/retry proceeds normally (Req 1.4).
4. Election/heartbeat ticks, consume reads, and `KnownLeader` need no force;
   they dispatch immediately (consume still reads the in-memory state machine).

Group commit falls out of step 1–3: N produces drained together become N
buffered appends + 1 `fsync`. `block_in_place` (step 3) keeps the runtime
schedulable during the force, fixing the starvation (Req 2). The metadata
driver (`MetadataDriver`) gets the same `block_in_place` treatment for its
appends; its volume is low so batching there is optional.

Also fix the incidental blocking read on the ack path: `offset_at` /
`base_offset_at` currently call `records_before` → `log.read(0, index)` (a disk
read on the durable backend, and O(n²)). Replace it with the in-memory
`StateMachine` offset already tracked on apply, so the dispatch path does no
disk I/O.

## Failure handling and correctness notes

- **Flush failure mid-batch**: the in-memory log may be ahead of the durable
  extent. `revert(durable_index())` restores the invariant "in-memory tail ==
  durable tail" before the next cycle; affected produces fail (caller retries),
  and no ack/commit was released for the non-durable entries (Req 1.4).
- **Single-node group**: the leader is its own majority. A produce still
  appends → flush → `Durable` → `advance_commit` commits at `durable_index` →
  reply. The reply is released only after the flush, so a single-node ack is
  still fsync-backed (Req 1.1).
- **Leader applying committed entries to the state machine**: unchanged; commit
  index only advances to `<= durable_index`, so committed (hence read-visible)
  entries are always locally durable on the leader.

## Testing strategy

- `vela-log`: unit tests for `Grouped` append-buffers / `flush`-forces-all-
  segments / `durable_index` advancement / flush-failure leaves extent
  unchanged. Proptest: for any append/flush interleaving, `durable_index <=
  last_index`, and after a `flush` with no failure `durable_index ==
  last_index`; a reopen recovers exactly the flushed prefix.
- `vela-raft`: proptest — across arbitrary append/replicate/flush schedules, the
  leader's `commit_index` never exceeds its `durable_index` (Req 1.3) and a
  follower's acked `match_index` never exceeds its `durable_index` (Req 1.2);
  and a batch driven with group commit yields the **same** committed offsets and
  values as the per-append path (Req 3.3).
- `vela-server`: the existing cross-node produce/consume integration tests must
  still pass; add one that drives concurrent batched produce and asserts no
  spurious leadership change and all records commit.
- End-to-end: the reproducing bench command completes `PASSED` (Req 4.1).
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test --workspace`.
