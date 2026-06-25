# Requirements: WAL Group Commit

## Problem

Under concurrent produce load against the durable backend, the cluster stalls:
commits stop advancing and partition leaders are spuriously deposed, surfacing
to clients as `no leader currently elected for <topic>/<partition>`. The bench
(`vela-bench --record-count 200000 --value-size 512 --partition-count 3
--batch-size 200 --endpoints …`) reproduces this at a 100% rate.

### Diagnosed root cause (confirmed, not assumed)

- No node crashes (containers show `RestartCount=0`, no OOM, no panic). The
  failure is a runtime livelock, not a process death.
- The durable backend runs `SyncPolicy::Always`, so every `append` /
  `append_entries` performs a blocking `fsync` (`sync_all`/`sync_data`).
- That `fsync` runs **inline inside `RaftNode::step`**, which the per-partition
  driver (`crates/vela-server/src/driver.rs`) calls directly on a `tokio` worker
  thread. There is no `spawn_blocking` / `block_in_place`.
- The server uses a default `#[tokio::main]` multi-threaded runtime (worker
  threads ≈ CPU count, capped by Docker). Under concurrent produce load, many
  partition drivers fsync at once and occupy every worker thread. While all
  workers are blocked in `fsync`, nothing else on the runtime runs: heartbeat /
  election timers fire late, inbound `AppendEntries` gRPC handlers are not
  scheduled, and replication acks are not processed.
- Result: commit index never advances (a stable leader committed 0 records in
  17s under load), and followers miss heartbeats past the 150–300 ms election
  timeout and start spurious elections → leadership churn → `NoLeader`.
- Confirmation: a light sequential run (concurrency 1, batch 1) passes but at
  only ~258 records/s — fully fsync-bound — while consume runs at ~226,000/s.

## Goal

Stop blocking the async runtime on `fsync`, and amortise `fsync` cost across
concurrently-queued appends (group commit), **without weakening durability**: a
produce is acknowledged only once its entry is `fsync`ed to disk on a majority
of replicas (each of which has itself `fsync`ed).

## Glossary

- **Durable_Index** — the highest log index a replica has `fsync`ed to its own
  stable storage. Distinct from `last_index` (highest appended, possibly still
  buffered).
- **Group_Commit** — coalescing the `fsync` for several buffered appends into a
  single force.
- **Offloaded_Fsync** — performing the `fsync` so it does not block a `tokio`
  worker thread.

## Requirements

### Requirement 1 — Durability is preserved end to end

**User Story:** As an operator, I want an acknowledged produce to mean the data
is on disk on a majority, so an acknowledged write is never lost to a crash of a
minority.

#### Acceptance Criteria

1. WHEN a producer receives `Ok(offset)` for a record or batch THEN that entry
   SHALL have been `fsync`ed on a strict majority of the partition's replicas
   (the leader counted only if it has `fsync`ed it locally).
2. WHEN a follower acknowledges an `AppendEntries` THEN the acknowledged
   `match_index` SHALL NOT exceed that follower's Durable_Index.
3. WHEN a leader advances its commit index to `n` THEN `n` SHALL be `<=` the
   leader's own Durable_Index AND `<= match_index` on a majority of peers.
4. WHEN a local `fsync` fails THEN the replica SHALL NOT report the affected
   entries as durable, SHALL NOT acknowledge or count them toward commit, and
   SHALL surface the failure (no silent data loss).
5. The crash-recovery guarantees of the existing durable WAL (recover the
   `fsync`ed prefix, drop the torn tail) SHALL be unchanged.

### Requirement 2 — `fsync` never blocks the async runtime

#### Acceptance Criteria

1. WHEN a partition driver forces its log to disk THEN the `fsync` SHALL run
   such that other `tokio` tasks on the runtime continue to be scheduled
   (Offloaded_Fsync).
2. WHEN one partition is forcing to disk THEN heartbeat/election timers and
   inbound RPC handlers for other partitions SHALL still be serviced.
3. The per-partition single-writer model SHALL be preserved: at most one
   in-flight mutation of a given partition's consensus + log state at a time
   (no new locking of consensus state).

### Requirement 3 — Group commit amortises `fsync`

#### Acceptance Criteria

1. WHEN multiple produce/replication appends for one partition are queued
   together THEN the driver SHALL append them all (buffered) and force them with
   a single `fsync` (Group_Commit).
2. A `flush` SHALL make durable **every** segment written since the last
   durable point, not only the active segment, and SHALL advance the
   Durable_Index to the highest appended index it forced.
3. Group commit SHALL NOT change offset assignment, ordering, or commit results
   versus the per-append path; only the timing of the force changes.

### Requirement 4 — Behaviour is verifiable

#### Acceptance Criteria

1. The reproducing bench command SHALL complete with `PASSED` and a non-trivial
   produce throughput.
2. Property tests (proptest, ≥100 iterations, in each crate's `tests/`) SHALL
   assert the durable-gating invariants (Requirement 1.2, 1.3) and the
   group-commit equivalence (Requirement 3.3).
3. The existing workspace test suite SHALL remain green, and the code SHALL stay
   `rustfmt`-clean and `clippy -D warnings`-clean.
