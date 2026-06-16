# Implementation Plan

## Overview

This plan wires the already-merged durable WAL (`vela_log::DurableWal`) into the
running cluster and makes log durability a **per-topic, client-selectable**
property. No new storage internals are written: the work is composition,
configuration, and two consensus-safety additions (durable Raft hard state and a
commit-index recovery handoff).

Work proceeds **inside-out**, so each task lands on a green workspace and the
crate dependency direction (`server → core → raft → log`) is never violated:

1. **`vela-log`** grows the additive `HardState` seam (unblocks `vela-raft`).
2. **`vela-raft`** persists hard state before emit and gains `recover`.
3. **`vela-proto`** adds the `LogBackend` enum (independent; parallelizable).
4. **`vela-core`** adds `PartitionLog`, the backend model, replica recovery, and
   the durable metadata group (needs the log seam + raft + proto).
5. **`vela-server`** adds `data_dir` config, path derivation, backend selection,
   and durable bootstrap ordering (needs core).
6. **Docker + quality gate** wire the named volume and run the final workspace
   gate plus integration tests.

Each task is test-inclusive. Property-based tests realize the design's
Correctness Properties 1–14; example, schema, and integration tests cover the
non-PBT criteria.

## Tasks

- [x] 1. vela-log: add the `HardState` type and additive `LogStorage` hard-state seam
  - Add a `HardState { current_term: u64, voted_for: Option<u64> }` value type in `vela-log` (derive `Debug, Clone, Copy, PartialEq, Eq, Default`) and re-export it from the crate root.
  - Add two additive, default-implemented methods to the `LogStorage` trait: `persist_hard_state(&mut self, HardState) -> Result<(), LogError>` defaulting to `Ok(())` (no-op) and `hard_state(&self) -> Option<HardState>` defaulting to `None`, mirroring the existing `flush` precedent.
  - Confirm `InMemoryLog` inherits both defaults so its `current_term`/`voted_for` stay volatile; keep the existing `InMemoryLog` and `DurableWal` test suites green.
  - _Requirements: 9.3, 10.1, 10.2_

- [x] 2. vela-log: persist hard state in the durable WAL manifest
  - Extend `ManifestState` with `hs_current_term: u64` and `hs_voted_for: Option<u64>`; grow the fixed-size slot codec by the term plus a tagged optional id, bump the slot `version`, and extend the CRC to cover the new fields while preserving the double-buffered, torn-slot-fallback discipline.
  - Implement `DurableWal::persist_hard_state` by writing a new manifest slot (reusing the alternate-slot + fsync path) and returning only after the fsync, giving the persist-before-return guarantee.
  - Implement `DurableWal::hard_state()` to return the recovered `(hs_current_term, hs_voted_for)` from the best slot at open; a fresh log reports `HardState::default()` (term 0, no vote).
  - _Requirements: 9.1, 9.2, 10.1, 10.2_

  - [x] 2.1 Unit + reopen tests for durable hard-state persistence
    - Slot-codec round-trip with the new fields, version bump, CRC, and torn-newest-slot fallback to the prior intact slot.
    - Persist a `(term, voted_for)`, drop and reopen the WAL, and assert `hard_state()` restores the exact value; assert a fresh log returns `HardState::default()`.
    - _Requirements: 9.1, 9.2, 10.1, 10.2_

  - [x] 2.2 Property test: durable hard-state round-trip
    - **Property 10 (log-level round-trip portion):** for any sequence of persisted `(term, voted_for)` values, the last persisted value is restored byte-for-byte after reopen.
    - _Requirements: 10.3_

- [x] 3. vela-raft: add `PersistError` and the `RaftOutput.persist_error` field
  - Add a `PersistError { op: &'static str }` type (`"grant_vote" | "adopt_term" | "start_election"`) and a `persist_error: Option<PersistError>` field to `RaftOutput`.
  - Update the simulation harness (`sim.rs`) and every existing `step`/`RaftOutput` call site and test that constructs or destructures `RaftOutput` to account for the new field (additive default `None`).
  - _Requirements: 9.4_

- [x] 4. vela-raft: persist Raft hard state before emit at the three mutation points
  - In `handle_request_vote`: when granting, call `persist_hard_state({ rv.term, Some(rv.candidate_id.0) })` first; on `Ok` set term/vote, reset the election timer, and push the `vote_granted: true` reply; on `Err` leave term/vote unchanged, push no grant, and set `out.persist_error`.
  - In `step_down` / higher-term adoption: persist `{ msg_term, None }` first; on `Ok` adopt and continue; on `Err` abort, emit nothing term-dependent, and set `out.persist_error`.
  - In `start_election`: persist `{ current_term + 1, Some(self.id.0) }` first; on `Ok` bump term, self-vote, and broadcast `RequestVote`; on `Err` stay a follower, broadcast nothing, set `out.persist_error`, and re-arm the election timer so a later attempt retries.
  - Keep the volatile `InMemoryLog` path byte-for-byte identical via the default no-op persist.
  - _Requirements: 9.1, 9.2, 9.3, 9.4_

  - [x] 4.1 Property test: persist-before-emit and suppress-on-failure
    - **Property 9:** drive `RaftNode<TestLog>` (an in-test `LogStorage` whose `persist_hard_state` can be made to fail) over random vote/term/election inputs; on success the dependent message is emitted and in-memory state reflects the persisted value; on failure term/vote are unchanged, no dependent message is emitted, and `persist_error` is set.
    - _Requirements: 9.1, 9.2, 9.4_

- [x] 5. vela-raft: add `RaftNode::recover` and `log()` accessor; keep `new` as a shim
  - Add `recover(id, peers, log)` that restores `current_term`/`voted_for` from `log.hard_state()` and initializes `commit_index = last_applied = log.commit_index()`, leaving volatile leader state empty.
  - Retain `new(id, peers, log)` as a thin delegation to `recover` over an empty log (term 0, no vote, no commit) so all existing call sites and tests compile unchanged.
  - Add a `log(&self) -> &S` accessor needed by core recovery to read the committed prefix.
  - Add a unit test asserting `recover` initializes `commit_index`/`last_applied` from the injected log's recovered commit index.
  - _Requirements: 10.1, 10.2, 11.1_

  - [x] 5.1 Property test: restart restores exact pre-restart hard state
    - **Property 10:** for any sequence of vote grants and term adoptions persisted to a `TestLog`, `recover` over the same log restores `current_term` and `voted_for` equal to the values held immediately before the simulated restart.
    - _Requirements: 10.1, 10.2, 10.3_

  - [x] 5.2 Property test: no second vote in the restored term
    - **Property 11:** a replica recovered with `voted_for = Some(C)` in persisted term `T` does not grant a vote to any candidate `C' != C` at term `T`.
    - _Requirements: 10.4_

  - [x] 5.3 Property test: no term regression after restart
    - **Property 12:** the recovered `current_term` equals the maximum persisted term, and a later message carrying a lower term does not lower it.
    - _Requirements: 10.5_

- [x] 6. vela-proto: add the `LogBackend` enum and carry it on the wire
  - Add a proto3 `LogBackend` enum (`LOG_BACKEND_UNSPECIFIED = 0`, `LOG_BACKEND_DURABLE = 1`, `LOG_BACKEND_IN_MEMORY = 2`) and a `log_backend` field to `CreateTopicRequest`, `TopicInfo`, and `CreateTopicCommand`; regenerate the tonic/prost types via `build.rs`.
  - _Requirements: 1.1, 2.1, 2.3, 2.4_

  - [x] 6.1 Schema test for the backend wire surface
    - Extend `vela-proto/tests/proto_surface.rs` to assert `log_backend` is present on `CreateTopicRequest`, `TopicInfo`, and `CreateTopicCommand`, and that `LogBackend` has the zero-valued `LOG_BACKEND_UNSPECIFIED` sentinel.
    - _Requirements: 2.1, 2.4_

- [x] 7. vela-core: add the `PartitionLog` enum implementing `LogStorage` by dispatch
  - Create `vela-core/src/partition_log.rs` with `enum PartitionLog { Durable(DurableWal), InMemory(InMemoryLog) }` and a `dispatch!` macro giving static per-variant dispatch.
  - Implement `LogStorage` for `PartitionLog` for every trait method (`append`, `append_entries`, `read`, `entry`, `last_index`, `term_at`, `commit_index`, `commit`, `revert`, `snapshot`, `flush`, `persist_hard_state`, `hard_state`), returning each backend's result unchanged.
  - Re-export `PartitionLog`, plus `WalConfig` and `SyncPolicy`, from the `vela-core` crate root so the server can build configs without a direct `vela-log` edge.
  - _Requirements: 4.1, 4.2, 4.3_

  - [x] 7.1 Property test: in-memory `PartitionLog` equals `InMemoryLog`
    - **Property 1:** for any sequence of `LogStorage` operations, `PartitionLog::InMemory(InMemoryLog)` returns per-operation results and observable state (`last_index`, `commit_index`, `term_at`, `entry`, `read`, `snapshot`) equal to a bare `InMemoryLog`.
    - _Requirements: 4.3, 4.4, 13.1_

- [x] 8. vela-core: add `LogBackend` to the domain model and thread it through metadata
  - Add `enum LogBackend { Durable, InMemory }` (`#[default] Durable`) to `model.rs`; add `Topic.backend: LogBackend` and `backend` to `ClusterCommand::CreateTopic`.
  - Update `ClusterMetadata::create_topic` to take a `backend` and store it on the `Topic`, and update `apply_command` for `CreateTopic` to copy the command's backend onto the inserted `Topic` so every node records the same backend; ensure no command ever mutates a recorded backend.
  - _Requirements: 3.1, 3.2_

  - [x] 8.1 Property test: backend round-trips through the replicated command
    - **Property 2:** for any backend value and starting `ClusterMetadata`, building a `CreateTopic` command and applying it via `apply_command` on two independent views records, on both, exactly one backend equal to the command's backend (and equal after a metadata replay).
    - _Requirements: 2.3, 3.1, 3.2, 18.3_

  - [x] 8.2 Property test: backend is immutable after creation
    - **Property 3:** for any topic created with a given backend and any subsequent command sequence (other-topic deletes, availability changes, other-topic creates), the topic's recorded backend stays equal to its creation backend.
    - _Requirements: 3.3_

  - [x] 8.3 Property test: backend selection picks the recorded variant
    - **Property 4:** the spawn-selection helper maps a topic recorded `Durable` to the `Durable` variant and a topic recorded `In_Memory` to the `InMemory` variant.
    - _Requirements: 3.4, 5.1, 5.4_

- [x] 9. vela-core: inject the log into `PartitionReplica` and add recovery
  - Change `PartitionReplica` to hold `RaftNode<PartitionLog>`; add `with_log(node_id, peers, log)` (fresh replica over an injected log) and update the `raft()` accessor type; retain the old `new(node_id, peers)` as a shim building `PartitionLog::InMemory(InMemoryLog::new())`.
  - Add `PartitionReplica::recover(node_id, peers, log)` that builds the raft node via `RaftNode::recover` then re-applies the committed prefix `read(0, commit_index)` to a fresh `StateMachine` exactly once in ascending order so committed records regain their offsets.
  - Add `RaftGroupFleet::create_group_with_log` and `create_recovered_group` (both reject duplicate keys), and re-express `create_group` in terms of `create_group_with_log` with an in-memory log.
  - _Requirements: 5.2, 11.1, 11.2, 11.3, 11.4, 13.2_

  - [x] 9.1 Property test: restart preserves committed records at identical offsets
    - **Property 13:** commit random record sequences on a `PartitionReplica` over `PartitionLog::Durable` (driven against the `vela-log` in-memory fault filesystem), capture `(offset, value)`, drop and reopen, and assert identical records/offsets, recovered `commit_index` equal to the log's, exactly-once ascending re-application, and a non-regressing high-water offset.
    - _Requirements: 11.1, 11.2, 11.3, 14.1, 14.3_

- [x] 10. vela-core: add the durable, recovering `MetadataController`
  - Add `MetadataController::recover_durable(node_id, peers, meta_path)` that opens (or creates) the durable `__meta/0` group with `SyncPolicy::Always`, creates it via `create_recovered_group` (restoring hard state + commit index), and rebuilds `ClusterMetadata` by re-applying every committed `Cluster` command in the recovered log via `apply_command` in ascending index order.
  - Add a `from_parts(metadata, fleet)` constructor and expose the recovered `metadata()` view (including each topic's backend) for the server to install.
  - _Requirements: 16.1, 16.6, 17.1, 17.2, 17.3, 17.4_

  - [x] 10.1 Property test: metadata recovery rebuilds the identical catalogue
    - **Property 14:** for any sequence of committed `ClusterCommand`s on the durable metadata group, reopening and re-applying the committed prefix rebuilds a `ClusterMetadata` equal to the pre-restart view, including each topic's recorded backend.
    - _Requirements: 17.4, 18.1, 18.3_

- [x] 11. vela-server: add `data_dir` configuration with fail-fast
  - Add `data_dir: Option<String>` to `CliArgs` with `#[arg(long, env = "VELA_DATA_DIR")]`; add `data_dir: PathBuf` to `Config`; in `from_cli` use the existing `require(...)` to yield `ConfigError::MissingRequired("data_dir")` when absent, and thread the validated `data_dir` onto `NodeShared`.
  - Add unit tests: `data_dir` is read from `VELA_DATA_DIR`; an absent value yields `ConfigError::MissingRequired("data_dir")` driving the existing structured-error + non-zero-exit path.
  - _Requirements: 6.1, 6.2, 6.3_

- [x] 12. vela-server: implement path derivation and topic-name sanitization
  - Implement `safe(topic)`: pass through `[A-Za-z0-9-]` bytes literally and escape every other byte (including `_`) as `_` followed by exactly two upper-case hex digits, so the output uses only `[A-Za-z0-9-_]` and can never contain the substring `__`.
  - Implement `partition_data_path(data_dir, topic, partition) = data_dir / safe(topic) / partition` and `metadata_data_path(data_dir) = data_dir / "__meta" / "0"`.
  - Add a unit test asserting the reserved literal `__meta` is itself a Safe_Path_Component.
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 16.2, 16.3_

  - [x] 12.1 Property test: path derivation is injective, stable, and rooted
    - **Property 6:** for any two distinct `(topic, partition)` pairs the derived paths differ; for any single pair two derivations are identical; and every derived path is beneath the configured data directory.
    - _Requirements: 6.3, 7.1, 7.2, 7.4_

  - [x] 12.2 Property test: the derived topic component is always safe
    - **Property 7:** for any topic-name byte string (including bytes outside the safe set), `safe(topic)` contains only characters drawn from `[A-Za-z0-9-_]`.
    - _Requirements: 7.3_

  - [x] 12.3 Property test: client paths never collide with the reserved metadata path
    - **Property 8:** for any topic name and partition index, the derived client path differs from `metadata_data_path`, because `safe(topic)` never equals and never contains the `__` prefix of `__meta`.
    - _Requirements: 16.4, 16.5_

- [x] 13. vela-server: map the wire backend in the create-topic service path
  - In `VelaClientService::create_topic`, map an unspecified wire `log_backend` to `Durable`, decode `LOG_BACKEND_DURABLE`/`LOG_BACKEND_IN_MEMORY` to the domain `LogBackend`, and reject any other integer with a new `CoreError::InvalidLogBackend` (mapped to `ErrorCode::VALIDATION`), creating no topic.
  - Carry the decoded backend into `ClusterCommand::CreateTopic` so it is replicated.
  - Add a unit test for the unspecified-to-`Durable` mapping.
  - _Requirements: 2.2, 2.5_

  - [x] 13.1 Property test: out-of-range backend is rejected with no side effects
    - **Property 5:** for any integer that is neither the unspecified sentinel nor a defined backend value, the wire-to-domain decoder yields a validation error and the cluster metadata is left unchanged (no topic created).
    - _Requirements: 2.5_

- [x] 14. vela-server: select and construct the backend in `spawn_partition`
  - Add `data_dir: PathBuf` to `NodeShared`; in `spawn_partition`, read the topic's backend from metadata (default `Durable` when absent).
  - For `InMemory`, build `PartitionLog::InMemory(InMemoryLog::new())` and `PartitionReplica::with_log`, creating no path and writing no files.
  - For `Durable`, derive `partition_data_path`, open `DurableWal::open(WalConfig::new(path).with_sync_policy(SyncPolicy::Always))`, and on success build `PartitionLog::Durable(wal)` and `PartitionReplica::recover`; on open failure log a structured `tracing::error!` with `topic` and `partition`, return without registering a driver (no in-memory fallback), and continue hosting the other partitions.
  - Add unit tests: durable backend rooted at the derived path; in-memory topics write no files; an open failure leaves the replica unstarted with a captured structured error while others continue; the `Always` policy is used at the construction site.
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 8.1, 8.2, 8.3, 12.1, 12.2, 13.3_

- [x] 15. vela-server: make `NodeShared::new` fallible and order the durable bootstrap
  - Change `NodeShared::new` to return `Result<Arc<Self>, StartupError>`; open and recover the durable `__meta` group via `MetadataController::recover_durable(self_raft_id, meta_peers, metadata_data_path(&data_dir))` (Always) **before** any client partition is spawned, returning `StartupError` (non-zero exit via `serve`) when the metadata log cannot be opened.
  - Install the recovered `ClusterMetadata`, then iterate the recovered topics and call `spawn_partition` for each local replica so durable topics reopen their existing segments and in-memory topics start empty.
  - Add an integration test: a full node cold-restart on a reattached data directory recovers the catalogue (including backends), reopens durable topics on existing segments, and starts in-memory topics empty.
  - _Requirements: 11.4, 13.2, 16.1, 16.6, 17.1, 17.2, 17.3, 17.4, 18.1, 18.2, 18.3_

- [x] 16. vela-client / vela-ctl: backend creation and description support
  - Add an optional `backend` (default `Durable`) to `AdminClient::create_topic`, validate it accepts exactly the two values client-side, send it on `CreateTopicRequest`, and surface `TopicInfo.log_backend` from `describe_topic`.
  - Add a `--backend durable|in-memory` flag (default `durable`) to the `vela-ctl create` command.
  - Add unit tests: default is `Durable`; a specified backend is sent; an invalid value is rejected before sending; `describe_topic` reports the backend.
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [x] 17. Docker: wire the per-node named volume and data directory
  - Update `docker-compose.yml` to set `VELA_DATA_DIR` for every node service and mount a per-node named volume at each node's data directory.
  - Add an integration test (or compose-inspection test): assert the generated compose sets `VELA_DATA_DIR` and a per-node volume for each service, and exercise a volume round-trip — produce to a durable topic, restart the container with the volume reattached, and consume the same records at the same offsets.
  - _Requirements: 15.1, 15.2, 15.3_

- [x] 18. Workspace integration tests and quality gate
  - Add a `vela-server` integration test for a single-node durable driver restart that resumes serving produce and consume for a durable partition and returns previously committed records at their original offsets.
  - Confirm the inward dependency direction is unchanged (no new `vela-server → vela-log` edge; core re-exports used).
  - Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` at the workspace root; ensure zero failures.
  - _Requirements: 14.1, 14.2, 14.3_

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1", "6", "11", "12"] },
    { "wave": 2, "tasks": ["2", "3"] },
    { "wave": 3, "tasks": ["4", "7", "8"] },
    { "wave": 4, "tasks": ["5", "13", "16"] },
    { "wave": 5, "tasks": ["9"] },
    { "wave": 6, "tasks": ["10", "14"] },
    { "wave": 7, "tasks": ["15"] },
    { "wave": 8, "tasks": ["17"] },
    { "wave": 9, "tasks": ["18"] }
  ]
}
```

```
1 (vela-log: HardState seam) ─────────────┐
├── 2 (vela-log: durable manifest persist) │
│    └── 7 (vela-core: PartitionLog) ──────┤
├── 3 (vela-raft: RaftOutput.persist_error)│
│    └── 4 (vela-raft: persist-before-emit)│
│         └── 5 (vela-raft: recover/new) ──┤
6 (vela-proto: LogBackend) ────────────────┤
│    └── 13 (vela-server: wire backend map)│
│    └── 16 (vela-client/ctl: backend)     │
8 (vela-core: domain LogBackend) ──────────┤
11 (vela-server: data_dir config) ─────────┤
12 (vela-server: path derivation) ─────────┤
                                           ├── 9 (vela-core: replica inject + recover)
                                           │    ├── 10 (vela-core: durable __meta recover)
                                           │    └── 14 (vela-server: spawn_partition select)
                                           │         └── 15 (vela-server: fallible bootstrap order)
                                           │              └── 17 (docker volume round-trip)
                                           │                   └── 18 (integration + quality gate)
```

- Task 1 (the `vela-log` hard-state seam) unblocks both `vela-raft` and
  `vela-core`; task 6 (`vela-proto`), task 11 (config), and task 12 (path
  derivation) are independent and run in the first wave alongside it.
- `vela-raft` edits a single file, so tasks 3 → 4 → 5 are sequenced across waves
  to avoid write conflicts.
- `vela-core` replica recovery (9) needs both `RaftNode::recover` (5) and
  `PartitionLog` (7); the durable metadata group (10) and server spawn (14) then
  build on the recovering replica and fleet.
- The fallible bootstrap (15) wires the metadata recovery (10) ahead of partition
  spawn (14); docker (17) and the final quality gate (18) come last.

## Notes

- Tasks are ordered inside-out so every task lands on a green workspace; the
  inward dependency direction `server → core → raft → log` is preserved and no
  new crate edge is introduced (`vela-core` already depends on `vela-log`).
- Property-based tests realize the design's Correctness Properties 1–14, each
  tagged `Feature: per-topic-log-durability, Property {n}` and run at the
  project's `proptest` minimum of 100 iterations. They sit close to the code they
  validate: Property 1 with `PartitionLog` (7.1), Properties 2–4 with the
  metadata model (8.1–8.3), Property 5 with the wire decoder (13.1), Properties
  6–8 with path derivation (12.1–12.3), Properties 9–12 with `RaftNode` (4.1,
  5.1–5.3), Property 13 with replica recovery (9.1), and Property 14 with metadata
  recovery (10.1).
- The volatile `InMemoryLog` path is preserved byte-for-byte: it inherits the
  default no-op `persist_hard_state`/`hard_state`, and `RaftNode::new` is retained
  as a `recover`-over-empty-log shim so existing call sites and tests are
  unaffected.
- Only the `Always` sync policy is consensus-safe; it is the sole policy
  constructed for durable client partitions (14) and the `__meta` group (15).
- The metadata group reuses the same `RaftNode<PartitionLog>` machinery as a
  durable client partition, so its hard-state and commit-recovery requirements
  (17.x) are the same mechanisms instantiated once for `Cluster` commands.
```
