# Implementation Plan: Deterministic Simulation Testing

## Overview

This plan builds the **DST_Harness** as a new `vela-sim` crate that composes
production `vela-core` / `vela-raft` / `vela-log` types into an in-process,
single-threaded, discrete-event **`SimRuntime`** (design Option A), drives them
through the `Clock` / `Transport` / `LogStorage` seams with seed-derived faults,
records a client `History`, and asserts the 22 correctness properties.

Sequencing follows the design's dependency direction and the review constraints:

1. **Two behavior-preserving production extractions come first and are verified
   non-regressive** before any harness code depends on them — (a) exposing the
   WAL `FileSystem` seam + fault filesystem in `vela-log` behind a non-default
   `sim` feature (production builds byte-for-byte unchanged with the feature
   off), and (b) lifting the pure `plan_reconcile` into `vela-core` so the
   harness and server share one planner.
2. Then the `vela-sim` scaffold and seed plumbing, scenario config + validation,
   the deterministic seams (clock → network → storage), cluster composition, the
   step loop, workload + history, the safety / Kafka-parity / liveness checkers,
   failure artifacts + shrinking, and finally CI wiring and the
   `GUARANTEES.md` Guarantee_Specification.

`vela-sim` depends **inward only** (`vela-core → vela-raft → vela-log`) and never
on `vela-server`. Implementation language is **Rust** (the design is specified in
Rust over the existing seams).

Optional test sub-tasks are marked `*`. The consensus-safety (Properties 11–15)
and Kafka-parity (Properties 16–20) property tests are the justification for the
whole feature and are written as explicit, traceable tasks. Requirement numbers
refer to `requirements.md`; properties P1–P22 are from `design.md`.

## Tasks

- [x] 1. Production extraction: expose the WAL `FileSystem` seam + fault filesystem in `vela-log` behind a `sim` feature
  - [x] 1.1 Add a non-default `sim` Cargo feature and a public `vela_log::sim` surface
    - Add a `sim = []` feature to `crates/vela-log/Cargo.toml` (off by default).
    - Behind `#[cfg(feature = "sim")]`, re-export the WAL `FileSystem` / `WalFile`
      traits (today `pub(crate)` in `wal/fs.rs`) and the in-memory fault
      filesystem (`MemFileSystem`, today `#[cfg(test)]`) as
      `vela_log::sim::{FileSystem, WalFile, FaultFileSystem}`, and expose
      `DurableWal::open_with` / `open_with_clock` constructors that accept an
      injected `FileSystem`.
    - Production code paths MUST NOT change: with the feature off, no new public
      item is exposed and no `#[cfg(test)]`/`pub(crate)` item changes visibility.
    - _Requirements: 3.2, 7.1_

  - [x] 1.2 Add a crash-durability boundary and storage-fault arming to `FaultFileSystem`
    - Extend the fault filesystem with a `crash()` that, per file, drops the
      un-fsynced tail (truncate each file to its last-fsynced length), modeling
      loss of unsynced writes; retain exactly bytes forced to stable storage.
    - Surface the existing torn-tail (`tear_last_write`/`truncate_file`) and
      I/O-error arming (`arm_fsync_failure_*` / `arm_read_failure_for` /
      `arm_write_failure_for`) on the `sim` public surface so the harness can arm
      them, returning errors through the `LogStorage`/`Result` contract.
    - _Requirements: 7.2, 7.3, 7.4_

  - [x] 1.3 Verify production builds are unchanged with the feature off
    - Assert `cargo build -p vela-log` (feature off) and the existing
      `vela-log` test suite still pass unchanged; add a test/CI note that the
      `sim` surface is gated and absent without the feature.
    - _Requirements: 3.2, 7.1_

  - [x] 1.4 Write unit tests for the crash durability boundary
    - With the `sim` feature on: fsynced bytes survive `crash()`, un-fsynced tail
      is discarded, and torn-tail / armed-I/O-error operations surface through the
      result type rather than panicking or silently succeeding.
    - _Requirements: 7.2, 7.3, 7.4_

- [x] 2. Production extraction: lift `plan_reconcile` into `vela-core`
  - [x] 2.1 Move the pure planner into `vela_core::reconcile`
    - Add `crates/vela-core/src/reconcile.rs` exposing
      `plan_reconcile(metadata, running, self_id) -> ReconcilePlan` (pure,
      deterministic, sorted output, excludes `__meta/0`), lifted verbatim from
      the server reconciler. Re-export it from `vela-core`'s `lib.rs`.
    - _Requirements: 3.2, 3.4_

  - [x] 2.2 Rewire the server reconciler to call the lifted planner
    - In `vela-server`'s reconciler, delete the local planner and call
      `vela_core::reconcile::plan_reconcile`; keep the `tokio` task wrapper and
      spawn/stop side-effects unchanged.
    - _Requirements: 3.2, 3.4_

  - [x] 2.3 Verify reconciler behavior is non-regressive
    - Confirm the existing `cross-node-metadata-propagation` reconciler diff
      property test (`Property 2: Reconciler diff correctness`) and unit tests
      still pass against the lifted planner unchanged.
    - _Requirements: 3.4_

- [x] 3. Scaffold the `vela-sim` crate and seed plumbing
  - [x] 3.1 Create `crates/vela-sim` and add it to the workspace
    - Create `crates/vela-sim` with a `Cargo.toml` depending on `vela-core`,
      `vela-raft`, and `vela-log` (with `features = ["sim"]`), `thiserror`, and a
      dev-dependency on `proptest`; expose a `sim` feature that enables
      `vela-log/sim`. Add `"crates/vela-sim"` to the workspace `members`.
      Create a `src/lib.rs` module skeleton (`scenario`, `scheduler`, `clock`,
      `network`, `storage`, `cluster`, `runtime`, `workload`, `history`,
      `checker`, `artifact`).
    - _Requirements: 3.1, 3.2_

  - [x] 3.2 Provide `SplitMix64` and `SeedStreams` per-subsystem RNG derivation
    - Re-export `SplitMix64` from `vela-raft::sim` (single implementation) and
      implement `SeedStreams::new(seed)` deriving six mutually-decorrelated
      streams (`election`, `network`, `storage`, `faults`, `workload`,
      `tiebreak`) by fixed XOR offsets, per design.
    - _Requirements: 1.1, 1.2_

  - [x] 3.3 Write a unit test for RNG stream independence
    - Distinct streams produce decorrelated sequences and identical `seed`
      reproduces identical sequences per stream.
    - _Requirements: 1.1, 1.2_

- [x] 4. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Scenario parameters, defaults, and validation
  - [x] 5.1 Implement `ScenarioParameters`, `Budget`, `RunConfig` with defaults and validation
    - Define `ScenarioParameters` (node_count, replication_factor,
      partition_count, `FaultIntensities`, workload_size, `Budget`), `Budget`
      (max_events, max_virtual), and `RunConfig { seed, params }`. Apply
      documented defaults for unspecified fields. Add `validate()` returning a
      typed `ScenarioError` (no panic) that accepts iff `replication_factor` is
      in `1..=node_count` and `partition_count >= 1` (so RF == node_count is
      accepted; RF > node_count or partition_count < 1 is rejected before any
      run).
    - _Requirements: 15.1, 15.4, 15.5_

  - [x] 5.2 Write property test for scenario-parameter validation
    - **Property 22: Scenario-parameter validation**
    - **Validates: Requirements 15.5**

  - [x] 5.3 Write unit tests for documented defaults
    - Unspecified parameters resolve to their documented default values.
    - _Requirements: 15.4_

- [x] 6. Discrete-event scheduler and Virtual_Clock core
  - [x] 6.1 Implement `VirtualInstant`/`VirtualDuration`, the `Event` enum, and the `Scheduler`
    - Implement `VirtualInstant` as a logical `u64` (never derived from the wall
      clock), `Scheduled { at, tie_break, event }`, the `Event` enum
      (`TimerFire`, `MessageDeliver`, `ClientOp`, `FaultApply`, `FaultHeal`), and
      a min-ordered `BinaryHeap` scheduler with a monotonic `next_seq` and a
      seed-derived `tiebreak` ordering for equal instants. The step loop pops the
      earliest event, advances `now` forward only, and ends the run when the
      `Budget` (max events / max virtual time) is reached after processing the
      bounding event.
    - _Requirements: 4.1, 4.4, 4.6, 1.5_

  - [x] 6.2 Write property test for event ordering and bounded termination
    - **Property 2: Event ordering and bounded termination**
    - **Validates: Requirements 4.4, 4.6**

- [x] 7. SimClock (Clock seam) with jitter, heartbeat, generation, and skew
  - [x] 7.1 Implement `SimClock` over the scheduler
    - Implement the `Clock` seam: `set_active(node, group)` before stepping;
      `arm(kind, dur)` enqueues a `TimerFire` at `now + delay`, where `delay` is
      exact `dur` for `Heartbeat` and `dur + jitter` for `Election` with `jitter`
      drawn from the `election` stream within `[base, 2*base)` (150–300 ms). A
      `generation` counter supersedes re-armed timers (stale `TimerFire` dropped).
      Apply a per-node bounded affine `view(t) = offset + t * rate` for
      Clock_Skew when computing a skewed node's firing instant, keeping the global
      queue ordered by true `VirtualInstant`.
    - _Requirements: 4.2, 4.3, 4.5_

  - [x] 7.2 Write property test for virtual-time timer semantics
    - **Property 3: Virtual-time timer semantics**
    - **Validates: Requirements 4.2, 4.3, 4.5**

- [x] 8. Sim_Network (Transport seam) with deterministic fault injection
  - [x] 8.1 Implement the in-memory bus, per-`(node,group)` transport handles, latency, reorder, drop, and duplicate
    - Implement `SimNetwork` and per-`(node, group)` `SimTransport` handles
      (stamped with sender + group, like `GrpcTransport`). On `send`, consult the
      `network` stream and current config to apply base one-way latency (every
      delivered message), bounded reorder delay (when enabled), seed-derived drop
      (never delivered), and seed-derived duplication (one extra copy), enqueuing
      `MessageDeliver` events ordered by `(deliver_at, seq)`. Count
      dropped/duplicated messages for diagnostics.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.8_

  - [x] 8.2 Implement partition cut sets, asymmetric partitions, heal, and crashed-node cut
    - A directed cut set blocks delivery when `(from, to)` straddles a partition;
      asymmetric partitions block one direction only; heal restores delivery for
      messages sent at or after the heal while other configured faults still
      apply; a crashed node is cut in both directions until restart.
    - _Requirements: 5.5, 5.6, 5.7, 6.2_

  - [x] 8.3 Write property test for network fault behavior
    - **Property 4: Network faults behave exactly as configured**
    - **Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 6.2**

  - [x] 8.4 Write property test that all inter-node messages flow through the Sim_Network
    - **Property 7: All inter-node messages flow through the Sim_Network**
    - **Validates: Requirements 3.3**

- [x] 9. Sim_Storage (LogStorage seam) over `DurableWal` + `FaultFileSystem`
  - [x] 9.1 Implement `SimBackend` / `SimStorageHandle` with crash and restart-reopen
    - Implement Sim_Storage as the real `DurableWal<FaultFileSystem, SimWalClock>`
      (durable-topic fidelity) and an `InMemoryLog` variant (in-memory-topic
      parity), with `SimWalClock` reading logical time only. `SimStorageHandle`
      holds the shared backing `FaultFileSystem` so a crash drops the volatile WAL
      handle (calling `crash()` to discard un-fsynced tail) while bytes forced to
      stable storage survive, and a restart reopens the same disk for the real
      recovery path. Use `SyncPolicy::Always`.
    - _Requirements: 3.2, 7.1, 7.2, 7.6_

  - [x] 9.2 Wire seed-derived torn-tail and I/O-error storage-fault arming
    - Select which operation/node a Storage_Fault hits from the `storage` stream;
      arm torn-tail (recovered by discarding the torn tail to the last intact
      record) and I/O errors (surfaced through the `LogStorage` result / WAL
      fail-stop, never silently succeeding).
    - _Requirements: 7.3, 7.4, 7.5_

  - [x] 9.3 Write property test for storage model fidelity and the durability boundary
    - **Property 5: Storage model fidelity and durability boundary**
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.4**

  - [x] 9.4 Write edge-case tests for torn-tail and armed I/O errors
    - Explicitly armed torn-tail recovers to the last intact record; an armed
      I/O error surfaces through the result type at the seed-derived operation.
    - _Requirements: 7.3, 7.4_

- [x] 10. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Simulated_Cluster and Sim_Node composition over production types
  - [x] 11.1 Define `Topology` and `SimNode` over production `vela-core` types
    - Build `Topology` once from `ScenarioParameters` (node ids, replication
      factor, fixed Replica_Sets — never mutated). Define `SimNode` holding a
      `MetadataController` (`__meta/0`), a `fleet: HashMap<GroupKey,
      PartitionReplica>`, the served `ClusterMetadata` mirror, a `running` flag,
      and the per-replica `SimStorageHandle`s — all production types.
    - _Requirements: 3.1, 3.5_

  - [x] 11.2 Assemble `SimulatedCluster` and wire the seams + durable recovery
    - Own `Vec<SimNode>` indexed by node id; construct each replica's
      `MetadataController` via `recover_durable` over a Sim_Storage-backed WAL and
      hand each replica a `SimClock` / `SimTransport` / Sim_Storage handle.
    - _Requirements: 3.1, 3.2_

  - [x] 11.3 Implement topic create/delete via the metadata-commit-and-reconcile path
    - On a committed `CreateTopic`, apply to each node's served catalogue, run
      `vela_core::reconcile::plan_reconcile`, and start a `PartitionReplica` for
      every partition whose Replica_Set contains that node; on `DeleteTopic` stop
      them. Never start/stop `__meta/0`.
    - _Requirements: 3.4_

  - [x] 11.4 Implement Node_Crash and Node_Restart application
    - Crash clears `running`, drops volatile replica state and cuts the node on
      the network; restart reopens each backing disk, recovers `__meta/0` and each
      partition replica (term, vote, committed prefix, applied catalogue), then
      reconciles to start a replica for every recovered assigned partition.
      Support crashing/restarting any subset (up to a minority of voters) at
      seed-derived times.
    - _Requirements: 6.1, 6.3, 6.4, 6.5_

  - [x] 11.5 Write property test for crash/restart recovery round-trip
    - **Property 6: Crash/restart recovery round-trip**
    - **Validates: Requirements 3.5, 6.1, 6.3, 6.4**

  - [x] 11.6 Write property test for topic-create coordination
    - **Property 8: Topic-create coordination**
    - **Validates: Requirements 3.4**

- [x] 12. SimRuntime discrete-event step loop (dispatch)
  - [x] 12.1 Wire event dispatch to `replica.step` and follow-on effects
    - For each popped event: `set_active`, feed the appropriate `RaftInput` to the
      right replica via `replica.step(input, clock)`, dispatch `out.sends` through
      the Sim_Network (after the step), apply `out.committed` to the state machine
      / catalogue, run `reconcile` after a committed metadata change, resolve
      pending client operations when their target index commits, and apply
      `FaultApply` / `FaultHeal` events. One event is processed atomically before
      the next is selected.
    - _Requirements: 3.2, 3.3, 4.4_

  - [x] 12.2 Write a unit test for atomic single-event processing
    - A single step never spans more than one event; `out.sends` are enqueued and
      `out.committed` applied before the next event is selected.
    - _Requirements: 4.4_

- [x] 13. Seed-driven workload generator
  - [x] 13.1 Implement workload generation, routing, record shape, interleaving, and redirect following
    - Generate exactly `workload_size` `Client_Operation`s (create/delete/produce/
      consume) from the `workload` stream. Route keyed produces via the production
      `PartitionRouter` (FNV-1a) and keyless produces to a seed-derived partition;
      draw value length `0..=65_536` and key length `1..=256` (keyed) and their
      contents from the stream. Schedule `ClientOp` events interleaved with the
      Fault_Schedule (issued while crashes/restarts/partitions are in effect).
      Follow leader redirects up to 5 hops; record an unresolved-redirection or
      no-leader response as a valid response rather than a violation.
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

  - [x] 13.2 Write property test for workload generation invariants
    - **Property 9: Workload generation invariants**
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4, 8.7**

  - [x] 13.3 Write edge-case tests for redirect exhaustion and no-leader
    - 5 successive redirections without reaching a leader records an
      unresolved-redirection response; issuing with no leader records the
      no-leader error — neither is a property violation.
    - _Requirements: 8.5, 8.6_

- [x] 14. History recorder
  - [x] 14.1 Implement `History` / `RecordedOp` recording
    - Record per operation: type + arguments, invocation `VirtualInstant`,
      response `VirtualInstant`, and response. Successful produce records target
      topic/partition, value, and committed offset; successful consume records
      topic/partition, start offset, and ordered records; failures/redirections
      are recorded as the response rather than discarded.
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

  - [x] 14.2 Write property test for history completeness
    - **Property 10: History completeness**
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4**

- [x] 15. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Consistency_Checker — Raft safety
  - [x] 16.1 Implement the Raft safety checks with run-time instrumentation
    - With full observability of every replica (logs, terms, commit indices,
      applied entries), implement: Election Safety (detect, do not prevent, a
      same-term double leader per group), Log Matching, Leader Completeness, State
      Machine Safety, and commit-index monotonicity. A violation ends the run with
      a failing Outcome naming the property and the detection `VirtualInstant`.
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6_

  - [x] 16.2 Write property test for Election Safety
    - **Property 11: Election Safety (Raft §5.2)**
    - **Validates: Requirements 10.1**

  - [x] 16.3 Write property test for Log Matching
    - **Property 12: Log Matching (Raft §5.3)**
    - **Validates: Requirements 10.2**

  - [x] 16.4 Write property test for Leader Completeness
    - **Property 13: Leader Completeness (Raft §5.4)**
    - **Validates: Requirements 10.3**

  - [x] 16.5 Write property test for State Machine Safety
    - **Property 14: State Machine Safety (Raft §5.4.3)**
    - **Validates: Requirements 10.4**

  - [x] 16.6 Write property test for commit monotonicity
    - **Property 15: Commit monotonicity**
    - **Validates: Requirements 10.5**

  - [x] 16.7 Write a synthetic-violation meta-test
    - Inject a fault that forces a known safety violation and assert the run ends
      failing, naming the right property and detection instant.
    - _Requirements: 10.6_

- [x] 17. Consistency_Checker — Kafka parity and linearizability
  - [x] 17.1 Implement the durability, offset, consume, linearizability, and convergence checks
    - Implement: acknowledged-record durability (every Acknowledged_Record stays
      at its returned offset under any minority-failure sequence), offset
      integrity (contiguous from 0, strictly increasing, no gaps/dupes), consume
      read-validity (committed records only, ascending offset, no phantom reads),
      per-partition linearizability against the single-leader committed log
      (offsets respect real-time order of non-overlapping ops; consumes observe a
      prefix-range), and metadata catalogue convergence over sorted topic keys.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7_

  - [x] 17.2 Write property test for acknowledged-record durability
    - **Property 16: Acknowledged-record durability**
    - **Validates: Requirements 7.6, 11.1, 11.2**

  - [x] 17.3 Write property test for offset integrity
    - **Property 17: Offset integrity**
    - **Validates: Requirements 11.3**

  - [x] 17.4 Write property test for consume read-validity (no phantom reads)
    - **Property 18: Consume read-validity (no phantom reads)**
    - **Validates: Requirements 11.4, 11.5**

  - [x] 17.5 Write property test for per-partition linearizability
    - **Property 19: Per-partition linearizability**
    - **Validates: Requirements 11.6**

  - [x] 17.6 Write property test for metadata catalogue convergence
    - **Property 20: Metadata catalogue convergence**
    - **Validates: Requirements 11.7**

- [x] 18. Liveness_Checker
  - [x] 18.1 Implement favorable-condition tracking and bounded-progress checks
    - Track per-group "favorable since" markers (set when a majority is running +
      mutually reachable with no further faults, cleared on any new fault). Verify
      exactly one leader is elected, a subsequently produced record commits, and a
      subsequent topic create/delete commits, all within the bounded budget; at a
      quiescent healed state every lagging replica converges to the leader's
      committed log. Require no progress of a group lacking a reachable majority,
      and declare a Liveness violation only after the budget is exceeded.
    - _Requirements: 6.6, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_

  - [x] 18.2 Write property test for liveness under healed faults
    - **Property 21: Liveness under healed faults**
    - **Validates: Requirements 6.6, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6**

- [x] 19. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 20. Single-run orchestration (a run is a pure function of its Seed)
  - [x] 20.1 Implement `SimRuntime::run(RunConfig) -> Outcome`
    - Validate parameters, build the `SimulatedCluster`, seed the workload and
      Fault_Schedule onto the timeline, step the scheduler to the budget (honoring
      an optional `VELA_DST_MAX_EVENTS` env override), feed the checkers
      incrementally and run final passes, and return `Outcome` (`Passed` or
      `Failed { property, at, detail }`). The whole run is single-threaded and
      depends only on `(seed, params)`.
    - _Requirements: 1.3, 2.3, 3.1, 4.6_

  - [x] 20.2 Write property test for reproducibility
    - **Property 1: Reproducibility (a run is a pure function of its Seed)**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 2.2, 5.8, 7.5, 9.5**

- [x] 21. Failure artifacts, shrinking, and regression persistence
  - [x] 21.1 Implement `FailureArtifact` and CI-collectable output
    - On a failing Outcome, produce a `FailureArtifact` (seed, params, violated
      property, detection instant, replayable event trace, History, structured
      diagnostics naming group/term/replicas without a re-run) and write a run
      summary regardless of Outcome plus the full artifact to a CI-collectable
      path (e.g. `target/dst-artifacts/`).
    - _Requirements: 2.1, 2.3, 13.1, 13.2, 13.4, 13.5_

  - [x] 21.2 Wire `proptest` strategies, shrinking, and regression persistence
    - Generate structured `ScenarioParameters` + the 64-bit seed via `proptest`
      strategies so failures shrink toward a minimal counterexample reproducing
      the same property; rely on `proptest`'s `proptest-regressions/` persistence
      to replay failing seeds on subsequent runs (persisted only for failing
      runs).
    - _Requirements: 2.2, 2.4, 13.3_

  - [x] 21.3 Write example tests for artifact content
    - A failing run reports the seed/params, identifies the violated property and
      detection instant, and includes a replayable trace/History + diagnostics.
    - _Requirements: 2.1, 2.3, 13.1, 13.2, 13.5_

  - [x] 21.4 Write tests for regression replay and shrinking
    - A persisted failing seed re-executes to the same failing Outcome and same
      violated property; a found failure shrinks toward a smaller scenario.
    - _Requirements: 2.2, 2.4, 13.3_

- [x] 22. Scenario coverage presets and defaults
  - [x] 22.1 Add named scenario presets
    - Provide `ScenarioParameters` constructors covering leader election/failover,
      log replication/follower catch-up, network partition/heal, node crash/durable
      restart, and concurrent topic administration, with cluster size and
      replication factor of at least three.
    - _Requirements: 15.2, 15.3_

  - [x] 22.2 Write integration tests exercising each preset
    - One run per preset asserts the targeted behavior occurs (election/failover,
      catch-up, partition/heal, crash/restart, concurrent admin).
    - _Requirements: 15.2, 15.3_

  - [x] 22.3 Write a defaults coverage test
    - A run built from defaults (unspecified parameters) executes within budget.
    - _Requirements: 15.4_

- [x] 23. CI integration — `dst` job
  - [x] 23.1 Add the `dst` job to `.github/workflows/ci.yml`
    - Add a `dst` job running `cargo test -p vela-sim --features sim --locked`
      with `PROPTEST_CASES` and `VELA_DST_MAX_EVENTS` budget env vars, and an
      `actions/upload-artifact@v4` step with `if: failure()` uploading
      `crates/vela-sim/proptest-regressions/` and `target/dst-artifacts/`; the
      build fails on any run failure whether or not the upload succeeds.
    - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5, 14.6_

  - [x] 23.2 Write a test that the per-run event budget env override is honored
    - `VELA_DST_MAX_EVENTS` bounds the number of processed Events for a run.
    - _Requirements: 14.5_

- [x] 24. Guarantee_Specification and drift guard
  - [x] 24.1 Author `crates/vela-sim/GUARANTEES.md`
    - Enumerate Vela's intended durability, ordering, and availability guarantees
      as concrete testable statements; map each to the Safety_/Liveness_Property
      that checks it or to a documented gap; record Kafka-parity per guarantee and
      describe divergences; identify guarantees the current architecture cannot
      yet provide.
    - _Requirements: 16.1, 16.2, 16.3, 16.4_

  - [x] 24.2 Write a test asserting the guarantee mapping cannot drift
    - The mapping in `GUARANTEES.md` covers every property the suite defines (no
      property unmapped, no mapping naming a non-existent property).
    - _Requirements: 16.2_

- [x] 25. Final checkpoint
  - Ensure the whole workspace builds, `cargo fmt --check` and
    `cargo clippy --workspace --all-targets --all-features -- -D warnings` are
    clean, and all tests pass (including `cargo test -p vela-sim --features sim`).

## Notes

- Tasks marked `*` are optional test sub-tasks. The consensus-safety tests
  (16.2–16.6 → Properties 11–15) and the Kafka-parity tests (17.2–17.6 →
  Properties 16–20), plus reproducibility (20.2 → Property 1), are the core
  justification for this feature; treat them as the highest-value tests even
  though they are formally optional.
- Each correctness property is implemented by a **single** `proptest`-based test
  (min 100 cases) tagged `Feature: deterministic-simulation-testing, Property
  {n}: {text}`, living in `crates/vela-sim/tests/` grouped by family
  (`prop_reproducibility.rs`, `prop_raft_safety.rs`, `prop_kafka_parity.rs`,
  `prop_liveness.rs`, `prop_network_faults.rs`, `prop_recovery.rs`, plus workload/
  history/scenario files), with `#[cfg(test)]` unit modules beside pure
  components (scheduler ordering, RNG independence, scenario validation).
- The two production extractions (tasks 1, 2) are behavior-preserving and gated on
  non-regression checks (1.3, 2.3) before any harness code depends on them;
  `vela-log`'s `sim` feature is off by default so production builds are unchanged.
- `vela-sim` depends inward only (`vela-core → vela-raft → vela-log`) and never on
  `vela-server`; this is what structurally guarantees no wall clock, real network,
  or OS scheduler can leak into a run.
- Expected in-simulation outcomes (non-leader redirect, unresolved redirection,
  no-leader, surfaced `LogStorage` I/O error, no-progress without a majority) are
  recorded as valid responses, never property violations.

## Task Dependency Graph

Same-file/same-module tasks are placed in different waves to avoid edit
conflicts: the cluster-composition tasks (11.2 → 11.3 → 11.4) are serialized
because they share `SimulatedCluster`, and the step loop (12.1) lands after the
cluster exists. The production extractions (waves 0–1) and crate scaffold (wave
0) precede all harness work; the seams (clock/network/storage, waves 3–6) precede
cluster composition; checkers (waves 12–13) precede run orchestration (wave 14);
artifacts/CI/guarantees (waves 15–17) come last.

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1", "3.1"] },
    { "id": 1, "tasks": ["1.2", "2.2", "3.2"] },
    { "id": 2, "tasks": ["1.3", "1.4", "2.3", "3.3", "5.1"] },
    { "id": 3, "tasks": ["5.2", "5.3", "6.1"] },
    { "id": 4, "tasks": ["6.2", "7.1", "8.1", "9.1"] },
    { "id": 5, "tasks": ["7.2", "8.2", "9.2"] },
    { "id": 6, "tasks": ["8.3", "8.4", "9.3", "9.4", "11.1"] },
    { "id": 7, "tasks": ["11.2"] },
    { "id": 8, "tasks": ["11.3"] },
    { "id": 9, "tasks": ["11.4"] },
    { "id": 10, "tasks": ["12.1", "13.1", "14.1"] },
    { "id": 11, "tasks": ["11.5", "11.6", "12.2", "13.2", "13.3", "14.2"] },
    { "id": 12, "tasks": ["16.1", "17.1", "18.1"] },
    { "id": 13, "tasks": ["16.2", "16.3", "16.4", "16.5", "16.6", "16.7", "17.2", "17.3", "17.4", "17.5", "17.6", "18.2"] },
    { "id": 14, "tasks": ["20.1"] },
    { "id": 15, "tasks": ["20.2", "21.1", "22.1", "24.1"] },
    { "id": 16, "tasks": ["21.2", "23.1", "24.2"] },
    { "id": 17, "tasks": ["21.3", "21.4", "22.2", "22.3", "23.2"] }
  ]
}
```
