# Implementation Plan: ctl-client-routing-and-repl

## Overview

This plan turns the design into code bottom-up so the workspace stays green at
every step. It begins with the shared canonical partitioner in `vela-proto`
(resolving the `router.rs` duplication), then the pure client-core primitives
(`MetadataCache`, `RetryBudget`, `AttemptOutcome`/`classify`, `ClientError`
additions), then the generalized `ClientCore::dispatch` engine, then
`AdminClient` routing via `dispatch_admin`, then the additive `DescribeCluster`
wire + `vela-server` handler + client registry seeding, and finally the
`vela-ctl` Producer REPL and per-partition Consumer loop plus the new CLI flags.

The 20 correctness properties from the design are each implemented as a single
`proptest` (≥100 cases) following the existing `prop_*.rs` naming and the
doc-comment + `Feature: ctl-client-routing-and-repl, Property N` tag convention
(see `crates/vela-core/tests/prop_keyed_routing.rs`). Pure-function properties
(P1–P13) run without a server; loop properties (P14–P20) run on a paused virtual
clock against the scripted `Clock`/`LineSource`/`Signal` seams and the in-process
fake `VelaClientServer`.

Steering is respected throughout: Rust + tokio, tonic/prost on the wire,
`thiserror` library errors, traits at crate seams, clippy-clean with
`-D warnings`, no `unsafe`, and inward-only runtime crate dependencies. The one
cross-crate edge — Property 2's partitioner-agreement test — uses a **test-only
`dev-dependency`** from `vela-client` to `vela-core`, leaving the runtime
direction intact (Open Decision 3, default direction).

## Tasks

- [x] 1. Canonical partitioner in `vela-proto` and router delegation
  - [x] 1.1 Add the canonical partitioner module to `vela-proto`
    - Create `crates/vela-proto/src/partition.rs` with the pinned
      `FNV_OFFSET_BASIS`/`FNV_PRIME` constants, `fnv1a_64(bytes: &[u8]) -> u64`,
      and `partition_for_key(key: &[u8], partition_count: u32) -> Option<u32>`
      returning `None` when `partition_count == 0` (fail-fast, no modulo-by-zero)
    - Re-export the module from `crates/vela-proto/src/lib.rs` so both
      `vela-client` and `vela-core` can call the identical code
    - _Requirements: 5.1, 5.5, 1.9_

  - [x] 1.2 Write property tests for the canonical partitioner
    - Create `crates/vela-proto/tests/prop_canonical_partitioner.rs`
    - **Property 1: Canonical partitioner determinism and range** — same key + N≥1
      always resolves to the same index in `0..N`
    - **Property 3: Zero-partition routing fails fast** — `partition_for_key` with
      `N == 0` returns `None`, never panics or divides by zero
    - **Validates: Requirements 5.1, 5.3, 1.9**

  - [x] 1.3 Delegate `vela-core`'s `PartitionRouter` keyed branch to the canonical fn
    - In `crates/vela-core/src/router.rs`, replace the local `fnv1a_64`/FNV
      constants and the keyed `hash % count` branch with a call to
      `vela_proto::partition::partition_for_key`, keeping the per-topic keyless
      round-robin counter and the `PartitionIndex` return type unchanged
    - Keep all existing `#[cfg(test)] mod tests` cases green after the refactor
    - _Requirements: 5.5_

  - [x] 1.4 Delegate `vela-client`'s `PartitionRouter` and add fail-fast + keyless strategy
    - In `crates/vela-client/src/router.rs`, delegate the keyed branch to
      `vela_proto::partition::partition_for_key`; remove the `.max(1)` clamp
    - Change `resolve` to return `Result<u32, RouteError>` (new `thiserror` enum)
      so a zero `partition_count` is rejected rather than clamped
    - Add `KeylessStrategy { RoundRobin, Sticky { run_length: u32 } }` (default
      `RoundRobin`); implement `Sticky` as `(current_partition, remaining_in_run)`
      per topic behind the existing `Mutex`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.6, 1.9_

  - [x] 1.5 Write unit tests for the client router fail-fast and keyless strategies
    - In `crates/vela-client/src/router.rs` `#[cfg(test)] mod tests`: zero count →
      `Err(RouteError)`; single-partition → `0`; empty key treated as keyless;
      sticky assigns a run then rotates; round-robin order preserved
    - _Requirements: 5.2, 5.3, 5.6, 1.9_

  - [x] 1.6 Write property tests for client keyless routing
    - Create `crates/vela-client/tests/prop_keyless_routing.rs`
    - **Property 4: Keyed routing does not advance keyless position**
    - **Property 5: Round-robin keyless routing covers all partitions in order**
    - **Property 6: Sticky keyless routing batches then distributes evenly**
    - **Validates: Requirements 5.2, 5.4, 5.6**

  - [x] 1.7 Write the cross-implementation partitioner-agreement property test
    - Create `crates/vela-client/tests/prop_partitioner_agreement.rs`; add
      `vela-core` as a **`[dev-dependencies]`** entry of `vela-client` (test-only;
      runtime inward direction preserved — Open Decision 3)
    - **Property 2: Cross-implementation partitioner agreement** — the client
      `PartitionRouter`, the core `PartitionRouter`, and
      `vela_proto::partition::partition_for_key` all resolve a non-empty key to the
      same index for N≥1
    - **Validates: Requirements 5.5**

- [x] 2. Client-core primitives (pure, server-free)
  - [x] 2.1 Add the new `ClientError` variants
    - In `crates/vela-client/src/error.rs` add `TopicNotFound { topic }` and
      `NoPartitions { topic }` with `thiserror` messages per the design taxonomy
    - _Requirements: 1.4, 1.8, 8.5_

  - [x] 2.2 Implement the `MetadataCache` module
    - Create `crates/vela-client/src/metadata_cache.rs` with `TopicMeta
      { partition_count, leaders: Vec<Option<String>>, learned_at }`, and
      `MetadataCache { entries: Mutex<HashMap<String, TopicMeta>>, ttl }`
    - Implement `get_fresh(topic, now) -> Option<TopicMeta>` (None when absent or
      aged `>= ttl`), `put`, and `invalidate`; take time via an injected `Clock`
      so freshness is deterministic; default TTL 30s
    - Register the module in `crates/vela-client/src/lib.rs`
    - _Requirements: 1.3, 1.5, 1.6, 1.7_

  - [x] 2.3 Implement the `RetryBudget` module
    - Create `crates/vela-client/src/retry.rs` with `RetryBudget { total, base,
      cap }` (defaults 5s / 100ms / 2s), `backoff(attempt) -> Duration` =
      `min(base * 2^attempt, cap)`, and `may_retry(elapsed) -> bool` =
      `elapsed < total`
    - Register the module in `crates/vela-client/src/lib.rs`
    - _Requirements: 3.4, 3.5, 4.3, 4.4_

  - [x] 2.4 Implement `AttemptOutcome` and the pure `classify` function
    - In `crates/vela-client/src/core.rs` add `AttemptOutcome { Success,
      NotLeader { hint }, Transport, StaleRouting, Fatal }` and
      `classify(&ClientError) -> AttemptOutcome` per the design's mapping table
      (NotLeader hint via `not_leader_hint`; `Unavailable`/connection → Transport;
      `PARTITION_UNAVAILABLE`/`NoLeader` on cached topic → StaleRouting;
      validation/not-found/payload-too-large/config errors → Fatal)
    - Include `#[cfg(test)]` example asserts: one per `AttemptOutcome` branch,
      including exact `VelaError` codes for `Fatal`
    - _Requirements: 1.6, 3.2, 3.3, 3.6_

  - [x] 2.5 Write unit tests for `MetadataCache`
    - In `crates/vela-client/src/metadata_cache.rs`: hit within TTL, miss when
      absent, stale at exactly the TTL boundary, `invalidate` forces a miss
    - _Requirements: 1.3, 1.5, 1.6_

  - [x] 2.6 Write unit tests for `RetryBudget`
    - In `crates/vela-client/src/retry.rs`: first backoff is exactly 100ms,
      doubling, capped at 2s; `may_retry` false at exactly `total`
    - _Requirements: 3.4, 3.5_

  - [x] 2.7 Write the metadata-cache freshness property test
    - Create `crates/vela-client/tests/prop_metadata_cache.rs`
    - **Property 12: Metadata cache freshness and refresh idempotence** — fresh iff
      `t1 - t0 < TTL`; at most one `DescribeTopic` per TTL window
    - **Validates: Requirements 1.3, 1.5**

  - [x] 2.8 Write the retry-policy property tests (pure)
    - Create `crates/vela-client/tests/prop_retry_backoff.rs` and
      `crates/vela-client/tests/prop_retry_budget.rs`
    - **Property 10: Retry backoff bounds and monotonicity** — `min(100ms*2^n, 2s)`,
      non-decreasing, never exceeds cap (`prop_retry_backoff.rs`)
    - **Property 11: Retry budget total-time bound and termination** — only retries
      starting within `total`; loop terminates with no-leader-after-retries
      (`prop_retry_budget.rs`)
    - **Validates: Requirements 3.4, 3.5, 4.3, 4.4**

- [x] 3. Checkpoint - partitioner and primitives
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Generalized dispatch engine and producer/consumer wiring
  - [x] 4.1 Generalize `ClientCore::dispatch` and integrate the new primitives
    - In `crates/vela-client/src/core.rs` replace the `topic_partitions:
      Mutex<HashMap<String,u32>>` field with `metadata: MetadataCache`, and add a
      `RetryBudget` plus an injected `Clock`
    - Rework `dispatch` to drive the loop off `classify(...)`: re-resolve the
      leader on `NotLeader { hint }` (hint via registry else `FindLeader`) and on
      `Transport` (invalidate `LeaderCache` entry + `FindLeader`); `MetadataCache::
      invalidate(topic)` on `StaleRouting`; surface `Fatal`/`Success` immediately;
      wait `RetryBudget::backoff(attempt)` between attempts and stop when
      `may_retry(elapsed)` is false, returning `NoLeaderAfterRetries`
    - Update `partition_count`/leader learning to read/populate `MetadataCache`
      (re-seeding `LeaderCache` from learned leaders); map a missing topic to
      `ClientError::TopicNotFound`
    - _Requirements: 1.1, 1.2, 1.3, 1.5, 1.6, 2.1, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [x] 4.2 Add the producer zero-partition guard and metadata-refresh retry
    - In `crates/vela-client/src/producer.rs` map the router's `Err(RouteError)`
      (zero partition count) onto a metadata-refresh retry loop bounded by a
      discovery timeout, returning `ClientError::NoPartitions` on timeout; otherwise
      resolve via the canonical partitioner and dispatch as today
    - _Requirements: 1.8, 1.9, 4.1 (produce), 5.1, 5.3_

  - [x] 4.3 Map consumer stale-routing through the generalized engine
    - In `crates/vela-client/src/consumer.rs` ensure `consume` flows through the
      generalized `dispatch` so `StaleRouting`/`Transport`/`NotLeader` are handled;
      surface `NoLeader` (partition-unavailable) distinctly from transport failure
    - _Requirements: 2.3, 3.1, 3.2, 3.3, 10.1, 10.2, 10.3_

  - [x] 4.4 Write the dispatch re-resolution property test
    - Create `crates/vela-client/tests/prop_dispatch_reresolution.rs`, driven on a
      paused clock against the in-process fake `VelaClientServer`
    - **Property 8: Leader re-resolution on both NotLeader and transport failure** —
      the next attempt targets the re-resolved address, not the failed one
    - **Validates: Requirements 3.2, 3.3, 10.1, 10.2**

  - [x] 4.5 Write the non-retryable-error property test
    - Create `crates/vela-client/tests/prop_classify_fatal.rs`
    - **Property 9: Non-retryable application errors pass through** — `classify`
      yields `Fatal` and dispatch surfaces after exactly one attempt
    - **Validates: Requirements 3.6**

  - [x] 4.6 Write the stale-routing-refresh property test
    - Create `crates/vela-client/tests/prop_stale_routing.rs`
    - **Property 13: Stale-routing errors force a metadata refresh** — out-of-range
      partition or no-leader on a cached topic → `StaleRouting` → metadata
      invalidated → next attempt refreshes
    - **Validates: Requirements 1.6**

  - [x] 4.7 Write the leader-resolution fold property test
    - Create `crates/vela-client/tests/prop_leader_resolution.rs`
    - **Property 7: Leader resolution fold** — `resolve_leader` returns the first
      named leader; else `NoLeaderElected` if any reachable-but-leaderless; else
      `AllFailed`, keeping unavailable distinct from transport failure
    - **Validates: Requirements 2.2, 2.3**

- [x] 5. Route `AdminClient` through the engine
  - [x] 5.1 Add `dispatch_admin` and route create/delete; transport-only retry for read-only
    - In `crates/vela-client/src/core.rs` add `dispatch_admin` (sibling of
      `dispatch`) that targets a configured/bootstrap node, redirects to the hinted
      `Metadata_Leader` on `NotLeader`, re-resolves on `Transport`, and reuses
      `RetryBudget`/`classify`/`not_leader_hint`
    - In `crates/vela-client/src/admin.rs` route `create_topic`/`delete_topic`
      through `dispatch_admin`; make `list_topics`/`describe_topic` retry only on
      transport failure (no `NotLeader` redirection)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 12.3, 12.4, 12.5_

  - [x] 5.2 Write admin redirect / read-only unit tests
    - In `crates/vela-client/src/admin.rs` tests against the fake server: create/
      delete follow a `NotLeader` hint then succeed; transport failure re-resolves;
      list/describe do not redirect on `NotLeader` but retry on transport failure
    - _Requirements: 4.1, 4.2, 4.5, 4.6, 12.3_

- [x] 6. Checkpoint - routing engine and admin
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. `DescribeCluster` wire, server handler, and client seeding
  - [x] 7.1 Add the additive `DescribeCluster` RPC to the proto
    - In `crates/vela-proto/proto/vela.proto` add `DescribeClusterRequest {}`,
      `DescribeClusterResponse { repeated Member members = 1; uint64 epoch = 2; }`,
      and `rpc DescribeCluster(...)` on the `VelaClient` service (no existing
      message changes — backward compatible)
    - _Requirements: 12.6, 12.7, 12.8, 13.1_

  - [x] 7.2 Implement the server `DescribeCluster` handler
    - In `crates/vela-server/src/service.rs` add the handler building the reply
      from `MetadataController::metadata().members` (id + addr + availability) and
      the metadata `epoch`; reuse/add any `Member` mapping in
      `crates/vela-server/src/convert.rs` (no `vela-core` change needed)
    - _Requirements: 12.7, 12.8_

  - [x] 7.3 Seed `NodeRegistry` from the `Member_Address_Map`, with `id=url` fallback
    - In `crates/vela-client/src/core.rs` call `DescribeCluster` against a bootstrap
      node (lazily on first leader resolution) and seed `NodeRegistry` from the
      returned members as the **primary** source; keep `id=url` endpoints as the
      fallback; on a failed/empty `DescribeCluster`, proceed with the `id=url`
      registry (degraded but functional); unmapped resolved id → `UnknownNode`
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_

  - [x] 7.4 Write the server `DescribeCluster` integration test
    - Create `crates/vela-server/tests/describe_cluster.rs`: a membership set in
      `ClusterMetadata` returns each member's id + addr; verify an older call path
      with no members leaves existing client behavior unchanged
    - _Requirements: 12.6, 12.7, 12.8_

  - [x] 7.5 Write client registry-seeding unit tests
    - In `crates/vela-client/src/core.rs` tests against the fake server: map ids
      resolve; `id=url` fills gaps; empty members → fallback only; unmapped id →
      `UnknownNode`
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.6_

- [x] 8. Producer REPL (`vela-ctl`)
  - [x] 8.1 Add the deterministic seam traits
    - Create `crates/vela-ctl/src/seams.rs` with `Clock` (now/sleep), `LineSource`
      (`async fn next_line() -> io::Result<Option<String>>`), and `Signal`
      (`async fn interrupted()`), plus production impls over `tokio::time`,
      `BufReader<Stdin>::lines()`, and `tokio::signal::ctrl_c()`
    - Register the module in `crates/vela-ctl/src/main.rs`
    - _Requirements: 6.1, 7.1, 7.2_

  - [x] 8.2 Implement the `Produce_Repl`
    - Create `crates/vela-ctl/src/produce.rs` with `run_repl(producer, topic, key,
      lines, signal, out)`: print `> `, await a line, `produce` it, print the
      offset, loop; keyed sessions apply the key to every line; produce errors
      print and continue without terminating; EOF exits zero; use `tokio::select!`
      over `next_line()` and `interrupted()` so the prompt stays responsive to Ctrl+C
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2_

  - [x] 8.3 Write the Producer REPL property tests
    - Create `crates/vela-ctl/tests/prop_produce_repl.rs` (paused clock, scripted
      `LineSource`, fake transport)
    - **Property 14: REPL produces exactly one record per input line, in order**
    - **Property 15: Keyed REPL applies the key to every record**
    - **Property 16: REPL survives per-line produce errors** (attempts every line once)
    - **Validates: Requirements 6.3, 6.4, 6.5**

  - [x] 8.4 Write REPL terminal/interrupt unit tests
    - In `crates/vela-ctl/src/produce.rs` tests: prompt-then-EOF exits zero (6.1,
      6.6); a triggered `Signal` stops reading mid-session (7.1, 7.2)
    - _Requirements: 6.1, 6.6, 7.1, 7.2_

- [x] 9. Continuous consumer (`vela-ctl`)
  - [x] 9.1 Implement the `Consume_Loop` with per-partition tasks
    - Create `crates/vela-ctl/src/consume.rs`: discover partitions + leaders
      (`DescribeCluster` + `DescribeTopic`), spawn one independent poll task per
      partition reporting over an `mpsc` to a single printer task that prints
      `partition, offset, value`; each task owns its in-memory `Next_Offset`
    - Initialize `Next_Offset` from `Offset_Reset` (`latest` = `next_offset` from a
      bounded `Consume` probe — Open Decision 2 default; `earliest` = `0`); wait
      `Polling_Interval` (default 500ms) via the injected `Clock` on empty polls;
      on retryable poll errors invalidate/​re-resolve and keep polling; on
      `PartitionUnavailable` wait and retry resolution; cancel all tasks on Ctrl+C;
      single-partition mode runs only the supplied partition; zero-partition topic
      retries discovery until timeout then `NoPartitions`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 10.1, 10.2, 10.3, 10.4, 10.5, 11.1, 11.2_

  - [x] 9.2 Write the Consume_Loop property tests
    - Create `crates/vela-ctl/tests/prop_consume_loop.rs` (paused clock, fake
      transport scripting batches/late appends/stalls)
    - **Property 17: Per-partition offset monotonicity and no-gap delivery**
    - **Property 18: Eventual delivery of late records**
    - **Property 19: Partition polling isolation and coverage** (one stalled
      partition never starves the others)
    - **Property 20: Output renders partition, offset, and value**
    - **Validates: Requirements 8.2, 8.3, 9.1, 9.3, 9.4, 9.6, 10.4, 10.5**

  - [x] 9.3 Write Consume_Loop unit tests for edge cases
    - In `crates/vela-ctl/src/consume.rs` tests: single-partition selection (8.4);
      zero-partition discovery timeout → `NoPartitions` (8.5); `latest`/`earliest`
      start positions (8.6, 8.7); non-committing behavior (8.8); empty-poll interval
      wait (9.2); interrupt responsiveness while waiting (11.1, 11.2)
    - _Requirements: 8.4, 8.5, 8.6, 8.7, 8.8, 9.2, 11.1, 11.2_

- [x] 10. CLI surface additions and command wiring
  - [x] 10.1 Add the new CLI flags and value parsers
    - In `crates/vela-ctl/src/cli.rs` add `--keyless {round-robin|sticky}` to
      `Produce`; `--offset-reset {latest|earliest}` (default `latest`),
      `--poll-interval <ms>` (default 500), and make `--partition` optional on
      `Consume`; add a global `--metadata-ttl <secs>` (default 30); reject bad
      values at parse time following the `parse_backend` pattern
    - _Requirements: 8.4, 8.6, 8.7, 9.5, 1.7, 5.2, 5.6_

  - [x] 10.2 Wire `produce`/`consume` to the REPL/loop and extend error mapping
    - In `crates/vela-ctl/src/cli.rs` `run`: build the `MetadataCache` TTL from
      `--metadata-ttl`, drive `produce` through `produce::run_repl` and `consume`
      through `consume`'s loop with production seams; extend `classify`/`CtlError`
      mapping for `TopicNotFound`, `NoPartitions`, `NoLeaderAfterRetries`, and the
      `UnknownNode` "id=url required for {node}" fail-fast message, all non-zero exit
    - _Requirements: 1.4, 1.8, 4.4, 6.x, 8.5, 13.4, 13.6_

  - [x] 10.3 Write CLI parsing unit tests for the new flags
    - In `crates/vela-ctl/src/cli.rs` tests: new flags parse, defaults apply,
      optional `--partition`, and bad values are rejected; new `ClientError` →
      `CtlError`/exit-code mappings
    - _Requirements: 1.4, 1.8, 4.4, 8.4, 8.6, 8.7, 9.5, 13.4, 13.6_

  - [x] 10.4 Write CLI end-to-end integration tests
    - Create `crates/vela-ctl/tests/repl_and_consume.rs` driving `run` against the
      fake `VelaClientServer`: a full produce session (scripted lines → offsets),
      a multi-partition consume session with late appends → eventual delivery,
      interrupt-driven shutdown, and the four admin commands with/without `NotLeader`
      redirection
    - _Requirements: 4.1, 6.2, 6.6, 9.3, 9.6, 11.1, 13.x_

- [x] 11. Checkpoint - CLI loops wired
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Quality gates
  - [x] 12.1 Run `cargo fmt --check` across the workspace and fix any drift
    - _Requirements: (steering: rustfmt)_
  - [x] 12.2 Run `cargo clippy --all-targets -- -D warnings` and resolve every lint
    - _Requirements: (steering: clippy-clean, no unsafe)_
  - [x] 12.3 Run `cargo test` across the workspace; confirm property tests at ≥100 cases
    - _Requirements: all_
  - [x] 12.4 Run `cargo mutants` on the new pure logic and add asserts to kill survivors
    - Target `classify`, `RetryBudget` (backoff cap, budget at exactly `total`),
      `MetadataCache` freshness (boundary at exactly TTL), and the partitioner
      delegation
    - _Requirements: 1.3, 1.5, 3.4, 3.5, 3.6, 5.5_

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a
  faster MVP; core implementation tasks are never optional.
- Each task references the specific requirement clause(s) and/or design property
  it implements for traceability.
- Property tests follow the existing `prop_*.rs` convention: a module doc-comment
  with the `Feature: ctl-client-routing-and-repl, Property N: <text>` tag, a
  `Validates: Requirements ...` line, and `ProptestConfig::with_cases(>=100)`.
- Open decisions default to the design's stated directions: partitioner in
  `vela-proto` (1); `latest` = bounded `Consume` probe / `earliest` = 0 (2,
  noted inline on task 9.1); `dev-dependency` edge for Property 2 (3); dedicated
  `DescribeCluster` RPC (4).
- The only inward-dependency exception is the **test-only** `vela-client →
  vela-core` `dev-dependency` for task 1.7 (Property 2).

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1", "2.2", "2.3", "7.1", "8.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "1.4", "2.4", "2.5", "2.6", "2.7", "2.8", "7.2"] },
    { "id": 2, "tasks": ["1.5", "1.6", "1.7", "4.1", "4.5", "4.6", "4.7"] },
    { "id": 3, "tasks": ["4.2", "4.3", "4.4", "5.1", "7.4"] },
    { "id": 4, "tasks": ["5.2", "7.3", "8.2"] },
    { "id": 5, "tasks": ["7.5", "8.3", "8.4", "9.1", "10.1"] },
    { "id": 6, "tasks": ["9.2", "9.3", "10.2"] },
    { "id": 7, "tasks": ["10.3", "10.4"] },
    { "id": 8, "tasks": ["12.1", "12.2", "12.3", "12.4"] }
  ]
}
```
