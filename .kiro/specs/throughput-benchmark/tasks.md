# Implementation Plan: Throughput Benchmark

## Overview

This plan builds the `vela-bench` crate incrementally, bottom-up: scaffold the
crate and wire it into the workspace first, then implement the pure-logic modules
(params, workload, metrics, verify, outcome, report, html) each with their
proptest properties, then the live-path components (cluster seam, produce/consume
phases), then the run harness and `main.rs`, then integration tests against the
in-process cluster, and finally the distinct `benchmark` CI job. Every task builds
on the ones before it, ending with everything wired together — no orphaned code.

Implementation language is **Rust** (per the design and `tech.md` steering).
Property-based tests use `proptest` at a minimum of 100 iterations each, live in
`crates/vela-bench/tests/`, and are tagged with a comment of the form
`// Feature: throughput-benchmark, Property {n}: {property text}`.

## Tasks

- [x] 1. Scaffold the `vela-bench` crate and wire it into the workspace
  - [x] 1.1 Create the crate skeleton and workspace wiring
    - Create `crates/vela-bench/` with `Cargo.toml` following the `vela-ctl` split: a `[lib]` target (`vela_bench`, `src/lib.rs`) plus a `[[bin]]` target (`vela-bench`, `src/main.rs`)
    - Add `crates/vela-bench` to `[workspace].members` in the root `Cargo.toml`; add `vela-server = { path = "crates/vela-server" }` to `[workspace.dependencies]`
    - Declare dependencies inward-only: `vela-client`, `vela-proto`, `vela-server`, plus `thiserror`, `tracing`, `clap` (derive/env), `tokio` (rt-multi-thread/macros/time/sync), `futures` (for `buffer_unordered`), `serde`/`serde_json`; add `proptest` and `tokio` test-util to `[dev-dependencies]`
    - Stub `src/lib.rs` with module declarations (`params`, `cli`, `workload`, `cluster`, `produce_phase`, `consume_phase`, `verify`, `metrics`, `report`, `html`, `outcome`, `run`) and a `thiserror`-based `BenchError` library error type
    - Add a minimal `src/main.rs` placeholder so the binary target compiles; verify with `cargo build -p vela-bench`
    - _Requirements: 3.1 (crate placement scaffolding), 8.4 (distinct buildable artifact)_

- [x] 2. Implement Workload_Parameters and validation (`params.rs`)
  - [x] 2.1 Implement parameter model, defaults, and range validation
    - Define `WorkloadParameters` (record_count, value_size, key_mode, partition_count, producer_concurrency, topic, warmup, time_budget, startup_budget, floor_produce_rps, floor_consume_rps) and `KeyMode { Keyed, Keyless }`
    - Implement documented defaults (record_count, value_size, key mode, partition_count, producer_concurrency, topic, warmup, time_budget 60s, startup_budget 60s) all within range
    - Implement `validate()` enforcing every range (record_count `1..=1_000_000_000`, value_size `0..=10_485_760`, partition_count `1..=10_000`, producer_concurrency `1..=10_000`, topic length `1..=255`, warmup `0..record_count`, time_budget `1..=86_400s`, startup_budget `1..=600s`), returning an error that names the offending field with no side effects
    - _Requirements: 3.5, 4.1, 4.2, 4.5, 4.6, 10.5_

  - [x]* 2.2 Write property test for parameter validation
    - **Property 4: Parameter validation accepts in-range inputs and rejects out-of-range ones by name**
    - In-range generator → validation succeeds and echoes inputs; single-field-out-of-range generator (including `partition_count < 1` and `warmup >= record_count`) → fails naming that field, no side effects
    - **Validates: Requirements 3.5, 4.1, 4.5, 4.6, 10.5**

  - [x]* 2.3 Write property test for defaults
    - **Property 5: Defaults are in range and a fully-defaulted configuration validates**
    - Each documented default lies within its range; a config built entirely from defaults passes `validate()`
    - **Validates: Requirements 4.2**

- [x] 3. Implement deterministic workload generation (`workload.rs`)
  - [x] 3.1 Implement payload and key generation
    - Implement `payload_for(position, value_size) -> Vec<u8>` (first `min(8, value_size)` bytes = little-endian `u64` position, remaining bytes a deterministic fill; empty for `value_size == 0`), pure in `(position, value_size)`
    - Implement `position_of(value, value_size) -> Option<u64>` (recovers position when `value_size >= 8`, else `None`)
    - Implement `key_for(position, mode) -> Option<Vec<u8>>` (Keyed → deterministic `Some`, Keyless → `None`)
    - _Requirements: 4.3, 5.6_

  - [x]* 3.2 Write property test for key mode
    - **Property 6: Key mode determines key presence for every record**
    - `key_for(p, Keyed)` is `Some` for all `p`; `key_for(p, Keyless)` is `None` for all `p`
    - **Validates: Requirements 4.3**

- [x] 4. Implement throughput arithmetic (`metrics.rs`)
  - [x] 4.1 Implement throughput computation and zero-window guard
    - Define `Throughput { records_per_sec, bytes_per_sec }` and `ZeroWindow` error
    - Implement `throughput(records, bytes, window) -> Result<Throughput, ZeroWindow>`: for `window > 0` compute `records / secs` and `bytes / secs` (finite); for `window == 0` return `Err(ZeroWindow)`, never NaN/infinite
    - _Requirements: 1.3, 1.5, 2.4_

  - [x]* 4.2 Write property test for throughput
    - **Property 2: Throughput is correct for any positive window and rejects a zero window**
    - `d > 0` → `Ok` with values within float tolerance; `d == 0` → `Err(ZeroWindow)`, never undefined
    - **Validates: Requirements 1.3, 1.5, 2.4**

- [x] 5. Implement data-integrity verification (`verify.rs`)
  - [x] 5.1 Implement count and per-position payload verification
    - Verify records-read count equals Acknowledged_Records (fail with count mismatch / over-read, retaining counts)
    - Verify each read record's payload equals `payload_for(position, value_size)`; when `value_size >= 8` use the embedded position, else fall back to a multiset/count comparison against the expected payload set
    - Report the affected record position on a payload mismatch
    - _Requirements: 5.1, 5.2, 5.5_

  - [x]* 5.2 Write property test for payload determinism, round-trip, and tamper detection
    - **Property 3: Payload generation is deterministic, verifiable, and tamper-evident**
    - `payload_for` referentially transparent, length exactly `s`, `position_of(payload_for(p,s),s) == Some(p)` for `s >= 8`; a correctly-read workload verifies; a single corrupted byte at `p` or an extra record fails identifying the position or count mismatch
    - **Validates: Requirements 5.1, 5.2, 5.5, 5.6**

- [x] 6. Implement outcome determination (`outcome.rs`)
  - [x] 6.1 Implement Outcome, FailureReason, and gating logic
    - Define `Outcome { Passed, Failed { reason } }`, `Phase { Produce, Consume }`, and the full `FailureReason` enum (TopicAlreadyExists, InvalidParameter, TopicCreationFailed, ClusterNotReady, ProduceError, ConsumeError, WarmupFailed, ZeroMeasurementWindow, TimeBudgetExceeded, IntegrityCountMismatch, IntegrityPayloadMismatch, FloorBreachProduce, FloorBreachConsume)
    - Implement outcome determination: `Passed` iff no operation error, cluster ready in budget, time budget not exceeded, integrity holds, and no configured floor breached; otherwise `Failed` with the corresponding typed reason; a `Failed` outcome never presents measured throughput as successful
    - Implement floor gating: breach iff measured rps `< floor` (equal or above passes), independently for produce and consume; when no floor configured, throughput never flips `Passed` to `Failed`
    - _Requirements: 5.3, 5.4, 8.3, 9.4, 11.1, 11.2, 11.3_

  - [x]* 6.2 Write property test for outcome determination
    - **Property 7: Outcome is determined solely by errors, integrity, time budget, and configured floors**
    - **Validates: Requirements 5.3, 5.4, 8.3, 9.4, 11.3**

  - [x]* 6.3 Write property test for floor gating
    - **Property 8: Floor gating fails strictly below the floor and passes at or above it**
    - Holds independently for the produce floor and the consume floor
    - **Validates: Requirements 11.1, 11.2**

  - [x]* 6.4 Write unit tests for FailureReason variants
    - Cover each `FailureReason` variant carrying the expected fields (topic/partition/cause, read/expected counts, position, measured/floor rps)
    - _Requirements: 5.4, 9.1, 9.2, 9.3, 10.6, 11.1, 11.2_

- [x] 7. Implement the Benchmark_Report model and emitters (`report.rs`)
  - [x] 7.1 Implement BenchmarkReport, JSON serialization, and stdout summary
    - Define `BenchmarkReport` (params, outcome, `produce_throughput: Option<Throughput>`, `consume_throughput: Option<Throughput>`, acknowledged_records, total_payload_bytes, total_elapsed, `failure_reason: Option<FailureReason>`) with `serde` derives so every value is a separately named field and absent phase figures serialize as `null`/absent (never `0`)
    - Implement `serde_json` emission to a file and/or stdout
    - Implement the human-readable stdout summary: produce and consume throughput in records/s and bytes/s, or explicit `not measured` for any unavailable figure; print the failure reason on a failing outcome
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [x]* 7.2 Write property test for report completeness and JSON round-trip
    - **Property 10: The Benchmark_Report carries every required field and round-trips through JSON**
    - **Validates: Requirements 6.1, 6.2, 6.5**

- [x] 8. Implement the HTML_Report rendering (`html.rs`)
  - [x] 8.1 Implement self-contained HTML rendering
    - Render a self-contained HTML document (inline `<style>`, no external assets/CDN/JS) from the `BenchmarkReport`: Workload_Parameters, Outcome, both throughputs (records/s and bytes/s), Acknowledged_Record count, total payload bytes, total elapsed time
    - Render the failure reason on a failing Outcome; render an explicit `not measured` indication for a phase that did not complete; HTML-escape substituted values
    - _Requirements: 6.6, 6.7, 6.8_

  - [x]* 8.2 Write property test for stdout and HTML rendering completeness
    - **Property 11: stdout and HTML renderings present every figure, the failure reason, and "not measured" for incomplete phases**
    - Both renderings contain per-phase records/s and bytes/s (or `not measured`), the failure reason text on `Failed` (HTML-escaped in HTML), and the HTML is self-contained (no external asset references)
    - **Validates: Requirements 6.3, 6.4, 6.6, 6.7, 6.8**

- [x] 9. Checkpoint - pure-logic modules complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement the Cluster seam (`cluster.rs`)
  - [x] 10.1 Implement the Cluster trait and InProcessCluster
    - Define the `Cluster` trait: `bootstrap() -> Vec<(String, String)>`, `await_ready(budget) -> Result<(), BenchError>`, `shutdown(self) -> Result<(), BenchError>`
    - Implement `InProcessCluster`: bind an ephemeral localhost port, build a validated single-node `Config` via `Config::from_cli(..)` (empty peers, `replication_factor = 1`), `tokio::spawn(vela_server::serve(config))`, mirroring `crates/vela-server/tests/cross_node_produce_consume.rs`
    - Implement `await_ready` as a fixed-interval polling loop (cheap client call / connection attempt) that succeeds when the cluster serves or errors with `ClusterNotReady` when the startup budget elapses
    - _Requirements: 3.2, 9.3_

  - [x]* 10.2 Write unit test for await_ready timeout
    - Drive a fake cluster that never becomes ready; assert `await_ready` errors with `ClusterNotReady` after the startup budget
    - _Requirements: 9.3_

- [x] 11. Implement the Producer_Phase (`produce_phase.rs`)
  - [x] 11.1 Implement warmup, concurrency, and measurement window
    - Implement the pure measured-set helper selecting positions `[warmup, total)` (measured count `total - warmup`; all positions when `warmup == 0`)
    - Issue exactly `warmup` produces first; abort the phase without opening the window on a warmup failure (`WarmupFailed`)
    - Open the window at the first measured produce invocation; issue the remaining `record_count - warmup` records via `futures::stream::iter(..).map(produce).buffer_unordered(producer_concurrency)` using `payload_for`/`key_for`; count a record only after `Ok(offset)` and only when measured; close the window at the last measured ack; record `acked_count` and `acked_value_bytes`
    - On any `Err` from `produce`, stop further produces and abort with `ProduceError { topic, partition, cause }`
    - _Requirements: 1.1, 1.2, 1.4, 3.7, 4.4, 9.1, 10.1, 10.4, 10.6_

  - [x]* 11.2 Write property test for the measured-set selection
    - **Property 1: Measured set excludes exactly the warmup operations**
    - **Validates: Requirements 1.2, 2.3, 10.1, 10.2, 10.4**

  - [x]* 11.3 Write property test for the concurrency bound
    - **Property 9: In-flight produce requests never exceed the configured concurrency**
    - Drive the phase against an instrumented async fake producer that records max simultaneous in-flight calls; assert `<= c` and all `n` records produced
    - **Validates: Requirements 4.4**

  - [x]* 11.4 Write unit test for produce error mapping
    - A fake producer returning `Err` yields `ProduceError` with the correct topic/partition/cause and stops further produces
    - _Requirements: 9.1_

- [x] 12. Implement the Consumer_Phase (`consume_phase.rs`)
  - [x] 12.1 Implement per-partition reads, warmup, and measurement window
    - Enumerate partitions from `describe_topic(..).partition_count`; consume each partition from offset 0, advancing by returned `next_offset`, until total records read across partitions equals `acked_count`
    - Apply the consume warmup (first `warmup` reads excluded); open the window at the first measured consume invocation and close at the last measured record; count a read only after delivery and after warmup completes; hand each consumed value to the verifier
    - On any `Err` from `consume`, stop further consumes and abort with `ConsumeError { topic, partition, cause }`
    - _Requirements: 2.1, 2.2, 2.3, 2.5, 9.2, 10.2_

  - [x]* 12.2 Write unit test for consume error mapping
    - A fake consumer returning `Err` yields `ConsumeError` with the correct topic/partition/cause and stops further consumes
    - _Requirements: 9.2_

- [x] 13. Implement the run harness (`run.rs`)
  - [x] 13.1 Sequence the Benchmark_Run and own the time budget
    - Sequence: validate params → start overall time-budget clock → `cluster.start()` + `await_ready(startup_budget)` → `describe_topic` precheck (`TopicAlreadyExists` abort) → `create_topic(name, partition_count)` (`TopicCreationFailed` abort) → Producer_Phase → Consumer_Phase → verify → assemble `BenchmarkReport` + Outcome
    - Wrap the run future in `tokio::time::timeout(time_budget, ..)`; on elapse terminate with `TimeBudgetExceeded { budget, read, expected }` retaining counts; ensure cluster startup and topic creation are inside the overall budget but excluded from both Measurement_Windows
    - On `ZeroMeasurementWindow` from `metrics::throughput`, fail rather than reporting undefined throughput; emit JSON + stdout + HTML from the single `BenchmarkReport`
    - _Requirements: 3.3, 3.4, 3.6, 5.4, 8.2, 8.3, 10.3_

- [x] 14. Implement the CLI and binary entry point (`cli.rs`, `main.rs`)
  - [x] 14.1 Implement the clap CLI surface
    - Define the `clap` derive model exposing every Workload_Parameter plus `--report-json`, `--report-html`, `--time-budget-secs`, `--startup-budget-secs`, and optional `--floor-produce-rps`/`--floor-consume-rps`; map parsed args to `WorkloadParameters`
    - _Requirements: 4.1, 8.1_

  - [x] 14.2 Implement main.rs Outcome → ExitCode mapping
    - Parse CLI, run a Benchmark_Run via `run.rs` on a tokio runtime, and map `Outcome::Passed` → exit code 0 and `Outcome::Failed` → non-zero `ExitCode`; reserve `anyhow` for the entry point only
    - _Requirements: 7.3_

  - [x]* 14.3 Write property test for exit-code mapping
    - **Property 12: Exit code reflects the Outcome**
    - `Passed` → 0, `Failed` → non-zero, for any Outcome
    - **Validates: Requirements 7.3**

- [x] 15. Checkpoint - full benchmark binary wired together
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Integration tests against the in-process cluster (`tests/`)
  - [x]* 16.1 Write the end-to-end happy-path integration test
    - Against a real `InProcessCluster` with a tiny workload (small record_count, small value_size, >=2 partitions): create → produce → consume → verify → `Passed`, with per-partition reads from offset 0 and total read == acked; mirror the `cross_node_produce_consume.rs` harness pattern
    - _Requirements: 1.1, 2.1, 2.2, 3.1, 3.2, 3.3_

  - [x]* 16.2 Write the topic-already-exists integration test
    - Pre-create the topic, then run the benchmark; assert a failing Outcome with `TopicAlreadyExists` before any producing
    - _Requirements: 3.4_

  - [x]* 16.3 Write the wall-clock accounting integration test
    - Assert `total_elapsed` spans startup → consume completion and both Measurement_Windows are positive and exclude startup/creation time
    - _Requirements: 1.4, 2.5, 8.2, 10.3_

  - [x]* 16.4 Write the warmup-exclusion integration test
    - Run with `warmup > 0`; assert the warmup operations are excluded from both Measurement_Windows
    - _Requirements: 10.1, 10.2_

- [x] 17. Wire the benchmark into CI (`.github/workflows/ci.yml`)
  - [x] 17.1 Add the distinct `benchmark` job
    - Add a `benchmark` job distinct from `fmt`, `clippy`, `test`, `msrv`, and `dst`, with `timeout-minutes: 30`; checkout, install stable toolchain, `Swatinem/rust-cache@v2`, `cargo build -p vela-bench --release --locked`
    - Run the binary with the CI-designated Workload_Parameters and `--time-budget-secs 120` (<= 300s), writing `target/bench/report.json` and `target/bench/report.html`; a failing Outcome exits non-zero and fails the job
    - Upload both reports with `if: always()` and `retention-days: 7` (`if-no-files-found: error`)
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 8.1, 8.4_

- [x] 18. Final checkpoint - workspace green
  - Ensure all tests pass (`cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features`, `cargo test --workspace`), ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional test tasks and can be skipped for a faster MVP; core implementation tasks are never optional.
- Each task references specific requirements/acceptance criteria for traceability.
- Checkpoints ensure incremental validation at natural boundaries.
- Property-based tests (`proptest`, >=100 iterations each) validate the universal correctness properties; each lives in its own file under `crates/vela-bench/tests/` tagged `// Feature: throughput-benchmark, Property {n}: ...`.
- Unit and integration tests validate specific examples, error wiring, and the live data path that is not amenable to PBT.
- Property 3's test depends on both `workload` and `verify`, so it is sequenced after task 5.1. Property 11's test depends on both `report` and `html`, so it is sequenced after task 8.1.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "3.2", "4.2", "5.1", "6.1", "10.1"] },
    { "id": 3, "tasks": ["5.2", "6.2", "6.3", "6.4", "7.1", "10.2", "11.1", "14.1"] },
    { "id": 4, "tasks": ["7.2", "8.1", "11.2", "11.3", "11.4", "12.1"] },
    { "id": 5, "tasks": ["8.2", "12.2", "13.1"] },
    { "id": 6, "tasks": ["14.2", "16.1", "16.2", "16.3", "16.4"] },
    { "id": 7, "tasks": ["14.3", "17.1"] }
  ]
}
```
