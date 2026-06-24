# Implementation Plan: Batched Produce

## Overview

This plan implements batched produce bottom-up across the workspace, respecting
the inward dependency rule (`structure.md`): each task builds on the ones before
it and ends with everything wired together — no orphaned code. The order follows
the design's layering:

1. `vela-proto` — the wire types (a `RecordBatch` payload arm, the
   `ProduceBatch` RPC and its request/response).
2. `vela-log` — a domain-agnostic `PayloadKind::RecordBatch` tag.
3. `vela-core` — the batch bounds, pure `validate_batch`, the
   `encode_record_batch`/`decode_record_batch` codec, `StateMachine::apply`
   returning `AppliedOffsets`, the new `CoreError` variants, and the
   `produce_batch` entry point.
4. `vela-server` — `convert.rs` `RecordBatch` arms, the
   `DriverCommand::ProduceBatch` driver path, and the `produce_batch` handler.
5. `vela-client` — the pure routing/grouping helper and
   `Producer::produce_batch`.
6. `vela-bench` — the `--batch-size` knob and the batched `ProduceSink` seam.
7. Integration tests against the in-process cluster.

Implementation language is **Rust** (per the design and `tech.md` steering). The
single-record produce path is preserved unchanged throughout (Requirement 4).

Property-based tests use `proptest` at a minimum of 100 iterations each, live in
their crate's top-level `tests/` directory, and are tagged with a comment of the
form `// Feature: batched-produce, Property {n}: {property text}`. Each property
test is placed immediately after the production code it validates.

## Tasks

- [x] 1. Add the batched-produce wire types to `vela-proto`
  - [x] 1.1 Define the `RecordBatch` payload arm, `ProduceBatch` RPC, and messages
    - Add `message RecordBatch { repeated Record records = 1; }` and a fourth `record_batch = 4` arm to the `EntryPayload` oneof, alongside the existing `record`/`noop`/`cluster` arms, so a batch replicates inside `AppendEntriesRequest` as one `LogEntry`
    - Add `message ProduceBatchRequest { string topic = 1; uint32 partition = 2; repeated Record records = 3; }` and `message ProduceBatchResponse { uint64 base_offset = 1; uint32 count = 2; }`
    - Add `rpc ProduceBatch(ProduceBatchRequest) returns (ProduceBatchResponse);` to the `VelaClient` service, leaving the existing `Produce` RPC and `ProduceRequest`/`ProduceResponse` untouched
    - Regenerate the prost/tonic types and verify with `cargo build -p vela-proto`
    - _Requirements: 1.1, 1.4, 4.1, 8.1, 8.3_

- [x] 2. Add the `RecordBatch` payload kind to `vela-log`
  - [x] 2.1 Add the `PayloadKind::RecordBatch` tag and exercise it through the log
    - Add a `RecordBatch` variant to `PayloadKind` (kept domain-agnostic: the payload `bytes` are an opaque length-delimited concatenation), updating any exhaustive matches/tag<->byte mappings in `vela-log`
    - Ensure `LogStorage::append`/`append_entries` and the `DurableWal` recovery/replay path accept and round-trip a `RecordBatch` entry without special-casing its bytes (one `append` under `SyncPolicy::Always` still forces exactly once)
    - _Requirements: 2.1, 7.1, 7.3_

  - [x] 2.2 Write unit tests for the new payload kind
    - Append a `RecordBatch` entry, read it back, and assert the kind and bytes round-trip; assert a recovered/replayed segment preserves a `RecordBatch` entry's kind and bytes
    - _Requirements: 2.1, 7.3_

- [x] 3. Implement batch bounds, validation, codec, offset assignment, and the produce entry point in `vela-core`
  - [x] 3.1 Add the batch-bound constants
    - Add `pub const MAX_BATCH_RECORDS: usize = 10_000;` and `pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;` beside `MAX_RECORD_BYTES` so the server handler and validation share one source of truth
    - _Requirements: 3.3, 3.4_

  - [x] 3.2 Implement `validate_batch` and `BatchRejection` (pure)
    - Define `BatchRejection { Empty, RecordTooLarge { index, size }, TooManyRecords { max, submitted }, TooLarge { max, submitted } }`
    - Implement `pub fn validate_batch(records: &[Record]) -> Result<(), BatchRejection>`: reject `Empty` for zero records; `RecordTooLarge { index, size }` for the first 0-based record whose combined key+value size exceeds `MAX_RECORD_BYTES` (1 MiB); `TooManyRecords { max, submitted }` over `MAX_BATCH_RECORDS`; `TooLarge { max, submitted }` over `MAX_BATCH_BYTES` on the encoded size; `Ok(())` only when every limit holds. Pure and side-effect-free — a rejection appends nothing
    - _Requirements: 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [x] 3.3 Write property test for batch validation
    - **Property 3: Batch validation accepts in-bounds batches and rejects out-of-bounds with the correct reason**
    - Generate in-/out-of-bounds batches (counts at 1, `MAX_BATCH_RECORDS`, and over; record sizes at 0, the 1 MiB boundary, and over; total bytes at and over `MAX_BATCH_BYTES`); assert `Ok` iff non-empty and all limits hold, else the exact `BatchRejection` variant with the reported `index`/`size`/`max`/`submitted` values, with no side effects
    - **Validates: Requirements 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**

  - [x] 3.4 Implement the batch payload codec (pure)
    - Implement `pub fn encode_record_batch(records: &[Record]) -> Vec<u8>` as a length-delimited concatenation of the batch's record **value** bytes in order (keys remain unpersisted, matching the single-record path), and its inverse `pub fn decode_record_batch(bytes: &[u8]) -> Vec<Vec<u8>>`
    - _Requirements: 1.2, 2.1, 7.3, 10.2_

  - [x] 3.5 Write property test for the codec round-trip
    - **Property 1: Batch payload round-trips through encode/decode**
    - For any ordered list of records, assert `decode_record_batch(encode_record_batch(records))` equals the input value sequence exactly and in order
    - **Validates: Requirements 1.2, 10.2**

  - [x] 3.6 Extend `StateMachine::apply` to assign a contiguous offset range
    - Define `pub enum AppliedOffsets { One(Offset), Range { base: Offset, count: u32 } }`
    - Change `apply(&mut self, entry: &LogEntry) -> Option<AppliedOffsets>`: a `RecordBatch` entry decodes its N values via `decode_record_batch`, captures `base = next_offset()` before the push, pushes all N values onto the dense `records` vector in order, and returns `Range { base, count: N }`; a `Record` entry returns `One(offset)`; `Noop`/`Cluster` return `None`. Record offsets stay gap-free and contiguous across single and batch entries
    - Update existing `apply` call sites for the new return type, preserving single-record behavior
    - _Requirements: 1.3, 2.1, 2.4, 2.5, 4.5_

  - [x] 3.7 Write property test for batch offset assignment
    - **Property 2: A committed batch assigns a contiguous offset range from the captured base**
    - Generate a prior length `base` and a batch of N >= 1, plus arbitrary interleavings of single-record and batch entries on one partition; assert a batch takes `base..base+N-1`, `next_offset()` advances by exactly N, each committed batch occupies a contiguous run with no other record's offset inside it, and offsets increase by exactly 1 in commit order
    - **Validates: Requirements 1.3, 2.1, 2.3, 2.4, 2.5, 4.5**

  - [x] 3.8 Add the `CoreError` variants and the `produce_batch` entry point
    - Add `CoreError` variants `EmptyBatch`, `RecordTooLargeAt { index, size }`, `BatchTooManyRecords { max, submitted }`, `BatchTooLarge { max, submitted }` with `thiserror` messages embedding the requirement-mandated values; map them to the existing `ErrorCode`s (`VALIDATION`, `PAYLOAD_TOO_LARGE`) so the wire enum and the client `classify` (Fatal, non-retryable) are unchanged
    - Implement `BatchOutcome { Committed { base_offset, count }, NotLeader, NotCommitted }` and `pub fn produce_batch(metadata, fleet, topic, partition, records, clock) -> Result<(Offset, u32), CoreError>`: run the same admission/partition/leadership checks as `produce`, call `validate_batch` (mapping `BatchRejection` to the new `CoreError` variants) **before** any append, then propose exactly one `RecordBatch` entry (`encode_record_batch`) and resolve the base offset + count on commit. The single-record `produce` is unchanged
    - _Requirements: 1.1, 1.3, 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 7.3_

  - [x] 3.9 Write property test for single-sync batch durability
    - **Property 6: A batch is made durable with a single sync, fewer than N for N > 1**
    - Append a generated batch of N to a `DurableWal` over the fsync-counting / fault filesystem (`sim`) seam; assert exactly one force covers the whole batch, strictly fewer than the N forces N single-record produces perform for every N > 1, and the batch is one log entry committing as one unit
    - **Validates: Requirements 7.1, 7.2, 7.3**

- [x] 4. Checkpoint - pure-logic layer complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Wire the `ProduceBatch` RPC through `vela-server`
  - [x] 5.1 Add `RecordBatch` arms to `convert.rs`
    - Add the `RecordBatch` arm to `entry_payload_from_proto`/`entry_payload_to_proto` so a `RecordBatch` `EntryPayload` round-trips proto↔domain and replicates through `AppendEntriesRequest`; add proto↔domain `RecordBatch` record-list conversion reusing `record_from_proto`/`record_to_proto`
    - _Requirements: 1.1, 2.1, 7.3_

  - [x] 5.2 Add the `DriverCommand::ProduceBatch` driver path
    - Add `DriverCommand::ProduceBatch { values: Vec<Vec<u8>>, reply: oneshot::Sender<Result<(Offset, u32), ProduceError>> }`; handle it by proposing exactly one `RaftInput::Propose(RecordBatch)`, tracking a single `Pending { target, reply }`, and resolving `(base_offset, count)` once the entry commits, with `offset_at` counting a batch entry's N record positions so the base offset is the count of records committed before the batch entry's index
    - _Requirements: 1.3, 2.1, 2.4, 6.3, 7.1, 7.2, 7.3_

  - [x] 5.3 Implement the `VelaClientService::produce_batch` handler and error mapping
    - Mirror `produce`: lock metadata, `ensure_producible` + partition existence, run `validate_batch` (mapping `BatchRejection` to the new `CoreError` variants), resolve the partition handle, send `DriverCommand::ProduceBatch`, and map the reply — `Ok((base, count)) -> ProduceBatchResponse { base_offset, count }`, `NotLeader -> CoreError::NotLeader { leader }` via the live-leader hint, `CommitTimeout -> CoreError::CommitTimeout` — all through `convert::core_error_to_status`
    - _Requirements: 1.1, 1.4, 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 6.1, 6.3, 6.4, 6.5, 6.6, 8.3_

  - [x] 5.4 Write unit tests for the handler admission and error mapping
    - Cover empty/oversized-record/too-many/too-large rejections appending nothing; topic-not-found, partition-not-found, and topic-deleting rejections; a non-leader reply mapped to a not-leader status carrying the leader hint; a commit-timeout reply mapped to the timeout status with no offset
    - _Requirements: 2.2, 3.2, 3.3, 3.4, 3.5, 3.6, 6.1, 6.3, 6.4, 6.5, 6.6_

- [x] 6. Implement `Producer::produce_batch` in `vela-client`
  - [x] 6.1 Implement the pure routing/grouping helper
    - Implement a pure helper that, given the input `Vec<(Option<Vec<u8>>, Vec<u8>)>` and the partition resolved for each record by the existing `PartitionRouter` (keyed/keyless rules unchanged), produces for each `(topic, partition)` the ordered sublist of records **and** their original input indices — placing each record in exactly one batch, one batch per distinct resolved partition, preserving per-partition input order — so offsets can be scattered back into input order from each batch's `base_offset + position`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [x] 6.2 Write property test for routing and grouping
    - **Property 4: Routing and grouping partition every record exactly once and preserve per-partition input order**
    - For any ordered input and any non-zero partition count, assert each record lands in exactly one per-`(topic, partition)` batch, exactly one batch per distinct resolved partition, and relative input order is preserved within each batch
    - **Validates: Requirements 5.1, 5.4, 5.5**

  - [x] 6.3 Implement `Producer::produce_batch` dispatch and offset reassembly
    - Implement `pub async fn produce_batch(&self, topic: &str, records: Vec<(Option<Vec<u8>>, Vec<u8>)>) -> Result<Vec<u64>>`: route + group via the helper, dispatch one `ProduceBatch` per resolved partition through `ClientCore::dispatch` (inheriting `NotLeader` redirection, transport re-resolution, retry budget), and scatter each batch's `base_offset + position` back into input order. Surface routing failures (`NoPartitions`) and unknown-leader (`NoLeader`) per partition without affecting other partitions; on any batch failure return that error and report no offsets. The single-record `produce` is unchanged
    - _Requirements: 5.6, 5.7, 5.8, 6.2, 8.1, 8.2, 8.3, 8.4_

  - [x] 6.4 Write property test for per-record offsets in input order
    - **Property 5: Per-record offsets are returned in input order**
    - Drive `produce_batch` over generated multi-partition inputs against a fake sink / in-process cluster; assert the returned offsets align one-to-one with the input in input order and each equals its batch's `base_offset` plus its 0-based position within that batch
    - **Validates: Requirements 1.4, 8.2, 8.3**

- [x] 7. Drive batched produce from `vela-bench`
  - [x] 7.1 Add the `--batch-size` CLI flag and validated parameter
    - Add `batch_size: u32` to `WorkloadParameters` with `DEFAULT_BATCH_SIZE = 1` and `BATCH_SIZE_RANGE = 1..=10_000`; add the `--batch-size` `clap` flag (default 1) mapped into `WorkloadParameters`; extend `WorkloadParameters::validate` to reject `batch_size < 1` (and above `MAX_BATCH_RECORDS`) as `InvalidParameter` naming `batch_size`, before the producer phase begins
    - _Requirements: 9.1, 9.5_

  - [x] 7.2 Add the batched `ProduceSink` seam and chunk the producer phase
    - Add `async fn produce_batch(&self, records: Vec<(Option<Vec<u8>>, Vec<u8>)>) -> Result<Vec<u64>, ProduceFailure>` to the `ProduceSink` trait (default groups via `produce`); override it on `VelaProduceSink` with `client.producer().produce_batch`; chunk the warmup and measured ranges into groups of `batch_size` (single-record behavior at `batch_size == 1`), keep up to `producer_concurrency` batches in flight, and count **each** acknowledged record (the sum of chunk sizes) toward produce throughput
    - _Requirements: 9.2, 9.3, 9.4_

  - [x] 7.3 Write unit test for batch-size validation and equivalence
    - Assert `--batch-size` defaults to 1 and parses; `validate` rejects `batch_size == 0` as `InvalidParameter` naming `batch_size`; a `batch_size == 1` run produces the same per-record results as the single-record path
    - _Requirements: 9.1, 9.3, 9.5_

- [x] 8. Checkpoint - full batched-produce path wired together
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Integration tests against the in-process cluster (`tests/`)
  - [x] 9.1 Write the happy-path batch produce + consume parity test
    - Against a real in-process cluster (mirroring `cross_node_produce_consume.rs`), produce a multi-record batch to a partition, assert the returned per-record offsets are contiguous from the base, then consume the partition and assert the records come back in ascending offset order with values byte-for-byte identical, gap-free, interleaving correctly with single-record produces
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 8.2, 10.1, 10.2, 10.4_

  - [x] 9.2 Write the not-leader redirect test
    - Send a `ProduceBatch` to a non-leader replica; assert it rejects with the live-leader hint and appends nothing, and that the client re-resolves and retries the identical batch at the new leader within the retry budget
    - _Requirements: 5.6, 6.1, 6.2_

  - [x] 9.3 Write the admission-error tests
    - Assert a missing topic, a missing partition, and a deleting topic each reject the batch with the matching caller-visible error and append nothing; assert a zero-partition topic surfaces a routing error and a leaderless partition surfaces an unknown-leader error, leaving other partitions' batches unaffected
    - _Requirements: 5.7, 5.8, 6.4, 6.5, 6.6_

  - [x] 9.4 Write the commit-timeout test
    - Drive a batch that cannot reach a majority within the 5,000 ms `Commit_Timeout`; assert it fails with a caller-visible timeout error, returns no offset, and leaves the partition's committed offset unchanged
    - _Requirements: 2.3, 6.3, 8.4_

  - [x] 9.5 Write the model-based equivalence and consume-parity test
    - **Property 7: A batch of N is equivalent to N single produces, and consumes back identically**
    - Generate a record sequence, drive it once as one batch of N and once as N single-record produces against the in-process cluster; assert identical committed offset sequences and stored values (a one-record batch takes the single-record offset), and that consuming returns all records ascending, byte-for-byte identical, contiguous and gap-free across mixed batch and single produces
    - **Validates: Requirements 4.2, 10.1, 10.2, 10.3, 10.4**

- [x] 10. Final checkpoint - workspace green
  - Ensure all tests pass (`cargo fmt --all --check`, `cargo clippy --workspace --all-targets`, `cargo test --workspace`), ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional test tasks and can be skipped for a faster MVP; core implementation tasks are never optional.
- Each task references specific acceptance criteria for traceability.
- Checkpoints ensure incremental validation at natural boundaries: after the pure-logic layer (`vela-core`), after the full path is wired, and a final workspace-green gate.
- Property-based tests (`proptest`, >=100 iterations each) validate the seven universal correctness properties; each lives in its own file under the relevant crate's `tests/` directory tagged `// Feature: batched-produce, Property {n}: ...`, placed immediately after the production code it validates.
- Unit and integration tests validate specific examples, error wiring, leadership redirection, and the live durability/throughput path that is not amenable to PBT.
- The single-record produce path (`Produce` RPC, `produce`, single-record `apply`) is preserved unchanged throughout (Requirement 4).
- Property placement: P1 after the codec (3.4→3.5), P2 after `StateMachine::apply` (3.6→3.7), P3 after `validate_batch` (3.2→3.3), P6 after the core/log append path (3.8→3.9), P4 after the grouping helper (6.1→6.2), P5 after `produce_batch` (6.3→6.4), and P7 as an integration test after the full path is wired (9.5).

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1"] },
    { "id": 2, "tasks": ["2.2", "3.2"] },
    { "id": 3, "tasks": ["3.3", "3.4"] },
    { "id": 4, "tasks": ["3.5", "3.6"] },
    { "id": 5, "tasks": ["3.7", "3.8"] },
    { "id": 6, "tasks": ["3.9", "5.1"] },
    { "id": 7, "tasks": ["5.2"] },
    { "id": 8, "tasks": ["5.3"] },
    { "id": 9, "tasks": ["5.4", "6.1"] },
    { "id": 10, "tasks": ["6.2", "6.3"] },
    { "id": 11, "tasks": ["6.4", "7.1"] },
    { "id": 12, "tasks": ["7.2"] },
    { "id": 13, "tasks": ["7.3", "9.1", "9.2", "9.3", "9.4", "9.5"] }
  ]
}
```
