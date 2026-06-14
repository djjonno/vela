# Implementation Plan: Vela Streaming Platform

## Overview

This plan builds Vela bottom-up, following the inward crate dependency order:
`vela-proto` and `vela-log` first, then `vela-raft` (with the deterministic
simulation harness wired up early so consensus can be property-tested as it is
built), then `vela-core`, then `vela-server`, `vela-client`, `vela-ctl`, and finally
the local-cluster artifacts. Every task is an incremental coding step that builds on
prior steps and ends integrated — no orphaned code.

Property-based tests (`proptest`) are central. The 39 correctness properties from the
design are each implemented as a single property test (minimum 100 iterations) tagged
`// Feature: vela-streaming-platform, Property N`, and are placed alongside the
component task that introduces the behavior they verify. Election and replication
properties run over the `ManualClock` + `InMemoryTransport` + `SimCluster` harness;
log/routing/pure-logic properties run directly. Unit/example, integration, and
smoke/structural tests from the design's Testing Strategy are included.

Sub-tasks marked with `*` are optional test tasks and may be skipped for a faster MVP.

## Tasks

- [x] 1. Scaffold the Cargo workspace and seven member crates
  - [x] 1.1 Create the workspace root and seven crate skeletons with inward-only dependencies
    - Create root `Cargo.toml` declaring exactly seven `[workspace] members`: `vela-log`, `vela-raft`, `vela-proto`, `vela-core`, `vela-server`, `vela-client`, `vela-ctl` under `crates/`
    - Create each crate's `Cargo.toml` + `src/lib.rs` (or `src/main.rs` for the `velad`/`vela-ctl` binaries) with dependency edges pointing inward only: `vela-raft -> vela-log`; `vela-core -> vela-raft, vela-log`; `vela-server -> vela-core, vela-proto`; `vela-client -> vela-proto`; `vela-ctl -> vela-client, vela-proto`; `vela-log` and `vela-proto` declare no Vela deps
    - Pin a sensible MSRV and add shared workspace dependencies (`thiserror`, `tracing`, `proptest` dev-dep) with explicit versions
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 1.2 Write structural smoke test for workspace membership and dependency direction
    - Parse the workspace manifests and assert exactly seven members exist and that `vela-log`/`vela-raft` declare no forbidden (outward) Vela dependencies
    - _Requirements: 1.1, 1.2, 1.4_

- [x] 2. Implement vela-proto wire types and gRPC service surfaces
  - [x] 2.1 Define protobuf messages and compile with prost/tonic
    - Author `.proto` definitions for records, log entries/payloads, Raft RPCs (`AppendEntries`, `RequestVote` and replies), topic-admin messages, metadata sync, and a shared `VelaError` (code + message + optional leader hint); wire up `prost`/`tonic` codegen in `build.rs`
    - _Requirements: 12.1, 12.4_
  - [x] 2.2 Define the VelaClient and VelaPeer gRPC services
    - Declare `VelaClient` (`Produce`, `Consume`, `CreateTopic`, `DeleteTopic`, `ListTopics`, `DescribeTopic`, `FindLeader`) and `VelaPeer` (`AppendEntries`, `RequestVote`, `Heartbeat`, `SyncMetadata`); ensure all partition RPCs carry `(topic, partition)`
    - _Requirements: 12.2, 12.3_
  - [x] 2.3 Write structural smoke test for proto-owned wire types and service surfaces
    - Assert the generated module exposes the two services and that wire/error types live in `vela-proto`
    - _Requirements: 12.1, 12.2, 12.3_

- [x] 3. Implement vela-log append-only log behind the Log_Storage trait
  - [x] 3.1 Define the LogStorage trait and supporting types
    - Define `LogEntry`, `EntryPayload` (opaque bytes + tag), `Snapshot`, `CommitIndex = Option<u64>`, `LogError` (thiserror), and the `LogStorage` trait (`append`, `append_entries`, `read`, `entry`, `last_index`, `term_at`, `commit_index`, `commit`, `revert`, `snapshot`)
    - _Requirements: 6.1_
  - [x] 3.2 Implement InMemoryLog
    - Implement `InMemoryLog` over `Vec<LogEntry>` + `commit_index`, enforcing 0-based indexing, monotonic commit bounds, and committed-entry protection on revert
    - _Requirements: 6.2, 6.3, 6.4, 6.5, 6.8, 6.10, 6.12_
  - [x] 3.3 Write property test for sequential index assignment
    - **Property 1: Append assigns the next sequential index**
    - **Validates: Requirements 6.3, 6.4**
  - [x] 3.4 Write property test for append/read round-trip
    - **Property 2: Append/read round-trip preserves entries**
    - **Validates: Requirements 6.13**
  - [x] 3.5 Write property test for range reads
    - **Property 3: Range read is ascending and gap-omitting**
    - **Validates: Requirements 6.5, 6.6**
  - [x] 3.6 Write property test for commit bounds
    - **Property 4: Commit advances only within valid bounds and never backward**
    - **Validates: Requirements 6.8, 6.9**
  - [x] 3.7 Write property test for revert semantics
    - **Property 5: Revert truncates the uncommitted suffix and protects committed entries**
    - **Validates: Requirements 6.10, 6.11**
  - [x] 3.8 Write property test for snapshot
    - **Property 6: Snapshot reflects exactly the committed prefix**
    - **Validates: Requirements 6.7, 6.12**
  - [x] 3.9 Write unit/example and smoke tests for log edge cases
    - Initial commit index reported as uncommitted/`None` (6.7); `start > end` read returns empty without error (6.6); smoke-assert the `Log_Storage` trait exists and `InMemoryLog` implements it (6.1, 6.2)
    - _Requirements: 6.6, 6.7, 6.1, 6.2_

- [x] 4. Checkpoint - log layer complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Build vela-raft core state machine and deterministic simulation harness
  - [x] 5.1 Define Raft trait seams, message types, and the RaftNode skeleton
    - Re-export `LogStorage`; define `Transport` and `Clock` traits, `NodeId`, `TimerKind`, `RaftMessage` (+ RequestVote/AppendEntries structs and replies), `Role`, `RaftInput`, `RaftOutput`, and the `RaftNode<S: LogStorage>` struct holding persistent/volatile/leader state; stub `RaftNode::step`
    - _Requirements: 1.4_
  - [x] 5.2 Build the deterministic simulation harness
    - Implement `ManualClock` (seeded RNG for election randomness, explicit time advance), `InMemoryTransport` (queues with reorder/delay/drop/duplicate/partition), and `SimCluster` (N `RaftNode`s sharing one clock + transport, with a `step()` that delivers one scheduled event)
    - _Requirements: 1.4, 7.2_

- [x] 6. Implement vela-raft leader election
  - [x] 6.1 Implement election logic in RaftNode::step
    - Election timeout (randomized 150–300 ms) → candidate, term+1, self-vote, broadcast RequestVote; candidate restart on timeout; vote granting with election restriction and at-most-one-vote-per-term; higher-term step-down; majority → leader; leader arms 50 ms heartbeat and emits empty AppendEntries
    - _Requirements: 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10_
  - [x] 6.2 Write property test for idle-follower election start (over SimCluster)
    - **Property 22: An idle follower becomes a candidate for the next term**
    - **Validates: Requirements 7.2, 7.3, 7.5**
  - [x] 6.3 Write property test for vote decisions
    - **Property 23: Vote decision follows term, log-currency, and single-vote rules**
    - **Validates: Requirements 7.7, 7.8**
  - [x] 6.4 Write property test for higher-term step-down
    - **Property 24: A higher term always forces step-down**
    - **Validates: Requirements 7.9**
  - [x] 6.5 Write property test for single-leader-per-term election
    - **Property 25: A majority of same-term votes elects exactly one leader per term**
    - **Validates: Requirements 7.4, 7.10**
  - [x] 6.6 Write property test for heartbeat interval vs election timeout
    - **Property 27: A leader heartbeats faster than the minimum election timeout**
    - **Validates: Requirements 7.6**
  - [x] 6.7 Write example test for leaders residing on different nodes
    - Drive two independent `SimCluster` groups and assert their elected leaders may differ
    - _Requirements: 7.11_

- [x] 7. Checkpoint - Raft election complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement vela-raft log replication
  - [x] 8.1 Implement replication logic in RaftNode::step
    - Leader sends ≤256 entries per AppendEntries carrying `(prev_log_index, prev_log_term)`; followers accept on match / reject with conflict hint; leader backs up `next_index` and retries with capped exponential backoff (1 s → 5 s); commit advance to highest majority-replicated current-term entry, monotonic; surface newly committed entries in `RaftOutput.committed`; bring lagging followers into agreement
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9_
  - [x] 8.2 Write property test for bounded AppendEntries with correct preceding entry
    - **Property 28: AppendEntries is bounded and carries the correct preceding entry**
    - **Validates: Requirements 8.1**
  - [x] 8.3 Write property test for follower accept/reject
    - **Property 29: Followers accept matching and reject conflicting AppendEntries**
    - **Validates: Requirements 8.2, 8.3**
  - [x] 8.4 Write property test for retry backoff (over SimCluster, simulated clock)
    - **Property 30: Replication retries back up and use capped exponential backoff**
    - **Validates: Requirements 8.4**
  - [x] 8.5 Write property test for commit-index advancement
    - **Property 31: Commit index advances exactly to the highest majority-replicated current-term entry**
    - **Validates: Requirements 8.5, 8.6**
  - [x] 8.6 Write property test for commit-index monotonicity
    - **Property 32: Commit index is monotonic**
    - **Validates: Requirements 8.7**
  - [x] 8.7 Write property test for state-machine apply ordering
    - **Property 33: The state machine applies committed entries once, in order, with no gaps**
    - **Validates: Requirements 8.8**
  - [x] 8.8 Write property test for lagging-follower convergence
    - **Property 34: A lagging follower converges to the leader's log**
    - **Validates: Requirements 8.9**
  - [x] 8.9 Write property test for the Log Matching Property
    - **Property 35: Log Matching holds across the group**
    - **Validates: Requirements 8.10**

- [x] 9. Checkpoint - Raft replication complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement vela-core topic model, creation, and deletion
  - [x] 10.1 Define core domain types
    - Define `NodeId`, `Record`, `Offset`, `PartitionIndex`, `Partition`, `Topic`, `TopicState`, `Member`, `NodeAvailability`, `ClusterMetadata`, and `ClusterCommand`
    - _Requirements: 2.2, 9.3_
  - [x] 10.2 Implement topic creation, validation, and balanced replica assignment
    - Validate name (1–255, `[A-Za-z0-9_-]`) and partition count (1–10000); reject duplicate names and insufficient-node clusters without side effects; register N partitions indexed 0..N-1; assign `replication_factor` distinct replica nodes with leadership balanced per topic
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 9.6, 10.1_
  - [x] 10.3 Write property test for partition registration
    - **Property 7: Topic creation registers N partitions indexed 0..N-1**
    - **Validates: Requirements 2.1, 2.2**
  - [x] 10.4 Write property test for replica assignment
    - **Property 8: Replica assignment uses replication-factor distinct member nodes**
    - **Validates: Requirements 2.3, 9.6**
  - [x] 10.5 Write property test for invalid-input rejection
    - **Property 9: Invalid topic-creation inputs are rejected without side effects**
    - **Validates: Requirements 2.5, 2.6, 2.7**
  - [x] 10.6 Write property test for balanced leadership
    - **Property 12: Leadership is balanced across nodes per topic**
    - **Validates: Requirements 10.1**
  - [x] 10.7 Write example test for duplicate-topic rejection
    - Assert creating a topic whose name already exists returns `TopicExists` and leaves metadata unchanged
    - _Requirements: 2.4_
  - [x] 10.8 Implement atomic topic deletion lifecycle
    - Remove the topic and all partitions atomically; stop each partition's Raft group before releasing its in-memory log; reject not-found deletions
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  - [x] 10.9 Write property test for atomic deletion
    - **Property 13: Deleting topics atomically removes all partitions**
    - **Validates: Requirements 3.1**
  - [x] 10.10 Write property test for operations on a deleting topic
    - **Property 14: Operations on a deleting topic are rejected**
    - **Validates: Requirements 3.7**
  - [x] 10.11 Write example tests for delete ordering and not-found
    - Using a mock fleet, assert the Raft group is stopped before the log is released (3.2, 3.3); assert delete of a missing topic returns not-found (3.4)
    - _Requirements: 3.2, 3.3, 3.4_

- [x] 11. Implement vela-core partition routing, fleet, and produce/consume
  - [x] 11.1 Implement the PartitionRouter
    - Keyed routing: `partition = hash(key) % partition_count`, stable across calls; keyless routing: round-robin coverage across all partitions via a per-topic atomic counter
    - _Requirements: 4.1, 4.2, 10.2, 10.3_
  - [x] 11.2 Write property test for deterministic keyed routing
    - **Property 10: Keyed routing is deterministic**
    - **Validates: Requirements 4.1, 10.2**
  - [x] 11.3 Write property test for keyless distribution
    - **Property 11: Keyless routing distributes across all partitions**
    - **Validates: Requirements 4.2, 10.3**
  - [x] 11.4 Implement RaftGroupFleet, PartitionReplica, and the State_Machine
    - Wrap a `RaftNode` + partition `State_Machine` (assigns gap-free 0-based offsets on apply) in `PartitionReplica`; key the `RaftGroupFleet` by `(topic, partition)` with create/stop lifecycle; instantiate exactly one Raft group per partition
    - _Requirements: 7.1, 4.7, 5.1, 3.2_
  - [x] 11.5 Write property test for one Raft group per partition
    - **Property 26: Exactly one Raft group exists per partition**
    - **Validates: Requirements 7.1**
  - [x] 11.6 Implement the produce path
    - Route then append on the leader at the next index; commit on majority and return the assigned offset; reject non-leaders with a leader-identifying redirect and no append; reject >1 MiB payloads; return a not-committed error on 5 s commit timeout without advancing commit; reject produce to a missing topic
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9_
  - [x] 11.7 Write property test for offset assignment
    - **Property 15: Committed records receive unique, gap-free, monotonic 0-based offsets**
    - **Validates: Requirements 4.3, 4.4, 4.7**
  - [x] 11.8 Write property test for non-leader produce redirection
    - **Property 16: Produce to a non-leader is redirected and writes nothing**
    - **Validates: Requirements 4.6, 11.2**
  - [x] 11.9 Write property test for the commit timeout
    - **Property 17: A record not replicated to a majority within the commit timeout is not committed**
    - **Validates: Requirements 4.9**
  - [x] 11.10 Write property test for oversized-payload rejection
    - **Property 18: Oversized payloads are rejected**
    - **Validates: Requirements 4.8**
  - [x] 11.11 Implement the consume path
    - Return only committed records in strictly ascending offset order from the requested offset; bound by requested max (1–10000) or default 500; reject invalid params; return empty beyond the highest committed offset; error on missing partition or no elected leader
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8_
  - [x] 11.12 Write property test for committed consume ordering
    - **Property 19: Consume returns only committed records in ascending offset order**
    - **Validates: Requirements 5.1, 5.2**
  - [x] 11.13 Write property test for the max-count bound
    - **Property 20: Consume respects the maximum-count bound**
    - **Validates: Requirements 5.5, 5.6**
  - [x] 11.14 Write property test for invalid consume parameters
    - **Property 21: Invalid consume parameters are rejected**
    - **Validates: Requirements 5.7**
  - [x] 11.15 Write example/edge tests for consume and not-found cases
    - Offset beyond commit returns empty (5.3); default max 500 (5.6); no-leader returns unavailable (5.8); missing topic/partition returns not-found (4.5, 5.4, 10.5)
    - _Requirements: 5.3, 5.6, 5.8, 4.5, 5.4, 10.5_

- [x] 12. Implement vela-core cluster metadata management
  - [x] 12.1 Implement ClusterMetadata availability and the metadata Raft group controller
    - Track per-node availability (exactly available/unavailable); run the dedicated `__meta/p0` Raft group; apply committed `ClusterCommand`s; propagate metadata (carrying `epoch`) via `SyncMetadata` and observe acks within 5 s, reporting laggards on delete; resolve `FindLeader`
    - _Requirements: 9.3, 3.1, 2.8, 3.5, 3.6, 10.4_
  - [x] 12.2 Write property test for the availability state set
    - **Property 36: Node availability is always exactly one of two states**
    - **Validates: Requirements 9.3**
  - [x] 12.3 Write example/edge tests for leader location
    - `FindLeader` on a missing topic/partition returns not-found (10.5); a partition mid-election returns leader-unavailable within 5 s without mutating metadata (10.6)
    - _Requirements: 10.5, 10.6_

- [x] 13. Checkpoint - core domain complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 14. Implement vela-server daemon, services, membership, and config
  - [x] 14.1 Implement configuration parsing and structured startup logging
    - Parse `node_id`, `listen_addr`, `peers`, `replication_factor` via `clap` + env vars; emit `tracing` logs for config errors (non-zero exit) and readiness
    - _Requirements: 14.4, 15.1, 15.2, 15.3_
  - [x] 14.2 Implement the tonic services, Transport adapter, and per-partition driver tasks
    - Implement `VelaClient` + `VelaPeer` services translating proto ↔ core/raft types; adapt the `Transport` trait onto outbound gRPC channels; run one driver task per hosted partition replica with a real-clock timer source feeding `RaftInput`
    - _Requirements: 12.2, 12.3, 15.1_
  - [x] 14.3 Implement the membership subsystem
    - Connect to peers with a 5 s timeout and 1 s retry, send 1 s heartbeats, mark a node unavailable after 3 consecutive missed heartbeats, and restore it to available on a successful response
    - _Requirements: 9.1, 9.2, 9.4, 9.5_
  - [x] 14.4 Write property test for heartbeat-based availability transitions (simulated clock)
    - **Property 37: Three consecutive missed heartbeats mark a node unavailable, and recovery restores it**
    - **Validates: Requirements 9.4, 9.5**
  - [x] 14.5 Implement the VelaError boundary mapping
    - Map each `LogError`/`RaftError`/`CoreError` to a `VelaError` (code + message + optional leader hint) at the gRPC boundary, preserving error code and identity
    - _Requirements: 12.4, 11.2_
  - [x] 14.6 Write property test for typed error round-tripping
    - **Property 39: Typed error mapping round-trips**
    - **Validates: Requirements 12.4**
  - [x] 14.7 Write example tests for role-transition and lifecycle logs
    - With a `tracing` test subscriber, assert a structured log per Raft role transition naming partition + new role (15.4), a config-error log + non-zero exit (15.2), and a readiness log (15.3)
    - _Requirements: 15.2, 15.3, 15.4_
  - [x] 14.8 Write integration tests for propagation, membership, and listener bind
    - Metadata propagation within 5 s and partial-failure reporting (2.8, 3.5, 3.6); peer connection timeout/retry and cluster discovery (9.1, 9.2, 14.3); gRPC listener binds on startup (15.1)
    - _Requirements: 2.8, 3.5, 3.6, 9.1, 9.2, 14.3, 15.1_

- [x] 15. Checkpoint - server daemon complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Implement vela-client leader routing
  - [x] 16.1 Implement Producer, Consumer, and AdminClient with a leader cache
    - Provide `Producer`, `Consumer`, `AdminClient`; cache leader locations from `FindLeader`/metadata; wrap partition routing so produce resolves the partition before dispatch and each request targets the believed leader
    - _Requirements: 11.1, 4.1, 4.2_
  - [x] 16.2 Implement leader-redirection retry logic
    - On a `NotLeader` redirect, retry against the identified leader after waiting ≥100 ms, up to 5 retries, then return a no-leader-found error
    - _Requirements: 11.2, 11.3, 11.4_
  - [x] 16.3 Write property test for redirection retry behavior
    - **Property 38: Client retries redirections with a minimum delay and a bounded count**
    - **Validates: Requirements 11.3, 11.4**
  - [x] 16.4 Write example test for leader-directed requests
    - Assert a partition request is directed to the cached leader node
    - _Requirements: 11.1_

- [x] 17. Implement vela-ctl CLI control tool
  - [x] 17.1 Implement the clap CLI over vela-client
    - Implement `create <name> --partitions N`, `delete <name>`, `list`, `describe <name>` over `vela-client`; exit 0 on success; report and exit non-zero on a 5 s connection failure or a cluster-returned error
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7_
  - [x] 17.2 Write example tests for CLI commands and exit codes
    - Against an in-process fake server, assert create/delete/list/describe output, success exit 0, connection-failure non-zero exit, and cluster-rejection non-zero exit
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7_

- [x] 18. Provide the local multi-node cluster artifacts
  - [x] 18.1 Add the Dockerfile for the velad daemon
    - Author a `Dockerfile` that builds and runs the `velad` binary
    - _Requirements: 14.1_
  - [x] 18.2 Add the docker-compose multi-node cluster
    - Author `docker-compose.yml` launching multiple `velad` nodes wired into one cluster, each node's peer list pointing at the others
    - _Requirements: 14.2, 14.3, 14.5_
  - [x] 18.3 Write structural smoke test for cluster artifacts
    - Assert `Dockerfile` and `docker-compose.yml` exist and the compose file declares multiple nodes with cross-referencing peer lists
    - _Requirements: 14.1, 14.2_
  - [x] 18.4 Write integration smoke test for end-to-end cluster behavior
    - Over real `tokio`/tonic, bring up a multi-node cluster, run leader election + replication, and exercise end-to-end produce/consume to confirm simulator-validated logic matches real timers
    - _Requirements: 14.3, 14.5_
  - [x] 18.5 Write structural smoke test for the whole-workspace build
    - Assert `cargo build` at the workspace root compiles all seven crates, failing with a non-zero status that names the failing crate on error
    - _Requirements: 1.5, 1.6_

- [x] 19. Final checkpoint - full workspace builds and all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP.
- Each task references specific requirements for traceability.
- Checkpoints ensure incremental validation between crate layers.
- Property tests (Properties 1–39) validate universal correctness invariants; election
  and replication properties run over the `SimCluster` harness, others run directly.
- Unit/example, integration, and smoke/structural tests cover the non-property
  behaviors described in the design's Testing Strategy.
- Each property test uses `proptest` with a minimum of 100 iterations and is tagged
  `// Feature: vela-streaming-platform, Property N`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1", "3.1"] },
    { "id": 2, "tasks": ["2.2", "3.2"] },
    { "id": 3, "tasks": ["2.3", "3.3", "3.4", "3.5", "3.6", "3.7", "3.8", "3.9", "5.1"] },
    { "id": 4, "tasks": ["5.2", "10.1"] },
    { "id": 5, "tasks": ["6.1", "10.2", "11.1"] },
    { "id": 6, "tasks": ["6.2", "6.3", "6.4", "6.5", "6.6", "6.7", "10.3", "10.4", "10.5", "10.6", "10.7", "10.8", "11.2", "11.3"] },
    { "id": 7, "tasks": ["8.1", "10.9", "10.10", "10.11"] },
    { "id": 8, "tasks": ["8.2", "8.3", "8.4", "8.5", "8.6", "8.7", "8.8", "8.9", "11.4"] },
    { "id": 9, "tasks": ["11.5", "11.6", "12.1"] },
    { "id": 10, "tasks": ["11.7", "11.8", "11.9", "11.10", "11.11", "12.2", "12.3"] },
    { "id": 11, "tasks": ["11.12", "11.13", "11.14", "11.15", "14.1"] },
    { "id": 12, "tasks": ["14.2"] },
    { "id": 13, "tasks": ["14.3", "14.5"] },
    { "id": 14, "tasks": ["14.4", "14.6", "14.7", "14.8", "16.1"] },
    { "id": 15, "tasks": ["16.2"] },
    { "id": 16, "tasks": ["16.3", "16.4", "17.1", "18.1"] },
    { "id": 17, "tasks": ["17.2", "18.2"] },
    { "id": 18, "tasks": ["18.3", "18.4", "18.5"] }
  ]
}
```
