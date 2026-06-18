# Implementation Plan: Cross-Node Metadata via a Dedicated Raft Group

## Overview

This plan finishes the original design's **Option A — a dedicated, cluster-wide
metadata Raft group** keyed `("__meta", 0)`, replacing the bespoke
epoch/ack/laggard/`SyncMetadata`-push machinery. The work is Raft-native: the
metadata group reuses `vela-raft`, the existing durable `__meta` WAL, and the
existing `AppendEntries` / `RequestVote` transport (no proto changes).

Sequencing reflects two review constraints:

- **Reconciliation runs off the Raft loop** (apply stays fast; a separate task
  spawns/stops partition drivers) so a slow reconcile never stalls metadata
  heartbeats or triggers a spurious election.
- **Core multi-node tests run BEFORE the bespoke machinery is removed**, so
  removal is gated on green end-to-end evidence rather than reverting blind.

Optional test sub-tasks are marked `*`. The safety-critical tests that justify
the architectural pivot — convergence, one-leader-per-term, durable recovery,
cross-node produce/consume, follower catch-up, single-node bootstrap — are
**not** optional. Requirement numbers refer to the revised (Raft-native)
`requirements.md`; properties P1–P6 are from `design.md`.

## Tasks

- [x] 1. Metadata commit-apply seam (`CommitSink` / `MetadataSink`)
  - [x] 1.1 Introduce the commit-apply seam; apply is fast and reconcile is off-loop
    - Add a `CommitSink` seam in `crates/vela-server/src/driver.rs` (or a new
      metadata-driver module) per design §2: `apply_committed(&mut self,
      entries: &[LogEntry])`, applied in ascending index order, exactly once.
    - Keep the partition path as a `RecordSink` wrapping the existing
      offset-assigning logic so `PartitionDriver` behavior is unchanged.
    - Implement `MetadataSink`: for each committed `PayloadKind::Cluster` entry,
      decode via `convert::cluster_command_from_bytes`, apply with
      `vela_core::apply_command` to the shared served `ClusterMetadata`, then
      **enqueue a reconcile signal** (channel/notify). Do NOT reconcile inline —
      apply must not block the Raft loop (H1). Ignore `Noop`/non-`Cluster`
      entries. On decode failure, record a structured error and leave the served
      catalogue unchanged (no partial/default apply).
    - _Requirements: 5.1, 5.2, 5.4, 10.4_

  - [x] 1.2 Write property test for in-order, idempotent metadata apply
    - **Property 1: In-order, idempotent metadata apply**
    - Drive arbitrary committed prefixes with redelivery/re-replication; assert
      the served catalogue is identical and each entry applied once in order.
    - **Validates: Requirements 5.1, 5.2, 5.4**

  - [x] 1.3 Write property test for convergence to one catalogue
    - **Property 3: Convergence to one catalogue** (non-optional: validates the
      core fix that justified the pivot)
    - Two independent `MetadataSink`s applying the same committed log prefix hold
      identical served catalogues at the same commit index.
    - **Validates: Requirements 5.3**

  - [x] 1.4 Write unit tests for sink edge cases
    - Leader `Noop` / non-`Cluster` entries ignored; an undecodable `Cluster`
      payload leaves the served catalogue unchanged.
    - _Requirements: 5.1, 5.4, 10.4_

- [x] 2. Partition_Reconciler as an off-loop task
  - [x] 2.1 Implement the reconciler diff and its own task
    - Add `crates/vela-server/src/reconciler.rs`. Given the served
      `ClusterMetadata`, the running `partitions` set, and `self_id`, compute
      `desired = {(topic, p.index) : self in p.replicas}` and `running =
      partitions \ {("__meta", 0)}`.
    - Spawn drivers for `desired \ running` (reuse `NodeShared::spawn_partition`,
      which registers peer replica addresses before the driver issues RPCs), stop
      drivers for `running \ desired` (reuse `stop_partition`), leave the
      intersection untouched, and never touch `("__meta", 0)`.
    - Run the reconciler as its own task consuming the reconcile signal from task
      1.1 (coalescing pending signals into one pass), so it runs OFF the metadata
      Raft loop (H1). A durable-log-open failure leaves that partition unstarted
      with a structured error and reconciliation continues over the rest.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_

  - [x] 2.2 Write property test for reconciler diff correctness
    - **Property 2: Reconciler diff correctness**
    - Over random catalogues / running sets / node identities: spawn set =
      `desired \ running`, stop set = `running \ desired`, intersection
      untouched, `("__meta", 0)` never started or stopped.
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.5, 6.6**

  - [x] 2.3 Write unit test for the spawn-failure skip path
    - A partition whose durable log fails to open is left unstarted and recorded
      as an error while remaining partitions still reconcile.
    - _Requirements: 6.7_

- [x] 3. Cluster_Command / snapshot serialization round-trip
  - [x] 3.1 Verify and extend the metadata-log codec
    - Confirm `convert::cluster_command_{to,from}_bytes` encodes every variant and
      all carried values (CreateTopic: name, full partition list with index,
      ordered Replica_Set, log backend; DeleteTopic: name), and the catalogue
      snapshot codec is faithful. Add encoding for any missing field.
    - _Requirements: 10.1, 10.3, 10.4_

  - [x] 3.2 Write property test for serialization round-trip
    - **Property 4: Serialization round-trip**
    - For all `ClusterCommand` values, encode→decode is identity; for all
      catalogue snapshots likewise.
    - **Validates: Requirements 10.2, 10.3**

- [x] 4. Checkpoint
  - Ensure all tests pass; ask the user if questions arise.

- [x] 5. Drive `__meta/0` as a multi-node Raft group at startup
  - [x] 5.1 Recover `__meta/0` with the real peer set and spawn its driver
    - In `node.rs` `NodeShared::new`, recover `__meta/0` durably (existing
      `metadata_data_path`, `SyncPolicy::Always`) but pass the real peer set
      (`raft_node_id` of every other configured node) so it is a multi-node voter
      set. A single-node deployment (no peers) yields a 1-voter group that
      self-elects through the normal election path (no `BootstrapClock`).
    - Spawn an async driver task for `__meta/0` wired with `TimerClock` and
      `GrpcTransport::new("__meta", 0, self_id, pool, tx)`, register its
      `DriverHandle` so `node.handle("__meta", 0)` resolves it, and register every
      metadata voter's address in the `PeerPool`. Confirm inbound metadata Raft
      RPCs route through the existing `VelaPeerService::dispatch_rpc` unchanged.
    - Recovery must not silently misread an old single-node `__meta` log as a
      multi-node one — read it compatibly or fail fast (G4).
    - _Requirements: 1.1, 1.2, 1.4, 2.1, 2.4, 9.1_

  - [x] 5.2 Wire the metadata sink in and reconcile after recovery
    - Drive `__meta/0` through the `MetadataSink` (task 1) and the off-loop
      reconciler (task 2). Run one reconciliation after recovery so partition
      drivers start for every recovered partition whose Replica_Set contains this
      node.
    - _Requirements: 5.1, 6.1, 9.2, 9.3_

  - [x] 5.3 Add a simulation test for metadata election safety
    - **Property 5: One metadata leader per term** (non-optional)
    - Reuse the deterministic Raft simulation harness against the metadata group;
      assert at most one leader per term and votes granted only to an
      at-least-as-up-to-date candidate.
    - **Validates: Requirements 2.2, 2.3**

- [x] 6. Leader-routed propose path for create/delete
  - [x] 6.1 Add the `ProposeCluster` driver command and await-commit logic
    - Add `DriverCommand::ProposeCluster { command, reply }` to the metadata
      driver. If not `Role::Leader`, reply `NotLeader { leader }` with the
      replica's known current leader. Otherwise append the encoded `Cluster`
      payload, register the target index as pending (reuse the existing
      `Pending`/commit-timeout pattern), and resolve on commit or with
      `CommitTimeout` after `COMMIT_TIMEOUT_MS`.
    - Document `CommitTimeout` as an **indeterminate** outcome (the entry may
      still commit under a new leader), not a failure (H2).
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 4.1_

  - [x] 6.2 Make `create_topic` / `delete_topic` leader-routed, idempotent proposals
    - In `node.rs`, replace the inline single-node commit with: validate + assign
      replicas on the leader only (`crates/vela-core/src/topic.rs`), propose via
      `ProposeCluster`, await commit, then read the applied topic from the served
      view. `delete_topic` mirrors this (validate → propose `DeleteTopic` → await).
    - Make admin **idempotent on topic name** (H2): re-creating an existing topic
      is a no-op success / `TopicExists`; re-deleting an absent topic is a no-op
      success — so a client retrying after a `CommitTimeout` cannot corrupt the
      catalogue.
    - Move partition-driver spawning OUT of the RPC handler; it now happens in the
      reconciler on commit, so it runs on every replica node, not just the origin.
    - _Requirements: 3.1, 3.4, 3.5, 4.1, 4.3, 6.1, 6.2_

  - [x] 6.3 Map `NotLeader` / no-leader in the client service
    - In `service.rs`, have `create_topic` / `delete_topic` call the node methods
      and translate `NotLeader { leader }` into the existing redirect status (with
      the metadata leader hint), and a no-elected-leader condition into a "no
      metadata leader available" error rather than committing locally.
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 6.4 Write unit tests for the propose path
    - A non-leader proposal returns `NotLeader` with the hint and commits nothing;
      a proposal that does not commit within the deadline returns `CommitTimeout`;
      re-creating an existing topic and re-deleting an absent topic are idempotent.
    - _Requirements: 3.4, 3.5, 4.1, 4.2_

- [x] 7. Live-leader routing for FindLeader / produce / consume
  - [x] 7.1 Surface each driver's known current leader
    - In `driver.rs`, expose the replica's known leader (own id when
      `Role::Leader`, else the `leader_id` last learned from `AppendEntries`) via
      `DriverCommand::KnownLeader { reply }` or a shared atomic updated on
      role/leader change.
    - _Requirements: 8.1, 8.2_

  - [x] 7.2 Route produce/consume/FindLeader by the live leader
    - In `service.rs`, use the driver's known leader (not the metadata `leader`
      field) for the `NotLeader` hint on produce/consume and for `FindLeader`;
      reply redirect/no-leader as appropriate. Treat any metadata `leader` field
      as a non-authoritative initial hint only.
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

  - [x] 7.3 Write unit tests for live-leader routing
    - Produce/consume to a non-leader replica redirect to the live leader;
      `FindLeader` returns the live leader or indicates none; the stale metadata
      leader field is not used once a partition has elected a leader.
    - _Requirements: 8.2, 8.3, 8.4_

- [x] 8. Checkpoint
  - Ensure all tests pass; ask the user if questions arise.

- [x] 9. Core multi-node integration tests (gate removal on these)
  - [x] 9.1 Single-node bootstrap smoke test
    - A 1-voter `__meta/0` self-elects through the normal election path (no
      `BootstrapClock`); create → produce → consume works on a single node (H3).
    - _Requirements: 1.1, 2.4, 3.4_

  - [x] 9.2 Admin rejected before a metadata leader exists
    - Before `__meta/0` has elected a leader, `CreateTopic` / `DeleteTopic` fail
      cleanly with the "no metadata leader available" error and commit nothing —
      no hang, no commit-nowhere (H3).
    - _Requirements: 4.2_

  - [x] 9.3 Create → cross-node produce/consume with leader redirect
    - Stand up a 3-node in-process cluster; create a topic on a follower (exercise
      the `NotLeader` redirect to the metadata leader); produce by key and
      consume, asserting the record round-trips through a partition whose leader
      is a different node.
    - _Requirements: 4.1, 4.3, 5.1, 7.1, 7.2, 7.4, 7.5_

  - [x] 9.4 Restart recovery of the full catalogue
    - **Property 6: Durable recovery of the full catalogue** (non-optional)
    - Commit several topics, restart a node, assert it recovers the full committed
      catalogue (including peer-originated topics) from its durable metadata log
      and re-spawns a driver for every recovered partition whose Replica_Set
      contains it.
    - **Validates: Requirements 9.2, 9.3**

  - [x] 9.5 Follower catch-up
    - Hold one node down during creates, bring it up, assert the metadata leader
      brings its metadata log up to date via `AppendEntries` and the node
      converges and reconciles.
    - _Requirements: 3.6, 9.4_

  - [x] 9.6 Apply-on-every-node and driver presence
    - After a create commits, `ListTopics` on every node shows the topic and each
      replica node runs the expected partition drivers.
    - _Requirements: 5.3, 6.1_

  - [x] 9.7 Concurrent admin from different nodes converges
    - Two concurrent creates issued at different nodes both serialize through the
      metadata leader, both commit, and every node converges to the same
      catalogue (captures the capability the pivot gained — G3).
    - _Requirements: 3.1, 5.3_

  - [x] 9.8 Delete propagation stops drivers on every node
    - Delete a topic; assert it commits, is applied on every node, and each node
      stops the topic's drivers.
    - _Requirements: 6.2_

- [x] 10. Checkpoint — gate removal on green core integration
  - Tasks 9.1–9.5 must be passing before starting task 11 (removal). Ask the user
    if questions arise.

- [x] 11. Remove the bespoke propagation machinery (after task 10)
  - [x] 11.1 Remove ack/laggard tracking from the controller
    - In `crates/vela-core/src/metadata.rs`, delete `record_ack`, `acked`,
      `laggards`, `confirm_delete_propagation`, the `acks` field, and the
      `epoch`-as-propagation-version contract (keep `epoch` only as a harmless
      applied-change counter if still referenced). Keep `step`, `apply`,
      `metadata`, `recover_durable`, `find_leader`, and group hosting.
    - _Requirements: 1.3_

  - [x] 11.2 Remove the `SyncMetadata` adopt logic and propagation deadline
    - In `service.rs`, remove `sync_metadata`'s adopt-fresher-snapshot logic and
      any use of `METADATA_PROPAGATION_TIMEOUT_MS`. Leave the RPC defined as a
      reserved no-op off the commit path (metadata reaches every node via
      `AppendEntries`).
    - _Requirements: 1.3_

  - [x] 11.3 Remove the single-node inline commit path
    - In `node.rs`, remove `propose_to_metadata_group` and the `BootstrapClock`
      single-node inline election/commit, superseded by the driver propose path.
      The single-node smoke test (9.1) guards that 1-voter bootstrap still works.
    - _Requirements: 1.1, 1.4_

  - [x] 11.4 Update or prune obsolete tests
    - Remove/replace unit tests asserting ack/laggard/`SyncMetadata`-adopt
      behavior so the suite reflects the single-consensus design.
    - _Requirements: 1.3_

- [x] 12. Periodic reconciliation retry
  - [x] 12.1 Wire a periodic reconcile tick
    - Reuse the membership cadence to re-poke the off-loop reconciler periodically
      so a partition left unstarted by a transient log-open failure is retried
      until it starts or is no longer assigned. Reconciliation is idempotent, so
      repeated runs are safe.
    - _Requirements: 6.8_

  - [x] 12.2 Write a unit test for retry of an unstarted partition
    - A partition that failed an initial start is re-attempted on the next tick
      and starts once its log can be opened, and is not retried once unassigned.
    - _Requirements: 6.8_

- [x] 13. Final checkpoint
  - Ensure the whole workspace builds, `cargo fmt --check` and
    `cargo clippy --all-targets -- -D warnings` are clean, and all tests pass.

## Notes

- Tasks marked `*` are optional test sub-tasks. The safety-critical tests are
  NOT optional: 1.3 (P3 convergence), 5.3 (P5 one-leader-per-term), 9.1/9.2
  (single-node + admin-before-leader), 9.3 (cross-node produce/consume), 9.4
  (P6 durable recovery), 9.5 (follower catch-up).
- **Removal (task 11) is gated on green core integration (tasks 9.1–9.5)** so the
  bespoke path is retired only once the new path is proven end-to-end (S2).
- Reconciliation runs OFF the metadata Raft loop (H1); apply only updates the
  served catalogue and signals the reconciler.
- `CommitTimeout` is indeterminate, not failure; admin is idempotent on topic
  name so retries are safe (H2).
- No proto/wire changes; reuse `vela-raft`, the durable `__meta` WAL, and the
  existing `AppendEntries`/`RequestVote` transport.

### Deferred follow-ons (tracked, not in this plan)

- **G1 — metadata log compaction / `InstallSnapshot`.** The `__meta` log grows
  unbounded and a catching-up node replays full history; the small derived
  catalogue is the natural first `InstallSnapshot` customer (Raft §7).
- **G2 — metadata voter subset at scale.** All-nodes-as-voters is right for 3–5
  nodes; a fixed odd subset (3/5) is the answer when clusters grow.

## Task Dependency Graph

Same-file tasks are kept in separate waves to avoid edit conflicts: `driver.rs`
(1.1, 6.1, 7.1), `node.rs` (5.1, 5.2, 6.2, 11.3), `service.rs` (6.3, 7.2, 11.2),
`reconciler.rs` (2.1, 12.1), `metadata.rs` (11.1). Removal (wave 7) runs only
after the core integration tests (wave 6) pass.

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1", "3.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "1.4", "2.2", "2.3", "3.2", "5.1"] },
    { "id": 2, "tasks": ["5.2", "6.1", "5.3"] },
    { "id": 3, "tasks": ["6.2", "7.1"] },
    { "id": 4, "tasks": ["6.3", "7.2", "6.4"] },
    { "id": 5, "tasks": ["7.3"] },
    { "id": 6, "tasks": ["9.1", "9.2", "9.3", "9.4", "9.5", "9.6", "9.7", "9.8"] },
    { "id": 7, "tasks": ["11.1", "11.2", "11.3", "11.4"] },
    { "id": 8, "tasks": ["12.1", "12.2"] }
  ]
}
```
