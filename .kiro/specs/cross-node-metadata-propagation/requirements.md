# Requirements Document

## Introduction

Vela runs **one Raft group per partition per topic**. The topic catalogue that
binds those groups together — which topics exist, how many partitions each has,
which nodes replicate each partition, and each topic's log backend — lives in
`ClusterMetadata`. That catalogue must itself be **agreed across the cluster**.

The original `vela-streaming-platform` design already decided how: in its
*Cluster Metadata Management* section it weighed a dedicated metadata Raft group
against a designated coordinator node and chose

> **Option A — a dedicated metadata Raft group** … It keeps the system to a single
> consensus mechanism, gives atomic topic create/delete via committed
> `ClusterCommand`s, … reuses the consensus we already build and test; no second
> mechanism.

The implementation **diverged** from that decision. Each node today recovers its
*own*, single-node metadata group
(`MetadataController::recover_durable(self_raft_id, Vec::new(), …)` in
`crates/vela-server/src/node.rs`), stepped inline by a `BootstrapClock` and never
replicated over the network. `create_topic` / `delete_topic` commit only to that
node-local group. A parallel, bespoke propagation protocol grew up to compensate
— `MetadataController::record_ack` / `laggards` / `confirm_delete_propagation`,
a `ClusterMetadata.epoch` counter, and a `SyncMetadata` snapshot push with a 5 s
acknowledgement deadline — effectively a *second* consensus mechanism, which is
exactly what Option A was chosen to avoid.

The consequences are the bug this feature exists to fix:

1. A topic created against one node is unknown to every other node, so the node
   assigned a partition's leadership returns "topic not found" on produce.
2. Peer replicas never learn they were assigned a partition, so they never spawn
   their partition drivers. A partition's Raft group (replication factor 3) can
   therefore never assemble a majority, elect a leader, or replicate.

**This feature finishes the originally-designed Option A.** It runs the metadata
catalogue as a single, dedicated, cluster-wide Raft group — `__meta` / partition
0 — whose voters are all configured nodes, reusing `vela-raft` and the existing
`VelaPeer` `AppendEntries` / `RequestVote` transport exactly as the per-partition
groups do. Topic-admin changes become `ClusterCommand` entries in that group's
replicated log; once **committed** (replicated to a majority, per the Raft paper),
every node applies them to its served catalogue in log order and **reconciles**
its locally-hosted partition drivers — spawning a driver for every partition it
now replicates and stopping drivers for partitions that are gone or no longer
assigned to it. With every replica of a partition running a driver, the
partition's Raft group can reach a majority, elect a leader, and serve
produce/consume end-to-end across the cluster.

The bespoke epoch / ack / laggard / `SyncMetadata`-push machinery is **removed**
in favor of Raft's own log replication and commit semantics. There is one
consensus mechanism in the system, used for both partition data and cluster
metadata.

### Reference: the Raft paper

All consensus behavior in this spec follows *In Search of an Understandable
Consensus Algorithm (Extended Version)* (Ongaro & Ousterhout), in
`context/raft.pdf`. Sections referenced below:

- **§5.1 Raft basics** — terms, the leader/follower/candidate roles, and the
  `AppendEntries` / `RequestVote` RPCs.
- **§5.2 Leader election** — randomized election timeouts; at most one leader per
  term.
- **§5.3 Log replication** — the leader appends and replicates entries; an entry
  is *committed* once stored on a majority; followers apply committed entries to
  their state machine in log order; the leader retries `AppendEntries` until every
  follower converges; servers persist log, `currentTerm`, and `votedFor`.
- **§5.4 Safety** — the election restriction (§5.4.1) and the rule for committing
  entries from previous terms (§5.4.2); Leader Completeness and State Machine
  Safety.
- **§7 Log compaction** — snapshotting a Raft log; how a lagging follower is
  brought up to date.
- **§8 Client interaction** — clients address the leader; a request that reaches a
  non-leader is redirected to the leader.

### Locked design decisions (constraints, not open for this spec)

1. **Mechanism — Option A.** Cluster metadata is agreed through **one dedicated
   cluster-wide Raft group** (`__meta` / partition 0), reusing `vela-raft` and the
   existing peer transport. This is the original design's decision; this feature
   implements it.
2. **One consensus mechanism.** The bespoke epoch / ack / laggard /
   `SyncMetadata`-push agreement protocol is removed. Metadata agreement uses Raft
   commit (replication to a majority), not a separate acknowledgement protocol.
3. **All nodes are metadata voters.** For this milestone the metadata group's
   voter set is exactly the statically configured node set; every node hosts a
   metadata replica and therefore receives committed `ClusterCommand`s directly
   via `AppendEntries`. No post-commit fan-out to non-voters is required.
4. **Partition leadership is not stored authoritatively in metadata.** The
   authoritative leader of a partition is the live Raft-elected leader reported by
   its Partition_Driver. Metadata records replica assignments; it does not commit
   a metadata change on every partition election. (Original design: per-partition
   leadership is "learned locally … rather than requiring a metadata commit on
   every election.")
5. **Unified node identity** (`id@host:port` → deterministic `raft_node_id`, via
   `crates/vela-server/src/registry.rs`) is a completed prerequisite, so the
   metadata group's voters address one another consistently.
6. **Durable metadata log.** The metadata group's log is a durable WAL (the
   `durable-wal` feature), so each node persists its metadata log, term, and vote
   and recovers committed metadata across restart (Raft §5.3).

### Non-goals

- **Dynamic metadata membership** (Raft §6 joint consensus). The metadata voter
  set is the static configured node set; adding/removing voters at runtime is out
  of scope.
- **A metadata group over a subset of nodes** with post-commit fan-out to
  non-voters. All nodes are voters this milestone (decision 3); the subset variant
  and any `SyncMetadata` fan-out it would require are deferred.
- **Rebalancing or re-assigning** an existing topic's partitions when membership
  changes.
- **Read-only/linearizable metadata reads through the leader** (Raft §8 read-only
  optimization). Catalogue reads (`ListTopics` / `DescribeTopic`) are served from a
  node's locally applied view; they may briefly trail the leader.
- **Replacing per-partition consensus.** Partition data Raft groups are unchanged;
  this feature only changes how the *metadata* group is run.

## Glossary

- **Vela**: the distributed event-streaming platform; the cluster as a whole.
- **Node**: a single `velad` process and its shared state (`NodeShared`).
- **Member**: a node recorded in `ClusterMetadata`, with an id and transport
  address.
- **Cluster_Metadata**: the catalogue — the Member list and the topics (each with
  its partitions, per-partition Replica_Set, and log backend).
- **Metadata_Group**: the single, dedicated, cluster-wide Raft group (`__meta` /
  partition 0) through which Cluster_Metadata changes are agreed. Its voters are
  all configured Nodes; each Node hosts one Metadata_Replica.
- **Metadata_Replica**: one Node's replica of the Metadata_Group (its Raft state
  and replicated metadata log).
- **Metadata_Leader**: the Node currently elected leader of the Metadata_Group for
  the current term.
- **Cluster_Command**: a replicated metadata mutation (CreateTopic, DeleteTopic),
  appended as a `PayloadKind::Cluster` entry to the Metadata_Group's log.
- **Committed**: a Metadata_Group log entry that has been stored on a majority of
  the Metadata_Group's voters and may therefore be applied (Raft §5.3).
- **Apply**: a Node updating its served Cluster_Metadata from a Committed
  Cluster_Command, in log order (Raft state-machine apply, §5.3).
- **Partition_Reconciler**: the component that aligns a Node's running
  Partition_Drivers with its served Cluster_Metadata after an Apply.
- **Partition_Driver**: the asynchronous task hosting one partition's Raft replica
  on a Node (`DriverHandle` in the partitions table).
- **Replica_Set**: the set of Member ids assigned to replicate a given partition.
- **Term**: Raft's monotonic logical clock; at most one Metadata_Leader per Term
  (Raft §5.1, §5.2).

## Requirements

### Requirement 1: Agree cluster metadata through one dedicated cluster-wide Raft group

**User Story:** As a Vela maintainer, I want the topic catalogue agreed by the same
Raft we already use for partitions, so that the cluster has one consensus
mechanism and no bespoke agreement protocol.

#### Acceptance Criteria

1. THE cluster SHALL run exactly one Metadata_Group, identified by the reserved topic `__meta` and partition index 0, agreed using the same `vela-raft` implementation and the same `VelaPeer` `AppendEntries` / `RequestVote` transport used by partition Raft groups (Raft §5.1).
2. THE voting membership of the Metadata_Group SHALL be exactly the set of statically configured Nodes, and each such Node SHALL host one Metadata_Replica.
3. THE cluster SHALL agree every Cluster_Metadata change solely through the Metadata_Group's replicated log, and SHALL NOT use any separate epoch-acknowledgement or snapshot-push protocol to agree Cluster_Metadata.
4. WHEN a Node starts, THE Node SHALL drive its Metadata_Replica on the same asynchronous election/heartbeat timers and transport as a partition replica, rather than stepping it only inline without network replication.

### Requirement 2: Elect a single metadata leader safely

**User Story:** As a Node, I want the metadata group to elect one leader per term
with the standard Raft safety guarantees, so that metadata never diverges.

#### Acceptance Criteria

1. THE Metadata_Group SHALL elect a leader using `RequestVote` with randomized election timeouts (Raft §5.2).
2. THE Metadata_Group SHALL have at most one Metadata_Leader in any single Term (Raft §5.2).
3. WHEN a Metadata_Replica receives a `RequestVote`, THE Metadata_Replica SHALL grant its vote only if the candidate's metadata log is at least as up-to-date as its own, where "up-to-date" is compared by last log term then last log index (Raft §5.4.1).
4. WHILE a majority of the Metadata_Group's voters are running and mutually reachable, THE Metadata_Group SHALL maintain an elected Metadata_Leader (Raft §5.2).
5. IF fewer than a majority of the Metadata_Group's voters are running or mutually reachable, THEN THE Metadata_Group SHALL have no Metadata_Leader and SHALL NOT commit new Cluster_Commands until a majority is restored.

### Requirement 3: Replicate and commit topic-admin changes as metadata log entries

**User Story:** As an operator, I want creating or deleting a topic to be a
committed Raft log entry, so that the change is atomic and agreed before it takes
effect.

#### Acceptance Criteria

1. WHEN the Metadata_Leader accepts a CreateTopic or DeleteTopic request, THE Metadata_Leader SHALL append a corresponding Cluster_Command entry to the Metadata_Group's log and replicate it via `AppendEntries` (Raft §5.3).
2. THE Metadata_Group SHALL treat a Cluster_Command entry as Committed only once it is stored on a majority of the Metadata_Group's voters (Raft §5.3).
3. WHEN the Metadata_Leader has appended an entry in its current Term and that entry is stored on a majority, THE Metadata_Leader SHALL advance its commit index to include that entry (Raft §5.3, §5.4.2).
4. THE Vela cluster SHALL return success for a CreateTopic or DeleteTopic operation only after its Cluster_Command entry is Committed.
5. IF a Cluster_Command entry is appended but not Committed within the operation's commit deadline, THEN THE Vela cluster SHALL return a commit-timeout error and SHALL NOT report the operation as succeeded.
6. THE Metadata_Leader SHALL retry `AppendEntries` to any follower whose log lags until that follower's metadata log matches the leader's, so a follower that missed entries eventually converges (Raft §5.3).

### Requirement 4: Route topic-admin requests to the metadata leader

**User Story:** As a client, I want my topic-admin request to reach whichever node
leads the metadata group, so that a request to a follower is not silently dropped
or applied locally.

#### Acceptance Criteria

1. IF a Node that is not the Metadata_Leader receives a CreateTopic or DeleteTopic request, THEN THE Node SHALL NOT commit the change locally and SHALL redirect the request to the current Metadata_Leader, identifying that leader where it is known (Raft §8).
2. WHILE the Metadata_Group has no elected leader, THE Vela cluster SHALL reject a CreateTopic or DeleteTopic request with an error indicating no metadata leader is currently available rather than committing it (Raft §8).
3. WHEN a redirected topic-admin request reaches the Metadata_Leader, THE Metadata_Leader SHALL process it as in Requirement 3.

### Requirement 5: Apply committed metadata on every node deterministically

**User Story:** As a Node, I want to apply the committed metadata log the same way
every other node does, so that all nodes converge to one catalogue.

#### Acceptance Criteria

1. WHEN a Cluster_Command entry becomes Committed, THE Node SHALL apply it to its served Cluster_Metadata in ascending log-index order (Raft §5.3).
2. THE Node SHALL apply each Committed Cluster_Command exactly once, so re-delivery or re-replication of an already-applied entry does not change the served Cluster_Metadata (Raft §5.3 state-machine apply is idempotent over the committed log).
3. WHEN two Nodes have applied the Metadata_Group's log up to the same commit index, THE two Nodes SHALL hold identical served Cluster_Metadata topic catalogues (Raft State Machine Safety, §5.4.3).
4. THE Node SHALL apply Cluster_Commands deterministically, such that applying the same committed log prefix always yields the same served Cluster_Metadata.

### Requirement 6: Reconcile locally-hosted partition drivers after applying metadata

**User Story:** As a replica Node, I want my partition drivers to start and stop as
committed metadata is applied, so that every partition I replicate is actually
running.

#### Acceptance Criteria

1. WHEN a Node applies a Committed Cluster_Command that changes its served topic catalogue, THE Partition_Reconciler SHALL start exactly one Partition_Driver for each partition whose Replica_Set contains the Node and for which no Partition_Driver is currently running.
2. WHEN a Node applies a Committed Cluster_Command that changes its served topic catalogue, THE Partition_Reconciler SHALL stop each currently running Partition_Driver whose (topic, partition) is absent from the served catalogue or whose Replica_Set no longer contains the Node.
3. WHILE a partition's Replica_Set in the served catalogue contains the Node and that partition's Partition_Driver is already running, THE Partition_Reconciler SHALL leave that Partition_Driver running without restarting it.
4. WHEN the Partition_Reconciler starts a Partition_Driver for a partition, THE Partition_Reconciler SHALL register the transport address of every other Member in that partition's Replica_Set before that Partition_Driver issues any Raft RPC for that partition.
5. THE Partition_Reconciler SHALL start Partition_Drivers only for partitions whose Replica_Set contains the Node.
6. THE Partition_Reconciler SHALL reconcile only client topic partitions and SHALL never start or stop the Metadata_Group.
7. IF a durable partition's log cannot be opened during reconciliation, THEN THE Partition_Reconciler SHALL leave that partition's Partition_Driver unstarted, SHALL record an error indication identifying that (topic, partition), and SHALL continue reconciling the remaining partitions.
8. WHILE a partition's Replica_Set in the served catalogue contains the Node but that partition has no running Partition_Driver because an earlier start attempt failed, THE Partition_Reconciler SHALL periodically re-attempt starting that Partition_Driver until it starts or the partition is no longer assigned to the Node.

### Requirement 7: Cross-node partitions reach quorum and serve produce/consume

**User Story:** As a producer and consumer, I want partitions replicated across
nodes to elect a leader and accept traffic, so that produce and consume work
end-to-end with replication.

#### Acceptance Criteria

1. WHEN a CreateTopic command for a topic has been Committed and applied on every Member of one of its partition's Replica_Set, THE partition's Raft group SHALL have a running Partition_Driver on every Member of its Replica_Set.
2. WHILE a majority of a partition's Replica_Set is running its Partition_Driver and is mutually reachable, THE partition's Raft group SHALL elect exactly one leader from among the Members running a Partition_Driver (Raft §5.2).
3. IF fewer than a majority of a partition's Replica_Set is running its Partition_Driver, or the running Members are not mutually reachable, THEN THE partition's Raft group SHALL designate no leader, AND THE Vela cluster SHALL reject Produce and Consume requests for that partition with an error indicating no available leader while retaining any already-committed records.
4. WHEN a partition has an elected leader and the leader Member receives a Produce request for that partition, THE Vela cluster SHALL acknowledge the Produce request only after the record is committed by replication to a majority of the partition's Replica_Set (Raft §5.3).
5. WHEN a partition has an elected leader and committed records, AND the leader Member receives a Consume request for that partition, THE Vela cluster SHALL return that partition's committed records in ascending offset order and SHALL NOT return uncommitted records.

### Requirement 8: Use the live Raft-elected leader for partition routing

**User Story:** As a client, I want produce/consume routed to a partition's real
current leader, so that routing follows Raft failover rather than a stale
assignment.

#### Acceptance Criteria

1. THE authoritative current leader of a partition, for routing Produce and Consume and for answering FindLeader, SHALL be the live Raft-elected leader reported by that partition's Partition_Driver, not any leader value stored in Cluster_Metadata.
2. WHEN a client issues FindLeader for a partition, THE Vela cluster SHALL return that partition's live Raft-elected leader where one exists, and SHALL indicate no current leader otherwise.
3. IF a Produce or Consume request for a partition is received by a Member that is not that partition's current live leader while a leader exists, THEN THE Vela cluster SHALL respond with a redirection to that partition's current live leader Member rather than serving the request locally (Raft §8).
4. WHERE Cluster_Metadata carries any per-partition leader field, THE Vela cluster SHALL treat it only as a non-authoritative initial hint and SHALL NOT rely on it for routing once the partition's Raft group has elected a leader.

### Requirement 9: Persist and recover committed metadata across restart

**User Story:** As a Node, I want to recover the whole committed catalogue after a
restart, so that a restart never regresses the cluster-wide catalogue.

#### Acceptance Criteria

1. BEFORE a Metadata_Replica responds to an `AppendEntries` or `RequestVote` RPC, THE Metadata_Replica SHALL have durably persisted the affected metadata log entries, its current Term, and its vote (Raft §5.3).
2. WHEN a Node restarts, THE Node SHALL recover its Metadata_Replica's committed metadata log from durable storage and rebuild its served Cluster_Metadata from it, including topics originated by any Node, not only those it originated itself.
3. WHEN a Node finishes recovering its catalogue on restart, THE Partition_Reconciler SHALL start a Partition_Driver for every recovered partition whose Replica_Set contains the Node.
4. WHEN a recovered Metadata_Replica's log lags the Metadata_Leader, THE Metadata_Leader SHALL bring it up to date through `AppendEntries` (and, where the leader has compacted earlier entries, a snapshot install), after which the recovered Node's served catalogue matches the committed log (Raft §5.3, §7).
5. THE Metadata_Group SHALL retain every Committed Cluster_Command across the simultaneous failure of any minority of its voters (Raft Leader Completeness, §5.4).

### Requirement 10: Faithful serialization of replicated metadata

**User Story:** As a Node, I want metadata log entries and snapshots to be
reconstructed exactly, so that replication and recovery never corrupt the
catalogue.

#### Acceptance Criteria

1. THE Cluster_Command log-entry serialization SHALL encode the command variant and all of its carried values — for CreateTopic, the topic name, the complete partition list with each partition's index, ordered Replica_Set, and log backend; for DeleteTopic, the topic name.
2. WHEN a Cluster_Command is encoded to its Metadata_Group log payload and then decoded back, THE decoded command SHALL equal the original — the same variant carrying the same values (round-trip property).
3. WHEN a Cluster_Metadata snapshot is encoded to its wire/stored form and then decoded back, THE decoded snapshot SHALL equal the original — the same Members in the same order and the same topics, each with the identical partition list, Replica_Set ordering, and log backend (round-trip property).
4. IF a Metadata_Group log payload or snapshot cannot be decoded into a valid value, THEN the decoder SHALL record an error indication and SHALL leave the Node's served Cluster_Metadata unchanged by that payload rather than applying a partial or default value.
