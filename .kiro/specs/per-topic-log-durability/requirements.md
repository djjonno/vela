# Requirements Document

## Introduction

This feature wires the already-merged durable Write-Ahead Log
(`vela_log::DurableWal`) into the running Vela cluster and makes log durability
a **per-topic, client-selectable** property. Today `vela-core`'s
`PartitionReplica` hardcodes the in-memory log (`InMemoryLog`) and nothing in
the running server (`vela-server`) ever writes a WAL, so committed records do
not survive a process or container restart. The durable log already implements
the same `LogStorage` trait as `InMemoryLog`, and `vela-raft`'s
`RaftNode<S: LogStorage>` already takes its log by injection — so the work here
is composition and configuration, not new storage internals.

A topic declares its log backend at creation time through the Client API. The
choice is one of two backends: **`durable`** (the default) backed by
`DurableWal`, or **`in-memory`** backed by the existing `InMemoryLog`. That
choice flows from the client, through the `CreateTopic` RPC, into the replicated
cluster metadata, and is read back when a node spawns each partition replica so
that every replica of a partition agrees on, and constructs, the same backend.

Making durability real also requires two consensus-safety pieces that are not
satisfied by the log alone. First, Raft requires that a replica's hard state —
`current_term` and `voted_for` — be durable, because a node that forgets its
vote or term after a restart can violate the at-most-one-vote-per-term and
monotonic-term guarantees. For durable topics this hard state MUST be persisted
and restored; for in-memory topics it stays volatile, consistent with the
backend. Second, a restarted durable replica MUST pick up its commit position
from the recovered log and re-apply the committed entries to the partition state
machine, so that previously committed records are observable again after
restart.

Each node is given a data directory (the `VELA_DATA_DIR` environment variable)
under which durable partitions store their segments at a per-partition subpath.
Because topic names are namespaces that may contain filesystem-unsafe
characters, the on-disk path component derived from a topic name must be
sanitized or hashed, and the derivation must be stable across restarts so a
durable partition reopens its existing segments. The local Docker cluster mounts
a per-node named volume at the data directory so durable state survives
container restarts.

Durability extends beyond per-partition topic data to the topic catalogue
itself. The dedicated metadata Raft group — the infrastructure group that holds
the topic definitions including each topic's backend choice — is itself made
durable in this feature. It uses the Durable backend at a fixed, reserved path
beneath the node data directory, persists and restores its Raft hard state, and
performs the same commit-index recovery handoff as a durable client partition.
As a result a *full-cluster cold restart* — every node restarting with its data
directory reattached — recovers the previously-committed cluster metadata, so
durable topics are recognized as durable and their partitions reopen their
existing segments.

### Locked design decisions (constraints, not open for this spec)

The following were decided before this spec and are treated as fixed
constraints. They are recorded here so the requirements stay consistent with
them; they are **not** to be re-litigated.

1. **Granularity:** durability is a per-topic property, chosen via the Client
   API at topic-creation time, stored in cluster metadata, replicated, and
   agreed by every replica of a partition.
2. **Terminology:** two backends named `durable` (the default) and `in-memory`;
   `in-memory` maps to the existing `InMemoryLog`.
3. **Injection shape:** one concrete `LogStorage` type holds either backend and
   is injected into `PartitionReplica` at spawn time; `PartitionReplica` stops
   hardcoding `InMemoryLog`.
4. **Raft hard state** (`current_term`, `voted_for`) is persisted durably for
   the `durable` backend and restored on restart; it stays volatile for
   `in-memory`. The persistence mechanism (WAL manifest vs. sidecar) is a design
   decision.
5. **Recovery handoff:** a restarted durable replica picks up `commit_index`
   (and re-derives `last_applied`) from the recovered log and re-applies
   committed entries to the partition state machine.
6. **Per-node data directory:** `VELA_DATA_DIR`; durable topics store segments
   under a per-partition subpath; the topic path component is sanitized or
   hashed.
7. **Durable sync policy:** `Always` (the only consensus-safe policy).
8. **Docker:** docker-compose mounts a per-node named volume at the data
   directory and sets `VELA_DATA_DIR`.

### Assumptions and confirmed decisions

These are choices this document makes that are **new** (not covered by the
locked decisions). All four have been confirmed by the reviewer and are now
decided; each is stated as the constraint the requirements below encode.

- **A1 — `VELA_DATA_DIR` is required at node startup. (Confirmed.)** Because
  `durable` is the default backend, any node could be asked to host a durable
  partition. The node treats `VELA_DATA_DIR` like the other required
  configuration values and fails fast (non-zero exit, structured configuration
  error) when it is absent, rather than deferring the failure to the first
  durable spawn. This applies unconditionally, including to a node that will only
  ever host in-memory topics. (Requirement 6.)
- **A2 — Metadata-group durability is in scope. (Confirmed.)** In addition to
  making per-**partition** topic data durable, this feature makes the dedicated
  metadata Raft group (`__meta`) — which holds the topic definitions including
  each topic's backend choice — durable. The metadata group always uses the
  Durable backend (it is infrastructure, not a client-selectable topic),
  persists and restores its Raft_Hard_State, and performs the same commit-index
  recovery handoff as a durable client partition. Consequently the topic
  catalogue survives a *full-cluster cold restart*. (Requirements 16, 17, 18.)
- **A3 — Backend is observable via describe. (Confirmed.)** The
  topic-description surface (the `TopicInfo` wire type and the client
  `describe_topic`) reports a topic's backend, so the selection is testable and
  visible to operators. (Requirements 1.4, 2.4.)
- **A4 — Backend is immutable after creation. (Confirmed.)** A topic's backend is
  fixed at creation and cannot be changed for the topic's lifetime.
  (Requirement 3.3.)

### Non-Goals

- Changing the on-disk WAL format or the internal design of the `vela-log`
  durable-wal crate (specified by the `durable-wal` feature).
- Follower catch-up or `InstallSnapshot`.
- Migrating or converting existing topics between backends.

## Glossary

- **Vela**: The distributed event-streaming platform this feature targets.
- **Client_API**: The `vela-client` administration surface (`AdminClient`) used
  to create, delete, list, and describe topics.
- **CreateTopic_RPC**: The `CreateTopic` RPC on the `VelaClient` gRPC service,
  defined in `vela-proto`, by which a client requests topic creation.
- **Log_Backend**: A topic's selected log storage backend. Exactly one of two
  values: **Durable** or **In_Memory**.
- **Durable**: The Log_Backend backed by `vela_log::DurableWal`. The default when
  a create request does not specify a Log_Backend.
- **In_Memory**: The Log_Backend backed by the existing `vela_log::InMemoryLog`.
- **LogStorage**: The existing `vela-log` trait defining the append-only log
  seam (`append`, `append_entries`, `read`, `entry`, `last_index`, `term_at`,
  `commit_index`, `commit`, `revert`, `snapshot`, `flush`). Consensus depends on
  this trait, not on a concrete implementation.
- **Partition_Log**: The single concrete `LogStorage` type introduced by this
  feature that holds either a Durable or an In_Memory backend and dispatches the
  trait operations to whichever it holds.
- **Partition_Replica**: The `vela-core` `PartitionReplica` — one partition's
  `RaftNode` paired with its partition State_Machine, hosted on a node.
- **Raft_Node**: The `vela-raft` `RaftNode<S: LogStorage>` consensus state
  machine for one partition replica.
- **Raft_Hard_State**: The Raft persistent state required for safety:
  `current_term` and `voted_for`.
- **State_Machine**: The `vela-core` partition `StateMachine` that applies
  committed log entries in order and assigns committed records gap-free, 0-based
  offsets.
- **Cluster_Metadata**: The `vela-core` `ClusterMetadata` (membership, topics,
  partition/replica assignments, leaders, epoch), agreed through the dedicated
  metadata Raft group and propagated across the cluster.
- **Server**: The `vela-server` node daemon (`velad`) that wires gRPC to the
  core and owns the per-partition driver lifecycle.
- **Node_Config**: The validated `vela-server` configuration parsed from CLI
  flags and environment variables.
- **Data_Directory**: The per-node filesystem directory supplied via the
  `VELA_DATA_DIR` environment variable, under which all Durable partition logs
  on the node store their segments.
- **Partition_Data_Path**: The per-partition subpath beneath the Data_Directory
  at which one Durable partition replica's segments are stored, derived from the
  topic and partition (for example `<data_dir>/<topic-component>/<partition>/`).
- **Safe_Path_Component**: A single filesystem path component containing only
  characters drawn from a defined safe set (for example ASCII alphanumerics, `-`,
  and `_`), produced by sanitizing or hashing a topic name that may contain
  characters outside that set.
- **Sync_Policy**: The Durable backend's durability policy. **Always** forces
  buffered writes to stable storage before a mutating operation returns and is
  the only consensus-safe policy.
- **Committed_Record**: A produced record whose log entry has been committed by
  the partition's Raft group and assigned a 0-based offset by the State_Machine.
- **Metadata_Raft_Group**: The dedicated infrastructure Raft group (also called
  the Metadata_Group) that replicates and agrees the Cluster_Metadata. It is not
  a client-created topic; it is reserved infrastructure identified by the
  reserved name `__meta`.
- **Full_Cluster_Cold_Restart**: A restart in which every node in the cluster is
  stopped and then started again, each node reattaching its own Data_Directory
  (its named volume), with no node retaining volatile in-memory state across the
  restart.

## Requirements

### Requirement 1: Declaring a Topic's Log Backend Through the Client API

**User Story:** As a client developer, I want to choose a topic's log backend
when I create it, so that I can opt into durability per topic while getting
durability by default.

#### Acceptance Criteria

1. WHERE a caller specifies a Log_Backend of Durable or In_Memory when creating a topic, THE Client_API SHALL include that Log_Backend in the CreateTopic_RPC request.
2. WHERE a caller does not specify a Log_Backend when creating a topic, THE Client_API SHALL set the Log_Backend in the CreateTopic_RPC request to Durable.
3. THE Client_API SHALL accept exactly two Log_Backend values, Durable and In_Memory, and SHALL reject any other value as invalid before sending a CreateTopic_RPC request.
4. WHEN a topic is described through the Client_API, THE Client_API SHALL report the topic's Log_Backend.

### Requirement 2: Carrying the Log Backend Over the Wire

**User Story:** As a platform developer, I want the backend choice carried on
the create-topic RPC and the replicated create command, so that the server and
every replica act on the same selection.

#### Acceptance Criteria

1. THE CreateTopic_RPC request message SHALL carry the requested Log_Backend.
2. WHEN the Server receives a CreateTopic_RPC request whose Log_Backend is unspecified on the wire, THE Server SHALL treat the Log_Backend as Durable.
3. THE replicated create-topic cluster command SHALL carry the topic's Log_Backend so that every node applying the committed command records the same Log_Backend.
4. THE topic-description wire type SHALL carry the topic's Log_Backend.
5. WHEN the Server receives a CreateTopic_RPC request whose Log_Backend value is neither Durable nor In_Memory nor unspecified, THE Server SHALL reject the request with a validation error and SHALL NOT create the topic.

### Requirement 3: Recording and Replicating the Backend in Cluster Metadata

**User Story:** As a platform developer, I want each topic's backend stored in
replicated cluster metadata, so that the choice is agreed cluster-wide and
durable for the topic's lifetime.

#### Acceptance Criteria

1. THE Cluster_Metadata SHALL record exactly one Log_Backend for each topic.
2. WHEN the create-topic command for a topic commits, THE Cluster_Metadata SHALL associate that topic with the Log_Backend carried by the command, identically on every node that applies the command.
3. THE Log_Backend recorded for a topic SHALL remain unchanged for the lifetime of the topic.
4. WHERE a topic's recorded Log_Backend is Durable, every partition replica of that topic SHALL use the Durable backend; WHERE a topic's recorded Log_Backend is In_Memory, every partition replica of that topic SHALL use the In_Memory backend.

### Requirement 4: A Single Injected Log Type That Holds Either Backend

**User Story:** As a platform developer, I want one concrete log type that can be
either backend, so that the Raft node and partition replica stay generic over a
single injected log.

#### Acceptance Criteria

1. THE Partition_Log SHALL implement the LogStorage trait.
2. THE Partition_Log SHALL hold exactly one backend instance, either Durable or In_Memory.
3. WHEN a LogStorage operation is invoked on a Partition_Log, THE Partition_Log SHALL apply the operation to the backend it holds and return that backend's result unchanged.
4. WHEN a sequence of LogStorage operations is applied to a Partition_Log holding an In_Memory backend, and the identical sequence is applied to an InMemoryLog directly, THE Partition_Log SHALL return, for every operation, a result equal to the InMemoryLog's result and SHALL report equal observable state from `last_index`, `commit_index`, `term_at`, `entry`, `read`, and `snapshot`.

### Requirement 5: Selecting and Constructing the Backend at Partition Spawn

**User Story:** As a platform developer, I want each partition replica built with
the backend its topic declared, so that durability is actually wired into the
running cluster instead of always using the in-memory log.

#### Acceptance Criteria

1. WHEN the Server spawns a partition replica, THE Server SHALL construct the Partition_Log backend named by the topic's Log_Backend in Cluster_Metadata.
2. THE Partition_Replica SHALL receive its Partition_Log by injection at construction and SHALL NOT construct a fixed In_Memory backend internally.
3. WHERE the topic's Log_Backend is Durable, THE Server SHALL construct the Durable backend rooted at the partition's Partition_Data_Path.
4. WHERE the topic's Log_Backend is In_Memory, THE Server SHALL construct the In_Memory backend.

### Requirement 6: Node Data-Directory Configuration

**User Story:** As an operator, I want to configure where a node stores durable
data, so that durable partitions have a defined on-disk home.

#### Acceptance Criteria

1. THE Node_Config SHALL read the Data_Directory from the `VELA_DATA_DIR` environment variable.
2. IF `VELA_DATA_DIR` is not supplied when the node starts, THEN THE Server SHALL terminate startup with a configuration error and a non-zero exit code, reporting the missing Data_Directory.
3. WHEN `VELA_DATA_DIR` is supplied, THE Server SHALL use its value as the root Data_Directory for every Durable partition log hosted on the node.

### Requirement 7: Per-Partition Path Derivation and Name Sanitization

**User Story:** As a platform developer, I want a stable, collision-free on-disk
path for each durable partition, so that segments are isolated per partition and
reopened correctly after a restart even when topic names contain unsafe
characters.

#### Acceptance Criteria

1. WHEN the Server constructs a Durable backend for a partition, THE Server SHALL derive a Partition_Data_Path beneath the Data_Directory that is specific to that topic-and-partition pair.
2. THE Server SHALL derive the Partition_Data_Path such that two distinct topic-and-partition pairs never resolve to the same Partition_Data_Path.
3. WHERE a topic name contains characters outside the Safe_Path_Component set, THE Server SHALL convert the topic name into a Safe_Path_Component by sanitizing or hashing it before using it in the Partition_Data_Path.
4. WHEN the same topic-and-partition pair is hosted again after a restart, THE Server SHALL derive the identical Partition_Data_Path so that the Durable backend reopens the partition's existing segments.

### Requirement 8: Handling a Failed Durable Log Open

**User Story:** As an operator, I want a durable partition that cannot open its
log to fail visibly rather than silently lose durability, so that a storage
problem never degrades a durable topic into an in-memory one.

#### Acceptance Criteria

1. IF constructing the Durable backend for a partition replica fails, THEN THE Server SHALL NOT start that partition replica with an In_Memory backend.
2. IF constructing the Durable backend for a partition replica fails, THEN THE Server SHALL leave that partition replica unstarted on the node and SHALL log a structured error identifying the topic and partition.
3. WHEN constructing the Durable backend for one partition replica fails, THE Server SHALL continue to host the partition replicas whose backends were constructed successfully.

### Requirement 9: Durable Persistence of Raft Hard State

**User Story:** As a platform developer, I want a durable replica's term and vote
persisted before they take effect, so that a restart cannot make the replica
violate Raft's voting and term-monotonicity guarantees.

#### Acceptance Criteria

1. WHERE a partition replica uses the Durable backend, WHEN the Raft_Node grants a vote in a term, THE Raft_Node SHALL persist that term and that vote to stable storage before the corresponding vote grant is emitted.
2. WHERE a partition replica uses the Durable backend, WHEN the Raft_Node adopts a higher `current_term`, THE Raft_Node SHALL persist the new `current_term` to stable storage before emitting any message that depends on the new term.
3. WHERE a partition replica uses the In_Memory backend, THE Raft_Node SHALL keep `current_term` and `voted_for` in volatile memory only.
4. IF persisting Raft_Hard_State fails for a Durable replica, THEN THE Raft_Node SHALL NOT emit the vote grant or term-dependent message whose persistence failed, and SHALL surface the failure.

### Requirement 10: Restoring Raft Hard State on Restart

**User Story:** As a platform developer, I want a restarted durable replica to
remember its last term and vote, so that consensus safety holds across restarts.

#### Acceptance Criteria

1. WHEN a Durable partition replica is restarted on existing persisted data, THE Raft_Node SHALL restore `current_term` from the persisted Raft_Hard_State.
2. WHEN a Durable partition replica is restarted on existing persisted data, THE Raft_Node SHALL restore `voted_for` from the persisted Raft_Hard_State.
3. FOR ALL sequences of term advances and vote grants applied to a Durable replica whose Raft_Hard_State was persisted, WHEN the replica is restarted on the same data, THE restored `current_term` and `voted_for` SHALL equal the values the replica held immediately before the restart.
4. WHEN a Durable replica is restarted, IF it had already granted a vote in its persisted `current_term` before the restart, THEN THE Raft_Node SHALL NOT grant a vote to a different candidate in that same term.
5. WHEN a Durable replica is restarted, THE Raft_Node SHALL NOT adopt a `current_term` lower than the highest `current_term` it persisted before the restart.

### Requirement 11: Commit-Index Recovery Handoff to the State Machine

**User Story:** As a platform developer, I want a restarted durable replica to
re-apply its committed entries, so that previously committed records are
observable again after a restart.

#### Acceptance Criteria

1. WHEN a Durable partition replica is restarted on a recovered log, THE Raft_Node SHALL initialize its `commit_index` from the recovered log's commit index.
2. WHEN a Durable partition replica is restarted on a recovered log, THE Raft_Node SHALL re-apply every committed entry of the recovered log to the State_Machine exactly once, in ascending index order.
3. WHEN a Durable partition replica is restarted on a recovered log that held Committed_Records, THE State_Machine SHALL report the same Committed_Records, at the same offsets, that it reported immediately before the restart.
4. WHERE a partition replica uses the In_Memory backend, WHEN the hosting process restarts, THE State_Machine SHALL begin with no Committed_Records.

### Requirement 12: Consensus-Safe Durable Sync Policy

**User Story:** As a platform developer, I want durable partition logs to use the
persist-before-acknowledge sync policy, so that an acknowledged committed record
is never lost on restart.

#### Acceptance Criteria

1. WHERE a partition replica uses the Durable backend, THE Server SHALL configure that Durable backend with the Always Sync_Policy.
2. THE Server SHALL NOT configure a Durable backend that backs consensus with the Periodic or the Never Sync_Policy.

### Requirement 13: In-Memory Topics Are Unchanged

**User Story:** As an operator, I want in-memory topics to behave exactly as they
do today, so that opting out of durability has no surprises.

#### Acceptance Criteria

1. WHERE a topic's Log_Backend is In_Memory, THE partition replica SHALL exhibit the same observable produce, consume, and commit behavior as a partition replica backed directly by InMemoryLog.
2. WHERE a topic's Log_Backend is In_Memory, WHEN the hosting process restarts, THE partition replica SHALL begin with no retained log entries and no Committed_Records.
3. WHERE a topic's Log_Backend is In_Memory, THE Server SHALL NOT create any Partition_Data_Path or write any segment files for that topic's partitions.

### Requirement 14: Durable Topics Survive a Process or Container Restart

**User Story:** As an operator, I want a durable topic's committed records to
survive a restart, so that durability is observable end-to-end in the running
cluster.

#### Acceptance Criteria

1. WHEN a record produced to a Durable topic's partition has been acknowledged as committed, and the hosting process or container subsequently restarts, THEN the restarted partition replica SHALL retain that Committed_Record and SHALL return it from a consume at the same offset it was assigned before the restart.
2. WHEN a Durable topic's partition replica completes recovery after a restart, THE partition replica SHALL resume serving produce and consume requests for that partition.
3. WHEN a Durable topic's partition replica is restarted, THE partition replica SHALL NOT report a committed offset lower than the highest committed offset acknowledged to a client before the restart.

### Requirement 15: Docker-Compose Volume Wiring

**User Story:** As an operator, I want the local Docker cluster to persist each
node's durable data, so that durable topics survive container restarts in local
multi-node runs.

#### Acceptance Criteria

1. THE docker-compose configuration SHALL set the `VELA_DATA_DIR` environment variable for every node service.
2. THE docker-compose configuration SHALL mount a per-node named volume at each node's Data_Directory.
3. WHEN a node container is restarted with its named volume reattached, THE node SHALL recover the Durable partition data stored in that volume before the restart.

### Requirement 16: Durable Metadata Raft Group at a Reserved Path

**User Story:** As a platform developer, I want the dedicated metadata Raft group
to be durable and stored at a fixed reserved location, so that the topic
catalogue is persisted as cluster infrastructure independent of any client
topic's backend choice.

#### Acceptance Criteria

1. THE Metadata_Raft_Group SHALL use the Durable backend for its log, and THE Server SHALL NOT make the Metadata_Raft_Group's backend selectable through the Client_API.
2. THE Server SHALL store the Metadata_Raft_Group's log at a fixed Partition_Data_Path beneath the Data_Directory derived from the reserved name `__meta`.
3. THE reserved path component derived from the name `__meta` SHALL be a Safe_Path_Component.
4. THE Server SHALL derive the Metadata_Raft_Group's Partition_Data_Path such that it never resolves to the same Partition_Data_Path as any client topic's partition.
5. WHERE a client topic name is converted into a Safe_Path_Component under Requirement 7, THE Server SHALL ensure that conversion can never produce the reserved component derived from `__meta`.
6. THE Server SHALL configure the Metadata_Raft_Group's Durable backend with the Always Sync_Policy.

### Requirement 17: Metadata Raft Group Hard State and Commit Recovery

**User Story:** As a platform developer, I want the metadata Raft group to persist
and restore its consensus state like a durable client partition, so that the
committed cluster metadata is recovered correctly after a restart.

#### Acceptance Criteria

1. WHEN the Metadata_Raft_Group grants a vote in a term, THE Metadata_Raft_Group SHALL persist that term and that vote to stable storage before the corresponding vote grant is emitted.
2. WHEN the Metadata_Raft_Group adopts a higher `current_term`, THE Metadata_Raft_Group SHALL persist the new `current_term` to stable storage before emitting any message that depends on the new term.
3. WHEN the Metadata_Raft_Group is restarted on existing persisted data, THE Metadata_Raft_Group SHALL restore `current_term` and `voted_for` from the persisted Raft_Hard_State.
4. WHEN the Metadata_Raft_Group is restarted on a recovered log, THE Metadata_Raft_Group SHALL initialize its `commit_index` from the recovered log's commit index and SHALL re-apply every committed entry of the recovered log exactly once, in ascending index order, to restore its committed Cluster_Metadata.

### Requirement 18: Full-Cluster Cold Restart Recovers the Topic Catalogue

**User Story:** As an operator, I want the cluster to recover the full topic
catalogue after every node restarts, so that durable topics created before a
full shutdown are still recognized and keep their data.

#### Acceptance Criteria

1. WHEN a Full_Cluster_Cold_Restart occurs and every node reattaches its Data_Directory, THE cluster SHALL recover the previously-committed Cluster_Metadata, including each topic's definition and each topic's recorded Log_Backend.
2. WHEN the Cluster_Metadata is recovered after a Full_Cluster_Cold_Restart, THE Server SHALL recognize each topic recorded as Durable as a Durable topic and SHALL reopen that topic's partitions on their existing segments at their derived Partition_Data_Path (see Requirement 14 for per-partition data survival).
3. WHEN a topic recorded as Durable is recovered after a Full_Cluster_Cold_Restart, THE recovered topic's Log_Backend SHALL equal the Log_Backend recorded when the topic was created.
