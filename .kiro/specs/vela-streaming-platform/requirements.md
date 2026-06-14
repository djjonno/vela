# Requirements Document

## Introduction

Vela is a distributed event-streaming platform implemented in Rust. It is a ground-up re-implementation and evolution of the kerala project (originally Kotlin, gRPC, and Raft). Vela stores ordered event records on partitioned topics and replicates them across a cluster of nodes using an in-house Raft consensus implementation.

The defining architectural change from kerala is the consensus model. Kerala runs a single Raft group for the entire cluster, so one node leads all writes. Vela instead runs one independent Raft group per partition per topic, distributing leadership and write load across every node in the cluster.

This specification covers the first roadmap milestone: a Cargo workspace of focused crates providing partitioned topic management, produce and consume operations, per-partition Raft leader election and log replication, an in-memory append-only log behind a storage trait, cluster membership, client-to-leader routing, a CLI control tool, and an easy local multi-node cluster via Docker Compose.

The following are explicit non-goals for this milestone and are documented in the Future Considerations section: durable log persistence and the ark-lang embedded stream-processing runtime.

## Glossary

- **Vela**: The distributed event-streaming platform defined by this specification.
- **Node**: A single running instance of the Vela server daemon (`velad`) that participates in a cluster and hosts partition replicas.
- **Cluster**: The set of Nodes that communicate with one another to form one Vela deployment.
- **Topic**: A named stream of event records, identified by a name within a namespace, divided into one or more Partitions.
- **Partition**: A shard of a Topic that is the unit of ordering, replication, and consensus. Each Partition has its own Raft_Group and Log.
- **Partition_Key**: A client-supplied value used to deterministically select the target Partition for a produced Record.
- **Record**: A single event entry produced to and consumed from a Partition, consisting of an opaque key and value byte payload.
- **Offset**: A monotonically increasing zero-based index identifying the position of a committed Record within a Partition's Log.
- **Log**: The append-only, ordered sequence of entries for a single Partition. In-memory for this milestone.
- **Log_Storage**: The trait abstraction behind which Log entries are stored, allowing the in-memory implementation to be replaced with a durable implementation later.
- **Log_Entry**: A single element of the Log, carrying a Raft term, an index, and a payload (a Record or a cluster command).
- **Raft_Group**: The set of replicas of one Partition that elect a leader and agree on Log order using the Raft algorithm.
- **Raft_Node**: One Partition replica's participation in a Raft_Group, holding one of the roles Follower, Candidate, or Leader.
- **Leader**: The Raft_Node in a Raft_Group that accepts writes and replicates Log_Entries to Followers.
- **Follower**: A Raft_Node that replicates Log_Entries from the Leader and responds to its RPCs.
- **Candidate**: A Raft_Node that has started an election and is requesting votes.
- **Term**: A monotonically increasing integer that identifies a single election epoch within a Raft_Group.
- **Election_Timeout**: The randomized duration a Follower waits without contact from a Leader before becoming a Candidate.
- **Heartbeat**: A periodic empty AppendEntries RPC sent by a Leader to maintain its leadership and reset Follower Election_Timeouts.
- **AppendEntries**: The Raft RPC used by a Leader to replicate Log_Entries to Followers and to send Heartbeats.
- **RequestVote**: The Raft RPC used by a Candidate to solicit votes from other Raft_Nodes during an election.
- **Commit_Index**: The highest Log index known to be replicated to a majority of a Raft_Group and therefore safe to apply.
- **State_Machine**: The component that applies committed Log_Entries in Offset order to produce Partition state.
- **Producer**: A client component that appends Records to a Partition.
- **Consumer**: A client component that reads Records from a Partition starting at a given Offset.
- **Admin_Client**: A client component that creates and deletes Topics and queries cluster metadata.
- **Partition_Router**: The Vela component that resolves a Topic and Partition_Key to a Partition and identifies the Node that currently leads that Partition.
- **Cluster_Metadata**: The set of Topics, their Partitions, replica assignments, and current Partition leaders, known to the Cluster.
- **Vela_Ctl**: The command-line control tool (`vela-ctl`) for administering and inspecting a Cluster.
- **gRPC_Transport**: The gRPC-based communication layer used for client-to-server and server-to-server messaging.

## Requirements

### Requirement 1: Cargo Workspace and Crate Structure

**User Story:** As a Vela developer, I want the codebase organized as a multi-crate Cargo workspace with decoupled consensus, log, and transport layers, so that core logic can be tested in isolation and persistence can be added later without changing consensus.

#### Acceptance Criteria

1. THE Vela codebase SHALL be organized as a single Cargo workspace whose root manifest declares exactly seven member crates: a log crate, a consensus crate, a protobuf-definitions crate, a domain-core crate, a server-daemon crate, a client-library crate, and a control-tool crate.
2. THE Vela workspace SHALL define crate dependencies that point inward only, such that the log crate declares no dependency on the consensus, core, server, client, control, or protobuf crates, and the consensus crate declares no dependency on the core, server, client, or control crates.
3. THE consensus crate SHALL declare a dependency on the log crate and SHALL obtain its replicated log type from that crate.
4. THE consensus crate SHALL express each of its three external boundaries — log storage, transport, and clock/timer — as a separate Rust trait, such that the consensus logic depends only on those traits and can be unit-tested with test doubles substituted for each boundary.
5. WHEN the cargo build command is executed at the workspace root, THE Vela workspace SHALL compile all seven member crates to completion with zero compilation errors.
6. IF any member crate fails to compile during a cargo build invocation at the workspace root, THEN THE Vela workspace SHALL terminate the build with a non-zero exit status and report the failing crate.

### Requirement 2: Topic Creation

**User Story:** As a platform user, I want to create a topic with a specified number of partitions, so that I can publish event streams that are sharded across the cluster.

#### Acceptance Criteria

1. WHEN an Admin_Client requests creation of a Topic with a name between 1 and 255 characters consisting only of alphanumeric characters, hyphens, and underscores, and a partition count between 1 and 10,000 inclusive, THE Vela SHALL register the Topic in the Cluster_Metadata with the requested number of Partitions.
2. WHEN a Topic is created with a partition count of N, THE Vela SHALL create N Partitions identified by indices zero through N minus one.
3. WHEN a Topic is created, THE Vela SHALL assign each Partition a Raft_Group whose replica count equals the Cluster's configured replication factor, placing exactly one replica per distinct Node.
4. IF an Admin_Client requests creation of a Topic with a name that already exists in the same namespace, THEN THE Vela SHALL reject the request, return an error indicating the Topic already exists, and leave the Cluster_Metadata unchanged.
5. IF an Admin_Client requests creation of a Topic with a partition count less than 1 or greater than 10,000, THEN THE Vela SHALL reject the request, return a validation error, and leave the Cluster_Metadata unchanged.
6. IF an Admin_Client requests creation of a Topic with a name shorter than 1 character, longer than 255 characters, or containing characters outside the set of alphanumeric characters, hyphens, and underscores, THEN THE Vela SHALL reject the request, return a validation error, and leave the Cluster_Metadata unchanged.
7. IF the number of Nodes available in the Cluster is less than the configured replication factor, THEN THE Vela SHALL reject the Topic creation request, return an error indicating insufficient Nodes, and leave the Cluster_Metadata unchanged.
8. WHEN a Topic creation request succeeds, THE Vela SHALL propagate the new Cluster_Metadata to every reachable Node in the Cluster within 5 seconds.

### Requirement 3: Topic Deletion

**User Story:** As a platform user, I want to delete a topic, so that I can reclaim its resources when the stream is no longer needed.

#### Acceptance Criteria

1. WHEN an Admin_Client requests deletion of an existing Topic, THE Vela SHALL remove the Topic and all of its Partitions from the Cluster_Metadata as a single atomic operation, such that either every Partition of the Topic is removed or none is.
2. WHEN a Topic is deleted, THE Vela SHALL stop the Raft_Group of each Partition belonging to that Topic before releasing that Partition's resources.
3. WHEN a Topic is deleted, THE Vela SHALL release the in-memory Log of each Partition belonging to that Topic.
4. IF an Admin_Client requests deletion of a Topic that does not exist, THEN THE Vela SHALL reject the request, return an error indicating the Topic was not found, and leave the Cluster_Metadata unchanged.
5. WHEN a Topic deletion request succeeds, THE Vela SHALL propagate the updated Cluster_Metadata to every reachable Node in the Cluster within 5 seconds.
6. IF propagation of the updated Cluster_Metadata to one or more Nodes does not complete within 5 seconds, THEN THE Vela SHALL return an error to the Admin_Client identifying the Nodes that did not acknowledge the update, while retaining the Topic removal recorded on the Nodes that did acknowledge.
7. WHILE a Topic deletion is in progress, IF an Admin_Client submits a produce or a duplicate deletion request for that same Topic, THEN THE Vela SHALL reject the request and return an error indicating the Topic is being deleted.

### Requirement 4: Producing Records

**User Story:** As a producer client, I want to produce records to a topic, so that events are durably ordered and replicated within their partition.

#### Acceptance Criteria

1. WHEN a Producer submits a Record to a Topic with a Partition_Key, THE Partition_Router SHALL deterministically map the Partition_Key to one Partition of that Topic.
2. WHEN a Producer submits a Record to a Topic without a Partition_Key, THE Partition_Router SHALL select one Partition of that Topic such that Records submitted without a Partition_Key are distributed across all Partitions of the Topic.
3. WHEN a Record is received by the Leader of its target Partition, THE Leader SHALL append the Record as a Log_Entry to that Partition's Log.
4. WHEN a Record's Log_Entry is replicated to a majority of the Partition's Raft_Group, THE Leader SHALL mark the Log_Entry as committed and return the assigned zero-based Offset to the Producer.
5. IF a Producer submits a Record to a Topic that does not exist, THEN THE Vela SHALL return an error indicating the Topic was not found and SHALL append no Log_Entry.
6. IF a Record is received by a Node that is not the Leader of the target Partition, THEN THE Vela SHALL return an error identifying the current Leader of that Partition and SHALL append no Log_Entry.
7. WHEN multiple Records are committed to the same Partition, THE Vela SHALL assign each Record a unique Offset that increases monotonically by one in commit order, starting at zero for the first committed Record.
8. IF a Producer submits a Record whose combined key and value payload exceeds 1,048,576 bytes (1 MiB), THEN THE Vela SHALL reject the Record with a validation error and SHALL append no Log_Entry.
9. IF the Leader cannot replicate a Record's Log_Entry to a majority of the Partition's Raft_Group within a commit timeout of 5,000 milliseconds, THEN THE Vela SHALL return an error indicating the Record was not committed, SHALL not advance the Partition's committed Offset, and SHALL not return an Offset to the Producer.

### Requirement 5: Consuming Records

**User Story:** As a consumer client, I want to read records from a topic partition starting at an offset, so that I can process the event stream in order.

#### Acceptance Criteria

1. WHEN a Consumer requests Records from an existing Partition starting at a valid Offset, where a valid Offset is an integer from 0 up to and including the Partition's highest committed Offset, THE Vela SHALL return committed Records in strictly ascending Offset order beginning at the requested Offset.
2. THE Vela SHALL return only committed Records to a Consumer and SHALL exclude any Record that has not been committed by the Partition's Raft group.
3. IF a Consumer requests an Offset greater than the highest committed Offset of the Partition, THEN THE Vela SHALL return a successful result containing zero Records.
4. IF a Consumer requests Records from a Partition that does not exist, THEN THE Vela SHALL return an error indicating the Partition was not found and SHALL return no Records.
5. WHEN a Consumer requests Records and specifies a maximum count between 1 and 10,000 inclusive, THE Vela SHALL return no more than the specified number of Records.
6. WHEN a Consumer requests Records without specifying a maximum count, THE Vela SHALL return no more than 500 Records.
7. IF a Consumer requests Records with an Offset less than 0, or with a maximum count less than 1 or greater than 10,000, THEN THE Vela SHALL return an error indicating the request parameters are invalid and SHALL return no Records.
8. IF a Consumer requests Records from an existing Partition that currently has no elected Raft leader, THEN THE Vela SHALL return an error indicating the Partition is unavailable and SHALL return no Records.

### Requirement 6: In-Memory Append-Only Log

**User Story:** As a Vela developer, I want an append-only log with append, read, commit, revert, and snapshot semantics behind a storage trait, so that consensus can use it now in memory and gain durable persistence later without code changes.

#### Acceptance Criteria

1. THE Log_Storage SHALL be defined as a trait that the consensus logic depends on rather than depending on a concrete storage implementation.
2. THE Vela SHALL provide an in-memory implementation of Log_Storage for this milestone.
3. WHEN an append operation is invoked with a Log_Entry on an empty Log, THE Log SHALL store the Log_Entry at index 0 and return the assigned index.
4. WHEN an append operation is invoked with a Log_Entry on a non-empty Log, THE Log SHALL store the Log_Entry at an index exactly one greater than the highest stored index and return the assigned index.
5. WHEN a read operation is invoked for an inclusive range of indices from start to end where start is less than or equal to end, THE Log SHALL return the stored Log_Entries whose indices fall within that range in ascending index order, omitting any index within the range that has no stored Log_Entry.
6. IF a read operation is invoked with a start index greater than its end index, THEN THE Log SHALL return zero Log_Entries without returning an error.
7. WHEN the Log is created and before any commit operation is invoked, THE Log SHALL report its Commit_Index as the uncommitted state preceding index 0.
8. WHEN a commit operation is invoked with an index greater than or equal to the current Commit_Index and less than or equal to the highest stored index, THE Log SHALL advance the Commit_Index to that index.
9. IF a commit operation is invoked with an index less than the current Commit_Index or greater than the highest stored index, THEN THE Log SHALL reject the operation, return an error, and leave the Commit_Index and stored Log_Entries unchanged.
10. WHEN a revert operation is invoked with an index greater than or equal to the current Commit_Index, THE Log SHALL remove all Log_Entries with an index greater than the specified index.
11. IF a revert operation is invoked with an index less than the current Commit_Index, THEN THE Log SHALL reject the operation, return an error, and leave the stored Log_Entries and Commit_Index unchanged.
12. WHEN a snapshot operation is invoked, THE Log SHALL produce a representation of the committed Log state up to the Commit_Index.
13. FOR ALL Log_Entries appended and then read back over their full index range without an intervening revert, THE Log SHALL return entries equal to those appended in the same order.

### Requirement 7: Per-Partition Raft Leader Election

**User Story:** As a platform operator, I want each partition to elect its own Raft leader independently, so that leadership and write load are distributed across the cluster rather than concentrated on one node.

#### Acceptance Criteria

1. THE Vela SHALL instantiate exactly one independent Raft_Group for each Partition of each Topic, such that the count of Raft_Groups equals the total count of Partitions across all Topics.
2. WHILE a Raft_Node is a Follower and no valid AppendEntries RPC is received before its Election_Timeout elapses, THE Raft_Node SHALL transition to Candidate, increment its current Term by 1, and vote for itself, where Election_Timeout is a value selected randomly per election from the inclusive range 150 ms to 300 ms.
3. WHEN a Raft_Node becomes a Candidate, THE Raft_Node SHALL send a RequestVote RPC carrying its current Term and last Log index and Term to every other Raft_Node of its Raft_Group.
4. WHEN a Candidate receives votes from a strict majority (more than half) of the Raft_Nodes in its Raft_Group within the same Term, THE Candidate SHALL transition to Leader.
5. WHILE a Candidate has neither won the election nor observed a Leader for the current Term and its Election_Timeout elapses, THE Candidate SHALL increment its current Term by 1 and start a new election.
6. WHILE a Raft_Node is a Leader, THE Raft_Node SHALL send a Heartbeat RPC to every other Raft_Node of its Raft_Group at a fixed interval of 50 ms, which is strictly shorter than the minimum Election_Timeout of 150 ms.
7. WHEN a Raft_Node receives a RequestVote RPC whose Term is greater than or equal to its current Term, from a Candidate whose Log is at least as up to date as its own, and for which the Raft_Node has not already granted a vote in that Term, THE Raft_Node SHALL grant its vote and set its current Term to the RPC Term.
8. IF a Raft_Node receives a RequestVote RPC whose Term is less than its current Term, OR whose Candidate Log is less up to date than its own, OR for which it has already granted a vote in that Term, THEN THE Raft_Node SHALL deny the vote and respond with its current Term.
9. IF a Raft_Node receives any RPC carrying a Term greater than its current Term, THEN THE Raft_Node SHALL set its current Term to the greater Term and transition to Follower.
10. THE Raft_Group SHALL grant no more than one Leader per Term.
11. WHERE Raft_Groups belong to different Partitions, THE Vela SHALL allow their Leaders to reside on different Nodes.

### Requirement 8: Per-Partition Log Replication

**User Story:** As a platform operator, I want each partition leader to replicate its log to followers and commit on majority acknowledgment, so that records survive node failures and remain consistently ordered.

#### Acceptance Criteria

1. WHEN a Leader appends one or more new Log_Entries, THE Leader SHALL send AppendEntries RPCs containing no more than 256 Log_Entries per RPC to its Followers, each RPC carrying the index and Term of the entry immediately preceding the entries it conveys.
2. WHEN a Follower receives an AppendEntries RPC whose preceding entry matches an entry in its Log at the same index and Term, THE Follower SHALL append the new Log_Entries to its Log and acknowledge the RPC.
3. IF a Follower receives an AppendEntries RPC whose preceding entry does not match its Log at the same index and Term, THEN THE Follower SHALL reject the RPC and respond so the Leader can retry with earlier entries.
4. WHEN a Leader receives a rejection of an AppendEntries RPC or no acknowledgment within an AppendEntries timeout of 1,000 milliseconds, THE Leader SHALL retry replication to that Follower starting from an earlier entry, increasing the backoff between successive retries up to a maximum interval of 5,000 milliseconds.
5. WHEN a Log_Entry has been replicated to a majority of the Raft_Group, THE Leader SHALL advance its Commit_Index to that Log_Entry's index.
6. IF a Log_Entry has not been replicated to a majority of the Raft_Group, THEN THE Leader SHALL retain that Log_Entry in its Log and SHALL not advance its Commit_Index past that entry.
7. THE Leader SHALL advance its Commit_Index monotonically such that the Commit_Index never decreases.
8. WHEN the Commit_Index advances, THE State_Machine SHALL apply each newly committed Log_Entry exactly once, in ascending index order, with no skipped index.
9. WHILE replicating, THE Leader SHALL bring a lagging Follower's Log into agreement with its own Log by sending the missing Log_Entries.
10. FOR ALL Raft_Nodes in a Raft_Group, THE Vela SHALL ensure that two Logs containing an entry with the same index and Term contain identical entries at every index up to and including that index.

### Requirement 9: Cluster Membership

**User Story:** As a platform operator, I want nodes to form a cluster and track membership, so that partitions can be assigned and replicated across the known set of nodes.

#### Acceptance Criteria

1. WHEN a Node starts with a configured list of peer Nodes, THE Node SHALL attempt to establish a gRPC_Transport connection to each configured peer within a per-peer connection timeout of 5 seconds.
2. IF a connection attempt to a peer Node fails or does not complete within 5 seconds, THEN THE Vela SHALL mark that peer as unavailable in its view of the Cluster_Metadata and retry the connection at a fixed interval of 1 second.
3. THE Vela SHALL maintain Cluster_Metadata that records, for each member Node, the Node's identity and an availability state that is exactly one of available or unavailable.
4. WHEN a Node misses 3 consecutive 1-second heartbeat intervals over gRPC_Transport, THE Vela SHALL mark that Node as unavailable in its view of the Cluster_Metadata.
5. WHEN a Node currently marked unavailable returns a successful gRPC_Transport response, THE Vela SHALL mark that Node as available in its view of the Cluster_Metadata.
6. THE Vela SHALL assign Partition replicas only to Nodes that are members of the Cluster.

### Requirement 10: Partition Assignment and Routing

**User Story:** As a producer or consumer client, I want my records routed to the correct partition and node, so that I interact with the right leader without manual coordination.

#### Acceptance Criteria

1. WHILE the Cluster has 2 or more member Nodes, WHEN a Topic is created, THE Vela SHALL assign each Partition's Raft_Group replicas across the member Nodes such that, for that Topic, the maximum and minimum counts of Partition leaderships held by any single Node differ by at most one.
2. WHILE a Topic's partition count is unchanged, WHEN the Partition_Router resolves that Topic and a non-empty Partition_Key, THE Partition_Router SHALL return the same Partition for the same Topic and Partition_Key on repeated calls.
3. IF the Partition_Router is asked to resolve a Topic with a null or empty Partition_Key, THEN THE Partition_Router SHALL select a Partition using its keyless distribution rule rather than the deterministic key mapping.
4. WHEN a client requests the location of a Partition's Leader and that Partition has an established Leader, THE Vela SHALL return the Node currently acting as Leader of that Partition's Raft_Group.
5. IF a client requests the location of the Leader of a Topic or Partition that does not exist, THEN THE Vela SHALL return an error indicating the Topic or Partition was not found and SHALL return no Node.
6. IF a Partition has no current Leader because an election is in progress, THEN THE Vela SHALL return an error indicating that the Leader is unavailable within 5 seconds and SHALL leave the Cluster_Metadata unchanged.

### Requirement 11: Client-to-Leader Routing

**User Story:** As a client developer, I want the client library to find and route requests to the leader of a partition, so that produce and consume requests reach the node that can serve them.

#### Acceptance Criteria

1. WHEN a Producer or Consumer issues a request for a Partition, THE client library SHALL direct the request to the Node identified as the current Leader of that Partition.
2. IF a client request reaches a Node that is not the Leader of the target Partition, THEN THE Vela SHALL respond with a redirection identifying the current Leader.
3. WHEN the client library receives a redirection identifying a different Leader, THE client library SHALL retry the request against the identified Leader, waiting at least 100 milliseconds before each retry.
4. IF the client library cannot locate a Leader for a Partition after 5 retries, THEN THE client library SHALL stop retrying and return an error to the caller indicating that no Leader was found.

### Requirement 12: gRPC Communication

**User Story:** As a Vela developer, I want gRPC used for both client-to-server and server-to-server communication with shared protobuf definitions, so that messaging is typed, versioned, and consistent across crates.

#### Acceptance Criteria

1. THE Vela SHALL define all wire message types as protobuf definitions owned by a dedicated protobuf crate.
2. THE Vela SHALL expose client-facing operations for producing Records, consuming Records, and administering Topics over gRPC_Transport.
3. THE Vela SHALL exchange AppendEntries and RequestVote RPCs between Nodes over gRPC_Transport.
4. IF a Node receives a gRPC request that it cannot process, THEN THE Node SHALL return a typed error response, defined in the protobuf crate, that identifies the failure.

### Requirement 13: CLI Control Tool

**User Story:** As a platform operator, I want a command-line control tool, so that I can create and delete topics and inspect cluster state without writing code.

#### Acceptance Criteria

1. WHEN an operator invokes Vela_Ctl to create a Topic with a name and partition count, THE Vela_Ctl SHALL send a Topic creation request to the Cluster and report the outcome.
2. WHEN an operator invokes Vela_Ctl to delete a Topic, THE Vela_Ctl SHALL send a Topic deletion request to the Cluster and report the outcome.
3. WHEN an operator invokes Vela_Ctl to list Topics, THE Vela_Ctl SHALL display the Topics known to the Cluster_Metadata along with each Topic's partition count.
4. WHEN an operator invokes Vela_Ctl to describe a Topic, THE Vela_Ctl SHALL display each Partition of the Topic and the Node currently leading it.
5. WHEN a Vela_Ctl command completes successfully, THE Vela_Ctl SHALL exit with a status of zero.
6. IF Vela_Ctl cannot establish a connection to any Node in the Cluster within 5 seconds, THEN THE Vela_Ctl SHALL report a connection error and exit with a non-zero status.
7. IF the Cluster rejects a Vela_Ctl request with an error, THEN THE Vela_Ctl SHALL report the error and exit with a non-zero status.

### Requirement 14: Local Multi-Node Cluster

**User Story:** As a developer, I want to run several node instances as a cluster locally with minimal effort, so that I can exercise distribution and consensus without external infrastructure.

#### Acceptance Criteria

1. THE Vela SHALL provide a Dockerfile that builds and runs the Node daemon.
2. THE Vela SHALL provide a Docker Compose configuration that launches multiple Nodes wired into a single Cluster.
3. WHEN an operator starts the Docker Compose configuration, THE Vela SHALL bring up the configured Nodes such that every Node discovers all other configured Nodes as Cluster members.
4. WHEN a Node daemon is started, THE Node SHALL accept its listen address and the addresses of its peer Nodes through command-line arguments or environment configuration.
5. WHILE the Docker Compose Cluster is running, THE Vela SHALL accept produce and consume requests for Topics created on the Cluster.

### Requirement 15: Node Startup and Configuration

**User Story:** As a platform operator, I want each node to start from clear configuration and report its status, so that I can run and observe nodes reliably.

#### Acceptance Criteria

1. WHEN a Node daemon starts, THE Node SHALL bind a gRPC_Transport listener on its configured address.
2. IF a Node daemon is started with an invalid or missing required configuration value, THEN THE Node SHALL emit a structured log entry describing the configuration error and exit with a non-zero status.
3. WHEN a Node daemon starts successfully, THE Node SHALL emit a structured log entry indicating that it is ready to serve requests.
4. WHILE a Node is running, THE Node SHALL emit a structured log entry for each Raft role transition of the Raft_Groups it hosts, identifying the Partition and the new role.

## Future Considerations (Non-Goals for This Milestone)

The following capabilities are intentionally out of scope for this milestone and are recorded here to bound the requirements above:

- **Durable log persistence**: The Log is in-memory only for this milestone. The Log_Storage trait (Requirement 6) is defined so a durable implementation can be added later without changing consensus logic. Crash recovery and on-disk log formats are deferred.
- **ark-lang embedded stream-processing runtime**: The planned model in which processor code is replicated as Log data and executed node-locally in a sandbox is a future follow-up. No processor execution, sandboxing, or stream-projection requirements are included in this milestone.
- **Consumer groups and offset tracking on the server**: Server-side tracking of consumer group membership and committed consumer offsets is not part of this milestone; Consumers supply their own start Offset (Requirement 5).
- **Partition rebalancing on membership change**: Automatic re-replication and leadership rebalancing when Nodes join or leave after Topic creation is deferred; initial assignment occurs at Topic creation time (Requirement 10).
- **Authentication, authorization, and transport encryption**: Securing gRPC_Transport is out of scope for this local-cluster milestone.
