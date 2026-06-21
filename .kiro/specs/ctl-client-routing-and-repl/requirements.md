# Requirements Document

## Introduction

`vela-ctl` is the command-line control tool for a Vela cluster. Its topic-admin
commands (`create`, `delete`, `list`, `describe`) and its data-plane commands
(`produce`, `consume`) are functional but immature: the producer and consumer
are one-shot, and topic-admin requests are sent straight to the first configured
endpoint with no leader discovery, so an operator frequently sees "not leader"
errors when the contacted node does not lead the targeted partition or the
cluster's metadata group.

This feature improves the `vela-ctl` client experience by making every command
leader-aware and by turning the producer and consumer into long-running,
interactive tools:

- **Discovery and routing** — before producing or consuming, the client
  discovers a topic's partitions and their leaders (and, for topic-admin
  mutations, the metadata/admin leader), routes each request to the correct
  node, and transparently redirects on a `NotLeader` response so operators stop
  seeing routing errors for normal cluster states.
- **Producer REPL** — `produce` opens an interactive `> ` prompt that produces
  each entered line as a record and loops until the operator terminates the
  program.
- **Continuous consumer** — `consume` discovers a topic's partitions and
  leaders, reads committed records, and keeps polling past the end of the log at
  a configured interval so newly produced records are eventually returned, until
  the operator terminates the program.

The Vela server is expected to remain unchanged where possible: the existing
client-facing contract (`FindLeader`, `DescribeTopic`, and the `NotLeader`
error with a leader hint) already supports leader-aware routing. Where the
existing contract is insufficient for a programmatic producer/consumer/admin
client, the gap is captured here and any required server change is scoped as a
separate, backward-compatible requirement rather than assumed. One such
backward-compatible addition is exposing the Member_Address_Map to programmatic
clients so the control tool seeds its node registry from server-provided
addresses; requiring `id=url` Endpoints becomes the explicit fallback used only
when the server does not provide member addresses.

## Glossary

- **Vela_Ctl**: The `vela-ctl` command-line binary that an operator runs to
  administer a cluster and to produce/consume records.
- **Client_Core**: The shared `vela-client` routing layer (connection pool,
  node-id→address registry, leader cache, partition router, topic-metadata
  cache) that resolves leaders and dispatches requests with redirect handling.
- **Producer**: The `vela-client` produce role that routes a record to a
  partition and sends it to that partition's leader.
- **Consumer**: The `vela-client` consume role that reads committed records from
  a partition's leader.
- **Admin_Client**: The `vela-client` topic-admin role that creates, deletes,
  lists, and describes topics.
- **Partition_Router**: The client-side component that resolves a
  `(topic, key)` pair to a partition index.
- **Leader_Cache**: The client-side map from `(topic, partition)` to the
  believed leader's transport address.
- **Produce_Repl**: The interactive read-produce-prompt loop the Vela_Ctl
  `produce` command runs.
- **Consume_Loop**: The continuous poll loop the Vela_Ctl `consume` command runs
  across a topic's partitions.
- **Server**: A Vela node serving the `VelaClient` gRPC service.
- **Metadata_Leader**: The current leader of the cluster's dedicated metadata
  Raft group (the `__meta/0` group), which serves cluster-metadata mutations;
  also referred to by operators as the "admin leader".
- **NotLeader_Error**: A `VelaError` whose code is `NOT_LEADER`, optionally
  carrying the believed current leader's node id as a redirect hint.
- **Partition_Count**: The number of partitions a topic was created with.
- **Next_Offset**: The offset a consumer must request next to continue reading a
  partition, as returned by the most recent poll of that partition.
- **Polling_Interval**: The wait the Consume_Loop applies before re-polling a
  partition that returned no new records.
- **Endpoint**: A configured cluster address supplied to the Vela_Ctl, optionally
  prefixed with an explicit cluster node id as `id=url`.
- **Metadata_Refresh**: A re-fetch of a topic's partition-and-leader metadata via
  `DescribeTopic` that replaces the cached Partition_Count and re-learns each
  partition's leader, used to pick up partitions added after a session started and
  to re-learn leaders after a failover.
- **Metadata_TTL**: The maximum age a cached topic-metadata entry may reach before
  the Client_Core performs a Metadata_Refresh on the next routing operation;
  default 30 seconds.
- **Canonical_Partitioner**: The single shared partition-assignment function
  `fnv1a_64(key) mod Partition_Count`, computed with the pinned 64-bit FNV-1a
  offset basis `0xcbf29ce484222325` and prime `0x00000100000001b3`, used
  byte-for-byte identically by Vela_Ctl, the vela-client Producer, and any internal
  repartition/key_by stage so the same key on the same topic resolves to the same
  partition regardless of which producer routed it.
- **Member_Address_Map**: The mapping from each cluster member's node id to its
  transport address (the `Member.addr` field of cluster metadata), exposed by the
  Server through its client-facing cluster-metadata contract.
- **Offset_Reset**: The consumer start-position selector, exactly one of `latest`
  (read only records produced after the session starts) or `earliest` (read from
  the earliest committed offset); default `latest`.
- **Retry_Budget**: The bound on data-plane and topic-admin redirect retries for a
  single request, expressed as a total elapsed-time budget (default 5 seconds)
  measured across all retries of that request, combined with an exponential
  inter-retry backoff that starts at 100 milliseconds and doubles each retry up to a
  2-second cap.
- **Sticky_Partitioner**: A keyless routing strategy that assigns a run of
  consecutive keyless records to a single partition before rotating to the next
  partition, distributing records evenly across every partition over time while
  preserving batching.

## Requirements

### Requirement 1: Topic metadata discovery

**User Story:** As an operator, I want the client to discover a topic's
partitions and leaders before producing or consuming, so that requests reach the
correct node instead of failing with routing errors.

#### Acceptance Criteria

1. WHEN the Vela_Ctl `produce` or `consume` command is invoked for a topic, THE Client_Core SHALL retrieve the topic's Partition_Count via `DescribeTopic` before routing any record.
2. WHEN the Vela_Ctl `consume` command is invoked for a topic, THE Client_Core SHALL retrieve each partition's current leader from the topic metadata before reading.
3. WHILE a topic's cached metadata age is within the Metadata_TTL, THE Client_Core SHALL reuse the cached Partition_Count instead of issuing a second `DescribeTopic` for that topic.
4. IF `DescribeTopic` reports that the named topic does not exist, THEN THE Vela_Ctl SHALL report a topic-not-found error and exit with a non-zero status.
5. WHEN a topic's cached metadata age reaches the Metadata_TTL, THE Client_Core SHALL perform a Metadata_Refresh for that topic on the next produce or consume routing operation, replacing the cached Partition_Count and re-learning each partition's leader.
6. IF a produce or consume routing operation fails because the cached metadata is stale (a routed partition index is out of range, or leader resolution for the topic fails), THEN THE Client_Core SHALL perform a Metadata_Refresh for that topic before the next routing attempt.
7. WHERE no Metadata_TTL is supplied to the Vela_Ctl, THE Client_Core SHALL apply a default Metadata_TTL of 30 seconds.
8. IF the topic exists but reports zero partitions when the `produce` command is invoked, THEN THE Client_Core SHALL perform a Metadata_Refresh on a retry interval until at least one partition exists or a discovery timeout elapses, and on timeout THE Vela_Ctl SHALL report a no-partitions error and exit with a non-zero status.
9. IF a record reaches the Partition_Router while the topic's Partition_Count is zero, THEN THE Partition_Router SHALL reject the record and fail fast rather than computing a partition assignment against a zero Partition_Count.

### Requirement 2: Per-partition leader resolution

**User Story:** As an operator, I want the client to resolve which node leads a
partition, so that produce and consume requests are sent to that leader.

#### Acceptance Criteria

1. WHEN a partition has no entry in the Leader_Cache, THE Client_Core SHALL resolve the partition's leader by issuing `FindLeader` to the configured nodes and SHALL record the resolved leader address in the Leader_Cache.
2. WHEN resolving a partition's leader, THE Client_Core SHALL accept a leader named by any reachable replica that knows the partition, regardless of which Endpoint was listed first.
3. IF every reachable node reports no elected leader for the partition, THEN THE Client_Core SHALL return a partition-unavailable result distinct from a transport failure.
4. IF a resolved leader node id has no address in the node registry, THEN THE Client_Core SHALL return an unknown-node error that identifies the unresolved node id.

### Requirement 3: Leader-directed produce and consume with redirect

**User Story:** As an operator, I want produce and consume requests that land on
a non-leader to be retried against the real leader, so that transient leadership
changes do not surface as errors.

#### Acceptance Criteria

1. WHEN a produce or consume request is dispatched, THE Client_Core SHALL send the request to the partition's believed leader address from the Leader_Cache.
2. IF a produce or consume request returns a NotLeader_Error, THEN THE Client_Core SHALL update the believed leader from the error's leader hint, or re-resolve via `FindLeader` when no hint is present, and SHALL retry the request.
3. IF a produce or consume request fails with a transport or connection failure to the believed leader, THEN THE Client_Core SHALL invalidate that partition's Leader_Cache entry, re-resolve the leader via `FindLeader`, and retry the request within the same Retry_Budget.
4. WHILE following NotLeader_Error redirects or transport-failure re-resolutions for a single request, THE Client_Core SHALL wait the Retry_Budget's exponential inter-retry backoff before each retry, beginning at 100 milliseconds.
5. WHEN a single request's Retry_Budget is exhausted without reaching a leader, THE Client_Core SHALL stop retrying and return a no-leader-after-retries error.
6. IF a produce or consume request fails with a non-retryable application error (an invalid-argument, topic-not-found, partition-not-found, or payload-too-large error), THEN THE Client_Core SHALL return that error to the caller without retrying.

### Requirement 4: Leader-directed topic-admin commands with redirect

**User Story:** As an operator, I want topic-admin commands to find and use the
metadata (admin) leader, so that `create` and `delete` stop failing with "not
leader" when the contacted node is not the metadata leader.

#### Acceptance Criteria

1. WHEN a topic-mutating admin command (`create` or `delete`) is issued, THE Admin_Client SHALL send the request to a configured node and, IF the node returns a NotLeader_Error, THEN THE Admin_Client SHALL redirect the request to the hinted Metadata_Leader and retry.
2. IF a topic-mutating admin command fails with a transport or connection failure to the contacted node, THEN THE Admin_Client SHALL re-resolve the Metadata_Leader and retry the request within the same Retry_Budget used for data-plane dispatch.
3. WHILE redirecting a topic-mutating admin command, THE Admin_Client SHALL apply the same Retry_Budget (exponential inter-retry backoff beginning at 100 milliseconds and a total elapsed-time budget) used for data-plane dispatch.
4. WHEN a topic-mutating admin command's Retry_Budget is exhausted without reaching the Metadata_Leader, THE Admin_Client SHALL report a no-leader-after-retries error and the Vela_Ctl SHALL exit with a non-zero status.
5. WHERE a topic-admin command is read-only (`list` or `describe`), THE Admin_Client SHALL send the request to a configured node without requiring redirection to the Metadata_Leader.
6. IF a read-only topic-admin command (`list` or `describe`) fails with a transport or connection failure to the contacted node, THEN THE Admin_Client SHALL re-resolve and retry the request against a configured node within the same Retry_Budget.

### Requirement 5: Producer partition routing

**User Story:** As an operator, I want produced records routed to the correct
partition by key, or spread evenly when keyless, so that ordering-by-key holds
and load is balanced.

#### Acceptance Criteria

1. WHEN a record with a non-empty key is produced to a topic, THE Partition_Router SHALL select the partition using the Canonical_Partitioner (`fnv1a_64(key) mod Partition_Count` with the pinned FNV-1a constants), so that the same key on the same topic resolves to the same partition for an unchanged Partition_Count.
2. WHEN a record with no key or an empty key is produced to a topic, THE Partition_Router SHALL distribute records evenly across the topic's partitions using either per-record round-robin selection or a Sticky_Partitioner.
3. THE Partition_Router SHALL select a partition index within the range `0..Partition_Count` for every produced record.
4. WHEN a record is produced with a key, THE Partition_Router SHALL leave the keyless routing position for that topic unchanged.
5. THE Canonical_Partitioner SHALL be the single shared partition-assignment implementation used by Vela_Ctl, the vela-client Producer, and any internal repartition/key_by stage, so that a given key and Partition_Count resolve to the same partition regardless of which producer routed the record.
6. WHERE the keyless routing strategy is a Sticky_Partitioner, THE Partition_Router SHALL assign consecutive keyless records to one partition before rotating to the next partition, so that records are distributed evenly across every partition over time.

### Requirement 6: Producer REPL

**User Story:** As an operator, I want the producer to open an interactive
prompt where each line I type is produced, so that I can produce many records in
one session without re-running the command.

#### Acceptance Criteria

1. WHEN the Vela_Ctl `produce` command starts, THE Produce_Repl SHALL display the prompt `> ` and wait for a line of input.
2. WHEN the operator enters a line and presses Enter, THE Produce_Repl SHALL produce the line's content as a record value to the topic, report the committed offset, and then display a new `> ` prompt.
3. WHILE the input stream is open and no termination signal has been received, THE Produce_Repl SHALL repeat the read-produce-prompt cycle, waiting for operator input before producing each record.
4. WHERE the `produce` command is invoked with a key option, THE Produce_Repl SHALL produce every entered line using that key.
5. IF producing an entered line returns an error, THEN THE Produce_Repl SHALL report the error and display a new `> ` prompt without terminating.
6. WHEN the input stream reaches end-of-input, THE Produce_Repl SHALL exit with a zero status.

### Requirement 7: Producer REPL termination

**User Story:** As an operator, I want to end the producer session with Ctrl+C,
so that I can stop producing at any time.

#### Acceptance Criteria

1. WHEN an interrupt signal is received while the Produce_Repl is running, THE Vela_Ctl SHALL stop reading further input and terminate the process.
2. WHILE waiting for a line of input, THE Produce_Repl SHALL remain responsive to an interrupt signal.

### Requirement 8: Consumer partition and leader discovery

**User Story:** As an operator, I want the consumer to read the whole topic, so
that I see records across all of its partitions without naming a partition.

#### Acceptance Criteria

1. WHEN the Vela_Ctl `consume` command starts for a topic, THE Consumer SHALL discover the topic's Partition_Count and each partition's leader before reading.
2. WHEN consuming a topic, THE Consumer SHALL read committed records from every partition of the topic.
3. THE Consumer SHALL return each partition's records in ascending offset order.
4. WHERE a single partition index is supplied to the `consume` command, THE Consumer SHALL read only that supplied partition and SHALL NOT read the topic's other partitions.
5. IF the topic exists but reports zero partitions, THEN THE Consumer SHALL continue discovering partitions on each poll until at least one partition exists or a discovery timeout elapses, and on timeout THE Vela_Ctl SHALL report a no-partitions error and exit with a non-zero status.
6. WHERE no Offset_Reset is supplied to the `consume` command, THE Consumer SHALL set each partition's initial Next_Offset to that partition's latest committed offset, so that only records produced after the session starts are read.
7. WHERE the `consume` command is invoked with an Offset_Reset of `earliest`, THE Consumer SHALL set each partition's initial Next_Offset to that partition's earliest committed offset, so that reading starts from the beginning of the log.
8. THE Consumer SHALL maintain each partition's Next_Offset in process memory only and SHALL operate as a standalone, non-committing consumer that neither persists offsets nor participates in consumer-group coordination.

### Requirement 9: Continuous consumer polling

**User Story:** As an operator, I want the consumer to keep polling after
reaching the end of the log, so that records produced later are eventually
returned without re-running the command.

#### Acceptance Criteria

1. WHILE no termination signal has been received, THE Consume_Loop SHALL repeatedly poll each consumed partition for committed records starting at that partition's Next_Offset.
2. WHEN a partition poll returns no new records, THE Consume_Loop SHALL wait for the Polling_Interval and then poll that partition again.
3. WHEN records are produced to a partition after the Consume_Loop reached the end of that partition, THE Consume_Loop SHALL return those records on a subsequent poll.
4. WHEN a partition poll returns records, THE Consume_Loop SHALL set that partition's Next_Offset to the `next_offset` value returned by that poll.
5. WHERE a Polling_Interval is supplied to the `consume` command, THE Consume_Loop SHALL wait that interval between empty polls; otherwise THE Consume_Loop SHALL wait a default interval of 500 milliseconds.
6. WHEN the Consume_Loop returns records to the operator, THE Consume_Loop SHALL print each record's partition, offset, and value, so that offsets remain distinguishable across partitions.

### Requirement 10: Consumer resilience during polling

**User Story:** As an operator, I want the consumer to recover from leadership
changes while polling, so that the session continues instead of exiting on a
transient error.

#### Acceptance Criteria

1. IF a partition poll returns a NotLeader_Error, THEN THE Consume_Loop SHALL re-resolve that partition's leader and continue polling that partition.
2. IF a partition poll fails with a transport or connection failure to the believed leader, THEN THE Consume_Loop SHALL invalidate that partition's Leader_Cache entry, re-resolve the leader via `FindLeader` on the next poll of that partition, and continue polling on the Polling_Interval rather than re-sending to the failed address.
3. IF a partition has no elected leader during polling, THEN THE Consume_Loop SHALL wait for the Polling_Interval and re-attempt leader resolution for that partition rather than terminating.
4. WHILE polling multiple partitions, THE Consume_Loop SHALL continue polling the remaining partitions when one partition's poll fails with any retryable error, including a NotLeader_Error or a transport failure.
5. WHILE polling multiple partitions, THE Consume_Loop SHALL poll partitions concurrently, each partition on an independent poll task, so that a slow, stuck, or dead partition leader cannot stall or starve polling of the other partitions.

### Requirement 11: Consumer termination

**User Story:** As an operator, I want to end the consumer session with Ctrl+C,
so that I can stop consuming at any time.

#### Acceptance Criteria

1. WHEN an interrupt signal is received while the Consume_Loop is running, THE Vela_Ctl SHALL stop polling and terminate the process.
2. WHILE waiting for the Polling_Interval between polls, THE Consume_Loop SHALL remain responsive to an interrupt signal.

### Requirement 12: Server-client contract for programmatic clients

**User Story:** As a client developer, I want the server's existing contract to
expose leader and partition information, so that programmatic producer, consumer,
and admin clients can route correctly without server changes.

#### Acceptance Criteria

1. THE Server SHALL expose partition leader resolution through the `FindLeader` RPC and topic partition-and-leader metadata through the `DescribeTopic` RPC for use by programmatic clients.
2. WHEN a partition-scoped request (`Produce` or `Consume`) reaches a node that does not lead the target partition, THE Server SHALL return a NotLeader_Error carrying the believed current leader node id when a leader is known.
3. WHEN a topic-mutating admin request reaches a node that is not the Metadata_Leader, THE Server SHALL return a NotLeader_Error carrying the Metadata_Leader node id when a leader is known.
4. THE feature SHALL keep reactive NotLeader_Error redirection available as a leader-discovery mechanism at all times, including when proactive Metadata_Leader discovery is available, so that reactive redirection always serves as a fallback.
5. THE feature SHALL ensure at least one leader-discovery mechanism (proactive Metadata_Leader discovery or reactive NotLeader_Error redirection) is available for topic-admin commands at all times.
6. IF a server-side contract change is required to expose Metadata_Leader discovery or member-address information to programmatic clients, THEN THE change SHALL be specified as a separate, explicitly scoped requirement and SHALL preserve compatibility with existing clients.
7. THE Server SHALL expose the Member_Address_Map (each member's node id and `Member.addr` transport address) to programmatic clients through its existing client-facing cluster-metadata contract, as a backward-compatible addition that leaves existing client behavior unchanged.
8. WHEN a programmatic client requests cluster metadata through the client-facing contract, THE Server SHALL include each known member's node id and transport address so the client can resolve any leader node id to an address without an `id=url` Endpoint.

### Requirement 13: Leader node-id to address resolution for the control tool

**User Story:** As an operator, I want the control tool to dial partition
leaders by address, so that produce and consume reach leaders led by nodes other
than the one I contacted first.

#### Acceptance Criteria

1. WHEN the Vela_Ctl resolves a leader node id to a transport address, THE Client_Core SHALL use a node registry seeded primarily from the Member_Address_Map provided by the Server's cluster-metadata contract and secondarily from the configured Endpoints.
2. WHEN the Server provides a Member_Address_Map, THE Client_Core SHALL seed its node registry from that map so that a leader returned by `FindLeader` resolves to an address without requiring an `id=url` Endpoint.
3. WHERE the Server does not provide member addresses, THE Client_Core SHALL fall back to `id=url` Endpoints, registering the explicit cluster node id supplied as `id=url` so that a leader returned by `FindLeader` for that node id resolves to the supplied address.
4. IF a resolved leader node id cannot be mapped to an address from either the Member_Address_Map or a configured `id=url` Endpoint, THEN THE Vela_Ctl SHALL report the unresolved node id and that an `id=url` Endpoint is required for that node, and SHALL exit with a non-zero status.
5. WHERE member addresses are discoverable through the Server's cluster-metadata contract, THE Client_Core SHALL use those addresses as the primary resolution source, and the `id=url` Endpoint mechanism SHALL serve only as the fallback specified in Requirement 12.7.
6. IF member addresses are not discoverable and no `id=url` Endpoint is supplied for a leader's node, THEN THE Vela_Ctl SHALL fail immediately with a non-zero status rather than continuing with degraded functionality.
