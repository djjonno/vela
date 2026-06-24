# Requirements Document

## Introduction

This feature adds **batched produce** to Vela: the ability for a producer to send
many event records to a partition in a **single produce request**, rather than one
record per request.

Today the produce path appends exactly one record per request. Each produced
record costs a full round trip — an RPC, a Raft propose, an append, a durable WAL
fsync (the durable log runs with a sync-on-every-append policy), a commit, and an
apply. Measured produce throughput is correspondingly low (roughly 12–44
records/sec). Batching multiple records into one request — appended and committed
together, with a single durability sync amortized over the whole batch — is the
highest-leverage way to raise produce throughput.

The deliverable is a **Produce_Batch**: a producer can submit an ordered set of
records for a single (topic, partition); the Vela_Cluster appends them together,
preserving the order given, and reports each record's committed offset. The
existing single-record produce behavior is preserved, the client-side partition
routing rules are unchanged (records are grouped into per-partition batches before
dispatch), per-record and batch-level size limits are validated, durability is
amortized over the batch, the throughput benchmark can drive batched produce at a
configurable batch size, and records produced in a batch are consumed back exactly
as if they had been produced one at a time.

### Non-goals

- **Cross-partition atomicity.** A single Produce_Batch targets exactly one
  (topic, partition). Producing to several partitions is a client-side fan-out of
  one batch per partition; there is no atomicity guarantee *across* partitions.
- **The wire encoding of a batch.** Whether a batch is carried by a new message,
  a repeated field on the existing request, a new RPC, or a single multi-record
  Raft entry is a design-phase decision. These requirements define observable
  behavior only.
- **The exact numeric batch bounds.** The maximum record count per batch
  (Max_Batch_Records) and maximum total batch size (Max_Batch_Bytes) are bounded,
  configurable quantities whose concrete default values the design selects.
- **Compression, record-set framing formats, and producer-side time/size-based
  auto-batching policies.** This feature defines the batch operation and its
  semantics; higher-level accumulation policies are out of scope.
- **Changing the per-record payload limit.** The existing 1 MiB per-record
  key+value limit is retained unchanged.

## Glossary

- **Vela**: The distributed event-streaming platform that hosts this feature.
- **Vela_Cluster**: The running set of Vela nodes that serves produce and consume
  requests; for a given partition, the server-side behavior is performed by that
  partition's Raft group and its leader.
- **Producer**: The `vela-client` producer component that resolves partitions and
  dispatches produce requests to partition leaders.
- **Consumer**: The `vela-client` consumer component that reads committed records
  back from a partition in ascending offset order.
- **Throughput_Benchmark**: The existing benchmark tool (`vela-bench`) that drives
  produce and consume through the public client APIs and measures throughput.
- **Partition**: A shard of a topic; the unit of ordering, replication, and
  consensus. Each Partition has its own Raft group and log.
- **Partition_Leader**: The replica of a Partition's Raft group that is currently
  permitted to append records to that Partition's log.
- **Record**: A single event entry consisting of an optional key (opaque bytes)
  and a value (opaque bytes).
- **Batch_Record**: A single Record carried within a Produce_Batch.
- **Produce_Batch**: An ordered collection of one or more Batch_Records submitted
  in a single produce request, all targeting the same (topic, Partition).
- **Committed_Offset**: The unique, 0-based, gap-free position assigned to a
  committed Record within its Partition.
- **Base_Offset**: The Committed_Offset assigned to the first (0-based position 0)
  Batch_Record of a committed Produce_Batch; the Nth Batch_Record receives
  `Base_Offset + N`.
- **Per_Record_Limit**: The maximum combined key-and-value size of a single
  Record: 1,048,576 bytes (1 MiB). Unchanged from the existing produce feature.
- **Max_Batch_Records**: The configured maximum number of Batch_Records permitted
  in a single Produce_Batch; a positive integer whose default the design selects.
- **Max_Batch_Bytes**: The configured maximum total encoded size, in bytes, of a
  single Produce_Batch; a positive integer whose default the design selects.
- **Commit_Timeout**: The maximum time a produce request waits for replication to
  a majority before failing: 5,000 milliseconds. Unchanged from the existing
  produce feature.
- **Acknowledged_Record**: A produced Record for which the Vela_Cluster returned a
  success response carrying a Committed_Offset.

## Requirements

### Requirement 1: Produce Multiple Records in One Request

**User Story:** As a producer client, I want to send many records to a partition
in a single produce request, so that I can achieve far higher produce throughput
than one record per request allows.

#### Acceptance Criteria

1. THE Vela_Cluster SHALL accept a Produce_Batch whose Batch_Record count is at least 1 and at most Max_Batch_Records and whose Batch_Records target a single (topic, Partition).
2. WHEN a Produce_Batch is committed, THE Vela_Cluster SHALL append the batch's Batch_Records to the target Partition's log in ascending 0-based position order, so that the Batch_Record at position N is appended before the Batch_Record at position N+1.
3. WHEN a Produce_Batch is committed, THE Vela_Cluster SHALL assign the Batch_Record at 0-based position N+1 a Committed_Offset exactly one greater than the Committed_Offset assigned to the Batch_Record at position N.
4. WHEN a Produce_Batch is committed, THE Vela_Cluster SHALL return a result that reports, for each Batch_Record in the Produce_Batch, the Committed_Offset assigned to that Batch_Record.

### Requirement 2: Atomic, Contiguous Batch Append

**User Story:** As a producer client, I want a batch to commit as a single
all-or-nothing unit with contiguous offsets, so that a partition never holds part
of a batch and a record's position is predictable.

#### Acceptance Criteria

1. WHEN a Produce_Batch is committed, THE Vela_Cluster SHALL assign a Committed_Offset to every Batch_Record in the Produce_Batch, such that either all Batch_Records receive a Committed_Offset or none do.
2. IF a Produce_Batch contains zero Batch_Records, THEN THE Vela_Cluster SHALL append none of the Produce_Batch's Batch_Records, SHALL leave the target Partition's log and committed offset unchanged, and SHALL return a caller-visible error reporting that the Produce_Batch is empty.
3. IF a Produce_Batch cannot be committed, THEN THE Vela_Cluster SHALL append none of the Produce_Batch's Batch_Records, SHALL leave the target Partition's log and committed offset unchanged, and SHALL return a caller-visible error and no Committed_Offset.
4. WHEN a Produce_Batch commits, THE Vela_Cluster SHALL assign the batch a Base_Offset equal to the target Partition's next offset captured before the batch is appended, and SHALL assign the Batch_Record at 0-based position N the Committed_Offset `Base_Offset + N`.
5. WHILE other Produce_Batches or single records are committed concurrently to the same Partition, THE Vela_Cluster SHALL keep each committed Produce_Batch's Committed_Offsets contiguous, with no other Record's Committed_Offset falling within a committed batch's offset range.

### Requirement 3: Per-Record and Batch Size Limits

**User Story:** As a platform operator, I want each record and each batch to be
size-bounded and validated, so that an oversized or empty batch is rejected
cleanly rather than appended or silently truncated.

#### Acceptance Criteria

1. WHEN every Batch_Record in a Produce_Batch has a combined key-and-value size of at most the Per_Record_Limit of 1,048,576 bytes, AND the Produce_Batch's Batch_Record count is at most Max_Batch_Records, AND the Produce_Batch's total encoded size is at most Max_Batch_Bytes, THE Vela_Cluster SHALL accept the Produce_Batch for append.
2. IF any Batch_Record's combined key and value size exceeds the Per_Record_Limit of 1,048,576 bytes, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a caller-visible error reporting the 0-based position of the offending Batch_Record and that Batch_Record's submitted combined size.
3. IF a Produce_Batch contains more Batch_Records than Max_Batch_Records, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a caller-visible error reporting Max_Batch_Records and the submitted Batch_Record count.
4. IF a Produce_Batch's total encoded size exceeds Max_Batch_Bytes, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a caller-visible error reporting Max_Batch_Bytes and the submitted total encoded size.
5. IF a Produce_Batch contains zero Batch_Records, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a caller-visible error reporting that the Produce_Batch is empty.
6. IF a Produce_Batch is rejected for any per-record size, batch count, batch size, or empty-batch reason, THEN THE Vela_Cluster SHALL append none of the Produce_Batch's Batch_Records and SHALL leave the target Partition's log and committed offset unchanged.

### Requirement 4: Coexistence with Single-Record Produce

**User Story:** As an existing client, I want the single-record produce behavior
to keep working, so that adopting batching is incremental and existing callers are
unaffected.

#### Acceptance Criteria

1. WHEN a produce request carrying a single Record is committed, THE Vela_Cluster SHALL append that Record and return its assigned Committed_Offset.
2. WHEN a Produce_Batch carries exactly one Batch_Record, THE Vela_Cluster SHALL assign that Batch_Record the same Committed_Offset the single-record produce path would assign for the same target Partition state.
3. IF a single Record produced through either the single-record path or a one-Batch_Record Produce_Batch fails validation, THEN THE Vela_Cluster SHALL reject it identically across both paths, appending nothing and returning a caller-visible validation error.
4. IF a single Record produced through either the single-record path or a one-Batch_Record Produce_Batch reaches a replica that is not the Partition_Leader, THEN THE Vela_Cluster SHALL reject it identically across both paths with a not-leader error carrying the believed current leader and append nothing.
5. THE Vela_Cluster SHALL assign Committed_Offsets within a Partition that increase by exactly 1 in commit order, regardless of whether Records are produced singly or in a Produce_Batch.

### Requirement 5: Client-Side Partition Routing and Grouping

**User Story:** As a producer client, I want records routed and grouped into
per-partition batches before dispatch, so that batching reuses the existing
partition routing rules and each batch targets one partition.

#### Acceptance Criteria

1. THE Producer SHALL resolve every Record to exactly one (topic, Partition) before dispatch, using the existing keyed and keyless routing rules.
2. WHERE a Record carries a non-empty key, THE Producer SHALL route the Record to a Partition deterministically derived from the key, so that Records with the same key and the same partition count resolve to the same Partition.
3. WHERE a Record carries no key, THE Producer SHALL route the Record using the round-robin keyless rule, so that successive keyless Records rotate across the available Partitions.
4. THE Producer SHALL group the Records that resolve to a given (topic, Partition) into exactly one Produce_Batch for that (topic, Partition), placing each Record in exactly one Produce_Batch and preserving the order in which those Records were supplied.
5. WHEN Records supplied together resolve to more than one Partition, THE Producer SHALL dispatch exactly one Produce_Batch per resolved (topic, Partition).
6. THE Producer SHALL send each Produce_Batch to the believed Partition_Leader of the batch's (topic, Partition).
7. IF the Producer cannot resolve a Record to a Partition because the topic routing metadata is unavailable, THEN THE Producer SHALL not dispatch a Produce_Batch for that (topic, Partition) and SHALL surface a caller-visible routing error, while leaving Produce_Batches for other resolved Partitions unaffected.
8. IF the Producer cannot determine the Partition_Leader for a resolved (topic, Partition), THEN THE Producer SHALL not dispatch that (topic, Partition)'s Produce_Batch and SHALL surface a caller-visible unknown-leader error, while leaving Produce_Batches for other resolved Partitions unaffected.

### Requirement 6: Batch Dispatch and Error Handling

**User Story:** As a producer client, I want clear, redirectable errors for a
batch, so that a misrouted, untimely, or invalid-target batch fails predictably
without partial appends.

#### Acceptance Criteria

1. IF a Produce_Batch reaches a replica that is not the Partition_Leader, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a not-leader error carrying the believed current leader, SHALL append none of the Produce_Batch's Batch_Records, and SHALL leave the target Partition's log and committed offset unchanged.
2. WHEN a Produce_Batch is rejected with a not-leader error, THE Producer SHALL re-resolve the Partition_Leader and retry the identical Produce_Batch within the Producer's retry budget.
3. IF a Produce_Batch is not committed to a majority within the Commit_Timeout of 5,000 milliseconds, THEN THE Vela_Cluster SHALL fail the Produce_Batch, SHALL leave the target Partition's committed offset unchanged, and SHALL return a caller-visible timeout error and no Committed_Offset.
4. IF a Produce_Batch targets a topic that does not exist, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a caller-visible topic-not-found error and SHALL append none of the Produce_Batch's Batch_Records.
5. IF a Produce_Batch targets a Partition that does not exist in the topic, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a caller-visible partition-not-found error and SHALL append none of the Produce_Batch's Batch_Records.
6. IF a Produce_Batch targets a topic that is being deleted, THEN THE Vela_Cluster SHALL reject the Produce_Batch with a caller-visible topic-deleting error and SHALL append none of the Produce_Batch's Batch_Records.

### Requirement 7: Durability Amortized Over the Batch

**User Story:** As a platform operator, I want the cost of making a batch durable
to be amortized over all its records, so that batching delivers the intended
throughput gain rather than paying a per-record durability cost.

#### Acceptance Criteria

1. WHERE the target Partition's log forces writes to stable storage on append, WHEN a Produce_Batch is appended, THE Vela_Cluster SHALL make all of the batch's Batch_Records durable using a single durability sync covering the entire Produce_Batch.
2. WHEN a Produce_Batch of N Batch_Records is appended and made durable, THE Vela_Cluster SHALL perform fewer durability syncs than appending the same N Records as N separate single-record produce requests would perform, for every N greater than 1.
3. WHEN a Produce_Batch is committed, THE Vela_Cluster SHALL commit the batch's Batch_Records as a single replicated unit rather than committing each Batch_Record through an independent replication round.

### Requirement 8: Client Batch API with Per-Record Offsets

**User Story:** As a producer client, I want an API that takes a set of records
and returns each record's committed offset, so that I can produce in bulk and
still learn where every record landed.

#### Acceptance Criteria

1. THE Producer SHALL expose an operation that accepts a topic and an ordered collection of Records, where each Record carries an optional key and a value, to be produced as one or more Produce_Batches.
2. WHEN a batch produce operation succeeds, THE Producer SHALL return, for each input Record, the Committed_Offset assigned to that Record.
3. THE Producer SHALL return per-Record Committed_Offsets such that, for each committed single-Partition Produce_Batch, the Committed_Offset of the Batch_Record at 0-based position N equals that batch's `Base_Offset + N`.
4. IF a batch produce operation fails, THEN THE Producer SHALL return a caller-visible error and SHALL report no Record of the failed Produce_Batch as committed.

### Requirement 9: Benchmark Uses Batched Produce

**User Story:** As a Vela developer, I want the throughput benchmark to drive
batched produce at a configurable batch size, so that the measured produce
throughput reflects batched performance.

#### Acceptance Criteria

1. THE Throughput_Benchmark SHALL accept a configurable batch size, an integer of at least 1, specifying the number of Records produced per Produce_Batch.
2. WHERE the configured batch size is greater than 1, THE Throughput_Benchmark SHALL produce Records through the Producer's batch produce operation.
3. WHERE the configured batch size equals 1, THE Throughput_Benchmark SHALL produce Records with behavior equivalent to one Record per produce request.
4. THE Throughput_Benchmark SHALL count each Acknowledged_Record in a committed Produce_Batch toward Produce_Throughput.
5. IF a supplied batch size is less than 1, THEN THE Throughput_Benchmark SHALL terminate the run with a failing outcome and a descriptive error identifying the invalid batch size before the producer phase begins.

### Requirement 10: Consume Parity for Batched Records

**User Story:** As a consumer client, I want records produced in a batch to be
consumed back exactly as if produced one at a time, so that data-integrity checks
hold regardless of how records were produced.

#### Acceptance Criteria

1. WHEN Records produced via a Produce_Batch are consumed, THE Consumer SHALL return those Records in ascending Committed_Offset order.
2. WHEN a Record produced via a Produce_Batch is consumed, THE Consumer SHALL return a value and key byte-for-byte identical to the value and key supplied in the Produce_Batch.
3. FOR ALL sequences of Records produced to one Partition, THE Vela_Cluster SHALL assign the same Committed_Offset sequence whether those Records are produced as one Produce_Batch or as the equivalent ordered sequence of single-record produces (model-based equivalence).
4. WHEN Records produced via Produce_Batches and Records produced via single-record produce are committed to the same Partition, THE Consumer SHALL return all committed Records as one contiguous, gap-free, ascending Committed_Offset sequence.
