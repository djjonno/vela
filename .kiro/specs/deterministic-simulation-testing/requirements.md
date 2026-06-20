# Requirements Document

## Introduction

This feature adds **Deterministic Simulation Testing (DST)** to Vela: a harness
that runs a multi-node Vela cluster inside a single process, under a fully
controlled and reproducible (seed-driven) environment, in order to battle-test
the consensus, replication, failover, consistency, and partition-tolerance
machinery that has been built so far:

- per-partition durable write-ahead logs (the `durable-wal` feature),
- the admin/metadata leader and the dedicated cluster-wide metadata Raft group
  (the `cross-node-metadata-propagation` feature),
- durable, persisted topics with leader failover and restart recovery,
- topic partitioning and per-partition replica assignment,
- the in-house per-partition Raft consensus implementation (`vela-raft`), and
- the coordinated spawning of per-partition Raft groups on topic creation.

The deliverable is the **DST_Harness**: a test apparatus that composes `N`
simulated nodes into one **Simulated_Cluster**, drives the *same* consensus, log
replication, metadata-coordination, and WAL-recovery logic used in production
through Vela's existing trait seams (`Clock`, `Transport`, `LogStorage`),
injects faults (node crash/restart, network partitions, message
drop/delay/reorder/duplication, clock skew, and disk/WAL faults), and asserts
that a set of **Safety_Properties** and **Liveness_Properties** hold across the
run. Determinism is a first-class goal: a single 64-bit **Seed** fully
determines a **Simulation_Run**, so any failing run can be replayed bit-for-bit
from its seed, and `proptest` can shrink a failing scenario toward a minimal
counterexample.

The properties asserted are framed to express **parity with Kafka's** core
durability and ordering guarantees as concrete, testable statements: an
acknowledged record is never lost while at most a minority of a partition's
replicas fail; committed records on a partition are totally ordered by
contiguous, monotonic offsets; consumers observe only committed records, in
ascending offset order; and the per-partition committed log is linearizable with
respect to the recorded client **History**. Liveness is asserted under healed
faults: once a majority of the relevant replicas are running and mutually
reachable, the cluster elects leaders, commits proposals, and serves
produce/consume within a bounded simulated-time / step budget.

Finally, the DST suite is wired into **Continuous Integration** (the existing
GitHub Actions workflow at `.github/workflows/ci.yml`) so it runs automatically
on every push and pull request, fails the build on any property violation, and
captures the failing seed and a replayable artifact so regressions are caught
immediately.

### Relationship to existing work

`vela-raft` already contains a deterministic, single-threaded simulation harness
for **one** partition's Raft group (`crates/vela-raft/src/sim.rs`:
`SimCluster`, `ManualClock`, `InMemoryTransport`, `SplitMix64`). That harness
established the seams and the discrete-event approach this feature builds on.
This feature **raises** deterministic simulation from a single Raft group to a
full multi-node cluster: many partition groups plus the dedicated metadata
group, coordinated on topic creation, backed by durable WALs, exercised through
node crash/restart and network partitions, with client-observable consistency
and liveness checked end-to-end.

### Architectural-review goal

The user requested a formal architectural and code review to confirm the project
is on the right track and at parity with Kafka before investing further. This
feature captures the outcome of that review as a concrete, maintained artifact: a
**Guarantee_Specification** that enumerates the durability, ordering, and
availability guarantees Vela intends to provide, maps each one either to a DST
property that checks it or to an explicitly documented gap, and records the
Kafka-parity comparison. The DST_Harness is the mechanism that holds Vela to
that specification over time.

### Determinism constraint (locked)

A run's outcome MUST depend only on its Seed and its declared scenario
parameters. The DST_Harness MUST NOT consult wall-clock time, the operating
system thread scheduler, real network I/O, filesystem timestamps, hash-map
iteration order that varies between runs, or any other unseeded entropy source
for any decision that affects a run's outcome. This is the property that makes
replay and shrinking possible, and it is the reason the production tokio-timer
clock (`TimerClock`, which draws election jitter from the wall clock) and the
gRPC transport (`GrpcTransport`) are replaced by deterministic implementations
inside the harness.

### Non-goals

- **Changing consensus, replication, or WAL behavior.** This feature only
  *exercises and verifies* existing logic; any behavioral bug it surfaces is
  fixed under the relevant feature, not here.
- **Real multi-process or networked clusters.** The Simulated_Cluster runs
  in-process; real-network and real-timer coverage remains the job of the
  existing cluster smoke/integration tests.
- **Performance, throughput, or latency benchmarking.** DST asserts correctness,
  not performance; simulated time is logical, not wall-clock.
- **Byzantine fault tolerance.** Faults are crash-recovery and network/disk
  faults (omission, delay, reorder, duplication, partition, crash, data loss on
  unsynced writes), not adversarial corruption of replicated protocol messages
  beyond what the transport and storage contracts already model.
- **Dynamic Raft membership changes** (joint consensus / runtime voter
  add/remove). The cluster's node set and each partition's Replica_Set are fixed
  for the duration of a Simulation_Run.
- **Choosing the integration mechanism** by which the harness drives the
  production async driver logic deterministically (e.g., extending the
  synchronous `SimCluster` model to multiple groups and nodes versus introducing
  a deterministic async executor). That is a design-phase decision; these
  requirements define observable behavior and guarantees only.

## Glossary

- **Vela**: The distributed event-streaming platform under test.
- **DST_Harness**: The deterministic simulation testing apparatus introduced by
  this feature. It builds a Simulated_Cluster, applies a Workload and a
  Fault_Schedule under a Virtual_Clock and a Sim_Network, records a History, and
  evaluates the Safety_Properties and Liveness_Properties.
- **Simulation_Run** (or **Run**): One execution of the DST_Harness from a single
  Seed and a single set of scenario parameters, producing a pass/fail Outcome.
- **Seed**: The 64-bit value from which the DST_Harness derives every random
  decision in a Simulation_Run (election jitter, fault timing and selection,
  workload generation, and event tie-breaking).
- **Scenario_Parameters**: The non-random configuration of a Run: cluster size,
  replication factor, partition count, fault intensities, Workload size, and the
  step or simulated-time budget.
- **Simulated_Cluster**: The in-process set of Sim_Nodes that together form one
  Vela cluster for a Run.
- **Sim_Node** (or **Node**): One simulated Vela node within a Simulated_Cluster,
  hosting one Metadata_Replica and the Partition_Replicas for the partitions
  assigned to it, each backed by a Sim_Storage log.
- **Partition_Replica**: One Sim_Node's replica of a single topic partition's
  Raft group.
- **Metadata_Replica**: One Sim_Node's replica of the dedicated cluster-wide
  metadata Raft group (`__meta` / partition 0).
- **Replica_Set**: The fixed set of Sim_Node ids assigned to replicate a given
  partition for the duration of a Run.
- **Virtual_Clock**: The harness's logical, manually-advanced clock implementing
  Vela's `Clock` seam. Time advances only when the simulation advances it; it is
  never read from the wall clock.
- **Sim_Network**: The harness's deterministic in-memory message bus implementing
  Vela's `Transport` seam, through which all inter-node messages flow and on
  which network faults are injected.
- **Sim_Storage**: The harness's storage layer implementing Vela's `LogStorage`
  seam for each replica, capable of modeling durable-WAL persistence semantics
  and injecting disk/WAL faults.
- **Event**: A single discrete simulation occurrence — a timer firing, a message
  delivery, a client operation, or a fault application — with a logical
  occurrence time on the Virtual_Clock.
- **Fault**: An injected deviation from healthy operation: a Node_Crash,
  Node_Restart, a network fault (drop, delay, reorder, duplication, or
  Partition), a Clock_Skew, or a Storage_Fault.
- **Fault_Schedule**: The seed-derived sequence of Faults applied over the course
  of a Run, including when each Fault begins and, where applicable, when it heals.
- **Node_Crash**: A Fault that stops a Sim_Node and discards its volatile (in
  memory, unsynced) state, retaining only what Sim_Storage had forced to stable
  storage.
- **Node_Restart**: Bringing a previously crashed Sim_Node back up, recovering its
  Metadata_Replica and Partition_Replicas from Sim_Storage.
- **Partition** (network): A Fault that severs message delivery between two
  disjoint groups of Sim_Nodes; it may be symmetric or asymmetric, and is removed
  by a Heal.
- **Heal**: The removal of a network Partition or other transient network fault,
  restoring delivery between the affected Sim_Nodes.
- **Clock_Skew**: A per-Node offset and/or rate difference applied to that Node's
  view of the Virtual_Clock, bounded so it cannot read real wall-clock time.
- **Storage_Fault**: An injected disk/WAL deviation: loss of unsynced writes at a
  Node_Crash, a torn-tail write, or an I/O error surfaced through the LogStorage
  contract.
- **Workload**: The seed-derived sequence of client operations (topic
  create/delete, produce, consume) applied to the Simulated_Cluster during a Run.
- **Client_Operation**: One Workload operation, with a recorded invocation time, a
  recorded response (success with returned values, redirection, or error), and a
  response time.
- **History**: The recorded, logically time-stamped sequence of Client_Operations
  and their responses produced during a Run, used to check consistency.
- **Acknowledged_Record**: A produced record for which the Simulated_Cluster
  returned a success response carrying a committed offset to the client.
- **Committed**: A Raft log entry stored on a majority of its group's replicas and
  therefore applicable (Raft §5.3); used both for partition records and for
  metadata commands.
- **Offset**: The 0-based position of a record within a partition's committed log.
- **Safety_Property**: A property that must hold at every point in a Run; its
  violation means the system did something it must never do (e.g., two leaders in
  one term, a lost Acknowledged_Record).
- **Liveness_Property**: A property asserting that, under stated favorable
  conditions, the system eventually makes progress within a bounded budget (e.g.,
  elects a leader, commits a proposal).
- **Consistency_Checker**: The component that evaluates the History against the
  per-partition linearizable log model and the Kafka-parity ordering and
  durability guarantees.
- **Outcome**: The pass/fail result of a Run; a fail identifies the violated
  property.
- **Counterexample**: The minimized failing scenario `proptest` reports for a
  failed Run — at minimum the Seed and Scenario_Parameters needed to reproduce it.
- **Failure_Artifact**: The persisted record of a failed Run: the Seed, the
  Scenario_Parameters, the violated property, and a replayable Event trace and/or
  History sufficient to diagnose the failure.
- **Guarantee_Specification**: The maintained document enumerating Vela's intended
  durability, ordering, and availability guarantees, mapping each to a DST
  property or a documented gap, and recording the Kafka-parity comparison.
- **CI**: The GitHub Actions workflow at `.github/workflows/ci.yml` that runs the
  project's checks on push and pull request.

## Requirements

### Requirement 1: Seed-Determined, Reproducible Simulation Runs

**User Story:** As a Vela developer, I want every simulation run to be fully
determined by its seed, so that any behavior I observe can be reproduced exactly.

#### Acceptance Criteria

1. THE DST_Harness SHALL derive every random decision in a Simulation_Run — election-timeout jitter, Fault selection and timing, Workload generation, and tie-breaking between simultaneous Events — solely from the Run's Seed and Scenario_Parameters.
2. THE DST_Harness SHALL NOT consult wall-clock time, the operating system thread scheduler, real network I/O, filesystem timestamps, or any other unseeded entropy source for any decision that affects a Run's Outcome.
3. WHEN the DST_Harness executes two Simulation_Runs configured with an identical Seed and identical Scenario_Parameters, THE DST_Harness SHALL produce, for both Runs, the identical ordered sequence of delivered Events, the identical per-replica committed log for every partition and for the metadata group, the identical recorded History, and the identical Outcome.
4. WHERE a Simulation_Run uses concurrency or asynchronous tasks internally, THE DST_Harness SHALL resolve the order in which ready Events and tasks execute as a deterministic function of the Seed, so that internal concurrency does not introduce run-to-run variation.
5. WHEN the DST_Harness selects the next Event to execute among multiple Events scheduled at the same logical Virtual_Clock instant, THE DST_Harness SHALL break the tie using a deterministic, Seed-derived ordering.

### Requirement 2: Replayable Failures

**User Story:** As a Vela developer, I want a failing run to hand me everything I
need to replay it, so that I can debug a consensus failure deterministically.

#### Acceptance Criteria

1. WHEN a Simulation_Run ends with a failing Outcome, THE DST_Harness SHALL report the Seed and the Scenario_Parameters that produced the failure.
2. WHEN a previously failing Seed and its Scenario_Parameters are re-executed by the DST_Harness, THE DST_Harness SHALL reproduce the same failing Outcome and identify the same violated property.
3. WHEN a Simulation_Run ends with a failing Outcome, THE DST_Harness SHALL identify which Safety_Property or Liveness_Property was violated and the logical Virtual_Clock instant at which the violation was detected.
4. WHEN a Simulation_Run ends with a failing Outcome, THE DST_Harness SHALL persist that Run's Seed as a regression seed so that the same Seed is re-executed on subsequent runs of the test suite; THE DST_Harness SHALL persist regression seeds only for failing Runs.

### Requirement 3: In-Process Multi-Node Simulated Cluster Over the Production Seams

**User Story:** As a Vela developer, I want the harness to drive the real
consensus and replication code, so that the tests verify production behavior
rather than a separate model.

#### Acceptance Criteria

1. THE DST_Harness SHALL construct a Simulated_Cluster of a configured number of Sim_Nodes within a single operating-system process, with each Sim_Node hosting one Metadata_Replica and one Partition_Replica for every partition whose Replica_Set contains that Sim_Node.
2. THE DST_Harness SHALL drive partition consensus, log replication, metadata-group consensus, committed-entry application, and WAL recovery using the same `vela-raft`, `vela-core`, and `vela-log` logic used in production, substituting only the `Clock`, `Transport`, and `LogStorage` seams with the Virtual_Clock, the Sim_Network, and Sim_Storage.
3. THE DST_Harness SHALL route every inter-node message — `RequestVote`, `AppendEntries`, and their replies, for both partition groups and the metadata group — through the Sim_Network.
4. WHEN a topic is created during a Run, THE DST_Harness SHALL coordinate the per-partition Raft groups across the assigned Replica_Sets through the same metadata-commit-and-reconcile path used in production, such that each assigned Sim_Node hosts a running Partition_Replica for each partition it replicates.
5. THE DST_Harness SHALL hold each partition's Replica_Set and the Simulated_Cluster's Sim_Node set fixed for the duration of a Simulation_Run.

### Requirement 4: Deterministic Virtual Time and Event Scheduling

**User Story:** As a Vela developer, I want simulated time to advance only under
the harness's control, so that elections and timeouts are reproducible and free
of real-time flakiness.

#### Acceptance Criteria

1. THE Virtual_Clock SHALL advance logical time only when the DST_Harness advances it, and SHALL NOT derive its current instant from the wall clock.
2. WHEN a replica arms an election timer through the Virtual_Clock, THE Virtual_Clock SHALL schedule the firing using Seed-derived randomization within the prescribed 150–300 ms election window.
3. WHEN a replica arms a heartbeat timer through the Virtual_Clock, THE Virtual_Clock SHALL schedule the firing at the exact configured heartbeat interval.
4. THE DST_Harness SHALL advance the Virtual_Clock to the occurrence time of the next scheduled Event and deliver that Event, so that no Event is processed before any earlier-scheduled Event.
5. WHERE Clock_Skew is configured for a Sim_Node, THE Virtual_Clock SHALL apply that Node's bounded offset and rate to that Node's view of time while keeping the global Event ordering deterministic.
6. THE DST_Harness SHALL bound each Simulation_Run by a configured maximum number of Events or maximum simulated-time duration, and WHEN the Event that reaches that bound has been processed, THE DST_Harness SHALL end the Run after processing that Event.

### Requirement 5: Deterministic Network Fault Injection

**User Story:** As a Vela developer, I want to inject network faults
deterministically, so that I can test replication and partition tolerance under
adverse but reproducible conditions.

#### Acceptance Criteria

1. THE Sim_Network SHALL apply a configurable base one-way message latency to every delivered message.
2. WHERE message reordering is enabled, THE Sim_Network SHALL apply Seed-derived per-message delay within a configured bound, so that messages may be delivered in an order other than the order they were sent.
3. WHERE a non-zero drop probability is configured, THE Sim_Network SHALL drop each message with that Seed-derived probability, and a dropped message SHALL never be delivered.
4. WHERE a non-zero duplication probability is configured, THE Sim_Network SHALL deliver a Seed-derived duplicate of a message in addition to the original.
5. WHEN a network Partition between two sets of Sim_Nodes is in effect, THE Sim_Network SHALL not deliver any message whose sender and recipient lie on opposite sides of that Partition.
6. WHERE a Partition is configured as asymmetric, THE Sim_Network SHALL block delivery in only the specified direction while continuing to deliver in the other direction.
7. WHEN a Partition or other transient network fault is Healed, THE Sim_Network SHALL resume delivering messages between the affected Sim_Nodes for messages sent at or after the Heal; a Heal SHALL restore delivery only, and any drop, delay, reorder, or duplication fault that remains configured SHALL continue to apply to those messages.
8. THE Sim_Network SHALL apply every drop, delay, reorder, duplication, and Partition decision as a deterministic function of the Seed.

### Requirement 6: Node Crash and Restart Fault Injection

**User Story:** As a Vela developer, I want to crash and restart nodes during a
run, so that I can test leader failover and durable recovery.

#### Acceptance Criteria

1. WHEN a Node_Crash is applied to a Sim_Node, THE DST_Harness SHALL stop that Sim_Node's Metadata_Replica and Partition_Replicas from processing further Events and SHALL discard that Sim_Node's volatile state that Sim_Storage had not forced to stable storage.
2. WHEN a Node_Crash is applied to a Sim_Node, THE Sim_Network SHALL cease delivering messages to that Sim_Node until it is restarted.
3. WHEN a Node_Restart is applied to a crashed Sim_Node, THE DST_Harness SHALL recover that Sim_Node's Metadata_Replica and each of its Partition_Replicas from Sim_Storage, restoring the current term, the vote, the committed log prefix, and the applied catalogue as the durable contract guarantees.
4. WHEN a restarted Sim_Node has recovered its catalogue, THE DST_Harness SHALL start a Partition_Replica for every recovered partition whose Replica_Set contains that Sim_Node.
5. THE DST_Harness SHALL support crashing and restarting any subset of Sim_Nodes, including concurrent crashes of up to a minority of any group's voters, at Seed-derived times within the Run.
6. IF a Node_Crash leaves fewer than a majority of a group's voters running or mutually reachable, THEN THE DST_Harness SHALL treat the absence of progress for that group as expected and SHALL NOT report it as a Liveness_Property violation until a majority is restored and faults are Healed.

### Requirement 7: Storage and WAL Fault Injection

**User Story:** As a Vela developer, I want disk and WAL faults injected during a
run, so that durability guarantees are tested against crashes and I/O failures.

#### Acceptance Criteria

1. THE Sim_Storage SHALL implement the `LogStorage` contract such that, for a Run with no Storage_Fault, its observable results equal those of the production durable WAL for the same sequence of operations.
2. WHEN a Node_Crash occurs, THE Sim_Storage SHALL retain exactly the writes that had been forced to stable storage before the crash and SHALL discard writes that had not been forced, consistent with the configured durability policy.
3. WHERE a torn-tail Storage_Fault is configured, THE Sim_Storage SHALL model an incomplete trailing write at a Node_Crash such that recovery discards the torn tail down to the last intact record.
4. WHERE an I/O-error Storage_Fault is configured, THE Sim_Storage SHALL surface the error through the `LogStorage` result type at a Seed-derived operation rather than by panicking or by silently succeeding.
5. THE Sim_Storage SHALL apply every Storage_Fault decision as a deterministic function of the Seed.
6. THE DST_Harness SHALL preserve, across a Node_Crash and Node_Restart with no Storage_Fault configured, every record that was Acknowledged_Record before the crash.

### Requirement 8: Seed-Driven Client Workload Generation

**User Story:** As a Vela developer, I want the harness to generate client
workloads automatically, so that produce/consume/admin traffic exercises the
cluster under fault.

#### Acceptance Criteria

1. THE DST_Harness SHALL generate a Workload whose number of Client_Operations equals the Workload size declared in the Scenario_Parameters, composed of topic create, topic delete, produce, and consume operations selected as a deterministic function of the Seed and Scenario_Parameters.
2. THE DST_Harness SHALL generate produce Client_Operations comprising both keyed and keyless records across a Workload, routing each keyed record to a partition by the same partitioning rule used in production and routing each keyless record to a partition selected as a deterministic function of the Seed from the target topic's set of partitions.
3. WHEN the DST_Harness generates a produce Client_Operation, THE DST_Harness SHALL set the record's value length between 0 and 65,536 bytes and, where the record is keyed, set the key length between 1 and 256 bytes, choosing both the lengths and the contents as a deterministic function of the Seed.
4. WHEN the DST_Harness issues a Client_Operation to a Sim_Node that is not the current leader of the target partition or metadata group, THE DST_Harness SHALL follow the redirection the cluster returns toward the current leader, for up to 5 successive redirections per Client_Operation, rather than treating any such redirection as a failure.
5. IF 5 successive redirections do not reach a current leader for a Client_Operation, THEN THE DST_Harness SHALL record the unresolved-redirection error as a valid response rather than as a property violation.
6. IF the DST_Harness issues a Client_Operation while no leader is available for the target group, THEN THE DST_Harness SHALL record the returned no-leader error as a valid response rather than as a property violation.
7. THE DST_Harness SHALL interleave the issuance of Client_Operations with the Fault_Schedule on the Virtual_Clock, continuing to issue produce and consume Client_Operations while Node_Crashes, Node_Restarts, and network Partitions are in effect rather than pausing issuance until those Faults are Healed.

### Requirement 9: History Recording

**User Story:** As a Vela developer, I want every client operation and its outcome
recorded with logical timestamps, so that consistency can be checked precisely.

#### Acceptance Criteria

1. THE DST_Harness SHALL record, for each Client_Operation, its operation type and arguments, its invocation instant on the Virtual_Clock, its response instant on the Virtual_Clock, and its response, into the History.
2. WHEN a produce Client_Operation succeeds, THE DST_Harness SHALL record the target topic, the target partition, the produced value, and the committed Offset returned to the client as the response.
3. WHEN a consume Client_Operation succeeds, THE DST_Harness SHALL record the target topic, the target partition, the requested starting Offset, and the ordered records returned as the response.
4. WHEN a Client_Operation fails or is redirected, THE DST_Harness SHALL record the specific error or redirection as the response rather than discarding the operation.
5. THE History SHALL be reproducible: for an identical Seed and identical Scenario_Parameters, THE DST_Harness SHALL record an identical History.

### Requirement 10: Consensus Safety Property Checking

**User Story:** As a Vela developer, I want the harness to assert Raft's safety
properties, so that a consensus bug fails a test immediately.

#### Acceptance Criteria

1. THE Consistency_Checker SHALL verify that, for every partition group and for the metadata group, at most one replica is leader in any single term across the entire Run (Election Safety, Raft §5.2); THE Consistency_Checker SHALL detect and flag a same-term double-leader condition as a violation rather than preventing it from occurring.
2. THE Consistency_Checker SHALL verify that, for any two replicas of the same group whose logs both contain an entry at a given index with the same term, the logs are identical in all entries up to and including that index (Log Matching, Raft §5.3).
3. THE Consistency_Checker SHALL verify that, once an entry is Committed in a given term, that entry is present at the same index in the log of every replica that is or becomes leader in any later term (Leader Completeness, Raft §5.4).
4. THE Consistency_Checker SHALL verify that no replica applies a different entry at a given log index than any other replica applies at that index (State Machine Safety, Raft §5.4.3).
5. THE Consistency_Checker SHALL verify that each replica's committed index never decreases over the course of the Run (commit monotonicity).
6. IF any of these Safety_Properties is violated at any point in a Run, THEN THE DST_Harness SHALL end the Run with a failing Outcome identifying the violated property.

### Requirement 11: Client Consistency and Kafka-Parity Guarantees

**User Story:** As a Vela user, I want the cluster to provide Kafka-equivalent
durability and ordering, so that acknowledged data is never lost, duplicated out
of order, or reordered.

#### Acceptance Criteria

1. THE Consistency_Checker SHALL verify that every Acknowledged_Record appears in the partition's committed log at the Offset that was returned to the client, and remains present for the remainder of the Run.
2. THE Consistency_Checker SHALL verify that no Acknowledged_Record is lost across any sequence of Node_Crashes, Node_Restarts, leader failovers, and network Partitions in which at most a minority of that partition's Replica_Set fails at once.
3. THE Consistency_Checker SHALL verify that a partition's committed Offsets are contiguous from 0 and strictly increasing, with no gaps and no Offset assigned to two distinct records.
4. THE Consistency_Checker SHALL verify that every record returned by a successful consume Client_Operation is a Committed record and that records are returned in ascending Offset order.
5. THE Consistency_Checker SHALL verify that no consume Client_Operation returns a record that is not present in the target partition's committed log at the Offset returned, regardless of how that record became Committed (no phantom reads); a record committed through replication or recovery without a recorded client acknowledgment is not a phantom read.
6. THE Consistency_Checker SHALL verify that the recorded History is consistent with a single linearizable per-partition committed log — there exists a total order of that partition's committed appends, consistent with the returned Offsets and with the real-time order of non-overlapping Client_Operations, that every successful consume observes a prefix of.
7. THE Consistency_Checker SHALL verify that two Sim_Nodes that have applied the metadata group's log to the same commit index hold identical served topic catalogues.

### Requirement 12: Liveness Under Healed Faults

**User Story:** As a Vela user, I want the cluster to recover and make progress
once faults clear, so that failures are transient rather than permanent stalls.

#### Acceptance Criteria

1. WHILE a majority of a group's voters are running and mutually reachable and no further Faults are introduced, THE Simulated_Cluster SHALL elect exactly one leader for that group within a bounded number of Events.
2. WHEN faults affecting a partition are Healed and a majority of its Replica_Set is running and mutually reachable, THE Simulated_Cluster SHALL commit a subsequently produced record to that partition within a bounded number of Events.
3. WHEN faults are Healed and a majority of the metadata group is running and mutually reachable, THE Simulated_Cluster SHALL commit a subsequently submitted topic create or delete within a bounded number of Events.
4. WHEN a Run reaches a quiescent state with a majority available and all faults Healed, THE Consistency_Checker SHALL verify that every lagging replica's log has converged to the leader's committed log.
5. IF the Simulated_Cluster fails to make the required progress while a majority is available and faults are Healed, THEN THE DST_Harness SHALL wait until the bounded budget is exceeded before ending the Run with a failing Outcome identifying the unmet Liveness_Property.
6. WHILE fewer than a majority of a group's voters are available or a Partition prevents a majority from communicating, THE DST_Harness SHALL NOT require that group to make progress.

### Requirement 13: Failure Diagnostics, Minimization, and Artifacts

**User Story:** As a Vela developer, I want a failing run to produce a minimal,
saved, diagnosable artifact, so that I can find the root cause quickly.

#### Acceptance Criteria

1. WHEN a Simulation_Run fails, THE DST_Harness SHALL produce a Failure_Artifact containing the Seed, the Scenario_Parameters, the violated property, and the logical instant of detection.
2. WHEN a Simulation_Run fails, THE DST_Harness SHALL include in the Failure_Artifact a replayable Event trace and/or the recorded History sufficient to reconstruct the sequence leading to the violation.
3. WHEN a property-based search finds a failing scenario, THE DST_Harness SHALL shrink the scenario toward a minimal Counterexample that still reproduces the same violated property.
4. THE DST_Harness SHALL write its output to a path that the CI workflow can collect as a build artifact, writing a run summary regardless of Outcome and writing the full Failure_Artifact to that path when a Run fails.
5. THE DST_Harness SHALL emit, for a failing Run, structured diagnostics identifying the affected group, term, and replicas without requiring a re-run to obtain them.

### Requirement 14: Continuous Integration Execution

**User Story:** As a maintainer, I want DST to run automatically in CI, so that
regressions in consensus, replication, or durability are caught on every change.

#### Acceptance Criteria

1. THE CI workflow SHALL execute the DST suite automatically on every push to the main branch and on every pull request targeting the main branch.
2. THE CI workflow SHALL execute, on every Run of the DST suite, the persisted regression Seeds and a Seed-derived set of additional Seeds within a configured per-suite Run budget.
3. IF any DST Simulation_Run fails in CI, THEN THE CI workflow SHALL fail the build and surface the failing Seed and the violated property in the job output; THE CI workflow SHALL surface failure information only for Runs that actually failed in the current job and SHALL NOT surface failure information from a Run that succeeded.
4. WHEN a DST Simulation_Run fails in CI, THE CI workflow SHALL upload the Failure_Artifact so it is retrievable from the CI run, and THE CI workflow SHALL fail the build whether or not the artifact upload itself succeeds.
5. THE DST suite SHALL complete within a configured time budget that keeps total CI duration within acceptable bounds, expressed as a bounded number of Simulation_Runs and a bounded number of Events per Run.
6. THE DST suite SHALL be runnable locally through the standard `cargo test` workflow with the same deterministic behavior it exhibits in CI.

### Requirement 15: Configurable Scenarios and Coverage

**User Story:** As a Vela developer, I want to configure cluster shape and fault
intensity, so that I can target consensus, replication, failover, consistency,
and partition tolerance across a range of conditions.

#### Acceptance Criteria

1. THE DST_Harness SHALL accept Scenario_Parameters for the Sim_Node count, the replication factor, the partition count, the Fault intensities, the Workload size, and the per-Run Event or simulated-time budget.
2. THE DST_Harness SHALL support a cluster size and replication factor of at least three so that a partition's Raft group can tolerate the failure of a minority of its Replica_Set.
3. THE DST suite SHALL include scenarios that exercise leader election and failover, log replication and follower catch-up, network Partition and Heal, node crash and durable restart, and concurrent topic administration.
4. WHERE a Scenario_Parameter is unspecified for a Run, THE DST_Harness SHALL apply a documented default value.
5. THE DST_Harness SHALL reject a Scenario_Parameter set that is internally inconsistent — such as a replication factor greater than the Sim_Node count, or a partition count below 1 — with an error rather than executing an invalid Run; THE DST_Harness SHALL accept a replication factor equal to the Sim_Node count.

### Requirement 16: Guarantee Specification and Kafka-Parity Review

**User Story:** As a maintainer, I want a documented record of the guarantees we
claim and how they map to tests, so that the architectural review's outcome is
explicit and maintained.

#### Acceptance Criteria

1. THE Guarantee_Specification SHALL enumerate Vela's intended durability, ordering, and availability guarantees as concrete, testable statements.
2. THE Guarantee_Specification SHALL map each enumerated guarantee either to the Safety_Property or Liveness_Property in the DST suite that checks it, or to an explicitly documented gap not yet checked.
3. THE Guarantee_Specification SHALL record, for each enumerated guarantee, whether Vela's behavior is at parity with Kafka's corresponding guarantee, and SHALL describe any divergence.
4. THE Guarantee_Specification SHALL identify any guarantee that the current architecture cannot yet provide, so that known parity gaps are explicit rather than implicit.
