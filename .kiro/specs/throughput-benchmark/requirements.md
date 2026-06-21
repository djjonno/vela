# Requirements Document

## Introduction

This feature adds a **throughput benchmark** to Vela: a repeatable measurement of
the system's end-to-end produce and consume throughput — *topic data in* and
*topic data out* — that runs automatically as part of CI.

Vela's two core data-path operations are **produce** (append ordered event
records to a topic's partitions) and **consume** (read those ordered records
back). This benchmark drives both operations against a running Vela cluster
through the same public `vela-client` `Producer` and `Consumer` APIs that
external clients use, measures how many records and bytes flow through each
operation per second, and emits a machine-readable report.

The deliverable is the **Throughput_Benchmark**: a tool that stands up (or
connects to) a **Cluster_Under_Test**, runs a configurable **Workload** through a
**Producer_Phase** and a **Consumer_Phase**, measures **Produce_Throughput** and
**Consume_Throughput** over a defined **Measurement_Window**, verifies that every
produced record is consumed back (so the numbers measure real work, not a broken
path), and writes a **Benchmark_Report**. Alongside the machine-readable
Benchmark_Report and the human-readable standard-output summary, each
Benchmark_Run also renders an **HTML_Report**: a browser-viewable rendering of the
same outcome that a developer can open directly. The benchmark is then wired into
the existing GitHub Actions workflow at `.github/workflows/ci.yml` so it executes
on every push and pull request, fails the build on a benchmark error, and
publishes the report — including the HTML_Report — as a retrievable artifact.

### Measurement-environment constraint

Shared CI runners are noisy: absolute throughput numbers vary run to run with the
hardware and contention the runner happens to get. This feature therefore treats
the CI benchmark primarily as a **continuously exercised, regression-detecting
measurement that always runs and always reports**, not as a precise performance
gate. Build failure is driven by benchmark **errors** (operation failures,
unmet data-integrity checks, exceeded time budget) and, only where explicitly
configured, by a measured throughput falling below a conservative **floor**
threshold — not by run-to-run variance against a moving baseline.

### Non-goals

- **Precise, low-variance performance gating on shared CI runners.** Strict
  regression detection against a baseline is out of scope; the CI benchmark
  reports throughput and fails only on errors or a configured floor.
- **Latency / tail-latency benchmarking.** This feature measures throughput
  (records and bytes per second), not per-operation latency distributions.
- **Multi-machine / networked load generation.** The Cluster_Under_Test and the
  load generator run within the CI job's single host.
- **Benchmarking the stream-processing runtime.** Only the produce and consume
  data path is measured.
- **Choosing the implementation mechanism** (dedicated benchmark crate vs.
  `cargo bench` / Criterion vs. a `vela-ctl` subcommand, and whether the
  Cluster_Under_Test is in-process or a local multi-node cluster). Those are
  design-phase decisions; these requirements define observable behavior only.

## Glossary

- **Vela**: The distributed event-streaming platform whose throughput is measured.
- **Throughput_Benchmark**: The tool introduced by this feature that generates a
  Workload, drives produce and consume against the Cluster_Under_Test, measures
  throughput, and emits a Benchmark_Report.
- **Cluster_Under_Test**: The running Vela cluster the Throughput_Benchmark drives
  for a single Benchmark_Run.
- **Benchmark_Run**: One execution of the Throughput_Benchmark against a
  Cluster_Under_Test using one set of Workload_Parameters, producing one
  Benchmark_Report and one pass/fail Outcome.
- **Workload_Parameters**: The configurable inputs to a Benchmark_Run: record
  count, record value size in bytes, key mode, partition count, producer
  concurrency, target topic name, and warmup count.
- **Workload**: The set of records, derived from the Workload_Parameters, that the
  Throughput_Benchmark produces and then consumes during a Benchmark_Run.
- **Producer_Phase**: The portion of a Benchmark_Run in which the
  Throughput_Benchmark produces the Workload's records to the target topic.
- **Consumer_Phase**: The portion of a Benchmark_Run in which the
  Throughput_Benchmark consumes records back from the target topic.
- **Acknowledged_Record**: A produced record for which the Cluster_Under_Test
  returned a success response carrying a committed offset.
- **Measurement_Window**: The wall-clock interval of a phase, beginning after any
  Warmup completes and ending when the phase's measured records have all been
  acknowledged (Producer_Phase) or read (Consumer_Phase), over which throughput is
  computed.
- **Warmup**: An initial set of operations in a phase, sized by the warmup count,
  that are executed but excluded from the Measurement_Window.
- **Produce_Throughput**: Acknowledged records per second and acknowledged payload
  bytes per second over the Producer_Phase Measurement_Window.
- **Consume_Throughput**: Records read per second and payload bytes read per second
  over the Consumer_Phase Measurement_Window.
- **Benchmark_Report**: The structured output of a Benchmark_Run recording the
  Workload_Parameters, the measured throughput figures, the record and byte
  counts, and the total elapsed time.
- **HTML_Report**: A browser-viewable HTML rendering of a Benchmark_Run's outcome,
  produced in addition to the Benchmark_Report and the standard-output summary,
  that presents the Workload_Parameters, the Outcome, the Produce_Throughput and
  Consume_Throughput in records per second and bytes per second, the
  Acknowledged_Record count, the total payload bytes, the total elapsed time, and,
  for a failing Outcome, the failure reason.
- **Floor_Threshold**: An optional, configured minimum acceptable throughput value
  for a Benchmark_Run, below which the Outcome is a failure.
- **Outcome**: The pass/fail result of a Benchmark_Run.
- **CI_Workflow**: The GitHub Actions workflow at `.github/workflows/ci.yml` that
  runs Vela's checks on push and pull request to `main`.
- **Benchmark_Job**: The CI_Workflow job that executes a Benchmark_Run and
  publishes its Benchmark_Report.

## Requirements

### Requirement 1: Measure Produce Throughput (Topic Data In)

**User Story:** As a Vela developer, I want to measure how fast records can be
produced into a topic, so that I can track the system's write throughput over
time.

#### Acceptance Criteria

1. WHEN a Benchmark_Run executes its Producer_Phase, THE Throughput_Benchmark SHALL produce the configured record count of records to the target topic through the `vela-client` Producer API.
2. THE Throughput_Benchmark SHALL count a produced record toward Produce_Throughput only after that record becomes an Acknowledged_Record and only when that record is a measured record that is not a Warmup operation.
3. WHEN the Producer_Phase Measurement_Window ends, THE Throughput_Benchmark SHALL compute Produce_Throughput as the number of Acknowledged_Records counted within the Measurement_Window divided by the Measurement_Window duration in seconds, and as the sum of the value byte lengths of those Acknowledged_Records divided by the Measurement_Window duration in seconds.
4. THE Throughput_Benchmark SHALL measure the Producer_Phase Measurement_Window duration as the elapsed wall-clock time from the first measured produce invocation to the acknowledgment of the last measured record.
5. IF the Producer_Phase Measurement_Window duration is zero, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and a descriptive error rather than reporting an undefined Produce_Throughput.

### Requirement 2: Measure Consume Throughput (Topic Data Out)

**User Story:** As a Vela developer, I want to measure how fast records can be
consumed from a topic, so that I can track the system's read throughput over time.

#### Acceptance Criteria

1. WHEN a Benchmark_Run executes its Consumer_Phase, THE Throughput_Benchmark SHALL consume records from every partition of the target topic through the `vela-client` Consumer API, beginning at offset zero (the first offset) of each partition.
2. THE Throughput_Benchmark SHALL continue the Consumer_Phase until the total number of records read during the Consumer_Phase, including any Warmup reads, equals the number of Acknowledged_Records from the Producer_Phase.
3. THE Throughput_Benchmark SHALL count a consumed record toward Consume_Throughput only after the record has been delivered to the Throughput_Benchmark by the `vela-client` Consumer API and after the Consumer_Phase Warmup has completed.
4. WHEN the Consumer_Phase Measurement_Window ends, THE Throughput_Benchmark SHALL compute Consume_Throughput as the number of records read within the Measurement_Window divided by the Measurement_Window duration in seconds, and as the total payload bytes of those records divided by the Measurement_Window duration in seconds.
5. THE Throughput_Benchmark SHALL measure the Consumer_Phase Measurement_Window duration as the elapsed wall-clock time from the first measured consume invocation to the receipt of the last measured record.

### Requirement 3: Drive the Real Produce/Consume Data Path End to End

**User Story:** As a Vela developer, I want the benchmark to exercise the same
client path real clients use, so that the measured throughput reflects production
behavior.

#### Acceptance Criteria

1. THE Throughput_Benchmark SHALL drive produce operations and consume operations through the public `vela-client` Producer and Consumer APIs rather than bypassing the client or invoking partition logs directly.
2. THE Throughput_Benchmark SHALL run each Benchmark_Run against a Cluster_Under_Test that serves produce and consume requests over the cluster's standard request path.
3. WHEN a Benchmark_Run begins, THE Throughput_Benchmark SHALL create the target topic with the configured partition count before the Producer_Phase begins, where the configured partition count is an integer of at least 1.
4. IF the target topic already exists at the start of a Benchmark_Run, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and a descriptive error indicating the topic already exists, rather than measuring against a pre-populated topic.
5. IF the configured partition count is less than 1, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and a descriptive error indicating the invalid partition count, before any topic is created.
6. IF target topic creation does not succeed at the start of a Benchmark_Run, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and a descriptive error indicating the creation failure, without entering the Producer_Phase.
7. IF a produce operation or a consume operation through the `vela-client` Producer or Consumer API returns an error during a Benchmark_Run, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and a descriptive error identifying the failed operation, rather than reporting a throughput measurement.

### Requirement 4: Configurable Workload

**User Story:** As a Vela developer, I want to configure the benchmark workload,
so that I can measure throughput under different record sizes, partition counts,
and concurrency levels.

#### Acceptance Criteria

1. THE Throughput_Benchmark SHALL accept Workload_Parameters specifying the record count (an integer from 1 to 1,000,000,000), the record value size in bytes (an integer from 0 to 10,485,760), the key mode (keyed or keyless), the partition count (an integer from 1 to 10,000), the producer concurrency (an integer from 1 to 10,000), the target topic name (a string of 1 to 255 characters), and the warmup count (an integer from 0 to the record count minus 1).
2. WHERE a Workload_Parameter is not supplied for a Benchmark_Run, THE Throughput_Benchmark SHALL apply a documented default value, within that parameter's accepted range, before the Producer_Phase begins.
3. WHERE the key mode is configured as keyed, THE Throughput_Benchmark SHALL attach a key to every produced record so that records are routed by the cluster's keyed partitioning rule.
4. WHERE the producer concurrency is configured to a value greater than one, THE Throughput_Benchmark SHALL keep up to that number of produce requests in flight concurrently during the Producer_Phase.
5. IF any supplied Workload_Parameter falls outside its accepted range, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and a descriptive error identifying the offending parameter before the Producer_Phase begins.
6. IF the configured warmup count is greater than or equal to the configured record count, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and a descriptive error identifying the invalid warmup count before the Producer_Phase begins.

### Requirement 5: Data-Integrity Verification

**User Story:** As a Vela developer, I want the benchmark to confirm every
produced record is read back, so that a reported throughput reflects real work
rather than a silently broken path.

#### Acceptance Criteria

1. WHEN the Consumer_Phase completes, THE Throughput_Benchmark SHALL verify that the number of records read equals the number of Acknowledged_Records produced during the Producer_Phase.
2. WHEN the Consumer_Phase completes, THE Throughput_Benchmark SHALL verify that the payload of each read record equals the payload deterministically generated for that record's position.
3. IF the number of records read equals the number of Acknowledged_Records produced during the Producer_Phase AND every read record's payload equals its expected payload, THEN THE Throughput_Benchmark SHALL complete the Benchmark_Run with a successful Outcome.
4. IF the per-Benchmark_Run time budget (a Workload_Parameter with a default of 60 seconds, constrained to the range 1 to 86,400 seconds) elapses before the number of records read reaches the number of Acknowledged_Records, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome, emit an error indicating the count of records read and the count of Acknowledged_Records expected, and retain the recorded counts.
5. IF the number of records read exceeds the number of Acknowledged_Records produced during the Producer_Phase, OR any read record's payload does not equal its expected payload, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome, emit an error indicating the integrity violation and the affected record position, and retain the recorded counts.
6. THE Throughput_Benchmark SHALL generate the value content of each produced record as a deterministic function of the Workload_Parameters, such that two Benchmark_Runs with identical Workload_Parameters produce records carrying identical payloads.

### Requirement 6: Emit a Benchmark Report

**User Story:** As a Vela developer, I want each run to produce a structured
report, a stdout summary, and a browser-viewable HTML report, so that throughput
results can be read by a person at a glance, opened in a browser, and parsed by
tooling.

#### Acceptance Criteria

1. WHEN a Benchmark_Run reaches an Outcome, THE Throughput_Benchmark SHALL emit exactly one Benchmark_Report recording the Workload_Parameters, the Outcome, the Produce_Throughput in records per second and bytes per second, the Consume_Throughput in records per second and bytes per second, the number of Acknowledged_Records, the total payload bytes, and the total elapsed wall-clock time of the Benchmark_Run.
2. THE Throughput_Benchmark SHALL emit the Benchmark_Report such that every reported value appears as a separately named, individually addressable field.
3. THE Throughput_Benchmark SHALL emit to standard output a human-readable summary stating the Produce_Throughput and Consume_Throughput in both records per second and bytes per second, or an explicit "not measured" indication for any figure that is unavailable.
4. WHEN a Benchmark_Run ends with a failing Outcome, THE Throughput_Benchmark SHALL record the failure reason as a named field in the Benchmark_Report and SHALL write that reason to standard output.
5. WHERE a phase did not complete, THE Throughput_Benchmark SHALL record that phase's throughput figures in the Benchmark_Report as explicitly absent rather than as a partial or zero measured value.
6. WHEN a Benchmark_Run reaches an Outcome, THE Throughput_Benchmark SHALL emit exactly one HTML_Report, in addition to the Benchmark_Report and the standard-output summary, that renders the Workload_Parameters, the Outcome, the Produce_Throughput in records per second and bytes per second, the Consume_Throughput in records per second and bytes per second, the number of Acknowledged_Records, the total payload bytes, and the total elapsed wall-clock time of the Benchmark_Run.
7. WHEN a Benchmark_Run ends with a failing Outcome, THE Throughput_Benchmark SHALL render the failure reason in the HTML_Report.
8. WHERE a phase did not complete, THE Throughput_Benchmark SHALL render that phase's throughput figures in the HTML_Report as an explicit "not measured" indication rather than as a partial or zero measured value.

### Requirement 7: Continuous Integration Execution

**User Story:** As a Vela developer, I want the throughput benchmark to run in
CI, so that the produce/consume data path is exercised and measured on every
change.

#### Acceptance Criteria

1. WHEN code is pushed to `main` or a pull request targeting `main` is opened or updated, THE CI_Workflow SHALL execute exactly one Benchmark_Job for that trigger.
2. WHEN the Benchmark_Job runs, THE Benchmark_Job SHALL execute exactly one Benchmark_Run using the CI-designated set of Workload_Parameters configured in the CI_Workflow.
3. WHEN a Benchmark_Run executed by the Benchmark_Job ends with a failing Outcome, THE Benchmark_Job SHALL exit with a non-zero status that marks the CI_Workflow as failed.
4. WHEN a Benchmark_Run executed by the Benchmark_Job completes, THE Benchmark_Job SHALL publish the Benchmark_Report and the HTML_Report as CI artifacts retrievable for at least 7 days after the Benchmark_Job ends, regardless of the Outcome.
5. IF a Benchmark_Run executed by the Benchmark_Job does not complete within a configured maximum wall-clock duration of 30 minutes, THEN THE Benchmark_Job SHALL terminate the Benchmark_Run and exit with a non-zero status that marks the CI_Workflow as failed.

### Requirement 8: Bounded CI Resource Usage

**User Story:** As a Vela developer, I want the CI benchmark to stay within a
sensible time budget, so that it gives a useful signal without consuming
excessive CI minutes.

#### Acceptance Criteria

1. THE Benchmark_Job SHALL use a CI-designated set of Workload_Parameters sized so that a Benchmark_Run completes within a configured time budget, where the configured time budget SHALL NOT exceed 300 seconds of total elapsed wall-clock time on the CI runner.
2. THE Throughput_Benchmark SHALL measure a Benchmark_Run's time budget as the total elapsed wall-clock time from the start of the Benchmark_Run, inclusive of Cluster_Under_Test startup and target topic creation, to the completion of the Consumer_Phase.
3. IF a Benchmark_Run's elapsed wall-clock time reaches its configured time budget before the Benchmark_Run completes, THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run, set its Outcome to failing, and emit a descriptive error indicating that the configured time budget was exceeded.
4. THE Benchmark_Job SHALL run as a CI job distinct from the existing format, lint, test, MSRV, and DST jobs, so that a benchmark failure is attributable to the benchmark.

### Requirement 9: Operation-Error Handling

**User Story:** As a Vela developer, I want the benchmark to fail loudly on
operation errors, so that a broken produce or consume path is caught rather than
hidden behind a throughput number.

#### Acceptance Criteria

1. IF a produce operation returns an error that the `vela-client` retry path does not resolve during the Producer_Phase, THEN THE Throughput_Benchmark SHALL stop issuing further produce operations, terminate the Benchmark_Run with a failing Outcome, and record in the Benchmark_Report the failed operation type, the target topic and partition, and the underlying error cause.
2. IF a consume operation returns an error that the `vela-client` retry path does not resolve during the Consumer_Phase, THEN THE Throughput_Benchmark SHALL stop issuing further consume operations, terminate the Benchmark_Run with a failing Outcome, and record in the Benchmark_Report the failed operation type, the target topic and partition, and the underlying error cause.
3. IF the Cluster_Under_Test does not become ready to serve requests within the configured startup time budget (default 60 seconds, configurable within the range 1 to 600 seconds), THEN THE Throughput_Benchmark SHALL terminate the Benchmark_Run with a failing Outcome and record in the Benchmark_Report an error indicating that the Cluster_Under_Test did not become ready within the startup time budget.
4. WHEN THE Throughput_Benchmark terminates a Benchmark_Run with a failing Outcome due to an operation error, THE Throughput_Benchmark SHALL mark the Benchmark_Report Outcome as failing and SHALL NOT present any measured throughput value for that Benchmark_Run as a successful result.

### Requirement 10: Warmup and Measurement Window

**User Story:** As a Vela developer, I want startup effects excluded from the
measurement, so that the reported throughput reflects steady-state operation.

#### Acceptance Criteria

1. WHERE a warmup count greater than zero is configured, THE Throughput_Benchmark SHALL execute exactly that number of produce operations at the start of the Producer_Phase before the Producer_Phase Measurement_Window opens, and SHALL begin the Producer_Phase Measurement_Window only after all warmup produce operations have completed.
2. WHERE a warmup count greater than zero is configured, THE Throughput_Benchmark SHALL execute exactly that number of consume operations at the start of the Consumer_Phase before the Consumer_Phase Measurement_Window opens, and SHALL begin the Consumer_Phase Measurement_Window only after all warmup consume operations have completed.
3. THE Throughput_Benchmark SHALL exclude topic creation time and Cluster_Under_Test startup time from both the Producer_Phase Measurement_Window and the Consumer_Phase Measurement_Window.
4. WHERE a warmup count of zero is configured or no warmup count is configured, THE Throughput_Benchmark SHALL begin each phase's Measurement_Window with the first operation of that phase and SHALL include every operation of that phase within its Measurement_Window.
5. IF a configured warmup count is negative, is non-integer, or is greater than or equal to the total number of operations planned for its phase, THEN THE Throughput_Benchmark SHALL reject the configuration before starting any phase, SHALL not open either Measurement_Window, and SHALL report an error indicating the invalid warmup count.
6. IF a warmup operation fails before its phase's Measurement_Window opens, THEN THE Throughput_Benchmark SHALL terminate the affected phase without opening that phase's Measurement_Window and SHALL report an error indicating the warmup failure.

### Requirement 11: Optional Throughput Floor

**User Story:** As a Vela developer, I want the option to fail a run when
throughput drops below a conservative floor, so that a severe regression is
caught without making CI flaky on normal variance.

#### Acceptance Criteria

1. WHERE a Floor_Threshold is configured for Produce_Throughput, IF the measured Produce_Throughput in records per second is strictly below that Floor_Threshold (a value equal to or above the Floor_Threshold passes), THEN THE Throughput_Benchmark SHALL set the Benchmark_Run Outcome to failing and record the floor breach, the measured Produce_Throughput in records per second, and the configured Floor_Threshold as the failure reason in the Benchmark_Report.
2. WHERE a Floor_Threshold is configured for Consume_Throughput, IF the measured Consume_Throughput in records per second is strictly below that Floor_Threshold (a value equal to or above the Floor_Threshold passes), THEN THE Throughput_Benchmark SHALL set the Benchmark_Run Outcome to failing and record the floor breach, the measured Consume_Throughput in records per second, and the configured Floor_Threshold as the failure reason in the Benchmark_Report.
3. WHERE no Floor_Threshold is configured for a Benchmark_Run, THE Throughput_Benchmark SHALL determine the Outcome from operation errors, data-integrity verification, and the time budget alone, and SHALL NOT fail the Benchmark_Run on the measured Produce_Throughput or Consume_Throughput value.
