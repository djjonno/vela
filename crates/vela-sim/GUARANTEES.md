# Vela Guarantee Specification

This is the **Guarantee_Specification** for Vela (Requirement 16). It enumerates
the durability, ordering, and availability guarantees Vela intends to provide as
concrete, testable statements, and maps each one either to the
Safety_Property / Liveness_Property in the Deterministic Simulation Testing
(DST) suite that checks it, or to an explicitly documented **gap** that is not
yet checked. For every guarantee it records whether Vela is at **parity with
Kafka** and describes any divergence, and a final section names the guarantees
the current architecture cannot yet provide.

The DST_Harness (`vela-sim`) is the mechanism that holds Vela to this
specification over time: it composes the production `vela-core` / `vela-raft` /
`vela-log` types into an in-process, single-threaded, deterministic cluster and
asserts the properties below on every run.

## How to read this document

Each guarantee is mapped to one or more checks by their **canonical property
identifier**, written in the fully-qualified form
`PropertyId::Variant`. These identifiers are the exact variants of the
`PropertyId` enum in [`src/checker/mod.rs`](src/checker/mod.rs) — the single
canonical list of properties the suite actually checks. A guarantee with no
property is marked `GAP` and explained.

> **Drift guard.** A test (task 24.2) parses every `PropertyId::Variant` token
> in this file and asserts the set is exactly the set of variants the suite
> defines: no property the suite checks is left unmapped here, and this document
> never names a property that does not exist. Keep mappings in the
> `PropertyId::Variant` form so the guard can see them.

The eleven properties the suite defines, by family:

| Family | Properties |
|---|---|
| Raft safety (Req 10) | `PropertyId::ElectionSafety`, `PropertyId::LogMatching`, `PropertyId::LeaderCompleteness`, `PropertyId::StateMachineSafety`, `PropertyId::CommitMonotonicity` |
| Client consistency / Kafka parity (Req 11) | `PropertyId::AcknowledgedRecordDurability`, `PropertyId::OffsetIntegrity`, `PropertyId::ConsumeReadValidity`, `PropertyId::PerPartitionLinearizability`, `PropertyId::MetadataConvergence` |
| Liveness under healed faults (Req 12) | `PropertyId::Liveness` |

A note on scope that recurs below: Vela's unit of ordering, replication, and
consensus is the **partition**, and each partition is an **independent Raft
group**. Every guarantee that mentions ordering or linearizability is therefore
**per partition**; there is no cross-partition ordering or atomicity. This is
the same boundary Kafka draws (ordering is per partition), so it is parity, not a
gap — but it is the root cause of the genuine gaps listed at the end (no
cross-partition transactions, no exactly-once).

---

## Durability guarantees

| ID | Guarantee (testable statement) | Checked by | Kafka parity |
|---|---|---|---|
| D1 | An **acknowledged record is never lost**: every record for which the cluster returned a committed offset still appears in that partition's committed log, at the returned offset with the returned value, for the remainder of the run — across any sequence of crashes, restarts, leader failovers, and partitions in which **at most a minority** of the partition's Replica_Set fails at once. | `PropertyId::AcknowledgedRecordDurability` | **At parity.** Matches Kafka's `acks=all` durability: an acknowledged write survives any minority failure. See divergence note below on the acknowledgement model. |
| D2 | A committed entry **survives leader failover**: once committed in some term it is present, at the same index, in every replica that later becomes leader, so a new leader can never silently drop committed data. | `PropertyId::LeaderCompleteness` | **At parity.** Equivalent to Kafka's leader-election constraint that a new leader must come from the in-sync set and cannot truncate committed data. |
| D3 | **No divergent committed history**: no two replicas ever hold a different committed entry at the same index. | `PropertyId::StateMachineSafety` | **At parity.** Equivalent to Kafka's guarantee that replicas of a partition do not commit conflicting records at the same offset. |
| D4 | **Durable restart recovery**: a record acknowledged before a crash is preserved across a crash and durable restart with no storage fault configured; only writes **not** forced to stable storage are lost at a crash. | `PropertyId::AcknowledgedRecordDurability` (the no-storage-fault crash/restart case is its special instance; the storage boundary itself is exercised by the harness's Sim_Storage over the real `DurableWal`) | **At parity** for the durability boundary (fsynced data survives, unsynced tail is lost on crash), given Vela runs the WAL with `SyncPolicy::Always`. Divergence: Kafka's default flush is asynchronous/OS-page-cache driven and tunable; Vela's tested policy forces every write. |

**Durability caveats.**

- D1 holds **only under minority failure** of a partition's Replica_Set. If a
  majority of a partition's replicas are simultaneously lost (e.g. permanent disk
  loss on a majority), acknowledged records can be lost; the suite does not
  assert durability across majority loss because consensus cannot provide it.
  This is the same limit Kafka has when a majority/all of the ISR is lost.
- D4 depends on the configured durability policy. The DST storage seam models the
  real WAL durability boundary (`crash()` drops the un-fsynced tail; a torn-tail
  fault recovers to the last intact record; an armed I/O error surfaces through
  the `LogStorage` result rather than silently succeeding). Durability is only as
  strong as the policy in force — under `SyncPolicy::Always` (what the harness
  uses) an acknowledged record is fsynced before acknowledgement.

---

## Ordering guarantees

| ID | Guarantee (testable statement) | Checked by | Kafka parity |
|---|---|---|---|
| O1 | **Contiguous, gap-free, monotonic offsets**: a partition's committed offsets are contiguous from 0, strictly increasing, with no gaps and no offset assigned to two distinct records. | `PropertyId::OffsetIntegrity` | **At parity.** Matches Kafka's per-partition monotonic offsets. (Divergence: Kafka offsets can advance by more than one per batch and reflect log-start/retention truncation; Vela offsets are dense from 0 with no retention/compaction yet — see gaps.) |
| O2 | **Consumers read only committed records, in order, with no phantom reads**: every record a successful consume returns is a committed record present at the returned offset, returned in ascending offset order; nothing is returned that is not in the committed log. | `PropertyId::ConsumeReadValidity` | **At parity.** Equivalent to a Kafka consumer reading only up to the high-watermark (committed) in offset order. |
| O3 | **Per-partition linearizability**: the recorded client History is consistent with a single linearizable per-partition committed log — a total order of committed appends consistent with the returned offsets and with the real-time order of non-overlapping operations, of which every successful consume observes a prefix-range. | `PropertyId::PerPartitionLinearizability` | **At parity, per partition.** Kafka likewise guarantees a single total order per partition and not across partitions. |
| O4 | **Log Matching**: any two replicas of a partition that agree on `(index, term)` for an entry agree on the entire log prefix up to that entry. | `PropertyId::LogMatching` | **At parity.** The Raft structural invariant underpinning consistent replicated order; equivalent in effect to Kafka replicas sharing an identical committed prefix. |
| O5 | **Commit index never regresses**: each replica's commit index is monotonic over a run. | `PropertyId::CommitMonotonicity` | **At parity.** A consumer never observes the committed frontier move backwards, as in Kafka's monotonic high-watermark. |
| O6 | **Metadata catalogue convergence**: two nodes that have applied the metadata group's log to the same commit index serve identical topic catalogues. | `PropertyId::MetadataConvergence` | **Roughly at parity** with Kafka's metadata convergence (KRaft controller quorum / older ZooKeeper). Divergence: Vela replicates metadata through a dedicated per-cluster `__meta/0` Raft group; convergence is only asserted *between nodes at the same metadata commit index*, so nodes lagging on metadata may transiently serve stale catalogues (expected, not a violation). |

**Ordering caveats.**

- O1's "dense from 0" property is **stronger** than Kafka in one direction
  (Vela has no retention or log-start advancement yet) and weaker in another
  (Kafka offsets carry batch/transaction-control semantics Vela does not have).
- O3 is **per partition only** and is specialized to Vela's
  single-writer-per-partition (single Raft leader) offset model. There is no
  cross-partition linearizability or global total order — see gaps G1.
- O3's real-time check only constrains **non-overlapping** operations; concurrent
  (overlapping) produces may land in either order, exactly as Kafka permits for
  concurrent producers to one partition.

---

## Availability / liveness guarantees

| ID | Guarantee (testable statement) | Checked by | Kafka parity |
|---|---|---|---|
| A1 | **Progress under a healthy majority**: while a majority of a group's voters are running and mutually reachable and no further faults are introduced, exactly one leader is elected, a subsequently produced record commits, and a subsequent topic create/delete commits — all within a bounded budget; and at a quiescent healed state every lagging replica converges to the leader's committed log. | `PropertyId::Liveness` | **At parity.** Equivalent to Kafka recovering and resuming writes once a majority/ISR is available, on a per-Raft-group basis. |
| A2 | **At most one leader per term per group** (so writes are not split-brained while still being available). | `PropertyId::ElectionSafety` | **At parity.** Equivalent to Kafka having a single leader per partition at a time (single epoch). |
| A3 | **Failures are transient, not permanent stalls**: a group lacking a reachable majority is *not required* to make progress, but is expected to resume once a majority is restored and faults heal (bounded-budget progress is re-armed at each heal). | `PropertyId::Liveness` | **At parity.** Kafka likewise blocks `acks=all` writes without a sufficient in-sync majority and resumes when one returns. |

**Liveness caveats (important — checked with conditions).**

- `PropertyId::Liveness` is asserted **only under favorable conditions**: a
  majority of a group's voters running *and every running voter pairwise mutually
  reachable*, with no new fault, sustained past the budget. The favorable
  condition is deliberately **conservative**: in awkward partial-partition shapes
  (a clean majority exists but an extra running voter is partitioned away) the
  checker does **not** require progress, so some real stalls there go unchecked.
  This is the safe direction for a property used to *fail* a run — it never
  raises a violation it cannot justify.
- No progress is required, ever, of a group with only a minority available or one
  a partition keeps from forming a majority (Requirement 6.6 / 12.6). Absence of
  progress in those states is expected, not a violation.
- A liveness violation is declared **only after** the bounded budget is exceeded,
  never eagerly.

---

## Guarantees the current architecture cannot yet provide

These are explicit parity gaps (Requirement 16.4). They are **not** checked by
any `PropertyId`; they are recorded here so the boundary of what Vela claims is
visible rather than implicit. Each is a `GAP`.

| ID | Missing guarantee | Status | Kafka comparison |
|---|---|---|---|
| G1 | **Cross-partition / cross-topic ordering or atomicity.** Each partition is an independent Raft group; there is no global order and no atomic multi-partition write. | `GAP` — architectural; not planned to change (matches Kafka's model). | Kafka also provides no cross-partition ordering. Vela is at parity on the *boundary*, but the stronger cross-partition guarantees below build on it and are absent. |
| G2 | **Exactly-once semantics (EOS) / idempotent producer.** Vela has no producer ID, no sequence-number dedup, and no transactional coordinator, so a retried produce after an uncertain acknowledgement can create a duplicate record at a new offset. The suite checks that *acknowledged* records are durable and ordered, not that produces are deduplicated. | `GAP` — not yet implemented. | **Behind Kafka.** Kafka offers idempotent producers and transactions (EOS). Vela currently provides at-least-once on producer retry. |
| G3 | **Transactions across partitions/topics** (atomic multi-partition append, transactional read-process-write). | `GAP` — depends on G1/G2; not yet designed. | **Behind Kafka.** Kafka has transactions; Vela has none. |
| G4 | **Consumer groups: offset commit, group membership, and rebalancing.** Vela consumes by explicit start offset; there is no server-side committed consumer offset, no group coordinator, and no partition assignment/rebalance protocol. (`PropertyId::ConsumeReadValidity` checks read correctness of an explicit-offset consume, not group semantics.) | `GAP` — not yet implemented. | **Behind Kafka.** Kafka has consumer groups and `__consumer_offsets`. Vela does not. |
| G5 | **Dynamic membership / replica reassignment.** A partition's Replica_Set and the cluster's node set are fixed for a run; there is no joint-consensus voter add/remove or partition reassignment. | `GAP` — explicit DST non-goal; not yet implemented. | **Behind Kafka.** Kafka supports partition reassignment and broker add/remove online. |
| G6 | **Configurable acknowledgement levels and ISR semantics.** Vela commits via Raft majority (the tested model is effectively `acks=all` over the Raft quorum); there is no `acks=0/1` selection, no min-ISR tuning, and no unclean-leader-election toggle. | `GAP` — only the majority-commit model exists and is tested (D1). | **Partial.** Kafka exposes `acks`, `min.insync.replicas`, and unclean-leader-election. Vela offers only the strong (majority) setting. |
| G7 | **Retention, compaction, and log-start truncation.** Offsets are dense from 0 and never reclaimed; there is no time/size retention, no log compaction, and no consumer-visible log-start offset advancement. | `GAP` — not yet implemented. | **Behind Kafka.** Kafka has retention and compaction. Vela's offset space is append-only and unbounded for now. |
| G8 | **Durability under majority loss / quorum loss.** As noted in D1, acknowledged data can be lost if a majority of a partition's replicas are permanently lost; the suite asserts durability only under minority failure. | `GAP` — fundamental consensus limit, documented for clarity. | At parity with Kafka, which also cannot preserve data if the full ISR is lost without unclean election. |

---

## Property coverage checklist (machine-checked)

Every property the suite defines must appear in the mapping above. Listed here
once more in `PropertyId::Variant` form for the drift guard, with the guarantee
each backs:

- `PropertyId::ElectionSafety` → A2 (single leader per term; no split brain)
- `PropertyId::LogMatching` → O4 (consistent replicated log prefix)
- `PropertyId::LeaderCompleteness` → D2 (committed data survives failover)
- `PropertyId::StateMachineSafety` → D3 (no divergent committed history)
- `PropertyId::CommitMonotonicity` → O5 (commit index never regresses)
- `PropertyId::AcknowledgedRecordDurability` → D1, D4 (acknowledged record never lost under minority failure)
- `PropertyId::OffsetIntegrity` → O1 (contiguous, gap-free, monotonic offsets)
- `PropertyId::ConsumeReadValidity` → O2 (committed-only, in-order reads; no phantom reads)
- `PropertyId::PerPartitionLinearizability` → O3 (per-partition linearizable log)
- `PropertyId::MetadataConvergence` → O6 (metadata catalogue convergence)
- `PropertyId::Liveness` → A1, A3 (bounded progress under a healed majority)

No guarantee in the durability/ordering/availability tables is mapped to a
property name that is not a `PropertyId` variant, and every `PropertyId` variant
the suite defines is mapped to at least one guarantee above.
