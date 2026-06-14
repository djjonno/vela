# Product

## Vela

Vela is a distributed event-streaming and stream-processing platform. It is a
ground-up Rust re-implementation of [kerala](https://github.com/djjonno/kerala)
(originally Kotlin), with a fundamentally different consensus model and an
embedded stream-processing runtime.

## What it does

- **Produce / Consume** — clients create topics, then produce and consume ordered
  event records on them.
- **Distribute** — topics are split into partitions; partitions and their leaders
  are balanced across the cluster.
- **Process (planned)** — run stream processors that transform or aggregate event
  data and project results into new topics, executing *in the same cluster* that
  stores the data.

## How it differs from kerala

Kerala uses a **single Raft group for the entire cluster** — one node is the
leader for everything, which bottlenecks writes and concentrates load.

Vela's core change: **one Raft group per partition per topic.**
- Topics are partitioned.
- Each partition is an independent Raft group with its own leader and replicated
  log.
- Leadership is therefore spread across the cluster, so write load and
  coordination scale horizontally.

## How it differs from Kafka

Vela aims to run **stream processing inside the same compute cluster as the data**,
rather than requiring separate compute (e.g. Kafka + Flink). Planned model:
- Processing input data is **node-local** — a processor reads partitions that live
  on its own node, so no cross-node data movement is needed to process.
- Processor code is itself **data on a topic**: replicated across the cluster like
  any other record. Because the code lives in the log, processing is fully
  **replayable**, and any node holding the partition can run it, enabling balanced
  processing across the cluster.
- The processing language is [ark-lang](https://github.com/djjonno/ark-lang) — an
  interpreted language run in a sandbox. Its runtime/language requirements will be
  defined later.

## Key concepts

- **Topic** — a named stream of event records, identified by a namespace.
- **Partition** — a shard of a topic; the unit of ordering, replication, and
  consensus. Each partition has its own Raft group and log.
- **Raft group** — the set of replicas for one partition that elect a leader and
  agree on log order.
- **Log** — the append-only, ordered sequence of entries for a partition. In-memory
  for now; durable persistence comes later.
- **Producer / Consumer** — clients that append to and read from partitions.
- **Processor (planned)** — replicated, sandboxed ark-lang code that consumes
  partitions and projects new streams.

## Roadmap (high level)

1. **Now** — partitioned topics, per-partition Raft consensus, in-memory log,
   produce/consume, multi-node local cluster.
2. **Next** — durable log persistence.
3. **Later** — embedded ark-lang stream-processing runtime (code-as-data,
   node-local execution, replayable).

## Status

Early-stage. Architecture and APIs are evolving — treat designs as provisional and
prefer clarifying intent when details are ambiguous.
