# Vela

**Vela** is a distributed event-streaming and stream-processing platform written in
Rust. It stores ordered event streams across a cluster and (soon) runs stream
processors *inside that same cluster* — no separate compute tier required.

Vela is a ground-up Rust re-implementation and evolution of
[kerala](https://github.com/djjonno/kerala).

- **Produce / Consume** — create topics, then produce and consume event records.
- **Distribute** — topics are partitioned, and partitions are balanced across the
  cluster.
- **Process** *(planned)* — run sandboxed processors that transform or aggregate
  events and project the results into new topics.

## Why Vela

**Distributed consensus, not a single leader.** Kerala runs one Raft group for the
whole cluster, so a single node is the leader for everything. Vela runs **one Raft
group per topic partition**, spreading leadership and write load across every node.

**Processing where the data lives.** Unlike Kafka + Flink, where stream processing
runs on separate compute, Vela aims to process data on the same nodes that store it:

- Processor input is **node-local** — a node processes the partitions it already
  holds, avoiding cross-node data movement.
- Processor code is **replicated data on a topic**, so processing is fully
  **replayable** and any node holding a partition can run it.
- The processing language is [ark-lang](https://github.com/djjonno/ark-lang), an
  interpreted language executed in a sandbox.

## Architecture

| Concept       | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| **Topic**     | A named stream of event records.                                            |
| **Partition** | A shard of a topic — the unit of ordering, replication, and consensus.      |
| **Raft group**| The replicas of one partition; elect a leader and agree on log order.       |
| **Log**       | The append-only ordered entries for a partition (in-memory for now).        |
| **Node**      | A cluster member hosting partition replicas and (later) processors.         |

Consensus uses an **in-house Raft implementation**. Persistence is **in-memory for
now** — durable storage is a planned follow-up.

## Project Layout

Vela is a Cargo workspace of focused crates:

```
crates/
├── vela-log/      # append-only log (in-memory now; storage trait for persistence later)
├── vela-raft/     # in-house Raft: states, elections, replication
├── vela-proto/    # protobuf definitions + generated gRPC types
├── vela-core/     # topics, partitions, routing, per-partition raft groups
├── vela-server/   # node daemon (`velad`): wires raft groups to gRPC services
├── vela-client/   # client library: producer, consumer, admin
└── vela-ctl/      # CLI control tool
```

> Status: early-stage. Source is being built out; the layout above is the target.

## Getting Started

### Prerequisites

- A recent stable **Rust** toolchain (via [rustup](https://rustup.rs)).
- **Docker** + **Docker Compose** (for the local cluster).

### Build

```bash
cargo build
```

### Run a single node

```bash
cargo run -p vela-server
```

### Run a local cluster

Bring up a multi-node cluster with Docker Compose:

```bash
docker compose up
```

## Development

```bash
cargo test                   # run tests
cargo fmt                    # format
cargo clippy -- -D warnings  # lint
cargo mutants                # mutation testing
```

## Roadmap

1. **Now** — partitioned topics, per-partition Raft consensus, in-memory log,
   produce/consume, local multi-node cluster.
2. **Next** — durable log persistence.
3. **Later** — embedded ark-lang stream-processing runtime (code-as-data,
   node-local execution, replayable).

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
