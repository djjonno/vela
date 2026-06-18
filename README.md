# Vela

**Vela** is a distributed event-streaming and stream-processing platform written in
Rust. It stores ordered event streams across a cluster and (soon) runs stream
processors *inside that same cluster* — no separate compute tier required.

- **Produce / Consume** — create topics, then produce and consume event records.
- **Distribute** — topics are partitioned, and partitions are balanced across the
  cluster.
- **Process** *(planned)* — run sandboxed processors that transform or aggregate
  events and project the results into new topics.

## Vision

Vela aims to be a better Kafka: the throughput and durability you expect from a
modern event log, without the operational weight and the separate processing tier.

**No single write bottleneck.** Many systems concentrate coordination on one leader.
Vela runs **one Raft group per topic partition**, so leadership and write load spread
across every node in the cluster and scale horizontally as you add partitions.

**Processing where the data lives.** Today, stream processing typically runs on a
separate compute tier (think Kafka + Flink), shuttling data across the network to be
processed. Vela aims to process data on the same nodes that store it:

- Processor input is **node-local** — a node processes the partitions it already
  holds, avoiding cross-node data movement.
- Processor code is **replicated data on a topic**, so processing is fully
  **replayable** and any node holding a partition can run it.
- Processors run in a **sandbox**, isolated from the node and from each other.

**One platform, fewer moving parts.** Storage, consensus, and (soon) processing live
in a single cluster you can run locally with one command, instead of stitching
together a broker, a coordinator, and a stream-processing framework.

## Architecture

| Concept       | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| **Topic**     | A named stream of event records.                                            |
| **Partition** | A shard of a topic — the unit of ordering, replication, and consensus.      |
| **Raft group**| The replicas of one partition; elect a leader and agree on log order.       |
| **Log**       | The append-only ordered entries for a partition, persisted durably to disk. |
| **Node**      | A cluster member hosting partition replicas and (later) processors.         |

Consensus uses an **in-house Raft implementation**, run **per partition** rather than
once per cluster. Each partition's log is **persisted durably** to a write-ahead log
on disk, so committed records and Raft state survive a node restart; an in-memory
backend remains available behind the same storage trait.

## Project Layout

Vela is a Cargo workspace of focused crates:

```
crates/
├── vela-log/      # append-only log (durable write-ahead log; in-memory backend behind the same storage trait)
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

The daemon is configured via flags or environment variables, including a
`--data-dir` (`VELA_DATA_DIR`) where each durable partition log stores its
segments:

```bash
cargo run -p vela-server -- \
  --node-id node-a \
  --listen-addr 127.0.0.1:7001 \
  --replication-factor 1 \
  --data-dir ./data
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

1. **Now** — partitioned topics, per-partition Raft consensus, durable log
   persistence, produce/consume, local multi-node cluster.
2. **Next** — embedded ark-lang stream-processing runtime (code-as-data,
   node-local execution, replayable).

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
