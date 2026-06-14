# Project Structure

## Current Layout

```
vela/
├── .kiro/steering/   # AI assistant steering docs
├── README.md         # Project overview
├── LICENSE           # Apache License 2.0
└── .gitignore        # Rust/Cargo ignores
```

Application source has not been added yet. The layout below is the intended target.

## Target Layout (Cargo workspace)

Vela is a **multi-crate Cargo workspace**. Separating consensus, log, server, and
client into their own crates keeps boundaries clear and lets the core logic be
tested in isolation. This mirrors kerala's `core` / `ctl` module split, expanded for
Rust conventions.

```
vela/
├── Cargo.toml              # workspace root: [workspace] members
├── Cargo.lock              # committed (this workspace ships binaries)
├── crates/
│   ├── vela-log/           # append-only log; in-memory now, storage trait for later
│   ├── vela-raft/          # in-house Raft: states, election, replication, RPC traits
│   ├── vela-proto/         # protobuf definitions + generated tonic/prost types
│   ├── vela-core/          # domain model: topics, partitions, partition routing,
│   │                       #   cluster membership, the set of per-partition raft groups
│   ├── vela-server/        # node daemon library: wires raft groups + gRPC services;
│   │                       #   produces the `velad` binary
│   ├── vela-client/        # client library: producer, consumer, admin
│   └── vela-ctl/           # CLI control tool (binary `vela-ctl`)
├── Dockerfile              # node daemon image
└── docker-compose.yml      # local multi-node cluster
```

Crate names and exact boundaries are a recommendation, not a mandate — adjust if a
cleaner split emerges, but keep consensus, log, and transport decoupled.

## Dependency Direction

Dependencies point inward. Lower layers must not depend on higher ones.

```
vela-ctl ─┐
          ├─> vela-client ─> vela-proto
vela-server ─> vela-core ─> vela-raft ─> vela-log
          └─> vela-proto
```

- `vela-log` and `vela-raft` know nothing about topics, gRPC, or the server.
- `vela-raft` depends on `vela-log` for the replicated log, and abstracts transport
  and timers behind traits so it stays testable.
- `vela-core` composes raft groups per partition and owns the topic/partition model.
- `vela-server` is the only crate that wires networking (tonic) to the core.

## Mapping from kerala (reference)

When porting, these kerala packages map roughly to vela crates:

| kerala (Kotlin)                         | vela crate     |
|-----------------------------------------|----------------|
| `core/.../consensus` (Raft, states)     | `vela-raft`    |
| `core/.../log`, `log/ds` (InMemoryLog)  | `vela-log`     |
| `core/.../runtime/topic` (Topic, registry) | `vela-core` |
| `core/.../server` (gRPC, cluster)       | `vela-server`  |
| `core/.../runtime/client` (producer/consumer) | `vela-client` |
| `ctl` module                            | `vela-ctl`     |
| `lib` (protobuf)                        | `vela-proto`   |

Key difference: kerala instantiates **one** Raft for the cluster; vela instantiates
**one Raft group per partition**, managed within `vela-core`.

## Organization Guidelines

- Group related functionality into focused modules; avoid large monolithic files.
- Put unit tests in a `#[cfg(test)] mod tests` block alongside the code they test.
- Put cross-cutting/integration tests in each crate's top-level `tests/` directory.
- Keep distribution/clustering concerns (replication, transport) separate from
  per-partition log and state-machine logic so partitions stay testable in isolation.
- Keep the storage layer behind a trait in `vela-log` so persistence can be added
  without changing `vela-raft`.

## Naming

- Files and modules: `snake_case`.
- Types and traits: `PascalCase`.
- Functions, variables, fields: `snake_case`.
- Constants: `SCREAMING_SNAKE_CASE`.
- Crates: `vela-*` (kebab-case on disk, `vela_*` when referenced in code).
