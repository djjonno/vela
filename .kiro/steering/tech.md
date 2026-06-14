# Tech Stack

## Language & Toolchain

- **Rust** — primary implementation language.
- **Cargo workspace** — build, dependency management, and test runner across
  multiple member crates.
- **rustfmt** — code formatting (the standard Rust formatter).
- **Clippy** — linting; code should be clippy-clean.
- **cargo-mutants** — mutation testing; write tests that meaningfully assert
  behavior so mutants are caught.

## Core Libraries (intended)

These mirror kerala's choices (gRPC + Raft) adapted to the Rust ecosystem. They are
the default direction; confirm before pinning if a better fit emerges.

- **tokio** — async runtime (networking, timers, tasks). Raft election/heartbeat
  timers and replication run on async tasks.
- **tonic** + **prost** — gRPC for server-to-server (replication, voting) and
  client-to-server (produce/consume/admin) communication; protobuf message types.
- **serde** — (de)serialization for config and non-wire data where needed.
- **clap** — CLI argument parsing for the server daemon and `vela-ctl`.
- **tracing** — structured logging and diagnostics.
- **thiserror** — typed library errors; reserve `anyhow` for binary/entry-point code.
- **proptest** — property-based tests for consensus/log invariants.

## Consensus & Log Notes

- Raft is implemented **in-house** (not a third-party crate) — this is a deliberate
  goal of the project, porting and evolving kerala's implementation.
- Consensus is **per partition**: many small Raft groups run concurrently on each
  node, not one cluster-wide group. Design the Raft module to be instantiated many
  times and driven independently.
- The log is **append-only** and **in-memory** for now (`append` / `read` /
  `commit` / `revert` / snapshot semantics). Keep the storage layer behind a trait
  so durable persistence can be added later without touching consensus.

## Conventions

- Format all code with `rustfmt` before committing; do not hand-format.
- Follow standard Rust idioms: `Result`/`Option` for error and absence handling,
  the `?` operator for propagation.
- Keep crate boundaries clean — depend inward (server → core → raft/log), never the
  reverse.
- Keep `unsafe` out of the codebase unless there is a documented, justified reason.
- Prefer traits at crate seams (log storage, transport, clock/timer) to keep
  consensus logic unit-testable and deterministic in tests.

## Common Commands

```sh
# Build
cargo build                  # debug build, whole workspace
cargo build --release        # optimized build
cargo build -p vela-raft     # build a single crate

# Run a node locally
cargo run -p vela-server -- --help

# Test
cargo test                   # run the workspace test suite
cargo test -p vela-raft      # test a single crate
cargo mutants                # run mutation testing

# Quality
cargo fmt                    # format the codebase
cargo fmt --check            # verify formatting (CI-friendly)
cargo clippy --all-targets   # lint
cargo clippy -- -D warnings  # lint, failing on warnings
```

## Local Multi-Node Cluster

A primary goal is running several nodes as a cluster locally with minimal friction.
Target workflow:

```sh
docker compose up            # bring up a multi-node vela cluster
```

Provide a `Dockerfile` for the node daemon and a `docker-compose.yml` that launches
several nodes wired into one cluster (kerala did this with a 4-node compose file).

## Notes

- `Cargo.toml` (workspace) and member crates are not yet committed. When adding
  them, pin a sensible MSRV and prefer well-maintained crates with explicit versions.
- Build artifacts (`target/`, `debug/`) and mutation output (`mutants.out*/`) are
  git-ignored — never commit them.
