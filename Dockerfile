# syntax=docker/dockerfile:1

# ---- Builder stage -------------------------------------------------------
# Pinned to a recent stable Rust compatible with the workspace MSRV (1.82).
# vela-proto's build.rs uses a vendored protoc (protoc-bin-vendored), so no
# system protoc package is required here.
FROM rust:1.83-bookworm AS builder

WORKDIR /build

# Copy the whole workspace (root Cargo.toml + Cargo.lock + crates/) and build
# only the velad binary in release mode.
COPY . .

RUN cargo build --release -p vela-server --bin velad

# ---- Runtime stage -------------------------------------------------------
# Slim Debian runtime that ships only the compiled binary. bookworm-slim
# matches the builder's glibc, so the dynamically linked binary runs as-is.
FROM debian:bookworm-slim AS runtime

# Run as an unprivileged user rather than root.
RUN useradd --system --create-home --uid 10001 vela
USER vela
WORKDIR /home/vela

COPY --from=builder /build/target/release/velad /usr/local/bin/velad

# Configuration is supplied entirely through environment variables, which the
# velad binary reads via clap (see crates/vela-server/src/config.rs).
# Defaults wire a single node listening on all interfaces; docker-compose
# overrides these per node to form a cluster.
ENV VELA_NODE_ID="" \
    VELA_LISTEN_ADDR="0.0.0.0:7001" \
    VELA_PEERS="" \
    VELA_REPLICATION_FACTOR="3"

# Default gRPC port served by the node daemon (VelaClient + VelaPeer).
EXPOSE 7001

ENTRYPOINT ["velad"]
