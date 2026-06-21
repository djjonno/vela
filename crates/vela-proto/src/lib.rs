//! `vela-proto` — protobuf wire definitions and generated gRPC types.
//!
//! Owns every wire message type and (from task 2.2 onward) the `VelaClient` /
//! `VelaPeer` gRPC service surfaces. Declares no dependency on any other Vela
//! crate so that types can flow outward to clients and the server without
//! creating dependency cycles.
//!
//! All generated types live under the [`v1`] module, mirroring the `vela.v1`
//! protobuf package.

/// Generated protobuf message types for the `vela.v1` package.
///
/// Includes records, log entries and payloads, the Raft RPCs
/// (`AppendEntries`, `RequestVote`, and their replies), produce/consume and
/// topic-admin messages, cluster metadata and propagation messages, and the
/// shared [`v1::VelaError`] typed error.
pub mod v1 {
    tonic::include_proto!("vela.v1");
}

/// The `Canonical_Partitioner`: the single shared key→partition mapping used by
/// `vela-ctl`, the `vela-client` `Producer`, `vela-core`, and any internal
/// repartition / `key_by` stage (Requirement 5.5). Lives here because
/// `vela-proto` is the one crate both `vela-client` and `vela-core` depend on.
pub mod partition;
