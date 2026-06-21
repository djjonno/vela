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

/// The maximum gRPC message size, in bytes, the client and server accept on the
/// wire (64 MiB).
///
/// tonic defaults the receive (decode) limit to 4 MiB, which a real consume or
/// replication payload routinely exceeds: a single `Consume` may return up to
/// the default 500-record batch, and an `AppendEntries` batch carries a slice of
/// the partition log, so either can run to tens of MiB even though each record
/// is capped at 1 MiB. Both the `VelaClient`/`VelaPeer` stubs and their server
/// services raise their decode/encode limits to this value so produce/consume
/// and replication work against a loaded cluster.
///
/// This is a transport guard against a single oversized frame, not the consume
/// contract's true upper bound: a caller may request up to 10,000 records, so a
/// pathological batch of max-size records could still exceed this. Bounding a
/// consume response by total bytes (not just record count) is the proper fix and
/// is left as follow-up; 64 MiB comfortably covers realistic batches in the
/// meantime.
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// The `Canonical_Partitioner`: the single shared key→partition mapping used by
/// `vela-ctl`, the `vela-client` `Producer`, `vela-core`, and any internal
/// repartition / `key_by` stage (Requirement 5.5). Lives here because
/// `vela-proto` is the one crate both `vela-client` and `vela-core` depend on.
pub mod partition;
