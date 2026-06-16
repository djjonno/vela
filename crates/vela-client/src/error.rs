//! The client library's typed error.
//!
//! [`ClientError`] is the single error surfaced to callers of [`Producer`],
//! [`Consumer`], and [`AdminClient`]. It distinguishes transport/RPC failures
//! from routing failures (no known leader, unknown node, no bootstrap nodes) so
//! the leader-redirection retry logic (task 16.2) can branch on them.
//!
//! [`Producer`]: crate::Producer
//! [`Consumer`]: crate::Consumer
//! [`AdminClient`]: crate::AdminClient

/// Errors returned by the Vela client library.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// No bootstrap nodes were configured, so the client has nowhere to send an
    /// initial request (e.g. `FindLeader` or a topic-admin call).
    #[error("no bootstrap nodes configured")]
    NoNodes,

    /// A node address could not be parsed into a valid endpoint URI.
    #[error("invalid node address `{addr}`: {source}")]
    InvalidAddress {
        /// The offending address.
        addr: String,
        /// The underlying transport parse error.
        source: tonic::transport::Error,
    },

    /// The cluster reports no elected leader for the partition (an election is
    /// in progress, or the partition is unavailable).
    #[error("no leader currently elected for {topic}/{partition}")]
    NoLeader {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: u32,
    },

    /// `FindLeader` named a leader node id the client cannot map to an address.
    /// The node is not in the client's registry of known nodes.
    #[error("leader node `{node}` for {topic}/{partition} has no known address")]
    UnknownNode {
        /// The unresolvable leader node id.
        node: String,
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: u32,
    },

    /// The client followed `NotLeader` redirections for the partition but could
    /// not land on a usable leader before exhausting its retry budget
    /// (`MAX_RETRIES`). The caller is told no leader was found rather than being
    /// retried indefinitely (Requirement 11.4).
    #[error("no leader found for {topic}/{partition} after exhausting redirection retries")]
    NoLeaderAfterRetries {
        /// Topic name.
        topic: String,
        /// Partition index.
        partition: u32,
    },

    /// A gRPC call failed with a status (transport-level or application error).
    /// Boxed because `tonic::Status` is large and would otherwise bloat every
    /// `Result` returned by the client.
    #[error("rpc failed: {0}")]
    Rpc(Box<tonic::Status>),

    /// The server returned a response missing a field the client requires
    /// (e.g. a successful `DescribeTopic` with no topic payload).
    #[error("malformed response: {0}")]
    MalformedResponse(String),

    /// A caller supplied a log-backend value the client does not recognize. The
    /// client accepts exactly `durable` and `in-memory` and rejects anything
    /// else *before* sending a `CreateTopic` request
    /// (per-topic-log-durability Requirement 1.3).
    #[error("invalid log backend `{value}` (expected `durable` or `in-memory`)")]
    InvalidBackend {
        /// The unrecognized backend value supplied by the caller.
        value: String,
    },
}

impl From<tonic::Status> for ClientError {
    fn from(status: tonic::Status) -> Self {
        ClientError::Rpc(Box::new(status))
    }
}

/// Convenience result alias for client operations.
pub type Result<T> = std::result::Result<T, ClientError>;
