//! The topic-administration client.
//!
//! [`AdminClient`] exposes the whole-topic operations of the `VelaClient`
//! service: create, delete, list, and describe topics (Requirement 13.1–13.4).
//! These are not partition-scoped, so they are sent to a bootstrap node, which
//! serves or forwards them to the metadata group as needed.
//!
//! # Selecting a log backend (per-topic-log-durability Requirement 1)
//!
//! A topic declares its log storage backend at creation time. The client
//! exposes the choice as the typed [`LogBackend`] enum — exactly two values,
//! [`LogBackend::Durable`] (the default) and [`LogBackend::InMemory`] — so an
//! out-of-range value is unrepresentable and rejected by the type system before
//! any request is sent (Requirement 1.3). [`AdminClient::create_topic`] maps the
//! chosen backend onto the `CreateTopicRequest` wire field (Requirement 1.1,
//! 1.2), and [`AdminClient::describe_topic`] reports the backend a topic was
//! created with (Requirement 1.4); [`LogBackend::from_wire`] decodes the wire
//! value into the client enum.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use vela_proto::v1::{
    CreateTopicRequest, DeleteTopicRequest, DescribeTopicRequest, ListTopicsRequest,
    LogBackend as WireLogBackend, TopicInfo,
};

use crate::core::ClientCore;
use crate::error::{ClientError, Result};

/// A topic's log storage backend, as selected through the client API.
///
/// Exactly two values are accepted (Requirement 1.3); [`Durable`](Self::Durable)
/// is the default so a caller that does not choose gets durability
/// (Requirement 1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogBackend {
    /// Persist the partition log to durable storage (the default).
    #[default]
    Durable,
    /// Keep the partition log in volatile memory only.
    InMemory,
}

impl LogBackend {
    /// The proto wire value (`CreateTopicRequest.log_backend`) for this backend.
    ///
    /// Always one of the two concrete `LogBackend` wire values — never the
    /// `UNSPECIFIED` sentinel — so the server records exactly the chosen backend
    /// (Requirement 1.1).
    pub fn to_wire(self) -> i32 {
        match self {
            LogBackend::Durable => WireLogBackend::Durable as i32,
            LogBackend::InMemory => WireLogBackend::InMemory as i32,
        }
    }

    /// Decode a proto wire value into the client backend, or `None` if the value
    /// is the unspecified sentinel or an unrecognized integer.
    ///
    /// Used to surface a described topic's backend in a usable form
    /// (Requirement 1.4).
    pub fn from_wire(value: i32) -> Option<Self> {
        match WireLogBackend::try_from(value) {
            Ok(WireLogBackend::Durable) => Some(LogBackend::Durable),
            Ok(WireLogBackend::InMemory) => Some(LogBackend::InMemory),
            Ok(WireLogBackend::Unspecified) | Err(_) => None,
        }
    }

    /// This backend's canonical lower-case name (`durable` / `in-memory`).
    pub fn as_str(self) -> &'static str {
        match self {
            LogBackend::Durable => "durable",
            LogBackend::InMemory => "in-memory",
        }
    }
}

impl fmt::Display for LogBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LogBackend {
    type Err = ClientError;

    /// Parse `durable` or `in-memory`, rejecting any other value as
    /// [`ClientError::InvalidBackend`] *before* a request is sent
    /// (Requirement 1.3).
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "durable" => Ok(LogBackend::Durable),
            "in-memory" => Ok(LogBackend::InMemory),
            other => Err(ClientError::InvalidBackend {
                value: other.to_string(),
            }),
        }
    }
}

/// Creates, deletes, lists, and describes topics.
#[derive(Debug, Clone)]
pub struct AdminClient {
    core: Arc<ClientCore>,
}

impl AdminClient {
    /// Create an admin client over a shared client core.
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }

    /// Create a topic `name` with `partitions` partitions and the given log
    /// `backend` (Requirement 13.1; per-topic-log-durability Requirement 1.1).
    ///
    /// The `backend` is a typed [`LogBackend`], so only the two valid values are
    /// representable (Requirement 1.3); pass [`LogBackend::Durable`] (its
    /// `Default`) for the durable-by-default behavior (Requirement 1.2). Returns
    /// the created topic's metadata.
    pub async fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        backend: LogBackend,
    ) -> Result<TopicInfo> {
        let mut client = self.core.bootstrap_client()?;
        let response = client
            .create_topic(CreateTopicRequest {
                name: name.to_string(),
                partitions,
                log_backend: backend.to_wire(),
            })
            .await?
            .into_inner();
        response
            .topic
            .ok_or_else(|| ClientError::MalformedResponse(format!("CreateTopic({name})")))
    }

    /// Delete the topic `name` (Requirement 13.2).
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let mut client = self.core.bootstrap_client()?;
        client
            .delete_topic(DeleteTopicRequest {
                name: name.to_string(),
            })
            .await?;
        Ok(())
    }

    /// List all topics known to cluster metadata, with their partition counts
    /// (Requirement 13.3).
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let mut client = self.core.bootstrap_client()?;
        let response = client.list_topics(ListTopicsRequest {}).await?.into_inner();
        Ok(response.topics)
    }

    /// Describe a single topic's partitions, current leaders, and log backend
    /// (Requirement 13.4; per-topic-log-durability Requirement 1.4).
    ///
    /// The returned [`TopicInfo`] carries the topic's backend on its
    /// `log_backend` field; decode it with [`LogBackend::from_wire`].
    pub async fn describe_topic(&self, name: &str) -> Result<TopicInfo> {
        let mut client = self.core.bootstrap_client()?;
        let response = client
            .describe_topic(DescribeTopicRequest {
                name: name.to_string(),
            })
            .await?
            .into_inner();
        response
            .topic
            .ok_or_else(|| ClientError::MalformedResponse(format!("DescribeTopic({name})")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_backend_is_durable() {
        // A caller that does not choose a backend gets durability by default
        // (Requirement 1.2).
        assert_eq!(LogBackend::default(), LogBackend::Durable);
    }

    #[test]
    fn backend_maps_to_the_concrete_wire_value() {
        // The chosen backend always maps to a concrete wire value, never the
        // unspecified sentinel (Requirement 1.1).
        assert_eq!(
            LogBackend::Durable.to_wire(),
            WireLogBackend::Durable as i32
        );
        assert_eq!(
            LogBackend::InMemory.to_wire(),
            WireLogBackend::InMemory as i32
        );
        assert_ne!(
            LogBackend::Durable.to_wire(),
            WireLogBackend::Unspecified as i32
        );
    }

    #[test]
    fn from_wire_decodes_known_backends_only() {
        // The two concrete backends decode; the unspecified sentinel and any
        // unrecognized integer do not (Requirement 1.4).
        assert_eq!(
            LogBackend::from_wire(WireLogBackend::Durable as i32),
            Some(LogBackend::Durable)
        );
        assert_eq!(
            LogBackend::from_wire(WireLogBackend::InMemory as i32),
            Some(LogBackend::InMemory)
        );
        assert_eq!(
            LogBackend::from_wire(WireLogBackend::Unspecified as i32),
            None
        );
        assert_eq!(LogBackend::from_wire(99), None);
    }

    #[test]
    fn parsing_accepts_exactly_the_two_backends() {
        assert_eq!(
            "durable".parse::<LogBackend>().unwrap(),
            LogBackend::Durable
        );
        assert_eq!(
            "in-memory".parse::<LogBackend>().unwrap(),
            LogBackend::InMemory
        );
    }

    #[test]
    fn parsing_rejects_an_invalid_backend_before_sending() {
        // An unrecognized value is rejected at parse time — before any
        // `CreateTopic` request is built or sent (Requirement 1.3).
        let err = "bogus".parse::<LogBackend>().unwrap_err();
        assert!(
            matches!(err, ClientError::InvalidBackend { ref value } if value == "bogus"),
            "expected InvalidBackend, got {err:?}"
        );
    }

    #[test]
    fn display_round_trips_through_parse() {
        for backend in [LogBackend::Durable, LogBackend::InMemory] {
            assert_eq!(backend.to_string().parse::<LogBackend>().unwrap(), backend);
        }
    }
}
