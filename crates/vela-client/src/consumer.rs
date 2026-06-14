//! The consume client.
//!
//! [`Consumer::consume`] reads committed records from an explicit
//! `(topic, partition)`, directing the request to that partition's believed
//! leader (Requirement 11.1). Unlike produce, consume targets a caller-chosen
//! partition, so there is no routing step — only leader resolution.

use std::sync::Arc;

use vela_proto::v1::{ConsumeRequest, ConsumedRecord};

use crate::core::ClientCore;
use crate::error::Result;

/// Reads committed records from topic partitions via their leaders.
#[derive(Debug, Clone)]
pub struct Consumer {
    core: Arc<ClientCore>,
}

impl Consumer {
    /// Create a consumer over a shared client core.
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }

    /// Consume up to `max` committed records from `(topic, partition)` starting
    /// at `offset`.
    ///
    /// The request is sent to the partition's believed leader, retrying through
    /// [`ClientCore::dispatch`] if the leader redirects us with `NotLeader`
    /// (Requirement 11.1–11.4). `max` is `None` to accept the server default
    /// (500), or a bound in `1..=10000` (Requirement 5.5, 5.6). Returns the
    /// committed records in ascending offset order together with the next offset
    /// to request.
    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max: Option<u32>,
    ) -> Result<ConsumeOutcome> {
        // Captured by the per-attempt closure; cloned on each attempt so a
        // redirect can re-send the identical request to the new leader.
        let core = Arc::clone(&self.core);
        let topic_owned = topic.to_string();

        self.core
            .dispatch(topic, partition, move |addr| {
                let core = Arc::clone(&core);
                let topic = topic_owned.clone();
                async move {
                    let mut client = core.client_for(&addr)?;
                    let response = client
                        .consume(ConsumeRequest {
                            topic,
                            partition,
                            offset,
                            max_count: max,
                        })
                        .await?
                        .into_inner();
                    Ok(ConsumeOutcome {
                        records: response.records,
                        next_offset: response.next_offset,
                    })
                }
            })
            .await
    }
}

/// The result of a [`Consumer::consume`] call: the committed records returned
/// and the next offset the consumer should request to continue.
#[derive(Debug, Clone)]
pub struct ConsumeOutcome {
    /// Committed records in ascending offset order.
    pub records: Vec<ConsumedRecord>,
    /// The offset to request next to continue reading.
    pub next_offset: u64,
}
