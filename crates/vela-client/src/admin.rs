//! The topic-administration client.
//!
//! [`AdminClient`] exposes the whole-topic operations of the `VelaClient`
//! service: create, delete, list, and describe topics (Requirement 13.1–13.4).
//! These are not partition-scoped, so they are sent to a bootstrap node, which
//! serves or forwards them to the metadata group as needed.

use std::sync::Arc;

use vela_proto::v1::{
    CreateTopicRequest, DeleteTopicRequest, DescribeTopicRequest, ListTopicsRequest, TopicInfo,
};

use crate::core::ClientCore;
use crate::error::{ClientError, Result};

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

    /// Create a topic `name` with `partitions` partitions (Requirement 13.1).
    /// Returns the created topic's metadata.
    pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<TopicInfo> {
        let mut client = self.core.bootstrap_client()?;
        let response = client
            .create_topic(CreateTopicRequest {
                name: name.to_string(),
                partitions,
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

    /// Describe a single topic's partitions and current leaders
    /// (Requirement 13.4).
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
