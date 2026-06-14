//! The tonic `VelaClient` and `VelaPeer` service implementations.
//!
//! These are the gRPC entry points that translate protobuf requests into
//! `vela-core`/`vela-raft` operations and back (Requirement 12.2, 12.3):
//!
//! - [`VelaClientService`] serves the client-facing API. `Produce` and
//!   `Consume` validate against cluster metadata and then forward to the target
//!   partition's driver task; the topic-admin RPCs delegate to `vela-core`'s
//!   validated [`ClusterMetadata`](vela_core::ClusterMetadata) operations and
//!   spawn/stop partition drivers as topics come and go.
//! - [`VelaPeerService`] serves the server-to-server API. `AppendEntries` and
//!   `RequestVote` are decoded and handed to the owning partition driver, which
//!   answers synchronously so the reply can be returned in the RPC response;
//!   `Heartbeat` and `SyncMetadata` carry the minimal membership/metadata
//!   behaviour this task needs (the full subsystems are tasks 14.3+).
//!
//! Errors cross the boundary as the shared typed [`VelaError`](vela_proto::v1::VelaError)
//! carried in a [`tonic::Status`] (Requirement 12.4), via
//! [`crate::convert::core_error_to_status`].

use std::sync::Arc;

use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use vela_core::{
    CoreError, PartitionIndex, DEFAULT_MAX_RECORDS, MAX_MAX_RECORDS, MAX_RECORD_BYTES,
    MIN_MAX_RECORDS,
};
use vela_raft::RaftMessage;

use vela_proto::v1;
use vela_proto::v1::vela_client_server::VelaClient;
use vela_proto::v1::vela_peer_server::VelaPeer;

use crate::convert;
use crate::driver::{DriverCommand, ProduceError};
use crate::node::NodeShared;

/// The client-facing gRPC service (produce, consume, topic admin).
#[derive(Clone)]
pub struct VelaClientService {
    node: Arc<NodeShared>,
}

impl VelaClientService {
    /// Create the client service over shared node state.
    pub fn new(node: Arc<NodeShared>) -> Self {
        Self { node }
    }

    /// Build a `PartitionNotFound` error for `(topic, partition)`.
    fn not_found(topic: &str, partition: u32) -> CoreError {
        CoreError::PartitionNotFound {
            topic: topic.to_string(),
            index: partition,
        }
    }
}

#[tonic::async_trait]
impl VelaClient for VelaClientService {
    async fn produce(
        &self,
        request: Request<v1::ProduceRequest>,
    ) -> Result<Response<v1::ProduceResponse>, Status> {
        let req = request.into_inner();
        let record = req
            .record
            .ok_or_else(|| Status::invalid_argument("produce request is missing a record"))?;
        let partition = PartitionIndex(req.partition);

        // Validate topic admission and partition existence against metadata,
        // capturing the believed leader for a redirect (Requirement 4.5, 4.6).
        let leader_hint = {
            let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            metadata
                .ensure_producible(&req.topic)
                .map_err(|e| convert::core_error_to_status(&e))?;
            let part = metadata
                .topics
                .get(&req.topic)
                .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
                .ok_or_else(|| {
                    convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
                })?;
            part.leader.clone()
        };

        // Reject an oversized payload before any append (Requirement 4.8).
        let record = convert::record_from_proto(record);
        let size = record.key.as_ref().map_or(0, |k| k.len()) + record.value.len();
        if size > MAX_RECORD_BYTES {
            return Err(convert::core_error_to_status(&CoreError::RecordTooLarge(
                size,
            )));
        }

        let handle = self.node.handle(&req.topic, req.partition).ok_or_else(|| {
            convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
        })?;

        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::Produce {
                value: record.value,
                reply: reply_tx,
            })
            .map_err(|_| Status::unavailable("partition driver is not running"))?;

        match reply_rx.await {
            Ok(Ok(offset)) => Ok(Response::new(v1::ProduceResponse { offset })),
            Ok(Err(ProduceError::NotLeader)) => {
                Err(convert::core_error_to_status(&CoreError::NotLeader {
                    leader: leader_hint,
                }))
            }
            Ok(Err(ProduceError::CommitTimeout)) => {
                Err(convert::core_error_to_status(&CoreError::CommitTimeout))
            }
            Err(_) => Err(Status::internal("produce result was dropped")),
        }
    }

    async fn consume(
        &self,
        request: Request<v1::ConsumeRequest>,
    ) -> Result<Response<v1::ConsumeResponse>, Status> {
        let req = request.into_inner();
        let partition = PartitionIndex(req.partition);

        // Validate the optional max count (Requirement 5.5, 5.6, 5.7).
        let max = match req.max_count {
            Some(n) if !(MIN_MAX_RECORDS..=MAX_MAX_RECORDS).contains(&n) => {
                return Err(convert::core_error_to_status(
                    &CoreError::InvalidConsumeParams,
                ));
            }
            Some(n) => n,
            None => DEFAULT_MAX_RECORDS,
        } as usize;

        // The partition must exist and have an elected leader (Requirement 5.4,
        // 5.8).
        {
            let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            let part = metadata
                .topics
                .get(&req.topic)
                .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
                .ok_or_else(|| {
                    convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
                })?;
            if part.leader.is_none() {
                return Err(convert::core_error_to_status(
                    &CoreError::PartitionUnavailable,
                ));
            }
        }

        let handle = self.node.handle(&req.topic, req.partition).ok_or_else(|| {
            convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
        })?;

        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::Consume {
                offset: req.offset,
                max,
                reply: reply_tx,
            })
            .map_err(|_| Status::unavailable("partition driver is not running"))?;

        let records = reply_rx
            .await
            .map_err(|_| Status::internal("consume result was dropped"))?;
        let next_offset = req.offset + records.len() as u64;
        let records = records
            .into_iter()
            .map(|r| v1::ConsumedRecord {
                offset: r.offset,
                record: Some(v1::Record {
                    key: None,
                    value: r.value,
                }),
            })
            .collect();

        Ok(Response::new(v1::ConsumeResponse {
            records,
            next_offset,
        }))
    }

    async fn create_topic(
        &self,
        request: Request<v1::CreateTopicRequest>,
    ) -> Result<Response<v1::CreateTopicResponse>, Status> {
        let req = request.into_inner();

        let topic = {
            let mut metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            metadata
                .create_topic(&req.name, req.partitions, self.node.replication_factor)
                .map_err(|e| convert::core_error_to_status(&e))?;
            metadata
                .topics
                .get(&req.name)
                .cloned()
                .expect("topic was just created")
        };

        // Start a driver for every partition this node replicates.
        for partition in &topic.partitions {
            self.node.spawn_partition(&req.name, partition);
        }

        Ok(Response::new(v1::CreateTopicResponse {
            topic: Some(convert::topic_to_proto(&topic)),
        }))
    }

    async fn delete_topic(
        &self,
        request: Request<v1::DeleteTopicRequest>,
    ) -> Result<Response<v1::DeleteTopicResponse>, Status> {
        let req = request.into_inner();

        // Capture the partitions, then atomically remove the topic; only on a
        // successful removal do we stop the local drivers (Requirement 3.1,
        // 3.2, 3.3).
        let partitions = {
            let mut metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            let partitions: Vec<u32> = metadata
                .topics
                .get(&req.name)
                .map(|t| t.partitions.iter().map(|p| p.index.0).collect())
                .unwrap_or_default();
            metadata
                .delete_topic(&req.name)
                .map_err(|e| convert::core_error_to_status(&e))?;
            partitions
        };

        for partition in partitions {
            self.node.stop_partition(&req.name, partition);
        }

        Ok(Response::new(v1::DeleteTopicResponse {}))
    }

    async fn list_topics(
        &self,
        _request: Request<v1::ListTopicsRequest>,
    ) -> Result<Response<v1::ListTopicsResponse>, Status> {
        let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
        let topics = metadata
            .topics
            .values()
            .map(convert::topic_to_proto)
            .collect();
        Ok(Response::new(v1::ListTopicsResponse { topics }))
    }

    async fn describe_topic(
        &self,
        request: Request<v1::DescribeTopicRequest>,
    ) -> Result<Response<v1::DescribeTopicResponse>, Status> {
        let req = request.into_inner();
        let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
        let topic = metadata.topics.get(&req.name).ok_or_else(|| {
            convert::core_error_to_status(&CoreError::TopicNotFound(req.name.clone()))
        })?;
        Ok(Response::new(v1::DescribeTopicResponse {
            topic: Some(convert::topic_to_proto(topic)),
        }))
    }

    async fn find_leader(
        &self,
        request: Request<v1::FindLeaderRequest>,
    ) -> Result<Response<v1::FindLeaderResponse>, Status> {
        let req = request.into_inner();
        let partition = PartitionIndex(req.partition);
        let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
        let part = metadata
            .topics
            .get(&req.topic)
            .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
            .ok_or_else(|| {
                convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
            })?;
        // The wire response models an absent leader as `None`; clients retry
        // until a leader is known (Requirement 10.4, 11.1).
        Ok(Response::new(v1::FindLeaderResponse {
            leader: part.leader.as_ref().map(|n| n.0.clone()),
        }))
    }
}

/// The server-to-server gRPC service (Raft RPCs, membership, metadata).
#[derive(Clone)]
pub struct VelaPeerService {
    node: Arc<NodeShared>,
}

impl VelaPeerService {
    /// Create the peer service over shared node state.
    pub fn new(node: Arc<NodeShared>) -> Self {
        Self { node }
    }

    /// Hand an inbound Raft `message` for `(topic, partition)` to the owning
    /// driver and await the synchronous reply it returns.
    async fn dispatch_rpc(
        &self,
        topic: &str,
        partition: u32,
        message: RaftMessage,
    ) -> Result<RaftMessage, Status> {
        let handle = self
            .node
            .handle(topic, partition)
            .ok_or_else(|| Status::not_found("partition is not hosted on this node"))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::PeerRpc {
                msg: message,
                reply: reply_tx,
            })
            .map_err(|_| Status::unavailable("partition driver is not running"))?;

        reply_rx
            .await
            .map_err(|_| Status::internal("raft reply was dropped"))?
            .ok_or_else(|| Status::internal("raft produced no reply"))
    }
}

#[tonic::async_trait]
impl VelaPeer for VelaPeerService {
    async fn append_entries(
        &self,
        request: Request<v1::AppendEntriesRequest>,
    ) -> Result<Response<v1::AppendEntriesReply>, Status> {
        let req = request.into_inner();
        let message = RaftMessage::AppendEntries(convert::append_entries_from_proto(&req));
        match self
            .dispatch_rpc(&req.topic, req.partition, message)
            .await?
        {
            RaftMessage::AppendEntriesReply(reply) => Ok(Response::new(
                convert::append_entries_reply_to_proto(&reply),
            )),
            _ => Err(Status::internal("unexpected reply to AppendEntries")),
        }
    }

    async fn request_vote(
        &self,
        request: Request<v1::RequestVoteRequest>,
    ) -> Result<Response<v1::RequestVoteReply>, Status> {
        let req = request.into_inner();
        let message = RaftMessage::RequestVote(convert::request_vote_from_proto(&req));
        match self
            .dispatch_rpc(&req.topic, req.partition, message)
            .await?
        {
            RaftMessage::RequestVoteReply(reply) => {
                Ok(Response::new(convert::request_vote_reply_to_proto(&reply)))
            }
            _ => Err(Status::internal("unexpected reply to RequestVote")),
        }
    }

    async fn heartbeat(
        &self,
        _request: Request<v1::HeartbeatRequest>,
    ) -> Result<Response<v1::HeartbeatReply>, Status> {
        // Membership liveness tracking is task 14.3; for now a node simply
        // identifies itself so a caller's heartbeat succeeds (Requirement 9.4,
        // 9.5).
        Ok(Response::new(v1::HeartbeatReply {
            node_id: self.node.self_id.clone(),
        }))
    }

    async fn sync_metadata(
        &self,
        request: Request<v1::SyncMetadataRequest>,
    ) -> Result<Response<v1::SyncMetadataReply>, Status> {
        let req = request.into_inner();
        let incoming = req
            .metadata
            .ok_or_else(|| Status::invalid_argument("sync request is missing metadata"))?;
        let incoming = convert::cluster_metadata_from_proto(&incoming);

        // Adopt the incoming view if it is at least as fresh as ours, and
        // acknowledge the applied epoch (Requirement 2.8, 3.5). Full ack
        // tracking and the propagation deadline are task 14.3+.
        let epoch = {
            let mut metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            if incoming.epoch >= metadata.epoch {
                *metadata = incoming;
            }
            metadata.epoch
        };

        Ok(Response::new(v1::SyncMetadataReply { epoch }))
    }
}
