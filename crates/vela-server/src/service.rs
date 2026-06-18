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
//!   `Heartbeat` carries the minimal membership behaviour this task needs, and
//!   `SyncMetadata` is a reserved no-op off the commit path (metadata reaches
//!   every node via `AppendEntries`, not a snapshot push — Requirement 1.3).
//!
//! Errors cross the boundary as the shared typed [`VelaError`](vela_proto::v1::VelaError)
//! carried in a [`tonic::Status`] (Requirement 12.4), via
//! [`crate::convert::core_error_to_status`].

use std::sync::Arc;

use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use vela_core::{
    CoreError, NodeId, PartitionIndex, DEFAULT_MAX_RECORDS, MAX_MAX_RECORDS, MAX_RECORD_BYTES,
    MIN_MAX_RECORDS,
};
use vela_raft::RaftMessage;

use vela_proto::v1;
use vela_proto::v1::vela_client_server::VelaClient;
use vela_proto::v1::vela_peer_server::VelaPeer;

use crate::convert;
use crate::driver::{DriverCommand, DriverHandle, ProduceError};
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

    /// Query the **live Raft-elected leader** currently known to a partition
    /// driver, as a domain [`NodeId`] (Requirement 8.1).
    ///
    /// This is the authoritative source for routing produce/consume and for
    /// answering `FindLeader`: it asks the driver task hosting the replica for
    /// its own id when it leads, or the leader it last learned from an
    /// `AppendEntries`, rather than reading any stale `leader` value carried in
    /// `ClusterMetadata` (Requirement 8.4). Returns `None` when the driver knows
    /// of no current leader or its task has stopped (the caller treats that as
    /// "no leader yet" and the client retries).
    async fn known_leader(handle: &DriverHandle) -> Option<NodeId> {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::KnownLeader { reply: reply_tx })
            .ok()?;
        reply_rx.await.ok().flatten()
    }
}

/// Translate a metadata-group topic-admin error (`create_topic` /
/// `delete_topic`) into a [`tonic::Status`] (Requirement 4.1, 4.2).
///
/// The dedicated `__meta/0` metadata Raft group routes an admin proposal to its
/// leader and never commits it on a non-leader (design §4). The two leadership
/// outcomes are distinguished by the `leader` hint the node returns:
///
/// - [`CoreError::NotLeader`] `{ leader: Some(_) }` — the request reached a
///   metadata replica that is **not** the leader but knows who is. This is the
///   Raft §8 redirect: it surfaces as the existing `NotLeader` status carrying
///   the metadata leader hint, so the client retries against that leader
///   (Requirement 4.1). It flows through the shared
///   [`convert::core_error_to_status`], which already encodes the leader hint.
/// - [`CoreError::NotLeader`] `{ leader: None }` — the metadata group currently
///   has **no** elected leader (no majority, or an election is in progress), so
///   the change was committed nowhere. Rather than a bare "not leader" with an
///   empty hint, it is surfaced as a clear "no metadata leader available" error
///   the client may retry once a leader is elected (Requirement 4.2). The wire
///   classification stays [`v1::ErrorCode::NotLeader`] with no hint, so a client
///   treats it as a leadership condition (re-resolve, then retry) rather than a
///   permanent failure.
///
/// Every other error (validation, `TopicExists`, the indeterminate
/// `CommitTimeout`, …) maps through [`convert::core_error_to_status`] unchanged.
fn admin_error_to_status(error: &CoreError) -> Status {
    match error {
        CoreError::NotLeader { leader: None } => convert::vela_error_to_status(&v1::VelaError {
            code: v1::ErrorCode::NotLeader as i32,
            message: "no metadata leader is currently available".to_string(),
            leader: None,
        }),
        other => convert::core_error_to_status(other),
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

        // Validate topic admission and partition existence against metadata.
        // Any metadata `leader` field is only a non-authoritative initial hint
        // (Requirement 8.4); the live Raft-elected leader for a redirect is read
        // from the partition driver below (Requirement 8.1, 8.3).
        {
            let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            metadata
                .ensure_producible(&req.topic)
                .map_err(|e| convert::core_error_to_status(&e))?;
            metadata
                .topics
                .get(&req.topic)
                .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
                .ok_or_else(|| {
                    convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
                })?;
        }

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
                // Redirect to the partition's live Raft-elected leader, not the
                // stale metadata `leader` field (Requirement 8.3, 8.4).
                let leader = Self::known_leader(&handle).await;
                Err(convert::core_error_to_status(&CoreError::NotLeader {
                    leader,
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

        // The partition must exist in the served catalogue (Requirement 5.4).
        {
            let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            metadata
                .topics
                .get(&req.topic)
                .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
                .ok_or_else(|| {
                    convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
                })?;
        }

        let handle = self.node.handle(&req.topic, req.partition).ok_or_else(|| {
            convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
        })?;

        // Route by the partition's live Raft-elected leader, not any metadata
        // `leader` field (Requirement 8.1, 8.4). With no elected leader the
        // partition is unavailable while committed records are retained
        // (Requirement 7.3); if this node is not the live leader, redirect the
        // client to it rather than serving a stale local read (Requirement 8.3).
        match Self::known_leader(&handle).await {
            None => {
                return Err(convert::core_error_to_status(
                    &CoreError::PartitionUnavailable,
                ));
            }
            Some(leader) if leader.as_str() != self.node.self_id => {
                return Err(convert::core_error_to_status(&CoreError::NotLeader {
                    leader: Some(leader),
                }));
            }
            Some(_) => {}
        }

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

        // Decode the requested log backend BEFORE touching metadata: an
        // unspecified wire value defaults to Durable (Requirement 2.2) and any
        // out-of-range value is rejected as a validation error, creating no
        // topic (Requirement 2.5).
        let backend = convert::log_backend_from_proto(req.log_backend)
            .map_err(|e| convert::core_error_to_status(&e))?;

        // Validate, assign replicas, and commit the create through the
        // dedicated `__meta/0` metadata Raft group, then read the applied topic
        // back from the served view. The metadata group routes the proposal to
        // its leader (redirecting a non-leader with `NotLeader`) and reports
        // success only once the change is committed to a majority (Req 3, 4.1).
        // Partition drivers are started by the commit-driven reconciler on every
        // replica node, not in this handler (Req 6.1).
        //
        // `NotLeader { leader: Some }` carries the metadata leader hint for the
        // client redirect (Req 4.1); `NotLeader { leader: None }` (no elected
        // metadata leader) becomes a "no metadata leader available" error rather
        // than a local commit (Req 4.2) — see [`admin_error_to_status`].
        let topic = self
            .node
            .create_topic(&req.name, req.partitions, backend)
            .await
            .map_err(|e| admin_error_to_status(&e))?;

        Ok(Response::new(v1::CreateTopicResponse {
            topic: Some(convert::topic_to_proto(&topic)),
        }))
    }

    async fn delete_topic(
        &self,
        request: Request<v1::DeleteTopicRequest>,
    ) -> Result<Response<v1::DeleteTopicResponse>, Status> {
        let req = request.into_inner();

        // Commit the deletion through the dedicated `__meta/0` metadata Raft
        // group (an absent topic is an idempotent no-op success, H2). The
        // metadata group routes the proposal to its leader and reports success
        // only once the removal commits (Req 3, 4.1). The commit-driven
        // reconciler stops the deleted topic's drivers on every replica node,
        // not in this handler (Req 6.2).
        //
        // `NotLeader { leader: Some }` carries the metadata leader hint for the
        // client redirect (Req 4.1); `NotLeader { leader: None }` (no elected
        // metadata leader) becomes a "no metadata leader available" error rather
        // than a local commit (Req 4.2) — see [`admin_error_to_status`].
        self.node
            .delete_topic(&req.name)
            .await
            .map_err(|e| admin_error_to_status(&e))?;

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

        // The partition must exist in the served catalogue; its replica
        // locations come from there, but its authoritative leader does not
        // (Requirement 8.1, 8.4).
        {
            let metadata = self.node.metadata.lock().expect("metadata mutex poisoned");
            metadata
                .topics
                .get(&req.topic)
                .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
                .ok_or_else(|| {
                    convert::core_error_to_status(&Self::not_found(&req.topic, req.partition))
                })?;
        }

        // Resolve to the live Raft-elected leader (Requirement 8.2): if this
        // node hosts the partition's replica, answer with its known current
        // leader; otherwise report no leader so the client retries against a
        // replica. The wire response models an absent leader as `None`.
        let leader = match self.node.handle(&req.topic, req.partition) {
            Some(handle) => Self::known_leader(&handle).await,
            None => None,
        };
        Ok(Response::new(v1::FindLeaderResponse {
            leader: leader.map(|n| n.0),
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
        _request: Request<v1::SyncMetadataRequest>,
    ) -> Result<Response<v1::SyncMetadataReply>, Status> {
        // Reserved no-op (Requirement 1.3). Cluster metadata is now agreed
        // solely through the dedicated `__meta/0` Raft group and reaches every
        // node via `AppendEntries`, not a snapshot push. The bespoke
        // adopt-fresher-snapshot / epoch-acknowledgement protocol is removed, so
        // this RPC never mutates the served catalogue. It stays defined to keep
        // the gRPC contract valid (and reserved for a future `InstallSnapshot`
        // path, Raft §7); it answers with the node's current applied-change
        // epoch read-only without adopting the incoming snapshot.
        let epoch = self
            .node
            .metadata
            .lock()
            .expect("metadata mutex poisoned")
            .epoch;
        Ok(Response::new(v1::SyncMetadataReply { epoch }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vela_core::NodeId;

    /// A non-leader metadata replica that knows the leader surfaces the Raft §8
    /// redirect: the `NotLeader` classification carrying the leader hint, so the
    /// client retries against that node (Requirement 4.1).
    #[test]
    fn admin_not_leader_with_hint_redirects_carrying_the_leader() {
        let status = admin_error_to_status(&CoreError::NotLeader {
            leader: Some(NodeId::new("node-2")),
        });
        let vela_error =
            convert::vela_error_from_status(&status).expect("status carries a typed VelaError");
        assert_eq!(vela_error.code, v1::ErrorCode::NotLeader as i32);
        assert_eq!(vela_error.leader.as_deref(), Some("node-2"));
        // It maps through the shared converter unchanged (same status code).
        assert_eq!(
            status.code(),
            convert::core_error_to_status(&CoreError::NotLeader {
                leader: Some(NodeId::new("node-2")),
            })
            .code()
        );
    }

    /// No elected metadata leader becomes a clear "no metadata leader available"
    /// error with no hint, rather than committing locally (Requirement 4.2).
    #[test]
    fn admin_no_metadata_leader_reports_no_leader_available() {
        let status = admin_error_to_status(&CoreError::NotLeader { leader: None });
        let vela_error =
            convert::vela_error_from_status(&status).expect("status carries a typed VelaError");
        assert_eq!(vela_error.code, v1::ErrorCode::NotLeader as i32);
        assert_eq!(vela_error.leader, None);
        assert!(
            vela_error.message.contains("no metadata leader"),
            "message should indicate no metadata leader is available, got: {}",
            vela_error.message
        );
    }

    /// A non-leadership admin error (here the indeterminate commit timeout) is
    /// passed through the shared converter unchanged.
    #[test]
    fn admin_other_errors_pass_through_unchanged() {
        let status = admin_error_to_status(&CoreError::CommitTimeout);
        let vela_error =
            convert::vela_error_from_status(&status).expect("status carries a typed VelaError");
        assert_eq!(vela_error.code, v1::ErrorCode::CommitTimeout as i32);
        assert_eq!(
            status.code(),
            convert::core_error_to_status(&CoreError::CommitTimeout).code()
        );
    }

    // -----------------------------------------------------------------------
    // Live-leader routing (task 7.3)
    //
    // `Produce` / `Consume` / `FindLeader` route by the partition driver's
    // known **live** Raft-elected leader, never the (non-authoritative) `leader`
    // field carried in `ClusterMetadata`. Each test seeds the served metadata
    // with a deliberately bogus `leader` value and a controllable mock driver,
    // then asserts the routing decision follows the driver's `KnownLeader`
    // answer rather than that stale field (Requirement 8.2, 8.3, 8.4).
    // -----------------------------------------------------------------------

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use tokio::sync::mpsc;

    use vela_core::{
        ClusterMetadata, CommittedRecord, LogBackend, Member, MetadataController, NodeAvailability,
        Offset, Partition, Topic, TopicState,
    };

    use crate::registry::raft_node_id;
    use crate::transport::PeerPool;

    /// A bogus `leader` value seeded into `ClusterMetadata` to prove routing
    /// never reads the stale metadata field once a partition has a live leader
    /// (Requirement 8.4).
    const STALE_METADATA_LEADER: &str = "stale-metadata-leader";

    /// A mock partition driver that answers the routing-relevant
    /// [`DriverCommand`]s with fixed, test-controlled values, standing in for a
    /// real [`PartitionDriver`](crate::driver::PartitionDriver) so a test can
    /// pin the partition's *live* known leader and its serve results.
    struct MockDriver {
        /// The live Raft-elected leader the driver reports for `KnownLeader`.
        known_leader: Option<NodeId>,
        /// The result a `Produce` command resolves to.
        produce: Result<Offset, ProduceError>,
        /// The records a `Consume` command resolves to.
        consume: Vec<CommittedRecord>,
    }

    impl MockDriver {
        /// A driver reporting `leader` as its live known leader; it rejects
        /// produces as a non-leader and reads back nothing (the defaults the
        /// redirect-path tests use).
        fn reporting(leader: Option<&str>) -> Self {
            Self {
                known_leader: leader.map(NodeId::new),
                produce: Err(ProduceError::NotLeader),
                consume: Vec::new(),
            }
        }

        /// Spawn the mock as a driver task, returning its command handle.
        fn spawn(self) -> DriverHandle {
            let (tx, mut rx) = mpsc::unbounded_channel::<DriverCommand>();
            tokio::spawn(async move {
                while let Some(command) = rx.recv().await {
                    match command {
                        DriverCommand::KnownLeader { reply } => {
                            let _ = reply.send(self.known_leader.clone());
                        }
                        DriverCommand::Produce { reply, .. } => {
                            let _ = reply.send(self.produce);
                        }
                        DriverCommand::Consume { reply, .. } => {
                            let _ = reply.send(self.consume.clone());
                        }
                        DriverCommand::Shutdown => break,
                        _ => {}
                    }
                }
            });
            tx
        }
    }

    /// Build node state for `self_id` hosting `topic`/`partition`, whose served
    /// metadata records `stale_leader` as the partition's (non-authoritative)
    /// `leader` field, with `driver` registered as the partition's running
    /// driver.
    fn node_hosting(
        self_id: &str,
        topic: &str,
        partition: u32,
        stale_leader: Option<&str>,
        driver: DriverHandle,
    ) -> Arc<NodeShared> {
        let mut metadata = ClusterMetadata::new();
        metadata.members.push(Member {
            id: NodeId::new(self_id),
            addr: "127.0.0.1:7001".to_string(),
            availability: NodeAvailability::Available,
        });
        metadata.topics.insert(
            topic.to_string(),
            Topic {
                name: topic.to_string(),
                partitions: vec![Partition {
                    index: PartitionIndex(partition),
                    replicas: vec![NodeId::new(self_id), NodeId::new("node-b")],
                    leader: stale_leader.map(NodeId::new),
                }],
                state: TopicState::Active,
                backend: LogBackend::InMemory,
            },
        );

        let node = Arc::new(NodeShared {
            self_id: self_id.to_string(),
            replication_factor: 3,
            metadata: Arc::new(Mutex::new(metadata)),
            partitions: Mutex::new(HashMap::new()),
            pool: Arc::new(PeerPool::new()),
            data_dir: PathBuf::from("unused-in-routing-tests"),
            controller: Arc::new(Mutex::new(MetadataController::new(
                raft_node_id(self_id),
                Vec::new(),
            ))),
        });
        node.partitions
            .lock()
            .expect("partitions mutex poisoned")
            .insert((topic.to_string(), partition), driver);
        node
    }

    /// A produce request for `topic`/`partition` carrying a single value.
    fn produce_request(topic: &str, partition: u32) -> Request<v1::ProduceRequest> {
        Request::new(v1::ProduceRequest {
            topic: topic.to_string(),
            partition,
            record: Some(v1::Record {
                key: None,
                value: b"v0".to_vec(),
            }),
        })
    }

    /// A consume request for `topic`/`partition` from offset 0.
    fn consume_request(topic: &str, partition: u32) -> Request<v1::ConsumeRequest> {
        Request::new(v1::ConsumeRequest {
            topic: topic.to_string(),
            partition,
            offset: 0,
            max_count: None,
        })
    }

    /// Requirement 8.3, 8.4: a `Produce` reaching a replica that is not the
    /// partition's live leader is redirected to that **live** leader, using the
    /// driver's known-leader hint rather than the stale metadata `leader` field.
    #[tokio::test]
    async fn produce_to_non_leader_redirects_to_the_live_leader() {
        let driver = MockDriver::reporting(Some("node-b")).spawn();
        let node = node_hosting("node-a", "orders", 0, Some(STALE_METADATA_LEADER), driver);
        let service = VelaClientService::new(node);

        let status = service
            .produce(produce_request("orders", 0))
            .await
            .expect_err("a non-leader produce is redirected");
        let error =
            convert::vela_error_from_status(&status).expect("status carries a typed VelaError");

        assert_eq!(error.code, v1::ErrorCode::NotLeader as i32);
        assert_eq!(
            error.leader.as_deref(),
            Some("node-b"),
            "produce must redirect to the live Raft-elected leader (Req 8.3)"
        );
        assert_ne!(
            error.leader.as_deref(),
            Some(STALE_METADATA_LEADER),
            "produce must NOT use the stale metadata leader field (Req 8.4)"
        );
    }

    /// Requirement 8.3, 8.4: a `Consume` reaching a replica that is not the
    /// partition's live leader is redirected to that **live** leader, again from
    /// the driver's known-leader hint, not the stale metadata field.
    #[tokio::test]
    async fn consume_to_non_leader_redirects_to_the_live_leader() {
        let driver = MockDriver::reporting(Some("node-b")).spawn();
        let node = node_hosting("node-a", "orders", 0, Some(STALE_METADATA_LEADER), driver);
        let service = VelaClientService::new(node);

        let status = service
            .consume(consume_request("orders", 0))
            .await
            .expect_err("a non-leader consume is redirected");
        let error =
            convert::vela_error_from_status(&status).expect("status carries a typed VelaError");

        assert_eq!(error.code, v1::ErrorCode::NotLeader as i32);
        assert_eq!(
            error.leader.as_deref(),
            Some("node-b"),
            "consume must redirect to the live Raft-elected leader (Req 8.3)"
        );
        assert_ne!(
            error.leader.as_deref(),
            Some(STALE_METADATA_LEADER),
            "consume must NOT use the stale metadata leader field (Req 8.4)"
        );
    }

    /// Requirement 8.2 (and 7.3): with no live leader elected, `Consume` is
    /// rejected as partition-unavailable rather than served from a stale local
    /// read, even though the metadata `leader` field names a (bogus) leader.
    #[tokio::test]
    async fn consume_with_no_elected_leader_is_partition_unavailable() {
        let driver = MockDriver::reporting(None).spawn();
        let node = node_hosting("node-a", "orders", 0, Some(STALE_METADATA_LEADER), driver);
        let service = VelaClientService::new(node);

        let status = service
            .consume(consume_request("orders", 0))
            .await
            .expect_err("consume with no live leader is unavailable");
        let error =
            convert::vela_error_from_status(&status).expect("status carries a typed VelaError");

        assert_eq!(
            error.code,
            v1::ErrorCode::PartitionUnavailable as i32,
            "with no live leader, consume is unavailable, not a stale local read (Req 8.2)"
        );
    }

    /// Requirement 8.2: when this node *is* the partition's live leader, the
    /// consume is served locally and returns the partition's committed records.
    #[tokio::test]
    async fn consume_on_the_live_leader_serves_committed_records() {
        let mut mock = MockDriver::reporting(Some("node-a"));
        mock.consume = vec![
            CommittedRecord {
                offset: 0,
                value: b"v0".to_vec(),
            },
            CommittedRecord {
                offset: 1,
                value: b"v1".to_vec(),
            },
        ];
        let node = node_hosting(
            "node-a",
            "orders",
            0,
            Some(STALE_METADATA_LEADER),
            mock.spawn(),
        );
        let service = VelaClientService::new(node);

        let response = service
            .consume(consume_request("orders", 0))
            .await
            .expect("the live leader serves the consume")
            .into_inner();

        let offsets: Vec<u64> = response.records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![0, 1]);
        assert_eq!(response.next_offset, 2);
    }

    /// Requirement 8.2, 8.4: `FindLeader` resolves to the partition's live
    /// Raft-elected leader reported by the driver, not the stale metadata
    /// `leader` field (here a bogus value the answer must not echo).
    #[tokio::test]
    async fn find_leader_returns_the_live_elected_leader() {
        let driver = MockDriver::reporting(Some("node-b")).spawn();
        let node = node_hosting("node-a", "orders", 0, Some(STALE_METADATA_LEADER), driver);
        let service = VelaClientService::new(node);

        let response = service
            .find_leader(Request::new(v1::FindLeaderRequest {
                topic: "orders".to_string(),
                partition: 0,
            }))
            .await
            .expect("find_leader succeeds")
            .into_inner();

        assert_eq!(
            response.leader.as_deref(),
            Some("node-b"),
            "FindLeader returns the live Raft-elected leader (Req 8.2)"
        );
        assert_ne!(
            response.leader.as_deref(),
            Some(STALE_METADATA_LEADER),
            "FindLeader must NOT echo the stale metadata leader field (Req 8.4)"
        );
    }

    /// Requirement 8.2: `FindLeader` indicates no current leader (an absent
    /// leader on the wire) when the partition's driver knows of none, regardless
    /// of any stale metadata `leader` value.
    #[tokio::test]
    async fn find_leader_indicates_no_leader_when_none_is_elected() {
        let driver = MockDriver::reporting(None).spawn();
        let node = node_hosting("node-a", "orders", 0, Some(STALE_METADATA_LEADER), driver);
        let service = VelaClientService::new(node);

        let response = service
            .find_leader(Request::new(v1::FindLeaderRequest {
                topic: "orders".to_string(),
                partition: 0,
            }))
            .await
            .expect("find_leader succeeds")
            .into_inner();

        assert_eq!(
            response.leader, None,
            "FindLeader indicates no current leader when none is elected (Req 8.2)"
        );
    }
}
