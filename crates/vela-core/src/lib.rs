//! `vela-core` — the domain layer.
//!
//! Owns the topic/partition model, partition routing, cluster metadata, and the
//! fleet of per-partition Raft groups hosted on a node. Composes [`vela_raft`]
//! and [`vela_log`] but knows nothing about gRPC.

pub mod consume;
pub mod fleet;
pub mod metadata;
pub mod model;
pub mod partition_log;
pub mod produce;
pub mod reconcile;
pub mod router;
pub mod topic;

pub use consume::{consume, DEFAULT_MAX_RECORDS, MAX_MAX_RECORDS, MIN_MAX_RECORDS};
pub use fleet::{
    AppliedOffsets, CommittedRecord, FleetError, GroupKey, PartitionReplica, RaftGroupFleet,
    StateMachine,
};
pub use metadata::{
    apply_command, metadata_group_key, MetadataController, MetadataRecoverError,
    METADATA_GROUP_PARTITION, METADATA_GROUP_TOPIC, METADATA_PROPAGATION_TIMEOUT_MS,
};
pub use model::{
    ClusterCommand, ClusterMetadata, LogBackend, Member, NodeAvailability, NodeId, Offset,
    Partition, PartitionIndex, Record, Topic, TopicState,
};
#[cfg(feature = "sim")]
pub use partition_log::SimWalClock;
pub use partition_log::{PartitionLog, PartitionLogKind};
pub use produce::{
    decode_record_batch, encode_record_batch, produce, produce_batch, validate_batch, BatchOutcome,
    BatchRejection, ProduceOutcome, COMMIT_TIMEOUT_MS, MAX_BATCH_BYTES, MAX_BATCH_RECORDS,
    MAX_RECORD_BYTES,
};
pub use reconcile::{plan_reconcile, ReconcilePlan};
pub use router::PartitionRouter;
pub use topic::CoreError;

// Re-export the durable-log configuration surface so the server can build
// partition-log configs without taking a direct `vela-server -> vela-log`
// dependency edge (`vela-core` already depends on `vela-log`).
pub use vela_log::{SyncPolicy, WalConfig};
