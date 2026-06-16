//! Core domain types for Vela's topic, partition, and cluster model.
//!
//! These are the in-memory shapes the domain layer operates on (the matching
//! wire types are declared in protobuf in `vela-proto`). This module defines
//! the data model only — topic creation, deletion, routing, and metadata
//! agreement land in later tasks (10.2, 10.8, 11.x, 12.x).
//!
//! ## A note on `NodeId`
//!
//! `vela-core` identifies nodes by a stable **string** identity
//! ([`NodeId`]), matching the design's Data Models. The inner consensus crate
//! [`vela_raft`] uses its own numeric `NodeId(u64)` to key the per-peer leader
//! state maps cheaply. The two are deliberately distinct: the domain layer
//! works in human-meaningful node identities, and the server crate is
//! responsible for mapping a core [`NodeId`] to the numeric `vela_raft::NodeId`
//! used within a partition's Raft group. Keeping them separate avoids leaking
//! the consensus representation into the domain model.

use std::collections::BTreeMap;

/// A stable node identity within the cluster (Requirement 9.3).
///
/// A string so node identities can be human-meaningful (e.g. `"node-a"`); maps
/// to a numeric `vela_raft::NodeId` at the consensus seam (see module docs).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub String);

impl NodeId {
    /// Construct a [`NodeId`] from anything string-like.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the identity as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A 0-based committed record position within a partition's log
/// (Requirement "Offset", 4.7, 5.1).
pub type Offset = u64;

/// A single event entry produced to and consumed from a partition.
///
/// Both `key` and `value` are opaque byte payloads. The combined key and value
/// size must not exceed 1 MiB; that bound is validated on the produce path
/// (Requirement 4.8), not here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Optional opaque key. The partition key used for routing is handled at
    /// the routing layer, not stored here.
    pub key: Option<Vec<u8>>,
    /// Opaque payload bytes.
    pub value: Vec<u8>,
}

impl Record {
    /// Construct a record with an optional key and a value.
    pub fn new(key: Option<Vec<u8>>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

/// The 0-based index of a partition within its topic (Requirement 2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionIndex(pub u32);

/// A shard of a topic: the unit of ordering, replication, and consensus.
///
/// Each partition is backed by its own independent Raft group (Requirement
/// 7.1). The `replicas` are distinct nodes whose count equals the cluster's
/// replication factor (Requirement 2.3), and `leader` is the node currently
/// leading the partition's Raft group, or `None` during an election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    /// This partition's 0-based index within the topic.
    pub index: PartitionIndex,
    /// The distinct nodes hosting replicas; `len == replication_factor`.
    pub replicas: Vec<NodeId>,
    /// The current leader, or `None` while an election is in progress.
    pub leader: Option<NodeId>,
}

/// The lifecycle state of a topic.
///
/// A topic is `Active` once created; it transitions to `Deleting` while a
/// deletion is in progress, during which produce and duplicate-delete requests
/// are rejected (Requirement 3.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicState {
    /// The topic is live and accepting produce/consume requests.
    Active,
    /// The topic is being deleted; new operations are rejected.
    Deleting,
}

/// A topic's log storage backend: exactly one of two values (locked decision
/// 2). [`LogBackend::Durable`] is the default — an omitted selection on the
/// create path resolves to it (Requirement 1.2, 2.2) — and is backed by
/// `vela_log::DurableWal`; [`LogBackend::InMemory`] is backed by the existing
/// `vela_log::InMemoryLog`.
///
/// The backend a topic is created with is recorded in [`ClusterMetadata`] and
/// is immutable for the topic's lifetime (Requirement 3.1, 3.3). Mapping a
/// backend to the concrete log variant to construct at spawn time is the job of
/// the spawn-selection helper on [`PartitionLog`](crate::partition_log::PartitionLog).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogBackend {
    /// The durable Write-Ahead-Log backend (`vela_log::DurableWal`). The default
    /// when a create request does not specify a backend (Requirement 1.2).
    #[default]
    Durable,
    /// The volatile in-memory backend (`vela_log::InMemoryLog`).
    InMemory,
}

/// A named stream of event records, divided into one or more partitions.
///
/// The `name` is 1–255 characters of `[A-Za-z0-9_-]` and the partition count is
/// `1..=10_000` (Requirement 2.1); those bounds are enforced at creation time
/// (task 10.2), not by this struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    /// The topic name, unique within its namespace.
    pub name: String,
    /// The topic's partitions, indexed `0..partition_count` (Requirement 2.2).
    pub partitions: Vec<Partition>,
    /// The topic's lifecycle state (Requirement 3.7).
    pub state: TopicState,
    /// The topic's log backend, fixed at creation and recorded here for the
    /// topic's lifetime (Requirement 3.1, 3.3).
    pub backend: LogBackend,
}

/// The availability of a cluster member, which is exactly one of two states
/// (Requirement 9.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeAvailability {
    /// The node is reachable and participating.
    Available,
    /// The node is unreachable (failed connection or missed heartbeats).
    Unavailable,
}

/// A member node of the cluster: its identity, network address, and current
/// availability (Requirement 9.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// The member's stable identity.
    pub id: NodeId,
    /// The member's network address (host:port).
    pub addr: String,
    /// Whether the member is currently available or unavailable.
    pub availability: NodeAvailability,
}

/// The cluster-wide metadata: membership, topics, and a monotonically
/// increasing epoch (Requirement 9.3).
///
/// `topics` is keyed by topic name within a namespace; `epoch` is bumped on
/// each committed metadata change so propagated views can be ordered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterMetadata {
    /// The member nodes known to the cluster (Requirement 9.3).
    pub members: Vec<Member>,
    /// Topics keyed by name within the namespace.
    pub topics: BTreeMap<String, Topic>,
    /// Bumped on each committed metadata change.
    pub epoch: u64,
}

impl ClusterMetadata {
    /// Create empty cluster metadata: no members, no topics, epoch 0.
    pub fn new() -> Self {
        Self::default()
    }
}

/// A replicated metadata mutation applied through the dedicated metadata Raft
/// group. Committing one of these is what atomically changes
/// [`ClusterMetadata`] across the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterCommand {
    /// Register a new topic with its assigned partitions.
    CreateTopic {
        /// The new topic's name.
        name: String,
        /// The partitions (with replica assignments) for the topic.
        partitions: Vec<Partition>,
        /// The log backend every node must record for the topic (Requirement
        /// 2.3, 3.2).
        backend: LogBackend,
    },
    /// Remove an existing topic and all of its partitions.
    DeleteTopic {
        /// The name of the topic to delete.
        name: String,
    },
    /// Change a node's availability in the cluster view.
    SetAvailability {
        /// The node whose availability is changing.
        node: NodeId,
        /// The new availability state.
        availability: NodeAvailability,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_construction_and_accessors() {
        let id = NodeId::new("node-a");
        assert_eq!(id.as_str(), "node-a");
        assert_eq!(id, NodeId("node-a".to_string()));
    }

    #[test]
    fn record_holds_optional_key_and_value() {
        let keyed = Record::new(Some(vec![1, 2]), vec![3, 4]);
        assert_eq!(keyed.key, Some(vec![1, 2]));
        assert_eq!(keyed.value, vec![3, 4]);

        let keyless = Record::new(None, vec![9]);
        assert!(keyless.key.is_none());
    }

    #[test]
    fn partition_construction() {
        let p = Partition {
            index: PartitionIndex(0),
            replicas: vec![NodeId::new("a"), NodeId::new("b"), NodeId::new("c")],
            leader: Some(NodeId::new("a")),
        };
        assert_eq!(p.index, PartitionIndex(0));
        assert_eq!(p.replicas.len(), 3);
        assert_eq!(p.leader, Some(NodeId::new("a")));
    }

    #[test]
    fn topic_construction_with_state() {
        let topic = Topic {
            name: "orders".to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas: vec![NodeId::new("a")],
                leader: None,
            }],
            state: TopicState::Active,
            backend: LogBackend::Durable,
        };
        assert_eq!(topic.name, "orders");
        assert_eq!(topic.partitions.len(), 1);
        assert_eq!(topic.state, TopicState::Active);
        assert_eq!(topic.backend, LogBackend::Durable);
    }

    #[test]
    fn member_carries_identity_addr_and_availability() {
        let member = Member {
            id: NodeId::new("node-a"),
            addr: "127.0.0.1:7001".to_string(),
            availability: NodeAvailability::Available,
        };
        assert_eq!(member.id, NodeId::new("node-a"));
        assert_eq!(member.addr, "127.0.0.1:7001");
        assert_eq!(member.availability, NodeAvailability::Available);
    }

    #[test]
    fn empty_cluster_metadata_defaults() {
        let meta = ClusterMetadata::new();
        assert!(meta.members.is_empty());
        assert!(meta.topics.is_empty());
        assert_eq!(meta.epoch, 0);
    }

    #[test]
    fn cluster_metadata_indexes_topics_by_name() {
        let mut meta = ClusterMetadata::new();
        meta.topics.insert(
            "orders".to_string(),
            Topic {
                name: "orders".to_string(),
                partitions: Vec::new(),
                state: TopicState::Active,
                backend: LogBackend::Durable,
            },
        );
        assert!(meta.topics.contains_key("orders"));
        assert_eq!(meta.topics["orders"].state, TopicState::Active);
    }

    #[test]
    fn cluster_command_variants_construct() {
        let create = ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas: vec![NodeId::new("a")],
                leader: None,
            }],
            backend: LogBackend::Durable,
        };
        let delete = ClusterCommand::DeleteTopic {
            name: "orders".to_string(),
        };
        let avail = ClusterCommand::SetAvailability {
            node: NodeId::new("a"),
            availability: NodeAvailability::Unavailable,
        };

        // Distinct variants are not equal to one another.
        assert_ne!(create, delete);
        assert_ne!(delete, avail);
    }
}
