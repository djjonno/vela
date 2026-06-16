//! Cluster metadata management: availability, the metadata Raft group, and
//! propagation tracking.
//!
//! `ClusterMetadata` (topics, partition/replica assignments, leaders, and
//! membership) must itself be agreed across the cluster. Following the design's
//! **Option A — a dedicated metadata Raft group**, this module runs a single,
//! well-known control group keyed `("__meta", PartitionIndex(0))` (the
//! `__meta/p0` group). `ClusterCommand`s are entries in that group's log; once
//! committed, [`apply_command`] folds the change into a [`ClusterMetadata`] and
//! bumps its `epoch`. After a change commits, the leader pushes the new
//! metadata (carrying its `epoch`) to every reachable node via `SyncMetadata`
//! and must observe acknowledgements within
//! [`METADATA_PROPAGATION_TIMEOUT_MS`], reporting laggards on delete
//! (Requirement 2.8, 3.5, 3.6).
//!
//! This module owns four concerns:
//!
//! - **Availability tracking** on [`ClusterMetadata`] — each member's
//!   availability is exactly one of [`NodeAvailability::Available`] or
//!   [`NodeAvailability::Unavailable`] (Requirement 9.3).
//! - **The metadata Raft group controller** — [`MetadataController`] hosts the
//!   `__meta/p0` group in a [`RaftGroupFleet`] and applies committed
//!   `ClusterCommand`s to its [`ClusterMetadata`] (Requirement 3.1, 9.3).
//! - **Propagation/ack tracking** — acks are modelled at the core level as the
//!   set of node ids that acknowledged a given `epoch`; the controller reports
//!   the expected-but-missing nodes as [`CoreError::MetadataPropagation`] on
//!   delete (Requirement 2.8, 3.5, 3.6).
//! - **`FindLeader` resolution** — [`MetadataController::find_leader`] resolves
//!   `(topic, partition)` to the current leader (Requirement 10.4).
//!
//! The 5-second wall-clock propagation deadline itself is enforced by the
//! server crate (it owns the real clock and the `SyncMetadata` RPCs); this
//! layer is given the set of nodes that acked before the deadline and computes
//! who is missing. Likewise, decoding committed log bytes back into a
//! [`ClusterCommand`] is the server's concern — [`apply_command`] operates on
//! an already-decoded, already-committed command so `vela-core` stays free of
//! the wire encoding.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use vela_log::{DurableWal, LogError, LogStorage, PayloadKind, SyncPolicy, WalConfig};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, RaftOutput};

use crate::fleet::{FleetError, GroupKey, RaftGroupFleet};
use crate::model::{
    ClusterCommand, ClusterMetadata, NodeAvailability, NodeId, Partition, PartitionIndex, Topic,
    TopicState,
};
use crate::partition_log::PartitionLog;
use crate::topic::CoreError;

/// The well-known topic name of the dedicated metadata Raft group (design
/// "Option A": the `__meta/p0` control group).
pub const METADATA_GROUP_TOPIC: &str = "__meta";

/// The partition index of the dedicated metadata Raft group: always 0, since
/// the control group is a single partition.
pub const METADATA_GROUP_PARTITION: PartitionIndex = PartitionIndex(0);

/// The propagation deadline for a committed metadata change: the leader must
/// observe acknowledgements from every reachable node within this many
/// milliseconds (Requirement 2.8, 3.5, 3.6).
///
/// The wall-clock enforcement of this deadline lives in the server crate, which
/// owns the real clock and the `SyncMetadata` RPCs; this constant documents the
/// contract and is the value the server times against.
pub const METADATA_PROPAGATION_TIMEOUT_MS: u64 = 5000;

/// The [`GroupKey`] of the dedicated metadata Raft group, `("__meta", p0)`.
pub fn metadata_group_key() -> GroupKey {
    (METADATA_GROUP_TOPIC.to_string(), METADATA_GROUP_PARTITION)
}

impl ClusterMetadata {
    /// Set the availability of the member identified by `node`, returning
    /// `true` if a matching member was found and updated (Requirement 9.3).
    ///
    /// Availability is a two-state value ([`NodeAvailability::Available`] or
    /// [`NodeAvailability::Unavailable`]); setting it simply replaces the
    /// member's current state. A `node` that is not a current member is not
    /// added — the call is a no-op and returns `false`, since availability is
    /// only meaningful for known members.
    pub fn set_availability(&mut self, node: &NodeId, availability: NodeAvailability) -> bool {
        match self.members.iter_mut().find(|m| &m.id == node) {
            Some(member) => {
                member.availability = availability;
                true
            }
            None => false,
        }
    }

    /// The current availability of the member identified by `node`, or `None`
    /// if `node` is not a member (Requirement 9.3).
    ///
    /// The returned value, when present, is always exactly one of the two
    /// [`NodeAvailability`] states.
    pub fn availability(&self, node: &NodeId) -> Option<NodeAvailability> {
        self.members
            .iter()
            .find(|m| &m.id == node)
            .map(|m| m.availability)
    }
}

/// Apply a committed [`ClusterCommand`] to `meta`, mutating it in place and
/// bumping its `epoch` (Requirement 3.1, 9.3).
///
/// This is the metadata Raft group's state-machine transition: it runs once per
/// committed command, after the command has already been validated (at propose
/// time) and agreed by the group. Because the command is already committed,
/// this function does not re-validate — it folds the change in directly:
///
/// - [`ClusterCommand::CreateTopic`] registers an [`TopicState::Active`] topic
///   with the committed partitions, recording the backend the command carries
///   so every node that applies the committed command stores the same backend
///   on its [`Topic`] (Requirement 2.3, 3.2).
/// - [`ClusterCommand::DeleteTopic`] removes the named topic (and, with it,
///   every partition the topic owns) in a single map removal.
/// - [`ClusterCommand::SetAvailability`] updates a member's availability.
///
/// Every applied command bumps `epoch`, so propagated views can be ordered and
/// acks attributed to a specific metadata version.
pub fn apply_command(meta: &mut ClusterMetadata, command: &ClusterCommand) {
    match command {
        ClusterCommand::CreateTopic {
            name,
            partitions,
            backend,
        } => {
            meta.topics.insert(
                name.clone(),
                Topic {
                    name: name.clone(),
                    partitions: partitions.clone(),
                    state: TopicState::Active,
                    backend: *backend,
                },
            );
        }
        ClusterCommand::DeleteTopic { name } => {
            meta.topics.remove(name);
        }
        ClusterCommand::SetAvailability { node, availability } => {
            meta.set_availability(node, *availability);
        }
    }
    meta.epoch += 1;
}

/// Errors raised while opening and recovering the durable metadata Raft group
/// (Requirement 16.1, 16.6, 17.x).
///
/// Recovering the `__meta/0` group performs real filesystem I/O (opening the
/// durable WAL) and a fleet insertion, so a failure in either is surfaced as a
/// typed error the server can fail fast on at startup rather than silently
/// degrading the catalogue.
#[derive(thiserror::Error, Debug)]
pub enum MetadataRecoverError {
    /// Opening (or creating) the durable metadata log failed.
    #[error("failed to open the durable metadata log: {0}")]
    Log(#[from] LogError),
    /// Registering the recovered metadata group in the fleet failed.
    #[error("failed to create the recovered metadata group: {0}")]
    Fleet(#[from] FleetError),
}

/// Manages how [`ClusterMetadata`] is agreed and propagated across the cluster
/// (design "Cluster Metadata Management").
///
/// The controller owns the node's view of the cluster metadata and hosts the
/// dedicated `__meta/p0` Raft group in a [`RaftGroupFleet`]. Committed
/// `ClusterCommand`s are applied through [`MetadataController::apply`]; the
/// resulting metadata is propagated by the server, which records the acks it
/// observes through [`MetadataController::record_ack`]. On a delete, the
/// controller reports any reachable node that failed to ack within the deadline
/// as [`CoreError::MetadataPropagation`].
pub struct MetadataController {
    /// The node-local view of cluster metadata, mutated as commands commit.
    metadata: ClusterMetadata,
    /// The fleet hosting the single `__meta/p0` Raft group.
    fleet: RaftGroupFleet,
    /// Per-epoch set of node ids that acknowledged the propagated metadata.
    acks: HashMap<u64, BTreeSet<NodeId>>,
}

impl MetadataController {
    /// Create a controller hosting the `__meta/p0` Raft group with this node's
    /// consensus identity `node_id` and its `peers`, starting from empty
    /// metadata.
    ///
    /// The dedicated metadata group is created eagerly so the controller is
    /// ready to agree `ClusterCommand`s immediately.
    pub fn new(node_id: RaftNodeId, peers: Vec<RaftNodeId>) -> Self {
        let mut fleet = RaftGroupFleet::new();
        // The fleet is fresh, so creating the single metadata group cannot
        // collide with an existing one.
        fleet
            .create_group(metadata_group_key(), node_id, peers)
            .expect("fresh fleet has no metadata group yet");
        Self {
            metadata: ClusterMetadata::new(),
            fleet,
            acks: HashMap::new(),
        }
    }

    /// Create a controller around an existing `metadata` view, hosting the
    /// `__meta/p0` group with the given identity and peers.
    pub fn with_metadata(
        metadata: ClusterMetadata,
        node_id: RaftNodeId,
        peers: Vec<RaftNodeId>,
    ) -> Self {
        let mut controller = Self::new(node_id, peers);
        controller.metadata = metadata;
        controller
    }

    /// Build a controller from an already-recovered `metadata` view and the
    /// `fleet` hosting its (recovered) `__meta/0` group.
    ///
    /// Unlike [`MetadataController::new`]/[`MetadataController::with_metadata`],
    /// which build a *fresh* in-memory metadata group, this installs a fleet the
    /// caller has already populated — the path
    /// [`MetadataController::recover_durable`] uses to hand back the durable,
    /// recovered group together with the catalogue rebuilt from its committed
    /// log. Acks start empty: they are per-process propagation bookkeeping, not
    /// recovered state.
    fn from_parts(metadata: ClusterMetadata, fleet: RaftGroupFleet) -> Self {
        Self {
            metadata,
            fleet,
            acks: HashMap::new(),
        }
    }

    /// Open (or create) the durable `__meta/0` metadata Raft group at
    /// `meta_path` and recover the committed cluster catalogue from it
    /// (Requirement 16.1, 16.6, 17.1, 17.2, 17.3, 17.4).
    ///
    /// The metadata group is infrastructure, not a client-selectable topic, so
    /// it always uses the [`Durable`](crate::model::LogBackend::Durable) backend
    /// with the only consensus-safe policy, [`SyncPolicy::Always`] (Requirement
    /// 16.1, 16.6). The durable WAL is opened directly at `meta_path` — path
    /// derivation is the server's concern, so the caller passes the reserved
    /// metadata path in — wrapped in a [`PartitionLog::Durable`], and the group
    /// is created through [`RaftGroupFleet::create_recovered_group`], which
    /// restores the Raft hard state and commit index from the recovered log
    /// (Requirement 17.1, 17.2, 17.3).
    ///
    /// The catalogue is then rebuilt by replaying the recovered log's committed
    /// prefix: every committed entry carrying a [`PayloadKind::Cluster`] payload
    /// is decoded back into a [`ClusterCommand`] and folded into a fresh
    /// [`ClusterMetadata`] via [`apply_command`], in ascending index order, so
    /// the recovered view equals the one held before the restart, including each
    /// topic's recorded backend (Requirement 17.4, 18.1, 18.3).
    ///
    /// Because `vela-core` must stay free of the wire encoding, the mapping from
    /// a committed entry's payload bytes back into a [`ClusterCommand`] is
    /// injected as `decode_cluster`; the server owns the codec and supplies the
    /// matching decoder.
    pub fn recover_durable(
        node_id: RaftNodeId,
        peers: Vec<RaftNodeId>,
        meta_path: &Path,
        decode_cluster: impl Fn(&[u8]) -> ClusterCommand,
    ) -> Result<Self, MetadataRecoverError> {
        // Open the durable metadata log at the reserved path with the only
        // consensus-safe sync policy (Requirement 16.6).
        let wal = DurableWal::open(WalConfig::new(meta_path).with_sync_policy(SyncPolicy::Always))?;
        let log = PartitionLog::Durable(wal);

        // Create the recovered group: this restores the Raft hard state and the
        // commit index from the recovered log (Requirement 17.1, 17.2, 17.3).
        let mut fleet = RaftGroupFleet::new();
        fleet.create_recovered_group(metadata_group_key(), node_id, peers, log)?;

        // Rebuild the catalogue by re-applying every committed `Cluster`
        // command in ascending index order (Requirement 17.4, 18.1, 18.3).
        let mut metadata = ClusterMetadata::new();
        let replica = fleet
            .get(&metadata_group_key())
            .expect("the metadata group was just created");
        if let Some(commit) = replica.raft().commit_index() {
            for entry in replica.raft().log().read(0, commit) {
                if entry.payload.kind == PayloadKind::Cluster {
                    apply_command(&mut metadata, &decode_cluster(&entry.payload.bytes));
                }
            }
        }

        Ok(Self::from_parts(metadata, fleet))
    }

    /// Shared, read-only access to the current cluster metadata view.
    pub fn metadata(&self) -> &ClusterMetadata {
        &self.metadata
    }

    /// Whether the controller is hosting the dedicated metadata Raft group.
    pub fn hosts_metadata_group(&self) -> bool {
        self.fleet.contains(&metadata_group_key())
    }

    /// Drive the dedicated `__meta/p0` Raft group one step with `input`, using
    /// `clock` for timing, returning the consensus [`RaftOutput`] for the caller
    /// to dispatch.
    ///
    /// Returns `None` only if the metadata group is somehow absent, which never
    /// happens for a controller built through [`MetadataController::new`].
    pub fn step(&mut self, input: RaftInput, clock: &mut impl Clock) -> Option<RaftOutput> {
        self.fleet
            .get_mut(&metadata_group_key())
            .map(|replica| replica.step(input, clock))
    }

    /// Apply a committed [`ClusterCommand`] to the controller's metadata view,
    /// bumping `epoch` (Requirement 3.1, 9.3).
    ///
    /// Returns the new `epoch` after the change, which identifies the metadata
    /// version the server then propagates and collects acks against.
    pub fn apply(&mut self, command: &ClusterCommand) -> u64 {
        apply_command(&mut self.metadata, command);
        self.metadata.epoch
    }

    /// Record that `node` acknowledged the propagated metadata at version
    /// `epoch` (Requirement 2.8, 3.5).
    ///
    /// Acks are modelled as the set of node ids that acknowledged a given
    /// epoch; recording the same `(epoch, node)` twice is idempotent.
    pub fn record_ack(&mut self, epoch: u64, node: NodeId) {
        self.acks.entry(epoch).or_default().insert(node);
    }

    /// The set of node ids that have acknowledged metadata version `epoch`.
    pub fn acked(&self, epoch: u64) -> BTreeSet<NodeId> {
        self.acks.get(&epoch).cloned().unwrap_or_default()
    }

    /// The `reachable` nodes that have **not** acknowledged metadata version
    /// `epoch`, in the order they appear in `reachable` (Requirement 3.6).
    ///
    /// `reachable` is the set of nodes the server attempted to propagate to
    /// (every reachable cluster node); a node missing from the recorded acks is
    /// a laggard.
    pub fn laggards(&self, epoch: u64, reachable: &[NodeId]) -> Vec<NodeId> {
        let acked = self.acks.get(&epoch);
        reachable
            .iter()
            .filter(|node| !acked.is_some_and(|set| set.contains(node)))
            .cloned()
            .collect()
    }

    /// Confirm that every `reachable` node acknowledged metadata version
    /// `epoch` before the propagation deadline, as required on a topic delete
    /// (Requirement 2.8, 3.5, 3.6).
    ///
    /// Returns `Ok(())` when all reachable nodes have acked. Otherwise returns
    /// [`CoreError::MetadataPropagation`] carrying the laggards — the nodes that
    /// did not acknowledge — while the deletion recorded on the nodes that did
    /// ack is retained (the controller's own metadata already reflects the
    /// delete). The wall-clock deadline of [`METADATA_PROPAGATION_TIMEOUT_MS`]
    /// is enforced by the server before calling this; the controller only
    /// decides who is missing from the acks gathered in time.
    pub fn confirm_delete_propagation(
        &self,
        epoch: u64,
        reachable: &[NodeId],
    ) -> Result<(), CoreError> {
        let laggards = self.laggards(epoch, reachable);
        if laggards.is_empty() {
            Ok(())
        } else {
            Err(CoreError::MetadataPropagation(laggards))
        }
    }

    /// Resolve the current leader of `(topic, partition)` (Requirement 10.4).
    ///
    /// Returns the [`NodeId`] currently leading the partition's Raft group when
    /// one is established. The error cases:
    ///
    /// - [`CoreError::PartitionNotFound`] if the topic does not exist or has no
    ///   partition with the given index (Requirement 10.5; the dedicated edge
    ///   tests are task 12.3).
    /// - [`CoreError::PartitionUnavailable`] if the partition exists but has no
    ///   current leader because an election is in progress (Requirement 10.6;
    ///   edge tests are task 12.3).
    pub fn find_leader(&self, topic: &str, partition: PartitionIndex) -> Result<NodeId, CoreError> {
        let partition = self.partition(topic, partition)?;
        partition
            .leader
            .clone()
            .ok_or(CoreError::PartitionUnavailable)
    }

    /// Borrow the partition `(topic, partition)` from the metadata, or
    /// [`CoreError::PartitionNotFound`] if the topic or partition is absent.
    fn partition(&self, topic: &str, partition: PartitionIndex) -> Result<&Partition, CoreError> {
        self.metadata
            .topics
            .get(topic)
            .and_then(|t| t.partitions.iter().find(|p| p.index == partition))
            .ok_or_else(|| CoreError::PartitionNotFound {
                topic: topic.to_string(),
                index: partition.0,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LogBackend, Member};

    fn member(id: &str, availability: NodeAvailability) -> Member {
        Member {
            id: NodeId::new(id),
            addr: format!("{id}:7001"),
            availability,
        }
    }

    fn partition(index: u32, leader: Option<&str>) -> Partition {
        Partition {
            index: PartitionIndex(index),
            replicas: vec![NodeId::new("a"), NodeId::new("b")],
            leader: leader.map(NodeId::new),
        }
    }

    // --- Availability tracking (Requirement 9.3) ----------------------------

    #[test]
    fn availability_set_and_get_is_two_state() {
        let mut meta = ClusterMetadata::new();
        meta.members = vec![member("node-a", NodeAvailability::Available)];

        // Get reflects the initial state.
        assert_eq!(
            meta.availability(&NodeId::new("node-a")),
            Some(NodeAvailability::Available)
        );

        // Set flips to the other of the two states and get reflects it.
        assert!(meta.set_availability(&NodeId::new("node-a"), NodeAvailability::Unavailable));
        assert_eq!(
            meta.availability(&NodeId::new("node-a")),
            Some(NodeAvailability::Unavailable)
        );

        // And back again — there are exactly two states.
        assert!(meta.set_availability(&NodeId::new("node-a"), NodeAvailability::Available));
        assert_eq!(
            meta.availability(&NodeId::new("node-a")),
            Some(NodeAvailability::Available)
        );
    }

    #[test]
    fn availability_of_unknown_node_is_none_and_set_is_a_no_op() {
        let mut meta = ClusterMetadata::new();
        meta.members = vec![member("node-a", NodeAvailability::Available)];

        assert_eq!(meta.availability(&NodeId::new("ghost")), None);
        // Setting an unknown node neither updates nor adds a member.
        assert!(!meta.set_availability(&NodeId::new("ghost"), NodeAvailability::Unavailable));
        assert_eq!(meta.members.len(), 1);
        assert_eq!(meta.availability(&NodeId::new("ghost")), None);
    }

    // --- apply_command for each variant (Requirement 3.1, 9.3) --------------

    #[test]
    fn apply_create_topic_inserts_active_topic_and_bumps_epoch() {
        let mut meta = ClusterMetadata::new();
        let command = ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![partition(0, Some("a")), partition(1, Some("b"))],
            backend: LogBackend::Durable,
        };

        apply_command(&mut meta, &command);

        assert!(meta.topics.contains_key("orders"));
        let topic = &meta.topics["orders"];
        assert_eq!(topic.state, TopicState::Active);
        assert_eq!(topic.partitions.len(), 2);
        assert_eq!(meta.epoch, 1);
    }

    #[test]
    fn apply_delete_topic_removes_topic_and_bumps_epoch() {
        let mut meta = ClusterMetadata::new();
        apply_command(
            &mut meta,
            &ClusterCommand::CreateTopic {
                name: "orders".to_string(),
                partitions: vec![partition(0, Some("a"))],
                backend: LogBackend::Durable,
            },
        );
        assert_eq!(meta.epoch, 1);

        apply_command(
            &mut meta,
            &ClusterCommand::DeleteTopic {
                name: "orders".to_string(),
            },
        );

        assert!(!meta.topics.contains_key("orders"));
        assert_eq!(meta.epoch, 2);
    }

    #[test]
    fn apply_set_availability_updates_member_and_bumps_epoch() {
        let mut meta = ClusterMetadata::new();
        meta.members = vec![member("node-a", NodeAvailability::Available)];

        apply_command(
            &mut meta,
            &ClusterCommand::SetAvailability {
                node: NodeId::new("node-a"),
                availability: NodeAvailability::Unavailable,
            },
        );

        assert_eq!(
            meta.availability(&NodeId::new("node-a")),
            Some(NodeAvailability::Unavailable)
        );
        assert_eq!(meta.epoch, 1);
    }

    // --- Controller hosts the dedicated metadata group ----------------------

    #[test]
    fn controller_hosts_the_metadata_raft_group() {
        let controller = MetadataController::new(RaftNodeId(0), vec![RaftNodeId(1), RaftNodeId(2)]);
        assert!(controller.hosts_metadata_group());
        assert_eq!(
            metadata_group_key(),
            ("__meta".to_string(), PartitionIndex(0))
        );
    }

    #[test]
    fn controller_apply_returns_new_epoch_and_mutates_view() {
        let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
        let epoch = controller.apply(&ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![partition(0, Some("a"))],
            backend: LogBackend::Durable,
        });
        assert_eq!(epoch, 1);
        assert!(controller.metadata().topics.contains_key("orders"));
    }

    // --- Propagation laggard reporting (Requirement 2.8, 3.5, 3.6) ----------

    #[test]
    fn confirm_delete_propagation_ok_when_all_reachable_nodes_ack() {
        let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
        let epoch = controller.apply(&ClusterCommand::DeleteTopic {
            name: "orders".to_string(),
        });
        let reachable = vec![NodeId::new("node-b"), NodeId::new("node-c")];

        controller.record_ack(epoch, NodeId::new("node-b"));
        controller.record_ack(epoch, NodeId::new("node-c"));

        assert_eq!(
            controller.confirm_delete_propagation(epoch, &reachable),
            Ok(())
        );
    }

    #[test]
    fn confirm_delete_propagation_reports_laggards_when_some_do_not_ack() {
        let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
        let epoch = controller.apply(&ClusterCommand::DeleteTopic {
            name: "orders".to_string(),
        });
        let reachable = vec![
            NodeId::new("node-b"),
            NodeId::new("node-c"),
            NodeId::new("node-d"),
        ];

        // Only node-b acks in time; node-c and node-d are laggards.
        controller.record_ack(epoch, NodeId::new("node-b"));

        assert_eq!(
            controller.confirm_delete_propagation(epoch, &reachable),
            Err(CoreError::MetadataPropagation(vec![
                NodeId::new("node-c"),
                NodeId::new("node-d"),
            ]))
        );
    }

    #[test]
    fn laggards_are_scoped_to_the_specific_epoch() {
        let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
        let reachable = vec![NodeId::new("node-b")];

        // An ack for epoch 1 does not satisfy a check for epoch 2.
        controller.record_ack(1, NodeId::new("node-b"));
        assert_eq!(controller.acked(1), BTreeSet::from([NodeId::new("node-b")]));
        assert_eq!(
            controller.confirm_delete_propagation(2, &reachable),
            Err(CoreError::MetadataPropagation(vec![NodeId::new("node-b")]))
        );
    }

    // --- FindLeader resolution (Requirement 10.4) ---------------------------

    #[test]
    fn find_leader_returns_established_leader() {
        let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
        controller.apply(&ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![partition(0, Some("node-a")), partition(1, Some("node-b"))],
            backend: LogBackend::Durable,
        });

        assert_eq!(
            controller.find_leader("orders", PartitionIndex(1)),
            Ok(NodeId::new("node-b"))
        );
    }

    #[test]
    fn find_leader_missing_topic_or_partition_is_not_found() {
        let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
        controller.apply(&ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![partition(0, Some("node-a"))],
            backend: LogBackend::Durable,
        });

        // Unknown topic.
        assert_eq!(
            controller.find_leader("ghost", PartitionIndex(0)),
            Err(CoreError::PartitionNotFound {
                topic: "ghost".to_string(),
                index: 0,
            })
        );
        // Known topic, out-of-range partition.
        assert_eq!(
            controller.find_leader("orders", PartitionIndex(5)),
            Err(CoreError::PartitionNotFound {
                topic: "orders".to_string(),
                index: 5,
            })
        );
    }

    #[test]
    fn find_leader_without_a_leader_is_unavailable() {
        let mut controller = MetadataController::new(RaftNodeId(0), Vec::new());
        controller.apply(&ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![partition(0, None)],
            backend: LogBackend::Durable,
        });

        assert_eq!(
            controller.find_leader("orders", PartitionIndex(0)),
            Err(CoreError::PartitionUnavailable)
        );
    }
}
