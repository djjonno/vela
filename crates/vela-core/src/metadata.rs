//! Cluster metadata management: availability, the metadata Raft group, and
//! propagation tracking.
//!
//! `ClusterMetadata` (topics, partition/replica assignments, leaders, and
//! membership) must itself be agreed across the cluster. Following the design's
//! **Option A — a dedicated metadata Raft group**, this module runs a single,
//! well-known control group keyed `("__meta", PartitionIndex(0))` (the
//! `__meta/p0` group). `ClusterCommand`s are entries in that group's log; once
//! committed, [`apply_command`] folds the change into a [`ClusterMetadata`].
//! Agreement and propagation are owned entirely by the metadata Raft group's
//! own log replication and commit semantics — committed entries reach every
//! node via `AppendEntries`, not a separate acknowledgement or snapshot-push
//! protocol.
//!
//! This module owns three concerns:
//!
//! - **Availability tracking** on [`ClusterMetadata`] — each member's
//!   availability is exactly one of [`NodeAvailability::Available`] or
//!   [`NodeAvailability::Unavailable`] (Requirement 9.3).
//! - **The metadata Raft group controller** — [`MetadataController`] hosts the
//!   `__meta/p0` group in a [`RaftGroupFleet`] and applies committed
//!   `ClusterCommand`s to its [`ClusterMetadata`] (Requirement 3.1, 9.3).
//! - **`FindLeader` resolution** — [`MetadataController::find_leader`] resolves
//!   `(topic, partition)` to the current leader (Requirement 10.4).
//!
//! Decoding committed log bytes back into a [`ClusterCommand`] is the server's
//! concern — [`apply_command`] operates on an already-decoded, already-committed
//! command so `vela-core` stays free of the wire encoding.

use std::path::Path;

use vela_log::{DurableWal, LogError, LogStorage, PayloadKind, SyncPolicy, WalConfig};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, RaftOutput, Role};

use crate::fleet::{FleetError, GroupKey, PartitionReplica, RaftGroupFleet};
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

/// Reserved (no longer drives behavior). Cluster metadata is now agreed solely
/// through the dedicated `__meta/0` Raft group and reaches every node via
/// `AppendEntries` (Requirement 1.3); there is no bespoke acknowledgement
/// deadline or `SyncMetadata` push to time against. This constant is retained
/// only as a reserved 5-second value for any future use and no longer
/// participates in metadata agreement.
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
/// bumping its `epoch` as a benign applied-change counter (Requirement 3.1,
/// 9.3).
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
/// Every applied command bumps `epoch`, a benign monotonic counter of how many
/// commands have been applied; it is no longer a propagation-ordering device.
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

/// Manages how [`ClusterMetadata`] is agreed across the cluster (design
/// "Cluster Metadata Management").
///
/// The controller owns the node's view of the cluster metadata and hosts the
/// dedicated `__meta/p0` Raft group in a [`RaftGroupFleet`]. Committed
/// `ClusterCommand`s are applied through [`MetadataController::apply`];
/// agreement and propagation are owned by the metadata group's own Raft log
/// replication and commit semantics, so there is no separate acknowledgement or
/// snapshot-push protocol.
pub struct MetadataController {
    /// The node-local view of cluster metadata, mutated as commands commit.
    metadata: ClusterMetadata,
    /// The fleet hosting the single `__meta/p0` Raft group.
    fleet: RaftGroupFleet,
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
    /// log.
    fn from_parts(metadata: ClusterMetadata, fleet: RaftGroupFleet) -> Self {
        Self { metadata, fleet }
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

    /// Build (recover) the `__meta/0` metadata Raft group over an
    /// **injected** [`PartitionLog`] rather than opening a real-filesystem WAL,
    /// rebuilding the committed cluster catalogue from it.
    ///
    /// Gated behind the non-default `sim` feature. This mirrors
    /// [`recover_durable`](Self::recover_durable) exactly — it creates the
    /// recovered group through [`RaftGroupFleet::create_recovered_group`]
    /// (restoring the Raft hard state and commit index from the recovered log)
    /// and replays every committed [`PayloadKind::Cluster`] entry into a fresh
    /// [`ClusterMetadata`] via [`apply_command`] in ascending index order — but
    /// takes the already-opened `log` by value instead of calling
    /// [`DurableWal::open`] itself.
    ///
    /// The `vela-sim` harness uses this to drive the **production**
    /// `__meta/0` group over a Sim_Storage-backed [`PartitionLog::Sim`]: the
    /// harness opens the WAL over its deterministic in-memory disk and passes
    /// the resulting log here, so recovery runs the real WAL/Raft path while the
    /// underlying disk stays injectable and reproducible (Requirement 3.2). As
    /// with [`recover_durable`], the byte codec is the caller's concern, so the
    /// committed-entry decoder is injected as `decode_cluster`.
    ///
    /// A fresh (empty) injected log recovers an empty catalogue, identical to a
    /// freshly created group — the path a brand-new simulated cluster takes.
    #[cfg(feature = "sim")]
    pub fn recover_durable_with_log(
        node_id: RaftNodeId,
        peers: Vec<RaftNodeId>,
        log: PartitionLog,
        decode_cluster: impl Fn(&[u8]) -> ClusterCommand,
    ) -> Result<Self, MetadataRecoverError> {
        // Create the recovered group from the injected log: this restores the
        // Raft hard state and commit index from the recovered log, exactly as
        // `recover_durable` does after opening the real WAL.
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

    /// Shared, read-only access to the `__meta/0` group's
    /// [`PartitionReplica`](crate::PartitionReplica), or `None` if the metadata
    /// group is somehow absent (which never happens for a controller built
    /// through [`MetadataController::new`] or the recovery constructors).
    ///
    /// The metadata group is a Raft group like any partition's, so exposing its
    /// replica lets an out-of-band, read-only observer (the DST
    /// `Consistency_Checker`) inspect the metadata group's log, term, role, and
    /// committed prefix through the same [`PartitionReplica`] surface it uses
    /// for client partitions, rather than reaching for a bespoke accessor per
    /// field. It grants no mutation and changes no behaviour — the consensus
    /// path drives the group exclusively through
    /// [`step`](MetadataController::step).
    #[must_use]
    pub fn meta_replica(&self) -> Option<&PartitionReplica> {
        self.fleet.get(&metadata_group_key())
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

    /// The role this node currently holds in the metadata group, or `None` if
    /// the group is somehow absent (never for a controller built through
    /// [`MetadataController::new`] or [`MetadataController::recover_durable`]).
    ///
    /// The leader-routed propose path reads this to decide whether to append a
    /// `ClusterCommand` here or redirect the caller to the metadata leader
    /// (Raft §8; Requirement 4.1).
    pub fn role(&self) -> Option<Role> {
        self.fleet.get(&metadata_group_key()).map(|r| r.role())
    }

    /// The metadata replica's known current leader, as a numeric
    /// [`RaftNodeId`], or `None` when it knows of none — it is mid-election or
    /// has just stepped down and not yet heard from a leader (Raft §5.2).
    ///
    /// This is the replica's own id once it has won the metadata election and
    /// the `leader_id` it last accepted from an `AppendEntries` otherwise. The
    /// server maps the numeric id back to the domain node id to use as the
    /// redirect hint when a non-leader receives a topic-admin proposal
    /// (Requirement 4.1, 8.1; Raft §8).
    pub fn leader_id(&self) -> Option<RaftNodeId> {
        self.fleet
            .get(&metadata_group_key())
            .and_then(|r| r.raft().leader_id())
    }

    /// The last index of the metadata group's replicated log, or `None` when the
    /// log is empty — i.e. the index immediately preceding the one a freshly
    /// proposed entry would occupy.
    ///
    /// The propose path uses this to compute the target index a `ClusterCommand`
    /// will land at, so it can await that index committing (Requirement 3.3,
    /// 3.4).
    pub fn last_log_index(&self) -> Option<u64> {
        self.fleet
            .get(&metadata_group_key())
            .and_then(|r| r.raft().log().last_index())
    }

    /// The highest committed index of the metadata group, or `None` if nothing
    /// has committed yet (Raft §5.3).
    ///
    /// The propose path compares this against a proposal's target index to
    /// detect when the entry has committed to a majority (Requirement 3.2, 3.3).
    pub fn commit_index(&self) -> Option<u64> {
        self.fleet
            .get(&metadata_group_key())
            .and_then(|r| r.raft().commit_index())
    }

    /// Apply a committed [`ClusterCommand`] to the controller's metadata view,
    /// bumping `epoch` (Requirement 3.1, 9.3).
    ///
    /// Returns the new `epoch` after the change — a benign applied-change
    /// counter, no longer a propagation-ordering device.
    pub fn apply(&mut self, command: &ClusterCommand) -> u64 {
        apply_command(&mut self.metadata, command);
        self.metadata.epoch
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
            advertised_addr: format!("{id}:7001"),
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

#[cfg(all(test, feature = "sim"))]
mod sim_tests {
    //! Tests for the `sim`-gated [`MetadataController::recover_durable_with_log`]
    //! injection path: recovering the `__meta/0` group over an injected
    //! [`PartitionLog::Sim`] backed by the deterministic fault filesystem.

    use super::*;
    use crate::partition_log::PartitionLog;
    use crate::SimWalClock;
    use vela_log::sim::FaultFileSystem;
    use vela_log::{DurableWal, EntryPayload, LogStorage, SyncPolicy, WalConfig};

    use crate::model::LogBackend;

    /// A minimal, test-owned codec: encodes a `CreateTopic` (tag `0`) carrying
    /// only a length-prefixed name; the harness owns the encoding, so this is
    /// sufficient to exercise the recovery replay path.
    fn encode_create(name: &str) -> Vec<u8> {
        let mut buf = vec![0u8];
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf
    }

    /// The matching decoder injected into `recover_durable_with_log`.
    fn decode(bytes: &[u8]) -> ClusterCommand {
        assert_eq!(bytes[0], 0, "test codec only encodes CreateTopic");
        let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let name = String::from_utf8(bytes[5..5 + len].to_vec()).expect("valid utf8");
        ClusterCommand::CreateTopic {
            name,
            partitions: Vec::new(),
            backend: LogBackend::Durable,
        }
    }

    /// Open a sim WAL over `fs` at `dir` with the consensus-safe `Always` policy.
    fn open_wal(fs: FaultFileSystem, dir: &str) -> DurableWal<FaultFileSystem, SimWalClock> {
        DurableWal::open_with_clock(
            WalConfig::new(dir).with_sync_policy(SyncPolicy::Always),
            fs,
            SimWalClock::new(),
        )
        .expect("open sim WAL")
    }

    #[test]
    fn recover_with_fresh_log_yields_empty_catalogue_hosting_meta_group() {
        // The path a brand-new simulated cluster takes: a fresh injected log
        // recovers an empty catalogue but a fully-formed `__meta/0` group.
        let wal = open_wal(FaultFileSystem::default(), "/sim/meta-fresh");
        let controller = MetadataController::recover_durable_with_log(
            RaftNodeId(0),
            Vec::new(),
            PartitionLog::sim(wal),
            decode,
        )
        .expect("recover over fresh sim log");

        assert!(controller.hosts_metadata_group());
        assert!(controller.metadata().topics.is_empty());
    }

    #[test]
    fn recover_rebuilds_catalogue_from_committed_cluster_entries() {
        // Write two committed `Cluster` entries through the real WAL, drop it,
        // then reopen over the same disk and recover: the catalogue is rebuilt
        // by replaying the committed prefix, exactly as production recovery does.
        let fs = FaultFileSystem::default();
        {
            let mut wal = open_wal(fs.clone(), "/sim/meta-replay");
            wal.append(
                EntryPayload::new(PayloadKind::Cluster, encode_create("orders")),
                1,
            )
            .unwrap();
            wal.append(
                EntryPayload::new(PayloadKind::Cluster, encode_create("events")),
                1,
            )
            .unwrap();
            wal.commit(1).unwrap();
        }

        let wal = open_wal(fs, "/sim/meta-replay");
        let controller = MetadataController::recover_durable_with_log(
            RaftNodeId(0),
            Vec::new(),
            PartitionLog::sim(wal),
            decode,
        )
        .expect("recover over reopened sim log");

        assert!(controller.metadata().topics.contains_key("orders"));
        assert!(controller.metadata().topics.contains_key("events"));
        // Two committed CreateTopic commands were applied.
        assert_eq!(controller.metadata().topics.len(), 2);
    }
}
