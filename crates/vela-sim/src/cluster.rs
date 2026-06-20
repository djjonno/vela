//! `SimulatedCluster` and `SimNode`: composition over production `vela-core`
//! types.
//!
//! This module defines the run-fixed cluster shape and the per-node container
//! the harness drives:
//!
//! - [`Topology`] — the node set, replication factor, and the **fixed**
//!   per-partition [`Replica_Set`]s, all computed once from the
//!   [`ScenarioParameters`] and never mutated for the rest of the run
//!   (Requirement 3.5). It also owns the deterministic domain-id ⇆ numeric
//!   `vela_raft` id mapping the harness uses everywhere it must address a
//!   replica.
//! - [`SimNode`] — one simulated node: the production
//!   [`MetadataController`] for the dedicated `__meta/0` group, a `fleet` of
//!   production [`PartitionReplica`]s keyed by `(topic, partition)`, the served
//!   [`ClusterMetadata`] mirror the reconciler reads, a `running` flag, and the
//!   per-replica [`SimStorageHandle`]s. Every field is a production type, so the
//!   harness drives real consensus / state-machine / metadata logic rather than
//!   a model (Requirement 3.1, 3.2).
//!
//! Scope boundary: task 11.1 *defined* [`Topology`] and [`SimNode`]; this task
//! (11.2) assembles them into a [`SimulatedCluster`] — recovering each
//! [`MetadataController`] via the durable-recovery path
//! ([`MetadataController::recover_durable_with_log`]) over a Sim_Storage-backed
//! [`PartitionLog::Sim`](vela_core::PartitionLog), minting each replica its
//! `__meta/0` [`SimTransport`] from the shared [`SimNetwork`], and holding the
//! shared [`SimClock`]. The topic create/delete reconcile path (11.3) and
//! crash/restart (11.4) build on the structure left here: they populate each
//! node's `fleet` / `served` / `storage` for client partitions.
//!
//! [`Replica_Set`]: crate

use std::collections::{HashMap, HashSet};

use vela_core::{
    apply_command, metadata_group_key, plan_reconcile, ClusterCommand, ClusterMetadata, GroupKey,
    MetadataController, MetadataRecoverError, NodeId, PartitionIndex, PartitionReplica,
};
use vela_log::LogError;
use vela_raft::{NodeId as RaftNodeId, RaftInput, RaftOutput};

use crate::clock::SimClock;
use crate::codec::decode_cluster_command;
use crate::network::{SimNetwork, SimTransport};
use crate::rng::{SeedStreams, SplitMix64};
use crate::scenario::{RunConfig, ScenarioError, ScenarioParameters};
use crate::scheduler::VirtualInstant;
use crate::storage::SimStorageHandle;

/// The domain node id assigned to the node at index `i`: `node-0`, `node-1`, ….
///
/// Node ids are assigned by the harness, so it controls the entire id space;
/// the `node-<index>` form makes the domain-id ⇆ numeric-id mapping a trivial,
/// stable index round-trip (see [`Topology::raft_id`] / [`Topology::domain_id`]).
fn node_id_for_index(index: usize) -> NodeId {
    NodeId::new(format!("node-{index}"))
}

/// The numeric `vela_raft` id for the node at index `i`: simply `RaftNodeId(i)`.
///
/// The production server derives this mapping from the string id with an FNV-1a
/// hash (`vela_server::registry::raft_node_id`), but that helper lives in
/// `vela-server`, which the harness must not depend on (it carries the very
/// `tokio` / gRPC / wall-clock machinery DST replaces). Because the harness
/// *assigns* the node ids, it is free to choose a simpler mapping with the same
/// guarantees the consensus layer needs — a stable, collision-free bijection
/// between a node and its numeric id — and an index-based one is deterministic,
/// trivially reversible, and needs no hashing. The two representations stay
/// distinct exactly as the production seam keeps them distinct (domain string
/// ids vs. the numeric ids Raft keys its per-peer state on); only the *mapping
/// function* differs, and nothing outside the harness observes it.
fn raft_id_for_index(index: usize) -> RaftNodeId {
    RaftNodeId(index as u64)
}

/// The fixed shape of a [`Simulated_Cluster`] for one [`Simulation_Run`]:
/// the node set, the replication factor, and the per-partition
/// [`Replica_Set`]s — all constructed once from the [`ScenarioParameters`] and
/// never mutated thereafter (Requirement 3.5).
///
/// # Replica-set assignment
///
/// Each partition index `p` is assigned a `Replica_Set` of `replication_factor`
/// nodes drawn contiguously from the node set, wrapping round-robin:
/// `{ node[(p + k) mod node_count] : k in 0..replication_factor }`. The
/// assignment is a pure function of the partition index, so every topic shares
/// the same per-index layout and a partition's `Replica_Set` is identical on
/// every node and for the whole run. Because `replication_factor` is validated
/// to be in `1..=node_count` before a run, each set holds exactly
/// `replication_factor` *distinct* nodes.
///
/// The dedicated metadata group `__meta/0` is **not** a client partition: it is
/// replicated by *every* node (it is the cluster-wide control group), so
/// [`replica_set_for_group`](Self::replica_set_for_group) returns the full node
/// set for it.
///
/// [`Simulated_Cluster`]: crate
/// [`Simulation_Run`]: crate
/// [`Replica_Set`]: crate
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    /// The cluster's node ids, indexed by node index (`nodes[i]` is `node-i`).
    nodes: Vec<NodeId>,
    /// The number of replicas per client partition (in `1..=nodes.len()`).
    replication_factor: usize,
    /// The number of partitions every topic is created with.
    partition_count: u32,
    /// The fixed `Replica_Set` for each partition index `0..partition_count`,
    /// precomputed so it is built once and never recomputed or mutated
    /// (Requirement 3.5).
    replica_sets: Vec<Vec<NodeId>>,
}

impl Topology {
    /// Build the run-fixed topology from `params`.
    ///
    /// Assigns `node-0 .. node-(node_count-1)` as the node set and precomputes
    /// the contiguous round-robin `Replica_Set` for every partition index
    /// `0..partition_count` (see the type-level docs). The result is immutable:
    /// the harness holds it for the whole run and never mutates it
    /// (Requirement 3.5).
    ///
    /// `params` is expected to have passed
    /// [`ScenarioParameters::validate`](crate::scenario::ScenarioParameters::validate)
    /// (`replication_factor` in `1..=node_count`, `partition_count >= 1`); under
    /// that contract every assigned `Replica_Set` holds exactly
    /// `replication_factor` distinct nodes.
    #[must_use]
    pub fn from_params(params: &ScenarioParameters) -> Self {
        let nodes: Vec<NodeId> = (0..params.node_count).map(node_id_for_index).collect();
        let replication_factor = params.replication_factor;
        let partition_count = params.partition_count;

        // Precompute the fixed replica set for each partition index once.
        let node_count = nodes.len();
        let replica_sets: Vec<Vec<NodeId>> = (0..partition_count)
            .map(|p| {
                (0..replication_factor)
                    .map(|k| {
                        // Contiguous, wrapping round-robin from the partition
                        // index. `node_count` is non-zero whenever
                        // `replication_factor >= 1`, which validation enforces.
                        let idx = (p as usize + k) % node_count;
                        nodes[idx].clone()
                    })
                    .collect()
            })
            .collect();

        Self {
            nodes,
            replication_factor,
            partition_count,
            replica_sets,
        }
    }

    /// The cluster's node ids, in index order (`node-0`, `node-1`, …).
    #[must_use]
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// The number of nodes in the cluster.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// The replication factor (replicas per client partition).
    #[must_use]
    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    /// The number of partitions every topic is created with.
    #[must_use]
    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }

    /// The fixed `Replica_Set` for a client `partition`, or `None` if the index
    /// is `>= partition_count`.
    ///
    /// The returned set is the precomputed, never-mutated assignment
    /// (Requirement 3.5).
    #[must_use]
    pub fn replica_set_for(&self, partition: PartitionIndex) -> Option<&[NodeId]> {
        self.replica_sets
            .get(partition.0 as usize)
            .map(Vec::as_slice)
    }

    /// The `Replica_Set` for a Raft `group`.
    ///
    /// The dedicated metadata group `__meta/0` is replicated by *every* node, so
    /// this returns the full node set for it; for a client partition it returns
    /// that partition's fixed `Replica_Set` (or `None` if the partition index is
    /// out of range). The topic name does not affect a client partition's
    /// assignment — only the partition index does — so every topic shares the
    /// same per-index layout.
    #[must_use]
    pub fn replica_set_for_group(&self, group: &GroupKey) -> Option<&[NodeId]> {
        if group == &metadata_group_key() {
            Some(&self.nodes)
        } else {
            self.replica_set_for(group.1)
        }
    }

    /// Whether `node` is in the `Replica_Set` of `group`.
    #[must_use]
    pub fn group_contains(&self, group: &GroupKey, node: &NodeId) -> bool {
        self.replica_set_for_group(group)
            .is_some_and(|set| set.contains(node))
    }

    /// The numeric `vela_raft` id for `node`, or `None` if `node` is not a
    /// member of this topology.
    ///
    /// The inverse of [`domain_id`](Self::domain_id); together they are the
    /// deterministic domain-id ⇆ numeric-id bijection the harness uses to
    /// address replicas and to map a replica's reported leader id back to a
    /// domain node id.
    #[must_use]
    pub fn raft_id(&self, node: &NodeId) -> Option<RaftNodeId> {
        self.nodes
            .iter()
            .position(|n| n == node)
            .map(raft_id_for_index)
    }

    /// The domain [`NodeId`] for a numeric `raft` id, or `None` if it does not
    /// correspond to a node in this topology.
    ///
    /// The inverse of [`raft_id`](Self::raft_id).
    #[must_use]
    pub fn domain_id(&self, raft: RaftNodeId) -> Option<&NodeId> {
        self.nodes.get(raft.0 as usize)
    }

    /// The numeric `vela_raft` peer ids for `node` within `group`: every replica
    /// in the group's `Replica_Set` **except** `node` itself.
    ///
    /// This is the `peers` list a [`RaftNode`](vela_raft::RaftNode) (and thus a
    /// [`PartitionReplica`] or [`MetadataController`]) is constructed with — the
    /// other voters of the group. Returns an empty vector if `node` is not in
    /// the group's `Replica_Set` (it then has no peers there). The ids are
    /// returned in node-index order for determinism.
    #[must_use]
    pub fn peers_for(&self, node: &NodeId, group: &GroupKey) -> Vec<RaftNodeId> {
        let Some(set) = self.replica_set_for_group(group) else {
            return Vec::new();
        };
        if !set.contains(node) {
            return Vec::new();
        }
        set.iter()
            .filter(|peer| *peer != node)
            .filter_map(|peer| self.raft_id(peer))
            .collect()
    }
}

/// One simulated Vela node, composed entirely of production `vela-core` types.
///
/// A `SimNode` plays the role `vela-server`'s `node.rs` plays in production,
/// minus the async glue: it hosts the dedicated `__meta/0` metadata group and
/// the fleet of partition replicas this node is assigned, mirrors the served
/// catalogue the reconciler reads, and owns the backing disks so a crash can
/// drop volatile state and a restart can reopen the same storage.
///
/// Fields are `pub(crate)` so the cluster-composition, reconcile, and
/// crash/restart tasks (11.2–11.4) that share this module can populate and
/// mutate them directly; external callers use the read accessors.
pub struct SimNode {
    /// The node's stable domain id (`node-i`).
    pub(crate) id: NodeId,
    /// The node's numeric `vela_raft` id (`RaftNodeId(i)`), cached so the step
    /// loop need not re-resolve it through the [`Topology`] on every event.
    pub(crate) raft_id: RaftNodeId,
    /// Whether the node is currently up. Cleared on a `Node_Crash` and set on a
    /// `Node_Restart` (Requirement 6.1, 6.3); a crashed node processes no events
    /// and is cut from the network.
    pub(crate) running: bool,
    /// The production controller for the dedicated `__meta/0` group, or `None`
    /// while the node is crashed.
    ///
    /// The controller, the [`fleet`](Self::fleet), and the
    /// [`transports`](Self::transports) are the node's **volatile** consensus
    /// state: a `Node_Crash` drops all three together (the live `MetadataController`
    /// and `PartitionReplica`s, with their un-fsynced log tails, are lost), so
    /// the controller is absent exactly when `running` is `false`
    /// (Requirement 6.1). A `Node_Restart` rebuilds the controller from the
    /// retained backing disk via
    /// [`MetadataController::recover_durable_with_log`] (Requirement 6.3). Built
    /// initially by the cluster-composition task (11.2).
    pub(crate) controller: Option<MetadataController>,
    /// The node's fleet of partition replicas, keyed by `(topic, partition)`.
    /// A replica is started for every partition whose `Replica_Set` contains
    /// this node when its topic is created (the reconcile path, task 11.3), and
    /// the `__meta/0` group is **never** in this map — it lives in
    /// [`controller`](Self::controller).
    pub(crate) fleet: HashMap<GroupKey, PartitionReplica>,
    /// The served [`ClusterMetadata`] mirror this node's reconciler reads — the
    /// catalogue produced by applying the committed `__meta/0` log.
    pub(crate) served: ClusterMetadata,
    /// The backing Sim_Storage handle for each hosted replica (including
    /// `__meta/0`), so a crash can drop the live WAL handle while the durable
    /// bytes survive and a restart can reopen the same disk for recovery.
    ///
    /// The [`SimulatedCluster`] assembly (11.2) inserts each node's `__meta/0`
    /// handle here; the reconcile / crash-restart paths (11.3–11.4) extend it
    /// with the client partitions' handles.
    pub(crate) storage: HashMap<GroupKey, SimStorageHandle>,
    /// The per-partition [`SimTransport`] this node dispatches a replica's
    /// outbound Raft messages through, keyed by `(topic, partition)`.
    ///
    /// A handle is minted from the shared [`SimNetwork`] when the reconcile path
    /// (task 11.3) starts a partition replica, and dropped when it stops the
    /// replica, so the map's keys track the node's `fleet` exactly. The
    /// `__meta/0` transport is **not** here — it lives on the
    /// [`SimulatedCluster`] in `meta_transports` — so this map holds only client
    /// partitions, mirroring how [`fleet`](Self::fleet) excludes the metadata
    /// group.
    pub(crate) transports: HashMap<GroupKey, SimTransport>,
}

impl SimNode {
    /// Create a running node around an already-built metadata `controller`.
    ///
    /// The controller is injected rather than built here so the
    /// cluster-composition task (11.2) owns *how* the `__meta/0` group is
    /// constructed (durable recovery over a Sim_Storage WAL). The node starts
    /// `running` with an empty `fleet`, an empty served catalogue, and no
    /// per-replica storage handles; the durable-recovery and reconcile paths
    /// (11.2–11.4) populate those.
    ///
    /// Called by the [`SimulatedCluster`] assembly in task 11.2.
    pub(crate) fn new(id: NodeId, raft_id: RaftNodeId, controller: MetadataController) -> Self {
        Self {
            id,
            raft_id,
            running: true,
            controller: Some(controller),
            fleet: HashMap::new(),
            served: ClusterMetadata::new(),
            storage: HashMap::new(),
            transports: HashMap::new(),
        }
    }

    /// The node's stable domain id.
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }

    /// The node's numeric `vela_raft` id.
    #[must_use]
    pub fn raft_id(&self) -> RaftNodeId {
        self.raft_id
    }

    /// Whether the node is currently up.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Shared, read-only access to the node's metadata controller, or `None`
    /// while the node is crashed (its volatile consensus state has been dropped
    /// and not yet recovered by a `Node_Restart`).
    #[must_use]
    pub fn controller(&self) -> Option<&MetadataController> {
        self.controller.as_ref()
    }

    /// Shared, read-only access to the node's served catalogue mirror.
    #[must_use]
    pub fn served(&self) -> &ClusterMetadata {
        &self.served
    }

    /// The number of partition replicas in the node's fleet (excluding the
    /// `__meta/0` group, which the controller owns).
    #[must_use]
    pub fn fleet_len(&self) -> usize {
        self.fleet.len()
    }

    /// Iterate the node's hosted partition replicas as `(group, replica)` pairs,
    /// excluding the `__meta/0` group (which the [`controller`](Self::controller)
    /// owns — reach it via [`MetadataController::meta_replica`]).
    ///
    /// A read-only window the DST `Consistency_Checker` uses to observe every
    /// client-partition replica on the node (its log, term, role, and committed
    /// prefix via [`PartitionReplica`]) without the checker needing access to
    /// the crate-private `fleet` map. Iteration order follows the underlying
    /// `HashMap` and so is unspecified; the checker treats the replicas as a set
    /// keyed by group, so order does not affect any decision it makes.
    pub fn fleet_replicas(&self) -> impl Iterator<Item = (&GroupKey, &PartitionReplica)> {
        self.fleet.iter()
    }

    /// The [`SimTransport`] this node dispatches `group`'s outbound Raft
    /// messages through, or `None` if the node hosts no replica for `group`.
    ///
    /// Covers only client-partition groups (minted by the reconcile spawn,
    /// task 11.3); the `__meta/0` transport is held on the
    /// [`SimulatedCluster`] and reached through
    /// [`SimulatedCluster::meta_transport`] (or the unified
    /// [`SimulatedCluster::transport_for`]).
    #[must_use]
    pub fn transport(&self, group: &GroupKey) -> Option<&SimTransport> {
        self.transports.get(group)
    }
}

/// An error raised while assembling a [`SimulatedCluster`] (Requirement 3.1,
/// 3.2).
///
/// Assembly is fallible at three points, each surfaced as a typed variant
/// rather than a panic so a caller can refuse to start an invalid or
/// unrecoverable run: validating the scenario parameters, opening a replica's
/// Sim_Storage WAL, and recovering the `__meta/0` group over the injected log.
#[derive(thiserror::Error, Debug)]
pub enum ClusterError {
    /// The [`ScenarioParameters`] were internally inconsistent (e.g. a
    /// replication factor outside `1..=node_count`); rejected before any node
    /// is built (Requirement 3.5, 15.5).
    #[error("invalid scenario parameters: {0}")]
    Scenario(#[from] ScenarioError),
    /// Opening a replica's Sim_Storage-backed WAL failed.
    #[error("failed to open a simulated replica log: {0}")]
    Storage(#[from] LogError),
    /// Recovering a node's `__meta/0` metadata group over its injected log
    /// failed.
    #[error("failed to recover the metadata group: {0}")]
    Recover(#[from] MetadataRecoverError),
}

/// The in-process set of [`SimNode`]s that together form one Vela cluster for a
/// [`Simulation_Run`] (Requirement 3.1).
///
/// `SimulatedCluster` owns the run-fixed [`Topology`], every [`SimNode`] indexed
/// by node id, and the shared deterministic seams the harness drives the
/// production consensus code through: the [`SimClock`] (Virtual_Clock), the
/// [`SimNetwork`] bus (Transport), and — held on each node — the
/// [`SimStorageHandle`]s (LogStorage). Every node's `__meta/0`
/// [`MetadataController`] is the production type, recovered via the
/// durable-recovery path over a [`PartitionLog::Sim`](vela_core::PartitionLog)
/// so the harness exercises real WAL recovery and Raft logic rather than a model
/// (Requirement 3.2).
///
/// # What 11.2 assembles
///
/// For each node the constructor builds its `__meta/0` group over a fresh
/// Sim_Storage disk (recovering an empty catalogue, the path a brand-new cluster
/// takes), mints that replica's `__meta/0` [`SimTransport`] over the shared
/// [`SimNetwork`] with the group's `raft id -> domain id` peer map, and stores
/// the `__meta/0` [`SimStorageHandle`] on the node. Client-partition fleets, the
/// served-catalogue reconcile path, and crash/restart are layered on by tasks
/// 11.3–11.4; the structure here leaves each node's `fleet` / `served` /
/// `storage` ready for them to extend.
///
/// The seed-derived `storage` / `faults` / `workload` / `tiebreak` streams are
/// retained for those later tasks; the `election` and `network` streams are
/// owned by the [`SimClock`] and [`SimNetwork`] respectively.
///
/// [`Simulation_Run`]: crate
pub struct SimulatedCluster {
    /// Every node, indexed by node id (`nodes[i]` is `node-i`).
    nodes: Vec<SimNode>,
    /// The run-fixed topology (node set, replication factor, Replica_Sets).
    topology: Topology,
    /// The shared deterministic message bus every replica sends through.
    network: SimNetwork,
    /// The shared Virtual_Clock every replica arms timers against.
    clock: SimClock,
    /// Each node's `__meta/0` transport, minted from [`network`](Self::network)
    /// and indexed by node id, so the step loop can dispatch a node's metadata
    /// sends without re-deriving its peer map.
    meta_transports: Vec<SimTransport>,
    /// The `storage` seed stream (torn-tail / I/O-error selection), retained for
    /// the storage-fault arming the reconcile / crash-restart tasks wire up.
    #[allow(dead_code)]
    storage_stream: SplitMix64,
    /// The `faults` seed stream (crash / restart / partition / skew schedule),
    /// retained for the fault-schedule task.
    #[allow(dead_code)]
    faults_stream: SplitMix64,
    /// The `workload` seed stream (op kind, routing, key/value), retained for the
    /// workload generator.
    #[allow(dead_code)]
    workload_stream: SplitMix64,
    /// The `tiebreak` seed stream (ordering of simultaneous events), handed to
    /// the [`Scheduler`](crate::scheduler::Scheduler) the
    /// [`SimRuntime`](crate::runtime::SimRuntime) drives this cluster with (see
    /// [`tiebreak_stream`](Self::tiebreak_stream)).
    tiebreak_stream: SplitMix64,
}

/// The `raft id -> domain id` peer map for `node`'s `group`: every other node
/// in the group's [`Replica_Set`].
///
/// This is the map a [`SimTransport`] resolves a numeric destination through.
/// `node` itself is excluded — Raft never sends to itself — matching the
/// per-replica peer set a [`MetadataController`] / [`PartitionReplica`] is
/// constructed with (the same set [`Topology::peers_for`] returns, as a numeric
/// `raft id -> domain id` map rather than a bare id list). It generalizes over
/// any group: the cluster-wide `__meta/0` group resolves to every other node,
/// and a client partition to the other members of its fixed `Replica_Set`.
///
/// [`Replica_Set`]: crate
fn peers_map_for(
    topology: &Topology,
    node: &NodeId,
    group: &GroupKey,
) -> HashMap<RaftNodeId, NodeId> {
    topology
        .replica_set_for_group(group)
        .unwrap_or(&[])
        .iter()
        .filter(|peer| *peer != node)
        .filter_map(|peer| topology.raft_id(peer).map(|rid| (rid, peer.clone())))
        .collect()
}

impl SimulatedCluster {
    /// Assemble a cluster from `config` (a seed plus [`ScenarioParameters`]).
    ///
    /// Validates the parameters (Requirement 15.5), builds the run-fixed
    /// [`Topology`] (Requirement 3.5), constructs the shared [`SimClock`] and
    /// [`SimNetwork`] from the run's `election` / `network` seed streams, and
    /// builds every node: each node's `__meta/0` [`MetadataController`] is
    /// recovered through [`MetadataController::recover_durable_with_log`] over a
    /// Sim_Storage-backed [`PartitionLog::Sim`](vela_core::PartitionLog)
    /// (Requirement 3.1, 3.2), its `__meta/0` [`SimTransport`] is minted from the
    /// shared network, and its `__meta/0` [`SimStorageHandle`] is stored on the
    /// node.
    ///
    /// Because each node starts on a fresh in-memory disk, recovery yields an
    /// empty catalogue — equivalent to creating the group fresh — which is the
    /// correct starting state for a new cluster; non-empty recovery is exercised
    /// by the crash/restart task (11.4).
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Scenario`] if the parameters are invalid (before
    /// any node is built), [`ClusterError::Storage`] if a replica's WAL cannot be
    /// opened, or [`ClusterError::Recover`] if a `__meta/0` group cannot be
    /// recovered.
    pub fn new(config: RunConfig) -> Result<Self, ClusterError> {
        let RunConfig { seed, params } = config;
        // Reject an internally inconsistent parameter set before building
        // anything (Requirement 15.5).
        params.validate()?;

        let topology = Topology::from_params(&params);

        // Derive the per-subsystem streams once. The clock and network own the
        // `election` / `network` streams; the rest are retained for later tasks.
        let streams = SeedStreams::new(seed);
        let clock = SimClock::new(streams.election.clone());
        let network = SimNetwork::new(&params.faults, streams.network.clone());

        let meta_group = metadata_group_key();
        let mut nodes = Vec::with_capacity(topology.node_count());
        let mut meta_transports = Vec::with_capacity(topology.node_count());

        for node in topology.nodes() {
            let id = node.clone();
            let raft_id = topology
                .raft_id(&id)
                .expect("a topology node always has a raft id");
            // The metadata group's peers are every other node (it is replicated
            // cluster-wide), in node-index order for determinism.
            let peers = topology.peers_for(&id, &meta_group);

            // Build `__meta/0` over a fresh Sim_Storage disk via the real WAL
            // recovery path: open the durable log as a `PartitionLog::Sim`, then
            // recover the (empty) catalogue and the durable group from it.
            let storage = SimStorageHandle::new(&id, &meta_group);
            let log = storage.open_partition_log()?;
            let controller = MetadataController::recover_durable_with_log(
                raft_id,
                peers,
                log,
                decode_cluster_command,
            )?;

            // Mint this replica's `__meta/0` transport over the shared bus,
            // resolving destinations through the group's `raft id -> domain id`
            // peer map.
            let peers_map = peers_map_for(&topology, &id, &meta_group);
            meta_transports.push(network.transport(id.clone(), meta_group.clone(), peers_map));

            // The node holds its own `__meta/0` storage handle so a later crash
            // can drop the live WAL and a restart can reopen the same disk.
            let mut sim_node = SimNode::new(id, raft_id, controller);
            sim_node.storage.insert(meta_group.clone(), storage);
            nodes.push(sim_node);
        }

        Ok(Self {
            nodes,
            topology,
            network,
            clock,
            meta_transports,
            storage_stream: streams.storage,
            faults_stream: streams.faults,
            workload_stream: streams.workload,
            tiebreak_stream: streams.tiebreak,
        })
    }

    /// The run-fixed [`Topology`] (node set, replication factor, Replica_Sets).
    #[must_use]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// The shared deterministic [`SimNetwork`] every replica sends through.
    #[must_use]
    pub fn network(&self) -> &SimNetwork {
        &self.network
    }

    /// The shared [`SimClock`] every replica arms timers against.
    #[must_use]
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// Mutable access to the shared [`SimClock`], for the step loop to mirror
    /// the scheduler instant and `set_active` the replica it is about to step.
    pub fn clock_mut(&mut self) -> &mut SimClock {
        &mut self.clock
    }

    /// A clone of the run's `tiebreak` RNG stream, for the
    /// [`Scheduler`](crate::scheduler::Scheduler) that orders simultaneous
    /// events.
    ///
    /// The cluster never draws from this stream itself — it owns it only so the
    /// [`SimRuntime`](crate::runtime::SimRuntime) can build its scheduler from
    /// the same seed-derived stream as the rest of the run. Cloning hands the
    /// scheduler the stream at its initial (post-derivation) state, exactly as
    /// if it had been moved, so the tie-break order stays a pure function of the
    /// run seed (Requirement 1.5).
    #[must_use]
    pub fn tiebreak_stream(&self) -> SplitMix64 {
        self.tiebreak_stream.clone()
    }

    /// Drive the replica for `group` on the node at `index` one step with
    /// `input`, returning the production [`RaftOutput`] for the caller to act on
    /// (Requirement 3.2).
    ///
    /// This is the single entry point the [`SimRuntime`](crate::runtime::SimRuntime)
    /// step loop feeds a [`RaftInput`] through. It exists on the cluster — rather
    /// than the runtime reaching in — because stepping a replica needs the
    /// node's `controller`/`fleet` **and** the shared [`SimClock`] borrowed
    /// together; the cluster owns both as distinct fields and can split the
    /// borrow soundly, whereas separate `nodes_mut()` / `clock_mut()` accessors
    /// could not be held at once.
    ///
    /// The method mirrors the scheduler instant `now` into the clock and
    /// attributes any timers the step arms to `(node, group)` via
    /// [`SimClock::set_active`], so they fire relative to the correct instant and
    /// for the right replica. The metadata group `__meta/0` is driven through the
    /// node's [`MetadataController`]; every other group through the matching
    /// [`PartitionReplica`] in the node's `fleet` — whose
    /// [`step`](PartitionReplica::step) already folds newly committed entries
    /// into its state machine, assigning record offsets on commit.
    ///
    /// Returns `None` — stepping nothing — when `index` is out of range, the node
    /// is crashed (`!running`, so it processes no events, Requirement 6.1), or the
    /// node hosts no replica for `group` (e.g. a message for a partition whose
    /// replica has been stopped). The caller still owns dispatching
    /// `out.sends` and draining the armed timers; this performs no I/O.
    pub fn step_replica(
        &mut self,
        index: usize,
        group: &GroupKey,
        now: VirtualInstant,
        input: RaftInput,
    ) -> Option<RaftOutput> {
        let node = self.nodes.get_mut(index)?;
        if !node.running {
            return None;
        }
        // Mirror the scheduler instant and attribute armed timers to this
        // replica before stepping (the `Clock::arm` signature carries no node).
        self.clock.set_now(now);
        self.clock.set_active(node.id.clone(), group.clone());

        if group == &metadata_group_key() {
            node.controller
                .as_mut()
                .and_then(|controller| controller.step(input, &mut self.clock))
        } else {
            node.fleet
                .get_mut(group)
                .map(|replica| replica.step(input, &mut self.clock))
        }
    }

    /// Every node, indexed by node id.
    #[must_use]
    pub fn nodes(&self) -> &[SimNode] {
        &self.nodes
    }

    /// Mutable access to every node, for the step loop and the reconcile /
    /// crash-restart paths.
    pub fn nodes_mut(&mut self) -> &mut [SimNode] {
        &mut self.nodes
    }

    /// The number of nodes in the cluster.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Shared, read-only access to the node at `index`, or `None` if out of
    /// range.
    #[must_use]
    pub fn node(&self, index: usize) -> Option<&SimNode> {
        self.nodes.get(index)
    }

    /// The `__meta/0` [`SimTransport`] for the node at `index`, or `None` if out
    /// of range.
    #[must_use]
    pub fn meta_transport(&self, index: usize) -> Option<&SimTransport> {
        self.meta_transports.get(index)
    }

    /// The [`SimTransport`] the node at `index` dispatches `group`'s outbound
    /// Raft messages through, or `None` if the index is out of range or the node
    /// hosts no replica for `group`.
    ///
    /// The single transport lookup the step loop (task 12.1) uses to route a
    /// replica's `out.sends`: it returns the node's `__meta/0` transport for the
    /// metadata group and the per-partition transport minted by the reconcile
    /// spawn ([`apply_committed_metadata`](Self::apply_committed_metadata)) for a
    /// client partition, so the caller need not know which kind of group it is
    /// dispatching for.
    #[must_use]
    pub fn transport_for(&self, index: usize, group: &GroupKey) -> Option<&SimTransport> {
        if group == &metadata_group_key() {
            self.meta_transport(index)
        } else {
            self.nodes.get(index).and_then(|node| node.transport(group))
        }
    }

    /// Apply a committed metadata `command` to every node and reconcile each
    /// node's partition fleet to match — the harness's
    /// **metadata-commit-and-reconcile** path (Requirement 3.4).
    ///
    /// This is the synchronous, single-threaded analogue of production's
    /// "apply the committed `Cluster` entry, then poke the off-loop reconciler":
    /// for each node it
    ///
    /// 1. folds `command` into the node's served [`ClusterMetadata`] mirror via
    ///    the production [`apply_command`] (so a `CreateTopic` registers the
    ///    topic and a `DeleteTopic` removes it, exactly as the metadata
    ///    state-machine transition does);
    /// 2. computes the node's currently-running replica set from its `fleet`
    ///    keys and runs the shared, pure [`plan_reconcile`] against the updated
    ///    served catalogue and the node's own id; and
    /// 3. starts a [`PartitionReplica`] for every partition the plan spawns
    ///    (each whose [`Replica_Set`] now contains this node) and stops the
    ///    replica for every partition the plan stops.
    ///
    /// A started replica is the production [`PartitionReplica`] built over a
    /// fresh Sim_Storage-backed [`PartitionLog::Sim`](vela_core::PartitionLog)
    /// for `(node, group)`, with the group's numeric peer set from the
    /// [`Topology`]; its backing [`SimStorageHandle`] and a freshly-minted
    /// [`SimTransport`] are tracked on the node alongside the fleet so a later
    /// crash can drop the disk and the step loop can dispatch the replica's
    /// sends. Stopping a replica drops it together with its storage handle and
    /// transport. The `__meta/0` group is **never** started or stopped here:
    /// [`plan_reconcile`] excludes it, and it is hosted by the node's
    /// `controller`, not its `fleet`.
    ///
    /// The pass is idempotent — applying the same committed command twice leaves
    /// the served catalogue and fleet unchanged the second time, since the
    /// reconcile plan is then empty — and deterministic: [`plan_reconcile`]'s
    /// output is sorted and the per-replica Sim_Storage disks use the
    /// deterministic [`data_dir_for`](crate::storage) layout, so the resulting
    /// fleet is a pure function of the committed metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Storage`] if a started replica's Sim_Storage WAL
    /// cannot be opened. The served catalogue is still updated on every node
    /// regardless, since the apply precedes the spawn.
    ///
    /// [`Replica_Set`]: crate
    pub fn apply_committed_metadata(
        &mut self,
        command: &ClusterCommand,
    ) -> Result<(), ClusterError> {
        // Borrow the run-fixed topology and the shared bus disjointly from the
        // mutable node iteration (distinct fields, so the split is sound).
        let topology = &self.topology;
        let network = &self.network;

        for node in &mut self.nodes {
            // A crashed node processes no events and holds no live consensus
            // state; it recovers its own served catalogue and fleet from its
            // durable disks on `Node_Restart`, so a committed metadata apply
            // while it is down must not touch it (Requirement 6.1, 6.3).
            if !node.running {
                continue;
            }

            // 1. Fold the committed command into this node's served catalogue,
            //    exactly as the production metadata state machine does.
            apply_command(&mut node.served, command);

            // 2. Reconcile against the running fleet. The fleet never holds
            //    `__meta/0`, and `plan_reconcile` excludes it, so the metadata
            //    group is never started or stopped here.
            let running: HashSet<(String, u32)> = node
                .fleet
                .keys()
                .map(|(topic, partition)| (topic.clone(), partition.0))
                .collect();
            let plan = plan_reconcile(&node.served, &running, node.id.as_str());

            // 3a. Start a replica for every newly-assigned partition, building
            //     the production `PartitionReplica` over a fresh Sim_Storage
            //     disk and minting its transport over the shared bus.
            for (topic, partition) in plan.spawn {
                let group: GroupKey = (topic, partition.index);
                let storage = SimStorageHandle::new(&node.id, &group);
                let log = storage.open_partition_log()?;
                let peers = topology.peers_for(&node.id, &group);
                let replica = PartitionReplica::with_log(node.raft_id, peers, log);

                let peers_map = peers_map_for(topology, &node.id, &group);
                let transport = network.transport(node.id.clone(), group.clone(), peers_map);

                node.fleet.insert(group.clone(), replica);
                node.storage.insert(group.clone(), storage);
                node.transports.insert(group, transport);
            }

            // 3b. Stop the replica for every partition no longer assigned,
            //     releasing its consensus state, backing disk, and transport.
            for (topic, partition) in plan.stop {
                let group: GroupKey = (topic, PartitionIndex(partition));
                node.fleet.remove(&group);
                node.storage.remove(&group);
                node.transports.remove(&group);
            }
        }

        Ok(())
    }

    /// The node index of `id`, or `None` if `id` is not a member of the
    /// cluster.
    ///
    /// Node indices and ids are in lock-step (`nodes[i]` is `node-i`), so this
    /// is the bridge a fault schedule uses to turn a seed-derived node id back
    /// into the index [`crash_node`](Self::crash_node) /
    /// [`restart_node`](Self::restart_node) take.
    #[must_use]
    pub fn index_of(&self, id: &NodeId) -> Option<usize> {
        self.nodes.iter().position(|node| &node.id == id)
    }

    /// Apply a `Node_Crash` to the node at `index` (Requirement 6.1, 6.2).
    ///
    /// A crash models a node losing power: its **volatile** consensus state is
    /// gone and only what Sim_Storage forced to stable storage survives. The
    /// method
    ///
    /// 1. clears the node's `running` flag, so the step loop stops feeding it
    ///    events;
    /// 2. drops the live `MetadataController`, every `PartitionReplica` in the
    ///    `fleet`, and every `SimTransport` — the un-fsynced in-memory consensus
    ///    state, lost on a crash. Dropping the controller and fleet releases the
    ///    live WAL handles (a `RaftNode` owns its [`LogStorage`] by value), and
    ///    thus the data-directory locks, **before** the backing disks are
    ///    crashed;
    /// 3. **keeps** every [`SimStorageHandle`] (the backing disk survives — the
    ///    durable bytes persist) and calls [`SimStorageHandle::crash`] on each so
    ///    its un-fsynced tail is discarded, modelling loss of unsynced writes
    ///    while every Acknowledged_Record and persisted `HardState` (all fsynced
    ///    under [`SyncPolicy::Always`]) survives (Requirement 6.1, 7.2); and
    /// 4. cuts the node off the [`SimNetwork`] in **both** directions until it is
    ///    restarted (Requirement 6.2).
    ///
    /// Returns `true` if a running node was crashed, or `false` if `index` is
    /// out of range or the node was already crashed (an idempotent no-op).
    ///
    /// [`SyncPolicy::Always`]: vela_log::SyncPolicy
    pub fn crash_node(&mut self, index: usize) -> bool {
        // Resolve the id (for the network cut) and bail out early if there is
        // nothing running to crash.
        let node_id = match self.nodes.get(index) {
            Some(node) if node.running => node.id.clone(),
            _ => return false,
        };

        let node = &mut self.nodes[index];
        node.running = false;
        // Drop the volatile consensus state. Dropping the controller and the
        // fleet's replicas releases their live WAL handles (and data-directory
        // locks) so the retained disks can be crashed and later reopened.
        node.controller = None;
        node.fleet.clear();
        node.transports.clear();
        // The backing disks survive the crash; discard only their un-fsynced
        // tails so unsynced writes are lost but fsynced bytes persist.
        for handle in node.storage.values() {
            handle.crash();
        }

        // Cut the node off the bus in both directions until it is restarted.
        self.network.crash_node(node_id);
        true
    }

    /// Apply a `Node_Restart` to the crashed node at `index` (Requirement 6.3,
    /// 6.4).
    ///
    /// A restart brings a previously crashed node back up by running the
    /// **real** durable-recovery path over its retained backing disks — the same
    /// WAL/Raft recovery production uses, not a model. The method
    ///
    /// 1. reopens the node's `__meta/0` disk via its retained
    ///    [`SimStorageHandle`] and rebuilds the `MetadataController` through
    ///    [`MetadataController::recover_durable_with_log`], recovering the
    ///    current term, the vote, the committed log prefix, and the applied
    ///    catalogue from the surviving durable bytes (Requirement 6.3);
    /// 2. sets the node's served catalogue to that recovered applied catalogue,
    ///    so reconcile sees exactly the topics the node had durably learned;
    /// 3. runs [`plan_reconcile`] against an empty running fleet (the crash
    ///    dropped every replica) and, for each recovered assigned partition,
    ///    reopens its retained disk and recovers its [`PartitionReplica`] via
    ///    [`PartitionReplica::recover`] — restoring that partition's committed
    ///    prefix and applied offsets (Requirement 6.3, 6.4) — minting a fresh
    ///    [`SimTransport`] over the shared bus; and
    /// 4. sets `running` and restores delivery on the [`SimNetwork`].
    ///
    /// A recovered assigned partition whose disk was not retained (its topic was
    /// created while the node was down — out of scope for the crash-then-restart
    /// path here) is given a fresh [`SimStorageHandle`] and recovered over an
    /// empty disk, so the restart never fails to start an assigned replica.
    ///
    /// Returns `true` if a crashed node was restarted, or `false` if `index` is
    /// out of range or the node was already running (an idempotent no-op).
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Storage`] if a replica's disk cannot be reopened
    /// or [`ClusterError::Recover`] if the `__meta/0` group cannot be recovered;
    /// in either case the node is left crashed (its `running` flag stays clear).
    pub fn restart_node(&mut self, index: usize) -> Result<bool, ClusterError> {
        // Only a crashed, in-range node can be restarted.
        let (node_id, raft_id) = match self.nodes.get(index) {
            Some(node) if !node.running => (node.id.clone(), node.raft_id),
            _ => return Ok(false),
        };

        // Borrow the run-fixed topology and shared bus disjointly from the
        // mutable node (distinct fields, so the split is sound).
        let topology = &self.topology;
        let network = &self.network;
        let meta_group = metadata_group_key();

        let node = &mut self.nodes[index];

        // 1. Recover `__meta/0` from its retained disk: reopen the WAL and
        //    rebuild the controller (term, vote, committed prefix, catalogue).
        let meta_storage = node
            .storage
            .get(&meta_group)
            .expect("a node always retains its `__meta/0` storage handle");
        let log = meta_storage.open_partition_log()?;
        let peers = topology.peers_for(&node_id, &meta_group);
        let controller = MetadataController::recover_durable_with_log(
            raft_id,
            peers,
            log,
            decode_cluster_command,
        )?;

        // 2. The recovered applied catalogue is the authoritative served view.
        node.served = controller.metadata().clone();
        node.controller = Some(controller);

        // 3. Reconcile against an empty running fleet (the crash dropped every
        //    replica and transport) and recover each assigned partition.
        node.fleet.clear();
        node.transports.clear();
        let running: HashSet<(String, u32)> = HashSet::new();
        let plan = plan_reconcile(&node.served, &running, node_id.as_str());
        for (topic, partition) in plan.spawn {
            let group: GroupKey = (topic, partition.index);
            // Reopen the partition's retained disk for the real recovery path;
            // a topic created while the node was down has no retained handle, so
            // recover over a fresh disk instead.
            let storage = node
                .storage
                .remove(&group)
                .unwrap_or_else(|| SimStorageHandle::new(&node_id, &group));
            let log = storage.open_partition_log()?;
            let peers = topology.peers_for(&node_id, &group);
            let replica = PartitionReplica::recover(raft_id, peers, log);

            let peers_map = peers_map_for(topology, &node_id, &group);
            let transport = network.transport(node_id.clone(), group.clone(), peers_map);

            node.fleet.insert(group.clone(), replica);
            node.storage.insert(group.clone(), storage);
            node.transports.insert(group, transport);
        }

        // 4. The node is up again; restore delivery on the bus.
        node.running = true;
        network.restart_node(&node_id);
        Ok(true)
    }

    /// Crash every node in `indices`, returning how many transitioned from
    /// running to crashed (Requirement 6.5).
    ///
    /// A convenience over [`crash_node`](Self::crash_node) for applying a
    /// concurrent crash to a subset of nodes — e.g. a minority of a group's
    /// voters at a seed-derived instant. Out-of-range or already-crashed indices
    /// are skipped, so the count reflects only the nodes actually crashed.
    pub fn crash_nodes(&mut self, indices: &[usize]) -> usize {
        indices
            .iter()
            .filter(|&&index| self.crash_node(index))
            .count()
    }

    /// Restart every node in `indices`, returning how many were brought back up
    /// (Requirement 6.5).
    ///
    /// A convenience over [`restart_node`](Self::restart_node). Out-of-range or
    /// already-running indices are skipped.
    ///
    /// # Errors
    ///
    /// Returns the first [`ClusterError`] a restart raises (a disk reopen or
    /// `__meta/0` recovery failure); nodes processed before it are left
    /// restarted.
    pub fn restart_nodes(&mut self, indices: &[usize]) -> Result<usize, ClusterError> {
        let mut restarted = 0;
        for &index in indices {
            if self.restart_node(index)? {
                restarted += 1;
            }
        }
        Ok(restarted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vela_core::metadata_group_key;

    /// Parameters with the given cluster shape; everything else stays at the
    /// documented defaults.
    fn params(
        node_count: usize,
        replication_factor: usize,
        partition_count: u32,
    ) -> ScenarioParameters {
        ScenarioParameters {
            node_count,
            replication_factor,
            partition_count,
            ..ScenarioParameters::default()
        }
    }

    #[test]
    fn topology_node_ids_are_node_index_in_order() {
        let topo = Topology::from_params(&params(3, 2, 4));
        assert_eq!(topo.node_count(), 3);
        assert_eq!(
            topo.nodes(),
            &[
                NodeId::new("node-0"),
                NodeId::new("node-1"),
                NodeId::new("node-2"),
            ]
        );
    }

    #[test]
    fn topology_construction_is_deterministic() {
        // Requirement 3.5: the topology is a pure function of the parameters,
        // so building it twice yields the identical, never-mutated shape.
        let p = params(5, 3, 7);
        assert_eq!(Topology::from_params(&p), Topology::from_params(&p));
    }

    #[test]
    fn replica_sets_have_replication_factor_distinct_nodes() {
        let topo = Topology::from_params(&params(5, 3, 5));
        for p in 0..topo.partition_count() {
            let set = topo
                .replica_set_for(PartitionIndex(p))
                .expect("partition index within range has a replica set");
            // Exactly `replication_factor` replicas...
            assert_eq!(set.len(), 3);
            // ...all distinct, and all real members of the cluster.
            let mut seen = set.to_vec();
            seen.sort();
            seen.dedup();
            assert_eq!(seen.len(), 3, "replica set must hold distinct nodes");
            assert!(set.iter().all(|n| topo.nodes().contains(n)));
        }
    }

    #[test]
    fn replica_sets_are_contiguous_round_robin_by_partition_index() {
        // node_count 3, rf 2: partition p -> { node-p, node-(p+1) } mod 3.
        let topo = Topology::from_params(&params(3, 2, 4));
        assert_eq!(
            topo.replica_set_for(PartitionIndex(0)).unwrap(),
            &[NodeId::new("node-0"), NodeId::new("node-1")]
        );
        assert_eq!(
            topo.replica_set_for(PartitionIndex(1)).unwrap(),
            &[NodeId::new("node-1"), NodeId::new("node-2")]
        );
        // Wraps round the node set.
        assert_eq!(
            topo.replica_set_for(PartitionIndex(2)).unwrap(),
            &[NodeId::new("node-2"), NodeId::new("node-0")]
        );
        assert_eq!(
            topo.replica_set_for(PartitionIndex(3)).unwrap(),
            &[NodeId::new("node-0"), NodeId::new("node-1")]
        );
    }

    #[test]
    fn replica_set_for_out_of_range_partition_is_none() {
        let topo = Topology::from_params(&params(3, 2, 2));
        assert!(topo.replica_set_for(PartitionIndex(2)).is_none());
    }

    #[test]
    fn metadata_group_is_replicated_by_every_node() {
        // The cluster-wide control group lives on all nodes, regardless of the
        // replication factor used for client partitions.
        let topo = Topology::from_params(&params(4, 2, 3));
        let meta = topo.replica_set_for_group(&metadata_group_key()).unwrap();
        assert_eq!(meta, topo.nodes());
    }

    #[test]
    fn replica_set_for_client_group_ignores_topic_name() {
        // Only the partition index drives assignment, so two different topics
        // share the same per-index layout.
        let topo = Topology::from_params(&params(3, 2, 3));
        let orders = ("orders".to_string(), PartitionIndex(1));
        let events = ("events".to_string(), PartitionIndex(1));
        assert_eq!(
            topo.replica_set_for_group(&orders),
            topo.replica_set_for_group(&events)
        );
    }

    #[test]
    fn raft_id_and_domain_id_round_trip() {
        let topo = Topology::from_params(&params(4, 2, 2));
        for (i, node) in topo.nodes().iter().enumerate() {
            let raft = topo.raft_id(node).expect("member has a raft id");
            assert_eq!(raft, RaftNodeId(i as u64));
            assert_eq!(topo.domain_id(raft), Some(node));
        }
        // A non-member has no raft id, and an out-of-range raft id no domain id.
        assert!(topo.raft_id(&NodeId::new("ghost")).is_none());
        assert!(topo.domain_id(RaftNodeId(99)).is_none());
    }

    #[test]
    fn peers_for_excludes_self_and_handles_non_members() {
        // node_count 3, rf 3: every node replicates partition 0, so each node's
        // peers are the other two voters in node-index order.
        let topo = Topology::from_params(&params(3, 3, 1));
        let group = ("orders".to_string(), PartitionIndex(0));
        assert_eq!(
            topo.peers_for(&NodeId::new("node-0"), &group),
            vec![RaftNodeId(1), RaftNodeId(2)]
        );
        assert_eq!(
            topo.peers_for(&NodeId::new("node-1"), &group),
            vec![RaftNodeId(0), RaftNodeId(2)]
        );
        // A node not in the replica set has no peers in that group.
        let topo = Topology::from_params(&params(3, 1, 1));
        // Partition 0's replica set is just { node-0 }, so node-1 is absent.
        assert!(topo.peers_for(&NodeId::new("node-1"), &group).is_empty());
        // And the lone member has no peers.
        assert!(topo.peers_for(&NodeId::new("node-0"), &group).is_empty());
    }

    #[test]
    fn group_contains_reflects_replica_set_membership() {
        let topo = Topology::from_params(&params(3, 2, 3));
        let group = ("orders".to_string(), PartitionIndex(0)); // { node-0, node-1 }
        assert!(topo.group_contains(&group, &NodeId::new("node-0")));
        assert!(topo.group_contains(&group, &NodeId::new("node-1")));
        assert!(!topo.group_contains(&group, &NodeId::new("node-2")));
        // Every node is in the metadata group.
        let meta = metadata_group_key();
        assert!(topo.group_contains(&meta, &NodeId::new("node-2")));
    }

    #[test]
    fn sim_node_starts_running_with_empty_fleet_and_storage() {
        // A node is constructed around an injected controller (11.2 owns how the
        // controller is built); it starts up, empty, ready for the reconcile and
        // recovery paths to populate it.
        let topo = Topology::from_params(&params(3, 3, 1));
        let id = NodeId::new("node-0");
        let raft_id = topo.raft_id(&id).unwrap();
        let peers = topo.peers_for(&id, &metadata_group_key());
        let controller = MetadataController::new(raft_id, peers);

        let node = SimNode::new(id.clone(), raft_id, controller);

        assert_eq!(node.id(), &id);
        assert_eq!(node.raft_id(), RaftNodeId(0));
        assert!(node.is_running());
        assert_eq!(node.fleet_len(), 0);
        assert!(node.served().topics.is_empty());
        assert!(node.storage.is_empty());
        // The injected controller really does host the dedicated metadata group.
        assert!(node.controller().unwrap().hosts_metadata_group());
    }

    // --- SimulatedCluster assembly (task 11.2) ------------------------------

    use crate::scenario::RunConfig;
    use vela_raft::{NodeId as VelaRaftNodeId, RaftMessage, RequestVoteReply, Transport};

    use crate::scheduler::VirtualInstant;

    /// Assemble a cluster of the given shape from seed `0`, defaults elsewhere.
    fn cluster(
        node_count: usize,
        replication_factor: usize,
        partition_count: u32,
    ) -> SimulatedCluster {
        SimulatedCluster::new(RunConfig {
            seed: 0,
            params: params(node_count, replication_factor, partition_count),
        })
        .expect("valid parameters assemble a cluster")
    }

    #[test]
    fn new_builds_one_running_node_per_id_each_hosting_meta_group() {
        // Requirement 3.1: a node per id, each hosting a `__meta/0` group built
        // over its own Sim_Storage disk; Requirement 3.2: the controller is the
        // production type recovered through the real WAL path.
        let cluster = cluster(3, 3, 4);
        assert_eq!(cluster.node_count(), 3);

        let meta = metadata_group_key();
        for (i, node) in cluster.nodes().iter().enumerate() {
            assert_eq!(node.id(), &NodeId::new(format!("node-{i}")));
            assert_eq!(node.raft_id(), RaftNodeId(i as u64));
            assert!(node.is_running());
            // Recovered over a fresh disk: empty catalogue, no client fleet yet.
            assert!(node.controller().unwrap().hosts_metadata_group());
            assert!(node.controller().unwrap().metadata().topics.is_empty());
            assert!(node.served().topics.is_empty());
            assert_eq!(node.fleet_len(), 0);
            // The `__meta/0` storage handle is held on the node for crash/restart.
            assert!(node.storage.contains_key(&meta));
            assert_eq!(node.storage.len(), 1);
        }
    }

    #[test]
    fn new_accepts_the_default_run_config() {
        // The documented defaults assemble a cluster (rf == node_count is valid).
        let cluster = SimulatedCluster::new(RunConfig::default())
            .expect("default config assembles a cluster");
        assert_eq!(cluster.node_count(), crate::scenario::DEFAULT_NODE_COUNT);
    }

    #[test]
    fn new_rejects_invalid_parameters_before_building() {
        // Requirement 15.5: an invalid set is refused (not panicked, not run).
        let bad = RunConfig {
            seed: 0,
            params: ScenarioParameters {
                node_count: 3,
                replication_factor: 4,
                ..ScenarioParameters::default()
            },
        };
        assert!(matches!(
            SimulatedCluster::new(bad),
            Err(ClusterError::Scenario(
                ScenarioError::ReplicationFactorTooHigh { .. }
            ))
        ));
    }

    #[test]
    fn assembly_is_deterministic_for_a_config() {
        // Requirement 3.5 / 1: the run-fixed shape is a pure function of the
        // config, so two assemblies agree on topology and node identity.
        let a = cluster(5, 3, 7);
        let b = cluster(5, 3, 7);
        assert_eq!(a.topology(), b.topology());
        assert_eq!(a.node_count(), b.node_count());
        for (na, nb) in a.nodes().iter().zip(b.nodes()) {
            assert_eq!(na.id(), nb.id());
            assert_eq!(na.raft_id(), nb.raft_id());
            assert!(na.controller().unwrap().hosts_metadata_group());
            assert!(nb.controller().unwrap().hosts_metadata_group());
        }
    }

    #[test]
    fn topology_network_and_clock_accessors_are_exposed() {
        // Requirement: the runtime/checkers reach the cluster's seams.
        let cluster = cluster(3, 2, 4);
        assert_eq!(cluster.topology().node_count(), 3);
        // A freshly built network has buffered nothing.
        assert_eq!(cluster.network().pending_len(), 0);
        // The shared clock starts at the logical origin.
        assert_eq!(cluster.clock().virtual_now(), VirtualInstant::ORIGIN);
        // Node accessor bounds.
        assert!(cluster.node(2).is_some());
        assert!(cluster.node(3).is_none());
    }

    #[test]
    fn each_node_has_a_meta_transport_routing_through_the_shared_network() {
        // Requirement 3.3: every inter-node `__meta/0` message flows through the
        // shared Sim_Network. A node's minted transport stamps its own id and
        // resolves a numeric destination to the right peer's domain id.
        let cluster = cluster(3, 3, 1);
        // One transport per node.
        for i in 0..cluster.node_count() {
            assert!(cluster.meta_transport(i).is_some());
        }
        assert!(cluster.meta_transport(3).is_none());

        // node-0 sends a metadata message to raft id 1 (= node-1).
        let tx = cluster
            .meta_transport(0)
            .expect("node-0 has a meta transport");
        cluster.network().set_now(VirtualInstant::from_nanos(1_000));
        tx.send(
            VelaRaftNodeId(1),
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term: 1,
                vote_granted: true,
                voter: VelaRaftNodeId(0),
            }),
        );

        let pending = cluster.network().drain_pending();
        assert_eq!(pending.len(), 1, "the message is routed through the bus");
        let (_, envelope) = &pending[0];
        assert_eq!(envelope.from, NodeId::new("node-0"));
        assert_eq!(envelope.to, NodeId::new("node-1"));
        assert_eq!(envelope.to_raft, VelaRaftNodeId(1));
        assert_eq!(envelope.group, metadata_group_key());
    }

    #[test]
    fn meta_group_storage_disks_are_distinct_per_node() {
        // Each node owns its own backing disk, so a crash on one node cannot
        // touch another's durable bytes.
        let cluster = cluster(3, 3, 1);
        let meta = metadata_group_key();
        let dirs: Vec<_> = cluster
            .nodes()
            .iter()
            .map(|n| n.storage[&meta].config().data_dir.clone())
            .collect();
        // All three data directories are distinct.
        assert_eq!(dirs.len(), 3);
        assert_ne!(dirs[0], dirs[1]);
        assert_ne!(dirs[1], dirs[2]);
        assert_ne!(dirs[0], dirs[2]);
    }

    // --- Topic create/delete reconcile path (task 11.3) ---------------------

    use vela_core::{LogBackend, Partition};

    /// A `CreateTopic` command for `name` whose partitions carry the topology's
    /// fixed `Replica_Set`s — exactly the catalogue the metadata group would
    /// commit, so reconcile spawns a replica on each assigned node.
    fn create_topic_for(topo: &Topology, name: &str) -> ClusterCommand {
        let partitions = (0..topo.partition_count())
            .map(|p| {
                let index = PartitionIndex(p);
                Partition {
                    index,
                    replicas: topo
                        .replica_set_for(index)
                        .expect("partition index within range")
                        .to_vec(),
                    leader: None,
                }
            })
            .collect();
        ClusterCommand::CreateTopic {
            name: name.to_string(),
            partitions,
            backend: LogBackend::Durable,
        }
    }

    /// The node's hosted partition keys, sorted for a stable comparison.
    fn fleet_keys(node: &SimNode) -> Vec<GroupKey> {
        let mut keys: Vec<GroupKey> = node.fleet.keys().cloned().collect();
        keys.sort();
        keys
    }

    #[test]
    fn create_topic_spawns_a_replica_on_exactly_the_assigned_nodes() {
        // Requirement 3.4: a committed CreateTopic makes each node host a running
        // PartitionReplica for every partition whose Replica_Set contains it.
        // node_count 3, rf 2, 3 partitions:
        //   p0 -> {node-0, node-1}, p1 -> {node-1, node-2}, p2 -> {node-2, node-0}
        let mut cluster = cluster(3, 2, 3);
        let command = create_topic_for(cluster.topology(), "orders");

        cluster.apply_committed_metadata(&command).unwrap();

        let topo = cluster.topology().clone();
        for node in cluster.nodes() {
            // The node hosts exactly the partitions whose replica set contains it.
            let expected: Vec<GroupKey> = (0..topo.partition_count())
                .map(PartitionIndex)
                .filter(|p| topo.replica_set_for(*p).unwrap().contains(node.id()))
                .map(|p| ("orders".to_string(), p))
                .collect();
            assert_eq!(fleet_keys(node), expected, "node {:?}", node.id());

            // Storage and transports track the fleet exactly, plus the node's
            // own `__meta/0` storage handle (never a fleet/transport entry).
            for key in &expected {
                assert!(node.storage.contains_key(key));
                assert!(node.transports.contains_key(key));
            }
            assert_eq!(node.transports.len(), expected.len());
            assert_eq!(node.storage.len(), expected.len() + 1);
            assert!(node.storage.contains_key(&metadata_group_key()));

            // The served catalogue reflects the committed create on every node.
            assert!(node.served().topics.contains_key("orders"));
        }
    }

    #[test]
    fn delete_topic_stops_every_replica_it_started() {
        let mut cluster = cluster(3, 2, 3);
        let create = create_topic_for(cluster.topology(), "orders");
        cluster.apply_committed_metadata(&create).unwrap();
        assert!(cluster.nodes().iter().any(|n| n.fleet_len() > 0));

        let delete = ClusterCommand::DeleteTopic {
            name: "orders".to_string(),
        };
        cluster.apply_committed_metadata(&delete).unwrap();

        for node in cluster.nodes() {
            // Every started replica, its disk, and its transport are released.
            assert_eq!(node.fleet_len(), 0);
            assert!(node.transports.is_empty());
            // Only the `__meta/0` storage handle remains.
            assert_eq!(node.storage.len(), 1);
            assert!(node.storage.contains_key(&metadata_group_key()));
            // The served catalogue no longer holds the topic.
            assert!(!node.served().topics.contains_key("orders"));
        }
    }

    #[test]
    fn applying_the_same_create_twice_is_idempotent() {
        let mut cluster = cluster(3, 2, 3);
        let command = create_topic_for(cluster.topology(), "orders");

        cluster.apply_committed_metadata(&command).unwrap();
        let first: Vec<Vec<GroupKey>> = cluster.nodes().iter().map(fleet_keys).collect();

        // A second apply finds the topic already served and every assigned
        // replica already running, so the reconcile plan is empty.
        cluster.apply_committed_metadata(&command).unwrap();
        let second: Vec<Vec<GroupKey>> = cluster.nodes().iter().map(fleet_keys).collect();

        assert_eq!(first, second);
    }

    #[test]
    fn reconcile_never_starts_or_stops_the_metadata_group() {
        // The `__meta/0` group is hosted by the controller, never the fleet, and
        // `plan_reconcile` excludes it — so no create/delete touches it.
        let mut cluster = cluster(3, 3, 2);
        let meta = metadata_group_key();
        let create = create_topic_for(cluster.topology(), "a");

        cluster.apply_committed_metadata(&create).unwrap();
        for node in cluster.nodes() {
            assert!(!node.fleet.contains_key(&meta));
            assert!(!node.transports.contains_key(&meta));
            assert!(node.controller().unwrap().hosts_metadata_group());
        }

        cluster
            .apply_committed_metadata(&ClusterCommand::DeleteTopic {
                name: "a".to_string(),
            })
            .unwrap();
        for node in cluster.nodes() {
            assert!(node.controller().unwrap().hosts_metadata_group());
            assert!(node.storage.contains_key(&meta));
        }
    }

    #[test]
    fn transport_for_resolves_partition_and_meta_groups() {
        // The unified lookup the step loop uses: a hosted partition resolves to
        // its minted transport, the metadata group to the node's meta transport,
        // and both route through the shared bus.
        let mut cluster = cluster(3, 2, 3);
        let create = create_topic_for(cluster.topology(), "orders");
        cluster.apply_committed_metadata(&create).unwrap();

        // node-0 hosts orders/p0 ({node-0, node-1}); its transport sends to the
        // peer (raft id 1 = node-1) through the bus.
        let group: GroupKey = ("orders".to_string(), PartitionIndex(0));
        let tx = cluster
            .transport_for(0, &group)
            .expect("node-0 hosts orders/p0");
        cluster.network().set_now(VirtualInstant::from_nanos(500));
        tx.send(
            VelaRaftNodeId(1),
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term: 2,
                vote_granted: true,
                voter: VelaRaftNodeId(0),
            }),
        );
        let pending = cluster.network().drain_pending();
        assert_eq!(pending.len(), 1);
        let (_, env) = &pending[0];
        assert_eq!(env.from, NodeId::new("node-0"));
        assert_eq!(env.to, NodeId::new("node-1"));
        assert_eq!(env.group, group);

        // The metadata group resolves to the node's `__meta/0` transport.
        assert!(cluster.transport_for(0, &metadata_group_key()).is_some());
        // A partition this node does not host has no transport.
        let unhosted: GroupKey = ("orders".to_string(), PartitionIndex(1));
        assert!(cluster.transport_for(0, &unhosted).is_none());
    }

    #[test]
    fn reconcile_outcome_is_deterministic_for_a_config() {
        // The resulting fleet is a pure function of the committed metadata, so
        // two clusters built from the same config agree on every node's fleet.
        let mut a = cluster(5, 3, 7);
        let mut b = cluster(5, 3, 7);
        let command = create_topic_for(a.topology(), "orders");
        a.apply_committed_metadata(&command).unwrap();
        b.apply_committed_metadata(&command).unwrap();

        for (na, nb) in a.nodes().iter().zip(b.nodes()) {
            assert_eq!(fleet_keys(na), fleet_keys(nb));
        }
    }

    // --- Node crash / restart (task 11.4) -----------------------------------

    use std::time::{Duration, Instant};
    use vela_log::{EntryPayload, PayloadKind};
    use vela_raft::{RaftInput, TimerKind};

    use crate::codec::encode_cluster_command;

    /// A no-op [`Clock`](vela_raft::Clock) for driving a replica through
    /// explicit [`RaftInput`]s in a test: time never advances and arming a timer
    /// is a no-op, so consensus is driven entirely by the inputs fed to `step`.
    #[derive(Default)]
    struct TestClock;

    impl vela_raft::Clock for TestClock {
        fn now(&self) -> Instant {
            // A fixed reference instant suffices; these tests never read it.
            Instant::now()
        }

        fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
    }

    /// Drive node 0's single-voter `__meta/0` group to leader and commit
    /// `command` as a durable `Cluster` entry, returning nothing — the entry is
    /// fsynced into the node's `__meta/0` WAL under `SyncPolicy::Always`.
    ///
    /// Only valid for a one-node cluster, where the self-vote is a majority and
    /// a proposal commits in the same step.
    fn commit_meta_command(cluster: &mut SimulatedCluster, command: &ClusterCommand) {
        let mut clock = TestClock;
        let controller = cluster.nodes_mut()[0]
            .controller
            .as_mut()
            .expect("node 0 is running");

        // Single voter: a Tick wins the election outright.
        controller.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        assert_eq!(controller.role(), Some(vela_raft::Role::Leader));

        // Propose the command as a `Cluster` entry; it commits immediately.
        let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(command));
        let out = controller
            .step(RaftInput::Propose(payload), &mut clock)
            .expect("the metadata group is present");
        assert_eq!(out.committed.len(), 1, "single voter commits at once");
    }

    #[test]
    fn crash_clears_running_drops_volatile_state_and_cuts_the_network() {
        // Requirement 6.1, 6.2: a crash stops the node, drops its volatile
        // consensus state (controller + fleet + transports), keeps the backing
        // disks, and cuts the node off the bus in both directions.
        let mut cluster = cluster(3, 2, 3);
        let create = create_topic_for(cluster.topology(), "orders");
        cluster.apply_committed_metadata(&create).unwrap();

        // node-0 hosts some partitions before the crash.
        assert!(cluster.node(0).unwrap().fleet_len() > 0);
        let retained_disks = cluster.node(0).unwrap().storage.len();

        assert!(cluster.crash_node(0));

        let node = cluster.node(0).unwrap();
        assert!(!node.is_running());
        // The volatile consensus state is gone...
        assert!(node.controller().is_none());
        assert_eq!(node.fleet_len(), 0);
        assert!(node.transports.is_empty());
        // ...but every backing disk survives the crash (durable bytes persist).
        assert_eq!(node.storage.len(), retained_disks);
        assert!(node.storage.contains_key(&metadata_group_key()));

        // The node is cut off the bus: a peer's message to it is not delivered.
        let tx = cluster
            .meta_transport(1)
            .expect("node-1 has a meta transport");
        cluster.network().set_now(VirtualInstant::from_nanos(10));
        tx.send(
            VelaRaftNodeId(0),
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term: 1,
                vote_granted: true,
                voter: VelaRaftNodeId(1),
            }),
        );
        assert_eq!(
            cluster.network().drain_pending().len(),
            0,
            "a crashed node receives nothing until it is restarted"
        );
    }

    #[test]
    fn crash_is_idempotent_and_bounds_checked() {
        let mut cluster = cluster(3, 3, 1);
        assert!(cluster.crash_node(0));
        // A second crash of the same node is a no-op.
        assert!(!cluster.crash_node(0));
        // An out-of-range index is a no-op too.
        assert!(!cluster.crash_node(99));
    }

    #[test]
    fn restart_recovers_meta_group_sets_running_and_restores_delivery() {
        // Requirement 6.3: a restart rebuilds the `__meta/0` controller from the
        // retained disk; with no durable commits the recovered catalogue is
        // empty, so the node comes back up hosting only the metadata group.
        let mut cluster = cluster(3, 3, 1);
        assert!(cluster.crash_node(1));
        assert!(cluster.restart_node(1).unwrap());

        let node = cluster.node(1).unwrap();
        assert!(node.is_running());
        assert!(node.controller().unwrap().hosts_metadata_group());
        // Recovered an empty catalogue, so no client replicas and only the
        // `__meta/0` disk.
        assert!(node.served().topics.is_empty());
        assert_eq!(node.fleet_len(), 0);
        assert!(node.storage.contains_key(&metadata_group_key()));

        // Delivery to the node is restored: a peer message now buffers for it.
        let tx = cluster
            .meta_transport(0)
            .expect("node-0 has a meta transport");
        cluster.network().set_now(VirtualInstant::from_nanos(20));
        tx.send(
            VelaRaftNodeId(1),
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term: 1,
                vote_granted: true,
                voter: VelaRaftNodeId(0),
            }),
        );
        assert_eq!(
            cluster.network().drain_pending().len(),
            1,
            "a restarted node receives messages again"
        );
    }

    #[test]
    fn restart_is_idempotent_and_bounds_checked() {
        let mut cluster = cluster(3, 3, 1);
        // A running node cannot be restarted.
        assert!(!cluster.restart_node(0).unwrap());
        // An out-of-range index is a no-op.
        assert!(!cluster.restart_node(99).unwrap());
    }

    #[test]
    fn restart_recovers_the_durable_catalogue_and_restarts_assigned_replicas() {
        // Requirement 6.3, 6.4: after a CreateTopic is committed durably to the
        // `__meta/0` WAL, a crash-then-restart recovers the applied catalogue
        // from disk and starts a Partition_Replica for every recovered assigned
        // partition. A one-node cluster lets us commit through real consensus.
        let mut cluster = cluster(1, 1, 2);
        let create = create_topic_for(cluster.topology(), "orders");
        commit_meta_command(&mut cluster, &create);

        // The crash discards the live controller and any un-fsynced tail; the
        // durable CreateTopic survives because Always fsynced it on commit.
        assert!(cluster.crash_node(0));
        assert!(cluster.node(0).unwrap().controller().is_none());

        assert!(cluster.restart_node(0).unwrap());

        let topo = cluster.topology().clone();
        let node = cluster.node(0).unwrap();
        assert!(node.is_running());
        // The recovered applied catalogue holds the durably-committed topic.
        assert!(node.controller().unwrap().hosts_metadata_group());
        assert!(node.served().topics.contains_key("orders"));
        // A replica is recovered for every assigned partition (both, since the
        // lone node is in every Replica_Set), each with its disk and transport.
        let expected: Vec<GroupKey> = (0..topo.partition_count())
            .map(PartitionIndex)
            .filter(|p| topo.replica_set_for(*p).unwrap().contains(node.id()))
            .map(|p| ("orders".to_string(), p))
            .collect();
        assert_eq!(fleet_keys(node), expected);
        for key in &expected {
            assert!(node.storage.contains_key(key));
            assert!(node.transports.contains_key(key));
        }
    }

    #[test]
    fn apply_committed_metadata_skips_crashed_nodes() {
        // Requirement 6.1, 6.3: a committed metadata apply must not touch a
        // crashed node — it recovers its own catalogue and fleet on restart.
        let mut cluster = cluster(3, 2, 3);
        assert!(cluster.crash_node(0));

        let create = create_topic_for(cluster.topology(), "orders");
        cluster.apply_committed_metadata(&create).unwrap();

        // The crashed node is untouched: no served topic, no fleet.
        let crashed = cluster.node(0).unwrap();
        assert!(!crashed.is_running());
        assert!(crashed.served().topics.is_empty());
        assert_eq!(crashed.fleet_len(), 0);
        // The running nodes still reconciled the create.
        assert!(cluster
            .node(1)
            .unwrap()
            .served()
            .topics
            .contains_key("orders"));
    }

    #[test]
    fn crash_and_restart_a_minority_subset() {
        // Requirement 6.5: any subset (here a minority of voters) can be crashed
        // and restarted concurrently. A 5-node cluster tolerates 2 crashes.
        let mut cluster = cluster(5, 5, 1);
        assert_eq!(cluster.crash_nodes(&[1, 3]), 2);
        assert!(!cluster.node(1).unwrap().is_running());
        assert!(!cluster.node(3).unwrap().is_running());
        // The majority remains up.
        assert_eq!(cluster.nodes().iter().filter(|n| n.is_running()).count(), 3);

        // Both restart and the whole cluster is up again.
        assert_eq!(cluster.restart_nodes(&[1, 3]).unwrap(), 2);
        assert!(cluster.nodes().iter().all(SimNode::is_running));
    }

    #[test]
    fn index_of_maps_node_ids_to_indices() {
        let cluster = cluster(3, 2, 1);
        assert_eq!(cluster.index_of(&NodeId::new("node-0")), Some(0));
        assert_eq!(cluster.index_of(&NodeId::new("node-2")), Some(2));
        assert_eq!(cluster.index_of(&NodeId::new("ghost")), None);
    }

    #[test]
    fn crash_restart_is_deterministic_for_a_config() {
        // The recovered shape is a pure function of the durable bytes, so two
        // clusters driven through the identical crash/restart agree node-for-node.
        let build = || {
            let mut c = cluster(1, 1, 2);
            let create = create_topic_for(c.topology(), "orders");
            commit_meta_command(&mut c, &create);
            c.crash_node(0);
            c.restart_node(0).unwrap();
            c
        };
        let a = build();
        let b = build();
        assert_eq!(a.node(0).unwrap().served(), b.node(0).unwrap().served());
        assert_eq!(
            fleet_keys(a.node(0).unwrap()),
            fleet_keys(b.node(0).unwrap())
        );
    }
}
