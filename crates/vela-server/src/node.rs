//! Node-wide shared state and the partition driver lifecycle.
//!
//! [`NodeShared`] is the state the two gRPC services operate on: this node's
//! identity and replication factor, its view of [`ClusterMetadata`], the table
//! of running partition drivers, and the shared peer connection
//! [`PeerPool`](crate::transport::PeerPool). It owns the create/stop lifecycle
//! of the per-partition driver tasks (Requirement 3.2): creating a topic spawns
//! a driver for each partition this node replicates, and deleting one stops them
//! and releases their replicas.
//!
//! ## Durable catalogue
//!
//! The topic catalogue is durable. [`NodeShared`] owns the recovering
//! [`MetadataController`] for the dedicated `__meta` Raft group: topic
//! create/delete are committed through that group's durable log and then
//! mirrored into the served [`ClusterMetadata`] view, so the catalogue (and
//! each topic's backend) survives a full cold restart. [`NodeShared::new`]
//! recovers the `__meta` group and rebuilds the served view from it *before*
//! spawning any client partition, so durable topics reopen their existing
//! segments on startup. Cross-node agreement is reached through the `__meta/0`
//! Raft group itself — committed `ClusterCommand`s reach every node via
//! `AppendEntries` and are reconciled into the running partition drivers. There
//! is one consensus mechanism; the former bespoke `SyncMetadata` ack/laggard
//! propagation protocol has been removed (Requirement 1.3).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot, Notify};

use vela_core::{
    ClusterCommand, ClusterMetadata, CoreError, LogBackend, Member, MetadataController,
    MetadataRecoverError, NodeAvailability, NodeId, Partition, PartitionLog, PartitionReplica,
    SyncPolicy, Topic, WalConfig, METADATA_GROUP_PARTITION, METADATA_GROUP_TOPIC,
};
use vela_log::{DurableWal, InMemoryLog};
use vela_raft::NodeId as RaftNodeId;

use crate::clock::TimerClock;
use crate::config::Config;
use crate::convert;
use crate::driver::{
    DriverCommand, DriverHandle, MetadataDriver, MetadataSink, PartitionDriver, ReconcileSignal,
};
use crate::paths::{metadata_data_path, partition_data_path};
use crate::registry::raft_node_id;
use crate::transport::{GrpcTransport, PeerPool};

/// Why a node failed to start.
///
/// [`NodeShared::new`] performs the durable bootstrap before serving: it opens
/// and recovers the dedicated `__meta` Raft group, which is real filesystem I/O
/// and can fail (a missing/unwritable data directory, a corrupt or locked
/// metadata log). Because the topic catalogue cannot be recovered safely in
/// that case, startup fails fast with a non-zero exit through
/// [`serve`](crate::serve) rather than serving an empty or partial catalogue
/// (Requirement 16, 18).
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    /// Opening or recovering the durable metadata Raft group failed.
    #[error("failed to recover the durable metadata group: {0}")]
    Metadata(#[from] MetadataRecoverError),
    /// An I/O error encountered while preparing the node's data directory.
    #[error("node startup I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Why [`NodeShared::spawn_partition`] could not start a partition replica.
///
/// The sole failure mode is a durable partition whose log could not be opened.
/// In that case the replica is deliberately left **unstarted** rather than
/// falling back to an in-memory backend, so a storage fault never silently
/// downgrades a durable topic to a volatile one (Requirement 8.1, 8.2). The
/// node continues to host the partitions whose backends opened successfully
/// (Requirement 8.3): the caller logs the error and moves on to the next
/// partition.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnError {
    /// Opening the durable Write-Ahead-Log for the partition failed; the
    /// replica was left unstarted.
    #[error("failed to open durable log for {topic}/{partition}: {source}")]
    DurableOpen {
        /// The topic whose partition could not open its durable log.
        topic: String,
        /// The partition index within the topic.
        partition: u32,
        /// The underlying log-open error.
        #[source]
        source: vela_log::LogError,
    },
}

/// Build the [`WalConfig`] for one durable partition log.
///
/// The config is rooted at the partition's derived [`partition_data_path`] and
/// uses the only consensus-safe sync policy, [`SyncPolicy::Always`], which
/// forces each mutating operation to stable storage before it returns
/// (Requirement 5.3, 12.1). Factored out so the construction site's path
/// rooting and sync policy are testable without filesystem I/O.
fn durable_wal_config(data_dir: &Path, topic: &str, partition: u32) -> WalConfig {
    WalConfig::new(partition_data_path(data_dir, topic, partition))
        .with_sync_policy(SyncPolicy::Always)
}

/// Shared state backing both gRPC services on a node.
pub struct NodeShared {
    /// This node's stable string identity.
    pub(crate) self_id: String,
    /// The configured replication factor for new topics.
    pub(crate) replication_factor: usize,
    /// The node's view of cluster metadata.
    ///
    /// Held behind an [`Arc`] so the metadata commit-apply seam
    /// ([`MetadataSink`](crate::driver::MetadataSink)) can share this one served
    /// catalogue: committed `ClusterCommand`s fold into it as `__meta/0`
    /// replicates, and the request path and the reconciler read the same view.
    pub(crate) metadata: Arc<Mutex<ClusterMetadata>>,
    /// The running partition drivers, keyed by `(topic, partition)`.
    pub(crate) partitions: Mutex<HashMap<(String, u32), DriverHandle>>,
    /// Shared, lazily-connected peer channels.
    pub(crate) pool: Arc<PeerPool>,
    /// Root directory under which Durable partition logs store their segments
    /// (Requirement 6.3). Threaded from [`Config`]; `spawn_partition` derives
    /// each durable partition's path beneath it.
    pub(crate) data_dir: PathBuf,
    /// The durable, recovering controller for the dedicated `__meta` Raft group.
    ///
    /// Catalogue changes (topic create/delete) are committed through this
    /// group's durable log so they survive a cold restart; [`NodeShared::new`]
    /// recovers it before any client partition is spawned and rebuilds the
    /// served [`metadata`](Self::metadata) view from it (Requirement 16, 17,
    /// 18). The served `metadata` mirror is what the request path reads; this
    /// controller is the durable source of truth the mirror is kept in step
    /// with on each committed command.
    ///
    /// Held behind an [`Arc`] so the [`MetadataDriver`] task can share the one
    /// recovered replica: the metadata group is now driven asynchronously (real
    /// timers + peer transport) yet a durable WAL permits only a single open, so
    /// the driver steps this same controller under its mutex rather than opening
    /// the `__meta` log a second time.
    pub(crate) controller: Arc<Mutex<MetadataController>>,
}

impl NodeShared {
    /// Build node state from `config`, recovering the durable catalogue and
    /// reopening this node's partition replicas, or fail fast with a
    /// [`StartupError`].
    ///
    /// Startup is ordered so the catalogue is durable end to end (Requirement
    /// 16, 17, 18):
    ///
    /// 1. **Recover the `__meta` group first.** The dedicated metadata Raft
    ///    group is opened durably at its reserved path with the consensus-safe
    ///    [`SyncPolicy::Always`] and its committed `ClusterCommand`s are
    ///    replayed to rebuild the catalogue *before* any client partition is
    ///    touched. If the metadata log cannot be opened the node refuses to
    ///    start (Requirement 16.1, 17.x, 18.1).
    /// 2. **Install the recovered catalogue** as the served view, seeding this
    ///    node as the sole available member (membership discovers peers later).
    ///    The served view carries each recovered topic and its backend so
    ///    list/describe and spawn selection see them (Requirement 18.1, 18.3).
    /// 3. **Reopen each recovered topic's local replicas.** Durable topics
    ///    reopen their existing segments at their derived paths; in-memory
    ///    topics start empty (Requirement 18.2, 14, 11.4). A per-partition
    ///    spawn failure is logged and skipped so one bad partition does not
    ///    abort the whole node (Requirement 8.3).
    pub fn new(config: &Config) -> Result<Arc<Self>, StartupError> {
        let self_id = config.node_id.as_str().to_string();
        let self_raft_id = raft_node_id(&self_id);
        let data_dir = config.data_dir.clone();

        // The metadata group's voter set is every statically configured node:
        // this node plus each configured peer, each mapped through the unified
        // identity to its numeric raft id (Req 1.2). A single-node deployment
        // (no peers) yields a 1-voter group that self-elects through the normal
        // election path; a multi-node deployment yields a real cluster-wide
        // voter set that elects and replicates over the `VelaPeer` transport.
        let meta_peers: Vec<RaftNodeId> = config
            .peers
            .iter()
            .map(|peer| raft_node_id(&peer.id))
            .collect();

        // Reverse map every metadata voter's numeric raft id back to its domain
        // id (this node plus each configured peer) so the metadata driver can
        // report its known leader as a domain `NodeId` for the redirect hint and
        // live-leader routing (Requirement 4.1, 8.1, 8.2).
        let meta_lookup: HashMap<RaftNodeId, NodeId> =
            std::iter::once((self_raft_id, NodeId::new(&self_id)))
                .chain(
                    config
                        .peers
                        .iter()
                        .map(|peer| (raft_node_id(&peer.id), NodeId::new(&peer.id))),
                )
                .collect();

        // 1. Recover the durable metadata group and its committed catalogue
        //    before spawning any client partition (Requirement 16, 17, 18.1).
        //
        //    G4 — recovery reads an existing `__meta` log *compatibly*. The
        //    voter set is construction-time configuration supplied here, not
        //    state encoded in the log, so an old single-node `__meta` log
        //    replays its committed `Cluster` entries identically; only the
        //    in-memory voter set differs, which is exactly the intended
        //    single-node -> multi-node membership change. There is therefore no
        //    log-format ambiguity to misread. A committed entry that cannot be
        //    decoded still fails fast through `MetadataRecoverError` rather than
        //    being silently misapplied.
        let meta_path = metadata_data_path(&data_dir);
        let controller = MetadataController::recover_durable(
            self_raft_id,
            meta_peers,
            &meta_path,
            convert::cluster_command_from_bytes,
        )?;

        // 2. Build the served view from the recovered catalogue, seeding this
        //    node as the sole available member (Requirement 18.1, 18.3).
        let mut metadata = ClusterMetadata::new();
        metadata.members.push(Member {
            id: NodeId::new(&self_id),
            addr: config.listen_addr.to_string(),
            availability: NodeAvailability::Available,
        });
        for topic in controller.metadata().topics.values() {
            metadata.topics.insert(topic.name.clone(), topic.clone());
        }
        metadata.epoch = controller.metadata().epoch;

        let node = Arc::new(Self {
            self_id,
            replication_factor: config.replication_factor as usize,
            metadata: Arc::new(Mutex::new(metadata)),
            partitions: Mutex::new(HashMap::new()),
            pool: Arc::new(PeerPool::new()),
            data_dir,
            controller: Arc::new(Mutex::new(controller)),
        });

        // Register every metadata voter's transport address so the metadata
        // driver can dial its peers' `VelaPeer` endpoints, keyed by the same
        // numeric raft id the voter set uses (Req 1.1). Idempotent with the
        // membership subsystem, which re-registers these same peers.
        for peer in &config.peers {
            node.pool
                .register_peer(raft_node_id(&peer.id), peer.addr.clone());
        }

        // 3. Drive `__meta/0` as an ordinary, asynchronously-driven Raft group:
        //    spawn its driver task wired to a real timer clock, the peer
        //    transport, and the metadata commit-apply seam, and spawn the
        //    off-loop reconciler the seam pokes. Registering the driver's handle
        //    makes inbound metadata Raft RPCs route through the existing
        //    `VelaPeerService::dispatch_rpc` (Req 1.1, 1.4, 2.1, 2.4). This
        //    replaces stepping the metadata group *only* inline.
        node.spawn_metadata_driver(meta_lookup);

        // 4. Reconcile once after recovery so this node starts a partition
        //    driver for every recovered partition whose Replica_Set contains it,
        //    reopening durable topics at their derived paths and starting
        //    in-memory topics empty (Requirement 9.2, 9.3, 18.2). This is the
        //    same idempotent pass the reconciler runs on every later commit; a
        //    per-partition durable-log-open failure is logged and skipped while
        //    the rest reconcile (Requirement 6.7, 8.3).
        crate::reconciler::reconcile(&node);

        Ok(node)
    }

    /// Create a topic by committing it through the dedicated `__meta/0`
    /// metadata Raft group, returning the applied topic (design §4).
    ///
    /// The request is validated and its replicas assigned against a snapshot of
    /// the served (applied) catalogue — only the leader's command commits, so
    /// validation/assignment is effectively performed against the leader's
    /// catalogue — building the [`ClusterCommand::CreateTopic`] without mutating
    /// anything. A rejection (invalid name or partition count, a duplicate
    /// topic, or insufficient available nodes) proposes nothing and leaves the
    /// catalogue untouched.
    ///
    /// The command is then **proposed** to the `__meta/0` group and the call
    /// awaits its commit: the metadata group routes the proposal to its leader,
    /// redirecting a non-leader with [`CoreError::NotLeader`] so the client can
    /// retry against the leader (Req 4.1; Raft §8), and reports success only
    /// once the entry is committed to a majority (Req 3.1–3.4). The same commit
    /// is folded into the served view by the
    /// [`MetadataSink`](crate::driver::MetadataSink) before the proposal
    /// resolves, so the applied topic is then read back from that view (Req
    /// 3.4).
    ///
    /// **Idempotent on topic name (H2).** Re-creating an existing topic is
    /// rejected with [`CoreError::TopicExists`] and proposes nothing, so a
    /// client retrying after an *indeterminate* [`CoreError::CommitTimeout`]
    /// (Req 3.5) cannot corrupt the catalogue.
    ///
    /// Partition drivers are **not** started here: the commit-driven reconciler
    /// starts them on every replica node (Req 6.1). This node reconciles
    /// promptly after its own commit so a client hitting the origin sees its
    /// drivers without waiting for the off-loop signal; every other replica node
    /// spawns from the same committed entry when it applies (design §4, §5).
    pub(crate) async fn create_topic(
        self: &Arc<Self>,
        name: &str,
        partition_count: u32,
        backend: LogBackend,
    ) -> Result<Topic, CoreError> {
        // Validate + assign on a snapshot so nothing is mutated until the
        // command commits (idempotent on name: a duplicate is `TopicExists`).
        let command = {
            let metadata = self.metadata.lock().expect("metadata mutex poisoned");
            let mut snapshot = metadata.clone();
            snapshot.create_topic(name, partition_count, self.replication_factor, backend)?;
            let topic = snapshot
                .topics
                .get(name)
                .cloned()
                .expect("topic was just created on the snapshot");
            ClusterCommand::CreateTopic {
                name: name.to_string(),
                partitions: topic.partitions,
                backend,
            }
        };

        // Propose to `__meta/0` and await commit; a non-leader redirects (Req
        // 4.1), a commit timeout is indeterminate (Req 3.5, H2).
        self.propose_cluster(command).await?;

        // The commit was already folded into the served view by the metadata
        // sink, so align this node's partition drivers with the new catalogue
        // now (Req 6.1) and read the applied topic back (Req 3.4).
        crate::reconciler::reconcile(self);
        let topic = self
            .metadata
            .lock()
            .expect("metadata mutex poisoned")
            .topics
            .get(name)
            .cloned()
            .ok_or_else(|| CoreError::TopicNotFound(name.to_string()))?;
        Ok(topic)
    }

    /// Delete a topic by committing the removal through the dedicated `__meta/0`
    /// metadata Raft group (design §4).
    ///
    /// The deletion is validated against the served (applied) catalogue and, for
    /// a present topic, proposed to the `__meta/0` group as a
    /// [`ClusterCommand::DeleteTopic`]; the call awaits its commit. As with
    /// create, the metadata group routes the proposal to its leader and
    /// redirects a non-leader with [`CoreError::NotLeader`] (Req 4.1; Raft §8),
    /// reporting success only once the entry commits to a majority (Req 3).
    ///
    /// **Idempotent on topic name (H2).** Re-deleting an absent topic is a
    /// **no-op success** that proposes nothing, so a client retrying after an
    /// *indeterminate* [`CoreError::CommitTimeout`] (Req 3.5) cannot corrupt the
    /// catalogue.
    ///
    /// Partition drivers are **not** stopped here directly: the commit-driven
    /// reconciler stops the deleted topic's drivers on every replica node (Req
    /// 6.2). This node reconciles promptly after its own commit; every other
    /// replica node stops them from the same committed entry when it applies.
    pub(crate) async fn delete_topic(self: &Arc<Self>, name: &str) -> Result<(), CoreError> {
        // A present topic yields a DeleteTopic command; an absent one is an
        // idempotent no-op success that proposes nothing (H2).
        let command = {
            let metadata = self.metadata.lock().expect("metadata mutex poisoned");
            if !metadata.topics.contains_key(name) {
                return Ok(());
            }
            ClusterCommand::DeleteTopic {
                name: name.to_string(),
            }
        };

        // Propose to `__meta/0` and await commit (Req 3, 4.1).
        self.propose_cluster(command).await?;

        // The commit was already applied to the served view, so stop this
        // node's drivers for the now-absent topic via the reconciler (running \
        // desired); every other replica node does the same on apply (Req 6.2).
        crate::reconciler::reconcile(self);
        Ok(())
    }

    /// Propose an already-validated, replica-assigned `command` to the dedicated
    /// `__meta/0` metadata Raft group and await its commit (design §3, §4).
    ///
    /// The command is handed to the metadata driver through its
    /// [`DriverHandle`] (`self.handle("__meta", 0)`) as a
    /// [`DriverCommand::ProposeCluster`]; the driver appends and replicates it
    /// **only on the metadata leader** and resolves the reply:
    ///
    /// - `Ok(())` once the entry commits to a majority (Req 3.1–3.4);
    /// - `Err(CoreError::NotLeader { leader })` on a non-leader, carrying the
    ///   known metadata-leader hint so the caller can redirect (Req 4.1; Raft
    ///   §8);
    /// - `Err(CoreError::CommitTimeout)` if the entry has not committed within
    ///   `COMMIT_TIMEOUT_MS` (Req 3.5).
    ///
    /// A [`CoreError::CommitTimeout`] is **indeterminate**, not a failure (H2):
    /// the entry may still commit under a new leader, so the caller must
    /// re-check (e.g. `DescribeTopic`) rather than assume the change did not take
    /// effect. Topic-admin being idempotent on topic name makes a retry safe.
    ///
    /// A missing handle or a closed driver queue is surfaced as
    /// [`CoreError::NotLeader`] (no metadata replica here is accepting the
    /// proposal); a dropped reply is surfaced as the indeterminate
    /// [`CoreError::CommitTimeout`].
    async fn propose_cluster(&self, command: ClusterCommand) -> Result<(), CoreError> {
        let handle = self
            .handle(METADATA_GROUP_TOPIC, METADATA_GROUP_PARTITION.0)
            .ok_or(CoreError::NotLeader { leader: None })?;
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::ProposeCluster {
                command,
                reply: reply_tx,
            })
            .map_err(|_| CoreError::NotLeader { leader: None })?;
        // A dropped reply (the driver stopped) is indeterminate; surface it as a
        // commit timeout so the caller re-checks rather than assuming success.
        reply_rx.await.unwrap_or(Err(CoreError::CommitTimeout))
    }

    /// The handle of the driver for `(topic, partition)`, if hosted here.
    pub(crate) fn handle(&self, topic: &str, partition: u32) -> Option<DriverHandle> {
        self.partitions
            .lock()
            .expect("partitions mutex poisoned")
            .get(&(topic.to_string(), partition))
            .cloned()
    }

    /// Spawn the asynchronous driver for the dedicated metadata Raft group
    /// `("__meta", 0)` and register its handle in the partitions table.
    ///
    /// The metadata group is driven exactly like a partition replica — a real
    /// [`TimerClock`] for its election/heartbeat timers and a [`GrpcTransport`]
    /// stamping the reserved `("__meta", 0)` key onto outbound RPCs — so it
    /// elects a leader and replicates over the same `VelaPeer` transport the
    /// partition groups use (Req 1.1, 1.4, 2.1, 2.4). Registering the driver's
    /// [`DriverHandle`] under `("__meta", 0)` makes [`NodeShared::handle`]
    /// resolve it, so inbound `AppendEntries` / `RequestVote` for the metadata
    /// group route through the existing `VelaPeerService::dispatch_rpc` with no
    /// change.
    ///
    /// The driver folds each step's committed entries through a
    /// [`MetadataSink`](crate::driver::MetadataSink) (design §2): a committed
    /// `ClusterCommand` is applied to the node's served catalogue and the shared
    /// [`ReconcileSignal`](crate::driver::ReconcileSignal) is poked. A companion
    /// reconciler task waits on that same signal and aligns this node's running
    /// partition drivers with the served catalogue **off** the metadata Raft
    /// loop (design §5, H1), so applying a commit never blocks metadata
    /// heartbeats. Apply and reconcile share the one served catalogue and the
    /// one signal (Requirement 5.1, 6.1).
    ///
    /// The driver shares the node's durable [`MetadataController`] replica behind
    /// its mutex rather than opening the durable `__meta` WAL a second time (a
    /// durable WAL permits only a single open); it is now the sole stepper of
    /// that replica.
    fn spawn_metadata_driver(self: &Arc<Self>, leader_lookup: HashMap<RaftNodeId, NodeId>) {
        let key = (METADATA_GROUP_TOPIC.to_string(), METADATA_GROUP_PARTITION.0);
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = TimerClock::new(tx.clone());
        let transport = GrpcTransport::new(
            METADATA_GROUP_TOPIC.to_string(),
            METADATA_GROUP_PARTITION.0,
            self.self_id.clone(),
            self.pool.clone(),
            tx.clone(),
        );

        // The reconcile signal is shared between the sink that pokes it (after
        // applying a committed catalogue change) and the off-loop reconciler
        // task that waits on it; `Notify` coalesces pokes into one idempotent
        // pass (design §5, H1).
        let signal: ReconcileSignal = Arc::new(Notify::new());
        let sink = MetadataSink::new(self.metadata.clone(), signal.clone());
        crate::reconciler::spawn_reconciler(self.clone(), signal.clone());
        // Periodically re-poke the reconciler on the membership cadence so a
        // partition left unstarted by a transient durable-log-open failure
        // (Requirement 6.7) is retried until it starts or is unassigned
        // (Requirement 6.8). Reconciliation is an idempotent diff, so the
        // periodic re-runs are safe (design §5).
        crate::reconciler::spawn_reconcile_ticker(signal);

        let driver = MetadataDriver::new(
            self.controller.clone(),
            self.self_id.clone(),
            clock,
            transport,
            rx,
            tx.clone(),
            sink,
            leader_lookup,
        );
        driver.spawn();
        self.partitions
            .lock()
            .expect("partitions mutex poisoned")
            .insert(key, tx);
    }

    /// Spawn a driver for `partition` of `topic` if this node is one of its
    /// replicas, registering peer addresses and wiring the timer clock and
    /// transport.
    ///
    /// The partition log backend is the one the topic declared in
    /// [`ClusterMetadata`] (defaulting to [`LogBackend::Durable`] when the topic
    /// or its backend is somehow absent), constructed by injection
    /// (Requirement 5.1-5.4): an in-memory topic builds an [`InMemoryLog`] and
    /// writes nothing to disk (Requirement 13.3), while a durable topic opens a
    /// [`DurableWal`] rooted at the partition's derived path with the
    /// consensus-safe [`SyncPolicy::Always`] and recovers its committed prefix
    /// (Requirement 11.1-11.3, 12.1).
    ///
    /// Returns `Ok(true)` if a driver was started, `Ok(false)` if this node does
    /// not replicate the partition or already runs it, and
    /// [`Err(SpawnError)`](SpawnError) if a durable log could not be opened — in
    /// which case the replica is left **unstarted** (no in-memory fallback) and
    /// the caller continues hosting the other partitions (Requirement 8.1-8.3).
    ///
    /// Idempotent at the table level: a partition already running is left as-is.
    pub(crate) fn spawn_partition(
        self: &Arc<Self>,
        topic: &str,
        partition: &Partition,
    ) -> Result<bool, SpawnError> {
        if !partition
            .replicas
            .iter()
            .any(|r| r.as_str() == self.self_id)
        {
            return Ok(false);
        }
        let key = (topic.to_string(), partition.index.0);
        let mut partitions = self.partitions.lock().expect("partitions mutex poisoned");
        if partitions.contains_key(&key) {
            return Ok(false);
        }

        // Map the other replicas to numeric ids and register their addresses so
        // the transport can reach them, and read the topic's backend while we
        // hold the metadata lock (defaulting to Durable when the topic is absent,
        // Requirement 5.1).
        let metadata = self.metadata.lock().expect("metadata mutex poisoned");
        let mut peers = Vec::new();
        for replica in &partition.replicas {
            if replica.as_str() == self.self_id {
                continue;
            }
            let peer_id = raft_node_id(replica.as_str());
            if let Some(member) = metadata.members.iter().find(|m| &m.id == replica) {
                self.pool.register_peer(peer_id, member.addr.clone());
            }
            peers.push(peer_id);
        }
        let backend = metadata
            .topics
            .get(topic)
            .map(|t| t.backend)
            .unwrap_or(LogBackend::Durable);
        drop(metadata);

        let self_raft_id = raft_node_id(&self.self_id);

        // Construct the backend the topic declared and build the replica over it
        // by injection (Requirement 5.2).
        let replica = match backend {
            // In-memory: a fresh volatile log; no path is derived and no files
            // are written (Requirement 5.4, 13.3).
            LogBackend::InMemory => PartitionReplica::with_log(
                self_raft_id,
                peers,
                PartitionLog::InMemory(InMemoryLog::new()),
            ),
            // Durable: open the WAL rooted at the derived path with the
            // consensus-safe Always policy. On failure, log a structured error
            // identifying the topic and partition and leave the replica
            // unstarted — never an in-memory fallback (Requirement 5.3, 8.1,
            // 8.2, 12.1).
            LogBackend::Durable => {
                let cfg = durable_wal_config(&self.data_dir, topic, partition.index.0);
                let wal = match DurableWal::open(cfg) {
                    Ok(wal) => wal,
                    Err(source) => {
                        tracing::error!(
                            topic,
                            partition = partition.index.0,
                            error = %source,
                            "failed to open durable partition log; leaving the replica unstarted"
                        );
                        return Err(SpawnError::DurableOpen {
                            topic: topic.to_string(),
                            partition: partition.index.0,
                            source,
                        });
                    }
                };
                // Recover the committed prefix and hard state from the reopened
                // log so previously committed records reappear at their original
                // offsets (Requirement 11.1-11.3).
                PartitionReplica::recover(self_raft_id, peers, PartitionLog::Durable(wal))
            }
        };

        let (tx, rx) = mpsc::unbounded_channel();
        let clock = TimerClock::new(tx.clone());
        let transport = GrpcTransport::new(
            topic.to_string(),
            partition.index.0,
            self.self_id.clone(),
            self.pool.clone(),
            tx.clone(),
        );
        // Reverse map every replica's numeric raft id back to its domain id so
        // the driver can report its known leader as a domain `NodeId` for
        // live-leader routing (Requirement 8.1, 8.2).
        let leader_lookup: HashMap<RaftNodeId, NodeId> = partition
            .replicas
            .iter()
            .map(|replica| (raft_node_id(replica.as_str()), replica.clone()))
            .collect();
        let driver = PartitionDriver::new(
            topic.to_string(),
            partition.index.0,
            self.self_id.clone(),
            replica,
            clock,
            transport,
            rx,
            tx.clone(),
            leader_lookup,
        );
        driver.spawn();

        partitions.insert(key, tx);
        Ok(true)
    }

    /// Stop the driver for `(topic, partition)` if running, releasing its
    /// replica (Raft state and in-memory log) (Requirement 3.2, 3.3).
    pub(crate) fn stop_partition(&self, topic: &str, partition: u32) {
        if let Some(handle) = self
            .partitions
            .lock()
            .expect("partitions mutex poisoned")
            .remove(&(topic.to_string(), partition))
        {
            // The driver breaks its loop and drops the replica on Shutdown; if
            // it has already stopped, the send fails harmlessly.
            let _ = handle.send(DriverCommand::Shutdown);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::process;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    use vela_core::{PartitionIndex, Topic, TopicState};

    /// Monotonic counter making temp-dir names unique within a process.
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// An owned temporary directory recursively removed when dropped.
    ///
    /// Cleanup is best-effort (a removal failure must not mask an assertion).
    /// The path is computed but not created, so an in-memory test can assert the
    /// directory never came into existence; a durable test lets `DurableWal`
    /// create it. The guard drops after the node (and its drivers/WALs), so the
    /// exclusive directory lock is released before cleanup.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        /// Create a uniquely-named path under the system temp directory: process
        /// id, a per-process counter, and the current nanoseconds, so concurrent
        /// binaries and repeated runs never collide.
        fn new(tag: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after the unix epoch")
                .as_nanos();
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let name = format!("vela-server-node-{tag}-{}-{unique}-{nanos}", process::id());
            Self {
                path: std::env::temp_dir().join(name),
            }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn config(node_id: &str, addr: &str, rf: u32, data_dir: &Path) -> Config {
        Config {
            node_id: NodeId::new(node_id),
            listen_addr: addr.parse().expect("valid addr"),
            peers: Vec::new(),
            replication_factor: rf,
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// A single local partition with index `index` replicated by `node-a`.
    fn local_partition(index: u32) -> Partition {
        Partition {
            index: PartitionIndex(index),
            replicas: vec![NodeId::new("node-a")],
            leader: None,
        }
    }

    /// Record `topic` in the node's metadata with the given `backend` and
    /// partitions, so `spawn_partition` reads the intended backend.
    fn record_topic(node: &NodeShared, topic: &str, backend: LogBackend, partitions: &[Partition]) {
        let mut metadata = node.metadata.lock().unwrap();
        metadata.topics.insert(
            topic.to_string(),
            Topic {
                name: topic.to_string(),
                partitions: partitions.to_vec(),
                state: TopicState::Active,
                backend,
            },
        );
    }

    /// A captured `tracing` event: its level plus the `topic`/`partition` fields
    /// the structured spawn errors carry.
    #[derive(Clone, Debug, Default)]
    struct Captured {
        level: Option<Level>,
        topic: Option<String>,
        partition: Option<u64>,
    }

    /// In-memory layer recording the level and the `topic`/`partition` fields of
    /// each event, used to assert a structured error was emitted.
    #[derive(Clone, Default)]
    struct CaptureLayer {
        events: Arc<Mutex<Vec<Captured>>>,
    }

    #[derive(Default)]
    struct FieldVisitor {
        topic: Option<String>,
        partition: Option<u64>,
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "topic" && self.topic.is_none() {
                self.topic = Some(format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "topic" {
                self.topic = Some(value.to_string());
            }
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "partition" {
                self.partition = Some(value);
            }
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            if field.name() == "partition" {
                self.partition = Some(value as u64);
            }
        }
    }

    impl<S: Subscriber> Layer<S> for CaptureLayer {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            self.events.lock().unwrap().push(Captured {
                level: Some(*event.metadata().level()),
                topic: visitor.topic,
                partition: visitor.partition,
            });
        }
    }

    #[tokio::test]
    async fn new_node_is_the_sole_available_member() {
        let tmp = TempDir::new("sole-member");
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1, tmp.path()))
            .expect("node startup succeeds");
        let metadata = node.metadata.lock().unwrap();
        assert_eq!(metadata.members.len(), 1);
        assert_eq!(metadata.members[0].id, NodeId::new("node-a"));
        assert_eq!(
            metadata.members[0].availability,
            NodeAvailability::Available
        );
        assert_eq!(node.replication_factor, 1);
    }

    #[test]
    fn durable_wal_config_is_rooted_at_the_derived_path_and_uses_always_policy() {
        // The construction site roots the WAL at the partition's derived path
        // and selects the only consensus-safe policy, `Always` (Requirement 5.3,
        // 12.1). Verified without I/O by inspecting the config the spawn path
        // builds.
        let data_dir = Path::new("/var/lib/vela");
        let cfg = durable_wal_config(data_dir, "orders", 2);
        assert_eq!(cfg.data_dir, partition_data_path(data_dir, "orders", 2));
        assert_eq!(cfg.sync_policy, SyncPolicy::Always);
    }

    #[tokio::test]
    async fn spawn_partition_only_hosts_local_replicas() {
        let tmp = TempDir::new("hosts-local");
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1, tmp.path()))
            .expect("node startup succeeds");

        // A partition this node does not replicate is not hosted; the
        // local-replica check short-circuits before any backend is built.
        let remote = Partition {
            index: PartitionIndex(0),
            replicas: vec![NodeId::new("node-b")],
            leader: Some(NodeId::new("node-b")),
        };
        assert!(!node.spawn_partition("orders", &remote).unwrap());
        assert!(node.handle("orders", 0).is_none());

        // A local in-memory partition is hosted (no files), and a duplicate
        // spawn is a no-op.
        let local = local_partition(1);
        record_topic(
            &node,
            "orders",
            LogBackend::InMemory,
            std::slice::from_ref(&local),
        );
        assert!(node.spawn_partition("orders", &local).unwrap());
        assert!(node.handle("orders", 1).is_some());
        assert!(!node.spawn_partition("orders", &local).unwrap());

        node.stop_partition("orders", 1);
        assert!(node.handle("orders", 1).is_none());
    }

    #[tokio::test]
    async fn durable_topic_creates_segments_under_the_derived_path() {
        let tmp = TempDir::new("durable-rooted");
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1, tmp.path()))
            .expect("node startup succeeds");

        let partition = local_partition(0);
        record_topic(
            &node,
            "orders",
            LogBackend::Durable,
            std::slice::from_ref(&partition),
        );

        assert!(node.spawn_partition("orders", &partition).unwrap());
        assert!(node.handle("orders", 0).is_some());

        // The durable backend is rooted at the derived per-partition path and
        // has created its data directory and on-disk files there (Requirement
        // 5.3, 7.1).
        let path = partition_data_path(tmp.path(), "orders", 0);
        assert!(
            path.is_dir(),
            "durable partition path {path:?} should exist"
        );
        let file_count = std::fs::read_dir(&path)
            .expect("derived partition directory should be readable")
            .count();
        assert!(
            file_count > 0,
            "opening the durable WAL should create files under {path:?}"
        );

        node.stop_partition("orders", 0);
    }

    #[tokio::test]
    async fn in_memory_topic_writes_no_files_under_data_dir() {
        let tmp = TempDir::new("in-memory-no-files");
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1, tmp.path()))
            .expect("node startup succeeds");

        let partition = local_partition(0);
        record_topic(
            &node,
            "events",
            LogBackend::InMemory,
            std::slice::from_ref(&partition),
        );

        assert!(node.spawn_partition("events", &partition).unwrap());
        assert!(node.handle("events", 0).is_some());

        // An in-memory topic derives no path and writes no segment files: the
        // partition path and its parent topic directory never come into
        // existence (Requirement 13.3).
        let part_path = partition_data_path(tmp.path(), "events", 0);
        assert!(
            !part_path.exists(),
            "in-memory partition must not create {part_path:?}"
        );
        let topic_dir = part_path.parent().expect("partition path has a parent");
        assert!(
            !topic_dir.exists(),
            "in-memory topic dir {topic_dir:?} must not exist"
        );

        node.stop_partition("events", 0);
    }

    #[tokio::test]
    async fn durable_open_failure_leaves_replica_unstarted_while_sibling_continues() {
        let tmp = TempDir::new("durable-open-failure");
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1, tmp.path()))
            .expect("node startup succeeds");

        // Topic "orders" recorded Durable with two local partitions.
        let p0 = local_partition(0);
        let p1 = local_partition(1);
        record_topic(
            &node,
            "orders",
            LogBackend::Durable,
            &[p0.clone(), p1.clone()],
        );

        // Sabotage partition 0's derived path: place a regular FILE where the
        // WAL must create its data directory, so `DurableWal::open` fails.
        let p0_path = partition_data_path(tmp.path(), "orders", 0);
        std::fs::create_dir_all(p0_path.parent().unwrap()).unwrap();
        std::fs::write(&p0_path, b"not a directory").unwrap();

        // Capture the structured error emitted on the open failure.
        let layer = CaptureLayer::default();
        let events = layer.events.clone();
        let subscriber = tracing_subscriber::registry().with(layer);
        let result =
            tracing::subscriber::with_default(subscriber, || node.spawn_partition("orders", &p0));

        // The durable open failed with a structured error naming the topic and
        // partition, and the replica was left unstarted — no in-memory fallback
        // (Requirement 8.1, 8.2).
        match result {
            Err(SpawnError::DurableOpen {
                topic, partition, ..
            }) => {
                assert_eq!(topic, "orders");
                assert_eq!(partition, 0);
            }
            other => panic!("expected a DurableOpen error, got {other:?}"),
        }
        assert!(
            node.handle("orders", 0).is_none(),
            "the failed partition must not be hosted"
        );

        // A structured ERROR identifying the topic and partition was logged.
        {
            let events = events.lock().unwrap();
            let err = events
                .iter()
                .find(|e| e.level == Some(Level::ERROR))
                .expect("an ERROR event must be emitted for the durable open failure");
            assert_eq!(err.topic.as_deref(), Some("orders"));
            assert_eq!(err.partition, Some(0));
        }

        // The sibling partition still starts: a fault on one partition does not
        // stop the node from hosting the others (Requirement 8.3).
        assert!(
            node.spawn_partition("orders", &p1).unwrap(),
            "the sibling partition should still start"
        );
        assert!(node.handle("orders", 1).is_some());

        node.stop_partition("orders", 1);
    }

    // --- Leader-routed propose idempotency (task 6.4, H2) -------------------
    //
    // A single-node node is its own metadata majority, so its `__meta/0` driver
    // self-elects through the normal asynchronous election path (the inline
    // `BootstrapClock` shortcut has been removed); once it is leader a proposal
    // commits on a majority-of-one. These tests exercise the topic-name
    // idempotency that makes a retry after an *indeterminate* `CommitTimeout`
    // safe — re-creating an existing topic is rejected as `TopicExists` and
    // re-deleting an absent topic is a no-op success, and in both cases nothing
    // new is proposed or committed (Requirement 3.4, 3.5, 4.1).

    /// Retry `create_topic` until the single-node metadata driver has
    /// self-elected through the normal async election path, so the first
    /// proposal commits rather than racing the election with `NotLeader`. The
    /// election timer fires in the randomized 150–300 ms window, so ~100
    /// attempts at 20 ms stay bounded: a group that never elects fails the test
    /// instead of hanging.
    async fn create_awaiting_metadata_leader(
        node: &Arc<NodeShared>,
        name: &str,
        partition_count: u32,
        backend: LogBackend,
    ) -> Result<Topic, CoreError> {
        for _ in 0..100 {
            match node.create_topic(name, partition_count, backend).await {
                Err(CoreError::NotLeader { .. }) => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                other => return other,
            }
        }
        node.create_topic(name, partition_count, backend).await
    }

    #[tokio::test]
    async fn recreating_an_existing_topic_is_idempotent_topic_exists() {
        let tmp = TempDir::new("recreate-idempotent");
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1, tmp.path()))
            .expect("node startup succeeds");

        // The first create commits once the single-node metadata driver elects.
        let created = create_awaiting_metadata_leader(&node, "orders", 1, LogBackend::InMemory)
            .await
            .expect("the first create_topic commits");
        assert_eq!(created.name, "orders");

        // Re-creating the same topic is rejected as TopicExists and proposes
        // nothing: a retry after an indeterminate CommitTimeout cannot corrupt
        // the catalogue (H2).
        let epoch_before = node.metadata.lock().unwrap().epoch;
        let result = node.create_topic("orders", 1, LogBackend::InMemory).await;
        assert_eq!(
            result,
            Err(CoreError::TopicExists("orders".to_string())),
            "re-creating an existing topic must be rejected as TopicExists"
        );

        let metadata = node.metadata.lock().unwrap();
        assert_eq!(metadata.topics.len(), 1, "no duplicate topic is added");
        assert_eq!(
            metadata.epoch, epoch_before,
            "a rejected re-create must commit nothing (epoch unchanged)"
        );
    }

    #[tokio::test]
    async fn redeleting_an_absent_topic_is_a_no_op_success() {
        let tmp = TempDir::new("redelete-idempotent");
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1, tmp.path()))
            .expect("node startup succeeds");

        // Deleting a topic that never existed is a no-op success that proposes
        // nothing — the catalogue (and its epoch) is untouched (H2). This holds
        // regardless of leadership, since the absent-topic check short-circuits
        // before the propose.
        assert_eq!(node.delete_topic("ghost").await, Ok(()));
        assert_eq!(
            node.metadata.lock().unwrap().epoch,
            0,
            "deleting an absent topic must commit nothing"
        );

        // Create then delete a real topic, then re-delete it: the second delete
        // is also a no-op success, leaving the catalogue empty and unchanged.
        create_awaiting_metadata_leader(&node, "orders", 1, LogBackend::InMemory)
            .await
            .expect("create commits");
        node.delete_topic("orders")
            .await
            .expect("the first delete commits");
        assert!(
            !node.metadata.lock().unwrap().topics.contains_key("orders"),
            "the topic is gone after the first delete"
        );

        let epoch_before = node.metadata.lock().unwrap().epoch;
        assert_eq!(
            node.delete_topic("orders").await,
            Ok(()),
            "re-deleting the now-absent topic is a no-op success"
        );
        assert_eq!(
            node.metadata.lock().unwrap().epoch,
            epoch_before,
            "a no-op re-delete must commit nothing (epoch unchanged)"
        );
    }
}
