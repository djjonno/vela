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
//! ## Durable catalogue (task 15)
//!
//! The topic catalogue is now durable. [`NodeShared`] owns the recovering
//! [`MetadataController`] for the dedicated `__meta` Raft group: topic
//! create/delete are committed through that group's durable log and then
//! mirrored into the served [`ClusterMetadata`] view, so the catalogue (and
//! each topic's backend) survives a full cold restart. [`NodeShared::new`]
//! recovers the `__meta` group and rebuilds the served view from it *before*
//! spawning any client partition, so durable topics reopen their existing
//! segments on startup. Cross-node agreement and `SyncMetadata` ack tracking
//! remain the membership/metadata work of later tasks; here a node starts as
//! the sole member of its own cluster, which is sufficient to bind, serve,
//! elect a single-node leader per partition, and drive produce/consume
//! locally.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use vela_core::{
    apply_command, ClusterCommand, ClusterMetadata, CoreError, LogBackend, Member,
    MetadataController, MetadataRecoverError, NodeAvailability, NodeId, Partition, PartitionLog,
    PartitionReplica, SyncPolicy, Topic, WalConfig,
};
use vela_log::{DurableWal, InMemoryLog};
use vela_raft::{Clock, EntryPayload, PayloadKind, RaftInput, TimerKind};

use crate::clock::TimerClock;
use crate::config::Config;
use crate::convert;
use crate::driver::{DriverCommand, DriverHandle, PartitionDriver};
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

/// A minimal [`Clock`] used to drive the durable metadata Raft group to commit
/// synchronously, at startup and on each catalogue change.
///
/// The `__meta` group is a single-node group stepped inline on the request /
/// startup thread (not on an async partition driver), so it needs no real
/// timers: `now` reads the wall clock and `arm` is a no-op. Election and
/// proposal each complete within the step that feeds them, because the lone
/// replica is its own majority.
struct BootstrapClock;

impl Clock for BootstrapClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
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
    pub(crate) metadata: Mutex<ClusterMetadata>,
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
    pub(crate) controller: Mutex<MetadataController>,
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

        // 1. Recover the durable metadata group and its committed catalogue
        //    before spawning any client partition (Requirement 16, 17, 18.1).
        let meta_path = metadata_data_path(&data_dir);
        let mut controller = MetadataController::recover_durable(
            self_raft_id,
            Vec::new(),
            &meta_path,
            convert::cluster_command_from_bytes,
        )?;

        // Elect the single-node metadata group so it can accept catalogue-change
        // proposals. The catalogue was already rebuilt from the recovered log,
        // so the leader's election entry does not affect it.
        {
            let mut clock = BootstrapClock;
            let _ = controller.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        }

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
            metadata: Mutex::new(metadata),
            partitions: Mutex::new(HashMap::new()),
            pool: Arc::new(PeerPool::new()),
            data_dir,
            controller: Mutex::new(controller),
        });

        // 3. Reopen each recovered topic's local partition replicas. Durable
        //    topics reopen their existing segments; in-memory topics start empty
        //    (Requirement 18.2). A per-partition failure is logged and skipped
        //    (Requirement 8.3).
        let topics: Vec<Topic> = node
            .metadata
            .lock()
            .expect("metadata mutex poisoned")
            .topics
            .values()
            .cloned()
            .collect();
        for topic in &topics {
            for partition in &topic.partitions {
                if let Err(err) = node.spawn_partition(&topic.name, partition) {
                    tracing::error!(
                        topic = %topic.name,
                        partition = partition.index.0,
                        %err,
                        "failed to reopen partition replica during startup; leaving it unstarted"
                    );
                }
            }
        }

        Ok(node)
    }

    /// Create a topic durably and return it.
    ///
    /// The request is validated and its replicas assigned against a snapshot of
    /// the served view, so a rejection (invalid name or partition count, a
    /// duplicate topic, or insufficient available nodes) touches neither the
    /// durable metadata log nor the served view. On success the resulting
    /// [`ClusterCommand::CreateTopic`] — carrying the assigned partitions and
    /// the selected `backend` — is committed through the durable `__meta` group
    /// and then folded into the served view, so the catalogue change survives a
    /// cold restart (Requirement 16, 18). The caller spawns the partition
    /// replicas for the returned topic.
    pub(crate) fn create_topic(
        &self,
        name: &str,
        partition_count: u32,
        backend: LogBackend,
    ) -> Result<Topic, CoreError> {
        // Validate + assign on a snapshot so nothing is mutated until the
        // command is durably committed.
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

        // Durably commit, then mirror into the served view.
        if !self.commit_cluster_command(&command) {
            return Err(CoreError::CommitTimeout);
        }
        let topic = self
            .metadata
            .lock()
            .expect("metadata mutex poisoned")
            .topics
            .get(name)
            .cloned()
            .expect("topic was just applied to the served view");
        Ok(topic)
    }

    /// Delete a topic durably, returning the partition indices the caller must
    /// stop.
    ///
    /// The deletion is validated against a snapshot of the served view, so a
    /// missing or already-deleting topic is rejected without touching the
    /// durable metadata log or the served view (Requirement 3.4, 3.7). On
    /// success the [`ClusterCommand::DeleteTopic`] is committed through the
    /// durable `__meta` group and folded into the served view, so the deletion
    /// survives a cold restart (Requirement 16, 18).
    pub(crate) fn delete_topic(&self, name: &str) -> Result<Vec<u32>, CoreError> {
        let (command, partitions) = {
            let metadata = self.metadata.lock().expect("metadata mutex poisoned");
            let mut snapshot = metadata.clone();
            let partitions: Vec<u32> = snapshot
                .topics
                .get(name)
                .map(|t| t.partitions.iter().map(|p| p.index.0).collect())
                .unwrap_or_default();
            snapshot.delete_topic(name)?;
            (
                ClusterCommand::DeleteTopic {
                    name: name.to_string(),
                },
                partitions,
            )
        };

        if !self.commit_cluster_command(&command) {
            return Err(CoreError::CommitTimeout);
        }
        Ok(partitions)
    }

    /// Durably commit `command` through the `__meta` group, then fold it into
    /// the served view (Requirement 16, 17, 18).
    ///
    /// Returns `false` if the metadata group could not commit the command, in
    /// which case the served view is left unchanged.
    fn commit_cluster_command(&self, command: &ClusterCommand) -> bool {
        if !self.propose_to_metadata_group(command) {
            return false;
        }
        let mut metadata = self.metadata.lock().expect("metadata mutex poisoned");
        apply_command(&mut metadata, command);
        true
    }

    /// Propose `command` to the durable single-node metadata group and drive it
    /// to commit, applying it to the controller's own view on success.
    ///
    /// The lone metadata replica is its own majority, so a proposal on the
    /// leader commits within the same step. If the group is somehow not the
    /// leader (e.g. a superseded election), one election is driven and the
    /// proposal retried once. Returns whether the command committed.
    fn propose_to_metadata_group(&self, command: &ClusterCommand) -> bool {
        let payload = EntryPayload::new(
            PayloadKind::Cluster,
            convert::cluster_command_to_bytes(command),
        );
        let mut clock = BootstrapClock;
        let mut controller = self.controller.lock().expect("controller mutex poisoned");

        let committed = controller
            .step(RaftInput::Propose(payload.clone()), &mut clock)
            .is_some_and(|out| !out.committed.is_empty());
        let committed = committed || {
            let _ = controller.step(RaftInput::Tick(TimerKind::Election), &mut clock);
            controller
                .step(RaftInput::Propose(payload), &mut clock)
                .is_some_and(|out| !out.committed.is_empty())
        };
        if committed {
            controller.apply(command);
        }
        committed
    }

    /// The handle of the driver for `(topic, partition)`, if hosted here.
    pub(crate) fn handle(&self, topic: &str, partition: u32) -> Option<DriverHandle> {
        self.partitions
            .lock()
            .expect("partitions mutex poisoned")
            .get(&(topic.to_string(), partition))
            .cloned()
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
        let driver = PartitionDriver::new(
            topic.to_string(),
            partition.index.0,
            self.self_id.clone(),
            replica,
            clock,
            transport,
            rx,
            tx.clone(),
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

    #[test]
    fn new_node_is_the_sole_available_member() {
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
}
