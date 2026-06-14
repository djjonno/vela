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
//! ## Scope for this task (14.2)
//!
//! Cluster metadata is held directly here and mutated through `vela-core`'s
//! validated [`ClusterMetadata`] operations. Agreeing metadata through the
//! dedicated `__meta` Raft group and propagating it across the cluster via
//! `SyncMetadata` with ack tracking is the membership/metadata work of task 14.3
//! and beyond; here a node starts as the sole member of its own cluster, which
//! is sufficient to bind, serve, elect a single-node leader per partition, and
//! drive produce/consume locally.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use vela_core::{ClusterMetadata, Member, NodeAvailability, NodeId, Partition, PartitionReplica};

use crate::clock::TimerClock;
use crate::config::Config;
use crate::driver::{DriverCommand, DriverHandle, PartitionDriver};
use crate::registry::raft_node_id;
use crate::transport::{GrpcTransport, PeerPool};

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
}

impl NodeShared {
    /// Build node state from `config`, seeding the metadata with this node as
    /// the sole available cluster member.
    ///
    /// Discovering peers and populating the rest of the membership is the
    /// concern of the membership subsystem (task 14.3); a fresh node begins as
    /// a one-member cluster so it can serve and drive its own partitions
    /// immediately.
    pub fn new(config: &Config) -> Arc<Self> {
        let self_id = config.node_id.as_str().to_string();
        let mut metadata = ClusterMetadata::new();
        metadata.members.push(Member {
            id: NodeId::new(&self_id),
            addr: config.listen_addr.to_string(),
            availability: NodeAvailability::Available,
        });

        Arc::new(Self {
            self_id,
            replication_factor: config.replication_factor as usize,
            metadata: Mutex::new(metadata),
            partitions: Mutex::new(HashMap::new()),
            pool: Arc::new(PeerPool::new()),
        })
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
    /// transport. Returns `true` if a driver was started.
    ///
    /// Idempotent at the table level: a partition already running is left as-is.
    pub(crate) fn spawn_partition(self: &Arc<Self>, topic: &str, partition: &Partition) -> bool {
        if !partition
            .replicas
            .iter()
            .any(|r| r.as_str() == self.self_id)
        {
            return false;
        }
        let key = (topic.to_string(), partition.index.0);
        let mut partitions = self.partitions.lock().expect("partitions mutex poisoned");
        if partitions.contains_key(&key) {
            return false;
        }

        // Map the other replicas to numeric ids and register their addresses so
        // the transport can reach them.
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
        drop(metadata);

        let self_raft_id = raft_node_id(&self.self_id);
        let replica = PartitionReplica::new(self_raft_id, peers);

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
        true
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

    fn config(node_id: &str, addr: &str, rf: u32) -> Config {
        Config {
            node_id: NodeId::new(node_id),
            listen_addr: addr.parse().expect("valid addr"),
            peers: Vec::new(),
            replication_factor: rf,
        }
    }

    #[test]
    fn new_node_is_the_sole_available_member() {
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1));
        let metadata = node.metadata.lock().unwrap();
        assert_eq!(metadata.members.len(), 1);
        assert_eq!(metadata.members[0].id, NodeId::new("node-a"));
        assert_eq!(
            metadata.members[0].availability,
            NodeAvailability::Available
        );
        assert_eq!(node.replication_factor, 1);
    }

    #[tokio::test]
    async fn spawn_partition_only_hosts_local_replicas() {
        let node = NodeShared::new(&config("node-a", "127.0.0.1:7001", 1));

        // A partition this node does not replicate is not hosted.
        let remote = Partition {
            index: vela_core::PartitionIndex(0),
            replicas: vec![NodeId::new("node-b")],
            leader: Some(NodeId::new("node-b")),
        };
        assert!(!node.spawn_partition("orders", &remote));
        assert!(node.handle("orders", 0).is_none());

        // A partition this node replicates is hosted, and a duplicate spawn is a
        // no-op.
        let local = Partition {
            index: vela_core::PartitionIndex(1),
            replicas: vec![NodeId::new("node-a")],
            leader: None,
        };
        assert!(node.spawn_partition("orders", &local));
        assert!(node.handle("orders", 1).is_some());
        assert!(!node.spawn_partition("orders", &local));

        node.stop_partition("orders", 1);
        assert!(node.handle("orders", 1).is_none());
    }
}
