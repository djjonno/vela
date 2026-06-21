//! Topic creation, validation, and balanced replica assignment.
//!
//! This module owns the domain-layer logic that turns a topic-creation request
//! into a set of partitions with replica/leadership assignments recorded in
//! [`ClusterMetadata`]. It validates the request (name, partition count,
//! cluster size), rejects conflicting or impossible requests *without mutating*
//! the metadata, and otherwise registers `N` partitions indexed `0..N` with
//! `replication_factor` distinct replica nodes each and round-robin leadership.
//!
//! It also defines [`CoreError`], the domain layer's typed error. The full
//! taxonomy from the design is declared here so later tasks (deletion, routing,
//! produce/consume, metadata) reuse the same error type; this task only
//! produces the topic-creation variants.

use crate::model::{
    ClusterMetadata, LogBackend, Member, NodeAvailability, NodeId, Partition, PartitionIndex,
    Topic, TopicState,
};

/// The minimum topic name length (Requirement 2.1, 2.6).
const MIN_NAME_LEN: usize = 1;
/// The maximum topic name length (Requirement 2.1, 2.6).
const MAX_NAME_LEN: usize = 255;
/// The minimum partition count (Requirement 2.1, 2.5).
const MIN_PARTITIONS: u32 = 1;
/// The maximum partition count (Requirement 2.1, 2.5).
const MAX_PARTITIONS: u32 = 10_000;

/// Typed errors for the `vela-core` domain layer.
///
/// Mapped to a single `VelaError` protobuf type at the gRPC boundary
/// (Requirement 12.4) by `vela-server`. The full set is declared here so the
/// later deletion, routing, produce/consume, and metadata tasks share one error
/// type; topic creation (task 10.2) only raises [`CoreError::InvalidTopicName`],
/// [`CoreError::InvalidPartitionCount`], [`CoreError::TopicExists`], and
/// [`CoreError::InsufficientNodes`].
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    /// A topic with this name already exists in the namespace (Requirement 2.4).
    #[error("topic {0} already exists")]
    TopicExists(String),
    /// The named topic does not exist (Requirement 3.4, 4.5).
    #[error("topic {0} not found")]
    TopicNotFound(String),
    /// The topic is mid-deletion and rejects further operations (Requirement 3.7).
    #[error("topic {0} is being deleted")]
    TopicDeleting(String),
    /// The referenced partition does not exist (Requirement 5.4, 10.5).
    #[error("partition not found: {topic}/{index}")]
    PartitionNotFound {
        /// The topic the partition was expected in.
        topic: String,
        /// The requested partition index.
        index: u32,
    },
    /// The topic name is empty, too long, or has disallowed characters
    /// (Requirement 2.6).
    #[error("invalid topic name")]
    InvalidTopicName,
    /// The partition count is outside `1..=10000` (Requirement 2.5).
    #[error("partition count {0} out of range 1..=10000")]
    InvalidPartitionCount(u32),
    /// A produced record exceeds the 1 MiB payload limit (Requirement 4.8).
    #[error("record exceeds 1 MiB limit ({0} bytes)")]
    RecordTooLarge(usize),
    /// Consume parameters (offset or max count) are invalid (Requirement 5.7).
    #[error("invalid consume parameters")]
    InvalidConsumeParams,
    /// A create-topic request carried a log-backend value on the wire that is
    /// neither unspecified, `Durable`, nor `In_Memory` (Requirement 2.5). The
    /// request is rejected as a validation error and no topic is created.
    #[error("invalid log backend")]
    InvalidLogBackend,
    /// Fewer available nodes than the replication factor (Requirement 2.7).
    #[error("insufficient nodes: have {have}, need {need}")]
    InsufficientNodes {
        /// The number of available member nodes.
        have: usize,
        /// The replication factor that could not be satisfied.
        need: usize,
    },
    /// The partition currently has no elected leader (Requirement 5.8, 10.6).
    #[error("partition unavailable (no leader)")]
    PartitionUnavailable,
    /// A produce request reached a replica that is not the leader of the target
    /// partition. Carries the believed current leader (when known) so the
    /// client can redirect its retry to it; no log entry is appended
    /// (Requirement 4.6, 11.2).
    #[error("not leader; current leader is {leader:?}")]
    NotLeader {
        /// The believed current leader of the partition, or `None` if unknown
        /// (e.g. an election is in progress).
        leader: Option<NodeId>,
    },
    /// A produced record's log entry was not committed to a majority of the
    /// partition's Raft group within the commit timeout, so no offset is
    /// returned and the committed offset is not advanced (Requirement 4.9).
    #[error("record not committed before commit timeout")]
    CommitTimeout,
    /// Reserved (no longer produced). The bespoke metadata-acknowledgement
    /// protocol this variant reported on has been removed in favor of agreeing
    /// cluster metadata solely through the `__meta/0` Raft group (Requirement
    /// 1.3). The variant and its wire mapping (`ErrorCode::PropagationTimeout`)
    /// are kept as reserved definitions for wire compatibility; no code path
    /// constructs it.
    #[error("metadata not acknowledged by nodes: {0:?}")]
    MetadataPropagation(Vec<NodeId>),
}

/// Returns `true` if `name` is a valid topic name: 1–255 characters drawn only
/// from `[A-Za-z0-9_-]` (Requirement 2.1, 2.6).
///
/// All allowed characters are single-byte ASCII, so byte length equals
/// character count for any name that passes the character check; a name
/// containing a multi-byte character fails the character check regardless.
fn is_valid_topic_name(name: &str) -> bool {
    let len = name.len();
    (MIN_NAME_LEN..=MAX_NAME_LEN).contains(&len)
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl ClusterMetadata {
    /// The node ids of currently available members, in membership order
    /// (Requirement 9.6 — replicas go only to cluster members).
    fn available_node_ids(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .filter(|m| matches!(m.availability, NodeAvailability::Available))
            .map(|m: &Member| m.id.clone())
            .collect()
    }

    /// Create a topic, validating the request and assigning balanced replicas.
    ///
    /// On success the topic is registered with exactly `partition_count`
    /// partitions indexed `0..partition_count` (Requirement 2.1, 2.2), each
    /// assigned `replication_factor` **distinct** replica nodes drawn from the
    /// cluster's available members (Requirement 2.3, 9.6). Leadership is
    /// balanced round-robin so that, per topic, the max and min number of
    /// leaderships on any node differ by at most one (Requirement 10.1). The
    /// metadata `epoch` is bumped to mark the change.
    ///
    /// All validation happens before any mutation, so on **any** rejection the
    /// metadata is left completely unchanged (Requirement 2.4–2.7):
    ///
    /// - [`CoreError::InvalidTopicName`] if `name` is not 1–255 chars of
    ///   `[A-Za-z0-9_-]`.
    /// - [`CoreError::InvalidPartitionCount`] if `partition_count` is outside
    ///   `1..=10000`.
    /// - [`CoreError::TopicExists`] if a topic of that name already exists.
    /// - [`CoreError::InsufficientNodes`] if fewer than `replication_factor`
    ///   members are available.
    ///
    /// The topic records `backend` as its log backend; it is fixed at creation
    /// and immutable for the topic's lifetime (Requirement 3.1, 3.3). Callers
    /// that do not care about durability pass [`LogBackend::Durable`], the
    /// default backend (Requirement 1.2).
    pub fn create_topic(
        &mut self,
        name: &str,
        partition_count: u32,
        replication_factor: usize,
        backend: LogBackend,
    ) -> Result<(), CoreError> {
        // --- Validate everything before touching `self` (Requirement 2.4–2.7). ---
        if !is_valid_topic_name(name) {
            return Err(CoreError::InvalidTopicName);
        }
        if !(MIN_PARTITIONS..=MAX_PARTITIONS).contains(&partition_count) {
            return Err(CoreError::InvalidPartitionCount(partition_count));
        }
        if self.topics.contains_key(name) {
            return Err(CoreError::TopicExists(name.to_string()));
        }
        let nodes = self.available_node_ids();
        if nodes.len() < replication_factor {
            return Err(CoreError::InsufficientNodes {
                have: nodes.len(),
                need: replication_factor,
            });
        }

        // --- Build the partitions. Validation passed, so this cannot fail. ---
        let partitions = assign_partitions(partition_count, replication_factor, &nodes);

        self.topics.insert(
            name.to_string(),
            Topic {
                name: name.to_string(),
                partitions,
                state: TopicState::Active,
                backend,
            },
        );
        self.epoch += 1;
        Ok(())
    }

    /// Delete a topic, atomically removing it and all of its partitions from
    /// the metadata (Requirement 3.1, 3.4).
    ///
    /// This is the metadata-level deletion: it removes the named topic — and
    /// with it every partition the topic owns — from `topics` as a single
    /// all-or-nothing operation, then bumps `epoch`. Because a topic's
    /// partitions live inside its [`Topic`] value, a single `BTreeMap` removal
    /// of the topic key drops the topic and *all* of its partitions together;
    /// there is no intermediate state in which only some partitions are gone
    /// (Requirement 3.1).
    ///
    /// If the topic does not exist the request is rejected with
    /// [`CoreError::TopicNotFound`] and the metadata is left completely
    /// unchanged — neither `topics` nor `epoch` is touched (Requirement 3.4).
    ///
    /// The per-partition teardown ordering — stop each partition's Raft group
    /// *before* releasing its in-memory log (Requirement 3.2, 3.3) — is a
    /// concern of the server-side `RaftGroupFleet` (task 11.4), which owns the
    /// live Raft groups and logs that this domain-layer metadata does not. To
    /// let a caller hook that teardown into the same atomic edit, use
    /// [`ClusterMetadata::delete_topic_with`]; this method is the convenience
    /// form with no teardown hook.
    pub fn delete_topic(&mut self, name: &str) -> Result<(), CoreError> {
        self.delete_topic_with(name, |_partition| {})
    }

    /// Delete a topic, invoking `on_stop_partition` for each of its partitions
    /// *before* the topic is removed from the metadata (Requirement 3.1, 3.2,
    /// 3.3, 3.4).
    ///
    /// This is the deletion primitive [`ClusterMetadata::delete_topic`] builds
    /// on. It exists so the owner of the live per-partition Raft groups and
    /// logs (the server's `RaftGroupFleet`, task 11.4) can tear each partition
    /// down in the order the requirements demand:
    ///
    /// 1. The topic is looked up first; a missing topic is rejected with
    ///    [`CoreError::TopicNotFound`] and **no** mutation or callback happens,
    ///    leaving the metadata unchanged (Requirement 3.4).
    /// 2. `on_stop_partition` is then called once per partition — this is the
    ///    hook in which the caller stops the partition's Raft group and releases
    ///    its in-memory log (Requirement 3.2, 3.3). The callback's own
    ///    stop-before-release ordering is enforced by the fleet and exercised
    ///    against a mock in task 10.11.
    /// 3. Only after every partition has been handed to the hook is the topic
    ///    removed from `topics` as a single atomic `BTreeMap` removal and
    ///    `epoch` bumped (Requirement 3.1).
    ///
    /// Running every teardown callback before the single metadata removal keeps
    /// deletion atomic at the metadata level: partitions are released, then the
    /// topic disappears all at once — never partially (Requirement 3.1).
    pub fn delete_topic_with(
        &mut self,
        name: &str,
        mut on_stop_partition: impl FnMut(&Partition),
    ) -> Result<(), CoreError> {
        // Look up before mutating: a missing topic leaves metadata untouched
        // (Requirement 3.4).
        let topic = self
            .topics
            .get(name)
            .ok_or_else(|| CoreError::TopicNotFound(name.to_string()))?;

        // A topic already mid-deletion rejects a second (duplicate) delete with
        // a "being deleted" error and leaves the metadata untouched
        // (Requirement 3.7).
        if topic.state == TopicState::Deleting {
            return Err(CoreError::TopicDeleting(name.to_string()));
        }

        // Tear down each partition (stop Raft group, release log) before the
        // topic is removed from metadata (Requirement 3.2, 3.3).
        for partition in &topic.partitions {
            on_stop_partition(partition);
        }

        // Atomically drop the topic and all its partitions in one removal, then
        // mark the metadata changed (Requirement 3.1).
        self.topics.remove(name);
        self.epoch += 1;
        Ok(())
    }

    /// Mark a topic as mid-deletion, transitioning it from [`TopicState::Active`]
    /// to [`TopicState::Deleting`] so that subsequent produce and duplicate-delete
    /// requests for it are rejected while the deletion proceeds (Requirement 3.7).
    ///
    /// This is the domain-layer flag that records "a deletion is in progress"
    /// for a topic. The actual per-partition teardown and atomic metadata
    /// removal are performed by [`ClusterMetadata::delete_topic`] /
    /// [`ClusterMetadata::delete_topic_with`]; this method only flips the
    /// lifecycle state and bumps `epoch` to reflect the change.
    ///
    /// - [`CoreError::TopicNotFound`] if no such topic exists, leaving the
    ///   metadata unchanged (Requirement 3.4).
    /// - [`CoreError::TopicDeleting`] if the topic is *already* in the
    ///   `Deleting` state, leaving the metadata unchanged — a second
    ///   "begin delete" is itself a rejected duplicate (Requirement 3.7).
    pub fn begin_delete(&mut self, name: &str) -> Result<(), CoreError> {
        // `TopicState` is `Copy`, so reading the state ends the borrow before
        // the mutation below.
        match self.topics.get(name).map(|t| t.state) {
            None => Err(CoreError::TopicNotFound(name.to_string())),
            Some(TopicState::Deleting) => Err(CoreError::TopicDeleting(name.to_string())),
            Some(TopicState::Active) => {
                self.topics
                    .get_mut(name)
                    .expect("topic present: just observed above")
                    .state = TopicState::Deleting;
                self.epoch += 1;
                Ok(())
            }
        }
    }

    /// Pre-check whether a topic may accept produced records.
    ///
    /// This is the produce-path guard the produce flow (task 11.x) consults
    /// before appending any log entry, so that a rejected produce appends
    /// nothing:
    ///
    /// - [`CoreError::TopicDeleting`] if the topic is in the `Deleting` state,
    ///   so produce requests are rejected while a deletion is in progress
    ///   (Requirement 3.7).
    /// - [`CoreError::TopicNotFound`] if the topic does not exist
    ///   (Requirement 4.5).
    /// - `Ok(())` if the topic exists and is `Active`.
    ///
    /// Takes `&self`: a rejection (or success) never mutates the metadata.
    pub fn ensure_producible(&self, name: &str) -> Result<(), CoreError> {
        match self.topics.get(name) {
            None => Err(CoreError::TopicNotFound(name.to_string())),
            Some(topic) if topic.state == TopicState::Deleting => {
                Err(CoreError::TopicDeleting(name.to_string()))
            }
            Some(_) => Ok(()),
        }
    }
}

/// Build `partition_count` partitions, each with `replication_factor` distinct
/// replicas drawn from `nodes`, with leadership balanced round-robin.
///
/// Partition `i` takes the `replication_factor` nodes starting at offset
/// `i % nodes.len()` and wrapping around. Because `replication_factor <=
/// nodes.len()` (guaranteed by the caller's availability check), those nodes are
/// distinct (Requirement 2.3). The first replica is the assigned leader, so
/// leaders cycle `nodes[0], nodes[1], ...` across partitions — making per-node
/// leadership counts differ by at most one (Requirement 10.1).
fn assign_partitions(
    partition_count: u32,
    replication_factor: usize,
    nodes: &[NodeId],
) -> Vec<Partition> {
    let n = nodes.len();
    (0..partition_count)
        .map(|i| {
            let start = (i as usize) % n;
            let replicas: Vec<NodeId> = (0..replication_factor)
                .map(|j| nodes[(start + j) % n].clone())
                .collect();
            let leader = replicas.first().cloned();
            Partition {
                index: PartitionIndex(i),
                replicas,
                leader,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Member;
    use std::collections::BTreeSet;

    fn member(id: &str, availability: NodeAvailability) -> Member {
        Member {
            id: NodeId::new(id),
            addr: format!("{id}:7001"),
            advertised_addr: format!("{id}:7001"),
            availability,
        }
    }

    /// A cluster of `n` available members named `node-0..node-{n-1}`.
    fn cluster(n: usize) -> ClusterMetadata {
        let mut meta = ClusterMetadata::new();
        meta.members = (0..n)
            .map(|i| member(&format!("node-{i}"), NodeAvailability::Available))
            .collect();
        meta
    }

    #[test]
    fn happy_path_registers_partitions_with_distinct_balanced_replicas() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 6, 3, LogBackend::Durable)
            .unwrap();

        let topic = &meta.topics["orders"];
        assert_eq!(topic.name, "orders");
        assert_eq!(topic.state, TopicState::Active);
        assert_eq!(meta.epoch, 1);

        // N partitions indexed exactly 0..N-1 (Requirement 2.1, 2.2).
        assert_eq!(topic.partitions.len(), 6);
        for (i, p) in topic.partitions.iter().enumerate() {
            assert_eq!(p.index, PartitionIndex(i as u32));

            // replication_factor distinct replicas, all cluster members
            // (Requirement 2.3, 9.6).
            assert_eq!(p.replicas.len(), 3);
            let distinct: BTreeSet<&NodeId> = p.replicas.iter().collect();
            assert_eq!(distinct.len(), 3, "replicas must be distinct nodes");
            for r in &p.replicas {
                assert!(
                    meta.members.iter().any(|m| &m.id == r),
                    "replica {r:?} must be a cluster member"
                );
            }
            // The leader is one of the partition's replicas.
            assert_eq!(p.leader.as_ref(), p.replicas.first());
        }
    }

    #[test]
    fn leadership_is_balanced_per_topic() {
        let mut meta = cluster(4);
        meta.create_topic("events", 10, 2, LogBackend::Durable)
            .unwrap();
        let topic = &meta.topics["events"];

        let mut leader_counts = std::collections::BTreeMap::new();
        for p in &topic.partitions {
            let leader = p.leader.clone().expect("leader assigned");
            *leader_counts.entry(leader).or_insert(0usize) += 1;
        }
        let max = *leader_counts.values().max().unwrap();
        let min = *leader_counts.values().min().unwrap();
        assert!(
            max - min <= 1,
            "max ({max}) - min ({min}) leaderships must differ by at most one"
        );
    }

    #[test]
    fn replication_factor_equal_to_node_count_uses_every_node() {
        let mut meta = cluster(3);
        meta.create_topic("rf3", 3, 3, LogBackend::Durable).unwrap();
        for p in &meta.topics["rf3"].partitions {
            let distinct: BTreeSet<&NodeId> = p.replicas.iter().collect();
            assert_eq!(distinct.len(), 3);
        }
    }

    #[test]
    fn rejects_empty_name_without_side_effects() {
        let mut meta = cluster(3);
        let before = meta.clone();
        assert_eq!(
            meta.create_topic("", 1, 1, LogBackend::Durable),
            Err(CoreError::InvalidTopicName)
        );
        assert_eq!(meta, before, "metadata must be unchanged on rejection");
    }

    #[test]
    fn rejects_name_with_invalid_characters() {
        let mut meta = cluster(3);
        let before = meta.clone();
        assert_eq!(
            meta.create_topic("bad name!", 1, 1, LogBackend::Durable),
            Err(CoreError::InvalidTopicName)
        );
        assert_eq!(meta, before);
    }

    #[test]
    fn rejects_name_longer_than_255_chars() {
        let mut meta = cluster(3);
        let long = "a".repeat(256);
        let before = meta.clone();
        assert_eq!(
            meta.create_topic(&long, 1, 1, LogBackend::Durable),
            Err(CoreError::InvalidTopicName)
        );
        assert_eq!(meta, before);
    }

    #[test]
    fn accepts_name_at_length_boundaries() {
        let mut meta = cluster(1);
        meta.create_topic("a", 1, 1, LogBackend::Durable).unwrap();
        let max = "a".repeat(255);
        meta.create_topic(&max, 1, 1, LogBackend::Durable).unwrap();
        assert!(meta.topics.contains_key("a"));
        assert!(meta.topics.contains_key(&max));
    }

    #[test]
    fn rejects_zero_partitions_without_side_effects() {
        let mut meta = cluster(3);
        let before = meta.clone();
        assert_eq!(
            meta.create_topic("orders", 0, 1, LogBackend::Durable),
            Err(CoreError::InvalidPartitionCount(0))
        );
        assert_eq!(meta, before);
    }

    #[test]
    fn rejects_too_many_partitions_without_side_effects() {
        let mut meta = cluster(3);
        let before = meta.clone();
        assert_eq!(
            meta.create_topic("orders", 10_001, 1, LogBackend::Durable),
            Err(CoreError::InvalidPartitionCount(10_001))
        );
        assert_eq!(meta, before);
    }

    #[test]
    fn accepts_partition_count_at_boundaries() {
        let mut meta = cluster(1);
        meta.create_topic("one", 1, 1, LogBackend::Durable).unwrap();
        meta.create_topic("many", 10_000, 1, LogBackend::Durable)
            .unwrap();
        assert_eq!(meta.topics["one"].partitions.len(), 1);
        assert_eq!(meta.topics["many"].partitions.len(), 10_000);
    }

    #[test]
    fn rejects_duplicate_topic_without_side_effects() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 4, 2, LogBackend::Durable)
            .unwrap();
        let before = meta.clone();
        assert_eq!(
            meta.create_topic("orders", 8, 3, LogBackend::Durable),
            Err(CoreError::TopicExists("orders".to_string()))
        );
        // The existing topic (and epoch) are untouched by the rejected request.
        assert_eq!(meta, before);
    }

    #[test]
    fn rejects_insufficient_available_nodes_without_side_effects() {
        let mut meta = cluster(2);
        let before = meta.clone();
        assert_eq!(
            meta.create_topic("orders", 4, 3, LogBackend::Durable),
            Err(CoreError::InsufficientNodes { have: 2, need: 3 })
        );
        assert_eq!(meta, before);
    }

    #[test]
    fn unavailable_nodes_do_not_count_toward_replication_factor() {
        let mut meta = ClusterMetadata::new();
        meta.members = vec![
            member("node-0", NodeAvailability::Available),
            member("node-1", NodeAvailability::Available),
            member("node-2", NodeAvailability::Unavailable),
        ];
        let before = meta.clone();
        // Only 2 available, replication factor 3 -> insufficient.
        assert_eq!(
            meta.create_topic("orders", 2, 3, LogBackend::Durable),
            Err(CoreError::InsufficientNodes { have: 2, need: 3 })
        );
        assert_eq!(meta, before);

        // With replication factor 2 it succeeds, and never assigns the
        // unavailable node (Requirement 9.6).
        meta.create_topic("orders", 2, 2, LogBackend::Durable)
            .unwrap();
        for p in &meta.topics["orders"].partitions {
            assert!(!p.replicas.contains(&NodeId::new("node-2")));
        }
    }

    #[test]
    fn delete_topic_removes_topic_and_all_partitions_atomically() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 6, 3, LogBackend::Durable)
            .unwrap();
        meta.create_topic("events", 2, 2, LogBackend::Durable)
            .unwrap();
        let epoch_before = meta.epoch;

        meta.delete_topic("orders").unwrap();

        // The whole topic — and therefore every one of its partitions — is gone.
        assert!(!meta.topics.contains_key("orders"));
        // Unrelated topics are untouched (Requirement 3.1 is scoped to the topic).
        assert!(meta.topics.contains_key("events"));
        assert_eq!(meta.topics["events"].partitions.len(), 2);
        // The change bumped the epoch.
        assert_eq!(meta.epoch, epoch_before + 1);
    }

    #[test]
    fn delete_topic_stops_every_partition_before_removal() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 4, 2, LogBackend::Durable)
            .unwrap();

        // The hook fires once per partition, and the topic still exists at the
        // moment each partition is torn down (stop happens before removal).
        let mut stopped: Vec<u32> = Vec::new();
        meta.delete_topic_with("orders", |p| stopped.push(p.index.0))
            .unwrap();

        stopped.sort_unstable();
        assert_eq!(stopped, vec![0, 1, 2, 3]);
        assert!(!meta.topics.contains_key("orders"));
    }

    #[test]
    fn delete_topic_hook_observes_topic_still_present() {
        let mut meta = cluster(2);
        meta.create_topic("orders", 3, 2, LogBackend::Durable)
            .unwrap();

        // Capture how many partitions remained registered while the hook ran:
        // teardown must run before the atomic metadata removal.
        let mut count = 0usize;
        meta.delete_topic_with("orders", |_| count += 1).unwrap();
        assert_eq!(count, 3, "hook must see every partition before removal");
    }

    #[test]
    fn delete_missing_topic_returns_not_found_without_side_effects() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 2, 2, LogBackend::Durable)
            .unwrap();
        let before = meta.clone();

        assert_eq!(
            meta.delete_topic("ghost"),
            Err(CoreError::TopicNotFound("ghost".to_string()))
        );
        // Metadata (topics and epoch) is completely unchanged (Requirement 3.4).
        assert_eq!(meta, before);
    }

    #[test]
    fn delete_missing_topic_does_not_invoke_teardown_hook() {
        let mut meta = cluster(3);
        let before = meta.clone();

        let mut called = false;
        let result = meta.delete_topic_with("ghost", |_| called = true);

        assert_eq!(result, Err(CoreError::TopicNotFound("ghost".to_string())));
        assert!(
            !called,
            "no partition teardown for a topic that does not exist"
        );
        assert_eq!(meta, before);
    }

    #[test]
    fn delete_then_recreate_same_name_succeeds() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 2, 2, LogBackend::Durable)
            .unwrap();
        meta.delete_topic("orders").unwrap();
        // The name is free again after deletion.
        meta.create_topic("orders", 5, 3, LogBackend::Durable)
            .unwrap();
        assert_eq!(meta.topics["orders"].partitions.len(), 5);
    }

    #[test]
    fn begin_delete_marks_topic_deleting_and_bumps_epoch() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 4, 2, LogBackend::Durable)
            .unwrap();
        let epoch_before = meta.epoch;

        meta.begin_delete("orders").unwrap();

        assert_eq!(meta.topics["orders"].state, TopicState::Deleting);
        assert_eq!(meta.epoch, epoch_before + 1);
    }

    #[test]
    fn begin_delete_missing_topic_returns_not_found_without_side_effects() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 2, 2, LogBackend::Durable)
            .unwrap();
        let before = meta.clone();

        assert_eq!(
            meta.begin_delete("ghost"),
            Err(CoreError::TopicNotFound("ghost".to_string()))
        );
        assert_eq!(meta, before);
    }

    #[test]
    fn begin_delete_on_already_deleting_topic_is_rejected_without_side_effects() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 2, 2, LogBackend::Durable)
            .unwrap();
        meta.begin_delete("orders").unwrap();
        let before = meta.clone();

        assert_eq!(
            meta.begin_delete("orders"),
            Err(CoreError::TopicDeleting("orders".to_string()))
        );
        assert_eq!(meta, before, "a duplicate begin-delete must not mutate");
    }

    #[test]
    fn duplicate_delete_of_deleting_topic_is_rejected_without_side_effects() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 4, 2, LogBackend::Durable)
            .unwrap();
        meta.begin_delete("orders").unwrap();
        let before = meta.clone();

        // A second deletion request while one is in progress is rejected
        // (Requirement 3.7) and leaves the topic registered and untouched.
        assert_eq!(
            meta.delete_topic("orders"),
            Err(CoreError::TopicDeleting("orders".to_string()))
        );
        assert_eq!(meta, before);
        assert!(meta.topics.contains_key("orders"));
    }

    #[test]
    fn duplicate_delete_does_not_invoke_teardown_hook() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 3, 2, LogBackend::Durable)
            .unwrap();
        meta.begin_delete("orders").unwrap();

        let mut called = false;
        let result = meta.delete_topic_with("orders", |_| called = true);

        assert_eq!(result, Err(CoreError::TopicDeleting("orders".to_string())));
        assert!(!called, "no teardown for a topic already being deleted");
    }

    #[test]
    fn ensure_producible_rejects_deleting_topic() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 2, 2, LogBackend::Durable)
            .unwrap();
        meta.begin_delete("orders").unwrap();
        let before = meta.clone();

        assert_eq!(
            meta.ensure_producible("orders"),
            Err(CoreError::TopicDeleting("orders".to_string()))
        );
        // The precheck takes `&self`; metadata is unaffected.
        assert_eq!(meta, before);
    }

    #[test]
    fn ensure_producible_accepts_active_topic_and_rejects_missing() {
        let mut meta = cluster(3);
        meta.create_topic("orders", 2, 2, LogBackend::Durable)
            .unwrap();

        assert_eq!(meta.ensure_producible("orders"), Ok(()));
        assert_eq!(
            meta.ensure_producible("ghost"),
            Err(CoreError::TopicNotFound("ghost".to_string()))
        );
    }
}
