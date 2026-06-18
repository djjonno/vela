//! Off-loop partition reconciler (Requirement 6).
//!
//! After a committed `ClusterCommand` is applied to a node's served
//! [`ClusterMetadata`], the node's running partition drivers must be brought
//! into line with the new catalogue: a driver is started for every partition
//! this node now replicates and stopped for every partition that is gone or no
//! longer assigned to it. That alignment is **reconciliation**.
//!
//! Reconciliation runs as its **own** `tokio` task, driven by a reconcile
//! signal, rather than inline on the metadata Raft loop (design §5, H1).
//! Applying a committed metadata entry only updates the served catalogue and
//! pokes the signal; the comparatively slow work here — opening durable logs,
//! registering peers, and spawning/stopping driver tasks — therefore never
//! blocks metadata heartbeats or elections and so cannot trigger a spurious
//! metadata election.
//!
//! The pass itself is an **idempotent diff** of desired vs running drivers
//! ([`plan_reconcile`]), so collapsing several pending signals into one pass is
//! safe and a periodic re-run only re-attempts work that still needs doing
//! (Requirement 6.8, wired later). The reserved metadata group `("__meta", 0)`
//! is never started or stopped here (Requirement 6.6); it is driven separately.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::time::{interval, MissedTickBehavior};
use vela_core::{ClusterMetadata, Partition};

use crate::driver::ReconcileSignal;
use crate::membership::HEARTBEAT_INTERVAL;
use crate::node::NodeShared;

/// The reserved topic name of the dedicated metadata Raft group.
///
/// Reconciliation operates only on client topic partitions and must never start
/// or stop this group (Requirement 6.6); it is driven by its own metadata
/// driver, not the reconciler.
const META_TOPIC: &str = "__meta";

/// The set of driver changes one reconciliation pass should make.
///
/// Computed purely from a served catalogue, the running-driver set, and this
/// node's identity by [`plan_reconcile`], so the diff logic is testable without
/// a runtime, a real driver, or any I/O.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ReconcilePlan {
    /// Partitions to start a driver for: `desired \ running` (Requirement 6.1).
    /// Each carries its full [`Partition`] so the spawn can register the
    /// partition's replica peers (Requirement 6.4).
    pub(crate) spawn: Vec<(String, Partition)>,
    /// `(topic, partition)` keys whose driver must stop: `running \ desired`
    /// (Requirement 6.2).
    pub(crate) stop: Vec<(String, u32)>,
}

/// Compute the reconcile diff for `self_id` against `metadata` and the `running`
/// driver set.
///
/// `desired = {(topic, p.index) : self_id in p.replicas}` over the served
/// catalogue (Requirement 6.5), excluding the reserved `__meta` group. The plan
/// then starts exactly `desired \ running`, stops exactly `running \ desired`,
/// and leaves the intersection untouched (Requirement 6.1, 6.2, 6.3). The
/// `("__meta", 0)` group is excluded from both sides, so it is never started or
/// stopped (Requirement 6.6).
pub(crate) fn plan_reconcile(
    metadata: &ClusterMetadata,
    running: &HashSet<(String, u32)>,
    self_id: &str,
) -> ReconcilePlan {
    // desired: the partitions whose replica set contains this node, with the
    // metadata group explicitly excluded (Requirement 6.5, 6.6). Track the keys
    // for the stop diff and carry each Partition for the spawn (Requirement
    // 6.4).
    let mut desired_keys: HashSet<(String, u32)> = HashSet::new();
    let mut spawn: Vec<(String, Partition)> = Vec::new();
    for topic in metadata.topics.values() {
        if topic.name == META_TOPIC {
            continue;
        }
        for partition in &topic.partitions {
            if partition.replicas.iter().any(|r| r.as_str() == self_id) {
                let key = (topic.name.clone(), partition.index.0);
                desired_keys.insert(key.clone());
                // Start a driver only for a partition not already running
                // (Requirement 6.1); an already-running one is left untouched
                // (Requirement 6.3).
                if !running.contains(&key) {
                    spawn.push((topic.name.clone(), partition.clone()));
                }
            }
        }
    }

    // stop: every running driver no longer desired, never the metadata group
    // (Requirement 6.2, 6.6).
    let mut stop: Vec<(String, u32)> = running
        .iter()
        .filter(|key| key.0 != META_TOPIC && !desired_keys.contains(*key))
        .cloned()
        .collect();
    // Deterministic order so a pass is reproducible regardless of HashSet
    // iteration order.
    stop.sort();

    ReconcilePlan { spawn, stop }
}

/// Run a single reconciliation pass against `node`'s current state.
///
/// Snapshots the running-driver set and the served catalogue (releasing each
/// lock before the next is taken, so this never holds both at once and cannot
/// deadlock against [`NodeShared::spawn_partition`], which takes them in the
/// opposite order), computes the diff, then applies it by reusing
/// `spawn_partition` / `stop_partition`.
///
/// `spawn_partition` registers every other replica's transport address before
/// the driver issues any Raft RPC (Requirement 6.4) and, on a durable-log-open
/// failure, leaves that partition **unstarted** while returning a structured
/// error. Here that error is logged and the pass **continues** with the
/// remaining partitions (Requirement 6.7); a later periodic pass re-attempts
/// the unstarted partition (Requirement 6.8, wired later).
pub(crate) fn reconcile(node: &Arc<NodeShared>) {
    // Snapshot the running set, releasing the partitions lock before taking the
    // metadata lock (lock-ordering safety, see the doc comment).
    let running: HashSet<(String, u32)> = {
        node.partitions
            .lock()
            .expect("partitions mutex poisoned")
            .keys()
            .cloned()
            .collect()
    };
    let plan = {
        let metadata = node.metadata.lock().expect("metadata mutex poisoned");
        plan_reconcile(&metadata, &running, &node.self_id)
    };

    for (topic, partition) in &plan.spawn {
        // A durable-log-open failure leaves this partition unstarted with a
        // structured error; keep reconciling the rest (Requirement 6.7).
        if let Err(err) = node.spawn_partition(topic, partition) {
            tracing::error!(
                topic = %topic,
                partition = partition.index.0,
                %err,
                "reconcile: failed to start partition driver; leaving it unstarted"
            );
        }
    }
    for (topic, partition) in &plan.stop {
        node.stop_partition(topic, *partition);
    }
}

/// Spawn the reconciler as its own `tokio` task, consuming the reconcile signal.
///
/// The task runs OFF the metadata Raft loop (design §5, H1): it waits to be
/// poked, then runs one idempotent [`reconcile`] pass. The [`ReconcileSignal`]
/// ([`tokio::sync::Notify`]) **coalesces** pokes — several raised before the
/// task next waits collapse into a single wakeup — which is correct because
/// each pass re-diffs the current state, so collapsing N pokes into one pass
/// loses nothing (design §5). The task runs for the node's lifetime; the shared
/// signal is also held by the metadata sink that pokes it.
pub(crate) fn spawn_reconciler(node: Arc<NodeShared>, signal: ReconcileSignal) {
    tokio::spawn(async move {
        loop {
            signal.notified().await;
            reconcile(&node);
        }
    });
}

/// Spawn a periodic tick that re-pokes the off-loop reconciler (Requirement 6.8).
///
/// A partition left **unstarted** by a *transient* durable-log-open failure
/// (Requirement 6.7) would otherwise have to wait for the next committed
/// metadata change before [`reconcile`] re-attempts it. This ticker re-pokes the
/// shared [`ReconcileSignal`] on a fixed cadence — reusing the membership
/// heartbeat cadence ([`HEARTBEAT_INTERVAL`]) rather than introducing a second
/// timer constant — so [`reconcile`] re-runs periodically and keeps re-trying
/// the unstarted partition until it starts or is no longer assigned to this node
/// (its replica set no longer contains it, so the diff simply drops it).
///
/// Re-running is safe because each pass is an **idempotent diff** of desired vs
/// running drivers ([`plan_reconcile`]): a tick that finds nothing outstanding
/// is a no-op, and an already-running partition is left untouched (design §5,
/// Requirement 6.3, 6.8). The tick only pokes the same signal the
/// [`MetadataSink`](crate::driver::MetadataSink) pokes, and `Notify` coalesces a
/// tick raised while a pass is mid-flight into a single follow-up pass, so the
/// periodic tick never queues redundant work.
///
/// Like the reconciler it drives, this runs OFF the metadata Raft loop and for
/// the node's lifetime; the signal is shared (held by the sink, the reconciler,
/// and this ticker), hence an [`Arc`].
pub(crate) fn spawn_reconcile_ticker(signal: ReconcileSignal) {
    tokio::spawn(async move {
        let mut ticker = interval(HEARTBEAT_INTERVAL);
        // After a delayed tick, resume the cadence without bursting to catch up,
        // matching the membership loop's behaviour.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // `interval`'s first tick fires immediately; the post-recovery reconcile
        // has already run, so consume it and re-poke only on later cadence ticks.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            signal.notify_one();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use proptest::prelude::*;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;
    use vela_core::{LogBackend, NodeId, PartitionIndex, Topic, TopicState};

    use crate::config::Config;
    use crate::paths::partition_data_path;

    /// A partition with the given index replicated by `replicas`.
    fn partition(index: u32, replicas: &[&str]) -> Partition {
        Partition {
            index: PartitionIndex(index),
            replicas: replicas.iter().map(|r| NodeId::new(*r)).collect(),
            leader: None,
        }
    }

    /// A catalogue holding one topic with the given partitions.
    fn catalogue_with(topic: &str, partitions: Vec<Partition>) -> ClusterMetadata {
        let mut metadata = ClusterMetadata::new();
        metadata.topics.insert(
            topic.to_string(),
            Topic {
                name: topic.to_string(),
                partitions,
                state: TopicState::Active,
                backend: LogBackend::Durable,
            },
        );
        metadata
    }

    fn running_set(keys: &[(&str, u32)]) -> HashSet<(String, u32)> {
        keys.iter().map(|(t, p)| (t.to_string(), *p)).collect()
    }

    #[test]
    fn spawns_assigned_partitions_not_already_running() {
        // Two partitions assigned to node-a; one already running. Only the
        // missing one is spawned (Requirement 6.1), the running one is left
        // untouched (Requirement 6.3).
        let metadata = catalogue_with(
            "orders",
            vec![partition(0, &["node-a"]), partition(1, &["node-a"])],
        );
        let running = running_set(&[("orders", 0)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert_eq!(plan.spawn.len(), 1);
        assert_eq!(plan.spawn[0].0, "orders");
        assert_eq!(plan.spawn[0].1.index, PartitionIndex(1));
        assert!(plan.stop.is_empty());
    }

    #[test]
    fn does_not_spawn_partitions_not_assigned_to_self() {
        // The partition's replica set does not contain this node, so nothing is
        // started for it (Requirement 6.5).
        let metadata = catalogue_with("orders", vec![partition(0, &["node-b", "node-c"])]);
        let running = running_set(&[]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert!(plan.spawn.is_empty());
        assert!(plan.stop.is_empty());
    }

    #[test]
    fn stops_running_partitions_no_longer_desired() {
        // A driver is running for a partition absent from the served catalogue,
        // and another whose replica set no longer contains this node; both stop
        // (Requirement 6.2).
        let metadata = catalogue_with("orders", vec![partition(0, &["node-a"])]);
        let running = running_set(&[("orders", 0), ("orders", 1), ("ghost", 0)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert!(plan.spawn.is_empty());
        assert_eq!(
            plan.stop,
            vec![("ghost".to_string(), 0), ("orders".to_string(), 1)]
        );
    }

    #[test]
    fn never_starts_or_stops_the_metadata_group() {
        // The running metadata group is never stopped even though it is not a
        // client topic in the catalogue, and a `__meta` topic in the catalogue
        // is never started (Requirement 6.6).
        let mut metadata = catalogue_with("orders", vec![partition(0, &["node-a"])]);
        metadata.topics.insert(
            META_TOPIC.to_string(),
            Topic {
                name: META_TOPIC.to_string(),
                partitions: vec![partition(0, &["node-a"])],
                state: TopicState::Active,
                backend: LogBackend::Durable,
            },
        );
        let running = running_set(&[(META_TOPIC, 0), ("orders", 0)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert!(
            plan.spawn.iter().all(|(t, _)| t != META_TOPIC),
            "the metadata group is never spawned by the reconciler"
        );
        assert!(
            plan.stop.iter().all(|(t, _)| t != META_TOPIC),
            "the metadata group is never stopped by the reconciler"
        );
    }

    #[test]
    fn intersection_is_left_untouched() {
        // Every desired partition is already running and nothing else is: an
        // empty plan (Requirement 6.3).
        let metadata = catalogue_with(
            "orders",
            vec![partition(0, &["node-a"]), partition(1, &["node-a"])],
        );
        let running = running_set(&[("orders", 0), ("orders", 1)]);

        let plan = plan_reconcile(&metadata, &running, "node-a");

        assert_eq!(plan, ReconcilePlan::default());
    }

    /// The pool of node identities replica sets and `self_id` are drawn from.
    const NODES: &[&str] = &["node-a", "node-b", "node-c", "node-d"];
    /// The pool of topic names. Includes [`META_TOPIC`] so the generated
    /// catalogue and running set both exercise the metadata-group exclusion.
    const TOPIC_NAMES: &[&str] = &["orders", "events", "logs", META_TOPIC];
    /// Exclusive upper bound on generated partition indices (kept small so
    /// catalogues and running sets overlap and cases stay fast).
    const PART_INDEX_MAX: u32 = 4;

    /// A distinct, possibly-empty replica set drawn from [`NODES`].
    fn replicas_strategy() -> impl Strategy<Value = Vec<NodeId>> {
        prop::collection::hash_set(0usize..NODES.len(), 0..=NODES.len())
            .prop_map(|idxs| idxs.into_iter().map(|i| NodeId::new(NODES[i])).collect())
    }

    /// An arbitrary catalogue: up to four topics drawn from [`TOPIC_NAMES`],
    /// each with up to four partitions keyed by distinct index. Using maps keeps
    /// topic names and per-topic partition indices unique, so each desired key
    /// is generated at most once.
    fn catalogue_strategy() -> impl Strategy<Value = ClusterMetadata> {
        let partitions =
            prop::collection::hash_map(0u32..PART_INDEX_MAX, replicas_strategy(), 0..=4);
        prop::collection::hash_map(
            prop::sample::select(TOPIC_NAMES.to_vec()),
            partitions,
            0..=4,
        )
        .prop_map(build_catalogue)
    }

    /// Build a catalogue from generated `(topic name -> index -> replicas)` data.
    fn build_catalogue(raw: HashMap<&'static str, HashMap<u32, Vec<NodeId>>>) -> ClusterMetadata {
        let mut metadata = ClusterMetadata::new();
        for (name, parts) in raw {
            let partitions = parts
                .into_iter()
                .map(|(index, replicas)| Partition {
                    index: PartitionIndex(index),
                    replicas,
                    leader: None,
                })
                .collect();
            metadata.topics.insert(
                name.to_string(),
                Topic {
                    name: name.to_string(),
                    partitions,
                    state: TopicState::Active,
                    backend: LogBackend::Durable,
                },
            );
        }
        metadata
    }

    /// An arbitrary running-driver set drawn from the same topic/index space as
    /// the catalogue (so it overlaps desired), including possible `__meta` keys.
    fn running_strategy() -> impl Strategy<Value = HashSet<(String, u32)>> {
        prop::collection::hash_set(
            (
                prop::sample::select(TOPIC_NAMES.to_vec()),
                0u32..PART_INDEX_MAX,
            ),
            0..=8,
        )
        .prop_map(|set| set.into_iter().map(|(t, p)| (t.to_string(), p)).collect())
    }

    /// The partitions assigned to `self_id` in `metadata`, excluding the
    /// reserved metadata group — the `desired` set recomputed independently of
    /// [`plan_reconcile`] for the property below.
    fn desired_keys(metadata: &ClusterMetadata, self_id: &str) -> HashSet<(String, u32)> {
        let mut desired = HashSet::new();
        for topic in metadata.topics.values() {
            if topic.name == META_TOPIC {
                continue;
            }
            for partition in &topic.partitions {
                if partition.replicas.iter().any(|r| r.as_str() == self_id) {
                    desired.insert((topic.name.clone(), partition.index.0));
                }
            }
        }
        desired
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Property 2: Reconciler diff correctness.
        ///
        /// Over random catalogues, running-driver sets, and node identities, the
        /// plan starts exactly `desired \ running`, stops exactly `running \
        /// desired`, leaves the intersection untouched, and never starts or
        /// stops the `("__meta", 0)` group.
        ///
        /// **Validates: Requirements 6.1, 6.2, 6.3, 6.5, 6.6**
        #[test]
        fn reconciler_diff_matches_desired_running_set_difference(
            metadata in catalogue_strategy(),
            running in running_strategy(),
            self_id in prop::sample::select(NODES.to_vec()),
        ) {
            let plan = plan_reconcile(&metadata, &running, self_id);

            let desired = desired_keys(&metadata, self_id);

            // The keys the plan would start, as a set (the generators make these
            // unique, so the count must match too).
            let spawn_keys: HashSet<(String, u32)> = plan
                .spawn
                .iter()
                .map(|(t, p)| (t.clone(), p.index.0))
                .collect();
            prop_assert_eq!(
                spawn_keys.len(),
                plan.spawn.len(),
                "spawn set must not contain duplicate keys"
            );

            // spawn == desired \ running (Requirement 6.1, 6.5).
            let expected_spawn: HashSet<(String, u32)> =
                desired.difference(&running).cloned().collect();
            prop_assert_eq!(&spawn_keys, &expected_spawn);

            // stop == running \ desired, excluding the metadata group, sorted
            // (Requirement 6.2, 6.6).
            let mut expected_stop: Vec<(String, u32)> = running
                .iter()
                .filter(|key| key.0 != META_TOPIC && !desired.contains(*key))
                .cloned()
                .collect();
            expected_stop.sort();
            prop_assert_eq!(&plan.stop, &expected_stop);

            // The intersection is left untouched: neither started nor stopped
            // (Requirement 6.3).
            for key in desired.intersection(&running) {
                prop_assert!(
                    !spawn_keys.contains(key),
                    "already-running desired partition {:?} must not be spawned",
                    key
                );
                prop_assert!(
                    !plan.stop.contains(key),
                    "already-running desired partition {:?} must not be stopped",
                    key
                );
            }

            // The metadata group is never started or stopped (Requirement 6.6).
            prop_assert!(
                plan.spawn.iter().all(|(t, _)| t != META_TOPIC),
                "the metadata group is never spawned"
            );
            prop_assert!(
                plan.stop.iter().all(|(t, _)| t != META_TOPIC),
                "the metadata group is never stopped"
            );

            // Each spawned entry carries the exact Partition from the catalogue,
            // so the spawn can register that partition's replica peers.
            for (topic, partition) in &plan.spawn {
                let cataloged = metadata
                    .topics
                    .get(topic)
                    .expect("a spawned topic must exist in the catalogue");
                prop_assert!(
                    cataloged.partitions.iter().any(|p| p == partition),
                    "spawned partition {:?} must match the catalogue entry",
                    partition.index
                );
            }
        }
    }

    // --- Spawn-failure skip path (task 2.3, Requirement 6.7) -----------------
    //
    // The diff tests above cover [`plan_reconcile`]. The test below drives the
    // full [`reconcile`] pass against a real [`NodeShared`] to prove the
    // skip-on-failure-continue behaviour: a partition whose durable log cannot
    // be opened is left unstarted and recorded as an error, while the remaining
    // assigned partitions still start.

    /// Monotonic counter making temp-dir names unique within a process.
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// An owned temporary directory recursively removed when dropped.
    ///
    /// The path is computed but not created (the durable metadata group and the
    /// partition WALs create what they need). Cleanup on drop is best-effort so
    /// a removal failure never masks an assertion; the guard drops after the
    /// node, so each driver's WAL lock is released before the directory is
    /// removed.
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
            let name = format!(
                "vela-server-reconcile-{tag}-{}-{unique}-{nanos}",
                process::id()
            );
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

    /// A single-node `velad` [`Config`] rooted at `data_dir`.
    fn node_config(node_id: &str, addr: &str, data_dir: &Path) -> Config {
        Config {
            node_id: NodeId::new(node_id),
            listen_addr: addr.parse().expect("valid addr"),
            peers: Vec::new(),
            replication_factor: 1,
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Record a durable `topic` with `partitions` in the node's served view, so
    /// [`reconcile`] sees it as desired and tries to start its drivers.
    fn record_durable_topic(node: &NodeShared, topic: &str, partitions: &[Partition]) {
        let mut metadata = node.metadata.lock().expect("metadata mutex poisoned");
        metadata.topics.insert(
            topic.to_string(),
            Topic {
                name: topic.to_string(),
                partitions: partitions.to_vec(),
                state: TopicState::Active,
                backend: LogBackend::Durable,
            },
        );
    }

    /// A captured `tracing` event: its level plus the `topic`/`partition` fields
    /// the structured reconcile errors carry.
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
            // `topic = %t` (Display) is delivered here; keep the first capture so
            // an explicit `record_str` (string fields) still takes precedence.
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
    async fn reconcile_skips_partition_whose_log_fails_to_open_and_continues() {
        // Two durable partitions of "orders", both assigned to this node. One
        // partition's durable log is made impossible to open; the reconciler
        // must leave it unstarted with a recorded error yet still start the
        // other (Requirement 6.7).
        let tmp = TempDir::new("spawn-failure-skip");
        let node = NodeShared::new(&node_config("node-a", "127.0.0.1:7050", tmp.path()))
            .expect("node startup succeeds");

        let p0 = partition(0, &["node-a"]);
        let p1 = partition(1, &["node-a"]);
        record_durable_topic(&node, "orders", &[p0, p1]);

        // Sabotage partition 0's derived log path: place a regular FILE where the
        // WAL must create its data directory, so `DurableWal::open` (and thus
        // `spawn_partition`) fails for partition 0 only.
        let p0_path = partition_data_path(tmp.path(), "orders", 0);
        fs::create_dir_all(p0_path.parent().expect("partition path has a parent"))
            .expect("create the topic directory");
        fs::write(&p0_path, b"not a directory").expect("write the sabotaging file");

        // Capture the structured error the reconciler emits for the skipped
        // partition.
        let layer = CaptureLayer::default();
        let events = layer.events.clone();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || reconcile(&node));

        // The partition whose durable log could not open is left UNSTARTED — no
        // in-memory fallback, no driver hosted (Requirement 6.7).
        assert!(
            node.handle("orders", 0).is_none(),
            "the partition whose log failed to open must be left unstarted"
        );

        // That (topic, partition) was recorded as an ERROR (Requirement 6.7).
        {
            let events = events.lock().unwrap();
            let err = events
                .iter()
                .find(|e| e.level == Some(Level::ERROR) && e.partition == Some(0))
                .expect("an ERROR identifying the skipped partition must be recorded");
            assert_eq!(err.topic.as_deref(), Some("orders"));
            assert_eq!(err.partition, Some(0));
        }

        // Reconciliation CONTINUED past the failure: the remaining assigned
        // partition still has a running driver (Requirement 6.7).
        assert!(
            node.handle("orders", 1).is_some(),
            "the remaining partition must still be started after the skip"
        );

        node.stop_partition("orders", 1);
    }

    // --- Retry of an unstarted partition (task 12.2, Requirement 6.8) --------
    //
    // The skip test above proves a single pass leaves a log-open failure
    // unstarted and continues. This test proves the *retry* contract: a later
    // pass re-attempts the unstarted partition and starts it once its log can be
    // opened, and the retry loop stops once the partition is no longer assigned
    // to this node.

    #[tokio::test]
    async fn reconcile_retries_unstarted_partition_then_drops_it_once_unassigned() {
        // Requirement 6.8: a partition left unstarted by a transient
        // durable-log-open failure is re-attempted on the next reconcile pass and
        // starts once its log can be opened, then is dropped (not retried) once it
        // is no longer assigned to this node.
        //
        // The periodic ticker (`spawn_reconcile_ticker`) only re-pokes the shared
        // reconcile signal on the membership cadence, which just makes
        // `reconcile` run again; driving `reconcile(&node)` directly per "tick" is
        // therefore equivalent to the ticker firing and far faster than waiting on
        // the real 1s timer.
        let tmp = TempDir::new("retry-unstarted");
        let node = NodeShared::new(&node_config("node-a", "127.0.0.1:7051", tmp.path()))
            .expect("node startup succeeds");

        // One durable partition assigned to this node.
        record_durable_topic(&node, "orders", &[partition(0, &["node-a"])]);

        // Sabotage the partition's derived log path so the first start attempt
        // fails: place a regular FILE where the WAL must create its data
        // directory, so `DurableWal::open` (and thus `spawn_partition`) fails.
        let p0_path = partition_data_path(tmp.path(), "orders", 0);
        fs::create_dir_all(p0_path.parent().expect("partition path has a parent"))
            .expect("create the topic directory");
        fs::write(&p0_path, b"not a directory").expect("write the sabotaging file");

        // First tick: the partition's log cannot be opened, so it is left
        // unstarted (Requirement 6.7).
        reconcile(&node);
        assert!(
            node.handle("orders", 0).is_none(),
            "a partition whose log fails to open is left unstarted on the first pass"
        );

        // Make the log openable, then run the next tick: the still-assigned
        // partition is re-attempted and now starts (Requirement 6.8).
        fs::remove_file(&p0_path).expect("remove the sabotaging file");
        reconcile(&node);
        assert!(
            node.handle("orders", 0).is_some(),
            "the unstarted partition must start on a later pass once its log opens"
        );

        // Unassign the partition: its replica set no longer contains this node.
        // The next pass stops the now-undesired driver (Requirement 6.2) and the
        // retry loop drops it rather than re-attempting a start (Requirement 6.8).
        record_durable_topic(&node, "orders", &[partition(0, &["node-b"])]);
        reconcile(&node);
        assert!(
            node.handle("orders", 0).is_none(),
            "an unassigned partition is stopped and not retried"
        );

        // A further tick does not resurrect it: once unassigned it stays dropped.
        reconcile(&node);
        assert!(
            node.handle("orders", 0).is_none(),
            "an unassigned partition is never re-started by a later pass"
        );
    }
}
