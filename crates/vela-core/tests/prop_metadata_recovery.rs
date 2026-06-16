//! Property test for durable recovery of the cluster metadata catalogue.
//!
//! Feature: per-topic-log-durability, Property 14
//!
//! Property 14: for any sequence of committed [`ClusterCommand`]s on the durable
//! metadata Raft group (`__meta/0`), dropping the group and reopening it on the
//! same data directory — then re-applying the committed prefix — rebuilds a
//! [`ClusterMetadata`] equal to the view held immediately before the restart,
//! including each topic's recorded backend.
//!
//! This is the `vela-core` realization of Requirement 17.4 (the metadata group
//! initializes its commit index from the recovered log and re-applies every
//! committed entry exactly once, in ascending index order, to restore its
//! committed cluster metadata) and of Requirements 18.1 and 18.3 (a cold
//! restart recovers the previously-committed catalogue, and a topic recorded as
//! `Durable`/`InMemory` keeps that recorded backend across the restart).
//!
//! The test drives a real end-to-end restart: it opens a genuine `DurableWal`
//! beneath [`std::env::temp_dir`] with the only consensus-safe policy
//! (`SyncPolicy::Always`, established inside
//! [`MetadataController::recover_durable`]), commits a random sequence of
//! `ClusterCommand`s through the single-node metadata group (the lone replica
//! is its own majority, so each proposal commits immediately and
//! deterministically), folds those committed commands into a reference
//! `ClusterMetadata` to capture the pre-restart view, drops the controller to
//! release the directory lock, then reopens and recovers the group and asserts
//! the rebuilt catalogue equals the reference.
//!
//! Because `vela-core` carries no wire codec, the test supplies its own
//! deterministic, lossless byte codec for `ClusterCommand` and injects the
//! matching decoder into `recover_durable`. Each command is proposed as a
//! `PayloadKind::Cluster` entry carrying the encoded bytes; the reference view
//! is built from the SAME committed entries in commit order via
//! [`apply_command`], so any difference after the restart is a true recovery
//! defect.
//!
//! Case count: each case performs real `fsync` I/O (every `Always` append and
//! commit forces to stable storage), so the proptest case count is held at the
//! project minimum of 100 with deliberately short command sequences, which
//! keeps the suite fast while exercising a wide range of committed catalogues.
//!
//! Validates: Requirements 17.4, 18.1, 18.3

use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proptest::prelude::*;
use vela_core::{
    apply_command, ClusterCommand, ClusterMetadata, LogBackend, MetadataController,
    NodeAvailability, NodeId, Partition, PartitionIndex,
};
use vela_log::{EntryPayload, LogEntry, PayloadKind};
use vela_raft::{Clock, NodeId as RaftNodeId, RaftInput, Role, TimerKind};

/// Monotonic counter making temp-dir names unique within a single process even
/// when two cases start within the same nanosecond.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// An owned temporary directory recursively removed when dropped.
///
/// Cleanup is best-effort: a failure to remove the directory must not mask a
/// test assertion, so the error is ignored. The guard is dropped only after the
/// locally-owned `MetadataController`/`DurableWal` values, so the exclusive
/// directory lock is released before cleanup.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a uniquely-named directory under the system temp directory,
    /// combining the process id, a per-process atomic counter, and the current
    /// nanosecond timestamp so concurrent binaries and repeated runs never
    /// collide.
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the unix epoch")
            .as_nanos();
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("vela-core-it-{tag}-{}-{unique}-{nanos}", process::id());
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

/// A minimal [`Clock`] that never advances on its own; arming a timer is a
/// no-op. The test drives consensus with explicit [`RaftInput`]s, so no real
/// timing is needed and runs stay deterministic.
struct TestClock {
    now: Instant,
}

impl TestClock {
    fn new() -> Self {
        Self {
            now: Instant::now(),
        }
    }
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        self.now
    }

    fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
}

// --- Test-owned, lossless byte codec for `ClusterCommand` -------------------
//
// `vela-core` deliberately holds no wire encoding, so the test controls its own
// deterministic codec and injects the decoder into `recover_durable`. Encoding
// is length-prefixed and self-describing, so any generated command round-trips
// byte-for-byte, including each `CreateTopic`'s recorded backend.

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_bytes(buf, s.as_bytes());
}

fn encode_command(command: &ClusterCommand) -> Vec<u8> {
    let mut buf = Vec::new();
    match command {
        ClusterCommand::CreateTopic {
            name,
            partitions,
            backend,
        } => {
            buf.push(0);
            put_str(&mut buf, name);
            buf.extend_from_slice(&(partitions.len() as u32).to_le_bytes());
            for partition in partitions {
                buf.extend_from_slice(&partition.index.0.to_le_bytes());
                buf.extend_from_slice(&(partition.replicas.len() as u32).to_le_bytes());
                for replica in &partition.replicas {
                    put_str(&mut buf, replica.as_str());
                }
                match &partition.leader {
                    Some(leader) => {
                        buf.push(1);
                        put_str(&mut buf, leader.as_str());
                    }
                    None => buf.push(0),
                }
            }
            buf.push(match backend {
                LogBackend::Durable => 0,
                LogBackend::InMemory => 1,
            });
        }
        ClusterCommand::DeleteTopic { name } => {
            buf.push(1);
            put_str(&mut buf, name);
        }
        ClusterCommand::SetAvailability { node, availability } => {
            buf.push(2);
            put_str(&mut buf, node.as_str());
            buf.push(match availability {
                NodeAvailability::Available => 0,
                NodeAvailability::Unavailable => 1,
            });
        }
    }
    buf
}

/// A forward-only reader over the encoded command bytes.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> u8 {
        let byte = self.data[self.pos];
        self.pos += 1;
        byte
    }

    fn u32(&mut self) -> u32 {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(&self.data[self.pos..self.pos + 4]);
        self.pos += 4;
        u32::from_le_bytes(raw)
    }

    fn bytes(&mut self) -> Vec<u8> {
        let len = self.u32() as usize;
        let value = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        value
    }

    fn string(&mut self) -> String {
        String::from_utf8(self.bytes()).expect("encoded strings are valid UTF-8")
    }
}

fn decode_command(data: &[u8]) -> ClusterCommand {
    let mut reader = Reader { data, pos: 0 };
    match reader.u8() {
        0 => {
            let name = reader.string();
            let partition_count = reader.u32() as usize;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let index = PartitionIndex(reader.u32());
                let replica_count = reader.u32() as usize;
                let mut replicas = Vec::with_capacity(replica_count);
                for _ in 0..replica_count {
                    replicas.push(NodeId::new(reader.string()));
                }
                let leader = if reader.u8() == 1 {
                    Some(NodeId::new(reader.string()))
                } else {
                    None
                };
                partitions.push(Partition {
                    index,
                    replicas,
                    leader,
                });
            }
            let backend = if reader.u8() == 0 {
                LogBackend::Durable
            } else {
                LogBackend::InMemory
            };
            ClusterCommand::CreateTopic {
                name,
                partitions,
                backend,
            }
        }
        1 => ClusterCommand::DeleteTopic {
            name: reader.string(),
        },
        2 => {
            let node = NodeId::new(reader.string());
            let availability = if reader.u8() == 0 {
                NodeAvailability::Available
            } else {
                NodeAvailability::Unavailable
            };
            ClusterCommand::SetAvailability { node, availability }
        }
        other => panic!("unknown command tag {other}"),
    }
}

/// Fold every committed `Cluster` entry of `committed` into `meta` in order,
/// decoding each with the test codec — the same path `recover_durable` takes,
/// so the reference view is built from the actually-committed commands in
/// commit order.
fn fold_committed(meta: &mut ClusterMetadata, committed: &[LogEntry]) {
    for entry in committed {
        if entry.payload.kind == PayloadKind::Cluster {
            apply_command(meta, &decode_command(&entry.payload.bytes));
        }
    }
}

// --- Generators -------------------------------------------------------------

fn node_id_strategy() -> impl Strategy<Value = NodeId> {
    "[a-d]{1,3}".prop_map(NodeId::new)
}

fn partition_strategy() -> impl Strategy<Value = Partition> {
    (
        0u32..4,
        prop::collection::vec(node_id_strategy(), 0..3),
        prop::option::of(node_id_strategy()),
    )
        .prop_map(|(index, replicas, leader)| Partition {
            index: PartitionIndex(index),
            replicas,
            leader,
        })
}

fn backend_strategy() -> impl Strategy<Value = LogBackend> {
    prop_oneof![Just(LogBackend::Durable), Just(LogBackend::InMemory)]
}

fn command_strategy() -> impl Strategy<Value = ClusterCommand> {
    prop_oneof![
        (
            "[a-z]{1,6}",
            prop::collection::vec(partition_strategy(), 0..3),
            backend_strategy(),
        )
            .prop_map(|(name, partitions, backend)| ClusterCommand::CreateTopic {
                name,
                partitions,
                backend,
            }),
        "[a-z]{1,6}".prop_map(|name| ClusterCommand::DeleteTopic { name }),
        (
            node_id_strategy(),
            prop_oneof![
                Just(NodeAvailability::Available),
                Just(NodeAvailability::Unavailable),
            ],
        )
            .prop_map(|(node, availability)| ClusterCommand::SetAvailability {
                node,
                availability
            }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: per-topic-log-durability, Property 14
    #[test]
    fn metadata_recovery_rebuilds_the_identical_catalogue(
        // A short sequence of committed cluster commands per case; kept small
        // because every committed command triggers a real fsync under the
        // Always policy the metadata group uses.
        commands in prop::collection::vec(command_strategy(), 1..9),
    ) {
        let tmp = TempDir::new("meta-recover");

        // The catalogue view held immediately before the simulated restart,
        // captured by folding the actually-committed commands in commit order.
        let reference: ClusterMetadata;

        // --- run: commit the command sequence on a fresh durable group. -----
        {
            let mut clock = TestClock::new();
            // Open a fresh durable __meta/0 group at the temp path. A brand-new
            // log has no committed entries, so the recovered catalogue is empty.
            let mut controller = MetadataController::recover_durable(
                RaftNodeId(0),
                Vec::new(),
                tmp.path(),
                decode_command,
            )
            .expect("opening a fresh durable metadata group should succeed");
            prop_assert!(controller.metadata().topics.is_empty());

            // Single-node group: the lone self-vote is a majority, so the
            // election makes the group leader and each proposal commits in the
            // same step.
            let election = controller
                .step(RaftInput::Tick(TimerKind::Election), &mut clock)
                .expect("the metadata group is hosted");
            prop_assert_eq!(election.role_change, Some(Role::Leader));

            let mut reference_meta = ClusterMetadata::new();
            // The election commits a leader Noop, which carries no catalogue
            // change; fold it for completeness (it is skipped as non-Cluster).
            fold_committed(&mut reference_meta, &election.committed);

            for command in &commands {
                let bytes = encode_command(command);
                let out = controller
                    .step(
                        RaftInput::Propose(EntryPayload::new(PayloadKind::Cluster, bytes)),
                        &mut clock,
                    )
                    .expect("the metadata group is hosted");
                fold_committed(&mut reference_meta, &out.committed);
            }

            reference = reference_meta;

            // `controller` (and its DurableWal) drop here, releasing the lock.
        }

        // --- restart: reopen the same data directory and recover. -----------
        let recovered = MetadataController::recover_durable(
            RaftNodeId(0),
            Vec::new(),
            tmp.path(),
            decode_command,
        )
        .expect("reopening the durable metadata group should succeed");

        // 17.4 / 18.1: the committed prefix is replayed in ascending index
        // order to rebuild a catalogue equal to the pre-restart view (members,
        // topics, and epoch).
        prop_assert_eq!(recovered.metadata(), &reference);

        // 18.3: each recovered topic keeps the exact backend recorded when it
        // was created (already implied by the equality above, asserted
        // explicitly to pin the property's intent).
        for (name, topic) in &reference.topics {
            let recovered_topic = recovered
                .metadata()
                .topics
                .get(name)
                .expect("every reference topic must be recovered");
            prop_assert_eq!(recovered_topic.backend, topic.backend);
        }
    }
}
