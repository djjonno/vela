//! Deterministic, lossless byte codec for [`ClusterCommand`].
//!
//! `vela-core` deliberately carries no wire encoding — encoding/decoding a
//! committed metadata command's bytes is the *caller's* concern (see
//! [`MetadataController::recover_durable`](vela_core::MetadataController::recover_durable)
//! and [`recover_durable_with_log`](vela_core::MetadataController::recover_durable_with_log),
//! both of which take an injected decoder). The DST harness *is* that caller, so
//! it owns the codec here.
//!
//! The encoding is length-prefixed and self-describing, so every
//! [`ClusterCommand`] round-trips byte-for-byte — including a `CreateTopic`'s
//! partitions, replica sets, per-partition leaders, and recorded
//! [`LogBackend`]. The harness proposes each committed metadata change as a
//! [`PayloadKind::Cluster`](vela_log::PayloadKind) entry carrying
//! [`encode_cluster_command`]'s bytes, and feeds [`decode_cluster_command`] to
//! the recovery path so a restarted node rebuilds the identical catalogue
//! (Requirement 3.4, and the durable-restart recovery of 11.4).
//!
//! Determinism: the codec is a pure function of its input — no maps, no
//! timestamps, no unseeded ordering — so identical commands always produce
//! identical bytes, preserving a run's reproducibility (Requirement 1).

use vela_core::{ClusterCommand, LogBackend, NodeAvailability, NodeId, Partition, PartitionIndex};

/// Append a length-prefixed (`u32` little-endian length) byte string to `buf`.
fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Append a length-prefixed UTF-8 string to `buf`.
fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_bytes(buf, s.as_bytes());
}

/// Encode a [`ClusterCommand`] into its self-describing byte form.
///
/// The first byte tags the variant (`0` = `CreateTopic`, `1` = `DeleteTopic`,
/// `2` = `SetAvailability`); the remainder is the length-prefixed payload. The
/// inverse is [`decode_cluster_command`].
#[must_use]
pub fn encode_cluster_command(command: &ClusterCommand) -> Vec<u8> {
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

/// A forward-only reader over encoded command bytes.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
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

/// Decode a [`ClusterCommand`] previously produced by
/// [`encode_cluster_command`].
///
/// # Panics
///
/// Panics on bytes this codec did not produce (an unknown variant tag or a
/// truncated buffer). The harness controls both ends of the codec, so a failure
/// here is a harness bug, surfaced loudly rather than silently mis-decoded.
#[must_use]
pub fn decode_cluster_command(data: &[u8]) -> ClusterCommand {
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
        other => panic!("unknown cluster-command tag {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(index: u32, replicas: &[&str], leader: Option<&str>) -> Partition {
        Partition {
            index: PartitionIndex(index),
            replicas: replicas.iter().map(|r| NodeId::new(*r)).collect(),
            leader: leader.map(NodeId::new),
        }
    }

    /// Assert a command survives an encode/decode round trip unchanged.
    fn assert_round_trips(command: &ClusterCommand) {
        let bytes = encode_cluster_command(command);
        assert_eq!(&decode_cluster_command(&bytes), command);
    }

    #[test]
    fn create_topic_round_trips_with_partitions_and_backend() {
        assert_round_trips(&ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![
                partition(0, &["node-0", "node-1"], Some("node-0")),
                partition(1, &["node-1", "node-2"], None),
            ],
            backend: LogBackend::Durable,
        });
        // The in-memory backend is preserved too.
        assert_round_trips(&ClusterCommand::CreateTopic {
            name: "events".to_string(),
            partitions: vec![partition(0, &["node-0"], Some("node-0"))],
            backend: LogBackend::InMemory,
        });
    }

    #[test]
    fn create_topic_with_no_partitions_round_trips() {
        assert_round_trips(&ClusterCommand::CreateTopic {
            name: "empty".to_string(),
            partitions: Vec::new(),
            backend: LogBackend::Durable,
        });
    }

    #[test]
    fn delete_topic_round_trips() {
        assert_round_trips(&ClusterCommand::DeleteTopic {
            name: "orders".to_string(),
        });
    }

    #[test]
    fn set_availability_round_trips_both_states() {
        assert_round_trips(&ClusterCommand::SetAvailability {
            node: NodeId::new("node-0"),
            availability: NodeAvailability::Available,
        });
        assert_round_trips(&ClusterCommand::SetAvailability {
            node: NodeId::new("node-1"),
            availability: NodeAvailability::Unavailable,
        });
    }

    #[test]
    fn encoding_is_deterministic_for_equal_commands() {
        // A pure function of the input: equal commands encode to identical bytes,
        // preserving run reproducibility (Requirement 1).
        let command = ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![partition(2, &["node-2", "node-0"], Some("node-2"))],
            backend: LogBackend::Durable,
        };
        assert_eq!(
            encode_cluster_command(&command),
            encode_cluster_command(&command)
        );
    }
}
