//! Node identity mapping between the domain and consensus layers.
//!
//! `vela-core` identifies nodes by a stable **string** identity
//! ([`vela_core::NodeId`]); `vela-raft` keys its per-peer state by a numeric
//! [`RaftNodeId`](vela_raft::NodeId). The server sits at that seam and must map
//! between the two whenever it translates a Raft RPC to or from the wire (the
//! proto RPCs carry string `candidate_id` / `leader_id`, while the in-memory
//! consensus messages carry the numeric id).
//!
//! The mapping is derived **deterministically** from the string id via a
//! dependency-free FNV-1a hash, so every node in the cluster agrees on the
//! numeric id for a given string id without any coordination. Collisions across
//! the small node counts of this milestone are vanishingly unlikely; a durable,
//! larger-scale build can replace this with an assigned-id scheme behind the
//! same functions.

use vela_raft::NodeId as RaftNodeId;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Map a domain string node id to the numeric [`RaftNodeId`] used inside a
/// partition's Raft group.
///
/// Deterministic and dependency-free (FNV-1a over the id bytes), so the same
/// string id always yields the same numeric id on every node, letting peers
/// address one another's replicas consistently.
pub fn raft_node_id(string_id: &str) -> RaftNodeId {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in string_id.as_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    RaftNodeId(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_is_deterministic() {
        assert_eq!(raft_node_id("node-a"), raft_node_id("node-a"));
        assert_eq!(raft_node_id("node-b"), raft_node_id("node-b"));
    }

    #[test]
    fn distinct_ids_map_to_distinct_numbers() {
        assert_ne!(raft_node_id("node-a"), raft_node_id("node-b"));
        assert_ne!(raft_node_id("node-a"), raft_node_id("node-c"));
        assert_ne!(raft_node_id("node-b"), raft_node_id("node-c"));
    }

    #[test]
    fn empty_id_is_handled() {
        // The offset basis is returned for an empty id; still deterministic.
        assert_eq!(raft_node_id(""), RaftNodeId(FNV_OFFSET_BASIS));
    }
}
