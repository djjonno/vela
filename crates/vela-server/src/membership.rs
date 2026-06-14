//! The cluster membership subsystem: peer connection, heartbeats, and
//! availability transitions.
//!
//! On startup a node attempts to reach each configured peer over the
//! [`VelaPeer`](vela_proto::v1::vela_peer_client) gRPC transport, then keeps a
//! lightweight liveness loop running per peer for the life of the process. The
//! behaviour is driven entirely by the four requirements this module owns:
//!
//! - **Connect with a 5 s timeout** ([`PEER_CONNECT_TIMEOUT`]): each heartbeat
//!   attempt is bounded so an unreachable peer cannot stall the loop
//!   (Requirement 9.1).
//! - **Retry every 1 s** ([`HEARTBEAT_INTERVAL`], which doubles as the retry
//!   interval): the loop ticks once per second, so a failed connection is
//!   retried on the next tick (Requirement 9.2).
//! - **Three consecutive misses → unavailable**
//!   ([`MISSED_HEARTBEATS_THRESHOLD`]): a peer that fails three heartbeats in a
//!   row is marked [`NodeAvailability::Unavailable`] in this node's view of the
//!   cluster metadata (Requirement 9.4).
//! - **A success restores availability**: any successful heartbeat resets the
//!   miss counter and, if the peer was unavailable, marks it
//!   [`NodeAvailability::Available`] again (Requirement 9.5).
//!
//! ## Testable transition core
//!
//! The 3-miss / recovery decision is isolated in [`MembershipState`], a small
//! synchronous state machine with no I/O or clock dependency. The async loop
//! only decides *whether a heartbeat succeeded* and feeds that into the state
//! machine; the state machine decides *what availability transition to apply*.
//! This keeps the transition rule (the behaviour Property 37 verifies in task
//! 14.4) deterministic and unit-testable without spinning a runtime or a clock.
//!
//! ## Peer identity
//!
//! Configured peers are plain `host:port` addresses. Until cross-node partition
//! assignment wires real node identities through, membership keys each peer by
//! its address: the address is used both as the [`NodeId`] recorded in the
//! members list and (hashed via [`raft_node_id`]) as the [`PeerPool`] key the
//! transport dials. This is consistent with how [`NodeShared`] seeds its own
//! member entry (identity + address) and is sufficient for the local-cluster
//! milestone.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, timeout, MissedTickBehavior};

use vela_core::{Member, NodeAvailability, NodeId};
use vela_raft::NodeId as RaftNodeId;

use vela_proto::v1::HeartbeatRequest;

use crate::node::NodeShared;
use crate::registry::raft_node_id;

/// The per-peer connection timeout for a heartbeat attempt (Requirement 9.1).
///
/// A heartbeat that does not complete within this window counts as a missed
/// heartbeat, so an unreachable peer never blocks the liveness loop.
pub const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The interval between heartbeats, which also serves as the connection retry
/// interval on failure (Requirement 9.2).
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// The number of consecutive missed heartbeats that marks a peer unavailable
/// (Requirement 9.4).
pub const MISSED_HEARTBEATS_THRESHOLD: u32 = 3;

/// The synchronous core of the membership rule: it tracks consecutive missed
/// heartbeats for a single peer and decides when the peer's availability must
/// flip.
///
/// This type performs no I/O and consults no clock — it is driven purely by
/// [`record_success`](Self::record_success) and [`record_miss`](Self::record_miss)
/// calls, so it can be unit- and property-tested deterministically (Property 37,
/// task 14.4). Each method returns `Some(new_state)` only when the call caused a
/// transition, so the caller applies a metadata change (and bumps the epoch)
/// exactly on the edges rather than on every heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipState {
    /// Consecutive missed heartbeats since the last success.
    consecutive_misses: u32,
    /// The peer's current availability as decided by this state machine.
    availability: NodeAvailability,
    /// The miss count at which the peer flips to unavailable.
    threshold: u32,
}

impl Default for MembershipState {
    fn default() -> Self {
        Self::new()
    }
}

impl MembershipState {
    /// A fresh state machine for a peer assumed available, using the standard
    /// [`MISSED_HEARTBEATS_THRESHOLD`].
    pub fn new() -> Self {
        Self::with_threshold(MISSED_HEARTBEATS_THRESHOLD)
    }

    /// A fresh state machine with an explicit miss `threshold`, for tests that
    /// drive the transition deterministically.
    pub fn with_threshold(threshold: u32) -> Self {
        Self {
            consecutive_misses: 0,
            availability: NodeAvailability::Available,
            threshold,
        }
    }

    /// The peer's current availability as last decided by this state machine.
    pub fn availability(&self) -> NodeAvailability {
        self.availability
    }

    /// The number of consecutive missed heartbeats recorded since the last
    /// success.
    pub fn consecutive_misses(&self) -> u32 {
        self.consecutive_misses
    }

    /// Record a missed heartbeat (failed connection, timeout, or error reply).
    ///
    /// Increments the consecutive-miss counter; once it reaches the threshold
    /// and the peer is not already unavailable, the peer transitions to
    /// [`NodeAvailability::Unavailable`] and `Some(Unavailable)` is returned so
    /// the caller can persist the change (Requirement 9.4). Otherwise returns
    /// `None`.
    pub fn record_miss(&mut self) -> Option<NodeAvailability> {
        self.consecutive_misses = self.consecutive_misses.saturating_add(1);
        if self.consecutive_misses >= self.threshold
            && self.availability != NodeAvailability::Unavailable
        {
            self.availability = NodeAvailability::Unavailable;
            Some(NodeAvailability::Unavailable)
        } else {
            None
        }
    }

    /// Record a successful heartbeat response.
    ///
    /// Resets the consecutive-miss counter; if the peer was previously
    /// unavailable it transitions back to [`NodeAvailability::Available`] and
    /// `Some(Available)` is returned so the caller can persist the recovery
    /// (Requirement 9.5). Otherwise returns `None`.
    pub fn record_success(&mut self) -> Option<NodeAvailability> {
        self.consecutive_misses = 0;
        if self.availability != NodeAvailability::Available {
            self.availability = NodeAvailability::Available;
            Some(NodeAvailability::Available)
        } else {
            None
        }
    }
}

/// Start the membership subsystem for `peers`, spawning one liveness task per
/// configured peer.
///
/// Each peer is first registered into this node's view of the cluster
/// (added to the members list as available, and registered in the
/// [`PeerPool`](crate::transport::PeerPool) so the transport can dial it), then
/// a per-peer heartbeat loop is spawned (Requirement 9.1, 9.2, 9.4, 9.5).
pub fn spawn_membership(node: Arc<NodeShared>, peers: Vec<String>) {
    register_peers(&node, &peers);
    for addr in peers {
        tokio::spawn(heartbeat_loop(node.clone(), addr));
    }
}

/// Add each peer to the members list (as available) and register its address in
/// the peer pool. Peers already present are left untouched.
fn register_peers(node: &Arc<NodeShared>, peers: &[String]) {
    let mut metadata = node.metadata.lock().expect("metadata mutex poisoned");
    for addr in peers {
        let id = NodeId::new(addr);
        if !metadata.members.iter().any(|m| m.id == id) {
            metadata.members.push(Member {
                id: id.clone(),
                addr: addr.clone(),
                availability: NodeAvailability::Available,
            });
        }
        node.pool.register_peer(raft_node_id(addr), addr.clone());
    }
}

/// The per-peer liveness loop: every [`HEARTBEAT_INTERVAL`] it sends a heartbeat
/// (bounded by [`PEER_CONNECT_TIMEOUT`]) and folds the outcome into a
/// [`MembershipState`], applying any availability transition to the shared
/// cluster metadata.
async fn heartbeat_loop(node: Arc<NodeShared>, addr: String) {
    let peer_id = raft_node_id(&addr);
    let member_id = NodeId::new(&addr);
    let mut state = MembershipState::new();

    // A 1 s tick that, after a slow heartbeat, resumes its cadence without
    // bursting to catch up — successive heartbeats stay ~1 s apart
    // (Requirement 9.2).
    let mut ticker = interval(HEARTBEAT_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let outcome = if send_heartbeat(&node, peer_id).await {
            state.record_success()
        } else {
            state.record_miss()
        };

        if let Some(availability) = outcome {
            let changed = {
                let mut metadata = node.metadata.lock().expect("metadata mutex poisoned");
                metadata.set_availability(&member_id, availability)
            };
            if changed {
                tracing::info!(
                    peer = %addr,
                    availability = ?availability,
                    "peer availability changed"
                );
            }
        }
    }
}

/// Send a single heartbeat to `peer`, returning `true` on a successful reply
/// within [`PEER_CONNECT_TIMEOUT`] and `false` on any failure (unknown peer,
/// connection/timeout, or error status) (Requirement 9.1).
async fn send_heartbeat(node: &NodeShared, peer: RaftNodeId) -> bool {
    let Some(mut client) = node.pool.peer_client(peer) else {
        return false;
    };
    let request = HeartbeatRequest {
        node_id: node.self_id.clone(),
    };
    matches!(
        timeout(PEER_CONNECT_TIMEOUT, client.heartbeat(request)).await,
        Ok(Ok(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_consecutive_misses_mark_unavailable() {
        let mut state = MembershipState::new();
        assert_eq!(state.availability(), NodeAvailability::Available);

        // The first two misses do not yet cross the threshold.
        assert_eq!(state.record_miss(), None);
        assert_eq!(state.record_miss(), None);
        assert_eq!(state.availability(), NodeAvailability::Available);

        // The third consecutive miss flips the peer to unavailable, exactly
        // once.
        assert_eq!(
            state.record_miss(),
            Some(NodeAvailability::Unavailable),
            "the third consecutive miss must transition to unavailable"
        );
        assert_eq!(state.availability(), NodeAvailability::Unavailable);

        // Further misses while already unavailable are not new transitions.
        assert_eq!(state.record_miss(), None);
        assert_eq!(state.availability(), NodeAvailability::Unavailable);
    }

    #[test]
    fn success_restores_availability() {
        let mut state = MembershipState::new();
        // Drive to unavailable.
        for _ in 0..MISSED_HEARTBEATS_THRESHOLD {
            state.record_miss();
        }
        assert_eq!(state.availability(), NodeAvailability::Unavailable);

        // A success from an unavailable peer restores it, exactly once.
        assert_eq!(
            state.record_success(),
            Some(NodeAvailability::Available),
            "a success must restore an unavailable peer"
        );
        assert_eq!(state.availability(), NodeAvailability::Available);

        // A further success while already available is not a new transition.
        assert_eq!(state.record_success(), None);
    }

    #[test]
    fn success_resets_the_miss_counter() {
        let mut state = MembershipState::new();
        state.record_miss();
        state.record_miss();
        assert_eq!(state.consecutive_misses(), 2);

        // A success clears the streak, so it takes a fresh run of three misses
        // to mark unavailable.
        assert_eq!(state.record_success(), None);
        assert_eq!(state.consecutive_misses(), 0);

        assert_eq!(state.record_miss(), None);
        assert_eq!(state.record_miss(), None);
        assert_eq!(state.availability(), NodeAvailability::Available);
        assert_eq!(state.record_miss(), Some(NodeAvailability::Unavailable));
    }

    #[test]
    fn an_available_peer_with_no_misses_needs_no_update() {
        // A steady stream of successes never produces a transition (avoids
        // needless metadata epoch churn).
        let mut state = MembershipState::new();
        for _ in 0..5 {
            assert_eq!(state.record_success(), None);
            assert_eq!(state.availability(), NodeAvailability::Available);
        }
    }
}
