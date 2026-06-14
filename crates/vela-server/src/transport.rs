//! Outbound transport: the [`vela_raft::Transport`] adapter onto gRPC.
//!
//! `vela-raft` emits the messages a replica must send as opaque
//! `(NodeId, RaftMessage)` pairs and leaves delivery to the
//! [`Transport`](vela_raft::Transport) seam. In the server, [`GrpcTransport`]
//! implements that seam by translating each outbound `RequestVote` /
//! `AppendEntries` into a [`VelaPeer`](vela_proto::v1::vela_peer_client) gRPC
//! call (Requirement 12.3) and feeding the response back into the originating
//! partition driver as a Raft reply message.
//!
//! Because `Transport::send` is fire-and-forget (it returns nothing), each call
//! spawns a task that performs the request/response RPC and posts the reply onto
//! the driver's queue. Connections are made lazily and cached per peer in a
//! shared [`PeerPool`], so a single channel between two nodes multiplexes every
//! co-hosted partition group (the design's shared server-to-server channel).
//!
//! A `GrpcTransport` is bound to one `(topic, partition)` because the
//! `Transport` trait carries no partition context; stamping the topic and
//! partition onto every outbound RPC is how the receiving node routes the
//! message to the right group. Reply messages the Raft core emits in response to
//! inbound RPCs are *not* sent here — the inbound gRPC handler returns those
//! directly — so this adapter only ever transmits requests and drops any reply
//! defensively.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use vela_raft::{NodeId as RaftNodeId, RaftInput, RaftMessage};

use vela_proto::v1::vela_peer_client::VelaPeerClient;

use crate::convert;
use crate::driver::{DriverCommand, DriverHandle};

/// The per-peer connection timeout for server-to-server channels
/// (Requirement 9.1).
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A shared pool of lazily-connected, cached gRPC channels to peer nodes.
///
/// Maps the numeric [`RaftNodeId`] of a peer to its transport address and a
/// cached [`Channel`]. Channels are created lazily (`connect_lazy`) so a peer
/// that is momentarily unreachable does not block; the actual connection is
/// established on first use and reused thereafter.
#[derive(Default)]
pub struct PeerPool {
    /// Peer transport addresses (`host:port`) keyed by numeric node id.
    addrs: Mutex<HashMap<RaftNodeId, String>>,
    /// Cached channels keyed by numeric node id.
    channels: Mutex<HashMap<RaftNodeId, Channel>>,
}

impl PeerPool {
    /// Create an empty pool with no known peers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or update) the transport address of a peer.
    pub fn register_peer(&self, id: RaftNodeId, addr: impl Into<String>) {
        self.addrs
            .lock()
            .expect("peer pool addr mutex poisoned")
            .insert(id, addr.into());
    }

    /// Obtain a [`VelaPeerClient`] for `peer`, creating and caching its channel
    /// on first use. Returns `None` if the peer's address is unknown.
    pub fn peer_client(&self, peer: RaftNodeId) -> Option<VelaPeerClient<Channel>> {
        if let Some(channel) = self
            .channels
            .lock()
            .expect("peer pool channel mutex poisoned")
            .get(&peer)
            .cloned()
        {
            return Some(VelaPeerClient::new(channel));
        }

        let addr = self
            .addrs
            .lock()
            .expect("peer pool addr mutex poisoned")
            .get(&peer)
            .cloned()?;

        let endpoint = Endpoint::from_shared(format!("http://{addr}"))
            .ok()?
            .connect_timeout(PEER_CONNECT_TIMEOUT);
        let channel = endpoint.connect_lazy();
        self.channels
            .lock()
            .expect("peer pool channel mutex poisoned")
            .insert(peer, channel.clone());
        Some(VelaPeerClient::new(channel))
    }
}

/// The [`Transport`](vela_raft::Transport) adapter for one partition replica.
pub struct GrpcTransport {
    /// The topic this transport stamps onto outbound RPCs.
    topic: String,
    /// The partition index this transport stamps onto outbound RPCs.
    partition: u32,
    /// This node's string identity (the candidate/leader id on outbound RPCs).
    self_id: String,
    /// The shared peer connection pool.
    pool: Arc<PeerPool>,
    /// Queue back to the owning driver, used to deliver RPC replies.
    driver: DriverHandle,
}

impl GrpcTransport {
    /// Create a transport bound to `(topic, partition)` for the node `self_id`,
    /// sharing `pool` and delivering replies onto `driver`.
    pub fn new(
        topic: String,
        partition: u32,
        self_id: String,
        pool: Arc<PeerPool>,
        driver: DriverHandle,
    ) -> Self {
        Self {
            topic,
            partition,
            self_id,
            pool,
            driver,
        }
    }
}

impl vela_raft::Transport for GrpcTransport {
    fn send(&self, to: RaftNodeId, msg: RaftMessage) {
        match msg {
            RaftMessage::RequestVote(rv) => {
                let request =
                    convert::request_vote_to_proto(&rv, &self.topic, self.partition, &self.self_id);
                let pool = self.pool.clone();
                let driver = self.driver.clone();
                tokio::spawn(async move {
                    let Some(mut client) = pool.peer_client(to) else {
                        tracing::debug!(peer = to.0, "no address for peer; dropping RequestVote");
                        return;
                    };
                    match client.request_vote(request).await {
                        Ok(response) => {
                            let reply =
                                convert::request_vote_reply_from_proto(&response.into_inner());
                            let _ = driver.send(DriverCommand::Raft(RaftInput::Message(
                                RaftMessage::RequestVoteReply(reply),
                            )));
                        }
                        Err(status) => {
                            tracing::debug!(peer = to.0, %status, "RequestVote RPC failed");
                        }
                    }
                });
            }
            RaftMessage::AppendEntries(ae) => {
                // On success the follower matches through the last index sent
                // (or `prev_log_index` for an empty heartbeat); the leader
                // supplies that when reconstructing the reply.
                let match_on_success = ae.entries.last().map(|e| e.index).or(ae.prev_log_index);
                let request = convert::append_entries_to_proto(
                    &ae,
                    &self.topic,
                    self.partition,
                    &self.self_id,
                );
                let pool = self.pool.clone();
                let driver = self.driver.clone();
                tokio::spawn(async move {
                    let Some(mut client) = pool.peer_client(to) else {
                        tracing::debug!(peer = to.0, "no address for peer; dropping AppendEntries");
                        return;
                    };
                    match client.append_entries(request).await {
                        Ok(response) => {
                            let reply = convert::append_entries_reply_from_proto(
                                &response.into_inner(),
                                to,
                                match_on_success,
                            );
                            let _ = driver.send(DriverCommand::Raft(RaftInput::Message(
                                RaftMessage::AppendEntriesReply(reply),
                            )));
                        }
                        Err(status) => {
                            tracing::debug!(peer = to.0, %status, "AppendEntries RPC failed");
                        }
                    }
                });
            }
            // Replies to inbound RPCs are returned by the gRPC handler directly,
            // not transmitted here; drop them defensively.
            RaftMessage::RequestVoteReply(_) | RaftMessage::AppendEntriesReply(_) => {}
        }
    }
}
