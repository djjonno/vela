//! Per-partition driver task.
//!
//! Vela hosts **one independent Raft group per partition** (Requirement 7.1),
//! and on a node each hosted replica is driven by its own `tokio` task — the
//! single-writer-per-partition model from the design's *Runtime Model*. The
//! driver owns the partition's [`PartitionReplica`] (a synchronous Raft state
//! machine) and is the only writer of that consensus state; everything reaches
//! it as a [`DriverCommand`] on an `mpsc` queue:
//!
//! - **timer ticks** — election/heartbeat timers armed through the real-clock
//!   [`TimerClock`](crate::clock::TimerClock) deliver [`DriverCommand::Tick`]s
//!   onto the queue (Requirement 7.2, 7.6);
//! - **peer RPCs** — inbound `AppendEntries` / `RequestVote` arrive as
//!   [`DriverCommand::PeerRpc`] and are answered synchronously through a
//!   `oneshot` so the gRPC handler can return the reply (Requirement 12.3);
//! - **peer replies** — the [`GrpcTransport`](crate::transport::GrpcTransport)
//!   feeds the responses to the leader's own outbound RPCs back as
//!   [`DriverCommand::Raft`];
//! - **client proposals** — a produce request arrives as
//!   [`DriverCommand::Produce`]; the driver appends on the leader and resolves
//!   the caller's `oneshot` with the assigned offset once the entry commits, or
//!   with [`ProduceError::CommitTimeout`] after the commit deadline
//!   (Requirement 4.4, 4.7, 4.9);
//! - **reads** — a consume request arrives as [`DriverCommand::Consume`] and is
//!   served from the partition state machine (Requirement 5.1).
//!
//! Outbound messages the Raft core emits are dispatched through the transport;
//! role transitions are logged (Requirement 15.4). The driver performs no
//! locking on consensus state — being the sole owner is what keeps each
//! partition group independent and the consensus core reusable in the
//! simulator.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot, Notify};

use vela_core::{
    apply_command, ClusterCommand, ClusterMetadata, CommittedRecord, CoreError, MetadataController,
    NodeId, Offset, PartitionReplica, StateMachine, COMMIT_TIMEOUT_MS,
};
use vela_raft::{
    Clock, EntryPayload, LogEntry, LogStorage, NodeId as RaftNodeId, PayloadKind, RaftInput,
    RaftMessage, RaftOutput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE,
};

use crate::clock::TimerClock;
use crate::convert;
use crate::transport::GrpcTransport;

/// The sending half of a partition driver's command queue.
///
/// Cloneable so the gRPC services, the timer clock, and the transport can all
/// enqueue work onto the single driver task that owns the replica.
pub type DriverHandle = mpsc::UnboundedSender<DriverCommand>;

/// Why a produce proposal did not yield a committed offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProduceError {
    /// This replica is not the partition leader; nothing was appended and the
    /// client should redirect to the current leader (Requirement 4.6).
    NotLeader,
    /// The entry was appended but not committed to a majority within the commit
    /// timeout; the committed offset was not advanced (Requirement 4.9).
    CommitTimeout,
}

/// A unit of work for a partition driver task.
pub enum DriverCommand {
    /// A Raft input to step the state machine with — used for peer RPC replies
    /// delivered by the transport.
    Raft(RaftInput),
    /// A timer of `kind` fired; `generation` identifies which arming produced it
    /// so a stale tick from a since-reset timer can be ignored.
    Tick {
        /// The timer that elapsed.
        kind: TimerKind,
        /// The arming generation this tick belongs to.
        generation: u64,
    },
    /// An inbound peer RPC (`RequestVote` / `AppendEntries`) that must be
    /// answered; the reply (if any) is returned through `reply`.
    PeerRpc {
        /// The decoded inbound message.
        msg: RaftMessage,
        /// Channel for the synchronous reply the gRPC handler returns.
        reply: oneshot::Sender<Option<RaftMessage>>,
    },
    /// A client produce: append `value` on the leader and report the committed
    /// offset (or an error) through `reply`.
    Produce {
        /// The record's value bytes to append.
        value: Vec<u8>,
        /// Channel for the produce result.
        reply: oneshot::Sender<Result<Offset, ProduceError>>,
    },
    /// The commit deadline for the pending produce targeting log index `target`
    /// elapsed (Requirement 4.9).
    ProduceTimeout {
        /// The log index whose pending produce timed out.
        target: u64,
    },
    /// A client consume: read up to `max` committed records from `offset`.
    Consume {
        /// The first offset to read from.
        offset: u64,
        /// The maximum number of records to return.
        max: usize,
        /// Channel for the committed records read.
        reply: oneshot::Sender<Vec<CommittedRecord>>,
    },
    /// Propose a metadata change on the dedicated `("__meta", 0)` group and
    /// report whether it committed (design §3). Handled only by the
    /// [`MetadataDriver`]; a partition driver ignores it.
    ///
    /// On the metadata leader the `command` is appended as a
    /// [`PayloadKind::Cluster`](vela_raft::PayloadKind::Cluster) entry and
    /// `reply` resolves `Ok(())` once that entry commits to a majority
    /// (Requirement 3.1–3.4), or `Err(CoreError::CommitTimeout)` if it has not
    /// committed within `COMMIT_TIMEOUT_MS` (Requirement 3.5). On a non-leader
    /// nothing is appended and `reply` resolves `Err(CoreError::NotLeader)` with
    /// the known metadata leader hint so the caller can redirect (Requirement
    /// 4.1; Raft §8).
    //
    // Constructed by the leader-routed `create_topic`/`delete_topic` propose
    // path (task 6.2); allow dead_code until that wiring lands so the crate
    // stays clippy-clean. The allow is a no-op once the consumer exists.
    #[allow(dead_code)]
    ProposeCluster {
        /// The already-validated, replica-assigned metadata mutation to commit.
        command: ClusterCommand,
        /// Channel for the propose result: `Ok(())` once committed, or a typed
        /// error (`NotLeader` / `CommitTimeout`).
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    /// The commit deadline for the pending metadata proposal targeting log index
    /// `target` elapsed (Requirement 3.5). Handled only by the
    /// [`MetadataDriver`]; a partition driver ignores it.
    ClusterCommitTimeout {
        /// The metadata-log index whose pending proposal timed out.
        target: u64,
    },
    /// Report the replica's **known current leader** as a domain
    /// [`NodeId`](vela_core::NodeId): its own id when it is the leader,
    /// otherwise the leader it last learned from an `AppendEntries`, or `None`
    /// when it knows of none (Raft §5.2; Requirement 8.1, 8.2).
    ///
    /// Handled by both the [`PartitionDriver`] and the [`MetadataDriver`]: the
    /// live-leader routing path (task 7.2) reads this to redirect produce /
    /// consume and to answer `FindLeader` by the real Raft-elected leader rather
    /// than any stale leader value stored in `ClusterMetadata` (Requirement 8.3,
    /// 8.4).
    KnownLeader {
        /// Channel for the known-leader answer: the believed current leader's
        /// domain id, or `None` when none is known.
        reply: oneshot::Sender<Option<NodeId>>,
    },
    /// Stop the driver task and release the replica (its Raft state and log).
    Shutdown,
}

/// A produce awaiting commit: the log index it landed at and the caller to
/// resolve once that index commits.
struct Pending {
    /// The log index the proposed record occupies.
    target: u64,
    /// The caller awaiting the committed offset.
    reply: oneshot::Sender<Result<Offset, ProduceError>>,
}

/// The owner of one partition replica and its command queue.
pub struct PartitionDriver {
    /// Human-readable topic name, for logging.
    topic: String,
    /// Partition index within the topic, for logging.
    partition: u32,
    /// This node's string identity, for logging role transitions.
    node_label: String,
    /// The consensus + state-machine replica this task exclusively owns.
    replica: PartitionReplica,
    /// Real-clock timer source feeding ticks onto the queue.
    clock: TimerClock,
    /// Adapter dispatching outbound Raft messages over gRPC.
    transport: GrpcTransport,
    /// The receiving half of the command queue.
    rx: mpsc::UnboundedReceiver<DriverCommand>,
    /// A clone of the sender, used to schedule produce-commit timeouts.
    self_tx: DriverHandle,
    /// Produces appended on the leader awaiting commit, in ascending target
    /// order.
    pending: VecDeque<Pending>,
    /// Reverse map from each replica's numeric [`RaftNodeId`] back to its domain
    /// [`NodeId`], built from this partition's replica set. Used to translate
    /// the replica's numeric known-leader id into the domain id the routing
    /// path redirects clients to (Requirement 8.1, 8.2).
    leader_lookup: HashMap<RaftNodeId, NodeId>,
}

impl PartitionDriver {
    /// Assemble a driver for one partition replica. Use [`PartitionDriver::spawn`]
    /// to start it on the async runtime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic: String,
        partition: u32,
        node_label: String,
        replica: PartitionReplica,
        clock: TimerClock,
        transport: GrpcTransport,
        rx: mpsc::UnboundedReceiver<DriverCommand>,
        self_tx: DriverHandle,
        leader_lookup: HashMap<RaftNodeId, NodeId>,
    ) -> Self {
        Self {
            topic,
            partition,
            node_label,
            replica,
            clock,
            transport,
            rx,
            self_tx,
            pending: VecDeque::new(),
            leader_lookup,
        }
    }

    /// Spawn the driver as a `tokio` task, returning immediately.
    pub fn spawn(self) {
        tokio::spawn(self.run());
    }

    /// The driver's main loop: arm the initial election timer, then process
    /// commands until the queue closes or a [`DriverCommand::Shutdown`] arrives.
    async fn run(mut self) {
        // Arm the first election timeout so an idle follower will start an
        // election (Requirement 7.2); a single-node group elects itself.
        self.clock.arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);

        while let Some(command) = self.rx.recv().await {
            match command {
                DriverCommand::Raft(input) => {
                    let out = self.replica.step(input, &mut self.clock);
                    self.after_step(out, None);
                }
                DriverCommand::Tick { kind, generation } => {
                    // Ignore a tick from a since-reset timer (Requirement 7.2,
                    // 7.6): only the latest arming of each kind is honoured.
                    if self.clock.is_current(kind, generation) {
                        let out = self.replica.step(RaftInput::Tick(kind), &mut self.clock);
                        self.after_step(out, None);
                    }
                }
                DriverCommand::PeerRpc { msg, reply } => {
                    let want = ReplyKind::for_request(&msg);
                    let out = self.replica.step(RaftInput::Message(msg), &mut self.clock);
                    let response = self.after_step(out, want);
                    let _ = reply.send(response);
                }
                DriverCommand::Produce { value, reply } => {
                    self.handle_produce(value, reply);
                }
                DriverCommand::ProduceTimeout { target } => {
                    if let Some(pos) = self.pending.iter().position(|p| p.target == target) {
                        let pending = self.pending.remove(pos).expect("position just found");
                        let _ = pending.reply.send(Err(ProduceError::CommitTimeout));
                    }
                }
                DriverCommand::Consume { offset, max, reply } => {
                    let _ = reply.send(self.replica.read(offset, max));
                }
                // Report this replica's known current leader for live-leader
                // routing (Requirement 8.1, 8.2).
                DriverCommand::KnownLeader { reply } => {
                    let _ = reply.send(self.known_leader());
                }
                // Metadata-group proposals never target a partition driver; drop
                // them defensively (closing any reply channel signals the
                // caller) rather than treating them as partition work.
                DriverCommand::ProposeCluster { .. }
                | DriverCommand::ClusterCommitTimeout { .. } => {}
                DriverCommand::Shutdown => break,
            }
        }
    }

    /// Append a produced record on the leader and either resolve immediately
    /// (single-node commit) or register it as pending with a commit deadline.
    fn handle_produce(
        &mut self,
        value: Vec<u8>,
        reply: oneshot::Sender<Result<Offset, ProduceError>>,
    ) {
        if self.replica.role() != Role::Leader {
            let _ = reply.send(Err(ProduceError::NotLeader));
            return;
        }

        // The log index this record will occupy once appended.
        let target = self.replica.raft().log().last_index().map_or(0, |i| i + 1);

        let payload = EntryPayload::new(PayloadKind::Record, value);
        let out = self
            .replica
            .step(RaftInput::Propose(payload), &mut self.clock);
        self.after_step(out, None);

        if self.committed_through(target) {
            // Committed within this step (the leader is its own majority in a
            // single-node group): resolve with the assigned offset now.
            let _ = reply.send(Ok(self.offset_at(target)));
            return;
        }

        // Otherwise await replication; resolve on commit or after the deadline.
        self.pending.push_back(Pending { target, reply });
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(COMMIT_TIMEOUT_MS)).await;
            let _ = tx.send(DriverCommand::ProduceTimeout { target });
        });
    }

    /// React to a [`RaftOutput`]: log any role transition, dispatch outbound
    /// messages (optionally extracting the one reply addressed to an inbound
    /// RPC), and resolve any produces whose entries have now committed.
    ///
    /// When `want` is `Some`, the single reply message of that kind is removed
    /// from the outbound set and returned (for the gRPC handler to answer with);
    /// every other message is dispatched through the transport.
    fn after_step(&mut self, out: RaftOutput, want: Option<ReplyKind>) -> Option<RaftMessage> {
        if let Some(role) = out.role_change {
            tracing::info!(
                topic = %self.topic,
                partition = self.partition,
                node = %self.node_label,
                ?role,
                "raft role transition"
            );
        }

        let mut response = None;
        for (to, msg) in out.sends {
            if want.is_some_and(|kind| kind.matches(&msg)) && response.is_none() {
                response = Some(msg);
            } else {
                self.transport.send(to, msg);
            }
        }

        self.resolve_pending();
        response
    }

    /// Resolve every pending produce whose target index is now committed, in
    /// order, assigning each its gap-free offset (Requirement 4.4, 4.7).
    fn resolve_pending(&mut self) {
        while let Some(front) = self.pending.front() {
            if self.committed_through(front.target) {
                let pending = self.pending.pop_front().expect("front just observed");
                let offset = self.offset_at(pending.target);
                let _ = pending.reply.send(Ok(offset));
            } else {
                break;
            }
        }
    }

    /// Whether the replica's commit index has reached `target`.
    fn committed_through(&self, target: u64) -> bool {
        self.replica
            .raft()
            .commit_index()
            .is_some_and(|c| c >= target)
    }

    /// The record offset assigned to the (committed) entry at log index
    /// `target`: its position among record-kind entries, 0-based.
    fn offset_at(&self, target: u64) -> Offset {
        let records = self
            .replica
            .raft()
            .log()
            .read(0, target)
            .iter()
            .filter(|e| e.payload.kind == PayloadKind::Record)
            .count() as u64;
        records.saturating_sub(1)
    }

    /// This replica's known current leader as a domain [`NodeId`]: its own id
    /// when it leads, otherwise the leader it last learned from an
    /// `AppendEntries`, mapped from the numeric [`RaftNodeId`] back through the
    /// replica-set lookup; `None` when no leader is known or the believed
    /// leader is not in the replica set (Raft §5.2; Requirement 8.1, 8.2).
    fn known_leader(&self) -> Option<NodeId> {
        if self.replica.role() == Role::Leader {
            return Some(NodeId::new(&self.node_label));
        }
        self.replica
            .raft()
            .leader_id()
            .and_then(|id| self.leader_lookup.get(&id).cloned())
    }
}

/// Which reply a synchronously-answered inbound RPC expects, so the driver can
/// pick that one message out of the Raft output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyKind {
    /// A reply to an inbound `RequestVote`.
    Vote,
    /// A reply to an inbound `AppendEntries`.
    Append,
}

impl ReplyKind {
    /// The reply kind expected for an inbound request message, or `None` if the
    /// message is not a request that is answered synchronously.
    fn for_request(msg: &RaftMessage) -> Option<Self> {
        match msg {
            RaftMessage::RequestVote(_) => Some(Self::Vote),
            RaftMessage::AppendEntries(_) => Some(Self::Append),
            _ => None,
        }
    }

    /// Whether `msg` is the reply this kind is waiting for.
    fn matches(self, msg: &RaftMessage) -> bool {
        matches!(
            (self, msg),
            (Self::Vote, RaftMessage::RequestVoteReply(_))
                | (Self::Append, RaftMessage::AppendEntriesReply(_))
        )
    }
}

// ---------------------------------------------------------------------------
// Metadata group driver (design §1)
// ---------------------------------------------------------------------------

/// A metadata proposal awaiting commit: the log index it landed at and the
/// caller to resolve once that index commits (design §3).
///
/// Mirrors [`Pending`] for the partition produce path, but resolves a
/// `ClusterCommand` proposal: `Ok(())` once `target` commits (Requirement 3.4)
/// or [`CoreError::CommitTimeout`] after the deadline (Requirement 3.5).
struct ClusterPending {
    /// The metadata-log index the proposed `ClusterCommand` occupies.
    target: u64,
    /// The caller awaiting the commit outcome.
    reply: oneshot::Sender<Result<(), CoreError>>,
}

/// The driver task for the dedicated metadata Raft group `("__meta", 0)`.
///
/// This feature runs the metadata catalogue as an ordinary *driven* Raft group:
/// the same asynchronous election/heartbeat timers ([`TimerClock`]) and peer
/// transport ([`GrpcTransport`]) a partition replica uses (design §1). Inbound
/// `AppendEntries` / `RequestVote` for `__meta/0` therefore route through the
/// existing `VelaPeerService::dispatch_rpc` to this task's
/// [`DriverHandle`](crate::driver::DriverHandle) with no change, and outbound
/// RPCs are stamped with the reserved `("__meta", 0)` key.
///
/// Unlike a [`PartitionDriver`], which owns its [`PartitionReplica`] outright,
/// the metadata replica is owned by the node's [`MetadataController`] — which
/// holds the durable `__meta` WAL and (until that path is removed in a later
/// task) is also stepped inline by the legacy `BootstrapClock` propose path. A
/// durable WAL permits only a single open, so rather than recover the log
/// twice this driver **shares** the controller's one replica behind its
/// [`Mutex`]: every step is serialized between this task's timer/RPC activity
/// and the inline propose path. The two never fight over leadership because a
/// leader ignores election ticks (`vela-raft`), so once a leader is established
/// neither path forces a needless re-election.
pub struct MetadataDriver {
    /// The node's metadata controller, hosting the durable `("__meta", 0)`
    /// group; shared with the inline propose path behind a mutex.
    controller: Arc<Mutex<MetadataController>>,
    /// This node's string identity, for logging role transitions.
    node_label: String,
    /// Real-clock timer source feeding ticks onto this driver's queue.
    clock: TimerClock,
    /// Adapter dispatching outbound metadata Raft messages over gRPC, stamped
    /// with the reserved `("__meta", 0)` key.
    transport: GrpcTransport,
    /// The receiving half of the command queue (timer ticks, inbound peer RPCs,
    /// and the transport's peer replies).
    rx: mpsc::UnboundedReceiver<DriverCommand>,
    /// A clone of the command sender, used to schedule a proposal's
    /// commit-timeout poke back onto this driver's own queue.
    self_tx: DriverHandle,
    /// The commit-apply seam: folds each step's newly committed `Cluster`
    /// entries into the node's served catalogue and pokes the off-loop
    /// reconciler (design §2). Fed [`RaftOutput::committed`] in ascending index
    /// order, exactly once per entry, so apply is in-order and idempotent
    /// (Requirement 5.1, 5.2; Raft §5.3).
    sink: MetadataSink,
    /// Metadata proposals appended on the leader awaiting commit, in ascending
    /// target order (design §3). Each resolves `Ok(())` once its target index
    /// commits (Requirement 3.4) or with [`CoreError::CommitTimeout`] after the
    /// deadline (Requirement 3.5).
    pending: VecDeque<ClusterPending>,
    /// Reverse map from each metadata voter's numeric [`RaftNodeId`] back to its
    /// domain [`NodeId`], built from the configured node set. Used to translate
    /// the metadata replica's numeric known-leader id into the domain id the
    /// `NotLeader` redirect hint carries (Requirement 4.1, 8.1).
    leader_lookup: HashMap<RaftNodeId, NodeId>,
}

impl MetadataDriver {
    /// Assemble the metadata driver over the shared `controller`, folding each
    /// step's committed entries through `sink`. `self_tx` is a clone of the
    /// command sender, used to schedule proposal commit-timeouts back onto this
    /// driver's queue. Use [`MetadataDriver::spawn`] to start it on the async
    /// runtime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        controller: Arc<Mutex<MetadataController>>,
        node_label: String,
        clock: TimerClock,
        transport: GrpcTransport,
        rx: mpsc::UnboundedReceiver<DriverCommand>,
        self_tx: DriverHandle,
        sink: MetadataSink,
        leader_lookup: HashMap<RaftNodeId, NodeId>,
    ) -> Self {
        Self {
            controller,
            node_label,
            clock,
            transport,
            rx,
            self_tx,
            sink,
            pending: VecDeque::new(),
            leader_lookup,
        }
    }

    /// Spawn the metadata driver as a `tokio` task, returning immediately.
    pub fn spawn(self) {
        tokio::spawn(self.run());
    }

    /// The driver's main loop: arm the initial election timer, then process
    /// commands until the queue closes or a [`DriverCommand::Shutdown`] arrives.
    async fn run(mut self) {
        // Arm the first election timeout so an idle metadata replica starts an
        // election (Raft §5.2): a single-node group is its own majority and a
        // multi-node group elects over the real `VelaPeer` transport.
        self.clock.arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);

        while let Some(command) = self.rx.recv().await {
            match command {
                DriverCommand::Raft(input) => {
                    let out = self.step(input);
                    self.after_step(out, None);
                }
                DriverCommand::Tick { kind, generation } => {
                    // Ignore a tick from a since-reset timer; only the latest
                    // arming of each kind is honoured.
                    if self.clock.is_current(kind, generation) {
                        let out = self.step(RaftInput::Tick(kind));
                        self.after_step(out, None);
                    }
                }
                DriverCommand::PeerRpc { msg, reply } => {
                    let want = ReplyKind::for_request(&msg);
                    let out = self.step(RaftInput::Message(msg));
                    let response = self.after_step(out, want);
                    let _ = reply.send(response);
                }
                DriverCommand::ProposeCluster { command, reply } => {
                    self.handle_propose(command, reply);
                }
                // Report this replica's known current metadata leader for the
                // routing/redirect path (Requirement 8.1, 8.2).
                DriverCommand::KnownLeader { reply } => {
                    let _ = reply.send(self.known_leader());
                }
                DriverCommand::ClusterCommitTimeout { target } => {
                    // Resolve the still-pending proposal at `target` as a commit
                    // timeout (Requirement 3.5). If it already committed it is no
                    // longer pending, so this is a no-op.
                    if let Some(pos) = self.pending.iter().position(|p| p.target == target) {
                        let pending = self.pending.remove(pos).expect("position just found");
                        let _ = pending.reply.send(Err(CoreError::CommitTimeout));
                    }
                }
                DriverCommand::Shutdown => break,
                // The metadata group is not a client topic, so it never receives
                // produce/consume work; drop these defensively (closing the reply
                // channel signals the caller) rather than treating them as Raft
                // inputs.
                DriverCommand::Produce { .. }
                | DriverCommand::Consume { .. }
                | DriverCommand::ProduceTimeout { .. } => {}
            }
        }
    }

    /// Step the shared metadata group one input, using this driver's real clock.
    ///
    /// The controller hosts the `("__meta", 0)` group for the life of the node,
    /// so the step never returns `None`; an absent group would yield an empty
    /// output.
    fn step(&mut self, input: RaftInput) -> RaftOutput {
        self.controller
            .lock()
            .expect("controller mutex poisoned")
            .step(input, &mut self.clock)
            .unwrap_or_default()
    }

    /// React to a [`RaftOutput`]: log any role transition and dispatch outbound
    /// messages, optionally extracting the single reply addressed to an inbound
    /// RPC (returned for the gRPC handler to answer with).
    ///
    /// When `want` is `Some`, the one reply message of that kind is removed from
    /// the outbound set and returned; every other message is dispatched through
    /// the transport. After dispatch, any pending metadata proposals whose
    /// target index has now committed are resolved (design §3), and this step's
    /// newly committed entries are folded through the [`MetadataSink`]
    /// (design §2).
    fn after_step(&mut self, out: RaftOutput, want: Option<ReplyKind>) -> Option<RaftMessage> {
        if let Some(role) = out.role_change {
            tracing::info!(
                topic = vela_core::METADATA_GROUP_TOPIC,
                partition = 0u32,
                node = %self.node_label,
                ?role,
                "raft role transition"
            );
        }

        // Fold this step's newly committed entries into the served catalogue and
        // poke the off-loop reconciler (design §2, §5). `vela-raft` emits
        // `committed` in ascending index order exactly once, so apply is
        // in-order and idempotent (Requirement 5.1, 5.2). Reconciliation runs
        // off this loop, so applying never blocks metadata heartbeats (H1).
        self.sink.apply_committed(&out.committed);

        let mut response = None;
        for (to, msg) in out.sends {
            if want.is_some_and(|kind| kind.matches(&msg)) && response.is_none() {
                response = Some(msg);
            } else {
                self.transport.send(to, msg);
            }
        }

        // Resolve any proposals whose target index has now committed
        // (Requirement 3.4).
        self.resolve_pending();
        response
    }

    /// Propose a `ClusterCommand` on the metadata group, awaiting its commit
    /// (design §3).
    ///
    /// Routes to the leader (Raft §8, Requirement 4.1): on a non-leader nothing
    /// is appended and the caller is redirected with the replica's known current
    /// leader hint. On the leader the command is appended as a
    /// [`PayloadKind::Cluster`] entry and either resolves immediately (a
    /// single-node group is its own majority, so it commits within this step) or
    /// is registered as pending with a commit deadline, resolving on commit
    /// (Requirement 3.1–3.4) or with [`CoreError::CommitTimeout`] after
    /// `COMMIT_TIMEOUT_MS` (Requirement 3.5).
    fn handle_propose(
        &mut self,
        command: ClusterCommand,
        reply: oneshot::Sender<Result<(), CoreError>>,
    ) {
        // Only the metadata leader appends; a non-leader redirects (Req 4.1).
        if self.role() != Role::Leader {
            let _ = reply.send(Err(CoreError::NotLeader {
                leader: self.known_leader(),
            }));
            return;
        }

        // The metadata-log index this command will occupy once appended.
        let target = self.last_log_index().map_or(0, |i| i + 1);

        let payload = EntryPayload::new(
            PayloadKind::Cluster,
            convert::cluster_command_to_bytes(&command),
        );
        let out = self.step(RaftInput::Propose(payload));
        self.after_step(out, None);

        if self.committed_through(target) {
            // Committed within this step (a single-node metadata group is its
            // own majority): report success now (Requirement 3.4).
            let _ = reply.send(Ok(()));
            return;
        }

        // Otherwise await replication to a majority; resolve on commit or after
        // the deadline. A `CommitTimeout` is **indeterminate**, not a failure
        // (H2): an entry that was appended and replicated but not yet committed
        // when leadership changed may still commit later under the new leader,
        // so the change may still take effect — the caller must re-check (e.g.
        // `DescribeTopic`) rather than assume it failed. Topic-admin is
        // idempotent on topic name, so a retry after a timeout is safe.
        self.pending.push_back(ClusterPending { target, reply });
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(COMMIT_TIMEOUT_MS)).await;
            let _ = tx.send(DriverCommand::ClusterCommitTimeout { target });
        });
    }

    /// Resolve every pending proposal whose target index is now committed, in
    /// ascending target order (Requirement 3.4).
    fn resolve_pending(&mut self) {
        while let Some(front) = self.pending.front() {
            if self.committed_through(front.target) {
                let pending = self.pending.pop_front().expect("front just observed");
                let _ = pending.reply.send(Ok(()));
            } else {
                break;
            }
        }
    }

    /// The role this node currently holds in the metadata group; a controller
    /// that has somehow lost the group is treated as a follower (it cannot
    /// propose).
    fn role(&self) -> Role {
        self.controller
            .lock()
            .expect("controller mutex poisoned")
            .role()
            .unwrap_or(Role::Follower)
    }

    /// The last index of the metadata group's replicated log, or `None` when the
    /// log is empty.
    fn last_log_index(&self) -> Option<u64> {
        self.controller
            .lock()
            .expect("controller mutex poisoned")
            .last_log_index()
    }

    /// Whether the metadata group's commit index has reached `target`.
    fn committed_through(&self, target: u64) -> bool {
        self.controller
            .lock()
            .expect("controller mutex poisoned")
            .commit_index()
            .is_some_and(|c| c >= target)
    }

    /// The metadata replica's known current leader, used as the redirect hint on
    /// a non-leader proposal (Requirement 4.1, 8.1; Raft §8) and to answer
    /// `KnownLeader` for live-leader routing (Requirement 8.2).
    ///
    /// Its own domain id when it leads the metadata group, otherwise the leader
    /// it last learned from an `AppendEntries`, mapped from the numeric
    /// [`RaftNodeId`] back through the voter-set lookup; `None` when no leader is
    /// known or the believed leader is not in the configured voter set.
    fn known_leader(&self) -> Option<NodeId> {
        let controller = self.controller.lock().expect("controller mutex poisoned");
        if controller.role() == Some(Role::Leader) {
            return Some(NodeId::new(&self.node_label));
        }
        controller
            .leader_id()
            .and_then(|id| self.leader_lookup.get(&id).cloned())
    }
}

// ---------------------------------------------------------------------------
// Commit-apply seam (design §2)
// ---------------------------------------------------------------------------
//
// A driven Raft group folds the entries that commit on each step into a state
// machine. Two kinds of group need *different* applies: a partition group
// assigns record offsets and serves reads, while the dedicated `__meta/0` group
// updates the node's served catalogue and triggers reconciliation. The
// [`CommitSink`] seam lets one driver shape serve both — a partition group uses
// a [`RecordSink`], the metadata group a [`MetadataSink`] — without the driver
// loop knowing which apply it is performing.

/// What a driven Raft group does with the entries that commit on each step.
///
/// `apply_committed` is fed [`RaftOutput::committed`], which `vela-raft` emits
/// in ascending index order, exactly once per entry; an implementation may
/// therefore assume its input is in-order and non-duplicating, so applying the
/// committed log is itself in-order and idempotent (Requirement 5.1, 5.2;
/// Raft §5.3).
pub trait CommitSink: Send {
    /// Apply newly committed `entries` to the sink's state, in the ascending
    /// index order they are given, exactly once.
    fn apply_committed(&mut self, entries: &[LogEntry]);
}

/// A shared handle the [`MetadataSink`] pokes after applying committed metadata,
/// so the off-loop reconciler can align this node's partition drivers without
/// blocking the metadata Raft step (design §5, H1).
///
/// [`Notify::notify_one`] **coalesces**: many pokes raised before the reconciler
/// next waits collapse into a single wakeup, which is correct because each
/// reconcile pass re-diffs the current served catalogue, so collapsing N pokes
/// into one pass loses nothing. The handle is shared (held by both the sink that
/// pokes it and the reconciler that waits on it), hence an [`Arc`].
pub type ReconcileSignal = Arc<Notify>;

// The `RecordSink` below is the partition commit-apply path. It is introduced
// with the seam (task 1.1) but is not driven by a partition driver yet (the
// `PartitionDriver` still folds commits through its `StateMachine` directly), so
// it has no non-test in-crate constructor until that wiring lands. Allow
// dead_code on it to keep the crate clippy-clean; the allow is a no-op once the
// partition path adopts it. The `MetadataSink` below it is wired into the
// `MetadataDriver` (task 5.2) and so carries no such allow.

/// The partition commit-apply path: wraps the offset-assigning
/// [`StateMachine`], the current partition behaviour.
///
/// A `RecordSink` exists so a partition group can be driven through the same
/// [`CommitSink`] seam as the metadata group while keeping its existing
/// semantics — each committed record entry is assigned the next gap-free,
/// 0-based [`Offset`] and stored so it can be read back, and non-record entries
/// are applied without consuming an offset (Requirement 4.7, 5.1).
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct RecordSink {
    /// The partition state machine that assigns offsets and serves reads.
    state: StateMachine,
}

#[allow(dead_code)]
impl RecordSink {
    /// Create a sink over a fresh, empty [`StateMachine`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Shared, read-only access to the underlying state machine, the source of
    /// committed-record reads.
    pub fn state_machine(&self) -> &StateMachine {
        &self.state
    }
}

impl CommitSink for RecordSink {
    fn apply_committed(&mut self, entries: &[LogEntry]) {
        // Delegate to the existing offset-assigning logic so partition behaviour
        // is unchanged.
        self.state.apply_committed(entries);
    }
}

/// The metadata commit-apply path: folds committed `ClusterCommand`s into the
/// node's shared served [`ClusterMetadata`] and signals the off-loop reconciler.
///
/// For each committed entry the sink:
///
/// - **Ignores** any non-[`PayloadKind::Cluster`] entry — a leader's `Noop`
///   carries no catalogue change (Requirement 5.1, 5.4).
/// - **Decodes** a `Cluster` entry's payload with
///   [`convert::cluster_command_try_from_bytes`] and, on success, applies it to
///   the served catalogue via [`vela_core::apply_command`] in ascending index
///   order (Requirement 5.1).
/// - On a **decode failure**, records a structured error and leaves the served
///   catalogue unchanged by that payload — never a partial or default apply
///   (Requirement 10.4).
///
/// Apply stays fast: it only updates the served view and pokes the
/// [`ReconcileSignal`]. The reconciler itself (opening durable logs, spawning
/// and stopping driver tasks) runs **off** the Raft loop, so applying a commit
/// never blocks metadata heartbeats or elections (H1).
pub struct MetadataSink {
    /// The node's served catalogue, shared with the request path; mutated as
    /// committed `ClusterCommand`s are applied.
    served: Arc<Mutex<ClusterMetadata>>,
    /// Poked once per batch that applied at least one command, waking the
    /// off-loop reconciler. A poke means "the catalogue may have changed,
    /// re-diff"; several pokes coalesce into one idempotent reconcile pass.
    reconcile: ReconcileSignal,
}

impl MetadataSink {
    /// Create a sink over the shared served `metadata`, poking `reconcile` after
    /// any applied catalogue change.
    pub fn new(served: Arc<Mutex<ClusterMetadata>>, reconcile: ReconcileSignal) -> Self {
        Self { served, reconcile }
    }
}

impl CommitSink for MetadataSink {
    fn apply_committed(&mut self, entries: &[LogEntry]) {
        let mut applied_any = false;
        {
            let mut served = self.served.lock().expect("served metadata mutex poisoned");
            for entry in entries {
                // A leader's `Noop` (or any non-`Cluster` entry) carries no
                // catalogue change (Requirement 5.1, 5.4).
                if entry.payload.kind != PayloadKind::Cluster {
                    continue;
                }
                match convert::cluster_command_try_from_bytes(&entry.payload.bytes) {
                    // Apply in the ascending index order the slice is given
                    // (Requirement 5.1).
                    Some(command) => {
                        apply_command(&mut served, &command);
                        applied_any = true;
                    }
                    // Undecodable payload: record a structured error and leave
                    // the served catalogue unchanged by it (Requirement 10.4).
                    None => {
                        tracing::error!(
                            index = entry.index,
                            term = entry.term,
                            "undecodable committed cluster metadata payload; \
                             leaving the served catalogue unchanged"
                        );
                    }
                }
            }
        }
        // Reconcile off the Raft loop, and only when something actually changed
        // (H1; Requirement 6.x is the reconciler's own concern). The poke
        // coalesces, so one signal per applied batch is enough.
        if applied_any {
            self.reconcile.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Layer;

    use vela_core::PartitionReplica;

    use crate::registry::raft_node_id;
    use crate::transport::PeerPool;

    /// A captured Raft role-transition log: its level, message, and the
    /// `partition` + `role` fields the driver attaches (Requirement 15.4).
    #[derive(Clone, Debug)]
    struct CapturedTransition {
        level: Level,
        message: String,
        partition: Option<u64>,
        role: Option<String>,
    }

    /// In-memory layer recording every event's level, message, and the
    /// partition/role fields of role-transition logs.
    #[derive(Clone, Default)]
    struct TransitionCapture {
        events: Arc<Mutex<Vec<CapturedTransition>>>,
    }

    /// Visitor extracting the `message`, `partition`, and `role` fields.
    #[derive(Default)]
    struct TransitionVisitor {
        message: String,
        partition: Option<u64>,
        role: Option<String>,
    }

    impl Visit for TransitionVisitor {
        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "partition" {
                self.partition = Some(value);
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            match field.name() {
                "message" => self.message = format!("{value:?}"),
                "role" => self.role = Some(format!("{value:?}")),
                _ => {}
            }
        }
    }

    impl<S: Subscriber> Layer<S> for TransitionCapture {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = TransitionVisitor::default();
            event.record(&mut visitor);
            self.events.lock().unwrap().push(CapturedTransition {
                level: *event.metadata().level(),
                message: visitor.message,
                partition: visitor.partition,
                role: visitor.role,
            });
        }
    }

    /// Assemble and spawn a single-node driver for `topic`/p0, returning its
    /// command handle. With no peers, the lone replica is its own majority, so
    /// the real-clock election timer promotes it to leader unaided.
    fn spawn_single_node(topic: &str) -> DriverHandle {
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = TimerClock::new(tx.clone());
        let transport = GrpcTransport::new(
            topic.to_string(),
            0,
            "node-a".to_string(),
            Arc::new(PeerPool::new()),
            tx.clone(),
        );
        let replica = PartitionReplica::new(raft_node_id("node-a"), Vec::new());
        let driver = PartitionDriver::new(
            topic.to_string(),
            0,
            "node-a".to_string(),
            replica,
            clock,
            transport,
            rx,
            tx.clone(),
            HashMap::from([(raft_node_id("node-a"), NodeId::new("node-a"))]),
        );
        driver.spawn();
        tx
    }

    /// Produce one value through the driver and await the assigned offset.
    async fn produce(handle: &DriverHandle, value: &[u8]) -> Result<Offset, ProduceError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::Produce {
                value: value.to_vec(),
                reply: reply_tx,
            })
            .expect("driver accepts produce");
        reply_rx.await.expect("driver replies to produce")
    }

    #[tokio::test]
    async fn single_node_driver_elects_then_serves_produce_and_consume() {
        let handle = spawn_single_node("orders");

        // Wait out the randomized election timeout (150–300 ms) so the lone
        // replica becomes leader on its real-clock tick (Requirement 7.2).
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Once elected, the replica reports itself as the known current leader
        // (Requirement 8.1, 8.2).
        assert_eq!(known_leader(&handle).await, Some(NodeId::new("node-a")));

        // Committed records receive gap-free, 0-based offsets (Requirement 4.4,
        // 4.7).
        assert_eq!(produce(&handle, b"v0").await, Ok(0));
        assert_eq!(produce(&handle, b"v1").await, Ok(1));
        assert_eq!(produce(&handle, b"v2").await, Ok(2));

        // Consume returns the committed records in ascending offset order
        // (Requirement 5.1).
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::Consume {
                offset: 0,
                max: 10,
                reply: reply_tx,
            })
            .expect("driver accepts consume");
        let records = reply_rx.await.expect("driver replies to consume");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value, b"v0".to_vec());
        assert_eq!(records[2].offset, 2);
        assert_eq!(records[2].value, b"v2".to_vec());

        // A mid-stream read begins exactly at the requested offset.
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::Consume {
                offset: 1,
                max: 10,
                reply: reply_tx,
            })
            .expect("driver accepts consume");
        let tail = reply_rx.await.expect("driver replies to consume");
        assert_eq!(
            tail.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![1, 2]
        );

        let _ = handle.send(DriverCommand::Shutdown);
    }

    /// Requirement 15.4: while running, the node emits a structured log entry
    /// for each Raft role transition, identifying the partition and the new
    /// role.
    ///
    /// A single-node group has no peers, so its lone replica is its own
    /// majority and promotes itself follower -> candidate -> leader off the
    /// real-clock election timer. A thread-local capture subscriber records the
    /// transition logs the driver emits; with the default current-thread
    /// runtime the spawned driver task runs on this same thread, so its logs
    /// are captured here.
    #[tokio::test]
    async fn role_transition_emits_structured_log_naming_partition_and_role() {
        let capture = TransitionCapture::default();
        let events = capture.events.clone();
        let subscriber = tracing_subscriber::registry().with(capture);
        let _guard = tracing::subscriber::set_default(subscriber);

        let handle = spawn_single_node("orders");

        // Wait out the randomized election timeout (150-300 ms) so the lone
        // replica promotes itself to leader (Requirement 7.2), logging its role
        // transitions on the way (Requirement 15.4).
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = handle.send(DriverCommand::Shutdown);

        let events = events.lock().unwrap();
        let transitions: Vec<&CapturedTransition> = events
            .iter()
            .filter(|e| e.message.contains("raft role transition"))
            .collect();

        assert!(
            !transitions.is_empty(),
            "at least one role-transition log must be emitted"
        );

        // Every transition entry is structured at INFO and names its partition
        // and new role (Requirement 15.4).
        for t in &transitions {
            assert_eq!(t.level, Level::INFO, "role transitions log at INFO");
            assert_eq!(
                t.partition,
                Some(0),
                "each role-transition log must name the partition"
            );
            assert!(
                t.role.is_some(),
                "each role-transition log must name the new role"
            );
        }

        // The lone replica reaches Leader, and that transition is logged with
        // the new role identified (Requirement 15.4).
        assert!(
            transitions
                .iter()
                .any(|t| t.role.as_deref().is_some_and(|r| r.contains("Leader"))),
            "the follower -> leader transition must be logged naming the Leader role"
        );
    }

    /// Query a driver's known current leader and await the answer.
    async fn known_leader(handle: &DriverHandle) -> Option<NodeId> {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::KnownLeader { reply: reply_tx })
            .expect("driver accepts KnownLeader");
        reply_rx.await.expect("driver replies to KnownLeader")
    }

    /// Requirement 8.1, 8.2: a follower reports the leader it last learned from
    /// an `AppendEntries`, mapped from the numeric raft id back to the domain id
    /// through the replica-set lookup.
    #[tokio::test]
    async fn known_leader_reports_the_leader_learned_from_append_entries() {
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = TimerClock::new(tx.clone());
        let transport = GrpcTransport::new(
            "orders".to_string(),
            0,
            "node-a".to_string(),
            Arc::new(PeerPool::new()),
            tx.clone(),
        );
        // A two-voter group: node-a alone is not a majority, so it stays a
        // follower until it hears from a leader.
        let replica = PartitionReplica::new(raft_node_id("node-a"), vec![raft_node_id("node-b")]);
        let lookup = HashMap::from([
            (raft_node_id("node-a"), NodeId::new("node-a")),
            (raft_node_id("node-b"), NodeId::new("node-b")),
        ]);
        let driver = PartitionDriver::new(
            "orders".to_string(),
            0,
            "node-a".to_string(),
            replica,
            clock,
            transport,
            rx,
            tx.clone(),
            lookup,
        );
        driver.spawn();

        // Deliver a heartbeat from node-b at a high term so node-a accepts it as
        // the current-term leader regardless of any election it may have begun.
        let (rpc_reply_tx, rpc_reply_rx) = oneshot::channel();
        tx.send(DriverCommand::PeerRpc {
            msg: RaftMessage::AppendEntries(vela_raft::AppendEntries {
                term: 100,
                leader_id: raft_node_id("node-b"),
                prev_log_index: None,
                prev_log_term: None,
                entries: Vec::new(),
                leader_commit: None,
            }),
            reply: rpc_reply_tx,
        })
        .expect("driver accepts the heartbeat");
        let _ = rpc_reply_rx.await.expect("driver answers the heartbeat");

        assert_eq!(known_leader(&tx).await, Some(NodeId::new("node-b")));
        let _ = tx.send(DriverCommand::Shutdown);
    }

    // -----------------------------------------------------------------------
    // Property 1: in-order, idempotent metadata apply
    // -----------------------------------------------------------------------
    //
    // *For any* committed metadata log prefix delivered in any redelivery or
    // re-replication pattern, applying it through the [`MetadataSink`] yields
    // the same served catalogue, and each committed entry is applied exactly
    // once in ascending index order.
    //
    // Validates: Requirements 5.1, 5.2, 5.4 (Raft §5.3, State Machine Safety
    // §5.4.3).
    //
    // The seam relies on the `vela-raft` contract that [`RaftOutput::committed`]
    // surfaces each newly committed entry exactly once, in ascending index
    // order: the lower-layer `AppendEntries` redelivery / re-replication is
    // absorbed by the log (a retransmitted, already-present entry is skipped),
    // so by the time entries reach `apply_committed` each appears once. A
    // re-replication run therefore manifests at this seam as the committed
    // prefix surfaced across successive calls as the commit index advances in
    // arbitrary jumps. This test models exactly that: it builds a committed
    // prefix and feeds it to the sink under an arbitrary *contiguous* chunking
    // (each entry in exactly one batch, batches in ascending order), then
    // asserts the served catalogue is independent of how the prefix was chunked
    // and equals a single in-order pass — and that each cluster command was
    // applied exactly once.

    use proptest::prelude::*;
    use vela_core::{
        ClusterCommand, LogBackend, NodeAvailability, NodeId, Partition, PartitionIndex,
    };

    /// A small node-id space so replica sets and `SetAvailability` targets
    /// recur across commands.
    fn pb_node_id() -> impl Strategy<Value = NodeId> {
        "[a-c]".prop_map(NodeId::new)
    }

    /// A partition with a small index space and 0..3 replicas.
    fn pb_partition() -> impl Strategy<Value = Partition> {
        (
            0u32..3,
            prop::collection::vec(pb_node_id(), 0..3),
            prop::option::of(pb_node_id()),
        )
            .prop_map(|(index, replicas, leader)| Partition {
                index: PartitionIndex(index),
                replicas,
                leader,
            })
    }

    /// Either of the two log backends.
    fn pb_backend() -> impl Strategy<Value = LogBackend> {
        prop_oneof![Just(LogBackend::Durable), Just(LogBackend::InMemory)]
    }

    /// An arbitrary [`ClusterCommand`]. The tiny topic-name space (`[a-c]`)
    /// makes creates and deletes collide, so the generated prefixes exercise
    /// create-then-delete, re-create, and delete-absent interactions rather
    /// than a flat list of distinct topics.
    fn pb_command() -> impl Strategy<Value = ClusterCommand> {
        prop_oneof![
            (
                "[a-c]",
                prop::collection::vec(pb_partition(), 0..3),
                pb_backend(),
            )
                .prop_map(|(name, partitions, backend)| ClusterCommand::CreateTopic {
                    name,
                    partitions,
                    backend,
                }),
            "[a-c]".prop_map(|name| ClusterCommand::DeleteTopic { name }),
            (
                pb_node_id(),
                prop_oneof![
                    Just(NodeAvailability::Available),
                    Just(NodeAvailability::Unavailable),
                ],
            )
                .prop_map(|(node, availability)| ClusterCommand::SetAvailability {
                    node,
                    availability,
                }),
        ]
    }

    /// One committed-log slot: `Some(command)` is a `PayloadKind::Cluster`
    /// entry; `None` is a leader `Noop`, which carries no catalogue change and
    /// must be ignored by the sink (Requirement 5.1, 5.4). Cluster entries are
    /// weighted higher so most generated prefixes carry real mutations.
    fn pb_slot() -> impl Strategy<Value = Option<ClusterCommand>> {
        prop_oneof![
            3 => pb_command().prop_map(Some),
            1 => Just(None),
        ]
    }

    /// Materialize the committed prefix: one [`LogEntry`] per slot at ascending
    /// index `0..n`, encoding each cluster command with the production codec so
    /// the sink decodes it back over the real `convert` path.
    fn build_entries(slots: &[Option<ClusterCommand>]) -> Vec<LogEntry> {
        slots
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                let payload = match slot {
                    Some(command) => EntryPayload::new(
                        PayloadKind::Cluster,
                        convert::cluster_command_to_bytes(command),
                    ),
                    None => EntryPayload::new(PayloadKind::Noop, Vec::new()),
                };
                LogEntry {
                    index: i as u64,
                    term: 1,
                    payload,
                }
            })
            .collect()
    }

    /// The catalogue produced by applying the whole committed prefix once, in
    /// ascending index order — the canonical single-pass result every chunking
    /// must reproduce. Decodes each cluster entry over the same `convert` path
    /// the sink uses and folds it with [`apply_command`], so this reference is
    /// the sink's behaviour with no batching, isolating the in-order /
    /// exactly-once property from codec fidelity (Property 4's concern).
    fn single_pass(entries: &[LogEntry]) -> ClusterMetadata {
        let mut meta = ClusterMetadata::new();
        for entry in entries {
            if entry.payload.kind != PayloadKind::Cluster {
                continue;
            }
            if let Some(command) = convert::cluster_command_try_from_bytes(&entry.payload.bytes) {
                apply_command(&mut meta, &command);
            }
        }
        meta
    }

    /// Split `entries` into contiguous, ascending batches whose sizes follow
    /// `weights` (cycled, clamped to what remains), covering every entry exactly
    /// once. This models the commit index advancing in arbitrary jumps as
    /// re-replication progresses; empty `weights` yields one batch carrying the
    /// whole prefix.
    fn split_contiguous<'a>(entries: &'a [LogEntry], weights: &[usize]) -> Vec<&'a [LogEntry]> {
        let mut batches = Vec::new();
        let mut pos = 0;
        let mut w = 0;
        while pos < entries.len() {
            let remaining = entries.len() - pos;
            let take = if weights.is_empty() {
                remaining
            } else {
                weights[w % weights.len()].clamp(1, remaining)
            };
            batches.push(&entries[pos..pos + take]);
            pos += take;
            w += 1;
        }
        batches
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Property 1: in-order, idempotent metadata apply.
        ///
        /// **Validates: Requirements 5.1, 5.2, 5.4**
        #[test]
        fn metadata_apply_is_in_order_and_idempotent(
            slots in prop::collection::vec(pb_slot(), 0..20),
            weights in prop::collection::vec(1usize..6, 0..12),
        ) {
            let entries = build_entries(&slots);

            // The catalogue a single in-order pass over the whole committed
            // prefix produces — what any delivery chunking must reproduce.
            let expected = single_pass(&entries);

            // Drive a fresh sink with an arbitrary contiguous chunking of the
            // same prefix: each entry surfaced exactly once, in ascending order,
            // exactly as `RaftOutput.committed` would across successive
            // commit-index advances.
            let served = Arc::new(Mutex::new(ClusterMetadata::new()));
            let reconcile: ReconcileSignal = Arc::new(Notify::new());
            let mut sink = MetadataSink::new(Arc::clone(&served), reconcile);
            for batch in split_contiguous(&entries, &weights) {
                sink.apply_committed(batch);
            }
            let got = served.lock().expect("served mutex not poisoned").clone();

            // In-order + chunking-invariant: applying the committed prefix in
            // any contiguous batching yields exactly the single-pass catalogue
            // (Requirement 5.1, 5.4; State Machine Safety §5.4.3).
            prop_assert_eq!(&got, &expected);

            // Exactly once: every decodable cluster command bumps `epoch` once,
            // so the served epoch equals the count of cluster entries — no entry
            // applied twice (which would over-count) or skipped (under-count)
            // (Requirement 5.2). Noop entries carry no command and never bump it.
            let cluster_count = entries
                .iter()
                .filter(|e| e.payload.kind == PayloadKind::Cluster)
                .filter(|e| convert::cluster_command_try_from_bytes(&e.payload.bytes).is_some())
                .count() as u64;
            prop_assert_eq!(got.epoch, cluster_count);
        }
    }
}

#[cfg(test)]
mod metadata_sink_tests {
    //! Edge-case unit tests for the [`MetadataSink`] commit-apply path (task
    //! 1.4): a leader's `Noop` and any other non-`Cluster` entry are ignored,
    //! and a committed `Cluster` payload that does not decode into a valid
    //! command leaves the served catalogue unchanged rather than applying a
    //! partial or default value (Requirement 5.1, 5.4, 10.4).
    //!
    //! Kept in its own module so it does not collide with the in-order /
    //! idempotent (task 1.2) and convergence (task 1.3) property tests that also
    //! exercise `MetadataSink`.

    use super::*;

    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::Notify;

    use vela_core::{
        ClusterCommand, ClusterMetadata, LogBackend, NodeId, Partition, PartitionIndex,
    };
    use vela_raft::{EntryPayload, LogEntry, PayloadKind};

    use crate::convert;

    /// A `MetadataSink` over a fresh empty served catalogue, returned alongside
    /// the shared catalogue handle and the reconcile signal so a test can
    /// inspect both after applying.
    fn sink() -> (MetadataSink, Arc<Mutex<ClusterMetadata>>, ReconcileSignal) {
        let served = Arc::new(Mutex::new(ClusterMetadata::new()));
        let reconcile: ReconcileSignal = Arc::new(Notify::new());
        let sink = MetadataSink::new(served.clone(), reconcile.clone());
        (sink, served, reconcile)
    }

    /// Build a committed log entry at `index` (term 1) carrying `payload`.
    fn entry(index: u64, payload: EntryPayload) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            payload,
        }
    }

    /// A valid `CreateTopic` command and its encoded `Cluster` payload bytes,
    /// used both as a positive control and to prove an ignored/undecodable
    /// entry around it does not perturb the apply.
    fn create_topic(name: &str) -> ClusterCommand {
        ClusterCommand::CreateTopic {
            name: name.to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas: vec![NodeId::new("node-a")],
                leader: None,
            }],
            backend: LogBackend::InMemory,
        }
    }

    /// Whether the reconcile signal carries a pending wakeup. `notify_one`
    /// stores a single permit that the first poll of `notified()` consumes, so
    /// a bounded wait completes immediately when the sink poked the signal and
    /// times out when it did not.
    async fn reconcile_signalled(reconcile: &Notify) -> bool {
        tokio::time::timeout(Duration::from_millis(50), reconcile.notified())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn noop_entry_is_ignored_and_leaves_catalogue_unchanged() {
        let (mut sink, served, reconcile) = sink();
        let before = served.lock().unwrap().clone();

        // A leader's `Noop` carries no catalogue change (Requirement 5.1, 5.4).
        sink.apply_committed(&[entry(0, EntryPayload::new(PayloadKind::Noop, Vec::new()))]);

        assert_eq!(
            *served.lock().unwrap(),
            before,
            "a Noop entry must leave the served catalogue unchanged"
        );
        assert!(
            !reconcile_signalled(&reconcile).await,
            "an ignored Noop entry must not poke the reconciler"
        );
    }

    #[tokio::test]
    async fn record_entry_is_ignored_and_leaves_catalogue_unchanged() {
        let (mut sink, served, reconcile) = sink();
        let before = served.lock().unwrap().clone();

        // A `Record` entry is partition data, not a metadata command, so the
        // metadata sink ignores it (Requirement 5.1, 5.4).
        sink.apply_committed(&[entry(
            0,
            EntryPayload::new(PayloadKind::Record, b"some-record".to_vec()),
        )]);

        assert_eq!(
            *served.lock().unwrap(),
            before,
            "a non-Cluster Record entry must leave the served catalogue unchanged"
        );
        assert!(
            !reconcile_signalled(&reconcile).await,
            "an ignored Record entry must not poke the reconciler"
        );
    }

    #[tokio::test]
    async fn corrupt_cluster_payload_leaves_catalogue_unchanged() {
        let (mut sink, served, reconcile) = sink();
        let before = served.lock().unwrap().clone();

        // Bytes that are not a valid prost `ClusterCommand` encoding (a
        // truncated field-tag varint) must not decode, so the served catalogue
        // is left unchanged rather than partially/default-applied (Requirement
        // 10.4).
        let garbage = vec![0xff, 0xff, 0xff, 0xff, 0xff];
        assert!(
            convert::cluster_command_try_from_bytes(&garbage).is_none(),
            "test fixture must be genuinely undecodable"
        );
        sink.apply_committed(&[entry(0, EntryPayload::new(PayloadKind::Cluster, garbage))]);

        assert_eq!(
            *served.lock().unwrap(),
            before,
            "an undecodable Cluster payload must leave the served catalogue unchanged"
        );
        assert!(
            !reconcile_signalled(&reconcile).await,
            "an undecodable Cluster payload must not poke the reconciler"
        );
    }

    #[tokio::test]
    async fn empty_cluster_payload_unset_command_leaves_catalogue_unchanged() {
        let (mut sink, served, reconcile) = sink();
        let before = served.lock().unwrap().clone();

        // Empty bytes decode to a `ClusterCommand` with no inner command (an
        // unset oneof); that is not a value the encoder ever produces and must
        // not be folded into a default mutation (Requirement 10.4).
        assert!(
            convert::cluster_command_try_from_bytes(&[]).is_none(),
            "an unset-oneof payload must be treated as undecodable"
        );
        sink.apply_committed(&[entry(
            0,
            EntryPayload::new(PayloadKind::Cluster, Vec::new()),
        )]);

        assert_eq!(
            *served.lock().unwrap(),
            before,
            "an unset-command Cluster payload must leave the served catalogue unchanged"
        );
        assert!(
            !reconcile_signalled(&reconcile).await,
            "an unset-command Cluster payload must not poke the reconciler"
        );
    }

    #[tokio::test]
    async fn valid_cluster_command_is_applied_and_pokes_reconciler() {
        // Positive control: with the same setup the ignore/undecodable tests
        // use, a *valid* command does change the catalogue and signals the
        // reconciler — so the "unchanged" assertions above are not vacuous.
        let (mut sink, served, reconcile) = sink();
        let command = create_topic("orders");

        sink.apply_committed(&[entry(
            0,
            EntryPayload::new(
                PayloadKind::Cluster,
                convert::cluster_command_to_bytes(&command),
            ),
        )]);

        let has_topic = {
            let view = served.lock().unwrap();
            view.topics.contains_key("orders")
        };
        assert!(
            has_topic,
            "a valid CreateTopic command must register the topic in the served catalogue"
        );
        assert!(
            reconcile_signalled(&reconcile).await,
            "an applied catalogue change must poke the reconciler"
        );
    }

    #[tokio::test]
    async fn ignored_and_undecodable_entries_do_not_disturb_a_valid_command_in_the_same_batch() {
        // A batch interleaving a Noop, a valid CreateTopic, an undecodable
        // Cluster payload, and a Record applies exactly the one valid command
        // (in index order) and pokes the reconciler once (Requirement 5.1, 5.4,
        // 10.4).
        let (mut sink, served, reconcile) = sink();
        let command = create_topic("orders");

        sink.apply_committed(&[
            entry(0, EntryPayload::new(PayloadKind::Noop, Vec::new())),
            entry(
                1,
                EntryPayload::new(
                    PayloadKind::Cluster,
                    convert::cluster_command_to_bytes(&command),
                ),
            ),
            entry(
                2,
                EntryPayload::new(PayloadKind::Cluster, vec![0xff, 0xff, 0xff]),
            ),
            entry(3, EntryPayload::new(PayloadKind::Record, b"r".to_vec())),
        ]);

        let (has_topic, topic_count) = {
            let view = served.lock().unwrap();
            (view.topics.contains_key("orders"), view.topics.len())
        };
        assert!(
            has_topic,
            "the one valid command in the batch must be applied"
        );
        assert_eq!(
            topic_count, 1,
            "only the valid command must change the catalogue; ignored/undecodable entries add nothing"
        );
        assert!(
            reconcile_signalled(&reconcile).await,
            "a batch that applied at least one command must poke the reconciler"
        );
    }
}

#[cfg(test)]
mod convergence_property_tests {
    //! Property 3 — Convergence to one catalogue (design §"Correctness
    //! Properties"). Two independent [`MetadataSink`]s that apply the *same*
    //! committed metadata log prefix hold identical served catalogues at the
    //! same commit index, regardless of how that prefix is chunked into
    //! `apply_committed` batches (the redelivery / re-replication shape a real
    //! follower sees). This is the core convergence guarantee that justified the
    //! Raft-native pivot: every node that has applied the log up to the same
    //! commit index agrees on one catalogue (Requirement 5.3, Raft State Machine
    //! Safety §5.4.3).

    use std::sync::{Arc, Mutex};

    use proptest::prelude::*;
    use tokio::sync::Notify;

    use vela_core::{
        apply_command, ClusterCommand, ClusterMetadata, LogBackend, NodeAvailability, NodeId,
        Partition, PartitionIndex,
    };
    use vela_raft::{EntryPayload, LogEntry, PayloadKind};

    use super::{CommitSink, MetadataSink};
    use crate::convert;

    /// A small pool of topic names so creates and deletes target *overlapping*
    /// topics, exercising real catalogue interaction (re-create, delete,
    /// delete-then-recreate) rather than a sequence of disjoint inserts.
    fn topic_name() -> impl Strategy<Value = String> {
        (0u8..5).prop_map(|n| format!("topic-{n}"))
    }

    /// A small pool of node identities for replica sets and availability
    /// commands.
    fn node_id() -> impl Strategy<Value = NodeId> {
        (0u8..4).prop_map(|n| NodeId::new(format!("node-{n}")))
    }

    /// Either of the two log backends a topic can carry.
    fn backend() -> impl Strategy<Value = LogBackend> {
        prop_oneof![Just(LogBackend::Durable), Just(LogBackend::InMemory)]
    }

    /// An arbitrary partition: an index, a non-empty ordered replica set, and an
    /// optional leader drawn from the node pool.
    fn partition() -> impl Strategy<Value = Partition> {
        (
            0u32..4,
            prop::collection::vec(node_id(), 1..4),
            prop::option::of(node_id()),
        )
            .prop_map(|(index, replicas, leader)| Partition {
                index: PartitionIndex(index),
                replicas,
                leader,
            })
    }

    /// An arbitrary committed metadata mutation: create/delete a topic from the
    /// shared name pool, or flip a node's availability.
    fn cluster_command() -> impl Strategy<Value = ClusterCommand> {
        prop_oneof![
            (
                topic_name(),
                prop::collection::vec(partition(), 1..4),
                backend()
            )
                .prop_map(|(name, partitions, backend)| ClusterCommand::CreateTopic {
                    name,
                    partitions,
                    backend,
                }),
            topic_name().prop_map(|name| ClusterCommand::DeleteTopic { name }),
            (
                node_id(),
                prop_oneof![
                    Just(NodeAvailability::Available),
                    Just(NodeAvailability::Unavailable)
                ],
            )
                .prop_map(|(node, availability)| ClusterCommand::SetAvailability {
                    node,
                    availability,
                }),
        ]
    }

    /// Encode `commands` into a contiguous committed metadata log prefix: one
    /// ascending-index `PayloadKind::Cluster` entry per command, using the
    /// server's own codec (the same encoding the durable `__meta` log stores).
    fn entries_for(commands: &[ClusterCommand]) -> Vec<LogEntry> {
        commands
            .iter()
            .enumerate()
            .map(|(i, command)| LogEntry {
                index: i as u64,
                term: 1,
                payload: EntryPayload::new(
                    PayloadKind::Cluster,
                    convert::cluster_command_to_bytes(command),
                ),
            })
            .collect()
    }

    /// A fresh sink over empty served metadata, with its served handle returned
    /// so the resulting catalogue can be read back for comparison.
    fn fresh_sink() -> (MetadataSink, Arc<Mutex<ClusterMetadata>>) {
        let served = Arc::new(Mutex::new(ClusterMetadata::new()));
        let sink = MetadataSink::new(served.clone(), Arc::new(Notify::new()));
        (sink, served)
    }

    /// Apply the whole `entries` prefix to `sink`, split into `apply_committed`
    /// batches of the given `sizes` (a delivery/redelivery shape). Any entries
    /// left after `sizes` is exhausted are applied in one final batch, so the
    /// sink always observes the *entire* prefix — i.e. both sinks end at the
    /// same commit index, differing only in how the prefix was chunked.
    fn apply_in_chunks(sink: &mut MetadataSink, entries: &[LogEntry], sizes: &[usize]) {
        let mut offset = 0;
        for &size in sizes {
            if offset >= entries.len() {
                break;
            }
            let size = size.clamp(1, entries.len() - offset);
            sink.apply_committed(&entries[offset..offset + size]);
            offset += size;
        }
        if offset < entries.len() {
            sink.apply_committed(&entries[offset..]);
        }
    }

    proptest! {
        /// **Property 3: Convergence to one catalogue.**
        ///
        /// Two independent sinks applying the same committed log prefix in
        /// arbitrary (and arbitrarily different) batch chunkings hold identical
        /// served catalogues at the same commit index. The catalogue is also
        /// exactly the deterministic result of applying the command sequence in
        /// order, anchoring "one catalogue" to a single well-defined value.
        ///
        /// **Validates: Requirements 5.3**
        #[test]
        fn two_sinks_applying_same_prefix_converge(
            commands in prop::collection::vec(cluster_command(), 0..24),
            chunks_a in prop::collection::vec(1usize..6, 0..12),
            chunks_b in prop::collection::vec(1usize..6, 0..12),
        ) {
            let entries = entries_for(&commands);

            // Two independent nodes, each applying the identical committed
            // prefix but seeing it delivered in different batch shapes.
            let (mut sink_a, served_a) = fresh_sink();
            let (mut sink_b, served_b) = fresh_sink();
            apply_in_chunks(&mut sink_a, &entries, &chunks_a);
            apply_in_chunks(&mut sink_b, &entries, &chunks_b);

            let catalogue_a = served_a.lock().unwrap().clone();
            let catalogue_b = served_b.lock().unwrap().clone();

            // Convergence: the two nodes agree on one catalogue at the same
            // commit index (Requirement 5.3).
            prop_assert_eq!(&catalogue_a, &catalogue_b);

            // And that one catalogue is the deterministic in-order apply of the
            // committed command sequence — so convergence is to the *correct*
            // value, not merely to a shared wrong one.
            let mut reference = ClusterMetadata::new();
            for command in &commands {
                apply_command(&mut reference, command);
            }
            prop_assert_eq!(&catalogue_a, &reference);
        }
    }
}

#[cfg(test)]
mod metadata_propose_tests {
    //! Unit tests for the metadata-group propose path on [`MetadataDriver`]
    //! (task 6.4): a proposal that reaches a non-leader is redirected with the
    //! known metadata-leader hint and commits nothing (Requirement 4.1; Raft
    //! §8), and a proposal a leader appends but cannot replicate to a majority
    //! resolves as an (indeterminate) [`CoreError::CommitTimeout`] without
    //! advancing the commit index (Requirement 3.5). The topic-name idempotency
    //! that makes a post-timeout retry safe (H2) is covered by the
    //! `create_topic` / `delete_topic` tests in `node.rs`.
    //!
    //! Kept in its own module, with distinct helper names, so it does not
    //! collide with the partition-driver, sink, and convergence test modules
    //! that also live in this file.
    //!
    //! Each test drives every state transition explicitly through the command
    //! queue. The driver's background election timer is harmless here: the
    //! non-leader test keeps node-a a follower behind a freshly-reset election
    //! timer (a valid `AppendEntries` re-arms it), and the commit-timeout test
    //! pre-elects node-a leader, which ignores election ticks. The commit-timeout
    //! test models the elapsed commit deadline by delivering the very
    //! [`DriverCommand::ClusterCommitTimeout`] the armed timer would post once
    //! `COMMIT_TIMEOUT_MS` passes, rather than waiting out that wall-clock
    //! interval.

    use super::*;

    use std::time::{Duration, Instant};

    use vela_core::{LogBackend, Partition, PartitionIndex};
    use vela_raft::RequestVoteReply;

    use crate::registry::raft_node_id;
    use crate::transport::PeerPool;

    /// A [`Clock`] that never advances and whose `arm` is a no-op, used to
    /// pre-drive the shared controller to leader synchronously before the async
    /// driver is spawned. Consensus is driven entirely by explicit inputs, so
    /// the replica is left in exactly the state the test steps it into.
    struct NoopClock;

    impl Clock for NoopClock {
        fn now(&self) -> Instant {
            Instant::now()
        }
        fn arm(&mut self, _kind: TimerKind, _dur: Duration) {}
    }

    /// Spawn a [`MetadataDriver`] over the shared `controller`, with `voters`
    /// (every metadata voter's domain id, including `self_id`) wired into the
    /// known-leader lookup so a redirect hint resolves to a domain [`NodeId`].
    /// Returns the driver's command handle and the shared served catalogue, so a
    /// test can both drive the driver and assert nothing was applied.
    fn spawn_meta_driver(
        self_id: &str,
        voters: &[&str],
        controller: Arc<Mutex<MetadataController>>,
    ) -> (DriverHandle, Arc<Mutex<ClusterMetadata>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = TimerClock::new(tx.clone());
        let transport = GrpcTransport::new(
            vela_core::METADATA_GROUP_TOPIC.to_string(),
            0,
            self_id.to_string(),
            Arc::new(PeerPool::new()),
            tx.clone(),
        );
        let served = Arc::new(Mutex::new(ClusterMetadata::new()));
        let signal: ReconcileSignal = Arc::new(Notify::new());
        let sink = MetadataSink::new(served.clone(), signal);
        let leader_lookup: HashMap<RaftNodeId, NodeId> = voters
            .iter()
            .map(|id| (raft_node_id(id), NodeId::new(*id)))
            .collect();
        let driver = MetadataDriver::new(
            controller,
            self_id.to_string(),
            clock,
            transport,
            rx,
            tx.clone(),
            sink,
            leader_lookup,
        );
        driver.spawn();
        (tx, served)
    }

    /// A minimal single-partition `CreateTopic` command for `name`, replicated
    /// by `node-a`. The exact assignment is irrelevant to the propose-path
    /// behaviour under test; it only needs to be a well-formed command.
    fn create_command(name: &str) -> ClusterCommand {
        ClusterCommand::CreateTopic {
            name: name.to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas: vec![NodeId::new("node-a")],
                leader: None,
            }],
            backend: LogBackend::InMemory,
        }
    }

    /// Propose `command` through the driver and await the commit outcome.
    async fn propose(handle: &DriverHandle, command: ClusterCommand) -> Result<(), CoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::ProposeCluster {
                command,
                reply: reply_tx,
            })
            .expect("driver accepts the proposal");
        reply_rx.await.expect("driver replies to the proposal")
    }

    /// Requirement 4.1 (Raft §8): a topic-admin proposal that reaches a
    /// non-leader metadata replica is **not** committed locally and is
    /// redirected to the current metadata leader, carrying that leader as the
    /// hint so the caller can retry against it.
    #[tokio::test]
    async fn non_leader_propose_redirects_with_leader_hint_and_commits_nothing() {
        // A two-voter metadata group: node-a alone is never a majority, so it
        // stays a follower until it hears from a leader.
        let controller = Arc::new(Mutex::new(MetadataController::new(
            raft_node_id("node-a"),
            vec![raft_node_id("node-b")],
        )));
        let (handle, served) =
            spawn_meta_driver("node-a", &["node-a", "node-b"], controller.clone());

        // Deliver a heartbeat from node-b at a high term so node-a accepts node-b
        // as the current-term metadata leader regardless of any election it may
        // have begun (Raft §5.2 followers track the current leader).
        let (rpc_reply_tx, rpc_reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::PeerRpc {
                msg: RaftMessage::AppendEntries(vela_raft::AppendEntries {
                    term: 100,
                    leader_id: raft_node_id("node-b"),
                    prev_log_index: None,
                    prev_log_term: None,
                    entries: Vec::new(),
                    leader_commit: None,
                }),
                reply: rpc_reply_tx,
            })
            .expect("driver accepts the heartbeat");
        let _ = rpc_reply_rx.await.expect("driver answers the heartbeat");

        // Proposing on the non-leader redirects to the known leader (node-b) and
        // appends/commits nothing (Requirement 4.1).
        let result = propose(&handle, create_command("orders")).await;
        assert_eq!(
            result,
            Err(CoreError::NotLeader {
                leader: Some(NodeId::new("node-b")),
            }),
            "a non-leader proposal must redirect with the known metadata-leader hint"
        );

        // Commits nothing and appends nothing: the metadata log and commit index
        // are untouched, and the served catalogue is unchanged.
        {
            let controller = controller.lock().expect("controller mutex not poisoned");
            assert_eq!(
                controller.commit_index(),
                None,
                "a non-leader proposal must not commit"
            );
            assert_eq!(
                controller.last_log_index(),
                None,
                "a non-leader proposal must append nothing"
            );
        }
        assert!(
            served
                .lock()
                .expect("served mutex not poisoned")
                .topics
                .is_empty(),
            "a redirected proposal must leave the served catalogue unchanged"
        );

        let _ = handle.send(DriverCommand::Shutdown);
    }

    /// Requirement 3.5: a proposal the leader appends but cannot replicate to a
    /// majority before the commit deadline resolves as [`CoreError::CommitTimeout`]
    /// and does not advance the commit index. The outcome is *indeterminate*
    /// (the entry may still commit under a new leader), so it must not be
    /// reported as success (H2).
    #[tokio::test]
    async fn leader_propose_that_never_commits_times_out() {
        // A two-voter group where node-b is never reachable, so no proposal can
        // reach a commit majority. Pre-drive node-a to leader synchronously
        // (self-vote on the first election tick, then node-b's granted vote)
        // before the async driver is spawned; a leader ignores later election
        // ticks, so it stays leader.
        let controller = Arc::new(Mutex::new(MetadataController::new(
            raft_node_id("node-a"),
            vec![raft_node_id("node-b")],
        )));
        {
            let mut controller = controller.lock().expect("controller mutex not poisoned");
            let mut clock = NoopClock;
            // First election tick: term 0 -> 1, casting node-a's self-vote.
            controller.step(RaftInput::Tick(TimerKind::Election), &mut clock);
            // node-b grants its vote for term 1, crossing the 2-voter majority.
            controller.step(
                RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                    term: 1,
                    vote_granted: true,
                    voter: raft_node_id("node-b"),
                })),
                &mut clock,
            );
            assert_eq!(
                controller.role(),
                Some(Role::Leader),
                "node-a must reach metadata leadership with a majority of votes"
            );
            assert_eq!(
                controller.commit_index(),
                None,
                "an empty leader log has nothing committed yet"
            );
        }

        let (handle, served) =
            spawn_meta_driver("node-a", &["node-a", "node-b"], controller.clone());

        // Propose on the leader: it appends the entry but cannot commit it,
        // since node-b never acknowledges replication.
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .send(DriverCommand::ProposeCluster {
                command: create_command("orders"),
                reply: reply_tx,
            })
            .expect("driver accepts the proposal");

        // Wait until the driver has appended the proposal — this proves the
        // pending entry is registered and its commit-timeout timer armed — then
        // capture the log index it occupies (the timer's target).
        let target = loop {
            if let Some(index) = controller
                .lock()
                .expect("controller mutex not poisoned")
                .last_log_index()
            {
                break index;
            }
            tokio::task::yield_now().await;
        };

        // Model the elapsed commit deadline by delivering the same
        // `ClusterCommitTimeout` the armed timer would post once
        // `COMMIT_TIMEOUT_MS` passes (Requirement 3.5).
        handle
            .send(DriverCommand::ClusterCommitTimeout { target })
            .expect("driver accepts the commit-timeout poke");

        let result = reply_rx.await.expect("driver replies to the proposal");
        assert_eq!(
            result,
            Err(CoreError::CommitTimeout),
            "a proposal that does not commit within the deadline must time out"
        );

        // The entry was appended but never committed, and an uncommitted entry
        // is never applied, so the served catalogue stays empty.
        assert_eq!(
            controller
                .lock()
                .expect("controller mutex not poisoned")
                .commit_index(),
            None,
            "a timed-out proposal must not advance the commit index"
        );
        assert!(
            served
                .lock()
                .expect("served mutex not poisoned")
                .topics
                .is_empty(),
            "no commit means no applied catalogue change"
        );

        let _ = handle.send(DriverCommand::Shutdown);
    }
}
