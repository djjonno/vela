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

use std::collections::VecDeque;

use tokio::sync::{mpsc, oneshot};

use vela_core::{CommittedRecord, Offset, PartitionReplica, COMMIT_TIMEOUT_MS};
use vela_raft::{
    Clock, EntryPayload, LogStorage, PayloadKind, RaftInput, RaftMessage, RaftOutput, Role,
    TimerKind, Transport, ELECTION_TIMEOUT_BASE,
};

use crate::clock::TimerClock;
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
}
