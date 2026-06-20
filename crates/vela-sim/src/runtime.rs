//! `SimRuntime`: the single-threaded discrete-event step loop (design Option A).
//!
//! The runtime is the harness's analogue of the production `tokio` driver: it
//! pops one [`Event`] from the [`Scheduler`] at a time and *dispatches* it
//! against the [`SimulatedCluster`], feeding the right [`RaftInput`] to the
//! right replica, routing the resulting effects, and enqueuing the follow-on
//! events the step produced — all on one thread, with no `tokio`, no real
//! network, and no wall clock (Requirement 3.2, 3.3).
//!
//! # The atomic-event invariant (Requirement 4.4)
//!
//! One [`SimRuntime::step`] processes exactly one event to completion before the
//! next is selected: a single `replica.step`, its `out.committed` applied, and
//! its `out.sends` enqueued — together with the timers the step (re-)armed — are
//! all scheduled back onto the timeline *before* the scheduler hands out the
//! next event. The [`Scheduler`] already enforces earliest-instant ordering with
//! a seed-derived tie-break; the runtime's job is to make sure every follow-on
//! effect flows back through that one queue so the whole run stays a pure
//! function of the seed.
//!
//! # Per-event dispatch
//!
//! - [`Event::TimerFire`] — drop the tick if it is stale (a later re-arm
//!   superseded it, [`SimClock::is_current`](crate::clock::SimClock::is_current))
//!   or its node is crashed; otherwise feed [`RaftInput::Tick`] to the replica
//!   and [process the output](Self::process_output).
//! - [`Event::MessageDeliver`] — feed [`RaftInput::Message`] to the recipient
//!   replica (dropping it if the recipient is crashed or no longer hosts the
//!   group) and process the output.
//! - [`Event::FaultHeal`] — lift the network cut installed under the heal id
//!   ([`SimNetwork::heal`](crate::network::SimNetwork::heal)).
//! - [`Event::FaultApply`] / [`Event::ClientOp`] — documented hooks the
//!   fault-schedule and workload/history wiring fill in (see each handler); they
//!   are accepted and dispatched here so the step loop is complete, but their
//!   payloads are still placeholders owned by concurrent tasks.
//!
//! # Follow-on effects (the shared "process output" routine)
//!
//! After a replica step, [`process_output`](Self::process_output):
//!
//! 1. **Commits.** For the metadata group, decodes each committed
//!    [`PayloadKind::Cluster`] entry and folds it into every node's served
//!    catalogue, then reconciles partition fleets, through
//!    [`SimulatedCluster::apply_committed_metadata`]. Partition-group commits
//!    need nothing here: [`PartitionReplica::step`](vela_core::PartitionReplica::step)
//!    already folded them into the state machine, assigning record offsets.
//! 2. **Pending client ops.** A hook ([`resolve_committed`](Self::resolve_committed))
//!    the workload + history wiring fills in.
//! 3. **Sends.** Dispatches `out.sends` through the node's [`SimTransport`] for
//!    the group (*after* the step, never inside it — exactly as production and
//!    `vela_raft::sim::SimCluster` do), which applies the network faults and
//!    buffers deliveries.
//! 4. **Timers.** Drains the clock's freshly-armed timers and schedules each as
//!    a [`Event::TimerFire`].
//! 5. **Deliveries.** Drains the network's buffered deliveries and schedules
//!    each as a [`Event::MessageDeliver`].
//!
//! Steps 4 and 5 run *after* the sends are dispatched, so a message sent this
//! step is drained and scheduled this step. The scheduler's `(at, tie_break)`
//! order then fixes delivery order deterministically.
//!
//! # Scope
//!
//! This module implements both the per-event **dispatch mechanism** (its
//! follow-on effects) and the **run orchestration** ([`SimRuntime::run`]):
//! validating the config, building the cluster, seeding the workload +
//! [`Fault_Schedule`](crate) onto the timeline, bootstrapping elections (the
//! metadata group at start, and each freshly spawned / recovered partition
//! replica thereafter), looping [`step`](SimRuntime::step) to the [`Budget`]
//! (honoring the optional `VELA_DST_MAX_EVENTS` override), feeding the checkers
//! incrementally, and returning an [`Outcome`]. Client operations are issued
//! against the routed leader (following redirects up to 5 hops) and their
//! responses recorded into the [`History`]; expected outcomes (redirect
//! exhaustion, no leader, an uncommitted proposal) are recorded as valid
//! responses, never property violations.

use vela_core::{
    metadata_group_key, ClusterCommand, GroupKey, LogBackend, NodeId, Offset, Partition,
    PartitionIndex, PartitionReplica, Record,
};
use vela_log::{EntryPayload, LogEntry, LogStorage, PayloadKind};
use vela_raft::{Clock, RaftInput, Role, TimerKind, Transport, ELECTION_TIMEOUT_BASE};

use crate::checker::kafka_parity::KafkaParityChecker;
use crate::checker::liveness::LivenessChecker;
use crate::checker::{PropertyId, RaftSafetyChecker, Violation};
use crate::cluster::{ClusterError, SimNode, SimulatedCluster};
use crate::codec::{decode_cluster_command, encode_cluster_command};
use crate::history::{History, OpArgs, OpResponse, RecordedOp};
use crate::rng::SeedStreams;
use crate::scenario::{Budget, RunConfig, ScenarioParameters};
use crate::scheduler::{
    ClientOp, Event, Fault, HealId, Scheduled, Scheduler, Step, VirtualDuration, VirtualInstant,
};
use crate::workload::{self, ClientOperation, Workload, MAX_REDIRECT_HOPS};

/// An error raised while dispatching an event against the cluster.
///
/// Dispatch is fallible only where the production reconcile path is: applying a
/// committed metadata command can spawn a partition replica, and opening its
/// Sim_Storage WAL can fail. Surfaced as a typed error rather than a panic so
/// the run orchestration (a later task) can end the run cleanly.
#[derive(thiserror::Error, Debug)]
pub enum RuntimeError {
    /// A cluster operation (a committed-metadata apply and its reconcile spawn)
    /// failed during dispatch.
    #[error("cluster operation failed during event dispatch: {0}")]
    Cluster(#[from] ClusterError),
}

/// The pass/fail result of a [`Simulation_Run`] (design "Run inputs/outputs").
///
/// A run is a pure function of its `(seed, params)` (Requirement 1.3), so two
/// runs of the same [`RunConfig`] in the same process produce the identical
/// `Outcome` — the contract the reproducibility property (Property 1) asserts.
///
/// - [`Passed`](Self::Passed) — the run completed (reached its [`Budget`] or went
///   quiescent) with no Safety_, Kafka-parity, or Liveness_Property violated.
/// - [`Failed`](Self::Failed) — a checker detected a property breach; carries the
///   violated [`PropertyId`], the logical [`VirtualInstant`] at which it was
///   detected, and a human-readable `detail` (Requirement 2.3, 10.6).
/// - [`Invalid`](Self::Invalid) — the run could not be *performed*: the
///   [`ScenarioParameters`] were rejected by
///   [`validate`](crate::scenario::ScenarioParameters::validate)
///   (Requirement 15.5), the cluster could not be assembled, or an unrecoverable
///   runtime error was hit mid-dispatch. This is reported rather than panicked
///   so a caller (and `proptest`) always gets a value back.
///
/// [`Simulation_Run`]: crate
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The run completed with every checked property holding.
    Passed,
    /// A property was violated; the run ended with this failure.
    Failed {
        /// The property that was breached.
        property: PropertyId,
        /// The logical instant the breach was detected.
        at: VirtualInstant,
        /// A human-readable description naming the affected group / term /
        /// replicas.
        detail: String,
    },
    /// The run could not be performed (invalid parameters, assembly failure, or
    /// an unrecoverable runtime error); carries a human-readable reason.
    Invalid {
        /// Why the run could not be performed.
        detail: String,
    },
}

impl From<Violation> for Outcome {
    /// Map a checker's [`Violation`] into a failing [`Outcome`].
    fn from(v: Violation) -> Self {
        Outcome::Failed {
            property: v.property,
            at: v.at,
            detail: v.detail,
        }
    }
}

/// The logical instant the first generated client operation is issued.
///
/// A small lead before the first operation gives the metadata group time to hold
/// its first election (the 150–300 ms window) so that early topic-admin
/// operations can reach a metadata leader rather than uniformly recording a
/// no-leader response.
const CLIENT_OP_START_NANOS: u64 = 400_000_000; // 400 ms

/// The logical spacing between successive client operations on the timeline.
///
/// Operations are issued at a steady cadence interleaved with the
/// [`Fault_Schedule`](crate), so produce / consume traffic continues while
/// crashes, restarts, and partitions are in effect (Requirement 8.7).
const CLIENT_OP_INTERVAL_NANOS: u64 = 20_000_000; // 20 ms

/// The bounded simulated-time span a favorable group is given to make progress
/// before a stall is a Liveness_Property violation (Requirement 12.5).
///
/// Generous relative to the election window (150–300 ms) and one-way latency, so
/// the check never fires for a group that is simply mid-election or mid-
/// replication — only for a group that is genuinely stuck while a majority is up
/// and mutually reachable.
const LIVENESS_BUDGET_SECS: u64 = 5;

/// How often (in processed events) the structural Raft-safety pass
/// ([`RaftSafetyChecker::check_logs`]) runs during the loop, in addition to the
/// final pass. The cheap incremental checks ([`RaftSafetyChecker::observe`]) run
/// every event regardless.
const CHECK_LOGS_INTERVAL: u64 = 2_000;

/// The deterministic instants of the conservative fault schedule (Requirement
/// 6.5): crash a minority, restart it, then partition a minority, then heal —
/// each strictly after the previous one ends so a majority always survives.
const CRASH_AT_NANOS: u64 = 1_000_000_000; // 1 s
const RESTART_AT_NANOS: u64 = 2_000_000_000; // 2 s
const PARTITION_AT_NANOS: u64 = 3_000_000_000; // 3 s
const PARTITION_HEAL_AT_NANOS: u64 = 4_000_000_000; // 4 s

/// The [`HealId`] the scheduled network partition is installed and healed under.
const PARTITION_HEAL_ID: u64 = 1;

/// What a pending (proposed, not-yet-committed) client operation will record
/// when its target log index commits.
#[derive(Debug, Clone)]
enum PendingKind {
    /// A produce: on commit, record `ProduceOk` with the assigned offset.
    Produce,
    /// A topic create: on commit, record `CreateTopicOk` for this topic.
    CreateTopic {
        /// The created topic name.
        name: String,
    },
    /// A topic delete: on commit, record `DeleteTopicOk` for this topic.
    DeleteTopic {
        /// The deleted topic name.
        name: String,
    },
}

impl PendingKind {
    /// The [`PayloadKind`] the committed entry at the target index must carry for
    /// this pending operation to count as committed (a `Record` for a produce, a
    /// `Cluster` command for a topic admin op).
    fn payload_kind(&self) -> PayloadKind {
        match self {
            PendingKind::Produce => PayloadKind::Record,
            PendingKind::CreateTopic { .. } | PendingKind::DeleteTopic { .. } => {
                PayloadKind::Cluster
            }
        }
    }
}

/// A client operation that has been proposed to a leader and is awaiting commit.
///
/// The runtime resolves it when the entry it occupies (`target`) commits on a
/// running replica of `group`, recording the response into the [`History`]. If
/// the proposing leader was deposed and a different entry committed at `target`,
/// the proposal was superseded and is recorded as an error rather than a
/// (false) success.
#[derive(Debug, Clone)]
struct Pending {
    /// The Raft group the operation was proposed to.
    group: GroupKey,
    /// The log index the proposed entry occupies on the leader.
    target: u64,
    /// The request arguments, recorded with the eventual response.
    args: OpArgs,
    /// The invocation instant on the virtual clock.
    invoked_at: VirtualInstant,
    /// The opaque payload bytes the proposal carried, used to confirm the entry
    /// that committed at `target` is actually this proposal.
    expected: Vec<u8>,
    /// What to record on commit.
    kind: PendingKind,
}

/// The result of resolving a leader for a client operation by following the
/// cluster's redirects (Requirement 8.4, 8.5, 8.6).
enum LeaderResolution {
    /// A current leader was found at this node index.
    Leader(usize),
    /// No leader is currently available for the group.
    NoLeader,
    /// Five successive redirections did not reach a current leader.
    Exhausted,
}

/// The single-threaded discrete-event runtime that drives a
/// [`SimulatedCluster`] through one [`Scheduler`] timeline.
///
/// Owns the cluster and the scheduler for a [`Simulation_Run`]. The run
/// orchestration (a later task) seeds the initial timeline through
/// [`scheduler_mut`](Self::scheduler_mut) and then loops [`step`](Self::step)
/// until the [`Budget`] ends the run; each `step` pops one event and applies its
/// follow-on effects atomically.
///
/// [`Simulation_Run`]: crate
pub struct SimRuntime {
    /// The in-process cluster of production replicas under test.
    cluster: SimulatedCluster,
    /// The global discrete-event queue and logical clock.
    scheduler: Scheduler,
    /// The seed-derived client workload; a [`ClientOp`] event's `seq` indexes
    /// into it. Empty unless the run orchestration ([`run`](Self::run)) populated
    /// it, so a runtime driven directly (as the dispatch unit tests do) carries
    /// no workload.
    workload: Workload,
    /// The recorded [`History`] of every issued client operation and its
    /// response (Requirement 9.1).
    history: History,
    /// Client operations proposed to a leader and awaiting commit, resolved by
    /// [`resolve_pending`](Self::resolve_pending) when their target index
    /// commits.
    pending: Vec<Pending>,
    /// The `(node, group)` replicas whose first election timer the orchestration
    /// has already armed, so each replica is kick-started exactly once per
    /// incarnation ([`seed_initial_elections`](Self::seed_initial_elections)); a
    /// node's entries are cleared on restart so its recovered replicas are
    /// re-seeded.
    seeded: std::collections::HashSet<(NodeId, GroupKey)>,
    /// Topic names whose committed `DeleteTopic` was applied since the last
    /// [`drive`](Self::drive) iteration drained them.
    ///
    /// Filled by [`process_output`](Self::process_output) when it decodes a
    /// committed [`ClusterCommand::DeleteTopic`] on the metadata group, and
    /// drained by the run loop to forget the deleted topic's groups in the
    /// [`RaftSafetyChecker`] — the group-incarnation boundary at which its
    /// `(group, term)` leader and commit high-water must be dropped so a
    /// same-name re-creation (term 0 / commit `None`) is not mistaken for a
    /// continuation. Detecting the *command* (rather than diffing served
    /// catalogues) catches a delete even when its re-create commits in the same
    /// step — including a crashed node that replays a missed delete+recreate
    /// together on restart.
    deleted_topics: Vec<String>,
}

impl SimRuntime {
    /// Build a runtime around an assembled `cluster`, with a [`Scheduler`]
    /// bounded by `budget` and ordering simultaneous events with the cluster's
    /// own `tiebreak` RNG stream.
    ///
    /// Drawing the scheduler's tie-break stream from the cluster
    /// ([`SimulatedCluster::tiebreak_stream`]) keeps every random decision in the
    /// run — timers, network faults, *and* event ordering — derived from the one
    /// seed, so the run stays reproducible (Requirement 1.5).
    #[must_use]
    pub fn new(cluster: SimulatedCluster, budget: Budget) -> Self {
        let scheduler = Scheduler::new(budget, cluster.tiebreak_stream());
        Self {
            cluster,
            scheduler,
            workload: Workload::default(),
            history: History::new(),
            pending: Vec::new(),
            seeded: std::collections::HashSet::new(),
            deleted_topics: Vec::new(),
        }
    }

    /// Shared, read-only access to the cluster (for the checkers and tests).
    #[must_use]
    pub fn cluster(&self) -> &SimulatedCluster {
        &self.cluster
    }

    /// Mutable access to the cluster (for the run orchestration and tests).
    pub fn cluster_mut(&mut self) -> &mut SimulatedCluster {
        &mut self.cluster
    }

    /// Shared, read-only access to the recorded [`History`] of client operations
    /// (for the checkers, artifacts, and tests).
    #[must_use]
    pub fn history(&self) -> &History {
        &self.history
    }

    /// Shared, read-only access to the scheduler (for diagnostics and tests).
    #[must_use]
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// Mutable access to the scheduler, so the run orchestration can seed the
    /// initial timeline (the first election timers, the workload, the
    /// fault schedule) before looping [`step`](Self::step).
    pub fn scheduler_mut(&mut self) -> &mut Scheduler {
        &mut self.scheduler
    }

    /// Advance the run by one event: pop the earliest pending event, dispatch
    /// it, and schedule its follow-on effects — or report the run is over.
    ///
    /// The whole of one event's processing happens here before the next event is
    /// selected (the atomic-event invariant, Requirement 4.4). Returns the
    /// [`Step`] the scheduler produced ([`Step::Event`] with the dispatched event
    /// for diagnostics, or [`Step::Done`] with the end reason).
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if dispatching the event failed (a committed
    /// metadata apply whose reconcile spawn could not open a replica's storage);
    /// the run orchestration ends the run on such an error.
    pub fn step(&mut self) -> Result<Step, RuntimeError> {
        let step = self.scheduler.step();
        if let Step::Event(scheduled) = &step {
            self.dispatch_event(scheduled)?;
        }
        Ok(step)
    }

    /// Dispatch one already-popped `scheduled` event against the cluster,
    /// applying its follow-on effects.
    ///
    /// The scheduler has already advanced `now` to the event's instant, so the
    /// runtime mirrors that instant onto the network bus and (via
    /// [`SimulatedCluster::step_replica`]) the clock before stepping.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if applying a committed metadata command failed.
    pub fn dispatch_event(&mut self, scheduled: &Scheduled) -> Result<(), RuntimeError> {
        let now = self.scheduler.now();
        match &scheduled.event {
            Event::TimerFire {
                node,
                group,
                kind,
                generation,
            } => {
                // Resolve the node and skip the tick if it is crashed (a crashed
                // node processes no events, Requirement 6.1).
                let Some(index) = self.cluster.index_of(node) else {
                    return Ok(());
                };
                if !self.cluster.node(index).is_some_and(|n| n.is_running()) {
                    return Ok(());
                }
                // Drop a stale timer superseded by a later re-arm (mirrors
                // `TimerClock::is_current`): election-timer reset with no
                // explicit cancellation path.
                if !self
                    .cluster
                    .clock()
                    .is_current(node, group, *kind, *generation)
                {
                    return Ok(());
                }
                self.cluster.network().set_now(now);
                if let Some(out) =
                    self.cluster
                        .step_replica(index, group, now, RaftInput::Tick(*kind))
                {
                    self.process_output(index, group, out)?;
                }
            }
            Event::MessageDeliver(envelope) => {
                // Deliver to the recipient replica, dropping the message if the
                // recipient is crashed or no longer hosts the group.
                let Some(index) = self.cluster.index_of(&envelope.to) else {
                    return Ok(());
                };
                if !self.cluster.node(index).is_some_and(|n| n.is_running()) {
                    return Ok(());
                }
                self.cluster.network().set_now(now);
                let input = RaftInput::Message(envelope.msg.clone());
                if let Some(out) = self
                    .cluster
                    .step_replica(index, &envelope.group, now, input)
                {
                    self.process_output(index, &envelope.group, out)?;
                }
            }
            Event::ClientOp(op) => self.dispatch_client_op(op.seq)?,
            Event::FaultApply(fault) => self.apply_fault(fault)?,
            Event::FaultHeal(heal) => {
                // Lift the network cut installed under this id; delivery resumes
                // for messages sent at or after the heal (Requirement 5.7).
                self.cluster.network().heal(*heal);
            }
        }
        Ok(())
    }

    /// Apply the follow-on effects of one `replica.step` for the replica of
    /// `group` on the node at `index`, in the order the design fixes.
    ///
    /// See the module docs for the full sequence. The ordering that matters for
    /// correctness: commits are applied (and metadata reconciled) before sends
    /// are dispatched, and the network is drained *after* the sends so a message
    /// sent this step is scheduled this step.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if a committed metadata apply (and its reconcile
    /// spawn) failed.
    fn process_output(
        &mut self,
        index: usize,
        group: &GroupKey,
        out: vela_raft::RaftOutput,
    ) -> Result<(), RuntimeError> {
        let is_meta = group == &metadata_group_key();

        // 1. Apply committed entries. A partition replica already folded its
        //    committed records into the state machine inside `step` (offsets
        //    assigned on commit), so only the metadata group needs work here:
        //    fold each committed `Cluster` command into every node's served
        //    catalogue and reconcile (Requirement 3.2).
        if is_meta {
            for entry in &out.committed {
                if entry.payload.kind == PayloadKind::Cluster {
                    let command = decode_cluster_command(&entry.payload.bytes);
                    // Record a committed topic delete so the run loop can forget
                    // the group's accumulated Raft-safety state at this
                    // incarnation boundary. Captured here, from the command
                    // itself, so it is seen even when the matching re-create
                    // commits in the same batch (e.g. a restarted node replaying
                    // a missed delete+recreate) — a served-catalogue diff would
                    // miss that, since the topic is present before and after.
                    if let ClusterCommand::DeleteTopic { name } = &command {
                        self.deleted_topics.push(name.clone());
                    }
                    self.cluster.apply_committed_metadata(&command)?;
                }
            }
        }

        // 2. Resolve any pending client operation whose target index just
        //    committed, recording its response into the History.
        self.resolve_pending();

        // 3. Dispatch `out.sends` through this replica's transport, *after* the
        //    step. The bus's `now` was mirrored in `dispatch_event`, so each
        //    delivery is scheduled relative to the event being processed. The
        //    transport is cloned (cheap: an `Rc` plus the group's peer map) to
        //    release the cluster borrow before the timer/network drains below.
        if let Some(transport) = self.cluster.transport_for(index, group) {
            let transport = transport.clone();
            for (to, msg) in out.sends {
                transport.send(to, msg);
            }
        }

        // 4. Schedule the timers this step (re-)armed. Clearing the active
        //    replica afterwards catches a stray arm with no active replica.
        for armed in self.cluster.clock_mut().drain_armed() {
            self.scheduler.schedule(armed.at, armed.to_event());
        }
        self.cluster.clock_mut().clear_active();

        // 5. Schedule the deliveries the sends just buffered.
        for (at, envelope) in self.cluster.network().drain_pending() {
            self.scheduler.schedule(at, Event::MessageDeliver(envelope));
        }

        Ok(())
    }

    // ----- Run orchestration (task 20.1) ----------------------------------

    /// Execute one [`Simulation_Run`] from `config` and return its [`Outcome`].
    ///
    /// The whole run is single-threaded and depends only on `(seed, params)`
    /// (Requirement 1.3): no wall clock, no real I/O, no threads. Two calls with
    /// an identical `config` in the same process yield the identical `Outcome`.
    ///
    /// The orchestration:
    ///
    /// 1. **Validate** `config.params` (Requirement 15.5). On a
    ///    [`ScenarioError`](crate::scenario::ScenarioError) it returns
    ///    [`Outcome::Invalid`] rather than panicking.
    /// 2. **Build** the [`SimulatedCluster`] (every node's `__meta/0` group
    ///    recovered over Sim_Storage). An assembly failure is [`Outcome::Invalid`].
    /// 3. **Budget.** The per-run event budget is `params.budget.max_events`,
    ///    overridden by the `VELA_DST_MAX_EVENTS` environment variable when it is
    ///    set and parses as a `u64` (so CI can bound run length without changing
    ///    code); the virtual-time budget is unchanged.
    /// 4. **Seed the timeline.** Generate the seed-driven [`Workload`] and place a
    ///    [`ClientOp`] event for each operation, plus the deterministic
    ///    [`Fault_Schedule`](crate), onto the scheduler; then arm the metadata
    ///    group's first election timers (bootstrap).
    /// 5. **Step to the budget.** Each [`step`](Self::step) processes one event;
    ///    after it the runtime arms the first election timer of any freshly
    ///    spawned partition replica, notifies the [`LivenessChecker`] of any fault
    ///    apply/heal, and feeds the checkers incrementally
    ///    ([`RaftSafetyChecker::observe`] every event, the structural pass
    ///    periodically). On the first [`Violation`] the run ends with
    ///    [`Outcome::Failed`] stamped with the detection instant (Requirement 2.3,
    ///    10.6).
    /// 6. **Final passes.** After the run ends (budget or quiescence) the
    ///    structural Raft-safety, Kafka-parity, and liveness checks run once over
    ///    the end state. With no violation the run [`Passed`](Outcome::Passed).
    ///
    /// [`Simulation_Run`]: crate
    /// [`ScenarioError`]: crate::scenario::ScenarioError
    #[must_use]
    pub fn run(config: RunConfig) -> Outcome {
        let seed = config.seed;
        let params = config.params;

        // Execute the run, keeping the recorded History so the artifacts can be
        // persisted (the History lives inside the runtime, which is dropped when
        // `run_collecting` returns).
        let (outcome, history, _events_processed) = Self::run_collecting(config);

        // Persist the run summary (always) and, on failure, the full
        // FailureArtifact to the CI-collectable directory (Requirement 13.4).
        // Guarded on the artifact-dir env var being set so the many unit /
        // property tests that call `run` repeatedly do not spam the filesystem;
        // CI sets the variable (a developer can opt in locally). A write failure
        // is deliberately swallowed: failing to persist diagnostics must not
        // change the run's Outcome.
        if std::env::var_os(crate::artifact::ARTIFACT_DIR_ENV).is_some() {
            let _ = crate::artifact::persist_run(seed, params, &outcome, &history);
        }

        outcome
    }

    /// Execute one [`Simulation_Run`] from `config` and return its [`Outcome`]
    /// together with the number of discrete [`Event`]s the run actually
    /// processed.
    ///
    /// This is a thin, behavior-preserving observation wrapper over the same
    /// orchestration [`run`](Self::run) performs: it makes the run's
    /// processed-event count observable so a caller can assert the per-run event
    /// budget — including the optional `VELA_DST_MAX_EVENTS` override applied at
    /// run start — bounds the run (Requirement 14.5). The returned count is
    /// always less than or equal to the effective `max_events` budget, since the
    /// [`Scheduler`] stops handing out events once the budget is reached.
    ///
    /// Unlike [`run`](Self::run) this does not persist failure artifacts; it is
    /// intended for tests and diagnostics that need the event count rather than
    /// the on-disk artifact.
    ///
    /// [`Simulation_Run`]: crate
    #[must_use]
    pub fn run_observed(config: RunConfig) -> (Outcome, u64) {
        let (outcome, _history, events_processed) = Self::run_collecting(config);
        (outcome, events_processed)
    }

    /// Execute one run and return its [`Outcome`], the recorded [`History`], and
    /// the number of [`Event`]s processed, so [`run`](Self::run) can persist
    /// artifacts (using the history) after the runtime is dropped and
    /// [`run_observed`](Self::run_observed) can surface the processed-event
    /// count. This carries the full run orchestration; see [`run`](Self::run)
    /// for the step-by-step description.
    fn run_collecting(config: RunConfig) -> (Outcome, History, u64) {
        // 1. Validate before building anything (Requirement 15.5).
        if let Err(err) = config.params.validate() {
            return (
                Outcome::Invalid {
                    detail: err.to_string(),
                },
                History::new(),
                0,
            );
        }
        let params = config.params;
        let seed = config.seed;

        // 2. Build the cluster (validates again internally; surfaced as Invalid).
        let cluster = match SimulatedCluster::new(config) {
            Ok(cluster) => cluster,
            Err(err) => {
                return (
                    Outcome::Invalid {
                        detail: err.to_string(),
                    },
                    History::new(),
                    0,
                )
            }
        };

        // 3. Apply the optional VELA_DST_MAX_EVENTS event-budget override.
        let mut budget = params.budget;
        if let Ok(raw) = std::env::var("VELA_DST_MAX_EVENTS") {
            if let Ok(max_events) = raw.trim().parse::<u64>() {
                budget.max_events = max_events;
            }
        }

        // 4. Generate the workload and seed the timeline.
        let mut streams = SeedStreams::new(seed);
        let workload = workload::generate(&params, &mut streams.workload);
        let mut rt = SimRuntime::new(cluster, budget);
        rt.workload = workload;

        rt.seed_workload_timeline();
        rt.seed_fault_schedule(&params, &mut streams.faults);
        // Bootstrap: arm the metadata group's first election timers.
        rt.seed_initial_elections();

        // 5–6. Step to the budget and run the final checker passes.
        let outcome = rt.drive();
        // The number of events the run actually processed — bounded by the
        // (possibly env-overridden) event budget. Read once after the run so a
        // caller (see `run_observed`) can assert the budget was honored without
        // changing run semantics.
        let events_processed = rt.scheduler.events_processed();
        (outcome, rt.history, events_processed)
    }

    /// Drive an already-seeded runtime to completion: step to the [`Budget`],
    /// feed the checkers incrementally, then run the final structural Raft-safety,
    /// Kafka-parity, and liveness passes over the end state, returning the run's
    /// [`Outcome`].
    ///
    /// The runtime keeps ownership of its [`History`] and [`Scheduler`] after
    /// this returns, so [`run_collecting`](Self::run_collecting) can read the
    /// recorded history and the processed-event count from the still-live
    /// runtime rather than threading them through every early return.
    fn drive(&mut self) -> Outcome {
        let mut raft_safety = RaftSafetyChecker::new();
        let kafka = KafkaParityChecker::new();
        let mut liveness = LivenessChecker::new(VirtualDuration::from_secs(LIVENESS_BUDGET_SECS));

        // 5. Step to the budget, feeding the checkers incrementally.
        loop {
            let step = match self.step() {
                Ok(step) => step,
                Err(err) => {
                    return Outcome::Invalid {
                        detail: err.to_string(),
                    }
                }
            };
            let scheduled = match step {
                Step::Event(scheduled) => scheduled,
                Step::Done(_) => break,
            };
            let now = self.scheduler.now();

            // Notify the liveness checker of fault apply/heal, and re-arm the
            // first election timer of any restarted node's recovered replicas.
            match &scheduled.event {
                Event::FaultApply(Fault::Crash { nodes }) => {
                    // A crash starts a new incarnation for the crashed nodes.
                    // Commit index is volatile per-incarnation Raft state: a
                    // restarted replica re-derives it from `None`, so reset each
                    // crashed node's commit-monotonicity high-water now (the
                    // incarnation boundary). Election Safety state is left intact
                    // — term/vote are persisted, so the restarted node resumes at
                    // its persisted term and can never lead an older term. The
                    // crash reset plus the fresh-incarnation observations cover
                    // the restart, so no further commit reset is needed there.
                    for &index in nodes {
                        if let Some(node) = self.cluster.node(index) {
                            let id = node.id().clone();
                            raft_safety.forget_node_commit(&id);
                        }
                    }
                    liveness.note_fault(now);
                }
                Event::FaultApply(Fault::Restart { nodes }) => {
                    let nodes = nodes.clone();
                    self.clear_seeded_for(&nodes);
                    liveness.note_heal(now);
                }
                Event::FaultApply(_) => liveness.note_fault(now),
                Event::FaultHeal(_) => liveness.note_heal(now),
                _ => {}
            }

            // Arm the first election timer for any freshly spawned / recovered
            // partition replica (reconcile spawns replicas without one).
            self.seed_initial_elections();

            // Forget the Raft-safety state of every group whose topic was
            // deleted by a committed `DeleteTopic` applied this step (recorded in
            // `deleted_topics` by `process_output`). This is the group-incarnation
            // boundary: forgetting clears both the group's `(group, term)` leader
            // and its commit high-water, so a same-name re-creation (term 0 /
            // commit `None`) starts clean rather than colliding with the deleted
            // incarnation — a false Election Safety or commit-monotonicity report.
            //
            // Draining *after* the step but *before* `observe` is what makes it
            // correct even when the re-create commits in the same step: the fresh
            // replica is created during the step but only observed here,
            // afterward, so the stale state is already gone. A node *crash* never
            // enqueues a delete (it commits nothing while down), so Election
            // Safety is preserved across the crash. Deterministic: it only mutates
            // checker maps and schedules nothing.
            if !self.deleted_topics.is_empty() {
                let partition_count = self.cluster.topology().partition_count();
                for topic in self.deleted_topics.drain(..) {
                    for p in 0..partition_count {
                        raft_safety.forget_group(&(topic.clone(), PartitionIndex(p)));
                    }
                }
            }

            // Incremental safety observation (cheap, every event).
            if let Err(violation) = raft_safety.observe(&self.cluster, now) {
                return violation.into();
            }
            liveness.observe(&self.cluster, now);

            // Periodic structural Raft-safety pass.
            if self.scheduler.events_processed() % CHECK_LOGS_INTERVAL == 0 {
                if let Err(violation) = raft_safety.check_logs(&self.cluster, now) {
                    return violation.into();
                }
            }
        }

        // 6. Final passes over the end state.
        self.flush_pending();
        let now = self.scheduler.now();
        if let Err(violation) = raft_safety.check_logs(&self.cluster, now) {
            return violation.into();
        }
        if let Err(violation) = kafka.check(&self.cluster, &self.history, now) {
            return violation.into();
        }
        if let Err(violation) = liveness.check(now) {
            return violation.into();
        }
        Outcome::Passed
    }

    /// Place a [`ClientOp`] event for every generated workload operation onto the
    /// timeline, at a steady cadence after a short bootstrap lead (Requirement
    /// 8.7).
    fn seed_workload_timeline(&mut self) {
        for seq in 0..self.workload.len() as u64 {
            let at = VirtualInstant::from_nanos(
                CLIENT_OP_START_NANOS + seq.saturating_mul(CLIENT_OP_INTERVAL_NANOS),
            );
            self.scheduler
                .schedule(at, Event::ClientOp(ClientOp { seq }));
        }
    }

    /// Seed a deterministic, conservative [`Fault_Schedule`](crate) onto the
    /// timeline from the `faults` RNG stream (Requirement 6.5).
    ///
    /// Faults are scheduled only for the fault classes whose intensity is
    /// non-zero, so a healthy-cluster run (the default) schedules none. At most a
    /// single node is crashed (then restarted) and a single node isolated (then
    /// healed), strictly in sequence, so a majority of every group always
    /// survives — keeping acknowledged records durable and never starving a group
    /// of a reachable majority for longer than the schedule's healed gaps.
    fn seed_fault_schedule(
        &mut self,
        params: &ScenarioParameters,
        faults: &mut crate::rng::SplitMix64,
    ) {
        let node_ids: Vec<NodeId> = self.cluster.topology().nodes().to_vec();
        let node_count = node_ids.len();
        // A safe minority crash/partition needs at least three nodes; for smaller
        // clusters skip the schedule rather than risk starving a majority.
        if node_count < 3 {
            return;
        }

        if params.faults.crash_prob > 0.0 {
            let victim = faults.next_below(node_count as u64) as usize;
            self.scheduler.schedule(
                VirtualInstant::from_nanos(CRASH_AT_NANOS),
                Event::FaultApply(Fault::Crash {
                    nodes: vec![victim],
                }),
            );
            self.scheduler.schedule(
                VirtualInstant::from_nanos(RESTART_AT_NANOS),
                Event::FaultApply(Fault::Restart {
                    nodes: vec![victim],
                }),
            );
        }

        if params.faults.partition_prob > 0.0 {
            let victim = faults.next_below(node_count as u64) as usize;
            let victim_id = node_ids[victim].clone();
            let others: Vec<NodeId> = node_ids
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != victim)
                .map(|(_, id)| id.clone())
                .collect();
            let id = HealId(PARTITION_HEAL_ID);
            self.scheduler.schedule(
                VirtualInstant::from_nanos(PARTITION_AT_NANOS),
                Event::FaultApply(Fault::Partition {
                    id,
                    side_a: vec![victim_id],
                    side_b: others,
                }),
            );
            self.scheduler.schedule(
                VirtualInstant::from_nanos(PARTITION_HEAL_AT_NANOS),
                Event::FaultHeal(id),
            );
        }
    }

    /// Arm the first election timer for every running replica — the metadata
    /// group at bootstrap and any newly spawned / recovered partition replica —
    /// that has not yet been seeded.
    ///
    /// A replica created by reconcile (or recovered on restart) is a fresh
    /// `RaftNode` that has armed no timer, so without this kick a group with no
    /// leader would never start an election (there are no heartbeats to reset an
    /// election timer that was never armed). Each `(node, group)` is seeded
    /// exactly once per incarnation; once elected, the normal tick / heartbeat
    /// machinery keeps the timers going. Candidates are collected in a
    /// deterministic order (node index, then sorted group) so the scheduled
    /// tie-break order is a pure function of the seed (Requirement 1.5).
    ///
    /// Before arming, the seeded markers are pruned to the replicas a *running*
    /// node still hosts: a topic delete (then re-create), or any other path that
    /// removes and later re-spawns a replica, drops the stale `(node, group)`
    /// marker so the fresh incarnation is treated as new and re-armed. This is
    /// self-correcting for delete, crash-without-restart, and reconcile churn,
    /// and complements [`clear_seeded_for`](Self::clear_seeded_for) on restart.
    /// Crashed nodes are left untouched (their hosting is dormant, not removed),
    /// so their markers survive until restart re-seeds them.
    fn seed_initial_elections(&mut self) {
        let now = self.scheduler.now();
        let meta = metadata_group_key();

        self.prune_seeded_to_hosted();

        let mut to_seed: Vec<(NodeId, GroupKey)> = Vec::new();
        for node in self.cluster.nodes() {
            if !node.is_running() {
                continue;
            }
            let id = node.id().clone();
            if node.controller().and_then(|c| c.meta_replica()).is_some() {
                let key = (id.clone(), meta.clone());
                if !self.seeded.contains(&key) {
                    to_seed.push(key);
                }
            }
            let mut groups: Vec<GroupKey> = node.fleet_replicas().map(|(g, _)| g.clone()).collect();
            groups.sort();
            for group in groups {
                let key = (id.clone(), group);
                if !self.seeded.contains(&key) {
                    to_seed.push(key);
                }
            }
        }
        if to_seed.is_empty() {
            return;
        }

        for (node_id, group) in &to_seed {
            self.cluster.clock_mut().set_now(now);
            self.cluster
                .clock_mut()
                .set_active(node_id.clone(), group.clone());
            self.cluster
                .clock_mut()
                .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
            self.seeded.insert((node_id.clone(), group.clone()));
        }
        for armed in self.cluster.clock_mut().drain_armed() {
            self.scheduler.schedule(armed.at, armed.to_event());
        }
        self.cluster.clock_mut().clear_active();
    }

    /// Drop seeded-election markers for replicas a *running* node no longer
    /// hosts, so a later re-spawn of the same `(node, group)` is treated as a
    /// fresh incarnation and re-armed by
    /// [`seed_initial_elections`](Self::seed_initial_elections).
    ///
    /// A topic delete makes reconcile stop and remove that topic's partition
    /// replicas (and the metadata replica is removed only when its controller is
    /// gone); without pruning, the `(node, group)` pair stays in `seeded`, so a
    /// subsequent re-create of the topic re-spawns the replicas but never re-arms
    /// their first election timer — the group then has a running majority that
    /// never starts an election, which the [`LivenessChecker`] correctly flags as
    /// a stall. Pruning by current hosting fixes this for delete and any other
    /// replica-removal path in one place.
    ///
    /// Only *running* nodes are pruned: a crashed node's replicas are dormant,
    /// not removed, so their markers are preserved until
    /// [`clear_seeded_for`](Self::clear_seeded_for) clears them on restart.
    /// Pruning removes set entries only and never schedules anything, so it does
    /// not affect the deterministic order in which timers are armed.
    fn prune_seeded_to_hosted(&mut self) {
        let meta = metadata_group_key();
        let mut running_hosted: std::collections::HashSet<(NodeId, GroupKey)> =
            std::collections::HashSet::new();
        let mut running_ids: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
        for node in self.cluster.nodes() {
            if !node.is_running() {
                continue;
            }
            let id = node.id().clone();
            running_ids.insert(id.clone());
            if node.controller().and_then(|c| c.meta_replica()).is_some() {
                running_hosted.insert((id.clone(), meta.clone()));
            }
            for (group, _) in node.fleet_replicas() {
                running_hosted.insert((id.clone(), group.clone()));
            }
        }
        // Keep a marker if its node is crashed (dormant, handled on restart) or
        // the running node still hosts that group; drop it otherwise.
        self.seeded
            .retain(|key| !running_ids.contains(&key.0) || running_hosted.contains(key));
    }

    /// Forget the seeded-election markers for `nodes`, so a restart re-arms the
    /// first election timer of each recovered replica on the next
    /// [`seed_initial_elections`](Self::seed_initial_elections) pass.
    fn clear_seeded_for(&mut self, nodes: &[usize]) {
        for &index in nodes {
            if let Some(node) = self.cluster.node(index) {
                let id = node.id().clone();
                self.seeded.retain(|(nid, _)| nid != &id);
            }
        }
    }

    /// Apply one fault to the cluster (Requirement 6.1, 6.3, 5.5).
    ///
    /// Crash / restart route to [`SimulatedCluster::crash_nodes`] /
    /// [`restart_nodes`](SimulatedCluster::restart_nodes); a partition installs a
    /// directed cut on the [`SimNetwork`](crate::network::SimNetwork) under its
    /// heal id (lifted later by an [`Event::FaultHeal`]). The liveness checker is
    /// notified by the run loop, which can see the event kind.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if a restart's durable recovery fails.
    fn apply_fault(&mut self, fault: &Fault) -> Result<(), RuntimeError> {
        match fault {
            Fault::Crash { nodes } => {
                self.cluster.crash_nodes(nodes);
            }
            Fault::Restart { nodes } => {
                self.cluster.restart_nodes(nodes)?;
            }
            Fault::Partition { id, side_a, side_b } => {
                self.cluster.network().install_partition(
                    *id,
                    side_a.iter().cloned(),
                    side_b.iter().cloned(),
                );
            }
        }
        Ok(())
    }

    /// Issue one client operation (resolved from the workload by `seq`) against
    /// the cluster, following leader redirects and recording the response into
    /// the [`History`] (Requirement 8.4–8.6, 9.x).
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if routing a proposal's follow-on effects (a
    /// metadata commit's reconcile spawn) failed.
    fn dispatch_client_op(&mut self, seq: u64) -> Result<(), RuntimeError> {
        let Some(op) = self.workload.op(seq).cloned() else {
            return Ok(());
        };
        let now = self.scheduler.now();
        match op {
            ClientOperation::CreateTopic {
                name,
                partition_count,
                replication_factor,
            } => self.issue_create_topic(now, name, partition_count, replication_factor),
            ClientOperation::DeleteTopic { name } => self.issue_delete_topic(now, name),
            ClientOperation::Produce {
                topic,
                partition,
                key,
                value,
            } => self.issue_produce(now, topic, partition, key, value),
            ClientOperation::Consume {
                topic,
                partition,
                start_offset,
                max_records,
            } => {
                self.issue_consume(now, topic, partition, start_offset, max_records);
                Ok(())
            }
        }
    }

    /// Issue a produce: route to the partition leader (following redirects),
    /// propose the record, and register it pending for offset resolution on
    /// commit; an unresolved redirect / no-leader is recorded as a valid response.
    fn issue_produce(
        &mut self,
        now: VirtualInstant,
        topic: String,
        partition: PartitionIndex,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        let group: GroupKey = (topic.clone(), partition);
        let args = OpArgs::Produce {
            topic,
            partition,
            key,
            value: value.clone(),
        };
        match self.resolve_leader(&group) {
            LeaderResolution::Leader(index) => {
                let target = self.leader_next_index(index, &group);
                self.cluster.network().set_now(now);
                let payload = EntryPayload::new(PayloadKind::Record, value.clone());
                let Some(out) =
                    self.cluster
                        .step_replica(index, &group, now, RaftInput::Propose(payload))
                else {
                    self.history
                        .record_failure(args, now, now, OpResponse::NoLeader);
                    return Ok(());
                };
                self.process_output(index, &group, out)?;
                self.pending.push(Pending {
                    group,
                    target,
                    args,
                    invoked_at: now,
                    expected: value,
                    kind: PendingKind::Produce,
                });
            }
            LeaderResolution::Exhausted => {
                self.history
                    .record_failure(args, now, now, OpResponse::UnresolvedRedirection);
            }
            LeaderResolution::NoLeader => {
                self.history
                    .record_failure(args, now, now, OpResponse::NoLeader);
            }
        }
        Ok(())
    }

    /// Issue a consume: route to the partition leader (following redirects) and
    /// read its committed records synchronously, recording them in ascending
    /// offset order (Requirement 9.3).
    ///
    /// The requested `start_offset` is clamped to the leader's committed length,
    /// so the recorded consume always observes a prefix-range *within* the
    /// committed log — the form the per-partition linearizability guarantee is
    /// stated over (a read past the end is a successful empty read in production,
    /// but the harness issues only in-range reads so the History stays a clean
    /// linearizable witness).
    fn issue_consume(
        &mut self,
        now: VirtualInstant,
        topic: String,
        partition: PartitionIndex,
        start_offset: Offset,
        max_records: u32,
    ) {
        let group: GroupKey = (topic.clone(), partition);
        match self.resolve_leader(&group) {
            LeaderResolution::Leader(index) => {
                let (start, records) = self
                    .replica_at(index, &group)
                    .map(|replica| {
                        // Clamp the start to the committed length so the observed
                        // range lies within the committed log.
                        let len = replica.state_machine().len() as Offset;
                        let start = start_offset.min(len);
                        let records = replica
                            .read(start, max_records as usize)
                            .into_iter()
                            .map(|rec| Record {
                                key: None,
                                value: rec.value,
                            })
                            .collect::<Vec<Record>>();
                        (start, records)
                    })
                    .unwrap_or((start_offset, Vec::new()));
                let args = OpArgs::Consume {
                    topic,
                    partition,
                    start_offset: start,
                    max_records,
                };
                self.history.record_consume_success(args, now, now, records);
            }
            LeaderResolution::Exhausted => {
                let args = OpArgs::Consume {
                    topic,
                    partition,
                    start_offset,
                    max_records,
                };
                self.history
                    .record_failure(args, now, now, OpResponse::UnresolvedRedirection);
            }
            LeaderResolution::NoLeader => {
                let args = OpArgs::Consume {
                    topic,
                    partition,
                    start_offset,
                    max_records,
                };
                self.history
                    .record_failure(args, now, now, OpResponse::NoLeader);
            }
        }
    }

    /// Issue a topic create: propose a `CreateTopic` command (carrying the
    /// topology's fixed replica sets) to the metadata leader, registering it
    /// pending for commit.
    fn issue_create_topic(
        &mut self,
        now: VirtualInstant,
        name: String,
        partition_count: u32,
        replication_factor: usize,
    ) -> Result<(), RuntimeError> {
        let args = OpArgs::CreateTopic {
            topic: name.clone(),
            partitions: partition_count,
            replication_factor: replication_factor as u32,
        };
        let command = self.build_create_topic(&name, partition_count);
        self.issue_metadata_command(now, args, command, PendingKind::CreateTopic { name })
    }

    /// Issue a topic delete: propose a `DeleteTopic` command to the metadata
    /// leader, registering it pending for commit.
    fn issue_delete_topic(
        &mut self,
        now: VirtualInstant,
        name: String,
    ) -> Result<(), RuntimeError> {
        let args = OpArgs::DeleteTopic {
            topic: name.clone(),
        };
        let command = ClusterCommand::DeleteTopic { name: name.clone() };
        self.issue_metadata_command(now, args, command, PendingKind::DeleteTopic { name })
    }

    /// Shared topic-admin issue path: resolve the metadata leader, propose the
    /// `command`, and register it pending (or record an unresolved-redirect /
    /// no-leader response).
    fn issue_metadata_command(
        &mut self,
        now: VirtualInstant,
        args: OpArgs,
        command: ClusterCommand,
        kind: PendingKind,
    ) -> Result<(), RuntimeError> {
        let meta = metadata_group_key();
        match self.resolve_leader(&meta) {
            LeaderResolution::Leader(index) => {
                let target = self.leader_next_index(index, &meta);
                let bytes = encode_cluster_command(&command);
                self.cluster.network().set_now(now);
                let payload = EntryPayload::new(PayloadKind::Cluster, bytes.clone());
                let Some(out) =
                    self.cluster
                        .step_replica(index, &meta, now, RaftInput::Propose(payload))
                else {
                    self.history
                        .record_failure(args, now, now, OpResponse::NoLeader);
                    return Ok(());
                };
                self.process_output(index, &meta, out)?;
                self.pending.push(Pending {
                    group: meta,
                    target,
                    args,
                    invoked_at: now,
                    expected: bytes,
                    kind,
                });
            }
            LeaderResolution::Exhausted => {
                self.history
                    .record_failure(args, now, now, OpResponse::UnresolvedRedirection);
            }
            LeaderResolution::NoLeader => {
                self.history
                    .record_failure(args, now, now, OpResponse::NoLeader);
            }
        }
        Ok(())
    }

    /// Build a `CreateTopic` command for `name` whose partitions carry the
    /// topology's fixed replica sets (capped to the topology's partition count).
    fn build_create_topic(&self, name: &str, partition_count: u32) -> ClusterCommand {
        let topology = self.cluster.topology();
        let count = partition_count.min(topology.partition_count());
        let partitions = (0..count)
            .map(|p| {
                let index = PartitionIndex(p);
                Partition {
                    index,
                    replicas: topology
                        .replica_set_for(index)
                        .map(<[NodeId]>::to_vec)
                        .unwrap_or_default(),
                    leader: None,
                }
            })
            .collect();
        ClusterCommand::CreateTopic {
            name: name.to_string(),
            partitions,
            backend: LogBackend::InMemory,
        }
    }

    /// Resolve the current leader of `group` by following the cluster's leader
    /// hints toward the elected leader, up to [`MAX_REDIRECT_HOPS`] successive
    /// redirections (Requirement 8.4, 8.5, 8.6).
    ///
    /// Starts at the first running replica-set member that hosts the group (the
    /// "contacted" node), then follows each non-leader's `leader_id` hint to the
    /// node it names. A node that *is* leader resolves the request; a hint that
    /// points nowhere usable (no hint, a crashed / non-hosting node, or back to a
    /// non-leader self) is [`NoLeader`](LeaderResolution::NoLeader); exceeding the
    /// hop bound without converging is [`Exhausted`](LeaderResolution::Exhausted).
    fn resolve_leader(&self, group: &GroupKey) -> LeaderResolution {
        let meta = metadata_group_key();
        let is_meta = group == &meta;
        let set: Vec<NodeId> = if is_meta {
            self.cluster.topology().nodes().to_vec()
        } else {
            match self.cluster.topology().replica_set_for_group(group) {
                Some(set) => set.to_vec(),
                None => return LeaderResolution::NoLeader,
            }
        };

        // The contacted node: the first running replica-set member hosting the
        // group, chosen in deterministic replica-set order.
        let Some(mut current) = set.iter().find_map(|id| {
            let index = self.cluster.index_of(id)?;
            let node = self.cluster.node(index)?;
            (node.is_running() && Self::hosts_group(node, group, is_meta)).then_some(index)
        }) else {
            return LeaderResolution::NoLeader;
        };

        // Follow hints: one initial contact plus up to MAX_REDIRECT_HOPS hops.
        for _ in 0..=MAX_REDIRECT_HOPS {
            let Some(node) = self.cluster.node(current) else {
                return LeaderResolution::NoLeader;
            };
            if !node.is_running() {
                return LeaderResolution::NoLeader;
            }
            let Some(replica) = Self::replica_of(node, group, is_meta) else {
                return LeaderResolution::NoLeader;
            };
            if replica.role() == Role::Leader {
                return LeaderResolution::Leader(current);
            }
            // Follow this non-leader's hint toward the leader it last learned.
            let Some(rid) = replica.raft().leader_id() else {
                return LeaderResolution::NoLeader;
            };
            let Some(domain) = self.cluster.topology().domain_id(rid) else {
                return LeaderResolution::NoLeader;
            };
            let Some(next) = self.cluster.index_of(domain) else {
                return LeaderResolution::NoLeader;
            };
            let hosts_next = self
                .cluster
                .node(next)
                .is_some_and(|n| n.is_running() && Self::hosts_group(n, group, is_meta));
            if !hosts_next || next == current {
                return LeaderResolution::NoLeader;
            }
            current = next;
        }
        LeaderResolution::Exhausted
    }

    /// Whether `node` hosts a replica for `group`.
    fn hosts_group(node: &SimNode, group: &GroupKey, is_meta: bool) -> bool {
        Self::replica_of(node, group, is_meta).is_some()
    }

    /// The replica `node` hosts for `group`, or `None` if it hosts none.
    fn replica_of<'a>(
        node: &'a SimNode,
        group: &GroupKey,
        is_meta: bool,
    ) -> Option<&'a PartitionReplica> {
        if is_meta {
            node.controller().and_then(|c| c.meta_replica())
        } else {
            node.fleet_replicas()
                .find(|(g, _)| *g == group)
                .map(|(_, replica)| replica)
        }
    }

    /// The replica for `group` on the node at `index`, or `None`.
    fn replica_at(&self, index: usize, group: &GroupKey) -> Option<&PartitionReplica> {
        let is_meta = group == &metadata_group_key();
        let node = self.cluster.node(index)?;
        Self::replica_of(node, group, is_meta)
    }

    /// The log index a freshly proposed entry will occupy on the leader for
    /// `group` at `index` (`last_index + 1`, or `0` for an empty log) — mirroring
    /// the production produce path.
    fn leader_next_index(&self, index: usize, group: &GroupKey) -> u64 {
        self.replica_at(index, group)
            .and_then(|replica| replica.raft().log().last_index())
            .map_or(0, |last| last + 1)
    }

    /// Resolve every pending client operation whose target index has now
    /// committed, recording its response into the [`History`].
    ///
    /// A pending op resolves to success only when the committed entry at its
    /// target index is *its* proposal (same payload bytes and kind); if a
    /// different entry committed there (the proposing leader was deposed), the
    /// proposal was superseded and is recorded as an error rather than a false
    /// success. Pending ops whose target has not yet committed are retained.
    fn resolve_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let now = self.scheduler.now();
        let pending = std::mem::take(&mut self.pending);
        let mut still_pending = Vec::new();

        for op in pending {
            let Some(entry) = self.committed_entry_at(&op.group, op.target) else {
                // Not yet committed (or the group is momentarily unobservable);
                // keep waiting.
                still_pending.push(op);
                continue;
            };
            let committed_as_proposed =
                entry.payload.kind == op.kind.payload_kind() && entry.payload.bytes == op.expected;
            if !committed_as_proposed {
                self.history.record_failure(
                    op.args,
                    op.invoked_at,
                    now,
                    OpResponse::Error {
                        message: "proposal superseded before commit".to_string(),
                    },
                );
                continue;
            }
            match op.kind {
                PendingKind::Produce => {
                    let offset = self.offset_at(&op.group, op.target);
                    self.history
                        .record_produce_success(op.args, op.invoked_at, now, offset);
                }
                PendingKind::CreateTopic { name } => {
                    self.history.record(RecordedOp::new(
                        op.args,
                        op.invoked_at,
                        now,
                        OpResponse::CreateTopicOk { topic: name },
                    ));
                }
                PendingKind::DeleteTopic { name } => {
                    self.history.record(RecordedOp::new(
                        op.args,
                        op.invoked_at,
                        now,
                        OpResponse::DeleteTopicOk { topic: name },
                    ));
                }
            }
        }
        self.pending = still_pending;
    }

    /// The committed log entry at index `target` in `group`, observed from the
    /// first running replica (in node order) whose commit index has reached
    /// `target`, or `None` if no running replica has committed through `target`.
    ///
    /// Committed entries agree across replicas (Raft State Machine Safety), so
    /// the first such replica is authoritative.
    fn committed_entry_at(&self, group: &GroupKey, target: u64) -> Option<LogEntry> {
        let is_meta = group == &metadata_group_key();
        for node in self.cluster.nodes() {
            if !node.is_running() {
                continue;
            }
            let Some(replica) = Self::replica_of(node, group, is_meta) else {
                continue;
            };
            if replica.raft().commit_index().is_some_and(|c| c >= target) {
                return replica
                    .raft()
                    .log()
                    .read(target, target)
                    .into_iter()
                    .find(|entry| entry.index == target);
            }
        }
        None
    }

    /// The record offset assigned to the committed entry at index `target` in
    /// `group`: its position among record-kind entries, 0-based — the production
    /// `offset_at` rule. Computed on the first running replica committed through
    /// `target` (committed prefixes agree, so any such replica yields the same
    /// offset).
    fn offset_at(&self, group: &GroupKey, target: u64) -> Offset {
        let is_meta = group == &metadata_group_key();
        for node in self.cluster.nodes() {
            if !node.is_running() {
                continue;
            }
            let Some(replica) = Self::replica_of(node, group, is_meta) else {
                continue;
            };
            if replica.raft().commit_index().is_some_and(|c| c >= target) {
                let records = replica
                    .raft()
                    .log()
                    .read(0, target)
                    .iter()
                    .filter(|entry| entry.payload.kind == PayloadKind::Record)
                    .count() as u64;
                return records.saturating_sub(1);
            }
        }
        0
    }

    /// Record every still-pending client operation as an unresolved (commit-
    /// timeout) error, so the [`History`] holds a response for every issued
    /// operation (Requirement 9.4). Called once after the run ends.
    fn flush_pending(&mut self) {
        let now = self.scheduler.now();
        for op in std::mem::take(&mut self.pending) {
            self.history.record_failure(
                op.args,
                op.invoked_at,
                now,
                OpResponse::Error {
                    message: "operation did not commit within the run budget".to_string(),
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{RunConfig, ScenarioParameters};
    use crate::scheduler::{EndReason, VirtualInstant};
    use vela_core::metadata_group_key;
    use vela_raft::{Role, TimerKind};

    /// Parameters for a small all-voters metadata cluster; defaults elsewhere.
    fn params(node_count: usize) -> ScenarioParameters {
        ScenarioParameters {
            node_count,
            replication_factor: node_count,
            partition_count: 1,
            ..ScenarioParameters::default()
        }
    }

    /// A runtime over a freshly assembled cluster of `node_count` nodes from
    /// `seed`, bounded by `max_events`.
    fn runtime(node_count: usize, seed: u64, max_events: u64) -> SimRuntime {
        let cluster = SimulatedCluster::new(RunConfig {
            seed,
            params: params(node_count),
        })
        .expect("valid scenario assembles");
        let budget = Budget {
            max_events,
            max_virtual_nanos: u64::MAX,
        };
        SimRuntime::new(cluster, budget)
    }

    /// Seed an initial election timer for every node's `__meta/0` replica at the
    /// origin, so the metadata group can start an election.
    ///
    /// Mirrors what the run orchestration will do at bootstrap: arm each
    /// replica's first election timer through the clock and schedule the
    /// resulting `TimerFire`s.
    fn seed_meta_elections(rt: &mut SimRuntime) {
        let meta = metadata_group_key();
        let now = VirtualInstant::ORIGIN;
        let node_ids: Vec<_> = rt
            .cluster()
            .nodes()
            .iter()
            .map(|n| n.id().clone())
            .collect();
        for node in node_ids {
            rt.cluster_mut().clock_mut().set_now(now);
            rt.cluster_mut().clock_mut().set_active(node, meta.clone());
            // Arm via the Clock seam so the generation matches `is_current`.
            use vela_raft::{Clock, ELECTION_TIMEOUT_BASE};
            rt.cluster_mut()
                .clock_mut()
                .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
        }
        for armed in rt.cluster_mut().clock_mut().drain_armed() {
            rt.scheduler_mut().schedule(armed.at, armed.to_event());
        }
        rt.cluster_mut().clock_mut().clear_active();
    }

    /// The number of nodes whose `__meta/0` replica currently believes it is
    /// leader.
    fn meta_leaders(rt: &SimRuntime) -> usize {
        rt.cluster()
            .nodes()
            .iter()
            .filter(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
            .count()
    }

    /// The index of the node currently leading `__meta/0`, if any.
    fn meta_leader_index(rt: &SimRuntime) -> Option<usize> {
        rt.cluster()
            .nodes()
            .iter()
            .position(|n| n.controller().and_then(|c| c.role()) == Some(Role::Leader))
    }

    /// Driving the seeded metadata election to a leader exercises the whole
    /// dispatch path: a `TimerFire` steps a replica, its `out.sends` are routed
    /// through the network and re-scheduled as `MessageDeliver`s, and replies
    /// flow back — ending in exactly one metadata leader. This confirms timers,
    /// sends, and deliveries all flow back through the one scheduler queue.
    #[test]
    fn metadata_group_elects_a_single_leader_through_dispatch() {
        let mut rt = runtime(3, 0xD57_u64, 50_000);
        seed_meta_elections(&mut rt);

        // No leader before any event is dispatched.
        assert_eq!(meta_leaders(&rt), 0);

        let mut elected = false;
        while let Step::Event(_) = rt.step().expect("dispatch never fails in a healthy run") {
            if meta_leaders(&rt) == 1 {
                elected = true;
                break;
            }
        }
        assert!(elected, "the metadata group should elect a leader");
        assert_eq!(meta_leaders(&rt), 1, "Election Safety: at most one leader");
        // The election only completes if RequestVote / reply messages actually
        // flowed through the network and back, proving sends were dispatched and
        // re-scheduled as deliveries.
        assert!(
            rt.cluster().network().delivered() > 0,
            "votes must have been routed through the Sim_Network"
        );
    }

    /// A committed metadata command flows through the dispatch path's follow-on
    /// effects: once a `CreateTopic` proposed to the metadata leader commits, the
    /// runtime folds it into every node's served catalogue and reconciles, so the
    /// assigned partition replicas are spawned (Requirement 3.2, 3.4).
    #[test]
    fn committed_metadata_is_applied_and_reconciled() {
        use crate::codec::encode_cluster_command;
        use vela_core::{ClusterCommand, LogBackend, Partition, PartitionIndex};
        use vela_log::EntryPayload;

        let mut rt = runtime(3, 0xD57_u64, 200_000);
        seed_meta_elections(&mut rt);

        // Drive to a metadata leader.
        let leader = loop {
            match rt.step().expect("dispatch succeeds") {
                Step::Event(_) => {
                    if let Some(idx) = meta_leader_index(&rt) {
                        break idx;
                    }
                }
                Step::Done(_) => panic!("no metadata leader was elected"),
            }
        };

        // Propose a CreateTopic to the leader, whose single partition is
        // replicated across the whole (rf == node_count) cluster.
        let meta = metadata_group_key();
        let replicas = rt
            .cluster()
            .topology()
            .replica_set_for(PartitionIndex(0))
            .expect("partition 0 has a replica set")
            .to_vec();
        let create = ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas,
                leader: None,
            }],
            backend: LogBackend::InMemory,
        };
        let now = rt.scheduler().now();
        rt.cluster_mut().network().set_now(now);
        let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&create));
        let out = rt
            .cluster_mut()
            .step_replica(leader, &meta, now, RaftInput::Propose(payload))
            .expect("leader accepts the proposal");
        rt.process_output(leader, &meta, out)
            .expect("processing the propose output succeeds");

        // Drive until the committed CreateTopic has been applied to a served
        // catalogue (the follow-on apply + reconcile).
        let mut applied = false;
        loop {
            if rt
                .cluster()
                .nodes()
                .iter()
                .any(|n| n.served().topics.contains_key("orders"))
            {
                applied = true;
                break;
            }
            match rt.step().expect("dispatch succeeds") {
                Step::Event(_) => {}
                Step::Done(_) => break,
            }
        }
        assert!(
            applied,
            "the committed CreateTopic must reach a served catalogue"
        );

        // Reconcile spawned the partition replica on every assigned node (the
        // whole cluster here), and the metadata group is never in the fleet.
        for node in rt.cluster().nodes() {
            assert!(
                node.served().topics.contains_key("orders"),
                "every running node converges on the created topic"
            );
            assert_eq!(
                node.fleet_len(),
                1,
                "reconcile spawns the assigned partition replica"
            );
        }
    }

    /// A `TimerFire` whose generation was superseded by a later re-arm is
    /// dropped without stepping the replica (mirrors `TimerClock::is_current`).
    #[test]
    fn stale_timer_fire_is_dropped() {
        let mut rt = runtime(3, 7, 50_000);
        let meta = metadata_group_key();
        let node = rt.cluster().nodes()[0].id().clone();

        use vela_raft::{Clock, ELECTION_TIMEOUT_BASE};
        rt.cluster_mut().clock_mut().set_now(VirtualInstant::ORIGIN);
        rt.cluster_mut()
            .clock_mut()
            .set_active(node.clone(), meta.clone());

        // Arm generation 1 and schedule it onto the timeline.
        rt.cluster_mut()
            .clock_mut()
            .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
        for armed in rt.cluster_mut().clock_mut().drain_armed() {
            assert_eq!(armed.generation, 1);
            rt.scheduler_mut().schedule(armed.at, armed.to_event());
        }

        // Re-arm (generation 2) and discard it: this only bumps the current
        // generation, so the already-scheduled generation-1 fire is now stale.
        rt.cluster_mut()
            .clock_mut()
            .arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let _ = rt.cluster_mut().clock_mut().drain_armed();
        rt.cluster_mut().clock_mut().clear_active();

        assert_eq!(rt.scheduler().pending_events(), 1);

        // Stepping pops the stale generation-1 fire; dispatch drops it, so the
        // replica neither steps nor schedules any follow-on event.
        let step = rt.step().expect("dispatch succeeds");
        assert!(matches!(step, Step::Event(_)));
        assert_eq!(meta_leaders(&rt), 0, "a stale tick must not start anything");
        assert_eq!(
            rt.scheduler().pending_events(),
            0,
            "a dropped stale tick schedules no follow-on events"
        );
    }

    /// An empty timeline ends immediately as quiescent, dispatching nothing.
    #[test]
    fn empty_timeline_is_quiescent() {
        let mut rt = runtime(3, 1, 50_000);
        assert!(matches!(
            rt.step().expect("no dispatch"),
            Step::Done(EndReason::Quiescent)
        ));
    }

    /// Whether any running node currently serves a topic named `topic`.
    fn any_node_serves(rt: &SimRuntime, topic: &str) -> bool {
        rt.cluster()
            .nodes()
            .iter()
            .any(|n| n.served().topics.contains_key(topic))
    }

    /// Requirement 4.4 (atomic single-event processing): one `step()` processes
    /// exactly one event — it never spans more than one — and the `out.sends`
    /// that event produced are enqueued *within that same step*, before the next
    /// event is selected.
    ///
    /// The first dispatched event is the earliest seeded election `TimerFire`:
    /// the replica it ticks becomes a candidate and broadcasts `RequestVote` to
    /// its peers. So after that single step we must observe, before any further
    /// step: the scheduler's `events_processed()` advanced by exactly 1, the
    /// network has buffered the broadcast sends (`delivered()` increased), and
    /// those follow-on events are now pending. Driving on to a leader, every
    /// `step()` advances `events_processed()` by exactly 1, proving no step ever
    /// spans more than one event regardless of how many follow-on events it
    /// enqueues.
    #[test]
    fn step_processes_exactly_one_event_and_enqueues_sends_within_the_step() {
        let mut rt = runtime(3, 0xD57_u64, 50_000);
        seed_meta_elections(&mut rt);

        // Nothing dispatched yet: no events processed, no sends buffered.
        assert_eq!(rt.scheduler().events_processed(), 0);
        assert_eq!(rt.cluster().network().delivered(), 0);

        // The first step dispatches exactly one event (the earliest election
        // timer) and, within that one step, enqueues the candidate's broadcast.
        let pending_before = rt.scheduler().pending_events();
        let step = rt.step().expect("dispatch never fails in a healthy run");
        assert!(matches!(step, Step::Event(_)), "an event was dispatched");

        assert_eq!(
            rt.scheduler().events_processed(),
            1,
            "a single step processes exactly one event, never more"
        );
        assert!(
            rt.cluster().network().delivered() > 0,
            "out.sends are enqueued within the step that produced them, \
             before the next event is selected"
        );
        assert!(
            rt.scheduler().pending_events() > pending_before,
            "the broadcast's deliveries (and re-armed timers) are scheduled \
             within the same step"
        );

        // Across the rest of the run to a leader, each step advances the
        // processed-event count by exactly one: a step never spans more than one
        // event, however many follow-on events it enqueues.
        let mut prev_processed = rt.scheduler().events_processed();
        let mut elected = false;
        while let Step::Event(_) = rt.step().expect("dispatch never fails in a healthy run") {
            let processed = rt.scheduler().events_processed();
            assert_eq!(
                processed,
                prev_processed + 1,
                "each step processes exactly one event"
            );
            prev_processed = processed;
            if meta_leaders(&rt) == 1 {
                elected = true;
                break;
            }
        }
        assert!(elected, "the metadata group should elect a leader");
    }

    /// Requirement 4.4 (atomic single-event processing): an event's
    /// `out.committed` effects are applied *within the single step* that
    /// processes the committing event — they are observable as soon as that
    /// `step()` returns, before the next event is selected.
    ///
    /// After a `CreateTopic` is proposed to the metadata leader, exactly one
    /// later `step()` processes the event that commits it; that same step folds
    /// the committed command into the served catalogues. We drive one event at a
    /// time and pinpoint the step in which the topic first becomes served: it
    /// was absent before the step and present the instant the step returned, and
    /// that step advanced the processed-event count by exactly one.
    #[test]
    fn committed_effects_apply_within_the_single_step_before_the_next() {
        use crate::codec::encode_cluster_command;
        use vela_core::{ClusterCommand, LogBackend, Partition, PartitionIndex};
        use vela_log::EntryPayload;

        let mut rt = runtime(3, 0xD57_u64, 200_000);
        seed_meta_elections(&mut rt);

        // Drive to a metadata leader.
        let leader = loop {
            match rt.step().expect("dispatch succeeds") {
                Step::Event(_) => {
                    if let Some(idx) = meta_leader_index(&rt) {
                        break idx;
                    }
                }
                Step::Done(_) => panic!("no metadata leader was elected"),
            }
        };

        // Propose a CreateTopic to the leader (the client-op wiring is a later
        // task, so inject the proposal directly through the replica step).
        let meta = metadata_group_key();
        let replicas = rt
            .cluster()
            .topology()
            .replica_set_for(PartitionIndex(0))
            .expect("partition 0 has a replica set")
            .to_vec();
        let create = ClusterCommand::CreateTopic {
            name: "orders".to_string(),
            partitions: vec![Partition {
                index: PartitionIndex(0),
                replicas,
                leader: None,
            }],
            backend: LogBackend::InMemory,
        };
        let now = rt.scheduler().now();
        rt.cluster_mut().network().set_now(now);
        let payload = EntryPayload::new(PayloadKind::Cluster, encode_cluster_command(&create));
        let out = rt
            .cluster_mut()
            .step_replica(leader, &meta, now, RaftInput::Propose(payload))
            .expect("leader accepts the proposal");
        rt.process_output(leader, &meta, out)
            .expect("processing the propose output succeeds");

        // The proposal is not yet committed, so no node serves the topic.
        assert!(
            !any_node_serves(&rt, "orders"),
            "the topic is not served before its CreateTopic commits"
        );

        // Drive one event at a time and find the step that applies the commit.
        // The transition from unserved to served must happen entirely within a
        // single step: absent before the step, present when it returns.
        let mut applied_in_one_step = false;
        loop {
            let served_before = any_node_serves(&rt, "orders");
            let processed_before = rt.scheduler().events_processed();
            match rt.step().expect("dispatch succeeds") {
                Step::Event(_) => {
                    let served_after = any_node_serves(&rt, "orders");
                    if !served_before && served_after {
                        assert_eq!(
                            rt.scheduler().events_processed(),
                            processed_before + 1,
                            "the commit is applied by exactly one event's processing"
                        );
                        applied_in_one_step = true;
                        break;
                    }
                }
                Step::Done(_) => break,
            }
        }
        assert!(
            applied_in_one_step,
            "the committed CreateTopic's catalogue effect is applied within the \
             single step that processes the committing event, before the next"
        );
    }

    // ----- Run orchestration (task 20.1) ----------------------------------

    /// A small but complete run config: a 3-node cluster, a short workload, and
    /// a bounded event budget so the run finishes quickly (heartbeats keep the
    /// timeline live, so the run ends on the event budget rather than quiescence).
    fn run_config(seed: u64, workload_size: usize, max_events: u64) -> RunConfig {
        RunConfig {
            seed,
            params: ScenarioParameters {
                node_count: 3,
                replication_factor: 3,
                partition_count: 2,
                workload_size,
                budget: Budget {
                    max_events,
                    max_virtual_nanos: u64::MAX,
                },
                ..ScenarioParameters::default()
            },
        }
    }

    /// Requirement 1.3 / Property 1: a run is a pure function of `(seed, params)`
    /// — two calls with an identical config yield the identical `Outcome`.
    #[test]
    fn run_is_deterministic_for_a_config() {
        let config = run_config(0xD57_5EED, 20, 20_000);
        let first = SimRuntime::run(config);
        let second = SimRuntime::run(config);
        assert_eq!(first, second, "a run must be reproducible from its config");
    }

    /// A healthy small run (no injected faults) completes without violating any
    /// Safety_, Kafka-parity, or Liveness_Property.
    #[test]
    fn healthy_run_passes() {
        let outcome = SimRuntime::run(run_config(0xC0FFEE, 20, 30_000));
        assert_eq!(
            outcome,
            Outcome::Passed,
            "a healthy run must pass every checked property"
        );
    }

    /// A run under injected crash/restart and partition/heal faults stays
    /// deterministic and — because the schedule only ever takes a strict minority
    /// — still upholds every property (a majority always survives).
    #[test]
    fn faulty_run_is_deterministic_and_passes() {
        let config = RunConfig {
            seed: 0xFA17_7EED,
            params: ScenarioParameters {
                node_count: 3,
                replication_factor: 3,
                partition_count: 1,
                workload_size: 20,
                faults: crate::scenario::FaultIntensities {
                    crash_prob: 0.5,
                    partition_prob: 0.5,
                    ..crate::scenario::FaultIntensities::default()
                },
                budget: Budget {
                    max_events: 60_000,
                    max_virtual_nanos: u64::MAX,
                },
            },
        };
        let first = SimRuntime::run(config);
        let second = SimRuntime::run(config);
        assert_eq!(first, second, "a faulty run must still be reproducible");
        assert_eq!(
            first,
            Outcome::Passed,
            "a strict-minority crash/restart + partition/heal schedule preserves \
             every property"
        );
    }

    /// An internally inconsistent parameter set is reported as
    /// [`Outcome::Invalid`] before any run, never a panic (Requirement 15.5).
    #[test]
    fn invalid_params_return_invalid_without_panicking() {
        let config = RunConfig {
            seed: 1,
            params: ScenarioParameters {
                node_count: 3,
                replication_factor: 4, // > node_count: rejected
                ..ScenarioParameters::default()
            },
        };
        assert!(matches!(SimRuntime::run(config), Outcome::Invalid { .. }));
    }

    /// Regression for the topic delete-then-recreate Raft-safety false positives
    /// (run-level fuzz, after the election-arming fix). When a topic is deleted,
    /// reconcile removes its partition Raft groups; a same-name re-creation spawns
    /// brand-new replicas that legitimately restart at term 0 / commit `None`.
    ///
    /// Before the `forget_group` fix the [`RaftSafetyChecker`] kept the old
    /// incarnation's per-`(group, term)` leader and `Some(_)` high-water commit
    /// keyed under the same `GroupKey`, so the new incarnation's different term-1
    /// leader tripped Election Safety ("two leaders in term 1") and its `None`
    /// commit tripped commit monotonicity ("regressed from Some(0) to None") —
    /// both spurious, since the incarnations are never concurrent. The drive loop
    /// now forgets a group's accumulated state when its topic disappears from the
    /// served catalogue (a committed delete), so the re-created group is treated
    /// as a fresh incarnation and the run no longer fails.
    ///
    /// This is the exact `(seed, params)` the fuzz minimized to for the
    /// Election Safety false positive.
    #[test]
    fn recreated_topic_election_safety_false_positive_regression() {
        let config = RunConfig {
            seed: 9_395_142_653_185_630_679,
            params: ScenarioParameters {
                node_count: 3,
                replication_factor: 2,
                partition_count: 2,
                workload_size: 22,
                faults: crate::scenario::FaultIntensities::default(),
                budget: Budget {
                    max_events: 4_000,
                    max_virtual_nanos: 6_000_000_000,
                },
            },
        };
        let outcome = SimRuntime::run(config);
        assert!(
            !matches!(outcome, Outcome::Failed { .. }),
            "a delete -> recreate of a topic restarts its partition groups as a \
             fresh Raft incarnation (term 0 / commit None); the checker must not \
             report a false Election Safety / commit-monotonicity breach against \
             the removed incarnation's state; got {outcome:?}"
        );
    }

    /// Regression for the commit-monotonicity false positive on the same
    /// delete-then-recreate path: the removed incarnation's `Some(0)` high-water
    /// commit followed by the fresh replica's `None` looked like a regression.
    /// Forgetting the group on delete clears the high-water mark so the new
    /// incarnation's `None` is a legitimate fresh start. This is the exact
    /// `(seed, params)` the fuzz minimized to for that false positive.
    #[test]
    fn recreated_topic_commit_monotonicity_false_positive_regression() {
        let config = RunConfig {
            seed: 4_680_881_932_535_480_163,
            params: ScenarioParameters {
                node_count: 3,
                replication_factor: 2,
                partition_count: 2,
                workload_size: 16,
                faults: crate::scenario::FaultIntensities::default(),
                budget: Budget {
                    max_events: 4_000,
                    max_virtual_nanos: 6_000_000_000,
                },
            },
        };
        let outcome = SimRuntime::run(config);
        assert!(
            !matches!(outcome, Outcome::Failed { .. }),
            "a re-created topic's partition replica legitimately restarts at \
             commit None; the checker must not report a false commit-monotonicity \
             regression from the removed incarnation's Some(_); got {outcome:?}"
        );
    }
}
