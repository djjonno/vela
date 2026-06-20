//! `vela-raft` — in-house Raft consensus for a single partition replica.
//!
//! Designed to be instantiated once per partition and driven step-by-step. Its
//! three external boundaries — log storage, transport, and clock/timer — are
//! expressed as traits so the consensus core stays unit-testable and
//! deterministically simulatable (Requirement 1.4). Depends inward on
//! [`vela_log`] only.
//!
//! This module defines the trait seams, the wire/message types exchanged
//! between replicas, and the [`RaftNode`] state machine skeleton. The actual
//! election and replication behaviour is implemented in later tasks; here
//! [`RaftNode::step`] is a no-op that returns an empty [`RaftOutput`].

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub mod sim;

// Boundary 1: the replicated log. Re-exported from `vela-log` so downstream
// crates can name the storage seam and the entry/payload types through
// `vela-raft` without taking a second dependency edge (Requirement 1.4).
pub use vela_log::{CommitIndex, EntryPayload, HardState, LogEntry, LogStorage, PayloadKind};

/// Base follower/candidate election timeout (Requirement 7.2).
///
/// A node arms its [`TimerKind::Election`] timer with this base duration; the
/// randomization that spreads the actual firing across the prescribed
/// 150–300 ms window is applied behind the [`Clock`] seam (the simulation clock
/// adds `[base, 2*base)` jitter; a real clock randomizes within 150–300 ms).
/// This keeps the consensus core deterministic and reproducible.
pub const ELECTION_TIMEOUT_BASE: Duration = Duration::from_millis(150);

/// Fixed leader heartbeat interval (Requirement 7.6).
///
/// Strictly shorter than the minimum election timeout (150 ms) so a healthy
/// leader keeps followers from timing out.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

/// Maximum number of log entries a leader places in a single `AppendEntries`
/// RPC (Requirement 8.1). Larger backlogs are drained over successive RPCs.
pub const MAX_ENTRIES_PER_APPEND: u64 = 256;

/// Base interval a leader waits before retransmitting an unacknowledged
/// `AppendEntries` to a follower (Requirement 8.4). The AppendEntries timeout
/// itself is 1 s; this is the starting point for the capped exponential
/// backoff applied to repeated, still-unacknowledged retries.
pub const REPLICATION_BACKOFF_BASE: Duration = Duration::from_millis(1000);

/// Upper bound on the replication retry backoff (Requirement 8.4). Successive
/// unacknowledged retries double the backoff from [`REPLICATION_BACKOFF_BASE`]
/// but never exceed this value.
pub const REPLICATION_BACKOFF_MAX: Duration = Duration::from_millis(5000);

/// Identity of a single node (partition replica) within a Raft group.
///
/// A small `Copy` newtype over a numeric id; hashable so it can key the
/// per-peer leader state maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// The kind of timer a [`Clock`] is asked to arm, delivered back as a
/// [`RaftInput::Tick`] when it elapses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimerKind {
    /// Randomized 150–300 ms follower/candidate election timeout
    /// (Requirement 7.2).
    Election,
    /// Fixed 50 ms leader heartbeat interval (Requirement 7.6).
    Heartbeat,
}

/// Boundary 2: how a node sends messages to its peers.
///
/// The transport knows nothing about gRPC or any concrete wire protocol; the
/// server crate adapts this onto real channels (Requirement 1.4).
pub trait Transport {
    /// Send `msg` to the peer identified by `to`. Delivery is best-effort; the
    /// Raft logic tolerates loss, reorder, and duplication.
    fn send(&self, to: NodeId, msg: RaftMessage);
}

/// Boundary 3: the source of time and timers.
///
/// Injecting time lets tests drive elections deterministically with a manual
/// clock and a seeded RNG for election randomness (Requirement 1.4, 7.2).
pub trait Clock {
    /// The current instant.
    fn now(&self) -> Instant;

    /// Schedule a timer of the given `kind` to fire after `dur`; it is
    /// delivered to the driver as a [`RaftInput::Tick`].
    fn arm(&mut self, kind: TimerKind, dur: Duration);
}

/// The role a replica currently holds within its Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Replicates from a leader and responds to its RPCs.
    Follower,
    /// Has started an election and is soliciting votes.
    Candidate,
    /// Accepts proposals and replicates entries to followers.
    Leader,
}

/// A `RequestVote` RPC: a candidate solicits a vote for `term`
/// (Requirements 7.3, 7.7, 7.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    /// Candidate's current term.
    pub term: u64,
    /// Candidate requesting the vote.
    pub candidate_id: NodeId,
    /// Index of the candidate's last log entry (`None` if its log is empty).
    pub last_log_index: Option<u64>,
    /// Term of the candidate's last log entry (`None` if its log is empty).
    pub last_log_term: Option<u64>,
}

/// Reply to a [`RequestVote`] RPC (Requirements 7.7, 7.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    /// Responder's current term, for the candidate to update itself.
    pub term: u64,
    /// Whether the vote was granted in `term`.
    pub vote_granted: bool,
    /// The identity of the responding voter. The candidate tallies votes by
    /// *distinct* voter so a duplicated reply (a legitimate network fault) is
    /// counted at most once, preserving "at most one leader per term"
    /// (Raft §5.2).
    pub voter: NodeId,
}

/// An `AppendEntries` RPC: a leader replicates entries (or, when `entries` is
/// empty, sends a heartbeat) (Requirements 8.1–8.3, 7.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    /// Leader's current term.
    pub term: u64,
    /// Leader's identity, so followers can redirect clients.
    pub leader_id: NodeId,
    /// Index of the entry immediately preceding `entries` (`None` when the
    /// batch begins at index 0).
    pub prev_log_index: Option<u64>,
    /// Term of the entry at `prev_log_index` (`None` when there is none).
    pub prev_log_term: Option<u64>,
    /// Entries to store; empty for a heartbeat. At most 256 per RPC
    /// (Requirement 8.1).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index, so the follower can advance its own.
    pub leader_commit: CommitIndex,
}

/// Reply to an [`AppendEntries`] RPC (Requirements 8.2–8.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    /// The follower that produced this reply, so the leader can update the
    /// right peer's replication cursors (the [`RaftInput::Message`] envelope
    /// carries no sender, mirroring how requests embed `leader_id` /
    /// `candidate_id`).
    pub from: NodeId,
    /// Responder's current term, for the leader to update itself.
    pub term: u64,
    /// Whether the preceding entry matched and the entries were appended.
    pub success: bool,
    /// On rejection, a hint at the index the leader should back up to so it can
    /// retry with earlier entries (Requirement 8.3, 8.4). `None` on success.
    pub conflict_index: Option<u64>,
    /// On success, the highest log index now known to agree with the leader on
    /// the follower (`None` when nothing yet agrees, e.g. an empty-batch ack
    /// against an empty log).
    ///
    /// Carrying the matched index in the reply lets the leader update
    /// `match_index`/`next_index` correctly even when replies are delayed,
    /// reordered, or duplicated by the transport — the leader only ever takes
    /// the maximum, so a stale ack can never regress replication progress
    /// (Requirements 8.5, 8.9). `None` on rejection.
    pub match_index: Option<u64>,
}

/// Every message exchanged between replicas of a Raft group.
///
/// Replies are modelled as messages too, so the [`Transport`] carries a single
/// type and the [`RaftNode::step`] function handles both directions uniformly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftMessage {
    /// A candidate's vote request.
    RequestVote(RequestVote),
    /// A reply to a vote request.
    RequestVoteReply(RequestVoteReply),
    /// A leader's replication / heartbeat RPC.
    AppendEntries(AppendEntries),
    /// A reply to a replication / heartbeat RPC.
    AppendEntriesReply(AppendEntriesReply),
}

/// An input that drives the [`RaftNode`] state machine one step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftInput {
    /// A previously armed timer of the given kind elapsed (election or
    /// heartbeat).
    Tick(TimerKind),
    /// An RPC or RPC reply arrived from a peer.
    Message(RaftMessage),
    /// A leader-side client proposal to append a payload to the log.
    Propose(EntryPayload),
}

/// The operation whose hard-state persistence failed during a [`RaftNode::step`]
/// (Requirement 9.4).
///
/// Raft persists its hard state (`current_term`, `voted_for`) through the
/// durable log seam *before* emitting any message that depends on the new
/// term or vote. When that persist fails, the step makes no externally visible
/// term/vote transition and emits no dependent message; the failing operation
/// is reported here so the driver can log it and the triggering peer can retry.
/// `op` is one of `"grant_vote"`, `"adopt_term"`, or `"start_election"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistError {
    /// The mutation point whose persist failed: `"grant_vote"`, `"adopt_term"`,
    /// or `"start_election"`.
    pub op: &'static str,
}

/// The effects produced by one call to [`RaftNode::step`].
///
/// `step` performs no I/O: it returns the messages to dispatch, the entries
/// that became committed, and any role change, leaving the driver to act on
/// them via the [`Transport`] and the state machine.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RaftOutput {
    /// RPCs (and replies) to dispatch through the [`Transport`].
    pub sends: Vec<(NodeId, RaftMessage)>,
    /// Newly committed entries to apply to the state machine, in ascending
    /// index order, exactly once (Requirement 8.8).
    pub committed: Vec<LogEntry>,
    /// The new role if this step changed it, for structured logging
    /// (Requirement 15.4).
    pub role_change: Option<Role>,
    /// Set when a hard-state persist failed this step. When present, the step
    /// made no externally visible term/vote transition and emitted no
    /// dependent message (Requirement 9.4). The driver logs it; the triggering
    /// peer retries. Always `None` for the volatile in-memory path, so existing
    /// output is unchanged.
    pub persist_error: Option<PersistError>,
}

/// The synchronous Raft state machine for one partition replica.
///
/// Generic over the [`LogStorage`] seam so the in-memory log can be swapped for
/// a durable one without touching consensus. Driven step-by-step via
/// [`RaftNode::step`]; all randomness (election timeouts) is injected through
/// the [`Clock`] so behaviour is reproducible in the simulation harness.
pub struct RaftNode<S: LogStorage> {
    // --- Persistent state (would survive restart in a durable build) ---
    /// Latest term this node has seen.
    current_term: u64,
    /// Candidate this node voted for in the current term, if any.
    voted_for: Option<NodeId>,
    /// The replicated log behind the storage seam.
    log: S,

    // --- Volatile state ---
    /// Current role within the group.
    role: Role,
    /// Highest log index known to be committed.
    commit_index: CommitIndex,
    /// Highest log index applied to the state machine.
    last_applied: CommitIndex,

    /// The set of distinct voters that have granted this node a vote in the
    /// current term while it is a candidate, including this node's own self-vote.
    /// Reset at the start of each election and consulted to decide when a
    /// majority is reached (Requirement 7.4). Tracking *distinct* granters (not
    /// a bare counter) makes duplicated `RequestVoteReply` messages idempotent,
    /// so a duplicated grant cannot push a candidate past `majority()` without
    /// genuine distinct-voter support (Raft §5.2).
    votes_granted: HashSet<NodeId>,
    /// The leader this replica currently believes is in charge: its own id once
    /// it wins an election, the `leader_id` it last accepted from a valid
    /// `AppendEntries`, or `None` while it is a candidate or has just stepped
    /// down and not yet heard from a leader (Raft §5.2 — followers track the
    /// current leader so clients can be redirected to it). Surfaced through
    /// [`RaftNode::leader_id`] for live-leader routing (Requirement 8.1, 8.2).
    leader_id: Option<NodeId>,

    // --- Volatile leader state (per peer) ---
    /// For each peer, the next log index to send.
    next_index: HashMap<NodeId, u64>,
    /// For each peer, the highest log index known to be replicated.
    match_index: HashMap<NodeId, u64>,
    /// For each peer, the current retransmission backoff for unacknowledged
    /// `AppendEntries` (Requirement 8.4). Starts at [`REPLICATION_BACKOFF_BASE`]
    /// and doubles on each successive unacknowledged retry up to
    /// [`REPLICATION_BACKOFF_MAX`]; reset to the base on a successful ack.
    replication_backoff: HashMap<NodeId, Duration>,

    // --- Group membership and identity ---
    /// This node's identity.
    id: NodeId,
    /// The other replicas in this group.
    peers: Vec<NodeId>,
}

impl<S: LogStorage> RaftNode<S> {
    /// Recover a replica from an injected log, restoring the persistent state a
    /// durable backend retained across a restart.
    ///
    /// Restores `current_term` and `voted_for` from the log's recovered
    /// [`HardState`] (`log.hard_state()`): a volatile or fresh log reports
    /// `None`, which yields term 0 and no vote, while a durable log that
    /// persisted a vote or term reports it so the restored replica cannot
    /// forget it (Requirements 10.1, 10.2). `commit_index` and `last_applied`
    /// are initialised from the log's recovered commit index so the caller can
    /// re-apply the committed prefix to the partition state machine exactly once
    /// (Requirement 11.1).
    ///
    /// Volatile leader state starts empty (role [`Role::Follower`], empty
    /// per-peer cursors, no gathered votes) and is populated on election to
    /// leader. For a fresh/empty log this is identical to a brand-new replica —
    /// term 0, no vote, no commit — so the fresh-create and restart cases share
    /// one path.
    pub fn recover(id: NodeId, peers: Vec<NodeId>, log: S) -> Self {
        let hard_state = log.hard_state().unwrap_or_default();
        let commit_index = log.commit_index();
        Self {
            current_term: hard_state.current_term,
            voted_for: hard_state.voted_for.map(NodeId),
            log,
            role: Role::Follower,
            commit_index,
            // Committed entries are re-applied to the state machine by the
            // caller; `last_applied` tracks the recovered commit index so the
            // replica does not re-surface entries it already applied (R11.1).
            last_applied: commit_index,
            votes_granted: HashSet::new(),
            leader_id: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            replication_backoff: HashMap::new(),
            id,
            peers,
        }
    }

    /// Create a follower for a fresh group: term 0, no vote, the given log,
    /// identity, and peer set. Volatile leader state starts empty and is
    /// populated on election to leader.
    ///
    /// A thin shim that delegates to [`RaftNode::recover`] over the given log.
    /// For the fresh/empty log every existing call site passes, recover yields
    /// identical state (term 0, no vote, no commit), so behaviour is unchanged.
    pub fn new(id: NodeId, peers: Vec<NodeId>, log: S) -> Self {
        Self::recover(id, peers, log)
    }

    /// This node's identity.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The other replicas in this group.
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    /// Latest term this node has seen.
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Candidate this node voted for in the current term, if any.
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Current role within the group.
    pub fn role(&self) -> Role {
        self.role
    }

    /// The leader this replica currently believes is in charge, or `None` when
    /// it knows of none (it is a candidate, or has stepped down and not yet
    /// heard from a leader).
    ///
    /// This is its own [`id`](RaftNode::id) once it has won an election and the
    /// `leader_id` it last accepted from a valid `AppendEntries` otherwise
    /// (Raft §5.2). The server maps this numeric id back to the domain node id
    /// to route produce/consume and answer `FindLeader` by the live
    /// Raft-elected leader (Requirement 8.1, 8.2).
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Highest log index known to be committed.
    pub fn commit_index(&self) -> CommitIndex {
        self.commit_index
    }

    /// Highest log index applied to the state machine.
    pub fn last_applied(&self) -> CommitIndex {
        self.last_applied
    }

    /// The next index to send to `peer`, if leader state has been initialised.
    pub fn next_index(&self, peer: NodeId) -> Option<u64> {
        self.next_index.get(&peer).copied()
    }

    /// The highest replicated index known for `peer`, if tracked.
    pub fn match_index(&self, peer: NodeId) -> Option<u64> {
        self.match_index.get(&peer).copied()
    }

    /// The current `AppendEntries` retransmission backoff for `peer`, if leader
    /// state has been initialised (Requirement 8.4). The driver waits this long
    /// before retransmitting an unacknowledged batch; it doubles on each
    /// successive unacknowledged retry up to [`REPLICATION_BACKOFF_MAX`] and is
    /// reset to [`REPLICATION_BACKOFF_BASE`] on a successful acknowledgment.
    pub fn replication_backoff(&self, peer: NodeId) -> Option<Duration> {
        self.replication_backoff.get(&peer).copied()
    }

    /// Shared, read-only access to the replicated log.
    pub fn log(&self) -> &S {
        &self.log
    }

    /// Advance the state machine one step: fold `input` and the current state
    /// into new state plus a set of [`RaftOutput`] effects.
    ///
    /// Implements Raft leader election (Requirements 7.2–7.10) and log
    /// replication (Requirements 8.1–8.9): election timeouts promote a
    /// follower/candidate to candidate and broadcast `RequestVote`; vote
    /// granting honours the election restriction and the at-most-one-vote-per-
    /// term rule; a higher term in any message forces a step-down; a majority of
    /// votes promotes a candidate to leader. A leader appends `Propose`d
    /// payloads at the next index and replicates them with `AppendEntries`
    /// (≤256 entries per RPC, carrying the preceding entry's coordinates);
    /// followers accept matching batches and reject conflicting ones with a
    /// back-up hint; the leader advances its commit index to the highest
    /// majority-replicated current-term entry and surfaces newly committed
    /// entries for the state machine to apply.
    ///
    /// Performs no I/O: outbound messages and role changes are returned in the
    /// [`RaftOutput`] for the driver to act on, and all timing/randomness flows
    /// through the injected [`Clock`].
    pub fn step(&mut self, input: RaftInput, clock: &mut impl Clock) -> RaftOutput {
        let entry_role = self.role;
        let mut out = RaftOutput::default();

        match input {
            RaftInput::Tick(TimerKind::Election) => {
                // A leader has no meaningful election timeout; ignore any stale
                // one. Followers and candidates (re)start an election (R7.2, R7.5).
                if self.role != Role::Leader {
                    self.start_election(clock, &mut out);
                }
            }
            RaftInput::Tick(TimerKind::Heartbeat) => {
                // Only a leader heartbeats; re-arm and broadcast (R7.6).
                if self.role == Role::Leader {
                    self.arm_heartbeat_timer(clock);
                    self.broadcast_heartbeat(&mut out);
                }
            }
            RaftInput::Message(msg) => {
                self.handle_message(msg, clock, &mut out);
            }
            // A client proposal: a leader appends the payload at the next index
            // in its current term and replicates it; non-leaders ignore it (the
            // produce path redirects clients to the leader at the core layer)
            // (Requirements 8.1, 8.5).
            RaftInput::Propose(payload) => {
                if self.role == Role::Leader {
                    self.propose(payload, &mut out);
                }
            }
        }

        out.role_change = (self.role != entry_role).then_some(self.role);
        out
    }

    // ---- election internals ------------------------------------------------

    /// Term of this node's last log entry, or `None` if its log is empty.
    fn last_log_term(&self) -> Option<u64> {
        self.log.last_index().and_then(|i| self.log.term_at(i))
    }

    /// Arm (or reset) the randomized election timeout (Requirement 7.2).
    fn arm_election_timer(&self, clock: &mut impl Clock) {
        clock.arm(TimerKind::Election, ELECTION_TIMEOUT_BASE);
    }

    /// Arm (or reset) the fixed leader heartbeat interval (Requirement 7.6).
    fn arm_heartbeat_timer(&self, clock: &mut impl Clock) {
        clock.arm(TimerKind::Heartbeat, HEARTBEAT_INTERVAL);
    }

    /// The number of votes that constitutes a strict majority of the group,
    /// counting this node (Requirement 7.4).
    fn majority(&self) -> u64 {
        let total = self.peers.len() as u64 + 1;
        total / 2 + 1
    }

    /// Begin a new election: become candidate, bump the term, vote for self,
    /// reset the election timer, and solicit votes from every peer
    /// (Requirements 7.2, 7.3, 7.5).
    fn start_election(&mut self, clock: &mut impl Clock, out: &mut RaftOutput) {
        // The incremented term and self-vote are term-dependent: the broadcast
        // `RequestVote` carries the new term. Persist them to stable storage
        // *before* any state mutation or broadcast (Requirement 9.2). On a
        // durable replica a persist failure means we stay a follower and
        // broadcast nothing; the failure is surfaced and the election timer is
        // re-armed so a later tick retries once storage recovers
        // (Requirement 9.4). The volatile in-memory path defaults to a no-op
        // `Ok`, so behaviour is unchanged (Requirement 9.3).
        let new_term = self.current_term + 1;
        if self
            .log
            .persist_hard_state(HardState {
                current_term: new_term,
                voted_for: Some(self.id.0),
            })
            .is_err()
        {
            out.persist_error = Some(PersistError {
                op: "start_election",
            });
            self.arm_election_timer(clock);
            return;
        }

        self.current_term = new_term;
        self.role = Role::Candidate;
        // A candidate knows of no leader for the new term until one emerges
        // (itself on winning, or another via `AppendEntries`) (Requirement 8.1).
        self.leader_id = None;
        self.voted_for = Some(self.id);
        self.votes_granted = HashSet::from([self.id]); // self-vote
        self.arm_election_timer(clock);

        let request = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.last_log_term(),
        };
        for &peer in &self.peers {
            out.sends
                .push((peer, RaftMessage::RequestVote(request.clone())));
        }

        // A single-node group reaches a majority with only the self-vote.
        self.maybe_become_leader(clock, out);
    }

    /// Promote this candidate to leader once it has gathered a majority of
    /// votes for the current term (Requirements 7.4, 7.10).
    fn maybe_become_leader(&mut self, clock: &mut impl Clock, out: &mut RaftOutput) {
        if self.role != Role::Candidate || (self.votes_granted.len() as u64) < self.majority() {
            return;
        }

        self.role = Role::Leader;
        // This replica now leads the current term; record itself as the known
        // leader so it can answer "who leads?" without waiting for its own
        // heartbeat to echo back (Requirement 8.1).
        self.leader_id = Some(self.id);
        // Retain the self-vote (`voted_for == Some(self.id)`, set in
        // `start_election` and persisted there) for the leader's current term.
        // Clearing it would let a sitting leader grant its vote to another
        // candidate soliciting in the *same* term — `not_yet_voted` would be
        // true — electing a second leader for that term and violating "at most
        // one leader per term" (Raft §5.2). The vote is cleared only on the
        // normal paths: stepping down to a higher term (`step_down`) or
        // adopting a higher term. Keeping it `Some(self.id)` here also matches
        // the hard state persisted at election start, so recovery is consistent.

        // Initialise per-peer replication cursors (refined as followers ack or
        // reject): next_index starts just past the leader's last entry and is
        // backed up on rejection; match_index starts empty (nothing known
        // replicated); the retry backoff starts at its base (Requirements 8.1,
        // 8.4, 8.9).
        let next = self.log.last_index().map_or(0, |i| i + 1);
        self.next_index.clear();
        self.match_index.clear();
        self.replication_backoff.clear();
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.replication_backoff
                .insert(peer, REPLICATION_BACKOFF_BASE);
        }

        // Heartbeat immediately so followers learn the leader and reset their
        // election timers, then keep heartbeating on a fixed interval (R7.6).
        self.arm_heartbeat_timer(clock);
        self.broadcast_heartbeat(out);
    }

    /// Send an `AppendEntries` to every peer based on its replication cursor
    /// (Requirements 7.6, 8.1, 8.9).
    ///
    /// On the heartbeat tick a leader contacts every peer. The RPC carries up
    /// to [`MAX_ENTRIES_PER_APPEND`] entries starting at that peer's
    /// `next_index`, so a caught-up follower receives an empty heartbeat while a
    /// lagging follower is fed the entries it is missing — driving it back into
    /// agreement (Requirement 8.9).
    fn broadcast_heartbeat(&self, out: &mut RaftOutput) {
        for &peer in &self.peers {
            self.send_append_entries(peer, out);
        }
    }

    /// The `(prev_log_index, prev_log_term)` pair preceding index `next`
    /// (Requirement 8.1). When `next` is 0 the batch begins at the head of the
    /// log and there is no preceding entry.
    fn prev_for(&self, next: u64) -> (Option<u64>, Option<u64>) {
        if next == 0 {
            (None, None)
        } else {
            let prev = next - 1;
            (Some(prev), self.log.term_at(prev))
        }
    }

    /// Build and enqueue an `AppendEntries` to `peer` from its current
    /// `next_index`, carrying the correct preceding-entry coordinates and at
    /// most [`MAX_ENTRIES_PER_APPEND`] entries (Requirements 8.1, 8.9).
    fn send_append_entries(&self, peer: NodeId, out: &mut RaftOutput) {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.log.last_index().map_or(0, |i| i + 1));
        let (prev_log_index, prev_log_term) = self.prev_for(next);

        // Up to MAX_ENTRIES_PER_APPEND entries from `next` onward; `read`
        // clamps the inclusive upper bound to the last stored index and yields
        // nothing when the follower is already caught up (Requirement 8.1).
        let entries = self
            .log
            .read(next, next.saturating_add(MAX_ENTRIES_PER_APPEND - 1));

        out.sends.push((
            peer,
            RaftMessage::AppendEntries(AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            }),
        ));
    }

    /// Append a client proposal to the log in the current term and replicate it
    /// (Requirements 8.1, 8.5). A single-node group reaches a majority with the
    /// leader alone, so the entry commits immediately.
    fn propose(&mut self, payload: EntryPayload, out: &mut RaftOutput) {
        // `append` cannot fail for the in-memory log; ignore the assigned index
        // (replication derives positions from the log itself).
        let _ = self.log.append(payload, self.current_term);
        for &peer in &self.peers {
            self.send_append_entries(peer, out);
        }
        // With no peers (or once others have already matched) the leader may be
        // able to advance the commit index right away.
        self.advance_commit(out);
    }

    /// Recompute the commit index after a change in replication progress and
    /// surface any newly committed entries (Requirements 8.5, 8.6, 8.7, 8.8).
    ///
    /// Following the extended Raft paper §5.4.2, the leader only commits an
    /// entry from its **current term** directly; committing such an entry at
    /// index `n` implicitly commits every preceding entry. The commit index
    /// advances to the highest index replicated on a majority whose entry is of
    /// the current term, and never moves backward.
    fn advance_commit(&mut self, out: &mut RaftOutput) {
        if self.role != Role::Leader {
            return;
        }
        let Some(last) = self.log.last_index() else {
            return;
        };

        let majority = self.majority();
        let start = self.commit_index.map_or(0, |c| c + 1);

        let mut new_commit = self.commit_index;
        for n in start..=last {
            // Only a current-term entry may be committed directly (§5.4.2).
            if self.log.term_at(n) != Some(self.current_term) {
                continue;
            }
            // The leader holds every entry in its own log; count it plus every
            // peer whose match_index has reached `n`.
            let replicas = 1 + self
                .peers
                .iter()
                .filter(|p| self.match_index.get(p).is_some_and(|&mi| mi >= n))
                .count() as u64;
            if replicas >= majority {
                new_commit = Some(n);
            }
        }

        self.commit_to(new_commit, out);
    }

    /// Advance the commit index to `target` (if it is greater than the current
    /// commit index), persist it to the log, and surface the newly committed
    /// entries in ascending order for the state machine to apply exactly once
    /// (Requirements 8.7, 8.8). Shared by leader commit and follower
    /// commit-index propagation.
    fn commit_to(&mut self, target: CommitIndex, out: &mut RaftOutput) {
        let Some(target) = target else {
            return;
        };
        // Monotonic: never move the commit index backward (Requirement 8.7).
        if self.commit_index.is_some_and(|c| target <= c) {
            return;
        }
        let from = self.commit_index.map_or(0, |c| c + 1);
        // The log enforces commit bounds and monotonicity; a rejection here
        // would indicate the target is out of range, so leave state untouched.
        if self.log.commit(target).is_err() {
            return;
        }
        self.commit_index = Some(target);
        out.committed.extend(self.log.read(from, target));
        self.last_applied = self.commit_index;
    }

    /// Step down to follower in `term`, clearing any vote (Requirement 7.9).
    fn step_down(&mut self, term: u64) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes_granted = HashSet::new();
        // The leader of the new, higher term is not yet known; it is relearned
        // from the first valid `AppendEntries` of that term (Requirement 8.1).
        self.leader_id = None;
    }

    /// Dispatch an incoming message, first adopting any higher term and
    /// stepping down (Requirement 7.9).
    fn handle_message(&mut self, msg: RaftMessage, clock: &mut impl Clock, out: &mut RaftOutput) {
        let msg_term = match &msg {
            RaftMessage::RequestVote(m) => m.term,
            RaftMessage::RequestVoteReply(m) => m.term,
            RaftMessage::AppendEntries(m) => m.term,
            RaftMessage::AppendEntriesReply(m) => m.term,
        };
        if msg_term > self.current_term {
            // Adopting a higher term is term-dependent: persist the new term
            // (clearing any vote) to stable storage *before* stepping down and
            // emitting any message that depends on it (Requirement 9.2). On a
            // durable replica a persist failure aborts the adoption — no term
            // transition, nothing term-dependent emitted — and is surfaced for
            // the driver to log and the peer to retry (Requirement 9.4). The
            // volatile in-memory path defaults to a no-op `Ok`, so behaviour is
            // unchanged there (Requirement 9.3).
            if self
                .log
                .persist_hard_state(HardState {
                    current_term: msg_term,
                    voted_for: None,
                })
                .is_err()
            {
                out.persist_error = Some(PersistError { op: "adopt_term" });
                return;
            }
            self.step_down(msg_term);
        }

        match msg {
            RaftMessage::RequestVote(rv) => self.handle_request_vote(rv, clock, out),
            RaftMessage::RequestVoteReply(reply) => {
                self.handle_request_vote_reply(reply, clock, out)
            }
            RaftMessage::AppendEntries(ae) => self.handle_append_entries(ae, clock, out),
            // A reply to replication: the leader updates the peer's replication
            // progress, advances the commit index, and retries on rejection
            // (Requirements 8.4, 8.5, 8.9).
            RaftMessage::AppendEntriesReply(reply) => self.handle_append_entries_reply(reply, out),
        }
    }

    /// Decide a `RequestVote`: grant only when the candidate's term is current,
    /// its log is at least as up to date, and no other vote has been cast this
    /// term (Requirements 7.7, 7.8).
    fn handle_request_vote(
        &mut self,
        rv: RequestVote,
        clock: &mut impl Clock,
        out: &mut RaftOutput,
    ) {
        let up_to_date = self.candidate_log_up_to_date(rv.last_log_term, rv.last_log_index);
        let not_yet_voted = self.voted_for.is_none() || self.voted_for == Some(rv.candidate_id);
        let grant = rv.term >= self.current_term && up_to_date && not_yet_voted;

        if grant {
            // Persist the term and vote to stable storage *before* emitting the
            // grant, so a restart can never forget this vote and grant a second
            // one to a different candidate in the same term (Requirement 9.1).
            // On a durable replica a persist failure leaves term/vote unchanged,
            // emits no grant, and is surfaced for the driver to log and the
            // candidate to retry (Requirement 9.4). The volatile in-memory path
            // defaults to a no-op `Ok`, so behaviour is unchanged (R9.3).
            if self
                .log
                .persist_hard_state(HardState {
                    current_term: rv.term,
                    voted_for: Some(rv.candidate_id.0),
                })
                .is_err()
            {
                out.persist_error = Some(PersistError { op: "grant_vote" });
                return;
            }
            self.current_term = rv.term;
            self.voted_for = Some(rv.candidate_id);
            // Granting a vote means we have heard from a viable candidate;
            // reset the election timeout so we do not immediately contend.
            self.arm_election_timer(clock);
        }

        out.sends.push((
            rv.candidate_id,
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term: self.current_term,
                vote_granted: grant,
                voter: self.id,
            }),
        ));
    }

    /// Whether a candidate's `(last_log_term, last_log_index)` is at least as up
    /// to date as this node's own last entry (Requirement 7.7).
    fn candidate_log_up_to_date(
        &self,
        cand_last_term: Option<u64>,
        cand_last_index: Option<u64>,
    ) -> bool {
        // Compare (term, index) lexicographically; an empty log is the lowest.
        let key = |term: Option<u64>, index: Option<u64>| {
            (term.unwrap_or(0), index.map_or(-1i128, |i| i as i128))
        };
        key(cand_last_term, cand_last_index) >= key(self.last_log_term(), self.log.last_index())
    }

    /// Count a granted vote and promote to leader on reaching a majority
    /// (Requirements 7.4, 7.10).
    ///
    /// Votes are tallied by *distinct* voter ([`RequestVoteReply::voter`]), so a
    /// duplicated reply — a legitimate network fault the simulator injects — is
    /// idempotent and cannot advance the tally twice. Without this, a single
    /// voter's duplicated grant could push a candidate past [`Self::majority`]
    /// without genuine majority support and elect a second leader for the term
    /// (Raft §5.2).
    fn handle_request_vote_reply(
        &mut self,
        reply: RequestVoteReply,
        clock: &mut impl Clock,
        out: &mut RaftOutput,
    ) {
        if self.role == Role::Candidate && reply.term == self.current_term && reply.vote_granted {
            self.votes_granted.insert(reply.voter);
            self.maybe_become_leader(clock, out);
        }
    }

    /// Handle an `AppendEntries` from a leader: reject a stale term, otherwise
    /// accept the leader for the term, run the log-matching check on the
    /// preceding entry, append or overwrite the conveyed entries, advance the
    /// follower's commit index, and acknowledge (Requirements 7.6, 7.9, 8.2,
    /// 8.3, 8.8, 8.10).
    fn handle_append_entries(
        &mut self,
        ae: AppendEntries,
        clock: &mut impl Clock,
        out: &mut RaftOutput,
    ) {
        if ae.term < self.current_term {
            // A leader from an older term: reject so it learns the newer term.
            out.sends.push((
                ae.leader_id,
                RaftMessage::AppendEntriesReply(AppendEntriesReply {
                    from: self.id,
                    term: self.current_term,
                    success: false,
                    conflict_index: None,
                    match_index: None,
                }),
            ));
            return;
        }

        // Valid leader for the current term: a candidate yields to it, and any
        // node resets its election timeout on contact (Requirements 7.6, 7.9).
        if self.role != Role::Follower {
            self.role = Role::Follower;
        }
        self.current_term = ae.term;
        // Record the contacting leader as this term's known leader so the node
        // can redirect clients to it (Raft §5.2; Requirement 8.1). A heartbeat
        // that later fails the log-matching check below is still from the
        // legitimate current-term leader, so learn it before that check.
        self.leader_id = Some(ae.leader_id);
        self.arm_election_timer(clock);

        // Log-matching check: the entry at prev_log_index must match in term
        // (Requirements 8.2, 8.3, 8.10). A batch beginning at index 0 (no
        // preceding entry) is always consistent.
        let matches = match ae.prev_log_index {
            None => true,
            Some(prev) => self.log.term_at(prev) == ae.prev_log_term,
        };
        if !matches {
            // Reject with a hint so the leader can back up `next_index` and
            // retry with earlier entries (Requirements 8.3, 8.4).
            let conflict_index = ae.prev_log_index.map(|prev| self.conflict_hint(prev));
            out.sends.push((
                ae.leader_id,
                RaftMessage::AppendEntriesReply(AppendEntriesReply {
                    from: self.id,
                    term: self.current_term,
                    success: false,
                    conflict_index,
                    match_index: None,
                }),
            ));
            return;
        }

        // The preceding entry matches. Append the conveyed entries, dropping any
        // conflicting uncommitted suffix first; entries already present and
        // identical are skipped so duplicate/retransmitted RPCs are idempotent
        // and committed entries are never disturbed (Requirements 8.2, 8.10).
        if !ae.entries.is_empty() {
            let split = ae
                .entries
                .iter()
                .position(|e| self.log.term_at(e.index) != Some(e.term));
            if let Some(k) = split {
                if self.log.append_entries(&ae.entries[k..]).is_err() {
                    // The only rejection here would be an attempt to overwrite a
                    // committed entry, which the matching check precludes; treat
                    // it defensively as a conflict for the leader to retry.
                    out.sends.push((
                        ae.leader_id,
                        RaftMessage::AppendEntriesReply(AppendEntriesReply {
                            from: self.id,
                            term: self.current_term,
                            success: false,
                            conflict_index: Some(self.log.last_index().map_or(0, |i| i + 1)),
                            match_index: None,
                        }),
                    ));
                    return;
                }
            }
        }

        // The highest index now known to agree with the leader: the last
        // conveyed entry, or — for an empty heartbeat — the matched preceding
        // entry.
        let match_index = match ae.entries.last() {
            Some(last) => Some(last.index),
            None => ae.prev_log_index,
        };

        // Advance the follower's commit index toward the leader's, bounded by
        // what is actually present locally, and surface newly committed entries
        // (Requirements 8.7, 8.8).
        if let (Some(leader_commit), Some(last)) = (ae.leader_commit, self.log.last_index()) {
            self.commit_to(Some(leader_commit.min(last)), out);
        }

        out.sends.push((
            ae.leader_id,
            RaftMessage::AppendEntriesReply(AppendEntriesReply {
                from: self.id,
                term: self.current_term,
                success: true,
                conflict_index: None,
                match_index,
            }),
        ));
    }

    /// Compute the conflict hint a follower returns when its log does not match
    /// the leader's preceding entry (Requirements 8.3, 8.4).
    ///
    /// If the follower's log is too short, it points the leader just past the
    /// follower's last entry. Otherwise the entry at `prev` is of the wrong
    /// term, so it points at the first index of that conflicting term — letting
    /// the leader skip the whole bad run in one round rather than backing up one
    /// index at a time.
    fn conflict_hint(&self, prev: u64) -> u64 {
        match self.log.last_index() {
            // Empty log: ask the leader to start from the head.
            None => 0,
            // Log too short: resume just past our last entry.
            Some(last) if prev > last => last + 1,
            // Term mismatch at `prev`: rewind to the first index of that term.
            Some(_) => {
                let bad_term = self.log.term_at(prev);
                let mut idx = prev;
                while idx > 0 && self.log.term_at(idx - 1) == bad_term {
                    idx -= 1;
                }
                idx
            }
        }
    }

    /// Handle a follower's reply to replication (Requirements 8.4, 8.5, 8.9).
    ///
    /// Only the current leader acts on replies for its current term. On success
    /// it advances the peer's `match_index`/`next_index` (taking the maximum so
    /// stale acks cannot regress progress), resets the retry backoff, and
    /// recomputes the commit index. On rejection it backs `next_index` up to the
    /// follower's conflict hint, grows the capped exponential backoff, and
    /// immediately retransmits with the earlier entries.
    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply, out: &mut RaftOutput) {
        // A reply for an older term, or one arriving after we have stepped
        // down, is stale and must be ignored.
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        let peer = reply.from;
        // Only replies from a known peer affect leader state.
        if !self.peers.contains(&peer) {
            return;
        }

        if reply.success {
            // Advance this peer's replication progress monotonically; a stale or
            // duplicated ack carrying a lower (or absent) match cannot regress it
            // (Requirements 8.5, 8.9).
            if let Some(matched) = reply.match_index {
                let improved = self
                    .match_index
                    .get(&peer)
                    .is_none_or(|&current| matched > current);
                if improved {
                    self.match_index.insert(peer, matched);
                }
                let next = self.next_index.entry(peer).or_insert(0);
                *next = (*next).max(matched + 1);
            }
            // The follower is making progress: reset its retry backoff.
            self.replication_backoff
                .insert(peer, REPLICATION_BACKOFF_BASE);
            // Newly acknowledged entries may now satisfy a majority.
            self.advance_commit(out);
        } else {
            // Log mismatch: back up next_index to the follower's hint (or by one
            // as a fallback) and grow the capped exponential backoff before
            // retrying with earlier entries (Requirements 8.3, 8.4).
            let backed_up = match reply.conflict_index {
                Some(hint) => hint,
                None => self
                    .next_index
                    .get(&peer)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(1),
            };
            self.next_index.insert(peer, backed_up);
            self.grow_backoff(peer);
            // Retransmit immediately from the backed-up cursor (Requirement 8.9).
            self.send_append_entries(peer, out);
        }
    }

    /// Double a peer's replication retry backoff, capped at
    /// [`REPLICATION_BACKOFF_MAX`] (Requirement 8.4).
    fn grow_backoff(&mut self, peer: NodeId) {
        let current = self
            .replication_backoff
            .get(&peer)
            .copied()
            .unwrap_or(REPLICATION_BACKOFF_BASE);
        let doubled = current.saturating_mul(2).min(REPLICATION_BACKOFF_MAX);
        self.replication_backoff.insert(peer, doubled);
    }
}

#[cfg(test)]
mod tests {
    use super::sim::{SimCluster, StepOutcome};
    use super::*;
    use vela_log::InMemoryLog;

    /// Step the cluster until some replica believes itself leader, or until the
    /// step budget is exhausted. Heartbeats keep a healthy cluster perpetually
    /// busy, so a bound is required.
    fn run_until_leader(sim: &mut SimCluster, budget: usize) -> Option<NodeId> {
        for _ in 0..budget {
            if let Some(leader) = sim.leader() {
                return Some(leader);
            }
            if matches!(sim.step(), StepOutcome::Idle) {
                break;
            }
        }
        sim.leader()
    }

    #[test]
    fn single_node_elects_itself_on_election_timeout() {
        let mut sim = SimCluster::new(1, 1);
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        // One step delivers the election timeout; with no peers the self-vote
        // is already a majority, so the node becomes leader immediately.
        assert!(matches!(sim.step(), StepOutcome::Timer { .. }));
        assert_eq!(sim.role(NodeId(0)), Some(Role::Leader));
        assert_eq!(sim.node(NodeId(0)).unwrap().current_term(), 1);
    }

    #[test]
    fn three_node_cluster_elects_a_single_leader() {
        let mut sim = SimCluster::new(3, 42);
        // Arm only one node so it times out first and wins cleanly.
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = run_until_leader(&mut sim, 100).expect("a leader should be elected");

        let leaders = (0..3)
            .filter(|&i| sim.role(NodeId(i)) == Some(Role::Leader))
            .count();
        assert_eq!(leaders, 1, "exactly one leader per term (R7.10)");
        assert!(sim.node(leader).unwrap().current_term() >= 1);
    }

    #[test]
    fn higher_term_request_vote_forces_step_down() {
        let mut sim = SimCluster::new(3, 7);
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = run_until_leader(&mut sim, 100).expect("a leader should be elected");
        let term = sim.node(leader).unwrap().current_term();

        // Inject a vote request from a peer carrying a strictly greater term.
        let peer = (0..3).map(NodeId).find(|&n| n != leader).unwrap();
        sim.send(
            peer,
            leader,
            RaftMessage::RequestVote(RequestVote {
                term: term + 10,
                candidate_id: peer,
                last_log_index: None,
                last_log_term: None,
            }),
        );

        // Step until the leader observes the higher term; it must adopt it and
        // revert to follower (R7.9).
        let mut adopted = false;
        for _ in 0..50 {
            sim.step();
            if sim.node(leader).unwrap().current_term() >= term + 10 {
                assert_eq!(sim.role(leader), Some(Role::Follower));
                adopted = true;
                break;
            }
        }
        assert!(adopted, "leader should adopt the higher term and step down");
    }

    #[test]
    fn vote_is_denied_for_a_stale_term() {
        // A node in term 5 must deny a candidate whose term is lower, and reply
        // with its own current term (Requirement 7.8).
        let mut node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], InMemoryLog::new());
        let mut clock = TestClock::default();

        // Raise the node to term 5 by feeding a higher-term heartbeat.
        node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: 5,
                leader_id: NodeId(1),
                prev_log_index: None,
                prev_log_term: None,
                entries: Vec::new(),
                leader_commit: None,
            })),
            &mut clock,
        );
        assert_eq!(node.current_term(), 5);

        // A stale-term vote request is denied; the reply carries term 5.
        let out = node.step(
            RaftInput::Message(RaftMessage::RequestVote(RequestVote {
                term: 3,
                candidate_id: NodeId(2),
                last_log_index: None,
                last_log_term: None,
            })),
            &mut clock,
        );
        let reply = expect_vote_reply(&out);
        assert_eq!(reply.term, 5);
        assert!(!reply.vote_granted);
        assert_eq!(node.voted_for(), None);
    }

    #[test]
    fn vote_is_granted_once_per_term_with_up_to_date_log() {
        // First eligible candidate is granted; a second, different candidate in
        // the same term is denied (at-most-one-vote-per-term, R7.7, R7.8).
        let mut node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], InMemoryLog::new());
        let mut clock = TestClock::default();

        let granted = node.step(
            RaftInput::Message(RaftMessage::RequestVote(RequestVote {
                term: 1,
                candidate_id: NodeId(1),
                last_log_index: None,
                last_log_term: None,
            })),
            &mut clock,
        );
        let reply = expect_vote_reply(&granted);
        assert!(reply.vote_granted);
        assert_eq!(node.voted_for(), Some(NodeId(1)));

        let denied = node.step(
            RaftInput::Message(RaftMessage::RequestVote(RequestVote {
                term: 1,
                candidate_id: NodeId(2),
                last_log_index: None,
                last_log_term: None,
            })),
            &mut clock,
        );
        let reply = expect_vote_reply(&denied);
        assert!(!reply.vote_granted);
        assert_eq!(node.voted_for(), Some(NodeId(1)));
    }

    #[test]
    fn fresh_node_knows_no_leader() {
        // A brand-new follower has heard from no leader yet (Requirement 8.1).
        let node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], InMemoryLog::new());
        assert_eq!(node.leader_id(), None);
    }

    #[test]
    fn append_entries_records_the_contacting_leader() {
        // A valid `AppendEntries` teaches a follower who the current leader is
        // so clients can be redirected to it (Raft §5.2; Requirement 8.1).
        let mut node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], InMemoryLog::new());
        let mut clock = TestClock::default();

        node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: 3,
                leader_id: NodeId(1),
                prev_log_index: None,
                prev_log_term: None,
                entries: Vec::new(),
                leader_commit: None,
            })),
            &mut clock,
        );

        assert_eq!(node.leader_id(), Some(NodeId(1)));
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn winning_an_election_records_self_as_leader() {
        // A single-node group is its own majority, so the election timeout
        // promotes it to leader; it records itself as the known leader
        // (Requirement 8.1).
        let mut sim = SimCluster::new(1, 1);
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        sim.step();
        assert_eq!(sim.role(NodeId(0)), Some(Role::Leader));
        assert_eq!(sim.node(NodeId(0)).unwrap().leader_id(), Some(NodeId(0)));
    }

    #[test]
    fn stepping_down_to_a_higher_term_clears_the_known_leader() {
        // After learning a leader, adopting a strictly higher term (here via a
        // higher-term vote request) steps the node down and forgets the leader
        // until a new one contacts it (Requirement 8.1).
        let mut node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], InMemoryLog::new());
        let mut clock = TestClock::default();

        node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: 2,
                leader_id: NodeId(1),
                prev_log_index: None,
                prev_log_term: None,
                entries: Vec::new(),
                leader_commit: None,
            })),
            &mut clock,
        );
        assert_eq!(node.leader_id(), Some(NodeId(1)));

        node.step(
            RaftInput::Message(RaftMessage::RequestVote(RequestVote {
                term: 5,
                candidate_id: NodeId(2),
                last_log_index: None,
                last_log_term: None,
            })),
            &mut clock,
        );
        assert_eq!(node.leader_id(), None);
    }

    #[test]
    fn starting_an_election_clears_the_known_leader() {
        // A follower that knew a leader and then times out becomes a candidate
        // for a new term, in which it knows of no leader yet (Requirement 8.1).
        let mut node = RaftNode::new(NodeId(0), vec![NodeId(1), NodeId(2)], InMemoryLog::new());
        let mut clock = TestClock::default();

        node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: None,
                prev_log_term: None,
                entries: Vec::new(),
                leader_commit: None,
            })),
            &mut clock,
        );
        assert_eq!(node.leader_id(), Some(NodeId(1)));

        node.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.leader_id(), None);
    }

    #[test]
    fn duplicated_vote_grant_from_one_voter_does_not_advance_the_tally_twice() {
        // A 5-node group has a majority of 3. The candidate's self-vote is one;
        // a granted reply from NodeId(1) makes two. Re-delivering that *same*
        // voter's grant (a legitimate network duplication) must be idempotent —
        // the tally stays at two and the node remains a candidate. Only a grant
        // from a *distinct* voter (NodeId(2)) reaches the majority of three and
        // promotes the node to leader. Counting the duplicate would elect a
        // leader without genuine majority support, allowing two leaders in one
        // term (Raft §5.2 Election Safety).
        let mut node = RaftNode::new(
            NodeId(0),
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
            InMemoryLog::new(),
        );
        let mut clock = TestClock::default();

        // Become a term-1 candidate with its own self-vote.
        node.step(RaftInput::Tick(TimerKind::Election), &mut clock);
        assert_eq!(node.role(), Role::Candidate);
        let term = node.current_term();

        let grant_from = |voter: NodeId| {
            RaftInput::Message(RaftMessage::RequestVoteReply(RequestVoteReply {
                term,
                vote_granted: true,
                voter,
            }))
        };

        // First grant from NodeId(1): tally is now {self, node-1} = 2 < 3.
        node.step(grant_from(NodeId(1)), &mut clock);
        assert_eq!(
            node.role(),
            Role::Candidate,
            "two distinct votes are short of the 3-of-5 majority"
        );

        // Duplicate grant from the *same* voter: must not advance the tally.
        node.step(grant_from(NodeId(1)), &mut clock);
        assert_eq!(
            node.role(),
            Role::Candidate,
            "a duplicated grant from one voter must not push the tally to a majority"
        );

        // A grant from a distinct voter reaches the majority of three → leader.
        node.step(grant_from(NodeId(2)), &mut clock);
        assert_eq!(
            node.role(),
            Role::Leader,
            "a third distinct vote is a genuine majority and elects the leader"
        );
    }

    /// A trivial [`Clock`] for direct, single-node `step` tests: time never
    /// advances on its own and armed timers are simply recorded.
    #[derive(Default)]
    struct TestClock {
        armed: Vec<TimerKind>,
    }

    impl Clock for TestClock {
        fn now(&self) -> Instant {
            // A fixed reference instant is sufficient; these tests never read it.
            Instant::now()
        }
        fn arm(&mut self, kind: TimerKind, _dur: Duration) {
            self.armed.push(kind);
        }
    }

    /// Extract the single `RequestVoteReply` from a step's outbound messages.
    fn expect_vote_reply(out: &RaftOutput) -> RequestVoteReply {
        out.sends
            .iter()
            .find_map(|(_, m)| match m {
                RaftMessage::RequestVoteReply(r) => Some(r.clone()),
                _ => None,
            })
            .expect("expected a RequestVoteReply in the step output")
    }

    /// Extract the single `AppendEntriesReply` from a step's outbound messages.
    fn expect_append_reply(out: &RaftOutput) -> AppendEntriesReply {
        out.sends
            .iter()
            .find_map(|(_, m)| match m {
                RaftMessage::AppendEntriesReply(r) => Some(r.clone()),
                _ => None,
            })
            .expect("expected an AppendEntriesReply in the step output")
    }

    fn record(byte: u8) -> EntryPayload {
        EntryPayload::new(PayloadKind::Record, vec![byte])
    }

    #[test]
    fn leader_replicates_and_commits_a_proposal() {
        let mut sim = SimCluster::new(3, 42);
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = run_until_leader(&mut sim, 200).expect("a leader should be elected");

        sim.propose(leader, record(7));

        // Drive replication until the leader observes the entry committed.
        for _ in 0..200 {
            if sim.node(leader).unwrap().commit_index().is_some() {
                break;
            }
            sim.step();
        }

        // The current-term entry replicated to a majority and committed (R8.5).
        assert_eq!(sim.node(leader).unwrap().commit_index(), Some(0));
        // It was surfaced exactly once for the state machine to apply (R8.8).
        let surfaced = sim.committed(leader);
        assert_eq!(surfaced.len(), 1);
        assert_eq!(surfaced[0].payload, record(7));
        // last_applied tracks the commit index (R8.8).
        assert_eq!(sim.node(leader).unwrap().last_applied(), Some(0));
    }

    #[test]
    fn followers_converge_to_leader_log_after_many_proposals() {
        let mut sim = SimCluster::new(3, 99);
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = run_until_leader(&mut sim, 200).expect("a leader should be elected");

        for k in 0..10u8 {
            sim.propose(leader, record(k));
        }
        sim.run_until_idle(2000);

        let leader_last = sim.node(leader).unwrap().log().last_index();
        assert_eq!(leader_last, Some(9));
        // The leader committed every current-term entry (R8.5).
        assert_eq!(sim.node(leader).unwrap().commit_index(), Some(9));

        // Every replica's log agrees with the leader's, entry for entry — the
        // Log Matching Property and lagging-follower convergence (R8.9, R8.10).
        for i in 0..3 {
            let node = sim.node(NodeId(i)).unwrap();
            assert_eq!(node.log().last_index(), leader_last, "node {i} converged");
            for idx in 0..=9 {
                assert_eq!(
                    node.log().entry(idx).map(|e| (e.index, e.payload)),
                    Some((idx, record(idx as u8))),
                    "node {i} entry {idx} matches leader"
                );
            }
        }
    }

    #[test]
    fn append_entries_batch_is_capped_at_256() {
        let mut sim = SimCluster::new(3, 5);
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        let leader = run_until_leader(&mut sim, 200).expect("a leader should be elected");

        // Pile up a backlog without delivering acks, so next_index stays at 0
        // and the next RPC would carry the whole backlog if it were not capped.
        for k in 0..300u32 {
            sim.propose(
                leader,
                EntryPayload::new(PayloadKind::Record, k.to_le_bytes().to_vec()),
            );
        }
        let out = sim
            .propose(leader, record(0))
            .expect("leader accepts the proposal");

        for (_, msg) in &out.sends {
            if let RaftMessage::AppendEntries(ae) = msg {
                assert!(
                    ae.entries.len() <= MAX_ENTRIES_PER_APPEND as usize,
                    "AppendEntries carried {} entries, exceeding the 256 cap (R8.1)",
                    ae.entries.len()
                );
            }
        }
    }

    #[test]
    fn follower_rejects_mismatched_prev_with_a_conflict_hint() {
        let mut node = RaftNode::new(NodeId(0), vec![NodeId(1)], InMemoryLog::new());
        let mut clock = TestClock::default();

        // Seed the follower with entry 0 (term 1) via a matching AppendEntries.
        let seeded = node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: 1,
                leader_id: NodeId(1),
                prev_log_index: None,
                prev_log_term: None,
                entries: vec![LogEntry {
                    index: 0,
                    term: 1,
                    payload: record(1),
                }],
                leader_commit: None,
            })),
            &mut clock,
        );
        assert!(expect_append_reply(&seeded).success);
        assert_eq!(node.log().last_index(), Some(0));

        // Now a leader claims a preceding entry (index 0) of term 2: the terms
        // disagree, so the follower must reject and hint where to back up (R8.3).
        let rejected = node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: 2,
                leader_id: NodeId(1),
                prev_log_index: Some(0),
                prev_log_term: Some(2),
                entries: vec![LogEntry {
                    index: 1,
                    term: 2,
                    payload: record(9),
                }],
                leader_commit: None,
            })),
            &mut clock,
        );
        let reply = expect_append_reply(&rejected);
        assert!(!reply.success);
        assert_eq!(reply.conflict_index, Some(0));
        // The conflicting entry was not appended (R8.3).
        assert_eq!(node.log().last_index(), Some(0));
    }

    #[test]
    fn follower_accepts_matching_append_and_advances_commit() {
        let mut node = RaftNode::new(NodeId(0), vec![NodeId(1)], InMemoryLog::new());
        let mut clock = TestClock::default();

        let out = node.step(
            RaftInput::Message(RaftMessage::AppendEntries(AppendEntries {
                term: 3,
                leader_id: NodeId(1),
                prev_log_index: None,
                prev_log_term: None,
                entries: vec![
                    LogEntry {
                        index: 0,
                        term: 3,
                        payload: record(0),
                    },
                    LogEntry {
                        index: 1,
                        term: 3,
                        payload: record(1),
                    },
                ],
                leader_commit: Some(1),
            })),
            &mut clock,
        );

        let reply = expect_append_reply(&out);
        assert!(reply.success);
        // Acknowledges up to the last conveyed index (R8.2).
        assert_eq!(reply.match_index, Some(1));
        // Adopts the leader's commit index and surfaces both entries in order
        // (R8.7, R8.8).
        assert_eq!(node.commit_index(), Some(1));
        let committed: Vec<u64> = out.committed.iter().map(|e| e.index).collect();
        assert_eq!(committed, vec![0, 1]);
    }

    #[test]
    fn recover_initializes_commit_index_and_last_applied_from_log() {
        // A restart restores the commit position from the recovered log: a log
        // carrying committed entries hands its commit index to both
        // `commit_index` and `last_applied`, so the replica re-applies the
        // committed prefix exactly once and never re-surfaces it (R10.1, R10.2,
        // R11.1). A fresh log carries no hard state, so term/vote default.
        let mut log = InMemoryLog::new();
        log.append(record(0), 1).expect("append entry 0");
        log.append(record(1), 1).expect("append entry 1");
        log.append(record(2), 1).expect("append entry 2");
        log.commit(2).expect("commit through index 2");
        let recovered_commit = log.commit_index();
        assert_eq!(recovered_commit, Some(2));

        let node = RaftNode::recover(NodeId(0), vec![NodeId(1), NodeId(2)], log);

        assert_eq!(node.commit_index(), recovered_commit);
        assert_eq!(node.last_applied(), recovered_commit);
        // A fresh log persisted no hard state, so term 0 and no vote.
        assert_eq!(node.current_term(), 0);
        assert_eq!(node.voted_for(), None);
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn single_node_leader_commits_proposal_immediately() {
        // With no peers the leader alone is a majority, so a proposal commits on
        // the same step it is appended (R8.5).
        let mut sim = SimCluster::new(1, 1);
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);
        sim.step();
        assert_eq!(sim.role(NodeId(0)), Some(Role::Leader));

        let out = sim.propose(NodeId(0), record(5)).expect("leader accepts");
        assert_eq!(sim.node(NodeId(0)).unwrap().commit_index(), Some(0));
        assert_eq!(out.committed.len(), 1);
        assert_eq!(out.committed[0].payload, record(5));
    }
}
