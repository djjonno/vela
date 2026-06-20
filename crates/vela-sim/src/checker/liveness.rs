//! Liveness checks under healed faults (Requirement 12, design Property 21):
//! per-group "favorable since" tracking and bounded-progress assertions (one
//! leader elected, a produced record commits, a topic admin command commits,
//! lagging replicas converge), with no progress required of a group lacking a
//! reachable majority.
//!
//! These checks consume the shared [`Violation`](super::Violation) /
//! [`PropertyId`](super::PropertyId) vocabulary defined in the parent module.
//!
//! # What the checker observes
//!
//! Liveness is asserted *only* under favorable conditions and *only* after a
//! bounded budget has elapsed (design "Liveness_Checker"). The checker is **fed
//! observations** by the run orchestration (task 20.1) rather than driving the
//! cluster itself:
//!
//! - [`observe`](LivenessChecker::observe) — called once per step. For every
//!   Raft group it recomputes the *favorable* condition (a majority of the
//!   group's voters are running **and** mutually reachable), opening a
//!   "favorable since" window the first step the condition holds and closing it
//!   the moment it stops holding. While a window is open it records the
//!   progress the group has made: whether exactly one leader has been elected
//!   (12.1) and whether the group has *converged* — the leader has committed
//!   the whole of its log and every running voter has caught up to the leader's
//!   commit index (12.2, 12.3, 12.4).
//! - [`note_fault`](LivenessChecker::note_fault) /
//!   [`note_heal`](LivenessChecker::note_heal) — called by the runtime whenever
//!   it applies or heals *any* fault (crash, restart, partition/heal, clock
//!   skew, storage fault). Either event restarts the favorable budget clock:
//!   the favorable condition requires that **no further faults** are introduced
//!   (design), and a heal is the instant a group may *become* favorable, so the
//!   bounded budget for progress is measured from the most recent change to the
//!   fault set. The next [`observe`](LivenessChecker::observe) re-opens a window
//!   for any group that is favorable at that instant.
//! - [`check`](LivenessChecker::check) — called to test for a violation. It
//!   flags [`PropertyId::Liveness`] for a group **only** when its favorable
//!   window has stayed open *longer than the budget* without the group making
//!   the required progress (12.5). A group whose window is closed — because it
//!   lacks a running, mutually-reachable majority — is never required to make
//!   progress (12.6, and 6.6 for the crash case), so it can never be flagged.
//!
//! # Soundness (no false positives)
//!
//! A run must never be failed for a liveness "violation" that is not real. The
//! favorable condition is therefore deliberately **conservative**: a group is
//! favorable only when a majority of its voters are running *and every running
//! voter is pairwise mutually reachable*. Whenever that holds, a leader can be
//! elected among a majority that can all communicate, so Raft is genuinely
//! expected to make progress — flagging a stall there is correct. When the
//! condition does *not* hold (for instance a clean majority exists but some
//! extra running voter is partitioned away), the checker simply does not
//! require progress; it can never raise a violation it cannot justify. The cost
//! is that some real stalls in awkward partial-partition shapes go unchecked,
//! which is the safe direction for a property the suite uses to *fail* a run.

use std::collections::{BTreeMap, BTreeSet};

use vela_core::{metadata_group_key, GroupKey, NodeId, PartitionReplica};
use vela_log::LogStorage;
use vela_raft::Role;

use super::{PropertyId, Violation};
use crate::cluster::{SimNode, SimulatedCluster};
use crate::scheduler::{VirtualDuration, VirtualInstant};

/// A read-only view of one voter of a group at one instant.
///
/// The checker works against these plain snapshots rather than the live
/// replicas so its favorable/progress logic is pure and unit-testable with
/// synthetic states (mirroring the snapshot approach in
/// [`raft_safety`](super::raft_safety)).
#[derive(Debug, Clone)]
struct VoterSnapshot {
    /// The voter's domain node id.
    node: NodeId,
    /// Whether this voter can currently participate in the group's consensus:
    /// its node is running **and** it actually hosts a replica for the group.
    /// A running node that has not (yet) started the replica counts as *down*
    /// for this group — it cannot vote or replicate.
    up: bool,
    /// The role the replica holds (meaningful only when [`up`](Self::up)).
    role: Role,
    /// The replica's commit index, or `None` if nothing has committed
    /// (meaningful only when [`up`](Self::up)).
    commit_index: Option<u64>,
    /// The replica's last log index, or `None` for an empty log (meaningful
    /// only when [`up`](Self::up)). Used to tell whether the leader still has
    /// uncommitted entries pending (12.2, 12.3).
    last_index: Option<u64>,
}

/// A read-only snapshot of one Raft group: every voter in its fixed
/// `Replica_Set` plus the pairwise mutual-reachability of the running voters.
#[derive(Debug, Clone)]
struct GroupSnapshot {
    /// The group key (a partition group or `__meta/0`).
    group: GroupKey,
    /// Every voter in the group's `Replica_Set`, in `Replica_Set` order.
    voters: Vec<VoterSnapshot>,
    /// Symmetric pairwise mutual-reachability among voters, indexed to align
    /// with [`voters`](Self::voters): `reachable[i][j]` is `true` iff voters
    /// `i` and `j` are both up and can exchange messages in both directions.
    /// The diagonal is `true`. Down voters are unreachable from everyone.
    reachable: Vec<Vec<bool>>,
}

impl GroupSnapshot {
    /// The strict majority size of the group's `Replica_Set`
    /// (`floor(n/2) + 1`).
    fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Indices of the voters that are currently up.
    fn up_indices(&self) -> Vec<usize> {
        (0..self.voters.len())
            .filter(|&i| self.voters[i].up)
            .collect()
    }

    /// Whether the group is *favorable*: a majority of its voters are up and
    /// every up voter is pairwise mutually reachable (Requirement 12.1, 12.6).
    ///
    /// This is the conservative condition described in the module docs: when it
    /// holds, a majority that can all communicate exists, so progress is
    /// genuinely expected.
    fn favorable(&self) -> bool {
        let up = self.up_indices();
        if up.len() < self.majority() {
            return false;
        }
        for (a, &i) in up.iter().enumerate() {
            for &j in &up[a + 1..] {
                if !self.reachable[i][j] {
                    return false;
                }
            }
        }
        true
    }

    /// The index of the sole up leader, or `None` unless **exactly one** up
    /// voter is in [`Role::Leader`] (Requirement 12.1's "exactly one leader").
    fn single_leader(&self) -> Option<usize> {
        let mut leader = None;
        for &i in &self.up_indices() {
            if self.voters[i].role == Role::Leader {
                if leader.is_some() {
                    return None; // two leaders: not the single-leader case
                }
                leader = Some(i);
            }
        }
        leader
    }

    /// Whether the group has *converged* under `leader`: the leader has
    /// committed the whole of its log (so any subsequently produced record or
    /// topic admin command has committed — 12.2, 12.3) and every up voter has
    /// caught up to the leader's commit index (lagging replicas converged —
    /// 12.4).
    fn converged(&self, leader: usize) -> bool {
        let l = &self.voters[leader];
        // The leader has no uncommitted backlog: everything it holds is
        // committed. With `Always` sync and a healthy majority this is reached
        // once the leader's term entries replicate.
        if l.commit_index != l.last_index {
            return false;
        }
        self.up_indices()
            .iter()
            .all(|&i| self.voters[i].commit_index == l.commit_index)
    }

    /// The progress observable this instant: whether a single leader is elected,
    /// and whether the group has fully converged behind it.
    fn progress(&self) -> Progress {
        match self.single_leader() {
            Some(leader) => Progress {
                leader_elected: true,
                converged: self.converged(leader),
            },
            None => Progress {
                leader_elected: false,
                converged: false,
            },
        }
    }
}

/// The progress a group has made at one observation.
#[derive(Debug, Clone, Copy)]
struct Progress {
    /// Exactly one leader is elected for the group (Requirement 12.1).
    leader_elected: bool,
    /// The group has converged: the leader committed its whole log and every up
    /// voter caught up to its commit index (Requirement 12.2, 12.3, 12.4).
    converged: bool,
}

/// The state of one group's "favorable since" window (design "Liveness_Checker"
/// — per-group markers set when the condition becomes true, cleared on any new
/// fault).
#[derive(Debug, Clone, Copy, Default)]
struct GroupWindow {
    /// The instant the current favorable window opened, or `None` when the group
    /// is not currently favorable (and so is required to make no progress).
    since: Option<VirtualInstant>,
    /// Whether exactly one leader has been observed during the open window
    /// (Requirement 12.1). Retained for diagnostics so [`check`] can say whether
    /// election or convergence is the unmet step.
    ///
    /// [`check`]: LivenessChecker::check
    leader_elected: bool,
    /// Whether the group has converged during the open window — the moment this
    /// becomes true the window's required progress is satisfied and it can never
    /// be flagged (Requirement 12.2, 12.3, 12.4).
    satisfied: bool,
}

impl GroupWindow {
    /// Open a fresh window at `now`, discarding any progress recorded for a
    /// prior window.
    fn open(&mut self, now: VirtualInstant) {
        self.since = Some(now);
        self.leader_elected = false;
        self.satisfied = false;
    }

    /// Close the window: the group is not favorable, so it is required to make
    /// no progress (Requirement 12.6) and a subsequent favorable spell starts a
    /// brand-new budget.
    fn close(&mut self) {
        self.since = None;
        self.leader_elected = false;
        self.satisfied = false;
    }
}

/// Asserts the Liveness_Properties under healed faults within a bounded budget
/// (Requirement 12, design Property 21).
///
/// Tracks one ["favorable since"](GroupWindow) window per group and the progress
/// made within it. A violation is raised only when a window has been open longer
/// than [`budget`](Self::budget) without the group converging — never for a
/// group that lacks a running, mutually-reachable majority.
///
/// The bounded budget is expressed as a [`VirtualDuration`] span: the run
/// orchestration (task 20.1) constructs the checker with a budget derived from
/// the scenario's [`Budget`](crate::scenario::Budget) (a fraction of
/// `max_virtual_nanos`, generous enough to cover election + replication under
/// the configured fault intensities).
#[derive(Debug, Clone)]
pub struct LivenessChecker {
    /// The bounded simulated-time span a favorable group has to make progress
    /// before a stall is a violation (Requirement 12.5).
    budget: VirtualDuration,
    /// Per-group favorable windows, keyed for deterministic iteration so the
    /// first violation [`check`](Self::check) reports is a pure function of the
    /// run (Requirement 1.5).
    windows: BTreeMap<GroupKey, GroupWindow>,
}

impl LivenessChecker {
    /// A fresh checker that flags a favorable-but-stalled group once its window
    /// exceeds `budget` of logical time without progress.
    #[must_use]
    pub fn new(budget: VirtualDuration) -> Self {
        Self {
            budget,
            windows: BTreeMap::new(),
        }
    }

    /// The bounded budget a favorable group is given to make progress.
    #[must_use]
    pub fn budget(&self) -> VirtualDuration {
        self.budget
    }

    /// Observe the cluster at instant `now`, updating every group's favorable
    /// window and the progress recorded within it.
    ///
    /// Call once per step. For each group it opens a window the first step the
    /// group is favorable (a majority up and mutually reachable), records
    /// leader-election and convergence while the window stays open, and closes
    /// the window the moment the group stops being favorable. Observation never
    /// flags a violation itself — a stall is only a violation *after* the budget
    /// elapses, which [`check`](Self::check) tests.
    pub fn observe(&mut self, cluster: &SimulatedCluster, now: VirtualInstant) {
        let snaps = collect_snapshots(cluster);
        self.observe_snapshots(&snaps, now);
    }

    /// The snapshot-driven core of [`observe`](Self::observe), split out so the
    /// favorable/progress logic can be unit-tested with synthetic group states.
    fn observe_snapshots(&mut self, snaps: &[GroupSnapshot], now: VirtualInstant) {
        let mut seen: BTreeSet<&GroupKey> = BTreeSet::new();
        for snap in snaps {
            seen.insert(&snap.group);
            let window = self.windows.entry(snap.group.clone()).or_default();
            if !snap.favorable() {
                window.close();
                continue;
            }
            if window.since.is_none() {
                window.open(now);
            }
            let progress = snap.progress();
            if progress.leader_elected {
                window.leader_elected = true;
            }
            if progress.converged {
                window.satisfied = true;
            }
        }
        // A group with no snapshot this step (its topic was deleted, or every
        // replica is down) has no reachable majority, so close its window: it is
        // required to make no progress (Requirement 12.6).
        for (group, window) in self.windows.iter_mut() {
            if !seen.contains(group) {
                window.close();
            }
        }
    }

    /// Note that the runtime applied a fault at `now`, restarting every group's
    /// favorable budget clock.
    ///
    /// The favorable condition requires that **no further faults** are
    /// introduced (design "Liveness_Checker"), so any newly-applied fault closes
    /// the open windows; the next [`observe`](Self::observe) re-opens a window
    /// for any group still favorable. Resetting every group (rather than only
    /// the faulted one) is conservative — it can only *delay* a violation, never
    /// raise a spurious one.
    pub fn note_fault(&mut self, now: VirtualInstant) {
        let _ = now;
        self.reset_windows();
    }

    /// Note that the runtime healed a fault at `now`, restarting every group's
    /// favorable budget clock.
    ///
    /// A heal is the instant a group may *become* favorable, and Requirement
    /// 12.2/12.3 measure the bounded budget from when faults are healed, so the
    /// progress budget is taken from the heal: the open windows are closed and
    /// the next [`observe`](Self::observe) re-opens them at the heal instant for
    /// any group that is now favorable.
    pub fn note_heal(&mut self, now: VirtualInstant) {
        let _ = now;
        self.reset_windows();
    }

    /// Close every open window (a fault was applied or healed).
    fn reset_windows(&mut self) {
        for window in self.windows.values_mut() {
            window.close();
        }
    }

    /// Test for a Liveness_Property violation at instant `now`.
    ///
    /// Returns a [`Violation`] of [`PropertyId::Liveness`] for the first group
    /// (in deterministic key order) whose favorable window has been open *longer
    /// than the budget* without converging (Requirement 12.5). A group that is
    /// not favorable — lacking a running, mutually-reachable majority — has a
    /// closed window and is never flagged (Requirement 12.6, 6.6). A group that
    /// has already converged is likewise never flagged.
    ///
    /// # Errors
    ///
    /// Returns a [`Violation`] when a favorable group has failed to make the
    /// required progress within [`budget`](Self::budget) of opening its window.
    pub fn check(&self, now: VirtualInstant) -> Result<(), Violation> {
        for (group, window) in &self.windows {
            let Some(since) = window.since else {
                continue;
            };
            if window.satisfied {
                continue;
            }
            if now.duration_since(since) > self.budget {
                let unmet = if window.leader_elected {
                    "a leader was elected but the group has not converged \
                     (leader backlog uncommitted or a voter is lagging)"
                } else {
                    "no leader was elected"
                };
                return Err(Violation::new(
                    PropertyId::Liveness,
                    now,
                    format!(
                        "group {:?} stayed favorable since t={}ns but made no progress within the \
                         {}ns budget: {}",
                        group,
                        since.as_nanos(),
                        self.budget.as_nanos(),
                        unmet,
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// The replica a node hosts for `group`, or `None` if it hosts none.
///
/// The `__meta/0` group lives on the node's [`MetadataController`] (reached via
/// [`MetadataController::meta_replica`]); every client partition lives in the
/// node's fleet. A crashed node holds neither (its volatile consensus state is
/// dropped until a restart), so this returns `None` for it.
///
/// [`MetadataController`]: vela_core::MetadataController
/// [`MetadataController::meta_replica`]: vela_core::MetadataController::meta_replica
fn replica_for<'a>(
    node: &'a SimNode,
    group: &GroupKey,
    meta: &GroupKey,
) -> Option<&'a PartitionReplica> {
    if group == meta {
        node.controller().and_then(|c| c.meta_replica())
    } else {
        node.fleet_replicas()
            .find(|(g, _)| *g == group)
            .map(|(_, replica)| replica)
    }
}

/// Snapshot every Raft group currently present in the cluster — the `__meta/0`
/// group (always present) plus every client partition group hosted by a running
/// node — for the liveness checks.
///
/// For each group it records every voter in the group's fixed `Replica_Set`
/// (whether up or down) and the pairwise mutual reachability of the running
/// voters, read from the [`SimNetwork`](crate::network::SimNetwork). Observation
/// is strictly read-only.
fn collect_snapshots(cluster: &SimulatedCluster) -> Vec<GroupSnapshot> {
    let meta = metadata_group_key();
    let topology = cluster.topology();
    let network = cluster.network();

    // The domain-id -> node map, so a group's Replica_Set members can be looked
    // up directly.
    let nodes: BTreeMap<&NodeId, &SimNode> = cluster.nodes().iter().map(|n| (n.id(), n)).collect();

    // Every group present now: `__meta/0` plus the partition groups any running
    // node hosts. A group with no running replica is absent — it has no
    // reachable majority, so liveness requires no progress of it.
    let mut groups: BTreeSet<GroupKey> = BTreeSet::new();
    groups.insert(meta.clone());
    for node in cluster.nodes() {
        if !node.is_running() {
            continue;
        }
        for (group, _) in node.fleet_replicas() {
            groups.insert(group.clone());
        }
    }

    groups
        .into_iter()
        .map(|group| {
            let members = topology.replica_set_for_group(&group).unwrap_or(&[]);
            let voters: Vec<VoterSnapshot> = members
                .iter()
                .map(|id| {
                    let replica = nodes
                        .get(id)
                        .filter(|node| node.is_running())
                        .and_then(|node| replica_for(node, &group, &meta));
                    match replica {
                        Some(replica) => {
                            let raft = replica.raft();
                            VoterSnapshot {
                                node: id.clone(),
                                up: true,
                                role: raft.role(),
                                commit_index: raft.commit_index(),
                                last_index: raft.log().last_index(),
                            }
                        }
                        None => VoterSnapshot {
                            node: id.clone(),
                            up: false,
                            role: Role::Follower,
                            commit_index: None,
                            last_index: None,
                        },
                    }
                })
                .collect();
            let reachable = reachability(&voters, network);
            GroupSnapshot {
                group,
                voters,
                reachable,
            }
        })
        .collect()
}

/// The symmetric pairwise mutual-reachability matrix for `voters`, read from
/// `network`.
///
/// `reachable[i][j]` is `true` iff voters `i` and `j` are both up and the
/// network cuts delivery in *neither* direction between them; the diagonal is
/// `true`. Down voters are unreachable from everyone.
fn reachability(voters: &[VoterSnapshot], network: &crate::network::SimNetwork) -> Vec<Vec<bool>> {
    let n = voters.len();
    let mut matrix = vec![vec![false; n]; n];
    for i in 0..n {
        matrix[i][i] = true;
        for j in (i + 1)..n {
            let reachable = voters[i].up
                && voters[j].up
                && !network.is_cut(&voters[i].node, &voters[j].node)
                && !network.is_cut(&voters[j].node, &voters[i].node);
            matrix[i][j] = reachable;
            matrix[j][i] = reachable;
        }
    }
    matrix
}

#[cfg(test)]
mod tests {
    use super::*;
    use vela_core::PartitionIndex;

    const BUDGET: VirtualDuration = VirtualDuration::from_nanos(1_000);

    fn at(nanos: u64) -> VirtualInstant {
        VirtualInstant::from_nanos(nanos)
    }

    fn group(topic: &str, partition: u32) -> GroupKey {
        (topic.to_string(), PartitionIndex(partition))
    }

    fn node(id: &str) -> NodeId {
        NodeId::new(id.to_string())
    }

    /// An up voter with the given role, commit index, and last index.
    fn up(id: &str, role: Role, commit: Option<u64>, last: Option<u64>) -> VoterSnapshot {
        VoterSnapshot {
            node: node(id),
            up: true,
            role,
            commit_index: commit,
            last_index: last,
        }
    }

    /// A down voter (its node is crashed or it has not started the replica).
    fn down(id: &str) -> VoterSnapshot {
        VoterSnapshot {
            node: node(id),
            up: false,
            role: Role::Follower,
            commit_index: None,
            last_index: None,
        }
    }

    /// A group snapshot whose up voters are all mutually reachable (the healed,
    /// quiescent shape): reachability is `true` for every pair of up voters.
    fn fully_reachable(g: GroupKey, voters: Vec<VoterSnapshot>) -> GroupSnapshot {
        let n = voters.len();
        let mut reachable = vec![vec![false; n]; n];
        for i in 0..n {
            reachable[i][i] = true;
            for j in (i + 1)..n {
                let r = voters[i].up && voters[j].up;
                reachable[i][j] = r;
                reachable[j][i] = r;
            }
        }
        GroupSnapshot {
            group: g,
            voters,
            reachable,
        }
    }

    /// A converged three-voter group: one leader that has committed its whole
    /// log and two followers caught up to the same commit index.
    fn converged_group(g: GroupKey) -> GroupSnapshot {
        fully_reachable(
            g,
            vec![
                up("node-0", Role::Leader, Some(3), Some(3)),
                up("node-1", Role::Follower, Some(3), Some(3)),
                up("node-2", Role::Follower, Some(3), Some(3)),
            ],
        )
    }

    // ----- favorable condition (12.1, 12.6) -------------------------------

    #[test]
    fn majority_up_and_reachable_is_favorable() {
        // Two of three up and mutually reachable: a majority.
        let snap = fully_reachable(
            group("orders", 0),
            vec![
                up("node-0", Role::Leader, Some(0), Some(0)),
                up("node-1", Role::Follower, Some(0), Some(0)),
                down("node-2"),
            ],
        );
        assert!(snap.favorable());
    }

    #[test]
    fn minority_up_is_not_favorable() {
        // Only one of three up: no majority, so no progress is required (6.6).
        let snap = fully_reachable(
            group("orders", 0),
            vec![
                up("node-0", Role::Follower, None, None),
                down("node-1"),
                down("node-2"),
            ],
        );
        assert!(!snap.favorable());
    }

    #[test]
    fn majority_up_but_partitioned_is_not_favorable() {
        // All three up, but node-2 is cut off from the other two: the running
        // voters are not all mutually reachable, so the checker conservatively
        // does not treat the group as favorable.
        let voters = vec![
            up("node-0", Role::Leader, Some(0), Some(0)),
            up("node-1", Role::Follower, Some(0), Some(0)),
            up("node-2", Role::Follower, None, None),
        ];
        let mut reachable = vec![vec![false; 3]; 3];
        for (i, row) in reachable.iter_mut().enumerate() {
            row[i] = true;
        }
        // node-0 <-> node-1 reachable; node-2 isolated.
        reachable[0][1] = true;
        reachable[1][0] = true;
        let snap = GroupSnapshot {
            group: group("orders", 0),
            voters,
            reachable,
        };
        assert!(!snap.favorable());
    }

    // ----- progress (12.1 - 12.4) -----------------------------------------

    #[test]
    fn single_leader_converged_group_reports_progress() {
        let snap = converged_group(group("orders", 0));
        let progress = snap.progress();
        assert!(progress.leader_elected);
        assert!(progress.converged);
    }

    #[test]
    fn no_leader_reports_no_progress() {
        let snap = fully_reachable(
            group("orders", 0),
            vec![
                up("node-0", Role::Follower, None, None),
                up("node-1", Role::Follower, None, None),
                up("node-2", Role::Follower, None, None),
            ],
        );
        let progress = snap.progress();
        assert!(!progress.leader_elected);
        assert!(!progress.converged);
    }

    #[test]
    fn leader_with_uncommitted_backlog_has_not_converged() {
        // A leader whose last index is ahead of its commit index still has work
        // to replicate: not yet converged (12.2 / 12.3 unmet).
        let snap = fully_reachable(
            group("orders", 0),
            vec![
                up("node-0", Role::Leader, Some(2), Some(5)),
                up("node-1", Role::Follower, Some(2), Some(2)),
                up("node-2", Role::Follower, Some(2), Some(2)),
            ],
        );
        let progress = snap.progress();
        assert!(progress.leader_elected);
        assert!(
            !progress.converged,
            "uncommitted leader backlog is not converged"
        );
    }

    #[test]
    fn lagging_follower_has_not_converged() {
        // Leader has committed everything, but a follower lags behind its commit
        // index: not converged (12.4 unmet).
        let snap = fully_reachable(
            group("orders", 0),
            vec![
                up("node-0", Role::Leader, Some(3), Some(3)),
                up("node-1", Role::Follower, Some(1), Some(3)),
                up("node-2", Role::Follower, Some(3), Some(3)),
            ],
        );
        let progress = snap.progress();
        assert!(progress.leader_elected);
        assert!(!progress.converged);
    }

    // ----- end-to-end window + check (12.1 - 12.6) ------------------------

    #[test]
    fn favorable_group_that_converges_within_budget_passes() {
        let mut checker = LivenessChecker::new(BUDGET);
        let g = group("orders", 0);
        // Observed favorable and already converged at t=10.
        checker.observe_snapshots(&[converged_group(g.clone())], at(10));
        // Even well past the budget, a converged group is never flagged.
        assert!(checker.check(at(10 + BUDGET.as_nanos() + 5_000)).is_ok());
    }

    #[test]
    fn group_lacking_majority_is_never_flagged() {
        // Requirement 6.6 / 12.6: a group with only a minority up is required to
        // make no progress, so it is never a liveness violation no matter how
        // long it stalls.
        let mut checker = LivenessChecker::new(BUDGET);
        let snap = fully_reachable(
            group("orders", 0),
            vec![
                up("node-0", Role::Follower, None, None),
                down("node-1"),
                down("node-2"),
            ],
        );
        checker.observe_snapshots(std::slice::from_ref(&snap), at(10));
        checker.observe_snapshots(&[snap], at(10 + BUDGET.as_nanos() + 1));
        assert!(checker.check(at(10 + 100 * BUDGET.as_nanos())).is_ok());
    }

    #[test]
    fn favorable_group_stalled_past_budget_is_flagged() {
        // Requirement 12.5: a favorable group that never elects a leader is a
        // violation, but only after the budget is exceeded.
        let mut checker = LivenessChecker::new(BUDGET);
        let g = group("orders", 0);
        let stalled = fully_reachable(
            g.clone(),
            vec![
                up("node-0", Role::Follower, None, None),
                up("node-1", Role::Follower, None, None),
                up("node-2", Role::Follower, None, None),
            ],
        );
        // Window opens at t=10.
        checker.observe_snapshots(std::slice::from_ref(&stalled), at(10));

        // Within the budget: not yet a violation (12.5).
        assert!(checker.check(at(10 + BUDGET.as_nanos())).is_ok());

        // Past the budget with no progress: a Liveness violation at this instant.
        let detect = at(10 + BUDGET.as_nanos() + 1);
        let err = checker
            .check(detect)
            .expect_err("a favorable stall past budget must be flagged");
        assert_eq!(err.property, PropertyId::Liveness);
        assert_eq!(err.at, detect);
        assert!(err.detail.contains("no leader was elected"));
    }

    #[test]
    fn a_new_fault_restarts_the_budget_clock() {
        // A fault applied mid-window restarts the favorable budget: a stall that
        // would have been flagged is given a fresh budget from the fault.
        let mut checker = LivenessChecker::new(BUDGET);
        let g = group("orders", 0);
        let stalled = fully_reachable(
            g,
            vec![
                up("node-0", Role::Follower, None, None),
                up("node-1", Role::Follower, None, None),
                up("node-2", Role::Follower, None, None),
            ],
        );
        checker.observe_snapshots(std::slice::from_ref(&stalled), at(10));
        // A fault at t=500 closes the window; the next observe re-opens it.
        checker.note_fault(at(500));
        checker.observe_snapshots(std::slice::from_ref(&stalled), at(500));
        // At t=10 + budget + 1 the *original* window would have tripped, but the
        // window now dates from t=500, so it is still within budget.
        assert!(checker.check(at(10 + BUDGET.as_nanos() + 1)).is_ok());
        // Past t=500 + budget the fresh window trips.
        let detect = at(500 + BUDGET.as_nanos() + 1);
        let err = checker
            .check(detect)
            .expect_err("the re-opened window must trip once its own budget elapses");
        assert_eq!(err.property, PropertyId::Liveness);
    }

    #[test]
    fn losing_majority_closes_the_window_and_clears_a_stall() {
        // A favorable window that then loses its majority is closed: the group
        // reverts to "no progress required" (12.6) and cannot be flagged.
        let mut checker = LivenessChecker::new(BUDGET);
        let g = group("orders", 0);
        let favorable = fully_reachable(
            g.clone(),
            vec![
                up("node-0", Role::Follower, None, None),
                up("node-1", Role::Follower, None, None),
                up("node-2", Role::Follower, None, None),
            ],
        );
        checker.observe_snapshots(&[favorable], at(10));
        // Two of three crash: only a minority remains up.
        let minority = fully_reachable(
            g,
            vec![
                up("node-0", Role::Follower, None, None),
                down("node-1"),
                down("node-2"),
            ],
        );
        checker.observe_snapshots(&[minority], at(900));
        assert!(checker.check(at(10 + 100 * BUDGET.as_nanos())).is_ok());
    }

    #[test]
    fn a_vanished_group_is_not_flagged() {
        // A group present while favorable, then absent (its topic was deleted):
        // its window is closed, so a later check never flags it.
        let mut checker = LivenessChecker::new(BUDGET);
        let g = group("orders", 0);
        let stalled = fully_reachable(
            g,
            vec![
                up("node-0", Role::Follower, None, None),
                up("node-1", Role::Follower, None, None),
                up("node-2", Role::Follower, None, None),
            ],
        );
        checker.observe_snapshots(&[stalled], at(10));
        // The group disappears from the next observation entirely.
        checker.observe_snapshots(&[], at(900));
        assert!(checker.check(at(10 + 100 * BUDGET.as_nanos())).is_ok());
    }
}
