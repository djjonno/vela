//! Raft Safety_Property checks with run-time instrumentation (Requirement 10).
//!
//! The [`RaftSafetyChecker`] has total, read-only observability of every replica
//! in the [`SimulatedCluster`] — its role, term, commit index, and full
//! replicated log — because the whole cluster runs in-process. It *detects*
//! violations; it never prevents them or alters consensus (Requirement 10.1).
//!
//! The five properties split naturally by how they are observed (design
//! "Consistency_Checker — Raft safety"):
//!
//! - **Election Safety (10.1)** and **commit monotonicity (10.5)** are *transient*
//!   conditions: a same-term double leader or a momentary commit-index regression
//!   may exist for only a step or two before the cluster moves on. They are
//!   therefore checked **incrementally**, per step, by [`observe`] — which
//!   accumulates the `(group, term) -> leader` map and the per-replica
//!   high-water commit index across the whole run, so a fleeting breach is
//!   caught at the instant it appears.
//! - **Log Matching (10.2)**, **Leader Completeness (10.3)**, and **State Machine
//!   Safety (10.4)** are *structural* properties of the logs themselves. A breach
//!   of any of them leaves durable evidence (committed entries are never
//!   rewritten), so they are checked by [`check_logs`] as a pass over a full
//!   snapshot of every replica's log. The run orchestration (task 20.1) may call
//!   [`check_logs`] as a final pass and/or periodically; it is a pure function of
//!   the snapshot and holds no state between calls.
//!
//! Any breach is returned as a [`Violation`] naming the property and the
//! detection [`VirtualInstant`] (Requirement 10.6, 2.3); the caller ends the run
//! with a failing Outcome.
//!
//! [`observe`]: RaftSafetyChecker::observe
//! [`check_logs`]: RaftSafetyChecker::check_logs

use std::collections::{BTreeMap, HashMap};

use vela_core::{metadata_group_key, GroupKey, NodeId, PartitionReplica};
use vela_log::{EntryPayload, LogEntry, LogStorage};
use vela_raft::Role;

use super::{PropertyId, Violation};
use crate::cluster::SimulatedCluster;
use crate::scheduler::VirtualInstant;

/// The identity of a single log entry for equality purposes: its term and its
/// opaque payload bytes.
///
/// Two entries at the *same index* are "the same entry" iff they share a
/// fingerprint. The index itself is excluded — entries are always compared at a
/// fixed index — so a fingerprint distinguishes a genuinely different entry
/// (different term, or same term but different payload, which is itself a bug) at
/// that index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    term: u64,
    payload: EntryPayload,
}

impl Fingerprint {
    fn of(entry: &LogEntry) -> Self {
        Self {
            term: entry.term,
            payload: entry.payload.clone(),
        }
    }
}

/// A committed entry as reconstructed from a cluster snapshot: its fingerprint
/// and the term it was committed in.
///
/// `commit_term` is the smallest `current_term` observed among the replicas that
/// currently hold the index committed. The leader that advanced commit past the
/// index did so under its own term and reports the index committed, while every
/// follower that learned of the commit adopted a term `>=` the leader's; so the
/// minimum recovers the committing leader's term (a conservative over-estimate
/// if that leader is no longer observable, which only ever *reduces* the set of
/// later-term leaders checked and so never yields a false positive).
#[derive(Debug, Clone)]
struct CommittedEntry {
    fingerprint: Fingerprint,
    commit_term: u64,
}

/// The per-replica commit-monotonicity high-water for one `(group, node)`,
/// tagged with the term of the Raft incarnation that produced it.
///
/// Commit index is **volatile, per-incarnation** state (it is not persisted): a
/// replica that crashes and is rebuilt — by durable recovery, or by a fresh
/// re-spawn after its group was deleted and recreated — re-derives its commit
/// index from scratch, so "commit never decreases" holds only *within* a single
/// incarnation. The term tags which incarnation set the high-water: a replica's
/// term is monotonic *within* an incarnation, so an observation at a strictly
/// **lower** term than the recorded high-water can only be a new incarnation
/// (`term` resets on a fresh `with_log` re-spawn), at which the commit reset is
/// correct and the high-water is dropped rather than compared. An observation at
/// the **same or a higher** term is the same (or an advanced) incarnation, so
/// commit monotonicity is enforced — a same-incarnation regression is never
/// masked, since the term cannot have decreased without a new incarnation.
#[derive(Debug, Clone)]
struct CommitHighWater {
    /// The highest term at which this replica's commit high-water was observed.
    term: u64,
    /// The highest commit index observed for this replica in that incarnation.
    commit: Option<u64>,
}

/// A read-only snapshot of one replica at one instant.
///
/// The checker works against snapshots rather than the live replicas so its
/// algorithms are pure and unit-testable with synthetic states, and so a single
/// borrow of the cluster produces an independent value the checks can iterate
/// freely. `log` is the replica's full replicated log in ascending index order
/// (empty for the cheap [`observe`](RaftSafetyChecker::observe) path, which needs
/// only role / term / commit index).
#[derive(Debug, Clone)]
struct ReplicaObservation {
    group: GroupKey,
    node: NodeId,
    role: Role,
    term: u64,
    commit_index: Option<u64>,
    log: Vec<LogEntry>,
}

impl ReplicaObservation {
    /// This replica's log as an `index -> fingerprint` map.
    fn index_map(&self) -> BTreeMap<u64, Fingerprint> {
        self.log
            .iter()
            .map(|entry| (entry.index, Fingerprint::of(entry)))
            .collect()
    }
}

/// Snapshot one replica, optionally reading its full log.
fn snapshot(
    group: GroupKey,
    node: NodeId,
    replica: &PartitionReplica,
    with_log: bool,
) -> ReplicaObservation {
    let raft = replica.raft();
    let log = if with_log {
        match raft.log().last_index() {
            // `read` is an inclusive range and carries each entry's own index,
            // so a (hypothetical) gap is represented by an absent map key rather
            // than a shifted position.
            Some(last) => raft.log().read(0, last),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    ReplicaObservation {
        group,
        node,
        role: raft.role(),
        term: raft.current_term(),
        commit_index: raft.commit_index(),
        log,
    }
}

/// Collect a snapshot of every running replica in the cluster — the metadata
/// `__meta/0` group (via [`MetadataController::meta_replica`]) plus every client
/// partition replica in each node's fleet (via [`SimNode::fleet_replicas`]).
///
/// Crashed nodes hold no live consensus state (their controller and fleet are
/// dropped until a restart recovers them), so they contribute nothing — exactly
/// the replicas the properties are stated over.
///
/// [`MetadataController::meta_replica`]: vela_core::MetadataController::meta_replica
/// [`SimNode::fleet_replicas`]: crate::cluster::SimNode::fleet_replicas
fn collect(cluster: &SimulatedCluster, with_log: bool) -> Vec<ReplicaObservation> {
    let meta_group = metadata_group_key();
    let mut obs = Vec::new();
    for node in cluster.nodes() {
        if !node.is_running() {
            continue;
        }
        if let Some(replica) = node.controller().and_then(|c| c.meta_replica()) {
            obs.push(snapshot(
                meta_group.clone(),
                node.id().clone(),
                replica,
                with_log,
            ));
        }
        for (group, replica) in node.fleet_replicas() {
            obs.push(snapshot(
                group.clone(),
                node.id().clone(),
                replica,
                with_log,
            ));
        }
    }
    obs
}

/// Detects breaches of Raft's Safety_Properties across a run with full,
/// read-only observability of every replica (Requirement 10).
///
/// Holds only the state the *incremental* checks need across steps: the
/// per-`(group, term)` leader seen so far (Election Safety) and the per-replica
/// high-water commit index (commit monotonicity). The structural checks
/// ([`check_logs`](Self::check_logs)) are stateless snapshot passes.
#[derive(Debug, Default)]
pub struct RaftSafetyChecker {
    /// `(group, term) -> the node observed as leader in that term`. A second,
    /// distinct node for the same key is an Election Safety breach (10.1).
    leaders: HashMap<(GroupKey, u64), NodeId>,
    /// `(group, node) -> the per-replica commit high-water and the term of the
    /// incarnation that set it`. A later observation below the high-water *at
    /// the same or a higher term* is a commit-monotonicity breach (10.5); a
    /// later observation at a strictly *lower* term is a fresh Raft incarnation
    /// (see [`CommitHighWater`]) at which the high-water is reset rather than
    /// compared.
    max_commit: HashMap<(GroupKey, NodeId), CommitHighWater>,
}

impl RaftSafetyChecker {
    /// A fresh checker with no observations recorded.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe the cluster at instant `now`, running the **incremental** safety
    /// checks (Election Safety 10.1, commit monotonicity 10.5).
    ///
    /// Call once per step. It reads only each running replica's role, term, and
    /// commit index — never its log — so it is cheap to run every event. It
    /// returns the first breach detected as a [`Violation`] stamped with `now`;
    /// otherwise it folds the observation into its accumulated state and returns
    /// `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns a [`Violation`] for [`PropertyId::ElectionSafety`] if two distinct
    /// replicas are leader of the same group in the same term, or
    /// [`PropertyId::CommitMonotonicity`] if a replica's commit index has
    /// decreased since a previous observation.
    pub fn observe(
        &mut self,
        cluster: &SimulatedCluster,
        now: VirtualInstant,
    ) -> Result<(), Violation> {
        let obs = collect(cluster, false);
        self.check_election_safety(&obs, now)?;
        self.check_commit_monotonicity(&obs, now)?;
        Ok(())
    }

    /// Run the **structural** safety checks over a full snapshot of every
    /// replica's log at instant `now` (Log Matching 10.2, Leader Completeness
    /// 10.3, State Machine Safety 10.4).
    ///
    /// Pure with respect to the snapshot — it holds no state between calls — so
    /// the run orchestration may call it as a single final pass or periodically
    /// during the run. It returns the first breach detected as a [`Violation`]
    /// stamped with `now`.
    ///
    /// # Errors
    ///
    /// Returns a [`Violation`] for [`PropertyId::StateMachineSafety`] if two
    /// replicas hold different committed entries at one index,
    /// [`PropertyId::LogMatching`] if two logs share an `(index, term)` but
    /// differ in a preceding entry, or [`PropertyId::LeaderCompleteness`] if a
    /// current leader is missing an entry committed in an earlier term.
    pub fn check_logs(
        &self,
        cluster: &SimulatedCluster,
        now: VirtualInstant,
    ) -> Result<(), Violation> {
        let obs = collect(cluster, true);
        // Building the committed view also checks State Machine Safety (10.4):
        // it fails if two replicas disagree on a committed entry.
        let committed = build_committed_view(&obs, now)?;
        check_log_matching(&obs, now)?;
        check_leader_completeness(&obs, &committed, now)?;
        Ok(())
    }

    /// Forget every accumulated incremental observation for `group`, so a later
    /// re-creation of the same [`GroupKey`] is treated as a *fresh Raft
    /// incarnation* (restarting at term 0 / commit `None`) rather than a
    /// continuation of the removed one.
    ///
    /// Called by the run orchestration when a topic's partition group is removed
    /// by a committed `DeleteTopic`: reconcile stops and drops that group's
    /// `RaftNode`s, and a subsequent re-creation of the same topic name spawns
    /// brand-new replicas that legitimately begin again at term 0 with no commit
    /// index. Without forgetting, the old incarnation's per-`(group, term)`
    /// leader and its `Some(_)` high-water commit would still be keyed under this
    /// `group`, so the new incarnation's (different) term-1 leader would look
    /// like a second leader in term 1 — a false [`PropertyId::ElectionSafety`]
    /// report — and its `None` commit would look like a regression from `Some(_)`
    /// — a false [`PropertyId::CommitMonotonicity`] report. Both are spurious:
    /// the two incarnations are never concurrent.
    ///
    /// Only a committed topic *delete* removes a group. A node **crash** is
    /// transient — the *same* incarnation recovers, and Election Safety / commit
    /// monotonicity must still hold across it — so this is called solely on a
    /// committed delete, never on a crash or partition. It mutates only this
    /// checker's maps and is idempotent: calling it for a `group` with no
    /// recorded state is a no-op, and it leaves every other group's state intact.
    pub fn forget_group(&mut self, group: &GroupKey) {
        self.leaders.retain(|(g, _term), _node| g != group);
        self.max_commit.retain(|(g, _node), _commit| g != group);
    }

    /// Forget the accumulated commit-monotonicity high-water for every group on
    /// `node`, so the next observation of that node — after it crashes and
    /// restarts — is treated as a *fresh Raft incarnation* for commit
    /// monotonicity (10.5) rather than a continuation of the pre-crash one.
    ///
    /// # Why commit but not term
    ///
    /// A replica's **commit index is volatile, per-incarnation state**: it is not
    /// persisted, so a restarted replica re-derives it from scratch and (as this
    /// implementation does) comes back at `None`, then re-advances as it rejoins
    /// the leader. "A replica's commit index never decreases" is therefore only a
    /// *within-incarnation* invariant; a crash is an incarnation boundary, and a
    /// reset from `Some(_)` to `None` across it is correct Raft behavior, not a
    /// regression. Without this reset the pre-crash high-water stays keyed under
    /// `(group, node)` and the post-restart `None` looks like a regression — a
    /// false [`PropertyId::CommitMonotonicity`] report.
    ///
    /// In contrast, **term and vote are persisted and monotonic**: a restarted
    /// node resumes at its persisted term and can never lead an *older* term, so
    /// Election Safety (10.1) still holds across the crash. This method therefore
    /// leaves `leaders` untouched — clearing it would discard real evidence and
    /// could hide a genuine same-term double leader.
    ///
    /// This does **not** weaken durability: acknowledged-record durability is
    /// checked separately ([`KafkaParityChecker`](crate::checker::kafka_parity::KafkaParityChecker))
    /// against the most-advanced *running* replica, so resetting a crashed node's
    /// commit high-water cannot mask a lost acknowledged record.
    ///
    /// Distinct from [`forget_group`](Self::forget_group), which clears *both*
    /// the leader and commit state because a deleted-then-recreated group is a
    /// brand-new group incarnation (its term resets to 0 as well). A crash keeps
    /// the same group and its persisted term, so only the volatile commit state
    /// is forgotten here.
    ///
    /// Mutates only this checker's `max_commit` map and is idempotent: calling it
    /// for a `node` with no recorded commit state is a no-op, and it leaves every
    /// other node's state — and all `leaders` state — intact.
    pub fn forget_node_commit(&mut self, node: &NodeId) {
        self.max_commit.retain(|(_group, n), _commit| n != node);
    }

    /// Election Safety (10.1): at most one leader per `(group, term)` across the
    /// whole run. Accumulates each observed leader and flags the first
    /// same-term, different-node clash.
    fn check_election_safety(
        &mut self,
        obs: &[ReplicaObservation],
        now: VirtualInstant,
    ) -> Result<(), Violation> {
        for o in obs {
            if o.role != Role::Leader {
                continue;
            }
            let key = (o.group.clone(), o.term);
            match self.leaders.get(&key) {
                Some(existing) if existing != &o.node => {
                    return Err(Violation::new(
                        PropertyId::ElectionSafety,
                        now,
                        format!(
                            "group {:?} had two leaders in term {}: {:?} and {:?}",
                            o.group, o.term, existing, o.node
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    self.leaders.insert(key, o.node.clone());
                }
            }
        }
        Ok(())
    }

    /// Commit monotonicity (10.5): a replica's commit index never decreases
    /// *within a Raft incarnation*. Compares each observation against the
    /// per-replica high-water mark, but treats an observation at a strictly
    /// lower term as a fresh incarnation (see [`CommitHighWater`]) whose volatile
    /// commit index legitimately resets, rather than a regression.
    fn check_commit_monotonicity(
        &mut self,
        obs: &[ReplicaObservation],
        now: VirtualInstant,
    ) -> Result<(), Violation> {
        for o in obs {
            let key = (o.group.clone(), o.node.clone());
            // A strictly lower term is a *new* Raft incarnation of this replica
            // (term is monotonic within an incarnation): a crash + durable
            // recovery or a delete-then-recreate re-spawn rebuilds the replica,
            // and its volatile commit index restarts. The reset is correct, so
            // the boundary is not compared.
            let new_incarnation = self
                .max_commit
                .get(&key)
                .is_some_and(|prev| o.term < prev.term);
            if !new_incarnation {
                if let Some(prev) = self.max_commit.get(&key) {
                    // Same or advanced incarnation: enforce monotonicity.
                    // `Option<u64>` orders `None < Some(_)` and `Some(a) <
                    // Some(b)` for `a < b`, exactly the commit-index order we
                    // need. A same-incarnation regression is never masked: the
                    // term cannot have decreased without a new incarnation.
                    if o.commit_index < prev.commit {
                        return Err(Violation::new(
                            PropertyId::CommitMonotonicity,
                            now,
                            format!(
                                "replica {:?} of group {:?} commit index regressed from {:?} to {:?}",
                                o.node, o.group, prev.commit, o.commit_index
                            ),
                        ));
                    }
                }
            }
            // Record the high-water. Within an incarnation commit is monotonic
            // by the check above, so the latest observation is the high-water;
            // across a reset the new incarnation's (lower) term and its commit
            // become the high-water.
            self.max_commit.insert(
                key,
                CommitHighWater {
                    term: o.term,
                    commit: o.commit_index,
                },
            );
        }
        Ok(())
    }
}

/// Build the committed view (`group -> index -> committed entry`) from a
/// snapshot, checking **State Machine Safety (10.4)** as it goes.
///
/// For every replica it folds each *committed* entry (index `<=` the replica's
/// commit index) into the view: the first replica fixes the fingerprint, and any
/// later replica that holds a different committed entry at the same index is a
/// State Machine Safety breach. The retained `commit_term` is the minimum
/// `current_term` among the replicas holding the index committed (see
/// [`CommittedEntry`]).
fn build_committed_view(
    obs: &[ReplicaObservation],
    now: VirtualInstant,
) -> Result<HashMap<GroupKey, BTreeMap<u64, CommittedEntry>>, Violation> {
    let mut view: HashMap<GroupKey, BTreeMap<u64, CommittedEntry>> = HashMap::new();
    for o in obs {
        let Some(commit) = o.commit_index else {
            continue;
        };
        let group_view = view.entry(o.group.clone()).or_default();
        for entry in &o.log {
            if entry.index > commit {
                break; // log is ascending; nothing past commit is committed
            }
            let fingerprint = Fingerprint::of(entry);
            match group_view.get_mut(&entry.index) {
                Some(existing) => {
                    if existing.fingerprint != fingerprint {
                        return Err(Violation::new(
                            PropertyId::StateMachineSafety,
                            now,
                            format!(
                                "group {:?} index {}: replica {:?} applied an entry (term {}) \
                                 differing from another replica's (term {})",
                                o.group,
                                entry.index,
                                o.node,
                                fingerprint.term,
                                existing.fingerprint.term
                            ),
                        ));
                    }
                    existing.commit_term = existing.commit_term.min(o.term);
                }
                None => {
                    group_view.insert(
                        entry.index,
                        CommittedEntry {
                            fingerprint,
                            commit_term: o.term,
                        },
                    );
                }
            }
        }
    }
    Ok(view)
}

/// Log Matching (10.2): for any two replicas of a group, if their logs hold an
/// entry at the same index with the same term, the logs are identical in every
/// entry up to and including that index.
///
/// For each unordered pair of replicas in a group it finds the first index at
/// which their overlapping entries diverge; a breach is any common index *at or
/// after* that divergence whose terms nonetheless agree (an agreeing term there
/// would require identical prefixes, contradicting the divergence — the
/// index-equal-term case is itself two different entries sharing `(index,
/// term)`).
fn check_log_matching(obs: &[ReplicaObservation], now: VirtualInstant) -> Result<(), Violation> {
    let mut by_group: HashMap<&GroupKey, Vec<&ReplicaObservation>> = HashMap::new();
    for o in obs {
        by_group.entry(&o.group).or_default().push(o);
    }
    for (group, replicas) in by_group {
        for i in 0..replicas.len() {
            for j in (i + 1)..replicas.len() {
                if let Some(v) = log_matching_pair(group, replicas[i], replicas[j], now) {
                    return Err(v);
                }
            }
        }
    }
    Ok(())
}

/// Check Log Matching for one pair of replicas; see [`check_log_matching`].
fn log_matching_pair(
    group: &GroupKey,
    a: &ReplicaObservation,
    b: &ReplicaObservation,
    now: VirtualInstant,
) -> Option<Violation> {
    let map_a = a.index_map();
    let map_b = b.index_map();
    // Indices present in both, ascending (BTreeMap keys iterate in order).
    let common: Vec<u64> = map_a
        .keys()
        .filter(|k| map_b.contains_key(k))
        .copied()
        .collect();

    // First index at which the overlapping entries differ.
    let Some(divergence) = common.iter().copied().find(|idx| map_a[idx] != map_b[idx]) else {
        return None; // identical over the overlap: Log Matching holds
    };

    // Any common index at or after the divergence whose terms agree breaks the
    // property (equal `(index, term)` demands identical prefixes).
    for idx in common.iter().copied().filter(|idx| *idx >= divergence) {
        if map_a[&idx].term == map_b[&idx].term {
            return Some(Violation::new(
                PropertyId::LogMatching,
                now,
                format!(
                    "group {:?}: replicas {:?} and {:?} share term {} at index {} but their logs \
                     diverge at index {}",
                    group, a.node, b.node, map_a[&idx].term, idx, divergence
                ),
            ));
        }
    }
    None
}

/// Leader Completeness (10.3): every entry committed in a given term is present,
/// at the same index, in the log of every replica that is leader in a later
/// term.
///
/// For each replica currently in [`Role::Leader`] with term `L`, every committed
/// entry whose `commit_term < L` must appear unchanged at its index in that
/// leader's log; a missing or differing entry is a breach.
fn check_leader_completeness(
    obs: &[ReplicaObservation],
    committed: &HashMap<GroupKey, BTreeMap<u64, CommittedEntry>>,
    now: VirtualInstant,
) -> Result<(), Violation> {
    for o in obs {
        if o.role != Role::Leader {
            continue;
        }
        let Some(group_committed) = committed.get(&o.group) else {
            continue;
        };
        let leader_log = o.index_map();
        for (idx, entry) in group_committed {
            // Only entries committed strictly before this leader's term are
            // constrained to be present in it (Raft §5.4).
            if entry.commit_term >= o.term {
                continue;
            }
            let present = leader_log
                .get(idx)
                .is_some_and(|fp| *fp == entry.fingerprint);
            if !present {
                return Err(Violation::new(
                    PropertyId::LeaderCompleteness,
                    now,
                    format!(
                        "leader {:?} of group {:?} in term {} is missing the entry committed at \
                         index {} in term {}",
                        o.node, o.group, o.term, idx, entry.commit_term
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vela_core::PartitionIndex;
    use vela_log::PayloadKind;

    use crate::cluster::SimulatedCluster;
    use crate::scenario::RunConfig;

    const T0: VirtualInstant = VirtualInstant::ORIGIN;

    fn at(nanos: u64) -> VirtualInstant {
        VirtualInstant::from_nanos(nanos)
    }

    fn group(topic: &str, partition: u32) -> GroupKey {
        (topic.to_string(), PartitionIndex(partition))
    }

    fn node(id: &str) -> NodeId {
        NodeId::new(id.to_string())
    }

    /// A log entry whose payload byte `tag` distinguishes "different entries"
    /// that happen to share an index and term.
    fn entry(index: u64, term: u64, tag: u8) -> LogEntry {
        LogEntry {
            index,
            term,
            payload: EntryPayload::new(PayloadKind::Record, vec![tag]),
        }
    }

    /// Build an observation. `log` is the full log in ascending index order; the
    /// payload `tag` of each entry defaults to its term so equal-term entries are
    /// equal unless a test sets distinct tags.
    fn obs(
        group: GroupKey,
        node: NodeId,
        role: Role,
        term: u64,
        commit_index: Option<u64>,
        log: Vec<LogEntry>,
    ) -> ReplicaObservation {
        ReplicaObservation {
            group,
            node,
            role,
            term,
            commit_index,
            log,
        }
    }

    /// A simple identical-term log `0..len`, each entry tagged by its index.
    fn linear_log(term: u64, len: u64) -> Vec<LogEntry> {
        (0..len).map(|i| entry(i, term, i as u8)).collect()
    }

    // ----- Election Safety (10.1) -----------------------------------------

    #[test]
    fn one_leader_per_term_passes() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let observations = vec![
            obs(g.clone(), node("node-0"), Role::Leader, 3, Some(0), vec![]),
            obs(
                g.clone(),
                node("node-1"),
                Role::Follower,
                3,
                Some(0),
                vec![],
            ),
            obs(g, node("node-2"), Role::Follower, 3, Some(0), vec![]),
        ];
        assert!(checker.check_election_safety(&observations, T0).is_ok());
    }

    #[test]
    fn same_node_re_observed_as_leader_is_not_a_violation() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let leader = obs(g, node("node-0"), Role::Leader, 5, Some(0), vec![]);
        assert!(checker
            .check_election_safety(std::slice::from_ref(&leader), at(1))
            .is_ok());
        // Same leader, same term, a later step: still fine.
        assert!(checker.check_election_safety(&[leader], at(2)).is_ok());
    }

    #[test]
    fn two_leaders_same_term_is_flagged_with_property_and_instant() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        // First step: node-0 leads term 4.
        assert!(checker
            .check_election_safety(
                &[obs(
                    g.clone(),
                    node("node-0"),
                    Role::Leader,
                    4,
                    Some(0),
                    vec![]
                )],
                at(10),
            )
            .is_ok());
        // Later step: node-1 also claims leadership of term 4 — a violation,
        // even though node-0 has since moved on.
        let err = checker
            .check_election_safety(
                &[obs(g, node("node-1"), Role::Leader, 4, Some(0), vec![])],
                at(20),
            )
            .expect_err("a same-term double leader must be flagged");
        assert_eq!(err.property, PropertyId::ElectionSafety);
        assert_eq!(err.at, at(20));
    }

    #[test]
    fn same_node_two_different_terms_leader_is_fine() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        assert!(checker
            .check_election_safety(
                &[obs(
                    g.clone(),
                    node("node-0"),
                    Role::Leader,
                    1,
                    Some(0),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
        // A different term may legitimately have a different (or the same) leader.
        assert!(checker
            .check_election_safety(
                &[obs(g, node("node-1"), Role::Leader, 2, Some(0), vec![])],
                at(2),
            )
            .is_ok());
    }

    // ----- Commit monotonicity (10.5) -------------------------------------

    #[test]
    fn commit_index_advancing_passes() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");
        for (t, commit) in [(1, None), (2, Some(0)), (3, Some(2)), (4, Some(2))] {
            assert!(checker
                .check_commit_monotonicity(
                    &[obs(g.clone(), n.clone(), Role::Follower, 1, commit, vec![])],
                    at(t),
                )
                .is_ok());
        }
    }

    #[test]
    fn commit_index_regression_is_flagged() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");
        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    n.clone(),
                    Role::Follower,
                    1,
                    Some(5),
                    vec![]
                )],
                at(10),
            )
            .is_ok());
        let err = checker
            .check_commit_monotonicity(&[obs(g, n, Role::Follower, 1, Some(3), vec![])], at(11))
            .expect_err("a commit-index regression must be flagged");
        assert_eq!(err.property, PropertyId::CommitMonotonicity);
        assert_eq!(err.at, at(11));
    }

    #[test]
    fn commit_index_regressing_to_none_is_flagged() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");
        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    n.clone(),
                    Role::Follower,
                    1,
                    Some(0),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
        let err = checker
            .check_commit_monotonicity(&[obs(g, n, Role::Follower, 1, None, vec![])], at(2))
            .expect_err("Some -> None is a regression");
        assert_eq!(err.property, PropertyId::CommitMonotonicity);
    }

    #[test]
    fn lower_term_observation_is_a_fresh_incarnation_and_resets_commit() {
        // A strictly lower term is a new Raft incarnation (term is monotonic
        // within an incarnation): e.g. a recovered replica (term 1, commit
        // Some(0)) whose group is deleted and recreated comes back as a fresh
        // `with_log` re-spawn at term 0 / commit `None`. The reset is correct,
        // not a regression — and the new incarnation then re-advances.
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");

        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    n.clone(),
                    Role::Follower,
                    1,
                    Some(0),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
        // Fresh incarnation at term 0: commit resets to None, no violation.
        assert!(checker
            .check_commit_monotonicity(
                &[obs(g.clone(), n.clone(), Role::Follower, 0, None, vec![])],
                at(2),
            )
            .is_ok());
        // The new incarnation then re-advances from its own baseline.
        assert!(checker
            .check_commit_monotonicity(&[obs(g, n, Role::Follower, 0, Some(0), vec![])], at(3),)
            .is_ok());
    }

    #[test]
    fn regression_within_the_new_incarnation_is_still_flagged() {
        // After an incarnation reset, commit monotonicity is enforced afresh
        // from the new incarnation's baseline — a regression *within* it (same
        // or higher term, commit decreasing) is a real breach.
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");

        // Old incarnation: term 2, commit Some(5).
        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    n.clone(),
                    Role::Follower,
                    2,
                    Some(5),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
        // Fresh incarnation at term 1, re-advances to Some(3).
        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    n.clone(),
                    Role::Follower,
                    1,
                    Some(3),
                    vec![]
                )],
                at(2),
            )
            .is_ok());
        // Within that incarnation (same term), a drop to Some(1) is a real
        // regression and must be flagged — the reset must not weaken detection.
        let err = checker
            .check_commit_monotonicity(&[obs(g, n, Role::Follower, 1, Some(1), vec![])], at(3))
            .expect_err("a same-incarnation regression must still be flagged");
        assert_eq!(err.property, PropertyId::CommitMonotonicity);
    }

    #[test]
    fn higher_term_commit_regression_is_flagged_as_same_incarnation() {
        // A commit decrease at the *same or a higher* term is the same (or an
        // advanced) incarnation — never a fresh one — so it is a real breach.
        // Only a strictly lower term marks a new incarnation.
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");

        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    n.clone(),
                    Role::Follower,
                    2,
                    Some(5),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
        let err = checker
            .check_commit_monotonicity(&[obs(g, n, Role::Follower, 3, Some(2), vec![])], at(2))
            .expect_err("a commit regression at a higher term is a same-incarnation breach");
        assert_eq!(err.property, PropertyId::CommitMonotonicity);
    }

    // ----- forget_group: delete -> recreate incarnations ------------------

    #[test]
    fn forget_group_clears_state_so_a_recreated_incarnation_does_not_flag() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");

        // Original incarnation: node-0 leads term 1 and commits index 0.
        assert!(checker
            .check_election_safety(
                &[obs(g.clone(), n.clone(), Role::Leader, 1, Some(0), vec![])],
                at(1),
            )
            .is_ok());
        assert!(checker
            .check_commit_monotonicity(
                &[obs(g.clone(), n.clone(), Role::Leader, 1, Some(0), vec![])],
                at(1),
            )
            .is_ok());

        // The topic is deleted: reconcile drops the group, so forget its state.
        checker.forget_group(&g);

        // The re-created incarnation is a fresh RaftNode: a *different* node may
        // be the term-1 leader and it restarts at commit `None`. With the old
        // state forgotten, neither is a violation.
        assert!(checker
            .check_election_safety(
                &[obs(
                    g.clone(),
                    node("node-1"),
                    Role::Leader,
                    1,
                    None,
                    vec![]
                )],
                at(2),
            )
            .is_ok());
        assert!(checker
            .check_commit_monotonicity(&[obs(g, n, Role::Follower, 1, None, vec![])], at(2),)
            .is_ok());
    }

    #[test]
    fn forget_group_is_scoped_and_leaves_other_groups_intact() {
        let mut checker = RaftSafetyChecker::new();
        let g0 = group("orders", 0);
        let g1 = group("orders", 1);

        // node-0 leads term 1 of both partition groups.
        assert!(checker
            .check_election_safety(
                &[
                    obs(g0.clone(), node("node-0"), Role::Leader, 1, Some(0), vec![]),
                    obs(g1.clone(), node("node-0"), Role::Leader, 1, Some(0), vec![]),
                ],
                at(1),
            )
            .is_ok());

        // Forget only g0; g1's accumulated state must survive.
        checker.forget_group(&g0);

        // g1 still remembers node-0 as its term-1 leader, so a *different* node
        // claiming term-1 leadership of g1 is still a real Election Safety breach
        // — forget_group must not weaken detection for groups it did not clear.
        let err = checker
            .check_election_safety(
                &[obs(g1, node("node-1"), Role::Leader, 1, Some(0), vec![])],
                at(2),
            )
            .expect_err("an un-forgotten group must still flag a same-term double leader");
        assert_eq!(err.property, PropertyId::ElectionSafety);
    }

    #[test]
    fn forget_group_on_an_unknown_group_is_a_noop() {
        let mut checker = RaftSafetyChecker::new();
        // Forgetting before anything was recorded is harmless.
        checker.forget_group(&group("never-seen", 0));
        // A subsequent observation still records and checks normally.
        assert!(checker
            .check_election_safety(
                &[obs(
                    group("orders", 0),
                    node("node-0"),
                    Role::Leader,
                    1,
                    Some(0),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
    }

    // ----- forget_node_commit: crash -> restart incarnations --------------

    #[test]
    fn forget_node_commit_lets_a_restarted_node_re_derive_commit_from_none() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let n = node("node-0");

        // Pre-crash incarnation: node-0 commits index 2 in this group.
        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    n.clone(),
                    Role::Follower,
                    3,
                    Some(2),
                    vec![]
                )],
                at(1),
            )
            .is_ok());

        // node-0 crashes: commit index is volatile per-incarnation state, so the
        // crash is an incarnation boundary for commit monotonicity.
        checker.forget_node_commit(&n);

        // The restarted replica re-derives its commit index from `None`, then
        // re-advances. Neither the reset to `None` nor a re-advance below the old
        // high-water is a regression.
        assert!(checker
            .check_commit_monotonicity(
                &[obs(g.clone(), n.clone(), Role::Follower, 3, None, vec![])],
                at(2),
            )
            .is_ok());
        assert!(checker
            .check_commit_monotonicity(&[obs(g, n, Role::Follower, 3, Some(1), vec![])], at(3),)
            .is_ok());
    }

    #[test]
    fn forget_node_commit_preserves_election_safety_state() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let crasher = node("node-0");

        // node-0 leads term 4 and commits index 0.
        assert!(checker
            .check_election_safety(
                &[obs(
                    g.clone(),
                    crasher.clone(),
                    Role::Leader,
                    4,
                    Some(0),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    g.clone(),
                    crasher.clone(),
                    Role::Leader,
                    4,
                    Some(0),
                    vec![]
                )],
                at(1),
            )
            .is_ok());

        // node-0 crashes: forget only its volatile commit state. Term is
        // persisted, so Election Safety must remain enforced across the crash.
        checker.forget_node_commit(&crasher);

        // A *different* node claiming leadership of the same group in the same
        // term is still a real Election Safety breach — the crash reset must not
        // clear `leaders`.
        let err = checker
            .check_election_safety(
                &[obs(g, node("node-1"), Role::Leader, 4, Some(0), vec![])],
                at(2),
            )
            .expect_err("forget_node_commit must not clear election-safety state");
        assert_eq!(err.property, PropertyId::ElectionSafety);
    }

    #[test]
    fn forget_node_commit_is_scoped_to_the_named_node() {
        let mut checker = RaftSafetyChecker::new();
        let g = group("orders", 0);
        let crasher = node("node-0");
        let survivor = node("node-1");

        // Both replicas commit index 5.
        for n in [&crasher, &survivor] {
            assert!(checker
                .check_commit_monotonicity(
                    &[obs(
                        g.clone(),
                        n.clone(),
                        Role::Follower,
                        1,
                        Some(5),
                        vec![]
                    )],
                    at(1),
                )
                .is_ok());
        }

        // Only node-0 crashes.
        checker.forget_node_commit(&crasher);

        // node-1 never crashed: its high-water survives, so a regression on it is
        // still flagged.
        let err = checker
            .check_commit_monotonicity(
                &[obs(g, survivor, Role::Follower, 1, Some(3), vec![])],
                at(2),
            )
            .expect_err("an un-crashed node's commit high-water must be preserved");
        assert_eq!(err.property, PropertyId::CommitMonotonicity);
    }

    #[test]
    fn forget_node_commit_on_an_unknown_node_is_a_noop() {
        let mut checker = RaftSafetyChecker::new();
        // Forgetting a node with no recorded commit state is harmless.
        checker.forget_node_commit(&node("never-seen"));
        // A subsequent observation still records and checks normally.
        assert!(checker
            .check_commit_monotonicity(
                &[obs(
                    group("orders", 0),
                    node("node-0"),
                    Role::Follower,
                    1,
                    Some(0),
                    vec![]
                )],
                at(1),
            )
            .is_ok());
    }

    #[test]
    fn identical_committed_logs_pass_state_machine_safety() {
        let g = group("orders", 0);
        let observations = vec![
            obs(
                g.clone(),
                node("node-0"),
                Role::Leader,
                2,
                Some(2),
                linear_log(2, 3),
            ),
            obs(
                g,
                node("node-1"),
                Role::Follower,
                2,
                Some(2),
                linear_log(2, 3),
            ),
        ];
        assert!(build_committed_view(&observations, T0).is_ok());
    }

    #[test]
    fn differing_committed_entry_is_flagged_as_state_machine_safety() {
        let g = group("orders", 0);
        // Both committed up to index 1, but their entry at index 1 differs.
        let log_a = vec![entry(0, 1, 0), entry(1, 1, 7)];
        let log_b = vec![entry(0, 1, 0), entry(1, 1, 9)];
        let observations = vec![
            obs(g.clone(), node("node-0"), Role::Leader, 1, Some(1), log_a),
            obs(g, node("node-1"), Role::Follower, 1, Some(1), log_b),
        ];
        let err = build_committed_view(&observations, at(42))
            .expect_err("a differing committed entry must be flagged");
        assert_eq!(err.property, PropertyId::StateMachineSafety);
        assert_eq!(err.at, at(42));
    }

    #[test]
    fn divergent_uncommitted_tails_do_not_trip_state_machine_safety() {
        let g = group("orders", 0);
        // Agree on committed prefix [0]; diverge only in the *uncommitted* tail.
        let log_a = vec![entry(0, 1, 0), entry(1, 2, 7)];
        let log_b = vec![entry(0, 1, 0), entry(1, 3, 9)];
        let observations = vec![
            obs(g.clone(), node("node-0"), Role::Follower, 2, Some(0), log_a),
            obs(g, node("node-1"), Role::Follower, 3, Some(0), log_b),
        ];
        assert!(build_committed_view(&observations, T0).is_ok());
    }

    // ----- Log Matching (10.2) --------------------------------------------

    #[test]
    fn identical_logs_pass_log_matching() {
        let g = group("orders", 0);
        let observations = vec![
            obs(
                g.clone(),
                node("node-0"),
                Role::Leader,
                2,
                Some(2),
                linear_log(2, 4),
            ),
            obs(
                g,
                node("node-1"),
                Role::Follower,
                2,
                Some(2),
                linear_log(2, 4),
            ),
        ];
        assert!(check_log_matching(&observations, T0).is_ok());
    }

    #[test]
    fn legitimate_conflicting_tail_passes_log_matching() {
        let g = group("orders", 0);
        // Shared prefix [0,1] (term 1); index 2 conflicts but with *different*
        // terms — a normal Raft conflict the leader will reconcile, not a breach.
        let log_a = vec![entry(0, 1, 0), entry(1, 1, 1), entry(2, 2, 7)];
        let log_b = vec![entry(0, 1, 0), entry(1, 1, 1), entry(2, 3, 9)];
        let observations = vec![
            obs(g.clone(), node("node-0"), Role::Follower, 2, Some(1), log_a),
            obs(g, node("node-1"), Role::Follower, 3, Some(1), log_b),
        ];
        assert!(check_log_matching(&observations, T0).is_ok());
    }

    #[test]
    fn same_index_term_but_different_payload_breaks_log_matching() {
        let g = group("orders", 0);
        // Index 1 shares (index=1, term=1) across both logs but the payloads
        // differ — two different entries with the same coordinates.
        let log_a = vec![entry(0, 1, 0), entry(1, 1, 7)];
        let log_b = vec![entry(0, 1, 0), entry(1, 1, 9)];
        let observations = vec![
            obs(g.clone(), node("node-0"), Role::Leader, 1, Some(0), log_a),
            obs(g, node("node-1"), Role::Follower, 1, Some(0), log_b),
        ];
        let err = check_log_matching(&observations, at(7))
            .expect_err("equal (index, term) with differing payloads is a Log Matching breach");
        assert_eq!(err.property, PropertyId::LogMatching);
        assert_eq!(err.at, at(7));
    }

    #[test]
    fn matching_term_above_an_earlier_divergence_breaks_log_matching() {
        let g = group("orders", 0);
        // Diverge at index 1 (different terms), but later agree on term at index
        // 2 — impossible in a correct Raft log, so a breach.
        let log_a = vec![entry(0, 1, 0), entry(1, 2, 1), entry(2, 5, 2)];
        let log_b = vec![entry(0, 1, 0), entry(1, 3, 1), entry(2, 5, 2)];
        let observations = vec![
            obs(g.clone(), node("node-0"), Role::Follower, 5, Some(0), log_a),
            obs(g, node("node-1"), Role::Follower, 5, Some(0), log_b),
        ];
        let err = check_log_matching(&observations, T0)
            .expect_err("an agreeing term above a divergence is a Log Matching breach");
        assert_eq!(err.property, PropertyId::LogMatching);
    }

    // ----- Leader Completeness (10.3) -------------------------------------

    #[test]
    fn leader_with_all_earlier_committed_entries_passes() {
        let g = group("orders", 0);
        // Entry at index 0 committed in term 1; the term-3 leader still has it.
        let committed = vec![obs(
            g.clone(),
            node("node-0"),
            Role::Follower,
            1,
            Some(0),
            vec![entry(0, 1, 0)],
        )];
        let view = build_committed_view(&committed, T0).unwrap();

        let leader = vec![obs(
            g,
            node("node-1"),
            Role::Leader,
            3,
            Some(1),
            vec![entry(0, 1, 0), entry(1, 3, 1)],
        )];
        assert!(check_leader_completeness(&leader, &view, T0).is_ok());
    }

    #[test]
    fn leader_missing_an_earlier_committed_entry_is_flagged() {
        let g = group("orders", 0);
        // Index 0 committed in term 1.
        let committed = vec![obs(
            g.clone(),
            node("node-0"),
            Role::Follower,
            1,
            Some(0),
            vec![entry(0, 1, 0)],
        )];
        let view = build_committed_view(&committed, T0).unwrap();

        // A later-term leader whose log lacks index 0 entirely — impossible under
        // the election restriction, so a Leader Completeness breach.
        let leader = vec![obs(g, node("node-2"), Role::Leader, 4, None, vec![])];
        let err = check_leader_completeness(&leader, &view, at(99))
            .expect_err("a later leader missing a committed entry must be flagged");
        assert_eq!(err.property, PropertyId::LeaderCompleteness);
        assert_eq!(err.at, at(99));
    }

    #[test]
    fn leader_in_the_commit_term_is_not_constrained() {
        let g = group("orders", 0);
        // Entry committed in term 2.
        let committed = vec![obs(
            g.clone(),
            node("node-0"),
            Role::Leader,
            2,
            Some(0),
            vec![entry(0, 2, 0)],
        )];
        let view = build_committed_view(&committed, T0).unwrap();
        // The leader's own term equals the commit term: Leader Completeness only
        // constrains *later* terms, so an (artificial) absence here is not a
        // breach of this property.
        let leader = vec![obs(g, node("node-1"), Role::Leader, 2, None, vec![])];
        assert!(check_leader_completeness(&leader, &view, T0).is_ok());
    }

    // ----- Cluster-backed smoke tests -------------------------------------

    #[test]
    fn fresh_cluster_observes_and_checks_clean() {
        // A freshly-assembled cluster has empty logs and no elected leaders, so
        // every safety property holds trivially; this exercises the live
        // collection path (fleet_replicas + meta_replica) end to end.
        let mut cluster =
            SimulatedCluster::new(RunConfig::default()).expect("default config builds a cluster");
        let mut checker = RaftSafetyChecker::new();
        assert!(checker.observe(&cluster, T0).is_ok());
        assert!(checker.check_logs(&cluster, T0).is_ok());
        // Repeated observation across steps stays clean.
        assert!(checker.observe(&cluster, at(1_000)).is_ok());
        // Touch `cluster` mutably to confirm the checker borrows read-only.
        let _ = cluster.clock_mut();
    }
}
