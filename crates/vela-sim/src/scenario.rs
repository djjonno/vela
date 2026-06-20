//! Scenario configuration: parameters, defaults, budgets, and validation.
//!
//! A [`Simulation_Run`] is configured by a [`RunConfig`] — a 64-bit seed plus a
//! set of [`ScenarioParameters`] describing the cluster shape, the fault
//! intensities, the workload size, and the per-run budget. Every field has a
//! documented default (see each type's [`Default`] impl), so a caller may
//! specify only the parameters it cares about and let the rest resolve to the
//! defaults (Requirement 15.4).
//!
//! Before a run starts, [`ScenarioParameters::validate`] rejects an internally
//! inconsistent set — a replication factor outside `1..=node_count`, or a
//! partition count below 1 — by returning a typed [`ScenarioError`] rather than
//! panicking or executing an invalid run. A replication factor *equal to* the
//! node count is accepted (Requirement 15.5).
//!
//! [`Simulation_Run`]: crate

use thiserror::Error;

/// The number of [`Sim_Node`]s in the default cluster.
///
/// Three is the smallest cluster whose per-partition Raft group can tolerate the
/// failure of a minority (one) of its replicas (Requirement 15.2).
///
/// [`Sim_Node`]: crate
pub const DEFAULT_NODE_COUNT: usize = 3;

/// The default replication factor.
///
/// Defaults to [`DEFAULT_NODE_COUNT`], i.e. every node replicates every
/// partition. A replication factor equal to the node count is a valid set
/// (Requirement 15.5).
pub const DEFAULT_REPLICATION_FACTOR: usize = DEFAULT_NODE_COUNT;

/// The default number of partitions per topic.
pub const DEFAULT_PARTITION_COUNT: u32 = 4;

/// The default number of client operations generated for a run.
pub const DEFAULT_WORKLOAD_SIZE: usize = 100;

/// The default per-run event budget (see [`Budget::max_events`]).
///
/// Matches the `VELA_DST_MAX_EVENTS` budget the CI workflow sets, so a run built
/// from defaults is bounded the same way locally and in CI.
pub const DEFAULT_MAX_EVENTS: u64 = 200_000;

/// The default per-run virtual-time budget in logical nanoseconds
/// (see [`Budget::max_virtual_nanos`]). Sixty seconds of logical time.
pub const DEFAULT_MAX_VIRTUAL_NANOS: u64 = 60_000_000_000;

/// The default base one-way network latency in logical nanoseconds
/// (see [`FaultIntensities::base_latency_nanos`]). One logical millisecond.
pub const DEFAULT_BASE_LATENCY_NANOS: u64 = 1_000_000;

/// The default bound on the extra reorder delay in logical nanoseconds
/// (see [`FaultIntensities::max_reorder_nanos`]). Five logical milliseconds.
///
/// This is only an upper bound on the perturbation applied *when reordering is
/// enabled*; with the default [`FaultIntensities::reorder_prob`] of `0.0` no
/// reorder delay is ever applied.
pub const DEFAULT_MAX_REORDER_NANOS: u64 = 5_000_000;

/// Per-fault-class intensity knobs for a run.
///
/// Each field is a deterministic input to the seed-derived [`Fault_Schedule`]
/// and the per-message / per-operation fault decisions; nothing here is random
/// itself. Probabilities are expressed in `0.0..=1.0`.
///
/// The [`Default`] models a **healthy cluster**: every probability and skew
/// bound is `0.0`, so no drop, duplication, reorder, partition, crash, clock
/// skew, or storage fault is injected unless explicitly configured. The one
/// non-zero default is [`base_latency_nanos`](Self::base_latency_nanos), because
/// every delivered message incurs the configured base one-way latency
/// (Requirement 5.1); a default cluster therefore still exercises real message
/// timing. Fault-heavy configurations are provided by named scenario presets
/// rather than by the defaults.
///
/// # Default values
///
/// | Field                  | Default                          |
/// |------------------------|----------------------------------|
/// | `base_latency_nanos`   | [`DEFAULT_BASE_LATENCY_NANOS`] (1 ms) |
/// | `reorder_prob`         | `0.0`                            |
/// | `max_reorder_nanos`    | [`DEFAULT_MAX_REORDER_NANOS`] (5 ms) |
/// | `drop_prob`            | `0.0`                            |
/// | `duplicate_prob`       | `0.0`                            |
/// | `partition_prob`       | `0.0`                            |
/// | `crash_prob`           | `0.0`                            |
/// | `max_clock_skew_nanos` | `0`                              |
/// | `max_clock_skew_rate`  | `0.0`                            |
/// | `torn_write_prob`      | `0.0`                            |
/// | `io_error_prob`        | `0.0`                            |
///
/// [`Fault_Schedule`]: crate
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FaultIntensities {
    /// Base one-way message latency, in logical nanoseconds, added to *every*
    /// delivered message (Requirement 5.1). Default
    /// [`DEFAULT_BASE_LATENCY_NANOS`].
    pub base_latency_nanos: u64,
    /// Probability (`0.0..=1.0`) that a delivered message receives an extra,
    /// bounded reorder delay so its delivery order can differ from its send
    /// order (Requirement 5.2). Default `0.0` (reordering disabled).
    pub reorder_prob: f64,
    /// Upper bound, in logical nanoseconds, on the extra delay applied when a
    /// message is selected for reordering. Default [`DEFAULT_MAX_REORDER_NANOS`].
    pub max_reorder_nanos: u64,
    /// Probability (`0.0..=1.0`) that a message is dropped and never delivered
    /// (Requirement 5.3). Default `0.0`.
    pub drop_prob: f64,
    /// Probability (`0.0..=1.0`) that a delivered message is duplicated with one
    /// extra copy (Requirement 5.4). Default `0.0`.
    pub duplicate_prob: f64,
    /// Relative likelihood (`0.0..=1.0`) that the fault schedule introduces a
    /// network partition between two sets of nodes (Requirements 5.5, 5.6).
    /// Default `0.0`.
    pub partition_prob: f64,
    /// Relative likelihood (`0.0..=1.0`) that the fault schedule crashes a node
    /// (later restarted) during the run (Requirement 6). Default `0.0`.
    pub crash_prob: f64,
    /// Bound, in logical nanoseconds, on the per-node clock-skew offset applied
    /// to a skewed node's view of time (Requirement 4.5). `0` disables offset
    /// skew. Default `0`.
    pub max_clock_skew_nanos: u64,
    /// Bound on the per-node clock-skew *rate* deviation from `1.0` (e.g. `0.05`
    /// permits a view rate in `0.95..=1.05`), bounded so it can never approach
    /// reading the wall clock (Requirement 4.5). `0.0` disables rate skew.
    /// Default `0.0`.
    pub max_clock_skew_rate: f64,
    /// Probability (`0.0..=1.0`) that a node's trailing write is torn at a crash,
    /// so recovery must discard the torn tail to the last intact record
    /// (Requirement 7.3). Default `0.0`.
    pub torn_write_prob: f64,
    /// Probability (`0.0..=1.0`) that a storage operation surfaces an I/O error
    /// through the `LogStorage` result type (Requirement 7.4). Default `0.0`.
    pub io_error_prob: f64,
}

impl Default for FaultIntensities {
    /// A healthy cluster: no injected faults, only the base one-way network
    /// latency. See the type-level table for every default value.
    fn default() -> Self {
        Self {
            base_latency_nanos: DEFAULT_BASE_LATENCY_NANOS,
            reorder_prob: 0.0,
            max_reorder_nanos: DEFAULT_MAX_REORDER_NANOS,
            drop_prob: 0.0,
            duplicate_prob: 0.0,
            partition_prob: 0.0,
            crash_prob: 0.0,
            max_clock_skew_nanos: 0,
            max_clock_skew_rate: 0.0,
            torn_write_prob: 0.0,
            io_error_prob: 0.0,
        }
    }
}

/// The bound that ends a run.
///
/// A run stops once *either* limit is reached, after processing the event that
/// reached it (Requirement 4.6). Virtual time is a logical `u64` of nanoseconds
/// (the same unit the scheduler's virtual clock uses), never derived from the
/// wall clock.
///
/// # Default values
///
/// | Field               | Default                                  |
/// |---------------------|------------------------------------------|
/// | `max_events`        | [`DEFAULT_MAX_EVENTS`] (`200_000`)        |
/// | `max_virtual_nanos` | [`DEFAULT_MAX_VIRTUAL_NANOS`] (60 s)      |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// Maximum number of discrete events processed before the run ends.
    /// Default [`DEFAULT_MAX_EVENTS`].
    pub max_events: u64,
    /// Maximum logical virtual time, in nanoseconds, the run may reach before it
    /// ends. Default [`DEFAULT_MAX_VIRTUAL_NANOS`].
    pub max_virtual_nanos: u64,
}

impl Default for Budget {
    /// See the type-level table for every default value.
    fn default() -> Self {
        Self {
            max_events: DEFAULT_MAX_EVENTS,
            max_virtual_nanos: DEFAULT_MAX_VIRTUAL_NANOS,
        }
    }
}

/// The non-random configuration of a [`Simulation_Run`]: cluster shape, fault
/// intensities, workload size, and budget (Requirement 15.1).
///
/// Construct with [`ScenarioParameters::default`] and override individual fields,
/// then call [`validate`](Self::validate) before handing the parameters to a run.
///
/// # Default values
///
/// | Field                | Default                                       |
/// |----------------------|-----------------------------------------------|
/// | `node_count`         | [`DEFAULT_NODE_COUNT`] (`3`)                   |
/// | `replication_factor` | [`DEFAULT_REPLICATION_FACTOR`] (`3`)           |
/// | `partition_count`    | [`DEFAULT_PARTITION_COUNT`] (`4`)              |
/// | `faults`             | [`FaultIntensities::default`]                  |
/// | `workload_size`      | [`DEFAULT_WORKLOAD_SIZE`] (`100`)              |
/// | `budget`             | [`Budget::default`]                            |
///
/// The default set is valid: replication factor (`3`) equals the node count
/// (`3`), which is accepted, and the partition count (`4`) is at least 1.
///
/// [`Simulation_Run`]: crate
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScenarioParameters {
    /// Number of [`Sim_Node`]s in the cluster. Default [`DEFAULT_NODE_COUNT`].
    ///
    /// [`Sim_Node`]: crate
    pub node_count: usize,
    /// Number of replicas per partition. Must lie in `1..=node_count`. Default
    /// [`DEFAULT_REPLICATION_FACTOR`].
    pub replication_factor: usize,
    /// Number of partitions per topic. Must be at least 1. Default
    /// [`DEFAULT_PARTITION_COUNT`].
    pub partition_count: u32,
    /// Per-fault-class intensities. Default [`FaultIntensities::default`].
    pub faults: FaultIntensities,
    /// Number of client operations generated for the run. Default
    /// [`DEFAULT_WORKLOAD_SIZE`].
    pub workload_size: usize,
    /// The per-run event / virtual-time budget. Default [`Budget::default`].
    pub budget: Budget,
}

impl Default for ScenarioParameters {
    /// See the type-level table for every default value.
    fn default() -> Self {
        Self {
            node_count: DEFAULT_NODE_COUNT,
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            partition_count: DEFAULT_PARTITION_COUNT,
            faults: FaultIntensities::default(),
            workload_size: DEFAULT_WORKLOAD_SIZE,
            budget: Budget::default(),
        }
    }
}

impl ScenarioParameters {
    /// Validate that the parameter set is internally consistent
    /// (Requirement 15.5).
    ///
    /// Returns `Ok(())` if and only if the replication factor lies in
    /// `1..=node_count` **and** the partition count is at least 1. A replication
    /// factor equal to the node count is accepted; a replication factor greater
    /// than the node count, a replication factor of zero, or a partition count
    /// below 1 are each rejected with the corresponding [`ScenarioError`].
    ///
    /// This never panics: an invalid set is reported as an `Err` so the caller
    /// can refuse to start an invalid run rather than failing mid-run.
    ///
    /// # Examples
    ///
    /// ```
    /// use vela_sim::scenario::{ScenarioError, ScenarioParameters};
    ///
    /// // The defaults (replication factor == node count) are valid.
    /// assert!(ScenarioParameters::default().validate().is_ok());
    ///
    /// // A replication factor greater than the node count is rejected.
    /// let bad = ScenarioParameters {
    ///     node_count: 3,
    ///     replication_factor: 4,
    ///     ..ScenarioParameters::default()
    /// };
    /// assert!(matches!(
    ///     bad.validate(),
    ///     Err(ScenarioError::ReplicationFactorTooHigh { .. })
    /// ));
    /// ```
    pub fn validate(&self) -> Result<(), ScenarioError> {
        if self.replication_factor == 0 {
            return Err(ScenarioError::ReplicationFactorZero);
        }
        if self.replication_factor > self.node_count {
            return Err(ScenarioError::ReplicationFactorTooHigh {
                replication_factor: self.replication_factor,
                node_count: self.node_count,
            });
        }
        if self.partition_count < 1 {
            return Err(ScenarioError::PartitionCountZero);
        }
        Ok(())
    }
}

/// Named scenario presets that target the coverage areas Requirement 15.3
/// enumerates: leader election / failover, log replication / follower catch-up,
/// network partition / heal, node crash / durable restart, and concurrent topic
/// administration.
///
/// Each preset is a [`ScenarioParameters`] tuned so that, paired with a workload
/// and a range of seeds, a run *exercises* the named behavior — it does not by
/// itself guarantee the behavior occurs on a single seed, but it shapes the
/// cluster and the [`FaultIntensities`] toward it (e.g. a non-zero
/// [`partition_prob`](FaultIntensities::partition_prob) for partition / heal, a
/// non-zero [`crash_prob`](FaultIntensities::crash_prob) for crash / restart).
///
/// Every preset uses a cluster of at least three nodes and a replication factor
/// of at least three (and never above the node count), per Requirement 15.2, so
/// that each partition's Raft group can tolerate the failure of a minority of
/// its Replica_Set. The presets override only the fields relevant to the
/// behavior they target; every other field resolves to its documented default
/// (Requirement 15.4).
///
/// The probabilities are deliberately moderate: high enough that the targeted
/// fault occurs across a seed range, low enough that the cluster still has
/// healed intervals in which it can make progress (so liveness can be checked).
/// The op mix of the [`generate`](crate::workload::generate)d workload is fixed
/// by the generator and is not a [`ScenarioParameters`] knob, so a preset shapes
/// admin-heavy or replication-heavy coverage through cluster shape, fault
/// intensities, and workload size rather than through an op weighting.
impl ScenarioParameters {
    /// A preset targeting **leader election and failover**.
    ///
    /// A five-node, fully-replicated cluster (every node replicates every
    /// partition, so a partition group tolerates a minority of two failures)
    /// with a non-zero [`crash_prob`](FaultIntensities::crash_prob) — so leaders
    /// are crashed (and later restarted) mid-run, forcing the surviving majority
    /// to elect a new leader — plus a small
    /// [`drop_prob`](FaultIntensities::drop_prob) so lost heartbeats can trip
    /// election timeouts even without a crash. Both together drive repeated
    /// elections and failovers while a majority remains to complete them.
    #[must_use]
    pub fn leader_failover() -> Self {
        Self {
            node_count: 5,
            replication_factor: 5,
            partition_count: 3,
            faults: FaultIntensities {
                crash_prob: 0.4,
                drop_prob: 0.05,
                ..FaultIntensities::default()
            },
            ..Self::default()
        }
    }

    /// A preset targeting **log replication and follower catch-up**.
    ///
    /// A three-node, fully-replicated cluster running a large workload so each
    /// partition's leader replicates a substantial log to its followers. A
    /// non-zero [`crash_prob`](FaultIntensities::crash_prob) crashes and later
    /// restarts a follower while produces continue; on restart the follower must
    /// catch up on the entries it missed via `AppendEntries`. The replication
    /// factor equals the node count, so a single crashed follower never costs the
    /// group its majority and the leader keeps committing throughout.
    #[must_use]
    pub fn log_replication_catch_up() -> Self {
        Self {
            node_count: 3,
            replication_factor: 3,
            partition_count: 2,
            faults: FaultIntensities {
                crash_prob: 0.25,
                ..FaultIntensities::default()
            },
            workload_size: 250,
            ..Self::default()
        }
    }

    /// A preset targeting **network partition and heal**.
    ///
    /// A five-node, fully-replicated cluster with a non-zero
    /// [`partition_prob`](FaultIntensities::partition_prob) so the fault schedule
    /// severs the cluster into two sides and later heals it. With five replicas a
    /// partition can leave a majority (three) on one side that keeps committing
    /// while the minority stalls; once the partition heals, the isolated replicas
    /// rejoin and reconcile to the leader's committed log. A small
    /// [`reorder_prob`](FaultIntensities::reorder_prob) perturbs delivery order
    /// across the heal boundary.
    #[must_use]
    pub fn network_partition_heal() -> Self {
        Self {
            node_count: 5,
            replication_factor: 5,
            partition_count: 3,
            faults: FaultIntensities {
                partition_prob: 0.4,
                reorder_prob: 0.05,
                ..FaultIntensities::default()
            },
            ..Self::default()
        }
    }

    /// A preset targeting **node crash and durable restart**.
    ///
    /// A three-node, fully-replicated cluster with a non-zero
    /// [`crash_prob`](FaultIntensities::crash_prob) and a produce-heavy workload,
    /// and *no* storage faults configured
    /// ([`torn_write_prob`](FaultIntensities::torn_write_prob) and
    /// [`io_error_prob`](FaultIntensities::io_error_prob) both zero) so the run
    /// exercises the durability boundary cleanly: a crash discards only un-fsynced
    /// state, a restart recovers term / vote / committed prefix / catalogue from
    /// the durable WAL, and every record acknowledged before a crash must survive
    /// it. The full replication factor keeps a majority running across a single
    /// crash so the cluster keeps acknowledging records to verify against.
    #[must_use]
    pub fn crash_durable_restart() -> Self {
        Self {
            node_count: 3,
            replication_factor: 3,
            partition_count: 2,
            faults: FaultIntensities {
                crash_prob: 0.35,
                ..FaultIntensities::default()
            },
            workload_size: 200,
            ..Self::default()
        }
    }

    /// A preset targeting **concurrent topic administration**.
    ///
    /// A five-node, fully-replicated cluster with a high partition count and a
    /// large workload, and no destructive faults configured, so the run drives
    /// many concurrent topic create / delete commands through the `__meta/0`
    /// metadata group and the metadata-commit-and-reconcile path that spawns and
    /// stops per-partition Raft groups across the assigned Replica_Sets. The high
    /// partition count maximises the number of partition replicas each committed
    /// `CreateTopic` reconciles into existence; the fault-free configuration keeps
    /// the metadata group's leader stable so admin throughput — not failover — is
    /// what is exercised.
    #[must_use]
    pub fn concurrent_topic_admin() -> Self {
        Self {
            node_count: 5,
            replication_factor: 5,
            partition_count: 8,
            workload_size: 200,
            ..Self::default()
        }
    }

    /// Every named coverage preset paired with a stable, behavior-identifying
    /// name (Requirement 15.3).
    ///
    /// Returned in a fixed order so callers — the per-preset integration tests
    /// (task 22.2) and any future drift guard — can iterate the full coverage set
    /// without hard-coding each constructor. The name identifies the targeted
    /// behavior and is suitable for a test label or an artifact tag.
    #[must_use]
    pub fn all_presets() -> Vec<(&'static str, ScenarioParameters)> {
        vec![
            ("leader_failover", Self::leader_failover()),
            ("log_replication_catch_up", Self::log_replication_catch_up()),
            ("network_partition_heal", Self::network_partition_heal()),
            ("crash_durable_restart", Self::crash_durable_restart()),
            ("concurrent_topic_admin", Self::concurrent_topic_admin()),
        ]
    }
}

/// A 64-bit seed plus the scenario parameters that together fully determine a
/// [`Simulation_Run`]'s outcome (Requirement 1).
///
/// The default config pairs seed `0` with [`ScenarioParameters::default`].
///
/// [`Simulation_Run`]: crate
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RunConfig {
    /// The 64-bit run seed from which every random decision is derived.
    pub seed: u64,
    /// The non-random scenario configuration.
    pub params: ScenarioParameters,
}

/// A typed error describing why a [`ScenarioParameters`] set was rejected by
/// [`ScenarioParameters::validate`] (Requirement 15.5).
///
/// Returned rather than panicked so an invalid set is refused before any run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ScenarioError {
    /// The replication factor exceeds the node count, so a partition could not
    /// be assigned a full replica set.
    #[error(
        "replication factor {replication_factor} exceeds node count {node_count} \
         (must be in 1..={node_count})"
    )]
    ReplicationFactorTooHigh {
        /// The configured replication factor.
        replication_factor: usize,
        /// The configured node count.
        node_count: usize,
    },
    /// The replication factor is zero, so a partition would have no replicas.
    #[error("replication factor must be at least 1")]
    ReplicationFactorZero,
    /// The partition count is below 1, so a topic would have no partitions.
    #[error("partition count must be at least 1")]
    PartitionCountZero,
}

#[cfg(test)]
mod tests {
    //! Unit tests asserting that every unspecified scenario parameter resolves
    //! to its documented default value (Requirement 15.4). Each assertion is
    //! exact so that a mutated constant or `Default` field is caught.

    use super::*;

    #[test]
    fn fault_intensities_default_is_healthy_cluster() {
        let faults = FaultIntensities::default();

        // The one non-zero default: every delivered message incurs the base
        // one-way latency (Requirement 5.1).
        assert_eq!(faults.base_latency_nanos, DEFAULT_BASE_LATENCY_NANOS);
        assert_eq!(faults.base_latency_nanos, 1_000_000);

        // The reorder bound is documented even though reordering is disabled.
        assert_eq!(faults.max_reorder_nanos, DEFAULT_MAX_REORDER_NANOS);
        assert_eq!(faults.max_reorder_nanos, 5_000_000);

        // Every fault probability / skew bound is zero: no fault is injected
        // unless explicitly configured.
        assert_eq!(faults.reorder_prob, 0.0);
        assert_eq!(faults.drop_prob, 0.0);
        assert_eq!(faults.duplicate_prob, 0.0);
        assert_eq!(faults.partition_prob, 0.0);
        assert_eq!(faults.crash_prob, 0.0);
        assert_eq!(faults.max_clock_skew_nanos, 0);
        assert_eq!(faults.max_clock_skew_rate, 0.0);
        assert_eq!(faults.torn_write_prob, 0.0);
        assert_eq!(faults.io_error_prob, 0.0);
    }

    #[test]
    fn budget_default_matches_documented_constants() {
        let budget = Budget::default();

        assert_eq!(budget.max_events, DEFAULT_MAX_EVENTS);
        assert_eq!(budget.max_events, 200_000);
        assert_eq!(budget.max_virtual_nanos, DEFAULT_MAX_VIRTUAL_NANOS);
        assert_eq!(budget.max_virtual_nanos, 60_000_000_000);
    }

    #[test]
    fn scenario_parameters_default_matches_documented_constants() {
        let params = ScenarioParameters::default();

        assert_eq!(params.node_count, DEFAULT_NODE_COUNT);
        assert_eq!(params.node_count, 3);
        assert_eq!(params.replication_factor, DEFAULT_REPLICATION_FACTOR);
        assert_eq!(params.replication_factor, 3);
        assert_eq!(params.partition_count, DEFAULT_PARTITION_COUNT);
        assert_eq!(params.partition_count, 4);
        assert_eq!(params.workload_size, DEFAULT_WORKLOAD_SIZE);
        assert_eq!(params.workload_size, 100);

        // Nested defaults delegate to the respective `Default` impls.
        assert_eq!(params.faults, FaultIntensities::default());
        assert_eq!(params.budget, Budget::default());
    }

    #[test]
    fn default_replication_factor_equals_default_node_count() {
        // The default set replicates every partition on every node; this is the
        // documented relationship between the two constants.
        assert_eq!(DEFAULT_REPLICATION_FACTOR, DEFAULT_NODE_COUNT);
    }

    #[test]
    fn scenario_parameters_default_is_valid() {
        // The documented defaults are a valid, internally-consistent set
        // (replication factor == node count is accepted, partition count >= 1).
        assert!(ScenarioParameters::default().validate().is_ok());
    }

    #[test]
    fn run_config_default_is_seed_zero_and_default_params() {
        let config = RunConfig::default();

        assert_eq!(config.seed, 0);
        assert_eq!(config.params, ScenarioParameters::default());
    }

    #[test]
    fn all_presets_are_valid_and_meet_minimum_cluster_shape() {
        // Requirement 15.2 / 15.3: every named coverage preset validates and
        // uses a cluster size and replication factor of at least three, with the
        // replication factor never exceeding the node count.
        let presets = ScenarioParameters::all_presets();
        assert!(!presets.is_empty(), "expected at least one preset");

        for (name, params) in &presets {
            assert!(
                params.validate().is_ok(),
                "preset `{name}` failed validation: {:?}",
                params.validate()
            );
            assert!(
                params.node_count >= 3,
                "preset `{name}` node_count {} < 3",
                params.node_count
            );
            assert!(
                params.replication_factor >= 3,
                "preset `{name}` replication_factor {} < 3",
                params.replication_factor
            );
            assert!(
                params.replication_factor <= params.node_count,
                "preset `{name}` replication_factor {} > node_count {}",
                params.replication_factor,
                params.node_count
            );
        }
    }

    #[test]
    fn all_presets_cover_the_required_behaviors() {
        // Requirement 15.3: the suite includes scenarios exercising leader
        // election / failover, log replication / follower catch-up, network
        // partition / heal, node crash / durable restart, and concurrent topic
        // administration. Assert the named set is exactly these five behaviors.
        let names: Vec<&str> = ScenarioParameters::all_presets()
            .into_iter()
            .map(|(name, _)| name)
            .collect();

        assert_eq!(
            names,
            vec![
                "leader_failover",
                "log_replication_catch_up",
                "network_partition_heal",
                "crash_durable_restart",
                "concurrent_topic_admin",
            ]
        );
    }

    #[test]
    fn presets_arm_the_fault_their_behavior_targets() {
        // Each preset must actually configure the fault that drives its targeted
        // behavior, so the scenario exercises that path rather than a healthy
        // cluster (Requirement 15.3).

        // Failover needs leaders to be lost: crashes (and dropped heartbeats).
        let failover = ScenarioParameters::leader_failover();
        assert!(failover.faults.crash_prob > 0.0);

        // Catch-up needs a follower to crash and restart behind a moving log.
        let catch_up = ScenarioParameters::log_replication_catch_up();
        assert!(catch_up.faults.crash_prob > 0.0);
        assert!(catch_up.workload_size > 0);

        // Partition / heal needs network partitions to be scheduled.
        let partition = ScenarioParameters::network_partition_heal();
        assert!(partition.faults.partition_prob > 0.0);

        // Crash / durable restart needs crashes but no storage faults, so the
        // durability boundary is exercised cleanly.
        let crash = ScenarioParameters::crash_durable_restart();
        assert!(crash.faults.crash_prob > 0.0);
        assert_eq!(crash.faults.torn_write_prob, 0.0);
        assert_eq!(crash.faults.io_error_prob, 0.0);

        // Concurrent admin keeps the cluster fault-free so admin throughput, not
        // failover, is exercised, and uses many partitions to reconcile.
        let admin = ScenarioParameters::concurrent_topic_admin();
        assert_eq!(admin.faults, FaultIntensities::default());
        assert!(admin.partition_count >= DEFAULT_PARTITION_COUNT);
    }
}
