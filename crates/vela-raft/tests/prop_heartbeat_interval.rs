//! Property test for the leader heartbeat interval versus the election timeout.
//!
//! Feature: vela-streaming-platform, Property 27
//!
//! **Property 27: A leader heartbeats faster than the minimum election timeout**
//! — for any node that is leader, heartbeat AppendEntries RPCs are emitted at
//! the fixed 50 ms interval, which is strictly shorter than the 150 ms minimum
//! election timeout, so a stable leader prevents follower timeouts. The
//! simulation clock realizes Raft's randomized election timeout by spreading
//! the firing across `[base, 2*base)` = `[150 ms, 300 ms)`; across many seeds
//! the realized election timeout always strictly exceeds the heartbeat
//! interval.
//!
//! Validates: Requirements 7.6

use std::time::Duration;

use proptest::prelude::*;
use vela_raft::sim::{SimCluster, StepOutcome};
use vela_raft::{NodeId, TimerKind, ELECTION_TIMEOUT_BASE, HEARTBEAT_INTERVAL};

/// The minimum and maximum bounds of the realized election-timeout window. A
/// node arms its election timer with [`ELECTION_TIMEOUT_BASE`] (150 ms) and the
/// clock spreads the firing over `[base, 2*base)` = `[150 ms, 300 ms)`
/// (Requirement 7.2).
const MIN_ELECTION_TIMEOUT: Duration = ELECTION_TIMEOUT_BASE;
const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(300);

/// Generate a `(seed, node_count)` pair. Varying the seed exercises the whole
/// randomized election-timeout window; varying the node count confirms the
/// timing is independent of group size.
fn cluster_strategy() -> impl Strategy<Value = (u64, u64)> {
    (any::<u64>(), 1u64..=5)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: vela-streaming-platform, Property 27
    #[test]
    fn leader_heartbeats_faster_than_minimum_election_timeout((seed, node_count) in cluster_strategy()) {
        // The fixed heartbeat interval is, by construction, strictly shorter
        // than the minimum election timeout (50 ms < 150 ms). This holds
        // regardless of the random seed (Requirement 7.6).
        prop_assert!(
            HEARTBEAT_INTERVAL < MIN_ELECTION_TIMEOUT,
            "heartbeat interval {:?} is not strictly less than the minimum election timeout {:?}",
            HEARTBEAT_INTERVAL,
            MIN_ELECTION_TIMEOUT,
        );

        // Arm an election timer on a follower and step the simulation to fire
        // it; the realized timeout is the logical time that elapses.
        let mut sim = SimCluster::new(node_count, seed);
        let start = sim.now();
        sim.arm(NodeId(0), TimerKind::Election, ELECTION_TIMEOUT_BASE);

        let outcome = sim.step();
        match outcome {
            StepOutcome::Timer { node, kind, .. } => {
                prop_assert_eq!(node, NodeId(0));
                prop_assert_eq!(kind, TimerKind::Election);
            }
            other => prop_assert!(false, "expected an election timer firing, got {:?}", other),
        }

        // The realized election timeout always lands in the randomized window
        // [150 ms, 300 ms) and therefore strictly exceeds the 50 ms heartbeat
        // interval, so a heartbeating leader resets follower timers well before
        // they can elapse (Requirement 7.6).
        let realized = sim.now().duration_since(start);
        prop_assert!(
            realized >= MIN_ELECTION_TIMEOUT,
            "realized election timeout {:?} is below the minimum {:?}",
            realized,
            MIN_ELECTION_TIMEOUT,
        );
        prop_assert!(
            realized < MAX_ELECTION_TIMEOUT,
            "realized election timeout {:?} reached or exceeded the maximum {:?}",
            realized,
            MAX_ELECTION_TIMEOUT,
        );
        prop_assert!(
            realized > HEARTBEAT_INTERVAL,
            "realized election timeout {:?} did not exceed the heartbeat interval {:?}",
            realized,
            HEARTBEAT_INTERVAL,
        );
    }
}
