//! Property test for the pure leader-resolution fold in `vela-client`.
//!
//! Feature: ctl-client-routing-and-repl, Property 7: Leader resolution fold —
//! `resolve_leader` returns the first named leader; else `NoLeaderElected` if
//! any reachable-but-leaderless; else `AllFailed`, keeping a reachable-but-
//! leaderless partition distinct from an all-failed/transport outcome.
//!
//! [`resolve_leader`] folds the per-node [`LeaderProbe`]s gathered across the
//! configured nodes (in order) into a single [`LeaderResolution`]. It is the
//! pure decision at the heart of [`ClientCore::refresh_leader`], so it runs
//! without a live server: this test feeds it arbitrary probe sequences and pins
//! the three-way outcome.
//!
//! The two requirements it realizes:
//!
//! - Requirement 2.2 — "WHEN resolving a partition's leader, THE Client_Core
//!   SHALL accept a leader named by any reachable replica that knows the
//!   partition, regardless of which Endpoint was listed first." Folding to the
//!   *first* `Leader(x)` in probe order — whatever its position — is exactly
//!   accepting a leader named by any reachable replica.
//! - Requirement 2.3 — "IF every reachable node reports no elected leader for
//!   the partition, THEN THE Client_Core SHALL return a partition-unavailable
//!   result distinct from a transport failure." A run with at least one
//!   `NoLeader` and no `Leader` folds to `NoLeaderElected`; a run of only
//!   `Failed` (or the empty run) folds to `AllFailed`. The two never collapse
//!   into one, so reachable-but-leaderless stays distinct from transport
//!   failure.
//!
//! Validates: Requirements 2.2, 2.3

use proptest::prelude::*;

use vela_client::{resolve_leader, LeaderProbe, LeaderResolution};

/// Generate one arbitrary probe outcome. `Leader` carries a short node id so a
/// `Found(x)` can be matched back to the exact probe that produced it.
fn probe_strategy() -> impl Strategy<Value = LeaderProbe> {
    prop_oneof![
        proptest::string::string_regex("[a-z][a-z0-9_-]{0,7}")
            .expect("valid regex")
            .prop_map(LeaderProbe::Leader),
        Just(LeaderProbe::NoLeader),
        Just(LeaderProbe::Failed),
    ]
}

/// Generate an arbitrary probe sequence, including the empty one (no configured
/// node yielded an answer), which must fold to `AllFailed`.
fn probes_strategy() -> impl Strategy<Value = Vec<LeaderProbe>> {
    prop::collection::vec(probe_strategy(), 0..=12)
}

/// The first `Leader(x)` in probe order, if any — the reference for what the
/// fold should resolve to.
fn first_named_leader(probes: &[LeaderProbe]) -> Option<&str> {
    probes.iter().find_map(|probe| match probe {
        LeaderProbe::Leader(node) => Some(node.as_str()),
        _ => None,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 7: Leader resolution fold.
    #[test]
    fn resolve_leader_folds_to_the_three_way_outcome(probes in probes_strategy()) {
        let resolution = resolve_leader(&probes);

        match (first_named_leader(&probes), probes.contains(&LeaderProbe::NoLeader)) {
            // A named leader is present: resolve to the FIRST one in order,
            // regardless of which endpoint listed it (Requirement 2.2).
            (Some(first), _) => {
                prop_assert_eq!(resolution, LeaderResolution::Found(first.to_string()));
            }
            // No leader named, but at least one node was reachable and knew the
            // partition: partition-unavailable, distinct from transport failure
            // (Requirement 2.3).
            (None, true) => {
                prop_assert_eq!(resolution, LeaderResolution::NoLeaderElected);
            }
            // No usable answer from any node (all `Failed`, or empty): surface
            // the all-failed/transport outcome (Requirement 2.3).
            (None, false) => {
                prop_assert_eq!(resolution, LeaderResolution::AllFailed);
            }
        }
    }

    // A reachable-but-leaderless partition NEVER folds to the all-failed
    // outcome, and vice-versa: the two stay distinct (Requirement 2.3).
    #[test]
    fn no_leader_elected_stays_distinct_from_all_failed(probes in probes_strategy()) {
        let resolution = resolve_leader(&probes);
        let any_reachable = probes.contains(&LeaderProbe::NoLeader);
        let any_leader = first_named_leader(&probes).is_some();

        if !any_leader {
            if any_reachable {
                // Reachable-but-leaderless is partition-unavailable, not transport.
                prop_assert_eq!(resolution, LeaderResolution::NoLeaderElected);
            } else {
                // Nothing reachable: transport/all-failed, not partition-unavailable.
                prop_assert_eq!(resolution, LeaderResolution::AllFailed);
            }
        }
    }
}

#[cfg(test)]
mod examples {
    use super::*;

    /// The first named leader wins even when a later node also names one — order
    /// is honored, and a leader anywhere in the list is accepted (Req 2.2).
    #[test]
    fn first_named_leader_wins_over_a_later_one() {
        let probes = vec![
            LeaderProbe::Failed,
            LeaderProbe::Leader("node-b".to_string()),
            LeaderProbe::NoLeader,
            LeaderProbe::Leader("node-c".to_string()),
        ];
        assert_eq!(
            resolve_leader(&probes),
            LeaderResolution::Found("node-b".to_string())
        );
    }

    /// Reachable-but-leaderless folds to `NoLeaderElected`, not `AllFailed`.
    #[test]
    fn reachable_but_leaderless_is_no_leader_elected() {
        let probes = vec![LeaderProbe::Failed, LeaderProbe::NoLeader];
        assert_eq!(resolve_leader(&probes), LeaderResolution::NoLeaderElected);
    }

    /// All probes failing — and the empty sequence — fold to `AllFailed`.
    #[test]
    fn all_failed_and_empty_fold_to_all_failed() {
        assert_eq!(
            resolve_leader(&[LeaderProbe::Failed, LeaderProbe::Failed]),
            LeaderResolution::AllFailed
        );
        assert_eq!(resolve_leader(&[]), LeaderResolution::AllFailed);
    }
}
