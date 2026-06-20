#![cfg(feature = "sim")]
//! Property test for seed-driven workload generation invariants in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 9: Workload generation
//! invariants
//!
//! Property 9: *For any* valid scenario shape (`workload_size`,
//! `partition_count`, `replication_factor`, `node_count`) and *any* 64-bit
//! seed, the [`Workload`] produced by
//! [`generate`](vela_sim::workload::generate) over the run's `workload` RNG
//! stream satisfies, simultaneously:
//!
//! - **Exact size (Requirement 8.1):** it contains exactly `workload_size`
//!   operations, and `len` / `is_empty` / `op(seq)` agree with that count.
//! - **Routing (Requirement 8.2):** every *keyed* produce routes to exactly the
//!   partition the production [`PartitionRouter`] (FNV-1a) selects for its key
//!   and the topic's partition count; every *keyless* produce — and every
//!   consume — targets a partition in `0..partition_count`.
//! - **Record shape (Requirement 8.3):** every produce value length is in
//!   `0..=`[`MAX_VALUE_LEN`], and every *keyed* key length is in
//!   [`MIN_KEY_LEN`]`..=`[`MAX_KEY_LEN`].
//! - **Determinism (Requirement 8.1):** the same `(seed, params)` reproduces a
//!   byte-identical workload, which is what lets a failing run replay.
//! - **Redirect bound (Requirement 8.4):** the successive-redirection hop bound
//!   [`MAX_REDIRECT_HOPS`] is exactly 5.
//!
//! # Framing of Requirement 8.7 (interleaving with faults)
//!
//! Requirement 8.7 requires the harness to *interleave* client-operation
//! issuance with the `Fault_Schedule` — to keep issuing produces and consumes
//! *while* crashes, restarts, and partitions are in effect rather than pausing
//! until a heal. The piece of that requirement owned by the **generator** is a
//! *generation-independence* invariant: the generated workload is a pure
//! function of the scenario parameters and the seed and consults **no fault
//! state**. There is no fault input to `generate` at all — so the same workload
//! is produced regardless of how faults are configured or scheduled. That
//! independence is precisely what *permits* the runtime (task 12.1) to issue the
//! pre-generated operations across the timeline interleaved with faults: the op
//! sequence never has to wait on, or change in response to, the fault schedule.
//!
//! This property checks that framing directly: holding the generation-relevant
//! shape (`workload_size`, `partition_count`, `replication_factor`) and the seed
//! fixed while varying every fault-related and fault-adjacent parameter
//! (`faults`, `node_count`, `budget`) leaves the generated workload byte-for-byte
//! identical. (The interleaved *scheduling* of these operations against faults
//! is exercised by the runtime tests, not the generator.)
//!
//! Validates: Requirements 8.1, 8.2, 8.3, 8.4, 8.7

use proptest::prelude::*;

use vela_core::{PartitionIndex, PartitionRouter};
use vela_sim::rng::SeedStreams;
use vela_sim::scenario::{Budget, FaultIntensities, ScenarioParameters};
use vela_sim::workload::{
    generate, ClientOperation, Workload, MAX_KEY_LEN, MAX_REDIRECT_HOPS, MAX_VALUE_LEN, MIN_KEY_LEN,
};

/// Upper bound on `workload_size` explored by the property.
///
/// Large enough that a single workload reliably mixes all four operation kinds
/// (and both keyed and keyless produces) so the routing / shape invariants are
/// exercised on real data, while small enough — alongside up-to-64 KiB produce
/// values — to keep 100+ cases fast and fully deterministic.
const MAX_WORKLOAD_SIZE: usize = 150;

/// Generate a valid scenario shape plus a seed.
///
/// `replication_factor` is constrained to `1..=node_count` (a validated set)
/// even though the generator only reads it to stamp `CreateTopic` operations;
/// `partition_count` is `1..=8` (topics always have at least one partition).
fn scenario_strategy() -> impl Strategy<Value = (u64, usize, usize, u32, usize)> {
    (1usize..=8)
        .prop_flat_map(|node_count| {
            let replication_factor = 1usize..=node_count;
            (Just(node_count), replication_factor)
        })
        .prop_flat_map(|(node_count, replication_factor)| {
            (
                any::<u64>(),
                Just(node_count),
                Just(replication_factor),
                1u32..=8,
                0usize..=MAX_WORKLOAD_SIZE,
            )
        })
}

/// Build the run's `workload` RNG stream for `seed`, exactly as a run does.
fn workload_stream(seed: u64) -> vela_sim::rng::SplitMix64 {
    SeedStreams::new(seed).workload
}

/// Assert every per-operation invariant (size-independent): routing
/// (Requirement 8.2) and record shape (Requirement 8.3).
fn assert_op_invariants(workload: &Workload, partition_count: u32) -> Result<(), TestCaseError> {
    // A fresh, stateless router reproduces the production keyed mapping; the
    // keyed path keeps no state, so this matches what the generator used.
    let router = PartitionRouter::new();

    for op in workload.ops() {
        match op {
            ClientOperation::Produce {
                topic,
                partition,
                key,
                value,
            } => {
                // Requirement 8.3: value length within the inclusive bound.
                prop_assert!(
                    value.len() <= MAX_VALUE_LEN,
                    "produce value length {} exceeds MAX_VALUE_LEN {}",
                    value.len(),
                    MAX_VALUE_LEN
                );

                match key {
                    Some(k) => {
                        // Requirement 8.3: keyed key length within bounds.
                        prop_assert!(
                            (MIN_KEY_LEN..=MAX_KEY_LEN).contains(&k.len()),
                            "keyed key length {} outside {}..={}",
                            k.len(),
                            MIN_KEY_LEN,
                            MAX_KEY_LEN
                        );
                        // Requirement 8.2: keyed produce routes via the
                        // production PartitionRouter.
                        let expected = router.resolve(topic, Some(k), partition_count);
                        prop_assert_eq!(
                            *partition,
                            expected,
                            "keyed produce routing diverged from PartitionRouter"
                        );
                    }
                    None => {
                        // Requirement 8.2: keyless produce targets a partition
                        // in 0..partition_count.
                        let PartitionIndex(idx) = *partition;
                        prop_assert!(
                            idx < partition_count,
                            "keyless produce partition {} outside 0..{}",
                            idx,
                            partition_count
                        );
                    }
                }
            }
            ClientOperation::Consume { partition, .. } => {
                let PartitionIndex(idx) = *partition;
                prop_assert!(
                    idx < partition_count,
                    "consume partition {} outside 0..{}",
                    idx,
                    partition_count
                );
            }
            // Create / delete carry no partition or record shape to check here.
            ClientOperation::CreateTopic { .. } | ClientOperation::DeleteTopic { .. } => {}
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: deterministic-simulation-testing, Property 9: Workload generation
    // invariants
    #[test]
    fn workload_generation_invariants(
        (seed, node_count, replication_factor, partition_count, workload_size)
            in scenario_strategy(),
    ) {
        let params = ScenarioParameters {
            node_count,
            replication_factor,
            partition_count,
            workload_size,
            ..ScenarioParameters::default()
        };

        let workload = generate(&params, &mut workload_stream(seed));

        // Requirement 8.1: exactly `workload_size` operations, with the
        // accessors agreeing on the count.
        prop_assert_eq!(workload.len(), workload_size);
        prop_assert_eq!(workload.ops().len(), workload_size);
        prop_assert_eq!(workload.is_empty(), workload_size == 0);
        // `op(seq)` indexes 0..len and is `None` exactly at the end.
        for (i, op) in workload.ops().iter().enumerate() {
            prop_assert_eq!(workload.op(i as u64), Some(op));
        }
        prop_assert_eq!(workload.op(workload_size as u64), None);

        // Requirements 8.2, 8.3: routing and record shape for every operation.
        assert_op_invariants(&workload, partition_count)?;

        // Requirement 8.1: determinism — the same (seed, params) reproduces a
        // byte-identical workload, the invariant that lets a failing run replay.
        let again = generate(&params, &mut workload_stream(seed));
        prop_assert_eq!(&again, &workload);

        // Requirement 8.7 (generation-independence framing): the workload is a
        // pure function of the generation-relevant shape and the seed and
        // consults no fault state. Holding `workload_size`, `partition_count`,
        // `replication_factor`, and the seed fixed while varying every
        // fault-related / fault-adjacent parameter leaves the generated workload
        // identical — which is what lets the runtime interleave its issuance
        // with the fault schedule rather than gating on it.
        let fault_heavy = ScenarioParameters {
            // Vary fields the generator must not consult.
            node_count: node_count + 3,
            faults: FaultIntensities {
                base_latency_nanos: 7_000_000,
                reorder_prob: 0.5,
                max_reorder_nanos: 9_000_000,
                drop_prob: 0.3,
                duplicate_prob: 0.2,
                partition_prob: 0.4,
                crash_prob: 0.6,
                max_clock_skew_nanos: 1_000_000,
                max_clock_skew_rate: 0.05,
                torn_write_prob: 0.25,
                io_error_prob: 0.15,
            },
            budget: Budget {
                max_events: 1,
                max_virtual_nanos: 1,
            },
            // Generation-relevant shape held fixed.
            replication_factor,
            partition_count,
            workload_size,
        };
        let under_faults = generate(&fault_heavy, &mut workload_stream(seed));
        prop_assert_eq!(
            &under_faults,
            &workload,
            "generated workload changed when only fault state varied; \
             generation must be fault-independent"
        );

        // Requirement 8.4: the successive-redirection hop bound is exactly 5.
        prop_assert_eq!(MAX_REDIRECT_HOPS, 5);
    }
}
