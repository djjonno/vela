//! Seeded per-subsystem random-number derivation.
//!
//! Determinism *and* independence require that distinct subsystems do not march
//! in lock-step: if the election jitter and the network fault decisions shared a
//! single generator, advancing one would perturb the other and obscure the
//! cause of a failure. The existing `vela-raft` [`SimCluster`] already
//! decorrelates its bus PRNG from its clock PRNG by offsetting the seed; DST
//! formalizes that idea as [`SeedStreams`].
//!
//! From the single 64-bit run [`Seed`](SeedStreams::new) the harness derives one
//! independent [`SplitMix64`] stream per subsystem, each seeded by a distinct,
//! fixed XOR offset so the streams are mutually decorrelated yet every stream is
//! still a pure function of the run seed. All randomness in a run flows through
//! these streams and nothing else, which is what makes a run reproducible from
//! its seed alone (Requirements 1.1, 1.2).
//!
//! [`SplitMix64`] is **re-exported from `vela_raft::sim`** rather than
//! reimplemented: it is already `pub` there (the `sim` module is un-gated and
//! the type is public), so a single implementation is shared across the two
//! harnesses with no visibility change to `vela-raft` and no new dependency.
//!
//! [`SimCluster`]: vela_raft::sim::SimCluster

/// The deterministic PRNG used for every seed-derived decision in a run.
///
/// Re-exported from `vela_raft::sim` so there is exactly one implementation of
/// the in-house SplitMix64 generator across the codebase (per the design).
pub use vela_raft::sim::SplitMix64;

/// Fixed, distinct XOR offsets used to derive each subsystem stream from the
/// run seed. Distinct constants guarantee the streams start from different
/// states, so they do not advance in lock-step (the same decorrelation trick
/// `SimCluster` applies between its clock and bus PRNGs).
const ELECTION_OFFSET: u64 = 0x1111_1111_1111_1111;
const NETWORK_OFFSET: u64 = 0x2222_2222_2222_2222;
const STORAGE_OFFSET: u64 = 0x3333_3333_3333_3333;
const FAULTS_OFFSET: u64 = 0x4444_4444_4444_4444;
const WORKLOAD_OFFSET: u64 = 0x5555_5555_5555_5555;
const TIEBREAK_OFFSET: u64 = 0x6666_6666_6666_6666;

/// One independent [`SplitMix64`] stream per simulation subsystem, all derived
/// from a single 64-bit run seed.
///
/// Each field owns its own generator state and is never shared, so consuming
/// one subsystem's randomness cannot perturb another's. This is the sole source
/// of randomness in a [`crate`] run: election-timeout jitter, network fault
/// decisions, storage fault selection, the fault schedule, workload generation,
/// and event tie-breaking each draw from their own stream (Requirements 1.1,
/// 1.2).
#[derive(Debug, Clone)]
pub struct SeedStreams {
    /// Election-timer jitter (the `[base, 2*base)` randomized election timeout).
    pub election: SplitMix64,
    /// Network faults: drop / delay / reorder / duplicate decisions.
    pub network: SplitMix64,
    /// Storage-fault selection and timing (torn tail, I/O errors).
    pub storage: SplitMix64,
    /// Fault-schedule selection and timing (crash / restart / partition / skew).
    pub faults: SplitMix64,
    /// Workload generation: op kind, topic/partition, key/value, lengths.
    pub workload: SplitMix64,
    /// Event tie-break ordering when multiple events share a logical instant.
    pub tiebreak: SplitMix64,
}

impl SeedStreams {
    /// Derive all six subsystem streams from a single run `seed`.
    ///
    /// Each stream is seeded with `seed` XORed by a distinct fixed offset, so
    /// the streams are mutually decorrelated while every stream remains a pure,
    /// reproducible function of `seed` (Requirements 1.1, 1.2).
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            election: SplitMix64::new(seed ^ ELECTION_OFFSET),
            network: SplitMix64::new(seed ^ NETWORK_OFFSET),
            storage: SplitMix64::new(seed ^ STORAGE_OFFSET),
            faults: SplitMix64::new(seed ^ FAULTS_OFFSET),
            workload: SplitMix64::new(seed ^ WORKLOAD_OFFSET),
            tiebreak: SplitMix64::new(seed ^ TIEBREAK_OFFSET),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SeedStreams;
    use vela_raft::sim::SplitMix64;

    /// Number of draws compared per stream. Long enough that two distinct
    /// SplitMix64 states are overwhelmingly unlikely to coincide, short enough
    /// to stay fast and fully deterministic.
    const N: usize = 64;

    /// Draw `N` consecutive `u64`s from a stream.
    fn draw(stream: &mut SplitMix64) -> Vec<u64> {
        (0..N).map(|_| stream.next_u64()).collect()
    }

    /// Snapshot all six subsystem streams' first-`N` sequences, in field order.
    fn sequences(seed: u64) -> Vec<Vec<u64>> {
        let mut s = SeedStreams::new(seed);
        vec![
            draw(&mut s.election),
            draw(&mut s.network),
            draw(&mut s.storage),
            draw(&mut s.faults),
            draw(&mut s.workload),
            draw(&mut s.tiebreak),
        ]
    }

    /// Requirements 1.1, 1.2: the same `seed` reproduces identical sequences per
    /// stream, so a run is a pure function of its seed.
    #[test]
    fn identical_seed_reproduces_identical_streams() {
        const SEED: u64 = 0xDEAD_BEEF_CAFE_F00D;

        let first = sequences(SEED);
        let second = sequences(SEED);

        assert_eq!(
            first, second,
            "two SeedStreams built from the same seed must produce identical \
             per-stream sequences"
        );
    }

    /// Requirements 1.1, 1.2: the six subsystem streams are mutually
    /// decorrelated — for a single seed no two streams produce the same
    /// first-`N` sequence, so advancing one subsystem cannot mirror another.
    #[test]
    fn distinct_streams_are_pairwise_decorrelated() {
        const SEED: u64 = 0x0123_4567_89AB_CDEF;

        let seqs = sequences(SEED);

        for i in 0..seqs.len() {
            for j in (i + 1)..seqs.len() {
                assert_ne!(
                    seqs[i], seqs[j],
                    "streams {i} and {j} produced identical sequences; \
                     subsystem streams must be decorrelated"
                );
            }
        }
    }

    /// Requirements 1.1, 1.2: different seeds yield different randomness, so the
    /// seed actually selects the run. Checked per stream against its counterpart
    /// under a neighbouring seed.
    #[test]
    fn different_seeds_produce_different_streams() {
        let a = sequences(1);
        let b = sequences(2);

        for (i, (sa, sb)) in a.iter().zip(b.iter()).enumerate() {
            assert_ne!(
                sa, sb,
                "stream {i} produced the same sequence for two different seeds"
            );
        }
    }
}
