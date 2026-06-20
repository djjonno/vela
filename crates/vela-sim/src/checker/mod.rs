//! Consistency / Liveness checkers: the correctness properties asserted per run.
//!
//! The checker has total observability of every replica (logs, terms, commit
//! indices, applied entries) plus the recorded [`History`](crate::history),
//! since the whole cluster runs in-process. It is fed observations *during* the
//! run (cheap invariants checked incrementally) and runs final passes at the end
//! (design "Consistency_Checker").
//!
//! The work is split across three submodules so each can be filled
//! independently without editing a shared file:
//!
//! - [`raft_safety`] — the Raft Safety_Properties (Election Safety, Log
//!   Matching, Leader Completeness, State Machine Safety, commit monotonicity;
//!   Requirement 10).
//! - [`kafka_parity`] — the client-consistency / Kafka-parity guarantees
//!   (durability, offset integrity, consume validity, per-partition
//!   linearizability, metadata convergence; Requirement 11).
//! - [`liveness`] — the Liveness_Properties asserted under healed faults
//!   (Requirement 12).
//!
//! # Shared checker vocabulary
//!
//! Every checker speaks the same two types, defined here so the three
//! submodules stay decoupled:
//!
//! - [`PropertyId`] names *which* property is at stake. It enumerates **all**
//!   property names up front — safety, Kafka-parity, and liveness — so a
//!   submodule references the variant it needs without editing this file or
//!   adding its own enum. The names mirror the design's Properties 11–21.
//! - [`Violation`] is what a checker returns when a property is breached: the
//!   [`PropertyId`], the logical [`VirtualInstant`] at which the breach was
//!   detected, and a human-readable `detail` string naming the affected group /
//!   term / replicas (Requirement 10.6, 2.3). `detail` is a plain `String`
//!   rather than a structured type so a checker can describe its own failure
//!   shape freely; the run-orchestration task (20.1) maps a [`Violation`] into
//!   the run's failing `Outcome`.
//!
//! A checker *detects* — it never prevents or alters consensus behaviour
//! (Requirement 10.1). Observation of the production replicas is strictly
//! read-only.

use std::fmt;

use crate::scheduler::VirtualInstant;

pub mod kafka_parity;
pub mod liveness;
pub mod raft_safety;

pub use raft_safety::RaftSafetyChecker;

/// The correctness properties the DST suite asserts, named once for every
/// checker to share (design Properties 11–21).
///
/// The enum lists all property names up front — including the Kafka-parity
/// (Requirement 11) and liveness (Requirement 12) properties implemented by the
/// [`kafka_parity`] and [`liveness`] submodules — so those submodules can name a
/// [`Violation`]'s property without extending this enum. Variant order follows
/// the design's property numbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyId {
    // --- Raft safety (Requirement 10, design Properties 11–15) ---
    /// Election Safety: at most one leader per term per group (Raft §5.2).
    ElectionSafety,
    /// Log Matching: equal `(index, term)` implies identical prefixes (§5.3).
    LogMatching,
    /// Leader Completeness: a committed entry survives in all later leaders
    /// (§5.4).
    LeaderCompleteness,
    /// State Machine Safety: no two replicas apply different entries at an index
    /// (§5.4.3).
    StateMachineSafety,
    /// Commit monotonicity: a replica's commit index never decreases.
    CommitMonotonicity,

    // --- Client consistency / Kafka parity (Requirement 11, Properties 16–20) ---
    /// Every acknowledged record stays at its returned offset.
    AcknowledgedRecordDurability,
    /// Committed offsets are contiguous from 0, strictly increasing, no
    /// gaps/dupes.
    OffsetIntegrity,
    /// Consumes return only committed records, in ascending offset order, with
    /// no phantom reads.
    ConsumeReadValidity,
    /// The recorded History is consistent with a single per-partition
    /// linearizable committed log.
    PerPartitionLinearizability,
    /// Nodes at the same metadata commit index hold identical catalogues.
    MetadataConvergence,

    // --- Liveness under healed faults (Requirement 12, Property 21) ---
    /// Under a healed, majority-available group, progress occurs within the
    /// bounded budget.
    Liveness,
}

impl PropertyId {
    /// Every [`PropertyId`] variant, listed exactly once in design property
    /// order.
    ///
    /// This is the canonical, machine-enumerable list of the properties the
    /// suite defines. It exists so callers (and the guarantee-mapping drift
    /// test, task 24.2) can iterate over all properties without hand-repeating
    /// the variant list. A newly added variant must be added here, or the
    /// `all_is_complete_and_distinct` unit test below fails.
    pub const ALL: [PropertyId; 11] = [
        PropertyId::ElectionSafety,
        PropertyId::LogMatching,
        PropertyId::LeaderCompleteness,
        PropertyId::StateMachineSafety,
        PropertyId::CommitMonotonicity,
        PropertyId::AcknowledgedRecordDurability,
        PropertyId::OffsetIntegrity,
        PropertyId::ConsumeReadValidity,
        PropertyId::PerPartitionLinearizability,
        PropertyId::MetadataConvergence,
        PropertyId::Liveness,
    ];

    /// The property's human-readable name, used in diagnostics and artifacts.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            PropertyId::ElectionSafety => "Election Safety",
            PropertyId::LogMatching => "Log Matching",
            PropertyId::LeaderCompleteness => "Leader Completeness",
            PropertyId::StateMachineSafety => "State Machine Safety",
            PropertyId::CommitMonotonicity => "Commit Monotonicity",
            PropertyId::AcknowledgedRecordDurability => "Acknowledged-Record Durability",
            PropertyId::OffsetIntegrity => "Offset Integrity",
            PropertyId::ConsumeReadValidity => "Consume Read-Validity",
            PropertyId::PerPartitionLinearizability => "Per-Partition Linearizability",
            PropertyId::MetadataConvergence => "Metadata Catalogue Convergence",
            PropertyId::Liveness => "Liveness Under Healed Faults",
        }
    }
}

impl fmt::Display for PropertyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A breached correctness property: which one, when it was detected, and a
/// human-readable description of the offending state.
///
/// A checker returns this from its observation / final-pass methods on the first
/// breach it finds; the run-orchestration task (20.1) turns it into the run's
/// failing `Outcome`, naming the property and the detection
/// [`VirtualInstant`] (Requirement 10.6, 2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The property that was breached.
    pub property: PropertyId,
    /// The logical instant at which the breach was detected.
    pub at: VirtualInstant,
    /// A human-readable description naming the affected group / term / replicas.
    pub detail: String,
}

impl Violation {
    /// Build a [`Violation`] of `property`, detected at `at`, described by
    /// `detail`.
    #[must_use]
    pub fn new(property: PropertyId, at: VirtualInstant, detail: impl Into<String>) -> Self {
        Self {
            property,
            at,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} violated at t={}ns: {}",
            self.property,
            self.at.as_nanos(),
            self.detail
        )
    }
}

#[cfg(test)]
mod tests {
    use super::PropertyId;
    use std::collections::HashSet;

    /// [`PropertyId::ALL`] must list every variant exactly once. We assert the
    /// expected length and that mapping each entry through `.name()` yields
    /// distinct names; a future variant that is added to the enum but omitted
    /// from `ALL` (or duplicated in it) trips one of these checks, forcing the
    /// list to stay in sync with the enum.
    #[test]
    fn all_is_complete_and_distinct() {
        // The suite defines eleven properties (design Properties 11–21).
        assert_eq!(
            PropertyId::ALL.len(),
            11,
            "PropertyId::ALL must list all 11 properties exactly once"
        );

        // Distinct human-readable names ⇒ no variant repeated in ALL.
        let names: HashSet<&'static str> = PropertyId::ALL.iter().map(|p| p.name()).collect();
        assert_eq!(
            names.len(),
            PropertyId::ALL.len(),
            "PropertyId::ALL contains a duplicate variant (names collide)"
        );

        // Distinct Debug identifiers as a second guard against duplicates.
        let idents: HashSet<String> = PropertyId::ALL.iter().map(|p| format!("{p:?}")).collect();
        assert_eq!(
            idents.len(),
            PropertyId::ALL.len(),
            "PropertyId::ALL contains a duplicate variant (identifiers collide)"
        );
    }
}
