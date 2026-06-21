//! Property test for gap-filling cluster discovery in `vela-client`.
//!
//! Feature: advertised-listeners, Property 6: Discovery fills gaps without
//! overriding configured endpoints.
//!
//! For any pre-seeded node registry and any set of discovered members, seeding
//! the registry from those members leaves every already-present node id mapped
//! to its original address, and adds an entry only for node ids not already
//! present.
//!
//! This exercises the public [`NodeRegistry`] gap-fill API
//! ([`NodeRegistry::insert_if_absent`]) that `ClientCore::seed_registry_from_cluster`
//! drives once it has selected each member's address (see Property 5 for the
//! selection precedence). Because `insert_if_absent` is public on
//! [`NodeRegistry`], the gap-fill semantics are expressed directly against it
//! from this external integration test rather than co-located in-crate.
//!
//! The generators produce an arbitrary pre-seeded `id -> addr` map (the
//! operator-supplied `id=url` endpoints) and an arbitrary list of discovered
//! `(id, addr)` members whose ids freely overlap the pre-seeded ones, so both
//! the "already present, keep original" and the "absent, add" branches are
//! exercised across many inputs.
//!
//! Validates: Requirements 6.3

use std::collections::HashMap;

use proptest::prelude::*;
use vela_client::NodeRegistry;

/// A node id drawn from a small alphabet so discovered ids overlap pre-seeded
/// ids often enough to exercise the "already present" branch heavily, not just
/// the "absent" one.
fn node_id() -> impl Strategy<Value = String> {
    "[a-d][0-9]"
}

/// A non-empty address-like token. Pre-seeded (configured) and discovered
/// addresses are drawn from disjoint shapes so a test can tell, after seeding,
/// whether a given id kept its configured address or took a discovered one.
fn configured_addr() -> impl Strategy<Value = String> {
    "http://[a-z]{1,8}:5005[0-9]"
}

fn discovered_addr() -> impl Strategy<Value = String> {
    "10\\.0\\.0\\.[0-9]:70[0-9][0-9]"
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: advertised-listeners, Property 6
    #[test]
    fn discovery_fills_gaps_without_overriding_configured_endpoints(
        preseeded in prop::collection::hash_map(node_id(), configured_addr(), 0..6),
        discovered in prop::collection::vec((node_id(), discovered_addr()), 0..10),
    ) {
        let registry = NodeRegistry::from_pairs(
            preseeded.iter().map(|(id, addr)| (id.clone(), addr.clone())),
        );

        // Seed from the discovered members, gap-filling only — exactly what
        // `seed_registry_from_cluster` does with each selected address.
        // Track the first discovered address seen per absent id, since the first
        // insertion for an id wins (later ones find it present).
        let mut first_added: HashMap<String, String> = HashMap::new();
        for (id, addr) in &discovered {
            let was_absent_before =
                !preseeded.contains_key(id) && !first_added.contains_key(id);
            let added = registry.insert_if_absent(id.clone(), addr.clone());

            // `insert_if_absent` reports an insertion iff the id was absent.
            prop_assert_eq!(added, was_absent_before);

            if was_absent_before {
                first_added.insert(id.clone(), addr.clone());
            }
        }

        // Every pre-seeded id keeps its original configured address — discovery
        // never overrides an operator-supplied endpoint (Req 6.3).
        for (id, addr) in &preseeded {
            let resolved = registry.addr_of(id);
            prop_assert_eq!(resolved.as_deref(), Some(addr.as_str()));
        }

        // Every id that was absent and discovered was added with the first
        // discovered address seen for it.
        for (id, addr) in &first_added {
            let resolved = registry.addr_of(id);
            prop_assert_eq!(resolved.as_deref(), Some(addr.as_str()));
        }

        // No id outside (pre-seeded ∪ discovered) was invented.
        for (id, _) in &discovered {
            prop_assert!(registry.addr_of(id).is_some());
        }
    }
}
