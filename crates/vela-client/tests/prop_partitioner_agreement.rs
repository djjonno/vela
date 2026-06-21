//! Cross-implementation partitioner-agreement property test.
//!
//! Feature: ctl-client-routing-and-repl, Property 2: Cross-implementation
//! partitioner agreement — for any non-empty key and any partition count
//! `N >= 1`, the client [`vela_client::PartitionRouter`], the core
//! [`vela_core::PartitionRouter`], and the canonical
//! [`vela_proto::partition::partition_for_key`] all resolve the key to the same
//! partition index. Keyed routing must agree byte-for-byte across every
//! implementation so a record routed on the client lands on the same partition
//! the server would have chosen.
//!
//! All three are exercised against the *same* `(key, partition_count)` inputs:
//!
//! - the canonical partitioner is the single source of truth both routers
//!   delegate to,
//! - the client router returns `Result<u32, RouteError>` (a non-empty key with
//!   `N >= 1` always yields `Ok`),
//! - the core router returns a `PartitionIndex(u32)` newtype.
//!
//! The generators constrain inputs to exactly the space the property quantifies
//! over: a non-empty byte key (`1..=256` bytes — an empty key would select the
//! keyless rule, which this property excludes) and a partition count of at least
//! one (`1..=10_000`, the topic partition bound). The topic name is varied too,
//! since keyed routing must be a pure function of the key bytes and partition
//! count, independent of the topic.
//!
//! This is a test-only dependency on `vela-core`; the runtime inward dependency
//! direction (`vela-client -> vela-proto`, never `vela-core`) is unchanged.
//!
//! Validates: Requirements 5.5

use proptest::prelude::*;
use vela_proto::partition::partition_for_key;

/// Generate a non-empty key: between 1 and 256 arbitrary bytes. An empty key
/// would fall through to the keyless rule, which Property 2 explicitly excludes.
fn non_empty_key_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..=256)
}

/// Generate a topic name. The topic does not affect keyed routing (the partition
/// is a function of the key bytes and count only), so any non-empty name
/// exercises the property.
fn topic_strategy() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Za-z0-9_-]{1,64}").expect("valid regex")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 2: Cross-implementation
    // partitioner agreement.
    #[test]
    fn client_core_and_canonical_partitioners_agree(
        topic in topic_strategy(),
        key in non_empty_key_strategy(),
        partition_count in 1u32..=10_000,
    ) {
        // The canonical partitioner is the single source of truth. For `N >= 1`
        // and any key it always yields `Some`.
        let canonical = partition_for_key(&key, partition_count)
            .expect("partition_count is non-zero");

        // The client router returns `Result<u32, RouteError>`; a non-empty key
        // with a non-zero count always succeeds.
        let client_router = vela_client::PartitionRouter::new();
        let client = client_router
            .resolve(&topic, Some(&key), partition_count)
            .expect("non-empty key with non-zero count resolves");

        // The core router returns a `PartitionIndex(u32)` newtype.
        let core_router = vela_core::PartitionRouter::new();
        let vela_core::PartitionIndex(core) =
            core_router.resolve(&topic, Some(&key), partition_count);

        // All three implementations resolve the key to the same partition
        // (Requirement 5.5).
        prop_assert_eq!(client, canonical, "client router disagrees with canonical");
        prop_assert_eq!(core, canonical, "core router disagrees with canonical");

        // Transitively the two routers agree, and the shared index is in range.
        prop_assert_eq!(client, core, "client and core routers disagree");
        prop_assert!(
            canonical < partition_count,
            "partition index {} out of range for count {}",
            canonical,
            partition_count
        );
    }
}
