//! Property test for stale-routing metadata refresh in `vela-client`.
//!
//! Feature: ctl-client-routing-and-repl, Property 13
//!
//! Property 13: Stale-routing errors force a metadata refresh. For any
//! stale-routing failure on a cached topic — a routed partition reported
//! unavailable (`PARTITION_UNAVAILABLE`), or no elected leader for the routed
//! partition (`ClientError::NoLeader`) — the dispatch/retry engine classifies
//! the outcome as `StaleRouting`, which invalidates the topic's cached metadata
//! so the *next* routing operation performs a `Metadata_Refresh` rather than
//! reusing the stale entry (Requirement 1.6).
//!
//! `classify` and the `StaleRouting` reaction are `pub(crate)`, so this exercises
//! their *observable* behavior through the public [`ClientCore::dispatch`] seam
//! every partition request flows through, plus the public `metadata()` accessor:
//!
//! 1. Seed a believed leader for the partition (so the first attempt is a cache
//!    hit and the operation runs without a `FindLeader` round-trip) and seed a
//!    *fresh* [`TopicMeta`] entry for the topic.
//! 2. Dispatch an operation that returns a stale-routing error. `dispatch`
//!    classifies it `StaleRouting`, calls `MetadataCache::invalidate(topic)`, then
//!    re-resolves the leader via `FindLeader` — which fails fast here (the
//!    bootstrap addresses refuse instantly, no server), so dispatch surfaces an
//!    error after exactly one operation attempt instead of looping.
//! 3. Assert that the topic's metadata entry is gone afterwards: a
//!    `metadata().get_fresh(topic, now)` against the *same* `now` that made the
//!    entry fresh now returns `None`, proving the entry was *invalidated* (not
//!    merely aged past its TTL). A subsequent routing operation would therefore
//!    refresh the topic's metadata.
//!
//! The generators range over both stale-routing-triggering errors and several
//! partition indices (some deliberately out of range of the cached
//! partition count, modelling the out-of-range-partition trigger), so the
//! invalidation guarantee is checked across the stale-routing space rather than
//! a single example. Dispatch runs on a paused tokio clock so the retry backoff
//! costs no real wall-clock time.
//!
//! Validates: Requirements 1.6

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use proptest::prelude::*;
use prost::Message as _;
use vela_client::{ClientCore, ClientError, Result, TopicMeta};
use vela_proto::v1;

/// A bootstrapped core whose node addresses refuse connections instantly.
///
/// After a stale-routing reaction invalidates the topic metadata it also
/// invalidates the partition's leader and re-resolves via `FindLeader` against
/// these bootstrap addresses. A loopback address on an unused port refuses the
/// connection immediately (no DNS, no timer), so the re-resolution fails fast
/// and `dispatch` returns deterministically without ever blocking on the
/// network — the metadata invalidation has already happened by then.
fn core() -> ClientCore {
    ClientCore::new([
        ("node-a".to_string(), "http://127.0.0.1:1".to_string()),
        ("node-b".to_string(), "http://127.0.0.1:1".to_string()),
    ])
}

/// Which stale-routing-triggering error the operation returns.
#[derive(Debug, Clone, Copy)]
enum StaleKind {
    /// A client-side "no leader elected" for the routed partition — a
    /// stale-routing trigger on a cached topic (`classify` → `StaleRouting`).
    NoLeader,
    /// A server `PARTITION_UNAVAILABLE`: a typed [`v1::VelaError`] travelling on
    /// an `Unavailable` status. The typed code wins over the transport code, so
    /// it classifies as `StaleRouting`, not bare transport.
    PartitionUnavailable,
}

/// The set of stale-routing-triggering errors from the design's `classify`
/// mapping table (Requirement 1.6).
fn stale_kind_strategy() -> impl Strategy<Value = StaleKind> {
    prop_oneof![
        Just(StaleKind::NoLeader),
        Just(StaleKind::PartitionUnavailable),
    ]
}

/// Build the stale-routing error the operation returns for `kind`, shaped
/// exactly as it arrives at the dispatch loop. The `PARTITION_UNAVAILABLE`
/// variant encodes a typed [`v1::VelaError`] into the status details, as the
/// server emits it on the wire.
fn stale_error(kind: StaleKind, topic: &str, partition: u32) -> ClientError {
    match kind {
        StaleKind::NoLeader => ClientError::NoLeader {
            topic: topic.to_string(),
            partition,
        },
        StaleKind::PartitionUnavailable => {
            let vela_error = v1::VelaError {
                code: v1::ErrorCode::PartitionUnavailable as i32,
                message: "partition unavailable".to_string(),
                leader: None,
            };
            let details = prost::bytes::Bytes::from(vela_error.encode_to_vec());
            ClientError::Rpc(Box::new(tonic::Status::with_details(
                tonic::Code::Unavailable,
                "partition unavailable",
                details,
            )))
        }
    }
}

/// Run one `dispatch` against a topic with fresh cached metadata, whose
/// operation always returns the stale-routing error for `kind`, on a paused
/// virtual clock so the retry backoff costs no real time.
///
/// Returns `(result, attempts, fresh_before, fresh_after)`: the dispatch
/// outcome, how many times the operation ran, and whether the topic's metadata
/// was fresh immediately before and after dispatch — both measured against the
/// *same* `now` so an `after == false` proves invalidation rather than ageing.
///
/// `proptest!` and `#[tokio::test]` do not compose, so the async dispatch is
/// driven on a current-thread runtime with a paused clock built inside the body.
fn run_stale_dispatch(
    kind: StaleKind,
    partition: u32,
    partition_count: u32,
) -> (Result<()>, u32, bool, bool) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("build paused current-thread runtime");

    rt.block_on(async move {
        let core = core();
        let topic = "orders";

        // The client believes node-a leads orders/`partition`, so dispatch
        // reaches the operation directly (cache hit, no `FindLeader`).
        core.leaders()
            .insert(topic, partition, "http://127.0.0.1:1");

        // Seed a fresh topic-metadata entry so the topic is cached/fresh before
        // routing — the precondition for a stale-routing *refresh*.
        let now = Instant::now();
        core.metadata().put(
            topic,
            TopicMeta {
                partition_count,
                leaders: vec![None; partition_count as usize],
                learned_at: now,
            },
        );
        let fresh_before = core.metadata().get_fresh(topic, now).is_some();

        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = Arc::clone(&calls);

        let result: Result<()> = core
            .dispatch(topic, partition, move |_addr| {
                let calls = Arc::clone(&calls_in);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(stale_error(kind, topic, partition))
                }
            })
            .await;

        let fresh_after = core.metadata().get_fresh(topic, now).is_some();
        (
            result,
            calls.load(Ordering::SeqCst),
            fresh_before,
            fresh_after,
        )
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 13
    #[test]
    fn stale_routing_errors_invalidate_cached_metadata(
        kind in stale_kind_strategy(),
        // A spread of partition indices, including some past the cached
        // partition count (the out-of-range-partition trigger).
        partition in 0u32..8,
        partition_count in 1u32..16,
    ) {
        let (result, attempts, fresh_before, fresh_after) =
            run_stale_dispatch(kind, partition, partition_count);

        // Precondition: the topic's metadata was cached and fresh before routing,
        // so a `None` afterwards can only be an invalidation.
        prop_assert!(fresh_before, "topic metadata must be fresh before dispatch");

        // The stale-routing operation runs once; the reaction then re-resolves
        // the leader (which fails here, with no server), so dispatch surfaces an
        // error rather than looping on the same stale belief.
        prop_assert_eq!(attempts, 1, "the stale-routing operation runs exactly once");
        prop_assert!(
            result.is_err(),
            "dispatch cannot reach a leader without a server, got {result:?}",
        );

        // The core property (Requirement 1.6): the `StaleRouting` reaction
        // invalidated the topic's cached metadata, so the next routing operation
        // performs a `Metadata_Refresh`. Checked against the same `now` that made
        // the entry fresh, so `None` proves the entry was removed (invalidated),
        // not merely aged past its TTL.
        prop_assert!(
            !fresh_after,
            "a stale-routing error must invalidate the topic metadata (Req 1.6)",
        );
    }
}
