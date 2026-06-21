//! Property test for non-retryable error pass-through in `vela-client`.
//!
//! Feature: ctl-client-routing-and-repl, Property 9
//!
//! Property 9: Non-retryable application errors pass through. For any
//! non-retryable application error, the dispatch/retry engine classifies the
//! outcome as `Fatal` and surfaces the error to the caller without retrying —
//! so the partition operation is invoked exactly once and the error is returned
//! unchanged (Requirement 3.6).
//!
//! `classify` is `pub(crate)`, so this exercises its *observable* behavior
//! through the public [`ClientCore::dispatch`] seam every partition request
//! flows through: a closure that returns a non-retryable application error
//! (a `tonic::Status` carrying a typed `VelaError` whose code is one of the
//! fatal application codes — `Validation`, `TopicNotFound`, `PartitionNotFound`,
//! or `PayloadTooLarge`). The believed leader is seeded so dispatch reaches the
//! operation without a `FindLeader` network call, and an `AtomicU32` counter
//! records how many times the operation ran. The property asserts the count is
//! exactly one (no retry) and that the surfaced error is byte-for-byte the one
//! the operation produced.
//!
//! The generators range over the full set of fatal application error codes and
//! an arbitrary error message, so the pass-through guarantee is checked for
//! every fatal code rather than a single example. Dispatch runs on a paused
//! tokio clock so that, were a retry ever (incorrectly) attempted, its backoff
//! would cost no real wall-clock time.
//!
//! Validates: Requirements 3.6

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use proptest::prelude::*;
use prost::Message as _;
use vela_client::{ClientCore, ClientError, Result};
use vela_proto::v1;

/// A bootstrapped core with two known nodes. The addresses double as the
/// registry seed, so a cached leader address resolves without any network call.
fn core() -> ClientCore {
    ClientCore::new([
        ("node-a".to_string(), "http://node-a:50051".to_string()),
        ("node-b".to_string(), "http://node-b:50051".to_string()),
    ])
}

/// Build the non-retryable application error the server emits: a `tonic::Status`
/// carrying a typed [`v1::VelaError`] encoded into its details, shaped exactly
/// as the wire produces it. The transport code is incidental — the typed code
/// is what classifies the error as `Fatal` — so a `FailedPrecondition` carrier
/// is used uniformly.
fn fatal_status_error(code: v1::ErrorCode, message: &str) -> ClientError {
    let vela_error = v1::VelaError {
        code: code as i32,
        message: message.to_string(),
        leader: None,
    };
    let details = prost::bytes::Bytes::from(vela_error.encode_to_vec());
    let status =
        tonic::Status::with_details(tonic::Code::FailedPrecondition, "typed error", details);
    ClientError::Rpc(Box::new(status))
}

/// The set of non-retryable application error codes from the design's `classify`
/// mapping table (Requirement 3.6). Each must surface unchanged after a single
/// attempt.
fn fatal_code_strategy() -> impl Strategy<Value = v1::ErrorCode> {
    prop_oneof![
        Just(v1::ErrorCode::Validation),
        Just(v1::ErrorCode::TopicNotFound),
        Just(v1::ErrorCode::PartitionNotFound),
        Just(v1::ErrorCode::PayloadTooLarge),
    ]
}

/// Run one `dispatch` against the seeded leader whose operation always returns
/// the fatal application error for `code`/`message`, on a paused virtual clock
/// so any (erroneous) backoff costs no real time.
///
/// The operation rebuilds the error on each call (rather than moving a single
/// value out) so that, if dispatch were to incorrectly retry, every attempt
/// would still produce the same error and the inflated call count would expose
/// the bug. Returns `(result, attempts)`: the dispatch outcome and the number of
/// times the operation was invoked.
///
/// `proptest!` and `#[tokio::test]` do not compose, so the async dispatch is
/// driven on a current-thread runtime with a paused clock built inside the test
/// body.
fn run_fatal_dispatch(code: v1::ErrorCode, message: String) -> (Result<u64>, u32) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("build paused current-thread runtime");

    rt.block_on(async move {
        let core = core();
        // The client believes node-a leads orders/0, so dispatch reaches the
        // operation directly (cache hit, no `FindLeader`).
        core.leaders().insert("orders", 0, "http://node-a:50051");

        let calls = Arc::new(AtomicU32::new(0));
        let calls_in = Arc::clone(&calls);

        let result: Result<u64> = core
            .dispatch("orders", 0, move |_addr| {
                let calls = Arc::clone(&calls_in);
                let message = message.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(fatal_status_error(code, &message))
                }
            })
            .await;

        (result, calls.load(Ordering::SeqCst))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: ctl-client-routing-and-repl, Property 9
    #[test]
    fn fatal_errors_surface_unchanged_after_exactly_one_attempt(
        code in fatal_code_strategy(),
        message in ".{0,64}",
    ) {
        let (result, attempts) = run_fatal_dispatch(code, message.clone());

        // A `Fatal` outcome is non-retryable: the operation runs exactly once,
        // with no redirect/transport/stale-routing retry (Requirement 3.6).
        prop_assert_eq!(attempts, 1, "a fatal error must not be retried");

        // The error is surfaced to the caller, not swallowed or transformed.
        prop_assert!(
            matches!(result, Err(ClientError::Rpc(_))),
            "expected the fatal Rpc error to surface, got {result:?}",
        );

        // ...and it is the *same* error the operation produced: same transport
        // code and the same typed `VelaError` payload, byte-for-byte.
        if let Err(ClientError::Rpc(status)) = &result {
            prop_assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            let decoded =
                v1::VelaError::decode(status.details()).expect("surfaced status carries a VelaError");
            prop_assert_eq!(decoded.code, code as i32);
            prop_assert_eq!(&decoded.message, &message);
            prop_assert_eq!(decoded.leader, None);
        }
    }
}
