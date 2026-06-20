#![cfg(feature = "sim")]
//! Edge-case (example) tests for client-operation **redirect exhaustion**
//! (Requirement 8.5) and the **no-leader** response (Requirement 8.6).
//!
//! # What these tests exercise, and why at this level
//!
//! Requirements 8.5/8.6 are about the *recorded outcome* of issuing a client
//! operation when redirect-following fails to reach a leader:
//!
//! - **8.5:** IF 5 successive redirections do not reach a current leader, THEN
//!   the harness records the unresolved-redirection error as a **valid
//!   response**, not a property violation.
//! - **8.6:** IF an operation is issued while no leader is available, THEN the
//!   harness records the returned no-leader error as a **valid response**, not a
//!   property violation.
//!
//! The runtime's redirect-following client-op *issue* path is a documented hook
//! that is not yet wired end-to-end: [`SimRuntime::dispatch_client_op`] and
//! [`SimRuntime::resolve_committed`] in `runtime.rs` are stubs pending the full
//! workload + history integration. So there is no production code path yet that
//! drives a live redirect chain through `SimRuntime`/`SimulatedCluster`. These
//! are therefore **example tests of the two pieces the current code does
//! support**, using only public APIs:
//!
//! 1. **The hop bound** — [`workload::MAX_REDIRECT_HOPS`] is exactly 5, the
//!    constant the runtime's redirect-following loop will honour (Requirement
//!    8.4 underpinning 8.5).
//! 2. **The recorded-response contract** — recording an
//!    [`OpResponse::UnresolvedRedirection`] or [`OpResponse::NoLeader`] through
//!    [`History::record_failure`] **retains** it as the operation's response
//!    (the op is present, not discarded, and is the recorded response for that
//!    op). This is precisely the "valid response, not a violation" contract of
//!    8.5/8.6: the failure-recording path is the *valid-response* path by design
//!    (see [`OpResponse`] docs), distinct from any safety/liveness violation.
//!
//! To make the "5 successive redirections → unresolved" behaviour explicit
//! rather than only asserting the constant, the redirect-exhaustion test models
//! the runtime's redirect-following loop with a small, test-local helper
//! ([`resolve_with_redirects`]) driven against deterministic cluster answers.
//! The helper mirrors the loop the runtime hook will perform — follow a redirect
//! toward the next node, up to [`MAX_REDIRECT_HOPS`] successive hops, and record
//! unresolved-redirection if the bound is reached without a leader — and the
//! resulting response is then recorded through the real `History`. The helper is
//! a *model of the issue loop*, deliberately separate from production code,
//! since that loop is not yet wired; what is asserted against production code is
//! the bound constant and the `History` recording contract.
//!
//! [`SimRuntime::dispatch_client_op`]: vela_sim::runtime::SimRuntime
//! [`SimRuntime::resolve_committed`]: vela_sim::runtime::SimRuntime
//! [`workload::MAX_REDIRECT_HOPS`]: vela_sim::workload::MAX_REDIRECT_HOPS
//!
//! Validates: Requirements 8.5, 8.6

use vela_sim::history::{History, OpArgs, OpResponse};
use vela_sim::scheduler::VirtualInstant;
use vela_sim::workload::MAX_REDIRECT_HOPS;

use vela_core::PartitionIndex;

/// A logical instant `nanos` from the origin.
fn at(nanos: u64) -> VirtualInstant {
    VirtualInstant::from_nanos(nanos)
}

/// A keyless produce against `topic`/`partition` — the request a redirect chain
/// or a no-leader response is recorded *against*. The recorded response is what
/// 8.5/8.6 govern; the request arguments are retained verbatim regardless.
fn produce_args(topic: &str, partition: u32) -> OpArgs {
    OpArgs::Produce {
        topic: topic.to_string(),
        partition: PartitionIndex(partition),
        key: None,
        value: b"payload".to_vec(),
    }
}

/// The cluster's answer when a client op is issued to one node.
///
/// This is the per-hop outcome the runtime's redirect-following loop reacts to:
/// either the contacted node is the leader (the op proceeds) or it is not and
/// redirects the client toward another node (Requirement 8.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IssueOutcome {
    /// The contacted node is the current leader; the operation would proceed.
    Leader,
    /// The contacted node is not the leader and redirects toward another node.
    Redirect,
}

/// The terminal result of modelling redirect-following for one client op.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Resolution {
    /// A leader was reached after following `redirects` successive redirects
    /// (`redirects <= MAX_REDIRECT_HOPS`).
    ReachedLeader { redirects: u8 },
    /// `MAX_REDIRECT_HOPS` successive redirects were followed without reaching a
    /// leader — the unresolved-redirection outcome of Requirement 8.5.
    Unresolved { redirects: u8 },
}

/// Model the runtime's redirect-following loop: issue an op and follow up to
/// [`MAX_REDIRECT_HOPS`] successive redirects, stopping with an unresolved
/// resolution once the bound is reached without a leader (Requirement 8.5).
///
/// `issue(redirects_so_far)` returns the cluster's answer for the node currently
/// contacted; `redirects_so_far` is the number of redirects already followed.
///
/// This is a test-local model of the not-yet-wired runtime issue loop, kept
/// separate from production code on purpose (the loop is a stub today). It exists
/// so the "5 successive redirections → unresolved" behaviour is exercised
/// explicitly rather than asserted only as a constant.
fn resolve_with_redirects(mut issue: impl FnMut(u8) -> IssueOutcome) -> Resolution {
    let mut redirects: u8 = 0;
    loop {
        match issue(redirects) {
            IssueOutcome::Leader => return Resolution::ReachedLeader { redirects },
            IssueOutcome::Redirect => {
                redirects += 1;
                if redirects >= MAX_REDIRECT_HOPS {
                    // The bound is reached and the chain has still not landed on
                    // a leader: record the unresolved-redirection response.
                    return Resolution::Unresolved { redirects };
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The hop bound (Requirement 8.4 underpinning 8.5)
// ---------------------------------------------------------------------------

/// The successive-redirection bound the runtime's issue loop honours is exactly
/// 5 (Requirement 8.4, the precondition of 8.5's "5 successive redirections").
#[test]
fn max_redirect_hops_is_five() {
    assert_eq!(MAX_REDIRECT_HOPS, 5);
}

// ---------------------------------------------------------------------------
// Redirect exhaustion (Requirement 8.5)
// ---------------------------------------------------------------------------

/// A redirect chain that never reaches a leader stops after exactly
/// [`MAX_REDIRECT_HOPS`] successive redirects (Requirement 8.5).
///
/// This drives the modelled issue loop with a cluster that always redirects, and
/// asserts the loop terminates with `Unresolved` after exactly 5 redirects — the
/// "5 successive redirections without reaching a leader" condition of 8.5.
#[test]
fn five_successive_redirects_without_a_leader_resolves_unresolved() {
    // Every contacted node redirects: a leader is never reached.
    let resolution = resolve_with_redirects(|_hops| IssueOutcome::Redirect);

    assert_eq!(
        resolution,
        Resolution::Unresolved {
            redirects: MAX_REDIRECT_HOPS
        },
        "an endless redirect chain must stop after exactly MAX_REDIRECT_HOPS \
         redirects with an unresolved resolution"
    );
}

/// The unresolved-redirection outcome of an exhausted chain is recorded as the
/// operation's **valid response** and retained — not discarded, not flagged as a
/// violation (Requirement 8.5).
///
/// Ties the modelled loop to the real `History`: the loop exhausts its hops, and
/// the resulting [`OpResponse::UnresolvedRedirection`] is recorded through
/// [`History::record_failure`] (the valid-response recording path). The op is
/// then present in the history, with `UnresolvedRedirection` as its response.
#[test]
fn exhausted_redirect_chain_records_unresolved_redirection_as_a_valid_response() {
    let mut history = History::new();
    let args = produce_args("orders", 0);

    // Model the issue: the chain never reaches a leader.
    let resolution = resolve_with_redirects(|_hops| IssueOutcome::Redirect);
    assert!(
        matches!(resolution, Resolution::Unresolved { .. }),
        "precondition: the chain must be exhausted to record unresolved-redirection"
    );

    // Record the unresolved-redirection response, exactly as the runtime hook
    // will once wired. This is the valid-response path (Requirement 8.5).
    let before = history.len();
    history.record_failure(
        args.clone(),
        at(100),
        at(160),
        OpResponse::UnresolvedRedirection,
    );

    // The op is retained (history grew by one), not discarded.
    assert_eq!(
        history.len(),
        before + 1,
        "the exhausted op must be retained"
    );
    let recorded = history.ops.last().expect("one op was recorded");
    // The request arguments are kept verbatim alongside the response.
    assert_eq!(recorded.args, args);
    // The recorded response for this op is exactly unresolved-redirection.
    assert_eq!(recorded.response, OpResponse::UnresolvedRedirection);
    assert_eq!(recorded.invoked_at, at(100));
    assert_eq!(recorded.responded_at, at(160));
}

/// Control: a chain that *does* reach a leader within the bound resolves to
/// `ReachedLeader` and does not exhaust — so the unresolved outcome above is
/// attributable to running out of hops, not to the loop itself.
///
/// The cluster redirects twice, then the third contacted node is the leader, so
/// the loop stops after 2 redirects without recording unresolved-redirection.
#[test]
fn redirect_chain_reaching_a_leader_within_the_bound_does_not_exhaust() {
    let leader_after: u8 = 2;
    let resolution = resolve_with_redirects(|hops| {
        if hops >= leader_after {
            IssueOutcome::Leader
        } else {
            IssueOutcome::Redirect
        }
    });

    assert_eq!(
        resolution,
        Resolution::ReachedLeader {
            redirects: leader_after
        },
        "a leader reached within MAX_REDIRECT_HOPS must not produce unresolved-redirection"
    );
    assert!(
        leader_after < MAX_REDIRECT_HOPS,
        "the leader is reached strictly within the redirect bound"
    );
}

// ---------------------------------------------------------------------------
// No leader available (Requirement 8.6)
// ---------------------------------------------------------------------------

/// Issuing an operation while no leader is available records the no-leader error
/// as the operation's **valid response** and retains it — not discarded, not
/// flagged as a violation (Requirement 8.6).
#[test]
fn no_leader_response_is_recorded_as_a_valid_response() {
    let mut history = History::new();
    let args = produce_args("orders", 1);

    let before = history.len();
    history.record_failure(args.clone(), at(200), at(205), OpResponse::NoLeader);

    // The op is retained (history grew by one), not discarded.
    assert_eq!(
        history.len(),
        before + 1,
        "the no-leader op must be retained"
    );
    let recorded = history.ops.last().expect("one op was recorded");
    assert_eq!(recorded.args, args);
    // The recorded response for this op is exactly no-leader.
    assert_eq!(recorded.response, OpResponse::NoLeader);
    assert_eq!(recorded.invoked_at, at(200));
    assert_eq!(recorded.responded_at, at(205));
}

/// Both expected non-success outcomes are recorded as responses and coexist in
/// the history alongside ordinary successes: each `record_*` call grows the
/// history by one and the responses match in issue order. Neither
/// unresolved-redirection nor no-leader is treated as a violation — both are
/// retained data in the recorded trace (Requirements 8.5, 8.6).
#[test]
fn unresolved_and_no_leader_coexist_as_recorded_responses() {
    let mut history = History::new();
    assert!(history.is_empty());

    // A successful produce, then an exhausted-redirect op, then a no-leader op.
    history.record_produce_success(produce_args("orders", 0), at(10), at(20), 0);
    assert_eq!(history.len(), 1);

    history.record_failure(
        produce_args("orders", 0),
        at(30),
        at(90),
        OpResponse::UnresolvedRedirection,
    );
    assert_eq!(history.len(), 2);

    history.record_failure(
        produce_args("orders", 0),
        at(100),
        at(105),
        OpResponse::NoLeader,
    );
    assert_eq!(history.len(), 3);

    // The recorded responses, in invocation order, are exactly what was issued —
    // the two expected non-success responses are retained, not dropped.
    let responses: Vec<&OpResponse> = history.iter().map(|op| &op.response).collect();
    assert!(matches!(responses[0], OpResponse::ProduceOk { .. }));
    assert_eq!(responses[1], &OpResponse::UnresolvedRedirection);
    assert_eq!(responses[2], &OpResponse::NoLeader);
}
