#![cfg(feature = "sim")]
//! Property test for `History` completeness in `vela-sim`.
//!
//! Feature: deterministic-simulation-testing, Property 10: History completeness
//!
//! Property 10: *For any* seed-derived sequence of recorded client operations —
//! a mix of topic create/delete, produce, and consume requests, answered with
//! success, redirection, and failure responses, each stamped with an invocation
//! and a response instant — recording them into a [`History`] retains **every**
//! operation, in invocation order, with its operation type, arguments,
//! invocation instant, response instant, and response intact. A produce success
//! carries the target topic/partition, the produced value, and the committed
//! offset; a consume success carries the target topic/partition, the requested
//! start offset, and the returned records in their original order; redirections
//! and failures are present as the recorded response rather than discarded. The
//! recorder is a pure function of its inputs, so recording the same generated
//! sequence twice yields equal `History` values.
//!
//! This realizes Requirement 9.1 (type + args + invocation/response instants +
//! response recorded per operation), Requirement 9.2 (produce success carries
//! topic/partition/value/offset), Requirement 9.3 (consume success carries
//! topic/partition/start-offset and the ordered records), and Requirement 9.4
//! (failures/redirections recorded, not dropped). Reproducibility under an
//! identical input underpins Requirement 9.5.
//!
//! The generator deliberately mixes all four operation kinds across all five
//! recording entry points (`record_produce_success`, `record_consume_success`,
//! `record_redirect`, `record_failure`, and the generic `record`), and biases
//! instants and payloads so empty values, keyless/keyed records, empty consume
//! batches, and multi-record ordered batches are all exercised rather than
//! reached only by chance.
//!
//! Validates: Requirements 9.1, 9.2, 9.3, 9.4

use proptest::prelude::*;
use vela_core::{NodeId, Offset, PartitionIndex, Record};
use vela_sim::history::{History, OpArgs, OpResponse, RecordedOp};
use vela_sim::scheduler::VirtualInstant;

/// Build a logical instant from a nanosecond count.
fn t(nanos: u64) -> VirtualInstant {
    VirtualInstant::from_nanos(nanos)
}

/// One generated recording action: which public `History` entry point to call,
/// and the inputs to call it with. Each variant knows both how to [`apply`]
/// itself to a `History` and the [`RecordedOp`] it is expected to produce,
/// computed independently from the requirement rather than read back from the
/// recorder.
///
/// [`apply`]: Recording::apply
#[derive(Debug, Clone)]
enum Recording {
    /// `record_produce_success` → `ProduceOk`.
    ProduceSuccess {
        args: OpArgs,
        invoked: u64,
        responded: u64,
        offset: Offset,
    },
    /// `record_consume_success` → `ConsumeOk`.
    ConsumeSuccess {
        args: OpArgs,
        invoked: u64,
        responded: u64,
        records: Vec<Record>,
    },
    /// `record_redirect` → `Redirect`.
    Redirect {
        args: OpArgs,
        invoked: u64,
        responded: u64,
        leader: NodeId,
    },
    /// `record_failure` → an expected failure response.
    Failure {
        args: OpArgs,
        invoked: u64,
        responded: u64,
        response: OpResponse,
    },
    /// Generic `record` of a pre-built op (topic create/delete success).
    Generic {
        args: OpArgs,
        invoked: u64,
        responded: u64,
        response: OpResponse,
    },
}

impl Recording {
    /// Record this action through the public `History` API.
    fn apply(&self, history: &mut History) {
        match self {
            Recording::ProduceSuccess {
                args,
                invoked,
                responded,
                offset,
            } => history.record_produce_success(args.clone(), t(*invoked), t(*responded), *offset),
            Recording::ConsumeSuccess {
                args,
                invoked,
                responded,
                records,
            } => history.record_consume_success(
                args.clone(),
                t(*invoked),
                t(*responded),
                records.clone(),
            ),
            Recording::Redirect {
                args,
                invoked,
                responded,
                leader,
            } => history.record_redirect(args.clone(), t(*invoked), t(*responded), leader.clone()),
            Recording::Failure {
                args,
                invoked,
                responded,
                response,
            } => history.record_failure(args.clone(), t(*invoked), t(*responded), response.clone()),
            Recording::Generic {
                args,
                invoked,
                responded,
                response,
            } => history.record(RecordedOp::new(
                args.clone(),
                t(*invoked),
                t(*responded),
                response.clone(),
            )),
        }
    }

    /// The `RecordedOp` this action must produce, derived from the requirement.
    fn expected(&self) -> RecordedOp {
        match self {
            Recording::ProduceSuccess {
                args,
                invoked,
                responded,
                offset,
            } => {
                let OpArgs::Produce {
                    topic,
                    partition,
                    value,
                    ..
                } = args
                else {
                    unreachable!("ProduceSuccess always carries OpArgs::Produce");
                };
                RecordedOp::new(
                    args.clone(),
                    t(*invoked),
                    t(*responded),
                    OpResponse::ProduceOk {
                        topic: topic.clone(),
                        partition: *partition,
                        value: value.clone(),
                        offset: *offset,
                    },
                )
            }
            Recording::ConsumeSuccess {
                args,
                invoked,
                responded,
                records,
            } => {
                let OpArgs::Consume {
                    topic,
                    partition,
                    start_offset,
                    ..
                } = args
                else {
                    unreachable!("ConsumeSuccess always carries OpArgs::Consume");
                };
                RecordedOp::new(
                    args.clone(),
                    t(*invoked),
                    t(*responded),
                    OpResponse::ConsumeOk {
                        topic: topic.clone(),
                        partition: *partition,
                        start_offset: *start_offset,
                        records: records.clone(),
                    },
                )
            }
            Recording::Redirect {
                args,
                invoked,
                responded,
                leader,
            } => RecordedOp::new(
                args.clone(),
                t(*invoked),
                t(*responded),
                OpResponse::Redirect {
                    leader: leader.clone(),
                },
            ),
            Recording::Failure {
                args,
                invoked,
                responded,
                response,
            }
            | Recording::Generic {
                args,
                invoked,
                responded,
                response,
            } => RecordedOp::new(args.clone(), t(*invoked), t(*responded), response.clone()),
        }
    }

    /// Whether this recording is a redirection or failure response, i.e. an
    /// outcome that must be retained rather than discarded (Requirement 9.4).
    fn is_redirect_or_failure(&self) -> bool {
        matches!(self, Recording::Redirect { .. } | Recording::Failure { .. })
    }
}

/// A small, varied set of topic names.
fn topic_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("orders".to_string()),
        Just("events".to_string()),
        Just("metrics".to_string()),
        "[a-z]{1,6}",
    ]
}

/// A byte payload of up to `max` bytes (including empty).
fn bytes_strategy(max: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=max)
}

/// An optional record key: keyless (`None`) or a non-empty key.
fn key_strategy() -> impl Strategy<Value = Option<Vec<u8>>> {
    prop_oneof![
        Just(None),
        prop::collection::vec(any::<u8>(), 1..=16).prop_map(Some),
    ]
}

/// A `PartitionIndex` from a small range so collisions across ops occur.
fn partition_strategy() -> impl Strategy<Value = PartitionIndex> {
    (0u32..8).prop_map(PartitionIndex)
}

/// A single record (keyed or keyless, possibly empty value).
fn record_strategy() -> impl Strategy<Value = Record> {
    (key_strategy(), bytes_strategy(32)).prop_map(|(key, value)| Record { key, value })
}

/// Produce arguments.
fn produce_args_strategy() -> impl Strategy<Value = OpArgs> {
    (
        topic_strategy(),
        partition_strategy(),
        key_strategy(),
        bytes_strategy(64),
    )
        .prop_map(|(topic, partition, key, value)| OpArgs::Produce {
            topic,
            partition,
            key,
            value,
        })
}

/// Consume arguments.
fn consume_args_strategy() -> impl Strategy<Value = OpArgs> {
    (topic_strategy(), partition_strategy(), 0u64..1000, 0u32..50).prop_map(
        |(topic, partition, start_offset, max_records)| OpArgs::Consume {
            topic,
            partition,
            start_offset,
            max_records,
        },
    )
}

/// Topic-create arguments.
fn create_args_strategy() -> impl Strategy<Value = OpArgs> {
    (topic_strategy(), 1u32..8, 1u32..5).prop_map(|(topic, partitions, replication_factor)| {
        OpArgs::CreateTopic {
            topic,
            partitions,
            replication_factor,
        }
    })
}

/// Any operation arguments (for kind-agnostic recording entry points).
fn any_args_strategy() -> impl Strategy<Value = OpArgs> {
    prop_oneof![
        produce_args_strategy(),
        consume_args_strategy(),
        create_args_strategy(),
        topic_strategy().prop_map(|topic| OpArgs::DeleteTopic { topic }),
    ]
}

/// An expected failure/redirection-chain response (everything `record_failure`
/// is used for).
fn failure_response_strategy() -> impl Strategy<Value = OpResponse> {
    prop_oneof![
        Just(OpResponse::UnresolvedRedirection),
        Just(OpResponse::NoLeader),
        "[a-z ]{0,24}".prop_map(|message| OpResponse::IoError { message }),
        "[a-z ]{0,24}".prop_map(|message| OpResponse::Error { message }),
    ]
}

/// A single generated recording action across all five entry points.
fn recording_strategy() -> impl Strategy<Value = Recording> {
    prop_oneof![
        (
            produce_args_strategy(),
            0u64..1_000_000,
            0u64..1_000_000,
            any::<u64>()
        )
            .prop_map(
                |(args, invoked, responded, offset)| Recording::ProduceSuccess {
                    args,
                    invoked,
                    responded,
                    offset,
                }
            ),
        (
            consume_args_strategy(),
            0u64..1_000_000,
            0u64..1_000_000,
            prop::collection::vec(record_strategy(), 0..6),
        )
            .prop_map(
                |(args, invoked, responded, records)| Recording::ConsumeSuccess {
                    args,
                    invoked,
                    responded,
                    records,
                }
            ),
        (
            any_args_strategy(),
            0u64..1_000_000,
            0u64..1_000_000,
            "[a-z0-9-]{1,8}"
        )
            .prop_map(|(args, invoked, responded, leader)| Recording::Redirect {
                args,
                invoked,
                responded,
                leader: NodeId::new(leader),
            }),
        (
            any_args_strategy(),
            0u64..1_000_000,
            0u64..1_000_000,
            failure_response_strategy(),
        )
            .prop_map(|(args, invoked, responded, response)| Recording::Failure {
                args,
                invoked,
                responded,
                response,
            }),
        (create_args_strategy(), 0u64..1_000_000, 0u64..1_000_000).prop_map(
            |(args, invoked, responded)| {
                let topic = args.topic().to_string();
                Recording::Generic {
                    args,
                    invoked,
                    responded,
                    response: OpResponse::CreateTopicOk { topic },
                }
            }
        ),
        (topic_strategy(), 0u64..1_000_000, 0u64..1_000_000).prop_map(
            |(topic, invoked, responded)| Recording::Generic {
                args: OpArgs::DeleteTopic {
                    topic: topic.clone(),
                },
                invoked,
                responded,
                response: OpResponse::DeleteTopicOk { topic },
            }
        ),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: deterministic-simulation-testing, Property 10: History completeness
    #[test]
    fn history_retains_every_operation_in_order_with_full_fidelity(
        ops in prop::collection::vec(recording_strategy(), 0..40),
    ) {
        let mut history = History::new();
        for recording in &ops {
            recording.apply(&mut history);
        }

        // Requirement 9.1, 9.4: every recorded op is retained — nothing is
        // dropped, including redirections and failures, so len equals the number
        // of operations issued.
        prop_assert_eq!(history.len(), ops.len());
        prop_assert_eq!(history.is_empty(), ops.is_empty());

        // Requirement 9.1: each op records its type, arguments, invocation
        // instant, response instant, and response; iteration is in invocation
        // order. A single structural equality against the independently computed
        // expectation covers type + args + both instants + response + ordering.
        let expected: Vec<RecordedOp> = ops.iter().map(Recording::expected).collect();
        let recorded: Vec<RecordedOp> = history.iter().cloned().collect();
        prop_assert_eq!(&recorded, &expected);

        // Requirement 9.1: the cached `kind` tag never drifts from `args`.
        for op in history.iter() {
            prop_assert_eq!(op.kind, op.args.kind());
        }

        // Per-recording, requirement-specific completeness checks.
        for (recording, op) in ops.iter().zip(history.iter()) {
            match recording {
                // Requirement 9.2: a produce success carries the target
                // topic/partition, the produced value, and the committed offset.
                Recording::ProduceSuccess { args, offset, .. } => {
                    let OpArgs::Produce { topic, partition, value, .. } = args else {
                        unreachable!();
                    };
                    prop_assert_eq!(
                        &op.response,
                        &OpResponse::ProduceOk {
                            topic: topic.clone(),
                            partition: *partition,
                            value: value.clone(),
                            offset: *offset,
                        }
                    );
                }
                // Requirement 9.3: a consume success carries the
                // topic/partition, the requested start offset, and the records
                // in their original order.
                Recording::ConsumeSuccess { args, records, .. } => {
                    let OpArgs::Consume { topic, partition, start_offset, .. } = args else {
                        unreachable!();
                    };
                    match &op.response {
                        OpResponse::ConsumeOk {
                            topic: rt,
                            partition: rp,
                            start_offset: rs,
                            records: rr,
                        } => {
                            prop_assert_eq!(rt, topic);
                            prop_assert_eq!(*rp, *partition);
                            prop_assert_eq!(*rs, *start_offset);
                            // Order is preserved exactly as returned.
                            prop_assert_eq!(rr, records);
                        }
                        other => prop_assert!(false, "expected ConsumeOk, got {:?}", other),
                    }
                }
                // Requirement 9.4: a redirection is present as the response.
                Recording::Redirect { leader, .. } => {
                    prop_assert_eq!(
                        &op.response,
                        &OpResponse::Redirect { leader: leader.clone() }
                    );
                }
                // Requirement 9.4: a failure is present as the response.
                Recording::Failure { response, .. } => {
                    prop_assert_eq!(&op.response, response);
                }
                Recording::Generic { response, .. } => {
                    prop_assert_eq!(&op.response, response);
                }
            }
        }

        // Requirement 9.4: redirections and failures are not discarded — the
        // count retained equals the count issued.
        let issued_redirect_or_failure =
            ops.iter().filter(|r| r.is_redirect_or_failure()).count();
        let recorded_redirect_or_failure = history
            .iter()
            .filter(|op| {
                matches!(
                    op.response,
                    OpResponse::Redirect { .. }
                        | OpResponse::UnresolvedRedirection
                        | OpResponse::NoLeader
                        | OpResponse::IoError { .. }
                        | OpResponse::Error { .. }
                )
            })
            .count();
        prop_assert_eq!(recorded_redirect_or_failure, issued_redirect_or_failure);

        // Reproducibility (underpins Requirement 9.5): recording the identical
        // generated sequence again yields an equal History.
        let mut replay = History::new();
        for recording in &ops {
            recording.apply(&mut replay);
        }
        prop_assert_eq!(history, replay);
    }
}
