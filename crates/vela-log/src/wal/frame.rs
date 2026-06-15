//! Record-frame encoding/decoding and CRC32C.
//!
//! A record frame is the on-disk encoding of one [`LogEntry`]:
//!
//! ```text
//! offset  size    field
//! 0       4       len   u32 LE  = byte length of BODY (= 17 + payload_len)
//! 4       len     body          = index(8 LE) ++ term(8 LE) ++ kind(1) ++ payload_bytes
//! 4+len   4       crc   u32 LE  = CRC32C over bytes [0 .. 4+len)  (len field + body)
//! ```
//!
//! The CRC covers the length field **and** the body, excluding only the
//! trailing CRC itself (Requirement 2.1), so a corrupted length is itself
//! detectable: it changes the covered bytes and shifts the CRC position, both
//! of which fail validation. CRC32C (Castagnoli) is implemented in-house to
//! avoid a new dependency.
//!
//! [`decode`] reports an explicit [`FrameDecode`] classification so the
//! recovery scan (Requirement 6, task 12) can distinguish an **incomplete tail**
//! ([`FrameDecode::Incomplete`] — a torn-write candidate) from **CRC-bad or
//! otherwise interior corruption** ([`FrameDecode::Corrupt`]); only the recovery
//! pass, which can see whether a valid frame follows, decides which is fatal.

// `allow(dead_code)`: every item in this module is reachable only once the
// `DurableWal` is assembled (task 8) and re-exported from `lib.rs` (task 16).
// Until the `wal` subtree is wired into a live root, the checksum primitive and
// the frame codec below have no in-crate caller and would otherwise trip the
// `dead_code` lint under `-D warnings`. `segment` (task 5) and `recovery`
// (task 12) are the first non-test consumers; the allow is removed then.
#![allow(dead_code)]

use crate::{EntryPayload, LogEntry, PayloadKind};

/// Lookup table for CRC32C (Castagnoli), generated at compile time.
///
/// The table is built from the reflected Castagnoli polynomial `0x82F63B78`
/// (the bit-reversal of `0x1EDC6F41`), which is what a reflected-input,
/// reflected-output CRC-32C uses.
const CRC32C_TABLE: [u32; 256] = build_table();

/// Build the 256-entry Castagnoli lookup table.
///
/// A `const fn` so the table is materialized at compile time with no runtime
/// initialization and no dependency, mirroring the project's in-house
/// `SplitMix64` precedent of keeping `vela-log`'s dependency surface minimal.
const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut byte = 0usize;
    while byte < 256 {
        let mut crc = byte as u32;
        let mut bit = 0;
        while bit < 8 {
            // Reflected division: shift right and conditionally fold in the
            // reflected polynomial.
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[byte] = crc;
        byte += 1;
    }
    table
}

/// A streaming CRC32C (Castagnoli) accumulator.
///
/// Standard CRC-32C: reflected input and output, initial value `0xFFFFFFFF`,
/// and final XOR with `0xFFFFFFFF`. The check value for the ASCII bytes
/// `"123456789"` is `0xE3069283`, and the CRC of the empty input is
/// `0x00000000`.
///
/// The accumulator is incremental: [`update`](Crc32c::update) folds successive
/// byte slices into the running state and returns the finalized CRC over
/// everything fed so far. This lets a caller checksum a frame whose length
/// prefix and body live in separate buffers without first concatenating them.
#[derive(Debug, Clone)]
pub(crate) struct Crc32c {
    /// The running, pre-finalization state (the finalized CRC is `!state`).
    state: u32,
}

impl Crc32c {
    /// Create a fresh accumulator initialized to the CRC-32C start value.
    pub(crate) fn new() -> Self {
        Self { state: 0xFFFF_FFFF }
    }

    /// Fold `bytes` into the running CRC and return the finalized CRC over all
    /// bytes consumed so far.
    pub(crate) fn update(&mut self, bytes: &[u8]) -> u32 {
        let mut state = self.state;
        for &b in bytes {
            let slot = ((state ^ u32::from(b)) & 0xFF) as usize;
            state = (state >> 8) ^ CRC32C_TABLE[slot];
        }
        self.state = state;
        // Final XOR (equivalently, the one's complement) yields the standard
        // CRC-32C output.
        !state
    }

    /// One-shot convenience: the CRC-32C of a single contiguous slice.
    pub(crate) fn checksum(bytes: &[u8]) -> u32 {
        Crc32c::new().update(bytes)
    }
}

/// Width of the leading length field, in bytes (`u32` LE).
const LEN_FIELD_LEN: usize = 4;
/// Width of the trailing CRC field, in bytes (`u32` LE).
const CRC_LEN: usize = 4;
/// Fixed body header: `index(8) + term(8) + kind(1)`, preceding payload bytes.
const BODY_HEADER_LEN: usize = 8 + 8 + 1;

/// The on-disk byte size of a frame whose payload is `payload_len` bytes.
///
/// `= len field + body(header + payload) + crc`. Segment rollover (task 5) uses
/// this to decide, before writing, whether a frame fits in the active segment.
pub(crate) fn encoded_len(payload_len: usize) -> usize {
    LEN_FIELD_LEN + BODY_HEADER_LEN + payload_len + CRC_LEN
}

/// Map a [`PayloadKind`] to its on-disk `kind` byte (`0=Record, 1=Cluster,
/// 2=Noop`).
fn kind_to_byte(kind: PayloadKind) -> u8 {
    match kind {
        PayloadKind::Record => 0,
        PayloadKind::Cluster => 1,
        PayloadKind::Noop => 2,
    }
}

/// Map an on-disk `kind` byte back to a [`PayloadKind`]; an unknown byte yields
/// `None`, which [`decode`] treats as corruption.
fn kind_from_byte(byte: u8) -> Option<PayloadKind> {
    match byte {
        0 => Some(PayloadKind::Record),
        1 => Some(PayloadKind::Cluster),
        2 => Some(PayloadKind::Noop),
        _ => None,
    }
}

/// Encode one [`LogEntry`] as a length-prefixed, CRC-protected record frame.
///
/// The returned buffer is exactly [`encoded_len`]`(payload.len())` bytes:
/// `len(u32 LE) | index(u64 LE) | term(u64 LE) | kind(u8) | payload | crc(u32 LE)`.
pub(crate) fn encode(entry: &LogEntry) -> Vec<u8> {
    let payload = &entry.payload.bytes;
    // `len` counts the body only: the fixed header plus the payload bytes.
    let body_len = BODY_HEADER_LEN + payload.len();
    let mut buf = Vec::with_capacity(LEN_FIELD_LEN + body_len + CRC_LEN);

    // Length prefix.
    buf.extend_from_slice(&(body_len as u32).to_le_bytes());
    // Body: index, term, kind, then payload bytes.
    buf.extend_from_slice(&entry.index.to_le_bytes());
    buf.extend_from_slice(&entry.term.to_le_bytes());
    buf.push(kind_to_byte(entry.payload.kind));
    buf.extend_from_slice(payload);
    // CRC over everything written so far (the len field plus the body).
    let crc = Crc32c::checksum(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    buf
}

/// The classification of an attempt to [`decode`] a frame from the front of a
/// byte buffer.
///
/// The three cases are exactly what the recovery scan needs to separate a
/// recoverable torn tail from fatal interior corruption:
///
/// - [`Ok`](FrameDecode::Ok): a complete, checksum-valid frame; `consumed` is
///   the number of leading bytes the frame occupied, so the caller can advance
///   to the next frame.
/// - [`Incomplete`](FrameDecode::Incomplete): fewer than 4 bytes are present for
///   the length field, or fewer than `len + 4` bytes are present for the body
///   and CRC (Requirement 2.4). This is a torn-write candidate: if no valid
///   frame follows, recovery discards it as the torn tail.
/// - [`Corrupt`](FrameDecode::Corrupt): the buffer held a full `len + 4` bytes
///   but the CRC did not match (Requirement 2.3), or the decoded contents were
///   structurally impossible (a body shorter than the fixed header, or an
///   unknown `kind` byte). A `Corrupt` frame followed by a valid frame is
///   interior corruption and fatal to recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FrameDecode {
    /// A complete, checksum-valid frame and the byte length it occupied.
    Ok {
        /// The decoded log entry.
        entry: LogEntry,
        /// Total bytes consumed by this frame (`= encoded_len(payload_len)`).
        consumed: usize,
    },
    /// Not enough bytes are present yet to form a complete frame (torn tail
    /// candidate).
    Incomplete,
    /// A fully-present frame whose CRC failed, or whose decoded contents were
    /// structurally invalid.
    Corrupt,
}

/// Attempt to decode a single record frame from the front of `buf`.
///
/// Does not consume `buf`; on [`FrameDecode::Ok`] the caller advances by
/// `consumed`. See [`FrameDecode`] for the meaning of each result.
pub(crate) fn decode(buf: &[u8]) -> FrameDecode {
    // Need the 4-byte length prefix before anything else.
    if buf.len() < LEN_FIELD_LEN {
        return FrameDecode::Incomplete;
    }
    let body_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    // Bytes covered by the CRC (len field + body), then the CRC itself.
    let framed_len = LEN_FIELD_LEN + body_len;
    let total = framed_len + CRC_LEN;

    // The whole body and the trailing CRC must be present; otherwise the frame
    // is torn (incomplete). A corrupted-large length lands here as well.
    if buf.len() < total {
        return FrameDecode::Incomplete;
    }

    // Compare the stored CRC against one recomputed over the len field + body.
    let stored_crc = u32::from_le_bytes([
        buf[framed_len],
        buf[framed_len + 1],
        buf[framed_len + 2],
        buf[framed_len + 3],
    ]);
    if Crc32c::checksum(&buf[..framed_len]) != stored_crc {
        return FrameDecode::Corrupt;
    }

    // CRC matched. Guard against a (CRC-valid but) structurally impossible body
    // too short to hold the fixed header before indexing into it.
    if body_len < BODY_HEADER_LEN {
        return FrameDecode::Corrupt;
    }

    let body = &buf[LEN_FIELD_LEN..framed_len];
    let index = u64::from_le_bytes(body[0..8].try_into().expect("8-byte index slice"));
    let term = u64::from_le_bytes(body[8..16].try_into().expect("8-byte term slice"));
    let kind = match kind_from_byte(body[16]) {
        Some(kind) => kind,
        // An unknown kind byte cannot be mapped back to a `PayloadKind`.
        None => return FrameDecode::Corrupt,
    };
    let payload_bytes = body[BODY_HEADER_LEN..].to_vec();

    FrameDecode::Ok {
        entry: LogEntry {
            index,
            term,
            payload: EntryPayload {
                kind,
                bytes: payload_bytes,
            },
        },
        consumed: total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64, kind: PayloadKind, bytes: Vec<u8>) -> LogEntry {
        LogEntry {
            index,
            term,
            payload: EntryPayload { kind, bytes },
        }
    }

    #[test]
    fn empty_input_is_zero() {
        // CRC-32C of the empty input is 0x00000000.
        assert_eq!(Crc32c::checksum(&[]), 0x0000_0000);
    }

    #[test]
    fn standard_check_vector() {
        // The canonical CRC-32C check value for "123456789".
        assert_eq!(Crc32c::checksum(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn known_short_vectors() {
        // Independently-verifiable CRC-32C values for small inputs.
        assert_eq!(Crc32c::checksum(&[0x00]), 0x527D_5351);
        assert_eq!(Crc32c::checksum(b"a"), 0xC1D0_4330);
    }

    #[test]
    fn streaming_matches_one_shot() {
        // Folding the input in across several `update` calls must equal the
        // one-shot checksum of the whole input.
        let data = b"123456789";
        let mut crc = Crc32c::new();
        crc.update(&data[..4]);
        crc.update(&data[4..7]);
        let streamed = crc.update(&data[7..]);
        assert_eq!(streamed, Crc32c::checksum(data));
        assert_eq!(streamed, 0xE306_9283);
    }

    #[test]
    fn single_bit_flip_changes_crc() {
        let original = Crc32c::checksum(b"123456789");
        let mut bytes = *b"123456789";
        bytes[0] ^= 0x01;
        assert_ne!(Crc32c::checksum(&bytes), original);
    }

    #[test]
    fn empty_update_does_not_change_state() {
        // Updating with no bytes leaves the finalized CRC unchanged.
        let mut crc = Crc32c::new();
        assert_eq!(crc.update(b"vela"), {
            let mut other = Crc32c::new();
            other.update(b"vela");
            other.update(&[])
        });
    }

    // --- Frame codec: round-trip ------------------------------------------

    /// Payload length of the canonical frame the corruption/truncation tests
    /// operate on. Five bytes is enough that flipping a payload byte is
    /// distinguishable from flipping the header or the CRC.
    const SAMPLE_PAYLOAD_LEN: usize = 5;

    /// A representative, well-formed non-empty frame: index 7, term 3, a
    /// `Record` payload of [`SAMPLE_PAYLOAD_LEN`] bytes.
    fn sample_frame() -> Vec<u8> {
        encode(&entry(7, 3, PayloadKind::Record, vec![10, 20, 30, 40, 50]))
    }

    #[test]
    fn round_trip_preserves_fields_and_reports_consumed() {
        let original = entry(7, 3, PayloadKind::Cluster, vec![1, 2, 3, 4]);
        let bytes = encode(&original);
        // The encoded size is exactly what `encoded_len` predicts, and `decode`
        // reports having consumed the whole frame.
        assert_eq!(bytes.len(), encoded_len(4));
        match decode(&bytes) {
            FrameDecode::Ok { entry, consumed } => {
                assert_eq!(entry, original);
                assert_eq!(consumed, bytes.len());
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_empty_payload() {
        // Empty payloads (e.g. a Noop entry) must round-trip exactly
        // (Requirement 2.5).
        let original = entry(0, 1, PayloadKind::Noop, Vec::new());
        let bytes = encode(&original);
        assert_eq!(bytes.len(), encoded_len(0));
        assert!(matches!(
            decode(&bytes),
            FrameDecode::Ok { entry, .. } if entry == original
        ));
    }

    #[test]
    fn round_trip_all_payload_kinds() {
        // Every `PayloadKind` maps to a distinct byte and back unchanged.
        for kind in [PayloadKind::Record, PayloadKind::Cluster, PayloadKind::Noop] {
            let original = entry(1, 1, kind, vec![9]);
            assert!(
                matches!(
                    decode(&encode(&original)),
                    FrameDecode::Ok { entry, .. } if entry == original
                ),
                "kind {kind:?} did not round-trip",
            );
        }
    }

    // --- Frame codec: single-bit corruption (Requirement 2.2, 2.3) ---------

    /// Flip one bit at `offset` in an otherwise-valid frame and assert the
    /// decoder classifies the result as [`FrameDecode::Corrupt`].
    fn assert_bit_flip_is_corrupt(offset: usize, what: &str) {
        let mut bytes = sample_frame();
        bytes[offset] ^= 0x01;
        assert_eq!(
            decode(&bytes),
            FrameDecode::Corrupt,
            "a bit flip in the {what} (offset {offset}) should be detected as Corrupt",
        );
    }

    #[test]
    fn single_bit_flip_anywhere_in_frame_is_corrupt() {
        let frame = sample_frame();
        let crc_off = frame.len() - CRC_LEN;
        // Body field offsets: len(4) | index(8) | term(8) | kind(1) | payload.
        let index_off = LEN_FIELD_LEN; // 4
        let term_off = LEN_FIELD_LEN + 8; // 12
        let kind_off = LEN_FIELD_LEN + 16; // 20
        let payload_off = LEN_FIELD_LEN + BODY_HEADER_LEN; // 21

        // A flip in any body field changes the bytes the CRC covers, so the
        // recomputed CRC no longer matches the stored one.
        assert_bit_flip_is_corrupt(index_off, "index field (first byte)");
        assert_bit_flip_is_corrupt(index_off + 7, "index field (last byte)");
        assert_bit_flip_is_corrupt(term_off, "term field");
        assert_bit_flip_is_corrupt(kind_off, "kind byte");
        assert_bit_flip_is_corrupt(payload_off, "payload (first byte)");
        assert_bit_flip_is_corrupt(payload_off + SAMPLE_PAYLOAD_LEN - 1, "payload (last byte)");
        // A flip in the stored CRC itself makes stored != recomputed.
        assert_bit_flip_is_corrupt(crc_off, "stored CRC (first byte)");
        assert_bit_flip_is_corrupt(frame.len() - 1, "stored CRC (last byte)");
    }

    #[test]
    fn crc_valid_but_unknown_kind_is_corrupt() {
        // Isolates the structural guard: the CRC is recomputed so the frame is
        // checksum-valid, and the *unknown kind byte* is the sole reason the
        // frame is rejected (rather than a checksum mismatch).
        let mut bytes = encode(&entry(4, 1, PayloadKind::Record, vec![0xAB]));
        bytes[LEN_FIELD_LEN + 16] = 0x7F; // kind byte → not a valid PayloadKind
        let framed_len = bytes.len() - CRC_LEN;
        let crc = Crc32c::checksum(&bytes[..framed_len]);
        bytes[framed_len..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode(&bytes), FrameDecode::Corrupt);
    }

    // --- Frame codec: length-field corruption (Requirement 2.4) ------------

    #[test]
    fn length_field_enlarged_is_incomplete() {
        // A `len` inflated beyond the bytes actually present makes the decoder
        // wait for a body+CRC that will never arrive: it is indistinguishable
        // from a torn tail, so it is classified Incomplete rather than Corrupt.
        let mut bytes = sample_frame();
        let inflated = (BODY_HEADER_LEN + SAMPLE_PAYLOAD_LEN + 100) as u32;
        bytes[..LEN_FIELD_LEN].copy_from_slice(&inflated.to_le_bytes());
        assert_eq!(decode(&bytes), FrameDecode::Incomplete);
    }

    #[test]
    fn length_field_shrunk_is_corrupt() {
        // A `len` shrunk below the true body length leaves the whole buffer
        // present, but the CRC is now read from the wrong offset and computed
        // over the wrong span, so validation fails → Corrupt. This is what
        // makes a corrupted length itself detectable (Requirement 2.4).
        let mut bytes = sample_frame();
        let shrunk = (BODY_HEADER_LEN + SAMPLE_PAYLOAD_LEN - 2) as u32;
        bytes[..LEN_FIELD_LEN].copy_from_slice(&shrunk.to_le_bytes());
        assert_eq!(decode(&bytes), FrameDecode::Corrupt);
    }

    // --- Frame codec: incomplete / truncated input (Requirement 2.4) -------

    #[test]
    fn fewer_bytes_than_length_prefix_is_incomplete() {
        // Zero to three bytes cannot even hold the 4-byte length prefix.
        let bytes = sample_frame();
        for n in 0..LEN_FIELD_LEN {
            assert_eq!(
                decode(&bytes[..n]),
                FrameDecode::Incomplete,
                "{n} byte(s) is too short for the length prefix",
            );
        }
    }

    #[test]
    fn body_or_crc_not_fully_present_is_incomplete() {
        let bytes = sample_frame();
        let framed_len = bytes.len() - CRC_LEN;

        // Length prefix present, body entirely absent.
        assert_eq!(decode(&bytes[..LEN_FIELD_LEN]), FrameDecode::Incomplete);
        // Length prefix + partial body (mid-frame truncation).
        assert_eq!(decode(&bytes[..LEN_FIELD_LEN + 3]), FrameDecode::Incomplete);
        // Whole body present, CRC entirely missing.
        assert_eq!(decode(&bytes[..framed_len]), FrameDecode::Incomplete);
        // Body present, CRC truncated by one byte (drops the trailing byte).
        assert_eq!(decode(&bytes[..bytes.len() - 1]), FrameDecode::Incomplete);
    }

    // --- Frame codec: property-based round-trip (Requirements 2.5, 12.4) ---

    use proptest::prelude::*;

    /// Strategy for a single `PayloadKind` tag, covering every variant.
    fn payload_kind() -> impl Strategy<Value = PayloadKind> {
        prop_oneof![
            Just(PayloadKind::Record),
            Just(PayloadKind::Cluster),
            Just(PayloadKind::Noop),
        ]
    }

    /// Strategy for an arbitrary `LogEntry`.
    ///
    /// Indices and terms span the full `u64` range; the payload is any byte
    /// sequence of bounded length, explicitly including the empty payload so
    /// the round-trip exercises both the zero-length and many-byte cases.
    fn log_entry() -> impl Strategy<Value = LogEntry> {
        (
            any::<u64>(),
            any::<u64>(),
            payload_kind(),
            prop::collection::vec(any::<u8>(), 0..256),
        )
            .prop_map(|(index, term, kind, bytes)| LogEntry {
                index,
                term,
                payload: EntryPayload { kind, bytes },
            })
    }

    proptest! {
        // Requirement 12.4 mandates at least 256 cases for the framing
        // round-trip property.
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// **Validates: Requirements 2.5, 12.4**
        ///
        /// For any `LogEntry`, encoding then decoding yields `FrameDecode::Ok`
        /// with an entry equal to the original, and the reported `consumed`
        /// equals both `encoded_len(payload_len)` and the encoded byte length.
        #[test]
        fn encode_decode_round_trip(original in log_entry()) {
            let payload_len = original.payload.bytes.len();
            let bytes = encode(&original);

            // The encoded size is exactly what `encoded_len` predicts.
            prop_assert_eq!(bytes.len(), encoded_len(payload_len));

            match decode(&bytes) {
                FrameDecode::Ok { entry, consumed } => {
                    prop_assert_eq!(entry, original);
                    prop_assert_eq!(consumed, encoded_len(payload_len));
                    prop_assert_eq!(consumed, bytes.len());
                }
                other => prop_assert!(false, "expected Ok, got {:?}", other),
            }
        }
    }
}
