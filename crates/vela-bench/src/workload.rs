//! Deterministic workload generation: payloads and key assignment.
//!
//! Payloads are a **deterministic function** of the Workload_Parameters and a
//! record's 0-based position (Requirement 5.6): two Benchmark_Runs with
//! identical parameters produce byte-identical payloads, and a consumer can
//! recompute the expected payload for any position without storing it.
//!
//! The first `min(8, value_size)` bytes of a payload encode the position as a
//! little-endian `u64`; the remaining bytes are a deterministic fill derived
//! from the position. When `value_size >= 8` the embedded position can be
//! recovered with [`position_of`], letting the verifier map any consumed record
//! back to its produce position regardless of which partition/offset it landed
//! at (see `verify.rs`). When `value_size < 8` the position cannot be embedded
//! and the verifier falls back to a multiset/count comparison.

use crate::params::KeyMode;

/// The number of leading payload bytes used to embed the record position.
const POSITION_PREFIX_LEN: usize = core::mem::size_of::<u64>();

/// Deterministic value payload for the record at 0-based `position`.
///
/// The first `min(8, value_size)` bytes encode `position` as a little-endian
/// `u64`; the remaining bytes are a deterministic byte pattern derived from
/// `position`. For `value_size == 0` the payload is empty. The returned vector
/// is always exactly `value_size` bytes long.
///
/// This function is pure / referentially transparent: it depends only on
/// `(position, value_size)`, so two calls with the same arguments return
/// byte-identical vectors.
#[must_use]
pub fn payload_for(position: u64, value_size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; value_size];
    if value_size == 0 {
        return payload;
    }

    // Embed the position as a little-endian u64 in the leading bytes. When
    // value_size < 8, only the first `value_size` bytes of the encoding are
    // written (a truncated, still-deterministic prefix).
    let prefix_len = value_size.min(POSITION_PREFIX_LEN);
    let position_bytes = position.to_le_bytes();
    payload[..prefix_len].copy_from_slice(&position_bytes[..prefix_len]);

    // Fill the remaining bytes with a deterministic pattern derived from the
    // position so that distinct positions produce distinct payloads and a
    // single corrupted byte is detectable.
    for (offset, byte) in payload.iter_mut().enumerate().skip(prefix_len) {
        *byte = fill_byte(position, offset);
    }

    payload
}

/// Recover the position encoded in a payload, when `value_size >= 8`.
///
/// Returns `Some(position)` when the payload is large enough to carry a fully
/// embedded position (`value_size >= 8` and at least 8 bytes are present);
/// otherwise returns `None`.
#[must_use]
pub fn position_of(value: &[u8], value_size: usize) -> Option<u64> {
    if value_size < POSITION_PREFIX_LEN || value.len() < POSITION_PREFIX_LEN {
        return None;
    }
    let mut bytes = [0u8; POSITION_PREFIX_LEN];
    bytes.copy_from_slice(&value[..POSITION_PREFIX_LEN]);
    Some(u64::from_le_bytes(bytes))
}

/// The key for the record at `position` under the configured key mode.
///
/// `KeyMode::Keyed` → `Some(deterministic key bytes)` so the cluster routes the
/// record by its keyed partitioning rule (Requirement 4.3); `KeyMode::Keyless`
/// → `None`. Pure in `(position, mode)`.
#[must_use]
pub fn key_for(position: u64, mode: KeyMode) -> Option<Vec<u8>> {
    match mode {
        KeyMode::Keyed => Some(position.to_le_bytes().to_vec()),
        KeyMode::Keyless => None,
    }
}

/// A deterministic fill byte for the payload byte at `offset` of the record at
/// `position`. Mixing the position and the offset keeps the pattern dependent
/// on both so distinct positions yield distinct payloads.
#[inline]
fn fill_byte(position: u64, offset: usize) -> u8 {
    (position.wrapping_add(offset as u64) & 0xFF) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_length_is_exactly_value_size() {
        for size in [0usize, 1, 7, 8, 9, 64, 1024] {
            assert_eq!(payload_for(42, size).len(), size, "size = {size}");
        }
    }

    #[test]
    fn payload_for_value_size_zero_is_empty() {
        assert!(payload_for(0, 0).is_empty());
        assert!(payload_for(123_456, 0).is_empty());
    }

    #[test]
    fn payload_is_deterministic() {
        assert_eq!(payload_for(7, 32), payload_for(7, 32));
        assert_eq!(payload_for(0, 8), payload_for(0, 8));
    }

    #[test]
    fn distinct_positions_produce_distinct_payloads() {
        assert_ne!(payload_for(1, 16), payload_for(2, 16));
    }

    #[test]
    fn position_round_trips_when_embeddable() {
        for position in [0u64, 1, 255, 256, 65_535, u64::MAX] {
            let payload = payload_for(position, 8);
            assert_eq!(position_of(&payload, 8), Some(position));
            let big = payload_for(position, 128);
            assert_eq!(position_of(&big, 128), Some(position));
        }
    }

    #[test]
    fn position_is_unrecoverable_below_eight_bytes() {
        for size in 0usize..8 {
            let payload = payload_for(9, size);
            assert_eq!(position_of(&payload, size), None, "size = {size}");
        }
    }

    #[test]
    fn keyed_mode_attaches_a_key_keyless_does_not() {
        assert_eq!(
            key_for(5, KeyMode::Keyed),
            Some(5u64.to_le_bytes().to_vec())
        );
        assert_eq!(key_for(5, KeyMode::Keyless), None);
    }

    #[test]
    fn keyed_keys_are_deterministic_and_position_specific() {
        assert_eq!(key_for(10, KeyMode::Keyed), key_for(10, KeyMode::Keyed));
        assert_ne!(key_for(10, KeyMode::Keyed), key_for(11, KeyMode::Keyed));
    }
}
