//! Per-partition on-disk path derivation and topic-name sanitization.
//!
//! Durable partition logs live beneath the node's [`Data_Directory`] at a
//! per-partition subpath that must be **stable** across restarts (so a partition
//! reopens its existing segments) and **collision-free** across distinct
//! topic-and-partition pairs (Requirement 7). Because a topic name is a
//! namespace that may contain filesystem-unsafe bytes, the topic component is
//! produced by an injective, reversible escape ([`safe`]) that emits only
//! `[A-Za-z0-9-_]`.
//!
//! The escape is designed so its output can **never** contain the substring
//! `__`: every emitted `_` is immediately followed by two hex digits. The
//! metadata Raft group therefore safely uses the literal reserved component
//! `__meta`, which no client topic's [`safe`] output can ever equal or contain
//! (Requirement 16.3-16.5).
//!
//! [`Data_Directory`]: crate::config::Config::data_dir

use std::path::{Path, PathBuf};

/// The reserved path component for the durable metadata Raft group.
///
/// Used directly (it is a literal, not a client topic name) by
/// [`metadata_data_path`]. It is itself a valid [`Safe_Path_Component`] — it
/// draws only from `[A-Za-z0-9-_]` — yet no client topic can collide with it
/// because [`safe`]'s output never contains the `__` substring this name leads
/// with (Requirement 16.2-16.5).
///
/// [`Safe_Path_Component`]: safe
pub(crate) const METADATA_COMPONENT: &str = "__meta";

/// Convert a topic name into a single [`Safe_Path_Component`].
///
/// Operates on the **raw bytes** of `topic` ([`str::as_bytes`]). Each byte in
/// the safe set `[A-Za-z0-9-]` passes through literally; **every** other byte
/// (including `_` and every byte of a non-ASCII / multibyte sequence) is escaped
/// as `_` followed by exactly two upper-case hex digits.
///
/// The result therefore:
/// - uses only characters from `[A-Za-z0-9-_]`, so it is always a valid path
///   component (Requirement 7.3);
/// - is an injective encoding — distinct names map to distinct components —
///   because the escape is uniquely decodable (Requirement 7.2);
/// - is a pure function of `topic`, so it is identical across restarts
///   (Requirement 7.4);
/// - can **never** contain the substring `__`: an output `_` is always
///   immediately followed by a hex digit, never another `_` (Requirement 16.5).
///
/// [`Safe_Path_Component`]: self
pub(crate) fn safe(topic: &str) -> String {
    // At least one byte per input byte; escaped bytes take three.
    let mut out = String::with_capacity(topic.len());
    for &byte in topic.as_bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' {
            out.push(byte as char);
        } else {
            // `_` + two UPPER-CASE hex digits. The leading `_` is always
            // followed by a hex digit, so the output never contains `__`.
            out.push('_');
            out.push(upper_hex_nibble(byte >> 4));
            out.push(upper_hex_nibble(byte & 0x0f));
        }
    }
    out
}

/// Map a 4-bit nibble (`0..=15`) to its upper-case hexadecimal digit.
fn upper_hex_nibble(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// Derive the [`Partition_Data_Path`] for one durable client partition.
///
/// `data_dir / safe(topic) / partition`. Distinct `(topic, partition)` pairs
/// never resolve to the same path (the topic component is injective and the
/// partition is appended as its own component), and the same pair always
/// resolves to the identical path so its segments are reopened after a restart
/// (Requirement 7.1, 7.2, 7.4).
///
/// [`Partition_Data_Path`]: self
pub(crate) fn partition_data_path(data_dir: &Path, topic: &str, partition: u32) -> PathBuf {
    data_dir.join(safe(topic)).join(partition.to_string())
}

/// Derive the fixed [`Partition_Data_Path`] for the durable metadata Raft group.
///
/// `data_dir / "__meta" / "0"`. The `__meta` component is the reserved literal
/// [`METADATA_COMPONENT`], which no client topic's [`safe`] output can equal or
/// contain, so this path never collides with a client partition path
/// (Requirement 16.2-16.5).
///
/// [`Partition_Data_Path`]: self
pub(crate) fn metadata_data_path(data_dir: &Path) -> PathBuf {
    data_dir.join(METADATA_COMPONENT).join("0")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Every character of `s` is drawn from the safe set `[A-Za-z0-9-_]`.
    fn is_safe_component(s: &str) -> bool {
        s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    }

    #[test]
    fn reserved_metadata_component_is_a_safe_path_component() {
        // The reserved metadata component is the LITERAL `__meta` used directly
        // by `metadata_data_path` (NOT `safe("__meta")`, which would escape the
        // underscores). It must itself be a valid Safe_Path_Component so it can
        // be used directly as a path component, while client topics can never
        // collide with it because `safe()` output never contains `__`
        // (Requirement 16.2, 16.3).
        assert_eq!(METADATA_COMPONENT, "__meta");
        assert!(
            is_safe_component(METADATA_COMPONENT),
            "reserved `__meta` must consist only of safe-set characters"
        );
    }

    #[test]
    fn safe_passes_through_unreserved_bytes_and_escapes_others() {
        // Alphanumerics and `-` pass through; `_` and other bytes are escaped.
        assert_eq!(safe("abcXYZ-09"), "abcXYZ-09");
        // `_` is 0x5F -> `_5F`; `.` is 0x2E -> `_2E`.
        assert_eq!(safe("a_b.c"), "a_5Fb_2Ec");
        // A multibyte UTF-8 char escapes each of its bytes (é = 0xC3 0xA9).
        assert_eq!(safe("é"), "_C3_A9");
        // The client sanitizer escapes the underscores in `__meta`, so it can
        // never produce the reserved literal component.
        assert_ne!(safe("__meta"), METADATA_COMPONENT);
        assert!(!safe("__meta").contains("__"));
    }

    /// Arbitrary topic strings, intentionally including bytes outside the safe
    /// set (underscores, dots, slashes, whitespace, and non-ASCII), so the
    /// escape is exercised across the full input space.
    fn topic_strategy() -> impl Strategy<Value = String> {
        prop_oneof![
            // Mixed printable + unsafe ASCII.
            "[a-zA-Z0-9._/ -]{0,32}",
            // Anything, including arbitrary Unicode / control bytes.
            ".{0,32}",
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Feature: per-topic-log-durability, Property 6
        ///
        /// Path derivation is injective, stable, and rooted: distinct
        /// `(topic, partition)` pairs derive different paths, the same pair
        /// derives identical paths on repeat, and every derived path is beneath
        /// the configured data directory.
        ///
        /// **Validates: Requirements 6.3, 7.1, 7.2, 7.4**
        #[test]
        fn path_derivation_is_injective_stable_and_rooted(
            topic_a in topic_strategy(),
            partition_a in any::<u32>(),
            topic_b in topic_strategy(),
            partition_b in any::<u32>(),
        ) {
            let data_dir = Path::new("/var/lib/vela");

            let path_a = partition_data_path(data_dir, &topic_a, partition_a);
            let path_b = partition_data_path(data_dir, &topic_b, partition_b);

            // Stable: deriving the same pair twice yields identical paths.
            prop_assert_eq!(
                &path_a,
                &partition_data_path(data_dir, &topic_a, partition_a),
                "derivation must be deterministic for ({:?}, {})",
                topic_a,
                partition_a
            );

            // Injective: distinct pairs derive different paths; identical pairs
            // derive equal paths. `safe` is an injective encoding and the
            // partition is appended as its own component, so the (topic,
            // partition) pair is recoverable from the path.
            if (topic_a.as_str(), partition_a) == (topic_b.as_str(), partition_b) {
                prop_assert_eq!(&path_a, &path_b);
            } else {
                prop_assert_ne!(
                    &path_a,
                    &path_b,
                    "distinct pairs ({:?}, {}) and ({:?}, {}) must derive different paths",
                    topic_a,
                    partition_a,
                    topic_b,
                    partition_b
                );
            }

            // Rooted: every derived path is beneath the data directory.
            prop_assert!(
                path_a.starts_with(data_dir),
                "{:?} must be rooted at {:?}",
                path_a,
                data_dir
            );
            prop_assert!(
                path_b.starts_with(data_dir),
                "{:?} must be rooted at {:?}",
                path_b,
                data_dir
            );
        }

        /// Feature: per-topic-log-durability, Property 7
        ///
        /// The derived topic component is always a valid Safe_Path_Component:
        /// for any topic-name byte string — including bytes outside the safe
        /// set such as `_`, punctuation, whitespace, control bytes, and the
        /// bytes of arbitrary multibyte Unicode — `safe(topic)` emits only
        /// characters drawn from `[A-Za-z0-9-_]`.
        ///
        /// **Validates: Requirements 7.3**
        #[test]
        fn safe_emits_only_safe_set_characters(topic in topic_strategy()) {
            let component = safe(&topic);
            prop_assert!(
                is_safe_component(&component),
                "safe({:?}) = {:?} contains a character outside [A-Za-z0-9-_]",
                topic,
                component
            );
        }

        /// Feature: per-topic-log-durability, Property 8
        ///
        /// Client paths never collide with the reserved metadata path: for any
        /// topic name (including the literal `__meta`, which `topic_strategy`
        /// can generate) and any partition index, the derived client
        /// `partition_data_path` differs from `metadata_data_path`. The
        /// underlying reason is that `safe(topic)` never equals the reserved
        /// `__meta` component and never contains the `__` substring that
        /// component leads with, so the topic component of a client path can
        /// never match the reserved metadata component.
        ///
        /// **Validates: Requirements 16.4, 16.5**
        #[test]
        fn client_paths_never_collide_with_metadata_path(
            topic in topic_strategy(),
            partition in any::<u32>(),
        ) {
            let data_dir = Path::new("/var/lib/vela");

            let client_path = partition_data_path(data_dir, &topic, partition);
            let metadata_path = metadata_data_path(data_dir);

            // The derived client path can never equal the reserved metadata path
            // (Requirement 16.4).
            prop_assert_ne!(
                &client_path,
                &metadata_path,
                "client path for ({:?}, {}) collided with the metadata path {:?}",
                topic,
                partition,
                metadata_path
            );

            // The underlying reason (Requirement 16.5): the sanitized topic
            // component never equals the reserved component, and never contains
            // the `__` prefix the reserved component leads with.
            let component = safe(&topic);
            prop_assert_ne!(
                &component,
                METADATA_COMPONENT,
                "safe({:?}) produced the reserved metadata component",
                topic
            );
            prop_assert!(
                !component.contains("__"),
                "safe({:?}) = {:?} contains the reserved `__` prefix",
                topic,
                component
            );
        }
    }
}
