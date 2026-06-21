//! Property test for advertised-address acceptance in `vela-server` config.
//!
//! Feature: advertised-listeners, Property 2: Any non-empty advertised value is
//! accepted. For any non-empty advertised string — including wildcard hosts such
//! as `0.0.0.0:7001`, bare hostnames, or arbitrary `host:port` forms —
//! validating an otherwise-valid configuration succeeds and records that value;
//! configuration validation never rejects a non-empty advertised address.
//!
//! The generator covers the forms the requirement calls out explicitly
//! (wildcard host, wildcard `host:port`, bare hostname, arbitrary `host:port`)
//! plus an arbitrary non-empty, non-whitespace token, so each generated value is
//! non-empty after trimming and must therefore be recorded verbatim.
//!
//! Validates: Requirements 1.4

use proptest::prelude::*;

use vela_server::{CliArgs, Config};

/// Generate a non-empty advertised value (with no surrounding whitespace, so its
/// trimmed form is itself), spanning the shapes Requirement 1.4 calls out.
fn non_empty_advertised_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("0.0.0.0".to_string()),
        Just("0.0.0.0:7001".to_string()),
        proptest::string::string_regex("[a-z][a-z0-9-]{0,30}").expect("valid regex"),
        (
            any::<u8>(),
            any::<u8>(),
            any::<u8>(),
            any::<u8>(),
            1u16..=u16::MAX
        )
            .prop_map(|(a, b, c, d, port)| format!("{a}.{b}.{c}.{d}:{port}")),
        proptest::string::string_regex("[A-Za-z0-9_.:-]{1,40}").expect("valid regex"),
    ]
}

/// Build an otherwise-valid set of [`CliArgs`] carrying the supplied advertised
/// value.
fn args(advertised: &str) -> CliArgs {
    CliArgs {
        node_id: Some("node-a".to_string()),
        listen_addr: Some("127.0.0.1:7001".to_string()),
        advertised_addr: Some(advertised.to_string()),
        peers: Vec::new(),
        replication_factor: Some("1".to_string()),
        data_dir: Some("/var/lib/vela".to_string()),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: advertised-listeners, Property 2
    #[test]
    fn any_non_empty_advertised_value_is_accepted(advertised in non_empty_advertised_strategy()) {
        // Validation never rejects a non-empty advertised value (Req 1.4): the
        // config parses, and the value is recorded verbatim (no surrounding
        // whitespace was generated, so trimming is the identity here).
        let config = Config::from_cli(args(&advertised))
            .expect("a non-empty advertised value must never be rejected");
        prop_assert_eq!(config.advertised_addr, advertised);
    }
}
