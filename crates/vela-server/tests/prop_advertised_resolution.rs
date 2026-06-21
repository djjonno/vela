//! Property test for advertised-address resolution in `vela-server` config.
//!
//! Feature: advertised-listeners, Property 1: Advertised address resolution
//! defaults to listen and trims input. For any otherwise-valid `CliArgs` and any
//! advertised input value, the resolved `Config.advertised_addr` equals the
//! input trimmed of surrounding whitespace when that trimmed value is non-empty,
//! and equals `listen_addr.to_string()` when the input is absent or blank after
//! trimming.
//!
//! The generators constrain inputs to the space the property quantifies over: a
//! bindable IPv4 `listen_addr` (so the resolved listen string is well-defined),
//! and an advertised input drawn from the shapes that matter — absent, blank /
//! whitespace-only, a padded value, and an arbitrary string — fed through the
//! public `Config::from_cli` resolution path with all other required fields
//! held valid.
//!
//! Validates: Requirements 1.1, 1.2, 1.3, 1.5, 2.3

use proptest::prelude::*;

use vela_server::{CliArgs, Config};

/// Generate a bindable IPv4 `host:port` listen address string. The host octets
/// and a non-zero port are arbitrary; the address only needs to parse as a
/// `SocketAddr`, which every `a.b.c.d:port` with `port >= 1` does.
fn listen_addr_strategy() -> impl Strategy<Value = String> {
    (
        any::<u8>(),
        any::<u8>(),
        any::<u8>(),
        any::<u8>(),
        1u16..=u16::MAX,
    )
        .prop_map(|(a, b, c, d, port)| format!("{a}.{b}.{c}.{d}:{port}"))
}

/// Generate an advertised input value covering the cases the resolution rule
/// distinguishes: absent (`None`), blank/whitespace-only, a value with
/// surrounding whitespace, and an arbitrary string.
fn advertised_input_strategy() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        Just(None),
        proptest::string::string_regex("[ \\t\\r\\n]{0,6}")
            .expect("valid regex")
            .prop_map(Some),
        proptest::string::string_regex("[ \\t]{0,3}[A-Za-z0-9_.:-]{1,40}[ \\t]{0,3}")
            .expect("valid regex")
            .prop_map(Some),
        any::<String>().prop_map(Some),
    ]
}

/// Build an otherwise-valid set of [`CliArgs`] with the supplied listen address
/// and advertised input; all other required fields are held at valid values so
/// only the advertised resolution varies.
fn args(listen_addr: &str, advertised: Option<String>) -> CliArgs {
    CliArgs {
        node_id: Some("node-a".to_string()),
        listen_addr: Some(listen_addr.to_string()),
        advertised_addr: advertised,
        peers: Vec::new(),
        replication_factor: Some("1".to_string()),
        data_dir: Some("/var/lib/vela".to_string()),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: advertised-listeners, Property 1
    #[test]
    fn advertised_resolution_defaults_to_listen_and_trims(
        listen in listen_addr_strategy(),
        advertised in advertised_input_strategy(),
    ) {
        let config = Config::from_cli(args(&listen, advertised.clone()))
            .expect("an otherwise-valid config must parse regardless of advertised input");

        // The resolution rule the property pins down: a non-empty trimmed value
        // is recorded verbatim (Req 1.1, 1.3); otherwise the advertised address
        // defaults to the resolved listen address (Req 1.2). The resolved value
        // is always exposed on the validated Config (Req 1.5, 2.3).
        let expected = match advertised.as_deref().map(str::trim) {
            Some(trimmed) if !trimmed.is_empty() => trimmed.to_string(),
            _ => config.listen_addr.to_string(),
        };
        prop_assert_eq!(config.advertised_addr, expected);
    }
}
