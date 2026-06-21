//! Property test for the `Member` wire round-trip in `vela-server`.
//!
//! Feature: advertised-listeners, Property 4: Member round-trips through proto
//! preserving both addresses. For any domain `Member` (including ones whose
//! `advertised_addr` equals its `addr`), converting to the wire `v1::Member` and
//! back yields a `Member` equal to the original — both `addr` and
//! `advertised_addr` are preserved and never conflated — and `to_proto` places
//! the listen address on `addr` and the advertised address on `advertised_addr`.
//!
//! The generator produces members with independently-varying `addr` and
//! `advertised_addr` strings (and both availability states), so a conversion
//! that dropped, swapped, or conflated the two address fields would be caught. A
//! companion case forces `advertised_addr == addr` to cover the unconfigured
//! shape explicitly.
//!
//! Validates: Requirements 3.2, 3.3, 5.2, 5.3

use proptest::prelude::*;

use vela_core::{Member, NodeAvailability, NodeId};
use vela_server::convert::{member_from_proto, member_to_proto};

/// Generate an availability state.
fn availability_strategy() -> impl Strategy<Value = NodeAvailability> {
    prop_oneof![
        Just(NodeAvailability::Available),
        Just(NodeAvailability::Unavailable),
    ]
}

/// Generate an address-like token. Empty is permitted so the round-trip is
/// exercised across the empty/non-empty boundary the wire defaulting hinges on.
fn addr_strategy() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Za-z0-9_.:-]{0,40}").expect("valid regex")
}

/// Generate a domain [`Member`] with independently-varying bind and advertised
/// addresses.
fn member_strategy() -> impl Strategy<Value = Member> {
    (
        proptest::string::string_regex("[A-Za-z0-9_-]{0,24}").expect("valid regex"),
        addr_strategy(),
        addr_strategy(),
        availability_strategy(),
    )
        .prop_map(|(id, addr, advertised_addr, availability)| Member {
            id: NodeId::new(id),
            addr,
            advertised_addr,
            availability,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Feature: advertised-listeners, Property 4
    #[test]
    fn member_round_trips_preserving_both_addresses(member in member_strategy()) {
        let proto = member_to_proto(&member);

        // to_proto places the listen address on `addr` and the advertised
        // address on `advertised_addr`, never conflating the two (Req 5.2).
        prop_assert_eq!(&proto.addr, &member.addr);
        prop_assert_eq!(&proto.advertised_addr, &member.advertised_addr);

        // The full domain -> wire -> domain round-trip is lossless (Req 3.2, 3.3).
        let back = member_from_proto(&proto);
        prop_assert_eq!(back, member);
    }

    // Feature: advertised-listeners, Property 4 (advertised == addr)
    #[test]
    fn member_with_equal_addresses_round_trips(
        id in proptest::string::string_regex("[A-Za-z0-9_-]{0,24}").expect("valid regex"),
        addr in addr_strategy(),
        availability in availability_strategy(),
    ) {
        // When advertised equals the bind address (the unconfigured shape), both
        // wire fields carry that same value and the round-trip still preserves it
        // (Req 5.3).
        let member = Member {
            id: NodeId::new(id),
            addr: addr.clone(),
            advertised_addr: addr,
            availability,
        };
        let proto = member_to_proto(&member);
        prop_assert_eq!(&proto.addr, &proto.advertised_addr);
        prop_assert_eq!(member_from_proto(&proto), member);
    }
}
