//! Drift guard for the Guarantee_Specification (`GUARANTEES.md`, task 24.1)
//! against the canonical property list (`PropertyId`, Requirement 16.2).
//!
//! `GUARANTEES.md` maps each durability / ordering / availability guarantee to
//! the property that checks it, using the fully-qualified token form
//! `PropertyId::Variant`. This test parses every such token out of the document
//! and asserts the mapping cannot silently drift away from the code:
//!
//! - **No property left unmapped (Req 16.2):** every variant in
//!   [`PropertyId::ALL`] appears at least once in the document, so a property
//!   the suite checks is always documented.
//! - **No phantom property:** every `PropertyId::X` token in the document names
//!   a real variant, so the document never references a property that does not
//!   exist (e.g. after a rename or removal).
//!
//! The test owns no second copy of the property list — it derives the set of
//! valid identifiers from `PropertyId::ALL` via `Debug`, so adding/removing a
//! variant automatically reshapes what the document must contain.

#![cfg(feature = "sim")]

use std::collections::BTreeSet;

use vela_sim::checker::PropertyId;

/// The literal prefix every canonical property reference uses in the document.
const PREFIX: &str = "PropertyId::";

/// The metavariable the document uses to describe the *form* of a canonical
/// reference (e.g. "written in the fully-qualified form `PropertyId::Variant`").
/// It is a documented placeholder, not a claim that a `Variant` property
/// exists, so the phantom-property check ignores it. Any other unknown name
/// still fails the check.
const PLACEHOLDER_IDENT: &str = "Variant";

/// Read the committed `GUARANTEES.md` next to this crate's `Cargo.toml`.
fn read_guarantees() -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/GUARANTEES.md");
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read GUARANTEES.md at {path}: {e}"))
}

/// The set of valid variant identifier spellings (e.g. `"ElectionSafety"`),
/// derived from [`PropertyId::ALL`] via `Debug` so there is no second list to
/// keep in sync.
fn valid_idents() -> BTreeSet<String> {
    PropertyId::ALL.iter().map(|p| format!("{p:?}")).collect()
}

/// Scan `doc` for every `PropertyId::<Ident>` token and return the set of
/// identifier strings that followed the prefix (e.g. `"ElectionSafety"`).
///
/// An identifier is the run of ASCII alphanumeric / `_` characters immediately
/// after the `PropertyId::` prefix; a prefix followed by no identifier char
/// contributes the empty string, which is reported as an invalid token.
fn mentioned_idents(doc: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut rest = doc;
    while let Some(pos) = rest.find(PREFIX) {
        let after = &rest[pos + PREFIX.len()..];
        let ident: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        found.insert(ident);
        // Advance past this prefix occurrence to find the next one.
        rest = after;
    }
    found
}

#[test]
fn every_property_is_documented() {
    let doc = read_guarantees();
    let mentioned = mentioned_idents(&doc);

    let missing: Vec<String> = PropertyId::ALL
        .iter()
        .map(|p| format!("{p:?}"))
        .filter(|ident| !mentioned.contains(ident))
        .collect();

    assert!(
        missing.is_empty(),
        "GUARANTEES.md does not mention these checked properties (Req 16.2 — \
         every property the suite defines must be mapped): {}. Add a \
         `PropertyId::<name>` mapping for each.",
        missing.join(", ")
    );
}

#[test]
fn no_documented_property_is_phantom() {
    let doc = read_guarantees();
    let valid = valid_idents();
    let mentioned = mentioned_idents(&doc);

    let unknown: Vec<String> = mentioned
        .into_iter()
        .filter(|ident| !valid.contains(ident) && ident != PLACEHOLDER_IDENT)
        .map(|ident| {
            if ident.is_empty() {
                "`PropertyId::` with no variant name".to_string()
            } else {
                format!("PropertyId::{ident}")
            }
        })
        .collect();

    assert!(
        unknown.is_empty(),
        "GUARANTEES.md names properties that are not `PropertyId` variants: \
         {}. Valid variants are: {}.",
        unknown.join(", "),
        valid_idents()
            .into_iter()
            .map(|i| format!("PropertyId::{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
}
