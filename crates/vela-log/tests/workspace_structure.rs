//! Structural smoke test for the Vela Cargo workspace.
//!
//! This test parses the workspace and member manifests directly (no build-graph
//! introspection) and asserts two structural invariants from the requirements:
//!
//! * The workspace root declares **exactly nine** member crates (Requirement
//!   1.1) — the original seven plus `vela-sim`, the deterministic-simulation
//!   harness crate, and `vela-bench`, the throughput-benchmark crate.
//! * The innermost crates `vela-log` and `vela-raft` declare **no forbidden
//!   (outward) Vela dependencies** — `vela-log` depends on no other Vela crate,
//!   and `vela-raft` depends only on `vela-log` (Requirements 1.2, 1.4).
//!
//! The test is intentionally dependency-free: it reads the `Cargo.toml` files as
//! text and extracts just the `[workspace] members` array and the dependency-table
//! keys it needs, so it stays deterministic and offline.

use std::path::{Path, PathBuf};

/// The exact set of member crates the workspace must declare (Requirement 1.1).
///
/// `vela-sim` is the deterministic-simulation harness crate and `vela-bench` is
/// the throughput-benchmark crate; both are legitimate workspace members that
/// depend inward only (`vela-core → vela-raft → vela-log`) and never on
/// `vela-server`.
const EXPECTED_MEMBERS: [&str; 9] = [
    "crates/vela-log",
    "crates/vela-raft",
    "crates/vela-proto",
    "crates/vela-core",
    "crates/vela-server",
    "crates/vela-client",
    "crates/vela-ctl",
    "crates/vela-sim",
    "crates/vela-bench",
];

/// Every Vela crate name, used to detect outward (forbidden) dependency edges.
const VELA_CRATES: [&str; 9] = [
    "vela-log",
    "vela-raft",
    "vela-proto",
    "vela-core",
    "vela-server",
    "vela-client",
    "vela-ctl",
    "vela-sim",
    "vela-bench",
];

/// Resolve the workspace root from this crate's manifest directory.
///
/// `CARGO_MANIFEST_DIR` points at `.../crates/vela-log`; the workspace root is two
/// levels up.
fn workspace_root() -> PathBuf {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir
        .parent() // .../crates
        .and_then(Path::parent) // workspace root
        .expect("vela-log crate should live two levels below the workspace root")
        .to_path_buf()
}

fn read_manifest(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read manifest {}: {e}", path.display()))
}

/// Strip a trailing `#` comment from a single manifest line.
fn strip_comment(line: &str) -> &str {
    match line.find('#') {
        Some(idx) => &line[..idx],
        None => line,
    }
}

/// Extract the string entries of the `members = [ ... ]` array under `[workspace]`.
///
/// Handles a multi-line array with interleaved comments, which is how the root
/// manifest is written.
fn parse_workspace_members(manifest: &str) -> Vec<String> {
    // Find the `members` key and the opening bracket of its array value.
    let members_pos = manifest
        .find("members")
        .expect("workspace manifest must declare a `members` array");
    let after_members = &manifest[members_pos..];
    let open = after_members
        .find('[')
        .expect("`members` must be assigned an array");
    let close = after_members
        .find(']')
        .expect("`members` array must be closed with `]`");
    let body = &after_members[open + 1..close];

    body.split(',')
        .map(|raw| strip_comment(raw).trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_matches(|c| c == '"' || c == '\'').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Collect the dependency names declared in any dependency table of a member
/// manifest (`[dependencies]`, `[dev-dependencies]`, `[build-dependencies]`, and
/// their `[target.*]` variants).
///
/// Dependencies in these manifests are written as dotted keys such as
/// `vela-log.workspace = true` or `thiserror.workspace = true`; the dependency
/// name is the segment before the first `.`, whitespace, or `=`.
fn parse_dependency_names(manifest: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut in_dep_table = false;

    for raw_line in manifest.lines() {
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        // Section header: decide whether the following keys are dependencies.
        if line.starts_with('[') {
            let header = line.trim_matches(|c| c == '[' || c == ']').trim();
            // Matches `dependencies`, `dev-dependencies`, `build-dependencies`,
            // and target-specific tables like `target.'cfg(..)'.dependencies`.
            in_dep_table = header
                .rsplit('.')
                .next()
                .map(|last| last.ends_with("dependencies"))
                .unwrap_or(false);
            continue;
        }

        if !in_dep_table {
            continue;
        }

        // Extract the key (dependency name) before `.`, whitespace, or `=`.
        let key_end = line
            .find(|c: char| c == '.' || c == '=' || c.is_whitespace())
            .unwrap_or(line.len());
        let name = line[..key_end].trim().trim_matches('"');
        if !name.is_empty() {
            names.push(name.to_string());
        }
    }

    names
}

#[test]
fn workspace_declares_exactly_nine_expected_members() {
    let root = workspace_root();
    let manifest = read_manifest(&root.join("Cargo.toml"));
    let members = parse_workspace_members(&manifest);

    assert_eq!(
        members.len(),
        9,
        "workspace must declare exactly nine member crates, found {}: {:?}",
        members.len(),
        members
    );

    for expected in EXPECTED_MEMBERS {
        assert!(
            members.iter().any(|m| m == expected),
            "workspace members must include `{expected}`; found {members:?}"
        );
    }
}

/// Returns the Vela crate dependencies declared by `crate_dir`'s manifest.
fn vela_dependencies_of(root: &Path, crate_dir: &str) -> Vec<String> {
    let manifest = read_manifest(&root.join(crate_dir).join("Cargo.toml"));
    parse_dependency_names(&manifest)
        .into_iter()
        .filter(|name| VELA_CRATES.contains(&name.as_str()))
        .collect()
}

#[test]
fn vela_log_declares_no_vela_dependencies() {
    let root = workspace_root();
    let vela_deps = vela_dependencies_of(&root, "crates/vela-log");

    assert!(
        vela_deps.is_empty(),
        "vela-log is an innermost crate and must declare no Vela dependencies, \
         but found: {vela_deps:?}"
    );
}

#[test]
fn vela_raft_depends_only_inward_on_vela_log() {
    let root = workspace_root();
    let vela_deps = vela_dependencies_of(&root, "crates/vela-raft");

    // vela-raft -> vela-log is the only permitted inward edge (Requirement 1.3);
    // any other Vela dependency would be an outward/forbidden edge.
    let forbidden: Vec<&String> = vela_deps.iter().filter(|d| *d != "vela-log").collect();

    assert!(
        forbidden.is_empty(),
        "vela-raft may only depend on vela-log among Vela crates, \
         but also declares forbidden outward dependencies: {forbidden:?}"
    );
}
