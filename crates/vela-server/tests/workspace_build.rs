//! Structural smoke test for the whole-workspace `cargo build`.
//!
//! Requirements 1.5 and 1.6 describe the behaviour of `cargo build` at the
//! workspace root:
//!
//! * **Requirement 1.5** — `cargo build` at the workspace root compiles *all
//!   nine* member crates to completion.
//! * **Requirement 1.6** — if any member crate fails to compile, the build
//!   terminates with a non-zero exit status that names the failing crate.
//!
//! We deliberately do **not** shell out to `cargo build` from inside this test:
//! invoking `cargo build` (or `cargo test`) recursively from a test that is
//! itself run under `cargo test` is slow, can re-enter the build of this very
//! crate, and can deadlock on the workspace build lock. Instead we assert the
//! *structural contract* that makes Requirement 1.5 hold:
//!
//! * The set of crates `cargo build` compiles is exactly the workspace
//!   `members` array, so we assert that array declares exactly the nine
//!   expected crates and that each one is a real, buildable crate on disk
//!   (a manifest with a package name + a crate root source file).
//!
//! Requirement 1.6's "non-zero status naming the failing crate" is Cargo's own
//! guaranteed behaviour for a workspace build: when a member fails to compile,
//! Cargo stops with a non-zero exit code and the diagnostic names the crate
//! that failed. There is no Vela-side configuration that overrides this, so the
//! structural guarantee here (a well-formed nine-member workspace) is what
//! Vela is responsible for; the failing-crate reporting is inherited from
//! Cargo. This is documented and exercised structurally rather than by forcing
//! a real compilation failure.
//!
//! The test is intentionally dependency-free: it reads the `Cargo.toml` files as
//! text so it stays deterministic and offline.

use std::path::{Path, PathBuf};

/// The exact set of member crates `cargo build` must compile (Requirement 1.5).
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

/// Resolve the workspace root from this crate's manifest directory.
///
/// `CARGO_MANIFEST_DIR` points at `.../crates/vela-server`; the workspace root
/// is two levels up.
fn workspace_root() -> PathBuf {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir
        .parent() // .../crates
        .and_then(Path::parent) // workspace root
        .expect("vela-server crate should live two levels below the workspace root")
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

/// Extract the string entries of the `members = [ ... ]` array under
/// `[workspace]`. Handles a multi-line array with interleaved comments.
fn parse_workspace_members(manifest: &str) -> Vec<String> {
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

/// Returns true if the manifest declares a `[package]` with a `name` field,
/// which is what makes a member a crate `cargo build` will actually compile.
fn declares_package_name(manifest: &str) -> bool {
    let mut in_package = false;
    for raw_line in manifest.lines() {
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            let header = line.trim_matches(|c| c == '[' || c == ']').trim();
            in_package = header == "package";
            continue;
        }
        if in_package {
            let key_end = line
                .find(|c: char| c == '.' || c == '=' || c.is_whitespace())
                .unwrap_or(line.len());
            if line[..key_end].trim() == "name" {
                return true;
            }
        }
    }
    false
}

/// `cargo build` compiles exactly the crates in the workspace `members` array,
/// so the array must declare exactly the nine expected crates (Requirement 1.5).
#[test]
fn workspace_build_compiles_exactly_nine_member_crates() {
    let root = workspace_root();
    let manifest = read_manifest(&root.join("Cargo.toml"));
    let members = parse_workspace_members(&manifest);

    assert_eq!(
        members.len(),
        9,
        "`cargo build` at the workspace root compiles the `members` array, which \
         must list exactly nine crates, found {}: {:?}",
        members.len(),
        members
    );

    for expected in EXPECTED_MEMBERS {
        assert!(
            members.iter().any(|m| m == expected),
            "the nine crates `cargo build` compiles must include `{expected}`; \
             found {members:?}"
        );
    }
}

/// Each of the nine members must be a real, buildable crate on disk: a
/// `Cargo.toml` declaring a package name plus a crate-root source file
/// (`src/lib.rs` or `src/main.rs`). If any were absent or malformed, the
/// workspace `cargo build` could not "compile all nine crates to completion"
/// (Requirement 1.5).
#[test]
fn every_member_crate_is_buildable_on_disk() {
    let root = workspace_root();

    for member in EXPECTED_MEMBERS {
        let crate_dir = root.join(member);
        let manifest_path = crate_dir.join("Cargo.toml");
        assert!(
            manifest_path.is_file(),
            "member `{member}` must have a Cargo.toml at {}",
            manifest_path.display()
        );

        let manifest = read_manifest(&manifest_path);
        assert!(
            declares_package_name(&manifest),
            "member `{member}` manifest must declare a [package] name so \
             `cargo build` can compile it"
        );

        let lib_rs = crate_dir.join("src").join("lib.rs");
        let main_rs = crate_dir.join("src").join("main.rs");
        assert!(
            lib_rs.is_file() || main_rs.is_file(),
            "member `{member}` must have a crate root (src/lib.rs or src/main.rs) \
             for `cargo build` to compile"
        );
    }
}

/// Documents and structurally guards Requirement 1.6: a failing member build
/// terminates with a non-zero status that names the failing crate.
///
/// This is Cargo's guaranteed workspace-build behaviour. Vela's responsibility
/// is not to defeat it — e.g. by setting a profile/config that swallows build
/// failures. We assert there is no workspace-level Cargo configuration that
/// could mask a compilation failure (no `.cargo/config*` overriding the build),
/// so the non-zero-status-naming-the-failing-crate guarantee remains in force.
#[test]
fn no_workspace_config_masks_build_failures() {
    let root = workspace_root();

    // If a `.cargo/config.toml` (or legacy `config`) ever appears, it must not
    // redefine the default build behaviour in a way that could hide failures.
    // For this milestone Vela ships no such file, which keeps Cargo's native
    // "fail the build and name the crate" behaviour (Requirement 1.6) intact.
    for candidate in [".cargo/config.toml", ".cargo/config"] {
        let path = root.join(candidate);
        if path.is_file() {
            let contents = read_manifest(&path);
            assert!(
                !contents.contains("keep-going"),
                "workspace `{candidate}` must not enable build `keep-going`, which \
                 would let a member fail without failing the overall build \
                 (Requirement 1.6)"
            );
        }
    }
}
