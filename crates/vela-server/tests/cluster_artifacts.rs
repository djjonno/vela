//! Structural smoke test for the local multi-node cluster artifacts.
//!
//! Task 18.3 asserts that the two artifacts that let an operator run a Vela
//! cluster locally exist and are wired correctly, without actually invoking
//! Docker:
//!
//! * A workspace-root `Dockerfile` exists and builds/runs the `velad` node
//!   daemon (Requirement 14.1).
//! * A workspace-root `docker-compose.yml` exists, declares **multiple** `velad`
//!   nodes, and gives each node a `VELA_PEERS` list that **cross-references the
//!   other nodes** — every node lists all the others and never itself
//!   (Requirement 14.2).
//!
//! The test is intentionally dependency-free: it reads both files as text and
//! extracts only the few fields it needs (no YAML crate), so it stays
//! deterministic and offline, matching the style of `vela-log`'s
//! `workspace_structure.rs` smoke test.

use std::path::{Path, PathBuf};

/// Resolve the workspace root from this crate's manifest directory.
///
/// `CARGO_MANIFEST_DIR` points at `.../crates/vela-server`; the workspace root
/// (where `Dockerfile` and `docker-compose.yml` live) is two levels up.
fn workspace_root() -> PathBuf {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir
        .parent() // .../crates
        .and_then(Path::parent) // workspace root
        .expect("vela-server crate should live two levels below the workspace root")
        .to_path_buf()
}

fn read_artifact(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read artifact {}: {e}", path.display()))
}

/// Strip a trailing `#` comment from a single line.
///
/// The compose file uses `#`-prefixed comment blocks; ignoring them keeps the
/// parser from mistaking commented-out text (e.g. the header that mentions
/// `velad`) for real configuration.
fn strip_comment(line: &str) -> &str {
    match line.find('#') {
        Some(idx) => &line[..idx],
        None => line,
    }
}

/// One parsed compose service: its node identity and the peer host names it
/// references via `VELA_PEERS`.
#[derive(Debug)]
struct NodeConfig {
    node_id: String,
    /// Host portions of each `host:port` entry in `VELA_PEERS`.
    peer_hosts: Vec<String>,
}

/// Parse the `VELA_NODE_ID` / `VELA_PEERS` pairs out of the compose file.
///
/// Every node service in the compose file declares both keys inside its
/// `environment:` block. We walk the lines top-to-bottom: each time we see a
/// `VELA_NODE_ID` we open a new node, and the `VELA_PEERS` that follows it
/// supplies that node's peer list. This avoids needing a full YAML parser while
/// still pairing the two values by the order they appear in each service block.
fn parse_node_configs(compose: &str) -> Vec<NodeConfig> {
    let mut nodes: Vec<NodeConfig> = Vec::new();
    let mut pending_id: Option<String> = None;

    for raw_line in compose.lines() {
        let line = strip_comment(raw_line);
        let trimmed = line.trim();

        if let Some(value) = field_value(trimmed, "VELA_NODE_ID") {
            // A new node block begins; record its id and await its peer list.
            pending_id = Some(value.to_string());
            continue;
        }

        if let Some(value) = field_value(trimmed, "VELA_PEERS") {
            let node_id = pending_id.take().unwrap_or_else(|| {
                panic!("found a VELA_PEERS entry with no preceding VELA_NODE_ID: {trimmed:?}")
            });
            let peer_hosts = value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|entry| {
                    // Each entry is `host:port`; keep the host portion.
                    entry
                        .split(':')
                        .next()
                        .expect("split always yields at least one segment")
                        .trim()
                        .to_string()
                })
                .collect();
            nodes.push(NodeConfig {
                node_id,
                peer_hosts,
            });
        }
    }

    nodes
}

/// If `line` declares `key`, return its value (handling both `KEY: value` and
/// `KEY=value` forms, with optional surrounding quotes).
fn field_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(key)?;
    // The character right after the key must be a separator, otherwise this is a
    // different key that merely shares a prefix.
    let rest = rest.trim_start();
    let value = rest.strip_prefix(':').or_else(|| rest.strip_prefix('='))?;
    Some(value.trim().trim_matches(|c| c == '"' || c == '\''))
}

#[test]
fn dockerfile_exists_and_builds_and_runs_velad() {
    let root = workspace_root();
    let dockerfile = root.join("Dockerfile");

    assert!(
        dockerfile.is_file(),
        "a workspace-root Dockerfile must exist (Requirement 14.1), expected at {}",
        dockerfile.display()
    );

    let contents = read_artifact(&dockerfile);

    // The Dockerfile must concern itself with the velad node daemon: it should
    // build the binary and run it as the container entrypoint.
    assert!(
        contents.contains("velad"),
        "the Dockerfile must build/run the `velad` node daemon (Requirement 14.1)"
    );
    assert!(
        contents.contains("cargo build") && contents.contains("vela-server"),
        "the Dockerfile must build the velad binary from the vela-server crate \
         (Requirement 14.1)"
    );
    assert!(
        contents.contains("ENTRYPOINT") || contents.contains("CMD"),
        "the Dockerfile must define how the velad daemon is run (ENTRYPOINT/CMD) \
         (Requirement 14.1)"
    );
}

#[test]
fn compose_declares_multiple_nodes() {
    let root = workspace_root();
    let compose = root.join("docker-compose.yml");

    assert!(
        compose.is_file(),
        "a workspace-root docker-compose.yml must exist (Requirement 14.2), expected at {}",
        compose.display()
    );

    let contents = read_artifact(&compose);
    let nodes = parse_node_configs(&contents);

    assert!(
        nodes.len() >= 2,
        "docker-compose.yml must declare multiple velad nodes wired into one \
         cluster (Requirement 14.2), found {}: {:?}",
        nodes.len(),
        nodes.iter().map(|n| &n.node_id).collect::<Vec<_>>()
    );

    // Node identities must be unique — duplicate ids would not form distinct
    // cluster members.
    let mut ids: Vec<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();
    ids.sort_unstable();
    let unique = {
        let mut u = ids.clone();
        u.dedup();
        u.len()
    };
    assert_eq!(
        unique,
        nodes.len(),
        "each compose node must have a distinct VELA_NODE_ID, found duplicates in {ids:?}"
    );
}

#[test]
fn compose_peer_lists_cross_reference_every_other_node() {
    let root = workspace_root();
    let contents = read_artifact(&root.join("docker-compose.yml"));
    let nodes = parse_node_configs(&contents);

    assert!(
        nodes.len() >= 2,
        "need at least two nodes to verify cross-referencing peer lists, found {}",
        nodes.len()
    );

    let all_ids: Vec<&str> = nodes.iter().map(|n| n.node_id.as_str()).collect();

    for node in &nodes {
        // A node must never list itself as a peer.
        assert!(
            !node.peer_hosts.iter().any(|p| p == &node.node_id),
            "node `{}` must not list itself in VELA_PEERS, found {:?}",
            node.node_id,
            node.peer_hosts
        );

        // A node must list every *other* node as a peer (full cross-reference).
        for other in &all_ids {
            if *other == node.node_id {
                continue;
            }
            assert!(
                node.peer_hosts.iter().any(|p| p == other),
                "node `{}` must reference peer `{}` in its VELA_PEERS, found {:?}",
                node.node_id,
                other,
                node.peer_hosts
            );
        }

        // And it must reference exactly the other nodes — no stray peers.
        assert_eq!(
            node.peer_hosts.len(),
            all_ids.len() - 1,
            "node `{}` should reference exactly the {} other node(s), found {:?}",
            node.node_id,
            all_ids.len() - 1,
            node.peer_hosts
        );
    }
}
