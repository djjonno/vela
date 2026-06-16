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

// ---------------------------------------------------------------------------
// Durable-volume wiring (Requirements 15.1, 15.2, 15.3)
//
// These tests assert the docker-compose WIRING for per-node durability: every
// node service sets `VELA_DATA_DIR`, mounts its OWN distinct named volume at
// that data directory, and every such volume is declared in the top-level
// `volumes:` section.
//
// The end-to-end "volume round-trip" described in tasks.md (produce to a
// durable topic, restart the container with the volume reattached, consume the
// same records at the same offsets) requires a real Docker daemon, which is not
// available in this test environment. That behaviour is already exercised at
// the process level by the cold-restart integration test (task 15). Here we
// only assert the static compose wiring, in the same dependency-free,
// text-parsing style as the tests above (no YAML crate).

/// Count the leading ASCII spaces on a line, used to tell a top-level key
/// (column 0) from an indented one inside a service block.
fn leading_spaces(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// A named-volume mount entry from a service `volumes:` block, e.g.
/// `node1-data:/var/lib/vela` -> `{ volume: "node1-data", target: "/var/lib/vela" }`.
#[derive(Debug)]
struct VolumeMount {
    volume: String,
    target: String,
}

/// One parsed node service's durability wiring.
#[derive(Debug)]
struct NodeVolumeConfig {
    node_id: String,
    /// Value of `VELA_DATA_DIR` for the service, if present.
    data_dir: Option<String>,
    /// Named-volume mounts declared in the service's `volumes:` block.
    mounts: Vec<VolumeMount>,
}

/// Parse a single `volume:target` mount entry (the text after the leading `-`).
///
/// Returns `None` for bind mounts or malformed entries; the compose file only
/// uses `<named-volume>:<container-path>` form here.
fn parse_volume_mount(item: &str) -> Option<VolumeMount> {
    let item = item.trim().trim_matches(|c| c == '"' || c == '\'');
    let (volume, target) = item.split_once(':')?;
    let volume = volume.trim();
    let target = target.trim();
    if volume.is_empty() || target.is_empty() {
        return None;
    }
    Some(VolumeMount {
        volume: volume.to_string(),
        target: target.to_string(),
    })
}

/// Parse each node service's `VELA_DATA_DIR` and its `volumes:` mounts.
///
/// We walk the file top-to-bottom like `parse_node_configs`: a `VELA_NODE_ID`
/// opens a new node, and the `VELA_DATA_DIR` and named-volume mounts that follow
/// it (until the next node) attach to it. To avoid mistaking a `ports:` list
/// item (`- "7001:7001"`) for a volume mount, we only treat a `- ...` entry as a
/// mount when the most recent block header was a service-level `volumes:`. The
/// top-level `volumes:` section (column 0) is excluded so its declarations are
/// not read as mounts.
fn parse_node_volume_configs(compose: &str) -> Vec<NodeVolumeConfig> {
    let mut nodes: Vec<NodeVolumeConfig> = Vec::new();
    let mut last_header: Option<String> = None;
    let mut in_top_level_volumes = false;

    for raw_line in compose.lines() {
        let line = strip_comment(raw_line);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Track top-level section boundaries (column 0). A top-level `volumes:`
        // section holds declarations, not service mounts.
        if leading_spaces(line) == 0 {
            in_top_level_volumes = trimmed == "volumes:";
        }

        // Remember the most recent pure block header (a key with no value,
        // e.g. `volumes:`, `ports:`, `environment:`) so list items below can be
        // attributed to the right block.
        if let Some(header) = trimmed.strip_suffix(':') {
            if !header.contains(char::is_whitespace) {
                last_header = Some(header.to_string());
            }
        }

        if let Some(value) = field_value(trimmed, "VELA_NODE_ID") {
            nodes.push(NodeVolumeConfig {
                node_id: value.to_string(),
                data_dir: None,
                mounts: Vec::new(),
            });
            continue;
        }

        if let Some(value) = field_value(trimmed, "VELA_DATA_DIR") {
            if let Some(node) = nodes.last_mut() {
                node.data_dir = Some(value.to_string());
            }
            continue;
        }

        // A `- entry` under a service-level `volumes:` block is a mount.
        if !in_top_level_volumes && last_header.as_deref() == Some("volumes") {
            if let Some(item) = trimmed.strip_prefix('-') {
                if let Some(mount) = parse_volume_mount(item) {
                    if let Some(node) = nodes.last_mut() {
                        node.mounts.push(mount);
                    }
                }
            }
        }
    }

    nodes
}

/// Collect the volume names declared in the top-level `volumes:` section.
///
/// The section starts at a column-0 `volumes:` line and runs until the next
/// column-0 key; each indented entry (`node1-data:` or `node1-data: {}`)
/// contributes its name.
fn parse_top_level_volume_declarations(compose: &str) -> Vec<String> {
    let mut volumes: Vec<String> = Vec::new();
    let mut in_section = false;

    for raw_line in compose.lines() {
        let line = strip_comment(raw_line);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if leading_spaces(line) == 0 {
            in_section = trimmed == "volumes:";
            continue;
        }

        if in_section {
            let name = trimmed.split_once(':').map(|(n, _)| n).unwrap_or(trimmed);
            let name = name.trim();
            if !name.is_empty() {
                volumes.push(name.to_string());
            }
        }
    }

    volumes
}

#[test]
fn compose_sets_data_dir_for_every_node_service() {
    let root = workspace_root();
    let contents = read_artifact(&root.join("docker-compose.yml"));
    let nodes = parse_node_volume_configs(&contents);

    assert!(
        nodes.len() >= 2,
        "need at least two node services to verify data-dir wiring, found {}",
        nodes.len()
    );

    for node in &nodes {
        let data_dir = node.data_dir.as_deref().unwrap_or("");
        assert!(
            !data_dir.is_empty(),
            "node `{}` must set VELA_DATA_DIR to an in-container path \
             (Requirement 15.1), found {:?}",
            node.node_id,
            node.data_dir
        );
    }
}

#[test]
fn compose_mounts_distinct_declared_per_node_volume_at_data_dir() {
    let root = workspace_root();
    let contents = read_artifact(&root.join("docker-compose.yml"));
    let nodes = parse_node_volume_configs(&contents);
    let declared = parse_top_level_volume_declarations(&contents);

    assert!(
        nodes.len() >= 2,
        "need at least two node services to verify per-node volume wiring, found {}",
        nodes.len()
    );

    // Each node must mount a named volume exactly at its own data directory, and
    // the volume names must be distinct per node (no shared storage).
    let mut used_volumes: Vec<&str> = Vec::new();

    for node in &nodes {
        let data_dir = node
            .data_dir
            .as_deref()
            .unwrap_or_else(|| panic!("node `{}` is missing VELA_DATA_DIR", node.node_id));

        let mount_at_data_dir = node
            .mounts
            .iter()
            .find(|m| m.target == data_dir)
            .unwrap_or_else(|| {
                panic!(
                    "node `{}` must mount a named volume at its data directory `{}` \
                     (Requirement 15.2), found mounts {:?}",
                    node.node_id, data_dir, node.mounts
                )
            });

        // The volume backing this node must be unique to it.
        assert!(
            !used_volumes.contains(&mount_at_data_dir.volume.as_str()),
            "node `{}` reuses named volume `{}`; each node must mount its OWN \
             distinct volume (Requirement 15.2)",
            node.node_id,
            mount_at_data_dir.volume
        );
        used_volumes.push(mount_at_data_dir.volume.as_str());

        // And that volume must be declared in the top-level `volumes:` section.
        assert!(
            declared.iter().any(|v| v == &mount_at_data_dir.volume),
            "named volume `{}` mounted by node `{}` must be declared in the \
             top-level volumes: section (Requirement 15.2), declared: {:?}",
            mount_at_data_dir.volume,
            node.node_id,
            declared
        );
    }

    assert_eq!(
        used_volumes.len(),
        nodes.len(),
        "every node service should mount exactly one distinct per-node volume"
    );
}
