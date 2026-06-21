# Design Document

## Overview

Vela nodes self-report a single network address in cluster metadata: the
`listen_addr` their gRPC listener binds on. A client that fills its node-id →
address registry through `DescribeCluster` therefore dials that bind address —
which in port-mapped, NAT, or Docker-published deployments (`0.0.0.0:7001`, or
an in-cluster hostname like `node2:7001`) is undialable from where the client
runs.

This feature adds a distinct **advertised address** per node — a
client-reachable `host:port` published in cluster metadata alongside the bind
address, mirroring Kafka's `advertised.listeners`. Each node may set
`--advertised-addr` / `VELA_ADVERTISED_ADDR`; when unset it falls back to the
bind address so zero-config deployments are byte-for-byte unchanged. The
advertised address is carried on a **new** wire field (`Member.advertised_addr`)
next to the existing `addr`, so old and new servers and clients interoperate.
The client prefers the advertised address when gap-filling its registry while
keeping operator-supplied `id=url` endpoints authoritative.

The change is deliberately small and threads through one well-worn seam at each
layer: config resolution (`vela-server`), the domain `Member`
(`vela-core`), the wire `Member` (`vela-proto` + `vela-server` convert), the
`DescribeCluster` handler, and the client's registry-seeding selector
(`vela-client`).

## How the advertised address propagates today (investigation)

The propagation requirement (Requirement 4) is framed around the `__meta`
Metadata_Group's committed view. Before committing to a mechanism I traced how a
member's **address** actually reaches every node today. The finding is decisive:

- **Member addresses are not agreed through the `__meta` Raft log.** The
  replicated `ClusterCommand` set is exactly `CreateTopic`, `DeleteTopic`,
  `SetAvailability` (`vela-core/src/model.rs`, `metadata.rs::apply_command`).
  None of them carries a member id → address mapping. `SetAvailability` only
  mutates the availability of an **already-present** member and is a no-op for
  an unknown node (`ClusterMetadata::set_availability` returns `false`).
- **The members list is seeded locally on each node.** In `NodeShared::new`
  the node pushes **its own** `Member` (`addr = config.listen_addr`), and
  `membership::register_peers` adds one `Member` per configured peer using the
  address from the local `--peers id@host:port` list. So every node already
  learns every other node's transport address purely from its **own** local
  configuration — never from the replicated log.
- **`DescribeCluster` reads that locally-seeded served view.** The handler in
  `service.rs` builds the response straight from `node.metadata.members`, and
  its own comment states: *"The durable `MetadataController`'s metadata carries
  only the committed topic catalogue (no `ClusterCommand` adds a member), so it
  would report no addresses."*

The consequence: for node B to return node A's advertised address from
`DescribeCluster`, node B must already hold it in its **local** members view —
which today means it must come from node B's local configuration, exactly as
node A's *bind* address does.

### Propagation options evaluated

| Option | Mechanism | Fit with existing flow | Verdict |
| --- | --- | --- | --- |
| (a) Config-carried | Each peer's advertised address travels in the local peer config, exactly as the peer's bind address does today; self advertised comes from `--advertised-addr`. | **Highest** — member addresses are already 100% local-config-derived; this extends the same path. | **Chosen** |
| (b) Runtime exchange (heartbeat / sync) | Peers swap advertised addresses at runtime. | Low — `HeartbeatRequest` carries only `node_id`; no member-address exchange exists. Requires a new exchange protocol. | Rejected (not minimal) |
| (c) Committed `RegisterMember` command | Add a membership `ClusterCommand` so member id→addr (and advertised) are agreed through `__meta`. | Medium — reuses the command log, but members are *not* committed today, so this introduces a whole new agreed-membership mechanism (registration, leader routing, recovery interplay). | Deferred — larger than this feature warrants |

**Decision (D1): Option (a) — carry the advertised address in local
configuration.** It is the only option that matches how member addresses
already reach every node, and it keeps the change minimal: no new RPC, no new
replicated command, no change to consensus. The advertised address lives on the
`Member` inside the served `ClusterMetadata` — the same structure the `__meta`
group's commits fold into and the same view `DescribeCluster` reads — so it
satisfies Requirement 4.2 (report from the served/applied view) directly.

**Explicit open decision (D2): peer-config grammar change.** Because peers'
advertised addresses must be locally known, the peer entry grammar must be able
to carry an advertised address. Today an entry is `id@host:port` (or a bare
`host:port`), and the list delimiter is a comma (clap `value_delimiter = ','`),
so a comma **cannot** separate the advertised address inside an entry (it would
be split into a second list element). The grammar is therefore extended to a
second `@`-delimited field:

```
id@listen[@advertised]      e.g.  node2@node2:7001@127.0.0.1:7002
host:port                   (bare; id = listen = advertised)
id@listen                   (advertised defaults to listen)
```

This is backward compatible: every existing `id@listen` and bare `host:port`
entry parses unchanged with `advertised = listen`.

**Explicit consequence/limitation (D3): cross-node consistency (Req 4.3) is
operator-driven, not Raft-agreed.** Two nodes report the same advertised address
for a member because operators configure it consistently, not because the
address is agreed through a quorum. This matches the existing behavior for the
*bind* address (also local-config-derived). If true consensus-agreed member
addresses are later required, Option (c) (a committed `RegisterMember` command)
is the upgrade path; it is out of scope here and noted as future work.

## Architecture

The data-flow diagram below shows how the advertised address moves config →
self/peer member → served metadata → `DescribeCluster` → client registry; the
lettered subsections under *Components and Interfaces* detail each component's
change.

```
                 ┌──────────────────────────────────────────────────────────┐
 CLI / env       │ vela-server                                                │
 --advertised-addr / VELA_ADVERTISED_ADDR                                     │
 --peers id@listen@advertised                                                 │
        │        │                                                            │
        ▼        │   Config::from_cli                                         │
  CliArgs ──────────► resolve: advertised = trim(value) or listen_addr        │
                 │        │                 (Req 1.1–1.5)                      │
                 │        ▼                                                    │
                 │   NodeShared::new          membership::register_peers      │
                 │   self Member{             peer Member{                     │
                 │     addr=listen,             addr=peer.listen,              │
                 │     advertised=cfg.adv }     advertised=peer.advertised }   │
                 │   (Req 2.1–2.3)            (D1 config-carried)              │
                 │        │                                                    │
                 │        ▼  served ClusterMetadata.members                    │
                 │   DescribeCluster ── member_to_proto ──► v1.Member{         │
                 │   (Req 4.2, 5.2)                            addr,           │
                 └─────────────────────────────────────────── advertised_addr }
                                                              │
                                                              ▼  wire (proto3)
                 ┌──────────────────────────────────────────────────────────┐
                 │ vela-client                                                │
                 │   seed_registry_from_cluster:                              │
                 │     pick = advertised_addr (if non-empty)                  │
                 │            else addr (if non-empty)                        │
                 │            else skip                  (Req 6.1, 6.2, 6.4)   │
                 │     registry.insert_if_absent(id, pick) (Req 6.3)          │
                 │   connection::normalize_addr adds http:// scheme           │
                 └──────────────────────────────────────────────────────────┘
```

Flow in words: configuration resolves the advertised address (or defaults it to
the listen address); the node stamps it onto its self `Member` and onto each
peer `Member` (from peer config); the served `ClusterMetadata.members` carries
both addresses; `DescribeCluster` serializes both onto the new wire field; the
client seeds its registry preferring the advertised address, only filling gaps.

## Data Models

These are the concrete data-shape changes; the layer-by-layer wiring follows in
*Components and Interfaces*.

### 1. `vela-proto` — `proto/vela.proto` `Member` (Req 5.1, 7.x)

Add `advertised_addr` as a **new** field. `addr` stays at field number 2 as the
internal/bind address; the next free number is 4 (`availability` holds 3).

```proto
// A cluster member node: its identity, internal/bind transport address, its
// client-reachable advertised address, and availability (Requirement 9.3,
// advertised-listeners 5.1).
message Member {
  string id = 1;
  string addr = 2;              // internal / bind / Listen_Address (unchanged)
  NodeAvailability availability = 3;
  string advertised_addr = 4;   // NEW: client-reachable address; empty => use addr
}
```

proto3 semantics give the compatibility for free (Req 7.1, 7.2): an old server
omits field 4, so a new client decodes `advertised_addr == ""` and falls back to
`addr`; an old client ignores the unknown field 4 and keeps reading `addr` at
field 2.

### 2. `vela-core` — `model.rs` `Member` (Req 3.1–3.3)

Add a distinct `advertised_addr: String` field next to `addr`:

```rust
pub struct Member {
    pub id: NodeId,
    /// Internal / bind transport address (host:port). Wire field `addr`.
    pub addr: String,
    /// Client-reachable advertised address (host:port). Distinct from `addr`;
    /// equals `addr` when not separately configured. Wire field
    /// `advertised_addr`.
    pub advertised_addr: String,
    pub availability: NodeAvailability,
}
```

`ClusterMetadata` needs no shape change — it already holds `Vec<Member>`, so the
new field propagates with each member through the served view and through the
proto `ClusterMetadata` conversion (Req 3.3).

### 3. `vela-server` — `config.rs` `Config` / `CliArgs` / `Peer` (Req 1.x, D2)

```rust
pub struct CliArgs {
    // ...existing fields...
    /// Client-reachable address advertised to clients (e.g. 127.0.0.1:7001).
    /// Defaults to listen_addr when unset/blank.
    #[arg(long, env = "VELA_ADVERTISED_ADDR")]
    pub advertised_addr: Option<String>,
    // peers grammar extended to id@listen[@advertised] (see Peer)
}

pub struct Config {
    // ...existing fields...
    /// Resolved client-reachable advertised address (host:port). Equals
    /// listen_addr.to_string() when --advertised-addr / VELA_ADVERTISED_ADDR is
    /// unset or blank after trimming (Req 1.2).
    pub advertised_addr: String,
}

pub struct Peer {
    pub id: String,
    pub addr: String,            // peer's listen/bind address (unchanged role)
    pub advertised_addr: String, // peer's advertised address; == addr when omitted
}
```

## Components and Interfaces

### A. Config resolution — `vela-server/src/config.rs`

Resolution mirrors the existing `require`/`trimmed_required` style and is a pure
function of `CliArgs`, so it is unit- and property-testable with no I/O.

1. Parse `node_id`, `listen_addr`, `replication_factor`, `data_dir` exactly as
   today.
2. Resolve advertised (Req 1.1–1.4):
   ```rust
   let advertised_addr = match args.advertised_addr.as_deref().map(str::trim) {
       Some(v) if !v.is_empty() => v.to_string(), // Req 1.1, 1.3 (trimmed)
       _ => listen_addr.to_string(),              // Req 1.2 (fallback)
   };
   ```
   No format validation is applied to the advertised value: any non-empty
   string (including a `0.0.0.0` wildcard host or a bare hostname) is accepted
   (Req 1.4). It is treated as an opaque address string, exactly like a peer
   address.
3. Expose `advertised_addr` on the returned `Config` (Req 1.5).
4. Extend `normalize_peers` to parse the optional third field (D2). After the
   first `@` split into `(id, rest)`, split `rest` once more on `@` into
   `(listen, advertised)`; when no second `@` is present, `advertised = listen`.
   Each half is trimmed; an empty `id` or empty `listen` half is rejected with
   the existing `ConfigError::EmptyPeer`; an explicitly-empty advertised half
   (`id@listen@`) is also rejected as `EmptyPeer` for symmetry with the
   listen half.

No new `ConfigError` variant is required.

### B. Self member construction — `vela-server/src/node.rs`

In `NodeShared::new`, the self `Member` push gains the advertised field
(Req 2.1–2.3):

```rust
metadata.members.push(Member {
    id: NodeId::new(&self_id),
    addr: config.listen_addr.to_string(),     // Req 2.2
    advertised_addr: config.advertised_addr.clone(), // Req 2.1 (== listen when unset, Req 2.3)
    availability: NodeAvailability::Available,
});
```

Because `config.advertised_addr` already defaulted to the listen string when
unset (step A.2), Req 2.3 holds with no extra branch here.

### C. Peer member construction — `vela-server/src/membership.rs`

`register_peers` (and the `Member` it builds) carries the peer's advertised
address straight from the parsed `Peer` (D1):

```rust
metadata.members.push(Member {
    id: id.clone(),
    addr: peer.addr.clone(),
    advertised_addr: peer.advertised_addr.clone(),
    availability: NodeAvailability::Available,
});
```

The `PeerPool` registration is unchanged: peers are dialed over their **bind**
address (`peer.addr`) for server-to-server transport; the advertised address is
client-facing metadata only and never used for inter-node dialing.

### D. Wire conversions — `vela-server/src/convert.rs` (Req 5.2, 5.3)

```rust
pub fn member_to_proto(member: &Member) -> v1::Member {
    v1::Member {
        id: member.id.0.clone(),
        addr: member.addr.clone(),                       // Req 5.2: addr = listen
        advertised_addr: member.advertised_addr.clone(), // Req 5.2: advertised
        availability: availability_to_proto(member.availability) as i32,
    }
}

pub fn member_from_proto(member: &v1::Member) -> Member {
    Member {
        id: NodeId::new(&member.id),
        addr: member.addr.clone(),
        advertised_addr: member.advertised_addr.clone(), // "" from an old server (Req 7.1)
        availability: availability_from_proto(member.availability),
    }
}
```

When `advertised == listen` (the unconfigured case), both wire fields carry the
same value (Req 5.3, 7.3) as a natural consequence — no special-casing. The
`cluster_metadata_to_proto` / `cluster_metadata_from_proto` helpers need no
edit; they already map members through these two functions, so the field rides
along (Req 3.3).

### E. `DescribeCluster` handler — `vela-server/src/service.rs` (Req 4.2)

No code change beyond what `member_to_proto` now emits: the handler already maps
each served member through `member_to_proto`, so the response carries
`advertised_addr` from the served (applied) view automatically.

### F. Client registry seeding — `vela-client/src/core.rs` (Req 6.x, 7.1)

`seed_registry_from_cluster` chooses, per member, the address to seed by
precedence, keeping `insert_if_absent` so operator endpoints stay authoritative.
The selection is factored into a small pure helper so it is directly
property-testable:

```rust
/// Choose the address to seed for a discovered member: prefer the advertised
/// address, fall back to the bind address, skip when both are empty
/// (Req 6.1, 6.2, 6.4, 7.1).
fn seed_address(member: &v1::Member) -> Option<&str> {
    if !member.advertised_addr.is_empty() {
        Some(&member.advertised_addr)   // Req 6.1
    } else if !member.addr.is_empty() {
        Some(&member.addr)              // Req 6.2 (also the old-server path, Req 7.1)
    } else {
        None                            // Req 6.4
    }
}

// in seed_registry_from_cluster:
for member in members {
    if let Some(addr) = seed_address(&member) {
        self.registry.insert_if_absent(member.id, addr.to_string()); // Req 6.3
    }
}
```

`connection::normalize_addr` already prepends `http://` to a schemeless
`host:port`, so an advertised address like `127.0.0.1:7002` dials correctly with
no further change.

### G. Local cluster — `docker-compose.yml` (Req 8.1–8.3)

Each node sets `VELA_ADVERTISED_ADDR` to its host-published address and keeps
`VELA_LISTEN_ADDR=0.0.0.0:7001` (Req 8.1, 8.2). Because propagation is
config-carried (D1/D3), to satisfy Req 8.3 — *querying one node returns every
member's host-published address* — each node's `VELA_PEERS` is also extended
with each peer's advertised address using the `id@listen@advertised` grammar:

```yaml
node1:
  environment:
    VELA_NODE_ID: node1
    VELA_LISTEN_ADDR: 0.0.0.0:7001
    VELA_ADVERTISED_ADDR: 127.0.0.1:7001
    VELA_PEERS: node2@node2:7001@127.0.0.1:7002,node3@node3:7001@127.0.0.1:7003,node4@node4:7001@127.0.0.1:7004
# node2: VELA_ADVERTISED_ADDR 127.0.0.1:7002, peers advertise 7001/7003/7004
# node3: VELA_ADVERTISED_ADDR 127.0.0.1:7003, peers advertise 7001/7002/7004
# node4: VELA_ADVERTISED_ADDR 127.0.0.1:7004, peers advertise 7001/7002/7003
```

A host client pointed at any single published node then discovers all four
host-reachable `127.0.0.1:700x` addresses and can dial any node (Req 8.3).

## Error handling

- **Config**: the advertised value has no failure mode of its own — any
  non-empty value is accepted and an absent value defaults (Req 1.4). The peer
  grammar reuses the existing `ConfigError::EmptyPeer` for an empty `id`,
  `listen`, or explicitly-empty `advertised` half; no new error variant is
  added.
- **Wire**: proto3 defaulting makes a missing `advertised_addr` an empty string
  rather than an error; the client's `seed_address` treats empty as "fall back"
  / "skip" (Req 7.1, 6.4), so a mixed-version cluster never errors on the field.
- **Client seeding** remains best-effort: a failed/empty `DescribeCluster`
  leaves the `id=url` registry untouched (unchanged behavior), and an empty
  member is skipped rather than inserting a blank address.

## Backward and forward compatibility

- **New field, stable numbering** (Req 5.1): `addr` stays at field 2;
  `advertised_addr` is the new field 4. Existing encoders/decoders are
  unaffected.
- **New client ↔ old server** (Req 7.1): old server omits field 4 ⇒
  `advertised_addr == ""` ⇒ client falls back to `addr`.
- **Old client ↔ new server** (Req 7.2): old client ignores the unknown field 4
  and keeps reading `addr` at field 2 — behavior identical to before.
- **New server, advertised unconfigured** (Req 7.3): `advertised_addr` defaults
  to the listen string, so both wire fields carry the listen address and the
  cluster behaves exactly as the prior version.
- **Peer grammar** (D2): every legacy `id@listen` / bare `host:port` peer entry
  parses unchanged, with `advertised = listen`.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all
valid executions of a system — a formal statement about what the system should
do. Properties bridge human-readable specifications and machine-verifiable
correctness guarantees.*

These properties target the pure, input-varying seams: config resolution, the
self-member mapping, the `Member` wire round-trip, and the client seeding
selector. Cross-node agreement (Req 4.1, 4.3, 8.3) and the static schema /
compose facts (Req 5.1, 7.2, 8.1, 8.2) are validated by integration and
smoke/example tests, not property tests, per the prework classification.

### Property 1: Advertised address resolution defaults to listen and trims input

For any otherwise-valid `CliArgs` and any advertised input value, the resolved
`Config.advertised_addr` equals the input trimmed of surrounding whitespace when
that trimmed value is non-empty, and equals `listen_addr.to_string()` when the
input is absent or blank after trimming.

**Validates: Requirements 1.1, 1.2, 1.3, 1.5, 2.3**

### Property 2: Any non-empty advertised value is accepted

For any non-empty advertised string (including wildcard hosts such as
`0.0.0.0:7001`, bare hostnames, or arbitrary `host:port` forms), validating an
otherwise-valid configuration succeeds and records that value — configuration
validation never rejects a non-empty advertised address.

**Validates: Requirements 1.4**

### Property 3: Self member mirrors configuration

For any resolved `Config`, the self `Member` built at startup has
`addr == config.listen_addr.to_string()` and
`advertised_addr == config.advertised_addr`.

**Validates: Requirements 2.1, 2.2**

### Property 4: Member round-trips through proto preserving both addresses

For any domain `Member` (including ones whose `advertised_addr` equals its
`addr`), converting to the wire `v1::Member` and back yields a `Member` equal to
the original — both `addr` and `advertised_addr` are preserved and never
conflated, and `to_proto` places the listen address on `addr` and the advertised
address on `advertised_addr`.

**Validates: Requirements 3.2, 3.3, 5.2, 5.3**

### Property 5: Client seeding precedence is advertised, then addr, then skip

For any wire `Member`, the address selected for registry seeding is the
`advertised_addr` when it is non-empty; otherwise the `addr` when it is
non-empty; otherwise no address is selected and the member is skipped.

**Validates: Requirements 6.1, 6.2, 6.4, 7.1**

### Property 6: Discovery fills gaps without overriding configured endpoints

For any pre-seeded node registry and any set of discovered members, seeding the
registry from those members leaves every already-present node id mapped to its
original address, and adds an entry only for node ids not already present.

**Validates: Requirements 6.3**

### Property 7: An unconfigured advertised address is indistinguishable from the prior version

For any configuration with no advertised address supplied, the serialized self
member has `addr == advertised_addr == listen_addr.to_string()` — both wire
fields carry the listen address, so the node behaves identically to the version
before the advertised field existed.

**Validates: Requirements 7.3**

## Testing strategy

- **Property tests** (proptest, ≥100 iterations each, tagged
  `Feature: advertised-listeners, Property N: ...`): Properties 1–7 above. They
  live beside the code they exercise — config resolution and self-member mapping
  in `vela-server`, the `Member` round-trip in `vela-server/src/convert.rs`
  tests, the seeding selector and gap-fill in `vela-client`.
- **Example/unit tests**: the new `Config.advertised_addr` field is populated
  (Req 1.5); a `Member` constructed with differing `addr`/`advertised_addr`
  reads both back (Req 3.1); the `DescribeCluster` handler emits advertised
  addresses from a served view with known values (Req 4.2); a `Member` decoded
  from bytes lacking field 4 yields `advertised_addr == ""` (Req 7.1).
- **Smoke/schema tests**: `addr` remains field 2 and `advertised_addr` is the
  new field, decodable independently (Req 5.1, 7.2); `docker-compose.yml` sets a
  distinct `VELA_ADVERTISED_ADDR` per node and keeps `VELA_LISTEN_ADDR`
  in-container (Req 8.1, 8.2).
- **Integration tests**: a multi-node cluster reports each member's configured
  advertised address from every node at the same epoch (Req 4.1, 4.3); a host
  client querying one published compose node discovers all host-reachable
  `127.0.0.1:700x` addresses and dials each (Req 8.3).
