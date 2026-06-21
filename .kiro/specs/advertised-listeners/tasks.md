# Implementation Plan: advertised-listeners

## Overview

This plan turns the design into code bottom-up so the workspace stays green at
every step. It threads the advertised address through one seam per layer in
dependency order: the wire `Member` (`vela-proto`), the domain `Member`
(`vela-core`), config resolution + peer grammar (`vela-server/config.rs`), the
self member (`node.rs`), the peer member (`membership.rs`), the wire conversions
(`convert.rs`), the `DescribeCluster` handler (`service.rs`), the client
registry-seeding selector (`vela-client/core.rs`), and finally the local
`docker-compose.yml`.

Per design decision **D1**, member advertised addresses are carried in local
configuration exactly as bind addresses are today — no new RPC, no new
replicated `ClusterCommand`, no consensus change — so the served
`ClusterMetadata.members` that `DescribeCluster` already reads is the only
propagation surface that changes. The peer grammar is extended to
`id@listen[@advertised]` (**D2**), which is backward compatible.

The 7 correctness properties from the design are each implemented as a single
dedicated `proptest` file (≥100 cases) following the existing `prop_*.rs`
convention (see `crates/vela-core/tests/prop_keyed_routing.rs`): a module
doc-comment carrying the `Feature: advertised-listeners, Property N: <text>`
tag, a `Validates: Requirements ...` line, and `ProptestConfig::with_cases(100)`
or higher. Properties target the pure seams (config resolution, self-member
mapping, `Member` wire round-trip, the client seeding selector); cross-node
agreement and static schema/compose facts are covered by integration and
example tests, per the design's prework classification.

Steering is respected throughout: Rust + tokio, tonic/prost on the wire,
`thiserror` library errors, traits at crate seams, clippy-clean with
`-D warnings`, no `unsafe`, and inward-only runtime crate dependencies.

## Tasks

- [ ] 1. Add the advertised address to the wire and domain models
  - [ ] 1.1 Add `Member.advertised_addr` (field 4) to the proto
    - In `crates/vela-proto/proto/vela.proto`, add
      `string advertised_addr = 4;` to `message Member`, keeping `addr` at field
      number 2 (internal/bind/Listen_Address) and `availability` at 3; document
      the field as "client-reachable address; empty => use addr"
    - Rebuild so the generated prost type carries the new field; proto3 defaulting
      gives mixed-version compatibility for free (old server omits 4 ⇒ `""`)
    - _Requirements: 5.1, 7.1, 7.2_

  - [ ] 1.2 Add `advertised_addr` to the `vela-core` `Member`
    - In `crates/vela-core/src/model.rs`, add
      `pub advertised_addr: String` to `Member` next to `addr`, distinct from the
      Listen_Address; doc it as equal to `addr` when not separately configured
    - Update every in-crate `Member` constructor and `#[cfg(test)]` case (and any
      `ClusterMetadata` propagation/conversion helpers in `metadata.rs`) to set and
      preserve the field; `ClusterMetadata` needs no shape change (`Vec<Member>`)
    - _Requirements: 3.1, 3.2, 3.3_

  - [ ]* 1.3 Write a unit test that `Member` retains both addresses
    - In `crates/vela-core/src/model.rs` `#[cfg(test)] mod tests`: a `Member` built
      with differing `addr`/`advertised_addr` reads both back unconflated
    - _Requirements: 3.1, 3.2_

- [ ] 2. Config resolution and peer grammar (`vela-server/src/config.rs`)
  - [ ] 2.1 Add `--advertised-addr` / `VELA_ADVERTISED_ADDR` and resolve it
    - Add `pub advertised_addr: Option<String>` to `CliArgs`
      (`#[arg(long, env = "VELA_ADVERTISED_ADDR")]`) and
      `pub advertised_addr: String` to `Config`
    - In the `Config` resolver, set `advertised_addr` to the trimmed flag/env value
      when non-empty, else fall back to `listen_addr.to_string()`; apply no format
      validation so any non-empty value (incl. `0.0.0.0` wildcard, bare hostname)
      is accepted; expose it on the validated `Config`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [ ] 2.2 Extend the peer grammar to `id@listen[@advertised]`
    - In `normalize_peers`, after splitting `(id, rest)` on the first `@`, split
      `rest` once more on `@` into `(listen, advertised)`; when no second `@` is
      present, `advertised = listen`; trim each half; add
      `pub advertised_addr: String` to `Peer`
    - Reject an empty `id`, empty `listen`, or explicitly-empty `advertised`
      (`id@listen@`) with the existing `ConfigError::EmptyPeer` — no new error
      variant; every legacy `id@listen` and bare `host:port` entry still parses
      with `advertised = listen`
    - _Requirements: 1.1, 1.3 (peer-carried per D1/D2)_

  - [ ]* 2.3 Write unit tests for config resolution and peer parsing
    - In `config.rs` `#[cfg(test)] mod tests`: flag set → recorded; env set →
      recorded; both blank/absent → defaults to listen; surrounding whitespace
      trimmed; `0.0.0.0:7001` accepted; `id@listen@advertised` parses all three
      fields; `id@listen` and bare `host:port` default advertised to listen;
      `id@listen@` → `EmptyPeer`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [ ]* 2.4 Write property test for advertised resolution (Property 1)
    - Create `crates/vela-server/tests/prop_advertised_resolution.rs`
    - **Property 1: Advertised address resolution defaults to listen and trims input**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.5, 2.3**

  - [ ]* 2.5 Write property test for non-empty acceptance (Property 2)
    - Create `crates/vela-server/tests/prop_advertised_accept.rs`
    - **Property 2: Any non-empty advertised value is accepted** (incl. wildcard
      hosts, bare hostnames, arbitrary `host:port`) — validation never rejects it
    - **Validates: Requirements 1.4**

- [ ] 3. Self member construction (`vela-server/src/node.rs`)
  - [ ] 3.1 Stamp the advertised address onto the self `Member`
    - In `NodeShared::new`, set the self `Member`'s
      `addr = config.listen_addr.to_string()` and
      `advertised_addr = config.advertised_addr.clone()`; because config already
      defaulted advertised to the listen string, the unconfigured case (Req 2.3)
      holds with no extra branch
    - _Requirements: 2.1, 2.2, 2.3_

  - [ ]* 3.2 Write property test for the self-member mapping (Property 3)
    - Create `crates/vela-server/tests/prop_self_member_mirror.rs`
    - **Property 3: Self member mirrors configuration** — for any resolved
      `Config`, the self `Member` has `addr == listen_addr.to_string()` and
      `advertised_addr == config.advertised_addr`
    - **Validates: Requirements 2.1, 2.2**

- [ ] 4. Peer member construction (`vela-server/src/membership.rs`)
  - [ ] 4.1 Carry the peer advertised address into the peer `Member`
    - In `register_peers`, set each peer `Member`'s
      `advertised_addr = peer.advertised_addr.clone()` alongside
      `addr = peer.addr.clone()`; keep `PeerPool` registration dialing the peer's
      **bind** address (`peer.addr`) for server-to-server transport — the
      advertised address is client-facing metadata only and never used for
      inter-node dialing
    - _Requirements: 3.2, 3.3, 4.1 (peer-carried per D1)_

  - [ ]* 4.2 Write a unit test that the peer member carries advertised, dialing uses bind
    - In `membership.rs` `#[cfg(test)] mod tests`: a peer parsed as
      `id@listen@advertised` produces a `Member` with both fields set, and the
      `PeerPool`/dial target is the bind `addr`, not the advertised value
    - _Requirements: 3.2, 4.1_

- [ ] 5. Wire conversions (`vela-server/src/convert.rs`)
  - [ ] 5.1 Carry `advertised_addr` through `member_to_proto` / `member_from_proto`
    - In `member_to_proto`, populate `addr` with the listen address and
      `advertised_addr` with the advertised address; in `member_from_proto`, copy
      `advertised_addr` back (`""` from an old server). The
      `cluster_metadata_to_proto` / `_from_proto` helpers map members through these
      two functions, so the field rides along with no edit
    - _Requirements: 5.2, 5.3, 3.3, 7.1_

  - [ ]* 5.2 Write property test for the `Member` round-trip (Property 4)
    - Create `crates/vela-server/tests/prop_member_roundtrip.rs`
    - **Property 4: Member round-trips through proto preserving both addresses**
      (including `advertised_addr == addr`), never conflating the two, with
      `to_proto` placing listen on `addr` and advertised on `advertised_addr`
    - **Validates: Requirements 3.2, 3.3, 5.2, 5.3**

  - [ ]* 5.3 Write property test for unset parity (Property 7)
    - Create `crates/vela-server/tests/prop_unset_parity.rs`: resolve a `Config`
      with no advertised supplied, build the self `Member`, convert via
      `member_to_proto`, and assert `addr == advertised_addr == listen_addr`
    - **Property 7: An unconfigured advertised address is indistinguishable from the prior version**
    - **Validates: Requirements 7.3**

  - [ ]* 5.4 Write a unit test that an old-server `Member` decodes to empty advertised
    - In `convert.rs` tests: a `v1::Member` with `advertised_addr` defaulted (no
      field 4) maps to a domain `Member` with `advertised_addr == ""`
    - _Requirements: 7.1_

- [ ] 6. `DescribeCluster` emits the advertised address (`vela-server/src/service.rs`)
  - [ ] 6.1 Confirm the handler emits advertised addresses from the served view
    - Verify the `DescribeCluster` handler maps each served member through
      `member_to_proto` so `advertised_addr` is emitted automatically; make the
      minimal edit only if the handler builds `v1::Member` inline rather than via
      `member_to_proto`
    - _Requirements: 4.2, 5.2_

  - [ ]* 6.2 Write the multi-node consistency integration test
    - Extend `crates/vela-server/tests/describe_cluster.rs`: a cluster whose
      members carry distinct advertised addresses reports each member's advertised
      address, and two nodes at the same epoch report the same advertised address
      for a given member
    - _Requirements: 4.1, 4.3, 8.3_

- [ ] 7. Checkpoint - server-side advertised address threaded end to end
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Client registry seeding precedence (`vela-client/src/core.rs`)
  - [ ] 8.1 Add the `seed_address` precedence helper and use it when seeding
    - Add a pure `seed_address(member: &v1::Member) -> Option<&str>` that returns
      `advertised_addr` when non-empty, else `addr` when non-empty, else `None`;
      in `seed_registry_from_cluster`, select via `seed_address` and keep
      `registry.insert_if_absent(id, pick)` so operator-supplied `id=url`
      endpoints stay authoritative; skip members with no usable address.
      Expose the helper so it is directly property-testable
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 7.1_

  - [ ]* 8.2 Write property test for seeding precedence (Property 5)
    - Create `crates/vela-client/tests/prop_seed_precedence.rs`
    - **Property 5: Client seeding precedence is advertised, then addr, then skip**
    - **Validates: Requirements 6.1, 6.2, 6.4, 7.1**

  - [ ]* 8.3 Write property test for gap-filling discovery (Property 6)
    - Create `crates/vela-client/tests/prop_seed_gap_fill.rs`
    - **Property 6: Discovery fills gaps without overriding configured endpoints** —
      already-present node ids keep their original address; only absent ids are added
    - **Validates: Requirements 6.3**

  - [ ]* 8.4 Write unit tests for the seeding selector edge cases
    - In `core.rs` tests: advertised preferred over addr; empty advertised falls
      back to addr; both empty → skipped; pre-seeded `id=url` left unchanged
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 7.1_

- [ ] 9. Local cluster advertises host-reachable addresses (`docker-compose.yml`)
  - [ ] 9.1 Set `VELA_ADVERTISED_ADDR` per node and extend `VELA_PEERS`
    - For each node service, set `VELA_ADVERTISED_ADDR` to its host-published
      `127.0.0.1:700x`, keep `VELA_LISTEN_ADDR=0.0.0.0:7001` in-container, and
      extend `VELA_PEERS` to the `id@listen@advertised` grammar so each node knows
      every peer's host-published advertised address (per D1/D3)
    - _Requirements: 8.1, 8.2, 8.3_

- [ ] 10. Quality gates
  - [ ] 10.1 Run `cargo fmt --check` across the workspace and fix any drift
    - _Requirements: (steering: rustfmt)_
  - [ ] 10.2 Run `cargo clippy --all-targets -- -D warnings` and resolve every lint
    - _Requirements: (steering: clippy-clean, no unsafe)_
  - [ ] 10.3 Run `cargo test` across the workspace; confirm property tests at ≥100 cases
    - _Requirements: all_
  - [ ]* 10.4 Run `cargo mutants` on the new pure logic and add asserts to kill survivors
    - Target config advertised resolution (trim/default boundaries), the peer
      grammar parse, `member_to_proto`/`member_from_proto`, and the client
      `seed_address` precedence (advertised vs addr vs skip)
    - _Requirements: 1.2, 1.3, 5.2, 5.3, 6.1, 6.2, 6.4_

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a
  faster MVP; core implementation tasks are never optional.
- Each task references the specific requirement clause(s) and/or design property
  it implements for traceability.
- Property tests follow the existing `prop_*.rs` convention: a module doc-comment
  with the `Feature: advertised-listeners, Property N: <text>` tag, a
  `Validates: Requirements ...` line, and `ProptestConfig::with_cases(>=100)`.
- Per design D1/D3, cross-node consistency (Req 4.3, 8.3) is operator-driven, not
  Raft-agreed, and is validated by the integration test (task 6.2), not a
  property test — matching the design's prework classification.
- Property 7 (unset parity) lives in `vela-server` because it spans config
  resolution, self-member construction, and proto serialization (task 5.3).

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3"] },
    { "id": 2, "tasks": ["2.1", "2.2"] },
    { "id": 3, "tasks": ["2.3", "2.4", "2.5", "3.1", "4.1", "5.1", "8.1"] },
    { "id": 4, "tasks": ["3.2", "4.2", "5.2", "5.3", "5.4", "6.1", "8.2", "8.3", "8.4", "9.1"] },
    { "id": 5, "tasks": ["6.2"] },
    { "id": 6, "tasks": ["10.1", "10.2", "10.3", "10.4"] }
  ]
}
```
