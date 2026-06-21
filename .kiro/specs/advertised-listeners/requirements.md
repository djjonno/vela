# Requirements Document

## Introduction

Vela nodes today advertise a single network address in cluster metadata: the
address the gRPC listener binds on (`listen_addr`). A client that discovers
nodes through `DescribeCluster` resolves leader node ids against that
self-reported address. In port-mapped, NAT, or Docker-published deployments the
bind address (e.g. `0.0.0.0:7001`, or an in-cluster hostname like `node2:7001`)
is not dialable from where the client runs, so discovery-filled endpoints are
undialable and only operator-supplied `id=url` endpoints work.

This feature introduces a distinct **advertised address** — a client-reachable
address each node publishes in cluster metadata, separate from its bind address
— mirroring Kafka's `advertised.listeners`. Each node may be configured with an
advertised address; when unset it falls back to its bind address so existing
zero-config deployments are unchanged. Advertised addresses propagate through
the replicated metadata Raft group so every node returns a consistent view from
`DescribeCluster`, and a new wire field carries the advertised address
alongside the existing internal/bind address for backward compatibility. The
local `docker-compose.yml` is updated to advertise each node's host-published
address so a client on the host can reach any node through discovery alone.

This document covers only the advertised-address concept and its propagation,
wire, configuration, client-precedence, compatibility, and local-cluster
demonstration requirements. It does not restate the broader client routing and
replication spec.

## Glossary

- **Advertised_Address**: The client-reachable `host:port` (or operator-chosen
  hostname) a node publishes in cluster metadata so that clients discovering the
  node through `DescribeCluster` obtain a dialable address. Distinct from the
  Listen_Address. Sourced from the `--advertised-addr` flag / `VELA_ADVERTISED_ADDR`
  environment variable on `velad`; when unset it equals the node's Listen_Address.
- **Listen_Address**: The `host:port` socket address the node's gRPC listener
  binds on, configured by `--listen-addr` / `VELA_LISTEN_ADDR`. Used as the
  internal/bind transport address and as the fallback Advertised_Address. Carried
  on the wire `Member.addr` field.
- **Member_Address_Map**: The set of cluster members returned by
  `DescribeCluster`, each entry carrying a node id, its Listen_Address (wire field
  `addr`), its Advertised_Address (new wire field `advertised_addr`), and its
  availability. Clients seed their node-id→address registry from this map.
- **Metadata_Group**: The dedicated `__meta` Raft group that replicates cluster
  metadata (membership and topics) so every node holds a consistent committed
  view. Member entries, including advertised addresses, propagate through this
  group.

## Requirements

### Requirement 1: Advertised address configuration on `velad`

**User Story:** As a cluster operator, I want to configure a client-reachable
advertised address per node, so that clients discovering nodes through
`DescribeCluster` receive dialable addresses in port-mapped or NAT deployments.

#### Acceptance Criteria

1. WHERE the `--advertised-addr` flag or `VELA_ADVERTISED_ADDR` environment
   variable is supplied with a non-empty value, THE Vela_Server SHALL record that
   value as the node's Advertised_Address.
2. IF the `--advertised-addr` flag and `VELA_ADVERTISED_ADDR` environment variable
   are both absent or empty after trimming, THEN THE Vela_Server SHALL set the
   node's Advertised_Address equal to its Listen_Address.
3. WHERE a value is supplied for `--advertised-addr` or `VELA_ADVERTISED_ADDR`,
   THE Vela_Server SHALL trim surrounding whitespace from the value before
   recording it as the Advertised_Address.
4. THE Vela_Server SHALL accept any non-empty Advertised_Address value, including
   a wildcard host such as `0.0.0.0`, without rejecting it during configuration
   validation.
5. WHEN configuration validation succeeds, THE Vela_Server SHALL expose the
   resolved Advertised_Address as a field on its validated `Config` value.

### Requirement 2: Self member advertises the advertised address

**User Story:** As a cluster operator, I want each node to publish its advertised
address in its own member entry, so that the cluster's view of the node carries a
client-reachable address.

#### Acceptance Criteria

1. WHEN a node builds its own member entry at startup, THE Vela_Server SHALL set
   that member's Advertised_Address to the resolved Advertised_Address from
   configuration.
2. WHEN a node builds its own member entry at startup, THE Vela_Server SHALL set
   that member's Listen_Address to the configured Listen_Address.
3. WHERE the Advertised_Address was not configured, THE Vela_Server SHALL publish
   a self member entry whose Advertised_Address equals its Listen_Address.

### Requirement 3: Core model carries the advertised address

**User Story:** As a developer, I want the core `Member` and `ClusterMetadata`
types to carry an advertised address, so that the domain layer can propagate it
through metadata without losing the distinction from the bind address.

#### Acceptance Criteria

1. THE Vela_Core_Member SHALL provide a field that stores the member's
   Advertised_Address as a string distinct from the existing Listen_Address field.
2. WHEN a `Member` value is constructed for a node, THE Vela_Core SHALL retain
   both the Listen_Address and the Advertised_Address for that member.
3. WHEN `ClusterMetadata` is propagated through the Metadata_Group, THE Vela_Core
   SHALL preserve each member's Advertised_Address across the committed view.

### Requirement 4: Advertised address propagation via the metadata Raft group

**User Story:** As a client, I want every node to return a consistent advertised
address for each member, so that discovery yields the same dialable addresses no
matter which node I query.

#### Acceptance Criteria

1. WHEN a member entry becomes part of the committed cluster metadata, THE
   Vela_Server SHALL include that member's Advertised_Address in the replicated
   metadata held by the Metadata_Group.
2. WHEN a node has applied a committed metadata view, THE Vela_Server SHALL report
   each member's Advertised_Address from that committed view in response to
   `DescribeCluster`.
3. WHILE two nodes have applied the same metadata epoch, THE Vela_Server SHALL
   report the same Advertised_Address for a given member from either node.

### Requirement 5: `DescribeCluster` wire format carries the advertised address

**User Story:** As a client developer, I want the `Member` wire message to carry
the advertised address in a new field, so that new clients can prefer it while
old clients keep working.

#### Acceptance Criteria

1. THE Vela_Proto_Member SHALL define a new `advertised_addr` field while
   retaining the existing `addr` field at field number 2 as the
   internal/bind/Listen_Address.
2. WHEN a node serializes a `Member` for a `DescribeCluster` response, THE
   Vela_Server SHALL populate the `addr` field with the member's Listen_Address
   and the `advertised_addr` field with the member's Advertised_Address.
3. WHERE a member's Advertised_Address equals its Listen_Address, THE Vela_Server
   SHALL populate both the `addr` and `advertised_addr` fields with that same
   value.

### Requirement 6: Client precedence and gap-filling discovery

**User Story:** As a client, I want to prefer the advertised address when filling
registry gaps, while keeping operator-supplied endpoints authoritative, so that
discovered addresses are dialable without overriding explicit configuration.

#### Acceptance Criteria

1. WHEN the Vela_Client seeds its node registry from the Member_Address_Map and a
   member's `advertised_addr` field is non-empty, THE Vela_Client SHALL use the
   `advertised_addr` value as that member's address.
2. IF a member's `advertised_addr` field is empty, THEN THE Vela_Client SHALL fall
   back to that member's `addr` value when seeding the registry.
3. WHEN the Vela_Client seeds the registry from the Member_Address_Map, THE
   Vela_Client SHALL insert a member's address only when no address for that node
   id is already present, leaving operator-supplied `id=url` endpoints unchanged.
4. IF both a member's `advertised_addr` and `addr` fields are empty, THEN THE
   Vela_Client SHALL skip that member when seeding the registry.

### Requirement 7: Backward and forward compatibility

**User Story:** As an operator running a mixed-version cluster, I want old and new
servers and clients to interoperate, so that adding the advertised address does
not break existing deployments.

#### Acceptance Criteria

1. WHEN a new Vela_Client receives a `Member` from an older Vela_Server that omits
   the `advertised_addr` field, THE Vela_Client SHALL treat the
   Advertised_Address as empty and fall back to the `addr` value.
2. WHEN an older Vela_Client receives a `Member` from a new Vela_Server, THE
   older Vela_Client SHALL continue reading the `addr` field and SHALL function as
   it did before the `advertised_addr` field existed.
3. WHERE a new Vela_Server has no Advertised_Address configured, THE Vela_Server
   SHALL behave identically to the prior version by advertising its Listen_Address
   in both the `addr` and `advertised_addr` fields.

### Requirement 8: Local cluster demonstrates host reachability

**User Story:** As a developer, I want the local docker-compose cluster to
advertise host-reachable addresses, so that a client on the host can reach any
node using discovery alone.

#### Acceptance Criteria

1. WHEN the `docker-compose.yml` defines each node service, THE Compose_Config
   SHALL set `VELA_ADVERTISED_ADDR` for that node to the node's host-published
   `host:port` address.
2. THE Compose_Config SHALL keep each node's `VELA_LISTEN_ADDR` bound to its
   in-container listener address, distinct from its `VELA_ADVERTISED_ADDR`.
3. WHEN a client on the host queries `DescribeCluster` against one published node,
   THE Vela_Server SHALL return each member's host-published Advertised_Address so
   the client can dial any node from the host.
