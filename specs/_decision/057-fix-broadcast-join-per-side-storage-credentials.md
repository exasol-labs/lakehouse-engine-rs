# Decisions: fix-broadcast-join-per-side-storage-credentials

## ADR: Route by path inside one registered store per bucket, not by a custom object-store registry

**ID:** join-route-by-path-inside-one-store-per-bucket
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

A broadcast join read both tables through the fact side's storage credential (issue #294). DataFusion keys its object-store registry on `scheme://host[:port]` and rejects URLs with a path, so one bucket has one registered store. Databricks makes a shared metastore bucket the normal case for two tables of one catalog.

### Decision

The adapter registers one `PrefixRoutingObjectStore` per bucket, holding one inner store per join side, each built from that side's own storage backend. Every `ObjectStore` call with a `Path` routes to the owning side's inner store.

### Options Considered

| Option | Verdict |
|--------|---------|
| A custom DataFusion registry keyed with userinfo | Rejected: the registry never receives a path, so it cannot separate two S3 tables in one bucket, and adopting `object_store`'s userinfo-retaining registry is a larger change against a pluggable seam |
| Refuse credential-divergent joins | Rejected: forfeits broadcast for the common Databricks case |
| Fall back to the N-scan renderer | Rejected: forfeits broadcast for the same case |

### Consequences

One code path serves every join with no credential comparison. Two credentials live behind one registered store.

## ADR: Route on each side's enumerated file paths first, table root only as a fallback

**ID:** join-route-by-enumerated-paths-then-table-root
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

The router must match a requested path to its owning side. The Iceberg spec permits a table's files outside its `location` (Appendix E, Version 4: absolute paths for files that share no prefix with the table location), so a root-prefix match alone can misroute.

### Decision

Each side's routing table is the set of its own data-file and positional-delete-file paths, matched exactly first. A path no side enumerated falls to a longest-table-root-prefix match, and a path matching neither is an error.

### Options Considered

| Option | Verdict |
|--------|---------|
| Longest table-root prefix only | Rejected: misroutes a spec-legal table whose files sit outside its `location` |

### Consequences

The scan discovers no files, so each side's spec names every path it requests, and the root-prefix fallback is unreachable on a well-formed spec. It stays as the interviewed prescription and is labelled unreachable, not load-bearing.

## ADR: Layer the router OUTSIDE the spec-sized store, one size index per side

**ID:** join-router-outside-spec-sized-store-per-side-index
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

The scan's spec-sized store answers `head` calls from a size index without a network call. With one whole-spec index, one side's store could answer the other side's metadata lookup.

### Decision

The router sits outside, wrapping one spec-sized store per side, each with its own size index.

### Options Considered

| Option | Verdict |
|--------|---------|
| One sized layer outside the router with the whole-spec index | Rejected: an unroutable `head` is answered from the index, so the routing failure surfaces later as an access denial instead of a plan defect |

### Consequences

One side's store cannot satisfy the other side's metadata lookup.

## ADR: `validate_sides_share_one_store` stays; prefix routing does not subsume it

**ID:** join-keep-validate-sides-share-one-store-guard
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

DataFusion's registry key drops URL userinfo, which on `abfss://` is the container. An Azure store is container-scoped and its paths are container-relative, so two containers of one storage account can produce identical paths that path-based routing cannot distinguish.

### Decision

The scan-time guard stays, and only its doc comment changes. It refuses a spec whose sides share one registry key but need different stores.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete it as subsumed by the router | Rejected: the router cannot separate two ADLS containers with identical relative paths |

### Consequences

This guard is the only guard over the ADLS different-container case, because `join-backend-guard-deleted-store-url-collision-is-the-rule` deletes the plan-time comparison that never covered it.

## ADR: Per-side scheme selection needs a plan-time join guard scoped to variant and account, not full backend equality

**ID:** join-backend-guard-scoped-to-variant-and-credential-served
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Superseded by join-backend-guard-deleted-store-url-collision-is-the-rule
**Supersedes:** join-backend-guard-scoped-to-variant-and-account

### Context

The earlier ADR scoped the plan-time backend comparison to variant and, for ADLS, `account_name`, because a per-prefix vended-credential collapse (#294) was deferred. This plan fixes that collapse, so that justification no longer applies.

### Decision

The plan-time comparison keeps its scope of variant and ADLS `account_name`, justified by what the scan cannot serve: a variant difference and an ADLS account difference. A credential difference is now served.

### Options Considered

| Option | Verdict |
|--------|---------|
| Widen to full backend equality | Rejected: rejects the credential divergence this plan now serves |
| Delete the guard | Rejected: variant and account divergence were believed unserveable |

### Consequences

The narrow scope states what is unserveable, not what is unverified.

## ADR: Strip Exasol's native `tableAlias` in `render_broadcast_join`, render everything bare

**ID:** join-strip-table-alias-render-bare-in-broadcast-renderer
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted

### Context

The broadcast renderer keeps Exasol's `tableAlias` in the join condition, filter, and projection, but the scan's derived sub-SELECTs are unaliased. Any aliased join query fails with a DataFusion schema error at scan time (issue #303), before the credential path is reached.

### Decision

The broadcast renderer strips `tableAlias` from the join condition, the WHERE filter, and the request passed to join projection extraction, right after the disjoint-schema guard passes. `build_join_sql` is unchanged. The N-scan side renderer already strips for the same reason, and the disjoint-schema guard already proves bare names are unambiguous.

### Options Considered

| Option | Verdict |
|--------|---------|
| Thread each side's alias through the wire and alias the sub-SELECTs | Rejected: needs the fact side's alias as well, must handle asymmetric aliasing and live-Exasol verification, and bare rendering already solves the problem |
| Parse the alias out of the rendered condition string | Rejected: string surgery on opaque SQL that cannot tell which alias belongs to which side |
| Declare aliased joins ineligible for broadcast | Rejected: forfeits broadcast for every aliased query |

### Consequences

Aliased broadcast joins now resolve, with no wire-format change. The specs' claim that broadcast rendering is side-agnostic bare-name is now true by construction.

## ADR: Delete the plan-time backend guard; the store-URL collision is the only rule, and the scan owns it

**ID:** join-backend-guard-deleted-store-url-collision-is-the-rule
**Plan:** fix-broadcast-join-per-side-storage-credentials
**Status:** Accepted
**Supersedes:** join-backend-guard-scoped-to-variant-and-credential-served

### Context

Code review of PR #306 found the plan-time guard's scope inverted. The scan builds each side's store from that side's own backend and registers one store per derived store URL. A variant difference is served, because different schemes give different registry keys. An ADLS account difference is served, because two accounts are two hosts. The same-account, different-container case is not served, and the guard admitted it because it compares only `account_name`. The guard therefore rejected two configurations the scan serves, for example two Iceberg tables in different ADLS accounts, and admitted the one it cannot.

### Decision

The adapter deletes the plan-time backend guard and its identity helper. The scan-time `validate_sides_share_one_store` precondition is the only owner of the real rule: two sides collapsing onto one registry key while needing different stores.

### Options Considered

| Option | Verdict |
|--------|---------|
| Re-scope the plan-time guard to compare derived store URLs | Rejected: re-derives DataFusion's registry-key formula in the adapter and still hard-errors a container collision the N-scan fallback serves |
| Keep the guard and widen or narrow its backend comparison | Rejected: backend identity is not what the scan's addressing depends on |
| Make the collision a broadcast disqualifier that falls to the N-scan fallback | Rejected for this plan: the best behavior, but it needs plan-time store-URL derivation and new eligibility and spec requirements, so it is recorded as the known improvement and needs its own tracked work |

### Consequences

Cross-backend joins and two-account ADLS joins plan and run normally. Two containers of one storage account fail at scan time, with a message naming both store URLs, instead of at plan time. Whether it errors depends on the chosen plan, since the N-scan fallback serves that shape. The superseded ADR's property that acceptance follows the configuration, not the data, is given up, because the alternative refuses a query the fallback reads correctly.
