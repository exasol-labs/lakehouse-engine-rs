# Decisions: add-native-unity-catalog-client

## ADR: Bespoke thin Unity Catalog REST client over the standard API

**ID:** bespoke-unity-catalog-rest-client
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Issue #318 needs a Unity Catalog client to enumerate catalogs, schemas, and tables and to load
table metadata over the standard `/api/2.1/unity-catalog/` API, authenticating with both a PAT
bearer and Databricks OAuth machine-to-machine. `crates/lakehouse-catalog` already carries
`reqwest 0.12` and `serde` for the Iceberg REST path. No mature standalone Rust Unity Catalog
client exists on crates.io.

### Decision

Build a thin bespoke Unity Catalog client in `crates/lakehouse-catalog`, over the standard
`/api/2.1/unity-catalog/` API, using the workspace `reqwest 0.12` + `serde`. Handle both auth
modes at our layer — a PAT passed straight through as a bearer, and Databricks OAuth M2M
(client-credentials grant to `{host}/oidc/v1/token`, HTTP Basic `client_id:secret`,
`grant_type=client_credentials&scope=all-apis` → `access_token`, 3600 s TTL, no refresh token)
minted and refreshed by us. One client serves both OSS and Databricks-managed Unity Catalog,
because the standard API is identical on both — no Databricks-specific code path.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bespoke thin client over the standard API | ✓ Chosen — no published general-purpose Rust UC client exists; the standard API is a handful of stable JSON endpoints exercised end-to-end against a live Databricks-managed workspace |
| `unitycatalog`/`unitycatalog-client` on crates.io | ✗ Rejected — reserved placeholders, not real published crates |
| `roeap/unitycatalog-rs` | ✗ Rejected — dead, folded into delta-kernel-rs |
| delta-kernel-rs UC crates (`unity-catalog-delta-rest-client`, `delta-kernel-unity-catalog`) | ✗ Rejected — target the `delta/v1` Delta Tables API, which has no list-catalogs/schemas/tables endpoints and is gated on Databricks behind an allowlisted connector User-Agent (HTTP 400 verified live) |

### Consequences

The client slots into the crate's existing REST-catalog shape exactly, with no new external
dependency. "Token versus OAuth" is not a library capability — both terminate in an
`Authorization: Bearer` header — so the only real work is the OAuth exchange and lifecycle, which
the project owns regardless of client. The delta-kernel crates stay on a watch-list only for the
coordinated-commits risk.

## ADR: CATALOG_KIND as a virtual-schema property that selects a client at one construction site

**ID:** catalog-kind-single-construction-site
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Adding Unity Catalog as a second catalog kind requires a way to select which catalog a virtual
schema resolves against, without breaking every existing Iceberg REST virtual schema or forcing
every createVirtualSchema operation to re-decide which catalog it is talking to.

### Decision

Select the catalog kind from a `CATALOG_KIND` VS property, modeled as a `CatalogKind` enum
(`IcebergRest` | `UnityCatalogNative`) in the engine adapter. Absent property → `IcebergRest`. The
kind is read from `props`, never from the CONNECTION password JSON, and `CatalogKind` lives in
`lakehouse-engine` because the catalog crate must not name the Exasol delivery mechanism. The kind
is matched EXHAUSTIVELY at exactly ONE site — the construction site that builds a
`Box<dyn CatalogClient>` — and nowhere else. After construction, createVirtualSchema runs a single
listing pipeline for both kinds; no operation re-matches the kind.

### Options Considered

| Option | Verdict |
|--------|---------|
| VS property, matched once at the construction site | ✓ Chosen — full backward compatibility with no config change; a third kind is a build failure at that one site, and exactly one listing pipeline is maintained |
| `catalog_kind` field inside the CONNECTION password JSON | ✗ Rejected — the kind is a schema-level routing decision, not a credential |
| Enum-matched fork matching `CatalogKind` at every operation site | ✗ Rejected — duplicates the listing pipeline, so every later listing change lands twice and the two paths can silently diverge |

### Consequences

Every pre-existing virtual schema keeps its current behavior with no configuration change.
Concentrating the match at the construction site keeps the `StorageBackend`-style compile-time
property — a third kind is a build failure, not a silent fall-through — at the price of matching
per operation, which would have bought the same compile-time safety by duplicating the pipeline.

## ADR: Unity Catalog vending is a third backend-selection site

**ID:** unity-catalog-vending-third-selector
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

The recorded Storage Backend Enum invariant caps backend selection at exactly two sites
(`storage_block` and `resolve_vended_storage`) reading disjoint inputs. Unity Catalog credential
vending returns a genuinely different response shape — the Unity Catalog temporary-credentials
response — that neither existing selector's input covers.

### Decision

Model Unity Catalog vending as a third backend-selection site, `resolve_uc_vended_storage`, beside
`storage_block` and `resolve_vended_storage`, reading the disjoint Unity Catalog
temporary-credentials response and selecting the variant from the storage-location scheme. The
Storage Backend Enum's "EXACTLY TWO sites" and "no third selector" clauses are explicitly
superseded, and the scheme-to-variant decision is extracted to one home shared by both vended
selectors.

### Options Considered

| Option | Verdict |
|--------|---------|
| Third backend-selection site reading a disjoint input | ✓ Chosen — the consumer defines the abstraction it needs; the invariant is revised now so the third selector is not a silent breach |
| Reshape UC credentials into an Iceberg `LoadTableResult` and reuse `resolve_vended_storage` | ✗ Rejected — couples UC to a provider type it does not use, a Dependency Inversion violation; the UC response is a genuinely disjoint shape |

### Consequences

The scan-path wiring of the third selector is deferred to #319/#320, but the selector and its unit
tests land in #318. The two-selector invariant is superseded in prose rather than left factually
false, and a shared scheme-to-variant-kind home keeps the classification itself from duplicating
across the two vended selectors.

## ADR: Unity Catalog auth reuses the existing CONNECTION credential fields

**ID:** unity-catalog-auth-reuses-connection-fields
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Unity Catalog authentication needs a PAT bearer mode, an OAuth machine-to-machine mode, and an
unauthenticated mode for OSS. The CONNECTION password JSON already parses `token`, `client_id`,
`client_secret`, `oauth2_server_uri`, and `scope` for the Iceberg REST catalog-auth path.

### Decision

Reuse `token` (PAT), `client_id`/`client_secret` (OAuth M2M), `oauth2_server_uri`, and `scope` —
already parsed — for Unity Catalog auth; add no new CONNECTION field. Validation becomes
catalog-kind-parameterized: `warehouse` is required under Iceberg REST only, SigV4 is rejected
under Unity Catalog, and every other Iceberg rule stays byte-identical. A Unity Catalog CONNECTION
with no auth field is accepted for OSS.

### Options Considered

| Option | Verdict |
|--------|---------|
| Reuse existing CONNECTION credential fields | ✓ Chosen — UC OAuth is standard OIDC client-credentials terminating in a bearer; the existing fields already carry it |
| New UC-specific credential fields | ✗ Rejected — no new field is warranted for a shape the existing fields already express |

### Consequences

Minimal surface change; the auth mode is selected from which fields are present, mirroring the
existing catalog-auth abstraction. Credential validation gains a `CatalogKind` parameter but
produces byte-identical acceptance and error text under the default Iceberg REST kind.

## ADR: The GET /tables list sweep is the createVirtualSchema listing path's column source

**ID:** unity-catalog-list-tables-column-source
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

The original `SPIKE_UC_CLIENT.md` recorded only `full_name`/`table_type`/`data_source_format`
from the Unity Catalog `GET /tables` list endpoint. A follow-up live verification against
`demo_sales_catalog.sales` (using `DATABRICKS_HOST`/`DATABRICKS_TOKEN` from `test.env`, per the
project's verification discipline) found the list response already returns each table's `columns[]`
inline by default, alongside `storage_location` and `table_id`.

### Decision

The Unity Catalog client's list-tables method surfaces the inline `columns[]` (ordered by declared
position), `storage_location`, and `table_id` from the `GET /tables` response — returning
fully-populated `UcTableInfo`/`UcColumn` values, not stripped list entries — and MUST NOT set
`omit_columns`. The createVirtualSchema listing path consumes those inline columns directly from
the single paginated list sweep and issues no per-table `GET /tables/{full_name}` for column
metadata. The single-table `GET /tables/{full_name}` load stays in the client's public surface,
reframed as the scan-path single-table load for #319/#320.

### Options Considered

| Option | Verdict |
|--------|---------|
| Consume inline columns from the single list sweep | ✓ Chosen — verified live that `GET /tables` returns columns inline by default; removes the per-table N+1 round-trip entirely |
| Model `GET /tables` as columns-free and fetch per table via `GET /tables/{full_name}` (1-list + N-per-table fan-out) | ✗ Rejected — fetches data the single list sweep already returns, possibly requiring bounded-concurrency machinery for no gain |

### Consequences

Enumerating a schema costs one paginated sweep rather than an N+1 fan-out, with no concurrency
machinery needed. A listed VIEW entry carries columns but no `storage_location`; the listing path
lists it with its columns regardless, the absent location mattering only to the deferred
scan/vending path.

## ADR: One shared CatalogClient trait with catalog-neutral return types, listing-only in #318

**ID:** shared-catalog-client-trait-neutral-types
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

Adding a second catalog kind risks forking the createVirtualSchema listing pipeline into two
divergent code paths that could silently drift apart on every later listing change, while #318
itself reads no Delta log and so has no consumer for a file-planning or scan return type.

### Decision

Both catalog kinds implement ONE `CatalogClient` trait declared in `crates/lakehouse-catalog`,
with two operations: `list_tables(namespace)` returning a `CatalogListing`, and `load_table(ident)`
returning a `CatalogTable`. The trait returns catalog-NEUTRAL types the crate also declares, so the
engine's listing pipeline is written once for both kinds. `list_tables` is fully populated for BOTH
kinds, and each implementation sources columns its own cheapest way. The trait carries NO
file-planning or scan method in #318. `CatalogColumn.source_type` is source-TAGGED and is mapped to
an Exasol type by one exhaustive match in the engine's `types/mapping.rs`.

### Options Considered

| Option | Verdict |
|--------|---------|
| One trait, catalog-neutral types, listing-only scope | ✓ Chosen — the consumer's need ("list a namespace's tables with their columns") is identical for both kinds; only construction differs |
| Enum-matched fork running two divergent listing paths | ✗ Rejected — every later listing change would land twice and the two paths could silently diverge |
| Return each catalog's wire types and map them in the adapter | ✗ Rejected — puts the fork straight back into the pipeline |
| Pre-map each implementation's columns to Exasol types inside the catalog crate | ✗ Rejected — `lakehouse-catalog` must not name the Exasol delivery mechanism or the engine's type-mapping home |
| Normalize both sources to one neutral scalar type descriptor | ✗ Rejected — gives the Iceberg-type decision two homes and risks the byte-identical Iceberg column output, discarding source fidelity #322 needs |
| Define a neutral file-planning return type now | ✗ Rejected — #318 reads no Delta log, so it would be designed against no consumer |

### Consequences

Because the Iceberg listing path moves behind the trait, the Iceberg guarantee softens from "takes
the identical code path" to "behavior-identical, refactored behind the shared trait." `load_table`
is the user-requested single-table load and the #319/#320 scan-path single-table source, promoted
to the trait so both kinds share one name for one operation; in #318 it has no `list_tables`
production caller and is exercised by the shared trait-contract tests.

## ADR: The Iceberg trait receiver is IcebergRestCatalogClient, composing CatalogSession

**ID:** iceberg-catalog-client-composes-session
**Plan:** add-native-unity-catalog-client
**Status:** Accepted

### Context

The shared `CatalogClient` trait needs an Iceberg REST receiver. `CatalogSession` is the resolved
Iceberg-REST session mechanism — one `(catalog_uri, warehouse)` auth strategy plus `/v1/config`
prefix — and a unit test pins that an empty ident batch against an unreachable URI under OAuth2
credentials builds no resolution `CatalogSession` and performs no resolution-phase OAuth2 grant.

### Decision

The Iceberg REST catalog CLIENT implements `CatalogClient`, composing `CatalogSession` internally,
rather than `CatalogSession` implementing the trait itself. `IcebergRestCatalogClient` holds
`catalog_uri`, `storage`, and `creds`; its `list_tables` enumerates, returns immediately on an
empty namespace, and otherwise builds exactly ONE `CatalogSession` for the enumeration.
`CatalogSession`, its constructor, and every scan-path call site stay untouched.
`list_namespace_tables` demotes from `pub` to crate-private because this client becomes its only
caller.

### Options Considered

| Option | Verdict |
|--------|---------|
| Dedicated client composing `CatalogSession` internally | ✓ Chosen — the stronger information-hiding split; keeps the session's one-sentence responsibility intact and preserves the empty-namespace no-resolution-grant guarantee |
| `impl CatalogClient for CatalogSession` | ✗ Rejected — listing needs `storage` and `creds`, which `CatalogSession` does not hold; the resolution session is deliberately built AFTER enumeration, which this option cannot honor without a second constructor and a lazily-filled auth cell |
| The same, plus making `CatalogSession::resolve` itself lazy and adding a `storage` parameter | ✗ Rejected — ripples through ten call sites and moves an OAuth grant failure on the scan path into the first table load, weakening a documented ordering guarantee |
| Accept eager construction of the resolution session and the empty-namespace regression | ✗ Rejected — charges every empty namespace a second resolution-phase grant it does not need, failing the unreachable-URI empty-batch guarantee test |

### Consequences

`CatalogSession` is the resolved Iceberg-REST session mechanism; a client is the trait-level thing
that enumerates a namespace and builds that session lazily. Separating them keeps the scan path out
of this refactor and preserves both guarantees the alternatives weaken, with no change to user
intent — one `CatalogClient` trait, one unified operation path, per-kind construction.
