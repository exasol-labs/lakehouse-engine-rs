# Plan: add-native-unity-catalog-client

## Summary

Both catalog kinds share ONE `CatalogClient` operation surface. This plan adds Unity Catalog as a second catalog kind beside Iceberg REST. A `CATALOG_KIND` virtual-schema property selects the kind and defaults to Iceberg REST. The scope covers a native Unity Catalog REST client, PAT and OAuth machine-to-machine auth, credential vending, and createVirtualSchema listing. It stops before Delta log reading or scan (#319/#320).

## Design

### Context

Today the engine supports one catalog kind: the Iceberg REST catalog, reached through free functions returning Iceberg-native types (`list_namespace_tables` → `Vec<iceberg::TableIdent>`, then a per-table `load_table_any_auth` → `LoadTableResult`). This plan implements GitHub issue #318 (milestone `native-unity-catalog-delta`), adding a native Unity Catalog client over the standard `/api/2.1/unity-catalog/` REST API. The library choice, auth model, and vended-credentials shape were de-risked with live REST calls in `SPIKE_UC_CLIENT.md`; the local OSS fixture harness was de-risked in #325. The design must add a catalog kind without regressing the Iceberg path, without pulling Delta scan or full type fidelity into #318, and without leaving two divergent catalog code paths behind.

- **Goals** — introduce a shared `CatalogClient` trait both kinds implement, so the adapter runs ONE listing pipeline; a catalog-kind seam defaulting to Iceberg REST that selects only which client is CONSTRUCTED; a thin bespoke Unity Catalog REST client in `crates/lakehouse-catalog` (list catalogs/schemas/tables, get table info, pagination); PAT and Databricks OAuth M2M auth with token lifecycle; per-table credential vending terminating in a `StorageBackend`; UC `catalog.schema.table` identity mapping and column metadata sufficient to list a schema; a local-fixture E2E asserting the listing.
- **Non-Goals** — Delta log reading, ScanSpec, and scan execution (#319/#320); pushdown parity (#321); full Delta type fidelity and reader-feature gating (#322); live Databricks E2E (#323); mission and cross-cutting spec reconciliation (#324). The Iceberg REST **scan and pushdown** path is untouched. The Iceberg REST **listing** path is refactored behind the shared trait and stays behavior-identical: the same enumerated tables, the same declared columns and casing, the same `TABLE_MAP`, the same generated SQL, and the same errors.

### Decision

Give both catalog kinds ONE operation surface. Define a `CatalogClient` trait in `crates/lakehouse-catalog` returning catalog-neutral table metadata, implemented by the Iceberg REST client and by the Unity Catalog session. Make the trait dyn-compatible without a new dependency: each method returns a boxed future (`Pin<Box<dyn Future<…> + Send>>`), because native `async fn` in a trait is not dyn-compatible under edition 2024 and `async-trait` is banned in `lakehouse-catalog` by the recorded `catalog-crate-structure` spec and its `catalog_crate_boundary.rs` test. `CATALOG_KIND` selects only which client the adapter CONSTRUCTS; after that single construction site, createVirtualSchema runs one listing pipeline for both kinds, and no operation asks which catalog it is talking to. Build the Unity Catalog client as a bespoke thin client reusing the crate's `reqwest`/`serde`/auth machinery — no new external dependency, and no `delta/v1` Delta Tables API (Databricks gates it behind an allowlisted connector; the standard API is portable across OSS and Databricks). Model Unity Catalog vending as a third backend-selection site alongside the two the Storage Backend Enum already defines, reading a disjoint input.

The trait's Iceberg receiver is a dedicated `IcebergRestCatalogClient` that composes `CatalogSession` internally rather than `CatalogSession` implementing the trait itself. `CatalogSession` is the resolved Iceberg-REST *session* mechanism — one `(catalog_uri, warehouse)` auth strategy plus `/v1/config` prefix. A *client* is the trait-level thing that enumerates a namespace and builds that session lazily. Keeping them distinct is what preserves the recorded guarantee that an empty namespace builds no resolution-phase `CatalogSession` and performs no resolution-phase OAuth2 grant (the enumeration `RestCatalog` still performs its own grant under OAuth2), and what keeps the scan path's session construction untouched.

#### Architecture

```
createVirtualSchema request
  → resolve_catalog_kind(props)        [engine adapter: CatalogKind::{IcebergRest | UnityCatalogNative}]
  → ONE construction site — exhaustive match on the kind, the ONLY place the two kinds diverge
      ├─ IcebergRest        → IcebergRestCatalogClient::new(catalog_uri, storage, creds)   [lakehouse-catalog]
      │                         list_tables: list_namespace_tables (crate-private; own enumeration grant under OAuth2)
      │                                      → empty batch? return, NO resolution CatalogSession, NO resolution grant
      │                                      → else ONE CatalogSession, reused per-ident via a
      │                                        PRIVATE load helper (distinct from trait load_table)
      │                                        (load_table_any_auth → location + schema fields)
      └─ UnityCatalogNative → UnityCatalogSession::new(base_url, creds)                    [lakehouse-catalog]
                                ├─ auth: PAT | OAuth M2M (mint/cache/refresh) | none  [crate-private strategy]
                                ├─ list_tables: GET /tables sweep — columns inline, no per-table get
                                ├─ load_table:  GET /tables/{full_name}                    [scan source, #319/#320]
                                └─ POST /temporary-table-credentials
                                     → resolve_uc_vended_storage → StorageBackend
                                       [unit-tested; scan wiring in #319/#320]
  → Box<dyn CatalogClient>
  → SINGLE listing pipeline (engine adapter) — identical for both kinds:
        client.list_tables(namespace)
          → CatalogListing { tables: [CatalogTable], skipped: [CatalogTableIdent] }
          → flatten + case-fold names · map ColumnSourceType → Exasol type (types/mapping.rs)
          → TABLE_MAP + schemaMetadata response · warn on each skipped identifier

pushdown request
  → resolve_catalog_kind(props)
      ├─ IcebergRest        → existing Iceberg file-resolution path
      │                       (CatalogSession, load_table_any_auth, LoadTableResult)   UNTOUCHED
      └─ UnityCatalogNative → refused: "scan not yet supported"                        [#319/#320]
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| One trait, one operation path | `CatalogClient` in `lakehouse-catalog` | Listing and single-table load have ONE name for both kinds; after construction the adapter never asks which catalog it holds |
| Construction-time exhaustive match | `resolve_catalog_kind` → the single `Box<dyn CatalogClient>` site | The kind is decided once; a third kind is a build failure at that one site instead of at every operation |
| Catalog-neutral, fully-populated return types | `CatalogListing` / `CatalogTable` / `CatalogColumn` | Each impl hides HOW it sources columns — Iceberg's per-table load, Unity's single sweep — behind one shape, so the pipeline stops caring |
| Source-tagged column type, one Exasol-mapping home | `ColumnSourceType` matched in `types/mapping.rs` | The catalog crate must not name Exasol types; the engine keeps one mapping home for both kinds and loses no source fidelity #322 will need |
| Lazy session build inside the Iceberg impl | `IcebergRestCatalogClient::list_tables` | Preserves the recorded guarantee that an empty namespace builds no resolution `CatalogSession` and performs no resolution-phase OAuth2 grant (the enumeration `RestCatalog` still grants under OAuth2) |
| Deep session object built once per request | `UnityCatalogSession`, `CatalogSession` | One client, one resolved auth strategy, reused across every table request |
| Consumer-defined credential-source abstraction | third vended selector reading a disjoint input | The selector defines the vended shape it needs; UC is not forced through Iceberg's `LoadTableResult` |
| One case-fold home | the shared pipeline folds every declared name, both kinds | Keeps the create-virtual-schema "one fold owner" invariant; no second differently-cased name path |
| Crate-internal type helper | `unity_type_name_to_exasol` (not public) | Reuses the Arrow-to-Exasol convention without touching the type-mapping public-surface invariant |

#### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One shared `CatalogClient` trait; the kind selects only which client is constructed | An enum-matched fork running two divergent code paths (`CatalogKind` matched at every operation site) | A per-operation fork duplicates the listing pipeline, so every later listing change lands twice and can silently diverge. Matching once at construction keeps the compile-time "a third kind is a build failure" property while leaving exactly one pipeline to maintain |
| Trait scope is listing-only and extensible | Define a neutral file-planning/scan return type now | #318 never reads a Delta log, so a neutral file-planning shape would be designed against no consumer. A `plan_scan` method is additive in #319/#320 and reshapes neither listing method |
| Catalog-neutral metadata types in the catalog crate | Return each catalog's wire types and map them in the adapter | Returning wire types puts the fork straight back into the pipeline. Neutral types are what make one pipeline possible, and they belong in the crate that declares the trait returning them |
| Source-tagged `ColumnSourceType`, mapped in the engine | Pre-map to Exasol types in each impl; or normalize both to one neutral scalar descriptor | Pre-mapping would force `lakehouse-catalog` to name Exasol types, breaking the one-way dependency. A normalized descriptor would put the Iceberg-type decision in two homes and risk the byte-identical Iceberg column output. A tagged type keeps full source fidelity for #322 and one Exasol-mapping home |
| `IcebergRestCatalogClient` implements the trait, composing `CatalogSession` | `CatalogSession` implements `CatalogClient` directly | Listing needs `storage` and `creds`, which `CatalogSession` does not hold, and the resolution session must be built AFTER enumeration so an empty namespace builds no resolution `CatalogSession` and performs no resolution-phase OAuth2 grant — the enumeration `RestCatalog` still grants under OAuth2, so an empty namespace costs one grant under OAuth2 and zero under no-auth/static-token (guarantee pinned by `client_tests.rs::empty_namespace_builds_no_session_and_no_grant`). Making the session the receiver would either widen it with a second constructor and a lazily-filled auth cell, or regress both that guarantee and the scan path's grant-failure ordering |
| Bespoke thin UC client in `lakehouse-catalog` | `unitycatalog`/`unitycatalog-client` crates; `roeap/unitycatalog-rs`; delta-kernel-rs UC crates | No published general-purpose Rust UC client exists; the delta-kernel UC crates target the `delta/v1` API, have no list endpoints, and are Databricks-connector-gated (HTTP 400). The standard API is a handful of stable JSON endpoints that slot into the crate's existing shape |
| `CATALOG_KIND` as a VS property, not a CONNECTION field | A `catalog_kind` field inside the CONNECTION password JSON | Absent property defaults to Iceberg REST → fully backward compatible; the kind is a schema-level routing decision, not a credential |
| Third backend-selection site for UC vending | Reshape UC creds into `LoadTableResult` and reuse `resolve_vended_storage` | UC's temporary-credentials response is a disjoint shape; forcing it through the Iceberg type would couple UC to a provider API it does not use. The Storage Backend Enum invariant is explicitly superseded to admit the third selector |
| Reuse `token`/`client_id`/`client_secret` for UC auth | New UC-specific CONNECTION fields | The fields are already parsed; UC OAuth is standard OIDC client-credentials terminating in a bearer, so no new field is warranted |
| Listing-sufficient Spark-type mapping; defer fidelity to #322 | Full Delta type fidelity in #318 | #318 stops at catalog metadata; reader-feature gating, timestamp precision, and type widening require reading the Delta log, which is #319+ |
| `GET /tables` list sweep is the UC listing path's column source (no N+1) | 1-list + N-per-table `GET /tables/{full_name}` fan-out for column metadata | The list response inlines `columns[]`, `storage_location`, and `table_id` per table by default (verified live against `demo_sales_catalog.sales`); consuming them from the single paginated sweep removes the per-table round-trip. The client leaves `omit_columns` unset; `GET /tables/{full_name}` is retained for the scan-path single-table load (#319/#320) |

### Spec compliance

The normative reference for this plan is the Unity Catalog REST API contract, not the Apache Iceberg table spec: `SPIKE_UC_CLIENT.md` records the verified endpoints (`GET /catalogs`, `GET /schemas`, `GET /tables`, `GET /tables/{full_name}` → `table_id`/`table_type`/`data_source_format`/`storage_location`/`columns[]`, `POST /temporary-table-credentials` → vended STS/SAS credentials), each returning HTTP 200 against a live Databricks-managed workspace and the OSS fixture. A follow-up live verification against `demo_sales_catalog.sales` confirmed `GET /tables` returns each table's fully-populated `TableInfo` — inline `columns[]`, `storage_location`, and `table_id` — by default (setting `omit_columns=true` drops the `columns[]` array), so the list sweep is the UC createVirtualSchema listing path's column source and needs no per-table get-table fan-out. Iceberg scanning, pushdown, and schema/type handling are untouched; the Iceberg listing path is refactored behind the shared trait with behavior-identical output, and backward compatibility holds through the default `CATALOG_KIND`. The deferral of full Delta type fidelity to #322 is a named trade-off, not a silent gap.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| catalog-kind-selection | NEW | `vs-adapter/catalog-kind-selection/spec.md` |
| unity-catalog-client | NEW | `vs-adapter/unity-catalog-client/spec.md` |
| unity-catalog-auth | NEW | `vs-adapter/unity-catalog-auth/spec.md` |
| unity-catalog-vended-credentials | NEW | `vs-adapter/unity-catalog-vended-credentials/spec.md` |
| unity-catalog-create-virtual-schema | NEW | `vs-adapter/unity-catalog-create-virtual-schema/spec.md` |
| unity-catalog-e2e-harness | NEW | `e2e-harness/unity-catalog-e2e-harness/spec.md` |
| storage-backend-enum | CHANGED | `vs-adapter/storage-backend-enum/spec.md` |
| connection-credentials | CHANGED | `vs-adapter/connection-credentials/spec.md` |
| catalog-crate-structure | CHANGED | `vs-adapter/catalog-crate-structure/spec.md` |
| pushdown-module-structure | CHANGED | `vs-adapter/pushdown-module-structure/spec.md` |
| pushdown-catalog-session | CHANGED | `vs-adapter/pushdown-catalog-session/spec.md` |
| create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |

## Impact

Operators gain a second catalog kind: a virtual schema created with `CATALOG_KIND=UNITY_CATALOG` over a Unity Catalog CONNECTION lists that catalog's tables and column metadata. Every existing virtual schema is unaffected — an absent `CATALOG_KIND` resolves Iceberg REST and every Iceberg-path result stays byte-identical, including over an empty namespace, whose grant behavior is byte-identical to today: the enumeration `RestCatalog` still grants under OAuth2 (so an empty namespace with unusable OAuth2 credentials still fails on that grant exactly as before), while the no-auth and static-token modes still perform no grant and keep succeeding over an empty namespace. A Unity Catalog virtual schema can be created and its tables listed, but a query against one returns a clear "scan not yet supported" error until #319/#320. Both kinds now resolve tables through one `CatalogClient` pipeline, so a later listing change lands once rather than per kind. No breaking changes.

## Dependencies

- The E2E depends on the #325 fixture harness (branch `spike/uc-delta-harness`, commit `3027850`): `docker-compose.unity.yml`, `scripts/unity/*`, the vendored Delta fixtures, and the `unity-up`/`unity-down` Makefile targets. The #318 implementation branch is where those harness files land and where the #325 ADR folds into `decision-log.md`; the branch-merge mechanics are an `/speq:implement` concern.
- No new crate enters the dependency graph, and `lakehouse-catalog` gains no manifest line. The UC client uses the workspace `reqwest 0.12` + `serde`/`serde_json`/`url` already in `lakehouse-catalog`. The `CatalogClient` trait is made dyn-compatible by returning boxed futures (`Pin<Box<dyn Future<…> + Send>>`) rather than by depending on `async-trait`, which the recorded `catalog-crate-structure` spec and `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` (its `FORBIDDEN_DIRECT_DEPENDENCIES` list) forbid the crate to declare.

## Implementation Tasks

1. Catalog crate — shared `CatalogClient` trait, neutral types, Iceberg implementation
   1. Declare `CatalogClient` and the catalog-neutral metadata types in a new module of `crates/lakehouse-catalog/src`. Declare each trait method to return a BOXED FUTURE so the trait is dyn-compatible with NO new dependency: `fn list_tables(&self, namespace: &[String]) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>>` and `fn load_table(&self, ident: &CatalogTableIdent) -> Pin<Box<dyn Future<Output = Result<CatalogTable, UdfError>> + Send + '_>>`, with `Send + Sync` so the engine can hold a `Box<dyn CatalogClient>`. Do NOT use `#[async_trait]` and do NOT add `async-trait` to the crate manifest — native `async fn` in a trait is not dyn-compatible under edition 2024, so the boxed-future return is the deliberate no-dependency mechanism for `Box<dyn CatalogClient>`, and `async-trait` is banned in `lakehouse-catalog` by the recorded `catalog-crate-structure` spec and `catalog_crate_boundary.rs`. Declare `CatalogTableIdent { namespace: Vec<String>, name: String }` (segments, never a pre-joined dotted string), `CatalogTable { ident, table_type, storage_location, columns }`, `CatalogColumn { name, source_type }`, `ColumnSourceType`, and `CatalogListing { tables, skipped }`. Add NO file-planning method — #319/#320 add one without reshaping the listing methods. [expert]
   2. Implement `CatalogClient` for the Iceberg REST path as `IcebergRestCatalogClient` holding `catalog_uri`, `storage`, and `creds`. `list_tables` enumerates through `list_namespace_tables` (whose enumeration `RestCatalog` performs its OWN grant under OAuth2 even for an empty namespace), returns immediately when the namespace holds no table — building NO resolution `CatalogSession` and performing NO resolution-phase OAuth2 grant — and otherwise builds exactly ONE `CatalogSession` for the whole enumeration and REUSES it across every identifier via a PRIVATE per-identifier load helper that TAKES the already-built session, DISTINCT from the trait `load_table`, so the one-session guarantee holds; an identifier the catalog reports as not a loadable Iceberg table routes into `CatalogListing.skipped` instead of failing. The trait `load_table` builds its OWN `CatalogSession` and delegates to that same private helper: it serves the user-requested single-table load and the #319/#320 scan-path single-table source, and in #318 it has no `list_tables` production caller. The shared load helper issues `load_table_any_auth` and returns the table's location plus its `current_schema()` fields as ordered `CatalogColumn`s carrying `ColumnSourceType::Iceberg`, each name in its ORIGINAL case. Demote `list_namespace_tables` from `pub` to crate-private — this client is its only remaining caller. Leave `CatalogSession`, `load_table_any_auth`, and every scan-path call site untouched. [expert]

2. Catalog crate — Unity Catalog client, auth, and credential vending
   1. Add a `unity` module in `crates/lakehouse-catalog/src` with `UnityCatalogSession`, the crate-private wire types, and the base-URL derivation from the CONNECTION address; implement `GET /catalogs`, `/schemas`, `/tables`, and `/tables/{full_name}` with `page_token`/`next_page_token` pagination. Implement `CatalogClient` for `UnityCatalogSession`: `list_tables` runs the single paginated `GET /tables` sweep and MUST NOT set `omit_columns`, converting each entry's inline `columns[]` (declared position order), `storage_location`, and `table_type` into `CatalogTable` values whose columns carry `ColumnSourceType` Unity variants, and returns an always-empty `skipped` list; `load_table` issues `GET /tables/{full_name}` as the scan-path single-table load (#319/#320). Keep the Unity wire types and session fields crate-private — the engine consumes only the neutral types.
   2. Implement the Unity Catalog authentication strategy: PAT verbatim bearer, Databricks OAuth M2M (`client_credentials` grant to `{host}/oidc/v1/token` or `oauth2_server_uri`, `scope=all-apis`, mint/cache/refresh before expiry), and the no-auth mode; keep the strategy crate-private. [expert]
   3. Implement `POST /temporary-table-credentials` and `resolve_uc_vended_storage`. Extract the scheme-to-variant-kind classification into one shared home both vended selectors call: that home classifies the URI scheme only and constructs no `StorageBackend`, while `resolve_uc_vended_storage` constructs its own variant from the UC vended credential family and terminates in a `StorageBackend` — so the single-home and probe-names-every-variant requirements both hold. Do NOT wire a selector dispatch onto it; the catalog-kind-to-credential-family dispatch is deferred to #319/#320. [expert]
   4. Extend `redaction.rs` so the vended `aws_temp_credentials`, `azure_user_delegation_sas`, and `gcp_oauth_token` values and the OAuth client secret never reach an error, SQL, or log line.
   5. Update `crates/lakehouse-catalog/tests/catalog_public_surface.rs`: add `CatalogClient`, the neutral metadata types, `IcebergRestCatalogClient`, `UnityCatalogSession`, the temporary-credentials type, and `resolve_uc_vended_storage`; assert both client types are usable as `Box<dyn CatalogClient>`; assert the Unity wire types are NOT reachable; assert `list_namespace_tables` is no longer declared `pub`; pin `resolve_uc_vended_storage`'s arity and return type; add the source-level probe asserting `resolve_uc_vended_storage` names every `StorageBackend` variant; keep the existing demotion assertions intact.

3. Engine adapter — catalog-kind seam and the single listing pipeline
   1. Add `CatalogKind` and `resolve_catalog_kind(props)` reading `PROP_CATALOG_KIND`, defaulting to `IcebergRest`; reject an unrecognized value.
   2. Thread `CatalogKind` into `read_connection`/`validate_creds`: `warehouse` required under Iceberg REST only, SigV4 rejected under Unity Catalog, all other Iceberg rules byte-identical. [expert]
   3. Add the single construction site: match `CatalogKind` exhaustively to build a `Box<dyn CatalogClient>`, and rewrite `handle_create_virtual_schema` to run ONE listing pipeline over `CatalogListing` for both kinds — flatten and case-fold each `CatalogTableIdent`, map each `CatalogColumn` to an Exasol column, record `TABLE_MAP`, reject a flatten collision, and warn once per skipped identifier with the message the Iceberg path emits today. Delete `resolve_namespace_virtual_tables` and `resolve_table_schema` (with its `pub use` in `adapter/pushdown/mod.rs`), whose only production caller this pipeline replaces. Update the TWO pushdown-façade probes to drop `resolve_table_schema` while still pinning `resolve_file_list`: `crates/lakehouse-engine/tests/pushdown_public_surface.rs` (drop the import; change its doc comment from "12 items … subset of that probe's 22" to "11 items … subset of that probe's 21") and `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs` (drop the import; change its doc-comment count from "22-item" to "21-item"); this façade-baseline reduction is recorded by the `vs-adapter/pushdown-module-structure` delta. Separately, edit `crates/lakehouse-engine/tests/catalog_session_signatures.rs` — drop the `resolve_table_schema` import, delete the `schema_resolution_entry_point_takes_a_shared_session` proof (and its `accepts_shared_session_for_schema_resolution` helper) and its covered-scenario doc line, and keep `file_resolution_entry_points_take_a_shared_session` pinning `resolve_file_list`; this edit is recorded by the `vs-adapter/pushdown-catalog-session` delta, NOT by `pushdown-module-structure`. Migrate the empty-namespace guarantee: the existing engine test `create_virtual_schema_over_empty_namespace_contacts_no_catalog_session` (`adapter_tests.rs:1718`, asserting no catalog session at `:1746`) is REMOVED once `resolve_namespace_virtual_tables` is deleted, and its unreachable-URI + OAuth client-credentials + empty-namespace → success assertion moves to `crates/lakehouse-catalog/src/client_tests.rs::empty_namespace_builds_no_session_and_no_grant` against `IcebergRestCatalogClient::list_tables`, reproducing the identical scenario. The Iceberg listing output — enumerated tables, declared column names and types, `TABLE_MAP`, warnings, and errors — MUST stay byte-identical, including the full-Unicode `to_uppercase` fold that turns `straße` into `STRASSE`. [expert]
   4. Map `ColumnSourceType` to an Exasol type in `types/mapping.rs` through one exhaustive match: the Iceberg variant delegates to the existing `iceberg_type_to_exasol`, the Unity variant to a new crate-internal `unity_type_name_to_exasol` (not public) mapping scalar Spark names per the Arrow-to-Exasol convention and declaring incompatible types and out-of-range decimals as `VARCHAR(2000000)`.
   5. Refuse a pushdown request under the Unity Catalog kind with a clear "scan not yet supported" error; do not route it through the Iceberg file-resolution path.

4. E2E — local Unity Catalog fixture
   1. Land the #325 harness files on the branch; add the `unity-e2e` cargo feature and a `test-e2e-unity` Makefile target invoking `make unity-up`.
   2. Verify the OSS inline-columns precondition: after `make unity-up`, query the #325 fixture's `GET /tables` for the `unity.delta_e2e` namespace and confirm every listed table returns its `columns[]` inline by default (no `omit_columns` set). This confirmation MUST pass before the task 4.3 column assertion is written, because the inline-columns behavior was verified live only against Databricks (`demo_sales_catalog.sales`), not the OSS server. If the OSS list endpoint omits inline columns, escalate before authoring the assertion — do not assume OSS parity.
   3. Add `crates/lakehouse-engine/tests/e2e_unity_test.rs`: create a Unity Catalog virtual schema over `unity.delta_e2e`, assert the fixture tables and a representative column set are listed, assert fail-not-skip when the stack is down, and assert no credential leaks.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (trait foundation) | 1.1 |
| Group A-iceberg | 1.2 |
| Group B (Unity client) | 2.1, 2.2, 2.3 |
| Group B-probe | 2.4, 2.5 |
| Group C (engine adapter) | 3.1, 3.2, 3.4, 3.5 |
| Group C-pipeline | 3.3 |
| Group D (E2E) | 4.1, 4.2, 4.3 |

Sequential dependencies:
- 1.1 → everything else: the trait and the neutral types are the shared contract
- 1.1 → 1.2 → 2.1 (both impls satisfy the same trait; the Iceberg impl lands first so the contract is proven against the behavior-identical path)
- 2.1, 2.2 → 2.3 (vending uses the session and auth); 2.1 → 2.5; 2.3 → 2.5 (probe pins the vended selector)
- 3.1 → 3.2, 3.3, 3.5; 1.2, 2.1, 3.4 → 3.3 (the pipeline consumes both impls and the Exasol mapping)
- Group A, Group B, Group C → Group D
- 4.1 → 4.2 → 4.3 (confirm the OSS fixture inlines columns before the column assertion is authored)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `resolve_namespace_virtual_tables` — `crates/lakehouse-engine/src/adapter/mod.rs` | The single `CatalogClient` listing pipeline replaces it; its session hoist and per-table resolve move inside `IcebergRestCatalogClient::list_tables`. The engine test `create_virtual_schema_over_empty_namespace_contacts_no_catalog_session` (`adapter_tests.rs:1718`, asserting at `:1746`) is removed with it; that test drove the resolution half only (a pre-enumerated empty ident batch against an unreachable URI under OAuth2 creds, asserting no `CatalogSession` is built), and its empty-batch no-resolution-session/no-grant assertion migrates to `crates/lakehouse-catalog/src/client_tests.rs::empty_namespace_builds_no_session_and_no_grant` |
| Function | `resolve_table_schema` and its `pub use` — `crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`, `adapter/pushdown/mod.rs` | Its only production caller was the listing path. The load-and-extract half moves into `IcebergRestCatalogClient::load_table`; the Exasol-mapping and uppercasing half moves into the shared pipeline (case-fold-home relocation recorded by the `vs-adapter/create-virtual-schema` delta). Removing it from the frozen `pushdown` façade edits the two façade probes — `tests/pushdown_public_surface.rs` (external probe, 12→11 items) and `src/adapter/pushdown_surface_probe_tests.rs` (in-crate probe, 22→21 items) — which keep pinning `resolve_file_list`; that façade reduction is recorded by the `vs-adapter/pushdown-module-structure` delta. The signature/one-session proof `tests/catalog_session_signatures.rs` drops its `schema_resolution_entry_point_takes_a_shared_session` proof and covered-scenario doc line, recorded by the `vs-adapter/pushdown-catalog-session` delta |
| Visibility | `list_namespace_tables` — `crates/lakehouse-catalog/src/namespace.rs` and its `lib.rs` re-export | Demoted from `pub` to crate-private: `IcebergRestCatalogClient::list_tables` is its only caller. The reachability probe drops it from the public set and asserts the demotion |

Every other Iceberg selector and each arm of `resolve_connection_config` stays in use under the default catalog kind.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Absent CATALOG_KIND resolves the Iceberg REST catalog kind | Unit | `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `absent_catalog_kind_resolves_iceberg_rest` |
| CATALOG_KIND naming Unity Catalog resolves the native Unity Catalog kind | Unit | `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `unity_catalog_value_resolves_native_kind` |
| An unrecognized CATALOG_KIND value is rejected with a clear error | Unit | `crates/lakehouse-engine/src/adapter/catalog_kind_tests.rs` | `unrecognized_catalog_kind_is_rejected` |
| The catalog kind selects which client is constructed and is matched nowhere else | Unit | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `catalog_kind_is_matched_only_at_the_construction_site` |
| Both catalog kinds resolve their tables through one shared listing pipeline | Integration | `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs` | `both_kinds_share_one_listing_pipeline` |
| The Iceberg REST client and the Unity Catalog session both satisfy the shared trait | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `both_clients_are_catalog_client_trait_objects` |
| The catalog crate exposes the shared trait and its neutral types and keeps the Unity wire types hidden | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `catalog_client_trait_and_neutral_types_are_reachable` |
| Namespace enumeration is demoted to crate-private now the client is its only caller | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `list_namespace_tables_is_no_longer_public` |
| An empty Iceberg identifier batch builds no resolution catalog session and performs no resolution-phase OAuth2 grant | Integration | `crates/lakehouse-catalog/src/client_tests.rs` | `empty_namespace_builds_no_session_and_no_grant` |
| A non-empty Iceberg namespace builds exactly one catalog session for the whole enumeration | Integration | `crates/lakehouse-catalog/src/client_tests.rs` | `enumeration_builds_exactly_one_session` |
| A table the catalog reports as not a loadable Iceberg table is skipped, not failed | Integration | `crates/lakehouse-catalog/src/client_tests.rs` | `unloadable_table_is_reported_skipped_not_failed` |
| Iceberg listing returns the same tables, columns, and casing as before the trait refactor | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `iceberg_listing_is_behavior_identical_behind_the_trait` |
| The pushdown façade drops resolve_table_schema when the shared catalog-client pipeline replaces its only caller | Unit (compile-time probe) | `crates/lakehouse-engine/src/adapter/pushdown_surface_probe_tests.rs`, `crates/lakehouse-engine/tests/pushdown_public_surface.rs` | both façade probes compile with `resolve_table_schema` dropped (21 in-crate / 11 external) |
| A neutral column maps to an Exasol type through one home for both catalog kinds | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `column_source_type_maps_to_exasol_in_one_home` |
| Unity Catalog validation does not require a warehouse and rejects SigV4 | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `unity_kind_validation_skips_warehouse_and_rejects_sigv4` |
| Iceberg REST validation is unchanged under the default catalog kind | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `iceberg_kind_validation_still_requires_warehouse` |
| A pushdown request under the Unity Catalog kind is refused as not yet executable | Integration | `crates/lakehouse-engine/src/adapter/adapter_tests.rs` | `unity_kind_pushdown_is_refused_not_iceberg_routed` |
| The client lists tables in a configured catalog and schema | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `lists_tables_in_catalog_schema` |
| The client retrieves a table's metadata including its columns | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `loads_table_metadata_with_columns` |
| The client follows pagination across every result page | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `follows_pagination_across_pages` |
| The client surfaces a transport or HTTP-status failure as a clear, credential-safe error | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `request_failure_is_credential_safe_error` |
| One session serves both OSS and Databricks-managed Unity Catalog | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `identical_request_shape_oss_and_databricks` |
| A personal access token is applied as the bearer verbatim | Integration | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `pat_is_applied_as_bearer_verbatim` |
| OAuth machine-to-machine mints a bearer token via the client-credentials grant | Integration | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `oauth_m2m_mints_bearer_via_client_credentials` |
| A minted OAuth token is cached and refreshed before expiry rather than re-minted per request | Integration | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `oauth_token_is_cached_and_refreshed_before_expiry` |
| The unauthenticated mode sends no Authorization header | Integration | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `unauthenticated_mode_sends_no_authorization_header` |
| A failed OAuth grant surfaces a clear, credential-safe error | Integration | `crates/lakehouse-catalog/src/unity/auth_tests.rs` | `failed_oauth_grant_is_credential_safe_error` |
| An S3 vended response terminates in an S3 storage backend | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `s3_vended_response_terminates_in_s3_backend` |
| An ADLS vended response terminates in an ADLS storage backend | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `adls_vended_response_terminates_in_adls_backend` |
| The storage-backend variant is selected from the location scheme alone | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `variant_selected_from_location_scheme` |
| A location scheme with no supported backend is a clear error | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `unsupported_scheme_is_error` |
| A vended response missing the credential the location's backend needs is a clear error | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `missing_matching_credential_is_error` |
| A vended plaintext endpoint is honored only with operator consent | Unit | `crates/lakehouse-catalog/src/unity/vended_tests.rs` | `plaintext_endpoint_requires_allow_http` |
| Create virtual schema enumerates every table in the configured Unity Catalog namespace | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `enumerates_unity_namespace_tables` |
| Create virtual schema lists the namespace with no per-table get-table call | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `listing_issues_no_per_table_get_table_call` |
| Create virtual schema lists a Unity Catalog view with its columns and no storage location | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `lists_view_with_columns_and_no_storage_location` |
| Unity Catalog Spark column types map to Exasol types sufficient for listing | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unity_spark_types_map_to_exasol` |
| An incompatible Unity Catalog column type is declared as VARCHAR rather than failing | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `incompatible_unity_types_declared_varchar` |
| Create virtual schema records the Exasol-name to Unity-Catalog-identifier map in adapterNotes | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `records_table_map_and_rejects_collision` |
| Create virtual schema fails clearly when the Unity Catalog is unreachable | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `unreachable_unity_catalog_is_credential_safe_error` |
| A Unity Catalog vended selector is admitted as a third backend-selection site | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `resolve_uc_vended_storage_signature_takes_no_connection_value` |
| The three-selector dispatch stays single-homed and probe-guarded | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `uc_vended_selector_source_names_every_storage_backend_variant` |
| Credential validation is parameterized by the resolved catalog kind | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `validation_is_parameterized_by_catalog_kind` |
| A Unity Catalog CONNECTION reuses the existing auth fields without a new credential field | Unit | `crates/lakehouse-engine/src/adapter/connection_tests.rs` | `unity_connection_reuses_existing_auth_fields` |
| The native Unity Catalog client extends the crate's public surface through an explicit reviewed edit | Unit | `crates/lakehouse-catalog/tests/catalog_public_surface.rs` | `unity_catalog_public_items_are_reachable` |
| Harness brings up Unity Catalog and seeds the Delta fixtures | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `setup` (via `make unity-up`) |
| Create virtual schema over a Unity Catalog namespace lists the fixture tables and columns | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_create_virtual_schema_lists_fixture_tables_and_columns` |
| The Unity Catalog E2E suite fails when the stack is unavailable | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_suite_fails_when_stack_unavailable` |
| The Unity Catalog E2E suite leaks no credential value | Integration (E2E) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_credentials_never_appear_in_output` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| catalog-crate-structure (shared trait) | `cargo test -p lakehouse-catalog client_tests` | Trait-contract, one-session, empty-namespace, and skipped-table tests pass |
| catalog-kind-selection / connection-credentials | `cargo test -p lakehouse-engine adapter::connection_tests` | Kind resolution and kind-aware validation tests pass |
| catalog-kind-selection (single path) | `cargo test -p lakehouse-engine adapter::catalog_client_tests` | One-construction-site probe and the shared-pipeline test pass |
| unity-catalog-client / unity-catalog-auth | `cargo test -p lakehouse-catalog unity::` | Client, pagination, and auth-lifecycle tests pass |
| unity-catalog-vended-credentials / storage-backend-enum / catalog-crate-structure | `cargo test -p lakehouse-catalog --test catalog_public_surface` | Trait-object, neutral-type, wire-type-hidden, demotion, and vended-variant probes compile and pass |
| unity-catalog-create-virtual-schema | `cargo test -p lakehouse-engine unity_schema_tests` | Namespace enumeration, TABLE_MAP, and type-mapping tests pass |
| Iceberg listing regression | `cargo test -p lakehouse-engine adapter::adapter_tests` | Pre-existing Iceberg listing assertions pass unchanged behind the trait |
| pushdown-module-structure (façade redraw) | `cargo build -p lakehouse-engine --tests` | Both pushdown-façade probes compile with `resolve_table_schema` dropped (21 in-crate / 11 external) |
| unity-catalog-e2e-harness (OSS inline-columns precondition) | `make unity-up && curl -s "$UNITY_CATALOG_URL/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=delta_e2e" \| jq '.tables[] \| {name, columns}'` | Every fixture table returns a non-empty `columns[]`, confirming the OSS `GET /tables` inlines columns by default before the column assertion is authored |
| unity-catalog-e2e-harness | `make unity-up && cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1` | Virtual schema lists the fixture tables and columns; fails (not skips) when the stack is down |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
| E2E (Iceberg regression) | `make test-e2e` | 0 failures; Iceberg listing and scan behavior unchanged |
| E2E (Unity) | `make unity-up && cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1` | 0 failures; fails (not skips) when the stack is unavailable |
