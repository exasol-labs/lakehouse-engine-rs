# Decision Log: add-native-unity-catalog-client

## Interview

**Q1 — How should the Unity Catalog client be built?**
**A:** A bespoke thin client inside `crates/lakehouse-catalog`, reusing the existing `reqwest 0.12` + `serde` and the `auth.rs`/`session.rs`/`vended.rs`/`redaction.rs` machinery. No new external dependency. Research confirmed no mature standalone Rust Unity Catalog client exists on crates.io.

**Q2 — Which auth modes are in scope for #318?**
**A:** Both PAT bearer and Databricks OAuth machine-to-machine, unit-tested with mocked token flows. The E2E stays on the local no-auth OSS fixture; live Databricks OAuth verification is deferred to #323.

**Q3 — How does the user select the catalog kind?**
**A:** Add `CATALOG_KIND` as a virtual-schema property — a createVirtualSchema adapter property read from the request `props` (as `resolve_connection_config(ctx, props)` already reads `CATALOG_CONNECTION` and `ALLOW_HTTP`), not a field inside the CONNECTION password JSON. Default to Iceberg REST when the property is absent, so the change is fully backward compatible.

**Q4 — Is credential vending in scope for #318?**
**A:** Yes. Implement the `POST /temporary-table-credentials` client and terminate it in a `StorageBackend` value, unit-tested. It is not exercised end to end until Delta scan execution lands (#319/#320).

### Revision interview

**Q5 — How much of the shared catalog trait should #318 define, given UC scan and file planning are deferred to #319/#320?**
**A:** Listing-only and extensible. The trait covers exactly what #318 needs: list the tables in a namespace, returning catalog-neutral table metadata including columns, and load one table's neutral metadata. A file-planning method is deferred to #319/#320; defining a neutral file-planning return type now would design it against no consumer. Shape the trait so a scan method is additive rather than a reshape of the listing methods.

**Q6 — How are the trait and its implementations named?**
**A:** The trait is `CatalogClient`. The Iceberg REST path and the new Unity Catalog session both implement it. During authoring the Iceberg receiver was refined from `CatalogSession` to a dedicated `IcebergRestCatalogClient` — see decision [10].

## Design Decisions

### [1] Bespoke thin Unity Catalog REST client over the standard API

- **Decision:** Build a thin bespoke Unity Catalog client in `crates/lakehouse-catalog`, over the standard `/api/2.1/unity-catalog/` API, using the workspace `reqwest 0.12` + `serde`. Handle both auth modes at our layer — a PAT passed straight through as a bearer, and Databricks OAuth M2M (client-credentials grant to `{host}/oidc/v1/token`, HTTP Basic `client_id:secret`, `grant_type=client_credentials&scope=all-apis` → `access_token`, 3600 s TTL, no refresh token) minted and refreshed by us. One client serves both OSS and Databricks-managed Unity Catalog, because the standard API is identical on both — no Databricks-specific code path.
- **Alternatives:** `unitycatalog`/`unitycatalog-client` on crates.io (reserved placeholders, not real); `roeap/unitycatalog-rs` (dead, folded into delta-kernel-rs); the delta-kernel-rs UC crates (`unity-catalog-delta-rest-client`, `delta-kernel-unity-catalog`). The delta-kernel crates target the `delta/v1` Delta Tables API, which has no list-catalogs/schemas/tables endpoints and is gated on Databricks behind an allowlisted connector User-Agent (HTTP 400 verified live) — wrong API and gated. Kept on a watch-list only for the coordinated-commits risk.
- **Rationale:** No published general-purpose Rust UC client exists; the standard API is a handful of stable JSON endpoints exercised end-to-end against a live Databricks-managed workspace (`SPIKE_UC_CLIENT.md`). The bespoke client slots into the crate's existing REST-catalog shape exactly. "Token versus OAuth" is not a library capability — both terminate in an `Authorization: Bearer` header — so the only real work is the OAuth exchange and lifecycle, which we own regardless of client.
- **Promotes to ADR:** yes

### [2] CATALOG_KIND as a virtual-schema property that selects a client at ONE construction site

- **Decision:** Select the catalog kind from a `CATALOG_KIND` VS property, modeled as a `CatalogKind` enum (`IcebergRest` | `UnityCatalogNative`) in the engine adapter. Absent property → `IcebergRest`. The kind is read from `props`, never from the CONNECTION password JSON, and `CatalogKind` lives in `lakehouse-engine` because the catalog crate must not name the Exasol delivery mechanism. The kind is matched EXHAUSTIVELY at exactly ONE site — the construction site that builds a `Box<dyn CatalogClient>` — and nowhere else. After construction, createVirtualSchema runs a single listing pipeline for both kinds; no operation re-matches the kind.
- **Alternatives:** A `catalog_kind` field inside the CONNECTION password JSON — rejected, the kind is a schema-level routing decision, not a credential, and a VS-property default keeps every existing virtual schema byte-identical. An enum-matched fork that matches `CatalogKind` at every operation site (the original shape of this decision) — rejected, see decision [9].
- **Rationale:** Full backward compatibility with no config change. Concentrating the match at the construction site keeps the `StorageBackend`-style compile-time property — a third kind is a build failure, not a silent fall-through — while leaving exactly one listing pipeline to maintain. Matching per operation would have bought the same compile-time safety at the price of duplicating that pipeline.
- **Promotes to ADR:** yes

### [3] Unity Catalog vending is a third backend-selection site

- **Decision:** Model Unity Catalog vending as a third backend-selection site, `resolve_uc_vended_storage`, beside `storage_block` and `resolve_vended_storage`, reading the disjoint Unity Catalog temporary-credentials response and selecting the variant from the storage-location scheme. The Storage Backend Enum's "EXACTLY TWO sites" and "no third selector" clauses are explicitly superseded, and the scheme-to-variant decision is extracted to one home shared by both vended selectors.
- **Alternatives:** Reshape the UC credentials into an Iceberg `LoadTableResult` and reuse `resolve_vended_storage`. Rejected — it would couple UC to a provider type it does not use (a Dependency Inversion violation); the UC response is a genuinely disjoint shape.
- **Rationale:** The consumer defines the abstraction it needs; the invariant is revised now so the third selector is not a silent breach. The scan-path wiring is deferred to #319/#320, but the selector and its unit tests land in #318.
- **Promotes to ADR:** yes

### [4] Unity Catalog auth reuses the existing CONNECTION credential fields

- **Decision:** Reuse `token` (PAT), `client_id`/`client_secret` (OAuth M2M), `oauth2_server_uri`, and `scope` — already parsed — for Unity Catalog auth; add no new CONNECTION field. Validation becomes catalog-kind-parameterized: `warehouse` is required under Iceberg REST only, SigV4 is rejected under Unity Catalog, and every other Iceberg rule stays byte-identical. A Unity Catalog CONNECTION with no auth field is accepted for OSS.
- **Alternatives:** New UC-specific credential fields. Rejected — UC OAuth is standard OIDC client-credentials terminating in a bearer; the existing fields already carry it.
- **Rationale:** Minimal surface change; the mode is selected from which fields are present, mirroring the existing catalog-auth abstraction.
- **Promotes to ADR:** yes

### [5] Reuse the ICEBERG_NAMESPACE property for the Unity Catalog namespace

- **Decision:** Under the Unity Catalog kind, the existing `ICEBERG_NAMESPACE` property carries the `catalog.schema` value; a catalog-neutral rename is deferred to #324 (mission/spec reconciliation).
- **Alternatives:** Introduce a new catalog-neutral namespace property now. Deferred — a rename is a cross-cutting concern #324 owns, and reusing the property keeps #318 additive and backward compatible.
- **Rationale:** Avoids a property proliferation #324 would have to reconcile; the value semantics (`catalog.schema`) are unambiguous.
- **Promotes to ADR:** no

### [6] Listing-sufficient Spark-type mapping; full fidelity deferred to #322

- **Decision:** Map scalar Unity Catalog Spark type names to Exasol types reusing the Arrow-to-Exasol convention; declare incompatible types and out-of-range decimals as `VARCHAR(2000000)`. The mapping lives in a crate-internal `unity_type_name_to_exasol` helper in `types/mapping.rs`, not a new public API item, so it does not touch the type-mapping public-surface invariant. Reader-feature gating, timestamp precision, type widening, and variant fidelity are deferred to #322.
- **Alternatives:** Full Delta type fidelity in #318. Rejected — reader-feature gating and type widening require reading the Delta log, which is #319+; #318 stops at catalog metadata.
- **Rationale:** #318's E2E asserts a listing with typed columns; a listing-sufficient mapping delivers that without pulling #322's work forward, and the boundary is named rather than silent.
- **Promotes to ADR:** no

### [7] A Unity Catalog pushdown is refused, not routed through the Iceberg path

- **Decision:** A pushdown request under the Unity Catalog kind returns a clear "scan not yet supported" error and is not routed through the Iceberg file-resolution path.
- **Alternatives:** Silently attempt the Iceberg path. Rejected — a Unity Catalog table is a Delta table the Iceberg path cannot read; a silent attempt surfaces a misleading catalog error.
- **Rationale:** Keeps the #318 boundary honest until #319/#320 land the scan.
- **Promotes to ADR:** no

### [8] The `GET /tables` list sweep is the createVirtualSchema listing path's column source

- **Decision:** The Unity Catalog client's list-tables method surfaces the inline `columns[]` (ordered by declared position), `storage_location`, and `table_id` from the `GET /tables` response — returning fully-populated `UcTableInfo`/`UcColumn` values, not stripped list entries — and MUST NOT set `omit_columns`. The createVirtualSchema listing path consumes those inline columns directly from the single paginated list sweep and issues no per-table `GET /tables/{full_name}` for column metadata. The single-table `GET /tables/{full_name}` load stays in the client's public surface, reframed as the scan-path single-table load for #319/#320, no longer the listing path's column source. A listed VIEW entry carries columns but no `storage_location`; the listing path lists it with its columns, the absent location mattering only to the deferred scan/vending path.
- **Alternatives:** Model `GET /tables` as returning only `full_name`/`table_type`/`data_source_format` and fetch columns per table via `GET /tables/{full_name}` — the 1-list + N-per-table (N+1) fan-out the earlier spec shape implied, possibly with bounded-concurrency machinery. Rejected — the fan-out and its concurrency handling fetch data the single list sweep already returns.
- **Rationale:** `GET /tables` returns columns inline by default, verified live against `demo_sales_catalog.sales`. The live REST call followed the project's verification discipline, using `DATABRICKS_HOST`/`DATABRICKS_TOKEN` from `test.env`. `GET /api/2.1/unity-catalog/tables?catalog_name=…&schema_name=…` returned a fully-populated `TableInfo` per table: `full_name`, `name`, `table_type`, `data_source_format`, `storage_location`, `table_id`, and `columns[]` (each with `name`, `type_name`, `type_text`, `type_json`, `type_precision`, `type_scale`, `nullable`, `position`). All five tables in the schema returned their columns inline; setting `omit_columns=true` dropped the `columns[]` array, which confirms columns are returned by default and the client must simply leave the parameter unset. The original `SPIKE_UC_CLIENT.md` recorded only `full_name`/`table_type`/`data_source_format` from the list endpoint because that was all it needed then, not because columns are absent. Consuming the inline columns removes the per-table N+1 round-trip entirely and needs no concurrency machinery.
- **Promotes to ADR:** yes

### [9] One shared `CatalogClient` trait with catalog-neutral return types, listing-only in #318

- **Decision:** Both catalog kinds implement ONE `CatalogClient` trait declared in `crates/lakehouse-catalog`, with two operations: `list_tables(namespace)` returning a `CatalogListing`, and `load_table(ident)` returning a `CatalogTable`. The trait returns catalog-NEUTRAL types the crate also declares — `CatalogTableIdent { namespace, name }`, `CatalogTable { ident, table_type, storage_location, columns }`, `CatalogColumn { name, source_type }`, `ColumnSourceType`, and `CatalogListing { tables, skipped }` — so the engine's listing pipeline is written once for both kinds. `list_tables` is fully populated for BOTH kinds, and each implementation sources columns its own cheapest way: the Iceberg client performs a per-table load inside its own implementation, the Unity client reads them inline from its single sweep. The trait carries NO file-planning or scan method in #318; #319/#320 add one without reshaping either listing method. `CatalogColumn.source_type` is source-TAGGED — an Iceberg type or a Unity Spark type name — and is mapped to an Exasol type by one exhaustive match in the engine's `types/mapping.rs`. Because the Iceberg listing path moves behind the trait, the Iceberg guarantee softens from "takes the identical code path" to "behavior-identical, refactored behind the shared trait": the enumerated tables, declared column names and types, `TABLE_MAP`, generated SQL, warnings, and errors stay byte-identical, but the code that produces them moves.
- **Alternatives:** (a) The enum-matched fork decision [2] originally recorded — the adapter matches `CatalogKind` and runs two divergent listing paths. Rejected: every later listing change would land twice and the two paths could silently diverge. (b) Return each catalog's wire types and map them in the adapter. Rejected: that puts the fork straight back into the pipeline. (c) Pre-map each implementation's columns to Exasol types inside the catalog crate. Rejected: `lakehouse-catalog` must not name the Exasol delivery mechanism or the engine's type-mapping home. (d) Normalize both sources to one neutral scalar type descriptor. Rejected: it would give the Iceberg-type decision two homes — the normalizer and the existing `iceberg_type_to_exasol` — and put the byte-identical Iceberg column output at risk, while discarding the source fidelity #322 needs. (e) Define a neutral file-planning return type now. Rejected: #318 reads no Delta log, so it would be designed against no consumer.
- **Rationale:** The consumer defines the abstraction it needs. The adapter's need is "list a namespace's tables with their columns", which is identical for both kinds; only construction differs. A source-tagged column type keeps the one-way crate dependency intact, keeps a single Exasol-mapping home, and loses no fidelity ahead of the single mapping site. The listing-only scope is what keeps the trait honest: a method with no #318 consumer would be designed blind. `load_table` is not speculative — it is the user-requested single-table load ("list tables and load a table") and the #319/#320 scan-path single-table source, promoted to the trait so both catalog kinds share one name for one operation. It is NOT the listing path's own per-table step: `IcebergRestCatalogClient::list_tables` reuses one `CatalogSession` across identifiers through a PRIVATE session-taking helper distinct from the trait `load_table`, so the one-session guarantee the `enumeration_builds_exactly_one_session` test pins holds. In #318 the trait `load_table` has no `list_tables` production caller and is exercised by the shared trait-contract tests.
- **Promotes to ADR:** yes

### [10] The Iceberg trait receiver is `IcebergRestCatalogClient`, composing `CatalogSession`

- **Decision:** Refine Q6's "`CatalogSession` implements `CatalogClient`" to "the Iceberg REST catalog CLIENT implements `CatalogClient`, composing `CatalogSession` internally". `IcebergRestCatalogClient` holds `catalog_uri`, `storage`, and `creds`; its `list_tables` enumerates, returns immediately on an empty namespace, and otherwise builds exactly ONE `CatalogSession` for the enumeration. `CatalogSession`, its constructor, and every scan-path call site stay untouched. `list_namespace_tables` demotes from `pub` to crate-private because this client becomes its only caller.
- **Alternatives:** (a) `impl CatalogClient for CatalogSession`. Rejected on two facts: listing needs `storage` and `creds`, which `CatalogSession` does not hold and `CatalogSession::resolve` does not take; and the resolution session is deliberately built AFTER enumeration so an empty ident batch performs no resolution-phase OAuth2 grant (the enumeration `RestCatalog` still performs its own grant under OAuth2) — a guarantee pinned by a unit test that resolves an empty ident list against an unreachable URI. Honoring it would need a second constructor plus a lazily-filled auth cell, widening a type whose responsibility is currently crisp. (b) The same, plus making `CatalogSession::resolve` itself lazy and adding a `storage` parameter. Rejected: it ripples through ten call sites and moves an OAuth grant failure on the SCAN path from the session-build seam into the first table load, weakening a documented ordering guarantee. (c) Accept eager construction of the resolution session and the empty-namespace regression. Rejected: eagerly building the resolution session would charge every empty namespace a resolution-phase grant it does not need — a second grant under OAuth2 — and would fail the unreachable-URI empty-batch guarantee test, breaking this plan's behavior-identical promise. (This concerns the RESOLUTION session only; the enumeration `RestCatalog` grant already runs under OAuth2 regardless, so an empty namespace with unusable OAuth2 credentials already fails on the enumeration grant, both before and after this refactor.)
- **Rationale:** The two types have genuinely different responsibilities. `CatalogSession` is the resolved Iceberg-REST session mechanism — one `(catalog_uri, warehouse)` auth strategy plus `/v1/config` prefix. A client is the trait-level thing that enumerates a namespace and builds that session lazily. Separating them is the stronger information-hiding split, not a workaround: it keeps the session's one-sentence responsibility intact, keeps the scan path out of this refactor, and preserves both guarantees the alternatives weaken. The user's intent is unaffected — one `CatalogClient` trait, one unified operation path, per-kind construction.
- **Promotes to ADR:** yes

## Carry-forwards (from SPIKE_UC_CLIENT.md — tracked, not #318 blockers)

- **Credential vending is per-principal grant-gated.** Vending needs `GRANT USE CATALOG` + `EXTERNAL USE SCHEMA` on each target schema for the authenticating principal — orthogonal to token-versus-OAuth. `demo_sales_catalog` via the PAT is the ready live-vend fixture for #319/#320.
- **Coordinated-commits risk for #319/#320.** Databricks managed Delta tables may carry latest commits not yet in `_delta_log` at `storage_location` (returned inline by the `delta/v1` `load_table`); the planned `_delta_log`-reading path could return a stale snapshot. Watch the delta-kernel `delta/v1` UC crates; confirm whether target managed tables use coordinated commits before #319/#320 ship.
- **OAuth token lifecycle.** Mint, cache, and refresh before the 3600 s expiry; never re-auth per request.
- **Pagination and full type mapping.** `page_token`/`next_page_token` handled in #318; deeper UC `type_json` → Arrow → Exasol fidelity is #322.
- **Implementation-commit CLAUDE.md facts.** Record: standard-API-only for Databricks (not `delta/v1`), OAuth = client-credentials → bearer, and the `EXTERNAL USE SCHEMA` prerequisite.

## Review Findings

### [plan-review] Third vended selector left the five-module variant-naming clause standing (BLOCKER-1)

- **Finding:** The `storage-backend-enum` delta superseded only the recorded "EXACTLY TWO sites" and "no third selector" clauses. It left standing the broader recorded clause: "the ONLY modules permitted to match on a `StorageBackend` variant ... SHALL be these five, exhaustively ... No other production module SHALL name a variant." `resolve_uc_vended_storage` in the new `unity` module constructs `StorageBackend::S3`/`::Adls` — a sixth variant-naming site — factually contradicting the still-standing clause and defeating decision [3]'s claim that the third selector is "not a silent breach."
- **Direction change:** Added an `*AND*` clause to Scenario 1 ("A Unity Catalog vended selector is admitted as a third backend-selection site") superseding the five-module exhaustive clause and amending the list to six, naming `resolve_uc_vended_storage` (`unity` module), and stated the source-level variant probe covers BOTH `resolve_vended_storage` and `resolve_uc_vended_storage` (pinned in Scenario 2's probe clause). Resolved the entangled INFORMATION_LEAKAGE finding by stating, in Scenario 2 and plan task 1.3, that the single shared home owns only the scheme-to-variant-kind classification (constructs no `StorageBackend`), while each vended selector constructs its own variant from its own disjoint credential family — so the single-home and probe-names-every-variant requirements are simultaneously satisfiable. Updated the delta Background to name the five-to-six supersession. Decision [3] already carries the ADR-level rationale for the third selector, so this is a local supersession of an existing recorded invariant.
- **Promotes to ADR:** no

### [plan-review] Two-level dispatch clause asserted structure #318 never builds (BLOCKER-2)

- **Finding:** Scenario 2 recorded a superseding "TWO-LEVEL dispatch — the resolved catalog kind selects the credential family." But #318 refuses a Unity Catalog pushdown before any credential or file resolution (`catalog-kind-selection`; plan task 2.5), so no site dispatches on catalog kind to select the UC credential family, and the mapped source probe (`uc_vended_selector_source_names_every_storage_backend_variant`) exercises only the shared-home and probe clauses, not a dispatch. The clause asserted structure #318 neither builds nor tests and would record as false at `/speq:record`.
- **Direction change:** Took the DEFER route. Rewrote the Scenario 2 clause to describe only what #318 wires — the single shared scheme-to-variant-kind home plus the source probe — and stated the recorded "exactly ONE site chooses between selectors" clause stays intact and is NOT superseded here. Deferred the catalog-kind-to-credential-family two-level dispatch to #319/#320, marked as a deferral like the plan's other #319/#320 deferrals. Added no dispatch code or task to #318, so every mapped test still matches what is built. This is a scope/deferral correction local to this plan.
- **Promotes to ADR:** no

### [plan-review] Client list scenario made VIEW `storage_location`/`data_source_format` optional (round-1 advisory)

- **Finding:** `unity-catalog-client` Scenario 1 asserted every listed entry carries `storage_location` and `data_source_format`, contradicting the create-virtual-schema VIEW scenario, where a VIEW entry has neither. An implementer modeling `UcTableInfo` to the client scenario verbatim would make both fields required and serde-deserialization of a VIEW list entry would fail, breaking the create-virtual-schema VIEW scenario.
- **Direction change:** Added an `*AND*` clause to `unity-catalog-client` Scenario 1 and a Background sentence stating a listed VIEW entry carries its `columns[]` but omits `storage_location` and carries a null `data_source_format`, so `UcTableInfo` models both fields as optional and the list method deserializes a VIEW entry without failing. Both deltas now agree on the wire shape.
- **Promotes to ADR:** no

### [plan-review] OSS fixture inline-columns pinned as a verified E2E precondition (round-1 advisory)

- **Finding:** The inline-columns behavior was verified live only against Databricks (`demo_sales_catalog.sales`), but the #318 E2E runs against the OSS #325 fixture. With the get-table fallback removed, the list sweep is the sole column source, so an OSS list response without inline columns would fail the column assertion and redden the fail-not-skip suite with no code bug.
- **Direction change:** Added plan task 3.2 to confirm, via `make unity-up`, that the OSS fixture's `GET /tables` inlines `columns[]` by default before the column assertion (now task 3.3) is authored, with a matching Manual Testing row, an intra-group sequential dependency (`3.1 → 3.2 → 3.3`), and the Group C parallelization row updated. Mirrored the precondition as a Background clause in the E2E harness spec. OSS parity must be confirmed against the running fixture, not assumed.
- **Promotes to ADR:** no

### [plan-review] Split decision [8] Rationale opening sentence (round-1 advisory)

- **Finding:** The decision [8] Rationale opened with a ~55-word sentence that buried the load-bearing conclusion behind a parenthetical and a field enumeration, exceeding the 25-word cap.
- **Direction change:** Split the opening so the conclusion leads ("`GET /tables` returns columns inline by default, verified live against `demo_sales_catalog.sales`"), moving the field enumeration and the `test.env`/verification-discipline note into following short sentences. No decision content changed.
- **Promotes to ADR:** no

### [plan-review] CatalogClient is dyn-compatible via boxed futures, not async-trait (trait-revision BLOCKER)

- **Finding:** Plan task 1.1 said "Add `async-trait` to the crate manifest" and § Dependencies said `lakehouse-catalog` "gains a manifest line only". The recorded `vs-adapter/catalog-crate-structure` MUST-NOT-declare clause and the existing `crates/lakehouse-catalog/tests/catalog_crate_boundary.rs` (`FORBIDDEN_DIRECT_DEPENDENCIES` includes `"async-trait"`) forbid exactly that direct dependency, so `cargo test` could never pass and `/speq:record` would merge a manifest line the recorded spec forbids.
- **Direction change:** Took Option A — keep the crate boundary intact and add NO dependency. Each `CatalogClient` method now returns a boxed future (`fn list_tables(&self, ns: &[String]) -> Pin<Box<dyn Future<Output = Result<CatalogListing, UdfError>> + Send + '_>>`, and likewise `load_table`) instead of `#[async_trait]`/`async fn`, because native `async fn` in a trait is not dyn-compatible under edition 2024. Removed "Add `async-trait` to the crate manifest" from task 1.1, deleted the async-trait sentence from § Dependencies (the crate gains no manifest line), and recorded the boxed-future mechanism in the Design § Decision. `catalog_crate_boundary.rs` is left unedited — under Option A its ban stays true.
- **Promotes to ADR:** no

### [plan-review] Façade reduction recorded via a pushdown-module-structure delta; in-crate probe edited (trait-revision BLOCKER)

- **Finding:** Deleting `resolve_table_schema` (task 3.3) removes an item from the frozen `pushdown` façade, pinned by TWO probes. Task 3.3 named only `tests/pushdown_public_surface.rs`, not the in-crate `src/adapter/pushdown_surface_probe_tests.rs`, whose 22-item `use` list also imports `resolve_table_schema` — so the workspace would fail to compile — and the plan carried NO delta against `vs-adapter/pushdown-module-structure`, so the baseline reduction would record as a silent façade change.
- **Direction change:** Added a CHANGED delta `vs-adapter/pushdown-module-structure` (new scenario plus Background supersessions) recording the reduction: `resolve_table_schema` leaves the façade, in-crate probe 22→21, external probe 12→11, `resolve_file_list` alone keeps its `pub` name; added it to plan § Features. Added `src/adapter/pushdown_surface_probe_tests.rs` to task 3.3's edit list (drop the import; doc comment "22-item"→"21-item"), stated the external probe's doc-comment count edit (12/22→11/21), and reflected both probe edits in the § Dead Code Removal `resolve_table_schema` row. Added Scenario-Coverage and Manual-Testing rows for the new scenario.
- **Promotes to ADR:** no

### [plan-review] UC source-tagged descriptor carries the full parameterized Spark type (trait-revision advisory)

- **Finding:** The neutral column's Unity descriptor was specified as carrying the bare "Unity Catalog type name", but the mapping scenario requires `DECIMAL(p,s)` (p,s ≤ 36) to be declared `DECIMAL(p,s)`. A bare `DECIMAL` type name carries no precision or scale, so the mapping was not testable for a parameterized decimal.
- **Direction change:** Specified in `unity-catalog-client` Scenarios 1-2 (and its Background), and in `unity-catalog-create-virtual-schema` Background and the Spark-types scenario, that the descriptor carries the FULL parameterized Spark type — type name plus precision and scale from `type_precision`/`type_scale` or `type_text` — sufficient to declare `DECIMAL(p,s)`.
- **Promotes to ADR:** no

### [plan-review] Empty-namespace no-grant guarantee test migration named (trait-revision advisory)

- **Finding:** Deleting `resolve_namespace_virtual_tables` orphans the existing engine test `create_virtual_schema_over_empty_namespace_contacts_no_catalog_session` (`adapter_tests.rs:1718`, asserting at `:1746`), which calls it directly; neither the task list nor Dead Code Removal named the test's fate, so the empty-namespace no-grant guarantee could silently weaken.
- **Direction change:** Named the test in task 3.3 and the § Dead Code Removal `resolve_namespace_virtual_tables` row: its unreachable-URI + OAuth + empty-namespace → success assertion migrates to `crates/lakehouse-catalog/src/client_tests.rs::empty_namespace_builds_no_session_and_no_grant` against `IcebergRestCatalogClient::list_tables`, and the engine test is removed once the function is deleted.
- **Promotes to ADR:** no

### [plan-review] One-session helper distinct from the trait load_table (trait-revision advisory)

- **Finding:** Task 1.2 said `list_tables` "calls `load_table` per identifier". Because `IcebergRestCatalogClient` holds no session, calling the trait `load_table` per identifier rebuilds a session per call → N sessions, contradicting the one-session guarantee `enumeration_builds_exactly_one_session` pins; and decision [9] justified `load_table` as "the listing path's own per-table step", which the consistent design falsifies.
- **Direction change:** Reworded task 1.2 (and the Architecture diagram) so `list_tables` builds ONE `CatalogSession` and reuses it across identifiers via a PRIVATE session-taking helper DISTINCT from the trait `load_table`. Restated decision [9]'s `load_table` warrant as the user-requested single-table load and the #319/#320 scan-path source, explicitly not the listing path's per-table step.
- **Promotes to ADR:** no

### [plan-review] Summary split into ≤25-word sentences (trait-revision advisory)

- **Finding:** The plan § Summary ran two ~30- and ~38-word sentences, each exceeding the 25-word cap, and stacked two "Iceberg REST" clauses before reaching the load-bearing conclusion.
- **Direction change:** Split the Summary into five sentences each ≤25 words, leading with the conclusion that both catalog kinds share one `CatalogClient` operation surface.
- **Promotes to ADR:** no

### [plan-review] Deleted resolve_table_schema left pushdown-catalog-session scenarios unauthored (round-2 BLOCKER)

- **Finding:** Deleting `resolve_table_schema` (task 3.3) invalidated two normative scenarios of the live recorded feature `vs-adapter/pushdown-catalog-session`, against which the plan carried no delta. Scenario "CatalogSession is public and every file-resolution entry point takes one" (line 76) still required `resolve_table_schema` to take `&CatalogSession`; scenario "createVirtualSchema resolves every table's schema on one shared session" (lines 60-70) described an adapter-side schema loop calling it per table — the exact mechanism the plan relocates. Task 3.3 also mis-attributed the `catalog_session_signatures.rs` edit to `vs-adapter/pushdown-module-structure`, whose delta covers only the façade re-export set, not these behavioral scenarios. At `/speq:record` the library would retain a `SHALL` clause naming a deleted function and a scenario describing a deleted mechanism, with its test removed — the same defect class as the round-1 façade blocker.
- **Direction change:** Added a CHANGED delta `vs-adapter/pushdown-catalog-session` (new Features row). It SUPERSEDES the line-76 clause so `resolve_file_list` becomes the sole `&CatalogSession`-taking file-resolution entry point and `resolve_table_schema` leaves that set, and SUPERSEDES the createVirtualSchema-schema-loop scenario so its one-session-per-enumeration, empty-namespace-no-grant, skip-non-loadable, two-grants-on-OAuth-mode, and grant-failure-before-loop guarantees relocate into `IcebergRestCatalogClient::list_tables`, tested by `crates/lakehouse-catalog/src/client_tests.rs::enumeration_builds_exactly_one_session` and `::empty_namespace_builds_no_session_and_no_grant`. Corrected task 3.3 and the § Dead Code Removal row so the `catalog_session_signatures.rs` edit (dropping the `schema_resolution_entry_point_takes_a_shared_session` proof and its covered-scenario doc line) is recorded by this new delta, not by `pushdown-module-structure`. The mechanism moves behind the trait; behavior stays byte-identical.
- **Promotes to ADR:** no

### [plan-review] Deleted resolve_table_schema left create-virtual-schema fold-home clause naming a dead function (round-2 BLOCKER)

- **Finding:** The plan moves the Iceberg column-name case-fold out of `resolve_table_schema` into the shared listing pipeline and deletes the function, but the live recorded feature `vs-adapter/create-virtual-schema` pins that fold to `resolve_table_schema` by name (scenario "Create virtual schema enumerates every table in the configured namespace", line 73: "that fold SHALL be owned by exactly ONE site, resolve_table_schema … and no other code path SHALL declare a differently-cased name"). The plan carried no delta against the feature, so the "one fold home = `resolve_table_schema`" invariant would record as false.
- **Direction change:** Added a CHANGED delta `vs-adapter/create-virtual-schema` (new Features row). It SUPERSEDES the line-73 clause so the single fold owner becomes the shared `CatalogClient` listing pipeline — the one home that folds every declared name for both catalog kinds — while keeping "owned by exactly ONE site" and "no other code path SHALL declare a differently-cased name" intact, and preserving byte-identical the full-Unicode expansion (`ß`→`SS`, `straße`→`STRASSE`) and the no-collision-check trade-off. The delta records that the enumeration mechanism moves behind the trait while enumerated tables, declared names and types, `TABLE_MAP`, warnings, and errors stay byte-identical.
- **Promotes to ADR:** no
