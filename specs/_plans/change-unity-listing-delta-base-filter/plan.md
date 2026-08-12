# Plan: change-unity-listing-delta-base-filter

## Summary

Scope the native Unity Catalog createVirtualSchema listing to Delta base tables only — report a virtual table iff `table_type` is `MANAGED` or `EXTERNAL` AND `data_source_format` is `DELTA`, and exclude and warn on every other listed entry. The filter lives inside the Unity Catalog client; the Iceberg REST path stays byte-identical.

## Design

### Context

The native Unity Catalog client (#318, PR #327) was meant to report only Delta-format base tables, but that intent was lost during planning and never recorded. The shipped client reports every entry the `GET /tables` sweep returns — views, non-`DELTA` formats (ICEBERG, CSV, PARQUET, JSON, …), and any `table_type` — and does not even deserialize `data_source_format`. The deferred Delta scan path (#319/#320) can only read Delta base tables, so every non-Delta or non-base virtual table this lists is unqueryable. This is listing-only scope (#318); the scan path is out of scope.

Three recorded invariants constrain the fix:

1. The shared listing pipeline (`build_listing_virtual_tables`) is kind-agnostic and MUST NOT branch on catalog kind (`vs-adapter/catalog-kind-selection`). The only site that matches `CatalogKind` is the client-construction site.
2. The Iceberg REST path — including its skipped-table warnings — MUST stay byte-identical (`vs-adapter/catalog-kind-selection` line 16; `vs-adapter/pushdown-catalog-session` line 74 pins the skip route and warning).
3. The only createVirtualSchema-time warning channel is `udf_log!(ctx, warn, …)` to the UDF script-output stream; it is not SQL-client visible.

- **Goals** — Report only Delta base tables under the Unity Catalog kind; warn per excluded entry with Unity-appropriate wording naming the reason; keep the Iceberg path and the shared pipeline untouched.
- **Non-Goals** — The Delta scan/planning path (#319/#320); reader-feature gating and full Delta type fidelity (#322); the `ICEBERG_NAMESPACE` property rename (#324); any change to the Iceberg REST listing, scan, or pushdown behavior.

### Decision

Put the Delta-base filter inside `UnityCatalogSession::list_tables`, and carry each skip's reason as neutral data on the skipped entry so the shared warn loop renders the message without a per-kind branch.

#### Architecture

```
GET /tables sweep (Unity)                     shared, kind-agnostic
   │  TableInfo{ table_type, data_source_format?, ... }
   ▼
UnityCatalogSession::list_tables            handle_create_virtual_schema
   │  admit iff Table && DELTA                  │  build_listing_virtual_tables  (UNTOUCHED)
   │  else → skipped(reason)                     │  warn loop: match SkippedTable.reason
   ▼                                             ▼    ├─ NotLoadableIcebergTable → byte-identical Iceberg line
CatalogListing {                               (no CatalogKind match here)
   tables:  Vec<CatalogTable>,                    └─ NotDeltaBaseTable{detail} → Unity line naming detail
   skipped: Vec<SkippedTable{ident, reason}>,
}
```

`data_source_format` is deserialized only inside the client and never enters a neutral type. The neutral `CatalogListing.skipped` element gains a neutral `SkipReason`; the adapter's existing warn loop matches that reason (neutral data, NOT `CatalogKind`) to render the per-kind sentence.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Filter at the source, hide the wire field | `UnityCatalogSession::list_tables` | The Delta/base decision needs `data_source_format`, a Unity-wire concept; deciding inside the client keeps the shared pipeline kind-agnostic and the field crate-private |
| Single owner of the skip decision; co-owned wording | client sets `SkipReason` with a disqualifier fragment; adapter owns the channel and surrounding sentence | The client that decides to skip owns why; message wording is intentionally co-owned (decision [2]) — the client supplies the `table_type=…` / `data_source_format=…` fragment as neutral detail, the adapter owns the log channel and the surrounding sentence — no back-door leakage, no second `CatalogKind` match |
| Render by neutral reason, not by kind | adapter warn loop matches `SkipReason` | Reproduces the Iceberg line byte-for-byte while giving Unity its own wording, without reintroducing a `CatalogKind` branch |
| Reuse the existing neutral classifier | `neutral_table_type` drives base-vs-not | Its `View`/`Other` variants, previously computed but unconsumed, now drive the skip decision |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Filter inside `UnityCatalogSession::list_tables`; `data_source_format` stays wire-private | Filter in the shared pipeline (kind branch); expose format on the neutral type | Keeps the pipeline kind-agnostic and the neutral type kind-free; matches interview Q1 |
| `CatalogListing.skipped: Vec<SkippedTable{ident, reason: SkipReason}>`, adapter renders per reason | (a) Generic kind-neutral message — loses the specific reason and changes the Iceberg text; (c) branch the warn loop on `CatalogKind` — reintroduces a second kind-match site and re-derives client knowledge in the adapter | Only this option preserves the byte-identical Iceberg warning AND gives Unity a specific reason AND adds no `CatalogKind` match |
| No delta to `vs-adapter/create-virtual-schema` or `vs-adapter/catalog-kind-selection` | Author an Iceberg-side delta | The Iceberg skip semantics and warning bytes are preserved; only internal plumbing changes, so those specs stay true |
| Unit + integration coverage for the exclusion; no new OSS E2E fixture | Add a VIEW / non-Delta OSS fixture | The wire vocabulary is spike-verified live; the decision logic is fully exercised by the mock-server client tests and the mock-UC engine tests |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| unity-catalog-create-virtual-schema | CHANGED | `vs-adapter/unity-catalog-create-virtual-schema/spec.md` |
| unity-catalog-client | CHANGED | `vs-adapter/unity-catalog-client/spec.md` |
| catalog-crate-structure | CHANGED | `vs-adapter/catalog-crate-structure/spec.md` |

## Impact

Under the native Unity Catalog kind, createVirtualSchema now exposes only Delta-format base tables (`MANAGED`/`EXTERNAL` + `DELTA`). Views and non-`DELTA`-format tables that #318 wrongly exposed — and that the deferred scan path could not read anyway — are no longer listed; each excluded entry is logged as one `warn` line in the UDF script-output stream, which is not SQL-client visible. This changes only the in-PR native Unity Catalog feature (#318 / PR #327, not yet merged to main); no released behavior changes. The Iceberg REST kind is unaffected — its listing, scan, pushdown, and skipped-table warnings stay byte-identical. The `lakehouse-catalog` public surface gains two neutral types (`SkipReason`, `SkippedTable`) and reshapes `CatalogListing.skipped`; this is an internal Rust-crate API within the one `.so`, not a user- or operator-facing surface.

## Dependencies

- Corrects #318. A GitHub issue tracking this correction SHALL be created (or #318 referenced) and cited in the implementing commit (`Closes #<n>`), per the project feature-tracking rule. The git/issue mechanics are a `/speq:implement` concern.
- **OSS `data_source_format` presence is a verification precondition, tracked in #323.** The entire filter turns on the `GET /tables` list response carrying `data_source_format` as uppercase `DELTA`. `SPIKE_UC_CLIENT.md` line 73 confirms this only against live Databricks; it does NOT confirm the OSS #325 harness list endpoint emits it. Before the exclusion assertions and `make test-e2e-unity` are trusted, confirm the OSS `GET /tables` list response carries `data_source_format=DELTA` for the vendored delta-kernel-rs Delta fixtures. If it does not (absent or lower-cased), the all-Delta fixtures are excluded — `make test-e2e-unity` yields an empty schema and production OSS deployments list zero tables, a regression from #318. The filter is case-sensitive by deliberate choice (decision [4]); an OSS casing or absence divergence is therefore caught by this precondition, not silently tolerated. If the precondition fails, seed `data_source_format=DELTA` into the #325 fixtures (or revisit decision [4]). If it cannot be verified before implement, it stays an explicit tracked assumption under #323, at the same rigor as the shallow-clone assumption (decision [5]).
- The shallow-clone inclusion assumption (a shallow clone surfaces as `MANAGED`/`EXTERNAL` + `DELTA`) is a tracked assumption (decision [5]); its live/OSS wire-shape verification is tracked in #323's fixture-matrix scope, cited here per the project `(#n)` tracking pattern — not a decision-log note that will not resurface.

## Implementation Tasks

1. **Neutral skip-reason model.** In `crates/lakehouse-catalog/src/client.rs`, add `SkipReason` (`NotLoadableIcebergTable`; `NotDeltaBaseTable { detail: String }`) and `SkippedTable { ident: CatalogTableIdent, reason: SkipReason }`; change `CatalogListing.skipped` to `Vec<SkippedTable>`; re-export both from `lib.rs`. [expert]
2. Update the Iceberg REST client's single 404-skip site (`resolve_listing`) to push `SkippedTable { ident, reason: SkipReason::NotLoadableIcebergTable }`; skip semantics stay unchanged.
3. **Unity Delta-base filter.** In `crates/lakehouse-catalog/src/unity/client.rs`, add `data_source_format: Option<String>` to `TableInfo` (serde default) and correct its doc comment; in `list_tables`, admit an entry as a neutral table iff `neutral_table_type` is `Table` AND `data_source_format` equals `DELTA` (uppercase compare), else push it into `skipped` with `SkipReason::NotDeltaBaseTable { detail }` where `detail` names `table_type=<raw>` or `data_source_format=<raw>`; extract the admission decision as a pure function for unit tests. [expert]
4. **Adapter warn render.** In `crates/lakehouse-engine/src/adapter/mod.rs`, thread the new `Vec<SkippedTable>` through `build_listing_virtual_tables` and the `handle_create_virtual_schema` warn loop; render one `warn` line per entry by matching `reason` — `NotLoadableIcebergTable` renders the byte-identical legacy Iceberg line, `NotDeltaBaseTable { detail }` renders a Unity line naming the identifier and detail — adding NO `CatalogKind` match. [expert]
5. Unity client unit tests (`crates/lakehouse-catalog/src/unity/client_tests.rs`): update `lists_tables_in_catalog_schema` (the VIEW entry is now skipped, not returned); add `includes_managed_and_external_delta_base_tables` (incl. a shallow-clone-shaped `MANAGED`/`EXTERNAL` + `DELTA` entry) and `skips_view_non_delta_and_other_type_with_reason`. **Fix the fixtures the new filter would silently exclude, symmetric to the engine `table_entry` fix in Task 6.** In `follows_pagination_across_pages`, add `"data_source_format":"DELTA"` to BOTH `MANAGED` page bodies (the `t1` and `t2` entries, lines ~172 and ~178) — without it they serde-default to `None`, are routed to `skipped`, and the `vec!["t1","t2"]` assertion goes red. Audit every other `list_tables`-driven fixture in the file and apply the named edits: `lists_tables_in_catalog_schema` / `tables_page_body` — no fixture edit (the `orders` entry already carries `DELTA`; the `orders_summary` VIEW is intentionally skipped, which this task's assertion update covers); `request_failure_is_credential_safe_error` — no edit (the 500 path errors before any entry is classified); `identical_request_shape_oss_and_databricks` / `empty_tables_body` — no edit (`{"tables":[]}` classifies no entries). The `single_table_body` fixtures drive `load_table`, not `list_tables`, so the Delta-base filter never runs on them — no edit.
6. Engine integration tests (`crates/lakehouse-engine/src/adapter/unity_schema_tests.rs`): set `data_source_format` `DELTA` on the `table_entry` fixture; replace `lists_view_with_columns_and_no_storage_location` with an exclusion test asserting the view is absent from the response and `TABLE_MAP`; add a non-`DELTA`-format + other-`table_type` exclusion test; keep `enumerates_unity_namespace_tables`, `listing_issues_no_per_table_get_table_call`, and `records_table_map_and_rejects_collision` green under the `DELTA` fixture.
7. **Catalog-crate mechanical `skipped` updates.** Update `crates/lakehouse-catalog/tests/catalog_public_surface.rs` — name `SkipReason` and `SkippedTable` in the imported `pub` set and construct `CatalogListing.skipped` with a `SkippedTable` entry (line ~169, `skipped: vec![ident]` → `skipped: vec![SkippedTable { ident, reason: ... }]`) — and `crates/lakehouse-catalog/src/client_tests.rs` (the Iceberg `resolve_listing` skip assertions now expect `SkippedTable`). Depends on Task 1 (types) and Task 2 (the Iceberg site pushes `SkippedTable`).
8. **Engine-side mechanical `skipped` updates.** Update `crates/lakehouse-engine/src/adapter/adapter_tests.rs` — `iceberg_listing_is_behavior_identical_behind_the_trait` (lines ~1658 and ~1690) constructs and asserts on the `skipped` element, whose type changes to `SkippedTable` when Task 4 changes `build_listing_virtual_tables`'s return shape — and `crates/lakehouse-engine/src/adapter/catalog_client_tests.rs`. Depends on Task 4.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group 1 (foundation) | Task 1 |
| Group 2 (after Task 1) | Task 2, Task 3, Task 4 |
| Group 3 (after prod code) | Task 5 (after Task 3), Task 6 (after Tasks 3 and 4), Task 7 (after Tasks 1 and 2), Task 8 (after Task 4) |

Sequential dependencies:
- Group 1 → Group 2 (the shared type must exist before the Iceberg site, Unity filter, and adapter render compile).
- Group 2 → Group 3 (every test task asserts on the production behavior it follows). Task 7's `adapter_tests.rs` was previously grouped parallel with Task 4, but its `iceberg_listing_is_behavior_identical_behind_the_trait` assertion depends on Task 4's return-shape change, so that portion moved to Task 8 in Group 3; Task 7 now holds only the catalog-crate fixtures, which follow Tasks 1 and 2.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | No production code becomes obsolete. The `View`/`Other` branches of `neutral_table_type`, previously computed but unconsumed, become consumed by the Unity filter rather than removed. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| unity-catalog-create-virtual-schema · Create virtual schema enumerates every table in the configured Unity Catalog namespace | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `enumerates_unity_namespace_tables` |
| unity-catalog-create-virtual-schema · Create virtual schema includes managed and external Delta base tables, including a shallow clone | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `lists_managed_external_and_shallow_clone_delta_tables` |
| unity-catalog-create-virtual-schema · Create virtual schema excludes every non-Delta-base entry and warns per exclusion | Integration | `crates/lakehouse-engine/src/adapter/unity_schema_tests.rs` | `excludes_view_non_delta_and_other_type_entries` |
| unity-catalog-client · The client lists tables in a configured catalog and schema | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `lists_tables_in_catalog_schema` |
| unity-catalog-client · The client returns managed and external Delta base tables including a shallow clone | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `includes_managed_and_external_delta_base_tables` |
| unity-catalog-client · The client routes a view, a non-Delta-format table, and any other table type into the skipped set with a reason | Integration | `crates/lakehouse-catalog/src/unity/client_tests.rs` | `skips_view_non_delta_and_other_type_with_reason` |

Both client tests exercise `UnityCatalogSession::list_tables` against the in-process mock HTTP server (no live network); the engine tests exercise `handle_create_virtual_schema` against the in-process `MockUnityCatalog`. The per-exclusion `warn` behavior is verified at the data level (the skipped set and its `SkipReason`), which is the surface that drives exactly one warn line per entry — the same way the Iceberg skip warning is exercised.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| unity-catalog-client | `cargo test -p lakehouse-catalog --lib unity::client` | New filter tests pass; the mock VIEW / non-`DELTA` entries land in `skipped` with a reason; `MANAGED`/`EXTERNAL` + `DELTA` entries are returned |
| unity-catalog-create-virtual-schema | `cargo test -p lakehouse-engine --lib adapter::unity_schema_tests` | The view and non-`DELTA` entries are absent from the response and `TABLE_MAP`; only the Delta base tables are listed |
| unity-catalog-create-virtual-schema (regression, all-Delta fixture) | `make test-e2e-unity` | The local UC fixture's Delta tables still list correctly against the live OSS container; suite fails (not skips) if the stack is unavailable. PRECONDITION: the OSS `GET /tables` list response MUST carry `data_source_format` as uppercase `DELTA` for the vendored fixtures (verify per Dependencies; tracked in #323). An empty schema here is the OSS `data_source_format` divergence surfacing, NOT a pass |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
