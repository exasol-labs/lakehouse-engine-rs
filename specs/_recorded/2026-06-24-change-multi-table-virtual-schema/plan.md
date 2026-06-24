# Plan: change-multi-table-virtual-schema

## Summary

Expand the lakehouse-engine Virtual Schema from a single create-time-fixed table (`TABLE_NAME`) to every table in a configured Iceberg namespace (`ICEBERG_NAMESPACE`), so an Exasol user can `SELECT`/`JOIN` across many lakehouse tables in one virtual schema. The change is confined to the VS-adapter layer: table identity moves from a fixed property to a create-time enumeration persisted in `adapterNotes` and recovered per pushdown from `involvedTables[0].name`.

## Design

### Context

Today the VS binds exactly one Iceberg table at `createVirtualSchema` time via `TABLE_NAME`. `resolve_connection_config` reads `TABLE_NAME`, `handle_create_virtual_schema` emits a single-element `schemaMetadata.tables`, and `handle_pushdown` always scans `catalog.table` (= `TABLE_NAME`), never consulting which table the request involves. Users want a namespace-scoped VS exposing all its tables.

The decisive protocol fact (verified by tracing the adapter and the Exasol VS request shape): **Exasol issues a separate single-table pushdown request per table, even for a JOIN.** JOIN is intentionally NOT advertised — Exasol joins the per-table result sets itself. Therefore each pushdown stays single-table and `CatalogProps`/`ScanSpec`/the scan UDF need **no change**; only the VS-adapter table-identity seam changes.

The hard sub-problem is round-tripping the table identity. Pushdown receives `involvedTables[0].name` = the Exasol table name (uppercased, `__`-flattened, e.g. `PROD__FINANCE__ORDERS`). It must map this back to the original-cased, multi-level Iceberg `TableIdent`. Exasol uppercases identifiers; Iceberg names are case-sensitive (usually lowercase). A naive reverse-flatten + lowercase will not reliably round-trip.

- **Goals** — Namespace-scoped multi-table VS; multi-level Iceberg namespaces flattened to Exasol names with `__`; deterministic, case-correct reverse mapping at pushdown; no change to the scan crate, sharding, or fan-out SQL.
- **Non-Goals** — JOIN pushdown to DataFusion (separate future plan); table-filter/allowlist properties; backward compatibility with `TABLE_NAME` (hard break); Iceberg view support (deferred — see Consequences).

### Decision

#### Architecture

```
createVirtualSchema (ICEBERG_NAMESPACE)
  → list_tables across the namespace + descendants (recursive)
  → for each table: flatten Iceberg ident → Exasol name (uppercase, "__")
  → schemaMetadata.tables = [ one virtual table per discovered table ]
  → adapterNotes.TABLE_MAP = { EXASOL_NAME: "orig.cased.iceberg.ident", ... }   (persisted by Exasol)

pushdown (involvedTables[0].name = EXASOL_NAME)
  → read TABLE_MAP back from schemaMetadataInfo.adapterNotes
  → look up EXASOL_NAME → original-cased Iceberg identifier
  → CatalogProps.table = that identifier   (single table, unchanged downstream)
  → resolve_file_list / build scan SQL exactly as today
```

**Chosen strategy: (B) adapterNotes name→identifier map.** `createVirtualSchema` must enumerate the namespace anyway (to build `schemaMetadata.tables`), so recording the `EXASOL_NAME → iceberg-identifier` map in `adapterNotes` at the same time is essentially free. Pushdown then reads it back — no second catalog round-trip, and the original Iceberg casing and multi-level namespace path are recovered exactly (deterministic, collision-detectable at create time). `adapterNotes` is already the project's persisted round-trip channel (`CLUSTER_NODES`, `NR_OF_CORES`, etc.), so the mechanism is proven.

Strategy (A) (re-list + case-insensitive match at pushdown) was rejected: it adds a catalog call per query and, critically, the **SigV4/Glue path has no signed listing implementation** — only `load_table` is self-issued/signed (`load_table_signed`). Re-listing under SigV4 would require implementing signed `list_namespaces`/`list_tables`, which strategy (B) avoids because the one listing happens at create time through the same path that already resolves the schema.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single shared flatten helper | `adapter` (new `tables` module or in `mod.rs`) | Create and pushdown MUST agree on the `__`/uppercase mapping; one function prevents drift |
| Persist-at-create / read-at-pushdown via `adapterNotes` | `build_adapter_notes` / `adapter_note` | Reuses the proven, Exasol-persisted round-trip channel; no per-query re-listing |
| Recursive namespace traversal | `Catalog::list_namespaces(parent)` + `list_tables` | `list_namespaces` returns only direct children; descendants need explicit recursion |
| Multi-segment identifier parse | `parse_table_ident` → `NamespaceIdent::from_vec` | Supports `prod.finance.orders`; the current `splitn(2, '.')` only handles one namespace level |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `TABLE_MAP` in `adapterNotes` (strategy B) | (A) re-list + case-insensitive match at pushdown | B avoids a per-query catalog call and a new signed-listing impl for the SigV4 path; recovers exact casing + multi-level path deterministically |
| Hard break: drop `TABLE_NAME`, add `ICEBERG_NAMESPACE` | Backward-compat shim accepting either | PoC/benchmark context; existing VS instances are recreated; a shim is dead complexity |
| Expose ALL tables in the namespace | Allowlist/filter property | User scopes by choosing a narrow namespace; no extra property surface |
| `__` collision → hard error at create time | Silently overwrite / suffix-disambiguate | Deterministic and safe; collision is a known accepted limitation, but must fail loudly, not corrupt the map |
| Defer Iceberg view support | List + map views alongside tables now | iceberg-rust 0.9.1 `Catalog` trait has no `list_views`; tables are the requirement. Views are a follow-up; noted, not built |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |

`vs-adapter/connection-credentials` needs **no delta**: it resolves only `CATALOG_CONNECTION` + the credential JSON and never reads `TABLE_NAME`. The property rename lives entirely in create-virtual-schema and pushdown-planning. `scan/spec.rs` (`CatalogProps`, `ScanSpec`) and `adapter/capabilities.rs` (JOIN stays unadvertised) are confirmed unchanged.

## Migration

| Current | New |
|---------|-----|
| `TABLE_NAME = 'ns.table'` VS property (single table) | `ICEBERG_NAMESPACE = 'ns'` VS property (all tables in namespace + descendants) |
| `schemaMetadata.tables` = 1 element | `schemaMetadata.tables` = N elements, one per discovered table |
| Pushdown scans fixed `catalog.table` | Pushdown derives the table from `involvedTables[0].name` via `adapterNotes.TABLE_MAP` |

Existing virtual schemas referencing `TABLE_NAME` MUST be dropped and recreated with `ICEBERG_NAMESPACE`. No shim.

## Implementation Tasks

1. **Shared flatten/identifier helpers**
   1.1 Add `flatten_table_name(configured_ns: &[String], ident: &TableIdent) -> String` and the identifier-string form (`dot-joined original-cased`) in a single adapter helper, with unit tests for single-level, multi-level, and casing.
   1.2 Replace `parse_table_ident`'s `splitn(2, '.')` with a multi-segment split building `NamespaceIdent::from_vec` (trailing segment = table name); keep the single-level case working. [expert]

2. **Namespace enumeration at createVirtualSchema**
   2.1 Add `PROP_ICEBERG_NAMESPACE`, remove `PROP_TABLE`; change `resolve_connection_config` to no longer require `TABLE_NAME` (catalog/storage/creds only; table identity resolved later).
   2.2 Implement recursive namespace listing (`list_namespaces(parent)` + `list_tables`) for the unsigned REST path, returning the full set of `TableIdent`s under the configured namespace. [expert]
   2.3 Implement the SigV4/Glue listing path for the same enumeration (signed `list_namespaces`/`list_tables` GETs mirroring `load_table_signed`), or, if the signed list endpoint is unavailable, resolve the documented limitation explicitly. [expert]
   2.4 For each discovered table, resolve its schema (reuse `resolve_table_schema` per table) and build one `schemaMetadata.tables` entry; map columns via the existing type-mapping.

3. **TABLE_MAP in adapterNotes**
   3.1 Build the `EXASOL_NAME → iceberg-identifier` map and serialize it into `adapterNotes` via `build_adapter_notes` (merge, not clobber, alongside the existing entries). [expert]
   3.2 Detect `__` collisions during map construction and return a clear error naming the colliding Exasol table name.
   3.3 Add an `adapter_note`-style reader that parses `TABLE_MAP` back into a lookup map at pushdown time.

4. **Pushdown table derivation**
   4.1 Read `involvedTables[0].name` in `handle_pushdown_request`/`handle_pushdown`, look it up in `TABLE_MAP`, and set the resolved Iceberg identifier on the `CatalogProps` used for this pushdown. [expert]
   4.2 Return a clear error when the involved virtual table name is absent from `TABLE_MAP` (no silent stale-table scan).
   4.3 Confirm `resolve_file_list` / `resolve_table_schema` consume the derived identifier unchanged (they already call `parse_table_ident`).

5. **Tests**
   5.1 Unit tests for flatten/unflatten round-trip, multi-level parse, collision detection, and pushdown table lookup (absent → error).
   5.2 Update E2E (`e2e_scan_test.rs`, `common/seed.rs`): seed a second table (and/or a child namespace) in `e2e_lakehouse`, create the VS with `ICEBERG_NAMESPACE`, and assert both tables are queryable and a two-table query (Exasol-side join) returns correct rows.

6. **Capability docs**
   6.1 Move `specs/_plans/change-multi-table-virtual-schema/capabilities.md` to `docs/capabilities.md` (repo docs root, created if absent); drop the "Destination" note banner. The table documents advertised capabilities with examples and the DataFusion-vs-Exasol execution split. Keep it as the source-of-truth pointer alongside `adapter/capabilities.rs`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 |
| Group B | 2.1, 2.2, 2.3 |
| Group C | 3.1, 3.2, 3.3, 4.1, 4.2, 4.3 |
| Group D | 5.1, 5.2, 6.1 |

Sequential dependencies:
- Group A → Group B (enumeration uses the helpers)
- Group B → Group C (map construction and pushdown lookup use enumeration + helpers)
- Group C → Group D (E2E exercises the full path)

Task 6.1 (docs move) is independent of the test tasks and may run any time within Group D.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Const | `adapter/mod.rs` `PROP_TABLE` | Replaced by `PROP_ICEBERG_NAMESPACE`; `TABLE_NAME` support dropped |
| Logic | `adapter/mod.rs` `resolve_connection_config` `TABLE_NAME` read + single `catalog_block(... table)` call | Table identity no longer fixed at config-resolution time |
| Logic | `adapter/mod.rs` `handle_create_virtual_schema` single-element `tables` + `catalog.table.split('.').next_back().to_uppercase()` | Replaced by namespace enumeration loop |
| Test | `e2e_scan_test.rs` create-VS block using `TABLE_NAME` | Replaced by `ICEBERG_NAMESPACE` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Create virtual schema enumerates every table in the configured namespace | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_create_vs_enumerates_namespace_tables` |
| Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `create_vs_records_table_map_in_adapter_notes` |
| Multi-level Iceberg namespaces flatten deterministically into Exasol table names | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `flatten_multilevel_namespace_and_detect_collision` |
| Create virtual schema fails clearly when the catalog is unreachable | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_create_vs_unreachable_catalog_errors_no_leak` |
| Pushdown derives the scanned Iceberg table from the involved virtual table | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_pushdown_scans_table_from_involved_tables` |
| Pushdown resolves the file list once and builds a scan-driving query | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_pushdown_resolves_files_once_multi_table` |
| Pushdown resolves multi-level namespace identifiers into the iceberg TableIdent | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `parse_table_ident_handles_multilevel_namespace` |
| (pushdown unknown involved table → error) | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` (tests) | `pushdown_unknown_involved_table_errors` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| create-virtual-schema | `CREATE VIRTUAL SCHEMA LH USING ...ADAPTER WITH CATALOG_CONNECTION='...' ICEBERG_NAMESPACE='e2e_lakehouse' SCAN_SCHEMA='...' ALLOW_HTTP='true';` then `SELECT TABLE_NAME FROM SYS.EXA_ALL_TABLES WHERE TABLE_SCHEMA='LH';` | One row per Iceberg table in `e2e_lakehouse` (and descendants), names uppercased and `__`-flattened |
| pushdown-planning | `SELECT a.id, b.label FROM LH.EVENTS a JOIN LH.OTHER_TABLE b ON a.id=b.id LIMIT 5;` | Correct joined rows; Exasol issues one pushdown per table, each scanning only its own Iceberg files |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, if no Exasol Docker) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
