# Feature: Create Virtual Schema

Lets an Exasol user register every table in a configured namespace as queryable virtual tables. This delta moves the column-name case-fold home from the deleted `resolve_table_schema` to the shared `CatalogClient` listing pipeline, keeping the single-fold-owner invariant and the full-Unicode trade-off byte-identical.

## Background

* This delta moves the column-name case-fold home from the deleted `resolve_table_schema` to the shared `CatalogClient` listing pipeline (plan `add-native-unity-catalog-client`, issue #318). The single-fold-owner invariant holds; only its owning site changes. The enumeration mechanism moves behind the trait, but the enumerated tables, declared names and types, `TABLE_MAP`, warnings, and errors stay byte-identical.
* This delta SUPERSEDES the `*AND* that fold SHALL be owned by exactly ONE site, resolve_table_schema (adapter/pushdown/file_resolution.rs)…` clause (line 73) of the scenario "Create virtual schema enumerates every table in the configured namespace". The one fold owner becomes the shared `CatalogClient` listing pipeline — the one home that folds every declared name for BOTH the Iceberg REST and native Unity Catalog kinds — replacing the `resolve_table_schema` naming.
* The supersession keeps "owned by exactly ONE site" and "no other code path SHALL declare a differently-cased name" intact, and preserves byte-identical the full-Unicode `to_uppercase` expansion (`ß`→`SS`, so `straße`→`STRASSE`) and the no-collision-check column trade-off. The fold rule, its Exasol identifier-resolution reason, and its trade-off are unchanged; only the owning site moves.
* Recorded Background prose citing `resolve_table_schema` at `file_resolution.rs:610-644` and `adapter/mod.rs:255/551/576` as the sole fold and column-declaration site is superseded by the same move. The `(name, Exasol type)` production relocates into the shared pipeline, and `IcebergRestCatalogClient::load_table` supplies the ordered original-cased columns the pipeline folds.
* No other scenario of this feature changes. Enumeration, the 404-skip and all-Hive-empty behavior, the non-404 abort, the unreachable-catalog error, `TABLE_MAP`/`adapterNotes` recording, the multi-level flatten collision, and the non-ASCII end-to-end round-trip all stay as recorded.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies an `ICEBERG_NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* when SigV4 is enabled, the adapter SHALL address the namespace and table list requests under the `catalogs/{warehouse}` REST prefix derived per `vs-adapter/pushdown-planning-cloud-credentials`, so a bare-account-id `warehouse` produces the Glue-required `catalogs/{account-id}` prefix
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table — whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing — and SHALL skip any listed table whose per-table `loadTable` returns HTTP 404 per the "One non-Iceberg table in the namespace is skipped" scenario
* *AND* the adapter SHALL declare each column's `"name"` as the Iceberg field name uppercased with FULL Unicode case mapping (Rust's `str::to_uppercase`), the same fold the table name receives, because Exasol resolves an unquoted identifier in user SQL by uppercasing it — so declaring the Iceberg casing verbatim would force every user query to double-quote every column name
* *AND* that fold SHALL be owned by exactly ONE site — the shared `CatalogClient` listing pipeline, which folds every declared name for BOTH catalog kinds and produces the `(name, Exasol type)` pairs the response's column list is built from — and no other code path SHALL declare a differently-cased name
* *AND* the full-Unicode fold's one-to-many expansions SHALL be recorded as a deliberate Exasol-target trade-off rather than left unstated: `ß` becomes `SS`, so an Iceberg column `straße` is queryable ONLY as the ASCII identifier `STRASSE` and the `ß`-bearing form resolves against no declared column, and two Iceberg columns in one table differing only in that expansion declare the same Exasol name with no collision check to reject it
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`
<!-- /DELTA:CHANGED -->
