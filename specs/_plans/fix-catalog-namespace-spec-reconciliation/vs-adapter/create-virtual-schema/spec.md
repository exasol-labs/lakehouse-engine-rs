# Feature: Create Virtual Schema

Lets an Exasol user register every Iceberg table in a configured namespace (resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests) as queryable virtual tables, so each table's columns appear to Exasol with correctly mapped SQL types, and records — in the response adapterNotes — the per-node core count, parallelism factor, DataFusion threading and memory-budget controls, and the Exasol-name to Iceberg-identifier map so later pushdowns can size sharding and recover the scanned table. The cluster's active node count is NOT among the recorded values: each pushdown reads it from its own UDF handshake (see `vs-adapter/create-virtual-schema-adapter-notes` and `vs-adapter/pushdown-planning`).

## Background

* **This delta renames ONE VS property and changes nothing else.** Issue #324 renames `ICEBERG_NAMESPACE` to `NAMESPACE`. The property names a namespace in BOTH catalog kinds — the Iceberg REST kind this feature serves and the native Unity Catalog kind `vs-adapter/unity-catalog-create-virtual-schema` serves — so its Iceberg-era name misdescribed it the moment the second kind shipped. `vs-adapter/unity-catalog-create-virtual-schema` recorded the deferral ("a catalog-neutral rename is deferred to #324"); this delta discharges it.
* **The rename carries NO backwards compatibility and needs none.** There is no deployed virtual schema to migrate, so `ICEBERG_NAMESPACE` is REMOVED rather than accepted as an alias. A `createVirtualSchema` request that still supplies the old name fails with this feature's existing required-property error naming `NAMESPACE`, which is the loud failure a silent alias would hide.
* **No enumeration, listing, casing, `TABLE_MAP`, skip, abort, credential-redaction, or type-mapping behavior changes.** Only the string that selects the namespace changes; every response this feature produces for an equivalent request is byte-identical.
* **Apache Iceberg spec check: not implicated.** The rename reads no manifest, no snapshot, and no schema, and changes no scanning, pushdown, or type handling. There is no normative section to quote and no deviation to record.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies a `NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* when SigV4 is enabled, the adapter SHALL address the namespace and table list requests under the `catalogs/{warehouse}` REST prefix derived per `vs-adapter/pushdown-planning-cloud-credentials`, so a bare-account-id `warehouse` produces the Glue-required `catalogs/{account-id}` prefix
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table — whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing — and SHALL skip any listed table whose per-table `loadTable` returns HTTP 404 per the "One non-Iceberg table in the namespace is skipped" scenario
* *AND* the adapter SHALL declare each column's `"name"` as the Iceberg field name uppercased with FULL Unicode case mapping (Rust's `str::to_uppercase`), the same fold the table name receives, because Exasol resolves an unquoted identifier in user SQL by uppercasing it — so declaring the Iceberg casing verbatim would force every user query to double-quote every column name
* *AND* that fold SHALL be owned by exactly ONE site — the shared `CatalogClient` listing pipeline, which folds every declared name for BOTH catalog kinds and produces the `(name, Exasol type)` pairs the response's column list is built from — and no other code path SHALL declare a differently-cased name
* *AND* the full-Unicode fold's one-to-many expansions SHALL be recorded as a deliberate Exasol-target trade-off rather than left unstated: `ß` becomes `SS`, so an Iceberg column `straße` is queryable ONLY as the ASCII identifier `STRASSE` and the `ß`-bearing form resolves against no declared column, and two Iceberg columns in one table differing only in that expansion declare the same Exasol name with no collision check to reject it
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`
* *AND* a `createVirtualSchema` request that supplies no `NAMESPACE` property SHALL fail with the required-property error naming `NAMESPACE`, and the adapter MUST NOT accept `ICEBERG_NAMESPACE` as an alias for it
<!-- /DELTA:CHANGED -->
