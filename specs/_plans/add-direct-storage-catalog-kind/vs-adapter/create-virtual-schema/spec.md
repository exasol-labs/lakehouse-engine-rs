# Feature: Create Virtual Schema

Registers every table of a configured namespace as queryable virtual tables and records the cluster and table-name map in adapterNotes. This delta changes no recorded description. The paragraph here is plan-local context only.

## Background

* This delta amends exactly ONE clause of ONE scenario, for issue #407. The clause is the closing
  step of "Create virtual schema enumerates every table in the configured namespace". That step
  makes `NAMESPACE` required unconditionally. The third catalog kind, `DIRECT_STORAGE`, reaches its
  tables from the CONNECTION address alone. `NAMESPACE` is therefore OPTIONAL under it and names a
  subtree of that address. Every other step of that scenario, and every other scenario of this
  feature, is carried unchanged.
* The Iceberg REST and native Unity Catalog kinds are UNAFFECTED. `NAMESPACE` stays required for
  both, with the same error and the same wording, so no existing test changes.
* The reason the requirement is kind-scoped rather than dropped is that the property means different
  things per kind. For a catalog kind it selects a catalog namespace that the adapter cannot guess.
  For the direct-storage kind the CONNECTION address already names a storage prefix, so an absent
  `NAMESPACE` has a correct meaning: enumerate that address itself.
* `vs-adapter/direct-storage-properties` owns the direct-storage reading of `NAMESPACE`: its
  slash-delimited form, its rejection rules, and how it composes onto the CONNECTION address. This
  feature records only that the requirement no longer binds that kind.

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
* *AND* a `createVirtualSchema` request that resolves to a CATALOG kind and supplies no `NAMESPACE` property SHALL fail with the required-property error naming `NAMESPACE`, SUPERSEDING the unconditional form of this clause, because the `DIRECT_STORAGE` kind reaches its tables from the CONNECTION address alone and treats an absent `NAMESPACE` as the address itself, per `vs-adapter/direct-storage-properties`
<!-- /DELTA:CHANGED -->
