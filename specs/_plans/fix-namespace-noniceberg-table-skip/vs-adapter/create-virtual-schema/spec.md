# Feature: Create Virtual Schema

Lets an Exasol user register every Iceberg table in a configured namespace (resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests) as queryable virtual tables, so each table's columns appear to Exasol with correctly mapped SQL types, and records — in the response adapterNotes — the cluster's active node count, per-node core count, parallelism factor, DataFusion threading and memory-budget controls, and the Exasol-name to Iceberg-identifier map so later pushdowns can size sharding and recover the scanned table.

## Background

The catalog endpoint and storage credentials are supplied through the Exasol CONNECTION object named by the `CATALOG_CONNECTION` property; the namespace to expose is supplied by the `ICEBERG_NAMESPACE` property. The adapter holds no state between requests other than the values it returns in `schemaMetadata.adapterNotes`, which Exasol persists.

* Catalog endpoint and storage credentials are supplied through a CONNECTION object
  named by `CATALOG_CONNECTION`. The adapter resolves credentials via `ctx.connection`
  and never persists catalog metadata between requests.
* Credentials MUST NOT appear in any returned response or error message.
* The adapter holds no state between requests; cluster information is resolved per
  request and returned in the `createVirtualSchema` response's `adapterNotes`
  (stringified JSON), which Exasol persists and round-trips back at pushdown time.
  The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so`; it speaks the
  Exasol virtual-schema JSON protocol (request in, JSON response out).
* Schema mapping MUST use the same mapping as the scan, defined in the
  `datafusion-scan/type-mapping` feature. Columns whose Arrow type Exasol cannot
  represent (list, struct, map, binary, out-of-range decimal, and the other incompatible
  types) are declared as `VARCHAR(2000000)` — they MUST NOT cause `createVirtualSchema`
  to error.
* Cluster configuration and the Exasol-name to Iceberg-identifier map are recorded in
  `adapterNotes` per `vs-adapter/create-virtual-schema-adapter-notes`.
* On the SigV4/Glue path the namespace/table enumeration requests address the
  catalog under the `catalogs/{account-id}` REST prefix derived per
  `vs-adapter/pushdown-planning-cloud-credentials` — the same derivation used by the
  `loadTable` path. This enumeration path is the one that produced the Glue
  `400 "Prefix must follow the 'catalogs/{catalogId}' format."` error (#123).
* A namespace listing can include non-Iceberg tables (e.g. AWS Glue Hive external
  tables). Per-table schema resolution loads each table via `loadTable`; a non-Iceberg
  table's `loadTable` returns HTTP 404 (Iceberg REST OpenAPI `NoSuchTableException`;
  Glue's `NoSuchIcebergTableException`). A 404 table is skipped, not fatal (#138); every
  other per-table load failure aborts the request. A namespace in which *every* listed
  table 404s (an all-Hive namespace) is not an error: `createVirtualSchema` still succeeds,
  yielding a virtual schema with an empty table set and a per-table warning for each skipped
  table.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies an `ICEBERG_NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* when SigV4 is enabled, the adapter SHALL address the namespace and table list requests under the `catalogs/{warehouse}` REST prefix derived per `vs-adapter/pushdown-planning-cloud-credentials`, so a bare-account-id `warehouse` produces the Glue-required `catalogs/{account-id}` prefix
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table — whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing — and SHALL skip any listed table whose per-table `loadTable` returns HTTP 404 per the "One non-Iceberg table in the namespace is skipped" scenario
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: One non-Iceberg table in the namespace is skipped rather than aborting the schema

* *GIVEN* a `createVirtualSchema` request whose configured namespace contains a mix of Iceberg tables and at least one non-Iceberg table (e.g. an AWS Glue Hive external table) that the catalog's table listing returns but whose per-table `loadTable` request returns HTTP 404 — the Iceberg REST OpenAPI `NoSuchTableException` response ("table to load does not exist"), which AWS Glue returns as `NoSuchIcebergTableException: Input table is not an iceberg table` (#138)
* *WHEN* the adapter enumerates the namespace and resolves each table's schema
* *THEN* the adapter SHALL exclude the non-Iceberg table from the response `schemaMetadata.tables` list and complete `createVirtualSchema` successfully with the remaining Iceberg tables
* *AND* the adapter SHALL exclude the non-Iceberg table's Exasol name from the `TABLE_MAP` recorded in `adapterNotes`, so no unqueryable virtual table is advertised
* *AND* the adapter SHALL write one warning line to script output via `udf_log!(ctx, warn, ...)` naming the skipped Iceberg identifier and the reason (HTTP 404 / not an Iceberg table), and that message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A namespace whose every table is non-Iceberg yields an empty virtual schema

* *GIVEN* a `createVirtualSchema` request whose configured namespace lists only non-Iceberg tables — every listed table's per-table `loadTable` returns HTTP 404
* *WHEN* the adapter enumerates the namespace and resolves each table's schema
* *THEN* the adapter SHALL complete `createVirtualSchema` successfully with an empty `schemaMetadata.tables` list and an empty `TABLE_MAP` in `adapterNotes`, rather than aborting
* *AND* the adapter SHALL write one `udf_log!(ctx, warn, ...)` warning line per skipped identifier, and none of those messages MUST contain storage access keys, secret keys, session tokens, or any SigV4 signing key
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A non-404 per-table load failure aborts createVirtualSchema

* *GIVEN* a `createVirtualSchema` request whose configured namespace lists a table whose per-table `loadTable` request fails with something other than HTTP 404 — a transport error, or an HTTP 401, 403, 419, 503, or any other non-404 status
* *WHEN* the adapter resolves that table's schema during enumeration
* *THEN* the adapter SHALL abort `createVirtualSchema` with an error describing that the table could not be loaded, preserving the "fails clearly when the catalog is unreachable" contract, and MUST NOT silently skip the table — a non-404 failure signals a catalog-wide fault (auth, throttling, outage), and skipping it would hide a misconfiguration behind a partial schema
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key
<!-- /DELTA:NEW -->
