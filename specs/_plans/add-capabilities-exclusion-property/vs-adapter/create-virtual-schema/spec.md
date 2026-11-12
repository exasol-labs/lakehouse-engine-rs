# Feature: Create Virtual Schema

An Exasol user registers every Iceberg table in a configured namespace as a virtual table, resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests. Each table's columns appear in Exasol with mapped SQL types, the response's schema-level `adapterNotes` records the resource budgets, and each virtual table's own `adapterNotes` records its catalog identifier for later pushdowns.

## Background

* The `CATALOG_CONNECTION` property names the Exasol CONNECTION that holds the catalog endpoint and storage credentials, and `NAMESPACE` names the namespace to expose. The adapter resolves credentials through `ctx.connection`.
* The adapter holds no state between requests other than the schema-level, table-level, and column-level `adapterNotes` it returns, which Exasol persists and returns at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties. `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` exposes the notes.
* Credentials never appear in a returned response or error message.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so` and speaks the Exasol virtual-schema JSON protocol.
* Schema mapping uses the mapping of `scan-types/type-mapping`. A column whose Arrow type Exasol cannot represent (list, struct, map, binary, out-of-range decimal) is declared `VARCHAR`, and does not make `createVirtualSchema` fail.
* The schema-level cluster-configuration entries follow `vs-properties/create-virtual-schema-adapter-notes`. The cluster node count is not recorded, because each pushdown reads it from its own UDF handshake (`pushdown/pushdown-planning`).
* A virtual table's identifier note is its table-level `adapterNotes`: a JSON object string whose `name` member holds the table's original-cased catalog identifier. Exasol persists it in `SYS.EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES` and returns it on every pushdown in `involvedTables[i].adapterNotes` (verified on Exasol 2025.1.16 and 8.29.13, #425).
* With SigV4 enabled, table enumeration addresses the catalog under the `catalogs/{account-id}` REST prefix derived per `storage-access/pushdown-planning-cloud-credentials`, the same derivation the `loadTable` path uses. Glue rejects another prefix with `400 "Prefix must follow the 'catalogs/{catalogId}' format."`.
* On Glue (a SigV4 catalog per `connection/connection-credentials-sigv4`), enumeration lists only the configured namespace, because Glue namespaces are single-level and Glue rejects a `parent` listing with `400 "Glue dataCatalog does not support multipart namespace."`. Glue selects up to 100 tables per page before it drops views, so a page can carry no identifier and still a `next-page-token`.
* A namespace listing can include non-Iceberg tables, such as AWS Glue Hive external tables or Databricks Delta tables. Per-table schema resolution loads each table through `loadTable`, and a non-Iceberg table returns HTTP 404 (Iceberg REST `NoSuchTableException`, Glue `NoSuchIcebergTableException`). Databricks Unity Catalog answers HTTP 400 instead (`is not an Iceberg compatible table`). The adapter skips a 404 or 400 table, with a 400's load error as the skip reason. Every other per-table load failure aborts the request. A namespace in which every table is skipped is not an error: the virtual schema has no tables and one warning per skipped table.
* The adapter declares each column name uppercased with full Unicode case mapping (`str::to_uppercase`), the same fold the table name receives. Exasol resolves an unquoted identifier by uppercasing it, so verbatim Iceberg casing would force double quotes on every column name in user SQL. The shared `CatalogClient` listing pipeline is the one site that folds declared names for both catalog kinds.
* The fold expands `ß` to `SS`, so an Iceberg column `straße` is queryable only as `STRASSE`. Two columns that differ only in that expansion declare the same Exasol name, and the column path has no collision check.
* `vs-adapter/create-virtual-schema-declaration-details` owns multi-level namespace flattening, the non-ASCII round trip, the `database_version()` read, and `TIMESTAMP(p)` serialization.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter reports its pushdown capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the adapter SHALL return a JSON response of type `getCapabilities` whose list is the adapter's `CAPABILITIES` list, less the names that the virtual schema's `EXCLUDED_CAPABILITIES` withholds per `vs-properties/excluded-capabilities`
* *AND* the capabilities list MUST NOT include `FN_PRED_GREATER` or `FN_PRED_GREATEREQUAL` (those names do not exist in the Exasol capability vocabulary, Exasol normalises `a > b` to `b < a` and `a >= b` to `b <= a` before it reaches the adapter), geospatial (`FN_ST_*`), Exasol-only session functions (`FN_CURRENT_USER`/`FN_SYS_GUID`/`FN_CURRENT_SCHEMA`), the now-family date/time functions (`FN_CURRENT_DATE`/`FN_CURRENT_TIMESTAMP`/`FN_SYSDATE`/`FN_SYSTIMESTAMP`), whose withdrawal and reason are owned by `pushdown-capabilities/pushdown-planning-capability-extensions`, `LITERAL_INTERVAL`, `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, any `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, or an outer, all-condition, or Cartesian join capability
* *AND* every advertised capability name MUST be one the adapter can either translate via the VS expression translator or decompose into a correct partial/merge plan, so the advertised set never claims behaviour the engine cannot execute correctly
<!-- /DELTA:CHANGED -->
