# Feature: Create Virtual Schema

An Exasol user registers every Iceberg table in a configured namespace as a virtual table, resolved through an Iceberg REST catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests. Each table's columns appear in Exasol with mapped SQL types, and the response `adapterNotes` records the resource budgets and the Exasol-name to Iceberg-identifier map for later pushdowns.

## Background

* The `CATALOG_CONNECTION` property names the Exasol CONNECTION that holds the catalog endpoint and storage credentials, and `NAMESPACE` names the namespace to expose. The adapter resolves credentials through `ctx.connection`.
* The adapter holds no state between requests other than the `adapterNotes` it returns, which Exasol persists and returns at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties. `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` exposes the notes.
* Credentials never appear in a returned response or error message.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so` and speaks the Exasol virtual-schema JSON protocol.
* Schema mapping uses the mapping of `scan-types/type-mapping`. A column whose Arrow type Exasol cannot represent (list, struct, map, binary, out-of-range decimal) is declared `VARCHAR(2000000)` and does not make `createVirtualSchema` fail.
* The adapter cluster-configuration entries and `TABLE_MAP` follow `vs-adapter/create-virtual-schema-adapter-notes`. The cluster node count is not recorded, because each pushdown reads it from its own UDF handshake (`pushdown/pushdown-planning`).
* With SigV4 enabled, namespace and table enumeration addresses the catalog under the `catalogs/{account-id}` REST prefix derived per `storage-access/pushdown-planning-cloud-credentials`, the same derivation the `loadTable` path uses. Glue rejects another prefix with `400 "Prefix must follow the 'catalogs/{catalogId}' format."`.
* A namespace listing can include non-Iceberg tables, such as AWS Glue Hive external tables. Per-table schema resolution loads each table through `loadTable`, and a non-Iceberg table returns HTTP 404 (Iceberg REST `NoSuchTableException`, Glue `NoSuchIcebergTableException`). The adapter skips a 404 table. Every other per-table load failure aborts the request. A namespace in which every table returns 404 is not an error: the virtual schema has no tables and one warning per skipped table.
* The adapter declares each column name uppercased with full Unicode case mapping (`str::to_uppercase`), the same fold the table name receives. Exasol resolves an unquoted identifier by uppercasing it, so verbatim Iceberg casing would force double quotes on every column name in user SQL. The shared `CatalogClient` listing pipeline is the one site that folds declared names for both catalog kinds.
* The fold expands `ß` to `SS`, so an Iceberg column `straße` is queryable only as `STRASSE`. Two columns that differ only in that expansion declare the same Exasol name, and the column path has no collision check.
* `vs-adapter/create-virtual-schema-declaration-details` owns multi-level namespace flattening, the non-ASCII round trip, the `database_version()` read, and `TIMESTAMP(p)` serialization.

## Scenarios

### Scenario: Adapter reports its pushdown capabilities

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the adapter SHALL return a JSON response of type `getCapabilities` whose list is the adapter's `CAPABILITIES` list
* *AND* the capabilities list MUST NOT include `FN_PRED_GREATER` or `FN_PRED_GREATEREQUAL` (those names do not exist in the Exasol capability vocabulary, Exasol normalises `a > b` to `b < a` and `a >= b` to `b <= a` before it reaches the adapter), geospatial (`FN_ST_*`), Exasol-only session functions (`FN_CURRENT_USER`/`FN_SYS_GUID`/`FN_CURRENT_SCHEMA`), the now-family date/time functions (`FN_CURRENT_DATE`/`FN_CURRENT_TIMESTAMP`/`FN_SYSDATE`/`FN_SYSTIMESTAMP`), whose withdrawal and reason are owned by `pushdown-capabilities/pushdown-planning-capability-extensions`, `LITERAL_INTERVAL`, `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, any `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, or an outer, all-condition, or Cartesian join capability
* *AND* every advertised capability name MUST be one the adapter can either translate via the VS expression translator or decompose into a correct partial/merge plan, so the advertised set never claims behaviour the engine cannot execute correctly

### Scenario: Create virtual schema enumerates every table in the configured namespace

* *GIVEN* an Iceberg REST catalog reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *AND* a `createVirtualSchema` request that supplies a `NAMESPACE` property naming an Iceberg namespace (one or more dot-separated levels, e.g. `finance` or `prod.finance`)
* *WHEN* Exasol sends the `createVirtualSchema` request
* *THEN* the adapter SHALL list every table contained in that namespace and in each of its descendant namespaces, resolving credentials via `CATALOG_CONNECTION` and SigV4-signing the catalog requests when enabled
* *AND* when SigV4 is enabled, the adapter SHALL address the namespace and table list requests under the `catalogs/{warehouse}` REST prefix derived per `storage-access/pushdown-planning-cloud-credentials`, so a bare-account-id `warehouse` produces the Glue-required `catalogs/{account-id}` prefix
* *AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg table, whose Exasol name is the namespace segments below the configured namespace plus the table name joined with `__` and uppercased, mapping each Iceberg field to an Exasol SQL type per the type-mapping table and declaring any incompatible type as VARCHAR rather than failing, and SHALL skip any listed table whose per-table `loadTable` returns HTTP 404 per the "One non-Iceberg table in the namespace is skipped" scenario
* *AND* the adapter SHALL declare each column's `"name"` as the Iceberg field name uppercased with FULL Unicode case mapping (Rust's `str::to_uppercase`), the same fold the table name receives, because Exasol resolves an unquoted identifier in user SQL by uppercasing it, so declaring the Iceberg casing verbatim would force every user query to double-quote every column name
* *AND* that fold SHALL be owned by exactly ONE site, the shared `CatalogClient` listing pipeline, which folds every declared name for BOTH catalog kinds and produces the `(name, Exasol type)` pairs the response's column list is built from, and no other code path SHALL declare a differently-cased name
* *AND* the full-Unicode fold's one-to-many expansions SHALL be recorded as a deliberate Exasol-target trade-off rather than left unstated: `ß` becomes `SS`, so an Iceberg column `straße` is queryable ONLY as the ASCII identifier `STRASSE` and the `ß`-bearing form resolves against no declared column, and two Iceberg columns in one table differing only in that expansion declare the same Exasol name with no collision check to reject it
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map and the skipped-table list (`SKIPPED_TABLES`, per `vs-adapter/create-virtual-schema-adapter-notes`) recorded in `adapterNotes`
* *AND* a `createVirtualSchema` request that resolves to a CATALOG kind and supplies no `NAMESPACE` property SHALL fail with the required-property error naming `NAMESPACE`, SUPERSEDING the unconditional form of this clause, because the `DIRECT_STORAGE` kind reaches its tables from the CONNECTION address alone and treats an absent `NAMESPACE` as the address itself, per `direct-storage/direct-storage-properties`

### Scenario: One non-Iceberg table in the namespace is skipped rather than aborting the schema

* *GIVEN* a `createVirtualSchema` request whose configured namespace contains a mix of Iceberg tables and at least one non-Iceberg table (e.g. an AWS Glue Hive external table) that the catalog's table listing returns but whose per-table `loadTable` request returns HTTP 404, the Iceberg REST OpenAPI `NoSuchTableException` response ("table to load does not exist"), which AWS Glue returns as `NoSuchIcebergTableException: Input table is not an iceberg table`
* *WHEN* the adapter enumerates the namespace and resolves each table's schema
* *THEN* the adapter SHALL exclude the non-Iceberg table from the response `schemaMetadata.tables` list and complete `createVirtualSchema` successfully with the remaining Iceberg tables
* *AND* the adapter SHALL exclude the non-Iceberg table's Exasol name from the `TABLE_MAP` recorded in `adapterNotes`, so no unqueryable virtual table is advertised
* *AND* the adapter SHALL write one warning line to script output via `udf_log!(ctx, warn, ...)` naming the skipped Iceberg identifier and the reason (HTTP 404 / not an Iceberg table), and that message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key

### Scenario: A namespace whose every table is non-Iceberg yields an empty virtual schema

* *GIVEN* a `createVirtualSchema` request whose configured namespace lists only non-Iceberg tables, every listed table's per-table `loadTable` returns HTTP 404
* *WHEN* the adapter enumerates the namespace and resolves each table's schema
* *THEN* the adapter SHALL complete `createVirtualSchema` successfully with an empty `schemaMetadata.tables` list and an empty `TABLE_MAP` in `adapterNotes`, rather than aborting
* *AND* the adapter SHALL write one `udf_log!(ctx, warn, ...)` warning line per skipped identifier, and none of those messages MUST contain storage access keys, secret keys, session tokens, or any SigV4 signing key

### Scenario: A non-404 per-table load failure aborts createVirtualSchema

* *GIVEN* a `createVirtualSchema` request whose configured namespace lists a table whose per-table `loadTable` request fails with something other than HTTP 404, a transport error, or an HTTP 401, 403, 419, 503, or any other non-404 status
* *WHEN* the adapter resolves that table's schema during enumeration
* *THEN* the adapter SHALL abort `createVirtualSchema` with an error describing that the table could not be loaded, preserving the "fails clearly when the catalog is unreachable" contract, and MUST NOT silently skip the table, a non-404 failure signals a catalog-wide fault (auth, throttling, outage), and skipping it would hide a misconfiguration behind a partial schema
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key

### Scenario: Create virtual schema fails clearly when the catalog is unreachable

* *GIVEN* the Iceberg REST catalog endpoint resolved from the CONNECTION cannot be reached
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key

### Scenario: Create virtual schema records the Exasol-name to Iceberg-identifier map in adapterNotes

* *GIVEN* a `createVirtualSchema` request that enumerates one or more tables in the configured namespace
* *WHEN* the adapter builds the `createVirtualSchema` response
* *THEN* the adapter SHALL record, inside the response's `schemaMetadata.adapterNotes` (a stringified JSON object), a `TABLE_MAP` entry mapping each uppercased `__`-flattened Exasol table name to its original-cased fully-qualified Iceberg identifier (dot-joined namespace segments plus table name)
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry (`PARALLELISM_FACTOR`, and the DataFusion threading and memory-budget entries) when writing `TABLE_MAP`
* *AND* the recorded map SHALL round-trip back to the adapter at pushdown time so a pushdown can recover the exact Iceberg identifier from the Exasol table name without re-listing the catalog
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `adapterNotes`

> Multi-level namespace flattening and its `__`-collision guard, the non-ASCII round trip, the
> single `ctx.database_version()` read that resolves the declared timestamp precision, and the
> `TIMESTAMP(p)` `fractionalSecondsPrecision` serialization live in
> `vs-adapter/create-virtual-schema-declaration-details`.
