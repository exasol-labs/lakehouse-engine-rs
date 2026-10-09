<!-- DELTA:CHANGED -->
# Feature: Refresh And Set Properties

Lets an Exasol user re-read the source of an existing virtual schema in place, through `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET`. The source is the catalog namespace, or under `CATALOG_KIND = 'DIRECT_STORAGE'` the storage base path. Tables and columns the source added, dropped, renamed, or changed in type then become queryable without a `DROP ... CASCADE` + `CREATE`, which loses dependent views and grants.
<!-- /DELTA:CHANGED -->

## Background

The Exasol virtual-schema JSON protocol sends a `refresh` request for `ALTER VIRTUAL SCHEMA ... REFRESH` and a `setProperties` request for `ALTER VIRTUAL SCHEMA ... SET`, and expects a response of the same `type`. These are the literal protocol strings; they are NOT `refreshVirtualSchema` or `refreshProperties`.

* The adapter is stateless per `vs-adapter/create-virtual-schema` — it holds no catalog metadata between requests other than what it returns in `schemaMetadata.adapterNotes`, which Exasol persists and round-trips back. `refresh` and `setProperties` are therefore not cache invalidation; each re-runs the same full namespace enumeration as `createVirtualSchema` and re-emits an updated `schemaMetadata`.
* Enumeration, schema resolution, type mapping, `adapterNotes` construction (including `TABLE_MAP`), and credential redaction reuse the `createVirtualSchema` path verbatim; the only differences are the request `type` recognised, the merge precedence of the incoming properties, the response `type` label, and the `requestedTables` echo.
* Iceberg schema evolution is picked up automatically because a re-enumeration re-reads each table's current metadata. Per the Apache Iceberg table spec, `current-schema-id` is the "ID of the table's current schema" and "points to the schema by ID for use when reading table data"; the allowed evolutions are "Adding, deleting, renaming, or reordering fields in structs" and "Type promotion". Columns are "selected by field id", so a re-read reflects an added, dropped, or renamed column and a promoted type without any diffing by the adapter. This feature adds no new schema-handling surface beyond `createVirtualSchema`; the known field-id projection exception (`scan-read-path/scan-execution-field-id-projection`, #27) is unchanged and out of scope here.
* Credentials (access keys, secret keys, session tokens, SigV4 signing keys) MUST NOT appear in any returned response or error message.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Refresh re-enumerates the namespace and returns a refresh response

* *GIVEN* a virtual schema created over an Iceberg namespace, or under `CATALOG_KIND = 'DIRECT_STORAGE'` over a storage base path, reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *WHEN* Exasol sends a request of type `refresh` (the literal protocol string Exasol emits for `ALTER VIRTUAL SCHEMA ... REFRESH`)
* *THEN* the adapter SHALL dispatch the request to the same full enumeration used by `createVirtualSchema` rather than rejecting it with an `unsupported VS request type` error
* *AND* the adapter SHALL re-list every table that enumeration finds, which is every table in the Iceberg namespace and its descendants, or every first-level directory of the direct-storage base path per `direct-storage/direct-storage-table-discovery`
* *AND* the adapter SHALL re-resolve each table's current schema, which is the current Iceberg schema, or the Parquet footers folded per `direct-storage/parquet-directory-seam`, and return a JSON response of type `refresh` whose `schemaMetadata.tables` describes one virtual table per discovered table with Exasol-mapped types per `scan-types/type-mapping`
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the `TABLE_MAP` and `SKIPPED_TABLES` entries recorded in `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Refresh reflects table and column structure changes

* *GIVEN* a virtual schema created over an Iceberg namespace or a direct-storage base path, and since creation the source has gained a new table, lost an existing table, and within a surviving table gained a column and promoted a column's type to a wider one, and for an Iceberg namespace a surviving table has also dropped a column and renamed a column
* *AND* for a direct-storage base path under a virtual schema that leaves `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent, so both resolve to TRUE per `direct-storage/direct-storage-properties`, the new table is a new first-level directory holding a data file, the lost table is a directory that holds no data file any more, the promoted column is stored wider by a new file per a widening pair `scan-types/type-relaxation` proves castable, and a surviving table has also gained a file under a `key=value` directory segment that no earlier file of it carried
* *WHEN* Exasol sends a `refresh` request
* *THEN* the returned `schemaMetadata.tables` SHALL include the newly added table and SHALL omit the lost table
* *AND* the surviving table's columns SHALL reflect its current schema: the added column present, the promoted type mapped to its current Exasol type per `scan-types/type-mapping`, and for an Iceberg table the dropped column absent and the renamed column under its new name
* *AND* for a direct-storage table, the added column SHALL read NULL for the rows of every file that lacks it, every row SHALL read back at the promoted type, and the new partition column SHALL be declared `VARCHAR(2000000)` per `direct-storage/direct-storage-hive-partitioning`, reading NULL for every file whose path lacks the segment
* *AND* for a direct-storage base path, a lost directory that still holds an object SHALL be recorded in `SKIPPED_TABLES` with the reason `holds no data file`, per `vs-adapter/create-virtual-schema-adapter-notes`
<!-- /DELTA:CHANGED -->
