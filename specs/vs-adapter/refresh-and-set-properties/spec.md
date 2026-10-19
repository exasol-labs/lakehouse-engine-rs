# Feature: Refresh And Set Properties

An Exasol user re-reads the source of an existing virtual schema in place with `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET`. The source is the catalog namespace, or under `CATALOG_KIND = 'DIRECT_STORAGE'` the storage base path. Tables and columns that the source added, dropped, renamed, or changed in type become queryable without a `DROP ... CASCADE` and `CREATE`.

## Background

* The Exasol virtual-schema protocol sends a `refresh` request for `ALTER VIRTUAL SCHEMA ... REFRESH` and a `setProperties` request for `ALTER VIRTUAL SCHEMA ... SET`, and expects a response of the same `type`. These are the literal protocol strings.
* The adapter is stateless per `vs-adapter/create-virtual-schema`. It keeps only what it returns in `schemaMetadata.adapterNotes` and in each column's `adapterNotes` (`vs-adapter/column-source-notes`), which Exasol persists and returns. `refresh` and `setProperties` are not cache invalidation. Each re-runs the full namespace enumeration of `createVirtualSchema` and re-emits `schemaMetadata`.
* Enumeration, schema resolution, type mapping, `adapterNotes` construction (including `TABLE_MAP` and every column note), and credential redaction reuse the `createVirtualSchema` path. The differences are the request `type`, the merge precedence of the incoming properties, the response `type`, and the `requestedTables` echo.
* Re-enumeration re-reads each table's current metadata, so Iceberg schema evolution (added, dropped, renamed, or reordered fields and type promotion) shows up without diffing by the adapter.
* Credentials (access keys, secret keys, session tokens, SigV4 signing keys) never appear in a returned response or error message.

## Scenarios

### Scenario: Refresh re-enumerates the namespace and returns a refresh response

* *GIVEN* a virtual schema created over an Iceberg namespace, or under `CATALOG_KIND = 'DIRECT_STORAGE'` over a storage base path, reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *WHEN* Exasol sends a request of type `refresh` (the literal protocol string Exasol emits for `ALTER VIRTUAL SCHEMA ... REFRESH`)
* *THEN* the adapter SHALL dispatch the request to the same full enumeration used by `createVirtualSchema` rather than rejecting it with an `unsupported VS request type` error
* *AND* the adapter SHALL re-list every table that enumeration finds, which is every table in the Iceberg namespace and its descendants, or every first-level directory of the direct-storage base path per `direct-storage/direct-storage-table-discovery`
* *AND* the adapter SHALL re-resolve each table's current schema, which is the current Iceberg schema, or the Parquet footers folded per `direct-storage/parquet-directory-seam`, and return a JSON response of type `refresh` whose `schemaMetadata.tables` describes one virtual table per discovered table with Exasol-mapped types per `scan-types/type-mapping`
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the `TABLE_MAP` and `SKIPPED_TABLES` entries recorded in `adapterNotes` and the column notes recorded per `vs-adapter/column-source-notes`

### Scenario: Refresh reflects table and column structure changes

* *GIVEN* a virtual schema created over an Iceberg namespace or a direct-storage base path, and since creation the source has gained a new table, lost an existing table, and within a surviving table gained a column and promoted a column's type to a wider one, and for an Iceberg namespace a surviving table has also dropped a column and renamed a column
* *AND* for a direct-storage base path under a virtual schema that leaves `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent, so both resolve to TRUE per `direct-storage/direct-storage-properties`, the new table is a new first-level directory holding a data file, the lost table is a directory that holds no data file any more, the promoted column is stored wider by a new file per a widening pair `scan-types/type-relaxation` proves castable, and a surviving table has also gained a file under a `key=value` directory segment that no earlier file of it carried
* *WHEN* Exasol sends a `refresh` request
* *THEN* the returned `schemaMetadata.tables` SHALL include the newly added table and SHALL omit the lost table
* *AND* the surviving table's columns SHALL reflect its current schema: the added column present, the promoted type mapped to its current Exasol type per `scan-types/type-mapping`, and for an Iceberg table the dropped column absent and the renamed column under its new name
* *AND* for a direct-storage table, the added column SHALL read NULL for the rows of every file that lacks it, every row SHALL read back at the promoted type, and the new partition column SHALL be declared `VARCHAR(2000000)` per `direct-storage/direct-storage-hive-partitioning`, reading NULL for every file whose path lacks the segment
* *AND* for a direct-storage base path, a lost directory that still holds an object SHALL be recorded in `SKIPPED_TABLES` with the reason `holds no data file`, per `vs-adapter/create-virtual-schema-adapter-notes`

### Scenario: Refresh rebuilds the table map and preserves other adapter notes

* *GIVEN* a `refresh` request whose `schemaMetadataInfo.adapterNotes` carries the persisted notes from creation (`PARALLELISM_FACTOR`, the DataFusion threading and memory-budget entries, and `TABLE_MAP`)
* *WHEN* the adapter builds the `refresh` response
* *THEN* the adapter SHALL rebuild `TABLE_MAP` from the re-enumerated tables, a full rebuild, never a diff or patch of the prior map
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry when writing the rebuilt `TABLE_MAP`, including an entry the adapter does not itself write, which survives the rebuild unread and inert
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `schemaMetadata.adapterNotes`

### Scenario: Refresh echoes requestedTables when present

* *GIVEN* a virtual schema created over an Iceberg namespace reachable through its CONNECTION
* *WHEN* Exasol sends a `refresh` request that carries a `requestedTables` array (a partial `ALTER VIRTUAL SCHEMA ... REFRESH TABLES ...`)
* *THEN* the adapter SHALL echo the same `requestedTables` array in the response because the protocol requires a well-formed response of type `refresh` to mirror the fields of the request it answers
* *AND* the adapter MUST NOT be relied upon to scope the resulting refresh to the echoed `requestedTables`, verified against the live engine, Exasol applies the adapter's full `schemaMetadata.tables` response to the whole namespace regardless of `requestedTables`, so a partial `REFRESH TABLES <t>` has the same real-world effect as a full `REFRESH`
* *AND* when the request carries no `requestedTables`, the response SHALL omit `requestedTables` so Exasol applies a full refresh

### Scenario: Set properties overrides persisted properties and re-enumerates

* *GIVEN* a virtual schema created with property `NAMESPACE` set to one namespace, whose persisted properties arrive in `schemaMetadataInfo.properties`
* *WHEN* Exasol sends a request of type `setProperties` (the literal protocol string for `ALTER VIRTUAL SCHEMA ... SET`) whose `properties` object sets `NAMESPACE` to a different namespace
* *THEN* the adapter SHALL treat the request's `properties` as overriding the persisted `schemaMetadataInfo.properties` on conflict, the newly set value wins, and a `null` value in the request's `properties` SHALL unset that property
* *AND* the adapter SHALL re-enumerate using the effective merged properties and return a JSON response of type `setProperties` whose `schemaMetadata` describes the tables of the newly targeted namespace
* *AND* a `setProperties` request that leaves a required property (`NAMESPACE` or `CATALOG_CONNECTION`) unset SHALL return a clear error naming the missing property

### Scenario: Refresh and set properties redact credentials on catalog failure

* *GIVEN* the Iceberg REST catalog endpoint resolved from the CONNECTION cannot be reached
* *WHEN* Exasol sends a `refresh` request or a `setProperties` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key
