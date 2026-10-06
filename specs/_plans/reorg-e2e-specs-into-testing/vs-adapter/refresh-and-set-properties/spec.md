# Feature: Refresh And Set Properties

Lets an Exasol user re-read the Iceberg catalog for an existing virtual schema in place — through `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET` — so a namespace's added, dropped, renamed, or type-changed tables and columns become queryable without a `DROP ... CASCADE` + `CREATE`, which loses dependent views and grants.

<!-- DELTA:CHANGED -->
## Background

The Exasol virtual-schema JSON protocol sends a `refresh` request for `ALTER VIRTUAL SCHEMA ... REFRESH` and a `setProperties` request for `ALTER VIRTUAL SCHEMA ... SET`, and expects a response of the same `type`. These are the literal protocol strings; they are NOT `refreshVirtualSchema` or `refreshProperties`.

* The adapter is stateless per `vs-adapter/create-virtual-schema` — it holds no catalog metadata between requests other than what it returns in `schemaMetadata.adapterNotes`, which Exasol persists and round-trips back. `refresh` and `setProperties` are therefore not cache invalidation; each re-runs the same full namespace enumeration as `createVirtualSchema` and re-emits an updated `schemaMetadata`.
* Enumeration, schema resolution, type mapping, `adapterNotes` construction (including `TABLE_MAP`), and credential redaction reuse the `createVirtualSchema` path verbatim; the only differences are the request `type` recognised, the merge precedence of the incoming properties, the response `type` label, and the `requestedTables` echo.
* Iceberg schema evolution is picked up automatically because a re-enumeration re-reads each table's current metadata. Per the Apache Iceberg table spec, `current-schema-id` is the "ID of the table's current schema" and "points to the schema by ID for use when reading table data"; the allowed evolutions are "Adding, deleting, renaming, or reordering fields in structs" and "Type promotion". Columns are "selected by field id", so a re-read reflects an added, dropped, or renamed column and a promoted type without any diffing by the adapter. This feature adds no new schema-handling surface beyond `createVirtualSchema`; the known field-id projection exception (`scan-read-path/scan-execution-field-id-projection`, #27) is unchanged and out of scope here.
* Credentials (access keys, secret keys, session tokens, SigV4 signing keys) MUST NOT appear in any returned response or error message.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Refresh re-enumerates the namespace and returns a refresh response

* *GIVEN* a virtual schema created over an Iceberg namespace reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *WHEN* Exasol sends a request of type `refresh` (the literal protocol string Exasol emits for `ALTER VIRTUAL SCHEMA ... REFRESH`)
* *THEN* the adapter SHALL dispatch the request to the same full namespace enumeration used by `createVirtualSchema` rather than rejecting it with an `unsupported VS request type` error
* *AND* the adapter SHALL re-list every table in the namespace and its descendants, re-resolve each table's current Iceberg schema, and return a JSON response of type `refresh` whose `schemaMetadata.tables` describes one virtual table per discovered table with Exasol-mapped types per `scan-types/type-mapping`
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Refresh reflects table and column structure changes

* *GIVEN* a virtual schema created over a namespace, and since creation the catalog has gained a new table, dropped an existing table, and within a surviving table added a column, dropped a column, renamed a column, and promoted a column's type
* *WHEN* Exasol sends a `refresh` request
* *THEN* the returned `schemaMetadata.tables` SHALL include the newly added table and SHALL omit the dropped table
* *AND* the surviving table's columns SHALL reflect its current Iceberg schema — the added column present, the dropped column absent, the renamed column under its new name, and the promoted type mapped to its current Exasol type per `scan-types/type-mapping`
<!-- /DELTA:CHANGED -->
