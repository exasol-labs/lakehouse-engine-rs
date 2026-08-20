# Feature: Refresh And Set Properties

Lets an Exasol user re-read the Iceberg catalog for an existing virtual schema in place — through `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET` — so a namespace's added, dropped, renamed, or type-changed tables and columns become queryable without a `DROP ... CASCADE` + `CREATE`, which loses dependent views and grants.

## Background

The Exasol virtual-schema JSON protocol sends a `refresh` request for `ALTER VIRTUAL SCHEMA ... REFRESH` and a `setProperties` request for `ALTER VIRTUAL SCHEMA ... SET`, and expects a response of the same `type`. These are the literal protocol strings; they are NOT `refreshVirtualSchema` or `refreshProperties`.

* The adapter is stateless per `vs-adapter/create-virtual-schema` — it holds no catalog metadata between requests other than what it returns in `schemaMetadata.adapterNotes`, which Exasol persists and round-trips back. `refresh` and `setProperties` are therefore not cache invalidation; each re-runs the same full namespace enumeration as `createVirtualSchema` and re-emits an updated `schemaMetadata`.
* Enumeration, schema resolution, type mapping, `adapterNotes` construction (including `TABLE_MAP`), and credential redaction reuse the `createVirtualSchema` path verbatim; the only differences are the request `type` recognised, the merge precedence of the incoming properties, the response `type` label, and the `requestedTables` echo.
* Iceberg schema evolution is picked up automatically because a re-enumeration re-reads each table's current metadata. Per the Apache Iceberg table spec, `current-schema-id` is the "ID of the table's current schema" and "points to the schema by ID for use when reading table data"; the allowed evolutions are "Adding, deleting, renaming, or reordering fields in structs" and "Type promotion". Columns are "selected by field id", so a re-read reflects an added, dropped, or renamed column and a promoted type without any diffing by the adapter. This feature adds no new schema-handling surface beyond `createVirtualSchema`; the known field-id projection exception (`datafusion-scan/scan-execution-field-id-projection`, #27) is unchanged and out of scope here.
* Credentials (access keys, secret keys, session tokens, SigV4 signing keys) MUST NOT appear in any returned response or error message.

## Scenarios

### Scenario: Refresh re-enumerates the namespace and returns a refresh response

* *GIVEN* a virtual schema created over an Iceberg namespace reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *WHEN* Exasol sends a request of type `refresh` (the literal protocol string Exasol emits for `ALTER VIRTUAL SCHEMA ... REFRESH`)
* *THEN* the adapter SHALL dispatch the request to the same full namespace enumeration used by `createVirtualSchema` rather than rejecting it with an `unsupported VS request type` error
* *AND* the adapter SHALL re-list every table in the namespace and its descendants, re-resolve each table's current Iceberg schema, and return a JSON response of type `refresh` whose `schemaMetadata.tables` describes one virtual table per discovered table with Exasol-mapped types per `datafusion-scan/type-mapping`
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the table-name map recorded in `adapterNotes`

### Scenario: Refresh reflects table and column structure changes

* *GIVEN* a virtual schema created over a namespace, and since creation the catalog has gained a new table, dropped an existing table, and within a surviving table added a column, dropped a column, renamed a column, and promoted a column's type
* *WHEN* Exasol sends a `refresh` request
* *THEN* the returned `schemaMetadata.tables` SHALL include the newly added table and SHALL omit the dropped table
* *AND* the surviving table's columns SHALL reflect its current Iceberg schema — the added column present, the dropped column absent, the renamed column under its new name, and the promoted type mapped to its current Exasol type per `datafusion-scan/type-mapping`

### Scenario: Refresh rebuilds the table map and preserves other adapter notes

* *GIVEN* a `refresh` request whose `schemaMetadataInfo.adapterNotes` carries the persisted notes from creation (`NR_OF_CORES`, `PARALLELISM_FACTOR`, the DataFusion threading and memory-budget entries, and `TABLE_MAP`)
* *WHEN* the adapter builds the `refresh` response
* *THEN* the adapter SHALL rebuild `TABLE_MAP` from the re-enumerated tables — a full rebuild, never a diff or patch of the prior map
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry when writing the rebuilt `TABLE_MAP`
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `schemaMetadata.adapterNotes`

### Scenario: Refresh echoes requestedTables when present

* *GIVEN* a virtual schema created over an Iceberg namespace reachable through its CONNECTION
* *WHEN* Exasol sends a `refresh` request that carries a `requestedTables` array (a partial `ALTER VIRTUAL SCHEMA ... REFRESH TABLES ...`)
* *THEN* the adapter SHALL echo the same `requestedTables` array in the response because the protocol requires a well-formed response of type `refresh` to mirror the fields of the request it answers
* *AND* the adapter MUST NOT be relied upon to scope the resulting refresh to the echoed `requestedTables` — verified against the live engine, Exasol applies the adapter's full `schemaMetadata.tables` response to the whole namespace regardless of `requestedTables`, so a partial `REFRESH TABLES <t>` has the same real-world effect as a full `REFRESH`
* *AND* when the request carries no `requestedTables`, the response SHALL omit `requestedTables` so Exasol applies a full refresh

### Scenario: Set properties overrides persisted properties and re-enumerates

* *GIVEN* a virtual schema created with property `NAMESPACE` set to one namespace, whose persisted properties arrive in `schemaMetadataInfo.properties`
* *WHEN* Exasol sends a request of type `setProperties` (the literal protocol string for `ALTER VIRTUAL SCHEMA ... SET`) whose `properties` object sets `NAMESPACE` to a different namespace
* *THEN* the adapter SHALL treat the request's `properties` as overriding the persisted `schemaMetadataInfo.properties` on conflict — the newly set value wins — and a `null` value in the request's `properties` SHALL unset that property
* *AND* the adapter SHALL re-enumerate using the effective merged properties and return a JSON response of type `setProperties` whose `schemaMetadata` describes the tables of the newly targeted namespace
* *AND* a `setProperties` request that leaves a required property (`NAMESPACE` or `CATALOG_CONNECTION`) unset SHALL return a clear error naming the missing property

### Scenario: Refresh and set properties redact credentials on catalog failure

* *GIVEN* the Iceberg REST catalog endpoint resolved from the CONNECTION cannot be reached
* *WHEN* Exasol sends a `refresh` request or a `setProperties` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key
