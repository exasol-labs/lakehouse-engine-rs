# Feature: Refresh And Set Properties

An Exasol user re-reads the source of an existing virtual schema in place with `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET`. The source is the catalog namespace, or under `CATALOG_KIND = 'DIRECT_STORAGE'` the storage base path. Tables and columns that the source added, dropped, renamed, or changed in type become queryable without a `DROP ... CASCADE` and `CREATE`.

<!-- DELTA:CHANGED -->
## Background

* The Exasol virtual-schema protocol sends a `refresh` request for `ALTER VIRTUAL SCHEMA ... REFRESH` and a `setProperties` request for `ALTER VIRTUAL SCHEMA ... SET`, and expects a response of the same `type`. These are the literal protocol strings. A `refresh` request carries `type`, `requestedTables` when present, and `schemaMetadataInfo` with `name`, `adapterNotes`, and `properties`, but no virtual table and no table-level note, so the adapter maps a requested Exasol name to a catalog identifier only by listing the source.
* The adapter is stateless per `vs-adapter/create-virtual-schema`. It keeps only what it returns in the schema-level `adapterNotes`, in each virtual table's identifier note, and in each column's note (`vs-adapter/column-source-notes`). Exasol persists and returns all three. `refresh` and `setProperties` are not cache invalidation.
* A `setProperties` request and a `refresh` without `requestedTables` re-run the full listing of `createVirtualSchema`, which re-reads each table's current metadata, so Iceberg schema evolution (added, dropped, renamed, or reordered fields and type promotion) shows up without diffing by the adapter. A `refresh` that carries `requestedTables` is a partial refresh: the adapter lists only the source's table identifiers, then loads and returns only the requested tables.
* Schema resolution, type mapping, `adapterNotes` construction, and credential redaction reuse the `createVirtualSchema` path. The differences are the request `type`, the merge precedence of the incoming properties, the property checks that `refresh` skips per `vs-properties/property-value-validation`, the response `type`, the `requestedTables` echo, and the partial refresh.
* Exasol scopes a partial refresh to the requested tables (verified on Exasol 2025.1.16 and 8.29.13 with a probe adapter, #425). Exasol replaces each requested virtual table the response returns, creates a returned table that does not exist yet, drops a requested table the response omits, and keeps every unrequested virtual table with its notes. Exasol replaces the schema-level `adapterNotes` with the response's value. An unquoted `REFRESH TABLES t` arrives as `T`.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Refresh and set properties redact credentials on catalog failure

* *GIVEN* the Iceberg REST catalog endpoint resolved from the CONNECTION cannot be reached
* *WHEN* Exasol sends a `refresh` request or a `setProperties` request
* *THEN* the adapter SHALL return an error describing that the catalog could not be reached or the namespace could not be listed
* *AND* the error message MUST NOT contain storage access keys, secret keys, session tokens, or any SigV4 signing key
