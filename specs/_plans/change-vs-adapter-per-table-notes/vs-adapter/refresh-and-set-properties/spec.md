# Feature: Refresh And Set Properties

An Exasol user re-reads the source of an existing virtual schema in place with `ALTER VIRTUAL SCHEMA ... REFRESH` and `ALTER VIRTUAL SCHEMA ... SET`. The source is the catalog namespace, or under `CATALOG_KIND = 'DIRECT_STORAGE'` the storage base path. Tables and columns that the source added, dropped, renamed, or changed in type become queryable without a `DROP ... CASCADE` and `CREATE`.

<!-- DELTA:CHANGED -->
## Background

* The Exasol virtual-schema protocol sends a `refresh` request for `ALTER VIRTUAL SCHEMA ... REFRESH` and a `setProperties` request for `ALTER VIRTUAL SCHEMA ... SET`, and expects a response of the same `type`. These are the literal protocol strings.
* The adapter is stateless per `vs-adapter/create-virtual-schema`. It keeps only what it returns in the schema-level `adapterNotes`, in each virtual table's identifier note, and in each column's note (`vs-adapter/column-source-notes`). Exasol persists and returns all three. `refresh` and `setProperties` are not cache invalidation.
* A `setProperties` request and a `refresh` without `requestedTables` re-run the full listing of `createVirtualSchema`. A `refresh` that carries `requestedTables` is a partial refresh: the adapter lists only the source's table identifiers, then loads and returns only the requested tables.
* Schema resolution, type mapping, `adapterNotes` construction, and credential redaction reuse the `createVirtualSchema` path. The differences are the request `type`, the merge precedence of the incoming properties, the response `type`, the `requestedTables` echo, and the partial refresh.
* Exasol scopes a partial refresh to the requested tables (verified on Exasol 2025.1.16 and 8.29.13 with a probe adapter, #425). Exasol replaces each requested virtual table the response returns, creates a returned table that does not exist yet, drops a requested table the response omits, and keeps every unrequested virtual table with its notes. Exasol replaces the schema-level `adapterNotes` with the response's value. An unquoted `REFRESH TABLES t` arrives as `T`.
* A `refresh` request carries `type`, `requestedTables` when present, and `schemaMetadataInfo` with `name`, `adapterNotes`, and `properties`. It carries no virtual table and no table-level note, so the adapter maps a requested Exasol name to a catalog identifier only by listing the source.
* Re-enumeration re-reads each table's current metadata, so Iceberg schema evolution (added, dropped, renamed, or reordered fields and type promotion) shows up without diffing by the adapter.
* Credentials (access keys, secret keys, session tokens, SigV4 signing keys) never appear in a returned response or error message.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Refresh re-enumerates the namespace and returns a refresh response

* *GIVEN* a virtual schema created over an Iceberg namespace, or under `CATALOG_KIND = 'DIRECT_STORAGE'` over a storage base path, reachable through the CONNECTION named by `CATALOG_CONNECTION`
* *WHEN* Exasol sends a request of type `refresh` (the literal protocol string Exasol emits for `ALTER VIRTUAL SCHEMA ... REFRESH`) that carries no `requestedTables`
* *THEN* the adapter SHALL dispatch the request to the same full enumeration used by `createVirtualSchema` rather than rejecting it with an `unsupported VS request type` error
* *AND* the adapter SHALL re-list every table that enumeration finds, which is every table in the Iceberg namespace and its descendants, or every first-level directory of the direct-storage base path per `direct-storage/direct-storage-table-discovery`
* *AND* the adapter SHALL re-resolve each table's current schema, which is the current Iceberg schema, or the Parquet footers folded per `direct-storage/parquet-directory-seam`, and return a JSON response of type `refresh` whose `schemaMetadata.tables` describes one virtual table per discovered table with Exasol-mapped types per `scan-types/type-mapping`
* *AND* the adapter MUST NOT persist any catalog metadata between requests other than the `SKIPPED_TABLES` entry recorded in `adapterNotes`, the identifier note recorded per virtual table per `vs-adapter/create-virtual-schema`, and the column notes recorded per `vs-adapter/column-source-notes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Refresh reflects table and column structure changes

* *GIVEN* a virtual schema created over an Iceberg namespace or a direct-storage base path, and since creation the source has gained a new table, lost an existing table, and within a surviving table gained a column and promoted a column's type to a wider one, and for an Iceberg namespace a surviving table has also dropped a column and renamed a column
* *AND* for a direct-storage base path under a virtual schema that leaves `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent, so both resolve to TRUE per `direct-storage/direct-storage-properties`, the new table is a new first-level directory holding a data file, the lost table is a directory that holds no data file any more, the promoted column is stored wider by a new file per a widening pair `scan-types/type-relaxation` proves castable, and a surviving table has also gained a file under a `key=value` directory segment that no earlier file of it carried
* *WHEN* Exasol sends a `refresh` request that carries no `requestedTables`
* *THEN* the returned `schemaMetadata.tables` SHALL include the newly added table and SHALL omit the lost table
* *AND* the surviving table's columns SHALL reflect its current schema: the added column present, the promoted type mapped to its current Exasol type per `scan-types/type-mapping`, and for an Iceberg table the dropped column absent and the renamed column under its new name
* *AND* for a direct-storage table, the added column SHALL read NULL for the rows of every file that lacks it, every row SHALL read back at the promoted type, and the new partition column SHALL be declared `VARCHAR(2000000)` per `direct-storage/direct-storage-hive-partitioning`, reading NULL for every file whose path lacks the segment
* *AND* for a direct-storage base path, a lost directory that still holds an object SHALL be recorded in `SKIPPED_TABLES` with the reason `holds no data file`, per `vs-adapter/create-virtual-schema-adapter-notes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Refresh rebuilds every identifier note and preserves other adapter notes

* *GIVEN* a `refresh` request without `requestedTables` whose schema-level `adapterNotes` holds the tuning values and one entry the adapter does not write, for a virtual schema whose tables carry identifier notes, or none because the schema predates them
* *WHEN* the adapter builds the `refresh` response
* *THEN* each returned virtual table's identifier note SHALL hold the catalog identifier this refresh resolved from the source, never a copy of a previous note
* *AND* a virtual table that had no identifier note SHALL carry one afterwards, so its pushdown no longer fails
* *AND* the adapter SHALL preserve every other schema-level `adapterNotes` entry, including the entry it does not write
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A partial refresh returns only the requested tables and keeps the others

* *GIVEN* a virtual schema over an Iceberg namespace with the tables `table_one` and `table_two`, each of which then gains the column `new_col`
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... REFRESH TABLES TABLE_ONE`, which Exasol sends with `requestedTables` `["TABLE_ONE"]`
* *THEN* the response SHALL return only `TABLE_ONE`, with column notes and an identifier note built from the source, and SHALL echo `requestedTables`, which the response to a `refresh` without `requestedTables` omits
* *AND* `TABLE_ONE` SHALL declare `NEW_COL`, and `TABLE_TWO` SHALL keep its columns, column notes, and identifier note, or its lack of one, and stay queryable
* *AND* the schema-level `adapterNotes` SHALL carry the tuning values derived from the request's properties and the `SKIPPED_TABLES` of the scenario "A partial refresh merges SKIPPED_TABLES with the persisted list"
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A partial refresh drops a requested virtual table the source no longer serves

* *GIVEN* a virtual schema over an Iceberg namespace with the tables `keep_tbl` and `gone_tbl`, after which `gone_tbl` is dropped from the catalog
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... REFRESH TABLES GONE_TBL`
* *THEN* the response SHALL omit `GONE_TBL`, so Exasol drops that virtual table, and `KEEP_TBL` SHALL stay declared and queryable
* *AND* a requested table that the source still lists but the load skips SHALL also be omitted, and its skip entry recorded per the scenario "A partial refresh merges SKIPPED_TABLES with the persisted list"
* *AND* a requested name that matches no listed identifier SHALL change no virtual table and SHALL NOT fail the statement
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A partial refresh resolves each requested name as a full refresh would

* *GIVEN* a direct-storage base path whose directories `Sales` and `other` hold Parquet files and whose directory `sales` holds only a `_SUCCESS` object, so `SALES` is declared from `Sales` and `sales` is skipped
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... REFRESH TABLES SALES`
* *THEN* the adapter SHALL load every listed identifier that flattens to a requested name, here `Sales` and `sales`, and run the name-collision check of `vs-adapter/create-virtual-schema-declaration-details` over the loaded tables only, so the statement succeeds and `SKIPPED_TABLES` keeps `sales`
* *AND* after `sales` gains a Parquet file, `REFRESH TABLES OTHER` SHALL succeed, because the adapter neither loads nor checks an unrequested identifier, and `REFRESH TABLES SALES` and a full `REFRESH` SHALL fail with the collision error
* *AND* after every object below `Sales` is deleted, `REFRESH TABLES SALES` SHALL succeed with an identifier note naming `sales`, because the adapter MUST NOT read the previous identifier note
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A partial refresh merges SKIPPED_TABLES with the persisted list

* *GIVEN* a persisted `SKIPPED_TABLES` with entries for `now_loads`, `still_skipped`, `unrequested`, `dropped_a`, and `dropped_b`, after which `now_loads` loads, `still_skipped` and `unrequested` are still skipped, the source no longer lists `dropped_a` or `dropped_b`, and the source now skips `newly_broken`, whose virtual table `NEWLY_BROKEN` exists
* *WHEN* Exasol sends a `refresh` whose `requestedTables` is `["NOW_LOADS", "STILL_SKIPPED", "DROPPED_A"]`
* *THEN* the response SHALL declare `NOW_LOADS`, and `SKIPPED_TABLES` SHALL hold, in listing order, only the fresh entry for `still_skipped` and the persisted entry for `unrequested`
* *AND* a persisted entry SHALL survive only when the identifier listing still returns its catalog identifier and its flattened Exasol name is not requested, and an unrequested table gains no entry, so `NEWLY_BROKEN` stays declared
* *AND* the adapter SHALL cap the merged list per `vs-adapter/create-virtual-schema-adapter-notes`, and `SKIPPED_TABLES_OMITTED` SHALL hold the persisted count plus the entries the cap drops, or be absent when that sum is 0
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A partial refresh loads the metadata of the requested tables only

* *GIVEN* a direct-storage virtual schema over the directories `t_append` and `t_conflict`, after which `t_append` gains a file with the new column `extra` and `t_conflict` gains a file whose column pair fails a refresh per `direct-storage/direct-storage-table-discovery`
* *WHEN* the user runs `ALTER VIRTUAL SCHEMA ... REFRESH`, then `ALTER VIRTUAL SCHEMA ... REFRESH TABLES T_APPEND`
* *THEN* the full `REFRESH` SHALL fail naming the column pair of `t_conflict`
* *AND* `REFRESH TABLES T_APPEND` SHALL succeed, `T_APPEND` SHALL declare `EXTRA`, and `T_CONFLICT` SHALL keep its declaration
* *AND* under every catalog kind the adapter MUST NOT load the metadata of a listed table whose flattened name is not requested, so an unrequested table that fails to load cannot fail the statement
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: Refresh rebuilds the table map and preserves other adapter notes

* *GIVEN* a `refresh` request whose `schemaMetadataInfo.adapterNotes` carries the persisted notes from creation (`PARALLELISM_FACTOR`, the DataFusion threading and memory-budget entries, and `TABLE_MAP`)
* *WHEN* the adapter builds the `refresh` response
* *THEN* the adapter SHALL rebuild `TABLE_MAP` from the re-enumerated tables, a full rebuild, never a diff or patch of the prior map
* *AND* the adapter SHALL preserve every other pre-existing `adapterNotes` entry when writing the rebuilt `TABLE_MAP`, including an entry the adapter does not itself write, which survives the rebuild unread and inert
* *AND* the adapter MUST NOT persist the map anywhere other than the returned `schemaMetadata.adapterNotes`
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Refresh echoes requestedTables when present

* *GIVEN* a virtual schema created over an Iceberg namespace reachable through its CONNECTION
* *WHEN* Exasol sends a `refresh` request that carries a `requestedTables` array (a partial `ALTER VIRTUAL SCHEMA ... REFRESH TABLES ...`)
* *THEN* the adapter SHALL echo the same `requestedTables` array in the response because the protocol requires a well-formed response of type `refresh` to mirror the fields of the request it answers
* *AND* the adapter MUST NOT be relied upon to scope the resulting refresh to the echoed `requestedTables`, verified against the live engine, Exasol applies the adapter's full `schemaMetadata.tables` response to the whole namespace regardless of `requestedTables`, so a partial `REFRESH TABLES <t>` has the same real-world effect as a full `REFRESH`
* *AND* when the request carries no `requestedTables`, the response SHALL omit `requestedTables` so Exasol applies a full refresh
<!-- /DELTA:REMOVED -->
