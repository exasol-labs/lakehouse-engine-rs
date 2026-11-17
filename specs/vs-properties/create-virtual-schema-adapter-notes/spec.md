# Feature: Create Virtual Schema, AdapterNotes

The `createVirtualSchema` response's schema-level `adapterNotes` records the skipped-table list and the Iceberg REST catalog prefix. Later pushdowns read the prefix to skip the catalog's `/v1/config` lookup. Each virtual table's catalog identifier lives in that table's own identifier note (`vs-adapter/create-virtual-schema`), not here.

## Background

* The schema-level `adapterNotes` holds only values a pushdown cannot recompute without a catalog request, plus `SKIPPED_TABLES`. Every pushdown resolves its tuning values from the virtual schema properties (`vs-properties/tuning-resolution`) and reads the cluster node count from its UDF handshake (`pushdown/pushdown-planning`).
* `SKIPPED_TABLES` is a diagnostic entry that no pushdown reads. It records the tables the listing skipped, so a user reads each skip reason from `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Only a partial refresh reads it back, to merge it (`vs-adapter/refresh-and-set-properties`).
* Exasol rejects an `adapterNotes` value longer than 2,000,000 characters with sqlCode `04000` on CREATE and REFRESH, and keeps the previous value.
* `adapterNotes` is stringified JSON, which Exasol persists and returns at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties.

## Scenarios

### Scenario: Schema-level adapterNotes hold only the skipped tables and the catalog prefix

* *GIVEN* an Exasol session that has installed the VS adapter script, with the catalog and storage connection properties supplied
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL write exactly `SKIPPED_TABLES` into the returned schema-level `adapterNotes`, plus `SKIPPED_TABLES_OMITTED` when it caps the skip list and `CATALOG_PREFIX` under the Iceberg REST catalog kind
* *AND* a `refresh` or `setProperties` response SHALL carry the same entries and keep every other entry of the persisted `adapterNotes`, per `vs-adapter/refresh-and-set-properties`
* *AND* the adapter MUST NOT write a tuning value, the cluster node count, or the per-node core count, because every pushdown derives them itself

### Scenario: The Iceberg REST catalog prefix is recorded at schema level

* *GIVEN* a virtual schema with an absent `CATALOG_KIND`, so its catalog is Iceberg REST
* *WHEN* createVirtualSchema, refresh, or setProperties completes
* *THEN* adapterNotes SHALL carry `CATALOG_PREFIX`, the REST path prefix the listing's catalog session used: the catalog's `/v1/config` prefix, an empty string when the catalog serves none, and `catalogs/{warehouse}` under `use_sigv4`
* *AND* a response under any other catalog kind SHALL carry no `CATALOG_PREFIX`, so a `SET CATALOG_KIND` away from Iceberg REST removes a recorded one

### Scenario: Every skipped table is recorded with its reason under every catalog kind

* *GIVEN* one listing per catalog kind that skips entries: Iceberg REST (a table whose `loadTable` returns 404, and one that returns 400), Unity Catalog (a view), direct storage (a directory holding no data file), and Glue (an ORC table and a partition-projection table)
* *WHEN* createVirtualSchema, setProperties, or a refresh that carries no `requestedTables` completes
* *THEN* adapterNotes SHALL carry a `SKIPPED_TABLES` entry holding a JSON array with one object per skipped entry, in listing order, whose `table` is the catalog identifier and whose `reason` states why the entry was skipped
* *AND* the `reason` SHALL be `catalog reported it is not a loadable Iceberg table` for an Iceberg REST 404, the load error for an Iceberg REST 400, and `holds no data file` for direct storage, and SHALL name the catalog value that decided the skip, such as `table_type=VIEW`, for Unity Catalog and Glue
* *AND* the adapter SHALL write one warning line per skip that states the same reason
* *AND* no reason SHALL contain a credential value

### Scenario: A listing with no skip records an empty list that replaces the previous one

* *GIVEN* a virtual schema whose adapterNotes hold a `SKIPPED_TABLES` array of two entries, and a refresh that carries no `requestedTables` and whose listing skips no entry
* *WHEN* the refresh completes
* *THEN* `SKIPPED_TABLES` SHALL be an empty array
* *AND* every other adapterNotes entry SHALL hold the value the refresh derives for it, whatever `SKIPPED_TABLES` holds

### Scenario: A namespace whose every table is skipped still creates an empty virtual schema

* *GIVEN* a namespace whose every entry the listing skips
* *WHEN* createVirtualSchema runs
* *THEN* createVirtualSchema SHALL succeed with no table, per `vs-adapter/create-virtual-schema`
* *AND* `SKIPPED_TABLES` SHALL hold every skipped entry
* *AND* the adapter MUST NOT fail createVirtualSchema because every entry was skipped

### Scenario: A skipped-table list too long for adapterNotes is capped to the longest prefix that fits

* *GIVEN* a listing that skips so many entries that the serialized adapterNotes would exceed 2,000,000 bytes
* *WHEN* createVirtualSchema, setProperties, or a refresh that carries no `requestedTables` completes
* *THEN* `SKIPPED_TABLES` SHALL hold the longest prefix of the skipped entries, in listing order, for which the serialized adapterNotes, `SKIPPED_TABLES_OMITTED` included, stays within 2,000,000 bytes
* *AND* adapterNotes SHALL carry `SKIPPED_TABLES_OMITTED`, a string holding the number of entries the prefix drops
* *AND* createVirtualSchema and refresh MUST NOT fail because the list exceeds the limit
* *AND* a listing whose every entry fits SHALL carry no `SKIPPED_TABLES_OMITTED`, and a refresh that carries no `requestedTables` SHALL remove one that a previous run recorded
* *AND* the adapter SHALL still write one warning line per skip, including each dropped entry
