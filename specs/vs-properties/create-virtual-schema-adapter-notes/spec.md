# Feature: Create Virtual Schema, AdapterNotes

The `createVirtualSchema` response's schema-level `adapterNotes` records the resource budgets and the skipped-table list. Later pushdowns read the budgets to bound each scan UDF instance. Each virtual table's catalog identifier lives in that table's own identifier note (`vs-adapter/create-virtual-schema`), not here. The notes exclude the cluster node count and the per-node core count.

## Background

* The schema-level `adapterNotes` holds only values a pushdown cannot recompute, plus `SKIPPED_TABLES`. Every pushdown reads the cluster node count from `UdfContext::node_count()` in its own UDF handshake (`pushdown/pushdown-planning`), so the notes carry no node count.
* `SKIPPED_TABLES` is a diagnostic entry that no pushdown reads. It records the tables the listing skipped, so a user reads each skip reason from `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Only a partial refresh reads it back, to merge it (`vs-adapter/refresh-and-set-properties`).
* Exasol rejects an `adapterNotes` value longer than 2,000,000 characters with sqlCode `04000` on CREATE and REFRESH, and keeps the previous value. The tuning entries have a fixed size.
* No virtual schema property sets the core count, and `vs-properties/property-name-validation` rejects a property name the adapter does not read. An operator who needs a smaller core count constrains the node's CPU affinity, which `available_parallelism()` honours.
* The adapter holds no state between requests. It returns the resolved tuning values of `vs-properties/create-virtual-schema-adapter-notes-resources` in `adapterNotes` (stringified JSON), which Exasol persists and returns at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties.

## Scenarios

### Scenario: createVirtualSchema adapterNotes omit the cluster node count

* *GIVEN* an Exasol session that has installed the VS adapter script, with the catalog and storage connection properties supplied
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL write exactly these schema-level entries into the returned `adapterNotes`: `PARALLELISM_FACTOR`, `DF_THREADING_MODE`, `DF_TARGET_PARTITIONS`, `DF_THREADS_PER_UDF`, `DF_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `S3_MAX_CONNECTIONS`, `JOIN_BROADCAST_MAX_BYTES`, and `SKIPPED_TABLES`, plus `SKIPPED_TABLES_OMITTED` when it caps the skip list
* *AND* a `refresh` or `setProperties` response SHALL carry the same entries and keep every other entry of the persisted `adapterNotes`, per `vs-adapter/refresh-and-set-properties`, so an inherited node-count entry stays unread
* *AND* the adapter MUST NOT write an entry for the cluster node count or the per-node core count, because every pushdown reads the node count from its own UDF handshake and derives the core count itself

### Scenario: Adapter derives the per-node core count from available_parallelism() on every request

* *GIVEN* an Exasol session that has installed the VS adapter script and supplies the catalog and storage connection properties
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL read the per-node core count from `std::thread::available_parallelism()` on the executing node, WITHOUT opening any connect-back session and WITHOUT reading any VS or connection property
* *AND* the adapter SHALL read that core count EXACTLY ONCE per request, at one call site, and pass the resolved value as a plain argument into the parallelism-factor, DataFusion-threading, and connection-concurrency derivations
* *AND* the adapter MUST NOT write the resolved core count into the response `adapterNotes`, and MUST NOT persist it anywhere else

### Scenario: Adapter uses a core count of 1 when available_parallelism() cannot report one

* *GIVEN* an executing node whose platform cannot report a core count, so `std::thread::available_parallelism()` returns an error
* *WHEN* the adapter resolves the per-node core count for a `createVirtualSchema` request
* *THEN* the adapter SHALL use a core count of `1`, and MUST NOT use a distinct unknown sentinel value such as `0`
* *AND* the adapter SHALL return a successful `createVirtualSchema` response
* *AND* every budget derived from that core count SHALL equal the budget a genuine single-core node produces

### Scenario: The adapter detects the node's CPU affinity set

* *GIVEN* an Exasol node whose CPU affinity set holds `k` CPUs, with `2 ≤ k <` the host's core count
* *AND* a virtual schema created with no `DATAFUSION_THREADING_MODE` property
* *WHEN* the adapter records `DF_THREADS_PER_UDF` in the schema's adapterNotes
* *THEN* the recorded value SHALL equal `k`, not the fallback of `1` or the host's core count

### Scenario: Every skipped table is recorded with its reason under every catalog kind

* *GIVEN* one listing per catalog kind that skips entries: Iceberg REST (a table whose `loadTable` returns 404), Unity Catalog (a view), direct storage (a directory holding no data file), and Glue (an ORC table and a partition-projection table)
* *WHEN* createVirtualSchema, setProperties, or a refresh that carries no `requestedTables` completes
* *THEN* adapterNotes SHALL carry a `SKIPPED_TABLES` entry holding a JSON array with one object per skipped entry, in listing order, whose `table` is the catalog identifier and whose `reason` states why the entry was skipped
* *AND* the `reason` SHALL be `catalog reported it is not a loadable Iceberg table` for Iceberg REST and `holds no data file` for direct storage, and SHALL name the catalog value that decided the skip, such as `table_type=VIEW`, for Unity Catalog and Glue
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
