# Feature: Create Virtual Schema, AdapterNotes

The `createVirtualSchema` response's schema-level `adapterNotes` records the resource budgets and the skipped-table list. Later pushdowns read the budgets to bound each scan UDF instance. Each virtual table's catalog identifier lives in that table's own identifier note (`vs-adapter/create-virtual-schema`), not here. The notes exclude the cluster node count and the per-node core count.

## Background

* The schema-level `adapterNotes` holds only values a pushdown cannot recompute, plus `SKIPPED_TABLES`. Every pushdown reads the cluster node count from `UdfContext::node_count()` in its own UDF handshake (`pushdown/pushdown-planning`), so the notes carry no node count.
* No pushdown reads the per-node core count. The adapter uses it as an input to the parallelism-factor, DataFusion-threading, and connection-concurrency derivations and discards it. Only the derived entries round-trip.
* `SKIPPED_TABLES` is a diagnostic entry that no pushdown reads. It records the tables the listing skipped, so a user reads each skip reason from `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Only a partial refresh reads it back, to merge it (`vs-adapter/refresh-and-set-properties`).
* Exasol rejects an `adapterNotes` value longer than 2,000,000 characters with sqlCode `04000` on CREATE and REFRESH, and keeps the previous value. The adapter caps `SKIPPED_TABLES` by serialized byte length and records the dropped count as `SKIPPED_TABLES_OMITTED`. The tuning entries have a fixed size and are not capped.
* The adapter reads the core count from `std::thread::available_parallelism()` on the executing node, once per request, at one call site. The scan UDF uses the same source for DataFusion `target_partitions` (`scan-runtime/scan-execution-threading`).
* When `available_parallelism()` returns an `io::Error`, the adapter uses a core count of `1`. There is no unknown sentinel, because every derivation floors its own result, so the unavailable case and a genuine single-core node produce identical budgets.
* No VS or connection property sets the core count. The adapter reads properties by name and ignores unknown names. An operator who needs a smaller core count constrains the node's CPU affinity, which `available_parallelism()` honours.
* The adapter opens no connect-back session for topology discovery, at create time or at pushdown time.
* The parallelism factor, `DATAFUSION_THREADING_MODE`, `DATAFUSION_TARGET_PARTITIONS`, `DATAFUSION_THREADS_PER_UDF`, `MEMORY_POOL_FRACTION`, and `INSTANCE_OVERHEAD_MB` are VS or connection properties. The adapter records the resolved values in `adapterNotes` and they round-trip into every per-shard scan spec. See `vs-adapter/create-virtual-schema-adapter-notes-resources`.
* The adapter holds no state between requests. It returns all values in `adapterNotes` (stringified JSON), which Exasol persists and returns at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties.

## Scenarios

### Scenario: createVirtualSchema adapterNotes omit the cluster node count

* *GIVEN* an Exasol session that has installed the VS adapter script, with the catalog and storage connection properties supplied
* *WHEN* Exasol sends a `createVirtualSchema`, `refresh`, or `setProperties` request naming an Iceberg table
* *THEN* the `adapterNotes` of the returned response MUST NOT carry a `CLUSTER_NODES` entry
* *AND* the `adapterNotes` MUST NOT carry an `NR_OF_CORES` entry either, because no pushdown reads the per-node core count back
* *AND* the `adapterNotes` SHALL still carry `PARALLELISM_FACTOR` and `SKIPPED_TABLES`, so omitting both counts does not disturb any other recorded entry

### Scenario: Adapter derives the per-node core count from available_parallelism() on every request

* *GIVEN* an Exasol session that has installed the VS adapter script and supplies the catalog and storage connection properties
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL read the per-node core count from `std::thread::available_parallelism()` on the executing node, WITHOUT opening any connect-back session and WITHOUT reading any VS or connection property
* *AND* the adapter SHALL read that core count EXACTLY ONCE per request, at one call site, and pass the resolved value as a plain argument into the parallelism-factor, DataFusion-threading, and connection-concurrency derivations
* *AND* the adapter MUST NOT write the resolved core count into the response `adapterNotes`, and MUST NOT persist it anywhere else
* *AND* the adapter SHALL accept a property name it does not recognize and SHALL resolve every budget to the same value it resolves without that name, so no property reaches the core count

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
