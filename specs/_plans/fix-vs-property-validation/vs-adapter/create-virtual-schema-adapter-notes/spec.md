# Feature: Create Virtual Schema, AdapterNotes

The `createVirtualSchema` response's schema-level `adapterNotes` records the resource budgets and the skipped-table list. Later pushdowns read the budgets to bound each scan UDF instance. Each virtual table's catalog identifier lives in that table's own identifier note (`vs-adapter/create-virtual-schema`), not here. The notes exclude the cluster node count and the per-node core count.

<!-- DELTA:CHANGED -->
## Background

* The schema-level `adapterNotes` holds only values a pushdown cannot recompute, plus `SKIPPED_TABLES`. Every pushdown reads the cluster node count from `UdfContext::node_count()` in its own UDF handshake (`pushdown/pushdown-planning`), so the notes carry no node count.
* `SKIPPED_TABLES` is a diagnostic entry that no pushdown reads. It records the tables the listing skipped, so a user reads each skip reason from `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`. Only a partial refresh reads it back, to merge it (`vs-adapter/refresh-and-set-properties`).
* Exasol rejects an `adapterNotes` value longer than 2,000,000 characters with sqlCode `04000` on CREATE and REFRESH, and keeps the previous value. The tuning entries have a fixed size.
* No virtual schema property sets the core count, and `vs-adapter/property-validation` rejects a property name the adapter does not read. An operator who needs a smaller core count constrains the node's CPU affinity, which `available_parallelism()` honours.
* The adapter holds no state between requests. It returns the resolved tuning values of `vs-adapter/create-virtual-schema-adapter-notes-resources` in `adapterNotes` (stringified JSON), which Exasol persists and returns at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: createVirtualSchema adapterNotes omit the cluster node count

* *GIVEN* an Exasol session that has installed the VS adapter script, with the catalog and storage connection properties supplied
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL write exactly these schema-level entries into the returned `adapterNotes`: `PARALLELISM_FACTOR`, `DF_THREADING_MODE`, `DF_TARGET_PARTITIONS`, `DF_THREADS_PER_UDF`, `DF_BATCH_SIZE`, `MEMORY_POOL_FRACTION`, `INSTANCE_OVERHEAD_MB`, `S3_MAX_CONNECTIONS`, `JOIN_BROADCAST_MAX_BYTES`, and `SKIPPED_TABLES`, plus `SKIPPED_TABLES_OMITTED` when it caps the skip list
* *AND* a `refresh` or `setProperties` response SHALL carry the same entries and keep every other entry of the persisted `adapterNotes`, per `vs-adapter/refresh-and-set-properties`, so an inherited node-count entry stays unread
* *AND* the adapter MUST NOT write an entry for the cluster node count or the per-node core count, because every pushdown reads the node count from its own UDF handshake and derives the core count itself
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter derives the per-node core count from available_parallelism() on every request

* *GIVEN* an Exasol session that has installed the VS adapter script and supplies the catalog and storage connection properties
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL read the per-node core count from `std::thread::available_parallelism()` on the executing node, WITHOUT opening any connect-back session and WITHOUT reading any VS or connection property
* *AND* the adapter SHALL read that core count EXACTLY ONCE per request, at one call site, and pass the resolved value as a plain argument into the parallelism-factor, DataFusion-threading, and connection-concurrency derivations
* *AND* the adapter MUST NOT write the resolved core count into the response `adapterNotes`, and MUST NOT persist it anywhere else
<!-- /DELTA:CHANGED -->
