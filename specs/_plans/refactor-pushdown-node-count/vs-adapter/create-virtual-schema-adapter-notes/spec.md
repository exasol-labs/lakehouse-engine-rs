# Feature: Create Virtual Schema — AdapterNotes

Records the per-node core count, the resource budgets, and the Exasol-name to Iceberg-identifier map in the `createVirtualSchema` response `adapterNotes`. Later pushdowns read these back to bound each scan UDF instance's CPU and memory usage and to recover the scanned Iceberg table from the involved virtual table name. `adapterNotes` carries only values a pushdown cannot recompute. The cluster node count is excluded, because every pushdown reads it from its own UDF handshake, and a `CLUSTER_NODES` entry inherited from a pre-refactor schema is removed rather than preserved.

## Background

<!-- DELTA:CHANGED -->
* The active cluster node count is NOT recorded in `adapterNotes`. It is UDF handshake
  metadata that every VS request already carries, so each `pushdown` reads it directly
  from `UdfContext::node_count()` instead of from a persisted note (see
  `vs-adapter/pushdown-planning`). `adapterNotes` is reserved for values derived at
  create time that a pushdown cannot recompute, such as `TABLE_MAP`.
* Not recording the node count is not the same as not removing it. The adapter builds
  its response notes by merging into the notes Exasol round-trips back, so a
  `CLUSTER_NODES` entry persisted by an earlier adapter version would otherwise survive
  every `refresh` and `setProperties` indefinitely. The adapter therefore removes that
  key explicitly while building the notes; the merge preserves every OTHER pre-existing
  entry, and `CLUSTER_NODES` is its one exception.
* The per-node core count is read directly on the executing node via
  `std::thread::available_parallelism()` and recorded as `NR_OF_CORES`; this is the
  same host-core-count source the scan UDF already trusts for DataFusion
  `target_partitions` (see `datafusion-scan/scan-execution-threading`).
* No topology value uses a connect-back session, at create time or at pushdown time;
  the adapter opens no read-only SQL session for topology discovery, issues no
  `SELECT NPROC()` or `SELECT PARAM_VALUE(...)`, and honours no `CONNECTION_NAME` VS
  property for this purpose. `CONNECTION_NAME` is no longer a supported VS property.
* An explicit `NR_OF_CORES` VS property (integer ≥ 1) overrides the
  `available_parallelism()` auto-detected core count; an absent, empty, or
  non-positive value falls back to auto-detect; if auto-detect also fails,
  `NR_OF_CORES` is recorded as `0`.
* The parallelism factor is supplied as a VS/connection property and recorded
  alongside `NR_OF_CORES`; when absent it defaults to a hardware-aware value derived
  from `NR_OF_CORES`.
<!-- /DELTA:CHANGED -->
* The DataFusion threading configuration is selected by a `DATAFUSION_THREADING_MODE`
  VS/connection property (`AUTO` or `FIXED`, default `AUTO`) and recorded in
  `adapterNotes`. In `FIXED` mode the two independent properties
  `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` are used verbatim
  (each defaulting to `max(NR_OF_CORES, 1)`); in `AUTO` mode the adapter derives a
  per-instance thread budget that does not oversubscribe a node (see
  `datafusion-scan/scan-execution-threading`). Whichever mode is selected, only the
  resolved integer `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF`
  values are round-tripped into the per-shard scan spec.
* The per-instance memory budget is two independent VS/connection properties —
  `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) —
  each recorded in `adapterNotes` and round-tripped into every per-shard scan spec,
  where the scan UDF sizes its DataFusion pool to
  `fraction × (per_instance_limit − overhead_bytes)`.
* The adapter holds no state between requests; all values are resolved per request
  and returned in the `createVirtualSchema` response's `adapterNotes` (stringified JSON),
  which Exasol persists and round-trips back at pushdown time.
  The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* See `vs-adapter/create-virtual-schema-adapter-notes-resources` for the resource
  configuration scenarios (parallelism factor, DataFusion threading, memory budget).

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: Adapter records the cluster node count in the virtual-schema adapterNotes

* *GIVEN* an Exasol session that has installed the VS adapter script
* *AND* the catalog and storage connection properties are supplied to the adapter
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL read the active cluster node count from `UdfContext::node_count()` (the UDF handshake metadata) WITHOUT opening any connect-back session
* *AND* the adapter SHALL return the resolved node count as a positive-integer `CLUSTER_NODES` entry inside the `createVirtualSchema` response's `adapterNotes` (stringified JSON), which Exasol persists and which is queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`
* *AND* the adapter MUST NOT persist the node count anywhere other than that returned `adapterNotes`
<!-- /DELTA:REMOVED -->

<!-- DELTA:REMOVED -->
### Scenario: Cluster node count defaults to one when it cannot be determined

* *GIVEN* `UdfContext::node_count()` returns `0` (a context carrying no live handshake node count)
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL write `CLUSTER_NODES: 1` into the `adapterNotes` of the `createVirtualSchema` response
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response describing the mapped table
* *AND* the resulting single-shard behaviour MUST be identical to the pre-sharding single-node execution path
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: createVirtualSchema adapterNotes omit the cluster node count

* *GIVEN* an Exasol session that has installed the VS adapter script, with the catalog and storage connection properties supplied
* *AND* the request's `schemaMetadataInfo.adapterNotes` either carry no notes at all or carry a `CLUSTER_NODES` entry persisted by an earlier adapter version
* *WHEN* Exasol sends a `createVirtualSchema`, `refresh`, or `setProperties` request naming an Iceberg table
* *THEN* the `adapterNotes` of the returned response MUST NOT carry a `CLUSTER_NODES` entry
* *AND* the adapter SHALL remove an inherited `CLUSTER_NODES` entry while building those notes, rather than letting the merge preserve it, so the entry disappears from the persisted notes on the first `refresh` or `setProperties` after the upgrade
* *AND* the `adapterNotes` SHALL still carry `NR_OF_CORES`, `PARALLELISM_FACTOR`, and `TABLE_MAP`, so removing the node count does not disturb any other recorded entry
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the per-node core count in the virtual-schema adapterNotes

* *GIVEN* an Exasol session that has installed the VS adapter script and supplies the catalog and storage connection properties
* *AND* no `NR_OF_CORES` VS property override is supplied
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL read the per-node core count from `std::thread::available_parallelism()` on the executing node WITHOUT opening any connect-back session
* *AND* the adapter SHALL record the resolved positive-integer core count as an `NR_OF_CORES` entry inside the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `PARALLELISM_FACTOR`
* *AND* the adapter SHALL write `NR_OF_CORES: 0` and still return a successful `createVirtualSchema` response when `available_parallelism()` cannot determine the core count, persisting the core count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->
