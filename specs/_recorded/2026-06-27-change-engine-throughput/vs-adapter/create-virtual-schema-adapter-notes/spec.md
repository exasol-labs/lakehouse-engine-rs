# Feature: Create Virtual Schema — AdapterNotes

Records cluster configuration and the Exasol-name to Iceberg-identifier map in the `createVirtualSchema` response `adapterNotes` so that later pushdowns can size oversubscribed work-unit shards, bound each scan UDF instance's CPU and memory usage, and recover the scanned Iceberg table from the involved virtual table name.

## Background

* `NPROC()` and `PARAM_VALUE('NR_OF_CORES')` are obtained over a single read-only
  connect-back session and recorded as `CLUSTER_NODES` and `NR_OF_CORES`.
* An explicit `NR_OF_CORES` VS property (integer ≥ 1) overrides the connect-back
  auto-detected core count; an absent, empty, or non-positive value falls back to
  auto-detect; if auto-detect also fails, `NR_OF_CORES` is recorded as `0`.
* The parallelism factor is supplied as a VS/connection property and recorded
  alongside `CLUSTER_NODES` and `NR_OF_CORES`; when absent it defaults to a
  hardware-aware value derived from `NR_OF_CORES`.
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

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Adapter records the DataFusion threading mode in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADING_MODE` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved threading mode (`AUTO` or `FIXED`) in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside the DataFusion target-partition and threads-per-UDF entries
* *AND* the adapter SHALL select `AUTO` when the property is absent, empty, or neither `AUTO` nor `FIXED` (case-insensitive)
* *AND* the adapter SHALL persist the mode nowhere other than that returned `adapterNotes`
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, connect-back auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the threading mode
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_TARGET_PARTITIONS` value when it is a positive integer and otherwise default to `max(nr_of_cores, 1)`
* *AND* in `AUTO` mode the adapter SHALL set the target partition count equal to the AUTO-derived `df_threads_per_udf` (per `datafusion-scan/scan-execution-threading`), ignoring any supplied `DATAFUSION_TARGET_PARTITIONS` value, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADS_PER_UDF` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, connect-back auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion threads-per-UDF count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, the threading mode, and the DataFusion target partition count
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_THREADS_PER_UDF` value when it is a positive integer and otherwise default to `max(nr_of_cores, 1)`
* *AND* in `AUTO` mode the adapter SHALL set the threads-per-UDF count to `max(1, floor(nr_of_cores / udf_instances_per_node))`, or to `1` when `nr_of_cores` is `0`, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->
