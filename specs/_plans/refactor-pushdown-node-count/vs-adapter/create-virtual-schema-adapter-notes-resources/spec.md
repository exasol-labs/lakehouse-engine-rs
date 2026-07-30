# Feature: Create Virtual Schema — AdapterNotes Resource Configuration

Records the computed resource budgets — parallelism factor, DataFusion threading
mode and per-instance thread/partition allocation, and memory-pool parameters — in
the `createVirtualSchema` response `adapterNotes` so that every per-shard scan UDF
instance receives a correctly sized CPU and memory envelope without the adapter
persisting any state between requests.

## Background

* All resource values are resolved at `createVirtualSchema` time from VS/connection
  properties and returned in `adapterNotes` (stringified JSON), which Exasol persists
  and round-trips back at pushdown time.
* The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* The per-node core count `nr_of_cores` resolves via the `NR_OF_CORES` property
  override, `std::thread::available_parallelism()` auto-detect, or `0` when unknown;
  it is no longer discovered over a connect-back session (see
  `vs-adapter/create-virtual-schema-adapter-notes`).
<!-- DELTA:CHANGED -->
* The parallelism factor is supplied as a VS/connection property and recorded
  alongside `NR_OF_CORES`; when absent it defaults to a hardware-aware value derived
  from `NR_OF_CORES`.
<!-- /DELTA:CHANGED -->
* The DataFusion threading configuration is selected by a `DATAFUSION_THREADING_MODE`
  VS/connection property (`AUTO` or `FIXED`, default `AUTO`). In `FIXED` mode the two
  independent properties `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF`
  are used verbatim (each defaulting to `max(NR_OF_CORES, 1)`); in `AUTO` mode the
  adapter derives a per-instance thread budget that does not oversubscribe a node (see
  `datafusion-scan/scan-execution-threading`). Only the resolved integer fields are
  round-tripped into the per-shard scan spec.
* The per-instance memory budget is two independent VS/connection properties —
  `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) —
  each recorded in `adapterNotes` and round-tripped into every per-shard scan spec,
  where the scan UDF sizes its DataFusion pool to
  `fraction × (per_instance_limit − overhead_bytes)`.
<!-- DELTA:CHANGED -->
* See `vs-adapter/create-virtual-schema-adapter-notes` for how the per-node core count
  (`NR_OF_CORES`) is discovered and recorded, and for why the cluster node count is
  deliberately NOT recorded here.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the parallelism factor in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that supplies a `PARALLELISM_FACTOR` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the supplied parallelism factor in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `NR_OF_CORES`
* *AND* when the `PARALLELISM_FACTOR` property is absent or not a positive integer the adapter SHALL default the parallelism factor to `NR_OF_CORES × 2`
* *AND* the adapter SHALL floor that default at 8 so that when `NR_OF_CORES` is 0, unavailable, or yields a product below 8 the parallelism factor is at least 8, persisting it nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, `std::thread::available_parallelism()` auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the threading mode
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_TARGET_PARTITIONS` value when it is a positive integer and otherwise default to `max(nr_of_cores, 1)`
* *AND* in `AUTO` mode the adapter SHALL set the target partition count equal to the AUTO-derived `df_threads_per_udf` (per `datafusion-scan/scan-execution-threading`), ignoring any supplied `DATAFUSION_TARGET_PARTITIONS` value, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADS_PER_UDF` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, `std::thread::available_parallelism()` auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion threads-per-UDF count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `NR_OF_CORES`, `PARALLELISM_FACTOR`, the threading mode, and the DataFusion target partition count
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_THREADS_PER_UDF` value when it is a positive integer and otherwise default to `max(nr_of_cores, 1)`
* *AND* in `AUTO` mode the adapter SHALL set the threads-per-UDF count to `max(1, floor(nr_of_cores / udf_instances_per_node))`, or to `1` when `nr_of_cores` is `0`, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the memory-pool fraction in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `MEMORY_POOL_FRACTION` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved memory-pool fraction in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion threading entries
* *AND* the adapter SHALL default the memory-pool fraction to `0.6` when the `MEMORY_POOL_FRACTION` property is absent, empty, not a positive number, or greater than `1.0`
* *AND* the adapter SHALL use the supplied value when it is a positive number not greater than `1.0`, persisting the fraction nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply an `INSTANCE_OVERHEAD_MB` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved instance-overhead megabytes in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `NR_OF_CORES`, `PARALLELISM_FACTOR`, the DataFusion threading entries, and the memory-pool fraction
* *AND* the adapter SHALL default the instance-overhead megabytes to `200` when the `INSTANCE_OVERHEAD_MB` property is absent, empty, or not a non-negative integer
* *AND* the adapter SHALL use the supplied value when it is a non-negative integer, persisting the overhead nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Recorded node count and parallelism factor drive later work-unit sharding

* *GIVEN* a `createVirtualSchema` request for which `UdfContext::node_count()` resolves the active node count
* *WHEN* the adapter returns the `createVirtualSchema` response
* *THEN* the `adapterNotes` SHALL carry both the resolved `CLUSTER_NODES` node count and the `PARALLELISM_FACTOR`
* *AND* both values SHALL be round-tripped back to the adapter at pushdown time so the shard count G can be computed as `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Recorded parallelism factor drives later work-unit sharding

* *GIVEN* a `createVirtualSchema` request whose resolved parallelism factor is P
* *WHEN* the adapter returns the `createVirtualSchema` response
* *THEN* the `adapterNotes` SHALL carry P as the `PARALLELISM_FACTOR` entry and SHALL carry no node-count entry
* *AND* P SHALL be round-tripped back to the adapter at pushdown time, where it is multiplied by the node count the pushdown reads from its own UDF handshake to give the shard count `G`, capped at 300
* *AND* the pushdown SHALL obtain the node-count factor of `G` from `UdfContext::node_count()` rather than from `adapterNotes` (see `vs-adapter/pushdown-planning`)
<!-- /DELTA:NEW -->
