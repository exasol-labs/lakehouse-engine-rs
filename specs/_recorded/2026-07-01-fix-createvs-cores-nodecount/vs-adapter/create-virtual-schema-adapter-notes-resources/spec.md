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
* The per-node core count `nr_of_cores` resolves via the `NR_OF_CORES` property
  override, `std::thread::available_parallelism()` auto-detect, or `0` when unknown;
  it is no longer discovered over a connect-back session (see
  `vs-adapter/create-virtual-schema-adapter-notes`).
* See `vs-adapter/create-virtual-schema-adapter-notes` for how the cluster topology
  (`CLUSTER_NODES`, `NR_OF_CORES`) is discovered and recorded.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, `std::thread::available_parallelism()` auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the threading mode
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_TARGET_PARTITIONS` value when it is a positive integer and otherwise default to `max(nr_of_cores, 1)`
* *AND* in `AUTO` mode the adapter SHALL set the target partition count equal to the AUTO-derived `df_threads_per_udf` (per `datafusion-scan/scan-execution-threading`), ignoring any supplied `DATAFUSION_TARGET_PARTITIONS` value, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADS_PER_UDF` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, `std::thread::available_parallelism()` auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion threads-per-UDF count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, the threading mode, and the DataFusion target partition count
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_THREADS_PER_UDF` value when it is a positive integer and otherwise default to `max(nr_of_cores, 1)`
* *AND* in `AUTO` mode the adapter SHALL set the threads-per-UDF count to `max(1, floor(nr_of_cores / udf_instances_per_node))`, or to `1` when `nr_of_cores` is `0`, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Recorded node count and parallelism factor drive later work-unit sharding

* *GIVEN* a `createVirtualSchema` request for which `UdfContext::node_count()` resolves the active node count
* *WHEN* the adapter returns the `createVirtualSchema` response
* *THEN* the `adapterNotes` SHALL carry both the resolved `CLUSTER_NODES` node count and the `PARALLELISM_FACTOR`
* *AND* both values SHALL be round-tripped back to the adapter at pushdown time so the shard count G can be computed as `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300
<!-- /DELTA:CHANGED -->
