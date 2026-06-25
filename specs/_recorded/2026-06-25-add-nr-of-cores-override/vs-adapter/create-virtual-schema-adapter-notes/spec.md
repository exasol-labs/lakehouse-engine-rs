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
* The DataFusion threading configuration is two independent VS/connection
  properties — `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` —
  each recorded in `adapterNotes`. When a property is absent or not a positive
  integer, the default is `max(NR_OF_CORES, 1)` so scans auto-parallelize to the
  detected or overridden core count; when `NR_OF_CORES` is `0` (unknown), the
  default falls back to `1`, preserving prior single-threaded behavior.
* The per-instance memory budget is two independent VS/connection properties —
  `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) —
  each recorded in `adapterNotes` and round-tripped into every per-shard scan spec.
* The adapter holds no state between requests; all values are resolved per request
  and returned in the `createVirtualSchema` response's `adapterNotes` (stringified JSON),
  which Exasol persists and round-trips back at pushdown time.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: NR_OF_CORES VS property overrides the connect-back auto-detected core count

* *GIVEN* a `createVirtualSchema` request that supplies an `NR_OF_CORES` connection/VS property set to a positive integer N
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL use N as the per-node core count and SHALL NOT issue `SELECT PARAM_VALUE('NR_OF_CORES')` over the connect-back session to discover the core count
* *AND* the adapter SHALL record N as the `NR_OF_CORES` entry in the `createVirtualSchema` response's `adapterNotes` (stringified JSON)
* *AND* the adapter MUST NOT persist the overridden core count anywhere other than that returned `adapterNotes`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: NR_OF_CORES VS property is ignored when absent, empty, or not a positive integer

* *GIVEN* a `createVirtualSchema` request that supplies an `NR_OF_CORES` connection/VS property that is absent, empty, zero, negative, or non-numeric
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL fall back to obtaining the core count via `SELECT PARAM_VALUE('NR_OF_CORES')` over the connect-back session, and SHALL write `NR_OF_CORES: 0` when that also fails
* *AND* the adapter SHALL NOT use the invalid property value as the core count
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, connect-back auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, and `PARALLELISM_FACTOR`
* *AND* the adapter SHALL default the target partition count to `max(nr_of_cores, 1)` when the `DATAFUSION_TARGET_PARTITIONS` property is absent, empty, zero, or not a positive integer
* *AND* the adapter SHALL use the supplied value when it is a positive integer, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADS_PER_UDF` connection/VS property
* *AND* the per-node core count resolves to `nr_of_cores` (via `NR_OF_CORES` property override, connect-back auto-detect, or `0` when unknown)
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion threads-per-UDF count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion target partition count
* *AND* the adapter SHALL default the threads-per-UDF count to `max(nr_of_cores, 1)` when the `DATAFUSION_THREADS_PER_UDF` property is absent, empty, zero, or not a positive integer
* *AND* the adapter SHALL use the supplied value when it is a positive integer, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->
