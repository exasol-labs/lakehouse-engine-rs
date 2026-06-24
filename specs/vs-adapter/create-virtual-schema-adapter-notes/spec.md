# Feature: Create Virtual Schema — AdapterNotes

Records cluster configuration and the Exasol-name to Iceberg-identifier map in the `createVirtualSchema` response `adapterNotes` so that later pushdowns can size oversubscribed work-unit shards, bound each scan UDF instance's CPU and memory usage, and recover the scanned Iceberg table from the involved virtual table name.

## Background

* `NPROC()` and `PARAM_VALUE('NR_OF_CORES')` are obtained over a single read-only
  connect-back session and recorded as `CLUSTER_NODES` and `NR_OF_CORES`.
* The parallelism factor is supplied as a VS/connection property and recorded
  alongside `CLUSTER_NODES` and `NR_OF_CORES`; when absent it defaults to a
  hardware-aware value derived from `NR_OF_CORES`.
* The DataFusion threading configuration is two independent VS/connection
  properties — `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` —
  each recorded in `adapterNotes` and each defaulting to `1`. They bound how many
  DataFusion partitions and Tokio worker threads a single scan UDF instance uses,
  so that the product of (per-node concurrent UDF instances) × (per-instance
  threads) does not oversubscribe a node's cores. The recommended scale-up value
  for `DATAFUSION_TARGET_PARTITIONS` is `max(1, floor(NR_OF_CORES / parallelism_factor))`;
  this is documented guidance, not enforced — the defaults stay `1` unless the user
  overrides them.
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

### Scenario: Adapter records the cluster node count in the virtual-schema adapterNotes

* *GIVEN* an Exasol session that has installed the VS adapter script
* *AND* the catalog and storage connection properties are supplied to the adapter
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL open a connect-back session to Exasol and run `SELECT NPROC()` to obtain the count of active cluster nodes
* *AND* the adapter SHALL return the resolved node count as a positive-integer `CLUSTER_NODES` entry inside the `createVirtualSchema` response's `adapterNotes` (stringified JSON), which Exasol persists and which is queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`
* *AND* the adapter MUST NOT persist the node count anywhere other than that returned `adapterNotes`

### Scenario: Cluster node count defaults to one when it cannot be determined

* *GIVEN* the VS adapter cannot open a connect-back session or `SELECT NPROC()` fails
* *WHEN* Exasol sends a `createVirtualSchema` request
* *THEN* the adapter SHALL write `CLUSTER_NODES: 1` into the `adapterNotes` of the `createVirtualSchema` response
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response describing the mapped table
* *AND* the resulting single-shard behaviour MUST be identical to the pre-sharding single-node execution path

### Scenario: Adapter records the per-node core count in the virtual-schema adapterNotes

* *GIVEN* an Exasol session that has installed the VS adapter script and supplies the catalog and storage connection properties
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL, in the same read-only connect-back session it opens for `SELECT NPROC()`, run `SELECT PARAM_VALUE('NR_OF_CORES')` to obtain the per-node core count
* *AND* the adapter SHALL parse the returned VARCHAR to a non-negative integer and record it as an `NR_OF_CORES` entry inside the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES` and `PARALLELISM_FACTOR`
* *AND* the adapter SHALL write `NR_OF_CORES: 0` and still return a successful `createVirtualSchema` response when the session cannot be opened, the query fails, or the value cannot be parsed, persisting the core count nowhere other than that returned `adapterNotes`

### Scenario: Adapter records the parallelism factor in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that supplies a `PARALLELISM_FACTOR` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the supplied parallelism factor in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES` and `NR_OF_CORES`
* *AND* when the `PARALLELISM_FACTOR` property is absent or not a positive integer the adapter SHALL default the parallelism factor to `NR_OF_CORES × 2`
* *AND* the adapter SHALL floor that default at 8 so that when `NR_OF_CORES` is 0, unavailable, or yields a product below 8 the parallelism factor is at least 8, persisting it nowhere other than that returned `adapterNotes`

### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, and `PARALLELISM_FACTOR`
* *AND* the adapter SHALL default the target partition count to `1` when the `DATAFUSION_TARGET_PARTITIONS` property is absent, empty, zero, or not a positive integer
* *AND* the adapter SHALL use the supplied value when it is a positive integer, persisting the count nowhere other than that returned `adapterNotes`

### Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADS_PER_UDF` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion threads-per-UDF count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion target partition count
* *AND* the adapter SHALL default the threads-per-UDF count to `1` when the `DATAFUSION_THREADS_PER_UDF` property is absent, empty, zero, or not a positive integer
* *AND* the adapter SHALL use the supplied value when it is a positive integer, persisting the count nowhere other than that returned `adapterNotes`

### Scenario: Recorded node count and parallelism factor drive later work-unit sharding

* *GIVEN* a `createVirtualSchema` request for which `NPROC()` resolves the active node count
* *WHEN* the adapter returns the `createVirtualSchema` response
* *THEN* the `adapterNotes` SHALL carry both the resolved `CLUSTER_NODES` node count and the `PARALLELISM_FACTOR`
* *AND* both values SHALL be round-tripped back to the adapter at pushdown time so the shard count G can be computed as `CLUSTER_NODES × PARALLELISM_FACTOR` capped at 300

### Scenario: Adapter records the memory-pool fraction in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `MEMORY_POOL_FRACTION` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved memory-pool fraction in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion threading entries
* *AND* the adapter SHALL default the memory-pool fraction to `0.6` when the `MEMORY_POOL_FRACTION` property is absent, empty, not a positive number, or greater than `1.0`
* *AND* the adapter SHALL use the supplied value when it is a positive number not greater than `1.0`, persisting the fraction nowhere other than that returned `adapterNotes`

### Scenario: Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply an `INSTANCE_OVERHEAD_MB` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved instance-overhead megabytes in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, the DataFusion threading entries, and the memory-pool fraction
* *AND* the adapter SHALL default the instance-overhead megabytes to `200` when the `INSTANCE_OVERHEAD_MB` property is absent, empty, or not a non-negative integer
* *AND* the adapter SHALL use the supplied value when it is a non-negative integer, persisting the overhead nowhere other than that returned `adapterNotes`

### Scenario: Recorded memory budget controls round-trip into the scan spec

* *GIVEN* a `createVirtualSchema` request that records a memory-pool fraction and instance-overhead megabytes in `adapterNotes`
* *WHEN* Exasol round-trips those `adapterNotes` back to the adapter at pushdown time and the adapter builds each per-shard scan spec
* *THEN* the adapter SHALL carry the resolved memory-pool fraction and instance-overhead bytes into every per-shard scan spec
* *AND* a scan spec that lacks these fields (a pre-existing spec) SHALL deserialize to the default fraction `0.6` and default overhead `200` megabytes
