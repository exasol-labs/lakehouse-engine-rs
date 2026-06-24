# Feature: Create Virtual Schema

Lets an Exasol user register an Iceberg table (resolved through an Iceberg REST
catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests)
as a queryable virtual schema, so the table's columns appear to Exasol with correctly
mapped SQL types, and records — in the response `adapterNotes` — the cluster's
active node count (via `NPROC()`), the per-node core count (via
`PARAM_VALUE('NR_OF_CORES')`), a parallelism factor, the per-UDF DataFusion
threading configuration (target partitions and Tokio worker threads), and the
per-instance memory budget controls (memory-pool fraction and container-overhead
megabytes), so later pushdowns can size the oversubscribed work-unit shard count
and bound each scan UDF instance's CPU and memory usage.

## Background

* Catalog endpoint and storage credentials are supplied through a CONNECTION object
  named by `CATALOG_CONNECTION`. The adapter resolves credentials via `ctx.connection`
  and never persists catalog metadata between requests.
* Credentials MUST NOT appear in any returned response or error message.
* The adapter holds no state between requests; cluster information is resolved per
  request and returned in the `createVirtualSchema` response's `adapterNotes`
  (stringified JSON), which Exasol persists and round-trips back at pushdown time.
  The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* `NPROC()` and `PARAM_VALUE('NR_OF_CORES')` are obtained over a single read-only
  connect-back session and recorded as `CLUSTER_NODES` and `NR_OF_CORES`.
* The parallelism factor is supplied as a VS/connection property and recorded
  alongside `CLUSTER_NODES` and `NR_OF_CORES`; when absent it defaults to a
  hardware-aware value derived from `NR_OF_CORES`.
* The DataFusion threading configuration is two independent VS/connection
  properties — `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` —
  each recorded in `adapterNotes` and each defaulting to `1`.
* The per-instance memory budget is two independent VS/connection properties —
  `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) —
  each recorded in `adapterNotes` and round-tripped into every per-shard scan spec,
  where the scan UDF sizes its DataFusion pool to
  `fraction × (per_instance_limit − overhead_bytes)`.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so`; it speaks the
  Exasol virtual-schema JSON protocol (request in, JSON response out).
* Schema mapping MUST use the same mapping as the scan, defined in the
  `datafusion-scan/type-mapping` feature.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Adapter records the memory-pool fraction in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `MEMORY_POOL_FRACTION` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved memory-pool fraction in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion threading entries
* *AND* the adapter SHALL default the memory-pool fraction to `0.6` when the `MEMORY_POOL_FRACTION` property is absent, empty, not a positive number, or greater than `1.0`
* *AND* the adapter SHALL use the supplied value when it is a positive number not greater than `1.0`, persisting the fraction nowhere other than that returned `adapterNotes`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply an `INSTANCE_OVERHEAD_MB` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved instance-overhead megabytes in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, the DataFusion threading entries, and the memory-pool fraction
* *AND* the adapter SHALL default the instance-overhead megabytes to `200` when the `INSTANCE_OVERHEAD_MB` property is absent, empty, or not a non-negative integer
* *AND* the adapter SHALL use the supplied value when it is a non-negative integer, persisting the overhead nowhere other than that returned `adapterNotes`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Recorded memory budget controls round-trip into the scan spec

* *GIVEN* a `createVirtualSchema` request that records a memory-pool fraction and instance-overhead megabytes in `adapterNotes`
* *WHEN* Exasol round-trips those `adapterNotes` back to the adapter at pushdown time and the adapter builds each per-shard scan spec
* *THEN* the adapter SHALL carry the resolved memory-pool fraction and instance-overhead bytes into every per-shard scan spec
* *AND* a scan spec that lacks these fields (a pre-existing spec) SHALL deserialize to the default fraction `0.6` and default overhead `200` megabytes
<!-- /DELTA:NEW -->
