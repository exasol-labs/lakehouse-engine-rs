# Feature: Create Virtual Schema

Lets an Exasol user register an Iceberg table (resolved through an Iceberg REST
catalog over S3-compatible storage, including AWS Glue with SigV4-signed requests)
as a queryable virtual schema, so the table's columns appear to Exasol with correctly
mapped SQL types, and records — in the response `adapterNotes` — the cluster's
active node count (via `NPROC()`), the per-node core count (via
`PARAM_VALUE('NR_OF_CORES')`), a parallelism factor, and the per-UDF DataFusion
threading configuration (target partitions and Tokio worker threads), so later
pushdowns can size the oversubscribed work-unit shard count and bound each scan
UDF instance's CPU usage.

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
  each recorded in `adapterNotes` and each defaulting to `1`. They bound how many
  DataFusion partitions and Tokio worker threads a single scan UDF instance uses,
  so that the product of (per-node concurrent UDF instances) × (per-instance
  threads) does not oversubscribe a node's cores. The recommended scale-up value
  for `DATAFUSION_TARGET_PARTITIONS` is `max(1, floor(NR_OF_CORES / parallelism_factor))`;
  this is documented guidance, not enforced — the defaults stay `1` unless the user
  overrides them.
* The adapter is the Rust ADAPTER SCRIPT entry point of a single `.so`; it speaks the
  Exasol virtual-schema JSON protocol (request in, JSON response out).
* Schema mapping (C.2) MUST use the same mapping as the scan, defined in the
  `datafusion-scan/type-mapping` feature. Columns whose Arrow type Exasol cannot
  represent (list, struct, map, binary, out-of-range decimal, and the other incompatible
  types) are declared as `VARCHAR(2000000)` — they MUST NOT cause `createVirtualSchema`
  to error.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Adapter records the per-node core count in the virtual-schema adapterNotes

* *GIVEN* an Exasol session that has installed the VS adapter script and supplies the catalog and storage connection properties
* *WHEN* Exasol sends a `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL, in the same read-only connect-back session it opens for `SELECT NPROC()`, run `SELECT PARAM_VALUE('NR_OF_CORES')` to obtain the per-node core count
* *AND* the adapter SHALL parse the returned VARCHAR to a non-negative integer and record it as an `NR_OF_CORES` entry inside the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES` and `PARALLELISM_FACTOR`
* *AND* the adapter SHALL write `NR_OF_CORES: 0` and still return a successful `createVirtualSchema` response when the session cannot be opened, the query fails, or the value cannot be parsed, persisting the core count nowhere other than that returned `adapterNotes`
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the parallelism factor in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that supplies a `PARALLELISM_FACTOR` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the supplied parallelism factor in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES` and `NR_OF_CORES`
* *AND* when the `PARALLELISM_FACTOR` property is absent or not a positive integer the adapter SHALL default the parallelism factor to `NR_OF_CORES × 2`
* *AND* the adapter SHALL floor that default at 8 so that when `NR_OF_CORES` is 0, unavailable, or yields a product below 8 the parallelism factor is at least 8, persisting it nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, and `PARALLELISM_FACTOR`
* *AND* the adapter SHALL default the target partition count to `1` when the `DATAFUSION_TARGET_PARTITIONS` property is absent, empty, zero, or not a positive integer
* *AND* the adapter SHALL use the supplied value when it is a positive integer, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADS_PER_UDF` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion threads-per-UDF count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `CLUSTER_NODES`, `NR_OF_CORES`, `PARALLELISM_FACTOR`, and the DataFusion target partition count
* *AND* the adapter SHALL default the threads-per-UDF count to `1` when the `DATAFUSION_THREADS_PER_UDF` property is absent, empty, zero, or not a positive integer
* *AND* the adapter SHALL use the supplied value when it is a positive integer, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:NEW -->
