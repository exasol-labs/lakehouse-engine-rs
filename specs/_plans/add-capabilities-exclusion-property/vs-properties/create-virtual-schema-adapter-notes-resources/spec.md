# Feature: Create Virtual Schema, AdapterNotes Resource Configuration

The adapter records the computed resource budgets in the `createVirtualSchema` response `adapterNotes`: parallelism factor, DataFusion threading mode and per-instance thread and partition counts, and memory-pool parameters. Each per-shard scan UDF instance then gets a sized CPU and memory envelope, and the adapter persists no state between requests.

## Background

* The adapter resolves all resource values at `createVirtualSchema` time from VS and connection properties and returns them in `adapterNotes` (stringified JSON). Exasol persists them and returns them at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties. `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` exposes the notes.
* `nr_of_cores` comes from `std::thread::available_parallelism()` alone, resolved once per request and defaulting to `1` when the platform cannot report it. No VS property overrides it, and it is not recorded in `adapterNotes` (see `vs-properties/create-virtual-schema-adapter-notes`). Each derivation takes the count as an argument, so a unit test can inject any count.
* The parallelism factor is a VS or connection property recorded in `adapterNotes`. When absent, it defaults to 1, one shard per node.
* `DATAFUSION_THREADING_MODE` (`AUTO` or `FIXED`, default `AUTO`) selects the threading configuration. `FIXED` uses `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` as given, each falling back to its `AUTO` value. `AUTO` gives the node's single scan instance every core (`scan-runtime/scan-execution-threading`). Only the resolved integers reach the per-shard scan spec.
* `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) are independent VS or connection properties. The adapter records both in `adapterNotes` and copies them into every per-shard scan spec. The scan UDF sizes its DataFusion pool to `fraction × (per_instance_limit − overhead_bytes)`.
* `METADATA_READ_CONCURRENCY` (default `16`) bounds the adapter's concurrent metadata reads.
* `vs-properties/create-virtual-schema-adapter-notes` covers how the core count is discovered, why it is not recorded, and why the cluster node count is not recorded.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the metadata read concurrency in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema`, `refresh`, or `setProperties` request that may supply a `METADATA_READ_CONCURRENCY` property
* *WHEN* the adapter resolves the request's properties
* *THEN* the adapter SHALL record the supplied value as the `METADATA_READ_CONCURRENCY` entry of the returned `adapterNotes`, and `16` when the property is absent or empty
* *AND* a value outside the range `vs-properties/property-value-validation` gives the property SHALL fail a `createVirtualSchema` or `setProperties` request, and the adapter MUST NOT default it to `16` there; a `refresh` reads such a value as absent per `vs-properties/property-value-validation`
<!-- /DELTA:CHANGED -->
