# Feature: Create Virtual Schema, AdapterNotes Resource Configuration

The adapter records the computed resource budgets in the `createVirtualSchema` response `adapterNotes`: parallelism factor, DataFusion threading mode and per-instance thread and partition counts, and memory-pool parameters. Each per-shard scan UDF instance then gets a sized CPU and memory envelope, and the adapter persists no state between requests.

## Background

* The adapter resolves all resource values at `createVirtualSchema` time from VS and connection properties and returns them in `adapterNotes` (stringified JSON). Exasol persists them and returns them at pushdown time. `schemaMetadata.properties` is not used, because Exasol 2025.2.1 silently drops adapter-returned properties. `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES` exposes the notes.
* `nr_of_cores` comes from `std::thread::available_parallelism()` alone, resolved once per request and defaulting to `1` when the platform cannot report it. No VS property overrides it, and it is not recorded in `adapterNotes` (see `vs-adapter/create-virtual-schema-adapter-notes`). Each derivation takes the count as an argument, so a unit test can inject any count.
* The parallelism factor is a VS or connection property recorded in `adapterNotes`. When absent, it defaults to 1, one shard per node.
* `DATAFUSION_THREADING_MODE` (`AUTO` or `FIXED`, default `AUTO`) selects the threading configuration. `FIXED` uses `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` as given, each falling back to its `AUTO` value. `AUTO` gives the node's single scan instance every core (`scan-runtime/scan-execution-threading`). Only the resolved integers reach the per-shard scan spec.
* `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) are independent VS or connection properties. The adapter records both in `adapterNotes` and copies them into every per-shard scan spec. The scan UDF sizes its DataFusion pool to `fraction × (per_instance_limit − overhead_bytes)`.
* `vs-adapter/create-virtual-schema-adapter-notes` covers how the core count is discovered, why it is not recorded, and why the cluster node count is not recorded.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the parallelism factor in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that supplies a `PARALLELISM_FACTOR` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the supplied parallelism factor in the `createVirtualSchema` response's `adapterNotes` (stringified JSON)
* *AND* when the `PARALLELISM_FACTOR` property is absent or empty the adapter SHALL default the parallelism factor to `1`, persisting it nowhere other than that returned `adapterNotes`
* *AND* a value outside the range `vs-adapter/property-validation` gives the property SHALL fail the request, and the adapter MUST NOT default it to `1`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion threading mode in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADING_MODE` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved threading mode (`AUTO` or `FIXED`) in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside the DataFusion target-partition and threads-per-UDF entries
* *AND* the adapter SHALL select `AUTO` when the property is absent or empty, and SHALL accept `AUTO` and `FIXED` without regard to case
* *AND* any other value SHALL fail the request per `vs-adapter/property-validation`, and the adapter MUST NOT select `AUTO` for it
* *AND* the adapter SHALL persist the mode nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *AND* a per-node core count `nr_of_cores` of at least `1`, resolved from `std::thread::available_parallelism()`
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `PARALLELISM_FACTOR` and the threading mode
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_TARGET_PARTITIONS` value, and SHALL fall back to the `AUTO` value when the property is absent or empty
* *AND* in `AUTO` mode the adapter SHALL set the target partition count to `2 × nr_of_cores` (per `scan-runtime/scan-execution-threading`), ignoring any supplied `DATAFUSION_TARGET_PARTITIONS` value, persisting the count nowhere other than that returned `adapterNotes`
* *AND* in either mode a value outside the range `vs-adapter/property-validation` gives the property SHALL fail the request
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion threads-per-UDF count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_THREADS_PER_UDF` connection/VS property
* *AND* a per-node core count `nr_of_cores` of at least `1`, resolved from `std::thread::available_parallelism()`
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion threads-per-UDF count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `PARALLELISM_FACTOR`, the threading mode, and the DataFusion target partition count
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_THREADS_PER_UDF` value, and SHALL fall back to the `AUTO` value when the property is absent or empty
* *AND* in `AUTO` mode the adapter SHALL set the threads-per-UDF count to `nr_of_cores`, which yields `1` for the single core the unavailable-detection case produces, persisting the count nowhere other than that returned `adapterNotes`
* *AND* in either mode a value outside the range `vs-adapter/property-validation` gives the property SHALL fail the request
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the memory-pool fraction in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `MEMORY_POOL_FRACTION` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved memory-pool fraction in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `PARALLELISM_FACTOR` and the DataFusion threading entries
* *AND* the adapter SHALL default the memory-pool fraction to `0.6` when the `MEMORY_POOL_FRACTION` property is absent or empty
* *AND* the adapter SHALL use the supplied value when it is a positive number not greater than `1.0`, persisting the fraction nowhere other than that returned `adapterNotes`
* *AND* any other value SHALL fail the request per `vs-adapter/property-validation`, and the adapter MUST NOT default it to `0.6`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the instance-overhead megabytes in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply an `INSTANCE_OVERHEAD_MB` connection/VS property
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved instance-overhead megabytes in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `PARALLELISM_FACTOR`, the DataFusion threading entries, and the memory-pool fraction
* *AND* the adapter SHALL default the instance-overhead megabytes to `200` when the `INSTANCE_OVERHEAD_MB` property is absent or empty
* *AND* the adapter SHALL use the supplied value when it is in the range `vs-adapter/property-validation` gives the property, persisting the overhead nowhere other than that returned `adapterNotes`
* *AND* any other value SHALL fail the request per `vs-adapter/property-validation`, and the adapter MUST NOT default it to `200`
<!-- /DELTA:CHANGED -->
