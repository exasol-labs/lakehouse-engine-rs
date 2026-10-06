# Feature: Create Virtual Schema — AdapterNotes Resource Configuration

Records the computed resource budgets — parallelism factor, DataFusion threading
mode and per-instance thread/partition allocation, and memory-pool parameters — in
the `createVirtualSchema` response `adapterNotes` so that every per-shard scan UDF
instance receives a correctly sized CPU and memory envelope without the adapter
persisting any state between requests.

<!-- DELTA:CHANGED -->
## Background

* All resource values are resolved at `createVirtualSchema` time from VS/connection
  properties and returned in `adapterNotes` (stringified JSON), which Exasol persists
  and round-trips back at pushdown time.
* The adapter MUST NOT use `schemaMetadata.properties` for this purpose, as Exasol
  2025.2.1 silently drops adapter-returned properties. The `adapterNotes` channel is
  queryable via `SYS.EXA_ALL_VIRTUAL_SCHEMAS.ADAPTER_NOTES`.
* The per-node core count `nr_of_cores` comes from `std::thread::available_parallelism()`
  alone, resolved once per request and defaulting to `1` when the platform cannot report
  it. No VS property overrides it, and it is not itself recorded in `adapterNotes` (see
  `vs-adapter/create-virtual-schema-adapter-notes`). Each derivation below takes the
  resolved count as a plain argument, so a unit test injects an arbitrary count without
  touching the host.
* The parallelism factor is supplied as a VS/connection property and recorded in
  `adapterNotes`; when absent it defaults to a hardware-aware value derived from
  `nr_of_cores`.
* The DataFusion threading configuration is selected by a `DATAFUSION_THREADING_MODE`
  VS/connection property (`AUTO` or `FIXED`, default `AUTO`). In `FIXED` mode the two
  independent properties `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF`
  are used verbatim (each defaulting to `max(nr_of_cores, 1)`); in `AUTO` mode the
  adapter derives a per-instance thread budget that does not oversubscribe a node (see
  `scan-runtime/scan-execution-threading`). Only the resolved integer fields are
  round-tripped into the per-shard scan spec.
* The per-instance memory budget is two independent VS/connection properties —
  `MEMORY_POOL_FRACTION` (default `0.6`) and `INSTANCE_OVERHEAD_MB` (default `200`) —
  each recorded in `adapterNotes` and round-tripped into every per-shard scan spec,
  where the scan UDF sizes its DataFusion pool to
  `fraction × (per_instance_limit − overhead_bytes)`.
* See `vs-adapter/create-virtual-schema-adapter-notes` for how the per-node core count
  is discovered, for why it is not recorded, and for why the cluster node count is
  deliberately NOT recorded here.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter records the DataFusion target partition count in the virtual-schema adapterNotes

* *GIVEN* a `createVirtualSchema` request that may supply a `DATAFUSION_TARGET_PARTITIONS` connection/VS property
* *AND* a per-node core count `nr_of_cores` of at least `1`, resolved from `std::thread::available_parallelism()`
* *WHEN* Exasol sends the `createVirtualSchema` request naming an Iceberg table
* *THEN* the adapter SHALL record the resolved DataFusion target partition count in the `createVirtualSchema` response's `adapterNotes` (stringified JSON) alongside `PARALLELISM_FACTOR` and the threading mode
* *AND* in `FIXED` mode the adapter SHALL use the supplied `DATAFUSION_TARGET_PARTITIONS` value when it is a positive integer and otherwise default to `max(nr_of_cores, 1)`
* *AND* in `AUTO` mode the adapter SHALL set the target partition count equal to the AUTO-derived `df_threads_per_udf` (per `scan-runtime/scan-execution-threading`), ignoring any supplied `DATAFUSION_TARGET_PARTITIONS` value, persisting the count nowhere other than that returned `adapterNotes`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Recorded parallelism factor drives later work-unit sharding

* *GIVEN* a `createVirtualSchema` request whose resolved parallelism factor is P
* *WHEN* the adapter returns the `createVirtualSchema` response
* *THEN* the `adapterNotes` SHALL carry P as the `PARALLELISM_FACTOR` entry and SHALL carry no node-count entry
* *AND* P SHALL be round-tripped back to the adapter at pushdown time, where it is multiplied by the node count the pushdown reads from its own UDF handshake to give the shard count `G`, capped at 300
* *AND* the pushdown SHALL obtain the node-count factor of `G` from `UdfContext::node_count()` rather than from `adapterNotes` (see `pushdown/pushdown-planning`)
<!-- /DELTA:CHANGED -->
