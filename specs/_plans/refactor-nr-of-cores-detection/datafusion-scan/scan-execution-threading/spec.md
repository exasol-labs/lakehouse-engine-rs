# Feature: DataFusion Scan Execution — Threading Configuration

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/datafusion-scan/scan-execution-threading/spec.md`.

<!-- DELTA:CHANGED -->
## Background

* The ScanSpec carries two CPU-bounding fields, `df_target_partitions` and
  `df_threads_per_udf`, both defaulting to `1` when absent from the JSON (so
  pre-existing scan specs remain backward-compatible). They are resolved in the
  adapter from the `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF`
  adapterNotes and round-tripped through the spec.
* Exasol runs multiple concurrent UDF instances per node, bounded by that node's
  VM pool, which Exasol sizes to the node's own core count. DataFusion's
  `SessionConfig::new()` otherwise defaults `target_partitions` to the host core
  count, so without an explicit setting each instance would spawn core-count
  partitions and the node would be oversubscribed by `(instances × cores)`. Setting
  both fields to `1` by default makes each instance use exactly one core; the
  cluster-level shard fan-out provides the parallelism.
* The ScanSpec fields are resolved before the Tokio runtime is constructed, so the
  runtime kind is chosen from the spec value.
* Under SDK-0.21.0 per-row scalar dispatch the scan `run()` is invoked once per input row,
  so runtime construction happens per call rather than once per batch; see
  `datafusion-scan/scan-execution` for the per-row dispatch contract.
* The thread/partition budget is selected by a `DATAFUSION_THREADING_MODE`
  VS/connection property with two values, `AUTO` and `FIXED`, resolved in the
  adapter at `createVirtualSchema` time and recorded in `adapterNotes`. The mode
  is a planning-time concept: it determines how `df_target_partitions` and
  `df_threads_per_udf` are computed, and only the resulting integer fields ever
  reach the scan UDF — the UDF stays mode-agnostic.
* The per-node core count `nr_of_cores` that both modes read comes from
  `std::thread::available_parallelism()` on the adapter's executing node, resolved
  once per request and defaulting to `1` when the platform cannot report it. It is
  not recorded in `adapterNotes`; see
  `vs-adapter/create-virtual-schema-adapter-notes`.
* In AUTO mode the adapter derives a per-instance thread budget that does not
  oversubscribe a node:
  `threads_per_udf = max(1, floor(nr_of_cores / udf_instances_per_node))`, where
  `udf_instances_per_node` is the per-node share of the oversubscribed work-unit
  shard count (`G = node_count × parallelism_factor`, capped 300; see
  `parallelism/work-unit-sharding`). `df_target_partitions` is held in lockstep
  with the derived `df_threads_per_udf` so partition count never exceeds the
  thread budget.
* In FIXED mode the adapter uses the operator-supplied `DATAFUSION_TARGET_PARTITIONS`
  / `DATAFUSION_THREADS_PER_UDF` values verbatim, each defaulting to
  `max(nr_of_cores, 1)` when absent.
* The current production default of one thread / one partition per instance has
  NEVER been measured against a multi-thread / multi-partition configuration;
  whether single-thread per instance is a throughput bottleneck is an open
  empirical question answered by benchmark sweeps, not by this spec. This spec
  only guarantees the configuration is selectable and correctly derived.
* See `datafusion-scan/scan-execution` for the core scan scenarios and
  `vs-adapter/create-virtual-schema-adapter-notes` for how the threading mode and
  properties are recorded in `adapterNotes`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: AUTO mode derives a per-instance thread budget that does not oversubscribe a node

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `AUTO`
* *AND* a resolved per-node core count `nr_of_cores` of at least `1` and a per-node UDF-instance share derived from the work-unit shard fan-out
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL compute `df_threads_per_udf` as `max(1, floor(nr_of_cores / udf_instances_per_node))` so that `(udf_instances_per_node × df_threads_per_udf)` does not exceed `nr_of_cores`
* *AND* the adapter SHALL set `df_target_partitions` equal to the derived `df_threads_per_udf` so the partition count never exceeds the per-instance thread budget
* *AND* the adapter SHALL record the resolved values and the selected mode in the `createVirtualSchema` response `adapterNotes`, so the per-shard scan spec carries integer fields the mode-agnostic scan UDF consumes unchanged
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: AUTO mode falls back to a single thread when the core count is unknown

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `AUTO`
* *AND* a resolved `NR_OF_CORES` of `0` (the unknown / unavailable sentinel)
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL set both `df_threads_per_udf` and `df_target_partitions` to `1`, preserving the prior single-threaded per-instance behaviour
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: AUTO mode yields a single thread on a one-core node

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `AUTO`
* *AND* a resolved per-node core count `nr_of_cores` of `1`, which is both a genuine single-core node and the value the adapter uses when `std::thread::available_parallelism()` cannot report a count
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL set both `df_threads_per_udf` and `df_target_partitions` to `1`, through the ordinary `max(1, floor(nr_of_cores / udf_instances_per_node))` formula
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: FIXED mode uses the operator-supplied thread and partition values verbatim

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `FIXED`
* *AND* explicit positive-integer `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` properties
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL record `df_target_partitions` and `df_threads_per_udf` equal to the supplied property values, without applying the AUTO derivation
* *AND* when a property is absent or not a positive integer the adapter SHALL fall back to `max(nr_of_cores, 1)` for that field
<!-- /DELTA:CHANGED -->
