# Feature: DataFusion Scan Execution — Threading Configuration

Controls how many DataFusion partitions and Tokio worker threads each scan UDF
instance uses, so that the product of (per-node concurrent UDF instances) ×
(per-instance threads) does not oversubscribe a node's cores. Configuration is
round-tripped from `adapterNotes` through `ScanSpec` to the scan UDF, and is
selected by an explicit AUTO-vs-FIXED mode so an operator can either let the
adapter derive a non-oversubscribing thread/partition budget per UDF instance
or pin exact values.

## Background

* The ScanSpec carries two CPU-bounding fields, `df_target_partitions` and
  `df_threads_per_udf`, both defaulting to `1` when absent from the JSON (so
  pre-existing scan specs remain backward-compatible). They are resolved in the
  adapter from the `DATAFUSION_TARGET_PARTITIONS` / `DATAFUSION_THREADS_PER_UDF`
  adapterNotes and round-tripped through the spec.
* Exasol runs multiple concurrent UDF instances per node (bounded by that node's
  `NR_OF_CORES`-sized VM pool). DataFusion's `SessionConfig::new()` otherwise
  defaults `target_partitions` to the host core count, so without an explicit
  setting each instance would spawn core-count partitions and the node would be
  oversubscribed by `(instances × cores)`. Setting both fields to `1` by default
  makes each instance use exactly one core; the cluster-level shard fan-out provides
  the parallelism.
* The ScanSpec fields are resolved before the Tokio runtime is constructed, so the
  runtime kind is chosen from the spec value.
* The thread/partition budget is selected by a `DATAFUSION_THREADING_MODE`
  VS/connection property with two values, `AUTO` and `FIXED`, resolved in the
  adapter at `createVirtualSchema` time and recorded in `adapterNotes`. The mode
  is a planning-time concept: it determines how `df_target_partitions` and
  `df_threads_per_udf` are computed, and only the resulting integer fields ever
  reach the scan UDF — the UDF stays mode-agnostic.
* In AUTO mode the adapter derives a per-instance thread budget that does not
  oversubscribe a node:
  `threads_per_udf = max(1, floor(NR_OF_CORES / udf_instances_per_node))`, where
  `udf_instances_per_node` is the per-node share of the oversubscribed work-unit
  shard count (`G = node_count × parallelism_factor`, capped 300; see
  `parallelism/work-unit-sharding`). `df_target_partitions` is held in lockstep
  with the derived `df_threads_per_udf` so partition count never exceeds the
  thread budget.
* In FIXED mode the adapter uses the operator-supplied `DATAFUSION_TARGET_PARTITIONS`
  / `DATAFUSION_THREADS_PER_UDF` values verbatim (the prior behaviour), each
  defaulting to `max(NR_OF_CORES, 1)` when absent.
* The current production default of one thread / one partition per instance has
  NEVER been measured against a multi-thread / multi-partition configuration;
  whether single-thread per instance is a throughput bottleneck is an open
  empirical question answered by benchmark sweeps, not by this spec. This spec
  only guarantees the configuration is selectable and correctly derived.
* See `datafusion-scan/scan-execution` for the core scan scenarios and
  `vs-adapter/create-virtual-schema-adapter-notes` for how the threading mode and
  properties are recorded in `adapterNotes`.

## Scenarios

### Scenario: Scan applies the explicitly-configured DataFusion target partition count

* *GIVEN* a scan spec whose `df_target_partitions` field is set to a positive integer N (default `1` when the field is absent from the spec JSON)
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL configure the DataFusion `SessionConfig` with `target_partitions` equal to N rather than the host core count
* *AND* when the spec JSON omits the field the UDF SHALL use a target partition count of `1`

### Scenario: Scan builds a single-threaded Tokio runtime when threads-per-UDF is 1

* *GIVEN* a scan spec whose `df_threads_per_udf` field is `1` (the default when absent from the spec JSON)
* *WHEN* the scan UDF constructs the Tokio runtime that drives the async DataFusion scan
* *THEN* the UDF SHALL build a current-thread Tokio runtime so the instance uses a single OS worker thread

### Scenario: Scan builds a multi-threaded Tokio runtime when threads-per-UDF exceeds 1

* *GIVEN* a scan spec whose `df_threads_per_udf` field is an integer M greater than 1
* *WHEN* the scan UDF constructs the Tokio runtime that drives the async DataFusion scan
* *THEN* the UDF SHALL build a multi-threaded Tokio runtime configured with M worker threads
* *AND* the UDF SHALL parse `df_threads_per_udf` from the ScanSpec before it constructs the runtime, so the runtime kind is chosen from the spec value

<!-- DELTA:NEW -->
### Scenario: AUTO mode derives a per-instance thread budget that does not oversubscribe a node

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `AUTO`
* *AND* a resolved per-node core count `NR_OF_CORES` greater than `0` and a per-node UDF-instance share derived from the work-unit shard fan-out
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL compute `df_threads_per_udf` as `max(1, floor(NR_OF_CORES / udf_instances_per_node))` so that `(udf_instances_per_node × df_threads_per_udf)` does not exceed `NR_OF_CORES`
* *AND* the adapter SHALL set `df_target_partitions` equal to the derived `df_threads_per_udf` so the partition count never exceeds the per-instance thread budget
* *AND* the adapter SHALL record the resolved values and the selected mode in the `createVirtualSchema` response `adapterNotes`, so the per-shard scan spec carries integer fields the mode-agnostic scan UDF consumes unchanged
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: AUTO mode falls back to a single thread when the core count is unknown

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `AUTO`
* *AND* a resolved `NR_OF_CORES` of `0` (the unknown / unavailable sentinel)
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL set both `df_threads_per_udf` and `df_target_partitions` to `1`, preserving the prior single-threaded per-instance behaviour
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: FIXED mode uses the operator-supplied thread and partition values verbatim

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `FIXED`
* *AND* explicit positive-integer `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` properties
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL record `df_target_partitions` and `df_threads_per_udf` equal to the supplied property values, without applying the AUTO derivation
* *AND* when a property is absent or not a positive integer the adapter SHALL fall back to `max(NR_OF_CORES, 1)` for that field, exactly as the pre-mode behaviour did
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Threading mode defaults to AUTO when the property is absent

* *GIVEN* a `createVirtualSchema` request that supplies no `DATAFUSION_THREADING_MODE` property, or supplies a value that is neither `AUTO` nor `FIXED`
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL select `AUTO` mode and derive the thread/partition budget per the AUTO scenario
* *AND* the adapter SHALL record `DATAFUSION_THREADING_MODE: AUTO` in the `createVirtualSchema` response `adapterNotes`
<!-- /DELTA:NEW -->
