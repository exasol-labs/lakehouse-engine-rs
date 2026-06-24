# Feature: DataFusion Scan Execution — Threading Configuration

Controls how many DataFusion partitions and Tokio worker threads each scan UDF
instance uses, so that the product of (per-node concurrent UDF instances) ×
(per-instance threads) does not oversubscribe a node's cores. Configuration is
round-tripped from `adapterNotes` through `ScanSpec` to the scan UDF.

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
* See `datafusion-scan/scan-execution` for the core scan scenarios and
  `vs-adapter/create-virtual-schema` for how the threading properties are recorded
  in `adapterNotes`.

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
