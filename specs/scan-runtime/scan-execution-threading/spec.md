# Feature: DataFusion Scan Execution, Threading Configuration

The adapter sets how many DataFusion partitions and Tokio worker threads each scan UDF instance uses. Exasol runs one scan instance per node, so the AUTO budget gives that instance every core. An AUTO or FIXED mode selects whether the adapter derives the budget or uses operator-supplied values.

## Background

* `ScanSpec` carries `df_target_partitions` and `df_threads_per_udf`, both defaulting to `1` when absent from the JSON. The adapter resolves them from the `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` adapterNotes and passes them through the spec.
* Exasol runs one scalar `LAKEHOUSE_SCAN` VM per node, which scans that node's shards one after another (see `specs/udf-context.md`). Per-node CPU parallelism therefore comes only from the DataFusion threads inside that instance.
* The scan UDF reads `df_threads_per_udf` from the spec before it builds the Tokio runtime, so the spec value chooses the runtime kind.
* The SDK calls the scan `run()` once per input row, so the runtime is built per call; see `datafusion-scan/scan-execution`.
* The `DATAFUSION_THREADING_MODE` VS/connection property takes `AUTO` or `FIXED`. The adapter resolves it at `createVirtualSchema` time and records it in `adapterNotes`. Only the resulting integer fields reach the scan UDF, which does not know the mode.
* `nr_of_cores` comes from `std::thread::available_parallelism()` on the adapter's executing node, resolved once per request and defaulting to `1` when the platform cannot report it. It is not recorded in `adapterNotes`; see `vs-properties/create-virtual-schema-adapter-notes`.
* In AUTO mode, `df_threads_per_udf = nr_of_cores` and `df_target_partitions = 2 × nr_of_cores`, independent of the parallelism factor. DataFusion splits the scan's file group into byte ranges up to `target_partitions`, so partitions beyond threads overlap object-store reads with decoding.
* In FIXED mode the adapter uses the supplied `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` values as given, each falling back to its AUTO value when absent.

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

### Scenario: AUTO mode gives the node's single scan instance every core

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `AUTO`
* *AND* a resolved per-node core count `nr_of_cores` of at least `1`
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL set `df_threads_per_udf` to `nr_of_cores` and `df_target_partitions` to `2 × nr_of_cores`, whatever the parallelism factor
* *AND* the adapter SHALL ignore a `DATAFUSION_TARGET_PARTITIONS` or `DATAFUSION_THREADS_PER_UDF` value that `vs-properties/property-value-validation` accepts, and a value it rejects SHALL fail the request
* *AND* the adapter SHALL record the resolved values and the selected mode in the `createVirtualSchema` response `adapterNotes`, so the per-shard scan spec carries integer fields the mode-agnostic scan UDF consumes unchanged

### Scenario: AUTO mode yields a single thread on a one-core node

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `AUTO`
* *AND* a resolved per-node core count `nr_of_cores` of `1`, which is both a genuine single-core node and the value the adapter uses when `std::thread::available_parallelism()` cannot report a count
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL set `df_threads_per_udf` to `1` and `df_target_partitions` to `2`, through the ordinary AUTO formula
* *AND* the adapter SHALL still return a successful `createVirtualSchema` response

### Scenario: FIXED mode uses the operator-supplied thread and partition values verbatim

* *GIVEN* a `createVirtualSchema` request whose `DATAFUSION_THREADING_MODE` property is `FIXED`
* *AND* explicit `DATAFUSION_TARGET_PARTITIONS` and `DATAFUSION_THREADS_PER_UDF` properties whose values `vs-properties/property-value-validation` accepts
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL record `df_target_partitions` and `df_threads_per_udf` equal to the supplied property values, without applying the AUTO derivation
* *AND* when a property is absent or empty the adapter SHALL fall back to that field's AUTO value
* *AND* a value that `vs-properties/property-value-validation` rejects SHALL fail the request, and the adapter MUST NOT fall back to the AUTO value for it

### Scenario: Threading mode defaults to AUTO when the property is absent

* *GIVEN* a `createVirtualSchema` request that supplies no `DATAFUSION_THREADING_MODE` property, or an empty one
* *WHEN* the adapter resolves the DataFusion threading configuration
* *THEN* the adapter SHALL select `AUTO` mode and derive the thread/partition budget per the AUTO scenario
* *AND* the adapter SHALL record the `adapterNotes` entry `DF_THREADING_MODE` with the value `AUTO` in the `createVirtualSchema` response
* *AND* a value that is neither `AUTO` nor `FIXED`, compared without regard to case, SHALL fail the request per `vs-properties/property-value-validation` instead of selecting `AUTO`

### Scenario: Scan constructs the Tokio runtime per scalar run() call and never caches it across calls

* *GIVEN* a scan spec whose `df_threads_per_udf` value arrives as a per-call UDF input parameter, carried in the `ScanSpec` reconstituted from that `run()` call's two VARCHAR arguments
* *WHEN* the scan UDF builds the Tokio runtime that drives one `run()` call's DataFusion scan
* *THEN* the UDF SHALL construct a fresh runtime sized from THAT call's `df_threads_per_udf` (current-thread when `1`, multi-thread with that many workers otherwise) and tear it down deterministically via `run_on_runtime` before the call returns
* *AND* the UDF MUST NOT reuse a runtime cached at process or `static` scope (for example via `OnceLock`) across `run()` calls
* *AND* because Exasol pools and reuses a UDF VM process across invocations that MAY belong to different queries carrying different `df_threads_per_udf` values, a process-cached runtime would apply stale sizing, so a resource whose construction depends on a per-call `UdfContext` input parameter MUST be rebuilt every call, preserving the stateless, no-cross-call-state UDF invariant
