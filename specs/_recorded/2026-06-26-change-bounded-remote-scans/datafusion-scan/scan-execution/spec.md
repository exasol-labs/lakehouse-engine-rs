# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion
`RuntimeEnv` memory pool from the per-instance memory limit reported in UDF metadata,
applies the pushed-down projection, filter, and LIMIT, and either streams the matching
rows back or — when the spec carries aggregate instructions — emits one node-local
partial-aggregate row per distinct group (or a single row for ungrouped aggregates).
It holds no state and discovers no files of its own.

## Background

* The scan UDF reads its ScanSpec from a single JSON VARCHAR input column.
* The UDF MUST register only its assigned files and MUST NOT discover additional files.
* On the raw-row path the UDF emits each Arrow `RecordBatch` via the SDK's Arrow-IPC
  emit path (`EmitBatch`, behind the `emit-arrow` feature), which serializes the batch
  to Arrow IPC bytes internally — only IPC bytes cross the `.so` boundary, never typed
  Arrow objects, and no `Vec<Value>` intermediate is built per batch.
* DataFusion execution is bounded; a memory bound that cannot spill MUST surface as a
  clean error, never an OOM VM crash.
* Credentials MUST NOT appear in any error message.
* See `datafusion-scan/scan-execution-memory-and-credentials` for pool sizing and
  decode-bound scenarios.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Arrow batches are emitted incrementally as Arrow IPC and never double-materialized

* *GIVEN* a scan whose result spans multiple Arrow record batches
* *WHEN* the scan UDF processes the result stream
* *THEN* the UDF SHALL emit each batch via the SDK's Arrow-batch emit path (the `EmitBatch` API, gated by the `emit-arrow` feature), serializing the batch to Arrow IPC bytes so only IPC bytes cross the `.so` boundary
* *AND* the UDF SHALL fetch one batch, emit it, and drop it before fetching the next, never materializing the entire result set
* *AND* the UDF MUST NOT build an intermediate `Vec<Value>` row collection on the raw-row scan path, and no typed Arrow value SHALL cross the `.so` boundary — only the serialized IPC byte buffer
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Scan surfaces a clean memory-exhaustion error instead of crashing the VM

* *GIVEN* a scan whose execution exhausts the configured DataFusion memory pool (a `ResourcesExhausted` condition) on a node whose `/tmp` is not spill-capable disk
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL surface a clean error that identifies memory/resource exhaustion as the cause, and MUST NOT crash the UDF VM
* *AND* the error-redaction path MUST NOT reclassify a `ResourcesExhausted` condition as an "assigned data could not be read" storage error
* *AND* the surfaced error message MUST NOT contain any storage access key, secret key, or session token
<!-- /DELTA:NEW -->
