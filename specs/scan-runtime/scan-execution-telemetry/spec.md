# Feature: DataFusion Scan Execution, On-Demand Phase Telemetry

The scan UDF can report phase timings so an operator running a benchmark can attribute a query's wall-clock to startup, object-storage import, and send-back to Exasol. Telemetry emits nothing unless enabled.

## Background

* Telemetry uses the per-UDF-tagged debug emit channel, `udf_log!`, and `ctx.debug_level()`, which returns the level that the `%udf_debug_level` directive in the scan script source sets (default `info`). Phase-timing lines appear only at `debug` level.
* Timing builds on the checkpoint infrastructure in the scan diagnostics module: a process-global monotonic sequence, per-PID file isolation so concurrent shard VMs never interleave, and best-effort RSS sampling from `/proc/self/statm`.
* Three phases are timed with a monotonic clock. Startup runs from UDF entry through Tokio-runtime build, session-context build, and plan construction, up to the first batch fetch. Object-storage import is the cumulative time awaiting batches from the DataFusion stream (the S3 read and Parquet decode). Send-back/emit is the cumulative time inside `emit_batch` and `emit` flushes. The three sum to the scan body's wall-clock within measurement error.
* Telemetry never alters scan results. A timing or logging failure does not fail the query, and the instrumentation does not change the streaming discipline: one batch fetched, emitted, and dropped before the next.

## Scenarios

### Scenario: Phase telemetry is silent at the default debug level

* *GIVEN* a scan UDF invocation whose effective debug level is the production default (`info`, i.e. telemetry not explicitly enabled)
* *WHEN* the scan UDF runs a query to completion
* *THEN* the UDF SHALL NOT emit any phase-timing telemetry line
* *AND* the query result SHALL be byte-for-byte identical to the result produced with the telemetry code absent
* *AND* the streaming discipline (fetch one batch, emit it, drop it before the next) SHALL be unchanged

### Scenario: Enabling telemetry emits the three phase timings for a completed scan

* *GIVEN* a scan UDF invocation whose debug level is `debug` (telemetry enabled), set by the `%udf_debug_level` directive in the scan script source and read through `ctx.debug_level()`
* *WHEN* the scan UDF runs a query to completion
* *THEN* the UDF SHALL emit one telemetry record reporting the startup phase duration, the cumulative object-storage import duration, and the cumulative send-back/emit duration, each as a monotonic-clock measurement
* *AND* each record SHALL carry the per-VM identity already provided by the debug surface (pid / node / session / vm) so per-shard timings are attributable
* *AND* the three reported phase durations SHALL account for the scan body wall-clock within measurement error (no phase silently omitted)

### Scenario: Telemetry attributes object-storage import time separately from emit time

* *GIVEN* a scan UDF invocation with telemetry enabled whose result spans multiple Arrow record batches
* *WHEN* the scan UDF streams and emits the batches
* *THEN* the UDF SHALL accumulate the time spent awaiting each batch from the DataFusion stream into the object-storage import phase
* *AND* the UDF SHALL accumulate the time spent inside the emit/flush calls returning each batch to Exasol into the send-back/emit phase
* *AND* the two accumulators SHALL be reported as distinct durations so a benchmark can tell a read-bound scan from an emit-bound scan

### Scenario: A telemetry failure never fails the scan

* *GIVEN* a scan UDF invocation with telemetry enabled
* *AND* the telemetry sink (the debug emit channel or the per-process file) cannot be written
* *WHEN* the scan UDF runs the query
* *THEN* the UDF SHALL complete the scan and return its result unaffected
* *AND* the UDF MUST NOT surface the telemetry-write failure as a query error
