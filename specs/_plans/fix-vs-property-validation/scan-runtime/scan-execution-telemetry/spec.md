# Feature: DataFusion Scan Execution, On-Demand Phase Telemetry

The scan UDF can report phase timings so an operator running a benchmark can attribute a query's wall-clock to startup, object-storage import, and send-back to Exasol. Telemetry emits nothing unless enabled.

<!-- DELTA:CHANGED -->
## Background

* Telemetry uses the per-UDF-tagged debug emit channel, `udf_log!`, and `ctx.debug_level()`, which returns the level that the `%udf_debug_level` directive in the scan script source sets (default `info`). Phase-timing lines appear only at `debug` level.
* Timing builds on the checkpoint infrastructure in the scan diagnostics module: a process-global monotonic sequence, per-PID file isolation so concurrent shard VMs never interleave, and best-effort RSS sampling from `/proc/self/statm`.
* Three phases are timed with a monotonic clock. Startup runs from UDF entry through Tokio-runtime build, session-context build, and plan construction, up to the first batch fetch. Object-storage import is the cumulative time awaiting batches from the DataFusion stream (the S3 read and Parquet decode). Send-back/emit is the cumulative time inside `emit_batch` and `emit` flushes. The three sum to the scan body's wall-clock within measurement error.
* Telemetry never alters scan results. A timing or logging failure does not fail the query, and the instrumentation does not change the streaming discipline: one batch fetched, emitted, and dropped before the next.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Enabling telemetry emits the three phase timings for a completed scan

* *GIVEN* a scan UDF invocation whose debug level is `debug` (telemetry enabled), set by the `%udf_debug_level` directive in the scan script source and read through `ctx.debug_level()`
* *WHEN* the scan UDF runs a query to completion
* *THEN* the UDF SHALL emit one telemetry record reporting the startup phase duration, the cumulative object-storage import duration, and the cumulative send-back/emit duration, each as a monotonic-clock measurement
* *AND* each record SHALL carry the per-VM identity already provided by the debug surface (pid / node / session / vm) so per-shard timings are attributable
* *AND* the three reported phase durations SHALL account for the scan body wall-clock within measurement error (no phase silently omitted)
<!-- /DELTA:CHANGED -->
