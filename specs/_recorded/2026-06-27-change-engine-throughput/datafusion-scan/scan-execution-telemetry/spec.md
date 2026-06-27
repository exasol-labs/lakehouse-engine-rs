# Feature: DataFusion Scan Execution — On-Demand Phase Telemetry

Adds opt-in, config-gated phase timing to the scan UDF so an operator running a
benchmark can attribute a query's wall-clock to its three coarse phases —
UDF/DataFusion startup, object-storage import (read), and send-back/emit to
Exasol — to localize the throughput bottleneck. Telemetry is silent and
effectively zero-overhead in production: it emits nothing unless explicitly
enabled, and final benchmark measurement runs execute with it OFF.

## Background

* Telemetry reuses the lc-rs 0.19.0 debug surface: the per-UDF-tagged emit
  channel keyed by `LAKEHOUSE_UDF_DEBUG_LEVEL` (default `info`), `udf_log!`, and
  `ctx.debug_level()`. Phase-timing lines are emitted only at `debug` level (or a
  dedicated telemetry flag); at the default level the scan UDF emits no telemetry.
* The phase timing builds on the reusable per-process checkpoint infrastructure
  preserved on `archive/udf-diagnostics-checkpoints`
  (`crates/lakehouse-engine/src/scan/diagnostics.rs`): a process-global monotonic
  sequence, per-PID file isolation so concurrent shard VMs never interleave, and
  best-effort RSS sampling from `/proc/self/statm`. Telemetry adds wall-clock
  phase boundaries (`std::time::Instant`) around the existing checkpoint sites
  rather than designing a new mechanism.
* Exactly three phases are timed, measured with a monotonic clock so they are not
  perturbed by wall-clock adjustment: (a) **startup** — from UDF entry through
  Tokio-runtime build, session-context build, and plan construction, up to the
  first batch fetch; (b) **object-storage import** — cumulative time awaiting
  batches from the DataFusion stream (the S3 read + Parquet decode the stream
  drives); (c) **send-back/emit** — cumulative time inside `emit_batch` / `emit`
  flushes returning batches to Exasol. The three SHALL sum to the scan body's
  wall-clock within measurement error.
* Telemetry is best-effort and MUST NOT alter scan results: a timing or logging
  failure never fails the query, and the timing instrumentation MUST NOT change
  the streaming discipline (one batch fetched, emitted, dropped before the next).
* The object-storage-import timing is the lever that later reveals S3 travel cost
  when an in-VPC S3 is introduced (a separate future plan); this feature only
  provides the measurement surface.
* See `datafusion-scan/scan-execution` for the core scan and streaming scenarios
  and the project CLAUDE.md "Live debugging" notes for the lc-rs debug surface.

## Scenarios

### Scenario: Phase telemetry is silent at the default debug level

* *GIVEN* a scan UDF invocation whose effective debug level is the production default (`info`, i.e. telemetry not explicitly enabled)
* *WHEN* the scan UDF runs a query to completion
* *THEN* the UDF SHALL NOT emit any phase-timing telemetry line
* *AND* the query result SHALL be byte-for-byte identical to the result produced with the telemetry code absent
* *AND* the streaming discipline (fetch one batch, emit it, drop it before the next) SHALL be unchanged

### Scenario: Enabling telemetry emits the three phase timings for a completed scan

* *GIVEN* a scan UDF invocation whose debug level is set to `debug` (telemetry enabled) via the configured channel (`LAKEHOUSE_UDF_DEBUG_LEVEL` / `ctx.debug_level()`)
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
