# Feature: Scan Module Structure

Decomposes the DataFusion scan-execution code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

<!-- DELTA:NEW -->
* `exasol-udf-sdk` 0.24.0 adds test doubles behind a `test-support` feature. The SDK double does NOT override `emit_record_batch_ipc` (trait default returns `Unimplemented`), so scan doubles that capture Arrow IPC emissions need a local wrapper.
* The `test-support` feature MUST be enabled only as a dev-dependency so it stays out of the production `.so`.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: One shared double owns every scan test's Arrow IPC capture

* *GIVEN* the scan integration tests under `crates/lakehouse-engine/tests/` that each hand-roll a `UdfContext` double with an identical `emit_record_batch_ipc` decode loop
* *WHEN* the scan integration-test suite compiles and runs
* *THEN* exactly one double in `tests/scan_fixture/` SHALL own the decode and delegate every other `UdfContext` method to the SDK double
* *AND* the shared double SHALL retain per-call capture grouping so payload count stays distinguishable from batch count
* *AND* no file under `crates/lakehouse-engine/tests/` SHALL declare its own `impl UdfContext` (bare or fully-qualified form)
* *AND* every existing scan integration test MUST pass with no change to any assertion or expected value
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Two in-crate doubles are retained for reachability

* *GIVEN* `src/scan/emit_tests.rs` `CapturingCtx` and `src/scan/test_support_tests.rs` `SinkCtx`
* *WHEN* the shared double lands in `tests/scan_fixture/`
* *THEN* both SHALL be retained: `tests/` modules are not nameable from `src/`, and neither duplicates the decode this delta consolidates
<!-- /DELTA:NEW -->
