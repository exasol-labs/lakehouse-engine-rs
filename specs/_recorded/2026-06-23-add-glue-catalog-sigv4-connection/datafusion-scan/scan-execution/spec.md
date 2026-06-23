# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion memory
pool from the per-instance memory limit reported in UDF metadata, applies the pushed-down
projection, filter, and LIMIT, and either streams the matching rows back or — when the
spec carries aggregate instructions — emits one node-local partial-aggregate row per
distinct group (or a single row for ungrouped aggregates). It holds no state and discovers
no files of its own.

## Background

* The scan UDF reads its ScanSpec from a single JSON VARCHAR input column and registers
  only its assigned files; it discovers no files of its own.
* The per-instance memory limit is read from `ctx.memory_limit()`; `0` means unknown.
* Storage credentials reach the UDF only inside the ScanSpec; the UDF never re-authenticates.
* Only SDK Value types cross the .so boundary; no Arrow types.
* Credentials MUST NOT appear in any error message.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Scan sizes its memory pool from the reported per-instance limit

* *GIVEN* a scan UDF invocation whose UDF context reports a positive per-instance memory limit via `ctx.memory_limit()`
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL read the per-instance limit from `ctx.memory_limit()` and size the DataFusion memory pool to a fraction (≈0.6) of that limit
* *AND* the UDF MUST NOT hardcode the pool budget to the unknown-limit default when a positive limit is reported

<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Scan falls back to the default budget when no memory limit is reported

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` returns `0` (the unknown / unavailable sentinel)
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL size the DataFusion memory pool to the conservative default budget
* *AND* the scan SHALL otherwise execute identically to the positive-limit path

<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Scan reads data files with vended credentials carried in the scan spec

* *GIVEN* a scan spec whose storage block carries vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads its assigned files
* *THEN* the UDF SHALL configure its S3 object store from the credentials in the scan spec
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials
* *AND* a credential value MUST NOT appear in any error message the UDF returns

<!-- /DELTA:NEW -->
