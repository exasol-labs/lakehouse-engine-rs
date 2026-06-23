# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from
`ctx.memory_limit()` and size the DataFusion memory pool from it (replacing the
hardcoded 0-sentinel), and to consume storage credentials carried in the scan spec
(including vended STS tokens) without re-authenticating to the catalog.

## Background

* The per-instance memory limit is read from `ctx.memory_limit()` (bytes; `0` =
  unknown sentinel); the pool is sized to ≈0.6× the limit, leaving headroom below
  the Exasol engine's 80% concurrency-stall threshold.
* Storage credentials (including vended S3 keys) reach the UDF only inside the
  ScanSpec; the UDF never contacts the catalog or re-requests credentials.
* Credentials MUST NOT appear in any error message.
* See `datafusion-scan/scan-execution` for the base scan execution scenarios.

## Scenarios

### Scenario: Scan sizes its memory pool from the reported per-instance limit

* *GIVEN* a scan UDF invocation whose UDF context reports a positive per-instance memory limit via `ctx.memory_limit()`
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL read the per-instance limit from `ctx.memory_limit()` and size the DataFusion memory pool to a fraction (≈0.6) of that limit
* *AND* the UDF MUST NOT hardcode the pool budget to the unknown-limit default when a positive limit is reported

### Scenario: Scan falls back to the default budget when no memory limit is reported

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` returns `0` (the unknown / unavailable sentinel)
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL size the DataFusion memory pool to the conservative default budget
* *AND* the scan SHALL otherwise execute identically to the positive-limit path

### Scenario: Scan reads data files with vended credentials carried in the scan spec

* *GIVEN* a scan spec whose storage block carries vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads its assigned files
* *THEN* the UDF SHALL configure its S3 object store from the credentials in the scan spec
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials
* *AND* a credential value MUST NOT appear in any error message the UDF returns
