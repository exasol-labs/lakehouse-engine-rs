# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from `ctx.memory_limit()` and size the DataFusion memory pool from a net budget, to bound the per-batch Parquet decode working set via a configured `batch_size`, to enable Parquet row-group and page pruning, and to consume storage credentials carried in the scan spec (including vended STS tokens) without re-authenticating to the catalog. The credentials and tuning knobs travel in the shard-invariant common spec argument, serialized once for the whole fan-out.

## Background

* Storage credentials (including vended S3 keys) reach the UDF only inside the shard-invariant common spec argument, serialized once for the whole fan-out rather than repeated per shard; the UDF never contacts the catalog or re-requests credentials.
* Credentials MUST NOT appear in any error message.
* See `datafusion-scan/scan-execution` for the base two-argument scan execution scenarios.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reads data files with vended credentials carried in the scan spec

* *GIVEN* a scan invocation whose shard-invariant common spec argument carries a storage block with vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads the files listed in its per-shard argument
* *THEN* the UDF SHALL configure its S3 object store from the credentials in the common spec argument
* *AND* the storage credentials SHALL travel in the shard-invariant common spec argument (serialized once for the whole fan-out), NOT be repeated per shard
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials
* *AND* a credential value MUST NOT appear in any error message the UDF returns
<!-- /DELTA:CHANGED -->
