# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from `ctx.memory_limit()` and size the DataFusion memory pool from a *net* budget — the per-instance limit minus a configurable container/binary overhead — scaled by a configurable fraction, to bound the per-batch Parquet decode working set via a configured `batch_size`, to enable Parquet row-group and page pruning so the scan reads only the byte ranges its predicate needs, and to consume storage credentials carried in the scan spec (including vended STS tokens) without re-authenticating to the catalog. The credentials and tuning knobs travel in the shard-invariant common spec argument, serialized once for the whole fan-out.

## Background

<!-- DELTA:NEW -->
* This delta amends ONE clause each in the two credential-passthrough scenarios and nothing else. `vs-adapter/storage-backend-enum` (issue #274) makes the common blob's storage block a backend value rather than a bare S3 props object, so the clauses that say the UDF configures "its S3 object store" from those credentials are restated as the UDF registering the object store the carried backend names. Every other scenario of this feature is unchanged, and no Background bullet is superseded.
* Nothing about the memory pool, the batch size, the Parquet pruning flags, or the shared metadata reader is affected: the backend value replaces the storage value the object-store construction already read, and the S3 store it builds is byte-identical.
* The one-store-per-side rule is unchanged and still owned here: data files and their positional-delete files are read through the SAME registered store. `vs-adapter/storage-backend-enum` owns how that store is derived and registered without the scan path naming a backend.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reads data files with vended credentials carried in the scan spec

* *GIVEN* a scan invocation whose shard-invariant common spec argument carries a storage backend holding vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads the files listed in its per-shard argument
* *THEN* the UDF SHALL register the object store the carried storage backend names, configured from the credentials that backend holds
* *AND* the UDF MUST NOT decide the storage backend itself, derive it from a file URI scheme, or read the backend's payload outside the single backend-dispatching registration function specified by `vs-adapter/storage-backend-enum`
* *AND* the storage credentials SHALL travel in the shard-invariant common spec argument (serialized once for the whole fan-out), NOT be repeated per shard
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials
* *AND* a credential value MUST NOT appear in any error message the UDF returns
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Positional-delete files are read with the same vended credentials

* *GIVEN* a scan invocation whose shard-invariant common spec carries a storage backend holding vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF reads a data file's associated positional-delete files from object storage
* *THEN* the UDF SHALL read the delete files through the SAME registered object store used for the data files, configured from that backend's credentials
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials to read a delete file
* *AND* a credential value MUST NOT appear in any error message the UDF returns while reading a delete file
<!-- /DELTA:CHANGED -->
