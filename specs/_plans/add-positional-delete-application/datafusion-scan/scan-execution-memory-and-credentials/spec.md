# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to size the DataFusion memory pool from the per-instance memory limit, bound
the Parquet decode working set via a configured batch size, enable Parquet row-group and page
pruning, and consume storage credentials carried in the scan spec without re-authenticating to the
catalog. This delta extends that credential and Parquet-configuration surface to the reading of
associated positional-delete files.

## Background

* Storage credentials (including vended STS tokens) travel once in the shard-invariant common spec
  and configure a single S3 object store reused for both data files and their delete files.
* No credential value may appear in any error message the UDF returns.
* Delete-carrying data files need their Parquet footer both for access-plan construction and by the
  opener; a shared reader factory / cached metadata reader avoids parsing the footer twice.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Positional-delete files are read with the same vended credentials

* *GIVEN* a scan invocation whose shard-invariant common spec carries a storage block with vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF reads a data file's associated positional-delete files from object storage
* *THEN* the UDF SHALL read the delete files through the SAME S3 object store configured from those common-spec credentials, reusing the object store built for the data files
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials to read a delete file
* *AND* a credential value MUST NOT appear in any error message the UDF returns while reading a delete file
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A shared Parquet metadata reader avoids a duplicate footer parse

* *GIVEN* a data file that carries positional deletes, whose Parquet footer is needed both to build the base `ParquetAccessPlan` and by the Parquet opener
* *WHEN* the scan UDF configures the `ParquetSource` for its assigned files
* *THEN* the UDF SHOULD install a `ParquetFileReaderFactory` (or an equivalent cached metadata reader) so the data file's footer metadata parsed for access-plan construction is reused by the opener rather than parsed a second time
* *AND* if no shared reader is installed, the UDF MAY accept one additional footer range GET per delete-carrying data file, but MUST NOT issue a HEAD request in either case
* *AND* the configured batch size and Parquet row-group / page pruning SHALL apply unchanged whether or not a shared reader is installed
<!-- /DELTA:NEW -->
