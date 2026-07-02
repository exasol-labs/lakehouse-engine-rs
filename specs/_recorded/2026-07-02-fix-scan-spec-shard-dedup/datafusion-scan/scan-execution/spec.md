# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion memory pool from the per-instance memory limit reported in UDF metadata, applies the pushed-down projection, filter, and LIMIT, and streams the matching rows back as Arrow IPC batches. It holds no state and discovers no files of its own. The UDF receives its scan spec as TWO VARCHAR arguments — a shard-invariant common spec serialized once for the whole fan-out, and a per-shard file-URI list — which it merges back into one `ScanSpec` at entry.

## Background

* The scan UDF receives two VARCHAR JSON arguments: `common` (shard-invariant: projection, filter, limit, aggregates, group keys, logical schema, EMITS types, storage credentials, and tuning knobs) and `files` (this shard's assigned file URIs). It merges them into one `ScanSpec` before running.
* The scan UDF registers ONLY its assigned files and never discovers files from the catalog.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* Only `Value::String` types cross the `.so` boundary; both arguments are VARCHAR JSON.
* Error messages MUST NOT contain storage access keys, secret keys, or session tokens.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan invocation receiving TWO VARCHAR arguments — a shard-invariant common spec argument (carrying the logical Iceberg schema, projection, filter, limit, storage credentials, and tuning knobs) and a per-shard files argument listing specific Iceberg Parquet files in MinIO
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL read the common spec from the first input argument and the file-URI list from the second, and reconstitute a single `ScanSpec` whose `files` come from the second argument and whose every other field comes from the first (only `Value::String` crossing the `.so` boundary — both arguments are VARCHAR JSON)
* *AND* the UDF SHALL create a DataFusion session and register ONLY the files from the second argument as one `scan_target` whose declared schema is the logical Iceberg schema (each field carrying its `field_id` metadata), NOT a schema inferred from the first file, and MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per scanned source row containing only the projected columns
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field and whose second argument is a JSON array of file URIs
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the file-list JSON and MERGE them into one `ScanSpec` value equivalent to the pre-split single-argument spec for the same shard
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
<!-- /DELTA:NEW -->
