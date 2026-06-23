# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion
`RuntimeEnv` memory pool from the per-instance memory limit reported in UDF metadata,
applies the pushed-down projection, filter, and LIMIT, and either streams the matching
rows back or — when the spec carries aggregate instructions — emits one node-local
partial-aggregate row per distinct group (or a single row for ungrouped aggregates).
It holds no state and discovers no files of its own.

## Background

* The scan UDF reads its ScanSpec from a single JSON VARCHAR input column.
* The UDF MUST register only its assigned files and MUST NOT discover additional files.
* Only SDK Value types cross the .so boundary; no Arrow types.
* Credentials MUST NOT appear in any error message.
* The per-instance memory limit is read from `ctx.memory_limit()` (bytes; `0` =
  unbounded/unknown sentinel), provided by the `language-container-rs:add-memory-limit-metadata`
  SDK accessor. Exasol enforces the same per-process heap limit via `setrlimit(RLIMIT_RSS)` and
  stalls additional concurrent VMs once usage reaches 80% of it, so a pool sized under the limit
  lets the engine self-manage concurrency.
* The spill backstop directory is `/tmp`; the UDF probes at runtime whether `/tmp` is real disk
  with free space (tmpfs detection via `/proc/mounts` plus `statvfs` free-space check). Any `/tmp`
  spill is transient per-invocation scratch — NOT persistent state.
* The emit buffer auto-flushes at 4,000,000 bytes; the UDF relies on that rather than
  collecting the full result set.
* Arrow→`Value` conversion MUST implement the full mapping defined in the
  `datafusion-scan/type-mapping` feature — every compatible Arrow type plus the JSON
  fallback for out-of-range Decimal128 and all incompatible types.
* The S3-compatible object store is MinIO; DataFusion's object store is configured with
  the supplied endpoint, region, and credentials and `validateservercertificate=0`
  semantics where applicable.
* Memory budgeting and credential-passthrough scenarios (including vended STS tokens)
  are in `datafusion-scan/scan-execution-memory-and-credentials`.

## Scenarios

### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan spec listing specific Iceberg Parquet files in MinIO
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL create a DataFusion session and register only the assigned files
* *AND* the UDF MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per scanned source row containing only the projected columns

### Scenario: Filter predicate restricts the emitted rows

* *GIVEN* a scan spec carrying a translatable filter predicate
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL apply the predicate to the DataFusion scan
* *AND* the UDF SHALL emit only rows that satisfy the predicate

### Scenario: LIMIT caps the emitted rows

* *GIVEN* a scan spec carrying a row limit smaller than the matching row count
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit no more rows than the limit

### Scenario: Arrow batches are converted to Value rows and emitted incrementally

* *GIVEN* a scan whose result spans multiple Arrow record batches
* *WHEN* the scan UDF processes the result stream
* *THEN* the UDF SHALL convert each batch to SDK `Value` rows and `ctx.emit` them before fetching the next batch
* *AND* the UDF MUST NOT materialize the entire result set in memory before emitting
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: Arrow types map to the correct SDK Value variants

* *GIVEN* a table with integer, floating-point, string, boolean, date, and timestamp columns
* *WHEN* the scan UDF converts a batch of those columns
* *THEN* each Arrow column value SHALL map to the corresponding SDK `Value` variant per the `datafusion-scan/type-mapping` table
* *AND* an Arrow null SHALL map to `Value::Null`

### Scenario: Incompatible Arrow columns are emitted as JSON strings

* *GIVEN* a scan result containing columns of types Exasol cannot represent (list, struct, map, binary, or out-of-range decimal)
* *WHEN* the scan UDF converts a batch of those columns
* *THEN* the UDF SHALL serialize each such value to a JSON string and emit it as `Value::String` per the `datafusion-scan/type-mapping` rules
* *AND* the UDF MUST NOT emit any array, list, struct, or map `Value`

### Scenario: Scan reports a clear error when an assigned file is unreadable

* *GIVEN* a scan spec referencing a file that cannot be read from object storage
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL return an error identifying that the assigned data could not be read
* *AND* the error message MUST NOT contain storage access keys or secret keys

### Scenario: Scan computes a node-local partial aggregate instead of raw rows

* *GIVEN* a scan spec carrying partial-aggregate instructions and the files assigned to this shard
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register only its assigned files and apply any pushed-down filter
* *AND* the UDF SHALL compute the requested aggregates over its assigned files locally in DataFusion
* *AND* the UDF SHALL emit a single partial-result row carrying the per-shard partial aggregate values rather than the scanned rows
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: Partial COUNT, SUM, MIN, and MAX are emitted in their merge-ready form

* *GIVEN* a scan spec requesting any of partial `COUNT`, `SUM`, `MIN`, or `MAX`
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* a partial `COUNT` SHALL be the count of matching rows in this shard, emitted as a value the wrapper can sum
* *AND* a partial `SUM` SHALL be the sum over this shard's matching rows, emitted as a value the wrapper can sum
* *AND* partial `MIN` and `MAX` SHALL be this shard's minimum and maximum, emitted as values the wrapper can re-`MIN`/`MAX`
* *AND* an empty shard SHALL emit a partial `COUNT` of zero and a NULL partial `SUM`/`MIN`/`MAX` that the wrapper's merge ignores

### Scenario: AVG is emitted as a partial sum and partial count pair

* *GIVEN* a scan spec requesting partial `AVG(col)`
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* the UDF SHALL emit a `(partial_sum, partial_count)` pair for that column
* *AND* the UDF MUST NOT emit a per-shard average
* *AND* the partial count SHALL exclude rows where the target column is NULL so the merged average matches single-node `AVG` semantics
