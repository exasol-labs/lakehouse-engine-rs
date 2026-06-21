# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to it, applies the pushed-down
projection, filter, and LIMIT to the scan, then streams the result back to Exasol by
converting each Arrow batch to SDK `Value` rows and emitting them. It holds no state
and discovers no files of its own.

## Background

* The UDF is the SET-script entry point of the same `.so` as the VS adapter; it reads
  its assigned scan spec (file list, projection, filter, limit, catalog/storage
  connection properties) from its input row(s) via `ctx.next()` / typed getters.
* Only SDK `Value` types cross the `.so` boundary — Arrow types MUST NOT cross it.
  Each Arrow `RecordBatch` is converted to rows of `Value` inside the UDF.
* The emit buffer auto-flushes at 4,000,000 bytes; the UDF relies on that rather than
  collecting the full result set.
* Arrow→`Value` conversion (B.4) MUST implement the full mapping defined in the
  `datafusion-scan/type-mapping` feature — every compatible Arrow type plus the JSON
  fallback for out-of-range Decimal128 and all incompatible types — not only the
  int/float/string/bool/date/timestamp subset.
* The S3-compatible object store is MinIO; DataFusion's object store is configured with
  the supplied endpoint, region, and credentials and `validateservercertificate=0`
  semantics where applicable.

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
