# Feature: DataFusion Scan Execution

A disposable Rust SCALAR EMIT UDF. For one call it builds a DataFusion session, registers only the Parquet files assigned to its shard, applies the pushed-down projection, filter, and LIMIT, and streams the matching rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* The SDK invokes `run()` once per input row of a SCALAR EMIT UDF. The UDF scans exactly that row's assigned file list per call and never iterates the input with `ctx.next()`.
* The UDF takes two VARCHAR JSON arguments per row: `common` (shard-invariant fields) and `files` (this shard's `(path, size)` entries). It merges them into one `ScanSpec`; see `datafusion-scan/scan-execution-spec-reconstitution`.
* Only serialized bytes cross the `.so` boundary: VARCHAR JSON in, Arrow IPC bytes out. The raw-row path emits each `RecordBatch` through the SDK's `EmitBatch` Arrow-IPC path (feature `emit-arrow`) and builds no `Vec<Value>` per batch.
* Per-file metadata and path resolution against the table root are in `datafusion-scan/scan-execution-file-metadata`.
* When the scan spec carries a logical schema, column projection binds each logical field by the key its format reader populated: field-id (Iceberg, Delta `id` column mapping), physical name (Delta `name` column mapping), or logical name (Delta `none` column mapping). Without a logical schema, the UDF infers the schema from the first file and binds by physical name.
* The scan registers files through the `ParquetSource`-backed table provider `PositionalDeleteScanTable`, not iceberg-rust's Arrow reader, so it sets INT96 coercion itself.
* INT96 is a legacy Parquet/Hive/Spark timestamp encoding that the Iceberg spec does not define. It carries nanosecond precision, and coercing to `"us"` truncates sub-microsecond digits.
* `coerce_int96_tz = "UTC"` makes the decoded physical type `Timestamp(Microsecond, "UTC")` for every column. An Iceberg `timestamp` column's logical type is `Timestamp(Microsecond, None)`, and `datafusion-scan/scan-execution-value-conversion` reconciles the difference at emit.
* DataFusion execution is memory-bounded. A bound that cannot spill surfaces as a clean error, never an OOM VM crash.
* Error messages never contain storage access keys, secret keys, or session tokens.

## Scenarios

### Scenario: Scan handles one input row per scalar run() call and never iterates with ctx.next()

* *GIVEN* the SDK-0.21.0 runtime invokes the SCALAR EMIT scan `run()` ONCE per input row, each call carrying that row's shard-invariant common spec argument (column 0) and its own per-shard files argument (column 1)
* *WHEN* the scan UDF runs for one such call
* *THEN* the UDF SHALL reconstitute exactly that one row's `ScanSpec`, register and scan only that row's assigned file list, and emit that row's surviving output rows
* *AND* the UDF MUST NOT call `ctx.next()`, which the SDK-0.21.0 runtime rejects with an error in scalar (`ExactlyOnce`) context
* *AND* across a multi-shard fan-out whose distributed rows each drive a SEPARATE `run()` call, every shard's rows SHALL be emitted, so NO shard is silently dropped (the regression the removed batch loop was hand-rolling around), and the DataFusion runtime SHALL be built and torn down once per call per `scan-runtime/scan-execution-threading`
* *AND* only serialized bytes (VARCHAR JSON arguments in, Arrow IPC bytes out) SHALL cross the `.so` boundary

### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan input row carrying TWO VARCHAR arguments — a shard-invariant common spec argument (carrying the logical schema, projection, filter, limit, a storage credential reference or a sealed vended storage envelope, the table root, and tuning knobs) and a per-shard files argument listing specific Parquet data files in SeaweedFS, each optionally carrying its associated positional-delete file references
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF processes that input row
* *THEN* the UDF SHALL read the common spec from the first input argument and the file list from the second, and reconstitute a single scan spec whose files (and their delete references) come from the second argument and whose every other field comes from the first (only serialized bytes crossing the `.so` boundary — both arguments are VARCHAR JSON)
* *AND* the UDF SHALL resolve each file entry to an absolute URI and register ONLY those files through the custom table provider whose declared schema is the logical schema, and MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per surviving source row containing only the projected columns
* *AND* the UDF SHALL run this same registration path for a spec produced by EITHER format reader, because the table root and the logical schema are neutral fields both populate

### Scenario: Filter predicate restricts the emitted rows

* *GIVEN* a scan spec carrying a translatable filter predicate
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL apply the predicate to the DataFusion scan
* *AND* the UDF SHALL emit only rows that satisfy the predicate

### Scenario: LIMIT caps the emitted rows

* *GIVEN* a scan spec carrying a row limit smaller than the matching row count
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit no more rows than the limit

### Scenario: Arrow batches are emitted incrementally as Arrow IPC and never double-materialized

* *GIVEN* a scan whose result spans multiple Arrow record batches
* *WHEN* the scan UDF processes the result stream
* *THEN* the UDF SHALL emit each batch via the SDK's Arrow-batch emit path (the `EmitBatch` API, gated by the `emit-arrow` feature), serializing the batch to Arrow IPC bytes so only IPC bytes cross the `.so` boundary
* *AND* the UDF SHALL fetch one batch, emit it, and drop it before fetching the next, never materializing the entire result set
* *AND* the UDF MUST NOT build an intermediate `Vec<Value>` row collection on the raw-row scan path, and no typed Arrow value SHALL cross the `.so` boundary — only the serialized IPC byte buffer

### Scenario: Scan reports a clear error when an assigned file is unreadable

* *GIVEN* a scan spec referencing a file that cannot be read from object storage
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL return an error identifying that the assigned data could not be read
* *AND* the error message MUST NOT contain storage access keys or secret keys

### Scenario: Scan surfaces a clean memory-exhaustion error instead of crashing the VM

* *GIVEN* a scan whose execution exhausts the configured DataFusion memory pool (a `ResourcesExhausted` condition) on a node whose `/tmp` is not spill-capable disk
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL surface a clean error that identifies memory/resource exhaustion as the cause, and MUST NOT crash the UDF VM
* *AND* the error-redaction path MUST NOT reclassify a `ResourcesExhausted` condition as an "assigned data could not be read" storage error
* *AND* the surfaced error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: Out-of-range INT96 timestamp columns decode at microsecond resolution without overflow

* *GIVEN* a scanned Iceberg Parquet data file whose Iceberg `timestamp` (WITHOUT time zone) column is physically encoded as Parquet INT96 (a legacy pre-Iceberg encoding outside the Iceberg-to-Parquet mapping), carrying a value outside the Arrow nanosecond range 1677-09-21 to 2262-04-11, such as `9999-12-31 23:59:59`
* *WHEN* the scan UDF constructs its `ParquetFormat`s, registers the file, and scans it
* *THEN* the UDF SHALL configure every `ParquetFormat` it constructs — both the decode-path provider and any legacy first-file schema inference — to coerce INT96 columns to microsecond resolution (`coerce_int96 = "us"`) with a UTC time zone (`coerce_int96_tz = "UTC"`)
* *AND* the scan SHALL decode the out-of-range timestamp WITHOUT an i64 nanosecond-overflow error, and on the legacy inference path the inferred schema and the decoded batch SHALL agree on the timestamp column's Arrow type
* *AND* on the field-id (production) path, where the logical schema maps the Iceberg `timestamp` column to `Timestamp(Microsecond, None)`, the decoded `Timestamp(Microsecond, "UTC")` batch SHALL be coerced to the Arrow type the declared EMITS ExaType requires before `emit_batch` — per `datafusion-scan/scan-execution-value-conversion / Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch` — so the None-vs-UTC difference between logical schema and decoded batch is reconciled at emit
* *AND* the emitted timestamp SHALL equal the source instant at microsecond resolution — INT96's sub-microsecond digits are deliberately truncated, consistent with Iceberg's microsecond `timestamp` model — per the existing Arrow-`Timestamp`-to-Exasol-`TIMESTAMP` mapping
