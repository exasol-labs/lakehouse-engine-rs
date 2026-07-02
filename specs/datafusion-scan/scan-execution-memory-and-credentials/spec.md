# Feature: DataFusion Scan Execution — Memory Budgeting and Credential Passthrough

Extends the scan UDF to read the real per-instance memory limit from
`ctx.memory_limit()` and size the DataFusion memory pool from a *net* budget — the
per-instance limit minus a configurable container/binary overhead — scaled by a
configurable fraction, to bound the per-batch Parquet decode working set via a
configured `batch_size`, to enable Parquet row-group and page pruning so the scan
reads only the byte ranges its predicate needs, and to consume storage credentials
carried in the scan spec (including vended STS tokens) without re-authenticating to
the catalog. The credentials and tuning knobs travel in the shard-invariant common
spec argument, serialized once for the whole fan-out.

## Background

* The per-instance memory limit is read from `ctx.memory_limit()` (bytes; `0` =
  unknown sentinel). For a positive limit the pool is sized to
  `fraction × (limit − overhead_bytes)`, floored at a minimum non-zero budget,
  leaving headroom below the Exasol engine's 80% concurrency-stall threshold.
* The memory-pool fraction (default `0.6`) and the per-instance container-overhead
  megabytes (default `200`) are VS properties carried into the scan spec; a scan
  spec lacking them deserializes to those defaults.
* When the limit is the `0` sentinel, a conservative default budget is used and the
  fraction and overhead are ignored.
* The DataFusion memory pool bounds aggregation, sort, and join — but NOT the
  Parquet→Arrow decode and scan buffers. The configured `batch_size` is the lever
  that bounds that out-of-pool working set.
* The `batch_size` is carried in the scan spec; a spec lacking it deserializes to a
  conservative built-in default, and a sub-1 value is clamped to 1.
* Scan efficiency depends on the Parquet reader skipping data the predicate cannot
  match: row-group pruning (per-row-group statistics), page-index pruning (page-level
  statistics), and pushed-down filters into the `ParquetExec`. These are
  configuration flags on the DataFusion session / Parquet scan options, distinct from
  Iceberg file-level pruning (`vs-adapter/pushdown-file-pruning`), which prunes whole
  files before the reader opens them. The two compose: Iceberg drops files, the
  Parquet reader then drops row groups and pages within the surviving files.
* Storage credentials (including vended S3 keys) reach the UDF only inside the
  shard-invariant common spec argument, serialized once for the whole fan-out rather
  than repeated per shard; the UDF never contacts the catalog or re-requests credentials.
* Credentials MUST NOT appear in any error message.
* See `datafusion-scan/scan-execution` for the base two-argument scan execution scenarios.

## Scenarios

### Scenario: Scan sizes its memory pool from the reported per-instance limit

* *GIVEN* a scan UDF invocation whose UDF context reports a positive per-instance memory limit via `ctx.memory_limit()`
* *AND* a scan spec carrying a memory-pool fraction and a per-instance container-overhead byte count
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL subtract the container-overhead bytes from the per-instance limit and size the DataFusion memory pool to the configured fraction of that net budget
* *AND* the resulting pool budget MUST stay below the Exasol engine's 80% concurrency-stall threshold for the reported limit
* *AND* the UDF MUST NOT hardcode the pool budget to the unknown-limit default when a positive limit is reported

### Scenario: Scan falls back to the default budget when no memory limit is reported

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` returns `0` (the unknown / unavailable sentinel)
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL size the DataFusion memory pool to the conservative default budget, ignoring the configured fraction and overhead
* *AND* the scan SHALL otherwise execute identically to the positive-limit path

### Scenario: Scan clamps the memory pool to a minimum floor when overhead exceeds the limit

* *GIVEN* a scan UDF invocation whose `ctx.memory_limit()` reports a positive per-instance limit
* *AND* a scan spec whose container-overhead bytes are greater than or equal to that limit
* *WHEN* the scan UDF builds its DataFusion session context
* *THEN* the UDF SHALL clamp the DataFusion memory pool budget to a minimum non-zero floor rather than producing a zero or negative budget
* *AND* the scan SHALL still build a usable session context that can execute a scan

### Scenario: Scan reads data files with vended credentials carried in the scan spec

* *GIVEN* a scan invocation whose shard-invariant common spec argument carries a storage block with vended S3 credentials (access key, secret key, session token) resolved once by the planning layer
* *WHEN* the scan UDF builds its object store and reads the files listed in its per-shard argument
* *THEN* the UDF SHALL configure its S3 object store from the credentials in the common spec argument
* *AND* the storage credentials SHALL travel in the shard-invariant common spec argument (serialized once for the whole fan-out), NOT be repeated per shard
* *AND* the UDF MUST NOT re-authenticate to the catalog or re-request vended credentials
* *AND* a credential value MUST NOT appear in any error message the UDF returns

### Scenario: Scan bounds the Parquet decode working set via a configured batch size

* *GIVEN* a scan UDF building its DataFusion session configuration for a scan spec
* *WHEN* the UDF builds the `SessionConfig` (`session_config_for_spec`)
* *THEN* the UDF SHALL set the DataFusion `batch_size` so the per-batch Parquet decode and scan working set stays bounded, rather than leaving it at the DataFusion default
* *AND* the configured `batch_size` SHALL be sourced from the scan spec when present and otherwise from a conservative built-in default, clamped to at least 1
* *AND* the bound SHALL apply on both the raw-row scan path and the partial-aggregate path, since both decode source Parquet files

### Scenario: Scan enables Parquet row-group and page pruning so the reader skips non-matching data

* *GIVEN* a scan UDF building its DataFusion session configuration for a scan spec carrying a filter predicate
* *WHEN* the UDF builds the session config and the `ParquetExec` for its assigned files
* *THEN* the UDF SHALL enable Parquet predicate pushdown, row-group statistics pruning, and page-index pruning on the Parquet scan options rather than relying on the DataFusion defaults
* *AND* a row group whose column statistics provably exclude the predicate SHALL NOT be decoded
* *AND* this Parquet-level pruning SHALL compose with the Iceberg file-level pruning of `vs-adapter/pushdown-file-pruning` — files dropped by Iceberg are never opened, and within the surviving files non-matching row groups and pages are skipped
* *AND* the emitted rows SHALL be identical to a scan with pruning disabled (pruning narrows what is read, never the result set)
