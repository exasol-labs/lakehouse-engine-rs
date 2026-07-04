# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion
`RuntimeEnv` memory pool from the per-instance memory limit reported in UDF metadata,
applies the pushed-down projection, filter, and LIMIT, and streams the matching rows
back as Arrow IPC batches. It holds no state and discovers no files of its own. The
UDF receives its scan spec as TWO VARCHAR arguments — a shard-invariant common spec
serialized once for the whole fan-out (including the Iceberg table root), and a per-shard
`(path, size)` file list — which it merges back into one `ScanSpec` at entry.

## Background

* The scan UDF receives two VARCHAR JSON arguments: `common` (shard-invariant: projection, filter, limit, aggregates, group keys, logical schema, EMITS types, storage credentials, the Iceberg table root, and tuning knobs) and `files` (this shard's assigned `(path, size)` entries). It merges them into one `ScanSpec` before running; see `datafusion-scan/scan-execution-spec-reconstitution` for the reconstitution and malformed-input scenarios.
* The UDF MUST register only its assigned files and MUST NOT discover additional files.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* Only `Value::String` types cross the `.so` boundary; both arguments are VARCHAR JSON.
* DataFusion execution is bounded; a memory bound that cannot spill MUST surface as a
  clean error, never an OOM VM crash.
* Error messages MUST NOT contain storage access keys, secret keys, or session tokens.
* The raw-row scan pipeline is throughput-sensitive: needless physical-plan stages
  (a `RepartitionExec`, a `CoalescePartitionsExec`, a global `SortExec`, or a global
  aggregate) on the single-shard raw-scan path add CPU and latency without changing
  the result, and MUST be avoided so the per-instance pipeline stays
  `ParquetExec → FilterExec → ProjectionExec → CoalesceBatchesExec → emit`. A bounded
  top-N (an `ORDER BY … LIMIT n` TopK) is the one intentional exception, present only
  when the scan spec carries an `order_by` (see the scenario below).

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Scan emits a bounded local top-N when the spec carries an order-by

* *GIVEN* a scan spec carrying an `order_by` sort-key list (each with a column, direction, and NULL placement), a row limit `n`, and no aggregates or group keys (the raw-row path)
* *WHEN* the scan UDF runs over its assigned files
* *THEN* the UDF SHALL apply `ORDER BY <keys> LIMIT n` in its DataFusion scan query so it emits at most `n` rows — its own local top-N over only its assigned files
* *AND* the rendered `ORDER BY` SHALL preserve each key's requested direction (`ASC`/`DESC`) and NULL placement (`NULLS FIRST`/`NULLS LAST`)
* *AND* the bounded sort SHALL be a top-N (retaining only the `n` extreme rows) rather than a full materialised global sort of the shard's rows
<!-- /DELTA:NEW -->
