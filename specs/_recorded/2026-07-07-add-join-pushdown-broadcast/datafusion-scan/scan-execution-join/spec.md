# Feature: DataFusion Scan Execution — Broadcast Join

Extends `datafusion-scan/scan-execution` with node-local broadcast inner equi-join execution. A join scan invocation receives, in addition to its per-shard fact-file subset, the FULL dimension-side file list carried once in the shard-invariant common spec. The UDF registers both sides as Iceberg tables in ONE DataFusion session, executes the inner equi-join with the pushed projection, filter, and LIMIT, and streams the joined rows back as Arrow IPC batches. It holds no state and discovers no files of its own.

## Background

* Only SDK `Value` types and Arrow IPC byte buffers cross the `.so` boundary; no typed Arrow value does.
* Both sides register from the file lists carried in the scan spec — the fact side from the per-shard argument, the dimension side from the common-spec join block — each declared against its own logical Iceberg schema.
* The DataFusion memory pool is sized from the per-instance memory limit exactly as the raw-scan path does; the bounded dimension side is the hash-join build side.
* Storage access keys and secret keys MUST NOT appear in any error message.

## Scenarios

### Scenario: Scan reconstitutes a join scan spec carrying two file lists

* *GIVEN* a scan invocation whose common-spec argument carries a join block (the dimension side's table root, full file list, logical schema, the rendered join condition, and the join type) and whose per-shard argument carries the fact side's `(path, size)` file subset
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL reconstitute one join `ScanSpec` whose fact files come from the per-shard argument and whose dimension side and every other field come from the common spec
* *AND* a parse failure on either argument SHALL surface an error identifying scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted spec MUST NOT carry any catalog identifier, because the scan UDF never contacts the catalog

### Scenario: Scan registers both tables and executes the inner equi-join

* *GIVEN* a reconstituted join scan spec
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL register the fact side's assigned files and the dimension side's full file list as two separate tables in ONE DataFusion session, each with its declared logical Iceberg schema and each exposing its columns under the Exasol-facing (uppercased) names the pushed condition and projection reference
* *AND* the UDF SHALL execute an inner equi-join of the two registered tables on the rendered join condition
* *AND* the UDF MUST NOT resolve or discover any file beyond the two file lists carried in the spec

### Scenario: Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC

* *GIVEN* a join scan spec carrying a projection spanning both sides, an optional filter, and an optional row limit
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL emit only the projected join-output columns, in spec order, for rows satisfying both the join condition and the filter
* *AND* the UDF SHALL emit no more rows than the limit when one is carried
* *AND* the UDF SHALL emit each result batch via the SDK Arrow-batch emit path (`emit_batch`), fetching one batch, emitting it, and dropping it before the next, never materializing the entire joined result set
* *AND* no typed Arrow value SHALL cross the `.so` boundary — only the serialized IPC byte buffer

### Scenario: The bounded dimension side is the hash-join build side

* *GIVEN* a join scan spec whose dimension side is below the broadcast threshold and whose fact side is a large sharded subset
* *WHEN* the scan UDF plans the join
* *THEN* the join SHALL build its hash table on the bounded dimension side and probe with the fact side, so per-instance memory is bounded by the dimension side rather than the fact shard
* *AND* the DataFusion memory pool SHALL be sized from the per-instance memory limit exactly as on the raw-scan path

### Scenario: Scan reports a clear error when an assigned join file is unreadable

* *GIVEN* a join scan spec referencing a fact-side or dimension-side file that cannot be read from object storage
* *WHEN* the scan UDF runs
* *THEN* the UDF SHALL return an error identifying that the assigned data could not be read
* *AND* the error message MUST NOT contain storage access keys or secret keys
