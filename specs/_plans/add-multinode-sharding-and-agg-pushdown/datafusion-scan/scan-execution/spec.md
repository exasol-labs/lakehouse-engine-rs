# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, applies the pushed-down
projection, filter, and LIMIT, and either streams the matching rows back or — when the
spec carries aggregate instructions — emits one node-local partial-aggregate row. It
holds no state and discovers no files of its own.

## Background

* The UDF reads its scan spec (files, projection, filter, limit, optional aggregate
  plan, catalog/storage connection properties) from its input row(s); it registers only
  its assigned files and never resolves catalog metadata.
* Only SDK `Value` types cross the `.so` boundary — Arrow types MUST NOT cross it.
* For an aggregate spec the UDF emits a single partial row per shard; the adapter's
  wrapper SQL merges the per-shard partials into the final result.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Scan computes a node-local partial aggregate instead of raw rows

* *GIVEN* a scan spec carrying partial-aggregate instructions and the files assigned to this shard
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL register only its assigned files and apply any pushed-down filter
* *AND* the UDF SHALL compute the requested aggregates over its assigned files locally in DataFusion
* *AND* the UDF SHALL emit a single partial-result row carrying the per-shard partial aggregate values rather than the scanned rows
* *AND* no Arrow type SHALL cross the `.so` boundary
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Partial COUNT, SUM, MIN, and MAX are emitted in their merge-ready form

* *GIVEN* a scan spec requesting any of partial `COUNT`, `SUM`, `MIN`, or `MAX`
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* a partial `COUNT` SHALL be the count of matching rows in this shard, emitted as a value the wrapper can sum
* *AND* a partial `SUM` SHALL be the sum over this shard's matching rows, emitted as a value the wrapper can sum
* *AND* partial `MIN` and `MAX` SHALL be this shard's minimum and maximum, emitted as values the wrapper can re-`MIN`/`MAX`
* *AND* an empty shard SHALL emit a partial `COUNT` of zero and a NULL partial `SUM`/`MIN`/`MAX` that the wrapper's merge ignores
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: AVG is emitted as a partial sum and partial count pair

* *GIVEN* a scan spec requesting partial `AVG(col)`
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* the UDF SHALL emit a `(partial_sum, partial_count)` pair for that column
* *AND* the UDF MUST NOT emit a per-shard average
* *AND* the partial count SHALL exclude rows where the target column is NULL so the merged average matches single-node `AVG` semantics
<!-- /DELTA:NEW -->
