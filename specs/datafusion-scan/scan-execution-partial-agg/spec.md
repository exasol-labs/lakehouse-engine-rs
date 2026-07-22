# Feature: DataFusion Scan Execution — Partial Aggregate Output

The scan UDF partial-aggregate path: computing node-local aggregates inside
DataFusion, emitting per-shard partial results in a form the Exasol wrapper SQL
can merge into the final query result.

## Background

* When a scan spec carries partial-aggregate instructions the UDF runs a DataFusion
  aggregation over its assigned files and emits partial results rather than raw rows.
* The partial results are shaped so the Exasol wrapper can combine them across shards
  with standard SQL aggregate functions (`SUM`, `MIN`, `MAX`). `COUNT(DISTINCT)` is NOT a
  partial aggregate on this path — the adapter dispatches it as a DISTINCT row-scan (see
  `vs-adapter/pushdown-planning-count-distinct` and the DISTINCT row-scan scenario below).
* The same file-assignment, filter, and pushdown rules from `datafusion-scan/scan-execution`
  apply on the partial-aggregate path.
* The partial-aggregate path registers the full logical table schema and builds its
  DataFusion query from the `aggregates` field. It does not consult the scan spec's
  `projection` field; DataFusion's own projection pushdown derives the physical column
  set from the partial-aggregate query text.
* Only SDK Value types cross the `.so` boundary; no Arrow types.
* See `datafusion-scan/scan-execution` for the base raw-row scan scenarios and emit model.
* See `datafusion-scan/scan-execution-grouped-agg` for grouped partial-aggregate
  memory, spill, and group-key scenarios.

## Scenarios

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

### Scenario: Partial aggregate over an expression argument is computed from the rendered expression

* *GIVEN* a scan spec whose aggregate plan carries a rendered DataFusion SQL expression as an aggregate argument (e.g. `LENGTH("L_COMMENT")`) rather than a bare column name
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* the UDF SHALL apply the aggregate function to that rendered expression in its node-local DataFusion aggregation, emitting the same partial column shape as the bare-column form of that aggregate
* *AND* the merged result over all shards SHALL equal the same aggregate-over-expression evaluated over all rows on a single node
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: COUNT(DISTINCT) runs as a DISTINCT row-scan rather than a partial aggregate

* *GIVEN* a scan spec whose projection is a single column (or rendered expression) with the `distinct` flag set, and the files assigned to this shard
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL apply DataFusion `.distinct()` to that single-column projection over its assigned files, streaming one row per locally-distinct value rather than computing an aggregate partial
* *AND* the UDF SHALL emit those rows through the raw row-scan `emit_batch` path, declaring the column with its actual Exasol EMITS type (the standard Arrow-to-Exasol mapping, including the JSON-string fallback for incompatible types), never serializing the whole distinct set into one JSON-array VARCHAR value
* *AND* the UDF MUST NOT accumulate the full distinct set into a single value and MUST NOT enforce any per-shard element or byte cap
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: Partial aggregate physically reads only the aggregate-referenced columns

* *GIVEN* a single-group partial-aggregate scan over a multi-column Parquet file whose aggregates reference a strict subset of the columns (e.g. `SUM(score)` over a table of `id`, `score`, `name`)
* *AND* a scan spec whose `projection` field is empty
* *WHEN* the scan UDF builds the DataFusion physical plan for the partial aggregate
* *THEN* the physical Parquet scan SHALL project ONLY the columns referenced by the aggregates, never the full column set
* *AND* the empty `projection` field SHALL NOT cause a full-column read, because DataFusion derives the physical projection from the partial-aggregate query text rather than from the scan spec's `projection` field
