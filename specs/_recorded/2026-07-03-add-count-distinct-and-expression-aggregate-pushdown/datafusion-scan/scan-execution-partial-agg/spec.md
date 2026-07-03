# Feature: DataFusion Scan Execution — Partial Aggregate Output

The scan UDF partial-aggregate path: computing node-local aggregates inside
DataFusion, emitting per-shard partial results in a form the Exasol wrapper SQL
can merge into the final query result.

## Background

* When a scan spec carries partial-aggregate instructions the UDF runs a DataFusion
  aggregation over its assigned files and emits partial results rather than raw rows.
* The partial results are shaped so the Exasol wrapper can combine them across shards
  with standard SQL aggregate functions (`SUM`, `MIN`, `MAX`) or a dedicated scalar
  merge UDF for `COUNT(DISTINCT)`.
* The same file-assignment, filter, and pushdown rules from `datafusion-scan/scan-execution`
  apply on the partial-aggregate path.
* Only SDK Value types cross the `.so` boundary; no Arrow types.
* See `datafusion-scan/scan-execution` for the base raw-row scan scenarios and emit model.
* See `datafusion-scan/scan-execution-grouped-agg` for grouped partial-aggregate
  memory, spill, and group-key scenarios.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Partial aggregate over an expression argument is computed from the rendered expression

* *GIVEN* a scan spec whose aggregate plan carries a rendered DataFusion SQL expression as an aggregate argument (e.g. `LENGTH("L_COMMENT")`) rather than a bare column name
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* the UDF SHALL apply the aggregate function to that rendered expression in its node-local DataFusion aggregation, emitting the same partial column shape as the bare-column form of that aggregate
* *AND* the merged result over all shards SHALL equal the same aggregate-over-expression evaluated over all rows on a single node
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: COUNT(DISTINCT) emits the shard's local distinct set as one VARCHAR partial value

* *GIVEN* a scan spec requesting a single-group `COUNT(DISTINCT col)` partial and the files assigned to this shard
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL compute the LOCAL distinct value set of that column over its assigned files inside DataFusion, excluding NULLs
* *AND* the UDF SHALL serialize that local distinct set to a JSON array string inside the UDF and emit it as exactly one VARCHAR partial value for that aggregate, so no Arrow list/array type crosses the `.so` boundary
* *AND* an empty shard SHALL emit an empty JSON array (`[]`) so the merge treats it as contributing no distinct values

### Scenario: COUNT(DISTINCT) enforces a bounded per-shard safety cap

* *GIVEN* a scan spec requesting a single-group `COUNT(DISTINCT col)` partial over a column whose per-shard local distinct set would exceed the configured cap — a maximum distinct-element count and a maximum serialized-byte size kept safely below the `VARCHAR(2000000)` wire limit
* *WHEN* the scan UDF accumulates the local distinct set and the cap is reached
* *THEN* the UDF SHALL stop and return a clean bounded-resource error identifying the offending column and the cap that was exceeded, consistent with the engine's `ResourcesExhausted` bounded-execution convention
* *AND* the UDF MUST NOT emit a truncated distinct set (which would produce a wrong merged count)
* *AND* the error message MUST NOT contain any credential value
<!-- /DELTA:NEW -->
