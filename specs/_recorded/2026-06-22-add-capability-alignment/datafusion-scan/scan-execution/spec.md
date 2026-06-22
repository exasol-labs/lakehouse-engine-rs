# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion
`RuntimeEnv` memory pool from the per-instance memory limit reported in UDF metadata,
applies the pushed-down projection, filter, and LIMIT, and either streams the matching
rows back or — when the spec carries aggregate instructions — emits one node-local
partial-aggregate row per distinct group (or a single row for ungrouped aggregates).
It holds no state and discovers no files of its own.

## Background

* The scan UDF reads its ScanSpec from a single JSON VARCHAR input column and registers
  only its assigned files; it discovers no files of its own.
* Only SDK Value types cross the .so boundary; no Arrow types.
* The projection may carry rendered DataFusion SQL select-list expressions (not just bare
  column names); the UDF places them verbatim in its SELECT list.
* Partial aggregates are emitted in their merge-ready form; statistical aggregates are
  emitted as `(count, sum, sum_sq)` sufficient statistics the wrapper reconstructs.
* Credentials MUST NOT appear in any error message.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Scan projects rendered select-list expressions

* *GIVEN* a scan spec whose projection carries rendered DataFusion SQL select-list expressions (e.g. `UPPER("NAME")`, `("PRICE" * "QTY")`, `EXTRACT(YEAR FROM "ORDER_DATE")`) rather than bare column names
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL place each rendered select-list expression verbatim in its DataFusion SELECT list, in spec order
* *AND* the UDF SHALL emit one output row per scanned source row carrying the evaluated expression values in that order
* *AND* the EMITS declaration in the scan-driving SQL MUST match the rendered select-list in order and result type
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: Scan emits sufficient statistics for a decomposable statistical aggregate

* *GIVEN* a scan spec requesting a partial `STDDEV`/`STDDEV_POP`/`STDDEV_SAMP`/`VARIANCE`/`VAR_POP`/`VAR_SAMP` over a column
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* the UDF SHALL emit the sufficient-statistics triple `COUNT(col)`, `SUM(col)`, and `SUM(col*col)` for that column rather than a per-shard standard deviation or variance
* *AND* the partial count SHALL exclude rows where the target column is NULL, so the merged statistic matches single-node semantics
* *AND* an empty shard (or empty group) SHALL emit a partial count of zero with NULL partial sums that the wrapper's merge ignores
* *AND* no Arrow type SHALL cross the `.so` boundary
<!-- /DELTA:NEW -->
