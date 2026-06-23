# Feature: DataFusion Scan Execution — Expression Pushdown

Extends `datafusion-scan/scan-execution` with the two new execution capabilities enabled
by the `add-capability-alignment` plan: rendering select-list expressions directly in the
DataFusion scan (rather than bare column names), and emitting sufficient statistics for
decomposable statistical aggregates (`STDDEV`/`VARIANCE` family).

## Background

* The scan UDF reads its ScanSpec from a single JSON VARCHAR input column.
* The projection may carry rendered DataFusion SQL select-list expressions (not just bare
  column names); the UDF places them verbatim in its SELECT list.
* Partial aggregates for statistical functions are emitted as `(count, sum, sum_sq)`
  sufficient statistics; the outer wrapper reconstructs variance/stddev from these.
* Only SDK Value types cross the `.so` boundary; no Arrow types.
* Credentials MUST NOT appear in any error message.

## Scenarios

### Scenario: Scan projects rendered select-list expressions

* *GIVEN* a scan spec whose projection carries rendered DataFusion SQL select-list expressions (e.g. `UPPER("NAME")`, `("PRICE" * "QTY")`, `date_part('YEAR', "ORDER_DATE")`) rather than bare column names
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL place each rendered select-list expression verbatim in its DataFusion SELECT list, in spec order
* *AND* the UDF SHALL emit one output row per scanned source row carrying the evaluated expression values in that order
* *AND* the EMITS declaration in the scan-driving SQL MUST match the rendered select-list in order and result type, with types derived from the `selectListDataTypes` array in the pushdown request
* *AND* no Arrow type SHALL cross the `.so` boundary

### Scenario: Scan emits sufficient statistics for a decomposable statistical aggregate

* *GIVEN* a scan spec requesting a partial `STDDEV`/`STDDEV_POP`/`STDDEV_SAMP`/`VARIANCE`/`VAR_POP`/`VAR_SAMP` over a column
* *WHEN* the scan UDF computes its shard's partial aggregate
* *THEN* the UDF SHALL emit the sufficient-statistics triple `COUNT(col)`, `SUM(col)`, and `SUM(col*col)` for that column rather than a per-shard standard deviation or variance
* *AND* the partial count SHALL exclude rows where the target column is NULL, so the merged statistic matches single-node semantics
* *AND* an empty shard (or empty group) SHALL emit a partial count of zero with NULL partial sums that the wrapper's merge ignores
* *AND* no Arrow type SHALL cross the `.so` boundary
