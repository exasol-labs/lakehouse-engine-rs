# Feature: DataFusion Scan Execution — Expression Pushdown

Extends `datafusion-scan/scan-execution` with the two new execution capabilities enabled
by the `add-capability-alignment` plan: rendering select-list expressions directly in the
DataFusion scan (rather than bare column names), and emitting sufficient statistics for
decomposable statistical aggregates (`STDDEV`/`VARIANCE` family).

<!-- DELTA:CHANGED -->
## Background

* The scan UDF reads its ScanSpec from a single JSON VARCHAR input column.
* The projection may carry rendered DataFusion SQL select-list expressions (not just bare
  column names); the UDF places them verbatim in its SELECT list.
* Partial aggregates for statistical functions are emitted as `(count, sum, sum_sq)`
  sufficient statistics; the outer wrapper reconstructs variance/stddev from these.
* Only SDK Value types cross the `.so` boundary; no Arrow types.
* Credentials MUST NOT appear in any error message.

* A rendered expression MAY call a function the scan session registers rather than a DataFusion
  built-in. The precedent is `lakehouse_render_nested_json`, registered by
  `build_session_context` for `scan-types/nested-json-rendering`. Issue #370 adds the second
  such function, `vs_checked_float_div`, which `crates/vs-expression` emits for every
  DataFusion-dialect `FLOAT_DIV` node (see `sql-comprehension/vs-expression-translator-float-div`).
* `build_session_context` is the ONE place a scan session is built in production. All three run
  paths take the session from it: `run_raw_scan_with_session`, `run_join_scan_with_session`, and
  `run_partial_aggregate`, dispatched by `run_scan_one`. Registering a function there therefore
  reaches every pushed expression the scan can evaluate: a projection item, a `WHERE` filter, an
  `ORDER BY` key, a `GROUP BY` key, a broadcast-join fact-leg filter, and an aggregate argument.
* A checked division is only meaningful because Exasol has no non-finite `DOUBLE`. Exasol rejects
  `CAST('inf' AS DOUBLE)` and `CAST('nan' AS DOUBLE)` at `22018` and `1E400` at `22003`. A
  non-finite value produced inside the scan can therefore never be a correct answer: in projection
  position the engine already rejects it at the emit boundary, and in predicate position it
  silently changed the row count, which is issue #370.
* This is not the same check as `arrow_value_at`'s `is_nan()` guard, and it MUST NOT be conflated
  with it. `arrow_value_at` sees a value read from a column and cannot tell a computed non-finite
  from a stored one. The checked division sees only the two operands of a division the pushdown
  itself synthesised.
* The checked division covers `FLOAT_DIV` and nothing else. `crates/lakehouse-engine/src/adapter/capabilities.rs`
  also advertises `FN_SQRT`, `FN_LN`, `FN_LOG`, `FN_ACOS`, `FN_ASIN`, `FN_EXP`, `FN_POWER`, and
  `FN_MOD`. Each is translated into a pushed predicate, and each can produce `NaN` or `±Inf` from
  in-domain column data. `WHERE SQRT(<negative_col>) > 0` and `WHERE EXP(<large_col>) > 0`
  reproduce issue #370's mechanism exactly: the comparison consumes a non-finite value inside the
  scan, and no emit-boundary check ever sees it. Those producers keep the gap this plan closes for
  `FLOAT_DIV`, and they are recorded as a tracked exception rather than a silent gap
  `(#394)`.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: Scan projects rendered select-list expressions

* *GIVEN* a scan spec whose projection carries rendered DataFusion SQL select-list expressions (e.g. `UPPER("NAME")`, `("PRICE" * "QTY")`, `date_part('YEAR', "ORDER_DATE")`) rather than bare column names
* *WHEN* the scan UDF runs for that spec
* *THEN* the UDF SHALL place each rendered select-list expression verbatim in its DataFusion SELECT list, in spec order
* *AND* the UDF SHALL emit one output row per scanned source row carrying the evaluated expression values in that order
* *AND* the EMITS declaration in the scan-driving SQL MUST match the rendered select-list in order and result type, with types derived from the `selectListDataTypes` array in the pushdown request
* *AND* no Arrow type SHALL cross the `.so` boundary
