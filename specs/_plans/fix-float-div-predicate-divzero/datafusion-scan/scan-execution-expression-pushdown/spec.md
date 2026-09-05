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

<!-- DELTA:NEW -->
* A rendered expression MAY call a function the scan session registers rather than a DataFusion
  built-in. The precedent is `lakehouse_render_nested_json`, registered by
  `build_session_context` for `datafusion-scan/nested-json-rendering`. Issue #370 adds the second
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
  `(#TODO-scalar-fns)`.
<!-- /DELTA:NEW -->

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

<!-- DELTA:NEW -->
### Scenario: The scan session registers the checked float-division function every pushed expression needs

* *GIVEN* a scan session built by `build_session_context`, the one production session builder that `run_scan_one` hands to all three run paths
* *WHEN* the session is constructed, before any spec-derived SQL is planned
* *THEN* the session SHALL register a scalar function under the exact name `crates/vs-expression` exports as its checked-division constant, read from that constant rather than restated as a literal
* *AND* the registration SHALL happen for EVERY scan spec, unconditionally, without inspecting whether the spec's SQL contains a division, because the raw-row path, the broadcast-join path, and both partial-aggregate paths all splice the same rendered `filter`, `projection`, `order_by`, `group_keys`, and aggregate-argument strings
* *AND* a spec whose rendered SQL contains no checked division SHALL be unaffected: the registration adds one entry to the session's function registry and changes no generated SQL, no plan shape, and no scan result
* *AND* the function SHALL be declared `Immutable`, so DataFusion treats two evaluations over equal input as equal and the expression remains eligible for the same plan-level handling any other scalar expression receives
* *AND* the function SHALL evaluate one whole Arrow array per call rather than one row per call, so the per-row cost stays a cast, a divide, and a finiteness test, and the change adds no per-row function-call overhead over the `/` operator it replaces
* *AND* the plan shape a scan produces SHALL be unchanged for every spec whose SQL contains no division: the scenarios of `datafusion-scan/scan-execution-plan-shape` and the Parquet pruning parity `tests/scan_parquet_pruning.rs` asserts MUST pass unedited
* *AND* the plan-shape effect on a spec that DOES contain a division SHALL be recorded rather than assumed: DataFusion cannot derive a min/max pruning bound from a function it does not know, so a conjunct containing a checked division prunes no file and no row group by itself, exactly as the `/` operator's own conjunct did not, because neither shape is a column-against-literal comparison
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A checked float division raises rather than producing a non-finite value

* *GIVEN* the registered checked-division function evaluating `vs_checked_float_div(<left>, <right>)` over one batch, with operands of any pairing of `Int32`, `Int64`, `Decimal128`, `Float32`, and `Float64` that Iceberg or Delta can present
* *WHEN* it evaluates a row
* *THEN* it SHALL coerce both operands to `Float64` and SHALL return `Float64` for every input pairing, so the caller needs no CAST of its own on either side
* *AND* a NULL in EITHER operand SHALL yield NULL for that row, with no error, including a NULL numerator over a zero divisor
* *AND* a row whose divisor is zero SHALL raise an error whose message contains the phrase `division by zero`, matching Exasol's own vocabulary (`data exception - division by zero`, SQL state `22012`), for a zero numerator (`0/0`) exactly as for a non-zero one
* *AND* negative zero SHALL be treated as zero, because IEEE-754 `-0.0` equals `0.0`
* *AND* a row whose result is not finite for any OTHER reason, such as an overflow to `±Inf` from a finite numerator over a tiny divisor or a non-finite operand read from the source table, SHALL also raise, with a message naming a numeric value out of range rather than a division by zero, so the two causes stay distinguishable in a support case
* *AND* the function MUST NOT return NULL for any of these cases, MUST NOT return a non-finite value, and MUST NOT silently substitute a finite one
* *AND* a legitimately stored `±Inf` or `NaN` operand raising here SHALL be recorded as a deliberate trade-off, not a defect: Exasol admits no non-finite `DOUBLE`, the same value already fails at the emit boundary in projection position, and reading the column without a division is untouched by this function
* *AND* the error SHALL reach the user WITHOUT the storage-read framing `classify_scan_error` applies to a scan failure, because `scan failed: assigned data could not be read` misnames a user arithmetic error
* *AND* the classifier SHALL recognise the error BY TYPE, through a dedicated error carried on the DataFusion error chain, and MUST NOT recognise it by matching text in a message string
* *AND* the message SHALL name a division by zero on EVERY route the error can take out of the scan, which MUST be established rather than assumed: DataFusion MAY fold a division over two literal operands during optimization, and the three scan paths surface a planning failure through `UdfError::User(format!("DataFusion SQL error: {e}"))` and `"partial aggregate SQL error: {e}"` rather than through `classify_scan_error`, so a fold that raises at plan time would otherwise bypass the framing above
* *AND* the aggregate paths SHALL raise identically: a checked division inside a pushed aggregate argument (`SUM(<a> / <b>)`) is spliced into the same SQL by `build_partial_agg_sql_filtered` and `build_grouped_partial_agg_sql`, and a zero divisor there SHALL fail the query rather than reach `arrow_value_at`'s separate `is_nan()` check
* *AND* credentials MUST NOT appear in the surfaced message, the rule every other scan error path already follows
* *AND* no Arrow type SHALL cross the `.so` boundary
<!-- /DELTA:NEW -->
</content>
</invoke>
