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
  Issues #431 and #201 add three more, `exa_trunc`, `exa_round`, and `exa_date_trunc`, which
  `crates/vs-expression` emits for `TRUNC`, `ROUND`, and `DATE_TRUNC` (see
  `sql-functions/vs-expression-translator-trunc-round`).
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

<!-- DELTA:NEW -->
### Scenario: The scan session registers the Exasol TRUNC, ROUND, and DATE_TRUNC functions every pushed expression needs

* *GIVEN* a scan session built by `build_session_context`, the one production session builder that `run_scan_one` hands to all three run paths
* *WHEN* the session is constructed, before any spec-derived SQL is planned
* *THEN* the session SHALL register three scalar functions under the exact names `crates/vs-expression` exports for `TRUNC`, `ROUND`, and `DATE_TRUNC`, read from those constants rather than restated as literals
* *AND* the registration SHALL happen for every scan spec, unconditionally, because every run path splices the same rendered `filter`, `projection`, `order_by`, `group_keys`, and aggregate-argument strings
* *AND* a spec whose SQL calls none of the three functions SHALL be unaffected: the scenarios of `datafusion-scan/scan-execution-plan-shape` and the Parquet pruning parity `tests/scan_parquet_pruning.rs` asserts MUST pass unedited
* *AND* each function SHALL be declared `Immutable` and SHALL evaluate one whole Arrow array per call, computing the power of ten for a literal scale once per call rather than once per row
* *AND* the type each function declares to DataFusion SHALL equal the type of the array it returns, for every accepted argument type, because that mismatch is the `round` defect issue #431 measured
* *AND* `exa_trunc` and `exa_round` SHALL accept a first argument of every integer type, `Decimal128` with a non-negative scale, `Float32`, `Float64`, `Date32`, and `Timestamp` in every unit with or without a zone, and `exa_date_trunc` SHALL accept `Date32` and every `Timestamp`
* *AND* any other argument type, such as a string or a Boolean, SHALL fail when DataFusion plans the query, with a message that names the function and the type, as DataFusion's own `trunc` fails for a string today. Native Exasol converts a VARCHAR argument instead, and this gap is a scoped exception tracked as `(#TBD)`
* *AND* a conjunct that calls one of the functions SHALL prune no file and no row group by itself, exactly as the conjunct with the DataFusion built-in did not, because neither shape is a column-against-literal comparison
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A TRUNC or ROUND failure reaches the user as the function's own error

* *GIVEN* a scan in which `exa_trunc` or `exa_round` raises: a `ROUND` overflow past 36 digits, a non-finite DOUBLE step, a datetime field overflow or underflow, an invalid date, a time format on a DATE, or a result label the proleptic Gregorian calendar lacks
* *WHEN* the failure leaves the scan
* *THEN* the message SHALL name `TRUNC` or `ROUND` and SHALL contain Exasol's own phrase for the cause: `numeric value out of range`, `datetime field overflow`, `datetime field underflow`, `invalid date value`, or `unsupported format`
* *AND* the message MUST NOT carry the storage-read framing `scan failed: assigned data could not be read`
* *AND* `classify_scan_error` SHALL recognise the error by type, through every wrapping layer, and MUST NOT recognise it by matching text in a message string
* *AND* the function SHALL also record its first failure as a typed value on the session that registered it, because the Parquet row filter flattens a predicate error to text, and the one dispatcher all run paths funnel through SHALL reframe a failed scan from that record under the rules the checked division follows: the recorded failure replaces the surfaced one, a memory exhaustion is composed after it, and the composed message is redacted at the dispatcher
* *AND* when the same scan also recorded a checked division, the dispatcher SHALL report the checked division, so the behavior issue #370 fixed stays unchanged
* *AND* a failure inside a predicate SHALL share the evaluation-set divergence tracked as `(#392)`: DataFusion can skip the function for a row another conjunct removed, so a query native Exasol fails can succeed, and it can evaluate the function for a row a guarding conjunct would exclude, so a query native Exasol completes can fail. A query that completes returns native Exasol's rows
* *AND* the failure SHALL reach the user with SQL state `22002` where native Exasol reports `22003`, `22008`, `22769`, or `22770`, a named trade-off recorded in `docs/semantic-differences.md`
* *AND* a call whose arguments are all literals SHALL surface the same message, which a unit test establishes rather than assumes, because DataFusion MAY evaluate such a call while planning, and the run paths report a planning failure without `classify_scan_error`
* *AND* credentials MUST NOT appear in the surfaced message
<!-- /DELTA:NEW -->
