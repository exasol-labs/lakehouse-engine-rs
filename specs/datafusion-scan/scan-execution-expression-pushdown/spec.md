# Feature: DataFusion Scan Execution, Expression Pushdown

The scan UDF renders pushed select-list expressions in its DataFusion SELECT list and emits sufficient statistics for the `STDDEV`/`VARIANCE` family of aggregates. It also registers the checked float-division function that those expressions call.

## Background

* The projection may carry rendered DataFusion SQL select-list expressions, not only bare column names.
* A partial statistical aggregate is emitted as `(count, mean, m2)`. The outer wrapper reconstructs variance and standard deviation from them.
* A rendered expression may call a function the scan session registers instead of a DataFusion built-in. `crates/vs-expression` emits `vs_checked_float_div` for every DataFusion-dialect `FLOAT_DIV` node.
* `build_session_context` is the one place production builds a scan session. `run_scan_one` hands that session to `run_raw_scan_with_session`, `run_join_scan_with_session`, and `run_partial_aggregate`, so a function registered there reaches every pushed expression: projection item, `WHERE` filter, `ORDER BY` key, `GROUP BY` key, broadcast-join fact-leg filter, and aggregate argument.
* Exasol has no non-finite `DOUBLE`, so a non-finite value computed inside the scan is never a correct answer.
* The checked division is a different check from the `is_nan()` guard in `arrow_value_at`. That guard sees a value read from a column and cannot tell a computed non-finite from a stored one.

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
* *THEN* the UDF SHALL emit the sufficient-statistics triple `COUNT(col)`, the mean, and `m2` (the sum of squared deviations from the mean) for that column rather than a per-shard standard deviation or variance
* *AND* the UDF SHALL compute the mean and `m2` in double precision with DataFusion's `VAR_POP`, and MUST NOT square the raw value, because `SUM(col * col)` overflows the column's integer type
* *AND* a shard whose non-NULL values are all equal SHALL emit that value as its mean and exactly `0` as its `m2`, because DataFusion's partition merge rounds both
* *AND* the partial count SHALL exclude rows where the target column is NULL, so the merged statistic matches single-node semantics
* *AND* an empty shard (or empty group) SHALL emit a partial count of zero with a NULL mean and `m2` that the wrapper's merge ignores
* *AND* no Arrow type SHALL cross the `.so` boundary

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

### Scenario: A checked float division raises rather than producing a non-finite value

* *GIVEN* the registered checked-division function evaluating `vs_checked_float_div(<left>, <right>)` over one batch, with operands of any pairing of `Int32`, `Int64`, `Decimal128`, `Float32`, and `Float64` that Iceberg or Delta can present
* *WHEN* it evaluates a row
* *THEN* it SHALL coerce both operands to `Float64` and SHALL return `Float64` for every input pairing, so the caller needs no CAST of its own on either side
* *AND* a NULL in EITHER operand SHALL yield NULL for that row, with no error, including a NULL numerator over a zero divisor
* *AND* a row whose divisor is zero SHALL raise an error whose message contains the phrase `division by zero`, matching Exasol's own vocabulary (`data exception - division by zero`, SQL state `22012`), for a zero numerator (`0/0`) exactly as for a non-zero one
* *AND* negative zero SHALL be treated as zero, because IEEE-754 `-0.0` equals `0.0`
* *AND* a row whose divisor is NON-ZERO and whose result is not finite SHALL also raise, with a message naming a numeric value out of range rather than a division by zero, so the two causes stay distinguishable in a support case: an overflow to `±Inf` from a finite numerator over a tiny divisor, or a non-finite operand read from the source table and divided by a non-zero value
* *AND* a row whose divisor IS zero SHALL name a division by zero whatever its numerator is, including a non-finite value read from the source table, because the classification tests the result's finiteness first and reads the divisor second. That ordering is the deliberate one: it is also what makes a `-0.0` divisor a division by zero
* *AND* the function MUST NOT return NULL for any of these cases, MUST NOT return a non-finite value, and MUST NOT silently substitute a finite one
* *AND* a legitimately stored `±Inf` or `NaN` operand raising here SHALL be recorded as a deliberate trade-off, not a defect: Exasol admits no non-finite `DOUBLE`, the same value already fails at the emit boundary in projection position, and reading the column without a division is untouched by this function
* *AND* the error SHALL reach the user WITHOUT the storage-read framing `classify_scan_error` applies to a scan failure, because `scan failed: assigned data could not be read` misnames a user arithmetic error
* *AND* the classifier SHALL recognise the error BY TYPE and MUST NOT recognise it by matching text in a message string
* *AND* the error SHALL be carried on the DataFusion error chain, where `classify_scan_error` recognises it by walking `Error::source` and downcasting through every wrapping layer (`External`, `Context`, `ArrowError::ExternalError`, `Arc`)
* *AND* the error chain SHALL NOT be the only carrier, because it does not survive every route, which was MEASURED rather than assumed: `datafusion-datasource-parquet` 54.1 `row_filter.rs` flattens ANY predicate error into `ArrowError::ComputeError(format!("Error evaluating filter predicate: {e:?}"))`, so a checked division raised inside a predicate DataFusion pushed into the Parquet row filter — issue #370's own route — reaches the classifier with its type destroyed and no longer distinguishable from a storage failure. Live pre-amendment evidence: `scan failed: assigned data could not be read: Parquet error: External: Compute error: Error evaluating filter predicate: External(ZeroDivisor { numerator: 1.0, divisor: 0.0 })`
* *AND* the function SHALL therefore ALSO record its first failure's message in the session's failure slot, which the scan's other raising functions share, and the ONE dispatcher all three run paths funnel through SHALL reframe a failed scan from it. Recovering the failure from the flattened message text is FORBIDDEN, being exactly the coupling this scenario rules out. The recorded value is scoped to one scan invocation, so no failure survives into another. A recorded failure means the division DID raise during that scan, so naming it is never a false report
* *AND* the recorded division SHALL REPLACE the surfaced failure, so the storage-read framing appears NOWHERE in the message, not merely after the division. On the flattened row-filter route the surfaced failure IS the same division wearing that framing, so appending it would republish `scan failed: assigned data could not be read` on this scenario's own primary route, defeating the clause above that forbids it. Nothing separates that case from a genuinely unrelated failure in the same classification branch without matching DataFusion's `Error evaluating filter predicate` wording, the coupling this scenario forbids
* *AND* the ONE exception SHALL be a memory exhaustion, which the dispatcher SHALL COMPOSE with the division rather than replace: the division first, because the user's own SQL caused it, then the memory exhaustion, so the `ResourcesExhausted` signal the bounded-execution guarantee rests on is never discarded. It is the one surfaced failure separable from the flattened division WITHOUT message matching, because `classify_scan_error` recognises `ResourcesExhausted` on the typed error root before any text exists. The dispatcher MAY recognise it from the label `classify_scan_error` writes, which is this codebase's own wording owned by a single constant read by both sides, and which is therefore NOT the forbidden coupling to DataFusion's wording
* *AND* the composed message SHALL be redacted at the dispatcher rather than trusted, because the run paths classify with the fact side's credentials alone while the dispatcher holds the union that also covers a join's dimension side
* *AND* an unrelated storage failure raised in another partition of a scan that also divided by zero SHALL therefore be MASKED, which is an accepted limitation rather than an oversight: the collision is real but rarer than this scenario's own route and has never been observed live, whereas the storage framing on a user's own division is MEASURED and is what `e2e_float_div_by_zero_in_filter_fails_like_native_exasol` asserts against
* *AND* the message SHALL name a division by zero on EVERY route the error can take out of the scan, which MUST be established rather than assumed: DataFusion MAY fold a division over two literal operands during optimization, and the three scan paths surface a planning failure through `UdfError::User(format!("DataFusion SQL error: {e}"))` and `"partial aggregate SQL error: {e}"` rather than through `classify_scan_error`, so a fold that raises at plan time would otherwise bypass the framing above. MEASURED on DataFusion 54.1: `ctx.sql` plans `vs_checked_float_div(0, 0)` successfully and the raise arrives from the stream, the same route a column operand takes, so no fold happens at plan time and those four planning sites need no framing of their own. Should a later DataFusion fold it at plan time, `checked_float_div_over_two_literals_surfaces_a_division_by_zero_message` fails and they do
* *AND* the aggregate paths SHALL raise identically: a checked division inside a pushed aggregate argument (`SUM(<a> / <b>)`) is spliced into the same SQL by `build_partial_agg_sql_filtered` and `build_grouped_partial_agg_sql`, and a zero divisor there SHALL fail the query rather than reach `arrow_value_at`'s separate `is_nan()` check
* *AND* credentials MUST NOT appear in the surfaced message, the rule every other scan error path already follows
* *AND* no Arrow type SHALL cross the `.so` boundary
