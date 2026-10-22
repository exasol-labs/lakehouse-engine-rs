# Feature: VS Expression Translator, Floating-Point Division

Renders `FLOAT_DIV` (Exasol's `/`) as true `DOUBLE` division. The DataFusion dialect calls a checked division function that raises on a zero divisor in every position, and the Exasol dialect keeps the bare `/` operator. `FLOAT_DIV` is an operator wire name, not an Exasol function name, so it never takes the verbatim rule in `sql-functions/vs-expression-translator-scalar-fns`.

## Background

* Exasol's `FN_FLOAT_DIV` always divides as true `DOUBLE` and raises `data exception - division by zero` (SQL state `22012`) on a zero divisor. DataFusion's `/` is operand-typed: it truncates integer operands and caps decimal results at scale 6, so a bare `/` returns wrong values.
* The translator has no operand-type context, so one type-blind rendering serves every operand type.
* The DataFusion dialect renders `vs_checked_float_div(<left>, <right>)`. The function coerces both operands to `Float64`, divides, propagates NULL, and raises when its result is not finite. The check sits at the division, so a zero divisor fails the same way in a projection and in a filter predicate.
* The Exasol dialect renders the bare `(<left> / <right>)`, because Exasol's `/` is `FN_FLOAT_DIV`.
* Exasol admits no non-finite `DOUBLE`, so a stored `NaN` or `+/-Inf` operand makes the checked division raise. This is a named Exasol target-type trade-off.
* A guard protects a division only when DataFusion orders the guard first. DataFusion's `PushDownFilter` moves every cheap conjunct (columns, literals, casts, comparison and arithmetic operators) ahead of every expensive one and keeps the written order within each class. Any function call is expensive, and the checked division is a function call. The Parquet row filter evaluates the conjuncts in that order and narrows the row selection after each one, because `datafusion.execution.parquet.reorder_filters` stays `false`. An inline `UNION ALL` of literal rows raises in neither order, so it is no oracle for a guarded division.

## Scenarios

### Scenario: FLOAT_DIV renders true float division in the DataFusion dialect

* *GIVEN* a VS expression node of type `function_scalar` named `FLOAT_DIV`, the wire name for Exasol's `/`, advertised as `FN_FLOAT_DIV`
* *AND* Exasol's `FN_FLOAT_DIV` always performs true float division and always results in `DOUBLE`, for every operand-type combination, verified live against the Docker Exasol container: `7/2 = 3.5` (NOT `3`), `CAST(711.56 AS DECIMAL(18,2))/CAST(7 AS DECIMAL(18,0)) = 101.65142857142857` at full double precision (NOT scale-capped), and a CTAS over each operand pairing types the result column `DOUBLE` in `EXA_ALL_COLUMNS`
* *AND* the translator carries no operand-type context of its own, so it cannot render one shape for integer operands and another for decimal or double operands
* *WHEN* the node is rendered through the DataFusion-dialect entry points (`render_expression`, `render_expression_safe`, `render_df_filter_safe`), the ones whose output DataFusion's SQL frontend parses inside the scan UDF
* *THEN* the translator SHALL return `vs_checked_float_div(<left>, <right>)`, a call to the single checked-division function the crate declares, with both operands rendered recursively and neither wrapped in a CAST
* *AND* the function name SHALL be exported from `crates/vs-expression` as one public constant that the rendering itself reads, so the name has ONE owner and the scan crate that registers the function cannot drift from the crate that emits it
* *AND* that constant's documentation SHALL state the full contract the registered implementation MUST satisfy: two arguments, both coerced to `Float64`, a `Float64` result, NULL propagated, and an error raised when the result is not finite
* *AND* it MUST NOT return the bare `(<left> / <right>)`, which DataFusion evaluates by operand type and which returns silently wrong values for all three operand classes: `L_ORDERKEY / L_LINENUMBER` at `(7, 2)` gives `3.0` against `3.5`; `C_DECIMAL_A / 7` at `40.99` gives `5.855714` against `5.855714285714286`; `C_DECIMAL_A / C_DECIMAL_B` (`DECIMAL(9,2)/DECIMAL(20,4)`) gives `0.000102` against `0.000102474999897525`; and `COUNT(*) WHERE L_ORDERKEY / L_LINENUMBER > 3 AND L_ORDERKEY = 7` gives `1` against `2`, so a truncated quotient changes row counts as well as values
* *AND* it MUST NOT return `(CAST(<left> AS DOUBLE) / <right>)` either: that shape fixes the truncation but leaves the `/` operator deciding the zero-divisor outcome
* *AND* decimal/decimal division SHALL NOT be treated as an already-correct control case: DataFusion's `Decimal128(24,6)` quotient is correct only when both operands carry few enough significant digits that scale 6 loses none
* *AND* the rendering SHALL stay type-blind and unconditional, correct for every operand-type combination the two lakehouse formats can present, `Int64`, `Decimal128`, `Float32` and `Float64` on either side, in any pairing, because the called function, not the SQL text, performs the coercion
* *AND* NULL SHALL propagate unchanged, a NULL left operand, a NULL right operand, and a NULL left operand over a zero right operand each yield NULL, not an error and not `NaN`
* *AND* the resulting `Float64` column SHALL match the `DOUBLE PRECISION` EMITS type the adapter declares for the item from Exasol's own `selectListDataTypes`, live-confirmed as `{"type":"DOUBLE"}` with `"emit_exa_types":["DOUBLE PRECISION"]`, so the emit-boundary type coercion keeps the column on its zero-copy fast path
* *AND* the result SHALL equal native Exasol's bit-exactly for an integer (scale-0) numerator and within ~1 ULP for a non-zero-scale decimal numerator, because Exasol's own decimal division is not bit-identical to converting the numerator to `DOUBLE` first, over a `DECIMAL(18,2)` column, 3335 rows × divisors `{2,3,7,11,13}`, 1027 results (31%) differed by exactly one ULP, max relative difference `3.17e-16`, while the same sweep over `DECIMAL(p,0)/DECIMAL(p,0)` was bit-exact at 0 of 3335

### Scenario: The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator

* *GIVEN* the same `function_scalar` node named `FLOAT_DIV`
* *WHEN* the node is rendered through the Exasol-dialect entry points (`render_expression_exasol`, `render_expression_exasol_safe`, `render_df_filter_exasol_safe`), the ones whose output Exasol's own core engine parses
* *THEN* the translator SHALL return the bare `(<left> / <right>)`, because Exasol's `/` IS `FN_FLOAT_DIV`: it already divides as `DOUBLE` for every operand type AND it already raises `22012` on a zero divisor, so it needs neither the cast nor the checked call
* *AND* the two dialects SHALL therefore DIVERGE on the same `FLOAT_DIV` node, `vs_checked_float_div(<l>, <r>)` in the DataFusion dialect, `(<l> / <r>)` in the Exasol dialect
* *AND* every Exasol-dialect consumer's SQL SHALL stay byte-identical, the qualified single-table wrapper, the N-scan join wrapper, the grouped merge, the single-group scalar-over-aggregate merge (`render_scalar_over_merge`, which calls `render_expression_exasol`), and the self-applied WHERE path
* *AND* the `/` characters in the AVG and statistical merge fragments (the scalar-over-aggregate merge's `SUM(<partial>) / NULLIF(SUM(<partial>), 0)` and the König–Huygens numerator) SHALL be unaffected in both dialects, because they are adapter-authored merge SQL that never passes through the translator's `FLOAT_DIV` arm

### Scenario: A pushed-down division by zero fails the query rather than returning a wrong value

* *GIVEN* a pushed-down `FLOAT_DIV` whose right operand evaluates to zero for at least one scanned row, the numerator being non-zero
* *AND* native Exasol raises `data exception - division by zero` (SQL state `22012`) for every operand pairing including `DOUBLE/DOUBLE`, verified live and column-driven so nothing is constant-folded, and never returns NULL and never returns infinity
* *WHEN* the DataFusion-dialect rendering `vs_checked_float_div(<left>, <right>)` is evaluated in the scan and the result is projected
* *THEN* the checked division SHALL raise, the scan SHALL fail, and the surfaced message SHALL name the cause as a division by zero
* *AND* the query SHALL NOT return a wrong value and SHALL NOT return NULL for the affected row
* *AND* the outcome SHALL differ from native Exasol only in the message and the raising layer: the scan's own division raises, where native Exasol raises in its engine, and the message uses Exasol's own division-by-zero vocabulary
* *AND* the SQL state the Exasol engine attaches to the scan UDF's error SHALL be recorded from a live run rather than assumed
* *AND* Exasol SHALL be understood to admit no non-finite `DOUBLE` at all, `CAST('inf' AS DOUBLE)`, `CAST('Infinity' AS DOUBLE)` and `CAST('nan' AS DOUBLE)` are each rejected at `22018`, and `1E400` at `22003`, which is why raising at the division is the only outcome consistent with what Exasol can represent

### Scenario: A division by zero inside a filter predicate fails the query rather than changing the row count

* *GIVEN* a pushed-down `FLOAT_DIV` by zero inside a `WHERE` filter predicate over the 20-row `FACT_LINEITEM` fixture, with each pushed filter read out of the `EXPLAIN VIRTUAL` `PUSHDOWN_SQL` ScanSpec
* *AND* native Exasol raises `data exception - division by zero` (SQL state `22012`) for this shape in predicate position exactly as in projection position, verified against an inline-literal-subquery native oracle
* *WHEN* the pushed filter is evaluated in the scan
* *THEN* the checked division SHALL raise and the query SHALL FAIL, in both comparison directions and for both the `x/0` and the `0/0` shape
* *AND* it MUST NOT succeed with a row count that disagrees with native Exasol: `(0 < (CAST("L_ORDERKEY" AS DOUBLE) / ("L_LINENUMBER" - "L_LINENUMBER")))` returning 20 of 20 rows, the same shape with `< 0` returning 0 of 20, or the `0/0` shape returning 0 of 20 for `> 0` and 20 of 20 for `< 0`
* *AND* the same outcome SHALL hold for a broadcast-join fact-leg filter whose conjunct lands in the fact leg as `((DATE '2024-01-05' <= "O_ORDERDATE") AND (0 < (CAST("O_ORDERKEY" AS DOUBLE) / ("O_CUSTKEY" - "O_CUSTKEY"))))`, with `EXPLAIN VIRTUAL` confirming the broadcast plan is retained (a `"join":{` common blob, no `LHS_T0` two-scan wrapper), because the join path adds no divergence of its own
* *AND* the raising SHALL come from the same single checked-division function the projection path uses, so the two positions cannot drift apart
* *AND* a NULL divisor SHALL still yield NULL and SHALL NOT raise, so `WHERE <a> / NULL > 0` returns no rows rather than failing
* *AND* the error SHALL be raised only for a row whose division the scan actually evaluates: DataFusion MAY skip the division for a row that another conjunct, file pruning, row-group pruning, or a LIMIT already removed, and whether native Exasol raises for such a row is not measured, so this residual is a tracked known gap scoped to error-raising alone
* *AND* the rows a successful query returns SHALL NOT be affected by that residual: a row reaches the result only when its division was evaluated and finite, so a query that does not raise returns exactly the rows native Exasol returns
* *AND* a GUARDED division with a cheap guard SHALL return the guarded rows without raising in BOTH conjunct orders: over the 20-row fixture, `WHERE (L_LINENUMBER - 1) <> 0 AND 0 < L_ORDERKEY / (L_LINENUMBER - 1)` and its reversed order each return 10 rows, while native Exasol raises `22012` for the same division without the guard. Over a native Exasol table, the reversed order also raises `22012`, which the suppression direction of the tracked known gap covers
* *AND* that protection SHALL come from conjunct order, not from batch selectivity: DataFusion's `PushDownFilter` places cheap conjuncts before expensive ones, the checked division is an expensive function call, and the Parquet row filter evaluates the conjuncts in that order while `datafusion.execution.parquet.reorder_filters` stays `false`
* *AND* the checked division MUST stay `Immutable`: DataFusion neither reorders a conjunction that holds a `Volatile` expression nor pushes such a filter into the scan, so a `Volatile` declaration would remove the only protection a guarded division has
* *AND* a guard that is itself a function call, such as `ABS(L_LINENUMBER - 1) <> 0`, SHALL protect the division only when written first: written second, the query raises `data exception - division by zero`, and the same query over a native Exasol table raises `22012`

### Scenario: Zero divided by zero fails the query instead of reaching the NaN-at-emit gap

* *GIVEN* a pushed-down `FLOAT_DIV` whose numerator AND denominator both evaluate to zero for a scanned row
* *WHEN* the DataFusion-dialect rendering is evaluated, in projection position or in predicate position
* *THEN* the checked division SHALL raise, because `0.0 / 0.0` is `NaN` and `NaN` is not finite, and the query SHALL fail with the same division-by-zero message as any other zero divisor, matching native Exasol, which raises `22012` for this shape too
* *AND* the projected `0/0` SHALL NOT return a silent NULL: the raw-scan path emits Arrow IPC bytes through `ctx.emit_batch` with no per-value check, so a `NaN` would reach Exasol as NULL, and the checked division prevents this by raising before the value is emitted
* *AND* a pushed `FLOAT_DIV` SHALL NOT produce a `NaN` at all, so it is not a route into the NaN-at-emit gap for any numerator type
* *AND* the NaN-at-emit gap SHALL remain a tracked known gap that this feature does not close: it covers every other `NaN` that reaches the raw-scan emit boundary, including an out-of-domain math kernel and a `NaN` stored in the source table, and `arrow_value_at` errors on the partial-aggregate path where `emit_batch` does not
* *AND* this feature MUST NOT close the remaining gap by widening `arrow_value_at`'s check from `is_nan()` to `!is_finite()`, because that boundary cannot distinguish a COMPUTED non-finite value from one legitimately STORED in the source table (Iceberg types the column "64-bit IEEE 754 floating point", Delta "Double precision (64-bit) IEEE 754 floating point number", and Parquet `DOUBLE` admits `±Inf` and `NaN`), so such a check would break reading a table that legitimately contains them
* *AND* this feature MUST NOT close the gap by rendering `NULLIF(<right>, 0)` either: NULL is exactly the wrong answer already observed, it makes a genuine zero divisor indistinguishable from a NULL divisor, and it would suppress the loud failure the `x/0` case already produces

### Scenario: FLOAT_DIV stays outside the verbatim rule in both dialects

* *GIVEN* the crate's single per-function declaration of each translated `function_scalar` name, which both gates the dispatch and drives the enforcing Exasol-dialect sweep test
* *AND* `FLOAT_DIV` declared with the dialect-shaped form rather than the verbatim form, alongside `ADD`, `SUB`, `MULT`, and `NEG`
* *WHEN* the sweep test renders every declared name through `render_expression_exasol` and compares it against that name's declared expectation
* *THEN* `FLOAT_DIV` SHALL keep its shaped declaration and MUST NOT be moved to the verbatim form, because Exasol has no function called `FLOAT_DIV`, a verbatim rendering would emit `FLOAT_DIV(<l>, <r>)`, which Exasol rejects the same way it rejects `SIGNUM` and `STRPOS` (`function or script <NAME> not found`, SQL code 42000)
* *AND* no Exasol-dialect fragment SHALL contain the checked-division function name, because Exasol has no such function and the name would fail at 42000 exactly as `SIGNUM` and `STRPOS` do
