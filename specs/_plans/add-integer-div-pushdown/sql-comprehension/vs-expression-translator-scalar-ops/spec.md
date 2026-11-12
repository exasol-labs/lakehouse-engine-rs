# Feature: VS Expression Translator, Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators and the safe entry points. CAST targets are in `sql-comprehension/vs-expression-translator-cast`, named scalar functions in `sql-functions/vs-expression-translator-scalar-fns`, date/time functions in `sql-functions/vs-expression-translator-date-fns`, and floating-point division (`FLOAT_DIV`) in `sql-comprehension/vs-expression-translator-float-div`.

<!-- DELTA:CHANGED -->
## Background

* Exasol sends `+`, `-`, `*`, `/`, and unary `-` as the `function_scalar` nodes `ADD`, `SUB`, `MULT`, `FLOAT_DIV`, and `NEG`, and its integer division function as `DIV`.
* The scan computes a pushed expression as DataFusion SQL, and a wrapper computes it as Exasol SQL. `EXPLAIN VIRTUAL` shows both.
* An expression the scan does not compute takes the path's fallback, which computes it in Exasol (`pushdown-types/pushdown-planning-scan-text-conversion`).
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a pushed `ADD`, `SUB`, `MULT`, or `NEG` node over operands the adapter renders, such as `L_EXTENDEDPRICE * L_DISCOUNT`
* *WHEN* the adapter renders it for the scan or for an Exasol wrapper
* *THEN* it SHALL render `(<left> + <right>)`, `(<left> - <right>)`, `(<left> * <right>)`, or `(-<operand>)`, with the same text for the scan and for the wrapper
* *AND* unary negation SHALL compose inside an aggregate argument, such as `SUM(-<operand>)`
* *AND* the adapter SHALL advertise exactly the arithmetic capabilities it renders: `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, `FN_DIV`, and `FN_NEG`
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: Integer division DIV is deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `DIV`, Exasol integer-quotient division, which truncates toward zero (`DIV(-7,2) = -3`, verified live) and raises a division-by-zero error
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return an error naming `DIV` as unsupported
* *AND* `render_scan_expression` SHALL return an error for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate `DIV`, because DataFusion has no `div` builtin and, unlike `FLOAT_DIV`, no single rendering reproduces it: `DIV` needs TRUNCATION, which DataFusion's `/` delivers for integer operands and not for any other kind. The renderer picks one rendering per function and does not consult the typed scan pass, so it does not render only the integer-operand case that pass identifies
* *AND* this decline SHALL NOT be read as resting on the division-by-zero divergence that `FLOAT_DIV` accepts (see `sql-comprehension/vs-expression-translator-float-div`): for `x/0` the query fails either way, and for `0/0` the divergence belongs to the tracked NaN-at-emit gap rather than to the rendering. `DIV`'s disqualifying defect is that a wrong rendering would be wrong on EVERY row for non-integer operands, not only when a divisor is zero, that is the difference between the two operators, and a future `TRUNC(m/n)` emulation would have to answer the per-row problem, not the zero-divisor one
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: Integer division over integer operands runs in the scan

* *GIVEN* a pushed `DIV(<left>, <right>)` whose operands are each an integer (`DECIMAL(p,0)`) or NULL, such as `DIV(qty - 7, 2)` over an integer column `qty`
* *WHEN* the adapter plans the request
* *THEN* the `DIV` node SHALL NOT be the reason the scan declines the enclosing select item, filter, or key, and wherever the scan computes that enclosing expression, its pushdown SQL SHALL call `vs_checked_int_div(<left>, <right>)`
* *AND* the quotient SHALL equal native Exasol's, which truncates toward zero: `DIV(-7, 2)` and `DIV(7, -2)` are `-3`, `DIV(-7, -2)` is `3`, and a numerator of `-9223372036854775808` over a divisor of `-1` returns `9223372036854775808` without an overflow
* *AND* the result SHALL have native Exasol's type, `DECIMAL(p,0)` with the numerator's precision p
* *AND* a NULL operand SHALL yield NULL, also over a zero divisor
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Integer division over a decimal or DOUBLE operand runs in Exasol

* *GIVEN* a pushed `DIV(<left>, <right>)` with an operand that is not an integer, such as a DOUBLE or a DECIMAL with a scale above 0, for example `DIV(price, 2)` over a `DECIMAL(9,2)` column `price`
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL NOT compute it, and the path's fallback SHALL compute it in Exasol SQL as `DIV(<left>, <right>)`
* *AND* the result SHALL equal native Exasol's in value and type: `DIV(7.5, 2)` is `3` of type `DECIMAL(p,0)`, and a DOUBLE numerator returns a DOUBLE
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A pushed-down integer division by zero fails the query

* *GIVEN* a `DIV` over integer operands that the scan computes, in the select list or in a WHERE filter
* *AND* a scanned row whose divisor is zero and whose numerator is not NULL, zero included
* *WHEN* the scan evaluates that row
* *THEN* the query SHALL fail with a message that names a division by zero, as native Exasol fails with `data exception - division by zero`
* *AND* the message SHALL NOT carry the storage-read framing `scan failed: assigned data could not be read`
<!-- /DELTA:NEW -->
