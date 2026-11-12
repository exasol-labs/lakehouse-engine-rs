# Feature: VS Expression Translator, Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators and the safe entry points. CAST targets are in `sql-comprehension/vs-expression-translator-cast`, named scalar functions in `sql-functions/vs-expression-translator-scalar-fns`, date/time functions in `sql-functions/vs-expression-translator-date-fns`, and floating-point division (`FLOAT_DIV`) in `sql-comprehension/vs-expression-translator-float-div`.

## Background

* Exasol sends `+`, `-`, `*`, `/`, and unary `-` as the `function_scalar` nodes `ADD`, `SUB`, `MULT`, `FLOAT_DIV`, and `NEG`, and its integer division function as `DIV`.
* The scan computes a pushed expression as DataFusion SQL, and a wrapper computes it as Exasol SQL. `EXPLAIN VIRTUAL` shows both.
* An expression the scan does not compute takes the path's fallback, which computes it in Exasol (`pushdown-types/pushdown-planning-scan-text-conversion`).

## Scenarios

### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a pushed `ADD`, `SUB`, `MULT`, or `NEG` node over operands the adapter renders, such as `L_EXTENDEDPRICE * L_DISCOUNT`
* *WHEN* the adapter renders it for the scan or for an Exasol wrapper
* *THEN* it SHALL render `(<left> + <right>)`, `(<left> - <right>)`, `(<left> * <right>)`, or `(-<operand>)`, with the same text for the scan and for the wrapper
* *AND* unary negation SHALL compose inside an aggregate argument, such as `SUM(-<operand>)`
* *AND* the adapter SHALL advertise exactly the arithmetic capabilities it renders: `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, `FN_DIV`, and `FN_NEG`

### Scenario: Unsupported node type returns an error

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_expression` is called
* *THEN* the function SHALL return an error containing the unrecognised node type name
* *AND* the error MUST NOT contain any credential or secret-looking string from the node payload

### Scenario: The scan entry point declines an unsupported node without panicking

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_scan_expression` is called
* *THEN* the function SHALL return an error
* *AND* the function MUST NOT panic

### Scenario: A NULL filter keeps no row

* *GIVEN* a call to `render_scan_filter` with an expression that renders to `NULL`
* *WHEN* the entry point evaluates the result
* *THEN* it SHALL return `Ok("FALSE")`, because a WHERE condition that is NULL keeps no row. Exasol folds a condition such as `NULL LIKE '%b%'` or `'' LIKE '%b%'` (an empty string is NULL in Exasol) to a bare `literal_null` filter, and omitting it would return every row
* *AND* `conjoin_scan_filters` SHALL apply the same rule, and an Exasol-dialect wrapper SHALL render a `NULL` filter as a `NULL` condition, which also keeps no row, so the single-table, grouped-aggregate, and join paths all return the zero rows native Exasol returns

### Scenario: Conversion format functions TO_CHAR and TO_NUMBER are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `TO_CHAR` or `TO_NUMBER`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return an error naming the function as unsupported
* *AND* `render_scan_expression` SHALL return an error for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, because DataFusion has no matching format model or `to_number`; a no-format string-to-number conversion remains reachable through `FN_CAST`

### Scenario: Bitwise operator functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`, the eleven bitwise operator functions Exasol names `FN_BIT_*` (the `function_scalar` name equals the capability name with `FN_` stripped)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return an error naming the function as unsupported, and `render_scan_expression` SHALL return an error for the same node without panicking
* *AND* for `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_LSHIFT`, and `BIT_RSHIFT`, which map to DataFusion's `&`, `|`, `#`, `<<`, and `>>` operators, the translator MUST NOT render them and the adapter SHALL let Exasol evaluate the function, because Exasol defines them over unsigned 64-bit integers (`0`–`18446744073709551615`, result `DECIMAL(20,0)`) while DataFusion's operators act on the operand's signed Arrow integer type (Iceberg carries only signed `int`/`long`, no unsigned primitive), a bit-63-set result reads as a large positive value in Exasol but negative under signed `Int64`, `BIT_RSHIFT`'s signed `>>` is arithmetic (sign-extending) versus Exasol's logical (zero-fill), and the translator cannot restrict rendering to the safe non-negative, bit-63-clear operand subset because operand values are unknown at plan time
* *AND* for `BIT_NOT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, and `BIT_TO_NUM` the translator MUST NOT render them because DataFusion provides no matching operator or scalar function: its SQL planner (`parse_sql_unary_op`) supports only logical `NOT`, unary `+`, and unary `-`, rejecting unary `~` with `not_impl_err`, and `datafusion-functions` registers no bit-rotate, bit-test, bit-set, or bits-to-number scalar function (its only `bit`-named function is the string `bit_length`, out of scope here)

### Scenario: Integer division over integer operands runs in the scan

* *GIVEN* a pushed `DIV(<left>, <right>)` whose operands are each an integer (`DECIMAL(p,0)`) or NULL, such as `DIV(qty - 7, 2)` over an integer column `qty`
* *WHEN* the adapter plans the request
* *THEN* the `DIV` node SHALL NOT be the reason the scan declines the enclosing select item, filter, or key, and wherever the scan computes that enclosing expression, its pushdown SQL SHALL call `vs_checked_int_div(<left>, <right>)`
* *AND* the quotient SHALL equal native Exasol's, which truncates toward zero: `DIV(-7, 2)` and `DIV(7, -2)` are `-3`, `DIV(-7, -2)` is `3`, and a numerator of `-9223372036854775808` over a divisor of `-1` returns `9223372036854775808` without an overflow
* *AND* the result SHALL have native Exasol's type, `DECIMAL(p,0)` with the numerator's precision p
* *AND* a NULL operand SHALL yield NULL, also over a zero divisor

### Scenario: Integer division over a decimal or DOUBLE operand runs in Exasol

* *GIVEN* a pushed `DIV(<left>, <right>)` with an operand that is not an integer, such as a DOUBLE or a DECIMAL with a scale above 0, for example `DIV(price, 2)` over a `DECIMAL(9,2)` column `price`
* *WHEN* the adapter plans the request
* *THEN* the scan SHALL NOT compute it, and the path's fallback SHALL compute it in Exasol SQL as `DIV(<left>, <right>)`
* *AND* the result SHALL equal native Exasol's in value and type: `DIV(7.5, 2)` is `3` of type `DECIMAL(p,0)`, and a DOUBLE numerator returns a DOUBLE

### Scenario: A pushed-down integer division by zero fails the query

* *GIVEN* a `DIV` over integer operands that the scan computes, in the select list or in a WHERE filter
* *AND* a scanned row whose divisor is zero and whose numerator is not NULL, zero included
* *WHEN* the scan evaluates that row
* *THEN* the query SHALL fail with a message that names a division by zero, as native Exasol fails with `data exception - division by zero`
* *AND* the message SHALL NOT carry the storage-read framing `scan failed: assigned data could not be read`
