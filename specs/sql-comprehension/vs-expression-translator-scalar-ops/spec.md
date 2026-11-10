# Feature: VS Expression Translator, Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators and the safe entry points. CAST targets are in `sql-comprehension/vs-expression-translator-cast`, named scalar functions in `sql-functions/vs-expression-translator-scalar-fns`, date/time functions in `sql-functions/vs-expression-translator-date-fns`, and floating-point division (`FLOAT_DIV`) in `sql-comprehension/vs-expression-translator-float-div`.

## Background

* The public DataFusion entry points are `render_scan_expression` and `render_scan_filter`, which take the column types and decline what the scan must not compute (`pushdown-types/pushdown-planning-scan-text-conversion`). Both return `Err` for an unsupported node and never panic, and `render_scan_filter` returns `Ok(None)` for a trivially-true result such as `TRUE` and `NULL`. The Exasol entry points `render_expression_exasol`, `render_expression_exasol_safe`, and `render_df_filter_exasol_safe` return `Err`, `None`, and `None` for an unsupported node, and the last also `None` for a trivially-true result.
* `+`, `-`, `*`, and unary `-` use the same syntax in both parsers. `/` (`FLOAT_DIV`) renders differently per dialect.
* A conversion or operator node is translated only when its DataFusion result matches Exasol. `DIV`, `TO_CHAR`, `TO_NUMBER`, and the bitwise functions fail that test and fall back to Exasol.
* The crate is a pure, stateless translator. It reads column types only through the lookup its caller passes.

## Scenarios

### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is the Exasol scalar-function name for addition, subtraction, or multiplication, or for unary negation
* *AND* the exact `name` strings Exasol emits for these operators have been verified against live `EXPLAIN VIRTUAL` output for an arithmetic pushdown (so the translator matches what Exasol actually sends, e.g. `MULT` for `*`, not an assumed `MUL`)
* *WHEN* `render_expression` or `render_expression_exasol` processes the node
* *THEN* the `ADD`, `SUB`, and `MULT` nodes SHALL return `(<left> <op> <right>)` where the operators are `+`, `-`, `*` respectively, for operands that are themselves any renderable expression (including two bare column references, e.g. `(L_EXTENDEDPRICE * L_DISCOUNT)`), byte-identically in BOTH dialects, the operator syntax is shared by both parsers, and these wire names are NOT Exasol function names (Exasol has no function called `ADD`), so the Exasol dialect's verbatim rule for named functions MUST NOT be applied to them
* *AND* unary negation SHALL return `(-<operand>)` and SHALL compose inside an aggregate argument (e.g. `SUM(-<operand>)`) so it flows through the arithmetic-aggregate decomposition path
* *AND* floating-point division (`FLOAT_DIV`) SHALL NOT be rendered by this shape, it is the one arithmetic operator whose rendering diverges by dialect, specified in `sql-comprehension/vs-expression-translator-float-div`; this scenario's "byte-identically in BOTH dialects" claim covers `ADD`, `SUB`, `MULT`, and `NEG` only
* *AND* the set of arithmetic `name` strings the translator matches SHALL correspond exactly to the arithmetic operator capabilities the adapter advertises (`pushdown-capabilities/pushdown-planning-capability-extensions`), `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, and `FN_NEG`, so no advertised operator is left unrenderable and no rendered operator is left unadvertised
* *AND* Exasol integer division (`DIV`) SHALL NOT be matched here and `FN_DIV` SHALL NOT be advertised

### Scenario: Unsupported node type returns error in raising mode

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_expression` is called (raising mode)
* *THEN* the function SHALL return an error containing the unrecognised node type name
* *AND* the error MUST NOT contain any credential or secret-looking string from the node payload

### Scenario: The scan entry point declines an unsupported node without panicking

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_scan_expression` is called
* *THEN* the function SHALL return an error
* *AND* the function MUST NOT panic

### Scenario: Trivially-true filter is suppressed

* *GIVEN* a call to `render_scan_filter` with an expression that renders to `TRUE` or `NULL`
* *WHEN* the entry point evaluates the result
* *THEN* it SHALL return `Ok(None)` so the adapter omits the redundant filter from the scan spec
* *AND* `render_df_filter_exasol_safe` SHALL suppress the same two trivially-true results, so the outer WHERE residual of the N-scan join wrapper omits a no-op conjunct on the Exasol path too

### Scenario: Integer division DIV is deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `DIV`, Exasol integer-quotient division, which truncates toward zero (`DIV(-7,2) = -3`, verified live) and raises a division-by-zero error
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming `DIV` as unsupported
* *AND* `render_scan_expression` SHALL return an error for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate `DIV`, because DataFusion has no `div` builtin and, unlike `FLOAT_DIV`, no single type-blind rendering reproduces it: `DIV` needs TRUNCATION, which DataFusion's `/` delivers for integer operands and not for any other kind, and unlike CAST's explicit `dataType` field, DIV's operand types are not carried in the expression node and the translator infers no expression type, so it does not identify and selectively render only the safe integer-operand case
* *AND* this decline SHALL NOT be read as resting on the division-by-zero divergence that `FLOAT_DIV` accepts (see `sql-comprehension/vs-expression-translator-float-div`): for `x/0` the query fails either way, and for `0/0` the divergence belongs to the tracked NaN-at-emit gap rather than to the rendering. `DIV`'s disqualifying defect is that a wrong rendering would be wrong on EVERY row for non-integer operands, not only when a divisor is zero, that is the difference between the two operators, and a future `TRUNC(m/n)` emulation would have to answer the per-row problem, not the zero-divisor one

### Scenario: Conversion format functions TO_CHAR and TO_NUMBER are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `TO_CHAR` or `TO_NUMBER`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported
* *AND* `render_scan_expression` SHALL return an error for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, because DataFusion has no matching format model or `to_number`; a no-format string-to-number conversion remains reachable through `FN_CAST`

### Scenario: Bitwise operator functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`, the eleven bitwise operator functions Exasol names `FN_BIT_*` (the `function_scalar` name equals the capability name with `FN_` stripped)
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported, and `render_scan_expression` SHALL return an error for the same node without panicking
* *AND* for `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_LSHIFT`, and `BIT_RSHIFT`, which map to DataFusion's `&`, `|`, `#`, `<<`, and `>>` operators, the translator MUST NOT render them and the adapter SHALL let Exasol evaluate the function, because Exasol defines them over unsigned 64-bit integers (`0`–`18446744073709551615`, result `DECIMAL(20,0)`) while DataFusion's operators act on the operand's signed Arrow integer type (Iceberg carries only signed `int`/`long`, no unsigned primitive), a bit-63-set result reads as a large positive value in Exasol but negative under signed `Int64`, `BIT_RSHIFT`'s signed `>>` is arithmetic (sign-extending) versus Exasol's logical (zero-fill), and the translator cannot restrict rendering to the safe non-negative, bit-63-clear operand subset because operand values are unknown at plan time
* *AND* for `BIT_NOT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, and `BIT_TO_NUM` the translator MUST NOT render them because DataFusion provides no matching operator or scalar function: its SQL planner (`parse_sql_unary_op`) supports only logical `NOT`, unary `+`, and unary `-`, rejecting unary `~` with `not_impl_err`, and `datafusion-functions` registers no bit-rotate, bit-test, bit-set, or bits-to-number scalar function (its only `bit`-named function is the string `bit_length`, out of scope here)
