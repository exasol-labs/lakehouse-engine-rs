# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, and the safe/fallback entry points. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`.

## Background

The `crates/vs-expression` crate exposes three public entry points:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol. `DIV`, `TO_CHAR`, and `TO_NUMBER` are left unsupported and fall back to Exasol. The bitwise operator functions (`BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM`) are likewise unsupported: Exasol defines them over an unsigned 64-bit integer domain that DataFusion's signed-integer operators and the `Int64` → `DECIMAL(20,0)` mapping do not reproduce, and six of the eleven have no DataFusion builtin at all (issue #108).

Exasol emits CAST as its own top-level node type, `function_scalar_cast` — not nested inside a generic `function_scalar` node — matching the same family pattern as `function_scalar_case` and `function_scalar_extract`. The translator also retains a defensive nested `function_scalar`+`name=CAST` arm for a legacy/alternate encoding, sharing the same rendering logic, but `function_scalar_cast` is the node type Exasol's live engine actually sends.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Bitwise operator functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `BIT_LSHIFT`, `BIT_RSHIFT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM` — the eleven bitwise operator functions Exasol names `FN_BIT_*` (the `function_scalar` name equals the capability name with `FN_` stripped)
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported, and `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* for `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_LSHIFT`, and `BIT_RSHIFT` — which map to DataFusion's `&`, `|`, `#`, `<<`, and `>>` operators — the translator MUST NOT render them and the adapter SHALL let Exasol evaluate the function, because Exasol defines them over unsigned 64-bit integers (`0`–`18446744073709551615`, result `DECIMAL(20,0)`) while DataFusion's operators act on the operand's signed Arrow integer type (Iceberg carries only signed `int`/`long`, no unsigned primitive) — a bit-63-set result reads as a large positive value in Exasol but negative under signed `Int64`, `BIT_RSHIFT`'s signed `>>` is arithmetic (sign-extending) versus Exasol's logical (zero-fill), and the value/type-blind translator cannot restrict rendering to the safe non-negative, bit-63-clear operand subset because operand types and values are not carried in the node (the same limitation the `DIV` decline records)
* *AND* for `BIT_NOT`, `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, and `BIT_TO_NUM` the translator MUST NOT render them because DataFusion 54.0.0 provides no matching operator or scalar function: its SQL planner (`parse_sql_unary_op`) supports only logical `NOT`, unary `+`, and unary `-`, rejecting unary `~` with `not_impl_err`, and `datafusion-functions` 54.0.0 registers no bit-rotate, bit-test, bit-set, or bits-to-number scalar function (its only `bit`-named function is the string `bit_length`, out of scope here)
<!-- /DELTA:NEW -->
