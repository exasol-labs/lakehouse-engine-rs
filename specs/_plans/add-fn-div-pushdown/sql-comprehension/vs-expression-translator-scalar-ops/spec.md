# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, and the safe/fallback entry points. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`.

## Background

The `crates/vs-expression` crate exposes three public entry points:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol. Exasol `DIV` returns the integer quotient by truncating toward zero — verified live: `DIV(-7,2) = -3` and `DIV(15.7,6.2) = 2` — and raises a division-by-zero error (SQL state 22012). DataFusion 54 has no `div` builtin; its `/` truncates only integer operands and divides non-integer operands fractionally, and float division by zero yields infinity instead of an error. No single rendering reproduces `DIV` across every operand type, so `DIV` stays unsupported. DataFusion 54 `to_char` uses strftime masks rather than Exasol's Oracle-style format models and rejects numeric formatting, and DataFusion 54 has no `to_number`. These three functions are therefore left unsupported and fall back to Exasol.

Exasol emits CAST as its own top-level node type, `function_scalar_cast` — not nested inside a generic `function_scalar` node — matching the same family pattern as `function_scalar_case` and `function_scalar_extract`. The translator also retains a defensive nested `function_scalar`+`name=CAST` arm for a legacy/alternate encoding, sharing the same rendering logic, but `function_scalar_cast` is the node type Exasol's live engine actually sends.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Integer division DIV is deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `DIV` — Exasol integer-quotient division, which truncates toward zero (`DIV(-7,2) = -3`, verified live) and raises a division-by-zero error
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming `DIV` as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate `DIV`, because DataFusion 54 has no `div` builtin and a `TRUNC(m/n)` emulation diverges from Exasol for DOUBLE operands on division by zero — Exasol raises SQL state 22012, DataFusion float division yields infinity — and unlike CAST's explicit `dataType` field, DIV's operand types are not carried in the expression node, so the translator cannot identify and selectively render only the safe integer-operand case
<!-- /DELTA:CHANGED -->
