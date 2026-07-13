# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, and the safe/fallback entry points. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`.

## Background

The `crates/vs-expression` crate exposes three public entry points:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

A conversion or operator node is translated only when its DataFusion 54 result matches Exasol. Exasol `DIV` (floor division) diverges from DataFusion `/` (truncates integer division toward zero) and DataFusion 54 has no `div` function; DataFusion 54 `to_char` uses strftime masks rather than Exasol's Oracle-style format models and rejects numeric formatting, and DataFusion 54 has no `to_number`. These three functions are therefore left unsupported and fall back to Exasol.

Exasol emits CAST as its own top-level node type, `function_scalar_cast` — not nested inside a generic `function_scalar` node — matching the same family pattern as `function_scalar_case` and `function_scalar_extract`. The translator also retains a defensive nested `function_scalar`+`name=CAST` arm for a legacy/alternate encoding, sharing the same rendering logic, but `function_scalar_cast` is the node type Exasol's live engine actually sends.

## Scenarios

### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is the Exasol scalar-function name for a binary arithmetic operator — addition, subtraction, multiplication, or floating-point division — or for unary negation
* *AND* the exact `name` strings Exasol emits for these operators have been verified against live `EXPLAIN VIRTUAL` output for an arithmetic pushdown (so the translator matches what Exasol actually sends, e.g. `MULT` for `*`, not an assumed `MUL`)
* *WHEN* `render_expression` processes the node
* *THEN* the binary arithmetic nodes SHALL return `(<left> <op> <right>)` where the operators are `+`, `-`, `*`, `/` respectively, for operands that are themselves any renderable expression (including two bare column references, e.g. `(L_EXTENDEDPRICE * L_DISCOUNT)`)
* *AND* unary negation SHALL return `(-<operand>)` and SHALL compose inside an aggregate argument (e.g. `SUM(-<operand>)`) so it flows through the arithmetic-aggregate decomposition path
* *AND* the set of arithmetic `name` strings the translator matches SHALL correspond exactly to the arithmetic operator capabilities the adapter advertises (`vs-adapter/pushdown-planning-capability-extensions`) — `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, and `FN_NEG` — so no advertised operator is left unrenderable and no rendered operator is left unadvertised
* *AND* Exasol integer division (`DIV`) SHALL NOT be matched here and `FN_DIV` SHALL NOT be advertised

### Scenario: CAST translates to DataFusion CAST syntax

* *GIVEN* a VS expression node of type `function_scalar_cast` with `name` equal to `CAST` — the top-level node type Exasol's engine serializer emits for CAST (verified against the Exasol engine source; `function_scalar`+`name=CAST` is retained only as a defensive nested/alternate encoding, not the primary wire shape)
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"CHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name, rendering `DECIMAL(p,s)` as `DECIMAL(p,s)`; `VARCHAR` and `CHAR` as `VARCHAR`; `DOUBLE` as `DOUBLE`; `BOOLEAN` as `BOOLEAN`; `DATE` as `DATE`; `TIMESTAMP` as `TIMESTAMP`
* *AND* a `dataType` whose `type` is an Exasol target with no faithful DataFusion mapping — `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH LOCAL TIME ZONE` — SHALL return an error in raising mode and `None` in the safe variants, so the adapter omits the CAST and Exasol evaluates it as a correctness backstop
* *AND* the set of CAST target types the translator renders SHALL be exactly the set whose DataFusion result matches Exasol's CAST result, so `FN_CAST` (advertised per `vs-adapter/pushdown-planning-capability-extensions`) is never advertised for a target the translator would render divergently

### Scenario: Unsupported node type returns error in raising mode

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_expression` is called (raising mode)
* *THEN* the function SHALL return an error containing the unrecognised node type name
* *AND* the error MUST NOT contain any credential or secret-looking string from the node payload

### Scenario: Safe variant returns None for unsupported nodes

* *GIVEN* a VS expression node with a `type` value not handled by the translator
* *WHEN* `render_expression_safe` is called
* *THEN* the function SHALL return `None`
* *AND* the function MUST NOT panic

### Scenario: Trivially-true filter suppressed in safe variant

* *GIVEN* a call to `render_df_filter_safe` with an expression that renders to `TRUE` or `NULL`
* *WHEN* the safe entry-point evaluates the result
* *THEN* it SHALL return `None` so the adapter omits the redundant filter from the scan spec

### Scenario: Integer division DIV is deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `DIV` (Exasol integer-quotient division)
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming `DIV` as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate `DIV`, because Exasol floor division has no faithful DataFusion 54 translation

### Scenario: Conversion format functions TO_CHAR and TO_NUMBER are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `TO_CHAR` or `TO_NUMBER`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, because DataFusion 54 has no matching format model or `to_number`; a no-format string-to-number conversion remains reachable through `FN_CAST`
