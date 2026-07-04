# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, and the safe/fallback entry points. Named math/string/conditional scalar functions are covered in `sql-comprehension/vs-expression-translator-scalar-fns`; date/time functions in `sql-comprehension/vs-expression-translator-date-fns`.

## Background

The `crates/vs-expression` crate exposes three public entry points:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results (e.g. `TRUE`, `NULL`) so the adapter can omit no-op filters from the scan spec

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Arithmetic operators translate to binary SQL expressions

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is the Exasol scalar-function name for a binary arithmetic operator — addition, subtraction, multiplication, or floating-point division — or for unary negation
* *AND* the exact `name` strings Exasol emits for these operators have been verified against live `EXPLAIN VIRTUAL` output for an arithmetic pushdown (so the translator matches what Exasol actually sends, e.g. `MULT` for `*`, not an assumed `MUL`)
* *WHEN* `render_expression` processes the node
* *THEN* the binary arithmetic nodes SHALL return `(<left> <op> <right>)` where the operators are `+`, `-`, `*`, `/` respectively, for operands that are themselves any renderable expression (including two bare column references, e.g. `(L_EXTENDEDPRICE * L_DISCOUNT)`)
* *AND* unary negation SHALL return `(-<operand>)`
* *AND* the set of arithmetic `name` strings the translator matches SHALL correspond exactly to the arithmetic operator capabilities the adapter advertises (`vs-adapter/pushdown-planning-capability-extensions`), so no advertised operator is left unrenderable and no rendered operator is left unadvertised
<!-- /DELTA:CHANGED -->

### Scenario: CAST translates to DataFusion CAST syntax

* *GIVEN* a VS expression node of type `function_scalar` with `name` equal to `CAST`
* *AND* the node carries a `dataType` field with at minimum a `type` string (e.g., `"VARCHAR"`, `"DECIMAL"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, `"TIMESTAMP"`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CAST(<expr> AS <target_type>)` where `<target_type>` maps the VS data-type descriptor to an equivalent DataFusion type name
* *AND* `DECIMAL(p,s)` SHALL render as `DECIMAL(p,s)`; `VARCHAR` as `VARCHAR`; `DOUBLE` as `DOUBLE`; `DATE` as `DATE`; `TIMESTAMP` as `TIMESTAMP`

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
