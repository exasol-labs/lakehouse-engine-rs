# Feature: VS Expression Translator — Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with named
scalar function translation: math functions, the modulo operator, string functions, CASE
expressions, GREATEST/LEAST, and the NULLIF/COALESCE shorthands. These are distinct from the
arithmetic operators and CAST scenarios in `vs-expression-translator-scalar-ops`. This delta touches
only the verbatim-exclusion table's operator row, which claimed all five operator wire names render
identically in both dialects — no longer true of `FLOAT_DIV` (issue #186).

## Background

<!-- DELTA:CHANGED -->
| Construct | Why it is not rendered verbatim |
|---|---|
| `ADD`, `SUB`, `MULT`, `NEG` | Wire names for operators, not Exasol function names — Exasol has no function called `ADD`. Both dialects render `(<l> + <r>)` and the rest. |
| `FLOAT_DIV` | Also an operator wire name, and the ONLY one of the five whose rendering DIVERGES by dialect: `(CAST(<l> AS DOUBLE) / <r>)` in the DataFusion dialect, a bare `(<l> / <r>)` in the Exasol dialect. Exasol's `/` IS `FN_FLOAT_DIV` — always true float division, whatever the operand types — while DataFusion's `/` is operand-typed and truncates integer and decimal operands (issue #186). The cast is what makes DataFusion reproduce Exasol; Exasol needs no help. Specified in `sql-comprehension/vs-expression-translator-scalar-ops`. |
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: FLOAT_DIV stays outside the verbatim rule in both dialects

* *GIVEN* the crate's single per-function declaration of each translated `function_scalar` name, which both gates the dispatch and drives the enforcing Exasol-dialect sweep test
* *AND* `FLOAT_DIV` declared with the dialect-shaped form rather than the verbatim form, alongside `ADD`, `SUB`, `MULT`, and `NEG`
* *WHEN* the sweep test renders every declared name through `render_expression_exasol` and compares it against that name's declared expectation
* *THEN* `FLOAT_DIV` SHALL keep its shaped declaration and MUST NOT be moved to the verbatim form, because Exasol has no function called `FLOAT_DIV` — a verbatim rendering would emit `FLOAT_DIV(<l>, <r>)`, which Exasol rejects the same way it rejects `SIGNUM` and `STRPOS` (`function or script <NAME> not found`, SQL code 42000)
* *AND* the sweep's Exasol-dialect expectation for `FLOAT_DIV` SHALL remain the bare `(<l> / <r>)` — unchanged by issue #186's fix, which adds the `CAST(... AS DOUBLE)` wrapper on the DataFusion side only
* *AND* the sweep's banned-token list SHALL continue to catch a DataFusion-only spelling leaking into an Exasol-parsed fragment, and `CAST` MUST NOT be added to that list, since `CAST` is valid Exasol SQL that the CAST scenarios legitimately emit in the Exasol dialect
<!-- /DELTA:NEW -->
