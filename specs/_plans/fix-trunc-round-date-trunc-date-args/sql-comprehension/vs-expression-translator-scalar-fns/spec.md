# Feature: VS Expression Translator — Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with
named scalar function translation: math functions, the modulo operator, string functions,
CASE expressions, and the NULLIF/COALESCE shorthands. These are distinct from the arithmetic
operators and CAST scenarios in `vs-expression-translator-scalar-ops`. `GREATEST`/`LEAST` are
specified in the sibling feature `sql-comprehension/vs-expression-translator-greatest-least`, and
`CONCAT`'s NULL-semantics rendering in `sql-comprehension/vs-expression-translator-concat`, both
split out to keep this feature's scenario count under the library threshold — the same treatment
`FLOAT_DIV` already received into `vs-expression-translator-float-div`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/sql-comprehension/vs-expression-translator-scalar-fns/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Math scalar functions translate to DataFusion math calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol math functions: `ABS`, `ROUND`, `FLOOR`, `CEIL`, `SQRT`, `POWER`, `EXP`, `LN`, `LOG`, `SIGN`, `TRUNC`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SINH`, `COSH`, `TANH`, `COT`, `DEGREES`, or `RADIANS`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render every listed name except `ROUND` and `TRUNC` as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SIGN`→`signum`, `CEIL`→`ceil`, `POWER`→`power`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* `ROUND` and `TRUNC` SHALL render as `sql-comprehension/vs-expression-translator-datetime-trunc` specifies for every argument type, including its decline of a second argument that is neither a numeric literal nor a vocabulary token
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* a node whose argument count does not match the function arity SHALL return an error in raising mode and `None` in the safe variants
<!-- /DELTA:CHANGED -->
