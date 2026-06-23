# Feature: VS Expression Translator — Scalar Operations

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with arithmetic operators, CAST, math/string/conditional scalar functions, and the safe/fallback entry points. These are the scenarios specific to scalar function translation and the None-returning safe variants used by the adapter fallback paths.

## Background

The `crates/vs-expression` crate exposes three public entry points:
- `render_expression` — raising mode, returns `Err` for unsupported nodes
- `render_expression_safe` — returns `None` for unsupported nodes, never panics
- `render_df_filter_safe` — same as `render_expression_safe` but also returns `None` for trivially-true results so the adapter can omit no-op filters from the scan spec

Most Exasol `FN_*` names lower-case directly to the DataFusion function of the same name; a documented set needs explicit aliasing (e.g. `SIGN`→`signum`, `LENGTH`→`character_length`, `MOD`→`%` operator, `INSTR`/`LOCATE`→`strpos`).

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Math scalar functions translate to DataFusion math calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol math functions: `ABS`, `ROUND`, `FLOOR`, `CEIL`, `SQRT`, `POWER`, `EXP`, `LN`, `LOG`, `SIGN`, `TRUNC`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SINH`, `COSH`, `TANH`, `COT`, `DEGREES`, or `RADIANS`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SIGN`→`signum`, `CEIL`→`ceil`, `POWER`→`power`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* a node whose argument count does not match the function arity SHALL return an error in raising mode and `None` in the safe variants

### Scenario: MOD translates to the modulo operator

* *GIVEN* a VS expression node of type `function_scalar` named `MOD` with two arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `(<left> % <right>)`, because DataFusion exposes modulo as the `%` operator rather than a `mod()` function
* *AND* both operands SHALL be rendered recursively

### Scenario: String scalar functions translate to DataFusion string calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol string functions: `CONCAT`, `LENGTH`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `CHR`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `INSTR`, `LOCATE`, `OCTET_LENGTH`, `UNICODE`, or `UNICODECHR`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SUBSTR`→`substr`, `LENGTH`→`character_length`, `OCTET_LENGTH`→`octet_length`, `INSTR`/`LOCATE`→`strpos` (with operands ordered string-then-substring per DataFusion `strpos(string, substring)`), `UNICODE`→`ascii`, `UNICODECHR`→`chr`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* `LOCATE`/`INSTR` argument reordering MUST preserve the Exasol semantics of "position of substring within string"

### Scenario: CASE expression translates to SQL CASE WHEN

* *GIVEN* a VS expression node of type `function_scalar` named `CASE` carrying its branch arguments (the test conditions, the result values, and the optional ELSE result) per the Exasol `function_scalar` CASE encoding
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return a `CASE WHEN <cond1> THEN <res1> [WHEN <condN> THEN <resN>]... [ELSE <else>] END` expression with each condition and result rendered recursively
* *AND* a CASE node with no WHEN branch SHALL return an error in raising mode and `None` in the safe variants

### Scenario: GREATEST and LEAST translate to DataFusion greatest/least

* *GIVEN* a VS expression node of type `function_scalar` named `GREATEST` or `LEAST` with one or more arguments
* *WHEN* `render_expression` processes the node
* *THEN* `GREATEST` SHALL render as `greatest(<a1>, <a2>, ...)` and `LEAST` SHALL render as `least(<a1>, <a2>, ...)` over the recursively rendered arguments

### Scenario: NULLIFZERO and ZEROIFNULL translate to NULLIF and COALESCE

* *GIVEN* a VS expression node of type `function_scalar` named `NULLIFZERO` or `ZEROIFNULL` with a single argument
* *WHEN* `render_expression` processes the node
* *THEN* `NULLIFZERO` SHALL render as `nullif(<arg>, 0)`
* *AND* `ZEROIFNULL` SHALL render as `coalesce(<arg>, 0)`
<!-- /DELTA:NEW -->
