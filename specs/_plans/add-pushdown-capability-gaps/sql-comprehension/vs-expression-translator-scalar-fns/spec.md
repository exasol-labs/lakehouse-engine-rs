# Feature: VS Expression Translator — Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with
named scalar function translation: math functions, the modulo operator, string functions,
CASE expressions, GREATEST/LEAST, and the NULLIF/COALESCE shorthands. These are distinct
from the arithmetic operators and CAST scenarios in `vs-expression-translator-scalar-ops`.

## Background

Most Exasol `FN_*` names lower-case directly to the DataFusion function of the same name;
a documented set needs explicit aliasing: `SIGN`→`signum`, `LENGTH`→`character_length`,
`MOD`→`%` operator, `INSTR`/`LOCATE`→`strpos` (with operand reorder), `UNICODE`→`ascii`,
`UNICODECHR`→`chr`, `NULLIFZERO`→`nullif(x,0)`, `ZEROIFNULL`→`coalesce(x,0)`.

The scalar regexp functions (`REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, `REGEXP_COUNT`)
are deliberately not translated. DataFusion 54 runs the Rust `regex` crate, whose dialect rejects
the pattern backreferences and lookaround Exasol's PCRE dialect accepts; DataFusion 54 has no
`regexp_substr`; and Exasol's position, occurrence, and return-option arguments have no matching
DataFusion argument shape. This is separate from the `FN_PRED_REGEXP_LIKE` predicate, which stays
advertised and is out of scope here.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Regexp scalar functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, or `REGEXP_COUNT`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, so `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT` remain unadvertised
* *AND* the exclusion MUST NOT alter the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement
<!-- /DELTA:NEW -->
