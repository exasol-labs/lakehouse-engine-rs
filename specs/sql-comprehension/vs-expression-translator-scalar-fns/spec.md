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
are deliberately not translated. Re-verified for issue #106 against the pinned DataFusion 54.0.0
and `regex` 1.12.4: DataFusion runs the Rust `regex` crate, whose dialect rejects the pattern
backreferences and lookaround Exasol's PCRE dialect accepts; DataFusion has no `regexp_substr`; and
Exasol's position, occurrence, and return-option arguments have no matching DataFusion argument
shape. A compile-time literal-pattern check would certify pattern syntax, not match parity with
Exasol's PCRE, so it cannot lift the decline (see issue #106). This is separate from the
`FN_PRED_REGEXP_LIKE` predicate, which stays advertised and is out of scope here.

## Scenarios

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

* *GIVEN* a VS expression node of type `function_scalar_case` (the Exasol node type for both simple and searched CASE, including the expansion of `NULLIF(...)`) carrying `arguments` (the WHEN test conditions) and `results` (the THEN values, with the optional ELSE as the last element when `results` has one more entry than `arguments`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return a `CASE WHEN <cond1> THEN <res1> [WHEN <condN> THEN <resN>]... [ELSE <else>] END` expression with each condition and result rendered recursively
* *AND* a `function_scalar_case` node with no WHEN branch SHALL return an error in raising mode and `None` in the safe variants

### Scenario: GREATEST and LEAST translate to DataFusion greatest/least

* *GIVEN* a VS expression node of type `function_scalar` named `GREATEST` or `LEAST` with one or more arguments
* *WHEN* `render_expression` processes the node
* *THEN* `GREATEST` SHALL render as `greatest(<a1>, <a2>, ...)` and `LEAST` SHALL render as `least(<a1>, <a2>, ...)` over the recursively rendered arguments

### Scenario: NULLIFZERO and ZEROIFNULL translate to NULLIF and COALESCE

* *GIVEN* a VS expression node of type `function_scalar` named `NULLIFZERO` or `ZEROIFNULL` with a single argument
* *WHEN* `render_expression` processes the node
* *THEN* `NULLIFZERO` SHALL render as `nullif(<arg>, 0)`
* *AND* `ZEROIFNULL` SHALL render as `coalesce(<arg>, 0)`

### Scenario: Regexp scalar functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, or `REGEXP_COUNT`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, so `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT` remain unadvertised — the investigation recorded under issue #106 re-verified this decline against the pinned DataFusion 54.0.0 and `regex` 1.12.4 and found no change (see issue #106)
* *AND* the exclusion MUST NOT alter the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement
