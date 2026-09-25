# Feature: VS Expression Translator — Scalar Functions

Unchanged by this plan, and reproduced here only because the delta validator requires a description. The recorded text stands as written in `specs/sql-comprehension/vs-expression-translator-scalar-fns/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/sql-comprehension/vs-expression-translator-scalar-fns/spec.md`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: String scalar functions translate to DataFusion string calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol string functions: `CONCAT`, `LENGTH`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `CHR`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `INSTR`, `LOCATE`, `OCTET_LENGTH`, `UNICODE`, or `UNICODECHR`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SUBSTR`→`substr`, `LENGTH`→`character_length`, `OCTET_LENGTH`→`octet_length`, `INSTR`/`LOCATE`→`strpos` (with operands ordered string-then-substring per DataFusion `strpos(string, substring)`), `UNICODE`→`ascii`, `UNICODECHR`→`chr`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator, and each string-converted argument SHALL render per `sql-comprehension/vs-expression-translator-string-conversion`
* *AND* `LOCATE`/`INSTR` argument reordering MUST preserve the Exasol semantics of "position of substring within string", and an `INSTR` or `LOCATE` call carrying more than two arguments SHALL be a render error per the same feature
* *AND* `CONCAT` SHALL be rendered by its own dedicated per-dialect rule, specified in `sql-comprehension/vs-expression-translator-concat`, not by this name-mapping table
<!-- /DELTA:CHANGED -->
