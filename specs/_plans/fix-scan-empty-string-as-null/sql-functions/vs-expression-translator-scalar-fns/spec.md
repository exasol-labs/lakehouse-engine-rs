# Feature: VS Expression Translator, Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with named scalar function translation: math functions, the modulo operator, string functions, CASE expressions, and the NULLIF and COALESCE shorthands. `vs-expression-translator-scalar-ops` covers the arithmetic operators and CAST. `sql-functions/vs-expression-translator-greatest-least` covers `GREATEST` and `LEAST`, and `sql-functions/vs-expression-translator-concat` covers the `CONCAT` NULL rendering.

## Background

The translator serves two SQL parsers through one recursive walker. The DataFusion entry points (`ScanTyping::render_expression`, `ScanTyping::render_filter`) feed DataFusion's SQL frontend inside the scan UDF. The Exasol entry points (`render_expression_exasol`, `ScanTyping::exasol_fragment`) feed outer wrapper SQL that Exasol's own engine parses.

In the DataFusion dialect, most Exasol `FN_*` names lower-case to the DataFusion function of the same name. These need explicit aliasing: `SIGN`→`signum`, `LENGTH`→`character_length`, `MOD`→`%` operator, `INSTR`/`LOCATE`→`strpos` (with operand reorder), `UNICODE`→`ascii`, `UNICODECHR`→`chr`, `NULLIFZERO`→`nullif(x,0)`, `ZEROIFNULL`→`coalesce(x,0)`.

In the Exasol dialect every one of those aliases is wrong, because Exasol has no function of the aliased name: `SIGNUM` and `STRPOS` return `function or script <NAME> not found` (SQL code 42000), and Exasol's parser rejects `%`. An Exasol scalar function therefore renders verbatim, with its original name, argument order and argument count, because Exasol's compiler emitted the call. The crate declares every translated `function_scalar` name once, with its Exasol-dialect form. That declaration gates the dispatch and drives the enforcing sweep test (see `sql-comprehension/vs-expression-translator`). A name with a DataFusion arm but no declaration is not translated. `GREATEST` and `LEAST` keep the `ExasolForm::VerbatimCall` declaration, and `CONCAT` keeps `ExasolForm::Shaped`.

These constructs are excluded from the verbatim rule and keep a dedicated rendering, because verbatim is impossible or wrong for them:

| Construct | Why it is not rendered verbatim |
|---|---|
| `ADD`, `SUB`, `MULT`, `NEG` | Wire names for operators, not Exasol function names. Both dialects render `(<l> + <r>)` and the rest. |
| `FLOAT_DIV` | An operator wire name whose rendering differs by dialect: `(CAST(<l> AS DOUBLE) / <r>)` in the DataFusion dialect, `(<l> / <r>)` in the Exasol dialect. Exasol's `/` is always float division, while DataFusion's `/` truncates integer and decimal operands. Specified in `sql-comprehension/vs-expression-translator-float-div`. |
| `MOD` | Exasol requires `MOD(a, b)`, and DataFusion offers only `%`. Its arm branches on dialect and validates arity. |
| `CONCAT` | Also the wire encoding of Exasol's `\|\|`, with a different rendering per dialect. Specified in `sql-functions/vs-expression-translator-concat`. |
| `CAST` | The target type differs: an Exasol character target needs an explicit length, and an Exasol `TIMESTAMP` target needs an explicit precision. Specified in `sql-comprehension/vs-expression-translator-cast`. |
| `function_scalar` named `CASE` | Exasol's interleaved-argument CASE encoding. Both dialects render `CASE WHEN … THEN … END`. It is distinct from the `function_scalar_case` node type. |

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: String scalar functions translate to DataFusion string calls

* *GIVEN* a `function_scalar` node named `CONCAT`, `LENGTH`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `CHR`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `INSTR`, `LOCATE`, `OCTET_LENGTH`, `UNICODE`, or `UNICODECHR`
* *WHEN* the adapter renders the node for the scan
* *THEN* the scan SQL SHALL call the DataFusion function of the same lower-cased name over the rendered arguments in order, except `LENGTH`→`character_length`, `INSTR`/`LOCATE`→`strpos` (string first, then substring), `UNICODE`→`ascii`, and `UNICODECHR`→`chr`
* *AND* a call that returns text SHALL be wrapped as `nullif(<call>, '')`, per `pushdown-types/pushdown-planning-scan-empty-string`
* *AND* an `INSTR` or `LOCATE` node with more than two arguments SHALL decline, because `strpos` takes no start position or occurrence
* *AND* `CONCAT` SHALL follow `sql-functions/vs-expression-translator-concat`, and `REPLACE` and a call with a NULL argument SHALL follow `pushdown-types/pushdown-planning-scan-empty-string`
<!-- /DELTA:CHANGED -->
