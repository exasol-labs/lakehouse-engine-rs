# Feature: VS Expression Translator — Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with
named scalar function translation: math functions, the modulo operator, string functions,
CASE expressions, and the NULLIF/COALESCE shorthands. These are distinct from the arithmetic
operators and CAST scenarios in `vs-expression-translator-scalar-ops`. `GREATEST`/`LEAST` are
specified in the sibling feature `sql-functions/vs-expression-translator-greatest-least`, and
`CONCAT`'s NULL-semantics rendering in `sql-functions/vs-expression-translator-concat`, both
split out to keep this feature's scenario count under the library threshold — the same treatment
`FLOAT_DIV` already received into `vs-expression-translator-float-div`.

<!-- DELTA:CHANGED -->
## Background

The translator serves two SQL parsers through one recursive walker, selected by the entry point:
the DataFusion trio (`render_expression`, `render_expression_safe`, `render_df_filter_safe`) feeds
DataFusion's SQL frontend inside the scan UDF, and the Exasol trio (`render_expression_exasol`,
`render_expression_exasol_safe`, `render_df_filter_exasol_safe`) feeds outer wrapper SQL that
Exasol's own core engine parses.

**In the DataFusion dialect**, most Exasol `FN_*` names lower-case directly to the DataFusion
function of the same name; a documented set needs explicit aliasing: `SIGN`→`signum`,
`LENGTH`→`character_length`, `MOD`→`%` operator, `INSTR`/`LOCATE`→`strpos` (with operand reorder),
`UNICODE`→`ascii`, `UNICODECHR`→`chr`, `NULLIFZERO`→`nullif(x,0)`, `ZEROIFNULL`→`coalesce(x,0)`.

**In the Exasol dialect** every one of those aliases is wrong, because the fragment is evaluated by
Exasol and Exasol has no function of the aliased name. The rule is therefore inverted and uniform:
an Exasol scalar function renders VERBATIM — original name, original argument order, original
argument count — because Exasol's own compiler emitted that call and Exasol can evaluate exactly
what it sent. Every translated `function_scalar` name is declared exactly once in the crate with its
Exasol-dialect form, and that one declaration both gates the dispatch and drives the enforcing sweep
test (see `sql-comprehension/vs-expression-translator`). A name that joins a DataFusion arm without
joining the declaration is therefore not translated at all, rather than silently rendering DataFusion
SQL on the Exasol path. Verified on live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`),
the aliases are hard compilation errors there: `SIGNUM` and `STRPOS` both return `function or script
<NAME> not found` (SQL code 42000), and `%` is rejected by Exasol's parser (issue #197).
`GREATEST`/`LEAST` also follow this verbatim rule on the Exasol dialect — both names keep the
`ExasolForm::VerbatimCall` declaration — but their DataFusion-dialect NULL-guard rendering and its
full evidence live in `sql-functions/vs-expression-translator-greatest-least`. `CONCAT` keeps
its `ExasolForm::Shaped` declaration and follows the verbatim rule on the Exasol dialect too — its
DataFusion-dialect NULL-guard rendering and its full evidence live in
`sql-functions/vs-expression-translator-concat`.

Five constructs are deliberately EXCLUDED from the verbatim rule and keep a dedicated rendering in
both dialects, because verbatim is either impossible or wrong for them:

| Construct | Why it is not rendered verbatim |
|---|---|
| `ADD`, `SUB`, `MULT`, `NEG` | Wire names for operators, not Exasol function names — Exasol has no function called `ADD`. Both dialects render `(<l> + <r>)` and the rest. |
| `FLOAT_DIV` | Also an operator wire name, and one of the TWO whose rendering DIVERGES by dialect: `(CAST(<l> AS DOUBLE) / <r>)` in the DataFusion dialect, a bare `(<l> / <r>)` in the Exasol dialect. Exasol's `/` IS `FN_FLOAT_DIV` — always true float division, whatever the operand types — while DataFusion's `/` is operand-typed and truncates integer and decimal operands (issue #186). The cast is what makes DataFusion reproduce Exasol; Exasol needs no help. Specified in `sql-comprehension/vs-expression-translator-float-div`, including the sweep-test scenario that keeps `FLOAT_DIV` out of this table's verbatim rule. |
| `MOD` | Exasol requires `MOD(a, b)`, DataFusion offers only the `%` operator (issue #197). Its arm branches on dialect and validates arity, which the verbatim rule does not. |
| `CONCAT` | Also the wire encoding of Exasol's `\|\|` operator, and the SECOND of the five whose rendering diverges by dialect: `nullif(concat(<a1>, <a2>, ...), '')` in the DataFusion dialect, chained `(<a1> \|\| <a2> ...)` in the Exasol dialect, because Exasol's `\|\|` treats a NULL operand as the empty string while DataFusion's `\|\|` propagates NULL (issue #374). Specified in `sql-functions/vs-expression-translator-concat`, including the sweep-test scenario that keeps `CONCAT` out of this table's verbatim rule. |
| `CAST` | The target type, not the name, is what differs: an Exasol character target needs an explicit length and an Exasol `TIMESTAMP` target needs an explicit precision. Its per-dialect rendering is specified in `sql-comprehension/vs-expression-translator-cast` and is unchanged by this feature. |
| `function_scalar` named `CASE` | Exasol's interleaved-argument CASE encoding; both dialects render `CASE WHEN … THEN … END`, so there is no call form to render verbatim. This is the `function_scalar`+`name=CASE` alternate encoding, distinct from the `function_scalar_case` node type scenario below. |

`REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, and `REGEXP_COUNT` are not
translated in either dialect, and Exasol evaluates them (issue #106, (#TBD)), because DataFusion's
Rust `regex` matching does not reproduce Exasol's PCRE matching.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Regexp scalar functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, or `REGEXP_COUNT`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return the error `unsupported scalar function: <NAME>`
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking, and `render_expression_exasol` / `render_expression_exasol_safe` SHALL decline these five names identically — one capability set serves both dialects, so the Exasol dialect's verbatim rule MUST NOT widen the translated set
* *AND* the adapter SHALL NOT advertise `FN_PRED_REGEXP_LIKE`, `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`, so Exasol evaluates each of them (see `pushdown-capabilities/pushdown-planning-string-conversion-capability`)
<!-- /DELTA:CHANGED -->
