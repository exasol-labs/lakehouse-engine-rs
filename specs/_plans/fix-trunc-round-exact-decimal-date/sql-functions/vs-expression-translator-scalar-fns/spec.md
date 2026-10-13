<!-- DELTA:CHANGED -->
# Feature: VS Expression Translator — Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with
named scalar function translation: math functions, the modulo operator, string functions,
CASE expressions, and the NULLIF/COALESCE shorthands. These are distinct from the arithmetic
operators and CAST scenarios in `vs-expression-translator-scalar-ops`. `GREATEST`/`LEAST` are
specified in the sibling feature `sql-functions/vs-expression-translator-greatest-least`, and
`CONCAT`'s NULL-semantics rendering in `sql-functions/vs-expression-translator-concat`, both
split out to keep this feature's scenario count under the library threshold — the same treatment
`FLOAT_DIV` already received into `vs-expression-translator-float-div`. `TRUNC` and `ROUND` are
specified in `sql-functions/vs-expression-translator-trunc-round`, because their DataFusion-dialect
rendering calls functions the scan session registers rather than a DataFusion built-in.
<!-- /DELTA:CHANGED -->

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
`UNICODE`→`ascii`, `UNICODECHR`→`chr`, `NULLIFZERO`→`nullif(x,0)`, `ZEROIFNULL`→`coalesce(x,0)`,
`TRUNC`→`exa_trunc`, `ROUND`→`exa_round` (specified in `sql-functions/vs-expression-translator-trunc-round`).

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

The scalar regexp functions (`REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, `REGEXP_COUNT`)
are deliberately not translated. Re-verified for issue #106 against the pinned DataFusion 54.0.0
and `regex` 1.12.4: DataFusion runs the Rust `regex` crate, whose dialect rejects the pattern
backreferences and lookaround Exasol's PCRE dialect accepts; DataFusion has no `regexp_substr`; and
Exasol's position, occurrence, and return-option arguments have no matching DataFusion argument
shape. A compile-time literal-pattern check would certify pattern syntax, not match parity with
Exasol's PCRE, so it cannot lift the decline (see issue #106). The decline holds in BOTH dialects:
the adapter advertises one capability set for both, so a name renderable only in the Exasol wrapper
would still be pushed at the DataFusion scan. This is separate from the `FN_PRED_REGEXP_LIKE`
predicate, which stays advertised and whose per-dialect rendering is specified in
`sql-comprehension/vs-expression-translator`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Math scalar functions translate to DataFusion math calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol math functions: `ABS`, `FLOOR`, `CEIL`, `SQRT`, `POWER`, `EXP`, `LN`, `LOG`, `SIGN`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SINH`, `COSH`, `TANH`, `COT`, `DEGREES`, or `RADIANS`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SIGN`→`signum`, `CEIL`→`ceil`, `POWER`→`power`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* a node whose argument count does not match the function arity SHALL return an error in raising mode and `None` in the safe variants
* *AND* `ROUND` and `TRUNC` MUST NOT render through this name mapping: their DataFusion-dialect rendering is specified in `sql-functions/vs-expression-translator-trunc-round`
<!-- /DELTA:CHANGED -->
