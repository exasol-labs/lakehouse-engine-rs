# Feature: VS Expression Translator — Scalar Functions

Extends the VS expression translator (`sql-comprehension/vs-expression-translator`) with
named scalar function translation: math functions, the modulo operator, string functions,
CASE expressions, GREATEST/LEAST, and the NULLIF/COALESCE shorthands. These are distinct
from the arithmetic operators and CAST scenarios in `vs-expression-translator-scalar-ops`.

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
`GREATEST`/`LEAST` are the one pair whose DataFusion counterpart shares Exasol's name AND arity but
NOT its NULL contract, so their DataFusion rendering is a NULL-guarded wrapper around the
same-named call rather than a name mapping (issue #202, below).

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

Five constructs are deliberately EXCLUDED from the verbatim rule and keep a dedicated rendering in
both dialects, because verbatim is either impossible or wrong for them:

| Construct | Why it is not rendered verbatim |
|---|---|
| `ADD`, `SUB`, `MULT`, `NEG` | Wire names for operators, not Exasol function names — Exasol has no function called `ADD`. Both dialects render `(<l> + <r>)` and the rest. |
| `FLOAT_DIV` | Also an operator wire name, and the ONLY one of the five whose rendering DIVERGES by dialect: `(CAST(<l> AS DOUBLE) / <r>)` in the DataFusion dialect, a bare `(<l> / <r>)` in the Exasol dialect. Exasol's `/` IS `FN_FLOAT_DIV` — always true float division, whatever the operand types — while DataFusion's `/` is operand-typed and truncates integer and decimal operands (issue #186). The cast is what makes DataFusion reproduce Exasol; Exasol needs no help. Specified in `sql-comprehension/vs-expression-translator-float-div`, including the sweep-test scenario that keeps `FLOAT_DIV` out of this table's verbatim rule. |
| `MOD` | Exasol requires `MOD(a, b)`, DataFusion offers only the `%` operator (issue #197). Its arm branches on dialect and validates arity, which the verbatim rule does not. |
| `CONCAT` | Both dialects render chained `\|\|`, never `concat()`: `concat()` silently drops NULL arguments while `\|\|` propagates NULL, and a boolean operand needs Exasol's `TRUE`/`FALSE` casing (issue #200). |
| `CAST` | The target type, not the name, is what differs: an Exasol character target needs an explicit length and an Exasol `TIMESTAMP` target needs an explicit precision. Its per-dialect rendering is specified in `sql-comprehension/vs-expression-translator-cast` and is unchanged by this feature. |
| `function_scalar` named `CASE` | Exasol's interleaved-argument CASE encoding; both dialects render `CASE WHEN … THEN … END`, so there is no call form to render verbatim. This is the `function_scalar`+`name=CASE` alternate encoding, distinct from the `function_scalar_case` node type scenario below. |

**`GREATEST`/`LEAST` diverge on the NULL CONTRACT, not on the name (issue #202).** Exasol returns
NULL if ANY argument is NULL; DataFusion returns NULL only if ALL arguments are NULL. Both halves
were captured for issue #202 rather than recalled. On the Exasol 2025.2.1 container pinned in
`docker-compose.yml`, `SELECT GREATEST(0.0, NULL), LEAST(1.0, NULL), GREATEST(1, 2, NULL),
SQRT(GREATEST(0.0, NULL)), GREATEST(CAST(NULL AS DOUBLE)), LEAST(NULL, NULL), GREATEST('a', NULL)
FROM dual` returned NULL in EVERY column, while `GREATEST(5)` returned `5`. The pinned DataFusion
54.1.0 documents the opposite for both names — "Returns the greatest value in a list of expressions.
Returns _null_ if all expressions are _null_" and the matching sentence for `least`
(`datafusion-functions-54.1.0/src/core/greatest.rs:40` and `.../least.rs:40`). Issue #202 captured
the end-to-end consequence against a native Exasol `TEST` schema: `WHERE LEAST(l_tax, l_discount,
NULL) IS NULL` matched all 9965 rows natively and 0 rows through the virtual schema, and
`GREATEST(c_acctbal, NULLIF(c_acctbal, c_acctbal))` returned NULL natively and `711.56` through the
virtual schema.

**The adapter owns the equivalence, so the guard belongs in the rendering.** `capabilities.rs`
advertises `FN_GREATEST` and `FN_LEAST`, and Exasol never independently re-checks or re-applies an
advertised capability — there is no Exasol-side fallback once a capability is advertised. The
adapter therefore MUST generate SQL equivalent to Exasol's own semantics for anything it advertises,
and a NULL contract is part of those semantics. Both capabilities stay advertised; the DataFusion
dialect carries the guard.

**The divergence is confined to this one pair.** Every arm of the `function_scalar` match was
audited against DataFusion's documented NULL semantics for issue #202. DataFusion documents
NULL-skipping for exactly five functions — `coalesce`, `concat`, `concat_ws`, `greatest`, and
`least` — and of the names this translator maps, only `CONCAT` and `GREATEST`/`LEAST` reach that
set. `CONCAT` was already fixed for this very reason (chained `||`, issue #200). `NULLIF` matches
Exasol's own documented `CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END` definition under
identical three-valued logic, so no divergence exists. `NULLIFZERO`→`nullif(x, 0)` and
`ZEROIFNULL`→`coalesce(x, 0)` pass a single argument plus a literal, so `coalesce`'s NULL-skipping is
the INTENDED behavior there rather than an accident. `MOD` renders the `%` operator, which propagates
NULL in both engines. The remaining multi-argument names — `LPAD`/`RPAD`, `TRANSLATE`, `REPLACE`,
`INSTR`/`LOCATE`, `POWER`/`ATAN2`, two-argument `ROUND`/`TRUNC`/`LOG`, `TO_DATE`/`TO_TIMESTAMP`, and
the `*_BETWEEN` family — carry no ignore-NULL behavior on either side. The scope boundary is
deliberate: no further name needs a guard, and this bullet exists so the question is not
re-litigated.

**The guard duplicates each argument's rendered TEXT, and that is safe here.** The rendered SQL
names every argument twice — once in its `IS NULL` clause, once inside the call — so DataFusion may
evaluate a nested argument expression twice. No translated `function_scalar` name is
non-deterministic: `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` are
deliberately absent from the declaration table and their capabilities are withdrawn, precisely
because their value depends on context the scan never receives. Two copies of one argument therefore
always evaluate to the same value.

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

## Scenarios

### Scenario: Math scalar functions translate to DataFusion math calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol math functions: `ABS`, `ROUND`, `FLOOR`, `CEIL`, `SQRT`, `POWER`, `EXP`, `LN`, `LOG`, `SIGN`, `TRUNC`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SINH`, `COSH`, `TANH`, `COT`, `DEGREES`, or `RADIANS`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SIGN`→`signum`, `CEIL`→`ceil`, `POWER`→`power`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* a node whose argument count does not match the function arity SHALL return an error in raising mode and `None` in the safe variants

### Scenario: Math scalar functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named with one of the math functions listed in the preceding scenario
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<arg1_sql>[, <arg2_sql>])` using the function's own uppercased Exasol name and its arguments in the order received, each rendered recursively in the Exasol dialect
* *AND* `SIGN` in particular MUST render as `SIGN(<arg_sql>)` and MUST NOT render as `signum(...)`, which Exasol rejects with `function or script SIGNUM not found` (SQL code 42000) when the fragment reaches an Exasol-parsed wrapper (issue #209)
* *AND* the DataFusion-dialect rendering of every listed name MUST remain byte-identical to the preceding scenario
* *AND* the Exasol dialect SHALL NOT impose its own arity check, because Exasol's compiler emitted a call its own engine accepts — the same rule the string-function family already follows for Exasol's three-argument `INSTR`

### Scenario: MOD translates to the modulo operator

* *GIVEN* a VS expression node of type `function_scalar` named `MOD` with two arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `(<left> % <right>)`, because DataFusion exposes modulo as the `%` operator rather than a `mod()` function
* *AND* both operands SHALL be rendered recursively
* *AND* `render_expression_exasol` SHALL return `MOD(<left>, <right>)` instead, because Exasol's parser rejects `%` (issue #197)
* *AND* a node whose argument count is not exactly two SHALL return an error in raising mode and `None` in the safe variants, in BOTH dialects

### Scenario: String scalar functions translate to DataFusion string calls

* *GIVEN* a VS expression node of type `function_scalar` whose `name` is one of the supported Exasol string functions: `CONCAT`, `LENGTH`, `LOWER`, `UPPER`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `ASCII`, `CHR`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, `INSTR`, `LOCATE`, `OCTET_LENGTH`, `UNICODE`, or `UNICODECHR`
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL render the node as the corresponding DataFusion SQL function applied to its rendered arguments in order, using these name mappings: `SUBSTR`→`substr`, `LENGTH`→`character_length`, `OCTET_LENGTH`→`octet_length`, `INSTR`/`LOCATE`→`strpos` (with operands ordered string-then-substring per DataFusion `strpos(string, substring)`), `UNICODE`→`ascii`, `UNICODECHR`→`chr`, and all other listed names lower-cased to their identically-named DataFusion function
* *AND* each argument SHALL be rendered recursively by the translator
* *AND* `LOCATE`/`INSTR` argument reordering MUST preserve the Exasol semantics of "position of substring within string"
* *AND* `CONCAT` SHALL be rendered by the dedicated chained-`||` rule (see Background) in both dialects, not by this name-mapping table

### Scenario: String scalar functions render verbatim in the Exasol dialect

* *GIVEN* a VS expression node of type `function_scalar` named with one of the string functions listed in the preceding scenario, `CONCAT` excepted
* *WHEN* `render_expression_exasol` processes the node
* *THEN* the translator SHALL return `<NAME>(<args_sql>)` using the function's own uppercased Exasol name and its arguments in the order received, with no name mapping and no operand reordering
* *AND* `INSTR` and `LOCATE` in particular MUST keep Exasol's own argument order and MUST NOT render as `strpos(...)`, which Exasol rejects with `function or script STRPOS not found` (SQL code 42000)
* *AND* an `INSTR` or `LOCATE` node carrying Exasol's optional start-position or occurrence argument SHALL render with that argument forwarded unchanged, because Exasol's native function accepts it — this is what lets the arity decline in `vs-adapter/pushdown-planning-string-fn-type-coercion` still evaluate correctly on the Exasol side
* *AND* the DataFusion-dialect rendering of every listed name MUST remain byte-identical to the preceding scenario

### Scenario: CASE expression translates to SQL CASE WHEN

* *GIVEN* a VS expression node of type `function_scalar_case` (the Exasol node type for both simple and searched CASE, including the expansion of `NULLIF(...)`) carrying `arguments` (the WHEN test conditions) and `results` (the THEN values, with the optional ELSE as the last element when `results` has one more entry than `arguments`)
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return a `CASE WHEN <cond1> THEN <res1> [WHEN <condN> THEN <resN>]... [ELSE <else>] END` expression with each condition and result rendered recursively
* *AND* a `function_scalar_case` node with no WHEN branch SHALL return an error in raising mode and `None` in the safe variants

<!-- DELTA:CHANGED -->
### Scenario: GREATEST and LEAST translate to DataFusion greatest/least

* *GIVEN* a VS expression node of type `function_scalar` named `GREATEST` or `LEAST` with one or more arguments
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `CASE WHEN <a1> IS NULL[ OR <a2> IS NULL]... THEN NULL ELSE <df_name>(<a1>, <a2>, ...) END`, where `<df_name>` is `greatest` for `GREATEST` and `least` for `LEAST` — REPLACING the recorded bare `greatest(<a1>, <a2>, ...)` / `least(<a1>, <a2>, ...)` rendering, which returns the largest or smallest NON-NULL argument where Exasol returns NULL and so silently returns wrong values and wrong row sets (issue #202)
* *AND* the guard SHALL name EVERY argument, in the order received, joined by ` OR `, so the expression yields NULL if ANY argument is NULL — matching Exasol, which returns NULL if ANY argument is NULL, rather than DataFusion's `greatest`/`least`, which return NULL only if ALL arguments are NULL
* *AND* each argument SHALL be rendered ONCE and its rendered SQL text referenced TWICE — once in its own `IS NULL` clause and once inside the call — so an argument's two occurrences cannot diverge, and duplicating the text SHALL be safe because the translated `function_scalar` surface declares no non-deterministic name (see Background)
* *AND* the `ELSE` branch SHALL keep the `<df_name>(...)` call and MUST NOT be collapsed to a bare NULL literal, because the call is what gives the expression its result TYPE: with an all-NULL-typed CASE, `LEAST(<col>, NULL)` would yield a Null-typed column instead of one carrying the arguments' common type
* *AND* a single-argument call SHALL render the degenerate one-clause guard `CASE WHEN <a1> IS NULL THEN NULL ELSE <df_name>(<a1>) END`, which is Exasol's own single-argument behavior — `GREATEST(5)` is `5` and `GREATEST(CAST(NULL AS DOUBLE))` is NULL, both captured live
* *AND* the guard SHALL be emitted for EVERY argument list, including one whose arguments are all provably non-nullable, because the translator receives no nullability metadata for a `column` node and MUST NOT infer it
* *AND* a node whose `arguments` key is absent SHALL return an error, and an EMPTY argument list SHALL return an error in raising mode and `None` in the safe variants
* *AND* `render_expression_exasol` SHALL render `GREATEST(<a1>, <a2>, ...)` / `LEAST(<a1>, <a2>, ...)` under the same verbatim rule as the other Exasol scalar functions, BYTE-IDENTICAL to its pre-delta output and carrying NO guard, because Exasol's own `GREATEST`/`LEAST` already propagate NULL — both names keep their `ExasolForm::VerbatimCall` declaration, so the gate ahead of the per-name dispatch still serves the Exasol dialect and the declaration-driven verbatim sweep test stays green unchanged
* *AND* `capabilities.rs` SHALL keep advertising `FN_GREATEST` and `FN_LEAST` unchanged, so the fix restores the semantics at the rendering site rather than withdrawing the pushdown
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A pushed-down GREATEST or LEAST over a NULL-producing argument returns NULL on the cluster

* *GIVEN* a virtual schema over the seeded 20-row fixture whose `id` runs 1..20 and whose `score` is `5.0 * id`, and a query calling `GREATEST` or `LEAST` with `NULLIF(MOD(id, 5), 0)` as one argument — NULL for the four multiples of 5 and non-NULL for the other sixteen rows
* *WHEN* Exasol pushes the query down and the scan UDF evaluates the expression in DataFusion
* *THEN* `SELECT COUNT(*) ... WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL` SHALL return `4`, NOT the `0` an unguarded `least(...)` returns, so the fixture discriminates correct from buggy behavior in the FILTER position
* *AND* `SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) ... ORDER BY id` SHALL return NULL for exactly the four multiples of 5 and the row's own `id` for the other sixteen, so the fixture discriminates in the VALUE position AND proves the guard leaves non-NULL rows unchanged
* *AND* the pushed-down SQL that `EXPLAIN VIRTUAL` reports SHALL carry the guarded form inside the scan spec, proving the expression was delegated to DataFusion rather than left for Exasol to evaluate — without that check a correct result would prove nothing about the translator, because Exasol evaluating the expression itself is also correct
* *AND* the test SHALL FAIL, not skip, when no Exasol container is reachable
<!-- /DELTA:NEW -->

### Scenario: NULLIFZERO and ZEROIFNULL translate to NULLIF and COALESCE

* *GIVEN* a VS expression node of type `function_scalar` named `NULLIFZERO` or `ZEROIFNULL` with a single argument
* *WHEN* `render_expression` processes the node
* *THEN* `NULLIFZERO` SHALL render as `nullif(<arg>, 0)`
* *AND* `ZEROIFNULL` SHALL render as `coalesce(<arg>, 0)`
* *AND* `render_expression_exasol` SHALL render `NULLIFZERO(<arg>)` / `ZEROIFNULL(<arg>)` verbatim, because both are native Exasol functions and the rewrite exists only to reach a DataFusion equivalent

### Scenario: NULLIF translates to the DataFusion nullif call

* *GIVEN* a VS expression node of type `function_scalar` named `NULLIF` with two arguments, distinct from the `function_scalar_case` node type that carries Exasol's own expansion of `NULLIF(...)` and whose rendering the CASE scenario above specifies
* *WHEN* `render_expression` processes the node
* *THEN* the translator SHALL return `nullif(<a>, <b>)` over the recursively rendered arguments
* *AND* `render_expression_exasol` SHALL return `NULLIF(<a>, <b>)` under the same verbatim rule as the other Exasol scalar functions, because `NULLIF` is a native Exasol function and the lower-cased call is a DataFusion form
* *AND* an argument count other than two SHALL return an error in raising mode and `None` in the safe variants in the DataFusion dialect
* *AND* the Exasol dialect SHALL NOT impose that arity check, for the reason given in the math-function scenario above: Exasol's compiler emitted a call its own engine accepts

### Scenario: Regexp scalar functions are deliberately not translated

* *GIVEN* a VS expression node of type `function_scalar` named `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, or `REGEXP_COUNT`
* *WHEN* `render_expression` processes the node in raising mode
* *THEN* the translator SHALL return an error naming the function as unsupported
* *AND* `render_expression_safe` SHALL return `None` for the same node without panicking, and `render_expression_exasol` / `render_expression_exasol_safe` SHALL decline these four names identically — one capability set serves both dialects, so the Exasol dialect's verbatim rule MUST NOT widen the translated set
* *AND* the adapter SHALL omit the expression and let Exasol evaluate it, so `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT` remain unadvertised — the investigation recorded under issue #106 re-verified this decline against the pinned DataFusion 54.0.0 and `regex` 1.12.4 and found no change (see issue #106)
* *AND* the exclusion MUST NOT alter the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement
