# Feature: VS Expression Translator — GREATEST/LEAST NULL Semantics

Splits `GREATEST`/`LEAST` out of the shared scalar-function shape in
`sql-comprehension/vs-expression-translator-scalar-fns`, because merging this feature's new
regression scenario into that feature would have pushed it past the 10-scenario library threshold
(the same reason `FLOAT_DIV` was already split out of `vs-expression-translator-scalar-ops` into
`sql-comprehension/vs-expression-translator-float-div`). `GREATEST`/`LEAST` diverge from their
DataFusion counterpart on the NULL CONTRACT, not on name or arity (issue #202): Exasol returns NULL
if ANY argument is NULL, while DataFusion's `greatest`/`least` return NULL only if ALL arguments are
NULL. Both names still follow the verbatim rule on the Exasol dialect specified in
`sql-comprehension/vs-expression-translator-scalar-fns` — this split covers only the DataFusion-
dialect NULL-guard rendering and its live regression coverage.

## Background

**GREATEST/LEAST diverge on the NULL CONTRACT, not on the name (issue #202).** Exasol returns
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
set. `CONCAT` is fixed in its own right, and in the OPPOSITE direction to this pair: Exasol's `||`
treats a NULL operand as the empty string, so `concat`'s NULL-skipping is what `CONCAT` NEEDS —
`sql-comprehension/vs-expression-translator-scalar-fns` records its `nullif(concat(...), '')`
rendering and the live evidence for it (issue #374, superseding the chained-`||` rendering issue
\#200 had introduced on a since-refuted reading of Exasol's `||`). `NULLIF` matches
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

## Scenarios

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

### Scenario: A pushed-down GREATEST or LEAST over a NULL-producing argument returns NULL on the cluster

* *GIVEN* a virtual schema over the seeded 20-row fixture whose `id` runs 1..20 and whose `score` is `5.0 * id`, and a query calling `GREATEST` or `LEAST` with `NULLIF(MOD(id, 5), 0)` as one argument — NULL for the four multiples of 5 and non-NULL for the other sixteen rows
* *WHEN* Exasol pushes the query down and the scan UDF evaluates the expression in DataFusion
* *THEN* `SELECT COUNT(*) ... WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL` SHALL return `4`, NOT the `0` an unguarded `least(...)` returns, so the fixture discriminates correct from buggy behavior in the FILTER position
* *AND* `SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) ... ORDER BY id` SHALL return NULL for exactly the four multiples of 5 and the row's own `id` for the other sixteen, so the fixture discriminates in the VALUE position AND proves the guard leaves non-NULL rows unchanged
* *AND* the pushed-down SQL that `EXPLAIN VIRTUAL` reports SHALL carry the guarded form inside the scan spec, proving the expression was delegated to DataFusion rather than left for Exasol to evaluate — without that check a correct result would prove nothing about the translator, because Exasol evaluating the expression itself is also correct
* *AND* the test SHALL FAIL, not skip, when no Exasol container is reachable
