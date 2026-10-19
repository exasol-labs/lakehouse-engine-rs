# Feature: VS Expression Translator, GREATEST/LEAST NULL Semantics

Renders `GREATEST` and `LEAST` in the DataFusion dialect with a NULL guard, so they match Exasol's NULL contract. The Exasol dialect follows the verbatim rule in `sql-functions/vs-expression-translator-scalar-fns`.

## Background

* Exasol returns NULL if any argument of `GREATEST` or `LEAST` is NULL. DataFusion's `greatest` and `least` return NULL only if all arguments are NULL.
* The adapter advertises `FN_GREATEST` and `FN_LEAST`, and Exasol does not re-apply an advertised capability. The adapter must therefore generate SQL with Exasol's semantics, and the DataFusion dialect carries the guard.
* The guard names each argument twice, once in its `IS NULL` clause and once in the call, so DataFusion may evaluate a nested argument twice. That is safe because no translated `function_scalar` name is non-deterministic. `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP` and `SYSTIMESTAMP` are absent from the declaration table, and their capabilities are withdrawn.

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
