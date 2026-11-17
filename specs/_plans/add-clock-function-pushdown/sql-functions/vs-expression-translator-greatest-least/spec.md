# Feature: VS Expression Translator, GREATEST/LEAST NULL Semantics

Renders `GREATEST` and `LEAST` in the DataFusion dialect with a NULL guard, so they match Exasol's NULL contract. The Exasol dialect follows the verbatim rule in `sql-functions/vs-expression-translator-scalar-fns`.

<!-- DELTA:CHANGED -->
## Background

* Exasol returns NULL if any argument of `GREATEST` or `LEAST` is NULL. DataFusion's `greatest` and `least` return NULL only if all arguments are NULL.
* The adapter advertises `FN_GREATEST` and `FN_LEAST`, and Exasol does not re-apply an advertised capability. The adapter must therefore generate SQL with Exasol's semantics, and the DataFusion dialect carries the guard.
* The guard names each argument twice, once in its `IS NULL` clause and once in the call, so DataFusion may evaluate a nested argument twice. That is safe because no translated `function_scalar` name is non-deterministic. A clock function reaches the translator only as the literal the adapter folds it into (`pushdown/pushdown-planning-clock-functions`).
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: A pushed-down GREATEST or LEAST over a NULL-producing argument returns NULL on the cluster

* *GIVEN* a virtual schema over the seeded 20-row fixture whose `id` runs 1..20 and whose `score` is `5.0 * id`, and a query calling `GREATEST` or `LEAST` with `NULLIF(MOD(id, 5), 0)` as one argument, NULL for the four multiples of 5 and non-NULL for the other sixteen rows
* *WHEN* Exasol pushes the query down and the scan UDF evaluates the expression in DataFusion
* *THEN* `SELECT COUNT(*) ... WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL` SHALL return `4`, NOT the `0` an unguarded `least(...)` returns, so the fixture discriminates correct from buggy behavior in the FILTER position
* *AND* `SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) ... ORDER BY id` SHALL return NULL for exactly the four multiples of 5 and the row's own `id` for the other sixteen, so the fixture discriminates in the VALUE position AND proves the guard leaves non-NULL rows unchanged
