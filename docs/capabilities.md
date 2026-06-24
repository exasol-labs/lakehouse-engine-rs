# Capability Support Overview

Mental model: **DataFusion does per-shard work inside the UDF; Exasol coordinates
across shards and handles anything not pushed down.** A capability is advertised only
if the VS can either translate it (vs-expression translator) or decompose it into a
correct partial/merge plan. Source of truth: `crates/lakehouse-engine/src/adapter/capabilities.rs`.

| Capability | Example SQL | DataFusion (in-UDF, per shard) | Exasol (cluster) | Status |
|---|---|---|---|---|
| `SELECTLIST_PROJECTION` | `SELECT id, name` | Reads only projected columns from Parquet | Concatenates shard outputs | ✅ |
| `SELECTLIST_EXPRESSIONS` | `SELECT price*1.2` | Evaluates the expression during scan | Pass-through | ✅ |
| `FILTER_EXPRESSIONS` + predicates (`FN_PRED_EQUAL/LESS/BETWEEN/IN/IS_NULL/LIKE/REGEXP_LIKE`, `AND/OR/NOT`) | `WHERE region='EU' AND qty>10` | Applies predicate during scan (skips non-matching rows/row-groups) | Re-checks only untranslatable predicates it kept | ✅ |
| Literals (`LITERAL_STRING/DOUBLE/DATE/TIMESTAMP/...`) | `WHERE d = DATE '2024-01-01'` | Consumes the typed literal in the predicate | — | ✅ |
| `LIMIT` | `... LIMIT 100` | Stops scanning early per shard | Re-applies `LIMIT` as correctness backstop across shards | ✅ |
| Math fns (`FN_ABS`, `FN_ROUND`, `FN_SQRT`, …25) | `SELECT ROUND(amt,2)` | Computes via vs-expression→DataFusion SQL | Pass-through | ✅ |
| String fns (`FN_CONCAT`, `FN_SUBSTR`, `FN_UPPER`, …24) | `WHERE UPPER(code)='AB'` | Computes during scan | Pass-through | ✅ |
| Date fns (`FN_EXTRACT`, `FN_DATE_TRUNC`, `FN_YEAR`, …14) | `WHERE YEAR(ts)=2024` | Computes during scan | Pass-through | ✅ |
| Conditional fns (`FN_CASE`, `FN_GREATEST`, `FN_NULLIFZERO`, …) | `SELECT CASE WHEN x>0 THEN 'p' END` | Computes during scan | Pass-through | ✅ |
| `AGGREGATE_SINGLE_GROUP` + `FN_AGG_COUNT/SUM/MIN/MAX` | `SELECT SUM(amt) FROM t` | Emits **partial** sum/count/min/max per shard | **Merges** partials: `SUM(partial_sum)`, `SUM(partial_count)`, `MIN/MAX` of extrema | ✅ |
| `FN_AGG_AVG` | `SELECT AVG(amt)` | Emits partial **sum + count** (not an average) | Computes `SUM(sum)/SUM(count)`, NULL-safe on zero | ✅ |
| Statistical (`FN_AGG_STDDEV*`, `FN_AGG_VAR*`) | `SELECT STDDEV(x)` | Emits sufficient stats (n, Σx, Σx²) per shard | Combines stats into final stddev/variance | ✅ |
| `AGGREGATE_GROUP_BY_COLUMN` / `_EXPRESSION` | `SELECT k, SUM(v) GROUP BY k` | Per-shard partial aggregate grouped by key | Final `GROUP BY` merge across shards | ✅ |
| `AGGREGATE_HAVING` | `... HAVING SUM(v)>100` | (filter applies post-merge) | Applies `HAVING` on the merged result | ✅ |
| **JOIN** (`JOIN`, `JOIN_TYPE_*`, `JOIN_CONDITION_*`) | `FROM a JOIN b ON a.id=b.id` | — (not pushed) | **Exasol** issues one pushdown per table, joins result sets itself | ⛔ not advertised → **enabled by multi-table VS** |
| `ORDER BY` | `... ORDER BY ts` | — | Sorts the final result | ⛔ not advertised; Exasol-side |
| `COUNT(DISTINCT)` / `MEDIAN` / `APPROX_COUNT_DISTINCT` | `COUNT(DISTINCT u)` | — (not decomposable into partial/merge) | Exasol computes after raw rows returned | ⛔ not advertised |
| `LISTAGG` / `GROUP_CONCAT` | `LISTAGG(name)` | — | Exasol-side | ⛔ not advertised |
| `AGGREGATE_GROUP_BY_TUPLE`, geospatial, Exasol session fns | — | — | Exasol-side / unsupported | ⛔ not advertised |

**Why the ⛔ rows aren't pushed down:** JOIN, ORDER BY, and DISTINCT/MEDIAN aggregates
don't fit the per-shard→merge model, so Exasol handles them after partial results
return — correct, just less pushdown. JOIN specifically becomes *usable* once the VS
exposes multiple tables (this plan), because Exasol can then resolve each table's
pushdown independently and join the result sets itself.
