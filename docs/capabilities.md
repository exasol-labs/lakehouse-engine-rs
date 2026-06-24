[lakehouse-engine](../README.md) › [Docs](index.md) › Capabilities

---

# Capability Support Overview

Mental model: **DataFusion does per-shard work inside the UDF; Exasol coordinates across
shards and handles anything not pushed down.** A capability is advertised only if the VS can
translate it (vs-expression translator) or decompose it into a correct partial/merge plan.
Source of truth: `crates/lakehouse-engine/src/adapter/capabilities.rs`.

## Projection & expressions ✅

| Capability | DataFusion (per shard) | Exasol |
|---|---|---|
| `SELECTLIST_PROJECTION` | Reads only projected columns from Parquet | Concatenates shard outputs |
| `SELECTLIST_EXPRESSIONS` | Evaluates the expression during scan | Pass-through |

## Filtering ✅

| Capability | DataFusion (per shard) | Exasol |
|---|---|---|
| `FILTER_EXPRESSIONS` + `FN_PRED_*` (`EQUAL/LESS/BETWEEN/IN/IS_NULL/LIKE/REGEXP_LIKE`, `AND/OR/NOT`) | Prunes whole data files via Iceberg manifest stats, then skips row-groups/rows during scan | Re-checks only predicates it couldn't translate |
| Literals (`LITERAL_STRING/DOUBLE/DATE/TIMESTAMP/…`) | Consumed inside the predicate | — |
| `LIMIT` | Stops scanning early per shard | Re-applies as a cross-shard backstop |

## Scalar functions ✅

Computed during the scan, then passed through.

| Family | Functions |
|---|---|
| Math | `FN_ABS`, `FN_ROUND`, `FN_SQRT`, … (25) |
| String | `FN_CONCAT`, `FN_SUBSTR`, `FN_UPPER`, … (24) |
| Date / time | `FN_EXTRACT`, `FN_DATE_TRUNC`, `FN_YEAR`, … (14) |
| Conditional | `FN_CASE`, `FN_GREATEST`, `FN_NULLIFZERO`, … |

## Aggregation ✅

Partial aggregate per shard → merged by Exasol.

| Capability | DataFusion (per shard) | Exasol (merge) |
|---|---|---|
| `AGGREGATE_SINGLE_GROUP` + `COUNT/SUM/MIN/MAX` | Partial sum / count / min / max | `SUM(partials)`, `MIN`/`MAX` of extrema |
| `FN_AGG_AVG` | Partial sum + count | `SUM(sum)/SUM(count)`, NULL-safe |
| Statistical (`STDDEV*`, `VAR*`) | Sufficient stats (n, Σx, Σx²) | Combines into final stddev / variance |
| `AGGREGATE_GROUP_BY_COLUMN` / `_EXPRESSION` | Partial aggregate grouped by key | Final `GROUP BY` merge |
| `AGGREGATE_HAVING` | — | Applies `HAVING` on the merged result |

## Not pushed down ⛔

Exasol handles these after partial results return — correct, just less pushdown.

| Capability | Where it runs |
|---|---|
| JOIN (`JOIN_TYPE_*`, `JOIN_CONDITION_*`) | Usable via multi-table VS: Exasol pushes down each table, then joins the result sets |
| `ORDER BY` | Exasol sorts the final result |
| `COUNT(DISTINCT)` / `MEDIAN` / `APPROX_COUNT_DISTINCT` | Not decomposable into partial/merge — Exasol computes on returned rows |
| `LISTAGG` / `GROUP_CONCAT` | Exasol-side |
| `AGGREGATE_GROUP_BY_TUPLE`, geospatial, session fns | Exasol-side / unsupported |
