[lakehouse-engine](../README.md) › [Docs](index.md) › Capabilities

---

# Capability Support Overview

Mental model: **DataFusion does per-shard work inside the UDF; Exasol coordinates across
shards and handles anything not pushed down.** A capability is advertised only if the VS can
translate it (vs-expression translator) or decompose it into a correct partial/merge plan.
Source of truth: `crates/lakehouse-engine/src/adapter/capabilities.rs`.

## Projection & expressions ✅

Reads only projected columns; select-list expressions are evaluated during the scan, then passed through.

| Capability | Example |
|---|---|
| `SELECTLIST_PROJECTION` | `SELECT id, name` |
| `SELECTLIST_EXPRESSIONS` | `SELECT price * 1.2` |

## Filtering ✅

Translated predicates prune whole data files via Iceberg manifest stats, then skip row-groups/rows during the scan; Exasol re-checks only what it couldn't translate. `LIMIT` stops the scan early per shard and is re-applied as a cross-shard backstop.

| Capability group | Capabilities | Example |
|---|---|---|
| Filter | `FILTER_EXPRESSIONS` | `WHERE region = 'EU'` |
| Logical | `FN_PRED_AND`, `FN_PRED_OR`, `FN_PRED_NOT` | `WHERE a AND NOT b` |
| Comparison | `FN_PRED_EQUAL`, `FN_PRED_NOTEQUAL`, `FN_PRED_LESS`, `FN_PRED_LESSEQUAL`, `FN_PRED_BETWEEN`, `FN_PRED_IN_CONSTLIST`, `FN_PRED_IS_NULL`, `FN_PRED_IS_NOT_NULL`, `FN_PRED_LIKE`, `FN_PRED_LIKE_ESCAPE`, `FN_PRED_REGEXP_LIKE` | `WHERE qty BETWEEN 1 AND 10` |
| Literals | `LITERAL_BOOL`, `LITERAL_DATE`, `LITERAL_DOUBLE`, `LITERAL_EXACTNUMERIC`, `LITERAL_NULL`, `LITERAL_STRING`, `LITERAL_TIMESTAMP`, `LITERAL_TIMESTAMP_UTC` | `WHERE d = DATE '2024-01-01'` |
| Limit | `LIMIT` | `... LIMIT 100` |

`FN_PRED_GREATER` / `FN_PRED_GREATEREQUAL` are not Exasol capability names — Exasol normalises `a > b` to `b < a` before it reaches the adapter.

## Scalar functions ✅

Computed during the scan, then passed through.

| Family | Capabilities | Example |
|---|---|---|
| Math | `FN_ABS`, `FN_ACOS`, `FN_ASIN`, `FN_ATAN`, `FN_ATAN2`, `FN_CEIL`, `FN_COS`, `FN_COSH`, `FN_COT`, `FN_DEGREES`, `FN_EXP`, `FN_FLOOR`, `FN_LN`, `FN_LOG`, `FN_MOD`, `FN_POWER`, `FN_RADIANS`, `FN_ROUND`, `FN_SIGN`, `FN_SIN`, `FN_SINH`, `FN_SQRT`, `FN_TAN`, `FN_TANH`, `FN_TRUNC` | `SELECT ROUND(amt, 2)` |
| String | `FN_ASCII`, `FN_CHR`, `FN_CONCAT`, `FN_INITCAP`, `FN_INSTR`, `FN_LEFT`, `FN_LENGTH`, `FN_LOCATE`, `FN_LOWER`, `FN_LPAD`, `FN_LTRIM`, `FN_OCTET_LENGTH`, `FN_REPEAT`, `FN_REPLACE`, `FN_REVERSE`, `FN_RIGHT`, `FN_RPAD`, `FN_RTRIM`, `FN_SUBSTR`, `FN_TRANSLATE`, `FN_TRIM`, `FN_UNICODE`, `FN_UNICODECHR`, `FN_UPPER` | `WHERE UPPER(code) = 'AB'` |
| Date / time | `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_DATE_TRUNC`, `FN_DAY`, `FN_EXTRACT`, `FN_HOUR`, `FN_MINUTE`, `FN_MONTH`, `FN_SECOND`, `FN_SYSDATE`, `FN_SYSTIMESTAMP`, `FN_TO_DATE`, `FN_TO_TIMESTAMP`, `FN_YEAR` | `WHERE YEAR(ts) = 2024` |
| Conditional | `FN_CASE`, `FN_GREATEST`, `FN_LEAST`, `FN_NULLIFZERO`, `FN_ZEROIFNULL` | `SELECT CASE WHEN x > 0 THEN 'p' END` |

## Aggregation ✅

Partial aggregate per shard → merged by Exasol.

| Capability group | Capabilities | Example |
|---|---|---|
| Single-group | `AGGREGATE_SINGLE_GROUP`, `FN_AGG_COUNT`, `FN_AGG_COUNT_STAR`, `FN_AGG_SUM`, `FN_AGG_MIN`, `FN_AGG_MAX` | `SELECT SUM(amt) FROM t` |
| Average | `FN_AGG_AVG` | `SELECT AVG(amt)` |
| Statistical | `FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, `FN_AGG_VAR_SAMP` | `SELECT STDDEV(x)` |
| Group by | `AGGREGATE_GROUP_BY_COLUMN`, `AGGREGATE_GROUP_BY_EXPRESSION` | `SELECT k, SUM(v) GROUP BY k` |
| Having | `AGGREGATE_HAVING` | `... HAVING SUM(v) > 100` |

`COUNT` emits partial sum/count and `AVG` emits sum + count (not an average); statistical aggregates emit sufficient stats (n, Σx, Σx²). Exasol combines them into the final result.

## Not pushed down ⛔

Exasol handles these after partial results return — correct, just less pushdown.

| Capability | Example | Where it runs |
|---|---|---|
| JOIN (`JOIN_TYPE_*`, `JOIN_CONDITION_*`) | `FROM a JOIN b ON a.id = b.id` | Usable via multi-table VS: Exasol pushes down each table, then joins the result sets |
| `ORDER BY` | `... ORDER BY ts` | Exasol sorts the final result |
| `COUNT(DISTINCT)`, `MEDIAN`, `APPROX_COUNT_DISTINCT` | `COUNT(DISTINCT u)` | Not decomposable into partial/merge — Exasol computes on returned rows |
| `LISTAGG` / `GROUP_CONCAT` | `LISTAGG(name)` | Exasol-side |
| `AGGREGATE_GROUP_BY_TUPLE`, geospatial, session fns | — | Exasol-side / unsupported |
