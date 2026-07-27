[lakehouse-engine](../README.md) › [Docs](index.md) › Capabilities

---

# Capability Support Overview

Mental model: **DataFusion does per-shard work inside the UDF; Exasol coordinates across
shards and handles anything not pushed down.** These capabilities are identical for every table,
whatever [catalog backend](catalogs.md) it lives in. A capability is advertised only if the VS can
translate it (via the vs-expression translator) or decompose it into a correct partial/merge plan.
Source of truth: `crates/lakehouse-engine/src/adapter/capabilities.rs`. For *how* the per-shard and
parent-level split works, see [Architecture](architecture.md); the [docs index](index.md) lists the
full guide set.

## Projection & expressions ✅

Reads only projected columns; select-list expressions are evaluated during the scan, then passed through.

| Capability | Example |
|---|---|
| `SELECTLIST_PROJECTION` | `SELECT id, name` |
| `SELECTLIST_EXPRESSIONS` | `SELECT price * 1.2` |

## Filtering ✅

Translated predicates prune whole data files via Iceberg manifest stats, then skip row-groups/rows during the scan; Exasol re-checks only what it couldn't translate. A bare `LIMIT` (no `ORDER BY`) stops the scan early per shard and is re-applied as a cross-shard backstop.

| Capability group | Capabilities | Example |
|---|---|---|
| Filter | `FILTER_EXPRESSIONS` | `WHERE region = 'EU'` |
| Logical | `FN_PRED_AND`, `FN_PRED_OR`, `FN_PRED_NOT` | `WHERE a AND NOT b` |
| Comparison | `FN_PRED_EQUAL`, `FN_PRED_NOTEQUAL`, `FN_PRED_LESS`, `FN_PRED_LESSEQUAL`, `FN_PRED_BETWEEN`, `FN_PRED_IN_CONSTLIST`, `FN_PRED_IS_NULL`, `FN_PRED_IS_NOT_NULL`, `FN_PRED_LIKE`, `FN_PRED_LIKE_ESCAPE`, `FN_PRED_REGEXP_LIKE` | `WHERE qty BETWEEN 1 AND 10` |
| Literals | `LITERAL_BOOL`, `LITERAL_DATE`, `LITERAL_DOUBLE`, `LITERAL_EXACTNUMERIC`, `LITERAL_NULL`, `LITERAL_STRING`, `LITERAL_TIMESTAMP`, `LITERAL_TIMESTAMP_UTC` | `WHERE d = DATE '2024-01-01'` |
| Limit | `LIMIT` | `... LIMIT 100` |
| Ordered top-N | `ORDER_BY_COLUMN` | `... ORDER BY price DESC LIMIT 20` |

`FN_PRED_GREATER` / `FN_PRED_GREATEREQUAL` are not Exasol capability names — Exasol normalises `a > b` to `b < a` before it reaches the adapter.

`ORDER BY ... LIMIT n` over a single table (no join, no `GROUP BY`) with every sort key a bare
projected column pushes down as a per-shard bounded top-N (a DataFusion `TopK`, not a full sort):
each shard emits only its own local top-`n` rows, and Exasol merges the `shard_count × n` rows with
a final `ORDER BY ... LIMIT n`. `ORDER_BY_EXPRESSION` (sort-by-expression) and `LIMIT_WITH_OFFSET`
remain unadvertised. Any `ORDER BY` shape the adapter can't bound this way (a join, a `GROUP BY`, an
unprojected or JSON-fallback-typed sort key) still returns correct results — the adapter renders its
own explicit final `ORDER BY`/`LIMIT` around the unoptimized full scan, since Exasol no longer
re-sorts once `ORDER_BY_COLUMN` is advertised.

## Scalar functions ✅

Computed during the scan, then passed through.

| Family | Capabilities | Example |
|---|---|---|
| Arithmetic | `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV` | `SELECT price * discount` |
| Math | `FN_ABS`, `FN_ACOS`, `FN_ASIN`, `FN_ATAN`, `FN_ATAN2`, `FN_CEIL`, `FN_COS`, `FN_COSH`, `FN_COT`, `FN_DEGREES`, `FN_EXP`, `FN_FLOOR`, `FN_LN`, `FN_LOG`, `FN_MOD`, `FN_POWER`, `FN_RADIANS`, `FN_ROUND`, `FN_SIGN`, `FN_SIN`, `FN_SINH`, `FN_SQRT`, `FN_TAN`, `FN_TANH`, `FN_TRUNC` | `SELECT ROUND(amt, 2)` |
| String | `FN_ASCII`, `FN_CHR`, `FN_CONCAT`, `FN_INITCAP`, `FN_INSTR`, `FN_LEFT`, `FN_LENGTH`, `FN_LOCATE`, `FN_LOWER`, `FN_LPAD`, `FN_LTRIM`, `FN_OCTET_LENGTH`, `FN_REPEAT`, `FN_REPLACE`, `FN_REVERSE`, `FN_RIGHT`, `FN_RPAD`, `FN_RTRIM`, `FN_SUBSTR`, `FN_TRANSLATE`, `FN_TRIM`, `FN_UNICODE`, `FN_UNICODECHR`, `FN_UPPER` | `WHERE UPPER(code) = 'AB'` |
| Date / time | `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_DATE_TRUNC`, `FN_DAY`, `FN_EXTRACT`, `FN_HOUR`, `FN_MINUTE`, `FN_MONTH`, `FN_SECOND`, `FN_SYSDATE`, `FN_SYSTIMESTAMP`, `FN_TO_DATE`, `FN_TO_TIMESTAMP`, `FN_YEAR` | `WHERE YEAR(ts) = 2024` |
| Conditional | `FN_CASE`, `FN_GREATEST`, `FN_LEAST`, `FN_NULLIFZERO`, `FN_ZEROIFNULL` | `SELECT CASE WHEN x > 0 THEN 'p' END` |

## Aggregation ✅

Partial aggregate per shard → merged by Exasol.

| Capability group | Capabilities | Example |
|---|---|---|
| Single-group | `AGGREGATE_SINGLE_GROUP`, `FN_AGG_COUNT`, `FN_AGG_COUNT_STAR`, `FN_AGG_SUM`, `FN_AGG_MIN`, `FN_AGG_MAX`, `FN_AGG_COUNT_DISTINCT` | `SELECT SUM(LENGTH(c)), COUNT(DISTINCT u) FROM t` |
| Average | `FN_AGG_AVG` | `SELECT AVG(amt)` |
| Statistical | `FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, `FN_AGG_VAR_SAMP` | `SELECT STDDEV(x)` |
| Group by | `AGGREGATE_GROUP_BY_COLUMN`, `AGGREGATE_GROUP_BY_EXPRESSION`, `AGGREGATE_GROUP_BY_TUPLE` | `SELECT k1, k2, SUM(v) GROUP BY k1, k2` |
| Having | `AGGREGATE_HAVING` | `... HAVING SUM(v) > 100` |

Aggregate arguments may be a column or a scalar expression (e.g. `SUM(LENGTH(c))`, or a two-column
binary arithmetic expression like `SUM(price * discount)`). `COUNT` emits partial sum/count and
`AVG` emits sum + count (not an average); statistical aggregates emit sufficient stats (n, Σx, Σx²);
single-group `COUNT(DISTINCT col|expr)` emits a per-shard local distinct set (JSON), merged by a
scalar UDF. Exasol combines them into the final result.

## Handled by Exasol 🤝

Not pushed to the scan — Exasol computes these on the returned partial results. Correct and fast; just not decomposable into a partial/merge plan.

| Capability | Example | Where it runs |
|---|---|---|
| JOIN (`JOIN_TYPE_*`, `JOIN_CONDITION_*`) | `FROM a JOIN b ON a.id = b.id` | Usable via multi-table VS: Exasol pushes down each table, then joins the result sets |
| `ORDER BY` over a join, `GROUP BY`, or an unprojected/JSON-fallback sort key | `SELECT a.x FROM a JOIN b ... ORDER BY a.x` | Not eligible for the ordered top-N pushdown above; the adapter still renders a correct final `ORDER BY`/`LIMIT` itself |
| Grouped `COUNT(DISTINCT)`, `MEDIAN`, `APPROX_COUNT_DISTINCT` | `SELECT k, COUNT(DISTINCT u) FROM t GROUP BY k` | Not decomposable into partial/merge — Exasol computes on returned rows |
| `LISTAGG` / `GROUP_CONCAT` | `LISTAGG(name)` | Exasol-side |
| Geospatial, session functions | — | Exasol-side / unsupported |
