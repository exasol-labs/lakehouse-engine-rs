[lakehouse-engine](../README.md) › [Docs](index.md) › Capabilities

---

# Capability Support Overview

Mental model: **DataFusion does the per-shard work inside the UDF. Exasol coordinates across shards and computes everything that is not pushed down.** These capabilities are the same for every table, in every [catalog backend](catalogs.md). The adapter advertises a capability only when it can translate that capability or decompose it into a correct partial/merge plan. The [Architecture](architecture.md) page describes the split between per-shard and parent-level work. The [docs index](index.md) lists the full guide set.

## Projection & expressions

The scan reads only the projected columns. The scan also evaluates select-list expressions and passes the results through.

| Capability | Example |
|---|---|
| `SELECTLIST_PROJECTION` | `SELECT id, name` |
| `SELECTLIST_EXPRESSIONS` | `SELECT price * 1.2` |

## Filtering

Translated predicates prune whole data files with Iceberg manifest stats. The predicates then skip row-groups and rows during the scan. Exasol re-checks only the predicates that the adapter cannot translate. A bare `LIMIT` (no `ORDER BY`) stops the scan early in each shard. Exasol re-applies that `LIMIT` as a cross-shard backstop.

| Capability group | Capabilities | Example |
|---|---|---|
| Filter | `FILTER_EXPRESSIONS` | `WHERE region = 'EU'` |
| Logical | `FN_PRED_AND`, `FN_PRED_OR`, `FN_PRED_NOT` | `WHERE a AND NOT b` |
| Comparison | `FN_PRED_EQUAL`, `FN_PRED_NOTEQUAL`, `FN_PRED_LESS`, `FN_PRED_LESSEQUAL`, `FN_PRED_BETWEEN`, `FN_PRED_IN_CONSTLIST`, `FN_PRED_IS_NULL`, `FN_PRED_IS_NOT_NULL`, `FN_PRED_LIKE`, `FN_PRED_LIKE_ESCAPE`, `FN_PRED_REGEXP_LIKE` | `WHERE qty BETWEEN 1 AND 10` |
| Literals | `LITERAL_BOOL`, `LITERAL_DATE`, `LITERAL_DOUBLE`, `LITERAL_EXACTNUMERIC`, `LITERAL_NULL`, `LITERAL_STRING`, `LITERAL_TIMESTAMP`, `LITERAL_TIMESTAMP_UTC` | `WHERE d = DATE '2024-01-01'` |
| Limit | `LIMIT` | `... LIMIT 100` |
| Limit with offset | `LIMIT_WITH_OFFSET` | `... LIMIT 20 OFFSET 40` |
| Ordered top-N | `ORDER_BY_COLUMN` | `... ORDER BY price DESC LIMIT 20` |
| Ordered by expression | `ORDER_BY_EXPRESSION` | `... ORDER BY price * discount DESC` |

`FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` are not Exasol capability names. Exasol normalizes `a > b` to `b < a` before the predicate reaches the adapter.

`ORDER BY ... LIMIT n` pushes down as a per-shard bounded top-N. This pushdown needs a single table
(no join, no `GROUP BY`), and every sort key must be a bare projected column. DataFusion runs a
`TopK`, not a full sort. Each shard emits only its own local top-`n` rows. Exasol then merges the
`shard_count × n` rows with a final `ORDER BY ... LIMIT n`. `LIMIT_WITH_OFFSET` is now advertised
(issue #191). The per-shard bounded top-N never carries an offset. A non-zero offset always
declines to the row-scan wrapper. That wrapper renders the full `ORDER BY ... LIMIT n OFFSET m`
window itself.

The adapter also advertises `ORDER_BY_EXPRESSION` (issue #198). A sort key that is an expression,
not a bare column, does not qualify for the bounded top-N above. Three paths still render the
result in the correct order:

- The single-table, no-join, no-`GROUP BY` row-scan wrapper adds the base columns of the
  expression as hidden columns, then renders the sort expression in the Exasol dialect over them.
  This path gives correctness only, as an unbounded full scan, not a per-shard top-N.
- The grouped-merge path renders an aggregate `ORDER BY` over the partial and merge columns. When
  the sort key is not an aggregate or a group key, this path routes to the plain `GROUP BY`
  wrapper instead.
- The qualified single-table, N-scan join wrapper renders any sortable expression directly,
  because it already qualifies column references.

When none of these three paths can render an `ORDER BY` shape, the adapter still returns correct
results. It renders its own explicit final `ORDER BY` and `LIMIT` around the unoptimized full
scan, because Exasol no longer re-sorts once the adapter advertises `ORDER_BY_COLUMN` and
`ORDER_BY_EXPRESSION`.

The adapter also cannot bound these `ORDER BY` shapes as a per-shard top-N:

- a join
- a `GROUP BY`
- an unprojected sort key
- a JSON-fallback-typed sort key

These shapes still return correct results the same way: the adapter generates its own explicit
final `ORDER BY` and `LIMIT` around the unoptimized full scan.

## Scalar functions

The scan computes these functions and passes the results through.

| Family | Capabilities | Example |
|---|---|---|
| Arithmetic | `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV` | `SELECT price * discount` |
| Math | `FN_ABS`, `FN_ACOS`, `FN_ASIN`, `FN_ATAN`, `FN_ATAN2`, `FN_CEIL`, `FN_COS`, `FN_COSH`, `FN_COT`, `FN_DEGREES`, `FN_EXP`, `FN_FLOOR`, `FN_LN`, `FN_LOG`, `FN_MOD`, `FN_POWER`, `FN_RADIANS`, `FN_ROUND`, `FN_SIGN`, `FN_SIN`, `FN_SINH`, `FN_SQRT`, `FN_TAN`, `FN_TANH`, `FN_TRUNC` | `SELECT ROUND(amt, 2)` |
| String | `FN_ASCII`, `FN_CHR`, `FN_CONCAT`, `FN_INITCAP`, `FN_INSTR`, `FN_LEFT`, `FN_LENGTH`, `FN_LOCATE`, `FN_LOWER`, `FN_LPAD`, `FN_LTRIM`, `FN_OCTET_LENGTH`, `FN_REPEAT`, `FN_REPLACE`, `FN_REVERSE`, `FN_RIGHT`, `FN_RPAD`, `FN_RTRIM`, `FN_SUBSTR`, `FN_TRANSLATE`, `FN_TRIM`, `FN_UNICODE`, `FN_UNICODECHR`, `FN_UPPER` | `WHERE UPPER(code) = 'AB'` |
| Date / time | `FN_DATE_TRUNC`, `FN_DAY`, `FN_EXTRACT`, `FN_HOUR`, `FN_MINUTE`, `FN_MONTH`, `FN_SECOND`, `FN_TO_DATE`, `FN_TO_TIMESTAMP`, `FN_YEAR` | `WHERE YEAR(ts) = 2024` |
| Conditional | `FN_CASE`, `FN_GREATEST`, `FN_LEAST`, `FN_NULLIFZERO`, `FN_ZEROIFNULL` | `SELECT CASE WHEN x > 0 THEN 'p' END` |

## Aggregation

Each shard computes a partial aggregate. Exasol merges the partial aggregates.

| Capability group | Capabilities | Example |
|---|---|---|
| Single-group | `AGGREGATE_SINGLE_GROUP`, `FN_AGG_COUNT`, `FN_AGG_COUNT_STAR`, `FN_AGG_SUM`, `FN_AGG_MIN`, `FN_AGG_MAX`, `FN_AGG_COUNT_DISTINCT` | `SELECT SUM(LENGTH(c)), COUNT(DISTINCT u) FROM t` |
| Average | `FN_AGG_AVG` | `SELECT AVG(amt)` |
| Statistical | `FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, `FN_AGG_VAR_SAMP` | `SELECT STDDEV(x)` |
| Group by | `AGGREGATE_GROUP_BY_COLUMN`, `AGGREGATE_GROUP_BY_EXPRESSION`, `AGGREGATE_GROUP_BY_TUPLE` | `SELECT k1, k2, SUM(v) GROUP BY k1, k2` |
| Having | `AGGREGATE_HAVING` | `... HAVING SUM(v) > 100` |

An aggregate argument can be a column or a scalar expression. Examples are `SUM(LENGTH(c))` and a
two-column binary arithmetic expression such as `SUM(price * discount)`.

Each aggregate emits partial values instead of a final value:

- `COUNT` emits a partial sum/count.
- `AVG` emits a sum and a count, not an average.
- Statistical aggregates emit sufficient stats (n, Σx, Σx²).
- Single-group `COUNT(DISTINCT col|expr)` emits a per-shard local distinct set as JSON. A scalar UDF
  merges these sets.

Exasol combines these partial values into the final result.

## Handled by Exasol

The adapter does not push these capabilities to the scan. Exasol computes them on the returned partial results. The results are correct and fast. A capability lives in this section for one of two reasons: (a) it is not decomposable into a partial/merge plan, or (b) the scan cannot evaluate it at all, because the scan holds no clock, time zone, or statement context.

| Capability | Example | Where it runs |
|---|---|---|
| JOIN (`JOIN_TYPE_*`, `JOIN_CONDITION_*`) | `FROM a JOIN b ON a.id = b.id` | Usable via multi-table VS: Exasol pushes down each table, then joins the result sets |
| `ORDER BY` over a join, `GROUP BY`, or an unprojected/JSON-fallback sort key | `SELECT a.x FROM a JOIN b ... ORDER BY a.x` | Not eligible for the ordered top-N pushdown. The adapter generates a correct final `ORDER BY`/`LIMIT` itself |
| Grouped `COUNT(DISTINCT)`, `MEDIAN`, `APPROX_COUNT_DISTINCT` | `SELECT k, COUNT(DISTINCT u) FROM t GROUP BY k` | Not decomposable into partial/merge. Exasol computes on the returned rows |
| `LISTAGG` / `GROUP_CONCAT` | `LISTAGG(name)` | Exasol-side |
| `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` | `WHERE created_at < CURRENT_TIMESTAMP` | The scan receives neither `SESSIONTIMEZONE` nor `DBTIMEZONE` and holds no statement anchor, so Exasol evaluates the clock itself, once per statement, in its own zones. Results are correct; a predicate over one of these names prunes no files |
| Geospatial, session functions | — | Exasol-side / unsupported |
