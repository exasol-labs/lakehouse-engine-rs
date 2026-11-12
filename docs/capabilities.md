[lakehouse-engine](../README.md) › [Docs](index.md) › Capabilities

---

# Capability Support Overview

Mental model: **DataFusion does the per-shard work inside the UDF. Exasol coordinates across shards and computes everything that is not pushed down.** An advertised capability is not always evaluated by DataFusion: depending on the query shape, the adapter either pushes it into the scan or renders it in Exasol SQL inside the pushdown query it returns, which Exasol evaluates over the scan output. These capabilities are the same for every table, in every [catalog backend](catalogs.md). The adapter advertises a capability only when it can translate that capability or decompose it into a correct partial/merge plan. The [Architecture](architecture.md) page describes the split between per-shard and parent-level work. The [Capability matrix](capability-matrix.md) lists every Exasol capability, including the ones the adapter does not advertise. The [docs index](index.md) lists the full guide set.

## Projection & expressions

The scan reads only the projected columns. The scan also evaluates select-list expressions when DataFusion can render all of them. Otherwise Exasol SQL evaluates them over the scanned columns.

| Capability | Example |
|---|---|
| `SELECTLIST_PROJECTION` | `SELECT id, name` |
| `SELECTLIST_EXPRESSIONS` | `SELECT price * 1.2` |

## Filtering

Translated predicates prune whole data files with Iceberg manifest stats. The predicates then skip row-groups and rows during the scan. Exasol applies the predicates whose capability the adapter does not advertise, such as `REGEXP_LIKE`. When DataFusion cannot render every conjunct of the pushed filter, the whole filter runs in Exasol SQL and the scan returns the referenced columns unfiltered. A bare `LIMIT` (no `ORDER BY`) stops the scan early in each shard. Exasol re-applies that `LIMIT` as a cross-shard backstop.

| Capability group | Capabilities | Example |
|---|---|---|
| Filter | `FILTER_EXPRESSIONS` | `WHERE region = 'EU'` |
| Logical | `FN_PRED_AND`, `FN_PRED_OR`, `FN_PRED_NOT` | `WHERE a AND NOT b` |
| Comparison | `FN_PRED_EQUAL`, `FN_PRED_NOTEQUAL`, `FN_PRED_LESS`, `FN_PRED_LESSEQUAL`, `FN_PRED_BETWEEN`, `FN_PRED_IN_CONSTLIST`, `FN_PRED_IS_NULL`, `FN_PRED_IS_NOT_NULL`, `FN_PRED_LIKE`, `FN_PRED_LIKE_ESCAPE` | `WHERE qty BETWEEN 1 AND 10` |
| Literals | `LITERAL_BOOL`, `LITERAL_DATE`, `LITERAL_DOUBLE`, `LITERAL_EXACTNUMERIC`, `LITERAL_NULL`, `LITERAL_STRING`, `LITERAL_TIMESTAMP`, `LITERAL_TIMESTAMP_UTC` | `WHERE d = DATE '2024-01-01'` |
| Limit | `LIMIT` | `... LIMIT 100` |
| Limit with offset | `LIMIT_WITH_OFFSET` | `... LIMIT 20 OFFSET 40` |
| Ordered top-N | `ORDER_BY_COLUMN` | `... ORDER BY price DESC LIMIT 20` |
| Ordered by expression | `ORDER_BY_EXPRESSION` | `... ORDER BY price * discount DESC` |

`FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` are not Exasol capability names. Exasol normalizes `a > b` to `b < a` before the predicate reaches the adapter.

`ORDER BY ... LIMIT n` pushes down as a per-shard bounded top-N. This pushdown needs a single table
or a broadcast join, no aggregation, and every sort key must be a bare projected column that is not
JSON-fallback typed. DataFusion runs a `TopK`, not a full sort. Each shard emits only its own local
top-`n` rows. Exasol SQL then merges the `shard_count × n` rows with a final `ORDER BY ... LIMIT n`.
The per-shard bounded top-N never carries an offset. A non-zero offset always
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

Exasol does not re-sort once the adapter advertises `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION`,
so the adapter renders every sort key Exasol pushes in Exasol SQL.

The adapter also cannot bound these `ORDER BY` shapes as a per-shard top-N:

- a join that is not broadcast
- a `GROUP BY` or an aggregate
- an unprojected sort key
- a JSON-fallback-typed sort key

For these shapes the adapter renders the final `ORDER BY` and `LIMIT` in Exasol SQL over the
full scan.

## Scalar functions

The scan computes these functions when DataFusion can render the whole filter or select list that contains them. Otherwise the adapter renders them in Exasol SQL, and Exasol computes them over the scanned columns. For example, `TO_DATE` with a format outside the supported subset runs in Exasol SQL. A few of these functions return a different result than native Exasol for some inputs. [Semantic differences](semantic-differences.md) lists them.

| Family | Capabilities | Example |
|---|---|---|
| Arithmetic | `FN_ADD`, `FN_SUB`, `FN_MULT`, `FN_FLOAT_DIV`, `FN_DIV`, `FN_NEG` | `SELECT price * discount` |
| Math | `FN_ABS`, `FN_ACOS`, `FN_ASIN`, `FN_ATAN`, `FN_ATAN2`, `FN_CEIL`, `FN_COS`, `FN_COSH`, `FN_COT`, `FN_DEGREES`, `FN_EXP`, `FN_FLOOR`, `FN_LN`, `FN_LOG`, `FN_MOD`, `FN_POWER`, `FN_RADIANS`, `FN_ROUND`, `FN_SIGN`, `FN_SIN`, `FN_SINH`, `FN_SQRT`, `FN_TAN`, `FN_TANH`, `FN_TRUNC` | `SELECT ROUND(amt, 2)` |
| String | `FN_ASCII`, `FN_CHR`, `FN_CONCAT`, `FN_INITCAP`, `FN_INSTR`, `FN_LEFT`, `FN_LENGTH`, `FN_LOCATE`, `FN_LOWER`, `FN_LPAD`, `FN_LTRIM`, `FN_OCTET_LENGTH`, `FN_REPEAT`, `FN_REPLACE`, `FN_REVERSE`, `FN_RIGHT`, `FN_RPAD`, `FN_RTRIM`, `FN_SUBSTR`, `FN_TRANSLATE`, `FN_TRIM`, `FN_UNICODE`, `FN_UNICODECHR`, `FN_UPPER` | `WHERE UPPER(code) = 'AB'` |
| Conversion | `FN_CAST` | `SELECT CAST(id AS VARCHAR(20))` |
| Date / time | `FN_ADD_DAYS`, `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_WEEKS`, `FN_DATE_TRUNC`, `FN_DAY`, `FN_DAYS_BETWEEN`, `FN_EXTRACT`, `FN_HOUR`, `FN_HOURS_BETWEEN`, `FN_MINUTE`, `FN_MINUTES_BETWEEN`, `FN_MONTH`, `FN_SECOND`, `FN_SECONDS_BETWEEN`, `FN_TO_DATE`, `FN_TO_TIMESTAMP`, `FN_WEEK`, `FN_YEAR` | `WHERE YEAR(ts) = 2024` |
| Conditional | `FN_CASE`, `FN_GREATEST`, `FN_LEAST`, `FN_NULLIFZERO`, `FN_ZEROIFNULL` | `SELECT CASE WHEN x > 0 THEN 'p' END` |
| JSON | `FN_JSON_VALUE` | `WHERE JSON_VALUE(doc, '$.locale') = 'de_DE'` |

`ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, and `ADD_MINUTES` run in the scan over a `DATE` or a `TIMESTAMP(0)` to `TIMESTAMP(6)` argument and a numeric count. Any other argument form (`TIMESTAMP(7)` to `TIMESTAMP(9)`, `TIMESTAMP WITH LOCAL TIME ZONE`, text, a `CASE` over two date values, another date function, a text count, or a numeric literal count above 1e10 in magnitude) runs in Exasol SQL.

`INSTR` with a start position or an occurrence, and `LOCATE` with a start position, run in Exasol SQL. With two arguments they run in the scan.

`JSON_VALUE` runs in the scan when all of these hold:

- the document is text;
- the path is `$` followed by `.name`, `."name"`, and `[n]` steps, with no whitespace;
- there is no `RETURNING` clause, or it names `VARCHAR(n)` in UTF8 or `BOOLEAN`;
- `ON EMPTY` and `ON ERROR` are `NULL`, or `DEFAULT` with a literal of the returning type that fits it.

Any other form runs in Exasol SQL over the document column, among them `RETURNING DECIMAL`, `DOUBLE`, `DATE`, and `TIMESTAMP`, which convert by the session's NLS settings, and `ERROR ON EMPTY` or `ERROR ON ERROR`, which raise Exasol's own error.

The scan reads a document nested at most about 200 levels deep. A deeper document takes the `ON ERROR` behavior.

### Text and session settings

Exasol writes a DATE, TIMESTAMP, DOUBLE, or DECIMAL with a scale as text, and reads text as one of these types, by the session's NLS settings. The pushdown request does not carry them, so these conversions run in Exasol SQL:

- a CAST between text and one of these types, and `TO_DATE` or `TO_TIMESTAMP` without a format;
- text that meets one of these types in a comparison, `IN`, `LIKE`, `||`, a string function, `CASE`, `COALESCE`, `GREATEST`, or arithmetic;
- `SUM`, `AVG`, `STDDEV`, or `VARIANCE` over text or a BOOLEAN, which Exasol converts to a number. The aggregate runs in Exasol SQL instead of splitting into scan partials.

Integers and BOOLEAN convert to text in the scan, and an integer compared with a digits-only literal (`id = '3'`) stays in the scan unless the literal has more digits than the integer's precision. A BOOLEAN cast to DECIMAL or compared with a number runs in the scan as 1 or 0, except an `IN` list of numbers over a BOOLEAN, which runs in Exasol SQL. A CAST to `CHAR(n)` runs in Exasol SQL, because the scan does not blank-pad.

Such a condition runs in Exasol SQL. On a single table it takes the whole WHERE with it, so the scan filters no rows, and in a join it loses the broadcast plan, though an N-scan join leg keeps its other conditions. File pruning still uses the conditions it can read, but the converted one prunes nothing. Write the literal in its type, or give the format:

| Runs in Exasol SQL | Runs in the scan |
|---|---|
| `d = '2024-01-02'` | `d = DATE '2024-01-02'` |
| `ts > '2024-01-01 00:00:00'` | `ts > TIMESTAMP '2024-01-01 00:00:00'` |
| `d LIKE '2024%'` | `d >= DATE '2024-01-01' AND d < DATE '2025-01-01'` |
| `TO_DATE(s)` | `TO_DATE(s, 'YYYY-MM-DD')` |
| `MAX(CAST(d AS VARCHAR(10)))` | `CAST(MAX(d) AS VARCHAR(10))` |

## Aggregation

When DataFusion can render the aggregates and group keys, each shard computes a partial aggregate and Exasol SQL merges them. Otherwise the scan only filters and projects, and Exasol SQL aggregates the scanned rows. `HAVING` always runs in Exasol SQL, over the merged aggregates.

| Capability group | Capabilities | Example |
|---|---|---|
| Single-group | `AGGREGATE_SINGLE_GROUP`, `FN_AGG_COUNT`, `FN_AGG_COUNT_STAR`, `FN_AGG_SUM`, `FN_AGG_MIN`, `FN_AGG_MAX`, `FN_AGG_COUNT_DISTINCT` | `SELECT COUNT(*), SUM(LENGTH(c)) FROM t` |
| Average | `FN_AGG_AVG` | `SELECT AVG(amt)` |
| Statistical | `FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, `FN_AGG_VAR_SAMP` | `SELECT STDDEV(x)` |
| Group by | `AGGREGATE_GROUP_BY_COLUMN`, `AGGREGATE_GROUP_BY_EXPRESSION`, `AGGREGATE_GROUP_BY_TUPLE` | `SELECT k1, k2, SUM(v) GROUP BY k1, k2` |
| Having | `AGGREGATE_HAVING` | `... HAVING SUM(v) > 100` |

An aggregate argument can be a column or a scalar expression. Examples are `SUM(LENGTH(c))` and a
two-column binary arithmetic expression such as `SUM(price * discount)`. Statistical aggregates take
the partial path only for a bare-column argument.

A partial aggregate needs every partial column and group key in a type the request states: a column, a CAST, or the same aggregate selected on its own. Otherwise the scan only filters and projects. For example, `LENGTH(MAX(UPPER(s)))` alone runs in Exasol SQL, and with `MAX(UPPER(s))` also selected it splits. A computed group key that is not selected, such as `GROUP BY qty * 2`, needs a CAST.

Each aggregate emits partial values instead of a final value:

- `COUNT` emits a partial count.
- `AVG` emits a sum and a count, not an average.
- Statistical aggregates emit a count, a mean, and a sum of squared deviations (M2), in double
  precision.
- A lone single-group `COUNT(DISTINCT col)` over a bare column emits each shard's distinct values,
  and Exasol's own `COUNT(DISTINCT)` counts them. Any other `COUNT(DISTINCT)` runs in Exasol SQL
  over the scanned rows.

Exasol SQL combines these partial values into the final result.

## Joins

| Capability group | Capabilities | Example |
|---|---|---|
| Inner equi-join | `JOIN`, `JOIN_TYPE_INNER`, `JOIN_CONDITION_EQUI` | `FROM a JOIN b ON a.id = b.id` |

A two-table inner equi-join, or a table joined to itself, whose smaller side is at or below `JOIN_BROADCAST_MAX_BYTES` ([tuning](tuning.md)) is broadcast into every shard of the larger side, and DataFusion joins it there. The larger side's scan skips every row group whose key range lies outside the smaller side's join-key range. When the smaller side holds at most 150 distinct join keys, the scan also skips every row group that holds none of them. Broadcast also requires, among other conditions, no aggregation, sort keys that are projected columns, and a join condition and filter that DataFusion can render. Any other inner equi-join scans each table separately, and the adapter joins the scans in Exasol SQL.

## Handled by Exasol

The adapter does not advertise these capabilities, so Exasol evaluates them itself over the rows the scan returns. Results are correct, but a predicate that Exasol keeps prunes no files. A capability is unadvertised for one of three reasons: (a) it does not decompose into a partial/merge plan, (b) the scan lacks the clock, time zone, or session context it needs, or (c) DataFusion's result differs from Exasol's. Advertised capabilities that the adapter renders in Exasol SQL are described in the sections above.

| Capability | Example | Where it runs |
|---|---|---|
| Outer, non-equi, and Cartesian joins (`JOIN_TYPE_LEFT_OUTER`, `JOIN_TYPE_RIGHT_OUTER`, `JOIN_TYPE_FULL_OUTER`, `JOIN_CONDITION_ALL`) | `FROM a LEFT JOIN b ON a.id = b.id` | Usable via multi-table VS: Exasol pushes down each table separately, then joins the result sets |
| `REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, `REGEXP_COUNT` | `WHERE name REGEXP_LIKE 'event-0[1-3]'` | Exasol evaluates them over the returned rows, because DataFusion's regex matching differs from Exasol's PCRE matching. The other `AND` conjuncts and the projection still push down, and an `OR` that contains one pushes no filter |
| `MEDIAN`, `APPROXIMATE_COUNT_DISTINCT` | `SELECT k, MEDIAN(v) FROM t GROUP BY k` | Not decomposable into partial/merge. Exasol computes on the returned rows |
| `LISTAGG` / `GROUP_CONCAT` | `LISTAGG(name)` | Exasol-side |
| `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` | `WHERE created_at < CURRENT_TIMESTAMP` | The scan receives neither `SESSIONTIMEZONE` nor `DBTIMEZONE` and holds no statement anchor, so Exasol evaluates the clock itself, once per statement, in its own zones. Results are correct; a predicate over one of these names prunes no files |
| Geospatial and session functions | — | Exasol evaluates them. No lakehouse type maps to GEOMETRY |

## Excluding a capability

The property `EXCLUDED_CAPABILITIES` withdraws advertised capabilities from one virtual schema. Exasol then evaluates each excluded construct itself over the rows the scan returns, as it does for the capabilities in the section above. The property takes a comma-separated list of capability names from the tables above. Use it to stop pushing a capability that misbehaves or runs slowly for a workload, with no new adapter release.

```sql
ALTER VIRTUAL SCHEMA my_vs SET EXCLUDED_CAPABILITIES = 'FN_PRED_LIKE';
```

After this statement, a `WHERE name LIKE 'event-1%'` predicate no longer reaches the scan, and a pushed-down query returns the same rows as before. The change applies from the next query, with no `REFRESH`. `SET EXCLUDED_CAPABILITIES = null` restores the full advertised set.

The exclusion subtracts the listed names and nothing else. The adapter does not withhold a capability because another one is excluded. Excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised changes the result of `ORDER BY ... LIMIT n OFFSET m`. [Semantic differences](semantic-differences.md#order-by-with-limit-under-excluded_capabilities) describes the effect and its workaround. [Parameters](tuning.md) lists the accepted values.

`CREATE`, `REFRESH`, and `SET` reject a name the adapter does not advertise and suggest the nearest one, for example `invalid value 'FN_PRED_LIK' for 'EXCLUDED_CAPABILITIES': expected an advertised capability name (did you mean 'FN_PRED_LIKE'?)`. When an upgrade stops advertising a listed name, `REFRESH` and `SET` fail until a `SET` of `EXCLUDED_CAPABILITIES` removes that name, while queries keep working. Unset `EXCLUDED_CAPABILITIES` before you downgrade to an adapter version without the property, because that version rejects it as an unknown property on `REFRESH` and `SET`.
