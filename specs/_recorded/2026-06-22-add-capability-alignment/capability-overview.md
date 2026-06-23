# Capability Overview: Exasol VS ⇔ DataFusion 54

Maps every Exasol Virtual Schema capability (from
[`virtual-schema-common-java/doc/development/api/capabilities_list.md`](https://github.com/exasol/virtual-schema-common-java/blob/main/doc/development/api/capabilities_list.md))
to the DataFusion 54 construct that backs it, with the status this plan
(`add-capability-alignment`) assigns it.

**The invariant:** a capability is advertised only if it round-trips — the
`vs-expression` translator emits a correct DataFusion fragment, or the aggregate
planner emits a correct shard-associative partial/merge plan.

**Status legend:** `KEEP` already advertised + backed · `ADD` newly advertised +
translated by this plan · `REMOVE` advertised but not a real Exasol capability ·
`SKIP` not advertised (DataFusion can't back it correctly, or out of scope).

---

## Main capabilities

| Exasol VS capability | DataFusion target | Status | Notes |
|----------------------|-------------------|--------|-------|
| `SELECTLIST_PROJECTION` | column projection | KEEP | |
| `SELECTLIST_EXPRESSIONS` | scalar expr in SELECT list | **ADD** | rendered via translator; falls back to bare-column on untranslatable |
| `FILTER_EXPRESSIONS` | `WHERE` | KEEP | |
| `LIMIT` | `LIMIT` | KEEP | retained at Exasol level as correctness backstop |
| `LIMIT_WITH_OFFSET` | `LIMIT ... OFFSET` | SKIP | not in scope |
| `AGGREGATE_SINGLE_GROUP` | whole-table aggregate | KEEP | partial/merge decomposition |
| `AGGREGATE_GROUP_BY_COLUMN` | `GROUP BY col` | KEEP | |
| `AGGREGATE_GROUP_BY_EXPRESSION` | `GROUP BY expr` | KEEP | |
| `AGGREGATE_GROUP_BY_TUPLE` | `GROUP BY (a,b)` tuple | SKIP | not advertised |
| `AGGREGATE_HAVING` | `HAVING` | **ADD** | applied in the OUTER merge wrapper only ([ADR-5]) |
| `ORDER_BY_COLUMN` / `ORDER_BY_EXPRESSION` | `ORDER BY` | SKIP | distributed shard-merge ordering semantics unclear |
| `JOIN*` (all) | joins | SKIP | multi-table out of scope |

## Literal capabilities

| Exasol VS capability | DataFusion target | Status | Notes |
|----------------------|-------------------|--------|-------|
| `LITERAL_BOOL` | `true`/`false` | KEEP | |
| `LITERAL_DATE` | `DATE '…'` | KEEP | |
| `LITERAL_DOUBLE` | bare numeric | KEEP | |
| `LITERAL_EXACTNUMERIC` | bare numeric | KEEP | |
| `LITERAL_NULL` | `NULL` | KEEP | |
| `LITERAL_STRING` | quoted string | KEEP | single-quotes escaped by doubling |
| `LITERAL_TIMESTAMP` | `TIMESTAMP '…'` | KEEP | |
| `LITERAL_TIMESTAMP_UTC` | timestamp-with-tz UTC | **ADD** | |
| `LITERAL_INTERVAL` | interval | SKIP | DataFusion interval support is partial |

## Predicate capabilities

| Exasol VS capability | DataFusion target | Status | Notes |
|----------------------|-------------------|--------|-------|
| `FN_PRED_AND` | `AND` | KEEP | |
| `FN_PRED_OR` | `OR` | KEEP | |
| `FN_PRED_NOT` | `NOT` | KEEP | |
| `FN_PRED_EQUAL` | `=` | KEEP | |
| `FN_PRED_NOTEQUAL` | `<>` | KEEP | |
| `FN_PRED_LESS` | `<` | KEEP | |
| `FN_PRED_LESSEQUAL` | `<=` | KEEP | |
| `FN_PRED_GREATER` | — | **REMOVE** | not an Exasol capability; engine normalises `a > b` → `b < a` ([ADR-2]). Translator arm kept as defensive no-op |
| `FN_PRED_GREATEREQUAL` | — | **REMOVE** | as above |
| `FN_PRED_BETWEEN` | `BETWEEN` | KEEP | |
| `FN_PRED_IN_CONSTLIST` | `IN (…)` | KEEP | empty list → `FALSE` |
| `FN_PRED_IS_NULL` | `IS NULL` | KEEP | |
| `FN_PRED_IS_NOT_NULL` | `IS NOT NULL` | KEEP | |
| `FN_PRED_LIKE` | `LIKE` | KEEP | |
| `FN_PRED_LIKE_ESCAPE` | `LIKE … ESCAPE` | **ADD** | already translated, was just unadvertised |
| `FN_PRED_REGEXP_LIKE` | `regexp_like(expr, pat)` | **ADD** | |
| `FN_PRED_IS_JSON` / `FN_PRED_IS_NOT_JSON` | — | SKIP | no DataFusion equivalent |

## Scalar functions — math (all **ADD**)

| Exasol VS capability | DataFusion fn | Exasol VS capability | DataFusion fn |
|----|----|----|----|
| `FN_ABS` | `abs` | `FN_LOG` | `log` |
| `FN_ACOS` | `acos` | `FN_MOD` | `%` operator ([ADR-7]) |
| `FN_ASIN` | `asin` | `FN_POWER` | `power` |
| `FN_ATAN` | `atan` | `FN_RADIANS` | `radians` |
| `FN_ATAN2` | `atan2` | `FN_ROUND` | `round` |
| `FN_CEIL` | `ceil` | `FN_SIGN` | `signum` |
| `FN_COS` | `cos` | `FN_SIN` | `sin` |
| `FN_COSH` | `cosh` | `FN_SINH` | `sinh` |
| `FN_COT` | `cot` | `FN_SQRT` | `sqrt` |
| `FN_DEGREES` | `degrees` | `FN_TAN` | `tan` |
| `FN_EXP` | `exp` | `FN_TANH` | `tanh` |
| `FN_FLOOR` | `floor` | `FN_TRUNC` | `trunc` |
| `FN_LN` | `ln` | | |

## Scalar functions — string (all **ADD**)

| Exasol VS capability | DataFusion fn | Exasol VS capability | DataFusion fn |
|----|----|----|----|
| `FN_ASCII` | `ascii` | `FN_REPLACE` | `replace` |
| `FN_CHR` | `chr` | `FN_REVERSE` | `reverse` |
| `FN_CONCAT` | `concat` | `FN_RIGHT` | `right` |
| `FN_INITCAP` | `initcap` | `FN_LEFT` | `left` |
| `FN_INSTR` | `strpos` (operand reorder) | `FN_RPAD` | `rpad` |
| `FN_LENGTH` | `character_length` | `FN_RTRIM` | `rtrim` |
| `FN_LOCATE` | `strpos` (operand reorder) | `FN_SUBSTR` | `substr` |
| `FN_LOWER` | `lower` | `FN_TRANSLATE` | `translate` |
| `FN_LPAD` | `lpad` | `FN_TRIM` | `trim` |
| `FN_LTRIM` | `ltrim` | `FN_UNICODE` | `ascii` |
| `FN_OCTET_LENGTH` | `octet_length` | `FN_UNICODECHR` | `chr` |
| `FN_REPEAT` | `repeat` | `FN_UPPER` | `upper` |

## Scalar functions — conditional / arithmetic

| Exasol VS capability | DataFusion target | Status | Notes |
|----------------------|-------------------|--------|-------|
| `FN_ADD` / `FN_SUB` / `FN_MULT` / `FN_FLOAT_DIV` / `FN_NEG` | `+ - * / -x` | KEEP | arithmetic, already translated |
| `FN_CAST` | `CAST(x AS T)` | KEEP | already translated |
| `FN_CASE` | `CASE WHEN … THEN … [ELSE …] END` | **ADD** | |
| `FN_GREATEST` | `greatest(…)` | **ADD** | |
| `FN_LEAST` | `least(…)` | **ADD** | |
| `FN_NULLIFZERO` | `nullif(x, 0)` | **ADD** | |
| `FN_ZEROIFNULL` | `coalesce(x, 0)` | **ADD** | |

## Scalar functions — date/time (new feature `vs-expression-translator-date-fns`)

| Exasol VS capability | DataFusion target | Status | Notes |
|----------------------|-------------------|--------|-------|
| `FN_EXTRACT` | `EXTRACT(field FROM src)` | **ADD** | |
| `FN_YEAR` / `FN_MONTH` / `FN_DAY` / `FN_HOUR` / `FN_MINUTE` / `FN_SECOND` | `EXTRACT(field FROM …)` | **ADD** | field-shortcut arms |
| `FN_DATE_TRUNC` | `date_trunc(unit, src)` | **ADD** | |
| `FN_CURRENT_DATE` / `FN_SYSDATE` | `current_date()` | **ADD** | |
| `FN_CURRENT_TIMESTAMP` / `FN_SYSTIMESTAMP` | `now()` | **ADD** | |
| `FN_TO_DATE` | `to_date(…)` | **ADD** | optional format arg |
| `FN_TO_TIMESTAMP` | `to_timestamp(…)` | **ADD** | optional format arg |
| `FN_ADD_DAYS/HOURS/…`, `FN_*_BETWEEN`, `FN_CONVERT_TZ`, `FN_POSIX_TIME`, `FN_DBTIMEZONE` | — | SKIP | fall through as unsupported (error/None) |

## Aggregate functions

| Exasol VS capability | DataFusion target | Status | Notes |
|----------------------|-------------------|--------|-------|
| `FN_AGG_COUNT` / `FN_AGG_COUNT_STAR` | `count` | KEEP | merged via `SUM` of partial counts |
| `FN_AGG_SUM` | `sum` | KEEP | `SUM` of partial sums |
| `FN_AGG_MIN` / `FN_AGG_MAX` | `min` / `max` | KEEP | extrema of partial extrema |
| `FN_AGG_AVG` | `avg` | KEEP | pushed as partial `(sum, count)` pair, divided in wrapper |
| `FN_AGG_STDDEV` / `FN_AGG_STDDEV_POP` / `FN_AGG_STDDEV_SAMP` | `stddev*` | **ADD** | via `(count, sum, sum_sq)` sufficient statistics ([ADR-3]) |
| `FN_AGG_VARIANCE` / `FN_AGG_VAR_POP` / `FN_AGG_VAR_SAMP` | `var*` | **ADD** | as above; pop divisor `n`, sample divisor `n-1` |
| `FN_AGG_MEDIAN` | `median` | SKIP | not shard-associative → row-scan fallback ([ADR-4]) |
| `FN_AGG_APPROXIMATE_COUNT_DISTINCT` | `approx_distinct` | SKIP | as above |
| `FN_AGG_*_DISTINCT` (all) | — | SKIP | distinct not mergeable across shards |
| `FN_AGG_LISTAGG` / `FN_AGG_GROUP_CONCAT` | — | SKIP | order-sensitive, not shard-associative |
| `FN_AGG_FIRST_VALUE` / `FN_AGG_LAST_VALUE` / `FN_AGG_EVERY` / `FN_AGG_SOME` / `FN_AGG_MUL` / `FN_AGG_ST_*` | — | SKIP | out of scope |

---

## Summary

| Status | Count (capability groups) |
|--------|---------------------------|
| **REMOVE** | 2 predicates (`FN_PRED_GREATER`, `FN_PRED_GREATEREQUAL`) |
| **ADD** | `FN_PRED_LIKE_ESCAPE`, `FN_PRED_REGEXP_LIKE`, `LITERAL_TIMESTAMP_UTC`, `SELECTLIST_EXPRESSIONS`, `AGGREGATE_HAVING`, ~25 math, ~24 string, 5 conditional, ~12 date/time scalar fns, 6 statistical aggregates |
| **KEEP** | projection, filter, LIMIT, all current predicates/literals, single-group + GROUP BY aggregates, COUNT/SUM/MIN/MAX/AVG |
| **SKIP** | ORDER_BY, JOIN, geospatial, IS_JSON, `LITERAL_INTERVAL`, `LIMIT_WITH_OFFSET`, MEDIAN, `*_DISTINCT`, LISTAGG/GROUP_CONCAT, session functions, non-associative date fns |

ADR references → [`decision-log.md`](decision-log.md). Full task list → [`plan.md`](plan.md).
