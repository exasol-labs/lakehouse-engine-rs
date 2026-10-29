[lakehouse-engine](../README.md) › [Docs](index.md) › Capability matrix

---

# Capability matrix

This page lists every Exasol Virtual Schema capability and whether the adapter advertises it.
Exasol evaluates an unadvertised capability itself, over the rows the scan returns.

Exasol delegates an advertised capability to the adapter and never re-checks it. The adapter
then either pushes it into the DataFusion scan, which evaluates it per shard, or renders it in
Exasol SQL inside the pushdown query it returns, which Exasol evaluates over the scan output.
The query shape decides which: a filter that DataFusion cannot fully evaluate stays in Exasol
SQL, and an aggregate runs as DataFusion partials that Exasol SQL merges. A ✓ therefore says
that the adapter receives the capability, not that DataFusion runs it.
[Capabilities](capabilities.md) explains how each pushed-down capability is executed.

The capability names come from the Exasol
[capabilities list](https://github.com/exasol/virtual-schema-common-java/blob/main/doc/development/api/capabilities_list.md).
The ✓ marks mirror `CAPABILITIES` in `crates/lakehouse-engine/src/adapter/capabilities.rs`.

## Main capabilities

| Capability | Advertised | Notes |
|---|:---:|---|
| `AGGREGATE_GROUP_BY_COLUMN` | ✓ | DataFusion computes partials per shard. Exasol SQL merges them. |
| `AGGREGATE_GROUP_BY_EXPRESSION` | ✓ | DataFusion computes partials per shard. Exasol SQL merges them. A key DataFusion cannot render moves the aggregation to Exasol SQL. |
| `AGGREGATE_GROUP_BY_TUPLE` | ✓ | DataFusion computes partials per shard. Exasol SQL merges them. |
| `AGGREGATE_HAVING` | ✓ | Exasol SQL, over the merged aggregates. |
| `AGGREGATE_SINGLE_GROUP` | ✓ | DataFusion computes partials per shard. Exasol SQL merges them. |
| `FILTER_EXPRESSIONS` | ✓ | DataFusion evaluates the filter when it can render every conjunct. Otherwise the whole filter runs in Exasol SQL. |
| `JOIN` | ✓ |  |
| `JOIN_CONDITION_ALL` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `JOIN_CONDITION_EQUI` | ✓ |  |
| `JOIN_TYPE_FULL_OUTER` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `JOIN_TYPE_INNER` | ✓ | Joined in DataFusion when the smaller side is at or below `JOIN_BROADCAST_MAX_BYTES` and the query fits the broadcast rules. Otherwise Exasol SQL joins the per-table scans. |
| `JOIN_TYPE_LEFT_OUTER` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `JOIN_TYPE_RIGHT_OUTER` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `LIMIT` | ✓ | On a plain row scan each shard stops early. Exasol SQL re-applies the limit. |
| `LIMIT_WITH_OFFSET` | ✓ | Exasol SQL applies the offset. A non-zero offset drops the per-shard limit. |
| `ORDER_BY_COLUMN` | ✓ | DataFusion runs a per-shard top-N for `ORDER BY ... LIMIT n` on projected columns, without aggregation or offset. Otherwise Exasol SQL sorts. See [Capabilities](capabilities.md#filtering). |
| `ORDER_BY_EXPRESSION` | ✓ | Always sorted in Exasol SQL, never as a per-shard top-N. A sort key the adapter cannot render fails the query. |
| `SELECTLIST_EXPRESSIONS` | ✓ | DataFusion evaluates them when it can render every item. Otherwise Exasol SQL does. |
| `SELECTLIST_PROJECTION` | ✓ |  |

## Literals

| Literal | Advertised | Notes |
|---|:---:|---|
| `BOOL` | ✓ |  |
| `DATE` | ✓ |  |
| `DOUBLE` | ✓ |  |
| `EXACTNUMERIC` | ✓ |  |
| `INTERVAL` |  | INTERVAL has no faithful DataFusion mapping. |
| `NULL` | ✓ |  |
| `STRING` | ✓ |  |
| `TIMESTAMP` | ✓ |  |
| `TIMESTAMP_UTC` | ✓ |  |

## Predicates

There is no `GREATER` or `GREATEREQUAL` predicate. Exasol rewrites `a > b` as `b < a` before it
sends the query to the adapter.

| Predicate | Advertised | Notes |
|---|:---:|---|
| `AND` | ✓ |  |
| `BETWEEN` | ✓ |  |
| `EQUAL` | ✓ |  |
| `IN_CONSTLIST` | ✓ |  |
| `IS_JSON` |  |  |
| `IS_NOT_JSON` |  |  |
| `IS_NOT_NULL` | ✓ |  |
| `IS_NULL` | ✓ |  |
| `LESS` | ✓ |  |
| `LESSEQUAL` | ✓ |  |
| `LIKE` | ✓ |  |
| `LIKE_ESCAPE` | ✓ |  |
| `NOT` | ✓ |  |
| `NOTEQUAL` | ✓ |  |
| `OR` | ✓ |  |
| `REGEXP_LIKE` |  | DataFusion regex matching differs from Exasol's PCRE matching. |

## Scalar functions

| Scalar function | Advertised | Notes |
|---|:---:|---|
| `ABS` | ✓ |  |
| `ACOS` | ✓ |  |
| `ADD` | ✓ |  |
| `ADD_DAYS` |  | Exasol returns DATE or TIMESTAMP by argument type, which the translator cannot see. |
| `ADD_HOURS` |  | Exasol infers `TIMESTAMP(0)` for a DATE argument, the scan would return `TIMESTAMP(3)`. |
| `ADD_MINUTES` |  | Exasol infers `TIMESTAMP(0)` for a DATE argument, the scan would return `TIMESTAMP(3)`. |
| `ADD_MONTHS` |  | Exasol keeps a month-end date at month-end. DataFusion keeps the day of month. |
| `ADD_SECONDS` |  | Parity with DataFusion for fractional seconds is not established. |
| `ADD_WEEKS` |  | Exasol returns DATE or TIMESTAMP by argument type, which the translator cannot see. |
| `ADD_YEARS` |  | Exasol keeps a month-end date at month-end. DataFusion keeps the day of month. |
| `ASCII` | ✓ |  |
| `ASIN` | ✓ |  |
| `ATAN` | ✓ |  |
| `ATAN2` | ✓ |  |
| `BIT_AND` |  | Exasol bit functions are unsigned 64-bit, DataFusion's are signed (#108). |
| `BIT_CHECK` |  | DataFusion has no equivalent (#108). |
| `BIT_LENGTH` |  |  |
| `BIT_LROTATE` |  | DataFusion has no equivalent (#108). |
| `BIT_LSHIFT` |  | Exasol bit functions are unsigned 64-bit, DataFusion's are signed (#108). |
| `BIT_NOT` |  | DataFusion has no equivalent (#108). |
| `BIT_OR` |  | Exasol bit functions are unsigned 64-bit, DataFusion's are signed (#108). |
| `BIT_RROTATE` |  | DataFusion has no equivalent (#108). |
| `BIT_RSHIFT` |  | Exasol bit functions are unsigned 64-bit, DataFusion's are signed (#108). |
| `BIT_SET` |  | DataFusion has no equivalent (#108). |
| `BIT_TO_NUM` |  | DataFusion has no equivalent (#108). |
| `BIT_XOR` |  | Exasol bit functions are unsigned 64-bit, DataFusion's are signed (#108). |
| `CASE` | ✓ |  |
| `CAST` | ✓ | A cast to INTERVAL, GEOMETRY, HASHTYPE, or TIMESTAMP WITH LOCAL TIME ZONE fails the query. |
| `CEIL` | ✓ |  |
| `CHR` | ✓ |  |
| `COLOGNE_PHONETIC` |  |  |
| `CONCAT` | ✓ |  |
| `CONVERT_TZ` |  | Depends on the session's `TIME_ZONE_BEHAVIOR`, which the scan cannot see. |
| `COS` | ✓ |  |
| `COSH` | ✓ |  |
| `COT` | ✓ |  |
| `CURRENT_CLUSTER` |  |  |
| `CURRENT_DATE` |  | The scan has no clock, time zone, or statement anchor. Exasol evaluates it once per statement. |
| `CURRENT_SCHEMA` |  | Exasol session function. |
| `CURRENT_SESSION` |  |  |
| `CURRENT_STATEMENT` |  |  |
| `CURRENT_TIMESTAMP` |  | The scan has no clock, time zone, or statement anchor. Exasol evaluates it once per statement. |
| `CURRENT_USER` |  | Exasol session function. |
| `DATE_TRUNC` | ✓ |  |
| `DAY` | ✓ |  |
| `DAYOFWEEK` |  | Depends on the session's `NLS_FIRST_DAY_OF_WEEK`, which the scan cannot see. |
| `DAYS_BETWEEN` | ✓ |  |
| `DBTIMEZONE` |  | The scan sees neither the session nor the database time zone. |
| `DEGREES` | ✓ |  |
| `DIV` |  | Needs truncating division, and the request carries no operand types. |
| `DUMP` |  |  |
| `EDIT_DISTANCE` |  |  |
| `EXP` | ✓ |  |
| `EXTRACT` | ✓ |  |
| `FLOAT_DIV` | ✓ |  |
| `FLOOR` | ✓ |  |
| `FROM_POSIX_TIME` |  |  |
| `GREATEST` | ✓ |  |
| `HASHTYPE_MD5` |  | Returns HASHTYPE, which has no faithful DataFusion mapping. |
| `HASHTYPE_SHA1` |  | Returns HASHTYPE, which has no faithful DataFusion mapping. |
| `HASHTYPE_SHA256` |  | Returns HASHTYPE, which has no faithful DataFusion mapping. |
| `HASHTYPE_SHA512` |  | Returns HASHTYPE, which has no faithful DataFusion mapping. |
| `HASHTYPE_TIGER` |  | Returns HASHTYPE, which has no faithful DataFusion mapping. |
| `HASH_MD5` |  |  |
| `HASH_SHA1` |  |  |
| `HASH_SHA256` |  |  |
| `HASH_SHA512` |  |  |
| `HASH_TIGER` |  |  |
| `HOUR` | ✓ |  |
| `HOURS_BETWEEN` | ✓ |  |
| `INITCAP` | ✓ | Non-ASCII text can differ. See [Semantic differences](semantic-differences.md). |
| `INSERT` |  |  |
| `INSTR` | ✓ |  |
| `IS_BOOLEAN` |  |  |
| `IS_DATE` |  |  |
| `IS_DSINTERVAL` |  |  |
| `IS_NUMBER` |  |  |
| `IS_TIMESTAMP` |  |  |
| `IS_YMINTERVAL` |  |  |
| `JSON_VALUE` |  |  |
| `LAST_DAY` |  | Not translated. |
| `LEAST` | ✓ |  |
| `LEFT` | ✓ |  |
| `LENGTH` | ✓ |  |
| `LN` | ✓ |  |
| `LOCALTIMESTAMP` |  | The scan has no clock, time zone, or statement anchor. Exasol evaluates it once per statement. |
| `LOCATE` | ✓ |  |
| `LOG` | ✓ |  |
| `LOWER` | ✓ | Non-ASCII text can differ. See [Semantic differences](semantic-differences.md). |
| `LPAD` | ✓ |  |
| `LTRIM` | ✓ |  |
| `MINUTE` | ✓ |  |
| `MINUTES_BETWEEN` | ✓ |  |
| `MIN_SCALE` |  |  |
| `MOD` | ✓ |  |
| `MONTH` | ✓ |  |
| `MONTHS_BETWEEN` |  | Exasol returns an Oracle-style fraction. DataFusion has no equivalent. |
| `MULT` | ✓ |  |
| `NEG` | ✓ |  |
| `NULLIFZERO` | ✓ |  |
| `NUMTODSINTERVAL` |  | Returns INTERVAL, which has no faithful DataFusion mapping. |
| `NUMTOYMINTERVAL` |  | Returns INTERVAL, which has no faithful DataFusion mapping. |
| `OCTET_LENGTH` | ✓ |  |
| `POSIX_TIME` |  |  |
| `POWER` | ✓ |  |
| `RADIANS` | ✓ |  |
| `RAND` |  |  |
| `REGEXP_COUNT` |  | DataFusion regex matching differs from Exasol's PCRE matching. |
| `REGEXP_INSTR` |  | DataFusion regex matching differs from Exasol's PCRE matching. |
| `REGEXP_REPLACE` |  | DataFusion regex matching differs from Exasol's PCRE matching. |
| `REGEXP_SUBSTR` |  | DataFusion regex matching differs from Exasol's PCRE matching. |
| `REPEAT` | ✓ |  |
| `REPLACE` | ✓ |  |
| `REVERSE` | ✓ |  |
| `RIGHT` | ✓ |  |
| `ROUND` | ✓ |  |
| `RPAD` | ✓ |  |
| `RTRIM` | ✓ |  |
| `SECOND` | ✓ |  |
| `SECONDS_BETWEEN` | ✓ |  |
| `SESSIONTIMEZONE` |  | The scan sees neither the session nor the database time zone. |
| `SESSION_PARAMETER` |  |  |
| `SIGN` | ✓ |  |
| `SIN` | ✓ |  |
| `SINH` | ✓ |  |
| `SOUNDEX` |  |  |
| `SPACE` |  |  |
| `SQRT` | ✓ |  |
| `ST_AREA` |  | No lakehouse type maps to GEOMETRY. |
| `ST_BOUNDARY` |  | No lakehouse type maps to GEOMETRY. |
| `ST_BUFFER` |  | No lakehouse type maps to GEOMETRY. |
| `ST_CENTROID` |  | No lakehouse type maps to GEOMETRY. |
| `ST_CONTAINS` |  | No lakehouse type maps to GEOMETRY. |
| `ST_CONVEXHULL` |  | No lakehouse type maps to GEOMETRY. |
| `ST_CROSSES` |  | No lakehouse type maps to GEOMETRY. |
| `ST_DIFFERENCE` |  | No lakehouse type maps to GEOMETRY. |
| `ST_DIMENSION` |  | No lakehouse type maps to GEOMETRY. |
| `ST_DISJOINT` |  | No lakehouse type maps to GEOMETRY. |
| `ST_DISTANCE` |  | No lakehouse type maps to GEOMETRY. |
| `ST_ENDPOINT` |  | No lakehouse type maps to GEOMETRY. |
| `ST_ENVELOPE` |  | No lakehouse type maps to GEOMETRY. |
| `ST_EQUALS` |  | No lakehouse type maps to GEOMETRY. |
| `ST_EXTERIORRING` |  | No lakehouse type maps to GEOMETRY. |
| `ST_FORCE2D` |  | No lakehouse type maps to GEOMETRY. |
| `ST_GEOMETRYN` |  | No lakehouse type maps to GEOMETRY. |
| `ST_GEOMETRYTYPE` |  | No lakehouse type maps to GEOMETRY. |
| `ST_INTERIORRINGN` |  | No lakehouse type maps to GEOMETRY. |
| `ST_INTERSECTION` |  | No lakehouse type maps to GEOMETRY. |
| `ST_INTERSECTS` |  | No lakehouse type maps to GEOMETRY. |
| `ST_ISCLOSED` |  | No lakehouse type maps to GEOMETRY. |
| `ST_ISEMPTY` |  | No lakehouse type maps to GEOMETRY. |
| `ST_ISRING` |  | No lakehouse type maps to GEOMETRY. |
| `ST_ISSIMPLE` |  | No lakehouse type maps to GEOMETRY. |
| `ST_LENGTH` |  | No lakehouse type maps to GEOMETRY. |
| `ST_NUMGEOMETRIES` |  | No lakehouse type maps to GEOMETRY. |
| `ST_NUMINTERIORRINGS` |  | No lakehouse type maps to GEOMETRY. |
| `ST_NUMPOINTS` |  | No lakehouse type maps to GEOMETRY. |
| `ST_OVERLAPS` |  | No lakehouse type maps to GEOMETRY. |
| `ST_POINTN` |  | No lakehouse type maps to GEOMETRY. |
| `ST_SETSRID` |  | No lakehouse type maps to GEOMETRY. |
| `ST_STARTPOINT` |  | No lakehouse type maps to GEOMETRY. |
| `ST_SYMDIFFERENCE` |  | No lakehouse type maps to GEOMETRY. |
| `ST_TOUCHES` |  | No lakehouse type maps to GEOMETRY. |
| `ST_TRANSFORM` |  | No lakehouse type maps to GEOMETRY. |
| `ST_UNION` |  | No lakehouse type maps to GEOMETRY. |
| `ST_WITHIN` |  | No lakehouse type maps to GEOMETRY. |
| `ST_X` |  | No lakehouse type maps to GEOMETRY. |
| `ST_Y` |  | No lakehouse type maps to GEOMETRY. |
| `SUB` | ✓ |  |
| `SUBSTR` | ✓ |  |
| `SYSDATE` |  | The scan has no clock, time zone, or statement anchor. Exasol evaluates it once per statement. |
| `SYSTIMESTAMP` |  | The scan has no clock, time zone, or statement anchor. Exasol evaluates it once per statement. |
| `SYS_GUID` |  | Exasol-only function. |
| `TAN` | ✓ |  |
| `TANH` | ✓ |  |
| `TO_CHAR` |  | DataFusion `to_char` takes chrono formats, not Exasol format models. |
| `TO_DATE` | ✓ | Only some formats run in DataFusion, the rest in Exasol SQL. See [Semantic differences](semantic-differences.md). |
| `TO_DSINTERVAL` |  | Returns INTERVAL, which has no faithful DataFusion mapping. |
| `TO_NUMBER` |  | DataFusion has no `to_number`. |
| `TO_TIMESTAMP` | ✓ | Only some formats run in DataFusion, the rest in Exasol SQL. See [Semantic differences](semantic-differences.md). |
| `TO_YMINTERVAL` |  | Returns INTERVAL, which has no faithful DataFusion mapping. |
| `TRANSLATE` | ✓ |  |
| `TRIM` | ✓ |  |
| `TRUNC` | ✓ |  |
| `TYPEOF` |  |  |
| `UNICODE` | ✓ |  |
| `UNICODECHR` | ✓ |  |
| `UPPER` | ✓ | Non-ASCII text can differ. See [Semantic differences](semantic-differences.md). |
| `WEEK` | ✓ |  |
| `WIDTH_BUCKET` |  |  |
| `YEAR` | ✓ |  |
| `YEARS_BETWEEN` |  | Exasol returns an Oracle-style fraction. DataFusion has no equivalent. |
| `ZEROIFNULL` | ✓ |  |

## Aggregate functions

| Aggregate function | Advertised | Notes |
|---|:---:|---|
| `APPROXIMATE_COUNT_DISTINCT` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `AVG` | ✓ | DataFusion sums and counts per shard. Exasol SQL divides. See [Semantic differences](semantic-differences.md). |
| `AVG_DISTINCT` |  |  |
| `CORR` |  |  |
| `COUNT` | ✓ |  |
| `COUNT_DISTINCT` | ✓ | DataFusion deduplicates per shard only for a lone single-group `COUNT(DISTINCT col)`. Otherwise Exasol SQL computes it over the scanned rows. |
| `COUNT_STAR` | ✓ |  |
| `COUNT_TUPLE` |  |  |
| `EVERY` |  |  |
| `FIRST_VALUE` |  |  |
| `GROUPING` |  |  |
| `GROUPING_ID` |  |  |
| `GROUP_CONCAT` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `GROUP_CONCAT_DISTINCT` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `GROUP_CONCAT_ORDER_BY` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `GROUP_CONCAT_SEPARATOR` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `LAST_VALUE` |  |  |
| `LISTAGG` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `LISTAGG_DISTINCT` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `LISTAGG_ON_OVERFLOW_ERROR` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `LISTAGG_ON_OVERFLOW_TRUNCATE` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `LISTAGG_ORDER_BY` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `LISTAGG_SEPARATOR` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `MAX` | ✓ |  |
| `MEDIAN` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `MIN` | ✓ |  |
| `MUL` |  |  |
| `MUL_DISTINCT` |  |  |
| `SOME` |  |  |
| `STDDEV` | ✓ | Computed in double precision for a bare-column argument. See [Semantic differences](semantic-differences.md). |
| `STDDEV_DISTINCT` |  |  |
| `STDDEV_POP` | ✓ | Computed in double precision for a bare-column argument. See [Semantic differences](semantic-differences.md). |
| `STDDEV_POP_DISTINCT` |  |  |
| `STDDEV_SAMP` | ✓ | Computed in double precision for a bare-column argument. See [Semantic differences](semantic-differences.md). |
| `STDDEV_SAMP_DISTINCT` |  |  |
| `ST_INTERSECTION` |  | No lakehouse type maps to GEOMETRY. |
| `ST_UNION` |  | No lakehouse type maps to GEOMETRY. |
| `SUM` | ✓ |  |
| `SUM_DISTINCT` |  |  |
| `VARIANCE` | ✓ | Computed in double precision for a bare-column argument. See [Semantic differences](semantic-differences.md). |
| `VARIANCE_DISTINCT` |  |  |
| `VAR_POP` | ✓ | Computed in double precision for a bare-column argument. See [Semantic differences](semantic-differences.md). |
| `VAR_POP_DISTINCT` |  |  |
| `VAR_SAMP` | ✓ | Computed in double precision for a bare-column argument. See [Semantic differences](semantic-differences.md). |
| `VAR_SAMP_DISTINCT` |  |  |
