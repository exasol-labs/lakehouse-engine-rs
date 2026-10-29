[lakehouse-engine](../README.md) › [Docs](index.md) › Capability matrix

---

# Capability matrix

This page lists every Exasol Virtual Schema capability and whether the adapter advertises it.
Exasol pushes an advertised capability down to the adapter. Exasol evaluates an unadvertised
capability itself, over the rows the scan returns, so the query still returns correct results.
[Capabilities](capabilities.md) explains how each pushed-down capability is executed.

The capability names come from the Exasol
[capabilities list](https://github.com/exasol/virtual-schema-common-java/blob/main/doc/development/api/capabilities_list.md).
The ✓ marks mirror `CAPABILITIES` in `crates/lakehouse-engine/src/adapter/capabilities.rs`.

## Main capabilities

| Capability | Advertised | Notes |
|---|:---:|---|
| `AGGREGATE_GROUP_BY_COLUMN` | ✓ |  |
| `AGGREGATE_GROUP_BY_EXPRESSION` | ✓ |  |
| `AGGREGATE_GROUP_BY_TUPLE` | ✓ |  |
| `AGGREGATE_HAVING` | ✓ |  |
| `AGGREGATE_SINGLE_GROUP` | ✓ |  |
| `FILTER_EXPRESSIONS` | ✓ |  |
| `JOIN` | ✓ |  |
| `JOIN_CONDITION_ALL` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `JOIN_CONDITION_EQUI` | ✓ |  |
| `JOIN_TYPE_FULL_OUTER` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `JOIN_TYPE_INNER` | ✓ | Broadcast when the smaller side is below `JOIN_BROADCAST_MAX_BYTES`, otherwise an unaccelerated join. |
| `JOIN_TYPE_LEFT_OUTER` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `JOIN_TYPE_RIGHT_OUTER` |  | Only inner equi-joins are pushed down. Exasol pushes each table separately and joins the results. |
| `LIMIT` | ✓ |  |
| `LIMIT_WITH_OFFSET` | ✓ | A non-zero offset runs as a row scan, not a per-shard top-N. |
| `ORDER_BY_COLUMN` | ✓ | Per-shard top-N only on a single table, without `GROUP BY`. See [Capabilities](capabilities.md#filtering). |
| `ORDER_BY_EXPRESSION` | ✓ | Ordered correctly, but never as a per-shard top-N. See [Capabilities](capabilities.md#filtering). |
| `SELECTLIST_EXPRESSIONS` | ✓ |  |
| `SELECTLIST_PROJECTION` | ✓ |  |

## Literals

| Literal | Advertised | Notes |
|---|:---:|---|
| `BOOL` | ✓ |  |
| `DATE` | ✓ |  |
| `DOUBLE` | ✓ |  |
| `EXACTNUMERIC` | ✓ |  |
| `INTERVAL` |  |  |
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
| `ADD_DAYS` |  | The DataFusion result differs from Exasol's. |
| `ADD_HOURS` |  | Exasol infers `TIMESTAMP(0)` for a DATE argument, the scan would return `TIMESTAMP(3)`. |
| `ADD_MINUTES` |  | Exasol infers `TIMESTAMP(0)` for a DATE argument, the scan would return `TIMESTAMP(3)`. |
| `ADD_MONTHS` |  | The DataFusion result differs from Exasol's. |
| `ADD_SECONDS` |  | The DataFusion result differs from Exasol's. |
| `ADD_WEEKS` |  | The DataFusion result differs from Exasol's. |
| `ADD_YEARS` |  | The DataFusion result differs from Exasol's. |
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
| `CONVERT_TZ` |  | The DataFusion result differs from Exasol's. |
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
| `DAYOFWEEK` |  | The DataFusion result differs from Exasol's. |
| `DAYS_BETWEEN` | ✓ |  |
| `DBTIMEZONE` |  |  |
| `DEGREES` | ✓ |  |
| `DIV` |  | No DataFusion translation matches Exasol. |
| `DUMP` |  |  |
| `EDIT_DISTANCE` |  |  |
| `EXP` | ✓ |  |
| `EXTRACT` | ✓ |  |
| `FLOAT_DIV` | ✓ |  |
| `FLOOR` | ✓ |  |
| `FROM_POSIX_TIME` |  |  |
| `GREATEST` | ✓ |  |
| `HASHTYPE_MD5` |  |  |
| `HASHTYPE_SHA1` |  |  |
| `HASHTYPE_SHA256` |  |  |
| `HASHTYPE_SHA512` |  |  |
| `HASHTYPE_TIGER` |  |  |
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
| `LAST_DAY` |  | The DataFusion result differs from Exasol's. |
| `LEAST` | ✓ |  |
| `LEFT` | ✓ |  |
| `LENGTH` | ✓ |  |
| `LN` | ✓ |  |
| `LOCALTIMESTAMP` |  |  |
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
| `MONTHS_BETWEEN` |  | The DataFusion result differs from Exasol's. |
| `MULT` | ✓ |  |
| `NEG` | ✓ |  |
| `NULLIFZERO` | ✓ |  |
| `NUMTODSINTERVAL` |  |  |
| `NUMTOYMINTERVAL` |  |  |
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
| `SESSIONTIMEZONE` |  |  |
| `SESSION_PARAMETER` |  |  |
| `SIGN` | ✓ |  |
| `SIN` | ✓ |  |
| `SINH` | ✓ |  |
| `SOUNDEX` |  |  |
| `SPACE` |  |  |
| `SQRT` | ✓ |  |
| `SUB` | ✓ |  |
| `SUBSTR` | ✓ |  |
| `SYSDATE` |  | The scan has no clock, time zone, or statement anchor. Exasol evaluates it once per statement. |
| `SYSTIMESTAMP` |  | The scan has no clock, time zone, or statement anchor. Exasol evaluates it once per statement. |
| `SYS_GUID` |  | Exasol session function. |
| `TAN` | ✓ |  |
| `TANH` | ✓ |  |
| `TO_CHAR` |  | No DataFusion translation matches Exasol. |
| `TO_DATE` | ✓ | Only some formats run in the scan. See [Semantic differences](semantic-differences.md). |
| `TO_DSINTERVAL` |  |  |
| `TO_NUMBER` |  | No DataFusion translation matches Exasol. |
| `TO_TIMESTAMP` | ✓ | Only some formats run in the scan. See [Semantic differences](semantic-differences.md). |
| `TO_YMINTERVAL` |  |  |
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
| `YEARS_BETWEEN` |  | The DataFusion result differs from Exasol's. |
| `ZEROIFNULL` | ✓ |  |

## Aggregate functions

| Aggregate function | Advertised | Notes |
|---|:---:|---|
| `APPROXIMATE_COUNT_DISTINCT` |  | Not decomposable into a partial/merge plan. Exasol computes it on the returned rows. |
| `AVG` | ✓ | See [Semantic differences](semantic-differences.md). |
| `AVG_DISTINCT` |  |  |
| `CORR` |  |  |
| `COUNT` | ✓ |  |
| `COUNT_DISTINCT` | ✓ | Single-group only. Inside a `GROUP BY`, Exasol computes it on the returned rows. |
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
| `STDDEV` | ✓ | Computed in double precision. See [Semantic differences](semantic-differences.md). |
| `STDDEV_DISTINCT` |  |  |
| `STDDEV_POP` | ✓ | Computed in double precision. See [Semantic differences](semantic-differences.md). |
| `STDDEV_POP_DISTINCT` |  |  |
| `STDDEV_SAMP` | ✓ | Computed in double precision. See [Semantic differences](semantic-differences.md). |
| `STDDEV_SAMP_DISTINCT` |  |  |
| `SUM` | ✓ |  |
| `SUM_DISTINCT` |  |  |
| `VARIANCE` | ✓ | Computed in double precision. See [Semantic differences](semantic-differences.md). |
| `VARIANCE_DISTINCT` |  |  |
| `VAR_POP` | ✓ | Computed in double precision. See [Semantic differences](semantic-differences.md). |
| `VAR_POP_DISTINCT` |  |  |
| `VAR_SAMP` | ✓ | Computed in double precision. See [Semantic differences](semantic-differences.md). |
| `VAR_SAMP_DISTINCT` |  |  |

## Geospatial functions

The adapter advertises none of the 42 geospatial capabilities. Exasol evaluates them itself:
`FN_AGG_ST_INTERSECTION`, `FN_AGG_ST_UNION`, `FN_ST_AREA`, `FN_ST_BOUNDARY`, `FN_ST_BUFFER`, `FN_ST_CENTROID`, `FN_ST_CONTAINS`, `FN_ST_CONVEXHULL`, `FN_ST_CROSSES`, `FN_ST_DIFFERENCE`, `FN_ST_DIMENSION`, `FN_ST_DISJOINT`, `FN_ST_DISTANCE`, `FN_ST_ENDPOINT`, `FN_ST_ENVELOPE`, `FN_ST_EQUALS`, `FN_ST_EXTERIORRING`, `FN_ST_FORCE2D`, `FN_ST_GEOMETRYN`, `FN_ST_GEOMETRYTYPE`, `FN_ST_INTERIORRINGN`, `FN_ST_INTERSECTION`, `FN_ST_INTERSECTS`, `FN_ST_ISCLOSED`, `FN_ST_ISEMPTY`, `FN_ST_ISRING`, `FN_ST_ISSIMPLE`, `FN_ST_LENGTH`, `FN_ST_NUMGEOMETRIES`, `FN_ST_NUMINTERIORRINGS`, `FN_ST_NUMPOINTS`, `FN_ST_OVERLAPS`, `FN_ST_POINTN`, `FN_ST_SETSRID`, `FN_ST_STARTPOINT`, `FN_ST_SYMDIFFERENCE`, `FN_ST_TOUCHES`, `FN_ST_TRANSFORM`, `FN_ST_UNION`, `FN_ST_WITHIN`, `FN_ST_X`, `FN_ST_Y`.
