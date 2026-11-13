[lakehouse-engine](../README.md) › [Docs](index.md) › Capability matrix

---

# Capability matrix

This page lists every Exasol Virtual Schema capability and whether the adapter advertises it.
Exasol evaluates an unadvertised capability itself. [Capabilities](capabilities.md) explains how
each advertised capability runs and which forms run in Exasol SQL.
[Semantic differences](semantic-differences.md) lists where results can differ from native Exasol.
`EXCLUDED_CAPABILITIES` withdraws an advertised capability per schema.

The names come from the Exasol
[capabilities list](https://github.com/exasol/virtual-schema-common-java/blob/main/doc/development/api/capabilities_list.md).

## Main capabilities

| Capability | Advertised | Notes |
|---|:---:|---|
| `AGGREGATE_GROUP_BY_COLUMN` | ✓ | Partials per shard, merged in Exasol SQL. |
| `AGGREGATE_GROUP_BY_EXPRESSION` | ✓ | Partials per shard, merged in Exasol SQL. |
| `AGGREGATE_GROUP_BY_TUPLE` | ✓ | Partials per shard, merged in Exasol SQL. |
| `AGGREGATE_HAVING` | ✓ | Runs in Exasol SQL. |
| `AGGREGATE_SINGLE_GROUP` | ✓ | Partials per shard, merged in Exasol SQL. |
| `FILTER_EXPRESSIONS` | ✓ | Some forms run in Exasol SQL. |
| `JOIN` | ✓ |  |
| `JOIN_CONDITION_ALL` |  | Only inner equi-joins push down. |
| `JOIN_CONDITION_EQUI` | ✓ |  |
| `JOIN_TYPE_FULL_OUTER` |  | Only inner equi-joins push down. |
| `JOIN_TYPE_INNER` | ✓ | Broadcast join when one side is small. |
| `JOIN_TYPE_LEFT_OUTER` |  | Only inner equi-joins push down. |
| `JOIN_TYPE_RIGHT_OUTER` |  | Only inner equi-joins push down. |
| `LIMIT` | ✓ | Stops each shard early. |
| `LIMIT_WITH_OFFSET` | ✓ | Offset runs in Exasol SQL. |
| `ORDER_BY_COLUMN` | ✓ | Per-shard top-N for some shapes. |
| `ORDER_BY_EXPRESSION` | ✓ | Sorts in Exasol SQL. |
| `SELECTLIST_EXPRESSIONS` | ✓ | Some forms run in Exasol SQL. |
| `SELECTLIST_PROJECTION` | ✓ |  |

## Literals

| Literal | Advertised | Notes |
|---|:---:|---|
| `BOOL` | ✓ |  |
| `DATE` | ✓ |  |
| `DOUBLE` | ✓ |  |
| `EXACTNUMERIC` | ✓ |  |
| `INTERVAL` |  | No DataFusion mapping. |
| `NULL` | ✓ |  |
| `STRING` | ✓ |  |
| `TIMESTAMP` | ✓ |  |
| `TIMESTAMP_UTC` | ✓ |  |

## Predicates

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
| `REGEXP_LIKE` |  | Regex dialect differs. |

## Scalar functions

| Scalar function | Advertised | Notes |
|---|:---:|---|
| `ABS` | ✓ |  |
| `ACOS` | ✓ |  |
| `ADD` | ✓ |  |
| `ADD_DAYS` | ✓ | Some forms run in Exasol SQL. |
| `ADD_HOURS` | ✓ | Some forms run in Exasol SQL. |
| `ADD_MINUTES` | ✓ | Some forms run in Exasol SQL. |
| `ADD_MONTHS` |  | Month-end handling differs. |
| `ADD_SECONDS` |  | Fractional-second parity unverified. |
| `ADD_WEEKS` | ✓ | Some forms run in Exasol SQL. |
| `ADD_YEARS` |  | Month-end handling differs. |
| `ASCII` | ✓ |  |
| `ASIN` | ✓ |  |
| `ATAN` | ✓ |  |
| `ATAN2` | ✓ |  |
| `BIT_AND` |  | Signedness differs (#108). |
| `BIT_CHECK` |  | No DataFusion equivalent (#108). |
| `BIT_LENGTH` |  |  |
| `BIT_LROTATE` |  | No DataFusion equivalent (#108). |
| `BIT_LSHIFT` |  | Signedness differs (#108). |
| `BIT_NOT` |  | No DataFusion equivalent (#108). |
| `BIT_OR` |  | Signedness differs (#108). |
| `BIT_RROTATE` |  | No DataFusion equivalent (#108). |
| `BIT_RSHIFT` |  | Signedness differs (#108). |
| `BIT_SET` |  | No DataFusion equivalent (#108). |
| `BIT_TO_NUM` |  | No DataFusion equivalent (#108). |
| `BIT_XOR` |  | Signedness differs (#108). |
| `CASE` | ✓ |  |
| `CAST` | ✓ | Some forms run in Exasol SQL. |
| `CEIL` | ✓ |  |
| `CHR` | ✓ |  |
| `COLOGNE_PHONETIC` |  |  |
| `CONCAT` | ✓ |  |
| `CONVERT_TZ` |  | Needs the session time zone. |
| `COS` | ✓ |  |
| `COSH` | ✓ |  |
| `COT` | ✓ |  |
| `CURRENT_CLUSTER` |  |  |
| `CURRENT_DATE` |  | Needs the statement clock. |
| `CURRENT_SCHEMA` |  | Session function. |
| `CURRENT_SESSION` |  |  |
| `CURRENT_STATEMENT` |  |  |
| `CURRENT_TIMESTAMP` |  | Needs the statement clock. |
| `CURRENT_USER` |  | Session function. |
| `DATE_TRUNC` | ✓ |  |
| `DAY` | ✓ |  |
| `DAYOFWEEK` |  | Needs the session's `NLS_FIRST_DAY_OF_WEEK`. |
| `DAYS_BETWEEN` | ✓ |  |
| `DBTIMEZONE` |  | Needs the session time zone. |
| `DEGREES` | ✓ |  |
| `DIV` | ✓ | Some forms run in Exasol SQL. |
| `DUMP` |  |  |
| `EDIT_DISTANCE` |  |  |
| `EXP` | ✓ |  |
| `EXTRACT` | ✓ |  |
| `FLOAT_DIV` | ✓ |  |
| `FLOOR` | ✓ |  |
| `FROM_POSIX_TIME` |  |  |
| `GREATEST` | ✓ |  |
| `HASHTYPE_MD5` |  | Result type has no DataFusion mapping. |
| `HASHTYPE_SHA1` |  | Result type has no DataFusion mapping. |
| `HASHTYPE_SHA256` |  | Result type has no DataFusion mapping. |
| `HASHTYPE_SHA512` |  | Result type has no DataFusion mapping. |
| `HASHTYPE_TIGER` |  | Result type has no DataFusion mapping. |
| `HASH_MD5` |  |  |
| `HASH_SHA1` |  |  |
| `HASH_SHA256` |  |  |
| `HASH_SHA512` |  |  |
| `HASH_TIGER` |  |  |
| `HOUR` | ✓ |  |
| `HOURS_BETWEEN` | ✓ |  |
| `INITCAP` | ✓ | Non-ASCII text can differ. |
| `INSERT` |  |  |
| `INSTR` | ✓ | Some forms run in Exasol SQL. |
| `IS_BOOLEAN` |  |  |
| `IS_DATE` |  |  |
| `IS_DSINTERVAL` |  |  |
| `IS_NUMBER` |  |  |
| `IS_TIMESTAMP` |  |  |
| `IS_YMINTERVAL` |  |  |
| `JSON_VALUE` | ✓ | Some forms run in Exasol SQL. |
| `LAST_DAY` |  | Not translated. |
| `LEAST` | ✓ |  |
| `LEFT` | ✓ |  |
| `LENGTH` | ✓ |  |
| `LN` | ✓ |  |
| `LOCALTIMESTAMP` |  | Needs the statement clock. |
| `LOCATE` | ✓ | Some forms run in Exasol SQL. |
| `LOG` | ✓ |  |
| `LOWER` | ✓ | Non-ASCII text can differ. |
| `LPAD` | ✓ |  |
| `LTRIM` | ✓ |  |
| `MINUTE` | ✓ |  |
| `MINUTES_BETWEEN` | ✓ |  |
| `MIN_SCALE` |  |  |
| `MOD` | ✓ |  |
| `MONTH` | ✓ |  |
| `MONTHS_BETWEEN` |  | No DataFusion equivalent. |
| `MULT` | ✓ |  |
| `NEG` | ✓ |  |
| `NULLIFZERO` | ✓ |  |
| `NUMTODSINTERVAL` |  | Result type has no DataFusion mapping. |
| `NUMTOYMINTERVAL` |  | Result type has no DataFusion mapping. |
| `OCTET_LENGTH` | ✓ |  |
| `POSIX_TIME` |  |  |
| `POWER` | ✓ |  |
| `RADIANS` | ✓ |  |
| `RAND` |  |  |
| `REGEXP_COUNT` |  | Regex dialect differs. |
| `REGEXP_INSTR` |  | Regex dialect differs. |
| `REGEXP_REPLACE` |  | Regex dialect differs. |
| `REGEXP_SUBSTR` |  | Regex dialect differs. |
| `REPEAT` | ✓ |  |
| `REPLACE` | ✓ |  |
| `REVERSE` | ✓ |  |
| `RIGHT` | ✓ |  |
| `ROUND` | ✓ |  |
| `RPAD` | ✓ |  |
| `RTRIM` | ✓ |  |
| `SECOND` | ✓ |  |
| `SECONDS_BETWEEN` | ✓ |  |
| `SESSIONTIMEZONE` |  | Needs the session time zone. |
| `SESSION_PARAMETER` |  |  |
| `SIGN` | ✓ |  |
| `SIN` | ✓ |  |
| `SINH` | ✓ |  |
| `SOUNDEX` |  |  |
| `SPACE` |  |  |
| `SQRT` | ✓ |  |
| `ST_AREA` |  | No GEOMETRY type. |
| `ST_BOUNDARY` |  | No GEOMETRY type. |
| `ST_BUFFER` |  | No GEOMETRY type. |
| `ST_CENTROID` |  | No GEOMETRY type. |
| `ST_CONTAINS` |  | No GEOMETRY type. |
| `ST_CONVEXHULL` |  | No GEOMETRY type. |
| `ST_CROSSES` |  | No GEOMETRY type. |
| `ST_DIFFERENCE` |  | No GEOMETRY type. |
| `ST_DIMENSION` |  | No GEOMETRY type. |
| `ST_DISJOINT` |  | No GEOMETRY type. |
| `ST_DISTANCE` |  | No GEOMETRY type. |
| `ST_ENDPOINT` |  | No GEOMETRY type. |
| `ST_ENVELOPE` |  | No GEOMETRY type. |
| `ST_EQUALS` |  | No GEOMETRY type. |
| `ST_EXTERIORRING` |  | No GEOMETRY type. |
| `ST_FORCE2D` |  | No GEOMETRY type. |
| `ST_GEOMETRYN` |  | No GEOMETRY type. |
| `ST_GEOMETRYTYPE` |  | No GEOMETRY type. |
| `ST_INTERIORRINGN` |  | No GEOMETRY type. |
| `ST_INTERSECTION` |  | No GEOMETRY type. |
| `ST_INTERSECTS` |  | No GEOMETRY type. |
| `ST_ISCLOSED` |  | No GEOMETRY type. |
| `ST_ISEMPTY` |  | No GEOMETRY type. |
| `ST_ISRING` |  | No GEOMETRY type. |
| `ST_ISSIMPLE` |  | No GEOMETRY type. |
| `ST_LENGTH` |  | No GEOMETRY type. |
| `ST_NUMGEOMETRIES` |  | No GEOMETRY type. |
| `ST_NUMINTERIORRINGS` |  | No GEOMETRY type. |
| `ST_NUMPOINTS` |  | No GEOMETRY type. |
| `ST_OVERLAPS` |  | No GEOMETRY type. |
| `ST_POINTN` |  | No GEOMETRY type. |
| `ST_SETSRID` |  | No GEOMETRY type. |
| `ST_STARTPOINT` |  | No GEOMETRY type. |
| `ST_SYMDIFFERENCE` |  | No GEOMETRY type. |
| `ST_TOUCHES` |  | No GEOMETRY type. |
| `ST_TRANSFORM` |  | No GEOMETRY type. |
| `ST_UNION` |  | No GEOMETRY type. |
| `ST_WITHIN` |  | No GEOMETRY type. |
| `ST_X` |  | No GEOMETRY type. |
| `ST_Y` |  | No GEOMETRY type. |
| `SUB` | ✓ |  |
| `SUBSTR` | ✓ |  |
| `SYSDATE` |  | Needs the statement clock. |
| `SYSTIMESTAMP` |  | Needs the statement clock. |
| `SYS_GUID` |  | Exasol-only. |
| `TAN` | ✓ |  |
| `TANH` | ✓ |  |
| `TO_CHAR` |  | Exasol format models unsupported. |
| `TO_DATE` | ✓ | Some formats run in Exasol SQL. |
| `TO_DSINTERVAL` |  | Result type has no DataFusion mapping. |
| `TO_NUMBER` |  | No DataFusion equivalent. |
| `TO_TIMESTAMP` | ✓ | Some formats run in Exasol SQL. |
| `TO_YMINTERVAL` |  | Result type has no DataFusion mapping. |
| `TRANSLATE` | ✓ |  |
| `TRIM` | ✓ |  |
| `TRUNC` | ✓ |  |
| `TYPEOF` |  |  |
| `UNICODE` | ✓ |  |
| `UNICODECHR` | ✓ |  |
| `UPPER` | ✓ | Non-ASCII text can differ. |
| `WEEK` | ✓ |  |
| `WIDTH_BUCKET` |  |  |
| `YEAR` | ✓ |  |
| `YEARS_BETWEEN` |  | No DataFusion equivalent. |
| `ZEROIFNULL` | ✓ |  |

## Aggregate functions

| Aggregate function | Advertised | Notes |
|---|:---:|---|
| `APPROXIMATE_COUNT_DISTINCT` |  | No partial/merge plan. |
| `AVG` | ✓ | Partials per shard, merged in Exasol SQL. |
| `AVG_DISTINCT` |  |  |
| `CORR` |  |  |
| `COUNT` | ✓ |  |
| `COUNT_DISTINCT` | ✓ | Some forms run in Exasol SQL. |
| `COUNT_STAR` | ✓ |  |
| `COUNT_TUPLE` |  |  |
| `EVERY` |  |  |
| `FIRST_VALUE` |  |  |
| `GROUPING` |  |  |
| `GROUPING_ID` |  |  |
| `GROUP_CONCAT` |  | No partial/merge plan. |
| `GROUP_CONCAT_DISTINCT` |  | No partial/merge plan. |
| `GROUP_CONCAT_ORDER_BY` |  | No partial/merge plan. |
| `GROUP_CONCAT_SEPARATOR` |  | No partial/merge plan. |
| `LAST_VALUE` |  |  |
| `LISTAGG` |  | No partial/merge plan. |
| `LISTAGG_DISTINCT` |  | No partial/merge plan. |
| `LISTAGG_ON_OVERFLOW_ERROR` |  | No partial/merge plan. |
| `LISTAGG_ON_OVERFLOW_TRUNCATE` |  | No partial/merge plan. |
| `LISTAGG_ORDER_BY` |  | No partial/merge plan. |
| `LISTAGG_SEPARATOR` |  | No partial/merge plan. |
| `MAX` | ✓ |  |
| `MEDIAN` |  | No partial/merge plan. |
| `MIN` | ✓ |  |
| `MUL` |  |  |
| `MUL_DISTINCT` |  |  |
| `SOME` |  |  |
| `STDDEV` | ✓ | Double precision. |
| `STDDEV_DISTINCT` |  |  |
| `STDDEV_POP` | ✓ | Double precision. |
| `STDDEV_POP_DISTINCT` |  |  |
| `STDDEV_SAMP` | ✓ | Double precision. |
| `STDDEV_SAMP_DISTINCT` |  |  |
| `ST_INTERSECTION` |  | No GEOMETRY type. |
| `ST_UNION` |  | No GEOMETRY type. |
| `SUM` | ✓ |  |
| `SUM_DISTINCT` |  |  |
| `VARIANCE` | ✓ | Double precision. |
| `VARIANCE_DISTINCT` |  |  |
| `VAR_POP` | ✓ | Double precision. |
| `VAR_POP_DISTINCT` |  |  |
| `VAR_SAMP` | ✓ | Double precision. |
| `VAR_SAMP_DISTINCT` |  |  |
