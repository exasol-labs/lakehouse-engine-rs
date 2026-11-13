[lakehouse-engine](../README.md) › [Docs](index.md) › Semantic differences

---

# Semantic differences from native Exasol

Some pushed-down queries return different rows or values than the same query on a native
Exasol table. Each difference on this page is a current compatibility gap that realistic data
can hit. [Capabilities](capabilities.md) says which forms of each function run in the scan.

## Case conversion of non-ASCII text

`UPPER`, `LOWER`, and `INITCAP` can return different text for non-ASCII characters. ASCII text
is not affected.

The difference comes from two case-mapping rules:

- Native Exasol maps each character to exactly one character. A character without a single-character mapping stays unchanged.
- The scan runs these functions in DataFusion, which applies Unicode full case mapping.

| Expression | Virtual schema | Native Exasol |
|---|---|---|
| `UPPER('straße')` | `STRASSE` | `STRAßE` |
| `LOWER('İ')` (Turkish dotted I) | `i̇` (two characters) | `i` |
| `INITCAP('ßa')` | `SSa` | `ßa` |

The difference also reaches results built on the converted text:

- `LENGTH(UPPER('ß'))` returns 2 instead of 1.
- A filter such as `WHERE UPPER(name) = 'STRAßE'` can return different rows.

## Dates outside Exasol's DATE range

Iceberg, Delta, and Parquet can store dates outside Exasol's DATE range (0001-01-01 to
9999-12-31).

A query fails if the scan would hand such a date to Exasol. The error names the column and the
stored day count:

```
DATE value out of range in output column STORED_DATE: 100000000 days from 1970-01-01 lies outside Exasol's DATE range 0001-01-01 to 9999-12-31
```

For `MIN`, `MAX`, and `GROUP BY` the column name is the scan's internal name (`PARTIAL_max_0`,
`PARTIAL_min_0`, `GK_0`), not the column in your query.

**Workaround:** evaluate the date in the query, so the scan returns a value Exasol can hold and
every row is kept. Either replace out-of-range dates with `NULL` (or any value you choose):

```sql
SELECT ID,
       CASE WHEN STORED_DATE > DATE '9999-12-31' OR STORED_DATE < DATE '0001-01-01'
            THEN NULL ELSE STORED_DATE END AS STORED_DATE
FROM t;
```

or return the date as a day count, which has no range limit:

```sql
SELECT ID, DAYS_BETWEEN(STORED_DATE, DATE '1970-01-01') AS STORED_DAYS FROM t;
```

## Precision of STDDEV and VARIANCE

For DECIMAL and integer columns, native Exasol computes `STDDEV`, `VARIANCE`, `VAR_POP`,
`VAR_SAMP`, `STDDEV_POP`, and `STDDEV_SAMP` exactly. The virtual schema computes them in double
precision, so its results can be less precise. The difference grows when the values are large
and close to each other.

## TO_DATE and TO_TIMESTAMP with a format

For a few input strings the scan fails where native Exasol parses the value:

| Expression | Virtual schema | Native Exasol |
|---|---|---|
| `TO_DATE('2024-01-01 ', 'YYYY-MM-DD')` (trailing space) | error | `2024-01-01` |
| `TO_TIMESTAMP('2024-01-01', 'YYYY-MM-DD HH24:MI:SS')` (input shorter than the format) | error | `2024-01-01 00:00:00` |
| `TO_TIMESTAMP('1600-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')` | error | `1600-01-01 00:00:00` |

## CAST of text to a shorter VARCHAR

The scan computes a cast of text to a `VARCHAR(n)` shorter than its source, or to an `ASCII` target, without Exasol's checks. Native Exasol fails on a value that does not fit, except that it cuts trailing spaces beyond `n` (`'ab   '` cast to `VARCHAR(3)` is `'ab '`). The scan keeps the whole value.

| Expression, over the values `'ab'`, `'ab   '`, `'cd'` | Virtual schema | Native Exasol |
|---|---|---|
| `WHERE CAST(val AS VARCHAR(3)) = 'ab '` | no rows | 1 row |
| `SELECT CAST(val AS VARCHAR(3))` | right-truncation error | `'ab '` |
| `WHERE CAST(val \|\| 'abcdefghij' AS VARCHAR(5)) IS NOT NULL` | returns rows | right-truncation error |
| `WHERE CAST(val \|\| 'é' AS VARCHAR(10) ASCII) IS NOT NULL` | returns rows | error |

## ADD_DAYS, ADD_WEEKS, ADD_HOURS, and ADD_MINUTES before 1582-10-15

The scan adds the count in the proleptic Gregorian calendar. Native Exasol uses the Julian calendar before 1582-10-15. A call returns a different value when the days between its argument and its result include February 29 of a year divisible by 100 but not by 400, which only the Julian calendar has, or cross 1582-10-15. A call whose arguments are all literals runs in Exasol, because Exasol computes it before pushdown.

| Call | Native Exasol (2025.1.16) | Scan |
|---|---|---|
| `ADD_DAYS(d, 1)` with `d` = 1000-02-28 | `1000-02-29` | `1000-03-01` |
| `ADD_DAYS(d, -1)` with `d` = 1500-03-01 | `1500-02-29` | `1500-02-28` |
| `ADD_HOURS(d, 24)` with `d` = 1500-02-28 | `1500-02-29 00:00:00` | `1500-03-01 00:00:00` |
| `ADD_DAYS(d, -1)` with `d` = 1582-10-15 | `1582-10-04` | `1582-10-14` |
| `ADD_DAYS(d, -200000)` with `d` = 2024-01-01 | `1476-05-24` | `1476-06-02` |

## JSON_VALUE without a DEFAULT ON ERROR

The scan reads a document only until no `JSON_VALUE` path over it can select another item, and it takes only the first member of each name in an object. Native Exasol reads and checks the whole document, and reads repeated members as several items. So a document that is broken after the selected value, or that repeats a member the path selects, can give a value where Exasol takes the `ON ERROR` behavior, or the reverse.

A `DEFAULT ... ON ERROR` clause on any `JSON_VALUE` call over the document, anywhere in the query, makes the scan read and check the whole document for every call over it, in the select list, the filter, the grouping, and the aggregates alike. The results then match native Exasol. Exasol sends `NULL ON ERROR` also when the clause is absent, and it sends `DEFAULT NULL ON ERROR` and `DEFAULT '' ON ERROR` as `NULL ON ERROR`, so none of these three asks for the whole read.

| Expression | Virtual schema | Native Exasol |
|---|---|---|
| `JSON_VALUE('{"a":1} x', '$.a')` | `'1'` | `NULL` (`ON ERROR`) |
| `JSON_VALUE('{"a":1,"a":2}', '$.a')` | `'1'` | `NULL` (`ON ERROR`) |
| `JSON_VALUE('{"a":{"c":2},"a":{"b":1}}', '$.a.b')` | `NULL` (`ON EMPTY`) | `'1'` |
| `JSON_VALUE('nul', '$.a' DEFAULT 'E' ON EMPTY)` | `'E'` (`ON EMPTY`) | `NULL` (`ON ERROR`) |

## ORDER BY with LIMIT under `EXCLUDED_CAPABILITIES`

When `EXCLUDED_CAPABILITIES` lists `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised, `ORDER BY ... LIMIT n OFFSET m` returns the first n rows and ignores the offset. `ORDER BY id LIMIT 5 OFFSET 3` over the ids 1 to 20 returns 1 to 5 instead of 4 to 8 (observed on Exasol 2026.2.0-nano.3). Advertising all three capabilities, or none, returns 4 to 8. Exasol documents ORDER BY as a known limit of virtual schemas, see the [virtual schema limitations](https://docs.exasol.com/db/latest/database_concepts/virtual_schema/user_guide.htm#Virtualschemalimitations).

**Workaround:** exclude `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION` together with `LIMIT` or `LIMIT_WITH_OFFSET`, so Exasol sorts and limits the rows itself:

```sql
ALTER VIRTUAL SCHEMA my_vs SET EXCLUDED_CAPABILITIES = 'LIMIT, LIMIT_WITH_OFFSET, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION';
```
