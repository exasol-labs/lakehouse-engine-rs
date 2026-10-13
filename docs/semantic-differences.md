[lakehouse-engine](../README.md) › [Docs](index.md) › Semantic differences

---

# Semantic differences from native Exasol

A few pushed-down functions, and a filter on a date outside Exasol's DATE range, return a
different result than the same query on a native Exasol table. Each difference on this page is a
current compatibility gap. The [Capabilities](capabilities.md) page lists which functions are
pushed down.

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

Iceberg, Delta, and Parquet store a date as a 32-bit day count from 1970-01-01, so a table can hold
a date outside Exasol's DATE range of 0001-01-01 to 9999-12-31. A native Exasol table cannot hold
such a date. A query fails when the scan passes such a date to Exasol. The error names the output
column and the stored day count, for example:

```
DATE value out of range in output column STORED_DATE: 100000000 days from 1970-01-01 lies outside Exasol's DATE range 0001-01-01 to 9999-12-31
```

This covers a plain column, a `MIN` or `MAX` result, and a `GROUP BY` key. On a `MIN`, `MAX`, or
`GROUP BY` the column name is the partial output column the scan declares (`PARTIAL_max_0`,
`PARTIAL_min_0`, `GK_0`), not the column in the query. Dates inside the range, including 0001-01-01
and 9999-12-31, are returned unchanged.

A query fails even when it does not return the value, if the scan passes the value to Exasol:
`SELECT COUNT(DISTINCT STORED_DATE) FROM t` fails, because each shard passes its distinct dates to
Exasol to count. A query that evaluates the value inside the scan does not fail. In the following
results, row 4 of the test table holds 100000000 days (year 275760) and row 5 holds 2932897 days (10000-01-01):

| Query | Virtual schema result |
|---|---|
| `SELECT ID FROM t WHERE STORED_DATE > DATE '9999-12-31' ORDER BY ID` | Rows 4 and 5 |
| `SELECT ID, YEAR(STORED_DATE) FROM t WHERE ID = 4` | `NULL` for the year |
| `SELECT COUNT(*) FROM t WHERE STORED_DATE > DATE '9999-12-31'` | 2 |

Native Exasol cannot evaluate these queries, because it cannot hold the stored value.
