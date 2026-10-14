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

Iceberg, Delta, and Parquet can store dates outside Exasol's DATE range (0001-01-01 to
9999-12-31), for example year 275760. A native Exasol table cannot hold such a date.

A query fails if the scan would hand such a date to Exasol, whether in a plain column, a `MIN` or
`MAX` result, a `GROUP BY` key, or `COUNT(DISTINCT)`. The error names the column and the stored day
count:

```
DATE value out of range in output column STORED_DATE: 100000000 days from 1970-01-01 lies outside Exasol's DATE range 0001-01-01 to 9999-12-31
```

For `MIN`, `MAX`, and `GROUP BY` the column name is the scan's internal name (`PARTIAL_max_0`,
`PARTIAL_min_0`, `GK_0`), not the column in your query.

**Workaround:** filter the bad rows out in the query, because the scan applies the filter before it
hands rows to Exasol:

```sql
SELECT ID, STORED_DATE FROM t
WHERE STORED_DATE BETWEEN DATE '0001-01-01' AND DATE '9999-12-31';
```

To find the offending rows, select a column other than the date:

```sql
SELECT ID FROM t WHERE STORED_DATE > DATE '9999-12-31' OR STORED_DATE < DATE '0001-01-01';
```
