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
