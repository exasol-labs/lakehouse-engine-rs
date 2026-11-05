[lakehouse-engine](../README.md) › [Docs](index.md) › Semantic differences

---

# Semantic differences from native Exasol

Some pushed-down queries return a different result than the same query on a native Exasol
table. Each difference on this page is a current compatibility gap. The
[Capabilities](capabilities.md) page lists which functions are pushed down.

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

## AVG of very large values

`AVG` returns an average where native Exasol fails with `data exception - numeric value out of
range`. This happens only when the sum of the averaged values exceeds about 38 digits, counting
the digits after the decimal point.

## Addition and subtraction with different scales

An expression that adds or subtracts DECIMAL values with different scales can return rows where
native Exasol fails with `data exception - numeric value out of range`. Before it adds them,
native Exasol converts the value with the smaller scale to `DECIMAL(36, s)`, where `s` is the
larger scale, and fails when that value has too many integer digits for that type. The virtual
schema converts the value to a 128-bit intermediate, which has room for it.

For example, `WHERE W + 0.0000001 > W` with `W` a `DECIMAL(36,6)` value of
`999999999999999999999999999999.999999` fails natively, because the 30 integer digits of `W` do
not fit `DECIMAL(36,7)`. The virtual schema returns the row. `WHERE H * H + 0.5 > 0` with `H` a
BIGINT of `4000000000000000000` differs in the same way. A projected sum that exceeds 36 digits
still fails in the virtual schema too.

## TO_DATE and TO_TIMESTAMP with a format

The scan runs `TO_DATE` and `TO_TIMESTAMP` with a string-literal format built from `YYYY`, `MM`, and
`DD` once each, in any letter case. `TO_TIMESTAMP` also accepts `HH24` and `MI`, then optionally
`SS`, then optionally one `FF3`, `FF6`, or `FF9`. The separators are `-` `/` `.` `,` `;` `:` `_`
and space. Other formats run in Exasol.

For a few input strings the scan and native Exasol parse differently:

| Expression | Virtual schema | Native Exasol |
|---|---|---|
| `TO_DATE('2024-01-01 ', 'YYYY-MM-DD')` (trailing space) | error | `2024-01-01` |
| `TO_TIMESTAMP('2024-01-01', 'YYYY-MM-DD HH24:MI:SS')` (input shorter than the format) | error | `2024-01-01 00:00:00` |
| `TO_TIMESTAMP('1600-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')` | error | `1600-01-01 00:00:00` |

The scan also accepts some strings Exasol rejects, such as a sign before the year, seconds 60, or a tab inside the value. Those queries return a result where Exasol raises an error.

## CAST to text

A cast to text that the scan computes can return a value or a row where native Exasol returns
another value or raises an error. The fix is planned (#587). The differences come from two rules:

- The scan applies no length check, no blank padding, and no ASCII check for a `CHAR(n)` target,
  for a `VARCHAR(n)` target shorter than 2,000,000, or for an ASCII target. This holds for a cast
  in a filter and for a cast in a select-list item.
- The scan writes a DATE, TIMESTAMP, BOOLEAN, DOUBLE, or DECIMAL with a scale above 0 in
  DataFusion's fixed text form, whatever the session's NLS settings say: ISO dates, a `T` between
  date and time, a `.` as decimal point, and lowercase booleans.

| Expression | Session setting | Virtual schema | Native Exasol |
|---|---|---|---|
| `WHERE CAST(name \|\| 'abcdefghijkl' AS VARCHAR(5)) IS NOT NULL` | default | returns rows | right-truncation error |
| `WHERE CAST(name \|\| 'é' AS VARCHAR(10) ASCII) IS NOT NULL` | default | returns rows | error |
| `CAST(ts AS VARCHAR(40))` | default | `2024-01-01T00:00:00.100` | `2024-01-01 00:00:00.100000` |
| `WHERE CAST(flag AS VARCHAR(2000000)) = 'TRUE'` | default | returns no rows | returns rows |
| `CAST(d AS VARCHAR(10))` | `NLS_DATE_FORMAT='DD.MM.YYYY'` | `2024-01-01` | `01.01.2024` |

**Workaround:** filter a date with a date range instead of a text comparison. The range pushes
down and prunes files:

```sql
WHERE d >= DATE '2024-01-01' AND d < DATE '2025-01-01'
```
