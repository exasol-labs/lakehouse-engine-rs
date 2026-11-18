# Feature: Pushdown Planning, Scan Empty String

The scan reads an empty string as NULL wherever it computes with it, so a pushed-down query returns the rows and values native Exasol returns.

## Background

* Exasol has no empty string: `'' IS NULL` is TRUE, and Exasol sends a `''` literal as `literal_null`. DataFusion keeps `''` as a value that is not NULL.
* An empty string reaches the scan from two sources: a string column of a data file that stores one, and a text function that returns one, such as `RTRIM(c, c)`.
* An empty string that the scan emits arrives in Exasol as NULL, so a bare projected column needs no change.
* The rule holds for every expression the scan computes, as listed in `pushdown-types/pushdown-planning-scan-text-conversion`.
* A binary column read as hex text follows `vs-adapter/binary-column-rendering`.

## Scenarios

### Scenario: A text function that returns an empty string returns NULL

* *GIVEN* a virtual table with a string column `s` that holds `event-01` in the row with `id` 1
* *WHEN* a query computes a text function in the scan whose result is empty, such as `RTRIM(s, s)`
* *THEN* the pushed scan SQL SHALL wrap the function call as `nullif(<call>, '')`
* *AND* `SELECT COUNT(*) FROM t WHERE RTRIM(s, s) IS NULL` SHALL count every row of `t`, and the same filter with `LTRIM` SHALL too
* *AND* `SELECT COUNT(*) FROM t WHERE LENGTH(RTRIM(s, s)) = 0` SHALL return 0
* *AND* `SELECT LENGTH(RTRIM(s, s)) FROM t WHERE id = 1` SHALL return NULL
* *AND* `SELECT NVL(RTRIM(s, s), 'EMPTY') FROM t WHERE id = 1` SHALL return `EMPTY`

### Scenario: A stored empty string reads as NULL in every computed expression

* *GIVEN* a virtual table whose string column `s` stores `''` in some rows, NULL in others, and non-empty text in the rest
* *WHEN* a query computes with `s` in the scan
* *THEN* the pushed scan SQL SHALL read `s` as `nullif("S", '')` inside a function argument, an aggregate argument, and a `CASE`
* *AND* `LENGTH(s)`, `NVL(s, 'E')`, and `s IS NULL` SHALL return for a row that stores `''` what they return for a row that stores NULL
* *AND* `COUNT(s)` SHALL NOT count a row that stores `''`, and `MIN(s)` SHALL return the smallest non-empty value
* *AND* `GROUP BY s` SHALL return one NULL group that holds the rows storing `''` and the rows storing NULL
* *AND* a bare `s` in the select list SHALL stay a bare column in the pushed scan SQL

### Scenario: A filter over a bare string column keeps the bare column

* *GIVEN* the table of the preceding scenario and a WHERE filter that compares the bare column `s` with string literals, reached from the filter's root through `AND` and `OR` only
* *WHEN* the adapter renders the filter for the scan
* *THEN* `s = 'x'`, `s > 'x'`, `s >= 'x'`, `s IN ('x', 'y')`, and `s BETWEEN 'a' AND 'b'` SHALL render over the bare column with no added condition, because none is TRUE for an empty string
* *AND* `s <> 'x'`, `s < 'x'`, `s <= 'x'`, and `s LIKE 'x%'` SHALL render over the bare column joined by `AND` with `"S" <> ''`
* *AND* each filter SHALL return the rows the native oracle returns, so `s <> 'x'` and `s < 'x'` SHALL NOT return a row that stores `''`
* *AND* the same comparison under a `NOT`, in a `CASE` condition, or in the select list SHALL read the column as `nullif("S", '')`, so `NOT (s = 'x')` SHALL NOT return a row that stores `''`
* *AND* `s IS NULL` SHALL render as `("S" IS NULL OR "S" = '')` and `s IS NOT NULL` as `("S" IS NOT NULL AND "S" <> '')` at any position, also under a `NOT`, and SHALL return the native oracle's rows

### Scenario: A guarded string filter still skips row groups

* *GIVEN* a Parquet file with several row groups whose statistics for a string column exclude the literal in all but one row group
* *WHEN* the scan runs a filter the preceding scenario renders over the bare column, with or without the `<> ''` condition, or a null test over a bare string column
* *THEN* the scan SHALL read only the row groups whose statistics admit a matching row

### Scenario: An equality of two string columns never matches empty strings

* *GIVEN* two virtual tables whose string key columns both store `''` in some rows
* *WHEN* a broadcast-eligible inner equi-join on those keys, or a WHERE filter `a = b` over two string columns of one table, is pushed down
* *THEN* the scan SHALL compare the bare columns and add that one of them is `<> ''`
* *AND* the join SHALL stay on the broadcast path
* *AND* the result SHALL hold no row whose key is an empty string, as on native Exasol
* *AND* any other comparison of two string columns SHALL read each as `nullif(<column>, '')`

### Scenario: REPLACE reads a NULL argument as the empty string

* *GIVEN* a virtual table with a string column `s` that holds `TRUCK` in some rows
* *WHEN* a pushed-down query calls `REPLACE(s, 'U')`, `REPLACE(s, 'U', '')`, or `REPLACE(s, 'U', NULL)`, which Exasol all sends with a NULL third argument
* *THEN* the scan SHALL compute the call, and it SHALL return `TRCK` for `TRUCK`
* *AND* `SELECT COUNT(*) FROM t WHERE REPLACE(s, 'U') = 'TRCK'` SHALL count the `TRUCK` rows
* *AND* a search argument that is NULL in a row SHALL return `s` unchanged for that row
* *AND* a call that removes every character of `s` SHALL return NULL

### Scenario: A text function with a NULL argument returns what native Exasol returns

* *GIVEN* a virtual table and a native table holding the same rows, with string columns that are NULL or `''` in some rows
* *WHEN* a pushed-down query calls `TRIM`, `LTRIM`, `RTRIM`, `TRANSLATE`, `LPAD`, `RPAD`, `INSTR`, `LOCATE`, `SUBSTR`, `LEFT`, `RIGHT`, `REPEAT`, `REVERSE`, `UPPER`, `LOWER`, `INITCAP`, or `LENGTH` with a NULL in any one argument, as a literal or as a column value
* *THEN* the query SHALL return the native table's result for every row
* *AND* a call the scan cannot compute with that result SHALL run in Exasol through the path's fallback of `pushdown-types/pushdown-planning-scan-text-conversion`
