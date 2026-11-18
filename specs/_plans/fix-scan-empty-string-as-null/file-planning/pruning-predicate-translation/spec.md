# Feature: Pruning Predicate Translation

The Iceberg, Delta, and Hive partition pruning paths translate the same filter shapes and convert each literal by one exactness rule, so they prune alike.

## Background

* Each path translates a comparison of a bare column with a literal (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, and `LIKE`, combined by `AND`, `OR`, and `NOT`. A function over the column, or a column compared with a column, constrains nothing.
* Exasol folds a constant `TO_DATE`, `TO_TIMESTAMP`, `CAST`, or `||` into a literal before it sends the pushdown request, and sends `''` as `literal_null`. A filter on an Iceberg `timestamptz` column, which maps to `TIMESTAMP`, carries a `literal_timestamp`. Exasol sends `literal_timestamputc` only for a `TIMESTAMP WITH LOCAL TIME ZONE` value, already shifted to UTC.
* Exasol compares each `BETWEEN` bound in its own kind, as `col >= low AND col <= high`, so a bound that does not convert constrains nothing and the other bound still prunes.
* Exasol sends `escapeChar` only for a `LIKE` that says `ESCAPE`. A plain `LIKE` escapes with the session's `DEFAULT_LIKE_ESCAPE_CHARACTER`, `\` unless changed, which the pushdown request does not carry.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A null test on a string column keeps the files that hold empty strings

* *GIVEN* a table with two data files, one whose string column holds `''` and no NULL, and one whose string column holds neither
* *WHEN* a pruning path translates `col IS NULL` or `col IS NOT NULL` over that column
* *THEN* `col IS NULL` SHALL translate as the column is NULL or equals `''`, so the file that holds `''` is kept
* *AND* `col IS NOT NULL` SHALL translate as the column is not NULL and differs from `''`, so `NOT (col IS NOT NULL)` keeps that file too
* *AND* `SELECT COUNT(*) FROM t WHERE col IS NULL` SHALL count the rows that store `''`
* *AND* a null test on a column of any other type SHALL translate as before
<!-- /DELTA:NEW -->
