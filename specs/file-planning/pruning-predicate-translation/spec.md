# Feature: Pruning Predicate Translation

The Iceberg, Delta, and Hive partition pruning paths translate the same filter shapes and convert each literal by one exactness rule, so they prune alike.

## Background

* Each path translates a comparison of a bare column with a literal (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, and `LIKE`, combined by `AND`, `OR`, and `NOT`. A function over the column, or a column compared with a column, constrains nothing.
* Exasol folds a constant `TO_DATE`, `TO_TIMESTAMP`, `CAST`, or `||` into a literal before it sends the pushdown request, and sends `''` as `literal_null`. A filter on an Iceberg `timestamptz` column, which maps to `TIMESTAMP`, carries a `literal_timestamp`. Exasol sends `literal_timestamputc` only for a `TIMESTAMP WITH LOCAL TIME ZONE` value, already shifted to UTC.
* Exasol compares each `BETWEEN` bound in its own kind, as `col >= low AND col <= high`, so a bound that does not convert constrains nothing and the other bound still prunes.
* Exasol sends `escapeChar` only for a `LIKE` that says `ESCAPE`. A plain `LIKE` escapes with the session's `DEFAULT_LIKE_ESCAPE_CHARACTER`, `\` unless changed, which the pushdown request does not carry.

## Scenarios

### Scenario: A numeric literal converts only when the column's type holds it exactly

* *GIVEN* columns of integer, decimal, float, and double type
* *WHEN* a pruning path converts a filter literal for one of them
* *THEN* an integer column SHALL take an exact numeral with no nonzero fraction digit that fits the column's range
* *AND* a decimal column SHALL take an exact numeral whose nonzero fraction digits fit its scale and whose value fits its precision, built at the column's own precision and scale
* *AND* a 32-bit float column SHALL take a numeric literal only when 32 bits hold its 64-bit value exactly, and a double column SHALL take it as a 64-bit value
* *AND* a `literal_double` SHALL convert only for a float or double column, because Exasol compares it with any other numeric column as a DOUBLE

### Scenario: A text or time literal converts only for a column of its own kind

* *GIVEN* columns of string, date, timestamp, UTC-adjusted timestamp, and nanosecond timestamp type
* *WHEN* a pruning path converts a filter literal for one of them
* *THEN* a timestamp column SHALL take a `literal_timestamp` whose significant fraction digits its unit holds, read as UTC on a UTC-adjusted column
* *AND* a string or date column SHALL take a non-empty literal of its own kind
* *AND* any other literal, including `literal_timestamputc`, SHALL constrain nothing for its node

### Scenario: A LIKE pattern prunes by its literal prefix

* *GIVEN* a string column and a `LIKE` pattern, with or without `ESCAPE`
* *WHEN* a pruning path translates it
* *THEN* a pattern without `ESCAPE` SHALL read `\` as its escape character, and an escaped character SHALL be that literal character
* *AND* a pattern with no unescaped wildcard SHALL translate as an equality, and a pattern that starts with a wildcard SHALL constrain nothing
* *AND* Iceberg and Delta SHALL prune a file only when its bounds admit no value that starts with the literal prefix, a translation exact only when nothing but `%` follows the prefix, so `NOT` negates only such a pattern
* *AND* the Hive partition predicate SHALL evaluate the pattern exactly against each value of a string partition column

### Scenario: A not-equal comparison prunes

* *GIVEN* a `<>` comparison of a column with a literal
* *WHEN* a pruning path translates it
* *THEN* Iceberg and Delta SHALL translate it as an exact not-equal predicate, and the Hive partition predicate SHALL evaluate it

### Scenario: Every pruning path keeps every file with a matching row for every column type

* *GIVEN* the pruning fixture's Iceberg, Delta, Hive direct-storage, and Unity Parquet copies, with a `decimal(4,1)` partition column and statistics columns of decimal, float, double, date, and UTC-adjusted timestamp type
* *WHEN* queries apply `=`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`, `<>`, and `LIKE` to them, including literals the column's type cannot hold exactly and constant functions Exasol folds
* *THEN* each query SHALL return the native oracle's rows, and each format SHALL scan exactly the files its partition and statistics evaluation keeps
* *AND* on an Exasol version with nanosecond timestamps, an Iceberg `timestamp_ns` column SHALL prune by a literal with nine fraction digits, and a microsecond column SHALL keep its file for a literal finer than its unit
