# Feature: Partition Predicate Over Declared Types

Prunes the files or partitions of a Parquet table from a pushed filter by comparing partition values under each partition column's declared type. One predicate serves direct storage, Unity Parquet, and Glue Parquet. Direct storage declares every partition column a string, so its pruning compares strings.

## Background

* The predicate's node set, its three-valued evaluation, and its uppercase-fold column resolution are those of `vs-adapter/direct-storage-hive-partitioning`. This feature adds the comparison type.
* The scan converts a partition value to its declared type with DataFusion's string-to-scalar conversion (`datafusion-scan/scan-execution-partition-values`). The predicate uses the same conversion, so the predicate and the scan agree on every value.

## Scenarios

### Scenario: A partition value compares under its column's declared type

* *GIVEN* partition columns `year` (int32), `day` (date32), `amount` (decimal128(10,2)), `ts` (timestamp, microseconds), and `region` (utf8), and files whose `year` values are `9` and `10`
* *WHEN* the predicate evaluates `year < 10`, `day = DATE '2024-03-01'`, `amount >= 1.50`, `ts < TIMESTAMP '2024-03-01 00:00:00'`, and `region = 'eu'`
* *THEN* the predicate SHALL convert the partition value and the literal to the column's declared type and SHALL compare them in that type's order
* *AND* `year < 10` SHALL keep the `9` file and prune the `10` file, which a string comparison would get wrong
* *AND* the literal set SHALL widen with the declared type: an integer or decimal column compares against an exact-numeric literal, a date column against a date literal, a timestamp column against a timestamp literal, and a boolean column against a boolean literal
* *AND* a utf8 column SHALL compare in codepoint order against a non-empty string literal only, per `vs-adapter/direct-storage-hive-partitioning`

### Scenario: A comparison the declared type cannot decide exactly keeps the file

* *GIVEN* the same partition columns, a float partition column, and a file whose `year` value is `abc`
* *WHEN* the predicate evaluates `year = '2024'`, `year = 2024.5`, `amount = 1.555`, and a comparison on the float column
* *THEN* each of these comparisons SHALL count as possibly TRUE, FALSE, or NULL, so it prunes no file
* *AND* a literal of another type than the column's, or a literal the declared type cannot represent exactly, SHALL keep every file rather than be converted by rounding
* *AND* only string, integer, decimal, date, timestamp, and boolean partition columns SHALL compare, so any other declared type keeps every file
* *AND* the file whose value cannot convert SHALL be kept, so the scan's conversion fails the query exactly as it does without pruning

### Scenario: Direct storage passes every partition column to the one predicate as a string

* *GIVEN* a direct-storage table whose folder names carry `year=2024` and `year=2025`
* *WHEN* the pushdown prunes it for `year = 2024`
* *THEN* direct storage SHALL declare `year` a string, so the numeric literal prunes no file
* *AND* the direct-storage, Unity Parquet, and Glue Parquet readers SHALL each call the ONE predicate, and none of them SHALL keep its own partition-value comparison
