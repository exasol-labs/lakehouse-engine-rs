# Feature: Direct-Storage Statistics Pruning

Drops a direct-storage file from the plan-time file list when its Parquet footer statistics prove
that no row group in it can satisfy the pushed filter. The footers come from the fold the planner
already ran, so pruning costs no object-storage request. A dropped file never enters the shard
assignment and never costs a UDF invocation. Pruning is sound, not complete: when a statistic
cannot decide a node, the file is kept, and the full filter still applies to the scanned rows.
Pruning reads only the part of the filter that the scan itself evaluates.

Float and literal handling, the live-pinned scan behaviors, the composition with the file-list consumers, and the filter scope are specified by `vs-adapter/direct-storage-statistics-pruning-float-and-scope`.

## Background

* Parquet is the normative source for this feature. The quotes below come from
  `apache/parquet-format` `src/main/thrift/parquet.thrift` (master, read 2026-10-02):
  * Deprecated `Statistics.min`/`max`: "These fields encode min and max values determined by
    signed comparison only."
  * `FileMetaData.column_orders`: "Without column_orders, the meaning of the min_value and
    max_value fields in the Statistics object and the ColumnIndex object is undefined."
  * `ColumnOrder`: "If the reader does not support the value of this union, min and max stats for
    this column should be ignored." For INT96: "If TYPE_ORDER is used for an INT96 column, readers
    should ignore all statistics". The `TYPE_ORDER` sort orders for logical types state
    "INTERVAL - undefined".
  * `Statistics.null_count`: "If null_count is not present, readers MUST NOT assume
    null_count == 0."
  * `Statistics.min_value`/`max_value`: they "can also be (more compact) values that do not exist
    on a page or column chunk ... Such more compact values must still be valid values within the
    column's logical type." A truncated string bound therefore stays a valid bound.
  * `Statistics.nan_count`: "If this field is not present, readers MUST assume NaNs may be present
    (i.e. MUST assume nan_count > 0 and MAY NOT assume nan_count == 0)."
  * `TYPE_ORDER` read rules for FLOAT and DOUBLE: "If the min is a NaN, it should be ignored. If
    the max is a NaN, it should be ignored." and "If the min is +0, the row group may contain -0
    values as well. If the max is -0, the row group may contain +0 values as well."
* The scan compares a column in the type its logical field declares. A column whose logical field
  carries the string tag `utf8`, such as a nested column or a DATE64, `Decimal256`, or FLOAT16
  column, compares as text, whatever its folded footer type (`vs-adapter/parquet-directory-seam`,
  `datafusion-scan/type-mapping`). A `Decimal128` column of precision 37 or 38 is the exception:
  the engine declares it `VARCHAR(2000000)`, while its logical field keeps its decimal tag, so the
  scan compares it as a decimal. It stays sound because a string literal does not convert against a
  decimal column, and a numeral converts only under the decimal literal rule below.
* Apache Iceberg and Delta specification check: NOT implicated for this reader, because direct
  storage implements neither format (`vs-adapter/direct-storage-table-planning`). The Delta
  protocol's § Per-file Statistics defines `minValues` as "A value that is equal to the smallest
  valid value present in the file for this column" and states no NaN rule.

## Scenarios

### Scenario: A footer whose row-group bounds exclude the filter drops the file

* *GIVEN* a direct-storage table `prune_types/` under `MERGE_SCHEMA` TRUE whose file under the Hive directory `grp=a/` holds `ID` values 1 to 8 and whose file under `grp=b/` holds `ID` values 9 to 16, each file written in two row groups of four rows with statistics on `ID`
* *WHEN* the adapter plans `SELECT ID FROM PRUNE_TYPES WHERE ID <= 5`
* *THEN* the resolved file list SHALL hold the `grp=a/` file only, because the minimum of every row group in the `grp=b/` file exceeds 5
* *AND* the returned rows SHALL be IDs 1 to 5, equal to the rows of the same query without statistics pruning, because the full filter still applies to the scanned rows
* *AND* the reader SHALL evaluate the parsed footers the directory seam returned, and MUST NOT read a footer or issue any object-storage request of its own
* *AND* `ScanSpec`, `FileEntry`, and `LogicalField` MUST NOT gain a field, so no statistic reaches the wire
* *AND* a request carrying no filter SHALL keep every file partition pruning keeps

### Scenario: Row-group statistics evaluate the filter under three-valued logic

* *GIVEN* a kept file whose footer the seam returned, a pushed filter, and the folded Arrow schema of the table
* *WHEN* the reader evaluates the filter on each row group of that file
* *THEN* the reader SHALL keep the file if and only if at least one row group can evaluate the filter TRUE, SHALL drop a file holding zero row groups, and SHALL count a row group holding zero rows as unable to evaluate TRUE
* *AND* the evaluation SHALL reuse the partition predicate's node set, its uppercase-fold column resolution, and its three-valued logic (`vs-adapter/direct-storage-hive-partitioning`), so a node it cannot translate counts as possibly TRUE, FALSE, or NULL
* *AND* a comparison of a column against a literal SHALL read the row group's bounds `[min, max]` as a value range: an ordering `less` SHALL be reachable if and only if `min < literal`, `equal` if and only if `min <= literal <= max`, and `greater` if and only if `max > literal`, and the comparison SHALL count as possibly TRUE or possibly FALSE for each reachable ordering it holds or fails under
* *AND* a partition value SHALL count as the one-value range `[value, value]` on the same evaluation path, so a filter mixing a partition column and a data column under `OR` prunes on both
* *AND* a row group whose null count is present and equals its row count SHALL count as holding only NULL in that column, and a row group whose null count is present SHALL decide `IS NULL` and `IS NOT NULL` from that count and its row count
* *AND* a literal on a column other than FLOAT or DOUBLE SHALL convert to the column's type only exactly, under the conversion rules of `vs-adapter/partition-predicate-declared-types`, a literal on a FLOAT or DOUBLE column SHALL convert by the literal rules of the scenario "A float bound or literal is widened or rejected so it never drops a matching row", and a literal that does not convert SHALL leave its node untranslatable
* *AND* a literal SHALL convert only to the value the scan itself compares against: against a decimal column only an exact-numeric integer numeral, with no `.` and no exponent, that DataFusion parses as a signed or unsigned 64-bit integer SHALL convert, because the scan parses every other numeral as a 64-bit float, and a timestamp literal SHALL convert only when its fraction holds no non-zero digit past the sixth, because the scan renders a timestamp literal at microsecond precision
* *AND* an `IN` list or a `BETWEEN` SHALL translate only when every one of its literals converts, because the scan coerces the column and every literal of that node to one common type

### Scenario: Integer, float, decimal, string, boolean, date, and timestamp columns prune on their footer bounds

* *GIVEN* the `prune_types/` table of the scenario "A footer whose row-group bounds exclude the filter drops the file", holding one column of each of these types, with the two files' value ranges disjoint in each column: signed integers of 8, 16, 32, and 64 bits; unsigned integers of 8, 16, 32, and 64 bits, whose `grp=b/` file holds each type's maximum value; FLOAT; DOUBLE; `DECIMAL(9,2)`, `DECIMAL(10,2)`, and `DECIMAL(30,2)`, which Parquet stores as INT32, INT64, and FIXED_LEN_BYTE_ARRAY; UTF8 and LARGE UTF8; BOOLEAN; DATE32; and TIMESTAMP in seconds, milliseconds, microseconds, and nanoseconds without a time zone
* *WHEN* the adapter plans, for each such column, a comparison whose literal the column's values in one file can satisfy and the other file's bounds exclude
* *THEN* the resolved file list SHALL hold only the file whose values can satisfy the comparison
* *AND* the returned rows SHALL equal the rows of the same query without statistics pruning
* *AND* bounds SHALL compare in the column's own type order, so negative signed-integer bounds, the maximum values of the unsigned 32-bit and 64-bit columns, and negative `DECIMAL(30,2)` bounds keep and drop the same files as the stored values do
* *AND* a UTF8 value longer than the writer's statistics truncation length SHALL keep its file for an equality on that full value, because a truncated bound stays a valid bound
* *AND* a column stored as INT32 in the `grp=a/` file and as INT64 in the `grp=b/` file SHALL prune on the `grp=a/` file's bounds cast to INT64
* *AND* the TIMESTAMP column in seconds, which Parquet stores as INT64 without a timestamp annotation, SHALL prune on its bounds typed by the Arrow schema the file embeds

### Scenario: A statistic whose ordering the Parquet specification leaves undefined keeps the file

* *GIVEN* row groups whose statistics for the filtered column are, in turn: from a footer without `column_orders`, of a column whose order is not `TYPE_DEFINED_ORDER` with a defined sort order (INT96, INTERVAL, or an unknown union member), deprecated `min`/`max` only, missing a minimum or a maximum, undecodable to the column's Arrow type, with a minimum greater than its maximum, or absent
* *WHEN* the reader evaluates a comparison on that column
* *THEN* each such statistic SHALL count as unknown, so the comparison counts as possibly TRUE, FALSE, or NULL and the file is kept
* *AND* a missing null count SHALL count as unknown and MUST NOT count as zero, so `IS NULL` and `IS NOT NULL` keep the file
* *AND* a truncated string bound SHALL be used as a bound, and the reader MUST NOT depend on `is_min_value_exact` or `is_max_value_exact`
* *AND* a statistics failure SHALL keep the file and MUST NOT fail the query, panic, or surface an error

### Scenario: A column the footer statistics cannot describe keeps the file

* *GIVEN* a filter on a column that is, in turn: a partition column, a Parquet column the fold dropped for a partition-key collision, absent from the folded schema, absent from one file, nested or non-primitive, declared by its logical field at a type other than its folded type, of a file type neither equal to the folded type nor widenable to it by `datafusion-scan/type-relaxation`'s pairs, a timestamp with a time zone, a FLOAT16, or in a file whose footer the seam did not read under `MERGE_SCHEMA` FALSE
* *WHEN* the reader evaluates a comparison on that column
* *THEN* the reader SHALL evaluate a partition column on its partition value alone and MUST NOT read the statistics of any Parquet column that folds to a partition key
* *AND* every other listed column SHALL count as unknown, so the comparison counts as possibly TRUE, FALSE, or NULL and the file is kept
* *AND* a file type that widens to the folded type SHALL have its bounds cast to the folded type before comparison
* *AND* a comparison on a nested column SHALL keep a file of several row groups whose per-group leaf statistics would exclude the rendered JSON document, as `datafusion-scan/nested-json-rendering` requires of every statistics pruning stage, and the test SHALL first assert that the file's leaf statistics hold those excluding bounds

### Scenario: Columns outside the prunable types keep both files end to end

* *GIVEN* the `prune_types/` table also holding, with the two files' value ranges disjoint in each column, a DATE64 column, a `DECIMAL(50,2)` column, a FLOAT16 column, a TIMESTAMP column with a UTC time zone, a list-of-strings column, and a 64-bit integer column written without statistics
* *WHEN* the adapter plans, for each such column, a comparison whose literal one file's bounds exclude in the column's own type
* *THEN* the resolved file list SHALL hold both files
* *AND* the returned rows SHALL equal the rows of the same query without statistics pruning
* *AND* for the `DECIMAL(50,2)` and the FLOAT16 column, which the engine declares `VARCHAR(2000000)` and the scan compares as text, the returned rows SHALL include rows of the file whose bounds exclude the literal in the column's own type
* *AND* for the list-of-strings column, the returned rows SHALL include the row whose rendered JSON document equals the literal, although the leaf statistics of every row group of its file exclude that document

