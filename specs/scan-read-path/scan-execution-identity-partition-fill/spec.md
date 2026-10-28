# Feature: DataFusion Scan Execution: Identity Partition Fill

Defines what the scan UDF emits for an Iceberg field that a data file lacks but the file's own partition spec maps through an `identity` transform. The VS carries each file's manifest partition value in that file's entry, and the scan emits it ahead of the `initial-default`. A column that the file holds always reads from the file, and a null partition value reads as NULL.

## Background

* The Iceberg table spec (`format/spec.md`, "Column Projection") resolves a field id absent from a data file in this order: (1) "Return the value from partition metadata if an Identity Transform exists for the field and the partition value is present in the `partition` struct on `data_file` object in the manifest", (2) `schema.name-mapping.default`, (3) "Return the default value if it has a defined `initial-default`", (4) "Return `null` in all other cases". The engine implements all four. Rule (2) is a binding rule of `scan-read-path/scan-execution-field-id-projection`, and rule (3) never reads `write-default`, which governs writer-side backfill.
* The `identity` transform is "Source value, unmodified" for "Any primitive except for `geometry` and `geography`". A data file's `partition` struct (field 102) is the "Partition data tuple, schema based on the partition spec output", and "Manifests in v3 and earlier store files for a single spec", so the files of one table carry different specs after partition evolution. A metadata-only Hive migration (Spark `migrate`, `snapshot`, or `add_files`) registers data files that hold no `identity` source column. Rule (1) covers top-level columns only, a scoped exception: a Hive import produces only top-level partition columns, so an `identity` source nested in a struct keeps rules (2) to (4).
* Iceberg Java fills an identity partition constant even when the file holds the column, and treats a null constant as present. iceberg-rust fills it only for an absent column and skips a null value. This engine follows the spec text on precedence and Iceberg Java on null. For a file whose physical values equal its partition value, all three return the same rows.

## Scenarios

### Scenario: The VS carries each data file's identity partition values from the file's own partition spec

* *GIVEN* an Iceberg table whose data files were written under two partition specs, the first with an `identity` field on the top-level column `region` and a `bucket` field on `id`, the second with no field on `region`, and one file of the first spec whose `partition` struct holds null for `region`
* *WHEN* the VS plans the table's files for a pushdown
* *THEN* the VS SHALL resolve each data file's partition spec from the spec the file was written with, and MUST NOT use the table's current or default spec for that file
* *AND* for each `identity` field of that spec whose source is a top-level column of the scan spec's logical schema, the VS SHALL carry the field's value from the file's `partition` struct in the file entry's `partition_values`, keyed by the column's logical name, as an explicit absent value when the struct holds null
* *AND* the VS MUST NOT carry a value for a `bucket`, `truncate`, `year`, `month`, `day`, `hour`, or `void` field, because those transforms do not reproduce the source value, and a file whose spec has no `identity` field on a column SHALL carry no `partition_values` entry for that column
* *AND* the VS SHALL leave the common spec's `partition_columns` empty, so the scan still reads the column from every file that holds it, and SHALL take the values from the file tasks it already plans once per query, with no further catalog or storage request

### Scenario: An identity partition value fills a column the data file lacks, per the file's own spec

* *GIVEN* an Iceberg table identity-partitioned on a nullable `region` column, with data files that a metadata-only import registered without a `region` column, one of them with a null partition value, and data files written natively that hold `region`
* *AND* after those files, the table's partition spec dropped the `identity` field on `region`, and one more imported file without `region` was registered under the new spec
* *WHEN* a query selects `id` and `region` through the virtual schema
* *THEN* each row from an imported file written under the `identity` spec SHALL carry that file's partition value, NULL only when that value is null, although the table's current spec has no `identity` field on `region`
* *AND* each row from a natively written file SHALL carry the file's own `region` value
* *AND* each row from the file registered under the new spec SHALL take rules (2) to (4), here NULL

### Scenario: A column the data file holds wins over the file's identity partition value

* *GIVEN* a scan spec whose `partition_columns` do not list a field and whose file entries carry an identity partition value for it, for two assigned files: one that holds a physical column bound to the field, by embedded field id or by name mapping, whose values differ from the partition value and include a NULL, and one that holds no such column
* *WHEN* the scan UDF reads both files in one scan invocation
* *THEN* the UDF SHALL emit the physical column's values, its NULL included, for rows of the file that holds the column, and MUST NOT emit the partition value for them, because rule (1) applies only to a field id absent from the file
* *AND* the UDF SHALL emit the partition value for rows of the file that lacks the column, so the precedence is decided per file
* *AND* a column that `partition_columns` lists SHALL keep the log-wins precedence of `scan-read-path/scan-execution-partition-values`, so the scan decides the precedence from the scan spec's content and never from the table format

### Scenario: An identity partition value precedes the initial-default, and a null partition value is present

* *GIVEN* a scan spec whose logical schema carries a nullable field with a primitive `initial-default`, absent from three assigned files
* *AND* the first file's entry carries a non-null identity partition value for the field, the second file's entry carries an explicit absent value for it, and the third file's entry carries no value for it
* *WHEN* the scan UDF reads the three files
* *THEN* rows of the first file SHALL carry the partition value and MUST NOT carry the `initial-default`
* *AND* rows of the second file SHALL be NULL and MUST NOT carry the `initial-default`, because a null in the `partition` struct counts as present, as in Iceberg Java, whose Parquet reader checks `idToConstant.containsKey`
* *AND* rows of the third file SHALL carry the `initial-default`, because rule (1) does not apply to a file whose spec has no `identity` field on the column

### Scenario: A required identity column without a physical counterpart reads its partition value

* *GIVEN* an Iceberg table identity-partitioned on a REQUIRED column that defines no `initial-default`, whose data files a metadata-only import registered without that column
* *WHEN* a query selects that column through the virtual schema
* *THEN* each row SHALL carry its file's partition value
* *AND* the scan UDF MUST NOT return the required-absent error of "Added required column absent from a file with no initial-default errors cleanly"

### Scenario: A reconstructed identity value reads the same as a stored one for every admitted type

* *GIVEN* an Iceberg table identity-partitioned on one column of each of these types: `boolean`, `int`, `long`, `float`, `double`, one `decimal` inside and one outside Exasol's catalog-decimal domain, `date`, and `string`
* *AND* the table holds one row in an imported file that holds none of those columns, one row with the same values in a natively written file that holds them, and one imported row whose partition values are all null
* *WHEN* a query selects every column ordered by `id` through the virtual schema
* *THEN* the row from the imported file SHALL equal the row from the native file in every column, so a reconstructed value cannot be told apart from a stored one
* *AND* each column SHALL keep the Exasol type that `scan-types/type-mapping` declares for it
* *AND* the all-null row SHALL be NULL in every partition column

### Scenario: Every admitted identity value survives the scan-spec round trip at the column's declared type

* *GIVEN* identity partition values of every Iceberg primitive type the read path admits, `boolean`, `int`, `long`, `float`, `double`, `decimal` inside and outside Exasol's catalog-decimal domain, `date`, `time`, `timestamp`, `timestamp_ns`, `timestamptz`, `timestamptz_ns`, and `string`, plus values written before an `int` to `long`, a `float` to `double`, and a `decimal` precision promotion
* *WHEN* the VS serializes each value into a file entry and the scan UDF converts it to the column's declared Arrow type
* *THEN* the converted value SHALL equal the value that a physical column of the file's own type reads as at the declared type, including a `float` widened to its exact `double` expansion and a `time` or out-of-domain `decimal` rendered as the text its `VARCHAR` column shows
* *AND* the VS SHALL use the string form, and the scan the conversion and error rule, of `scan-read-path/scan-execution-partition-values`, so one conversion serves both kinds of partition value
* *AND* an empty `string` value SHALL read as NULL, the value Exasol stores for an empty `VARCHAR`
* *AND* the VS SHALL carry no value for a `binary`, `fixed(L)`, or `uuid` column, which `vs-adapter/binary-column-refusal` refuses at plan time

### Scenario: Pushed-down filters, aggregates, TopN, and joins see the reconstructed value

* *GIVEN* the table of "An identity partition value fills a column the data file lacks, per the file's own spec"
* *WHEN* queries through the virtual schema push down a `WHERE region = 'eu'` filter, a `GROUP BY region` aggregate with partial aggregation, `COUNT(DISTINCT region)`, `ORDER BY region` with `LIMIT`, and a broadcast inner equi-join that projects `region` from this table
* *THEN* each query SHALL reach the scan UDF and SHALL return the rows that the same query returns over the table's true values: the partition value for an imported file, and the stored value for a native file
* *AND* the filter SHALL return every row whose reconstructed value matches, so plan-time pruning on the manifest partition values keeps every file that holds a matching row
* *AND* the grouped aggregate SHALL count each imported file's rows under that file's partition value, and MUST NOT count them under NULL
