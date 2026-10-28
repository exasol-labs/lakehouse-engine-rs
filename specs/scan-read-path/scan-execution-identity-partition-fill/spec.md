# Feature: DataFusion Scan Execution: Identity Partition Fill

Defines what the scan UDF emits for an Iceberg field that the file's own partition spec maps through an `identity` transform. The VS carries each file's manifest partition value in that file's entry, and the scan emits it for that file's rows, ahead of the file's own column and the `initial-default`. A null partition value reads as NULL.

## Background

* The Iceberg table spec (`format/spec.md`, "Column Projection") resolves a field id absent from a data file in this order: (1) "Return the value from partition metadata if an Identity Transform exists for the field and the partition value is present in the `partition` struct on `data_file` object in the manifest", (2) `schema.name-mapping.default`, (3) "Return the default value if it has a defined `initial-default`", (4) "Return `null` in all other cases". The engine implements all four. Rule (2) is a binding rule of `scan-read-path/scan-execution-field-id-projection`, and rule (3) never reads `write-default`, which governs writer-side backfill. A column that only name mapping or its name binds has no field id in the file, so rule (1) precedes that binding.
* The `identity` transform is "Source value, unmodified" for "Any primitive except for `geometry` and `geography`". A data file's `partition` struct (field 102) is the "Partition data tuple, schema based on the partition spec output", and "Manifests in v3 and earlier store files for a single spec", so the files of one table carry different specs after partition evolution. A metadata-only Hive migration (Spark `migrate`, `snapshot`, or `add_files`) registers data files that hold no `identity` source column. Rule (1) covers top-level columns only, a scoped exception: a Hive import produces only top-level partition columns, so an `identity` source nested in a struct keeps rules (2) to (4).
* The scan reuses the partition-value mechanism of `scan-read-path/scan-execution-partition-values`, where a file's value wins over the data file, so a file that carries an identity value reads it even where the file holds the column under the field's id. Rule (1) names only absent field ids, so this is a scoped exception. The spec also requires "Partition values for a data file must be the same for all records stored in the data file", so a conforming file returns the same rows either way. Iceberg Java fills the constant the same way and treats a null constant as present, which this engine follows too. iceberg-rust fills it only for a column absent after name mapping and skips a null value.

## Scenarios

### Scenario: The VS carries each data file's identity partition values from the file's own partition spec

* *GIVEN* an Iceberg table whose data files were written under two partition specs, the first with an `identity` field on the top-level column `region` and a `bucket` field on `id`, the second with no field on `region`, and one file of the first spec whose `partition` struct holds null for `region`
* *WHEN* the VS plans the table's files for a pushdown
* *THEN* the VS SHALL resolve each data file's partition spec from the spec the file was written with, and MUST NOT use the table's current or default spec for that file
* *AND* for each `identity` field of that spec whose source is a top-level column of the scan spec's logical schema, the VS SHALL carry the field's value from the file's `partition` struct in the file entry's `partition_values`, keyed by the column's logical name, as an explicit absent value when the struct holds null
* *AND* the VS MUST NOT carry a value for a `bucket`, `truncate`, `year`, `month`, `day`, `hour`, or `void` field, because those transforms do not reproduce the source value, and a file whose spec has no `identity` field on a column SHALL carry no `partition_values` entry for that column
* *AND* the VS SHALL leave the common spec's `partition_columns` empty, so a file that carries no value for the column still reads it from the data file, and SHALL take the values from the file tasks it already plans once per query, with no further catalog or storage request

### Scenario: An identity partition value fills a column the data file lacks, per the file's own spec

* *GIVEN* an Iceberg table identity-partitioned on a nullable `region` column, with data files that a metadata-only import registered without a `region` column, one of them with a null partition value, and data files written natively that hold `region`
* *AND* after those files, the table's partition spec dropped the `identity` field on `region`, and one more imported file without `region` was registered under the new spec
* *WHEN* a query selects `id` and `region` through the virtual schema
* *THEN* each row from an imported file written under the `identity` spec SHALL carry that file's partition value, NULL only when that value is null, although the table's current spec has no `identity` field on `region`
* *AND* each row from a natively written file SHALL carry the file's own `region` value, and each row from the file registered under the new spec SHALL take rules (2) to (4), here NULL
* *AND* a pushed-down filter on `region` SHALL return exactly the rows whose value above matches, so plan-time pruning keeps each imported file whose partition value matches

### Scenario: An identity partition value wins over a column the data file holds

* *GIVEN* an Iceberg table identity-partitioned on a nullable `region` column, whose `schema.name-mapping.default` maps `region`, with a data file that a metadata-only import registered under the partition value `eu`, holding a `region` column whose values are `us` and NULL
* *AND* a natively written data file whose spec has no `identity` field on `region`, holding `region` under its field id
* *WHEN* a query selects `id` and `region` through the virtual schema
* *THEN* each row of the imported file SHALL carry `eu`, and MUST NOT carry the file's physical `region` values, whether the file binds `region` by field id, by name mapping, or by name
* *AND* each row of the natively written file SHALL carry the file's own `region` value, so the precedence is decided per file, from whether the file entry carries a value and never from the table format
* *AND* a pushed-down filter on `region` SHALL compare the imported file's rows against `eu`, although plan-time pruning MAY skip that file by its `region` column metrics, which contradict its partition value, as Iceberg Java's metrics evaluator does

### Scenario: An identity partition value precedes the initial-default, and a null partition value is present

* *GIVEN* a scan spec whose logical schema carries a nullable field with a primitive `initial-default`, absent from three assigned files
* *AND* the first file's entry carries a non-null identity partition value for the field, the second file's entry carries an explicit absent value for it, and the third file's entry carries no value for it
* *WHEN* the scan UDF reads the three files
* *THEN* rows of the first file SHALL carry the partition value and MUST NOT carry the `initial-default`
* *AND* rows of the second file SHALL be NULL and MUST NOT carry the `initial-default`, because a null in the `partition` struct counts as present, as in Iceberg Java, whose Parquet reader checks `idToConstant.containsKey`
* *AND* rows of the third file SHALL carry the `initial-default`, because rule (1) does not apply to a file whose spec has no `identity` field on the column

### Scenario: A required identity column without a physical counterpart reads its partition value

* *GIVEN* a scan spec whose logical schema carries a REQUIRED field with no `initial-default`, absent from every assigned file, and each file entry carries an identity partition value for it
* *WHEN* the scan UDF reads the files
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
