# Feature: EXPORT INTO SCRIPT Into a New Iceberg Table

The export writes the source rows as Parquet data files into a new Iceberg table, through the Iceberg REST catalog that the CONNECTION names, on S3-compatible storage or Azure Data Lake Storage Gen2. It creates the namespace and the table, and each writer commits its own files exactly once. With `SINGLE_COMMIT = 'true'` the table appears only together with all its rows: export/export-single-commit. An export into an existing table appends to it: export/export-append-iceberg-table.

## Background

* Statement and options: export/export-options § Background.
* Read through a virtual schema: export/export-into-script § Background.
* Catalog, storage, and credentials: export/export-catalog-storage § Background.
* Before it reads any source row the export makes catalog calls only: it authenticates, checks whether the table exists, and, when it does not, creates every missing level of the namespace, even when it then writes no rows or fails. It never writes to storage at that point, so a storage error shows when the writers write, and in parallel mode a failed export can leave an empty table behind. A table that another statement creates after that check fails the writers with an error that names the table.
* `SINGLE_COMMIT`: export/export-single-commit § Background.
* Type mapping. The export creates format-version 2 tables, unpartitioned unless `PARTITION_BY` is given (export/export-partitioned-iceberg-table). Every column is optional and carries the source column's name as Exasol reports it, so an unquoted identifier arrives in upper case. Column names must be distinct and must not name one of Iceberg's reserved metadata columns, such as `_file`.

  | Exasol type | Iceberg type |
  |-------------|--------------|
  | `DECIMAL(p,s)` | `decimal(p,s)` |
  | `DOUBLE` | `double` |
  | `BOOLEAN` | `boolean` |
  | `VARCHAR(n)`, `CHAR(n)` | `string`; a `CHAR` value keeps its trailing blanks |
  | `DATE` | `date` |
  | `TIMESTAMP(p)`, p ≤ 6 | `timestamp` |

  Every other type is refused. Exasol itself refuses a `GEOMETRY`, `HASHTYPE`, `INTERVAL`, or `TIMESTAMP WITH LOCAL TIME ZONE` column before the export reads it: with `Column type not supported: <type>`, or with `VM error: Unknown data type found`, which names no column. Which of the two a column gets varies between statements. The export refuses `TIMESTAMP(p)` with p > 6, and any other type that reaches it, with an error that names the column and its type and suggests a cast to `TIMESTAMP(6)`.
* Iceberg table spec § Primitive Types: `decimal(P,S)` "Scale is fixed, precision must be 38 or less", which holds Exasol's precision of at most 36. `timestamp` is "Timestamp, microsecond precision, without timezone", and note 1 states "Timestamp values _without time zone_ represent a date and time of day regardless of zone", as Exasol's `TIMESTAMP` does. `timestamp_ns` is added by v3, so a version 2 table has no lossless target for `TIMESTAMP(7)` to `TIMESTAMP(9)`: refusing them is this feature's scoped exception. `string` is "Encoded with UTF-8". § Reserved Field IDs reserves metadata columns "that can be used in user data schemas, like the `_file` column".
* Every snapshot that an export commits carries the summary property `lakehouse.export-id`, which identifies the export statement.
* Between two attempts of one commit the writer waits the table's `commit.retry.min-wait-ms`, doubled after each retry up to its `commit.retry.max-wait-ms`, plus up to a tenth more at random. A property that is absent or not a whole number of milliseconds takes Iceberg's default of 100 ms or 60 s.
* Every commit pins the schema and partition spec the writer checked. The writer binds source columns to the table's fields by name and writes their field IDs into its data files, so a commit after another engine changed the schema could land values that read as NULL under the names they were bound to, for example after a column is dropped and added again. Its partition values follow the checked spec, which a manifest under a newer default spec would misdescribe.

## Scenarios

### Scenario: The export creates a missing namespace and table

* *GIVEN* a two-level namespace `<a>.<b>` of which no level exists in the catalog
* *WHEN* a user exports a source of 1,000 rows with `TABLE_NAME = '<a>.<b>.<table>'`
* *THEN* the catalog SHALL hold the namespace `<a>.<b>` and the table, with the column names and Iceberg types § Background maps
* *AND* the table SHALL hold the 1,000 rows

### Scenario: Every supported type reads back unchanged, NULLs included

* *GIVEN* an Exasol table with one column of each type § Background maps
* *AND* rows that hold NULL in every column, a `DECIMAL(18,2)` value with trailing zeros such as `1.50`, and a `CHAR` value shorter than its length
* *WHEN* a user exports the table and reads the new table through a virtual schema
* *THEN* every value read back, NULLs included, SHALL equal the source value

### Scenario: A column of an unsupported type fails the export before any data is written

* *GIVEN* a source with a column of a type § Background does not map, such as `HASHTYPE`, `GEOMETRY`, or `TIMESTAMP(9)`
* *WHEN* a user exports it into a table that does not exist
* *THEN* the export SHALL fail with the error § Background states for that type
* *AND* the export SHALL NOT create the table

### Scenario: A source column name the new table cannot hold is refused before any data is read

* *GIVEN* a source whose rows raise an error when they are read, with two columns of the same name or a column named `"_file"`
* *WHEN* a user exports it
* *THEN* the export SHALL fail before any source row is read, with an error that names the column, or with Exasol's own refusal of the statement

### Scenario: Two exports that create the same table leave one winner

* *GIVEN* two statements that export different sources into the same table name, which does not exist when either statement checks the table
* *WHEN* both run at the same time
* *THEN* exactly one statement SHALL succeed
* *AND* the other SHALL fail with an error that names the table
* *AND* the table SHALL hold only the rows of the statement that succeeded

### Scenario: Concurrent writers create one table, and every commit carries the export ID

* *GIVEN* two tables that do not exist and a source of 16,000 rows
* *WHEN* a user exports the source into each table on one node with `WRITERS_PER_NODE = '16'`
* *THEN* each export SHALL succeed with a row count of 16,000
* *AND* each table SHALL have one snapshot per writer and hold every source row exactly once
* *AND* every snapshot of one table SHALL carry the same `lakehouse.export-id`, and the two tables SHALL carry different values

### Scenario: A commit retried after an unknown outcome adds its rows once

* *GIVEN* a writer whose commit the catalog applied, but whose answer the writer never received
* *WHEN* the writer retries the commit
* *THEN* the writer SHALL succeed
* *AND* the table SHALL hold the writer's rows exactly once

### Scenario: A commit fails when the table changed schema or partition spec after the writer checked it

* *GIVEN* a writer that checked a table, and another engine that then changes the table's current schema or default partition spec
* *WHEN* the writer commits its data files
* *THEN* the commit SHALL assert the schema ID and the partition spec ID that the writer checked
* *AND* a commit that reloads the changed table, before its first attempt or before a retry, SHALL fail with an error that names the table and what changed, and SHALL add no snapshot
