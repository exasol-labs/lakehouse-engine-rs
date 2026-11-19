# Feature: EXPORT INTO SCRIPT Into a New Iceberg Table

The export writes the source rows as Parquet data files into a new Iceberg table, through the Iceberg REST catalog that the CONNECTION names, on S3-compatible storage or Azure Data Lake Storage Gen2. It creates the namespace and the table, and each writer commits its own files exactly once. With `SINGLE_COMMIT = 'true'` the table appears only together with all its rows: export/export-single-commit. An export into an existing table appends to it: export/export-append-iceberg-table.

<!-- DELTA:CHANGED -->
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
* Data files. The export writes every data file as Parquet, whatever the table's `write.format.default`, with the table's `write.parquet.compression-codec`, `write.parquet.compression-level`, `write.parquet.row-group-size-bytes`, `write.parquet.page-size-bytes`, `write.parquet.page-row-limit`, `write.parquet.dict-size-bytes`, and `write.parquet.content-defined-chunking.*`. A row group ends near `write.parquet.row-group-size-bytes`, because the Parquet writer estimates how many rows fit, or at 1,048,576 rows. A writer starts a new data file once the open one passes the table's `write.target-file-size-bytes`. A property the table lacks takes Iceberg's default: `zstd`, 128 MiB row groups, and 512 MiB files. A writer holds the row group in progress in memory, so a larger row group size costs writer memory.
* A table the export creates carries `write.parquet.compression-codec = zstd`, `write.parquet.row-group-size-bytes = 134217728`, and `write.target-file-size-bytes = 268435456`, so every later writer of the table uses the same settings. The 256 MiB file size is half Iceberg's default, because a virtual schema shards a scan by data file.
* Iceberg table spec § Table Metadata Fields: `properties` is "A string to string map of table properties. This is used to control settings that affect reading and writing". § Manifests: a data file's `file_format` is "String file format name, `avro`, `orc`, `parquet`, or `puffin`", so the data files of one table may differ in format.
* Every snapshot that an export commits carries the summary property `lakehouse.export-id`, which identifies the export statement.
* Between two attempts of one commit the writer waits the table's `commit.retry.min-wait-ms`, doubled after each retry up to its `commit.retry.max-wait-ms`, plus up to a tenth more at random. A property that is absent or not a whole number of milliseconds takes Iceberg's default of 100 ms or 60 s.
* Every commit pins the schema and partition spec the writer checked. The writer binds source columns to the table's fields by name and writes their field IDs into its data files, so a commit after another engine changed the schema could land values that read as NULL under the names they were bound to, for example after a column is dropped and added again. Its partition values follow the checked spec, which a manifest under a newer default spec would misdescribe.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: The export creates a missing namespace and table

* *GIVEN* a two-level namespace `<a>.<b>` of which no level exists in the catalog
* *WHEN* a user exports a source of 1,000 rows with `TABLE_NAME = '<a>.<b>.<table>'`
* *THEN* the catalog SHALL hold the namespace `<a>.<b>` and the table, with the column names and Iceberg types § Background maps
* *AND* the table SHALL hold the 1,000 rows

<!-- DELTA:NEW -->
### Scenario: A table the export creates carries its write properties

* *GIVEN* a table that does not exist
* *WHEN* a user exports a source into it, with several writers or with `SINGLE_COMMIT = 'true'`
* *THEN* the table's properties SHALL hold the three write properties that § Background lists for a table the export creates
* *AND* every data file that the export writes SHALL be compressed with `zstd`, and no row group SHALL exceed 134217728 bytes by more than a tenth
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: An export into an unpartitioned table rolls its data files at the target file size

* *GIVEN* an unpartitioned table whose `write.target-file-size-bytes` is smaller than the source's encoded size
* *WHEN* a user exports the source into it with `SINGLE_COMMIT = 'true'`
* *THEN* the writer SHALL start a new data file once the open one passes `write.target-file-size-bytes`, so the export writes several data files
<!-- /DELTA:NEW -->
