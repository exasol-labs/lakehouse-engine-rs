<!-- DELTA:CHANGED -->
# Feature: EXPORT INTO SCRIPT Into a New Iceberg Table

The export writes the source rows as Parquet data files into a new Iceberg table, through the Iceberg REST catalog that the CONNECTION names, on S3-compatible storage or Azure Data Lake Storage Gen2. It creates the namespace and the table, and each writer commits its own files exactly once. With `SINGLE_COMMIT = 'true'` the table appears only together with all its rows. An export into an existing table appends to it: export/export-append-iceberg-table.
<!-- /DELTA:CHANGED -->

## Background

* Statement and options: export/export-options § Background.
* Read through a virtual schema: export/export-into-script § Background.
* Catalog, storage, and credentials: export/export-catalog-storage § Background.
* Before it reads any source row the export makes catalog calls only: it authenticates, checks whether the table exists, and, when it does not, creates every missing level of the namespace, even when it then writes no rows or fails. It never writes to storage at that point, so a storage error shows when the writers write, and in parallel mode a failed export can leave an empty table behind. A table that another statement creates after that check fails the writers with an error that names the table.
* `SINGLE_COMMIT` means the table appears only with its data. Iceberg REST does it by staged table creation (`stage-create`), so only a `SINGLE_COMMIT` export that creates its table needs a catalog that accepts it.
* Type mapping. The export creates format-version 2 tables. Every column is optional and carries the source column's name as Exasol reports it, so an unquoted identifier arrives in upper case. Column names must be distinct and must not name one of Iceberg's reserved metadata columns, such as `_file`.

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
* Every commit pins the schema and partition spec the writer checked. The writer binds source columns to the table's fields by name and writes their field IDs into its data files, so a commit after another engine changed the schema could land values that read as NULL under the names they were bound to, for example after a column is dropped and added again. Its partition values follow the checked spec, which a manifest under a newer default spec would misdescribe.

## Scenarios

### Scenario: The export creates a missing namespace and table

* *GIVEN* a two-level namespace `<a>.<b>` of which no level exists in the catalog
* *WHEN* a user exports a source of 1,000 rows with `TABLE_NAME = '<a>.<b>.<table>'`
* *THEN* the catalog SHALL hold the namespace `<a>.<b>` and the table, with the column names and Iceberg types § Background maps
* *AND* the table SHALL hold the 1,000 rows
