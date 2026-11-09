# Feature: EXPORT INTO SCRIPT Into a New Iceberg Table

The export writes the source rows as Parquet data files into a new Iceberg table, through the Iceberg REST catalog that the CONNECTION names, on S3-compatible storage. It creates the namespace and the table, and each writer commits its own files exactly once. With `SINGLE_COMMIT = 'true'` the table appears only together with all its rows (#566).

## Background

* Statement and options: export/export-options § Background.
* Read through a virtual schema: export/export-into-script § Background.
* Storage credentials are the CONNECTION's static storage keys or, when the CONNECTION sets `"use_vended_credentials": true`, the credentials that the catalog vends for the new table.
* On a catalog that offers a credentials endpoint (Iceberg REST `loadCredentials`), the export renews vended credentials through it before they expire.
* Before it reads any source row the export makes catalog calls only: it authenticates, refuses an existing table, and creates every missing level of the namespace, even when it then writes no rows or fails. It never writes to storage at that point, so a storage error shows when the writers write, and in parallel mode a failed export can leave an empty table behind. It creates only new tables.
* `SINGLE_COMMIT` means the table appears only with its data. Iceberg REST does it by staged table creation (`stage-create`), so only a `SINGLE_COMMIT` export needs a catalog that accepts it.
* The export writes only onto S3-compatible storage, through an Iceberg REST catalog that does not need AWS SigV4 signing.
* Type mapping. The export creates format-version 2 tables. Every column is optional and carries the source column's name as Exasol reports it, so an unquoted identifier arrives in upper case. Column names must be distinct and must not name one of Iceberg's reserved metadata columns, such as `_file`.

  | Exasol type | Iceberg type |
  |-------------|--------------|
  | `DECIMAL(p,s)` | `decimal(p,s)` |
  | `DOUBLE` | `double` |
  | `BOOLEAN` | `boolean` |
  | `VARCHAR(n)`, `CHAR(n)` | `string`; a `CHAR` value keeps its trailing blanks |
  | `DATE` | `date` |
  | `TIMESTAMP(p)`, p ≤ 6 | `timestamp` |

  Every other type is refused. Exasol itself refuses a `GEOMETRY`, `HASHTYPE`, `INTERVAL`, or `TIMESTAMP WITH LOCAL TIME ZONE` column before the export reads it: with `Column type not supported: <type>`, or for `GEOMETRY` and `INTERVAL YEAR TO MONTH` with `VM error: Unknown data type found`, which names no column. The export refuses `TIMESTAMP(p)` with p > 6, and any other type that reaches it, with an error that names the column and its type and suggests a cast to `TIMESTAMP(6)`.
* Iceberg table spec § Primitive Types: `decimal(P,S)` "Scale is fixed, precision must be 38 or less", which holds Exasol's precision of at most 36. `timestamp` is "Timestamp, microsecond precision, without timezone", and note 1 states "Timestamp values _without time zone_ represent a date and time of day regardless of zone", as Exasol's `TIMESTAMP` does. `timestamp_ns` is added by v3, so a version 2 table has no lossless target for `TIMESTAMP(7)` to `TIMESTAMP(9)`: refusing them is this feature's scoped exception. `string` is "Encoded with UTF-8". § Reserved Field IDs reserves metadata columns "that can be used in user data schemas, like the `_file` column".
* Every snapshot that an export commits carries the summary property `lakehouse.export-id`, which identifies the export statement.

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

### Scenario: An export into an existing table is refused

* *GIVEN* a table that exists in the catalog
* *AND* a source whose rows raise an error when they are read
* *WHEN* a user exports it with `TABLE_NAME` naming that table
* *THEN* the export SHALL fail before any source row is read, with an error that names the table and states that the export only creates new tables
* *AND* the table SHALL keep its snapshots unchanged

### Scenario: Two exports that create the same table leave one winner

* *GIVEN* two statements that export different sources into the same table name, which does not exist
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

### Scenario: Static and vended storage credentials both write the table

* *GIVEN* one CONNECTION with static storage keys, and one CONNECTION that sets `"use_vended_credentials": true` and holds no storage keys
* *WHEN* a user exports the same source through each CONNECTION into a new table
* *THEN* both exports SHALL succeed
* *AND* each new table, read through a virtual schema, SHALL hold the source rows

### Scenario: A writer that outlives its vended credentials refreshes them, or fails naming their expiry

* *GIVEN* a CONNECTION that sets `"use_vended_credentials": true`, and vended credentials that expire before the writer finishes
* *WHEN* a user exports a source into a new table, with or without `SINGLE_COMMIT = 'true'`, through a catalog that offers a credentials endpoint
* *THEN* the writer SHALL renew the credentials through that endpoint before they expire and the export SHALL succeed
* *WHEN* the catalog offers no credentials endpoint
* *THEN* the export SHALL fail with an error that names the table and states that its vended credentials expired and that the catalog offers no credentials endpoint
* *AND* the error MUST NOT contain a storage credential

### Scenario: A bad CONNECTION fails before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it through a CONNECTION whose catalog address is unreachable or whose catalog credentials are wrong
* *THEN* the export SHALL fail before any source row is read, with an error that names the catalog as the failing side
* *AND* the error MUST NOT contain a secret of the CONNECTION or a storage credential
* *AND* the export SHALL NOT create the table

### Scenario: A wrong storage key fails the writer, naming the storage

* *GIVEN* a CONNECTION whose static storage keys cannot write to the catalog's storage, and a source of 1,000 rows
* *WHEN* a user exports it
* *THEN* the export SHALL fail with an error that names the storage as the failing side and contains no storage credential
* *AND* with `SINGLE_COMMIT = 'true'` the export SHALL NOT create the table, and without it the table, if it exists, SHALL hold no rows

### Scenario: A CONNECTION the export does not support is refused before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it, with or without `SINGLE_COMMIT`, through a CONNECTION with Azure storage credentials, or one that signs catalog requests with AWS SigV4
* *THEN* the export SHALL fail before any source row is read, with an error that names the unsupported storage or signing
* *AND* the export SHALL NOT create the table

### Scenario: A table location outside S3-compatible storage is refused

* *GIVEN* a catalog that answers a table location whose scheme is not `s3` or `s3a`
* *WHEN* a user exports into a new table
* *THEN* the writer SHALL fail with an error that names the table and the scheme

### Scenario: SINGLE_COMMIT on a catalog without staged creation fails and creates no table

* *GIVEN* a catalog that refuses staged table creation
* *WHEN* a user exports a source of 1,000 rows with `SINGLE_COMMIT = 'true'`
* *THEN* the export SHALL fail with an error that names the catalog and states that `SINGLE_COMMIT` needs staged table creation
* *AND* the export SHALL NOT create the table
* *AND* the same export without `SINGLE_COMMIT` SHALL succeed

### Scenario: SINGLE_COMMIT creates the table only together with its rows

* *GIVEN* a table that does not exist and a source of 1,000 rows
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'`
* *THEN* the table SHALL have exactly one snapshot, which holds the 1,000 rows
* *AND* no earlier version of the table's metadata SHALL exist

### Scenario: A failed SINGLE_COMMIT export leaves no table

* *GIVEN* a source of 1,000 rows and a namespace that does not exist
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'`, and either the source raises an error at its 500th row or the writer fails after it has written its data files and before it commits
* *THEN* the statement SHALL fail, with the source's error when the source failed
* *AND* the catalog SHALL NOT hold the table, and the namespace that the export created SHALL remain
* *AND* the data files that the writer wrote SHALL NOT remain in storage
