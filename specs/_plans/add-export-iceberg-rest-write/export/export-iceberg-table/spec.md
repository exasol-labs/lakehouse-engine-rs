# Feature: EXPORT INTO SCRIPT Into a New Iceberg Table

The export writes the source rows as Parquet data files into a new Iceberg table, through the Iceberg REST catalog that the CONNECTION names, on S3-compatible storage. It creates the namespace and the table, and each writer commits its own files exactly once. With `SINGLE_COMMIT = 'true'` the table appears together with all its rows, or not at all (#566).

## Background

* Statement and options: export/export-options § Background.
* Read through a virtual schema: export/export-into-script § Background.
* Storage credentials are the CONNECTION's static storage keys or, when the CONNECTION sets `"use_vended_credentials": true`, the credentials that the catalog vends for the new table.
* The export creates every missing level of the namespace before it reads any source row, even when it then writes no rows or fails. It creates only new tables.
* Every export needs a catalog that accepts staged table creation (Iceberg REST `stage-create`).
* The export writes only onto S3-compatible storage, through an Iceberg REST catalog that does not need AWS SigV4 signing.
* Type mapping. Every column of the new table is optional and carries the source column's name as Exasol reports it, so an unquoted identifier arrives in upper case. The export creates format-version 2 tables.

  | Exasol type | Iceberg type |
  |-------------|--------------|
  | `DECIMAL(p,s)` | `decimal(p,s)` |
  | `DOUBLE` | `double` |
  | `BOOLEAN` | `boolean` |
  | `VARCHAR(n)`, `CHAR(n)` | `string`; a `CHAR` value keeps its trailing blanks |
  | `DATE` | `date` |
  | `TIMESTAMP(p)`, p ≤ 6 | `timestamp` |

  Every other type is refused. Exasol itself refuses a `GEOMETRY`, `HASHTYPE`, `INTERVAL`, or `TIMESTAMP WITH LOCAL TIME ZONE` column before the export reads it. The export refuses `TIMESTAMP(p)` with p > 6, and any other type that reaches it, with an error that names the column and its type.
* Iceberg table spec § Primitive Types: `decimal(P,S)` "Scale is fixed, precision must be 38 or less", which holds Exasol's precision of at most 36. `timestamp` is "Timestamp, microsecond precision, without timezone", and note 1 states "Timestamp values _without time zone_ represent a date and time of day regardless of zone", as Exasol's `TIMESTAMP` does. `timestamp_ns` is added by v3, so a version 2 table has no lossless target for `TIMESTAMP(7)` to `TIMESTAMP(9)`: refusing them is this feature's scoped exception. `string` is "Encoded with UTF-8".
* Every snapshot that an export commits carries the summary property `lakehouse.export-id`. All writers of one statement write the same value, and two statements never write the same value.

## Scenarios

### Scenario: The export creates a missing namespace and table

* *GIVEN* a two-level namespace `<a>.<b>` of which no level exists in the catalog
* *WHEN* a user exports a source of 1,000 rows with `TABLE_NAME = '<a>.<b>.<table>'`
* *THEN* the catalog SHALL hold the namespace `<a>.<b>` and the table, with the column names and Iceberg types § Background maps
* *AND* the table SHALL hold the 1,000 rows

### Scenario: Every supported type reads back unchanged, NULLs included

* *GIVEN* an Exasol table with one column of each type § Background maps, including `DECIMAL(9,0)`, `DECIMAL(18,0)`, `DECIMAL(18,2)`, `DECIMAL(36,6)`, `CHAR(5)`, `TIMESTAMP`, and `TIMESTAMP(6)`
* *AND* rows that hold NULL in every column, a `DECIMAL(18,2)` value with trailing zeros such as `1.50`, the lowest and highest `DATE` and `TIMESTAMP(6)`, a non-ASCII `VARCHAR`, and a `CHAR` value shorter than its length
* *WHEN* a user exports the table and reads the new table through a virtual schema
* *THEN* every value read back, NULLs included, SHALL equal the source value

### Scenario: A column of a type Exasol cannot pass to the export fails before any data is written

* *GIVEN* a source with a column of type `HASHTYPE`, `INTERVAL DAY TO SECOND`, `TIMESTAMP WITH LOCAL TIME ZONE`, `GEOMETRY`, or `INTERVAL YEAR TO MONTH`
* *WHEN* a user exports it into a table that does not exist
* *THEN* the export SHALL fail with Exasol's `Column type not supported: <type>` for `HASHTYPE`, `INTERVAL DAY TO SECOND`, and `TIMESTAMP WITH LOCAL TIME ZONE`, and with Exasol's `VM error: Unknown data type found`, which names no column, for `GEOMETRY` and `INTERVAL YEAR TO MONTH`
* *AND* the export SHALL NOT create the table

### Scenario: A TIMESTAMP column with more than six fractional digits fails before any data is written

* *GIVEN* a source with a `TIMESTAMP(9)` column
* *WHEN* a user exports it into a table that does not exist
* *THEN* the export SHALL fail with an error that names the column and its type and suggests a cast to `TIMESTAMP(6)`
* *AND* the export SHALL NOT create the table

### Scenario: A source with a repeated column name is refused before any data is read

* *GIVEN* a source with two columns of the same name, whose rows raise an error when they are read
* *WHEN* a user exports it
* *THEN* the export SHALL fail before any source row is read, with an error that names the repeated column, or with Exasol's own refusal of the statement

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

### Scenario: Concurrent writers create one table and all commit

* *GIVEN* a table that does not exist and a source of 16,000 rows
* *WHEN* a user exports it on one node with `WRITERS_PER_NODE = '16'`
* *THEN* the export SHALL succeed with a row count of 16,000
* *AND* the catalog SHALL hold one table with one snapshot per writer, and the table SHALL hold every source row exactly once

### Scenario: Every commit carries the export ID

* *GIVEN* two exports, each of several writers, into two new tables
* *WHEN* both succeed
* *THEN* every snapshot of each table SHALL carry `lakehouse.export-id`
* *AND* the value SHALL be the same on every snapshot of one table and differ between the two tables

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

### Scenario: A bad CONNECTION fails before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it through a CONNECTION whose catalog address is unreachable, whose catalog credentials are wrong, or whose static storage keys cannot write to the catalog's storage
* *THEN* the export SHALL fail before any source row is read, with an error that names the catalog or the storage as the failing side
* *AND* the error MUST NOT contain a secret of the CONNECTION
* *AND* the export SHALL NOT create the table

### Scenario: A catalog that refuses staged table creation fails the export before any data is read

* *GIVEN* a catalog that refuses staged table creation
* *AND* a source whose rows raise an error when they are read
* *WHEN* a user exports it, with or without `SINGLE_COMMIT`
* *THEN* the export SHALL fail before any source row is read, with an error that names the catalog and states that the export needs staged table creation
* *AND* the export SHALL NOT create the table

### Scenario: A CONNECTION the export cannot write through is refused before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it through a CONNECTION with Azure storage credentials, through one that signs catalog requests with AWS SigV4, or through a catalog whose table location is not on S3-compatible storage
* *THEN* the export SHALL fail before any source row is read, with an error that names the unsupported storage, location scheme, or signing

### Scenario: SINGLE_COMMIT creates the table together with its rows

* *GIVEN* a table that does not exist and a source of 1,000 rows
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'`
* *THEN* the table SHALL have exactly one snapshot, which holds the 1,000 rows
* *AND* no earlier version of the table's metadata SHALL exist

### Scenario: A failed SINGLE_COMMIT export leaves no table

* *GIVEN* a source of 1,000 rows and a namespace that does not exist
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'`, and the writer fails after it has written its data files and before it commits
* *THEN* the statement SHALL fail
* *AND* the catalog SHALL NOT hold the table
* *AND* the namespace that the export created SHALL remain

### Scenario: A source error under SINGLE_COMMIT leaves no table

* *GIVEN* a source of 1,000 rows that raises an error at its 500th row
* *WHEN* a user exports it with `SINGLE_COMMIT = 'true'` into a table that does not exist
* *THEN* the statement SHALL fail with the source's error
* *AND* the catalog SHALL NOT hold the table
