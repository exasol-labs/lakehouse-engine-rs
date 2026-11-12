# Feature: EXPORT INTO SCRIPT Into an Existing Iceberg Table

When `TABLE_NAME` names an Iceberg table that already exists, including one that another engine created, the export appends the source rows to it. It binds every table column to a source column by name, accepts a source type that the table column's type widens, and refuses with one error that lists every mismatch before it writes any data. With `SINGLE_COMMIT = 'true'` the append makes exactly one commit or none (#567).

## Background

* Statement, options, and writer count: export/export-options § Background. Storage credentials and the export ID of each snapshot: export/export-iceberg-table § Background. Read through a virtual schema: export/export-into-script § Background.
* When the catalog holds the table that `TABLE_NAME` names, the export appends to it. Otherwise export/export-iceberg-table applies.
* Before it reads any source row, the export loads the table and refuses a table whose format version is not 2, or whose default partition spec has a field, with an error that names the table and the format version or the partitioning. The append never changes the table's schema, partition spec, or properties.
* Binding: a source column binds to the table column of the same name. When no table column has exactly that name, it binds to the one table column whose name differs only in letter case. Column order does not matter. Every table column, required or optional, must be bound.
* Mismatches are these four kinds:

  - a source column that binds no table column, or binds several
  - a table column that several source columns bind
  - a table column that no source column binds
  - a source column whose type its table column does not accept

  Each writer checks the binding before it writes any data. It fails with one error that names the table and lists every mismatch, each with the column and, for a type mismatch, the source type and the table column's type.
* Accepted types. Every pair the table does not list is a mismatch, so a table with a column of an unlisted type, such as `float` or a struct, takes no append.

  | Source column type | Table column types it appends to |
  |--------------------|----------------------------------|
  | `DECIMAL(p,s)` | `decimal(P,s)` with P ≥ p |
  | `DECIMAL(p,0)`, p ≤ 9 | also `int` and `long` |
  | `DECIMAL(p,0)`, 9 < p ≤ 18 | also `long` |
  | `DOUBLE` | `double` |
  | `BOOLEAN` | `boolean` |
  | `VARCHAR(n)`, `CHAR(n)` | `string`; a `CHAR` value keeps its trailing blanks |
  | `DATE` | `date` |
  | `TIMESTAMP(p)`, p ≤ 6 | `timestamp` |

* A source with no rows runs no writer, so no column is checked: it succeeds with a row count of 0 even when its columns do not match the table.
* A writer that reads a NULL for a column bound to a required table column fails with an error that names the table and the column, and commits none of its rows.
* Each writer commits its rows in one snapshot. With `SINGLE_COMMIT = 'true'` the one writer makes exactly one commit, or none when it fails, and deletes the data files it wrote.
* Iceberg table spec § Schema Evolution: "Valid primitive type promotions are:" among them `int` to `long` and `decimal(P, S)` to `decimal(P', S) if P' > P`, "Widen precision only". The accepted-type table widens a decimal the same way, at an unchanged scale. A `DECIMAL(p,0)` source for an `int` or `long` column is no promotion: every value of the source type is a value of the table type. § Nested Types: "Each field can be either optional or required, meaning that values can (or cannot) be null."
* Iceberg table spec § Default values: "Omitting a known field when writing a data file is never allowed." and "The write default for a field must be written if a field is not supplied to a write." The append binds every table column, so every data file holds every table column and the export writes no default. § Parquet: "Column IDs are required to be stored as field IDs on the parquet schema.", so the name binding only selects which table field ID carries a source column's values.
* Iceberg table spec § Sorting: "Writers should use this default sort order to sort the data on write, but are not required to if the default order is prohibitively expensive, as it would be for streaming writes." The export streams each writer's rows into its data files in arrival order and does not sort them, even for a table that declares a sort order. Not sorting is this feature's scoped exception.
* Format versions 1 and 3 are out of scope. The export refuses such a table before it reads any source row, so it writes nothing that their rules govern.

## Scenarios

### Scenario: An export appends to a table that an earlier export created

* *GIVEN* a table that an export created and that holds 1,000 rows
* *WHEN* a user exports another source of 1,000 rows with the same column names into it
* *THEN* the statement's row count SHALL be 1,000
* *AND* the table, read through a virtual schema, SHALL hold all 2,000 rows, each exactly once
* *AND* the table SHALL keep every snapshot it had before the export

### Scenario: A table that another engine created takes the source rows by column name

* *GIVEN* a table that another engine created, whose lower-case column names come in another order than the source's upper-case names
* *AND* a source that names every table column
* *WHEN* a user exports the source into it
* *THEN* each source value SHALL read back in the table column of its name
* *AND* the table's schema SHALL be unchanged

### Scenario: A source type that the table column widens appends unchanged values

* *GIVEN* a table with a `long` column and a `decimal(12,2)` column
* *AND* a source that names every table column, with the types `DECIMAL(18,0)` and `DECIMAL(10,2)` for those two columns
* *WHEN* a user exports the source into the table
* *THEN* the export SHALL succeed
* *AND* every value read back SHALL equal its source value

### Scenario: Every column mismatch is reported in one error before any data is written

* *GIVEN* a table with a required column and an optional column
* *AND* a source that lacks both columns, has a column that the table lacks, and has a `DECIMAL(36,0)` column for a `long` table column
* *WHEN* a user exports the source into the table
* *THEN* the export SHALL fail with one error that names the table and each of the four columns
* *AND* the table SHALL keep its schema and snapshots unchanged
* *AND* the export SHALL write no data file

### Scenario: A table with a column that no source type appends to takes no append

* *GIVEN* a table with a `float` column and a struct column
* *AND* a source that names every table column
* *WHEN* a user exports the source into the table
* *THEN* the export SHALL fail with one error that names the table and both columns, each with its source type and its table column's type
* *AND* the table SHALL keep its snapshots unchanged

### Scenario: A table the export cannot append to is refused before any data is read

* *GIVEN* a source whose rows raise an error when they are read
* *WHEN* a user exports it into a table whose default partition spec has a field, or into a table of format version 1
* *THEN* the export SHALL fail before any source row is read, with an error that names the table and the partitioning or the format version
* *AND* the table SHALL keep its snapshots unchanged

### Scenario: SINGLE_COMMIT appends all rows in exactly one snapshot

* *GIVEN* a table that exists and a source of 1,000 rows
* *WHEN* a user exports the source into the table with `SINGLE_COMMIT = 'true'`
* *THEN* the table SHALL gain exactly one snapshot, which adds the 1,000 rows
* *AND* the table SHALL keep every snapshot it had before the export

### Scenario: A failed SINGLE_COMMIT append leaves the table unchanged

* *GIVEN* a table that exists and a source of 1,000 rows
* *WHEN* a user exports the source into the table with `SINGLE_COMMIT = 'true'`, and either the source raises an error at its 500th row, or a row holds NULL in a column bound to a required table column, or the writer fails after it has written its data files and before it commits
* *THEN* the statement SHALL fail, with the source's error when the source failed, or with an error that names the table and the required column when a row held NULL in it
* *AND* the table SHALL keep its snapshots unchanged
* *AND* the data files that the writer wrote SHALL NOT remain in storage

### Scenario: Parallel writers each append their rows once

* *GIVEN* a table that an export created and a source of 16,000 rows
* *WHEN* a user exports the source into the table on one node with `WRITERS_PER_NODE = '4'`
* *THEN* the statement's row count SHALL be 16,000
* *AND* the table SHALL gain one snapshot per writer, each carrying the same `lakehouse.export-id`
* *AND* the table SHALL hold every source row exactly once

### Scenario: An empty source appends nothing

* *GIVEN* a table that exists
* *AND* a query that returns no rows and whose columns do not match the table
* *WHEN* a user exports the query into the table
* *THEN* the statement SHALL succeed with a row count of 0
* *AND* the table SHALL keep its snapshots unchanged
