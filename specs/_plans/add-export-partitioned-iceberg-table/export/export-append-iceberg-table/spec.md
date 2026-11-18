# Feature: EXPORT INTO SCRIPT Into an Existing Iceberg Table

When the table that `TABLE_NAME` names exists, the export appends the source rows to it. It matches the source columns to the table's columns by name, accepts the source types that § Background lists for each table column type, and never changes the table's existing data or schema.

<!-- DELTA:CHANGED -->
## Background

* Statement and options: export/export-options § Background. Catalog, storage, and credentials: export/export-catalog-storage § Background. The `lakehouse.export-id` snapshot property: export/export-iceberg-table § Background. Read through a virtual schema: export/export-into-script § Background.
* The export checks whether the table exists before it reads any source row. If the table exists, the export appends to it. An append only adds snapshots: the table's earlier snapshots and its schema stay as they are.
* Column match. An export column matches the table column of the same name. An export column with no such table column matches the one remaining table column whose name differs from it only in letter case, so an unquoted Exasol name such as `ID` matches `id`. A remaining table column is one that no export column matches exactly. A name is ambiguous when it matches, only by letter case, several remaining columns on the other side. So export columns `CODE` and `Code` into table columns `code` and `Code` match `Code` exactly, and `CODE` then matches the one remaining column `code`. Column order does not matter.
* A column of an existing table accepts an export column of these types. A table that the export creates uses the mapping in export/export-iceberg-table § Background.

  | Exasol type | Iceberg column type |
  |-------------|---------------------|
  | `DECIMAL(p,s)` | `decimal(P,s)` with P ≥ p |
  | `DECIMAL(p,0)` | `int`, `long` |
  | `DOUBLE` | `double` |
  | `BOOLEAN` | `boolean` |
  | `VARCHAR(n)`, `CHAR(n)` | `string` |
  | `DATE` | `date` |
  | `TIMESTAMP(p)`, p ≤ 6 | `timestamp`, `timestamptz` |

  Every other pair is a type mismatch. No Exasol type fills a `float`, `timestamp_ns`, `timestamptz_ns`, `time`, `uuid`, `fixed`, `binary`, `struct`, `list`, or `map` column.
* An `int` or `long` column takes a `DECIMAL(p,0)` of any precision, so Exasol `INTEGER` and `BIGINT` fill both, and a value outside the column's range fails the export. A `timestamptz` column takes a `TIMESTAMP` value as a date and time in UTC, the inverse of how a virtual schema reads a `timestamptz` column.
* A column mismatch is a table column that no export column matches, even an optional one, an export column that matches no table column, a matched pair whose types § Background does not accept, or an ambiguous name. A table column that an ambiguous name matches is listed under that ambiguity only, not as unmatched. One error names the table and lists every mismatch, with the names of its columns and the Exasol type of each export column and the Iceberg type of each table column.
* The export refuses a table of format version 3 or later, with an error that names the table and its format version. It appends to a partitioned table as export/export-partitioned-iceberg-table states.
* The export also refuses a table whose current partition spec has a `void` field named like a table column, the shape Apache Spark leaves when it drops an identity partition field of a format version 1 table. The error names the table and the field.
* The export checks the table and the columns before it writes any data, and only when the source has rows (export/export-into-script § Background). Every writer checks the same table, so a column mismatch fails the export before any write in every mode.
* A value failure is a NULL for a required column or a value outside an `int` or `long` column's range. Each writer finds it only in its own rows, and the error names the table and the column of the first such value. The one-error mismatch list covers column mismatches only. With `SINGLE_COMMIT` a value failure leaves the table unchanged. Without it, writers that committed before the failure keep their commits.
* An append writes into the schema and partition spec that the writer checked. Every commit asserts the table's current schema ID and default partition spec ID (Iceberg REST `assert-current-schema-id`, `assert-default-spec-id`). Each commit reloads the table before its first attempt and before every retry. A reload that shows another current schema ID, another default partition spec ID, or format version 3 or later fails the commit with an error that names the table and what changed, and adds no snapshot. Iceberg REST has no requirement on the format version, so an upgrade to version 3 that lands between that reload and the commit, and changes neither the snapshot, the schema, nor the partition spec, is not detected: this is a named residual risk.
* All writers of an append commit to the one table, so an append retries a commit up to the table's `commit.retry.num-retries`, and at least 20 times, as often as on a table the export creates.
* Iceberg table spec § Primitive Types: `int` holds "32-bit signed integers" and `long` "64-bit signed integers", so the export checks each `DECIMAL(p,0)` value against that range. `timestamptz` is "Timestamp, microsecond precision, with timezone", whose values "are stored as UTC and do not retain a source time zone". § Schema Evolution promotes `decimal(P, S)` to `decimal(P', S)` "if `P' > P`" ("Widen precision only") and `float` to `double`, never `double` to `float`. § Column Projection: "Columns in Iceberg data files are selected by field id. The table schema's column names and order may change after a data file is written", so a column added again under the same name has a new field ID and reads files written under the old one as NULL. § Default values: "Omitting a known field when writing a data file is never allowed." § Partition Transforms: `void` "Always produces `null`", and a version 1 table drops a partition field by replacing its transform with `void`. § Format Versioning lists "Row Lineage tracking" and "Table encryption keys" for version 3: refusing version 3 tables is this feature's scoped exception.
<!-- /DELTA:CHANGED -->

## Scenarios

### Scenario: An export appends its rows and leaves the table's data unchanged

* *GIVEN* a table that an earlier export created and filled
* *WHEN* a user exports a source whose columns name the table's columns in another order
* *THEN* the statement's row count SHALL equal the source row count
* *AND* the table, read through a virtual schema, SHALL hold its earlier rows unchanged and every source row once
* *AND* the table SHALL keep its earlier snapshots and column types, and each new snapshot SHALL carry the export's `lakehouse.export-id`

<!-- DELTA:REMOVED -->
### Scenario: A partitioned or format-version 3 table is refused before any data is written

* *GIVEN* a table whose current partition spec has a field with a transform other than `void`, or a table of format version 3
* *WHEN* a user exports a source with rows into it
* *THEN* the export SHALL fail with an error that names the table and its partition fields, or its format version
* *AND* the table SHALL keep its snapshots unchanged
* *AND* the export SHALL write no data file
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: A format-version 3 table is refused before any data is written

* *GIVEN* a table of format version 3
* *WHEN* a user exports a source with rows into it
* *THEN* the export SHALL fail with an error that names the table and its format version
* *AND* the table SHALL keep its snapshots unchanged
* *AND* the export SHALL write no data file
<!-- /DELTA:NEW -->
