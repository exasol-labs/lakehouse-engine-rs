# Feature: Parquet Directory Seam: File Listing

This feature covers how the Parquet directory seam lists the data files under a storage prefix. It lists recursively in a deterministic order, applies one of two file patterns, and excludes hidden segments and zero-length objects. It applies a file-keep predicate to each file's partition values before any footer is read, and it lists a catalog-registered location by its raw object key.

It also covers the listing answer, which serves a caller whose catalog already declares the schema and the partition columns. That answer returns the files and their partition values, reads no footer, and shares its file rules with the schema-resolving answer.

<!-- DELTA:CHANGED -->
## Background

* The seam names NO catalog kind, NO table format, and NO Exasol virtual-schema property. The
  listing answer takes an object store, a prefix, the caller's declared partition columns, and a
  file-keep predicate over partition values. Any later consumer that needs "the Parquet files under
  this prefix" calls it directly.
* It answers TWO questions. The schema-resolving answer serves direct-storage table enumeration,
  which maps the folded schema to the neutral catalog columns and their declarations
  (`direct-storage/parquet-directory-seam`). The listing answer serves every pushdown over Parquet
  files: direct-storage query planning, whose partition columns come from the column notes
  (`vs-adapter/column-source-notes`), and the catalog-declared Parquet reader
  (`unity-catalog/unity-parquet-table-planning`, `glue/glue-table-planning`), whose catalog
  declares the schema.
* The plain listing step, which applies the file pattern and the fixed hidden-segment rule, is
  separate from the folder-name partition inference layered on it. The Glue reader uses the plain
  step alone, because Glue supplies each partition's values.
* Hive-style partition discovery belongs to this seam. `direct-storage/direct-storage-hive-partitioning`
  specifies its rules, including the one key-name rule both answers apply: a key matches its column
  under the uppercase fold, and two spellings of one key fail.
* See `direct-storage/parquet-directory-seam` for the seam's single-function contract, the merge mode, and the schema fold over Parquet footers.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A file-keep predicate narrows the files before any footer is read

* *GIVEN* a prefix whose files sit under `year=2025/` and `year=2026/` directories, a caller that declares the partition column `year`, and a file-keep predicate that rejects every file whose `year` value is not `2026`
* *WHEN* the caller asks the listing answer for that prefix's files
* *THEN* the seam SHALL evaluate the predicate on each file's filled partition values and SHALL return only the kept files
* *AND* the seam SHALL read no footer, kept or rejected
* *AND* the schema-resolving answer SHALL take no file-keep predicate, so enumeration always declares a table from its complete listing
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The listing answer serves a caller that declares its own partition columns

* *GIVEN* a storage prefix holding `YEAR=2024/region=eu/a.parquet`, `YEAR=2025/b.parquet`, `other=x/c.parquet`, and `_SUCCESS`, and a caller that declares the partition columns `year` and `region`
* *WHEN* the caller asks the seam for that prefix's files against those declared columns
* *THEN* the seam SHALL return the files the recursive-listing rules of this feature select, in the same deterministic order, each with its byte size and its partition values
* *AND* each file's partition-value map SHALL carry EVERY declared column, keyed by the caller's spelling and filled from the path segment whose key equals that column under the uppercase fold, the deepest such segment winning, so `YEAR=2025` fills `year` and a column with no segment reads no value
* *AND* a segment naming no declared column SHALL contribute nothing, so `other=x` is a plain directory
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Two spellings of one declared partition key fail the listing answer

* *GIVEN* a storage prefix holding `year=2024/a.parquet` and `Year=2025/b.parquet`, and a caller that declares the partition column `year`
* *WHEN* the caller asks the seam for that prefix's files against that declared column
* *THEN* the seam SHALL fail with an error naming both spellings and the path of a file carrying each, per `direct-storage/direct-storage-hive-partitioning`
* *AND* the seam SHALL detect the two spellings before it applies the file-keep predicate, so a predicate that would reject one of the files does not hide the conflict
* *AND* two spellings of a key that names no declared column SHALL NOT fail, because such a segment contributes nothing
<!-- /DELTA:NEW -->
