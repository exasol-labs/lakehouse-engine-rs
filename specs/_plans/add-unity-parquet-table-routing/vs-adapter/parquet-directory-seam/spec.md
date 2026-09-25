<!-- DELTA:CHANGED -->
# Feature: Parquet Directory Seam

Answers "what are the data files under this storage prefix, what are their partition values, and
what is their combined schema" in ONE place, from an object store, a prefix, and two layout
switches. Table enumeration and query planning therefore read the same files, declare the same
partition columns, and fold the same footers. The two callers cannot disagree about a table's
columns. A caller whose catalog already declares the schema and the partition columns asks the
same seam for the file list alone.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The seam names NO catalog kind, NO table format, and NO Exasol virtual-schema property. It takes
  an object store, a prefix, a merge mode, a partitioning switch, and a file-keep predicate over
  partition values. Any later consumer that needs "the Parquet files under this prefix and their
  schema" calls it directly.
* It answers TWO questions. The schema-resolving answer serves direct-storage table enumeration,
  which maps the folded schema to the neutral catalog columns the listing pipeline declares, and
  direct-storage query planning, which maps it to the scan spec's logical fields. One
  implementation is what keeps `MERGE_SCHEMA` and `HIVE_PARTITIONING` from needing a second policy
  per path. The listing answer serves the Unity Catalog Parquet reader
  (`vs-adapter/unity-parquet-table-planning`), whose catalog declares the schema.
* The widening rules the fold applies are NOT new. `datafusion-scan/type-relaxation` already owns
  the set of physical-to-logical pairs this engine casts at scan time. That set is proven castable
  pair by pair and grounded in the Apache Iceberg and Delta promotion tables. The fold picks the
  wider member of a pair from that set and nothing else. Every column the fold widens is therefore
  a column the scan can already read.
* The scan side needs no new mechanism. A logical field carrying neither a field-id nor a declared
  physical name binds by its own name. That is the identity binding
  `datafusion-scan/scan-execution-field-id-projection` already specifies. The same column-binding
  adapter that serves Iceberg and Delta inserts the per-file cast the widened declaration implies.
* Per-file Parquet metadata is returned alongside the schema rather than discarded. Plan-time file
  pruning from footer statistics is issue
  [#412](https://github.com/exasol-labs/lakehouse-engine-rs/issues/412). A seam that returned
  the schema alone would force that plan to re-read every footer it had already parsed.
* Hive-style partition discovery belongs to this seam. `vs-adapter/direct-storage-hive-partitioning`
  specifies its rules.
* Apache Iceberg and Delta specification check: NOT implicated. This seam reads raw Parquet
  footers and implements neither table format. Its ONE point of contact with either specification
  is the widening pair set. The seam reads that set from `datafusion-scan/type-relaxation` rather
  than re-deriving it. That feature grounds the set in the Iceberg specification's § Schema
  Evolution promotion table (rows 1 to 3) and in the Delta protocol's § Reader Requirements for
  Type Widening.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: The listing answer serves a caller that declares its own partition columns

* *GIVEN* a storage prefix holding `year=2024/region=eu/a.parquet`, `Year=2025/b.parquet`, `other=x/c.parquet`, and `_SUCCESS`, and a caller that declares the partition columns `year` and `region`
* *WHEN* the caller asks the seam for that prefix's files against those declared columns
* *THEN* the seam SHALL return the files the recursive-listing rules of this feature select, in the same deterministic order, each with its byte size and its partition values
* *AND* each file's partition-value map SHALL carry EVERY declared column, keyed by the caller's spelling and filled from the path segment whose key equals that column under the uppercase fold, the deepest such segment winning, so `Year=2025` fills `year` and a column with no segment reads no value
* *AND* a segment naming no declared column SHALL contribute nothing, so `other=x` is a plain directory
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The listing answer reads no footer and shares its file rules with the schema-resolving answer

* *GIVEN* the same declaring caller and storage prefix
* *WHEN* the caller asks the seam for that prefix's files against its declared columns
* *THEN* the seam SHALL read NO footer, fold NO schema, and declare NO partition column from the paths, because the caller owns the schema, and the file-keep predicate SHALL run on the filled values before the files are returned, exactly as it does for the schema-resolving answer
* *AND* the listing answer SHALL share the listing, the data-file rule, the segment parser, and the value decoding with the schema-resolving answer, so the two answers cannot disagree about which objects are data files or how a segment value decodes
<!-- /DELTA:NEW -->
