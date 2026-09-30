<!-- DELTA:CHANGED -->
# Feature: Parquet Directory Seam

Answers "what are the data files under this storage prefix, what are their partition values, and
what is their combined schema" in ONE place, from an object store, a prefix, a file pattern, and
two layout switches. Table enumeration and query planning therefore read the same files, declare
the same partition columns, and fold the same footers. The two callers cannot disagree about a
table's columns. A caller whose catalog already declares the schema and the partition columns asks
the same seam for the file list alone.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The seam names NO catalog kind, NO table format, and NO Exasol virtual-schema property. It takes
  an object store, a prefix, a file pattern, a merge mode, a partitioning switch, and a file-keep
  predicate over partition values. Any later consumer that needs "the Parquet files under this
  prefix and their schema" calls it directly.
* It answers TWO questions. The schema-resolving answer serves direct-storage table enumeration,
  which maps the folded schema to the neutral catalog columns the listing pipeline declares, and
  direct-storage query planning, which maps it to the scan spec's logical fields. One
  implementation is what keeps `MERGE_SCHEMA` and `HIVE_PARTITIONING` from needing a second policy
  per path. The listing answer serves the catalog-declared Parquet reader
  (`vs-adapter/unity-parquet-table-planning`, `vs-adapter/glue-table-planning`), whose catalog
  declares the schema.
* The plain listing step, which applies the file pattern and the fixed hidden-segment rule, is
  separate from the folder-name partition inference layered on it. The Glue reader uses the plain
  step alone, because Glue supplies each partition's values.
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

<!-- DELTA:CHANGED -->
### Scenario: Data files are listed recursively in a deterministic order

* *GIVEN* a storage prefix holding `p1.parquet`, `a/p2.parquet`, `a/b/p3.parquet`, `_SUCCESS`, `_metadata`, `p1.parquet.crc`, `_staging/p4.parquet`, and `.hidden/p5.parquet`
* *WHEN* the seam lists that prefix's data files under the `**/*.parquet` file pattern
* *THEN* it SHALL return `p1.parquet`, `a/p2.parquet`, and `a/b/p3.parquet`, recursing to unlimited depth, so a plain subdirectory contributes files rather than being skipped or becoming its own unit
* *AND* it SHALL return ONLY objects whose name ends in `.parquet`, so `_SUCCESS`, `_metadata`, and `p1.parquet.crc` are excluded by that rule alone
* *AND* it SHALL exclude every object any of whose path segments below the prefix begins with `_` or `.`, so `_staging/p4.parquet` and `.hidden/p5.parquet` are excluded even though their file names qualify
* *AND* it SHALL carry each returned file's byte size from the listing response, so no consumer issues an object-store HEAD for a size the listing already reported
* *AND* it SHALL return the files in a DETERMINISTIC order that does not depend on the store's listing order, because the sample-one-file mode reads the same footer on the enumeration path and the plan path only when both see one order
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: The file pattern selects the listing depth and the file-name rule

* *GIVEN* a storage prefix holding `a.parquet`, `b` (no extension), `sub/c.parquet`, `sub/d`, `_SUCCESS`, `.hidden`, and a zero-length object `e.parquet`
* *WHEN* the seam lists that prefix under the file patterns `**/*.parquet`, `*.parquet`, and `*`
* *THEN* `**/*.parquet` SHALL return `a.parquet` and `sub/c.parquet`
* *AND* `*.parquet` SHALL return `a.parquet` alone, and `*` SHALL return `a.parquet` and `b`
* *AND* a pattern without a `**` segment SHALL list only the direct children of the prefix through a delimiter listing, so no object below a subdirectory is fetched
* *AND* under every pattern, an object with a segment below the prefix that begins with `_` or `.`, and a zero-length object, SHALL NOT be a data file
* *AND* the pattern SHALL be an internal parameter: direct storage and Unity Parquet pass `**/*.parquet`, Glue passes `*`, and no virtual-schema property sets it
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A catalog-registered location is listed by its raw object key

* *GIVEN* the Glue location `s3://bucket/tbl/p_str=a b%2Fc/` and an object stored at the literal key `tbl/p_str=a b%2Fc/f1`
* *WHEN* the seam derives the store prefix of that location as a raw key and lists it
* *THEN* the prefix SHALL be `tbl/p_str=a b%2Fc`, taken verbatim without percent-decoding, so the listing returns `f1`
* *AND* the path the seam returns for `f1` SHALL resolve back to the same object key through the scan's path reconstruction
* *AND* the seam SHALL decode a direct-storage or Unity Parquet storage URI as a URL
<!-- /DELTA:NEW -->
