# Feature: Parquet Directory Seam

Answers "what are the data files under this storage prefix, what are their partition values, and
what is their combined schema" in ONE place, from an object store, a prefix, a file pattern, and
two layout switches. Table enumeration and query planning therefore read the same files, declare
the same partition columns, and fold the same footers. The two callers cannot disagree about a
table's columns. A caller whose catalog already declares the schema and the partition columns asks
the same seam for the file list alone.

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
* Per-file Parquet metadata is returned alongside the schema rather than discarded.
  `vs-adapter/direct-storage-statistics-pruning` prunes files from those footers at plan time. A
  seam that returned the schema alone would force that pruning to re-read every footer the fold
  already parsed.
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

### Scenario: One seam answers the file list and the schema for both callers

* *GIVEN* an object store, a storage prefix, a merge mode, a partitioning switch, and a file-keep predicate
* *WHEN* table enumeration and query planning each ask for that prefix's data files and schema
* *THEN* exactly ONE function SHALL answer both, returning the kept data files with each file's byte size and partition values, the declared partition columns, the folded Arrow schema, and each read file's parsed Parquet metadata
* *AND* that function SHALL name no catalog kind, no table format, no Exasol connection, and no virtual-schema property, so it takes two switch values rather than the properties that set them
* *AND* neither caller SHALL carry its own listing filter, its own footer reader, its own merge policy, or its own partition parser, because two policies over one property is the drift this seam exists to prevent
* *AND* the returned per-file metadata SHALL be the PARSED footer rather than the schema alone, so a later consumer that prunes files from footer statistics re-reads no footer for a file whose footer this call already read
* *AND* that no-re-read promise SHALL be SCOPED to the fold-every-file mode, which is the only mode under which every kept file has a parsed footer, and a consumer needing statistics for a file the sample-one-file mode left unread SHALL ask the SEAM for them rather than opening that footer behind the seam's back, so the seam stays the one place a footer is read
* *AND* a consumer that prunes from footer statistics MUST NOT use the statistics of a Parquet column the fold dropped for a partition-key collision (`vs-adapter/direct-storage-hive-partitioning`), because those statistics describe values the scan never emits
* *AND* every failure SHALL be returned as an error value rather than raised as a panic, and no error message SHALL contain a credential value
