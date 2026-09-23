<!-- DELTA:CHANGED -->
# Feature: Parquet Directory Seam

Answers "what are the data files under this storage prefix, what are their partition values, and
what is their combined schema" in ONE place, from an object store, a prefix, and two layout
switches. Table enumeration and query planning therefore read the same files, declare the same
partition columns, and fold the same footers. The two callers cannot disagree about a table's
columns.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The seam names NO catalog kind, NO table format, and NO Exasol virtual-schema property. It takes
  an object store, a prefix, a merge mode, a partitioning switch, and a file-keep predicate over
  partition values. Any later consumer that needs "the Parquet files under this prefix and their
  schema" calls it directly.
* It has TWO consumers today, and they ask the same question in different contexts. Table
  enumeration maps the folded schema to the neutral catalog columns the listing pipeline declares.
  Query planning maps it to the scan spec's logical fields. One implementation is what keeps
  `MERGE_SCHEMA` and `HIVE_PARTITIONING` from needing a second policy per path.
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
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Data files are listed recursively in a deterministic order

* *GIVEN* a storage prefix holding `p1.parquet`, `a/p2.parquet`, `a/b/p3.parquet`, `_SUCCESS`, `_metadata`, `p1.parquet.crc`, `_staging/p4.parquet`, and `.hidden/p5.parquet`
* *WHEN* the seam lists that prefix's data files
* *THEN* it SHALL return `p1.parquet`, `a/p2.parquet`, and `a/b/p3.parquet`, recursing to unlimited depth, so a plain subdirectory contributes files rather than being skipped or becoming its own unit
* *AND* it SHALL return ONLY objects whose name ends in `.parquet`, so `_SUCCESS`, `_metadata`, and `p1.parquet.crc` are excluded by that rule alone
* *AND* it SHALL exclude every object any of whose path segments below the prefix begins with `_` or `.`, so `_staging/p4.parquet` and `.hidden/p5.parquet` are excluded even though their file names qualify
* *AND* it SHALL carry each returned file's byte size from the listing response, so no consumer issues an object-store HEAD for a size the listing already reported
* *AND* it SHALL return the files in a DETERMINISTIC order that does not depend on the store's listing order, because the sample-one-file mode reads the same footer on the enumeration path and the plan path only when both see one order
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The merge mode selects every footer or exactly one

* *GIVEN* the same prefix, once under the fold-every-file mode and once under the sample-one-file mode
* *WHEN* the seam resolves the schema
* *THEN* the fold-every-file mode SHALL read every kept file's footer, and the sample-one-file mode SHALL read EXACTLY ONE footer, that of the FIRST file in the deterministic listing order
* *AND* the sample-one-file mode SHALL return the same file LIST as the other mode, so the mode narrows which footers are read and never which files are scanned
* *AND* the returned per-file metadata SHALL be PAIRED with the files whose footers the mode actually read and SHALL be ABSENT for every other returned file, so the return carries a presence-per-file shape rather than one entry per listed file: with every file kept, the sample-one-file mode returns N files with metadata present for exactly ONE of them, and the fold-every-file mode returns N files with metadata present for all N
* *AND* a consumer SHALL read that PRESENCE rather than assume an entry exists, and MUST NOT index the metadata positionally against the file list, because the two sequences have different lengths under the sample-one-file mode
* *AND* the two modes SHALL be two values of ONE argument to ONE function, so both callers reach the same behaviour from the same code
* *AND* a prefix holding exactly one data file SHALL produce an IDENTICAL schema under both modes, so the mode is observable only where footers differ
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: The folded column set is the union of the files' column sets

* *GIVEN* a prefix whose first file carries columns `A` and `B` and whose second file carries `A` and `C`, under the fold-every-file mode
* *WHEN* the seam folds those footers
* *THEN* the folded schema SHALL carry `A`, `B`, and `C`, ordered by each column's first appearance in the deterministic listing order, so a golden encoding of one folded schema is stable across runs
* *AND* every folded column SHALL be declared NULLABLE, because a column absent from one file is filled with NULL for that file's rows and a column declared required but absent would instead fail the scan
* *AND* two columns whose names are equal after the declaration's uppercase fold SHALL fail the fold with an error naming both spellings and the files that carry them, because the declaration would otherwise advertise a duplicate column name and identity binding cannot bind two logical fields to one physical name
* *AND* a Parquet column whose uppercase fold equals a declared partition key SHALL be EXEMPT from this union and from the collision failure above: it SHALL be dropped rather than added, per the collision rule `vs-adapter/direct-storage-hive-partitioning` specifies, so the union described above names only columns that name no declared key
* *AND* the seam MUST NOT reorder, rename, or case-fold a column name it returns, so the logical name it produces is the name the scan binds against in each file
* *AND* the seam's returned schema SHALL end with the declared partition columns, appended after every column of the union above
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Footers fold into one schema under the proven-castable widening pairs

* *GIVEN* a prefix whose data files declare one column as a 32-bit integer in one file and a 64-bit integer in another, a second column as a 32-bit float and a 64-bit float, and a third column as `decimal(10,2)` and `decimal(12,2)`
* *AND* the merge mode that folds every file
* *WHEN* the seam folds those footers
* *THEN* it SHALL read every file's footer and SHALL resolve each column to the WIDER member of the pair, so the folded schema carries a 64-bit integer, a 64-bit float, and `decimal(12,2)`
* *AND* it SHALL widen ONLY across pairs the recorded relaxation set already proves castable, reading that set from its ONE owner and MUST NOT carry its own pair table, because a pair the fold invented would declare a type the scan cannot cast a file up to
* *AND* it SHALL fold a column PAIRWISE across files in the deterministic listing order, so a column appearing at three types resolves to the widest one reachable by successive supported widenings
* *AND* it SHALL read the footers CONCURRENTLY through the caller's store, bounded by the admission limiter that store carries, so a prefix holding many files costs one bounded fan-out rather than one serialized round-trip per file
* *AND* a column whose two declared types are covered by NO supported pair SHALL fail the fold with an error naming the column, BOTH conflicting types, and BOTH file paths, so an operator can locate the offending files without listing the prefix
* *AND* the fold MUST NOT resolve such a conflict by declaring the column as a string, dropping it, or taking one file's type, because each of those answers a correctness question by guessing
* *AND* a Parquet column whose uppercase fold equals a declared partition key SHALL be EXEMPT from this widening check: it SHALL be dropped before types are compared, per the collision rule `vs-adapter/direct-storage-hive-partitioning` specifies, so two files that disagree on that column's own type never reach this check
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A file-keep predicate narrows the files before any footer is read

* *GIVEN* a prefix whose files sit under `year=2025/` and `year=2026/` directories, and a file-keep predicate that rejects every file whose `year` value is not `2026`
* *WHEN* the seam resolves that prefix
* *THEN* it SHALL declare the partition columns from the UNFILTERED listing, so the declared columns never depend on the predicate
* *AND* it SHALL evaluate the predicate on each file's partition values before it reads any footer, and SHALL return only the kept files
* *AND* the fold-every-file mode SHALL read the footers of the kept files alone, so a rejected file costs no footer read
* *AND* the sample-one-file mode SHALL still read the footer of the first file in the UNFILTERED listing, kept or not, so the enumeration path and the plan path sample the same file
* *AND* table enumeration SHALL pass a predicate that keeps every file
<!-- /DELTA:NEW -->
