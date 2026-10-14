# Feature: Parquet Directory Seam

The Parquet directory seam answers three questions about a storage prefix in one place: which data files it holds, what their partition values are, and what their combined schema is. It takes an object store, a prefix, a file pattern, and two layout switches. Table enumeration and query planning both call it, so they read the same files, declare the same partition columns, and fold the same footers, and the two callers cannot disagree about a table's columns.

This feature covers the seam's single-function contract, the merge mode that decides which footers are read, and how those footers fold into one schema: the column union, type widening across proven-castable pairs, the JSON string declaration for nested and unrepresentable types, and the split between the declared Exasol type and the plan-time footer type.

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
  (`unity-catalog/unity-parquet-table-planning`, `glue/glue-table-planning`), whose catalog
  declares the schema.
* The widening rules the fold applies are NOT new. `scan-types/type-relaxation` already owns
  the set of physical-to-logical pairs this engine casts at scan time. That set is proven castable
  pair by pair and grounded in the Apache Iceberg and Delta promotion tables. The fold picks the
  wider member of a pair from that set and nothing else. Every column the fold widens is therefore
  a column the scan can already read.
* The scan side needs no new mechanism. A logical field carrying neither a field-id nor a declared
  physical name binds by its own name. That is the identity binding
  `scan-read-path/scan-execution-field-id-projection` already specifies. The same column-binding
  adapter that serves Iceberg and Delta inserts the per-file cast the widened declaration implies.
* Per-file Parquet metadata is returned alongside the schema rather than discarded. Plan-time file
  pruning from footer statistics is issue
  [#412](https://github.com/exasol-labs/lakehouse-engine-rs/issues/412). A seam that returned
  the schema alone would force that plan to re-read every footer it had already parsed.
* Hive-style partition discovery belongs to this seam. `direct-storage/direct-storage-hive-partitioning`
  specifies its rules.
* Apache Iceberg and Delta specification check: NOT implicated. This seam reads raw Parquet
  footers and implements neither table format. Its ONE point of contact with either specification
  is the widening pair set. The seam reads that set from `scan-types/type-relaxation` rather
  than re-deriving it. That feature grounds the set in the Iceberg specification's § Schema
  Evolution promotion table (rows 1 to 3) and in the Delta protocol's § Reader Requirements for
  Type Widening.
* See `direct-storage/parquet-directory-seam-file-listing` for the recursive file listing, the file patterns, the file-keep predicate, and the listing answer for a caller that declares its own schema.

## Scenarios

### Scenario: One seam answers the file list and the schema for both callers

* *GIVEN* an object store, a storage prefix, a merge mode, a partitioning switch, and a file-keep predicate
* *WHEN* table enumeration and query planning each ask for that prefix's data files and schema
* *THEN* exactly ONE function SHALL answer both, returning the kept data files with each file's byte size and partition values, the declared partition columns, the folded Arrow schema, and each read file's parsed Parquet metadata
* *AND* that function SHALL name no catalog kind, no table format, no Exasol connection, and no virtual-schema property, so it takes two switch values rather than the properties that set them
* *AND* neither caller SHALL carry its own listing filter, its own footer reader, its own merge policy, or its own partition parser, because two policies over one property is the drift this seam exists to prevent
* *AND* the returned per-file metadata SHALL be the PARSED footer rather than the schema alone, so a later consumer that prunes files from footer statistics re-reads no footer for a file whose footer this call already read
* *AND* that no-re-read promise SHALL be SCOPED to the fold-every-file mode, which is the only mode under which every kept file has a parsed footer, and a consumer needing statistics for a file the sample-one-file mode left unread SHALL ask the SEAM for them rather than opening that footer behind the seam's back, so the seam stays the one place a footer is read
* *AND* a consumer that prunes from footer statistics MUST NOT use the statistics of a Parquet column the fold dropped for a partition-key collision (`direct-storage/direct-storage-hive-partitioning`), because those statistics describe values the scan never emits
* *AND* every failure SHALL be returned as an error value rather than raised as a panic, and no error message SHALL contain a credential value

### Scenario: The merge mode selects every footer or exactly one

* *GIVEN* the same prefix, once under the fold-every-file mode and once under the sample-one-file mode
* *WHEN* the seam resolves the schema
* *THEN* the fold-every-file mode SHALL read every kept file's footer, and the sample-one-file mode SHALL read EXACTLY ONE footer, that of the FIRST file in the deterministic listing order
* *AND* the sample-one-file mode SHALL return the same file LIST as the other mode, so the mode narrows which footers are read and never which files are scanned
* *AND* the returned per-file metadata SHALL be PAIRED with the files whose footers the mode actually read and SHALL be ABSENT for every other returned file, so the return carries a presence-per-file shape rather than one entry per listed file: with every file kept, the sample-one-file mode returns N files with metadata present for exactly ONE of them, and the fold-every-file mode returns N files with metadata present for all N
* *AND* a consumer SHALL read that PRESENCE rather than assume an entry exists, and MUST NOT index the metadata positionally against the file list, because the two sequences have different lengths under the sample-one-file mode
* *AND* the two modes SHALL be two values of ONE argument to ONE function, so both callers reach the same behaviour from the same code
* *AND* a prefix holding exactly one data file SHALL produce an IDENTICAL schema under both modes, so the mode is observable only where footers differ

### Scenario: The folded column set is the union of the files' column sets

* *GIVEN* a prefix whose first file carries columns `A` and `B` and whose second file carries `A` and `C`, under the fold-every-file mode
* *WHEN* the seam folds those footers
* *THEN* the folded schema SHALL carry `A`, `B`, and `C`, ordered by each column's first appearance in the deterministic listing order, so a golden encoding of one folded schema is stable across runs
* *AND* every folded column SHALL be declared NULLABLE, because a column absent from one file is filled with NULL for that file's rows and a column declared required but absent would instead fail the scan
* *AND* two columns whose names are equal after the declaration's uppercase fold SHALL fail the fold with an error naming both spellings and the files that carry them, because the declaration would otherwise advertise a duplicate column name and identity binding cannot bind two logical fields to one physical name
* *AND* a Parquet column whose uppercase fold equals a declared partition key SHALL be EXEMPT from this union and from the collision failure above: it SHALL be dropped rather than added, per the collision rule `direct-storage/direct-storage-hive-partitioning` specifies, so the union described above names only columns that name no declared key
* *AND* the seam MUST NOT reorder, rename, or case-fold a column name it returns, so the logical name it produces is the name the scan binds against in each file
* *AND* the seam's returned schema SHALL end with the declared partition columns, appended after every column of the union above

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
* *AND* a Parquet column whose uppercase fold equals a declared partition key SHALL be EXEMPT from this widening check: it SHALL be dropped before types are compared, per the collision rule `direct-storage/direct-storage-hive-partitioning` specifies, so two files that disagree on that column's own type never reach this check

### Scenario: A nested or unrepresentable Parquet type folds to the JSON string declaration

* *GIVEN* a prefix whose files carry a struct column, a list column, a map column, and a column whose Parquet type Exasol cannot represent directly
* *WHEN* the seam folds those footers and the caller maps the result to the scan's logical fields
* *THEN* each such column SHALL be declared `VARCHAR(2000000)` and SHALL carry the string Arrow-type tag, read from the SAME Arrow classifier the engine already owns rather than from a second classification in this seam
* *AND* each struct, list, and map column SHALL ADDITIONALLY carry the format-neutral nested descriptor the JSON renderer consumes, naming every nested member's logical name recursively, because the renderer is selected by that descriptor's presence and a nested column reaching the cast path instead would fail with no string kernel
* *AND* each nested member SHALL carry NO binding key, matching the identity binding the top-level fields carry, so nested resolution is by name throughout
* *AND* a decimal column whose precision or scale falls outside Exasol's decimal domain SHALL be declared `VARCHAR(2000000)` with the string Arrow tag, decided by the SAME ONE Arrow classifier that already answers both the Exasol type string and the JSON-fallback flag, so the declared Exasol type and the logical Arrow tag stay in lockstep and this caller adds no second classification

### Scenario: The declaration decides the emitted width and the footer decides the structure

* *GIVEN* a virtual schema whose declared column types were folded over the WHOLE table at enumeration time, and a later query whose plan-time footer set is NARROWER than that whole table because the merge mode sampled one file or because a later change prunes files before planning
* *WHEN* the planning caller builds the scan's logical fields from the seam's result and the scan emits its rows
* *THEN* the scan's logical field for each column SHALL carry the Arrow type the PLAN-TIME fold resolved, after the string substitution the JSON-fallback scenario above states for a nested or unrepresentable column, exactly as the Iceberg and Delta readers carry the type their own source resolved, so this kind introduces no second rule for deriving a logical type
* *AND* the DECLARED Exasol type SHALL still decide what Exasol receives, because the emit boundary already coerces every column to the declared `EMITS` type, so a plan-time type narrower than the declaration reaches Exasol at the DECLARED width and the narrower plan-time fold cannot narrow a stored declaration
* *AND* the footer set SHALL be the sole source of each column's STRUCTURE and nested members, because the declared type of a nested column is `VARCHAR(2000000)` and carries none
* *AND* a plan-time fold WIDER than the declaration SHALL be treated as the ordinary stale-declaration case the recorded relaxation rules already own, resolved by `REFRESH VIRTUAL SCHEMA` rather than by any planning-side compensation, so this kind adds no new staleness mechanism
* *AND* this division SHALL be recorded as a property of the seam rather than of one caller, so a later reader does not read the resulting asymmetry as a defect and repair it by narrowing the declaration to the plan-time fold
