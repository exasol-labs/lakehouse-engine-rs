<!-- DELTA:CHANGED -->
# Feature: Parquet Directory Seam

The Parquet directory seam answers three questions about a storage prefix in one place: which data files it holds, what their partition values are, and what their combined schema is. It takes an object store, a prefix, a file pattern, and two layout switches. Table enumeration calls its schema-resolving answer, and query planning calls only its listing answer and takes the schema from the column notes enumeration recorded, so the two callers cannot disagree about a table's columns.

This feature covers the seam's single-function contract, the merge mode that decides which footers are read, and how those footers fold into one schema: the column union, type widening across proven-castable pairs, and the JSON string declaration for nested and unrepresentable types.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* The seam names NO catalog kind, NO table format, and NO Exasol virtual-schema property. Its
  schema-resolving answer takes an object store, a prefix, a merge mode, and a partitioning switch.
  Any later consumer that needs "the Parquet files under this prefix and their schema" calls it
  directly.
* It answers TWO questions. The schema-resolving answer serves direct-storage table enumeration,
  which maps the folded schema to the neutral catalog columns and to each column's declaration that
  the listing pipeline records as the column's note (`vs-adapter/column-source-notes`). One
  implementation is what keeps `MERGE_SCHEMA` and `HIVE_PARTITIONING` from needing a second policy.
  The listing answer serves every pushdown over Parquet files: direct-storage query planning and
  the catalog-declared Parquet reader (`unity-catalog/unity-parquet-table-planning`,
  `glue/glue-table-planning`), each of which takes its schema from the column notes.
* The widening rules the fold applies are NOT new. `scan-types/type-relaxation` already owns
  the set of physical-to-logical pairs this engine casts at scan time. That set is proven castable
  pair by pair and grounded in the Apache Iceberg and Delta promotion tables. The fold picks the
  wider member of a pair from that set and nothing else. Every column the fold widens is therefore
  a column the scan can already read.
* The scan side needs no new mechanism. A logical field carrying neither a field-id nor a declared
  physical name binds by its own name. That is the identity binding
  `scan-read-path/scan-execution-field-id-projection` already specifies. The same column-binding
  adapter that serves Iceberg and Delta inserts the per-file cast the widened declaration implies.
* The schema-resolving answer returns no per-file Parquet metadata. No pushdown reads a footer, so
  footer-statistics pruning (#412) is closed as superseded, per
  `direct-storage/direct-storage-table-planning`.
* Hive-style partition discovery belongs to this seam. `direct-storage/direct-storage-hive-partitioning`
  specifies its rules.
* Apache Iceberg and Delta specification check: NOT implicated. This seam reads raw Parquet
  footers and implements neither table format. Its ONE point of contact with either specification
  is the widening pair set. The seam reads that set from `scan-types/type-relaxation` rather
  than re-deriving it. That feature grounds the set in the Iceberg specification's § Schema
  Evolution promotion table (rows 1 to 3) and in the Delta protocol's § Reader Requirements for
  Type Widening.
* See `direct-storage/parquet-directory-seam-file-listing` for the recursive file listing, the file patterns, the file-keep predicate, and the listing answer for a caller that declares its own schema.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
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
<!-- /DELTA:REMOVED -->

<!-- DELTA:NEW -->
### Scenario: One seam answers the file list and the folded schema for enumeration

* *GIVEN* an object store, a storage prefix, a merge mode, and a partitioning switch
* *WHEN* table enumeration asks for that prefix's data files and schema
* *THEN* exactly ONE function SHALL answer, returning every data file with its byte size and partition values, the declared partition columns, the folded Arrow schema, and the binary columns the fold found
* *AND* that function SHALL name no catalog kind, no table format, no Exasol connection, and no virtual-schema property, so it takes two switch values rather than the properties that set them
* *AND* that function SHALL take no file-keep predicate, because enumeration keeps every file and no pushdown calls it
* *AND* no caller SHALL carry its own listing filter, its own footer reader, its own merge policy, or its own partition parser, because two policies over one property is the drift this seam exists to prevent
* *AND* every failure SHALL be returned as an error value rather than raised as a panic, and no error message SHALL contain a credential value
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: The merge mode selects every footer or exactly one

* *GIVEN* the same prefix, once under the fold-every-file mode and once under the sample-one-file mode
* *WHEN* the seam resolves the schema
* *THEN* the fold-every-file mode SHALL read every listed file's footer, and the sample-one-file mode SHALL read EXACTLY ONE footer, that of the FIRST file in the deterministic listing order
* *AND* the sample-one-file mode SHALL return the same file LIST as the other mode, so the mode narrows which footers are read and never which files are listed
* *AND* the two modes SHALL be two values of ONE argument to ONE function, so every caller reaches the same behaviour from the same code
* *AND* a prefix holding exactly one data file SHALL produce an IDENTICAL schema under both modes, so the mode is observable only where footers differ
<!-- /DELTA:CHANGED -->

<!-- DELTA:REMOVED -->
### Scenario: The declaration decides the emitted width and the footer decides the structure

* *GIVEN* a virtual schema whose declared column types were folded over the WHOLE table at enumeration time, and a later query whose plan-time footer set is NARROWER than that whole table because the merge mode sampled one file or because a later change prunes files before planning
* *WHEN* the planning caller builds the scan's logical fields from the seam's result and the scan emits its rows
* *THEN* the scan's logical field for each column SHALL carry the Arrow type the PLAN-TIME fold resolved, after the string substitution the JSON-fallback scenario above states for a nested or unrepresentable column, exactly as the Iceberg and Delta readers carry the type their own source resolved, so this kind introduces no second rule for deriving a logical type
* *AND* the DECLARED Exasol type SHALL still decide what Exasol receives, because the emit boundary already coerces every column to the declared `EMITS` type, so a plan-time type narrower than the declaration reaches Exasol at the DECLARED width and the narrower plan-time fold cannot narrow a stored declaration
* *AND* the footer set SHALL be the sole source of each column's STRUCTURE and nested members, because the declared type of a nested column is `VARCHAR(2000000)` and carries none
* *AND* a plan-time fold WIDER than the declaration SHALL be treated as the ordinary stale-declaration case the recorded relaxation rules already own, resolved by `REFRESH VIRTUAL SCHEMA` rather than by any planning-side compensation, so this kind adds no new staleness mechanism
* *AND* this division SHALL be recorded as a property of the seam rather than of one caller, so a later reader does not read the resulting asymmetry as a defect and repair it by narrowing the declaration to the plan-time fold
<!-- /DELTA:REMOVED -->
