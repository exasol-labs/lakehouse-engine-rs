# Feature: Parquet Directory Seam

Answers "what are the data files under this storage prefix, and what is their combined schema" in
ONE place, from an object store and a prefix alone. Table enumeration and query planning therefore
read the same files and fold the same footers. The two callers cannot disagree about a table's
columns.

## Background

* The seam names NO catalog kind, NO table format, and NO Exasol virtual-schema property. It takes
  an object store, a prefix, and a merge mode. Any later consumer that needs "the Parquet files
  under this prefix and their schema" calls it directly.
* It has TWO consumers today, and they ask the same question in different contexts. Table
  enumeration maps the folded schema to the neutral catalog columns the listing pipeline declares.
  Query planning maps it to the scan spec's logical fields. One implementation is what keeps
  `MERGE_SCHEMA` from needing a second policy per path.
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
* `key=value` path segments are split out of each file's path and returned unused. Acting on them
  is issue [#408](https://github.com/exasol-labs/lakehouse-engine-rs/issues/408).
* Apache Iceberg and Delta specification check: NOT implicated. This seam reads raw Parquet
  footers and implements neither table format. Its ONE point of contact with either specification
  is the widening pair set. The seam reads that set from `datafusion-scan/type-relaxation` rather
  than re-deriving it. That feature grounds the set in the Iceberg specification's § Schema
  Evolution promotion table (rows 1 to 3) and in the Delta protocol's § Reader Requirements for
  Type Widening.

## Scenarios

### Scenario: One seam answers the file list and the schema for both callers

* *GIVEN* an object store, a storage prefix, and a merge mode
* *WHEN* table enumeration and query planning each ask for that prefix's data files and schema
* *THEN* exactly ONE function SHALL answer both, taking the store, the prefix, and the mode, and returning the data-file list with each file's byte size, the folded Arrow schema, and each file's parsed Parquet metadata
* *AND* that function SHALL name no catalog kind, no table format, no Exasol connection, and no virtual-schema property, so it takes a store and a prefix rather than a configuration
* *AND* neither caller SHALL carry its own listing filter, its own footer reader, or its own merge policy, because two policies over one `MERGE_SCHEMA` value is the drift this seam exists to prevent
* *AND* the returned per-file metadata SHALL be the PARSED footer rather than the schema alone, so a later consumer that prunes files from footer statistics re-reads no footer for a file whose footer this call already read
* *AND* that no-re-read promise SHALL be SCOPED to the fold-every-file mode, which is the only mode under which every listed file has a parsed footer, and a consumer needing statistics for a file the sample-one-file mode left unread SHALL ask the SEAM for them rather than opening that footer behind the seam's back, so the seam stays the one place a footer is read
* *AND* every failure SHALL be returned as an error value rather than raised as a panic, and no error message SHALL contain a credential value

### Scenario: Data files are listed recursively in a deterministic order

* *GIVEN* a storage prefix holding `p1.parquet`, `a/p2.parquet`, `a/b/p3.parquet`, `_SUCCESS`, `_metadata`, `p1.parquet.crc`, `_staging/p4.parquet`, and `.hidden/p5.parquet`
* *WHEN* the seam lists that prefix's data files
* *THEN* it SHALL return `p1.parquet`, `a/p2.parquet`, and `a/b/p3.parquet`, recursing to unlimited depth, so a plain subdirectory contributes files rather than being skipped or becoming its own unit
* *AND* it SHALL return ONLY objects whose name ends in `.parquet`, so `_SUCCESS`, `_metadata`, and `p1.parquet.crc` are excluded by that rule alone
* *AND* it SHALL exclude every object any of whose path segments below the prefix begins with `_` or `.`, so `_staging/p4.parquet` and `.hidden/p5.parquet` are excluded even though their file names qualify
* *AND* it SHALL carry each returned file's byte size from the listing response, so no consumer issues an object-store HEAD for a size the listing already reported
* *AND* it SHALL return the files in a DETERMINISTIC order that does not depend on the store's listing order, because the merge mode that samples one footer must sample the same one on the enumeration path and the plan path
* *AND* it SHALL split each returned file's `key=value` path segments into a per-file map and SHALL leave that map UNREAD by this plan, so the parser exists once when issue #408 acts on it

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

### Scenario: The merge mode selects every footer or exactly one

* *GIVEN* the same prefix, once under the fold-every-file mode and once under the sample-one-file mode
* *WHEN* the seam resolves the schema
* *THEN* the fold-every-file mode SHALL read every listed file's footer, and the sample-one-file mode SHALL read EXACTLY ONE footer, that of the FIRST file in the deterministic listing order
* *AND* the sample-one-file mode SHALL return the same file LIST as the other mode, so the mode narrows which footers are read and never which files are scanned
* *AND* the returned per-file metadata SHALL be PAIRED with the files whose footers the mode actually read and SHALL be ABSENT for every other listed file, so the return carries a presence-per-file shape rather than one entry per listed file: the sample-one-file mode returns N files with metadata present for exactly ONE of them, and the fold-every-file mode returns N files with metadata present for all N
* *AND* a consumer SHALL read that PRESENCE rather than assume an entry exists, and MUST NOT index the metadata positionally against the file list, because the two sequences have different lengths under the sample-one-file mode
* *AND* the two modes SHALL be two values of ONE argument to ONE function, so both callers reach the same behaviour from the same code
* *AND* a prefix holding exactly one data file SHALL produce an IDENTICAL schema under both modes, so the mode is observable only where footers differ

### Scenario: The folded column set is the union of the files' column sets

* *GIVEN* a prefix whose first file carries columns `A` and `B` and whose second file carries `A` and `C`, under the fold-every-file mode
* *WHEN* the seam folds those footers
* *THEN* the folded schema SHALL carry `A`, `B`, and `C`, ordered by each column's first appearance in the deterministic listing order, so a golden encoding of one folded schema is stable across runs
* *AND* every folded column SHALL be declared NULLABLE, because a column absent from one file is filled with NULL for that file's rows and a column declared required but absent would instead fail the scan
* *AND* two columns whose names are equal after the declaration's uppercase fold SHALL fail the fold with an error naming both spellings and the files that carry them, because the declaration would otherwise advertise a duplicate column name and identity binding cannot bind two logical fields to one physical name
* *AND* the seam MUST NOT reorder, rename, or case-fold a column name it returns, so the logical name it produces is the name the scan binds against in each file

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
