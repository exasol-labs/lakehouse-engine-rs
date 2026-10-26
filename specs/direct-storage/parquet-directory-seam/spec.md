# Feature: Parquet Directory Seam

The Parquet directory seam answers three questions about a storage prefix: which data files it holds, what their partition values are, and what their combined schema is. It takes an object store, a prefix, a file pattern, and two layout switches. Table enumeration calls its schema-resolving answer, and query planning calls its listing answer and takes the schema from the recorded column notes, so the two callers cannot disagree about a table's columns.

## Background

* The seam names no catalog kind, no table format, and no Exasol virtual-schema property. Its schema-resolving answer takes an object store, a prefix, a merge mode, and a partitioning switch. It maps the folded schema to the neutral catalog columns and to each column's declaration, which the listing pipeline records as the column's note (`vs-adapter/column-source-notes`). One implementation keeps `MERGE_SCHEMA` and `HIVE_PARTITIONING` from needing a second policy.
* The listing answer serves every pushdown over Parquet files (`direct-storage/direct-storage-table-planning`, `unity-catalog/unity-parquet-table-planning`, `glue/glue-table-planning`), each of which takes its schema from the column notes.
* The fold widens only along the pairs `scan-types/type-relaxation` already casts at scan time. Every widened column is therefore one the scan can read.
* A logical field with neither a field-id nor a declared physical name binds by its own name (`scan-read-path/scan-execution-field-id-projection`). The column-binding adapter that serves Iceberg and Delta inserts the per-file cast the widened declaration implies.
* The schema-resolving answer returns no per-file Parquet metadata, because no pushdown reads a footer.
* `direct-storage/direct-storage-hive-partitioning` specifies Hive-style partition discovery. `direct-storage/parquet-directory-seam-file-listing` specifies the file listing.


## Scenarios

### Scenario: The merge mode selects every footer or exactly one

* *GIVEN* the same prefix, once under the fold-every-file mode and once under the sample-one-file mode
* *WHEN* the seam resolves the schema
* *THEN* the fold-every-file mode SHALL read every listed file's footer, and the sample-one-file mode SHALL read EXACTLY ONE footer, that of the FIRST file in the deterministic listing order
* *AND* the sample-one-file mode SHALL return the same file LIST as the other mode, so the mode narrows which footers are read and never which files are listed
* *AND* the two modes SHALL be two values of ONE argument to ONE function, so every caller reaches the same behaviour from the same code
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
* *AND* it SHALL widen ONLY across pairs the relaxation set of `scan-types/type-relaxation` proves castable, reading that set from its ONE owner and MUST NOT carry its own pair table, because a pair the fold invented would declare a type the scan cannot cast a file up to
* *AND* it SHALL fold a column PAIRWISE across files in the deterministic listing order, so a column appearing at three types resolves to the widest one reachable by successive supported widenings
* *AND* it SHALL read the footers CONCURRENTLY through the caller's store, at most the store's admission cap at once, and SHALL fold each footer in listing order as it arrives, so a prefix holding many files costs one bounded fan-out and never holds all its parsed footers at once
* *AND* it SHALL read a footer of at most 64 KiB, length field included, with ONE ranged GET
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

### Scenario: One seam answers the file list and the folded schema for enumeration

* *GIVEN* an object store, a storage prefix, a merge mode, and a partitioning switch
* *WHEN* table enumeration asks for that prefix's data files and schema
* *THEN* exactly ONE function SHALL answer, returning every data file with its byte size and partition values, the declared partition columns, the folded Arrow schema, and the binary columns the fold found
* *AND* that function SHALL name no catalog kind, no table format, no Exasol connection, and no virtual-schema property, so it takes two switch values rather than the properties that set them
* *AND* that function SHALL take no file-keep predicate, because enumeration keeps every file and no pushdown calls it
* *AND* no caller SHALL carry its own listing filter, its own footer reader, its own merge policy, or its own partition parser, because two policies over one property is the drift this seam exists to prevent
* *AND* every failure SHALL be returned as an error value rather than raised as a panic, and no error message SHALL contain a credential value
