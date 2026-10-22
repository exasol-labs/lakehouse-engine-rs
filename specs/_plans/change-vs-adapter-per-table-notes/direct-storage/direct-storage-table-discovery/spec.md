# Feature: Direct-Storage Table Discovery

The adapter turns a storage prefix holding directories of Parquet files into enumerated, named, and mapped virtual tables through the unchanged `createVirtualSchema` listing pipeline. A lakehouse with no catalog service is therefore queryable.

<!-- DELTA:CHANGED -->
## Background

* The direct-storage client implements `CatalogClient` and is declared in `lakehouse-engine`, not in `lakehouse-catalog`. It needs the engine-side object-store builder, and the catalog crate does not depend on `object_store` (`architecture.md`). The engine still reaches every enumeration and table-load operation through the trait.
* The client's base path is resolved at construction from the CONNECTION address and the `NAMESPACE` property (`direct-storage/direct-storage-properties`), inside the one construction site that may name a catalog kind.
* `direct-storage/parquet-directory-seam-file-listing` owns which objects under a table root count as data files and their order. This feature owns which directories are tables and how each is named.
* `lakehouse-catalog`'s neutral skip reason has a variant for a directory holding no data file.
* The neutral table identifier the client returns has an empty namespace, because a direct-storage table's identity is its directory name. The shared flatten and the shared recording of each virtual table's identifier note per `vs-adapter/create-virtual-schema` then produce the bare directory name without branching on catalog kind.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A first-level directory holding no data file is skipped, not failed

* *GIVEN* a storage base path holding a first-level directory `empty/` whose only object is `_SUCCESS`, alongside at least one directory that does hold data files
* *WHEN* the client enumerates the base path
* *THEN* the client SHALL exclude `empty/` from its resolved tables and SHALL report it in the listing's skipped set, pairing its identifier with a neutral skip reason naming the absence of a data file
* *AND* `createVirtualSchema` SHALL complete successfully with the remaining tables, and the shared listing pipeline SHALL write one warning line per skipped entry without branching on catalog kind
* *AND* the skipped directory SHALL NOT be returned as a virtual table, so no identifier note is recorded for it and no unqueryable virtual table is advertised
* *AND* a base path holding NO first-level directory with a data file SHALL yield a successful, EMPTY virtual schema rather than an error, matching the behavior for a namespace that holds no Iceberg table
* *AND* the skip reason SHALL name no catalog kind, no table format, and no engine symbol, so it stays a neutral value of the catalog crate
* *AND* the absence of a data file SHALL be the ONLY reason to skip a directory: a directory whose enumeration fails for any other reason, such as a column pair no widening rule folds (`direct-storage/parquet-directory-seam`), SHALL fail the whole `CREATE VIRTUAL SCHEMA` or `REFRESH` statement, and the client MUST NOT skip that directory or return a partial table set
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Table naming and the identifier note round trip reuse the shared helpers

* *GIVEN* a base path holding first-level directories whose names differ in letter case, and a virtual schema created over it
* *WHEN* the adapter builds the response and a later pushdown recovers the scanned table
* *THEN* the Exasol table name SHALL be the directory name uppercased by the SAME shared flatten helper both other kinds use, so the fold has one owner
* *AND* the adapter SHALL record each virtual table's catalog identifier in the `name` member of that table's identifier note as the ORIGINAL-CASED directory name alone, and MUST NOT record a dotted identifier, because a directory name can itself contain a dot and a dotted form could not be split back unambiguously
* *AND* two directories that each hold a data file and whose names fold to the same Exasol name SHALL be rejected by the SAME shared collision check both other kinds use, with an error naming the colliding Exasol name and both directories, rather than silently dropping one, and a directory skipped for holding no data file SHALL collide with no other directory
* *AND* a pushdown whose recovered identifier is empty or carries a path separator SHALL fail with an error naming the identifier and telling the operator to recreate the virtual schema, because such a value names no first-level directory and composing a table root from it would reach outside the base path
* *AND* the table root SHALL be recomposed at pushdown from the CONNECTION address, the `NAMESPACE` property, and the recorded directory name, so no table root is persisted and a moved base path is followed by recreating the CONNECTION alone
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A scoped refresh lists directory names and reads only the requested directories

* *GIVEN* a direct-storage base path and a partial refresh per `vs-adapter/refresh-and-set-properties`
* *WHEN* the adapter lists the base path's identifiers and then loads the directories it selects
* *THEN* the identifier-only listing SHALL return every first-level directory name, sorted, from the one delimiter listing the full listing uses, and SHALL read nothing below any directory
* *AND* the load SHALL resolve each given directory through `direct-storage/parquet-directory-seam` as the full listing does, skip one that holds no data file with the reason `holds no data file`, and read no other directory
<!-- /DELTA:NEW -->

<!-- DELTA:REMOVED -->
### Scenario: Table naming and the TABLE_MAP round trip reuse the shared helpers

* *GIVEN* a base path holding first-level directories whose names differ in letter case, and a virtual schema created over it
* *WHEN* the adapter builds the response and a later pushdown recovers the scanned table
* *THEN* the Exasol table name SHALL be the directory name uppercased by the SAME shared flatten helper both other kinds use, so the fold has one owner
* *AND* `TABLE_MAP` SHALL record each Exasol name against the ORIGINAL-CASED directory name alone, and MUST NOT record a dotted identifier, because a directory name may itself contain a dot and a dotted form could not be split back unambiguously
* *AND* two directories whose names fold to the same Exasol name SHALL be rejected by the SAME shared collision check both other kinds use, with an error naming the colliding Exasol name and both directories, rather than silently dropping one
* *AND* a pushdown whose recovered identifier is empty or carries a path separator SHALL fail with an error naming the identifier and telling the operator to recreate the virtual schema, because such a value names no first-level directory and composing a table root from it would reach outside the base path
* *AND* the table root SHALL be recomposed at pushdown from the CONNECTION address, the `NAMESPACE` property, and the recorded directory name, so no table root is persisted and a moved base path is followed by recreating the CONNECTION alone
<!-- /DELTA:REMOVED -->
