# Feature: Direct-Storage Table Discovery

Turns a storage prefix holding directories of Parquet files into the same enumerated, named, and
mapped virtual tables a catalog produces. A lakehouse with no catalog service is therefore
queryable through the unchanged `createVirtualSchema` listing pipeline.

## Background

* **The direct-storage client implements `CatalogClient` but is DECLARED IN `lakehouse-engine`, not
  in `lakehouse-catalog`.** It needs the engine-side object-store builder.
  `vs-adapter/catalog-crate-structure` forbids the catalog crate from declaring `object_store` as a
  direct dependency. Declaring the client there would point the dependency edge backwards. Rust's
  orphan rule permits the impl, because the type is local even though the trait is not. No recorded
  rule requires an implementor to live in the catalog crate. The recorded requirement is that the
  engine reach every enumeration and table-load operation THROUGH the trait. This placement keeps
  that requirement.
* The single `Box<dyn CatalogClient>` construction site is already engine-side, so the placement
  adds no new seam.
* The client's base path is resolved at CONSTRUCTION from the CONNECTION address and the `NAMESPACE`
  property (`vs-adapter/direct-storage-properties`). That resolution happens inside the one
  construction site already permitted to name a catalog kind.
* `vs-adapter/parquet-directory-seam` owns which objects under a table root count as data files and
  in what order they are returned. This feature owns which directories are tables and how each one
  is named. It reads the file rules from that seam rather than restating them.
* `lakehouse-catalog`'s neutral skip reason gains one variant for a directory holding no data file,
  per `vs-adapter/catalog-crate-public-surface-extensions`.
* The neutral table identifier this client returns carries an EMPTY namespace, because a
  direct-storage table's identity inside its virtual schema is its directory name alone. That makes
  the shared flatten and `TABLE_MAP` construction produce the bare directory name with no branch on
  catalog kind.

## Scenarios

### Scenario: The direct-storage client is a boxed catalog client declared in the engine crate

* *GIVEN* the shared `CatalogClient` trait declared in `lakehouse-catalog`, and the engine-side object-store builder that opens a store over a storage root
* *WHEN* the adapter constructs the catalog client for `CatalogKind::DirectStorage`
* *THEN* the direct-storage client type SHALL be declared in `lakehouse-engine` and SHALL implement `CatalogClient`, so the adapter reaches this client's ENUMERATION through `list_tables` exactly as it does for the other two kinds
* *AND* `load_table` SHALL be UNREACHABLE for this kind, because the pushdown path resolves a direct-storage table from its COMPOSED ROOT rather than from a catalog load, and the trait's single engine-side `load_table` call site serves the Unity Catalog path alone
* *AND* this client's `load_table` SHALL nevertheless return a clear error naming the direct-storage kind and stating that a direct-storage table is not loaded through the catalog trait, and MUST NOT panic and MUST NOT return an empty or synthesized table, so a future caller meets a message rather than a crash or a silently wrong result
* *AND* `lakehouse-catalog` SHALL gain NO source file, NO manifest dependency, and NO public item for this kind, so the one-way crate dependency and the catalog crate's forbidden-dependency list are both unchanged
* *AND* the adapter SHALL run the SAME listing pipeline over this client's result that it runs for the other two kinds, and that pipeline MUST NOT name or match the catalog kind
* *AND* the client SHALL surface every failure as an error value rather than a panic, and no error message SHALL contain a credential value
* *AND* this placement SHALL be recorded as a deliberate decision rather than left implicit, because the two shipped implementors live in the catalog crate and a reader would otherwise read the third as a mistake

### Scenario: A first-level directory under the base path is a table

* *GIVEN* a storage base path holding the directories `orders/` and `events/`, a loose object `notes.parquet` directly under the base path, and a directory `orders/2026/` nested below a table root
* *WHEN* the client enumerates the base path
* *THEN* the client SHALL return exactly TWO tables, named for the FIRST-LEVEL directories `orders` and `events`, so table identity is a directory and nothing else
* *AND* the client MUST NOT return a table for `notes.parquet`, because a data file directly under the base path belongs to no directory and naming a table after a file would make the base path's own contents a table
* *AND* the client MUST NOT return a table for `orders/2026/`, because every directory below a table root supplies files to that table rather than a table of its own
* *AND* each returned table SHALL carry its own table root — the base path followed by that directory name — as its storage location, so planning needs no second composition rule
* *AND* each returned table SHALL carry the Parquet table-format tag and NO credential-vending key, because this kind vends nothing

### Scenario: A table's columns and data files come from the one shared directory seam

* *GIVEN* a first-level directory holding data files at more than one depth, alongside objects the seam's filter excludes
* *WHEN* the client resolves that table's columns
* *THEN* the client SHALL obtain the table's data-file list and its folded schema from the ONE shared seam `vs-adapter/parquet-directory-seam` specifies, passing that seam the table root and the resolved merge mode, and MUST NOT carry its own listing filter or its own footer reader
* *AND* the client SHALL map the folded schema to the neutral ordered column list the shared listing pipeline consumes, each column carrying a SOURCE-TAGGED type descriptor rather than an Exasol type, so the catalog crate stays free of the Exasol type mapping
* *AND* that descriptor SHALL carry the column's logical Arrow type as the TAG STRING of the engine's existing scan-spec tag vocabulary, rather than as an Arrow type value, because `vs-adapter/catalog-crate-structure` forbids the catalog crate's manifest from declaring `arrow` and a neutral column type may therefore name no Arrow type
* *AND* the client SHALL apply the string substitution for a nested or unrepresentable column BEFORE it renders that tag, so every tag it emits names a type the vocabulary can express and the round trip back to an Arrow type is lossless
* *AND* a folded Arrow type that SURVIVES that substitution and that the tag vocabulary still cannot express SHALL FAIL the enumeration with an error naming the column and that Arrow type, and the client MUST NOT render it as the string tag, because a silent string tag declares the column `VARCHAR(2000000)` and registers it as a string with no error anywhere, so the losslessness this clause asserts is proven by a failure rather than assumed
* *AND* the columns SHALL appear in the folded schema's own order, so two enumerations of an unchanged directory declare the same columns in the same order

### Scenario: A first-level directory holding no data file is skipped, not failed

* *GIVEN* a storage base path holding a first-level directory `empty/` whose only object is `_SUCCESS`, alongside at least one directory that does hold data files
* *WHEN* the client enumerates the base path
* *THEN* the client SHALL exclude `empty/` from its resolved tables and SHALL report it in the listing's skipped set, pairing its identifier with a neutral skip reason naming the absence of a data file
* *AND* `createVirtualSchema` SHALL complete successfully with the remaining tables, and the shared listing pipeline SHALL write one warning line per skipped entry without branching on catalog kind
* *AND* the skipped table's name SHALL be excluded from `TABLE_MAP`, so no unqueryable virtual table is advertised
* *AND* a base path holding NO first-level directory with a data file SHALL yield a successful, EMPTY virtual schema rather than an error, matching the recorded all-non-Iceberg-namespace behaviour
* *AND* the skip reason SHALL name no catalog kind, no table format, and no engine symbol, so it stays a neutral value of the catalog crate

### Scenario: Table naming and the TABLE_MAP round trip reuse the shared helpers

* *GIVEN* a base path holding first-level directories whose names differ in letter case, and a virtual schema created over it
* *WHEN* the adapter builds the response and a later pushdown recovers the scanned table
* *THEN* the Exasol table name SHALL be the directory name uppercased by the SAME shared flatten helper both other kinds use, so the fold has one owner
* *AND* `TABLE_MAP` SHALL record each Exasol name against the ORIGINAL-CASED directory name alone, and MUST NOT record a dotted identifier, because a directory name may itself contain a dot and a dotted form could not be split back unambiguously
* *AND* two directories whose names fold to the same Exasol name SHALL be rejected by the SAME shared collision check both other kinds use, with an error naming the colliding Exasol name and both directories, rather than silently dropping one
* *AND* a pushdown whose recovered identifier is empty or carries a path separator SHALL fail with an error naming the identifier and telling the operator to recreate the virtual schema, because such a value names no first-level directory and composing a table root from it would reach outside the base path
* *AND* the table root SHALL be recomposed at pushdown from the CONNECTION address, the `NAMESPACE` property, and the recorded directory name, so no table root is persisted and a moved base path is followed by recreating the CONNECTION alone

### Scenario: One admission-limited object store serves every table of one adapter call

* *GIVEN* an adapter call that enumerates many tables at `createVirtualSchema`, and a pushdown call that resolves both sides of a two-table join
* *WHEN* the adapter opens object storage for that call
* *THEN* the adapter SHALL build EXACTLY ONE object store per adapter call, wrapped in an admission limiter with a fixed in-flight request cap, and SHALL share that one store across every table the call touches
* *AND* the cap SHALL therefore bound the WHOLE call, so a two-table join holds at most the cap in flight in total rather than the cap per side
* *AND* the store's HTTP connection-retention budget SHALL be at least the limiter's cap, so an admitted request never finds its connection already evicted, and the two values SHALL be derived from ONE declared constant rather than set independently
* *AND* the limiter SHALL be an application-level admission gate, DISTINCT from the scan UDF's per-instance connection budget that `datafusion-scan/scan-execution-connection-concurrency` owns, and MUST NOT read or modify that budget
* *AND* the chosen cap SHALL be recorded as a deliberately conservative starting value to revisit with measurements, because a refresh sweeping many thousands of footers tolerates far more concurrency than a per-query plan
