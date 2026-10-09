# Feature: Direct-Storage Table Discovery

Turns a storage prefix holding directories of Parquet files into the same enumerated, named, and
mapped virtual tables a catalog produces. A lakehouse with no catalog service is therefore
queryable through the unchanged `createVirtualSchema` listing pipeline.

## Background

* **The direct-storage client implements `CatalogClient` but is DECLARED IN `lakehouse-engine`, not
  in `lakehouse-catalog`.** It needs the engine-side object-store builder.
  `catalog/catalog-crate-structure` forbids the catalog crate from declaring `object_store` as a
  direct dependency. Declaring the client there would point the dependency edge backwards. Rust's
  orphan rule permits the impl, because the type is local even though the trait is not. No recorded
  rule requires an implementor to live in the catalog crate. The recorded requirement is that the
  engine reach every enumeration and table-load operation THROUGH the trait. This placement keeps
  that requirement.
* The single `Box<dyn CatalogClient>` construction site is already engine-side, so the placement
  adds no new seam.
* The client's base path is resolved at CONSTRUCTION from the CONNECTION address and the `NAMESPACE`
  property (`direct-storage/direct-storage-properties`). That resolution happens inside the one
  construction site already permitted to name a catalog kind.
* `direct-storage/parquet-directory-seam-file-listing` owns which objects under a table root count as data files and
  in what order they are returned. This feature owns which directories are tables and how each one
  is named. It reads the file rules from that seam rather than restating them.
* `lakehouse-catalog`'s neutral skip reason gains one variant for a directory holding no data file,
  per `catalog/catalog-crate-public-surface-extensions`.
* The neutral table identifier this client returns carries an EMPTY namespace, because a
  direct-storage table's identity inside its virtual schema is its directory name alone. That makes
  the shared flatten and `TABLE_MAP` construction produce the bare directory name with no branch on
  catalog kind.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A refresh that a column pair fails keeps the previous declaration queryable

* *GIVEN* a direct-storage virtual schema that leaves `MERGE_SCHEMA` absent, whose last successful `CREATE VIRTUAL SCHEMA` or `REFRESH` declared several tables, one of whose directories has since gained a file storing a column `X` at a type that no widening pair folds with the type another file of that table stores
* *WHEN* Exasol sends a `refresh` request
* *THEN* the `ALTER VIRTUAL SCHEMA ... REFRESH` statement SHALL fail with the fold error naming `X`, both types, and both file paths, per `direct-storage/parquet-directory-seam`, and the adapter MUST NOT skip that directory or return a partial table set, per the scenario "A first-level directory holding no data file is skipped, not failed"
* *AND* the error message MUST NOT contain a credential value
* *AND* Exasol SHALL keep the tables and columns the last successful `CREATE VIRTUAL SCHEMA` or `REFRESH` declared, so every other table SHALL stay queryable and SHALL return the rows of its current data files
<!-- /DELTA:NEW -->
