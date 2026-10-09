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

<!-- DELTA:CHANGED -->
### Scenario: A table's columns and data files come from the one shared directory seam

* *GIVEN* a first-level directory holding data files at more than one depth, alongside objects the seam's filter excludes
* *WHEN* the client resolves that table's columns
* *THEN* the client SHALL obtain the table's data-file list, its folded schema, its partition columns, and its binary columns from the ONE shared seam `direct-storage/parquet-directory-seam` specifies, passing that seam the table root and the resolved directory options, and MUST NOT carry its own listing filter, its own footer reader, or its own partition parser
* *AND* the client SHALL map the folded schema, partition columns included, to the neutral ordered column list the shared listing pipeline consumes, each column carrying a SOURCE-TAGGED type descriptor rather than an Exasol type, so the catalog crate stays free of the Exasol type mapping
* *AND* that descriptor SHALL carry the column's logical Arrow type as the TAG STRING of the engine's existing scan-spec tag vocabulary, rather than as an Arrow type value, because `catalog/catalog-crate-structure` forbids the catalog crate's manifest from declaring `arrow`, so a neutral column type names no Arrow type
* *AND* each column SHALL ALSO carry its engine-encoded declaration: the logical field built from the same folded field (nested member descriptor included, no binding key), its position among the partition columns when it is one, and the refusal reason `vs-adapter/binary-column-refusal` gives a binary column, which the listing pipeline records as the column's note per `vs-adapter/column-source-notes`
* *AND* the client SHALL apply the string substitution for a nested or unrepresentable column BEFORE it renders that tag, so every tag it emits names a type the vocabulary can express and the round trip back to an Arrow type is lossless
* *AND* a folded Arrow type that SURVIVES that substitution and that the tag vocabulary still cannot express SHALL FAIL the enumeration with an error naming the column and that Arrow type, and the client MUST NOT render it as the string tag, because a silent string tag declares the column `VARCHAR(2000000)` and registers it as a string with no error anywhere, so the losslessness this clause asserts is proven by a failure rather than assumed
* *AND* the columns SHALL appear in the folded schema's own order, so two enumerations of an unchanged directory declare the same columns in the same order
<!-- /DELTA:CHANGED -->
