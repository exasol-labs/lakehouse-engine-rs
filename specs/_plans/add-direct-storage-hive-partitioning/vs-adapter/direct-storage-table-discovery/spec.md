# Feature: Direct-Storage Table Discovery

Turns a storage prefix holding directories of Parquet files into the same enumerated, named, and
mapped virtual tables a catalog produces. A lakehouse with no catalog service is therefore
queryable through the unchanged `createVirtualSchema` listing pipeline.

## Background

* This delta amends ONE scenario: the client passes the seam both layout switches and a keep-all
  file predicate, and the folded schema it maps now ends with the partition columns. Every other
  scenario and the recorded Background are unchanged.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A table's columns and data files come from the one shared directory seam

* *GIVEN* a first-level directory holding data files at more than one depth, alongside objects the seam's filter excludes
* *WHEN* the client resolves that table's columns
* *THEN* the client SHALL obtain the table's data-file list and its folded schema from the ONE shared seam `vs-adapter/parquet-directory-seam` specifies, passing that seam the table root, the resolved directory options, and a file predicate that keeps every file, and MUST NOT carry its own listing filter, its own footer reader, or its own partition parser
* *AND* the client SHALL map the folded schema, partition columns included, to the neutral ordered column list the shared listing pipeline consumes, each column carrying a SOURCE-TAGGED type descriptor rather than an Exasol type, so the catalog crate stays free of the Exasol type mapping
* *AND* that descriptor SHALL carry the column's logical Arrow type as the TAG STRING of the engine's existing scan-spec tag vocabulary, rather than as an Arrow type value, because `vs-adapter/catalog-crate-structure` forbids the catalog crate's manifest from declaring `arrow`, so a neutral column type names no Arrow type
* *AND* the client SHALL apply the string substitution for a nested or unrepresentable column BEFORE it renders that tag, so every tag it emits names a type the vocabulary can express and the round trip back to an Arrow type is lossless
* *AND* a folded Arrow type that SURVIVES that substitution and that the tag vocabulary still cannot express SHALL FAIL the enumeration with an error naming the column and that Arrow type, and the client MUST NOT render it as the string tag, because a silent string tag declares the column `VARCHAR(2000000)` and registers it as a string with no error anywhere, so the losslessness this clause asserts is proven by a failure rather than assumed
* *AND* the columns SHALL appear in the folded schema's own order, so two enumerations of an unchanged directory declare the same columns in the same order
<!-- /DELTA:CHANGED -->
