# Feature: Direct-Storage Table Discovery

The adapter turns a storage prefix holding directories of Parquet files into enumerated, named, and mapped virtual tables through the unchanged `createVirtualSchema` listing pipeline. A lakehouse with no catalog service is therefore queryable.

<!-- DELTA:CHANGED -->
## Background

* The adapter resolves the base path from the CONNECTION address and the `NAMESPACE` property (`direct-storage/direct-storage-properties`).
* The base path and each listed directory name are raw object keys (`file-planning/pushdown-planning-file-encoding`). The adapter requests a table's objects under its directory name as listed and does not escape the name again.
* `direct-storage/parquet-directory-seam-file-listing` owns which objects under a table root count as data files and their order. This feature owns which directories are tables and how each is named.
* A direct-storage table's identity is its directory name, so the identifier note of its virtual table (`vs-adapter/create-virtual-schema`) records the bare directory name with no namespace.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:REMOVED -->
### Scenario: The direct-storage client is a boxed catalog client declared in the engine crate

* *GIVEN* the recorded scenario of this title
* *WHEN* this plan is recorded
* *THEN* this scenario SHALL be REMOVED, because it states code placement rather than behavior
<!-- /DELTA:REMOVED -->
