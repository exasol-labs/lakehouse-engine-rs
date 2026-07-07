# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument input: a
shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1), which the UDF
deserializes and merges into one scan spec before running the shared scan path.

## Background

* The per-shard file list is the ONLY per-shard field; every other field comes from the common spec.
* Each file entry stores its path verbatim (relative or absolute) without resolving it; path
  reconstruction is deferred to file registration.
* Per-file positional-delete references travel with their data-file entry in the per-shard argument.
* The same `.so` produces and consumes the spec within one deploy, so there is no cross-version
  wire-compatibility requirement — legacy entries without deletes reconstitute with an empty list.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Reconstitution carries per-file positional-delete references

* *GIVEN* a scan invocation whose second argument is a JSON array of per-shard file entries, each carrying a data-file path, its byte size, and zero or more associated positional-delete file references (each with a path, byte size, and delete content type)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each file entry together with its associated delete-file references and MERGE them into one scan spec whose per-shard files (with deletes) come from the second argument and whose every other field comes from the first
* *AND* the merge SHALL store each data-file and delete-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* the reconstituted scan spec MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A file-list argument that predates the delete encoding still reconstitutes

* *GIVEN* a scan invocation whose second argument holds legacy file entries that carry a path and byte size but NO delete-file references (a spec that predates positional-delete support)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each legacy entry with its associated delete list defaulting to empty, so the entry reconstitutes as a delete-free data file
* *AND* a missing table root SHALL still be treated as "all paths are absolute" so no path is joined onto a root
* *AND* the resulting scan spec SHALL be usable by the shared scan path unchanged, because the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)
<!-- /DELTA:CHANGED -->
