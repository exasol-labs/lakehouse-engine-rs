# Feature: DataFusion Scan Execution — File Metadata (No-HEAD Registration)

Extends `datafusion-scan/scan-execution` with how the scan UDF turns a per-shard file entry into a
registered, absolute, sized file — without issuing a per-file object-store HEAD request the
adapter's already-resolved size makes redundant — and extends the same no-HEAD guarantee to the
associated positional-delete files.

## Background

* Every data-file and delete-file byte size is authoritative from the per-shard spec entry, so the
  UDF never issues a HEAD to discover a size.
* Relative paths are joined onto the common-spec table root; absolute paths pass through unchanged;
  an empty table root means every entry is treated as absolute. Delete-file paths follow the same
  rule as data-file paths.
* Building a data file's base `ParquetAccessPlan` needs its per-row-group row counts, obtained by
  reading the Parquet footer via a range GET (not a HEAD), ideally parsed once and reused.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Delete files also carry their size and incur no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries, for each associated positional-delete file, its byte size alongside its path
* *WHEN* the scan UDF reads a data file's associated delete files
* *THEN* the UDF SHALL construct each delete file's object metadata — at minimum its byte size — from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (HEAD) request for a delete file to discover its size, because the size is authoritative from the spec
* *AND* the emitted rows SHALL be identical to those produced when a delete file's size is instead discovered from object storage
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Data-file Parquet footer is read via a range GET, not a HEAD, and not twice

* *GIVEN* a data file that carries positional deletes, whose per-row-group row counts are needed to build the base `ParquetAccessPlan`
* *WHEN* the scan UDF constructs the access plan for that data file
* *THEN* the UDF SHALL obtain the per-row-group row counts by reading the Parquet footer via an object-store range GET (the file size is already known from the spec), and MUST NOT issue a HEAD request
* *AND* the UDF SHOULD parse each data file's footer at most once per scan, reusing the parsed metadata for both access-plan construction and the Parquet opener (via a shared reader factory / cached metadata) rather than reading the footer twice
* *AND* the emitted rows SHALL be identical regardless of how many times the footer is physically fetched
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Delete-file relative and absolute paths resolve like data-file paths

* *GIVEN* a scan invocation whose common spec carries a non-empty Iceberg table root and whose per-shard files argument mixes relative delete-file entries (paths under that root) with at least one absolute delete-file entry (a path not under the root)
* *WHEN* the scan UDF resolves a data file's associated delete files for reading
* *THEN* the UDF SHALL join each relative delete-file entry onto the table root to form its absolute URI and SHALL pass each already-absolute delete-file entry through unchanged, exactly as it does for data-file paths
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every delete-file entry as absolute and join none of them
<!-- /DELTA:NEW -->
