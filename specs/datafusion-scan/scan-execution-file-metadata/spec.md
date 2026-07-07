# Feature: DataFusion Scan Execution — File Metadata (No-HEAD Registration)

Extends `datafusion-scan/scan-execution` with how the scan UDF turns a per-shard `(path,
size)` file entry into a registered, absolute, sized file — without issuing a per-file
object-store `HEAD` request the adapter's already-resolved size makes redundant — and extends
the same no-HEAD guarantee to the associated positional-delete files.

## Background

* Each per-shard file entry carries the file's byte size; the UDF constructs each assigned
  file's object metadata from that size and MUST NOT issue a per-file object-store metadata
  (`HEAD`) request to re-discover a size the adapter already resolved. Every data-file and
  delete-file byte size is authoritative from the per-shard spec entry, so the UDF never
  issues a HEAD to discover a size for either.
* A per-shard file path is resolved to an absolute URI before registration: an entry that is
  already absolute (contains a `://` scheme) passes through unchanged; a relative entry is
  joined onto the common spec's table root (normalizing the trailing `/`). Delete-file paths
  follow the same rule as data-file paths.
* When the common spec carries an empty table root, every entry is treated as absolute and
  none are joined.
* Field-id-based column projection (`datafusion-scan/scan-execution-field-id-projection`) is
  preserved regardless of how per-file metadata is supplied.
* Building a data file's base `ParquetAccessPlan` needs its per-row-group row counts, obtained
  by reading the Parquet footer via a range GET (not a HEAD), ideally parsed once and reused.
* See `datafusion-scan/scan-execution` for the overall scan invocation and registration flow.

## Scenarios

### Scenario: Scan builds file metadata from the spec and issues no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries every assigned file's byte size alongside its path
* *WHEN* the scan UDF registers its assigned files and builds the scan
* *THEN* the UDF SHALL construct each assigned file's object metadata — at minimum its byte size — from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (`HEAD`) request to discover a file's size before scanning, because the size is authoritative from the spec
* *AND* the rows the UDF emits SHALL be identical to those produced when the size is instead discovered from object storage, so supplying the size changes only the pre-scan metadata round-trips, never the result

### Scenario: Relative paths resolve against the table root and absolute paths pass through

* *GIVEN* a scan invocation whose common spec carries a non-empty Iceberg table root and whose per-shard files argument mixes relative entries (paths under that root) with at least one absolute entry (a path not under the root, carrying its own `://` scheme)
* *WHEN* the scan UDF resolves its assigned files for registration
* *THEN* the UDF SHALL join each relative entry onto the table root (normalizing the boundary `/`) to form the absolute URI, and SHALL pass each already-absolute entry through unchanged
* *AND* the set of registered absolute file URIs SHALL equal the original resolved data-file URIs the adapter partitioned into this shard
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every entry as absolute and join none of them

### Scenario: Delete files also carry their size and incur no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries, for each associated positional-delete file, its byte size alongside its path
* *WHEN* the scan UDF reads a data file's associated delete files
* *THEN* the UDF SHALL construct each delete file's object metadata — at minimum its byte size — from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (HEAD) request for a delete file to discover its size, because the size is authoritative from the spec
* *AND* the emitted rows SHALL be identical to those produced when a delete file's size is instead discovered from object storage

### Scenario: Data-file Parquet footer is read via a range GET, not a HEAD, and not twice

* *GIVEN* a data file that carries positional deletes, whose per-row-group row counts are needed to build the base `ParquetAccessPlan`
* *WHEN* the scan UDF constructs the access plan for that data file
* *THEN* the UDF SHALL obtain the per-row-group row counts by reading the Parquet footer via an object-store range GET (the file size is already known from the spec), and MUST NOT issue a HEAD request
* *AND* the UDF SHOULD parse each data file's footer at most once per scan, reusing the parsed metadata for both access-plan construction and the Parquet opener (via a shared reader factory / cached metadata) rather than reading the footer twice
* *AND* the emitted rows SHALL be identical regardless of how many times the footer is physically fetched

### Scenario: Delete-file relative and absolute paths resolve like data-file paths

* *GIVEN* a scan invocation whose common spec carries a non-empty Iceberg table root and whose per-shard files argument mixes relative delete-file entries (paths under that root) with at least one absolute delete-file entry (a path not under the root)
* *WHEN* the scan UDF resolves a data file's associated delete files for reading
* *THEN* the UDF SHALL join each relative delete-file entry onto the table root to form its absolute URI and SHALL pass each already-absolute delete-file entry through unchanged, exactly as it does for data-file paths
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every delete-file entry as absolute and join none of them
