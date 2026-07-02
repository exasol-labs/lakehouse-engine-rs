# Feature: DataFusion Scan Execution — File Metadata (No-HEAD Registration)

Extends `datafusion-scan/scan-execution` with how the scan UDF turns a per-shard `(path,
size)` file entry into a registered, absolute, sized file — without issuing a per-file
object-store `HEAD` request the adapter's already-resolved size makes redundant.

## Background

* Each per-shard file entry carries the file's byte size; the UDF constructs each assigned
  file's object metadata from that size and MUST NOT issue a per-file object-store metadata
  (`HEAD`) request to re-discover a size the adapter already resolved.
* A per-shard file path is resolved to an absolute URI before registration: an entry that is
  already absolute (contains a `://` scheme) passes through unchanged; a relative entry is
  joined onto the common spec's table root (normalizing the trailing `/`).
* When the common spec carries an empty table root, every entry is treated as absolute and
  none are joined.
* Field-id-based column projection (`datafusion-scan/scan-execution-field-id-projection`) is
  preserved regardless of how per-file metadata is supplied.
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
