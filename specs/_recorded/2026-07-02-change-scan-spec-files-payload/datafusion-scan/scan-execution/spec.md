# Feature: DataFusion Scan Execution

A disposable Rust SET UDF that, for one query, builds a DataFusion session, registers
exactly the Iceberg/Parquet data files assigned to its shard, sizes its DataFusion
`RuntimeEnv` memory pool from the per-instance memory limit reported in UDF metadata,
applies the pushed-down projection, filter, and LIMIT, and streams the matching rows
back as Arrow IPC batches. It holds no state and discovers no files of its own. The
UDF receives its scan spec as TWO VARCHAR arguments — a shard-invariant common spec
serialized once for the whole fan-out (including the Iceberg table root), and a per-shard
`(path, size)` file list — which it merges back into one `ScanSpec` at entry.

## Background

* The scan UDF receives two VARCHAR JSON arguments: `common` (shard-invariant, including the
  Iceberg table root) and `files` (this shard's assigned `(path, size)` entries). It merges
  them into one `ScanSpec` before running; see `datafusion-scan/scan-execution-spec-reconstitution`.
* Each per-shard file entry carries the file's byte size; the UDF constructs each assigned
  file's object metadata from that size and MUST NOT issue a per-file object-store metadata
  (`HEAD`) request to re-discover a size the adapter already resolved.
* A per-shard file path is resolved to an absolute URI before registration: an entry that is
  already absolute (contains a `://` scheme) passes through unchanged; a relative entry is
  joined onto the common spec's table root (normalizing the trailing `/`).
* The UDF MUST register only its assigned files and MUST NOT discover additional files.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* Field-id-based column projection (`datafusion-scan/scan-execution-field-id-projection`) is
  preserved regardless of how per-file metadata is supplied.
* Only `Value::String` types cross the `.so` boundary; both arguments are VARCHAR JSON.
* See `datafusion-scan/scan-execution-field-id-projection`, `-memory-and-credentials`,
  `-telemetry`, `-partial-agg`, `-grouped-agg`, and `-spec-reconstitution` for the related
  scan behaviors.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan invocation receiving TWO VARCHAR arguments — a shard-invariant common spec argument (carrying the logical Iceberg schema, projection, filter, limit, storage credentials, the Iceberg table root, and tuning knobs) and a per-shard files argument listing specific Iceberg Parquet files in MinIO as `(path, size)` entries
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF runs for that invocation
* *THEN* the UDF SHALL read the common spec from the first input argument and the `(path, size)` file list from the second, and reconstitute a single `ScanSpec` whose files come from the second argument and whose every other field comes from the first (only `Value::String` crossing the `.so` boundary — both arguments are VARCHAR JSON)
* *AND* the UDF SHALL resolve each file entry to an absolute URI (absolute entries pass through; relative entries are joined onto the common spec's table root) and register ONLY those files as one `scan_target` whose declared schema is the logical Iceberg schema (each field carrying its `field_id` metadata), NOT a schema inferred from the first file, and MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per scanned source row containing only the projected columns
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Scan builds file metadata from the spec and issues no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries every assigned file's byte size alongside its path
* *WHEN* the scan UDF registers its assigned files and builds the scan
* *THEN* the UDF SHALL construct each assigned file's object metadata — at minimum its byte size — from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (`HEAD`) request to discover a file's size before scanning, because the size is authoritative from the spec
* *AND* the rows the UDF emits SHALL be identical to those produced when the size is instead discovered from object storage, so supplying the size changes only the pre-scan metadata round-trips, never the result
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: Relative paths resolve against the table root and absolute paths pass through

* *GIVEN* a scan invocation whose common spec carries a non-empty Iceberg table root and whose per-shard files argument mixes relative entries (paths under that root) with at least one absolute entry (a path not under the root, carrying its own `://` scheme)
* *WHEN* the scan UDF resolves its assigned files for registration
* *THEN* the UDF SHALL join each relative entry onto the table root (normalizing the boundary `/`) to form the absolute URI, and SHALL pass each already-absolute entry through unchanged
* *AND* the set of registered absolute file URIs SHALL equal the original resolved data-file URIs the adapter partitioned into this shard
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every entry as absolute and join none of them
<!-- /DELTA:NEW -->
