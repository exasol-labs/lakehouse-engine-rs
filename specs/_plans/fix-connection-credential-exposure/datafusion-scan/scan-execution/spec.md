# Feature: DataFusion Scan Execution

A disposable Rust SCALAR EMIT UDF that, for one query, builds a DataFusion session,
registers exactly the Parquet data files assigned to its shard, sizes its
DataFusion `RuntimeEnv` memory pool from the per-instance memory limit reported in UDF
metadata, applies the pushed-down projection, filter, and LIMIT, and streams the matching
rows back as Arrow IPC batches. It holds no state and discovers no files of its own. As a
SCALAR EMIT UDF under SDK 0.21.0, the framework invokes `run()` ONCE per input row, so the
UDF scans exactly one row's assigned file list per call and never iterates the input with
`ctx.next()`. The UDF receives its scan spec as TWO VARCHAR arguments — a shard-invariant
common spec serialized once for the whole fan-out (including the table root), and a
per-shard `(path, size)` file list — which it merges back into one `ScanSpec` per call.
Arrow-column-to-SDK-`Value` conversion at the emit boundary — type mapping, incompatible-
column JSON rendering, and EMITS-type coercion — is owned by
`datafusion-scan/scan-execution-value-conversion`.

## Background

* **This delta is issue #135. It amends ONE scenario and one Background enumeration, and changes no scan rule.** File registration, projection, filter, LIMIT, incremental Arrow IPC emission, the unreadable-file error, the memory-exhaustion error, and INT96 decoding are all UNCHANGED.
* **SUPERSEDES the recorded Background enumeration listing "storage credentials" among the shard-invariant common argument's contents.** The common argument now carries, per side, EITHER a reference to the Exasol CONNECTION that supplies that side's storage credentials OR the sealed envelope carrying the storage backend the planning layer vended — specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan registers only its assigned files and returns matching rows

* *GIVEN* a scan input row carrying TWO VARCHAR arguments — a shard-invariant common spec argument (carrying the logical schema, projection, filter, limit, a storage credential reference or a sealed vended storage envelope, the table root, and tuning knobs) and a per-shard files argument listing specific Parquet data files in MinIO, each optionally carrying its associated positional-delete file references
* *AND* a projection naming a subset of columns
* *WHEN* the scan UDF processes that input row
* *THEN* the UDF SHALL read the common spec from the first input argument and the file list from the second, and reconstitute a single scan spec whose files (and their delete references) come from the second argument and whose every other field comes from the first (only serialized bytes crossing the `.so` boundary — both arguments are VARCHAR JSON)
* *AND* the UDF SHALL resolve each file entry to an absolute URI and register ONLY those files through the custom table provider whose declared schema is the logical schema, and MUST NOT resolve or discover any additional files from the catalog
* *AND* the UDF SHALL emit one output row per surviving source row containing only the projected columns
* *AND* the UDF SHALL run this same registration path for a spec produced by EITHER format reader, because the table root and the logical schema are neutral fields both populate
<!-- /DELTA:CHANGED -->
