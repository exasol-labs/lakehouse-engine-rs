# Feature: DataFusion Scan Execution, File Metadata (No-HEAD Registration)

The scan UDF registers each assigned file from the `(path, size)` entry the adapter already resolved, resolves it to an absolute URI, and issues no per-file object-store `HEAD` request. The same holds for positional-delete files.

## Background

* Each per-shard file entry carries the file's byte size. A data-file or delete-file size from the spec entry is authoritative.
* An entry that contains a `://` scheme is absolute and passes through unchanged. A relative entry is joined onto the common spec's table root, normalizing the trailing `/`. Delete-file paths follow the same rule.
* An empty table root is unreachable from the current adapter. The rule stays defined for it (every entry is absolute) so path resolution is a total function over the wire format.
* Building a data file's base `ParquetAccessPlan` needs only its per-row-group row counts, read from the Parquet footer.
* A footer read without a size hint costs two object-store round-trips: the last 8 bytes, then the metadata range. A `metadata_size_hint` of N fetches the last N bytes first, so a footer that fits in N costs one request.
* DataFusion already carries the hint as `datafusion.execution.parquet.metadata_size_hint` (default `Some(512 * 1024)`) on the `ParquetFormat` the provider owns, and `ParquetFormat::create_physical_plan` copies it to the `ParquetSource` the opener uses.
* A footer larger than the hint is re-requested in full. The result is still correct.
* With an empty `logical_schema`, `register_file_list` runs `ParquetFormat::infer_schema`, which fetches and caches the footer before access-plan construction. Only a non-empty `logical_schema`, as the adapter supplies in production, measures the access-plan fetch.

## Scenarios

### Scenario: Scan builds file metadata from the spec and issues no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries every assigned file's byte size alongside its path
* *WHEN* the scan UDF registers its assigned files and builds the scan
* *THEN* the UDF SHALL construct each assigned file's object metadata — at minimum its byte size — from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (`HEAD`) request to discover a file's size before scanning, because the size is authoritative from the spec
* *AND* the rows the UDF emits SHALL be identical to those produced when the size is instead discovered from object storage, so supplying the size changes only the pre-scan metadata round-trips, never the result

### Scenario: Relative paths resolve against the table root and absolute paths pass through

* *GIVEN* a scan invocation whose common spec carries a non-empty table root and whose per-shard files argument mixes relative entries (paths under that root) with at least one absolute entry (a path not under the root, carrying its own `://` scheme)
* *WHEN* the scan UDF resolves its assigned files for registration
* *THEN* the UDF SHALL join each relative entry onto the table root (normalizing the boundary `/`) to form the absolute URI, and SHALL pass each already-absolute entry through unchanged
* *AND* the set of registered absolute file URIs SHALL equal the original resolved data-file URIs the adapter partitioned into this shard
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every entry as absolute and join none of them
* *AND* this rule SHALL apply to a spec produced by EITHER format reader, because the table root is a neutral field both populate

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
* *AND* the UDF SHALL supply that read with a metadata size hint, so a data file whose Parquet footer and metadata fit within the hint costs EXACTLY ONE object-store range GET rather than a suffix probe followed by a second GET, and that hint SHALL be the SAME value the Parquet opener uses for its own footer reads — read back from the one `ParquetFormat` the provider both builds access plans against and hands to the physical-plan builder, so the two sites cannot drift and no separate constant and no operator-facing knob is introduced
* *AND* the UDF SHALL request only the metadata the access plan consumes — the per-row-group row counts — and MUST NOT additionally request the Parquet page index, which access-plan construction never reads
* *AND* the UDF SHOULD parse each data file's footer at most once per scan, reusing the parsed metadata for both access-plan construction and the Parquet opener (via a shared reader factory / cached metadata) rather than reading the footer twice
* *AND* attaching positional deletes to a data file MUST NOT increase the number of object-store requests issued against THAT data file, compared with the same scan of the same file with no deletes attached, measured with a non-empty `logical_schema` — the production configuration in which no `ParquetFormat::infer_schema` pass pre-populates the metadata cache, since measuring it under the legacy inference fallback proves nothing about the access-plan fetch
* *AND* the emitted rows SHALL be identical regardless of how many times the footer is physically fetched

### Scenario: Delete-file relative and absolute paths resolve like data-file paths

* *GIVEN* a scan invocation whose common spec carries a non-empty table root and whose per-shard files argument mixes relative delete-file entries (paths under that root) with at least one absolute delete-file entry (a path not under the root)
* *WHEN* the scan UDF resolves a data file's associated delete files for reading
* *THEN* the UDF SHALL join each relative delete-file entry onto the table root to form its absolute URI and SHALL pass each already-absolute delete-file entry through unchanged, exactly as it does for data-file paths
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every delete-file entry as absolute and join none of them
