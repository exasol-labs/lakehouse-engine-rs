# Feature: DataFusion Scan Execution, File Metadata (No-HEAD Registration)

The scan UDF registers each assigned file from the `(path, size)` entry the adapter already resolved, resolves it to an absolute URI, and issues no per-file object-store `HEAD` request. The same holds for positional-delete files and Puffin deletion-vector files.

## Background

* Each per-shard file entry carries the file's byte size. A data-file or delete-file size from the spec entry is authoritative.
* An entry that contains a `://` scheme is absolute and passes through unchanged. A relative entry is joined onto the common spec's table root, normalizing the trailing `/`. Delete-file paths follow the same rule.
* An empty table root is unreachable from the current adapter. The rule stays defined for it (every entry is absolute) so path resolution is a total function over the wire format.
* One shared reader per scan table reads its data files. A file's footer, the bytes fetched with it, and its page index each load once, however many byte ranges read the file. The first range that needs a load starts it, and the others wait for it.
* The footer read is one GET of the file's last `metadata_size_hint` bytes (`datafusion.execution.parquet.metadata_size_hint`, default 512 KiB, on the `ParquetFormat` the provider owns). A file of at most 8 MiB is fetched whole in that GET.
* The fetched bytes stay in memory while the file is read and serve every read that falls inside them. `scan-runtime/scan-execution-memory-and-credentials` owns their reservation.
* With an empty `logical_schema`, registration runs `ParquetFormat::infer_schema`, which reads the first file's footer on its own. A request count is therefore only meaningful with a non-empty `logical_schema`, as the adapter supplies in production.

## Scenarios

### Scenario: Scan builds file metadata from the spec and issues no per-file HEAD

* *GIVEN* a scan invocation whose per-shard files argument carries every assigned file's byte size alongside its path
* *WHEN* the scan UDF registers its assigned files and builds the scan
* *THEN* the UDF SHALL construct each assigned file's object metadata, at minimum its byte size, from the per-shard spec entry
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

* *GIVEN* a scan invocation whose per-shard files argument carries, for each associated positional-delete file or Puffin deletion-vector file, its byte size alongside its path
* *WHEN* the scan UDF reads a data file's associated delete files
* *THEN* the UDF SHALL construct each delete file's object metadata, at minimum its byte size, from the per-shard spec entry
* *AND* the UDF MUST NOT issue a per-file object-store metadata (HEAD) request for a delete file to discover its size, because the size is authoritative from the spec
* *AND* the emitted rows SHALL be identical to those produced when a delete file's size is instead discovered from object storage

### Scenario: Planning a scan reads nothing

* *GIVEN* a scan invocation with a non-empty `logical_schema`, whose data files may carry row-position deletes
* *WHEN* the scan UDF registers its files and builds the physical plan
* *THEN* the UDF MUST NOT issue an object-store request
* *AND* the UDF SHALL read a data file's footer and its deletes when DataFusion opens the file

### Scenario: A data file's footer is fetched once for all its byte ranges

* *GIVEN* a data file that the scan cut into several byte ranges
* *WHEN* the scan's streams open those ranges, at the same time or one after another
* *THEN* the UDF SHALL fetch and parse the file's footer once, with one range GET of the file's last `metadata_size_hint` bytes and no HEAD
* *AND* a range that holds none of the file's row groups MUST NOT issue a request
* *AND* the number of requests against the file MUST NOT depend on the number of streams

### Scenario: A footer larger than the size hint costs one more request

* *GIVEN* a data file or a positional-delete file whose footer is larger than the metadata size hint
* *WHEN* the UDF reads its footer
* *THEN* the UDF SHALL issue one second GET, for the leading part of the footer that the first GET did not fetch

### Scenario: A data file's page index is fetched once for all its byte ranges

* *GIVEN* a data file whose page index lies outside the bytes fetched with its footer, and a predicate that needs page pruning
* *WHEN* several byte ranges of the file load the page index
* *THEN* the UDF SHALL fetch the page index once for the file
* *AND* a page index inside the bytes fetched with the footer MUST NOT cost a request

### Scenario: A small data file is read in one request

* *GIVEN* a data file of at most 8 MiB, with or without row-position deletes
* *WHEN* the scan reads it
* *THEN* the UDF SHALL issue exactly one GET against the file, for the whole file, and serve the footer, the page index, and every column read from those bytes
* *AND* a positional-delete file within the metadata size hint SHALL also cost one GET
* *AND* when the file's bytes do not fit the memory pool, the UDF SHALL read the file as it reads a larger one

### Scenario: Delete-file relative and absolute paths resolve like data-file paths

* *GIVEN* a scan invocation whose common spec carries a non-empty table root and whose per-shard files argument mixes relative delete-file entries (paths under that root) with at least one absolute delete-file entry (a path not under the root)
* *WHEN* the scan UDF resolves a data file's associated delete files for reading
* *THEN* the UDF SHALL join each relative delete-file entry onto the table root to form its absolute URI and SHALL pass each already-absolute delete-file entry through unchanged, exactly as it does for data-file paths
* *AND* when the common spec carries an empty table root, the UDF SHALL treat every delete-file entry as absolute and join none of them
