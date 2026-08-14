# Feature: DataFusion Scan Execution — Delta Deletion Vector Application

Extends the scan UDF so a Delta Lake deletion vector is applied on read, so a query over a Delta
table whose rows were deleted returns the post-delete row set rather than the raw Parquet content.
A deletion vector names row positions inside ONE data file, so it converges on the machinery Iceberg
positional deletes already use: a `RoaringTreemap` of deleted positions becomes a base
`RowSelection`, which becomes a `ParquetAccessPlan` the Parquet opener intersects with predicate,
row-group, and page pruning. Only the step that PRODUCES the bitmap is new, and it is delegated to
`delta_kernel`'s own protocol-conformant decoder rather than re-implemented. This closes the refusal
the read-time backstop currently raises for the `DeltaDeletionVector` delete mechanism.

## Background

The Delta protocol's normative shapes this feature reads, quoted from the Delta Lake protocol
specification (`delta-io/delta`, `PROTOCOL.md`, `master`):

- Semantics — *"Deletion Vectors are basically sets of row indexes, that is 64-bit integers that
  describe the position (index) of a row in a parquet file starting from zero."* and *"If a snapshot
  contains logical files with records that are invalidated by a DV, then these records must not be
  returned in the output."*
- Descriptor — `storageType` is *"A single character to indicate how to access the DV. Legal options
  are: `['u', 'i', 'p']`."*; `offset` is *"Start of the data for this DV in number of bytes from the
  beginning of the file it is stored in. Always `None` (absent in JSON) when `storageType = 'i'`.
  Interpret as `0` if absent for other `storageType`s."*; `sizeInBytes` is *"Size of the serialized
  DV in bytes (raw data size, i.e. before base85 encoding, if inline)."*; `cardinality` is *"Number
  of rows the given DV logically removes from the file."*
- Path reconstruction — *"If `storageType='p'`, just use the already absolute path. If
  `storageType='u'`, the DV is stored at `<parent path>/<random prefix>/deletion_vector_<uuid in
  canonical textual representation>.bin`."*, where *"The random prefix is recovered as the extra
  characters before the (20 characters fixed length) uuid"* and *"The concrete Base85 variant used
  is Z85"*.
- File container — *"The format for storing DVs in file storage is one (or more) DV ... per file,
  together with a checksum for each DV."*, framed *"with all numerical values written in big endian
  byte order"* as byte `0` = *"The format version of this file: `1` for the format described here."*,
  then per DV a 4-byte `dataSize`, then `bitmapData`, then a *"CRC-32 checksum of `bitmapData`"*.
- Bitmap payload — *"all numerical values are written in little endian byte order"*: 4-byte
  `magicNumber` = *"1681511377"*, then *"A serialized 64-bit bitmap in the portable standard format
  ... This can be treated as a black box by any Delta implementation that has a native,
  standard-compliant RoaringBitmap library available to pass these bytes to."*
- Sharing — *"A DV file contains one or more serialised DV, each describing the set of invalidated
  ... rows for a particular data file it is associated with. For data with partition values, DV
  files are not kept in the same directory hierarchy as data files, as each one can contain DVs for
  files from multiple partitions."*

The engine already carries the descriptor verbatim to the scan: the `DeltaDeletionVector` variant of
the format-neutral delete mechanism holds the storage kind, the path-or-inline payload, the offset,
the size in bytes, and the cardinality (see `vs-adapter/delta-table-planning`). Nothing about the
descriptor is resolved at plan time.

The decoder is `delta_kernel`'s `DeletionVectorDescriptor::read`, already a workspace dependency and
already reachable without any additional feature flag. It returns the `roaring::RoaringTreemap` type
this crate already uses for Iceberg positional deletes — one `roaring` crate instance resolves for
the whole workspace — so the bitmap drops straight into the shipped `RowSelection` and
`ParquetAccessPlan` builders with no conversion and no second delete pipeline.

Every scenario below runs inside the scan UDF, under the same bounded memory pool, the same shared
object-store connection budget, and the same streaming `emit_batch` path as every other scan.

## Scenarios

### Scenario: A UUID-relative deletion vector removes exactly its flagged rows

* *GIVEN* a scan invocation whose assigned files include a Delta data file carrying a deletion-vector
  delete mechanism whose storage kind is UUID-relative, whose payload is the Z85-encoded UUID of a
  `deletion_vector_<uuid>.bin` sidecar under the scan's table root, and whose declared cardinality is
  the number of deleted rows
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL reconstruct the sidecar's absolute path from the table root, the payload's
  optional random prefix, and the trailing 20-character Z85 UUID, decode the deletion vector at the
  descriptor's offset into a set of 0-based row positions, and attach the resulting row selection to
  that data file's scan as a base `ParquetAccessPlan`
* *AND* the UDF MUST NOT emit any row whose position is present in the decoded set
* *AND* the UDF SHALL emit every other row of that data file unchanged
* *AND* the number of positions the decoded set holds MUST equal the descriptor's declared
  cardinality, and a mismatch SHALL fail the scan rather than emit a row set the log contradicts
* *AND* the UDF SHALL apply the deletion vector to THAT data file only, because a deletion vector's
  positions are indexes into one Parquet file

### Scenario: An inline deletion vector is decoded with no object-store access at all

* *GIVEN* a scan invocation whose assigned files include a Delta data file carrying a deletion-vector
  delete mechanism whose storage kind is inline, whose payload is the Z85-encoded bitmap bytes, and
  whose offset is absent
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL decode the row positions from the payload itself and MUST NOT resolve a
  sidecar path, issue an object-store GET, or acquire a permit from the shared connection limiter for
  that deletion vector
* *AND* the emitted post-delete row set SHALL be identical to the set the same positions produce when
  they arrive as a sidecar file, because the bitmap payload is the same format in both storage kinds
* *AND* the UDF MUST NOT echo the inline payload in any error or telemetry message, because it is an
  opaque encoded blob rather than a diagnostic

### Scenario: An absolute-path deletion vector is read without reconstructing a path

* *GIVEN* a scan invocation whose assigned files include a Delta data file carrying a deletion-vector
  delete mechanism whose storage kind is absolute path
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL read the deletion vector from that path verbatim and MUST NOT join it onto the
  table root, prepend a random prefix, or apply the `deletion_vector_<uuid>.bin` naming rule
* *AND* the post-delete row set SHALL be identical to the same positions delivered under the
  UUID-relative storage kind

### Scenario: A deletion-vector file shared by several data files is fetched once per shard

* *GIVEN* a shard whose assigned data files include two or more files whose deletion vectors resolve
  to the SAME sidecar path at DIFFERENT offsets
* *WHEN* the scan UDF builds the shard's per-data-file access plans
* *THEN* the UDF SHALL fetch that sidecar's bytes from object storage AT MOST ONCE for the whole
  shard, keyed on the resolved absolute path, and SHALL decode each descriptor against the retained
  bytes at that descriptor's own offset
* *AND* the UDF MUST NOT issue an object-store `HEAD` for a deletion-vector file, because the
  descriptor carries the deletion vector's size and not the sidecar's, so the size the scan would
  learn from a `HEAD` is a size it never needs
* *AND* the UDF SHALL fetch the WHOLE sidecar object rather than the byte range the descriptor's
  offset and size describe, because the decoder validates the container's leading format-version byte
  at file position 0 and a range starting at the descriptor's offset does not carry it
* *AND* the decoded positions for each referencing data file SHALL be identical to fetching and
  decoding that sidecar separately per data file

### Scenario: Concurrent deletion-vector reads stay within the connection budget

* *GIVEN* a shard whose assigned data files reference more unique deletion-vector sidecar paths than
  the resolved connection budget N
* *WHEN* the scan UDF fetches those sidecars to build its per-data-file access plans
* *THEN* the number of concurrently in-flight deletion-vector object-store reads MUST NOT exceed N at
  any instant, counted across every fan-out active in the scan invocation — a single table, or both
  sides of a broadcast join sharing the ONE size-N limiter that already bounds delete-file reads and
  data-file footer fetches
* *AND* the UDF SHALL fetch sidecars concurrently up to N in flight rather than strictly one at a time
* *AND* the resulting per-data-file position sets SHALL be identical to a strictly serial fetch,
  because decoding a deletion vector depends on no other deletion vector
* *AND* a sidecar fetch that fails SHALL surface as a credential-redacted user error naming the DATA
  file whose deletion vector could not be read, with no partial access plan attached and no row
  emitted for the shard

### Scenario: The decoder is handed bytes and never a live storage client

* *GIVEN* the scan UDF's deletion-vector pipeline, which fetches every sidecar it needs on the scan's
  own asynchronous, budget-bounded path before decoding anything
* *WHEN* the UDF invokes the `delta_kernel` deletion-vector decoder for a descriptor
* *THEN* the UDF SHALL satisfy the decoder's storage dependency with a read-only adapter that serves
  the ALREADY-FETCHED bytes from memory, so decoding performs no I/O, opens no object store, and
  starts no second async runtime inside the UDF
* *AND* every operation on that adapter other than reading an already-fetched object — listing,
  writing, copying, deleting, and metadata lookup — SHALL return a clean error naming the operation
  as unsupported, and MUST NOT panic, because a panic inside a UDF is an abnormal VM exit that makes
  the engine SIGKILL every sibling VM of the statement part
* *AND* the UDF MUST NOT construct a `delta_kernel` engine, a Delta snapshot, or any other
  scan-driving kernel object inside the scan, because DataFusion is this engine's only execution
  engine and a second one would compete for the UDF's bounded memory pool

### Scenario: A deletion vector the scan cannot trust fails loud before any row is emitted

* *GIVEN* a scan invocation whose assigned Delta data file carries a deletion vector that the decoder
  rejects — a container whose leading version byte is not `1`, a stored size that disagrees with the
  descriptor's declared size, a bitmap whose magic number is not the portable-format value, a bitmap
  whose CRC-32 checksum does not match, or a Z85 payload that does not decode
* *WHEN* the scan UDF prepares that data file's scan
* *THEN* the UDF SHALL return a clean user error that names the DATA file whose deletion vector could
  not be applied and states which validation failed, BEFORE emitting any row for that data file
* *AND* the UDF MUST NOT fall back to scanning the data file without the deletion vector, because
  pre-delete rows are wrong rows rather than a degraded result
* *AND* the error MUST be returned as an error value, never raised as a panic
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token, and
  MUST NOT echo an inline deletion vector's payload

### Scenario: Deletion vectors compose with projection, filter, LIMIT, and aggregation

* *GIVEN* a scan spec whose Delta data file carries a deletion vector AND whose common spec carries a
  projection, a filter predicate, and a LIMIT, and a second spec over the same file carrying a
  partial aggregate
* *WHEN* the scan UDF builds the DataFusion plan with the base `ParquetAccessPlan` attached
* *THEN* the Parquet opener SHALL intersect the injected row selection WITH predicate, row-group, and
  page pruning, so a row group provably excluded by the predicate is still skipped and a deleted row
  is still removed
* *AND* the rows the filter, the LIMIT, and any aggregation observe SHALL already be the post-delete
  rows, so a `COUNT(*)` over a data file reports the live row count rather than the physical one
* *AND* the emitted result SHALL equal the result of applying the deletion vector, projection, filter,
  and LIMIT over the full data on a single node

### Scenario: A Delta data file carrying no deletion vector scans unchanged

* *GIVEN* a scan invocation whose assigned Delta data files carry an EMPTY delete-mechanism list
* *WHEN* the scan UDF registers those files
* *THEN* the UDF SHALL scan them through the same provider with NO base `ParquetAccessPlan` attached,
  and MUST NOT fetch a deletion-vector sidecar or acquire a limiter permit for them
* *AND* the emitted rows SHALL be identical to the rows the same files produce for an equivalent
  Iceberg scan with no deletes attached, because the delete-free path is one path for both formats
