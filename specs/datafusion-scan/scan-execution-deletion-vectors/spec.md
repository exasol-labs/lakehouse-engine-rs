# Feature: DataFusion Scan Execution — Iceberg v3 Deletion Vector Application

Extends the scan UDF so that Iceberg format-version-3 **deletion vectors** (DVs) — Roaring-bitmap
delete sets stored as `deletion-vector-v1` Puffin blobs, one DV per data file — are applied on
read, so a query over a table whose deletes are encoded as DVs returns the post-delete row set
instead of silently returning pre-delete rows. This is the followup to the Parquet
positional-delete work (`datafusion-scan/scan-execution-positional-deletes`, issue #68) and
closes the deletion-vector half of the issue #11 silent-correctness bug (tracked as issue #12).
DVs matter because Databricks UniForm steers managed-Iceberg tables toward v3 DVs, so a DV-blind
reader returns wrong results on those tables.

The scan decodes the DV blob itself (iceberg-rust 0.10 reads the Puffin file container but does
NOT decode `deletion-vector-v1` payloads) into the SAME per-data-file deleted-position set used by
the positional-delete path, then reuses the existing `RowSelection` → `ParquetAccessPlan` machinery
verbatim, so DVs compose with projection / filter / LIMIT / row-group and page pruning exactly as
positional deletes do, and DataFusion's `ParquetSource` remains the scan engine.

## Background

* Scope is **Parquet data files whose deletes are encoded as one v3 `deletion-vector-v1` Puffin
  blob per data file**. Equality deletes, ORC/Avro data or delete files, and any non-`deletion-
  vector-v1` Puffin blob type remain OUT OF SCOPE and MUST fail loud (authoritative gate at plan
  time — see `vs-adapter/pushdown-file-pruning`).
* A DV is addressed through the normalized per-shard wire (see
  `datafusion-scan/scan-execution-spec-reconstitution`): the Puffin container is interned once in
  the `deleteFiles` pool (`type` `DV`, `format` `PUFFIN`, path + byte size) and the DV-backed data
  file carries a `deletes` reference whose `df` indexes that pool slot plus the blob's `offset` and
  `length` within the Puffin file. The scan MUST NOT re-derive these coordinates; the planning
  layer resolves them once from the manifest. The wire carries no `referenced_data_file` — the
  association is structural (the reference lives on the data file's entry) and the decoder
  cross-checks the blob's referenced-data-file from the Puffin `BlobMetadata` at read time.
* The scan opens the Puffin file through iceberg-rust's `PuffinReader` (file container + footer
  parsing + blob decompression), obtains the raw `deletion-vector-v1` blob bytes, and decodes
  those bytes itself. The engine MUST NOT rely on iceberg-rust to decode the blob payload
  (iceberg-rust has no DV decode support at the pinned version).
* The `deletion-vector-v1` blob payload layout the decoder MUST honor: a 4-byte big-endian
  combined length of (magic + serialized vector), a 4-byte magic sequence `D1 D3 39 64`, the
  position vector in the Roaring "portable" format (an 8-byte little-endian count of 32-bit
  bitmaps, then for each a 4-byte little-endian high key followed by a serialized 32-bit Roaring
  bitmap), and a 4-byte big-endian CRC-32 checksum over the magic bytes plus the serialized
  vector. Each 64-bit deleted row position is the 32-bit high key combined with a value from that
  key's 32-bit bitmap. The blob is never Puffin-compressed (`compression-codec` is omitted for
  `deletion-vector-v1`).
* **Cardinality validation:** the DV blob's Puffin `BlobMetadata` carries a `cardinality`
  property stating the expected number of deleted rows. After decoding the bitmap the scan MUST
  cross-check the decoded position count against that property and fail loud on mismatch — a
  mismatch signals a corrupt Puffin file or a parser bug, and silently misapplying it is exactly
  the silent-correctness failure this effort exists to close.
* **Magic and checksum validation:** the decoder MUST verify the magic bytes and the CRC-32
  checksum and fail loud (clean, credential-redacted error) on either mismatch, rather than
  emitting a wrong post-delete set.
* **Referenced-data-file cross-check:** because the wire no longer carries `referenced_data_file`,
  the scan MUST read the blob's `referenced-data-file` from the Puffin `BlobMetadata` and confirm
  it matches the data file the DV is being applied to, failing loud (clean, credential-redacted
  error) on mismatch. This preserves the correctness the dropped wire field would otherwise have
  carried.
* The decoded positions become a per-data-file `RoaringTreemap`, unioned into the SAME delete set
  the positional-delete path builds. The existing whole-file `RowSelection` and per-row-group
  `ParquetAccessPlan` construction is reused UNCHANGED — a DV-derived selection is
  indistinguishable downstream from a positional-delete-derived one.
* **Mixed-mechanism, per-data-file resolution:** one scan invocation's assigned files MAY include
  some data files backed by legacy Parquet positional-delete files and others backed by DVs (the
  realistic v2→v3 migration case). Each data file's delete mechanism is chosen independently from
  the content type of its own delete reference(s); the Iceberg v3 spec guarantees at most one DV
  per data file, so a given data file carries EITHER positional-delete file(s) OR one DV, never
  both — but different files in the same shard MAY use different mechanisms.
* Deletes are applied at the scan/decode layer, so the rows the DataFusion filter, LIMIT, top-N,
  and aggregation observe are already the post-delete rows.
* See `datafusion-scan/scan-execution-positional-deletes` for the shared per-data-file union point
  and the `RowSelection`/`ParquetAccessPlan` machinery, `datafusion-scan/scan-execution-spec-
  reconstitution` for the DV-carrying wire format, `vs-adapter/pushdown-file-pruning` for the
  plan-time DV-reference extraction from the manifest walk, and `packaging/e2e-harness-deletion-
  vectors` for the full-stack matrix.

## Scenarios

### Scenario: A deletion vector removes flagged rows

* *GIVEN* a scan invocation whose assigned files include a Parquet data file backed by one v3 `deletion-vector-v1` Puffin blob that marks specific row positions of that data file as deleted
* *WHEN* the scan UDF runs over its assigned files
* *THEN* the UDF SHALL resolve the data file's `deletes` reference through its `df` index to the pooled Puffin entry, open the Puffin file, fetch the blob at the reference's `offset`/`length`, decode the blob's Roaring position bitmap into a per-data-file deleted-position set, and attach the resulting row selection to the data file's scan as a base `ParquetAccessPlan`
* *AND* the UDF MUST NOT emit any row whose position is marked deleted by the deletion vector
* *AND* the UDF SHALL emit every non-deleted row of the data file unchanged

### Scenario: The decoder honors the deletion-vector-v1 binary layout

* *GIVEN* a raw `deletion-vector-v1` blob payload consisting of the 4-byte big-endian combined length, the `D1 D3 39 64` magic bytes, a Roaring "portable"-format position vector, and the trailing 4-byte big-endian CRC-32
* *WHEN* the scan UDF decodes the blob
* *THEN* the decoder SHALL reconstruct each 64-bit deleted row position by combining each Roaring bitmap's 32-bit high key with each value in that bitmap
* *AND* the decoder SHALL treat multi-key blobs (positions spanning more than 2^32) correctly by keying each 32-bit bitmap on its serialized high key
* *AND* the decoded position set SHALL equal the set of positions the writer encoded

### Scenario: A cardinality mismatch fails loud

* *GIVEN* a deletion-vector blob whose decoded position count differs from the `cardinality` property recorded in its Puffin `BlobMetadata`
* *WHEN* the scan UDF decodes the blob and compares the decoded count to the declared cardinality
* *THEN* the UDF SHALL return a clean error reporting the cardinality mismatch BEFORE emitting any row for the affected data file
* *AND* the UDF MUST NOT silently apply the mismatched delete set nor silently emit pre-delete rows
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: A corrupt magic or checksum fails loud

* *GIVEN* a deletion-vector blob whose magic bytes are not `D1 D3 39 64` or whose trailing CRC-32 does not match the CRC-32 of the magic bytes plus serialized vector
* *WHEN* the scan UDF decodes the blob
* *THEN* the UDF SHALL return a clean error identifying the malformed deletion-vector blob BEFORE emitting any row for the affected data file
* *AND* the UDF MUST NOT emit pre-delete rows for that data file
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: A referenced-data-file mismatch fails loud

* *GIVEN* a deletion-vector blob whose Puffin `BlobMetadata` records a `referenced-data-file` that does not match the data file the scan is applying it to
* *WHEN* the scan UDF decodes the blob and cross-checks the blob's referenced-data-file against the data file being read
* *THEN* the UDF SHALL return a clean error reporting the referenced-data-file mismatch BEFORE emitting any row for the affected data file
* *AND* the UDF MUST NOT silently apply the mismatched delete set nor silently emit pre-delete rows
* *AND* the error message MUST NOT contain any storage access key, secret key, or session token

### Scenario: A fully deleted data file yields no rows

* *GIVEN* a data file every one of whose row positions is marked deleted by its deletion vector
* *WHEN* the scan UDF runs over that data file
* *THEN* the UDF SHALL emit no rows from that data file
* *AND* the UDF MUST NOT error, because an empty post-delete result is a valid result

### Scenario: Deletion vectors compose with projection, filter, LIMIT, and pruning

* *GIVEN* a scan spec whose data file is backed by a deletion vector AND whose common spec carries a projection, a filter predicate, and a LIMIT
* *WHEN* the scan UDF builds the DataFusion plan with the DV-derived base `ParquetAccessPlan` attached
* *THEN* the Parquet opener SHALL intersect the injected delete row selection WITH predicate, row-group, and page pruning, so a row group provably excluded by the predicate is still skipped and a deleted row is still removed
* *AND* the rows the filter, LIMIT, and any aggregation observe SHALL already be the post-delete rows
* *AND* the emitted result SHALL equal the result of applying the deletes, projection, filter, and LIMIT over the full data on a single node

### Scenario: Mixed positional-delete and deletion-vector files in one shard resolve per data file

* *GIVEN* a single scan invocation whose assigned files include one data file backed by Parquet positional-delete file(s) and another data file backed by a v3 deletion vector
* *WHEN* the scan UDF prepares each data file's scan
* *THEN* the UDF SHALL choose each data file's delete mechanism independently from the `type` of the pooled `deleteFiles` entry each of that file's `deletes` references resolves to (via its `df` index), applying the positional-delete path to the `POS_DEL`-backed file and the deletion-vector path to the `DV`-backed file
* *AND* both delete-set constructions SHALL feed the SAME `RowSelection`/`ParquetAccessPlan` machinery, so the post-delete result is correct for every assigned file regardless of which mechanism backs it
* *AND* the emitted result SHALL equal the seeded rows minus every row deleted by either mechanism
