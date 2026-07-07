# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard files argument (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* The scan UDF's first argument is the shard-invariant common spec (projection, filter,
  limit, aggregates, group keys, logical schema, EMITS types, storage credentials, the
  Iceberg table root, and tuning knobs), serialized once per fan-out; the second argument is
  this shard's files argument. See `datafusion-scan/scan-execution` for the scan behavior once
  the spec is merged.
<!-- DELTA:CHANGED -->
* The per-shard argument is a JSON OBJECT with two arrays — an interned `deleteFiles` pool and a
  `dataFiles` list — NOT a bare array of file entries. Each `dataFiles` entry carries `path`
  (either relative to the common spec's table root or an absolute URI) and `size` (the file's byte
  size resolved from the Iceberg manifest by the adapter), plus an OPTIONAL `deletes` array that is
  omitted or empty when the data file has no deletes.
<!-- /DELTA:CHANGED -->
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* A parse failure on either argument MUST surface an error identifying scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or
  session token.
<!-- DELTA:CHANGED -->
* The `deleteFiles` pool interns each physical delete file or container EXACTLY ONCE per shard,
  regardless of how many data files reference it. Each pool entry carries a `path`, a `size`, a
  `type` (`POS_DEL`, `EQ_DEL`, or `DV`), and a `format` (`PARQUET`, `AVRO`, `ORC`, or `PUFFIN`),
  serialized in SCREAMING_SNAKE_CASE.
* A `dataFiles` entry associates its data file with delete files structurally: each element of its
  `deletes` array is a reference object carrying `df` (an integer index into the `deleteFiles`
  pool) plus OPTIONAL `offset` and `length`. The `offset`/`length` pair is present ONLY for a
  blob-addressed source — a deletion-vector blob inside a Puffin container — and is ABSENT for a
  whole-file positional-delete or equality-delete file. The association between a delete file and a
  data file therefore lives on the data file's `deletes` list; no `referenced_data_file` field is
  carried on the wire. The deletion-vector decoder can still cross-check the blob's
  referenced-data-file against the Puffin `BlobMetadata` at read time, so no correctness is lost by
  dropping it.
* Because the same `.so` produces and consumes this spec within one deploy, there is NO
  cross-version wire-compatibility requirement; the per-shard argument is this normalized object
  form only, with no legacy tuple encoding to accept.
* When the common-spec blob (arg 0) carries a broadcast-join dimension side, that dimension file
  list uses this SAME normalized `deleteFiles`/`dataFiles` shape (its pool scoped to the
  shard-invariant join block), so a dimension table's own positional deletes and deletion vectors
  reconstitute exactly as the per-shard fact-side files do — there is one file-set encoding across
  both the fact (arg 1) and dimension (arg 0) sides.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field (including the Iceberg table root) and whose second argument is a JSON object with a `deleteFiles` pool and a `dataFiles` list
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the per-shard object and MERGE them into one `ScanSpec` value whose `files` are the `dataFiles` entries (each with its resolved delete references) and whose every other field — including the table root — comes from the first argument, equivalent to the pre-split single-argument spec for the same shard
* *AND* the merge SHALL store each data-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration (see `datafusion-scan/scan-execution`)
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Reconstitution interns each delete file once and resolves df-indexed references

* *GIVEN* a scan invocation whose second argument is a per-shard object in which one partition-granularity positional-delete file is referenced by two or more `dataFiles` entries, the delete file appearing EXACTLY ONCE in the `deleteFiles` pool and each referencing data file carrying a `deletes` entry whose `df` indexes that one pool slot
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the `deleteFiles` pool once and, for each data file, resolve every `deletes` entry's `df` index to the corresponding pooled delete file, so the shared positional-delete file is associated with every data file that references it WITHOUT being repeated on the wire
* *AND* the merge SHALL store each data-file and delete-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* a positional-delete reference SHALL carry no `offset`/`length`, and the reconstituted scan spec MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Reconstitution carries per-file deletion-vector references

* *GIVEN* a scan invocation whose second argument's `deleteFiles` pool contains a Puffin container entry (`type` `DV`, `format` `PUFFIN`) and whose `dataFiles` list has a data-file entry carrying a `deletes` reference with a `df` index into that pool plus an `offset` and a `length` locating the blob within the Puffin container
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the deletion-vector reference with its `df`, `offset`, and `length` intact and resolve `df` to the pooled Puffin entry, associating the blob coordinates with the data-file entry they belong to
* *AND* the merge SHALL store the Puffin file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* the reconstituted reference SHALL carry no `referenced_data_file` field, because the association is structural (it lives on the data file's `deletes` list) and the decoder re-derives and cross-checks it from the Puffin `BlobMetadata` at read time
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: A mixed positional-delete and deletion-vector shard round-trips

* *GIVEN* a per-shard object whose `deleteFiles` pool holds both a positional-delete file (`type` `POS_DEL`) and a Puffin deletion-vector container (`type` `DV`), and whose `dataFiles` list has one data file referencing the positional-delete pool slot and another referencing the deletion-vector pool slot, and MAY include a single data file that references BOTH
* *WHEN* the scan UDF serializes and then re-parses the per-shard object
* *THEN* every `deletes` reference SHALL round-trip with its `df` index resolving to the correct pooled delete file and its content `type` preserved, and ONLY the deletion-vector references SHALL carry `offset`/`length`
* *AND* a data file that references both a positional-delete file and a deletion vector SHALL reconstitute with both references present in its `deletes` list
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: A data file with no deletes reconstitutes in the compact form

* *GIVEN* a scan invocation whose second argument holds `dataFiles` entries that carry a `path` and `size` but OMIT the `deletes` array (or carry an empty one), and whose `deleteFiles` pool is empty
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each such entry with its delete list defaulting to empty, so the entry reconstitutes as a delete-free data file
* *AND* a missing table root SHALL still be treated as "all paths are absolute" so no path is joined onto a root
* *AND* the resulting scan spec SHALL be usable by the shared scan path unchanged, because the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement and no legacy tuple form to accept)
<!-- /DELTA:CHANGED -->
</content>
</invoke>
