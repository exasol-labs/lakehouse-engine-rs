# Feature: DataFusion Scan Execution, Spec Reconstitution

The scan UDF takes two arguments, a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1). It deserializes both and merges them into one `ScanSpec` before running the shared scan path.

## Background

* The common spec carries the projection, filter, limit, aggregates, group keys, logical schema, a storage reference, the table root, partition-column names, and tuning knobs. The adapter serializes it once per fan-out.
* The scan reads its declared output types from `UdfContext::output_column`, not from the spec.
* The per-shard file list is a JSON array of entries. Each entry has a `path`, either relative to the table root or an absolute URI, and a `size`, the file's byte size from the table's own metadata (an Iceberg manifest's `file_size_in_bytes`, a Delta `add` action's `size`).
* A file entry serializes in the shortest form its content allows: a `[path, size]` 2-tuple with no deletes and no partition values, a 3-tuple with deletes and no partition values, a JSON object with partition values.
* Per-file delete references (positional-delete files, Iceberg deletion vectors, and Delta deletion vectors) and per-file partition values travel with their data-file entry in the per-shard argument.
* `ScanSpec` carries no catalog identifier block. Every field is scan-time data (a path, a partition value, a deletion-vector byte range, a physical column name), never a catalog handle.
* The `storage` value in the common blob is the externally-tagged credential wrapper of `storage-access/scan-spec-credential-reference`. A join block carries its own required `storage` value in the same encoding.
* Delete mechanisms are format-neutral and self-describing, and serialize through a separate private wire form. An Iceberg deletion-vector member appends `content_offset`, `content_size_in_bytes`, and `record_count` to the `path`, `size`, and `content_type` keys. No other Iceberg member carries them.
* The same `.so` produces and consumes the spec within one deploy, so there is no cross-version wire-compatibility requirement.

## Scenarios

### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field (including the table root) and whose second argument is a JSON array of `[path, size]` 2-tuples
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the per-shard file-list JSON and MERGE them into one `ScanSpec` value whose `files` are the `(path, size)` entries from the second argument and whose every other field, including the table root, comes from the first argument, equivalent to the pre-split single-argument spec for the same shard
* *AND* the merge SHALL store each file entry's path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration (see `datafusion-scan/scan-execution`)
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
* *AND* the merge SHALL follow these same rules for arguments produced by EITHER format reader, because the table root and each entry's byte size are neutral values both populate

### Scenario: Reconstitution carries per-file delete references

* *GIVEN* a scan invocation whose second argument is a JSON array of per-shard file entries, each carrying a data-file path, its byte size, and zero or more associated delete mechanisms (an Iceberg positional-delete reference carrying a path, byte size, and delete content type, or an Iceberg deletion-vector reference that also carries its blob's offset, length, and cardinality)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each file entry together with its associated delete mechanisms and MERGE them into one scan spec whose per-shard files (with deletes) come from the second argument and whose every other field comes from the first
* *AND* each Iceberg positional-delete member SHALL deserialize from, and re-serialize to, its original `{"path":…,"size":…,"content_type":"position_deletes"}` encoding with its key order unchanged, so every committed 3-tuple golden encoding passes unedited
* *AND* an Iceberg deletion-vector member SHALL carry `content_offset`, `content_size_in_bytes`, and `record_count` after those three keys, and a member that carries any of them without being a deletion vector, or is a deletion vector missing one, SHALL fail with an error identifying scan-files deserialization
* *AND* the merge SHALL store each data-file and delete-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* the reconstituted scan spec MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog

### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, MUST NOT contain a `files` key, a `catalog` key, or an `emit_exa_types` key, and MUST NOT carry any declared output types (the call-site `EMITS (...)` clause is the sole declaration, read through `UdfContext::output_column`)
* *AND* the `storage` value SHALL be the externally-tagged scan-spec storage WRAPPER specified by `storage-access/scan-spec-credential-reference`, a `connection` reference variant carrying a name and `allow_http` and no credential; a `sealed` variant carrying a connection name and the base64 nonce-plus-AES-GCM-ciphertext of the externally-tagged storage-backend encoding of `storage-access/storage-backend-enum`; or an `inline` variant whose payload is that same backend encoding in plaintext, accepted for host-test spec construction
* *AND* the join block's `storage` value SHALL use that SAME wrapper encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* the per-shard files-list JSON SHALL be unaffected by the common blob's encoding, because `storage` and declared output types are both shard-invariant and appear only in the common blob
* *AND* reconstitution from the two arguments SHALL produce a `ScanSpec` value equal to the one the two-argument contract produces for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy

### Scenario: A file-list entry without delete references reconstitutes as a delete-free data file

* *GIVEN* a scan invocation whose second argument holds legacy file entries that carry a path and byte size but NO delete-file references (a spec that predates positional-delete support)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each legacy entry with its associated delete list defaulting to empty, so the entry reconstitutes as a delete-free data file
* *AND* a missing table root SHALL still be treated as "all paths are absolute" so no path is joined onto a root
* *AND* the resulting scan spec SHALL be usable by the shared scan path unchanged, because the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)

### Scenario: Reconstitution carries neutral partition values and a neutral delete mechanism list

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying the table's
  ordered partition-column names and a logical schema whose fields carry a field-id, a physical name,
  or neither, and whose second argument is a JSON array of per-shard file entries, each carrying a
  data-file path, its byte size, its partition values, and a delete list holding either Iceberg
  delete-file references or one Delta deletion-vector descriptor
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize both arguments and MERGE them into one scan spec whose per-shard
  partition values and delete mechanisms come from the second argument and whose partition-column
  names, logical schema, and every other shard-invariant field come from the first
* *AND* the merge SHALL store each data-file path and each deletion-vector `pathOrInlineDv` verbatim
  without resolving either, so path reconstruction stays deferred to file registration
* *AND* a file entry's partition values SHALL distinguish a partition column whose value is NULL from
  one that is absent from the map, because a NULL partition value is a value the scan materializes and
  an absent column is a planning defect
* *AND* each delete mechanism SHALL be SELF-DESCRIBING, naming its own mechanism on the wire, so the
  scan side reads one delete list and dispatches on its content without ever asking which table format
  produced the spec
* *AND* the neutral fields SHALL each be absent from JSON when empty, so an Iceberg common blob and an
  Iceberg file-list entry serialize byte-identically to their original encoding and every committed
  golden fixture passes unedited
* *AND* a file-list entry carrying partition values SHALL be a self-describing JSON OBJECT rather than
  a fourth tuple slot, so the 2-tuple legacy form and the 3-tuple delete-carrying form keep their
  exact encodings and their deserialization precedence
* *AND* the round trip SHALL be LOSSLESS in both directions for every combination the types admit, so
  no field is silently dropped by the shortest-form serialization rule
* *AND* an entry whose delete list MIXES a Delta deletion vector with an Iceberg delete-file reference
  SHALL be REFUSED with an error naming the entry by index, because the two are independent delete
  mechanisms and applying both to one data file returns wrong rows; the error MUST NOT echo the raw
  input
* *AND* the reconstituted scan spec MUST NOT carry the table's catalog-assigned credential-vending key
  or any other catalog identifier field, because the scan UDF never contacts the catalog
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or session token
