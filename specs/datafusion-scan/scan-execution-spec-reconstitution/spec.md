# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* The scan UDF's first argument is the shard-invariant common spec (projection, filter,
  limit, aggregates, group keys, logical schema, a storage reference the UDF resolves
  itself, the table root, partition-column names, and tuning knobs), serialized once per
  fan-out; the second argument is this shard's file list. The scan reads its declared
  output types from `UdfContext::output_column`, not from the spec. See
  `datafusion-scan/scan-execution` for the scan behavior once the spec is merged.
* The per-shard file list is a JSON array of compact `[path, size]` 2-tuples, where `path` is
  either relative to the common spec's table root or an absolute URI, and `size` is the file's
  byte size resolved from the table's own metadata by its format reader — an Iceberg manifest's
  `file_size_in_bytes` for an Iceberg table, a Delta `add` action's `size` for a Delta one.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
  Every field the spec carries is scan-time data (a path, a partition value, a deletion-vector
  byte range, a physical column name), never a catalog handle.
* A parse failure on either argument MUST surface an error identifying scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or
  session token.
* Per-file positional-delete references and per-file partition values travel with their
  data-file entry in the per-shard argument.
* The `storage` value in the common blob is an externally-tagged credential WRAPPER specified
  by `vs-adapter/scan-spec-credential-reference`. A join block carries its own REQUIRED
  `storage` value in the same wrapper encoding, so a join block that names no dimension
  backend fails to deserialize rather than silently reusing the fact-side backend.
* Delete mechanisms are format-neutral and self-describing. `DeleteMechanism` routes
  (de)serialization through a private wire enum so the Rust-level type and the frozen JSON
  encoding are independent decisions. An entry whose delete list mixes a deletion vector
  with an Iceberg delete-file reference is refused.
* File-entry serialization form is selected by content: compact 2-tuple when neither
  deletes nor partition values are present, 3-tuple when deletes but no partition values,
  self-describing JSON object when partition values are present.
* There is no cross-version wire-compatibility requirement — the same `.so` produces and
  consumes the spec within one deploy.

## Scenarios

### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field (including the table root) and whose second argument is a JSON array of `[path, size]` 2-tuples
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the per-shard file-list JSON and MERGE them into one `ScanSpec` value whose `files` are the `(path, size)` entries from the second argument and whose every other field — including the table root — comes from the first argument, equivalent to the pre-split single-argument spec for the same shard
* *AND* the merge SHALL store each file entry's path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration (see `datafusion-scan/scan-execution`)
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog
* *AND* the merge SHALL follow these same rules for arguments produced by EITHER format reader, because the table root and each entry's byte size are neutral values both populate

### Scenario: Reconstitution carries per-file positional-delete references

* *GIVEN* a scan invocation whose second argument is a JSON array of per-shard file entries, each carrying a data-file path, its byte size, and zero or more associated delete mechanisms (an Iceberg positional-delete reference carrying a path, byte size, and delete content type)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each file entry together with its associated delete mechanisms and MERGE them into one scan spec whose per-shard files (with deletes) come from the second argument and whose every other field comes from the first
* *AND* each Iceberg positional-delete member SHALL deserialize from, and re-serialize to, its pre-#342 `{"path":…,"size":…,"content_type":"position_deletes"}` encoding with its key order unchanged, so every committed 3-tuple golden encoding passes unedited
* *AND* the merge SHALL store each data-file and delete-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* the reconstituted scan spec MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog

### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, MUST NOT contain a `files` key, a `catalog` key, or an `emit_exa_types` key, and MUST NOT carry any declared output types (the call-site `EMITS (...)` clause is the sole declaration, read through `UdfContext::output_column`)
* *AND* the `storage` value SHALL be the externally-tagged scan-spec storage WRAPPER specified by `vs-adapter/scan-spec-credential-reference` — a `connection` reference variant carrying a name and `allow_http` and no credential; a `sealed` variant carrying a connection name and the base64 nonce-plus-AES-GCM-ciphertext of the externally-tagged storage-backend encoding of `vs-adapter/storage-backend-enum`; or an `inline` variant whose payload is that same backend encoding in plaintext, accepted for host-test spec construction
* *AND* the join block's `storage` value SHALL use that SAME wrapper encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* the per-shard files-list JSON SHALL be unaffected by the common blob's encoding, because `storage` and declared output types are both shard-invariant and appear only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy

### Scenario: A file-list argument that predates the delete encoding still reconstitutes

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
  positional-delete references or one deletion-vector descriptor
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
  Iceberg file-list entry serialize byte-identically to their pre-#342 encoding and every committed
  golden fixture passes unedited
* *AND* a file-list entry carrying partition values SHALL be a self-describing JSON OBJECT rather than
  a fourth tuple slot, so the 2-tuple legacy form and the 3-tuple delete-carrying form keep their
  exact encodings and their deserialization precedence
* *AND* the round trip SHALL be LOSSLESS in both directions for every combination the types admit, so
  no field is silently dropped by the shortest-form serialization rule
* *AND* an entry whose delete list MIXES a deletion vector with an Iceberg delete-file reference
  SHALL be REFUSED with an error naming the entry by index, because the two are independent delete
  mechanisms and applying both to one data file returns wrong rows; the error MUST NOT echo the raw
  input
* *AND* the reconstituted scan spec MUST NOT carry the table's catalog-assigned credential-vending key
  or any other catalog identifier field, because the scan UDF never contacts the catalog
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or session token
