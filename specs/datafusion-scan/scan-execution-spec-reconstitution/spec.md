# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* The scan UDF's first argument is the shard-invariant common spec (projection, filter,
  limit, aggregates, group keys, logical schema, EMITS types, storage credentials, the
  Iceberg table root, and tuning knobs), serialized once per fan-out; the second argument is
  this shard's file list. See `datafusion-scan/scan-execution` for the scan behavior once the
  spec is merged.
* The per-shard file list is a JSON array of compact `[path, size]` 2-tuples, where `path` is
  either relative to the common spec's table root or an absolute URI, and `size` is the file's
  byte size resolved from the Iceberg manifest by the adapter.
* `ScanSpec` carries no catalog identifier block — the scan UDF never contacts the catalog.
* A parse failure on either argument MUST surface an error identifying scan-spec
  deserialization failure and MUST NOT contain any storage access key, secret key, or
  session token.
* Per-file positional-delete references travel with their data-file entry in the per-shard
  argument.
* This delta amends ONE clause of the two-argument-wire scenario and nothing else. `vs-adapter/storage-backend-enum` (issue #274) wraps the common blob's `storage` value in an externally-tagged backend variant, so the clause requiring the common blob to be byte-identical to the pre-consolidation encoding needs the `storage` value carved out. Every other scenario of this feature is unchanged, and no Background bullet is superseded.
* The carve-out is safe on this feature's own recorded terms: the legacy-file-list scenario already states that "the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)". The tag is therefore a self-consistent intra-deploy encoding change, not a compatibility break, and that bullet's reasoning is unchanged.
* The per-shard file-list argument (arg 1) is untouched by the tag: `storage` is shard-invariant and lives only in the common blob.

## Scenarios

### Scenario: Scan reconstitutes the ScanSpec from the common and per-shard arguments

* *GIVEN* a scan invocation whose first argument is a common-spec JSON blob carrying every shard-invariant field (including the Iceberg table root) and whose second argument is a JSON array of `[path, size]` 2-tuples
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize the common-spec JSON and the per-shard file-list JSON and MERGE them into one `ScanSpec` value whose `files` are the `(path, size)` entries from the second argument and whose every other field — including the table root — comes from the first argument, equivalent to the pre-split single-argument spec for the same shard
* *AND* the merge SHALL store each file entry's path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration (see `datafusion-scan/scan-execution`)
* *AND* a parse failure on either argument SHALL surface an error that identifies scan-spec deserialization failure and MUST NOT contain any storage access key, secret key, or session token
* *AND* the reconstituted `ScanSpec` MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog

### Scenario: Reconstitution carries per-file positional-delete references

* *GIVEN* a scan invocation whose second argument is a JSON array of per-shard file entries, each carrying a data-file path, its byte size, and zero or more associated positional-delete file references (each with a path, byte size, and delete content type)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each file entry together with its associated delete-file references and MERGE them into one scan spec whose per-shard files (with deletes) come from the second argument and whose every other field comes from the first
* *AND* the merge SHALL store each data-file and delete-file path verbatim (relative or absolute) without resolving it, so path reconstruction is deferred to file registration
* *AND* the reconstituted scan spec MUST NOT carry any catalog identifier field, because the scan UDF never contacts the catalog

### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, byte-identical to the pre-consolidation encoding EXCEPT for the `storage` value, and MUST NOT contain a `files` key or a `catalog` key
* *AND* the `storage` value SHALL be the externally-tagged storage-backend encoding specified by `vs-adapter/storage-backend-enum`, whose tagged payload is itself byte-identical to the pre-consolidation `storage` object, so the variant tag is the ONLY difference from the pre-consolidation common blob
* *AND* the per-shard files-list JSON SHALL be byte-identical to the pre-consolidation encoding, because `storage` is shard-invariant and appears only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy

### Scenario: A file-list argument that predates the delete encoding still reconstitutes

* *GIVEN* a scan invocation whose second argument holds legacy file entries that carry a path and byte size but NO delete-file references (a spec that predates positional-delete support)
* *WHEN* the scan UDF parses its two input arguments
* *THEN* the UDF SHALL deserialize each legacy entry with its associated delete list defaulting to empty, so the entry reconstitutes as a delete-free data file
* *AND* a missing table root SHALL still be treated as "all paths are absolute" so no path is joined onto a root
* *AND* the resulting scan spec SHALL be usable by the shared scan path unchanged, because the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)
