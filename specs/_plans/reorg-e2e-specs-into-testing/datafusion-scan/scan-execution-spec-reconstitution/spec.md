# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

<!-- DELTA:CHANGED -->
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
  by `storage-access/scan-spec-credential-reference`. A join block carries its own REQUIRED
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
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, MUST NOT contain a `files` key, a `catalog` key, or an `emit_exa_types` key, and MUST NOT carry any declared output types (the call-site `EMITS (...)` clause is the sole declaration, read through `UdfContext::output_column`)
* *AND* the `storage` value SHALL be the externally-tagged scan-spec storage WRAPPER specified by `storage-access/scan-spec-credential-reference` — a `connection` reference variant carrying a name and `allow_http` and no credential; a `sealed` variant carrying a connection name and the base64 nonce-plus-AES-GCM-ciphertext of the externally-tagged storage-backend encoding of `storage-access/storage-backend-enum`; or an `inline` variant whose payload is that same backend encoding in plaintext, accepted for host-test spec construction
* *AND* the join block's `storage` value SHALL use that SAME wrapper encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* the per-shard files-list JSON SHALL be unaffected by the common blob's encoding, because `storage` and declared output types are both shard-invariant and appear only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy
<!-- /DELTA:CHANGED -->
