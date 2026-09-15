# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

<!-- DELTA:NEW -->
* **This delta is issue #399.** It SUPERSEDES one Background bullet and amends ONE clause of ONE
  scenario. It changes no other clause, adds no scenario, and changes no merge, delete-encoding, or
  file-list rule.
* This delta SUPERSEDES the preceding Background bullet "The scan UDF's first argument is the
  shard-invariant common spec (projection, filter, limit, aggregates, group keys, logical schema,
  EMITS types, a storage reference the UDF resolves itself, the table root, and tuning knobs),
  serialized once per fan-out; the second argument is this shard's file list. See
  `datafusion-scan/scan-execution` for the scan behavior once the spec is merged." The common spec
  no longer carries EMITS types. `CommonScanSpec::emit_exa_types` is removed and the scan reads the
  declared output types from `UdfContext::output_column` instead. The field list is otherwise
  unchanged and reads: projection, filter, limit, aggregates, group keys, logical schema, a storage
  reference the UDF resolves itself, the table root, and tuning knobs.
* **The per-shard files-list argument is untouched**, because `emit_exa_types` was shard-invariant
  and lived only in the common blob.
* **Only a row-scan golden fixture changes.** The field carried
  `skip_serializing_if = "Vec::is_empty"`, so an aggregate spec already omitted it.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, byte-identical to the pre-consolidation encoding EXCEPT for the `storage` value, for the `emit_exa_types` key issue #399 removed, and, when a join block is present, that block's own `storage` value, and MUST NOT contain a `files` key or a `catalog` key
* *AND* the common-blob JSON MUST NOT contain an `emit_exa_types` key at all, because the call-site `EMITS (...)` clause is the sole declaration of the scan's output types and the scan reads it through `UdfContext::output_column`
* *AND* the `storage` value SHALL be the externally-tagged scan-spec storage WRAPPER specified by `vs-adapter/scan-spec-credential-reference` — a `connection` reference variant carrying a name and `allow_http` and no credential; a `sealed` variant carrying a connection name and the base64 nonce-plus-AES-GCM-ciphertext of the externally-tagged storage-backend encoding of `vs-adapter/storage-backend-enum`, which is byte-identical to the pre-consolidation `storage` object once unsealed; or an `inline` variant whose payload is that same backend encoding in plaintext, emitted by no adapter path and accepted for host-test spec construction
* *AND* the join block's `storage` value SHALL use that SAME wrapper encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* a common blob carrying NO join block SHALL be byte-identical to its pre-change encoding EXCEPT for the `storage` value's wrapper and the removed `emit_exa_types` key, so a committed golden common-blob fixture for a non-join spec passes unedited only when it carries neither, and is REGENERATED when it carries either
* *AND* the per-shard files-list JSON SHALL be byte-identical to the pre-consolidation encoding, because `storage` and the removed `emit_exa_types` were both shard-invariant and appeared only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy
<!-- /DELTA:CHANGED -->
