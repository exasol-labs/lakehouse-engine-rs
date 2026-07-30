# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1), which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

<!-- DELTA:NEW -->
* This delta amends ONE clause of the two-argument-wire scenario and nothing else. `vs-adapter/storage-backend-enum` (issue #274) wraps the common blob's `storage` value in an externally-tagged backend variant, so the clause requiring the common blob to be byte-identical to the pre-consolidation encoding needs the `storage` value carved out. Every other scenario of this feature is unchanged, and no Background bullet is superseded.
* The carve-out is safe on this feature's own recorded terms: the legacy-file-list scenario already states that "the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)". The tag is therefore a self-consistent intra-deploy encoding change, not a compatibility break, and that bullet's reasoning is unchanged.
* The per-shard file-list argument (arg 1) is untouched by the tag: `storage` is shard-invariant and lives only in the common blob.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, byte-identical to the pre-consolidation encoding EXCEPT for the `storage` value, and MUST NOT contain a `files` key or a `catalog` key
* *AND* the `storage` value SHALL be the externally-tagged storage-backend encoding specified by `vs-adapter/storage-backend-enum`, whose tagged payload is itself byte-identical to the pre-consolidation `storage` object, so the variant tag is the ONLY difference from the pre-consolidation common blob
* *AND* the per-shard files-list JSON SHALL be byte-identical to the pre-consolidation encoding, because `storage` is shard-invariant and appears only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy
<!-- /DELTA:CHANGED -->
