# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

<!-- DELTA:NEW -->
* **This delta widens ONE carve-out in the two-argument-wire scenario and nothing else.** Issue #294 adds a REQUIRED `storage` field to the common blob's join block, so the clause requiring the common blob to be byte-identical to the pre-consolidation encoding needs that field carved out alongside the whole-spec `storage` value already carved out for `vs-adapter/storage-backend-enum`. Every other scenario of this feature is unchanged and no Background bullet is superseded.
* **The carve-out is safe on this feature's own recorded terms.** The legacy-file-list scenario already states that "the same `.so` produces and consumes the spec within one deploy (there is no cross-version wire-compatibility requirement)", so adding a required field inside the join block is a self-consistent intra-deploy encoding change, not a compatibility break.
* **The field is REQUIRED rather than defaulted, deliberately.** A `#[serde(default)]` on the join block's storage would let a join block that names no dimension backend deserialize into one that silently reuses the whole-spec (fact-side) backend — reinstating exactly the collapse issue #294 removes. Making it required turns "every join block names its own backend" into a property of the type rather than a rule an auditor has to verify at each of the seven `JoinSpec` construction sites.
* **The per-shard files-list argument (arg 1) is untouched.** The join block, and therefore its storage backend, is shard-invariant and lives only in the common blob.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, byte-identical to the pre-consolidation encoding EXCEPT for the `storage` value and, when a join block is present, that block's own `storage` value, and MUST NOT contain a `files` key or a `catalog` key
* *AND* the `storage` value SHALL be the externally-tagged storage-backend encoding specified by `vs-adapter/storage-backend-enum`, whose tagged payload is itself byte-identical to the pre-consolidation `storage` object, so the variant tag is the ONLY difference from the pre-consolidation common blob
* *AND* the join block's `storage` value SHALL use that SAME externally-tagged encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* a common blob carrying NO join block SHALL be byte-identical to its pre-change encoding, so every committed golden common-blob fixture for a non-join spec passes unedited
* *AND* the per-shard files-list JSON SHALL be byte-identical to the pre-consolidation encoding, because `storage` is shard-invariant and appears only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy
<!-- /DELTA:CHANGED -->
