# Feature: DataFusion Scan Execution — Spec Reconstitution

Extends `datafusion-scan/scan-execution` with the mechanics of the scan UDF's two-argument
input: a shard-invariant common-spec JSON blob (arg 0) and a per-shard file list (arg 1),
which the UDF deserializes and merges into one `ScanSpec` before running the shared scan path.

## Background

* **This delta is issue #135. It amends ONE scenario and changes no reconstitution rule.** The two-argument contract, the per-shard `[path, size]` encoding, the positional-delete 3-tuple encoding, the legacy-entry defaulting, the neutral partition values, and the no-catalog-block rule are all UNCHANGED. What changes is what the `storage` value holds.
* **SUPERSEDES the recorded Background enumeration listing "storage credentials" among the shard-invariant common spec's contents.** The common spec now carries, per side, EITHER a reference to the Exasol CONNECTION that supplies that side's storage credentials OR an inline storage backend the planning layer vended.
* **SUPERSEDES the recorded clause that made the variant tag "the ONLY difference from the pre-consolidation common blob".** The `storage` value now carries a further enclosing wrapper whose reference variant holds no backend at all, specified by `vs-adapter/scan-spec-credential-reference`, which this feature CITES.
* **SUPERSEDES the recorded byte-identity clause for a non-join common blob.** A common blob carrying no join block is NO LONGER byte-identical to its pre-change encoding: its `storage` value gains the wrapper. Every committed golden common-blob fixture for a non-join spec that carries a `storage` value is regenerated; the six `empty_*` fixtures carry no `storage` value at all and stay byte-identical.
* **The per-shard files-list argument is still byte-identical**, because `storage` is shard-invariant and appears only in the common blob.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Consolidating the shard-invariant fields preserves the two-argument wire

* *GIVEN* a `ScanSpec` whose shard-invariant fields are held in one embedded `CommonScanSpec` value and whose only own field beside it is the per-shard `files` list
* *WHEN* the adapter serializes the shard-invariant common blob (UDF argument 0) and the per-shard files list (UDF argument 1)
* *THEN* the common-blob JSON SHALL carry every shard-invariant field at the top level, byte-identical to the pre-consolidation encoding EXCEPT for the `storage` value and, when a join block is present, that block's own `storage` value, and MUST NOT contain a `files` key or a `catalog` key
* *AND* the `storage` value SHALL be the externally-tagged scan-spec storage WRAPPER specified by `vs-adapter/scan-spec-credential-reference` — a `connection` reference variant carrying a name and `allow_http` and no credential, or an `inline` variant whose payload is the externally-tagged storage-backend encoding of `vs-adapter/storage-backend-enum`, itself byte-identical to the pre-consolidation `storage` object
* *AND* the join block's `storage` value SHALL use that SAME wrapper encoding and SHALL be a REQUIRED key of the join block, so a join block serialized without it fails to deserialize instead of defaulting to the whole-spec value
* *AND* a common blob carrying NO join block SHALL be byte-identical to its pre-change encoding EXCEPT for the `storage` value's wrapper, so a committed golden common-blob fixture for a non-join spec passes unedited only when it carries no `storage` value and is REGENERATED when it does
* *AND* the per-shard files-list JSON SHALL be byte-identical to the pre-consolidation encoding, because `storage` is shard-invariant and appears only in the common blob
* *AND* `from_parts_json` over the two arguments SHALL reconstitute a `ScanSpec` value equal to the one the pre-consolidation two-argument contract produced for the same shard, with the storage backend in place of the bare storage props
* *AND* `files` SHALL remain the sole per-shard field, now guaranteed structurally by the single embedded common value rather than by a field-by-field copy
<!-- /DELTA:CHANGED -->
