# Feature: Pushdown Module Dedup Consolidation

Tracks the pushdown planning layer's internal duplication-elimination extractions — one shared
shard-invariant dispatch base, one shared qualified-single-table-wrapper helper, one shared
request-shape classifier, one shared blind column-collecting traversal primitive, one shared
type-rewrite post-order primitive, and one shared ordered type-rewrite pipeline function — each
replacing a previously hand-duplicated mechanism with a single owned implementation.

This is the sibling of `vs-adapter/pushdown-module-structure`, split out once the base feature's
scenario count crossed this library's per-spec organization threshold.
`vs-adapter/pushdown-module-structure` owns the module boundary itself — the directory layout, the
frozen façade, and the behavior-preservation gate; this feature owns the running history of
internal-duplication extractions that module made room for.

## Background

* **This delta is issue #135. It amends ONE scenario and changes no consolidation rule.** The shared shard-invariant base, the two fallback guards, the request-shape classifier, the blind traversal primitive, the three type-rewrite guards, and the ordered pipeline are all UNCHANGED.
* **SUPERSEDES the recorded shared-base clause for `storage` and the byte-identity clause that followed it.** The shared base still carries the storage value and no construction site re-derives it; what it carries is now the tagged wrapper of `vs-adapter/scan-spec-credential-reference` rather than a bare backend, and the wrapper's reference variant carries no credential payload at all.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: The dispatcher builds each fan-out spec from one shared shard-invariant base

* *GIVEN* the pushdown dispatcher's fan-out construction sites, each previously repeating the same shard-invariant tail verbatim — logical schema, name mapping, absent join, storage, the four DataFusion tuning fields, the memory-pool fields, and the S3 connection budget — plus an empty files list
* *WHEN* the dispatcher constructs the scan spec for the grouped-aggregate, group-by fallback, lone-`COUNT(DISTINCT)`, multi/mixed-`COUNT(DISTINCT)` decline, and single-group/row-scan dispatch shapes
* *THEN* every site SHALL derive its shard-invariant fields from one shared base value and set only the fields that differ at that site
* *AND* the shared-base rule SHALL hold for `storage` as the scan-spec storage WRAPPER exactly as it held for a bare `StorageBackend` and before that a bare `StorageProps`: the base still carries the one storage value, no construction site re-derives it or chooses its variant, and the wrapper adds NO per-site field
* *AND* the scan-driving SQL generated for each dispatch shape MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value, which `vs-adapter/scan-spec-credential-reference` re-encodes as a reference carrying no credential or as a sealed envelope over that backend's byte-identical (pre-encryption) encoding
<!-- /DELTA:CHANGED -->
