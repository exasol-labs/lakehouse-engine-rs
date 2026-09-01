# Feature: Scan Module Structure

Decomposes the DataFusion scan-execution code in `scan/mod.rs` into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

* **This delta is issue #135. It amends ONE scenario and changes no module boundary.** The public scan façade, the import-free consumer compilation, the consolidated SQL-builder helpers, and the per-submodule test layout are all UNCHANGED.
* **SUPERSEDES this feature's byte-identity gate for the `storage` value alone.** The scan-driving SQL stays byte-identical for every spec shape EXCEPT for the `storage` value, which becomes the tagged wrapper of `vs-adapter/scan-spec-credential-reference`.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Behavior is unchanged across the refactor

* *GIVEN* the pre-refactor unit and integration test suites for the scan-execution layer
* *WHEN* the suites run against the refactored code
* *THEN* every test MUST pass with no change to any test assertion or expected value, EXCEPT for the two edits this delta's `storage` carve-out forces and no others: the eighteen credential-bearing golden dispatch fixtures are REGENERATED so their `storage` value carries the wrapper, and `common_blob_wire_is_byte_stable`'s pinned bytes gain the wrapper around the same backend encoding
* *AND* those two edits MUST change the `storage` value and nothing else — the six `empty_*` fixtures carry no `storage` value and SHALL stay byte-identical, and no assertion MUST be weakened, disabled, or deleted to accommodate the change
* *AND* the scan-driving SQL generated for a given raw-scan, broadcast-join, single-group partial-aggregate, and grouped partial-aggregate spec MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value, which carries the tagged wrapper of `vs-adapter/scan-spec-credential-reference`
<!-- /DELTA:CHANGED -->
