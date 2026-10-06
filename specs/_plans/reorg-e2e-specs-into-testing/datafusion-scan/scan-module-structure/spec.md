# Feature: Scan Module Structure

Decomposes the DataFusion scan-execution code in `scan/mod.rs` into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

<!-- DELTA:CHANGED -->
## Background

* The refactor changes code organization only. It changes no scan, pushdown, aggregate, join, field-id-projection, positional-delete, type-mapping, memory, concurrency, threading, or telemetry behavior, so every scenario in the `datafusion-scan/scan-execution`, `scan-runtime/scan-execution-connection-concurrency`, `datafusion-scan/scan-execution-expression-pushdown`, `scan-read-path/scan-execution-field-id-projection`, `datafusion-scan/scan-execution-file-metadata`, `scan-aggregation/scan-execution-grouped-agg`, `scan-aggregation/scan-execution-join`, `scan-runtime/scan-execution-memory-and-credentials`, `scan-aggregation/scan-execution-partial-agg`, `datafusion-scan/scan-execution-plan-shape`, `scan-read-path/scan-execution-positional-deletes`, `datafusion-scan/scan-execution-spec-reconstitution`, `scan-runtime/scan-execution-telemetry`, `scan-runtime/scan-execution-threading`, `datafusion-scan/scan-execution-value-conversion`, and `scan-types/type-mapping` features stays accurate and unedited.
* Because the refactor changes no scanning, pushdown, or schema/type behavior, the project's Iceberg-table-spec compliance check does not apply — the move touches file layout only, not read semantics. A behavior correction found during the close read is out of scope for this structural refactor and MUST be raised as a separate finding, never folded into a move.
* The scan layer decomposes into cohesive submodules (raw-row scan, broadcast join, partial aggregate, object-store and session setup, field-id projection) plus one shared SQL-support submodule for cross-cutting identifier helpers and one shared test-support submodule. The exact submodule list is a design decision recorded in the plan, not a normative contract.
* `crate::scan` stays a directory module. The pre-existing `pub mod` submodules (`convert`, `diagnostics`, `emit`, `positional_deletes`, `runtime`, `spec`) are untouched and keep their qualified `crate::scan::<submodule>::<name>` paths. New submodules are declared private (`mod`) and their public items are re-exported flat from `scan/mod.rs`, so the flat import path `crate::scan::<name>` is unchanged for every consumer.
* A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader visibility than it had before.
* The CI/lint file-size guardrail (the second half of issue #129) is out of scope for this feature and remains open under issue #129. This feature is partial progress on issue #129 and does not close it.
* **This delta is issue #135. It amends ONE scenario and changes no module boundary.** The public scan façade, the import-free consumer compilation, the consolidated SQL-builder helpers, and the per-submodule test layout are all UNCHANGED.
* **This feature's byte-identity gate is now carved out for the `storage` value alone.** The scan-driving SQL stays byte-identical for every spec shape EXCEPT for the `storage` value, which becomes the tagged wrapper of `storage-access/scan-spec-credential-reference`.
* `exasol-udf-sdk` 0.24.0 adds test doubles behind a `test-support` feature. The SDK double does NOT override `emit_record_batch_ipc` (trait default returns `Unimplemented`), so scan doubles that capture Arrow IPC emissions need a local wrapper.
* The `test-support` feature MUST be enabled only as a dev-dependency so it stays out of the production `.so`.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Behavior is unchanged across the refactor

* *GIVEN* the pre-refactor unit and integration test suites for the scan-execution layer
* *WHEN* the suites run against the refactored code
* *THEN* every test MUST pass with no change to any test assertion or expected value, EXCEPT for the two edits this delta's `storage` carve-out forces and no others: the eighteen credential-bearing golden dispatch fixtures are REGENERATED so their `storage` value carries the wrapper, and `common_blob_wire_is_byte_stable`'s pinned bytes gain the wrapper around the same backend encoding
* *AND* those two edits MUST change the `storage` value and nothing else — the six `empty_*` fixtures carry no `storage` value and SHALL stay byte-identical, and no assertion MUST be weakened, disabled, or deleted to accommodate the change
* *AND* the scan-driving SQL generated for a given raw-scan, broadcast-join, single-group partial-aggregate, and grouped partial-aggregate spec MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value, which carries the tagged wrapper of `storage-access/scan-spec-credential-reference`
<!-- /DELTA:CHANGED -->
