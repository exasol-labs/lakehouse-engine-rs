# Feature: Scan Module Structure

Decomposes the DataFusion scan-execution code in `scan/mod.rs` into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

* The refactor changes code organization only. It changes no scan, pushdown, aggregate, join, field-id-projection, positional-delete, type-mapping, memory, concurrency, threading, or telemetry behavior, so every scenario in the `datafusion-scan/scan-execution`, `datafusion-scan/scan-execution-connection-concurrency`, `datafusion-scan/scan-execution-expression-pushdown`, `datafusion-scan/scan-execution-field-id-projection`, `datafusion-scan/scan-execution-file-metadata`, `datafusion-scan/scan-execution-grouped-agg`, `datafusion-scan/scan-execution-join`, `datafusion-scan/scan-execution-memory-and-credentials`, `datafusion-scan/scan-execution-partial-agg`, `datafusion-scan/scan-execution-plan-shape`, `datafusion-scan/scan-execution-positional-deletes`, `datafusion-scan/scan-execution-spec-reconstitution`, `datafusion-scan/scan-execution-telemetry`, `datafusion-scan/scan-execution-threading`, and `datafusion-scan/type-mapping` features stays accurate and unedited.
* Because the refactor changes no scanning, pushdown, or schema/type behavior, the project's Iceberg-table-spec compliance check does not apply — the move touches file layout only, not read semantics. A behavior correction found during the close read is out of scope for this structural refactor and MUST be raised as a separate finding, never folded into a move.
* The scan layer decomposes into cohesive submodules (raw-row scan, broadcast join, partial aggregate, object-store and session setup, field-id projection) plus one shared SQL-support submodule for cross-cutting identifier helpers and one shared test-support submodule. The exact submodule list is a design decision recorded in the plan, not a normative contract.
* `crate::scan` stays a directory module. The pre-existing `pub mod` submodules (`convert`, `diagnostics`, `emit`, `positional_deletes`, `runtime`, `spec`) are untouched and keep their qualified `crate::scan::<submodule>::<name>` paths. New submodules are declared private (`mod`) and their public items are re-exported flat from `scan/mod.rs`, so the flat import path `crate::scan::<name>` is unchanged for every consumer.
* A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader visibility than it had before.
* The CI/lint file-size guardrail (the second half of issue #129) is out of scope for this feature and remains open under issue #129. This feature is partial progress on issue #129 and does not close it.

## Scenarios

### Scenario: Public scan façade resolves at every pre-refactor path

* *GIVEN* a `name → visibility` snapshot of every symbol reachable via the flat path `crate::scan::<name>`, captured from the pre-refactor `scan/mod.rs` before any code moves
* *WHEN* the same extraction re-runs against the refactored `scan/mod.rs` re-export façade
* *THEN* the re-extracted `name → visibility` set MUST diff empty against the captured baseline — no reachable item added, removed, narrowed, or widened
* *AND* every pre-refactor path `crate::scan::<name>` MUST still resolve to the same item at the same external visibility (`pub` or `pub(crate)`)
* *AND* a `#[cfg(test)]` reachability probe naming every pre-refactor `pub` and `pub(crate)` item through the flat façade MUST compile, so an effective narrowing masked by a re-export is a compile error

### Scenario: Existing scan consumers compile without import edits

* *GIVEN* the untouched sibling module `crate::scan::positional_deletes` and every `tests/` integration crate
* *WHEN* they compile against the refactored code
* *THEN* `positional_deletes` MUST compile without editing its `use crate::scan::{...}` import
* *AND* every `tests/` integration crate MUST compile without editing any flat `use lakehouse_engine::scan::<name>` path
* *AND* the qualified `scan::<submodule>::...` paths of the untouched `pub mod` submodules (`convert`, `diagnostics`, `emit`, `positional_deletes`, `runtime`, `spec`) MUST remain resolvable unchanged

### Scenario: Behavior is unchanged across the refactor

* *GIVEN* the pre-refactor unit and integration test suites for the scan-execution layer
* *WHEN* the suites run against the refactored code
* *THEN* every test MUST pass with no change to any test assertion or expected value
* *AND* the scan-driving SQL generated for a given raw-scan, broadcast-join, single-group partial-aggregate, and grouped partial-aggregate spec MUST be byte-identical to the pre-refactor output

### Scenario: Consolidated SQL-builder helpers produce identical output

* *GIVEN* the uppercase-alias inner-SELECT construction, previously duplicated inline across the raw-scan SQL builder, the partial-aggregate builder, and the grouped partial-aggregate builder
* *WHEN* it is consolidated onto the single shared `build_alias_items` helper and each builder renders SQL for a representative spec
* *THEN* the rendered alias list and aliased inner SELECT MUST be byte-identical to the pre-refactor inline construction for every builder
* *AND* no builder's emitted SQL string SHALL change

### Scenario: Each scan submodule owns its tests

* *GIVEN* the refactored scan submodules
* *WHEN* the test suite compiles
* *THEN* each functional submodule MUST contain a `#[cfg(test)] mod tests` covering only that submodule's own items
* *AND* no single central scan test module SHALL remain in `scan/mod.rs` beyond the entry-and-dispatch tests for the items `mod.rs` retains
* *AND* a test helper shared across submodules MUST live in one shared `scan/test_support.rs` module rather than being duplicated
