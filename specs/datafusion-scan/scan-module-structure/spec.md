# Feature: Scan Module Structure

The DataFusion scan-execution code in `scan/mod.rs` is split into single-responsibility submodules behind a preserved public façade. Behavior stays byte-identical, except for the `storage` value, and each submodule's tests sit beside it.

## Background

* The refactor changes code organization only. It changes no scan, pushdown, aggregate, join, field-id-projection, positional-delete, type-mapping, memory, concurrency, threading, or telemetry behavior.
* `crate::scan` stays a directory module. The `pub mod` submodules `convert`, `diagnostics`, `emit`, `positional_deletes`, `runtime`, and `spec` keep their qualified `crate::scan::<submodule>::<name>` paths. New submodules are private (`mod`) and their public items are re-exported flat from `scan/mod.rs`, so `crate::scan::<name>` is unchanged for every consumer.
* The scan layer splits into raw-row scan, broadcast join, partial aggregate, object-store and session setup, and field-id projection submodules, plus one shared SQL-support submodule and one shared test-support submodule.
* The byte-identity gate has one carve-out: the `storage` value is the tagged wrapper of `storage-access/scan-spec-credential-reference`.
* `exasol-udf-sdk` ships test doubles behind a `test-support` feature, enabled only as a dev-dependency so it stays out of the production `.so`. The SDK double does not override `emit_record_batch_ipc`, whose trait default returns `Unimplemented`, so a scan double that captures Arrow IPC emissions needs a local wrapper.

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
* *THEN* every test MUST pass with no change to any test assertion or expected value, EXCEPT for the two edits this delta's `storage` carve-out forces and no others: the eighteen credential-bearing golden dispatch fixtures are REGENERATED so their `storage` value carries the wrapper, and `common_blob_wire_is_byte_stable`'s pinned bytes gain the wrapper around the same backend encoding
* *AND* those two edits MUST change the `storage` value and nothing else — the six `empty_*` fixtures carry no `storage` value and SHALL stay byte-identical, and no assertion MUST be weakened, disabled, or deleted to accommodate the change
* *AND* the scan-driving SQL generated for a given raw-scan, broadcast-join, single-group partial-aggregate, and grouped partial-aggregate spec MUST be byte-identical to the pre-refactor output EXCEPT for the `storage` value, which carries the tagged wrapper of `storage-access/scan-spec-credential-reference`

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
* *AND* a test helper shared across submodules MUST live in one shared `scan/test_support_tests.rs` module rather than being duplicated

### Scenario: One shared double owns every scan test's Arrow IPC capture

* *GIVEN* the scan integration tests under `crates/lakehouse-engine/tests/` that each hand-roll a `UdfContext` double with an identical `emit_record_batch_ipc` decode loop
* *WHEN* the scan integration-test suite compiles and runs
* *THEN* exactly one double in `tests/scan_fixture/` SHALL own the decode and delegate every other `UdfContext` method to the SDK double
* *AND* the shared double SHALL retain per-call capture grouping so payload count stays distinguishable from batch count
* *AND* no file under `crates/lakehouse-engine/tests/` SHALL declare its own `impl UdfContext` (bare or fully-qualified form)
* *AND* every existing scan integration test MUST pass with no change to any assertion or expected value

### Scenario: Two in-crate doubles are retained for reachability

* *GIVEN* `src/scan/emit_tests.rs` `CapturingCtx` and `src/scan/test_support_tests.rs` `SinkCtx`
* *WHEN* the shared double lands in `tests/scan_fixture/`
* *THEN* both SHALL be retained: `tests/` modules are not nameable from `src/`, and neither duplicates the decode this delta consolidates
