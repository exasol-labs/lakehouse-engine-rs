# Feature: Pushdown Joins Module Structure

Splits the oversized `pushdown::joins` submodule into a nested `joins/` directory module organized by concern behind an unchanged public façade, keeping generated SQL byte-identical and co-locating each concern's tests.

## Background

* The refactor changes code organization only. It changes no query, pushdown, file-pruning, or type-handling behavior, so every scenario in `vs-adapter/pushdown-planning-join` and `vs-adapter/pushdown-planning-join-fallback` stays accurate and unedited.
* `pushdown::joins` becomes a directory module: `joins/mod.rs` plus concern submodules (join planning, alias-qualified expression/filter/projection rendering, broadcast + N-scan SQL assembly). The exact submodule list is a design decision recorded in the plan, not a normative contract.
* `pushdown/mod.rs` keeps its `mod joins;`, `pub(crate) use joins::{...}`, and `use joins::{...}` statements byte-unchanged: a `joins.rs` file and a `joins/mod.rs` directory are interchangeable to `mod joins;`, so no consumer `use` path is edited.
* A helper used across concern submodules widens to the narrowest visibility that compiles (`pub(super)`, `super` = `joins`), never to a broader visibility than it held before.
* The shared `pushdown::test_support` fixtures stay at their pre-refactor location and visibility. Each joins concern submodule's test module reaches them across the added nesting level without duplicating them.
* This is the first nested module-within-a-module in `pushdown/`; every existing `pushdown/` submodule is a flat sibling file.
* The CI/lint file-size guardrail (the second half of issue #129) is out of scope for this feature and remains open under issue #129, alongside the other oversized files that issue tracks.

## Scenarios

### Scenario: Join pushdown façade resolves at every pre-refactor path

* *GIVEN* a `name → visibility` snapshot of every symbol the `joins` module exports to `crate::adapter::pushdown`, captured before any code moves — the nine `pub(crate)` items (`DetectedJoin`, `IneligibleJoinReason`, `JoinLeaf`, `JoinShape`, `JoinSides`, `RenderedJoinPushdown`, `ResolvedJoinSide`, `detect_join`, `render_broadcast_join`) and the five `pub(super)` items (`plan_join`, `qualify_udf`, `ineligible_join_decline`, `full_row_projection`, `build_grouped_qualified_fallback_sql`)
* *WHEN* the split completes and the whole crate compiles
* *THEN* the re-extracted `name → visibility` set MUST diff empty against the captured baseline — no item added, removed, narrowed, or widened
* *AND* both in-crate consumers MUST compile — the reachability probe (`src/adapter/pushdown_surface_probe.rs`), so narrowing any `pub(crate)` join symbol below `pub(crate)` becomes a build error rather than a silent gap, and `pushdown::grouped_agg`'s test module, which imports `build_grouped_qualified_fallback_sql` and `full_row_projection` via `super::super::joins::{...}` and MUST keep resolving after both move to the SQL-assembly submodule
* *AND* `pushdown/mod.rs` MUST keep its `mod joins;`, `pub(crate) use joins::{...}`, and `use joins::{...}` statements byte-unchanged
* *AND* every pre-refactor path `crate::adapter::pushdown::<name>` MUST resolve to the same item at the same visibility

### Scenario: joins becomes a nested directory module organized by concern

* *GIVEN* the pre-refactor single-file `pushdown/joins.rs`
* *WHEN* the split completes
* *THEN* `joins` MUST be a directory module — `joins/mod.rs` plus concern submodules — rather than a single file
* *AND* `joins/mod.rs` MUST retain the `plan_join` orchestrator and the cross-cutting `qualify_udf` and `ineligible_join_decline` helpers
* *AND* each concern submodule MUST own a single responsibility: join-shape detection with broadcast-side selection; alias-qualified expression, filter, and projection rendering; broadcast and N-scan SQL assembly
* *AND* a helper used across concern submodules MUST widen only to `pub(super)` (`super` = `joins`), never to a broader visibility than it held before

### Scenario: Generated join SQL is byte-identical across the split

* *GIVEN* a pre-refactor golden-SQL baseline capturing the exact built string from each join code path this refactor's duplication reductions can touch, before any code moves: a broadcast join (`build_broadcast_join_sql`), an N-scan fallback (`build_n_scan_join_sql`, with a fixture that includes both a side-local WHERE conjunct and a cross-side residual conjunct so the `side_local_filter`/`cross_side_residual_filter` shared shape is exercised), a grouped-qualified fallback (`build_grouped_qualified_fallback_sql`), and an ineligible decline (`ineligible_join_decline`'s `UdfError` message)
* *WHEN* the split and any duplication extraction complete and each code path is re-planned against the refactored code
* *THEN* a full-string equality assertion over each code path's built SQL — or the decline `UdfError` message — MUST equal its captured golden baseline byte-for-byte, verified by characterization tests that assert the entire returned string rather than a substring
* *AND* every scenario in `vs-adapter/pushdown-planning-join` and `vs-adapter/pushdown-planning-join-fallback` MUST still pass with no change to any test assertion or expected value, including `plan_join`'s empty-side short-circuit (which delegates to `pushdown::file_resolution::empty_result_sql`, a function this refactor does not move or touch, so it is verified by the existing suite rather than a new golden test here)

### Scenario: Each joins submodule owns its tests behind the shared fixtures

* *GIVEN* the refactored joins submodules
* *WHEN* the test suite compiles
* *THEN* each concern submodule MUST contain a `#[cfg(test)] mod tests` covering only that submodule's own items
* *AND* no single central `joins` test module SHALL remain
* *AND* the submodule test modules MUST reach the shared `pushdown::test_support` fixtures across the added nesting level without duplicating any fixture and without widening `test_support`'s visibility
