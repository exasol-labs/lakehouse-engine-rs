# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

* The refactor changes code organization only. It changes no query, pushdown, file-pruning, or type-handling behavior, so every scenario in the `vs-adapter/pushdown-planning*` and `vs-adapter/pushdown-file-pruning` features stays accurate and unedited.
* The pushdown planning layer decomposes into cohesive capability submodules (catalog credentials, file resolution, single-group aggregate, grouped aggregate, joins, top-N, namespace listing) plus one shared support submodule for cross-cutting SQL-builder and utility helpers. The exact submodule list is a design decision recorded in the plan, not a normative contract.
* `crate::adapter::pushdown` becomes a directory module (`pushdown/mod.rs` plus sibling files), so the import path `crate::adapter::pushdown::<name>` is unchanged for every consumer.
* A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader public than it had before.
* The CI/lint file-size guardrail (the second half of issue #129) is out of scope for this feature and remains open under issue #129.

## Scenarios

### Scenario: Public pushdown façade resolves at every pre-refactor path

* *GIVEN* a `name → visibility` snapshot of every symbol reachable via `crate::adapter::pushdown::<name>`, captured from the pre-refactor module before any code moves
* *WHEN* the same extraction re-runs against the refactored `pushdown/mod.rs` façade and all in-repo consumers compile
* *THEN* the re-extracted `name → visibility` set MUST diff empty against the captured baseline — no reachable item added, removed, narrowed, or widened
* *AND* every pre-refactor path `crate::adapter::pushdown::<name>` MUST still resolve to the same item at the same external visibility (`pub` or `pub(crate)`)
* *AND* the `adapter`, `scan`, and `capabilities` consumers MUST compile without editing any `use crate::adapter::pushdown::...` path
* *AND* a `#[cfg(test)]` reachability probe naming every pre-refactor `pub` and `pub(crate)` item from outside the `pushdown` module MUST compile, so an effective narrowing masked by a re-export is a compile error

### Scenario: Behavior is unchanged across the refactor

* *GIVEN* the pre-refactor unit and integration test suites for the pushdown planning layer
* *WHEN* the suites run against the refactored code
* *THEN* every test MUST pass with no change to any test assertion or expected value
* *AND* the scan-driving SQL generated for a given pushdown request MUST be byte-identical to the pre-refactor output

### Scenario: Each pushdown submodule owns its tests

* *GIVEN* the refactored pushdown submodules
* *WHEN* the test suite compiles
* *THEN* each capability submodule MUST contain a `#[cfg(test)] mod tests` covering only that submodule's own items
* *AND* no single central pushdown test module SHALL remain
* *AND* a test helper shared across submodules MUST live in one shared `#[cfg(test)]` support module rather than being duplicated
