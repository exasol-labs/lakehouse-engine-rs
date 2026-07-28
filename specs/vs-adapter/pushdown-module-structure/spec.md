# Feature: Pushdown Module Structure

Decomposes the virtual-schema pushdown-planning code into single-responsibility submodules behind a preserved public façade, keeps behavior byte-identical, and co-locates each submodule's tests.

## Background

* The refactor changes code organization only. It changes no query, pushdown, file-pruning, or type-handling behavior, so every scenario in the `vs-adapter/pushdown-planning*` and `vs-adapter/pushdown-file-pruning` features stays accurate and unedited.
* The pushdown planning layer decomposes into cohesive capability submodules (catalog credentials, file resolution, single-group aggregate, grouped aggregate, joins, top-N, namespace listing) plus one shared support submodule for cross-cutting SQL-builder and utility helpers. The exact submodule list is a design decision recorded in the plan, not a normative contract.
* `crate::adapter::pushdown` becomes a directory module (`pushdown/mod.rs` plus sibling files), so the import path `crate::adapter::pushdown::<name>` is unchanged for every consumer.
* A cross-submodule private helper widens to the narrowest visibility that compiles (`pub(super)`), never to a broader public than it had before.
* The CI/lint file-size guardrail (the second half of issue #129) is out of scope for this feature and remains open under issue #129.
* This delta amends one clause set of the shared-classifier scenario: the classifier now
  resolves the grouped HAVING's merge-rendering as part of the routing decision, and returns
  the rendered fragment instead of the raw `having` node. Every other module-structure
  scenario is unchanged.
* The reason the rendering moves into the classifier is a routing reason, not a rendering
  one: whether the HAVING can be rewritten over the `PARTIAL_*` merge columns decides WHICH
  shape is available (partial/merge grouped, or the qualified single-table wrapper), so the
  decision cannot be deferred to a path that has already committed to one shape. See
  `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback` for the fallback behavior this enables
  (issue #195).
* Each path still renders its own SQL: the non-empty dispatch path splices the classifier's
  rendered HAVING fragment into the outer merge wrapper without re-rendering it, and the
  empty path ignores it because a zero-row result satisfies any HAVING.
* One rendering-level decline remains in the dispatcher — a grouped ORDER BY whose sort key
  resolves to no grouped output column — because it does not change the reachable shape set.
* This delta also removes the classifier's LAST grouped-tier hard error, the non-numeric
  aggregate column type carrying a HAVING. It rested on the same disproven premise: the
  qualified single-table wrapper renders the HAVING natively, so nothing is dropped. Both
  grouped declines — gate failure and unmergeable HAVING — now share the one fall-through exit
  to the wrapper, so the grouped tier returns `Ok` for every input.

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

### Scenario: The dispatcher builds each fan-out spec from one shared shard-invariant base

* *GIVEN* the pushdown dispatcher's fan-out construction sites, each previously repeating the same shard-invariant tail verbatim — logical schema, name mapping, absent join, storage, the four DataFusion tuning fields, the memory-pool fields, and the S3 connection budget — plus an empty files list
* *WHEN* the dispatcher constructs the scan spec for the grouped-aggregate, group-by fallback, lone-`COUNT(DISTINCT)`, multi/mixed-`COUNT(DISTINCT)` decline, and single-group/row-scan dispatch shapes
* *THEN* every site SHALL derive its shard-invariant fields from one shared base value and set only the fields that differ at that site
* *AND* the scan-driving SQL generated for each dispatch shape MUST be byte-identical to the pre-refactor output

### Scenario: Both qualified single-table fallback guards call one shared helper

* *GIVEN* the two near-identical dispatch guards that route to the qualified single-table wrapper — a `GROUP BY` request that declined grouped decomposition, and a multi or mixed `COUNT(DISTINCT)` single-group request
* *WHEN* each guard builds its referenced-column projection, its fan-out spec, and its wrapper SQL
* *THEN* both guards SHALL call one shared helper that performs the referenced-column-projection, fan-out-spec, and wrapper-SQL sequence
* *AND* the wrapper SQL each guard produces MUST be byte-identical to the pre-refactor output

### Scenario: One classifier decides the request shape for both the dispatch and empty-result paths

* *GIVEN* the request-routing decision — grouped aggregate first, then single-group aggregate, then row scan, applying the same aggregate-column-type validation gates and the same HAVING-present hard-error decline — previously encoded twice, once in the non-empty dispatcher and once in the empty-result path
* *WHEN* the adapter plans a pushdown request, whether data files remain or every file is pruned
* *THEN* the request shape SHALL be computed once by one shared classifier that both paths consume
* *AND* each path SHALL render only its own SQL from the shared decision — the non-empty path its scan-driving SQL, the empty path its shape-correct empty response
* *AND* the classifier SHALL additionally resolve the grouped HAVING's merge-rendering, because an unmergeable HAVING removes the partial/merge grouped shape from the reachable set and so is a routing decision, and SHALL carry the rendered HAVING fragment on the grouped shape it returns
* *AND* the non-empty dispatch path SHALL splice that rendered fragment into its outer merge wrapper WITHOUT re-rendering it, and MUST NOT retain its own HAVING-rendering decline, so exactly one place decides whether a HAVING can be merged
* *AND* the classifier SHALL raise NO grouped-tier hard error at all: a grouped request that does not decompose — for any reason, including a non-numeric aggregate column type whether or not a HAVING is present — SHALL fall through to the qualified single-table wrapper shape on both paths, because that wrapper renders the HAVING itself rather than dropping it
* *AND* a grouped request whose HAVING cannot be merged SHALL surface the same qualified-single-table-wrapper shape on both paths — the scan-driving wrapper SQL on the non-empty path, the typed zero-row wrapper shape on the empty path
* *AND* the scan-driving SQL and the empty-result response for every request whose HAVING merges unchanged MUST each remain byte-identical to their pre-delta output
