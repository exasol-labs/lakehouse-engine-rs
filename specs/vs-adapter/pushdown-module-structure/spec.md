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
* This delta adds ONE scenario, the blind column-collecting traversal primitive (issue #177). Every existing scenario of this feature is unchanged, and no scenario of any `vs-adapter/pushdown-planning*` feature changes, because the extraction moves no decision and alters no generated SQL.
* `pub(super)` on an item of `adapter::pushdown::support` makes it visible in `adapter::pushdown` AND every descendant of `adapter::pushdown`, including `adapter::pushdown::joins::rendering`. The primitive therefore reaches both joins-side walks at the visibility ceiling the `vs-adapter/pushdown-joins-module-structure` feature already imposes, so no `use` path and no visibility widens on the joins side.
* The joins-side verification asset already exists and is already scoped to this change: `vs-adapter/pushdown-joins-module-structure`'s "Generated join SQL is byte-identical across the split" scenario captures a golden-SQL baseline over "any duplication extraction" across the broadcast, N-scan-fallback, grouped-qualified-fallback, and ineligible-decline paths, and the in-code gate carries the instruction to re-run after every dedup extraction. This delta consumes that baseline rather than restating it, so `vs-adapter/pushdown-joins-module-structure` needs no delta.
* The two case-folding calls this codebase uses are NOT interchangeable. `str::to_uppercase` applies full Unicode case mapping; `str::to_ascii_uppercase` leaves every non-ASCII byte alone. The three walks disagree today (`collect_all_column_names` uses the Unicode form, both joins walks use the ASCII form), and reconciling that disagreement is a behavior change outside this feature's scope.
* Issue #257 owns a SECOND, different traversal primitive: a curated-field, post-order rewrite walker for the three type-rewrite guards. The two primitives stay separate because a rewrite MUST NOT descend into and rebuild `dataType` or `name` sub-objects, whereas a collect is read-only and so must traverse every field. Neither issue merges them.
* Scope boundary of this delta: the rewrite-shaped walks are untouched — `annotate_columns_with_alias`, `strip_table_alias`, and the three `support` type-rewrite guards keep their own recursion, none of which is a column-collecting traversal.

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

### Scenario: One blind traversal primitive backs every column-collecting walk

* *GIVEN* the three read-only column-collecting walks that each hand-roll the same recursion — match a JSON object, act when its `type` is `column`, then recurse over every field value; match a JSON array and recurse over every element; stop at any other node — namely `collect_all_column_names` in `pushdown/support.rs`, plus `collect_column_tables` and `collect_side_column_names` in `pushdown/joins/rendering.rs`
* *WHEN* the pushdown layer collects column references from a `selectList`, a `filter` or `having` expression tree, a `groupBy` or `orderBy` array, or a join condition
* *THEN* exactly one traversal primitive in the shared `support` submodule SHALL own both the recursion and the `type == "column"` test, invoking a caller-supplied callback once per `column` object node and passing that node's own field map
* *AND* the primitive SHALL traverse blindly — every field of every object and every element of every array — because a collect rebuilds nothing, and a column reference can sit arbitrarily deep inside a function call, a `CASE`, or a comparison predicate
* *AND* the primitive SHALL be declared `pub(super)` in `support`, which already reaches `pushdown` and its `joins::rendering` descendant, so NO item's visibility widens and no join-module `use` path changes
* *AND* each of the three walks SHALL reduce to one call to that primitive with a closure over the `column` node's field map, and SHALL NOT retain a JSON-object or JSON-array recursion arm of its own, so the pushdown module tree holds ONE blind column-collecting traversal instead of three
* *AND* each closure MUST keep its predecessor's case-folding call verbatim — the Unicode `to_uppercase` for `collect_all_column_names`, the ASCII-only `to_ascii_uppercase` for both joins walks — because the two disagree for non-ASCII column and table names, so unifying them SHALL NOT happen under this scenario
* *AND* `collect_column_tables` MUST keep its `pub(super)` visibility and its three accumulator out-parameters, and `collect_side_column_names` MUST keep its private visibility and its signature, so `conjunct_single_side`, `referenced_side_columns`, and the N-scan side-attribution caller in `joins/sql_builders.rs` compile unedited
* *AND* every existing pushdown, joins, and top-N test MUST pass with no change to any test assertion or expected value, including the four join golden-SQL full-string assertions, the two `dispatch_golden` decline-wrapper assertions — the declined `GROUP BY` fallback and the multi/mixed `COUNT(DISTINCT)` decline, whose committed goldens both carry a narrowed inner-scan `projection` — and the declined-`ORDER BY` hidden-column assertions; this is the gate that makes "no behavior change" falsifiable here, because the collected sets drive side-local versus cross-side conjunct partitioning, per-side column narrowing, the qualified-wrapper inner-scan projection, and the hidden-column append order, every one of which is visible in the generated SQL
