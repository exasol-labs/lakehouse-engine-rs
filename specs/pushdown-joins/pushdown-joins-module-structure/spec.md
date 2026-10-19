# Feature: Pushdown Joins Module Structure

Organizes the `pushdown::joins` submodule as a nested `joins/` directory module split by concern behind a stable `pub(crate)` surface. Generated SQL is pinned by golden tests and each concern's tests live beside it.

## Background

* The module structure is a code-organization contract. It adds no query, pushdown, file-pruning, or type-handling behavior.
* `joins/mod.rs` plus concern submodules (leg attribution, join planning, expression, filter, and projection rendering, broadcast and N-scan SQL assembly) hold the join pushdown. The exact submodule list is not a normative contract.
* `pushdown/mod.rs` declares `mod joins;` and re-exports the join surface through `pub(crate) use joins::{...}` and `use joins::{...}`.
* The shared `pushdown::test_support` fixtures are the single home of join test fixtures.

## Scenarios

### Scenario: Join pushdown façade resolves at every pre-refactor path

* *GIVEN* the `joins` module's exports to `crate::adapter::pushdown`: the nine `pub(crate)` items (`DetectedJoin`, `IneligibleJoinReason`, `JoinLeaf`, `JoinShape`, `JoinSides`, `RenderedJoinPushdown`, `ResolvedJoinSide`, `detect_join`, `render_broadcast_join`) and the `pub(super)` items the dispatcher and its tests consume
* *WHEN* the whole crate compiles
* *THEN* every `crate::adapter::pushdown::<name>` path for a `pub(crate)` item MUST resolve to the same item at `pub(crate)` visibility, with no item added, removed, narrowed, or widened
* *AND* both in-crate consumers MUST compile: the reachability probe (`src/adapter/pushdown_surface_probe_tests.rs`), so narrowing any `pub(crate)` join symbol becomes a build error rather than a silent gap, and the `pushdown::grouped_agg` test module, which imports join helpers through `super::super::joins::{...}`
* *AND* `pushdown/mod.rs` MUST declare `mod joins;` and re-export the surface through `pub(crate) use joins::{...}` and `use joins::{...}`

### Scenario: joins becomes a nested directory module organized by concern

* *GIVEN* the join pushdown code
* *WHEN* the crate is organized
* *THEN* `joins` MUST be a directory module: `joins/mod.rs` plus concern submodules, not a single file
* *AND* `joins/mod.rs` MUST retain the `plan_join` orchestrator and the cross-cutting `qualify_udf` and `ineligible_join_decline` helpers
* *AND* each concern submodule MUST own a single responsibility: leg attribution; join-shape detection with broadcast-side selection; alias-qualified expression, filter, and projection rendering; broadcast and N-scan SQL assembly
* *AND* a helper used across concern submodules MUST be no wider than `pub(super)` (`super` = `joins`)

### Scenario: Generated join SQL is byte-identical across the split

* *GIVEN* a golden-SQL baseline capturing the exact built string from each join code path: a broadcast join (`build_broadcast_join_sql`), an N-scan fallback (`build_n_scan_join_sql`, with a fixture that includes both a side-local WHERE conjunct and a cross-side residual conjunct), a grouped-qualified fallback, and an ineligible decline (`ineligible_join_decline`'s `UdfError` message)
* *WHEN* each code path is planned against the current code
* *THEN* a full-string equality assertion over each code path's built SQL, or the decline `UdfError` message, MUST equal its golden baseline byte-for-byte except for the scan spec's `storage` value, which `storage-access/storage-backend-enum` and `storage-access/scan-spec-credential-reference` own, asserted over the entire returned string rather than a substring
* *AND* the `ineligible_join_decline` message MUST be asserted in full with no exception, because it embeds no scan spec
* *AND* every scenario in `pushdown-joins/pushdown-planning-join` and `pushdown-joins/pushdown-planning-join-fallback` MUST pass, including `plan_join`'s empty-side short-circuit, which delegates to `pushdown::empty_result::empty_result_sql`

### Scenario: Each joins submodule owns its tests behind the shared fixtures

* *GIVEN* the joins submodules
* *WHEN* the test suite compiles
* *THEN* each concern submodule MUST declare its tests as a sibling `<name>_tests.rs` file through `#[cfg(test)] #[path = "..."] mod tests`, covering only that submodule's own items
* *AND* no single central test module for the submodules' items SHALL exist
* *AND* the submodule test modules MUST reach the shared `pushdown::test_support` fixtures without duplicating any fixture and without widening `test_support`'s visibility

### Scenario: One shared template renders all six qualified N-scan render declines

* *GIVEN* the `UdfError::User` declines the qualified N-scan render path raises, one per clause it cannot render: a select-list item, absent involved-table column metadata, an unrenderable join condition, a GROUP BY key, HAVING, and an ORDER BY key
* *WHEN* the path raises any of them
* *THEN* each message MUST come from one shared decline constructor (`join_render_decline`) that receives only the clause-specific fragment and writes the sentence `join pushdown declined: <clause>; this is a hard error, not a native re-plan`
* *AND* every message MUST keep the `; this is a hard error, not a native re-plan` tail, so `pushdown-joins/pushdown-planning-join`'s "declined safely" contract, a hard client error and never a native re-plan, holds at every site
* *AND* the shared constructor MUST NOT absorb `ineligible_join_decline`, whose message carries the additional `the adapter cannot render this join shape` clause and is a different sentence

### Scenario: One clause walk feeds both wrapper column-narrowing routines

* *GIVEN* `referenced_leg_columns` and `referenced_column_projection`, which both narrow columns over the clause set whose rendered SQL can name a source column: `selectList`, a non-null `filter`, `groupBy`, `orderBy`, and a non-null `having`
* *WHEN* both narrow over one shared clause walk (`referenced_clause_values`) that takes its per-node collector from the caller
* *THEN* the clause set MUST have exactly one owner for `selectList`, `filter`, `groupBy`, `orderBy`, and `having`, so adding or removing one of those clauses SHALL require editing one function rather than two
* *AND* `referenced_leg_columns` MUST keep its empty-`selectList` short-circuit outside the shared walk, because that guard is a fallback policy the walk deliberately does not own
* *AND* each caller MUST keep its own collector and therefore its own case folding: `referenced_column_projection` folds through `collect_all_column_names`' Unicode `to_uppercase`, `referenced_leg_columns` folds ASCII-only through `to_ascii_uppercase`, because `pushdown/pushdown-module-structure`'s "One blind traversal primitive backs every column-collecting walk" scenario forbids reconciling that disagreement
* *AND* the two narrowing policies MUST stay divergent: an absent or empty `selectList` MUST make `referenced_leg_columns` return every column of `full_cols` without inspecting any other clause while `referenced_column_projection` MUST still narrow through the remaining clauses in that same case, and a narrowing that selects no column MUST yield all of `full_cols` for `referenced_leg_columns` but exactly the first column of `all_cols` for `referenced_column_projection`, which MUST NOT return a zero-column projection because an empty Exasol `EMITS` clause is invalid
* *AND* `referenced_leg_columns` MUST collect from the join condition passed to it separately, a clause `referenced_column_projection` has no equivalent of and MUST NOT acquire

### Scenario: The two join-rendering pass-through wrappers are deleted rather than retained

* *GIVEN* the pass-through wrappers `render_join_condition` and `render_selectlist_item_qualified`, whose entire bodies would be one call to `vs_expression::render_expression_safe` and to `render_expression_qualified` respectively
* *WHEN* the crate is built
* *THEN* both names MUST be absent from the crate, with every production and test call site naming its delegate directly
* *AND* neither name belongs to the `pub(crate)` surface, so their absence MUST NOT narrow it
* *AND* the design intent of each wrapper MUST live on the surviving delegate: for the select-list path, that one recursive translator renders columns, literals, scalar expressions, a top-level `function_aggregate`, and a `function_aggregate` nested inside a scalar function; for the join condition, that `render_expression_safe` rather than the filter renderer is used so a boolean condition is returned verbatim and never suppressed as trivially true
