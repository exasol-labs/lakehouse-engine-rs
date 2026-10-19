# Feature: Pushdown Joins Module Structure

Splits the `pushdown::joins` submodule into a nested `joins/` directory module organized by concern behind an unchanged public façade. Generated SQL stays byte-identical and each concern's tests live beside it.

## Background

* The split changes code organization only. No query, pushdown, file-pruning, or type-handling behavior changes.
* `joins/mod.rs` plus concern submodules (join planning, alias-qualified expression, filter and projection rendering, broadcast and N-scan SQL assembly) replace `pushdown/joins.rs`. The exact submodule list is not a normative contract.
* `pushdown/mod.rs` keeps its `mod joins;`, `pub(crate) use joins::{...}`, and `use joins::{...}` statements unchanged.
* The shared `pushdown::test_support` fixtures stay at their location and visibility.

## Scenarios

### Scenario: Join pushdown façade resolves at every pre-refactor path

* *GIVEN* a `name → visibility` snapshot of every symbol the `joins` module exports to `crate::adapter::pushdown`, captured before any code moves — the nine `pub(crate)` items (`DetectedJoin`, `IneligibleJoinReason`, `JoinLeaf`, `JoinShape`, `JoinSides`, `RenderedJoinPushdown`, `ResolvedJoinSide`, `detect_join`, `render_broadcast_join`) and the five `pub(super)` items (`plan_join`, `qualify_udf`, `ineligible_join_decline`, `full_row_projection`, `build_grouped_qualified_fallback_sql`)
* *WHEN* the split completes and the whole crate compiles
* *THEN* the re-extracted `name → visibility` set MUST diff empty against the captured baseline — no item added, removed, narrowed, or widened
* *AND* both in-crate consumers MUST compile — the reachability probe (`src/adapter/pushdown_surface_probe_tests.rs`), so narrowing any `pub(crate)` join symbol below `pub(crate)` becomes a build error rather than a silent gap, and `pushdown::grouped_agg`'s test module, which imports `build_grouped_qualified_fallback_sql` and `full_row_projection` via `super::super::joins::{...}` and MUST keep resolving after both move to the SQL-assembly submodule
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
* *THEN* a full-string equality assertion over each code path's built SQL — or the decline `UdfError` message — MUST equal its captured golden baseline byte-for-byte EXCEPT for the scan spec's `storage` value, which `storage-access/storage-backend-enum` re-encodes as an externally-tagged backend variant over a byte-identical payload and which `storage-access/scan-spec-credential-reference` then replaces with a connection reference or a sealed envelope over that same payload, verified by characterization tests that assert the entire returned string rather than a substring
* *AND* the permitted `storage` edit SHALL be the ONLY edit to any of the four golden strings, and the `ineligible_join_decline` message MUST stay unedited because it embeds no scan spec, so the decline path retains a fully untouched full-string gate
* *AND* every scenario in `pushdown-joins/pushdown-planning-join` and `pushdown-joins/pushdown-planning-join-fallback` MUST still pass with no change to any test assertion or expected value outside a `storage` value, including `plan_join`'s empty-side short-circuit (which delegates to `pushdown::file_resolution::empty_result_sql`, a function this refactor does not move or touch, so it is verified by the existing suite rather than a new golden test here)

### Scenario: Each joins submodule owns its tests behind the shared fixtures

* *GIVEN* the refactored joins submodules
* *WHEN* the test suite compiles
* *THEN* each concern submodule MUST contain a `#[cfg(test)] mod tests` covering only that submodule's own items
* *AND* no single central `joins` test module SHALL remain
* *AND* the submodule test modules MUST reach the shared `pushdown::test_support` fixtures across the added nesting level without duplicating any fixture and without widening `test_support`'s visibility

### Scenario: One shared template renders all six qualified N-scan render declines

* *GIVEN* the six `UdfError::User` declines the qualified N-scan render path raises, one per clause it cannot render — a select-list item (`n_scan_join_select_items`), absent involved-table column metadata and an unrenderable join condition (both `build_n_scan_join_sql`), a GROUP BY key (`qualified_join_group_by`), HAVING (`qualified_join_having`), and an ORDER BY key (`qualified_join_order_by`) — each of which writes the whole sentence `join pushdown declined: <clause>; this is a hard error, not a native re-plan` as its own string literal, and none of which has any assertion on its message text today
* *WHEN* all six are re-expressed through one shared decline constructor that receives only the clause-specific fragment
* *THEN* each of the six produced `UdfError::User` messages MUST equal its pre-refactor text byte-for-byte, asserted by full-string equality over the entire message rather than a substring check
* *AND* every message MUST keep the `; this is a hard error, not a native re-plan` tail, so `pushdown-joins/pushdown-planning-join`'s "declined safely" contract — a hard client error, never a native re-plan — still holds at all six sites
* *AND* the shared constructor MUST NOT absorb `ineligible_join_decline`, whose message carries the additional `the adapter cannot render this join shape` clause and is therefore a different sentence rather than a seventh instance of this one

### Scenario: One clause walk feeds both wrapper column-narrowing routines

* *GIVEN* `referenced_side_columns` (`joins/rendering.rs`) and `referenced_column_projection` (`joins/sql_builders.rs`), which each hand-roll the same walk over the clause set whose rendered SQL can name a source column — `selectList`, a non-null `filter`, `groupBy`, `orderBy`, and a non-null `having`
* *WHEN* both are re-expressed over one shared clause walk that takes its per-node collector from the caller
* *THEN* the clause set MUST have exactly one owner for `filter`, `groupBy`, `orderBy`, and `having`, so adding or removing one of those clauses SHALL require editing one function rather than two
* *AND* `referenced_side_columns` MUST keep naming `selectList` a second time in its short-circuit guard, because that guard is a fallback policy the walk deliberately does not own — so `selectList` is the one clause of the five with two named sites after the extraction, and that is a retained exception rather than an incomplete reduction
* *AND* each caller MUST keep its own collector and therefore its own case folding — `referenced_column_projection` still folds through `collect_all_column_names`' Unicode `to_uppercase`, `referenced_side_columns` still folds through `collect_side_column_names`' ASCII-only `to_ascii_uppercase` — because `pushdown/pushdown-module-structure`'s "One blind traversal primitive backs every column-collecting walk" scenario forbids reconciling that disagreement
* *AND* both divergent narrowing policies MUST survive unchanged — an absent or empty `selectList` MUST make `referenced_side_columns` return every column of `full_cols` without inspecting any other clause while `referenced_column_projection` MUST still narrow through the remaining clauses in that same case, and a narrowing that selects no column MUST yield all of `full_cols` for `referenced_side_columns` but exactly the first column of `all_cols` for `referenced_column_projection`, which MUST NOT return a zero-column projection because an empty Exasol `EMITS` clause is invalid
* *AND* `referenced_side_columns` MUST still collect from the join condition passed to it separately, a clause `referenced_column_projection` has no equivalent of and MUST NOT acquire

### Scenario: The two join-rendering pass-through wrappers are deleted rather than retained

* *GIVEN* `render_join_condition` and `render_selectlist_item_qualified` in `joins/rendering.rs`, whose entire bodies are one call — to `vs_expression::render_expression_safe` and to `render_expression_qualified` respectively — passing the same arguments through unchanged
* *WHEN* the duplication reduction completes
* *THEN* both names MUST be absent from the crate, with every production and test call site naming its delegate directly, so that no pass-through wrapper survives
* *AND* neither name appears in the façade baseline captured by "Join pushdown façade resolves at every pre-refactor path", so that baseline MUST still diff empty and this deletion MUST NOT be recorded as narrowing the façade
* *AND* the design intent each wrapper's doc comment carried MUST move onto the surviving delegate rather than be deleted with the wrapper — for the select-list path, that one recursive translator renders columns, literals, scalar expressions, a top-level `function_aggregate`, and a `function_aggregate` nested inside a scalar function, byte-compatibly with the former `render_aggregate_qualified`; for the join condition, that `render_expression_safe` rather than the filter renderer is used so a boolean condition is returned verbatim and never suppressed as trivially true
