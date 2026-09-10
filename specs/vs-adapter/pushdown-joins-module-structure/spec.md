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
* A second duplication-reduction pass (issue #181) runs inside the already-split `joins/` directory module. It moves no code between submodules and changes no visibility upward, so the façade baseline and the concern split recorded above stay accurate and unedited.
* Two reductions have no observable surface of their own and are verified entirely by the golden-SQL scenario below, so they get no scenario: `collect_column_tables` returning its three outputs as `column_tables` instead of writing three `&mut` out-params (its two callers each build a fresh set per use, so the tuple form is the natural one), and the shared `shard_count` → `partition_files_by_bytes` → `relativize_shards_to_root` prefix of `build_side_fan_out_sql` and `build_broadcast_join_sql` moving into one side-sharding helper.
* The trailing `build_scan_driving_sql(…, None, None, &[], &[], …)` argument tail that both fan-out builders repeat is deliberately NOT wrapped. A wrapper taking the six arguments that genuinely differ, to elide four literal empties, would be a shallow layer rather than a reduction.
* Issue #181's finding 2 (`involved_table_columns` versus `extract_all_column_types`) is tracked separately as issue #265 and is out of scope here; neither function is touched.
* This delta carves the scan spec's `storage` value out of ONE clause — the byte-for-byte golden-baseline clause of the "Generated join SQL is byte-identical across the split" scenario. It amends no other clause, supersedes no Background bullet, and changes no module boundary, visibility rule, or render path.
* `vs-adapter/storage-backend-enum` (issue #274) wraps the scan spec's `storage` value in an externally-tagged backend variant, and `vs-adapter/scan-spec-credential-reference` then replaces it with a connection reference or a sealed envelope over that same payload. Three of the four golden strings embed a scan spec and therefore change: the broadcast join, the N-scan fallback, and the grouped-qualified fallback. The fourth — `ineligible_join_decline`'s `UdfError` message — embeds no scan spec and is unedited, so the decline path keeps an untouched full-string gate.
* The carve-out permits an edit to the `storage` value ALONE. Every other byte of each golden string stays as captured, which is what keeps this scenario's full-string equality assertion a working proof rather than a retired one.

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
* *THEN* a full-string equality assertion over each code path's built SQL — or the decline `UdfError` message — MUST equal its captured golden baseline byte-for-byte EXCEPT for the scan spec's `storage` value, which `vs-adapter/storage-backend-enum` re-encodes as an externally-tagged backend variant over a byte-identical payload and which `vs-adapter/scan-spec-credential-reference` then replaces with a connection reference or a sealed envelope over that same payload, verified by characterization tests that assert the entire returned string rather than a substring
* *AND* the permitted `storage` edit SHALL be the ONLY edit to any of the four golden strings, and the `ineligible_join_decline` message MUST stay unedited because it embeds no scan spec, so the decline path retains a fully untouched full-string gate
* *AND* every scenario in `vs-adapter/pushdown-planning-join` and `vs-adapter/pushdown-planning-join-fallback` MUST still pass with no change to any test assertion or expected value outside a `storage` value, including `plan_join`'s empty-side short-circuit (which delegates to `pushdown::file_resolution::empty_result_sql`, a function this refactor does not move or touch, so it is verified by the existing suite rather than a new golden test here)

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
* *AND* every message MUST keep the `; this is a hard error, not a native re-plan` tail, so `vs-adapter/pushdown-planning-join`'s "declined safely" contract — a hard client error, never a native re-plan — still holds at all six sites
* *AND* the shared constructor MUST NOT absorb `ineligible_join_decline`, whose message carries the additional `the adapter cannot render this join shape` clause and is therefore a different sentence rather than a seventh instance of this one

### Scenario: One clause walk feeds both wrapper column-narrowing routines

* *GIVEN* `referenced_side_columns` (`joins/rendering.rs`) and `referenced_column_projection` (`joins/sql_builders.rs`), which each hand-roll the same walk over the clause set whose rendered SQL can name a source column — `selectList`, a non-null `filter`, `groupBy`, `orderBy`, and a non-null `having`
* *WHEN* both are re-expressed over one shared clause walk that takes its per-node collector from the caller
* *THEN* the clause set MUST have exactly one owner for `filter`, `groupBy`, `orderBy`, and `having`, so adding or removing one of those clauses SHALL require editing one function rather than two
* *AND* `referenced_side_columns` MUST keep naming `selectList` a second time in its short-circuit guard, because that guard is a fallback policy the walk deliberately does not own — so `selectList` is the one clause of the five with two named sites after the extraction, and that is a retained exception rather than an incomplete reduction
* *AND* each caller MUST keep its own collector and therefore its own case folding — `referenced_column_projection` still folds through `collect_all_column_names`' Unicode `to_uppercase`, `referenced_side_columns` still folds through `collect_side_column_names`' ASCII-only `to_ascii_uppercase` — because `vs-adapter/pushdown-module-structure`'s "One blind traversal primitive backs every column-collecting walk" scenario forbids reconciling that disagreement
* *AND* both divergent narrowing policies MUST survive unchanged — an absent or empty `selectList` MUST make `referenced_side_columns` return every column of `full_cols` without inspecting any other clause while `referenced_column_projection` MUST still narrow through the remaining clauses in that same case, and a narrowing that selects no column MUST yield all of `full_cols` for `referenced_side_columns` but exactly the first column of `all_cols` for `referenced_column_projection`, which MUST NOT return a zero-column projection because an empty Exasol `EMITS` clause is invalid
* *AND* `referenced_side_columns` MUST still collect from the join condition passed to it separately, a clause `referenced_column_projection` has no equivalent of and MUST NOT acquire

### Scenario: The two join-rendering pass-through wrappers are deleted rather than retained

* *GIVEN* `render_join_condition` and `render_selectlist_item_qualified` in `joins/rendering.rs`, whose entire bodies are one call — to `vs_expression::render_expression_safe` and to `render_expression_qualified` respectively — passing the same arguments through unchanged
* *WHEN* the duplication reduction completes
* *THEN* both names MUST be absent from the crate, with every production and test call site naming its delegate directly, per `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md` "Fold by deleting the wrapper, not by leaving a pass-through"
* *AND* neither name appears in the façade baseline captured by "Join pushdown façade resolves at every pre-refactor path", so that baseline MUST still diff empty and this deletion MUST NOT be recorded as narrowing the façade
* *AND* the design intent each wrapper's doc comment carried MUST move onto the surviving delegate rather than be deleted with the wrapper — for the select-list path, that one recursive translator renders columns, literals, scalar expressions, a top-level `function_aggregate`, and a `function_aggregate` nested inside a scalar function, byte-compatibly with the former `render_aggregate_qualified`; for the join condition, that `render_expression_safe` rather than the filter renderer is used so a boolean condition is returned verbatim and never suppressed as trivially true
