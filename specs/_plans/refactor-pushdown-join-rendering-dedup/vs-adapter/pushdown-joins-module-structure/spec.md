# Feature: Pushdown Joins Module Structure

Splits the oversized `pushdown::joins` submodule into a nested `joins/` directory module organized by concern behind an unchanged public façade, keeping generated SQL byte-identical and co-locating each concern's tests.

## Background

<!-- DELTA:NEW -->
* A second duplication-reduction pass (issue #181) runs inside the already-split `joins/` directory module. It moves no code between submodules and changes no visibility upward, so the façade baseline and the concern split recorded below stay accurate and unedited.
* Two reductions have no observable surface of their own and are verified entirely by the golden-SQL scenario below, so they get no scenario: `collect_column_tables` returning its three outputs as `column_tables` instead of writing three `&mut` out-params (its two callers each build a fresh set per use, so the tuple form is the natural one), and the shared `shard_count` → `partition_files_by_bytes` → `relativize_shards_to_root` prefix of `build_side_fan_out_sql` and `build_broadcast_join_sql` moving into one side-sharding helper.
* The trailing `build_scan_driving_sql(…, None, None, &[], &[], …)` argument tail that both fan-out builders repeat is deliberately NOT wrapped. A wrapper taking the six arguments that genuinely differ, to elide four literal empties, would be a shallow layer rather than a reduction.
* Issue #181's finding 2 (`involved_table_columns` versus `extract_all_column_types`) is tracked separately as issue #265 and is out of scope here; neither function is touched.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: One shared template renders all six qualified N-scan render declines

* *GIVEN* the six `UdfError::User` declines the qualified N-scan render path raises, one per clause it cannot render — a select-list item (`n_scan_join_select_items`), absent involved-table column metadata and an unrenderable join condition (both `build_n_scan_join_sql`), a GROUP BY key (`qualified_join_group_by`), HAVING (`qualified_join_having`), and an ORDER BY key (`qualified_join_order_by`) — each of which writes the whole sentence `join pushdown declined: <clause>; this is a hard error, not a native re-plan` as its own string literal, and none of which has any assertion on its message text today
* *WHEN* all six are re-expressed through one shared decline constructor that receives only the clause-specific fragment
* *THEN* each of the six produced `UdfError::User` messages MUST equal its pre-refactor text byte-for-byte, asserted by full-string equality over the entire message rather than a substring check
* *AND* every message MUST keep the `; this is a hard error, not a native re-plan` tail, so `vs-adapter/pushdown-planning-join`'s "declined safely" contract — a hard client error, never a native re-plan — still holds at all six sites
* *AND* the shared constructor MUST NOT absorb `ineligible_join_decline`, whose message carries the additional `the adapter cannot render this join shape` clause and is therefore a different sentence rather than a seventh instance of this one
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: One clause walk feeds both wrapper column-narrowing routines

* *GIVEN* `referenced_side_columns` (`joins/rendering.rs`) and `referenced_column_projection` (`joins/sql_builders.rs`), which each hand-roll the same walk over the clause set whose rendered SQL can name a source column — `selectList`, a non-null `filter`, `groupBy`, `orderBy`, and a non-null `having`
* *WHEN* both are re-expressed over one shared clause walk that takes its per-node collector from the caller
* *THEN* the clause set MUST have exactly one owner for `filter`, `groupBy`, `orderBy`, and `having`, so adding or removing one of those clauses SHALL require editing one function rather than two
* *AND* `referenced_side_columns` MUST keep naming `selectList` a second time in its short-circuit guard, because that guard is a fallback policy the walk deliberately does not own — so `selectList` is the one clause of the five with two named sites after the extraction, and that is a retained exception rather than an incomplete reduction
* *AND* each caller MUST keep its own collector and therefore its own case folding — `referenced_column_projection` still folds through `collect_all_column_names`' Unicode `to_uppercase`, `referenced_side_columns` still folds through `collect_side_column_names`' ASCII-only `to_ascii_uppercase` — because `vs-adapter/pushdown-module-structure`'s "One blind traversal primitive backs every column-collecting walk" scenario forbids reconciling that disagreement
* *AND* both divergent narrowing policies MUST survive unchanged — an absent or empty `selectList` MUST make `referenced_side_columns` return every column of `full_cols` without inspecting any other clause while `referenced_column_projection` MUST still narrow through the remaining clauses in that same case, and a narrowing that selects no column MUST yield all of `full_cols` for `referenced_side_columns` but exactly the first column of `all_cols` for `referenced_column_projection`, which MUST NOT return a zero-column projection because an empty Exasol `EMITS` clause is invalid
* *AND* `referenced_side_columns` MUST still collect from the join condition passed to it separately, a clause `referenced_column_projection` has no equivalent of and MUST NOT acquire
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: The two join-rendering pass-through wrappers are deleted rather than retained

* *GIVEN* `render_join_condition` and `render_selectlist_item_qualified` in `joins/rendering.rs`, whose entire bodies are one call — to `vs_expression::render_expression_safe` and to `render_expression_qualified` respectively — passing the same arguments through unchanged
* *WHEN* the duplication reduction completes
* *THEN* both names MUST be absent from the crate, with every production and test call site naming its delegate directly, per `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md` "Fold by deleting the wrapper, not by leaving a pass-through"
* *AND* neither name appears in the façade baseline captured by "Join pushdown façade resolves at every pre-refactor path", so that baseline MUST still diff empty and this deletion MUST NOT be recorded as narrowing the façade
* *AND* the design intent each wrapper's doc comment carried MUST move onto the surviving delegate rather than be deleted with the wrapper — for the select-list path, that one recursive translator renders columns, literals, scalar expressions, a top-level `function_aggregate`, and a `function_aggregate` nested inside a scalar function, byte-compatibly with the former `render_aggregate_qualified`; for the join condition, that `render_expression_safe` rather than the filter renderer is used so a boolean condition is returned verbatim and never suppressed as trivially true
<!-- /DELTA:NEW -->
