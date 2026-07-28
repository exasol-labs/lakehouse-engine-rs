# Plan Review Findings: refactor-pushdown-collect-walk-dedup (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 7 (Blockers: 2, Advisory: 5)
- Intent Fidelity blockers: 0

## Premortem

Three failure stories, routed into the taxonomy below.

1. **The structural gate misfires and drags the implementer into #257's code.** Task 5.3 and the
   Manual Testing row both demand zero `Json::Array` in `joins/rendering.rs`. Two occurrences must
   survive there — `annotate_columns_with_alias`'s rebuild arm (`:79`) and `referenced_side_columns`'s
   `selectList` match (`:293`). The implementer sees `2`, declares the migration incomplete, and
   rewrites `annotate_columns_with_alias` — the walk #177 descopes and #257 owns. →
   `[AMBIGUOUS_REQUIREMENT]` BLOCKER.
2. **The characterization gate is thinner than claimed and a projection regression ships.** One of
   the three named `dispatch_golden` anchors for `collect_all_column_names` renders through
   `empty_result_sql`, which never reaches the collector. The verification report claims coverage the
   suite does not have. → `[TRACEABILITY_GAP]` BLOCKER.
3. **A later reader "fixes" the case-folding divergence, or re-merges the two primitives.** Both are
   named prohibitions in the spec delta and both decision-log entries, so this story is already
   closed by the artifacts. No finding.

## Intent Fidelity

[no objection — axis checked: all three work items from `gh issue view 177` are covered
(`plan.md` § Goals lines 21); the three descoped items are named as Non-Goals verbatim
(`plan.md:22`) — no `prop_parsed<T>`/`note_parsed<T>`, no config table, no `Visitor` trait, no
typed AST, no rewrite walker, and `annotate_columns_with_alias`/`strip_table_alias` untouched
(task 4.6). `resolve_s3_max_connections` is not folded, and its doc's `str_prop` cross-reference at
`mod.rs:811` is scheduled for rename (task 1.4) — verified: line 811 carries `str_prop → parse →
filter(>=1)`, line 812 carries `resolve_df_threads_per_udf`. The three type-rewrite guards are
untouched, so #257 is neither pre-empted nor blocked. The `to_uppercase` /
`to_ascii_uppercase` divergence is preserved per closure in four places — `plan.md:86`, task 3.3,
task 4.4, and the delta scenario's sixth AND — and never unified. The two deviations from the
issue's literal wording (`walk_column_nodes` over `walk_json`; deleting `str_prop`/`str_field`
instead of leaving pass-throughs) are the answers the user gave in Q2 and Q3 and are therefore
settled.]

## Feasibility

[no objection — axis checked: the borrow-checker risk in task 3.1/4.2 is genuinely retired for all
three closures, not just the tested one. `walk_column_nodes(value, f)` where `f: &mut F` reborrows
implicitly at a known reference-typed parameter, so the recursion compiles with or without
`&mut *f`; monomorphization terminates because `F` is fixed across the recursive call. Each closure
captures only distinct `&mut` bindings from its enclosing signature — `collect_column_tables`'s
three out-params (`tables`, `has_untagged`, `any_column`) are separate bindings captured by unique
borrow in one `FnMut`, `collect_side_column_names` captures `out` plus a shared `&str`, and
`collect_all_column_names` captures `names` alone. None aliases another; none needs a `RefCell`.
Statement order inside `collect_column_tables`'s closure is behaviourally irrelevant (a `HashSet`
insert and two `bool` sets commute), so the "same statement order as today" instruction is safe
either way.

Location claims verified line by line: `collect_column_tables` at `rendering.rs:126`,
`collect_side_column_names` at `:245`, `collect_all_column_names` at `support.rs:1244`, `str_prop`
at `mod.rs:449` with exactly the 13 production call sites the plan enumerates plus test call sites
`:1225`/`:1232` and comment lines `:1981`/`:2033`/`:2084`/`:2242`, `str_field` at `connection.rs:205`
with exactly 11 call sites, and `resolve_df_target_partitions`/`resolve_df_threads_per_udf` at
`mod.rs:784`/`:797` with byte-identical bodies, two production call sites at `:755`/`:756`, and
exactly the 14 test call sites listed. No name collision: `nonempty_str`, `walk_column_nodes`, and
`resolve_df_fixed_count` are absent from `crates/`. `pub(super)` on a `support` item does reach
`joins::rendering` — empirically proven by `collect_all_column_names`, already imported that way
from `joins/sql_builders.rs:11`. No NFR surface: no I/O, no concurrency, no wire format, no
migration; `make cross-musl-udf-build` is in the checklist. A grep of the whole adapter tree found
exactly three `map.values()` recursions — the three the plan migrates — so no fourth duplicate is
missed.]

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: `plan.md` § Verification / Manual Testing (row `grep -c 'Json::Array' …rendering.rs` → `0`) and § Implementation Tasks task 5.3 ("no `Json::Array` in `joins/rendering.rs`")
- Issue: the expected value is wrong and the check points at out-of-scope code. `joins/rendering.rs` holds four `Json::Array` occurrences today: `:79` (`annotate_columns_with_alias`'s array rebuild arm), `:147` and `:265` (the two collectors' array arms, both deleted by task 4), and `:293` (`referenced_side_columns`'s `Some(Json::Array(list)) if !list.is_empty()` match on `selectList`). A correct implementation leaves **2**, not `0`. The gate therefore either fails on correct work or drives the implementer to edit `annotate_columns_with_alias` — the walk issue #177 explicitly descopes and issue #257 owns, and the one edit task 4.6 forbids. This is the plan's only structural falsifier for "no second traversal survives on the joins side", so as written the requirement is unverifiable.
- Fix: In `plan.md` § Verification / Manual Testing, replace the `grep -c 'Json::Array' crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` → `0` row with a row whose expected output is `2` and whose Expected-Output cell names both surviving occurrences (`annotate_columns_with_alias`'s rebuild arm and `referenced_side_columns`'s `selectList` match), plus a second row scoped to the two migrated functions — simply `grep -A 12 'fn collect_column_tables\|fn collect_side_column_names' …/rendering.rs | grep -c 'Json::'` → `0`. In task 5.3, restate the check as "neither `collect_column_tables` nor `collect_side_column_names` retains a `Json::Object` or `Json::Array` arm; `annotate_columns_with_alias`'s and `referenced_side_columns`'s occurrences MUST survive untouched."

#### [TRACEABILITY_GAP] BLOCKER
- Location: `plan.md` § Verification / Scenario Coverage row "`collect_all_column_names` wrapper projection" (names `group_by_fallback_matches_golden`, `multi_count_distinct_decline_matches_golden`, `empty_group_by_wrapper_matches_golden`); and `vs-adapter/pushdown-module-structure/spec.md` final AND ("the `dispatch_golden` grouped-aggregate and `COUNT(DISTINCT)` wrapper assertions")
- Issue: `empty_group_by_wrapper_matches_golden` cannot fail if `walk_column_nodes` is wrong. It calls `empty_sql(...)` → `pushdown::file_resolution::empty_result_sql`, which never calls `referenced_column_projection` and so never reaches `collect_all_column_names`; its committed golden is `SELECT CAST(NULL AS VARCHAR(2000000)), CAST(NULL AS DECIMAL(30,4)) FROM DUAL WHERE 1=0` — no inner scan, no projection. Naming it as a falsifier inflates the gate and will produce a verification report claiming coverage the suite does not have. The spec clause compounds it: the only `dispatch_golden` test named "grouped-aggregate" is `grouped_aggregate_matches_golden`, which takes the partial/merge decomposition path and also never reaches the collector — the two tests that DO reach it are the two *decline* wrappers (`group_by_fallback_matches_golden` → golden `"projection":["REGION","NAME"]` narrowed from a four-column universe, and `multi_count_distinct_decline_matches_golden` → `"projection":["NAME","ID"]`), and they are the complete `dispatch_golden` coverage for this collector.
- Fix: In `plan.md` § Verification / Scenario Coverage, delete `empty_group_by_wrapper_matches_golden` from the `collect_all_column_names` wrapper-projection row, leaving `group_by_fallback_matches_golden` and `multi_count_distinct_decline_matches_golden`. In `vs-adapter/pushdown-module-structure/spec.md`, rewrite the final AND's `dispatch_golden` phrase as "the two `dispatch_golden` decline-wrapper assertions — the declined `GROUP BY` fallback and the multi/mixed `COUNT(DISTINCT)` decline, whose committed goldens both carry a narrowed inner-scan `projection`" so the clause names paths that actually route through the primitive.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `vs-adapter/pushdown-module-structure/spec.md`, the scenario's 7th and 8th AND steps ("the primitive MUST stay separate from issue #257's curated post-order rewrite primitive and MUST NOT be merged with it…"; "the rewrite-shaped walks SHALL be left unchanged by this extraction…")
- Issue: the 7th AND is not verifiable against this change — issue #257's primitive does not exist, so no pass/fail test can be written for "MUST NOT be merged with it". It is a coordination statement, and the identical statement already sits in Background (bullet 9: "Issue #257 owns a SECOND, different traversal primitive…"). The 8th AND is verifiable (a diff check) but restates task 4.6 rather than stating an obligation on the primitive. Together they inflate the scenario to 8 AND steps — the highest count in the entire spec library (library maximum elsewhere is 7) — and the justification offered for that count is factually wrong: the cited neighbour `One classifier decides the request shape for both the dispatch and empty-result paths` carries **6** AND steps, not 8; the 8-AND scenario the validator flags is this plan's own.
- Fix: In `vs-adapter/pushdown-module-structure/spec.md`, delete the 7th AND (its content is already Background bullet 9) and fold the 8th AND's list into Background as a scope bullet. That leaves 6 AND steps, matching the largest in-feature precedent. Then correct `plan.md` § Design / Consequences (or wherever the AND-count justification is restated) to cite 6, not 8.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `plan.md` § Implementation Tasks task 3.4 (the new `walk_column_nodes` test)
- Issue: the specced fixture nests `column` nodes inside a function's `arguments`, a `CASE`'s `results`, and a comparison's `left`/`right` — all cases where the *parent* is not a `column`. Nothing in the plan pins that traversal continues **through** a matched `column` node into its own field map, which is what today's three walks do (`f(map)` then `for v in map.values()`, unconditionally). An implementer who writes `if column { f(map) } else { recurse }` passes the specced test and every existing golden, because no real `column` node carries a nested `column`. The delta scenario asserts "every field of every object", so the plan states a property with no falsifier.
- Fix: In `plan.md` task 3.4, extend the fixture with one `column` object carrying a child object that itself is a `column` node, and add the assertion that the callback fires for **both** — pinning that a matched `column` node's own fields are still descended.

## Task Breakdown

[no objection — axis checked: every delta scenario has an implementing task and every task
implements something in scope. `pushdown-module-structure`'s one scenario → tasks 3, 4, 5;
`adapter-module-structure` scenario 1 → task 1; scenario 2 → task 2. No orphan task. Parallelization
holds on file disjointness as claimed: Group A is task 1 (`adapter/mod.rs` + `adapter/connection.rs`)
against task 3 (`adapter/pushdown/support.rs`); Group B is task 2 (`adapter/mod.rs`) against task 4
(`adapter/pushdown/joins/rendering.rs`). The two stated sequential edges are both real — task 2's
folded resolver calls `nonempty_str`, and task 4 calls the primitive task 3 adds — and they correctly
prevent the two `adapter/mod.rs` tasks from colliding on the adjacent `S3_MAX_CONNECTIONS` doc lines
`:811`/`:812` that tasks 1.4 and 2.4 each edit. Granularity is right: task 4 alone is `[expert]`, and
it is the only task carrying a non-mechanical step (the three-`&mut`-capture closure). The skipped
tests for `nonempty_str` and `resolve_df_fixed_count` are defensible — both folds preserve the
signature exactly, so a mis-edit is a compile error, not a silent behaviour change, and the resolver
rule is asserted by ten existing tests of which two round-trip through `build_adapter_notes`
(`df_target_partitions_uses_supplied_value`, `df_threads_per_udf_uses_supplied_value`), which is what
makes the `adapterNotes`-identity clause falsifiable.]

#### [PROSE_UNCLEAR] ADVISORY
- Location: `plan.md` § Implementation Tasks task 2.3 ("the six affected tests stay the behavioral characterization of `vs-adapter/create-virtual-schema-adapter-notes-resources`") and `decision-log.md` § Design Decisions [2] ("the six affected tests remain the behavioral characterization")
- Issue: "six" contradicts the plan's own Verification table, which names ten resolver tests, and matches no reading of the code — fourteen call sites sit in ten test functions, of which exactly two (`df_target_partitions_uses_supplied_value` at `mod.rs:1829`, `df_threads_per_udf_uses_supplied_value` at `:1935`) round-trip through `build_adapter_notes` and so actually characterize `create-virtual-schema-adapter-notes-resources`. An implementer cannot tell how many tests to expect to touch.
- Fix: In `plan.md` task 2.3 and `decision-log.md` [2], replace "the six affected tests" with "the ten affected tests", and name `df_target_partitions_uses_supplied_value` and `df_threads_per_udf_uses_supplied_value` as the two that characterize `vs-adapter/create-virtual-schema-adapter-notes-resources` via `build_adapter_notes`.

## Design Depth

[no objection — axis checked. `walk_column_nodes` is the deeper of the three candidate shapes: it
absorbs both the recursion and the `type == "column"` test, so no caller repeats either, and each
caller's residual body is 2–4 lines — verified against the three current bodies at
`rendering.rs:126`, `:245`, and `support.rs:1244`, each of which is ~20 lines of which ~14 is
traversal. No information leakage: the traversal decision ends up in exactly one module, and the
`pub(super)` placement in `support` needs no `use`-path or visibility change on the joins side.
Deleting `str_prop`, `str_field`, `resolve_df_target_partitions`, and `resolve_df_threads_per_udf`
rather than leaving pass-throughs is the correct call against the pass-through red flag; the plan's
Dead Code Removal table lists all four plus the three recursion bodies. `resolve_df_fixed_count`'s
added `key` parameter is a configuration parameter, which the philosophy would normally push back
on — here it is the entire point of the fold, blessed by the issue and by the user in Q1, so it is
settled. No boundary violation: nothing crosses into a delivery mechanism or storage engine.

The deliberate omission of a `vs-adapter/pushdown-joins-module-structure` delta is sound. Its
"Generated join SQL is byte-identical across the split" scenario is scoped to "any duplication
extraction" over `build_broadcast_join_sql`, `build_n_scan_join_sql`, `build_grouped_qualified_fallback_sql`,
and `ineligible_join_decline` — and `build_n_scan_join_sql` provably drives BOTH migrated collectors:
`sql_builders.rs:376` calls `referenced_side_columns` (→ `collect_side_column_names`) and `:359`/`:378`
call `cross_side_residual_filter`/`side_local_filter` (→ `conjunct_single_side` → `collect_column_tables`).
Its GIVEN even pins the fixture shape this needs ("both a side-local WHERE conjunct and a cross-side
residual conjunct"), and `golden_n_scan_join_sql_unchanged` at `sql_builders.rs:1929` matches: three
conjuncts, two side-local and one cross-side, with the golden showing per-leg pushed filters and the
cross-side conjunct in the outer WHERE. Its second scenario already caps a cross-submodule helper at
`pub(super)`. A mirror scenario would restate two live requirements.

The new `vs-adapter/adapter-module-structure` feature is warranted, not sprawl. The accessor's ~26
call sites span four behavioural features that all exist in the library — `connection-credentials`,
`create-virtual-schema`, `create-virtual-schema-adapter-notes-resources`, `refresh-and-set-properties`
— so no one of them can own the structural decision without leaking it across a boundary, and
`*-module-structure` has two verified in-tree precedents (`vs-adapter/pushdown-module-structure`,
`datafusion-scan/scan-module-structure`). Two scenarios, well under the 10-scenario threshold. Note
for the record step, not a finding: `vs-adapter` already carries 35 features, so adding a 36th will
trip `/speq:spec-merge`'s ">8 domain features" signal and require a user decision before archiving —
a pre-existing condition of this domain, not a defect in this plan.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: `plan.md` § Summary (line 5), § Context (line 19, closing sentence), § Design/Context (line 34, closing sentence), § Verification (line 200, closing sentence); `decision-log.md` [1], [3], [6], [7], [9]
- Issue: two guardrail violations, both systematic. First, the Summary's opening sentence runs 62 words against the 25-word cap. Second, a repeated pattern of prose defending the artifact rather than stating the decision: "This conclusion is stated so a reviewer can check the call rather than infer it from an omission" (line 19); "A future caller that genuinely needs non-column nodes can widen the primitive then, with a real use case in hand" (line 34, repeated verbatim in `decision-log.md` [1]); "a refactor whose scenarios needed new behavioral tests would not be a refactor" (line 200); "recorded here so it is not read as an oversight" ([1]); "a future planner must not re-litigate it after both primitives land" ([3]); "Recorded because the divergence looks like an inconsistency a later reader would 'fix'" ([6]); "Both boundaries are recorded so a later planner does not re-open them" ([7]); and decision [9], whose entire Rationale restates `plan.md` line 19. Each defends a choice already stated, which is what the terseness rule bans.
- Fix: Split `plan.md` line 5 into two sentences of at most 25 words each. Delete the closing sentence of `plan.md` lines 19, 34, and 200, and the quoted closing sentence of `decision-log.md` [1], [3], [6], and [7]. Reduce `decision-log.md` [9] to its Decision bullet and a one-sentence Rationale, or drop the entry and keep only `plan.md` line 19's first two sentences.
