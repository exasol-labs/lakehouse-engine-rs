# Plan: refactor-pushdown-join-rendering-dedup

## Summary

Collapse five copy-paste duplications in the join-pushdown rendering path (issue #181) — two clause-walk routines, a decline-message template written six times, `collect_column_tables`' out-param boilerplate, the two fan-out builders' sharding prefix, and two one-line pass-through wrappers plus a convoluted attach-point expression. Pure refactor: generated join SQL and all decline messages stay byte-identical.

## Design

### Context

`joins/` was split into a directory module by the preceding refactor (`vs-adapter/pushdown-joins-module-structure`), which reduced file size but deliberately left duplication reduction to a follow-up. Issue #181 is that follow-up. The forces:

- The duplications are real but small. Each reduction must earn its keep by removing a *decision* that currently lives in two places, not merely by removing lines.
- Byte-identical output is a hard correctness bar, and the existing safety net is uneven: four golden tests pin the generated SQL, but **not one of the six decline messages has any assertion on its text**. Some of that net must be built before the first edit.
- One reduction (the two clause-walk routines) looks mechanical and is not. The two routines differ in five ways, and one of those differences — their case folding — is explicitly forbidden from being reconciled by two sources: the `walk_column_nodes` doc comment in `crates/lakehouse-engine/src/adapter/pushdown/support.rs` ("Case folding is deliberately NOT owned here … Those two MUST NOT be unified"), and `specs/vs-adapter/pushdown-module-structure/spec.md` — its §Background bullet recording that the two case-folding calls are NOT interchangeable, plus the case-folding *AND* of its "One blind traversal primitive backs every column-collecting walk" scenario. `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md` does **not** state the case-folding constraint and MUST NOT be cited for it; it is this plan's source for the wrapper-deletion precedent only.

**Goals** — one owner for each duplicated decision; byte-identical SQL and decline messages; test coverage added exactly where the refactor would otherwise be unverified.

**Non-Goals** — issue #181's finding 2 (`involved_table_columns` vs `extract_all_column_types`), split off to issue #265; any wider `walk_json` primitive, which `specs/_decision/037` rejected on the record; the `collect_all_column_names` caller on the TopN hidden-column path, which is outside this issue's named sites; the CI file-size guardrail (issue #129).

### Decision

Five independent reductions, each landing its helper at the narrowest visibility that compiles and in the module its callers already import from.

#### Architecture

```
joins/rendering.rs                      joins/sql_builders.rs
┌────────────────────────────────┐      ┌─────────────────────────────────┐
│ referenced_clause_values ──────┼─────▶│ referenced_column_projection    │
│   (pub(super), NEW)            │      │   (own collector + own fallback)│
│      ▲                         │      │                                 │
│ referenced_side_columns        │      │ join_render_decline (NEW, priv) │
│   (own collector + fallback)   │      │   ◀── all 6 decline sites       │
│                                │      │                                 │
│ column_tables ─────────────────┼─────▶│ shard_side (NEW, priv)          │
│   (was collect_column_tables,  │      │   ◀── build_side_fan_out_sql    │
│    now returns its 3 outputs)  │      │   ◀── build_broadcast_join_sql  │
│                                │      │                                 │
│ ✗ render_join_condition        │      └─────────────────────────────────┘
│ ✗ render_selectlist_item_qual. │        callers name the delegate directly
└────────────────────────────────┘
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Caller-supplied visitor over a shared traversal | `referenced_clause_values` | The clause set is shared; the collector, the case folding, and the fallback policy are not. Passing the collector in is what lets one walk serve two genuinely different callers without unifying what must stay divergent. |
| Parametrised message constructor | `join_render_decline(clause) -> UdfError` | One sentence, six clause nouns. The template is the decision; the noun is the parameter. |
| Return values instead of `&mut` out-params | `column_tables(expr) -> (HashSet<String>, bool, bool)` | Both call sites want three fresh values, and one is inside a loop where freshness per iteration is required. Returning them makes that structural instead of conventional. |
| Delete the wrapper, migrate the callers | `render_join_condition`, `render_selectlist_item_qualified` | Follows `specs/_decision/037` "Fold by deleting the wrapper, not by leaving a pass-through". A body that is one call with the same arguments is the pass-through red flag. |

#### Key interfaces

```rust
// joins/rendering.rs — pub(super); sql_builders.rs already imports from here.
pub(super) fn referenced_clause_values(pushdown_req: &Json, visit: impl FnMut(&Json));
pub(super) fn column_tables(expr: &Json) -> (HashSet<String>, bool, bool);

// joins/sql_builders.rs — private; all callers are in this file.
fn join_render_decline(clause: &str) -> UdfError;
fn shard_side(side: &ResolvedJoinSide, tuning: &JoinScanTuning) -> Vec<Vec<FileEntry>>;
```

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `referenced_clause_values` takes the collector as a closure; each caller keeps its own filter, case folding, and empty-result fallback | One unified function returning the narrowed column list for both callers | The two routines differ in five ways: the extra join-condition argument, per-table vs all-table attribution, the absent-`selectList` short-circuit, the empty-result fallback (`full_cols` vs first column), and the return type. A full merge would have to reconcile the ASCII/Unicode case folding, which the `walk_column_nodes` doc comment in `adapter/pushdown/support.rs` and `specs/vs-adapter/pushdown-module-structure/spec.md` (its §Background bullet on the two non-interchangeable folds, and the case-folding *AND* of its "One blind traversal primitive backs every column-collecting walk" scenario) both forbid — not `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md`, which is silent on case folding — and no test in the crate uses a non-ASCII identifier, so that break would pass the whole suite silently. |
| `referenced_clause_values` lives in `joins/rendering.rs` at `pub(super)` | `support.rs` next to `walk_column_nodes`; `joins/mod.rs` as a cross-cutting helper | Both callers are inside `joins/`, and `sql_builders.rs` already imports `collect_column_tables` and `referenced_side_columns` from `super::rendering`. Reusing that seam adds no new one; `support.rs` would widen reach to all of `pushdown` for no caller that exists. |
| `join_render_decline` stays private to `sql_builders.rs` and excludes `ineligible_join_decline` | Hoisting it to `joins/mod.rs` beside `ineligible_join_decline`; one constructor for all seven declines | All six callers are in `sql_builders.rs`, so `pub(super)` would widen for nothing. The seventh message inserts `the adapter cannot render this join shape, ` before the shared tail — a different sentence, not a seventh instance. Cross-referencing doc comments cover discoverability. |
| The fan-out builders' shared `build_scan_driving_sql(…, None, None, &[], &[], …)` tail is left as is | A second helper wrapping the call | Six of the ten arguments genuinely differ. Wrapping to elide four literal empties trades a duplication for a shallow layer. |
| The attach-point `match` becomes a let-chain `if`, keeping `.max()` inside the condition | `if resolvable && last_join_point >= 1 { …max().unwrap() }` as issue #181 suggests | Identical behaviour without an `unwrap()` whose safety rests on the unstated "`resolvable` implies `tables` is non-empty" invariant. The existing comment explaining the `last_join_point >= 1` guard — that `clamp(1, 0)` would panic for a single leg — is non-obvious and is carried over verbatim. |
| Findings 4 and 5 get Background bullets but no scenario | A structural scenario each | Their only observable effect is the generated SQL, already pinned byte-for-byte by the existing golden scenario. Finding 1 earns a scenario because its divergences are hidden behaviour that no existing test pins. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-joins-module-structure | CHANGED | `vs-adapter/pushdown-joins-module-structure/spec.md` |
| vs-adapter/pushdown-module-structure | CHANGED | `vs-adapter/pushdown-module-structure/spec.md` |
| vs-adapter/pushdown-planning-selectlist-expressions | CHANGED | `vs-adapter/pushdown-planning-selectlist-expressions/spec.md` |

The second and third features are in scope because each pins normatively a function this plan changes, and neither would otherwise be reconciled by `/speq:record`:

- `vs-adapter/pushdown-module-structure` pins `collect_column_tables`' `pub(super)` visibility **and its three accumulator out-parameters**, which task 3 replaces with a returned tuple. Its delta re-states that clause in the return form and re-scopes the accompanying "compile unedited" guarantee; its case-folding clause and the matching §Background bullet stay unchanged and stay binding on this plan.
- `vs-adapter/pushdown-planning-selectlist-expressions` names `render_selectlist_item_qualified` inside the dialect-chain *AND* of its widened-projection scenario, which task 4.3 deletes. Its delta rewrites that one parenthetical to name the surviving delegate; no dialect behaviour changes.

`vs-adapter/pushdown-planning-join` and `vs-adapter/pushdown-planning-join-fallback` were checked and need no change: neither names any function this plan touches, and no pushed-down behaviour changes.

## Impact

None. No query result, generated SQL, error message, capability, or configuration changes. Internal to `crates/lakehouse-engine`; no public API, no `.so` ABI, and no Iceberg spec surface is affected (this plan touches neither scanning, pushdown semantics, nor schema/type handling — only how existing SQL-building code is factored).

## Requirements

| Requirement | Details |
|-------------|---------|
| Byte-identical SQL | Every string built by `build_broadcast_join_sql`, `build_n_scan_join_sql`, `build_side_fan_out_sql`, and `build_grouped_qualified_fallback_sql` is unchanged. |
| Byte-identical messages | All six qualified N-scan render-decline messages, and `ineligible_join_decline`'s, are unchanged. |
| No façade change | The nine `pub(crate)` and five `pub(super)` items in the `joins` façade baseline stay identical in name and visibility. |
| No case-folding reconciliation | `collect_all_column_names` keeps Unicode `to_uppercase`; `collect_side_column_names` and `column_tables` keep ASCII `to_ascii_uppercase`. |

## Dependencies

None outstanding. Issue #181's stated prerequisite ("best sequenced after the generic-helpers issue, for `walk_json`") is already satisfied: issue #177 shipped in PR #261 as the narrower `walk_column_nodes`, and `specs/_decision/037` records that narrowing as deliberate. This plan builds on `walk_column_nodes` as-is and MUST NOT introduce a wider `walk_json`.

## Implementation Tasks

1. **Baseline and close the decline-message coverage gap.**
   - [ ] 1.1 Read `specs/vs-adapter/pushdown-joins-module-structure/spec.md` and `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md` in full, plus the `walk_column_nodes` doc comment in `adapter/pushdown/support.rs` and the "One blind traversal primitive backs every column-collecting walk" scenario in `specs/vs-adapter/pushdown-module-structure/spec.md`, before touching any code. Which source carries which constraint: `specs/_decision/037-refactor-pushdown-collect-walk-dedup.md` is the source of the **wrapper-deletion precedent only** and says nothing about case folding; the **case-folding constraint** comes from `support.rs`' `walk_column_nodes` doc comment ("Those two MUST NOT be unified") and from that `pushdown-module-structure` scenario's case-folding *AND* plus its §Background bullet on the two non-interchangeable folds.
   - [ ] 1.2 On unmodified HEAD, run `cargo test -p lakehouse-engine` and confirm the four existing golden tests pass: `golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged`, `golden_grouped_qualified_fallback_sql_unchanged` (`joins/sql_builders.rs`), `golden_ineligible_decline_message_unchanged` (`joins/mod.rs`).
   - [ ] 1.3 Add `golden_n_scan_render_decline_messages_unchanged` to `joins/sql_builders.rs`'s test module: trigger or construct each of the six declines and assert the **full** message string with `assert_eq!`. Transcribe each expected string from HEAD, resolving Rust's `\`-newline continuations by hand (the continuation eats the newline and the next line's leading whitespace, leaving a single space). This test MUST pass against unmodified HEAD before task 2 begins — a failure here means the transcription is wrong, not the code.
   - [ ] 1.4 Confirm the existing `ineligible_join_decline` substring assertion in `joins/sql_builders.rs` (`msg.contains("join pushdown declined") && msg.contains("cannot")`) targets the seventh, separate template and is unaffected by tasks 2 onward.

2. **Finding 3 — one decline constructor for six sites.**
   - [ ] 2.1 Add private `fn join_render_decline(clause: &str) -> UdfError` to `joins/sql_builders.rs`, producing `join pushdown declined: {clause}; this is a hard error, not a native re-plan`. Doc-comment it with a cross-reference to `ineligible_join_decline` stating why the two are not merged; add the reciprocal cross-reference on `ineligible_join_decline`.
   - [ ] 2.2 Migrate all six sites, each passing only its clause fragment: `n_scan_join_select_items`; `build_n_scan_join_sql`'s no-column-metadata `return Err` and its unrenderable-join-condition `ok_or_else`; `qualified_join_group_by`; `qualified_join_having`; `qualified_join_order_by` (whose existing local `let decline = || …` closure collapses into the shared call).
   - [ ] 2.3 Verify: `cargo test -p lakehouse-engine`, the four goldens plus 1.3's new test green, `cargo clippy --all-targets`, `cargo fmt --check`. Zero diff in generated SQL.

3. **Finding 4 — `column_tables` returns its three outputs.**
   - [ ] 3.1 Replace `collect_column_tables(expr, &mut tables, &mut has_untagged, &mut any_column)` in `joins/rendering.rs` with `pub(super) fn column_tables(expr: &Json) -> (HashSet<String>, bool, bool)`, keeping the existing doc comment's `tableName`-attribution rationale and its ASCII `to_ascii_uppercase` fold verbatim. Update `walk_column_nodes`' doc comment in `adapter/pushdown/support.rs` (line 1304) to name `column_tables`, leaving the rest of the case-folding paragraph verbatim.
   - [ ] 3.2 Update both call sites to destructure the tuple: `conjunct_single_side` (`joins/rendering.rs`) and the `for` loop in `build_n_scan_join_from` (`joins/sql_builders.rs`) — where the tuple must be rebound per iteration, which destructuring inside the loop body gives for free. Update `sql_builders.rs`'s `use super::rendering::{…}` list.
   - [ ] 3.3 Verify as in 2.3.

4. **Finding 6 — attach-point clarity and wrapper deletion.**
   - [ ] 4.1 In `build_n_scan_join_from`, replace the `resolvable.then(…).flatten()` `match` with a let-chain `if resolvable && last_join_point >= 1 && let Some(m) = tables.iter().map(|t| leg_index[t]).max() { … } else { residual.push(…) }`. Carry the existing multi-line comment explaining the `last_join_point >= 1` guard and the `clamp(1, 0)` panic across verbatim.
   - [ ] 4.2 Delete `render_join_condition` from `joins/rendering.rs`. Its one production caller (`joins/sql_builders.rs:69`) and its one test caller (`joins/rendering.rs:396`) call `vs_expression::render_expression_safe` directly; add that import to `sql_builders.rs`. Move the wrapper's "uses `render_expression_safe`, not the filter renderer, so a boolean is returned verbatim rather than suppressed as trivially true" rationale to the production call site.
   - [ ] 4.3 Delete `render_selectlist_item_qualified` from `joins/rendering.rs`. Migrate its one production caller (`n_scan_join_select_items`) and all test callers and test names in `joins/sql_builders.rs` to `render_expression_qualified`. Relocate its doc comment's design intent — one recursive translator covering columns, literals, scalar expressions, top-level and scalar-nested `function_aggregate`, byte-compatible with the former `render_aggregate_qualified` — onto `render_expression_qualified`'s doc comment. Do not drop it.
   - [ ] 4.4 Drop both names from `sql_builders.rs`'s `use super::rendering::{…}` list, and fix the stale `render_join_condition` mention in the `joins/rendering.rs:97` doc comment. Then run `grep -rn "render_join_condition\|render_selectlist_item_qualified" crates/ specs/` — the `specs/` arm is what makes a stale *spec* reference fail this gate too, not only a stale code reference. Zero hits are required under `crates/`. Under `specs/`, zero hits are required in every recorded feature spec EXCEPT `specs/vs-adapter/pushdown-planning-selectlist-expressions/spec.md`, whose single `render_selectlist_item_qualified` hit is expected until `/speq:record` merges this plan's delta for that feature — at this gate, verify instead that `specs/_plans/refactor-pushdown-join-rendering-dedup/vs-adapter/pushdown-planning-selectlist-expressions/spec.md` exists and contains `reached by \`render_expression_qualified\``. Hits are also permitted in `specs/_decision/001-migrate-legacy-decision-log.md`, an immutable archived record, and in this plan's own `specs/_plans/refactor-pushdown-join-rendering-dedup/` files including `review/`, which name both wrappers precisely to record their deletion.
   - [ ] 4.5 Verify as in 2.3, and additionally confirm the `joins` façade baseline is unchanged — neither deleted name is among the nine `pub(crate)` or five `pub(super)` façade items, so `src/adapter/pushdown_surface_probe.rs` must still compile untouched.

5. **Finding 5 — one side-sharding helper.**
   - [ ] 5.1 Add private `fn shard_side(side: &ResolvedJoinSide, tuning: &JoinScanTuning) -> Vec<Vec<FileEntry>>` to `joins/sql_builders.rs`, wrapping `shard_count(tuning.cluster_nodes, tuning.parallelism_factor, side.files.len())` → `partition_files_by_bytes(side.files.clone(), g)` → `relativize_shards_to_root(shards, &side.table_root)`. Take `&ResolvedJoinSide` rather than issue #181's suggested `(files, root, tuning)`: both call sites already hold one, so the tighter signature cannot be called with a mismatched files/root pair.
   - [ ] 5.2 Replace the prefix in `build_side_fan_out_sql` (over `side`) and `build_broadcast_join_sql` (over `sides.fact`). Leave both `build_scan_driving_sql` calls unchanged.
   - [ ] 5.3 Verify as in 2.3. `golden_broadcast_join_sql_unchanged` and `golden_n_scan_join_sql_unchanged` are the direct proof here.

6. **Finding 1 — one clause walk, two divergent callers. [expert]**
   - [ ] 6.1 Establish coverage for the divergences *first*, since a naive merge breaks them silently. Check which of these three cases already has a test and add only the missing ones to the owning test module: (a) `referenced_column_projection` with absent or empty `selectList` but a `filter` naming a column returns only that column — it MUST NOT gain `referenced_side_columns`' short-circuit; (b) `referenced_column_projection` over a request naming no source column returns exactly one column, `all_cols.first()`; (c) `referenced_side_columns` whose narrowing selects nothing returns all of `full_cols`. Already covered and not to be duplicated: `referenced_side_columns_narrows_to_used_columns`, `referenced_side_columns_keeps_all_when_select_list_absent`, `fallback_projection_narrows_to_referenced_columns`.
   - [ ] 6.2 Add a non-ASCII characterisation test pinning the forbidden reconciliation: a column named with a character whose Unicode and ASCII upper-casings differ (for example `ß`, which folds to `SS` under `to_uppercase` and stays `ß` under `to_ascii_uppercase`) must be collected as `SS` by `collect_all_column_names` and unchanged by `collect_side_column_names`. Nothing in the crate enforces this today, which is exactly why a unification would pass the whole suite.
   - [ ] 6.3 Add `pub(super) fn referenced_clause_values(pushdown_req: &Json, visit: impl FnMut(&Json))` to `joins/rendering.rs`, visiting `selectList`, a non-null `filter`, `groupBy`, `orderBy`, then a non-null `having` — the existing order and the existing null filters, unchanged. Doc-comment it as the single owner of "which clauses of a pushdown request can name a source column", and state that the collector is a parameter because the two callers must keep divergent case folding and divergent fallbacks, and state that `referenced_side_columns` deliberately keeps its own absent/empty-`selectList` short-circuit before this walk, so `selectList` is named twice by design; that guard MUST NOT be folded in here, because doing so would give `referenced_column_projection` a short-circuit that `vs-adapter/pushdown-joins-module-structure`'s "One clause walk feeds both wrapper column-narrowing routines" scenario forbids it.
   - [ ] 6.4 Re-express `referenced_column_projection` (`joins/sql_builders.rs`) as `referenced_clause_values(pushdown_req, |v| collect_all_column_names(v, &mut names))`, leaving its projection build and its first-column fallback untouched. Import the helper via the existing `use super::rendering::{…}`.
   - [ ] 6.5 Re-express `referenced_side_columns` (`joins/rendering.rs`): keep the absent/empty-`selectList` early return to `full_cols.to_vec()` *before* the walk, then collect the `condition`, then `referenced_clause_values(pushdown_req, |v| collect_side_column_names(v, table_name, &mut names))`. Confirm the two remaining behavioural equivalences hold: passing the whole `selectList` array to the walk is equivalent to the old per-item loop because `walk_column_nodes` descends arrays; and collecting `condition` before rather than after `selectList` is equivalent because every collector inserts into a `HashSet`.
   - [ ] 6.6 Verify as in 2.3, plus 6.1's and 6.2's new tests and both pre-existing `referenced_side_columns_*` tests green with no edit to any assertion or expected value.

7. **Full verification.**
   - [ ] 7.1 `cargo test` (whole workspace), `cargo clippy --all-targets` with zero warnings, `cargo fmt` clean.
   - [ ] 7.2 `make cross-musl-udf-build`, then bring the compose stack up and run the join E2E binary — `cargo test --features exasol-e2e --test e2e_join_test -- --test-threads=1`. The stack is not started by the target; without it these tests FAIL rather than skip, which mimics a real regression. Also check for a stray `bench/.env` before blaming a hang.
   - [ ] 7.3 Confirm net line count fell and the plan's four Requirements all hold.

## Parallelization

None. Every task edits `joins/rendering.rs`, `joins/sql_builders.rs`, or both, and tasks 2, 3, 4, and 6 additionally contend for `sql_builders.rs`' single `use super::rendering::{…}` block. Tasks 3 and 4 both rewrite the body of `build_n_scan_join_from`. Sequential execution costs little on a refactor this size and removes every merge hazard.

Hard ordering constraints:
- 1 → everything else. Task 1.3's test is the only coverage the six decline messages have; task 2 is unverifiable without it.
- 3 → 4 (both edit `build_n_scan_join_from`).
- 2 → 4 (both edit `n_scan_join_select_items`).

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `joins/rendering.rs::render_join_condition` | One-line pass-through to `vs_expression::render_expression_safe`; callers name the delegate |
| Function | `joins/rendering.rs::render_selectlist_item_qualified` | One-line pass-through to `render_expression_qualified`; callers name the delegate |
| Function | `joins/rendering.rs::collect_column_tables` | Superseded by `column_tables`, which returns the three values instead of writing `&mut` out-params |
| Statements | Six `UdfError::User` string literals in `joins/sql_builders.rs` | Superseded by `join_render_decline` |
| Closure | `let decline = \|\| …` in `joins/sql_builders.rs::qualified_join_order_by` | Superseded by `join_render_decline` |
| Statements | The `shard_count`/`partition_files_by_bytes`/`relativize_shards_to_root` prefix in `build_side_fan_out_sql` and `build_broadcast_join_sql` | Superseded by `shard_side` |
| Imports | `render_join_condition`, `render_selectlist_item_qualified`, `collect_column_tables` in `sql_builders.rs`' `use super::rendering::{…}` | Names deleted or renamed |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| One shared template renders all six qualified N-scan render declines | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `golden_n_scan_render_decline_messages_unchanged` |
| One shared template renders all six qualified N-scan render declines (seventh template stays separate) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/mod.rs` | `golden_ineligible_decline_message_unchanged` (existing, unchanged) |
| One clause walk feeds both wrapper column-narrowing routines (no short-circuit leaks into the projection path) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `referenced_column_projection_narrows_without_select_list` |
| One clause walk feeds both wrapper column-narrowing routines (first-column fallback) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `referenced_column_projection_falls_back_to_first_column` |
| One clause walk feeds both wrapper column-narrowing routines (full-set fallback, and per-table narrowing) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `referenced_side_columns_keeps_all_when_narrowing_empty`, plus existing `referenced_side_columns_narrows_to_used_columns` and `referenced_side_columns_keeps_all_when_select_list_absent` |
| One clause walk feeds both wrapper column-narrowing routines (divergent case folding preserved) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `column_collectors_keep_divergent_case_folding` |
| The two join-rendering pass-through wrappers are deleted rather than retained | Integration | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged`, `golden_grouped_qualified_fallback_sql_unchanged` (existing, unchanged) — plus the crate compiling with neither name present |
| One blind traversal primitive backs every column-collecting walk (amended: returned tuple replaces the three out-params) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, `.../joins/rendering.rs` | `golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged`, `golden_grouped_qualified_fallback_sql_unchanged`, the two `dispatch_golden` decline-wrapper assertions, and `column_collectors_keep_divergent_case_folding` — the first four existing and unedited, proving the collected table set still drives conjunct partitioning identically; the last proving the still-binding case-folding clause |
| A widened derived projection routes to a native wrapper on every path (amended: dialect chain names the surviving delegate) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `golden_grouped_qualified_fallback_sql_unchanged` (existing, unchanged) plus the task 4.4 `crates/ specs/` grep gate — the wrapper's rendered output is unchanged and the authored delta removes the hop from the recorded dialect chain when `/speq:record` merges it |

These are unit tests rather than integration tests because every scenario asserts a pure string-building or pure column-selection computation with no I/O, and because the existing golden characterization suite the plan reuses is already sited there. The runtime proof that the whole refactored path still executes is the join E2E binary below.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-joins-module-structure | `cargo test -p lakehouse-engine golden_` | 5 tests pass: the four pre-existing goldens plus `golden_n_scan_render_decline_messages_unchanged`, all with unedited expected values |
| vs-adapter/pushdown-planning-selectlist-expressions | `grep -rn "render_join_condition\|render_selectlist_item_qualified" crates/ specs/` | Zero hits are required under `crates/`. Under `specs/`, zero hits are required in every recorded feature spec EXCEPT `specs/vs-adapter/pushdown-planning-selectlist-expressions/spec.md`, whose single `render_selectlist_item_qualified` hit is expected until `/speq:record` merges this plan's delta for that feature — at this gate, verify instead that `specs/_plans/refactor-pushdown-join-rendering-dedup/vs-adapter/pushdown-planning-selectlist-expressions/spec.md` exists and contains `reached by \`render_expression_qualified\``. Hits are also permitted in `specs/_decision/001-migrate-legacy-decision-log.md`, an immutable archived record, and in this plan's own `specs/_plans/refactor-pushdown-join-rendering-dedup/` files including `review/`, which name both wrappers precisely to record their deletion. |
| vs-adapter/pushdown-module-structure | `cargo test -p lakehouse-engine dispatch_golden` | Both decline-wrapper goldens pass with unedited committed fixtures, so the returned-tuple column-tables walk partitions conjuncts exactly as the out-param form did |
| vs-adapter/pushdown-joins-module-structure | `make cross-musl-udf-build && cargo test --features exasol-e2e --test e2e_join_test -- --test-threads=1` (compose stack up) | All join E2E tests pass; broadcast and N-scan fallback queries return the same rows as before the refactor |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `cargo test --features exasol-e2e --test e2e_join_test -- --test-threads=1` | 0 failures (FAILS, not skips, without the compose stack) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
