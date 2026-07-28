# Plan: refactor-pushdown-collect-walk-dedup

## Summary

Collapse three hand-rolled blind JSON collect walks onto one extracted `walk_column_nodes` primitive, and fold two duplicated adapter helper pairs into one each (issue #177). Pure refactor: no behavior change, verified against the golden-SQL characterization baselines already in-tree.

## Context

Issue #177 names three duplications, all in the virtual-schema layer.

**Three blind collect walks.** `collect_column_tables` (`adapter/pushdown/joins/rendering.rs:126`), `collect_side_column_names` (same file, `:245`), and `collect_all_column_names` (`adapter/pushdown/support.rs:1244`) each hand-roll the identical recursion: match a JSON object, act when its `type` is `column`, recurse over every field value; match a JSON array, recurse over every element; stop otherwise. They differ only in what they do at the `column` node — one records owning table names plus two flags, one records this side's column names, one records every column name. Blind traversal is correct for all three because a collect rebuilds nothing, and it is what makes a column buried inside a `CASE` or a function call reachable.

**Two identical string accessors.** `str_prop` (`adapter/mod.rs:449`) and `str_field` (`adapter/connection.rs:205`) have the same body — `.get(key).and_then(as_str).filter(|s| !s.is_empty())` — and differ only in whether they spell the type `Json` or `serde_json::Value`. Thirteen production call sites and two test call sites use the first, eleven the second.

**Two identical resolvers.** `resolve_df_target_partitions` (`adapter/mod.rs:784`) and `resolve_df_threads_per_udf` (`:797`) have byte-identical bodies and near-identical doc comments, differing only in the `PROP_*` constant. Both are called from the adjacent lines `:755` and `:756` in `resolve_df_threading`'s `Fixed` arm.

The scope stops there. Issue #177's Descoped section rejected, on 2026-07-28, a generic property/note-parser framework over the other resolvers, and assigned the rewrite/transform walker to issue #257. Re-proposing either is intent drift.

**Iceberg-spec compliance gate: does not apply.** CLAUDE.md requires any plan touching scanning, pushdown, or schema/type handling to be checked against the Apache Iceberg table spec. This change touches VS-layer pushdown-*planning* JSON traversal and adapter property reading only. It reads no Iceberg metadata, resolves no files, prunes nothing, changes no pushdown semantics, and maps no types — the scan path, `iceberg_predicate`, and the type-mapping tables are untouched. The gate therefore does not bite.

- **Goals** — one blind column-collecting traversal in the pushdown module tree instead of three; one non-empty-string JSON accessor in the adapter root instead of two; one FIXED-mode DataFusion count resolver instead of two. Closes #177 as written.
- **Non-Goals** — the rewrite/transform walker (`rewrite_expr_tree`, #257), a generic `prop_parsed<T>` / `note_parsed<T>` framework, any `Visitor` trait or typed AST, the two descoped transform walks (`annotate_columns_with_alias`, `strip_table_alias`), the three `support` type-rewrite guards, folding `resolve_s3_max_connections`, and reconciling the `to_uppercase` / `to_ascii_uppercase` divergence between the three walks.

## Design

### Context

Three forces shape the collect primitive.

First, all three current collectors act ONLY on a `column` node. A primitive that visited every object node — the shape issue #177 literally suggests, `walk_json(expr, &mut impl FnMut(&Map))` — would push the `type == "column"` test back into all three closures, replacing one duplication with a smaller one three times over.

Second, no current caller needs to see arrays or scalar nodes, so a widest-form `FnMut(&Json)` variant would hand every closure a node shape it must immediately narrow.

Third, the reuse surface is worth narrowing deliberately. `walk_column_nodes` is a deeper module than `walk_json` by the `/speq:design-philosophy` test: its interface is one function plus a one-argument closure, it absorbs both the recursion and the node-type test, and every caller's remaining body is 2–4 lines.

For the two accessor folds the force is different: both issue #177's literal wording ("`str_prop` / `str_field` both call it") and the obvious minimal diff leave one-line pass-through wrappers behind. A function whose entire body is a call to another function with the same arguments is the pass-through red flag from `/speq:design-philosophy`. The fold deletes the wrappers instead.

### Decision

Extract one primitive that owns traversal AND the `column` test; delete every wrapper.

```rust
// adapter/pushdown/support.rs
pub(super) fn walk_column_nodes(expr: &Json, f: &mut impl FnMut(&Map<String, Json>))
```

Placed in `support.rs` at `pub(super)`. That visibility already reaches `adapter::pushdown` and every descendant, `adapter::pushdown::joins::rendering` included, so the joins side needs no widening — which is what `vs-adapter/pushdown-joins-module-structure`'s `pub(super)` ceiling requires.

#### Architecture

```
                    adapter/pushdown/support.rs
                    walk_column_nodes(expr, &mut f)
                      ├─ Json::Object(map) ─ type=="column" ─▶ f(map)
                      │                    └─ recurse map.values()
                      ├─ Json::Array       ─▶ recurse each element
                      └─ _                 ─▶ stop
                                 ▲ pub(super): reaches pushdown + all descendants
             ┌───────────────────┼───────────────────────┐
             │                   │                       │
  collect_all_column_names   collect_column_tables   collect_side_column_names
  (support.rs)               (joins/rendering.rs)    (joins/rendering.rs)
   names.insert(              any_column = true;      if tn == table_name
     name.to_uppercase())     tables.insert(            out.insert(
                                tn.to_ascii_upper())      name.to_ascii_upper())
                              | has_untagged = true

                    adapter/mod.rs
                    nonempty_str(v, key) -> Option<&str>       [private to `adapter`]
                      ├─ 13 property call sites in mod.rs + 2 in its tests
                      └─ 11 credential call sites in connection.rs (`super::nonempty_str`)

                    resolve_df_fixed_count(props, key, nr_of_cores) -> usize
                      └─ ThreadingMode::Fixed arm, once per PROP_* key
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Primitive owns traversal AND the node-type test | `walk_column_nodes` | All three callers act only on `column` nodes; keeping the test in the primitive removes it from three closures instead of relocating it |
| `&mut impl FnMut` callback, not a trait | `walk_column_nodes` | The IR is untyped `serde_json`; a free function plus a closure is the honest size. #257 takes the same position for its rewrite primitive |
| Blind recursion over `map.values()` | `walk_column_nodes` | A collect rebuilds nothing, so descending into every field is safe and is what reaches a nested column |
| Callback receives `&Map<String, Json>`, not `&Json` | `walk_column_nodes` | The caller has already been told the node is a `column` object; handing it a `Json` would force every closure to re-narrow |
| `pub(super)` in `support`, no widening | `walk_column_nodes` | `pub(super)` there already reaches `joins::rendering`; satisfies the joins-module visibility ceiling with no `use`-path change |
| Case-folding call stays in each closure, verbatim | all three closures | `to_uppercase` and `to_ascii_uppercase` differ for non-ASCII input; unifying them is a behavior change, out of scope |
| Delete the wrapper, migrate the call sites | `nonempty_str`, `resolve_df_fixed_count` | A body that is one call with the same arguments is a pass-through, the shallow-module red flag |
| Helper stays private to `adapter` | `nonempty_str` | `connection` is a child module and can name a private parent item; hoisting widens nothing |
| Existing golden-SQL baselines are the gate | verification | "No behavior change" is otherwise unfalsifiable; the goldens assert full strings, and the in-code gate already says to re-run after every dedup extraction |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `walk_column_nodes` visiting `column` nodes only | Issue #177's suggested `walk_json` over every Object node; a widest-form `FnMut(&Json)` | Both alternatives repeat the `type == "column"` test in all three closures and no current caller needs a non-column node. Deliberate narrowing of the reuse surface; the smaller total diff. An intentional departure from the issue's suggested name and shape |
| One `nonempty_str`, both call sites migrated | Issue #177's literal "both call it", leaving `str_prop` and `str_field` as one-line pass-throughs | Two pass-throughs is the exact shallow-module red flag; ~26 mechanical call-site edits buy the deletion of two names |
| Delete both `resolve_df_*` names, migrate the 14 test call sites | Keep both as pass-throughs so the tests need no edit | Same pass-through argument. Test names and expected values are unchanged; only the callee spelling and the added key argument change |
| No delta to `vs-adapter/pushdown-joins-module-structure` | Add a joins-side scenario mirroring the primitive requirement | Its existing "Generated join SQL is byte-identical across the split" scenario already scopes a golden baseline over "any duplication extraction" across the exact four paths this change touches, and its "widen only to `pub(super)`" clause already caps visibility. A mirror scenario would restate two binding requirements |
| New feature `vs-adapter/adapter-module-structure` for the two adapter-root folds | Put them in `create-virtual-schema-adapter-notes-resources`, or split across the four behavioral features that consume the accessor | The duplication spans four behavioral features (`connection-credentials`, `create-virtual-schema`, `create-virtual-schema-adapter-notes-resources`, `refresh-and-set-properties`); no one of them can own it without leaking a structural decision across a boundary. `*-module-structure` is the established home for structural contracts here — two precedents in-tree |
| Collect primitive stays separate from #257's rewrite primitive | One shared walker for both | A rewrite MUST NOT descend into and rebuild `dataType`/`name` sub-objects, hence #257's curated child-field list; a collect is read-only, so blind traversal is both correct and necessary. Both issues state the separation in writing |
| `resolve_s3_max_connections` not folded | Fold all three resolvers | Its fallback derives an AUTO value from `auto_threads_per_udf` rather than `max(nr_of_cores, 1)`. Folding it needs a second parameter that re-splits the function at every call. Its doc comment's cross-reference to `str_prop` still needs renaming |
| No generic property-parsing framework | `prop_parsed<T>` / `note_parsed<T>` plus a config table over the eleven `resolve_*` readers | Rejected in issue #177 on 2026-07-28: individually documented one-liners with differing defaults and validation; a generic relocates them into indirection without cutting complexity |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-module-structure | CHANGED | `vs-adapter/pushdown-module-structure/spec.md` |
| vs-adapter/adapter-module-structure | NEW | `vs-adapter/adapter-module-structure/spec.md` |

## Impact

None. No user-facing, operator-facing, or wire-facing change: the generated scan-driving SQL, the `adapterNotes` payload, the advertised capabilities, and every error message stay byte-identical. No breaking change. No VS property, connection property, or `.so` interface is added, removed, or renamed.

## Dependencies

None. No new crate dependency; `serde_json::Map` is already in the dependency graph. Independent of issue #257, which this plan must neither pre-empt nor block.

## Implementation Tasks

1. **Fold the two non-empty-string JSON accessors into `nonempty_str`**
   1. Add `fn nonempty_str<'a>(v: &'a Json, key: &str) -> Option<&'a str>` to `crates/lakehouse-engine/src/adapter/mod.rs`, private, body copied verbatim from `str_prop`. Its doc comment states the contract the callers depend on: `Some` only for a present, string-typed, non-empty value, so absent, null, non-string, and empty-string all reach the caller's default.
   2. Delete `str_prop` (`mod.rs:449`) and update its 13 production call sites (`:187`, `:189`, `:209`, `:697`, `:731`, `:785`, `:798`, `:845`, `:866`, `:877`, `:889`, `:901`, `:914`) and 2 test call sites (`:1225`, `:1232`) to call `nonempty_str`.
   3. Delete `str_field` (`connection.rs:205`), add `use super::nonempty_str;`, and update its 11 call sites (`:213`–`:218`, `:231`–`:235`).
   4. Rename every prose reference to the deleted names: the `S3_MAX_CONNECTIONS` doc comment's `str_prop → parse → filter(>=1)` phrase (`mod.rs:811`) and the four test comments that read "str_prop filters empty strings" (`:1981`, `:2033`, `:2084`, `:2242`).
   5. No new test. The empty-string-to-`None` contract is already asserted by those four property tests and by the `connection-credentials` suite; adding a direct test of a moved one-liner would duplicate existing coverage.

2. **Fold the two DataFusion FIXED-mode count resolvers**
   1. Replace `resolve_df_target_partitions` (`mod.rs:784`) and `resolve_df_threads_per_udf` (`:797`) with one `fn resolve_df_fixed_count(props: &Json, key: &str, nr_of_cores: u32) -> usize` carrying the merged doc comment: explicit positive integer wins, otherwise `max(nr_of_cores, 1)`, and `1` when `nr_of_cores` is `0`. Name it for the FIXED mode it serves so it is not mistaken for a general property reader.
   2. Update the two production call sites in `resolve_df_threading`'s `Fixed` arm (`:755`, `:756`) to pass `PROP_DF_TARGET_PARTITIONS` and `PROP_DF_THREADS_PER_UDF`.
   3. Update the 14 test call sites (`:1818`, `:1821`, `:1824`, `:1831`, `:1924`, `:1927`, `:1930`, `:1937`, `:2287`, `:2298`, `:2309`, `:2321`, `:2332`, `:2343`) to pass the key. Keep every test function name, every asserted expected value, and every assertion message unchanged across all ten affected test functions. Two of the ten round-trip through `build_adapter_notes` and so characterize `vs-adapter/create-virtual-schema-adapter-notes-resources`: `df_target_partitions_uses_supplied_value` (`mod.rs:1829`) and `df_threads_per_udf_uses_supplied_value` (`:1935`).
   4. Update the three doc cross-references to the deleted names: `DEFAULT_DF_TARGET_PARTITIONS` (`:76`), `DEFAULT_DF_THREADS_PER_UDF` (`:80`), and the `S3_MAX_CONNECTIONS` doc (`:812`).
   5. Leave `resolve_s3_max_connections` and `auto_threads_per_udf` unchanged apart from those doc renames.

3. **Extract `walk_column_nodes` and migrate `collect_all_column_names`**
   1. Add `pub(super) fn walk_column_nodes(expr: &Json, f: &mut impl FnMut(&serde_json::Map<String, Json>))` to `crates/lakehouse-engine/src/adapter/pushdown/support.rs`: on `Json::Object(map)`, call `f(map)` when `map.get("type").and_then(|t| t.as_str()) == Some("column")`, then recurse over `map.values()`; on `Json::Array(items)`, recurse over each element; otherwise stop. Recursive calls reborrow the callback (`f`, or `&mut *f` if the reborrow needs spelling out).
   2. Write the doc comment as design intent, not a restatement of the name: it owns both the recursion and the `type == "column"` test because every caller acts only on `column` nodes; it traverses blindly because a collect rebuilds nothing, which is what reaches a column buried in a `CASE` or function call; and it MUST NOT be merged with issue #257's curated post-order rewrite primitive, which cannot descend into `dataType`/`name` sub-objects.
   3. Reduce `collect_all_column_names` (`support.rs:1244`) to one `walk_column_nodes` call with a closure that inserts `name.to_uppercase()`. Keep the full Unicode `to_uppercase` verbatim, keep the existing signature and `pub(super)` visibility, and keep the existing doc comment's caller-facing explanation of why nested fields matter.
   4. Add one unit test for `walk_column_nodes` in `support.rs`'s test module — the only new test in this plan, because the primitive is new non-trivial logic: over a fixture nesting `column` nodes inside a function's `arguments` array, a `CASE`'s `results`, and a comparison predicate's `left`/`right`, the callback fires exactly once per `column` node and never for a non-`column` object, a scalar, or an array node.
   5. Extend that fixture with one `column` object carrying a child object that is itself a `column` node, and assert the callback fires for BOTH. This pins the invariant all three current walks hold — `f(map)` runs, then `map.values()` is descended unconditionally — which no other assertion catches: an implementation written as `if column { f(map) } else { recurse }` passes every other case in the fixture and every existing golden, because no production request nests a `column` inside a `column` today.

4. **Migrate the two joins collect walks onto `walk_column_nodes`** [expert]
   1. Import `walk_column_nodes` alongside the existing `use super::super::support::{project_columns, quote_ident};` in `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs`. No visibility widens: `pub(super)` in `support` already reaches this module.
   2. Reduce `collect_column_tables` (`:126`) to one `walk_column_nodes` call whose closure sets `*any_column = true`, then inserts `tn.to_ascii_uppercase()` into `tables` when `tableName` is present or sets `*has_untagged = true` when it is absent — same statement order as today. One `FnMut` captures all three `&mut` out-parameters by unique borrow, which is the borrow-checker-sensitive step in this plan. Keep the signature, the three out-parameters, and `pub(super)` unchanged so `conjunct_single_side` (`:167`) and the N-scan side-attribution caller (`joins/sql_builders.rs:145`) compile unedited.
   3. Reduce `collect_side_column_names` (`:245`) to one `walk_column_nodes` call whose closure reads `tableName` and `name`, requires both, requires `tn.eq_ignore_ascii_case(table_name)`, then inserts `name.to_ascii_uppercase()` into `out`. Keep the signature and private visibility.
   4. Keep both closures' `to_ascii_uppercase` calls verbatim. They deliberately differ from `collect_all_column_names`'s Unicode `to_uppercase`; unifying them would change behavior for non-ASCII names and is out of scope.
   5. Confirm no `Json::Object` or `Json::Array` recursion arm survives in either function, so exactly one blind column-collecting traversal remains in the pushdown module tree.
   6. Touch none of `annotate_columns_with_alias`, `strip_table_alias`, or the three `support` type-rewrite guards. They are rewrite-shaped and belong to issue #257.

5. **Run the gates and confirm the structural claims**
   1. `cargo fmt`, then `cargo clippy --all-targets`, then `cargo test`. Clippy passing does not imply `fmt` clean — run both.
   2. Confirm the four join golden-SQL assertions, the `dispatch_golden` goldens, and the declined-`ORDER BY` hidden-column tests pass with no edit to any golden string or expected value. A required edit to a golden string means behavior changed and the migration is wrong.
   3. Confirm the deletions by grep, so the "one implementation" requirements are checked rather than asserted: no `fn str_prop`, no `fn str_field`, no `fn resolve_df_target_partitions`, no `fn resolve_df_threads_per_udf` anywhere in `crates/`. For the traversal, scope the check to the two migrated functions: neither `collect_column_tables` nor `collect_side_column_names` retains a `Json::Object` or `Json::Array` arm, while `annotate_columns_with_alias`'s and `referenced_side_columns`'s `Json::Array` occurrences MUST survive untouched — `joins/rendering.rs` goes from four occurrences to two, never to zero. Zero means out-of-scope code was edited: `annotate_columns_with_alias` is the walk issue #177 descopes, issue #257 owns, and task 4.6 forbids touching.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 3 |
| Group B | Task 2, Task 4 |
| Group C | Task 5 |

Sequential dependencies:

- Task 1 → Task 2 (the folded resolver calls `nonempty_str`; both edit `adapter/mod.rs`)
- Task 3 → Task 4 (Task 4 calls the primitive Task 3 adds)
- Group A, Group B → Group C (the gates run over the finished change)

Group A's two tasks touch disjoint files (`adapter/mod.rs` + `adapter/connection.rs` versus `adapter/pushdown/support.rs`) and can run concurrently; the same holds for Group B (`adapter/mod.rs` versus `adapter/pushdown/joins/rendering.rs`).

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/mod.rs::str_prop` | Replaced by `nonempty_str`; not kept as a pass-through |
| Function | `crates/lakehouse-engine/src/adapter/connection.rs::str_field` | Replaced by `nonempty_str`; not kept as a pass-through |
| Function | `crates/lakehouse-engine/src/adapter/mod.rs::resolve_df_target_partitions` | Replaced by `resolve_df_fixed_count`; not kept as a pass-through |
| Function | `crates/lakehouse-engine/src/adapter/mod.rs::resolve_df_threads_per_udf` | Replaced by `resolve_df_fixed_count`; not kept as a pass-through |
| Recursion body | `adapter/pushdown/support.rs::collect_all_column_names` — its `Json::Object` / `Json::Array` arms | Replaced by `walk_column_nodes` |
| Recursion body | `adapter/pushdown/joins/rendering.rs::collect_column_tables` — its `Json::Object` / `Json::Array` arms | Replaced by `walk_column_nodes` |
| Recursion body | `adapter/pushdown/joins/rendering.rs::collect_side_column_names` — its `Json::Object` / `Json::Array` arms | Replaced by `walk_column_nodes` |

No test is removed: every existing test is a characterization gate for this refactor.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| One blind traversal primitive backs every column-collecting walk — primitive contract | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `walk_column_nodes_visits_every_nested_column_node_once` (new) |
| One blind traversal primitive backs every column-collecting walk — `collect_column_tables` side attribution | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `golden_n_scan_join_sql_unchanged` (existing, unedited) |
| One blind traversal primitive backs every column-collecting walk — `collect_side_column_names` per-side narrowing | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `referenced_side_columns_narrows_to_used_columns`, `referenced_side_columns_keeps_all_when_select_list_absent` (existing, unedited) |
| One blind traversal primitive backs every column-collecting walk — `collect_all_column_names` wrapper projection | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `group_by_fallback_matches_golden`, `multi_count_distinct_decline_matches_golden` (existing, unedited) — the complete `dispatch_golden` coverage for this collector: both are decline-wrapper paths whose goldens carry an inner-scan `projection` narrowed from the four-column fixture universe to `["REGION","NAME"]` and `["NAME","ID"]`. The empty-result and partial/merge grouped goldens are deliberately NOT named: they render through `empty_result_sql` and the decomposition path, neither of which reaches `referenced_column_projection` |
| One blind traversal primitive backs every column-collecting walk — `collect_all_column_names` hidden-column append order | Unit | `crates/lakehouse-engine/src/adapter/pushdown/topn.rs` | `declined_order_by_expression_appends_referenced_columns_as_hidden`, `declined_order_by_two_expression_keys_renders_both_and_leaks_none` (existing, unedited) |
| One blind traversal primitive backs every column-collecting walk — broadcast and grouped-qualified join SQL | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `golden_broadcast_join_sql_unchanged`, `golden_grouped_qualified_fallback_sql_unchanged` (existing, unedited) |
| One accessor reads a non-empty string field for both adapter-root modules — property side | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | the four empty-string-to-default property tests at `:1981`, `:2033`, `:2084`, `:2242`, plus `set_properties` merge tests at `:1225`, `:1232` (existing, assertions unedited) |
| One accessor reads a non-empty string field for both adapter-root modules — credential side | Integration | `crates/lakehouse-engine/src/adapter/connection.rs` | the `parse_creds` / `read_connection` credential-field suite (existing, unedited) |
| One resolver reads both DataFusion FIXED-mode count properties | Unit | `crates/lakehouse-engine/src/adapter/mod.rs` | `df_target_partitions_defaults_to_one`, `df_target_partitions_uses_supplied_value`, `df_threads_per_udf_defaults_to_one`, `df_threads_per_udf_uses_supplied_value`, `df_target_partitions_explicit_wins`, `df_target_partitions_defaults_to_nr_of_cores`, `df_target_partitions_unknown_cores_defaults_to_1`, `df_threads_per_udf_explicit_wins`, `df_threads_per_udf_defaults_to_nr_of_cores`, `df_threads_per_udf_unknown_cores_defaults_to_1` (existing; callee spelling changes, every expected value unedited) |

These are unit tests, not integration tests, because every scenario in this plan is a structural property of pure JSON-to-string computation with no I/O — the exception `/speq:planning` allows. One new test only: the new primitive.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-module-structure | `cargo test -p lakehouse-engine golden_` | All 4 join golden assertions pass; no golden string edited in the diff |
| vs-adapter/pushdown-module-structure | `cargo test -p lakehouse-engine walk_column_nodes` | The new primitive test passes |
| vs-adapter/pushdown-module-structure | `cargo test -p lakehouse-engine adapter::pushdown` | 0 failures across the pushdown, joins, top-N, and dispatch-golden suites |
| vs-adapter/pushdown-module-structure | `grep -c 'Json::Array' crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `2`, down from 4 — exactly the two arms that MUST survive untouched: `annotate_columns_with_alias`'s rebuild arm (`:79`, issue #257's territory) and `referenced_side_columns`'s `selectList` match (`:293`). `0` here means the implementer edited out-of-scope code |
| vs-adapter/pushdown-module-structure | `grep -A 12 'fn collect_column_tables\|fn collect_side_column_names' crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs \| grep -c 'Json::'` | `0` — neither migrated collector kept a traversal arm of its own |
| vs-adapter/pushdown-module-structure | `grep -rn 'fn walk_column_nodes' crates/` | Exactly one hit, in `adapter/pushdown/support.rs` |
| vs-adapter/adapter-module-structure | `cargo test -p lakehouse-engine adapter::tests` | 0 failures; the ten resolver tests keep their names and expected values |
| vs-adapter/adapter-module-structure | `cargo test -p lakehouse-engine connection` | 0 failures across the credential-parsing suite |
| vs-adapter/adapter-module-structure | `grep -rn 'fn str_prop\|fn str_field\|fn resolve_df_target_partitions\|fn resolve_df_threads_per_udf' crates/` | No hits — all four deleted, none kept as a pass-through |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Format | `cargo fmt` | No changes after the run |
| Lint | `cargo clippy --all-targets` | 0 errors, 0 warnings |
| Test | `cargo test` | 0 failures, no test assertion or expected value edited except the resolver callee spelling |
| Build | `make cross-musl-udf-build` | Exit 0 — the `.so` still builds in `rust:1.94-bookworm`; never `cargo build --release` on the host |
