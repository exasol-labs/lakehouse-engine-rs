# Code Review Findings: refactor-pushdown-collect-walk-dedup

## Summary
- Files reviewed: 4
- Total findings: 3 (standard: 2, expert: 1)

Verified clean, no finding raised:

- **No behavior change.** All three migrated collectors are semantically byte-for-byte equivalent to their predecessors: `walk_column_nodes` fires the callback on a `column` object, then descends `map.values()` unconditionally, then descends array elements — the exact arm order and unconditional-descent shape all three originals had. `*any_column = true` still precedes the `tableName` match in `collect_column_tables`. `cargo test -p lakehouse-engine` is green (672 unit tests + 15 integration binaries, 0 failures); `cargo fmt --check` and `cargo clippy --all-targets` are clean.
- **No golden-SQL string or expected value was edited.** `git diff -U0 | grep -iE '^[+-].*(golden|SELECT |expected)'` returns nothing. The only test-body edits are the resolver callee spelling plus the added `PROP_*` key argument; every test name, expected value, and assertion message is unchanged.
- **The case-folding divergence was preserved verbatim** — `collect_all_column_names` keeps Unicode `to_uppercase` (support.rs:1281), the two joins closures keep `to_ascii_uppercase` (rendering.rs:136, :245). (It is preserved but undocumented — see the expert finding.)
- **`walk_column_nodes` visibility is exactly `pub(super)`** with no widening anywhere; `grep -rn 'fn walk_column_nodes' crates/` yields exactly one definition.
- **Deletions confirmed, none left as a pass-through.** `str_prop`, `str_field`, `resolve_df_target_partitions`, `resolve_df_threads_per_udf` have zero remaining hits repo-wide, prose references included.
- **No out-of-scope code was edited.** `Json::Array` in `joins/rendering.rs` went 4 → 2, exactly the two arms that had to survive (`annotate_columns_with_alias`'s rebuild arm and `referenced_side_columns`'s `selectList` match); `grep -rn 'Some("column")'` confirms only the descoped rewrite walk retains its own traversal.
- **`resolve_df_fixed_count(props, key, nr_of_cores)`** is 3 arguments and `key` is data, not a branch selector (the body is identical for both keys) — no `[TOO_MANY_ARGUMENTS]`, no `[SELECTOR_ARGUMENT]`.
- The `#257` cross-reference in `walk_column_nodes`' doc comment is plan-mandated design rationale and matches an established in-repo convention (`resolve_join_broadcast_max_bytes`' `BL-001` reference, `strip_table_alias`' `issue #193`) — not `[WORK_TRACKING_COMMENT]`.

## Standard fixes

### crates/lakehouse-engine/src/adapter/mod.rs

#### [IMPLEMENTATION_IN_NAME] Folded accessor's parameter still named for one caller's payload

- Location: line 455
- Issue: `fn nonempty_str<'a>(props: &'a Json, key: &str)` kept `str_prop`'s parameter name after the fold widened the accessor beyond VS properties. 11 of its 26 call sites now pass CONNECTION credential JSON, not properties (`connection.rs:208`–`:234`, e.g. `nonempty_str(json, "secret_key")`). The doc comment directly above already generalizes correctly ("Read `key` from a JSON object as a non-empty string"), so the signature now contradicts its own contract, and a reader arriving from `connection.rs` lands on a parameter named for a payload that module never handles.
- Fix: In crates/lakehouse-engine/src/adapter/mod.rs line 455, rename `nonempty_str`'s first parameter from `props` to `obj` in the signature and in the three `props`-spelled expressions of its body. Change nothing else — the parameter is positional, so no call site in `mod.rs` or `connection.rs` is affected. Then run `cargo fmt` and `cargo clippy --all-targets`.

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [MISSING_BOUNDARY_TEST] Primitive's non-container-root no-op is unpinned despite being a live production input

- Location: lines 1439–1487 (`walk_column_nodes_visits_every_nested_column_node_once`); primitive's `_ => {}` arm at line 1260
- Issue: the new test's fixture root is always a `Json::Object`, so the `_ => {}` arm is exercised only for nested scalars, never for the root. Production does reach the primitive with a non-container root: `referenced_column_projection` (`joins/sql_builders.rs:721`, `:726`–`:730`) and `referenced_side_columns` (`rendering.rs:288`–`:290`) pass `pushdown_req.get("selectList")` / `get("groupBy")` / `get("orderBy")` straight through with no `is_null()` guard — unlike the `filter` and `having` reads two lines away in both functions, which do guard with `.filter(|f| !f.is_null())`. A request carrying `"groupBy": null` therefore hands `walk_column_nodes` a `Json::Null` root. Behavior is unchanged by this refactor (the originals had the same catch-all arm), but the primitive is new code and the contract those four unguarded call sites depend on has no test.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, add one test to the existing `mod tests` next to `walk_column_nodes_visits_every_nested_column_node_once`, named `walk_column_nodes_never_invokes_callback_for_a_non_container_root`. Call `walk_column_nodes` four times against `serde_json::Value::Null`, `serde_json::json!("REGION")`, `serde_json::json!(7)`, and `serde_json::json!({})`, each incrementing a shared `usize` counter from the callback, then assert the counter is `0` with the message stating that a null, scalar, or empty-object root must be a no-op because `groupBy`/`orderBy`/`selectList` reach the primitive unguarded. Do not modify the existing test. Run `cargo test -p lakehouse-engine walk_column_nodes`.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [MISSING_DESIGN_INTENT] Primitive's doc comment omits the one rationale its callers most need — the deliberate case-folding divergence

- Location: lines 1235–1246 (`walk_column_nodes` doc comment); divergent folds at support.rs:1281 and joins/rendering.rs:136, :245
- Issue: before this change the three walks were three ~15-line recursive functions, and nobody had cause to compare their case folds. They are now 2–8 line closures over one shared primitive, and the folds visibly disagree: `collect_all_column_names` uses Unicode `to_uppercase` (support.rs:1281) while `collect_column_tables` (rendering.rs:136) and `collect_side_column_names` (rendering.rs:245) use `to_ascii_uppercase`. The divergence is behavior-load-bearing — `ß` folds to `SS` under one and stays `ß` under the other — and the plan's Patterns table makes preserving it verbatim a hard requirement, but that requirement lives only in the plan, which is archived at record time. Nothing in the code says the disagreement is deliberate. The refactor also brought the two folds into one feature's single code path: the join decline wrapper narrows its inner-scan projection via `collect_all_column_names` (`joins/sql_builders.rs:722`–`:734`, Unicode) and its per-side fan-out columns via `collect_side_column_names` (`rendering.rs:245`, ASCII), and both compare the resulting names against the same `full_cols` list. `walk_column_nodes`' doc comment explains at length why traversal is blind and why it must stay separate from issue #257's rewrite primitive, yet says nothing about case folding. No golden and no unit test anywhere in the crate uses a non-ASCII column name, so unifying the three folds — the obvious next cleanup now that the closures sit side by side — passes the entire suite while silently changing behavior.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, append a final paragraph to `walk_column_nodes`' doc comment (lines 1235–1246) stating that (a) the primitive deliberately owns no case folding — each callback applies its own; (b) the three current callers deliberately disagree, `collect_all_column_names` using Unicode `to_uppercase` and `collect_column_tables` / `collect_side_column_names` in `joins/rendering.rs` using `to_ascii_uppercase`; and (c) the two MUST NOT be unified, because they differ for non-ASCII identifiers and no test in the crate uses one, so a unification is a silent behavior change that the whole suite passes. Change no `to_uppercase` or `to_ascii_uppercase` call, no signature, and no test — this fix is doc-comment text only. Then run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test -p lakehouse-engine`.
