# Tasks: fix-select-list-like-type-coercion

## Phase 2: Implementation (Group A)
- [x] 2.1 Add `like_subject_type_guard` as the FIRST pass of `apply_select_item_type_rewrites` (`support.rs:1078`); rewrite its doc comment (state 3-pass order, delete "TRACKED GAP (issue #219)" paragraph, no caller/renderer names); fix stale cross-references at `support.rs:735` and `:945`

## Phase 2: Implementation (Group B)
- [x] 2.2 Rename `select_list_pipeline_omits_like_pass_pending_219` (`support.rs:4220`) to `select_list_pipeline_runs_like_guard`; flip its second assertion from `Some(filter.clone())` to `None`; rewrite doc comment; drop `(#219)` citation
- [x] 2.3 Add select-list projection tests to `support.rs`'s `mod tests`: `selectlist_like_over_date_projects_cast_expr`, `selectlist_like_over_non_string_subject_falls_back_to_full_row` (table-driven), `selectlist_like_inside_case_over_decimal_falls_back_to_full_row`
- [x] 2.4 Add `join_projection_like_guard_reaches_join_select_list` to `joins/rendering.rs`'s `mod tests`

## Phase 2: Implementation (Group C)
- [x] 2.5 Collapse `apply_filter_type_rewrites` and `apply_select_item_type_rewrites` into one `pub(super) fn apply_type_rewrites`; delete the select-item function outright (no alias); update all 8 `mod.rs` sites + `support.rs:1203`/`:4229`; update remaining doc sites `mod.rs:855`, `mod.rs:891`, `support.rs:734-735`, `support.rs:945`; fold `select_list_pipeline_runs_like_guard` to one assertion and rename to `type_rewrite_pipeline_runs_like_guard`

## Phase 2: Implementation (Group D)
- [x] 2.6 Run the gate: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, `cargo test --features exasol-e2e --no-run`; verify `git diff` shows exactly one flipped assertion and `grep -rn "apply_select_item_type_rewrites\|apply_filter_type_rewrites" --include="*.rs"` returns zero hits

## Phase 3: Verification
- [ ] 3.1 Run test suite (cargo test)
- [ ] 3.2 Run linter (cargo clippy --all-targets)
- [ ] 3.3 Run formatter check (cargo fmt)
- [ ] 3.4 Scenario coverage audit against plan's Verification > Scenario Coverage table
- [ ] 3.5 Manual verification steps (grep checks; e2e requires Docker stack)

## Phase 4: Review Fixes
- [x] 4.1 In crates/lakehouse-engine/src/adapter/pushdown/support.rs, rename `apply_type_rewrites`' first parameter from `filter` to `expr` and rename the three shadowed `let filter = …` bindings in its body to `expr`, leaving the type signature, the pass order, and every call site untouched. Then re-run `cargo test -p lakehouse-engine --lib`, `cargo clippy --all-targets`, and `cargo fmt`.
- [x] 4.2 In crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs, in `join_projection_like_guard_reaches_join_select_list`, bind the third tuple element as `widened` in both `extract_join_projection` calls; assert `!widened` in the `C_NAME` pass-through half with a message stating that a VARCHAR subject must keep the broadcast projection, and assert `widened` in the `C_CUSTKEY` decline half with a message stating that the flag is what declines the broadcast join to the N-scan fallback (`joins/sql_builders.rs:85`). In the same decline half, also assert that every item of `projection` matches `ProjectionItem::Column(_)`, so a same-length vector of rendered `Expr` items cannot pass. Then re-run `cargo test -p lakehouse-engine --lib join_projection_like_guard_reaches_join_select_list`, `cargo clippy --all-targets`, and `cargo fmt`.
