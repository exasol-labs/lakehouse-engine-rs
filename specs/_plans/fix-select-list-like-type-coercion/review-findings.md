# Code Review Findings: fix-select-list-like-type-coercion

## Summary
- Files reviewed: 3
- Total findings: 2 (standard: 2, expert: 0)

Gate evidence collected during review: `cargo test -p lakehouse-engine --lib` → 687 passed / 0
failed; `cargo clippy --all-targets` → clean; `cargo fmt --check` → clean;
`grep -rn "apply_filter_type_rewrites\|apply_select_item_type_rewrites" --include="*.rs" .` → zero
hits; `grep -rn "219" crates/lakehouse-engine/src/adapter/pushdown/` → 2 hits, both describing the
closed fix rather than an open gap. No dead code, no obsolete test, no swallowed error, no
unmeasured optimization, and no `[MAGIC_NUMBER]`/`[TOO_MANY_ARGUMENTS]`/`[SIDE_EFFECT]` violation
found in the diff. The collapse to one `apply_type_rewrites` matches the `pushdown-module-structure`
delta clause-for-clause (one function, `pub(super)`, no alias, caller-agnostic doc).

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [IMPLEMENTATION_IN_NAME] Surviving pipeline function still names its parameter after one caller's render surface
- Location: line 1049 (`apply_type_rewrites`), and the three shadowed locals at lines 1050-1052
- Issue: the collapse made this function serve both render surfaces, and its own doc comment two
  lines above says so verbatim ("One ordered pass list serves every caller, whether the tree is a
  whole filter or a single select-list item", lines 1024-1026), yet the parameter is still
  `filter: &Json` and each intermediate binding is `let filter = …`. The select-list caller at
  `support.rs:1175` passes a select-list item `e`, not a filter, so the binding name is false at
  half the call sites. This also breaks the plan's own caller-agnostic rule for the survivor and the
  `pushdown-module-structure` delta clause "that function SHALL take **the expression tree** and the
  column-type list" — `filter` is one caller's vocabulary, and the function is otherwise named for
  the transformation exactly as the delta requires. Renaming the binding does not change the
  signature the plan pinned (`(&Json, &[(String, String)]) -> Option<Json>` is unchanged), so no
  call site moves.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, rename `apply_type_rewrites`'
  first parameter from `filter` to `expr` and rename the three shadowed `let filter = …` bindings in
  its body to `expr`, leaving the type signature, the pass order, and every call site untouched.
  Then re-run `cargo test -p lakehouse-engine --lib`, `cargo clippy --all-targets`, and `cargo fmt`.

### crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs

#### [MISSING_BOUNDARY_TEST] Join select-list test discards the widening flag that decides whether the broadcast join runs at all
- Location: lines 604 and 629 (`let (projection, _types, _widened) = …`), assertions at 607-619 and 634-639
- Issue: `render_broadcast_join` routes purely on `extract_join_projection`'s third return value —
  `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs:85-87` is
  `if widened { return Ok(None); }`, i.e. a widened projection declines the broadcast fan-out to the
  unaccelerated N-scan wrapper. That flag is therefore the whole observable behavior of the join
  surface for a declined LIKE, and `join_projection_like_guard_reaches_join_select_list` binds it to
  `_widened` in both halves and never asserts it. The decline half then asserts only
  `projection.len() == expected_full_row_len`, which a same-length vector of `ProjectionItem::Expr`
  items would also satisfy — the sibling `selectlist_*` tests in `support.rs` do assert both the
  flag and that every fallback item is a bare `Column`, so this test is the weaker of the pair on
  the surface where the flag actually gates acceleration. Failure mode is a passing test over the
  #196 shape bug (full row projected, flag left `false`, broadcast fan-out emitting the wrong column
  shape).
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs, in
  `join_projection_like_guard_reaches_join_select_list`, bind the third tuple element as `widened`
  in both `extract_join_projection` calls; assert `!widened` in the `C_NAME` pass-through half with
  a message stating that a VARCHAR subject must keep the broadcast projection, and assert `widened`
  in the `C_CUSTKEY` decline half with a message stating that the flag is what declines the
  broadcast join to the N-scan fallback (`joins/sql_builders.rs:85`). In the same decline half, also
  assert that every item of `projection` matches `ProjectionItem::Column(_)`, so a same-length
  vector of rendered `Expr` items cannot pass. Then re-run
  `cargo test -p lakehouse-engine --lib join_projection_like_guard_reaches_join_select_list`,
  `cargo clippy --all-targets`, and `cargo fmt`.

## Expert fixes
[none]
