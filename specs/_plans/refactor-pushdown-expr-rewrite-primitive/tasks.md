# Tasks: refactor-pushdown-expr-rewrite-primitive

## Commit 1 — pure refactor, byte-identical rendered SQL

### Group A
- [x] 1. Add the leaf-equivalence characterization test BEFORE any migration, and prove it against
  the UNMIGRATED code: one test asserting `rewrite_decimal_stringifications` returns a non-object
  node unchanged (mirroring `string_fn_guard_passes_through_non_object_node`). MUST pass against
  today's code with its `!node.is_object()` early return still in place, and MUST re-run unchanged
  after task 4.

### Group B
- [x] 2. Add `EXPR_ARRAY_FIELDS`, `EXPR_SINGLE_FIELDS`, and a PRIVATE `fn rewrite_expr_tree` to
  `support.rs`. Doc comment MUST state post-order contract, decline-propagation contract, the
  always-`Some` composition for the infallible walker, and WHY the field list is curated. Child
  conditions MUST reproduce today's exactly: array field only when `Json::Array`, single field only
  when `child.is_object()`. [expert]

### Group C (tasks 3 and 4 touch disjoint functions in one file; serialize edits if overlap risk)
- [x] 3. Migrate `string_function_arg_type_guard` onto `rewrite_expr_tree`, deleting its hand-rolled
  step-1 loops and its `!node.is_object()` early return; step 2 becomes the per-node closure.
- [x] 4. Migrate `rewrite_decimal_stringifications` onto `rewrite_expr_tree` with an always-`Some`
  closure, keeping `-> Json` via `.unwrap_or_else(|| node.clone())` — NOT `.expect`. State the
  always-`Some` invariant in the doc comment, not in a panic message.

### Group D
- [x] 5. Update both migrated guards' doc comments to name the shared primitive as the single owner
  of the traversal (implements `pushdown-planning-decimal-string-format` and
  `pushdown-planning-string-fn-type-coercion` deltas — reattribution only, no rendered-byte change).

### Group E
- [x] 6. Run the commit-1 gate: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, and
  `cargo test --features exasol-e2e --no-run`. No test assertion may be edited.

## Commit 2 — LIKE guard onto the primitive, behavior change

### Group F
- [x] 7. Add failing regression tests: `support.rs` unit tests for a DECIMAL-column LIKE inside a
  `function_scalar_case`'s `arguments` (declines) and a DATE-column LIKE at the same position
  (rewritten in place, CASE structure preserved); plus one wired-chain test in `mod.rs`'s `mod tests`
  replicating the production chain over
  `predicate_equal(function_scalar_case(... predicate_like(<decimal col>) ...), 1)` asserting the
  whole filter is omitted.

### Group G
- [x] 8. Migrate `like_subject_type_guard` onto `rewrite_expr_tree`, reducing it to a per-node
  closure dispatching `predicate_like`/`predicate_like_regexp` to `guard_like_subject`, returning
  every other node unchanged. MUST verify and document two equivalences: (a) visiting a LIKE node's
  own `expression`/`pattern` children before dispatch is inert; (b) the old `predicate_not` arm's
  recursion into a non-object `expression` child is equivalent to the primitive skipping it. [expert]

### Group H
- [x] 9. Rewrite `like_subject_type_guard`'s ENTIRE traversal paragraph (actual location
  `support.rs:565-572`, shifted from the plan's stale `:500-507`) — enumeration, caveat, and closing
  sentence all false now. Replaced with shared-primitive reach (via `rewrite_expr_tree`), the closed
  `function_scalar_case`/comparison-predicate/nested-argument blind spot, and the decline trade in
  both sub-cases (resolved non-string type = fixed hard scan failure; unresolved name = possibly-lost
  working pushdown traded for correct native evaluation).
- [x] 10. Swept the remaining stale reach claims (line numbers re-verified against current file, all
  shifted from the plan's stale numbers): `support.rs:923` (`string_function_arg_type_guard` doc —
  dropped the contrast, kept WHY the wide field list matters); `support.rs:5619-5621` (test doc on
  `string_fn_guard_reaches_function_under_comparison_predicate` — rationale only, test body
  untouched); `mod.rs:948-953` (test doc on
  `where_filter_string_fn_under_comparison_predicate_coerced` — rationale only, body untouched);
  `mod.rs:188-209` chain comment (parenthetical contrast fixed). Confirmed moot per plan: the
  `support.rs:563-564` inline `_`-arm comment ("cannot nest one in this grammar") no longer exists —
  task 8 already deleted it along with the match arms it annotated. Confirmed no-edit-needed:
  `mod.rs:1013-1018` and `joins/rendering.rs` (checked current content — no reach claim; the
  `rendering.rs` grep hit is "conjunction", a substring false-positive, not "junction"). Ran
  `grep -rn "junction" crates/` before and after — remaining hits are either unrelated
  (`request_shape.rs`/`grouped_agg.rs` HAVING-junction rendering, `vs-expression`'s `render_junction`),
  false positives ("conjunction"), or accurately-framed historical text (`mod.rs:1049`,
  `support.rs:4222`, both explicitly say "pre-migration").

### Group I
- [x] 11. Ran the commit-2 gate: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, and
  `cargo test --features exasol-e2e --no-run`.

## Phase 4: Review Fixes
- [x] 4.1 In `support.rs`, rewrite `decimal_rewrite_passes_through_non_object_node`'s doc comment
  (lines 4464-4468) to state only what the test pins about current behavior — that a non-object
  node is returned unchanged, because `rewrite_expr_tree` finds no curated child on it and the
  always-`Some` closure's catch-all arm clones it. Do not change the test body or its assertion.
- [x] 4.2 In `support.rs`, delete the parenthetical "(issue tracked in the
  pushdown-expr-rewrite-primitive refactor)" from `decimal_rewrite_passes_through_non_object_node`'s
  doc comment at line 4468.
- [x] 4.3 In `support.rs`, correct `rewrite_expr_tree`'s curation rationale (lines 525-527) to name
  `dataType` as the object-valued non-expression field a blind `map.values()` walk would hand to
  `f`, and drop the `name`-sub-object claim (state instead that `name` carries an identifier string,
  so it is excluded to keep identifiers unrewritable). Apply the same correction to
  `expr_tree_recurses_only_into_curated_fields_of_the_expected_shape`'s doc comment (lines
  4364-4365), and remove the fabricated `"name": {"type": "not_an_expression"}` fixture line (4371)
  together with `"name"` in the `skipped` array (line 4397).
- [x] 4.4 In `support.rs`, trim `like_guard_decimal_inside_case_declines`'s doc comment (4218-4223)
  and `like_guard_date_inside_case_wraps_cast`'s (4251-4257) to state only the behavior each test
  pins under the current code — a `LIKE` at a non-junction position (inside a `function_scalar_case`'s
  `arguments`) is type-guarded like any other, declining for DECIMAL and rewrapping in
  `CAST(<col> AS VARCHAR)` for DATE with the enclosing CASE preserved — and delete the sentences
  describing the pre-migration `_`-arm behavior. Apply the same trim to
  `where_filter_like_decimal_inside_case_declines_whole_filter`'s doc comment at `mod.rs:1046-1052`.
  Keep the `#207 blind spot` framing and every test body and assertion unchanged.
- [x] 4.5 In `support.rs`, add a unit test `like_guard_varchar_inside_case_unchanged` next to
  `like_guard_date_inside_case_wraps_cast` that runs `like_subject_type_guard` over the same
  `function_scalar_case` fixture with a VARCHAR-typed subject column (`col_types` mapping the column
  to `VARCHAR(20)`) and asserts the returned tree equals the input tree exactly, so the widened
  traversal is pinned as a no-op wherever the subject type is already a string.
- [x] 4.6 In `support.rs`, replace the `for visited in ["expression", "results"]` loop at lines
  4385-4396 with two direct `assert_eq!` calls — one on `out["expression"]["visited"]` and one on
  `out["results"][0]["visited"]`, each keeping an assertion message naming the field.
- [x] 4.7 In `mod.rs`, rewrite the guard-chain comment at lines 188-210 so its first sentence states
  that `like_subject_type_guard` walks the whole curated expression tree through the shared
  post-order primitive — reaching a `LIKE` nested inside a `function_scalar_case`, under a
  comparison operand, or inside a scalar function's `arguments` — and that a decline anywhere there
  drops the entire filter to native Exasol evaluation. Then name that primitive explicitly where
  `string_function_arg_type_guard` is introduced instead of the antecedent-less "the same shared
  post-order traversal", and reflow the whole paragraph so line 194's orphan `// type: a bare`
  fragment is gone. Leave the `filter_json_raw`-stays-unmodified sentence at lines 207-210 unchanged.

## Phase 5: Verification
- [x] V1. Automated checks (build/test/lint/format/UDF build/e2e per plan Checklist) — all green
- [x] V2. Scenario coverage audit — all 19 named tests confirmed present and passing
- [x] V3. Manual verification steps — see verification-report.md Notes for 1 flagged plan-checklist
  wording inconsistency and 3 deployed-VS rows not independently re-run (covered by the E2E suite)
