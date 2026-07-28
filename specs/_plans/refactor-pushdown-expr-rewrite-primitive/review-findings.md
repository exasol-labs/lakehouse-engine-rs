# Code Review Findings: refactor-pushdown-expr-rewrite-primitive

## Summary
- Files reviewed: 2
- Total findings: 7 (standard: 7, expert: 0)

## Verified (no fix needed — do not create tasks from this section)

- `rewrite_expr_tree`'s doc comment (`support.rs:496-533`) states all four plan-required contracts: post-order, decline-propagation-to-root, always-`Some` composition for the infallible walker, and why the field list is curated.
- No panic path was introduced. `rewrite_decimal_stringifications`'s closure is statically always-`Some` (every arm returns `Some`, no `?` inside), and `.unwrap_or_else(|| node.clone())` cannot panic. `out[field] = …` and `out["arguments"][i] = …` are each reached only under a guard that proves the container shape, exactly as before.
- Commit-1 byte-identity holds. No `-` line in either file's diff touches an `assert`; the five named SQL-asserting tests in `mod.rs` are unedited and pass. Leaf-equivalence re-derived: for a non-object node `Value::get(&str)` returns `None`, so the primitive descends nowhere and hands the node to `f`, whose catch-all clones it — identical to both deleted `!node.is_object()` early returns. Evidence: `cargo test -p lakehouse-engine --lib adapter::pushdown` → 391 passed, 0 failed; `cargo clippy --all-targets` clean; `cargo fmt --check` clean.
- No live doc comment claims `like_subject_type_guard` has junction-only reach. `grep -rn "junction" crates/` returns only correctly-scoped historical references (see the finding below on how those are phrased) and unrelated `predicate_and`/`predicate_or` rendering code.
- Commit-2 migration equivalence re-derived and sound: the old `predicate_not` arm's unconditional recursion into a non-object `expression` reassigned the same value, and post-order descent into a `LIKE` node's own `expression`/`pattern` is inert because the closure is identity on every non-`LIKE` node. The synthesized DATE `function_scalar_cast` is created after the descent, so it cannot be double-wrapped.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [OUTDATED_COMMENT] Leaf-characterization test's doc describes code deleted by task 4
- Location: lines 4464-4468
- Issue: `decimal_rewrite_passes_through_non_object_node`'s doc comment says it "characterizes the leaf early-return this function shares with the string guard, ahead of the migration that folds both onto a shared recursion primitive". Task 4 deleted that early return from `rewrite_decimal_stringifications` and task 3 deleted it from `string_function_arg_type_guard`, and the migration has landed — so both the present-tense claim and "ahead of the migration" are false against the code the test now runs on.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, rewrite `decimal_rewrite_passes_through_non_object_node`'s doc comment (lines 4464-4468) to state only what the test pins about current behavior — that a non-object node is returned unchanged, because `rewrite_expr_tree` finds no curated child on it and the always-`Some` closure's catch-all arm clones it. Do not change the test body or its assertion.

#### [WORK_TRACKING_COMMENT] Doc comment references the plan directory as a tracking pointer
- Location: line 4468
- Issue: the same doc comment ends "(issue tracked in the pushdown-expr-rewrite-primitive refactor)". Work-tracking references are banned in comments, and this one points at a `specs/_plans/` directory that `/speq:record` archives, so the pointer dies on merge.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, delete the parenthetical "(issue tracked in the pushdown-expr-rewrite-primitive refactor)" from `decimal_rewrite_passes_through_non_object_node`'s doc comment at line 4468.

#### [OUTDATED_COMMENT] Curated-field rationale cites an object-valued `name` the grammar never sends
- Location: lines 525-527 (and lines 4364-4365, 4371, 4397)
- Issue: `rewrite_expr_tree`'s "Why the field list is curated" section justifies curation with "A blind walk would descend into a node's `dataType` and `name` sub-objects … letting a guard rewrite a declared type or an identifier". Only the `dataType` half is real: `dataType` is object-valued (`{"type":"VARCHAR"}`), but `name` is always a bare string in this grammar (`"name": "CASE"`, `"name": "c_decimal_a"`). Evidence: `grep -rn '"name": *{' crates/` returns exactly one hit repo-wide — line 4371, this change's own test fixture. The primitive's only curation test therefore fabricates a node shape the adapter can never receive, and the doc's stated rationale is half fiction on the very point the plan required it to explain.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, correct `rewrite_expr_tree`'s curation rationale (lines 525-527) to name `dataType` as the object-valued non-expression field a blind `map.values()` walk would hand to `f`, and drop the `name`-sub-object claim (state instead that `name` carries an identifier string, so it is excluded to keep identifiers unrewritable). Apply the same correction to `expr_tree_recurses_only_into_curated_fields_of_the_expected_shape`'s doc comment (lines 4364-4365), and remove the fabricated `"name": {"type": "not_an_expression"}` fixture line (4371) together with `"name"` in the `skipped` array (line 4397).

#### [OUTDATED_COMMENT] Three new test docs narrate the deleted junction-only traversal as if it still exists
- Location: lines 4218-4223 and 4251-4257 (plus `mod.rs:1046-1052`)
- Issue: `like_guard_decimal_inside_case_declines`, `like_guard_date_inside_case_wraps_cast`, and `mod.rs`'s `where_filter_like_decimal_inside_case_declines_whole_filter` each carry two to four lines re-telling the traversal commit 2 deleted, in present tense about it ("so `function_scalar_case` falls to the `_` arm and is returned unchanged — this assertion is false under that code"). There is no `_` arm any more. This is RED-phase rationale frozen into permanent documentation: it describes a code shape a future reader cannot find, and it is what keeps `grep -rn "junction" crates/` returning reach claims attached to `like_subject_type_guard` after the plan's sweep was meant to clear them.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, trim `like_guard_decimal_inside_case_declines`'s doc comment (4218-4223) and `like_guard_date_inside_case_wraps_cast`'s (4251-4257) to state only the behavior each test pins under the current code — a `LIKE` at a non-junction position (inside a `function_scalar_case`'s `arguments`) is type-guarded like any other, declining for DECIMAL and rewrapping in `CAST(<col> AS VARCHAR)` for DATE with the enclosing CASE preserved — and delete the sentences describing the pre-migration `_`-arm behavior. Apply the same trim to `where_filter_like_decimal_inside_case_declines_whole_filter`'s doc comment at crates/lakehouse-engine/src/adapter/pushdown/mod.rs:1046-1052. Keep the `#207 blind spot` framing and every test body and assertion unchanged.

#### [MISSING_BOUNDARY_TEST] The widened LIKE reach has no test pinning it as a no-op for a string subject
- Location: lines 4225-4304 (`like_guard_decimal_inside_case_declines`, `like_guard_date_inside_case_wraps_cast`)
- Issue: both new commit-2 tests exercise only the directions the traversal widening *changes* — DECIMAL declines, DATE rewraps. Nothing pins the case that covers nearly every real query: a VARCHAR-subject `LIKE` at a newly-reached position must still come back byte-identical, i.e. the widening must not cost a working pushdown. `like_guard_varchar_subject_unchanged` (line 3999) only covers a bare top-level `LIKE`, so a future tightening of the closure's catch-all or of `guard_like_subject`'s VARCHAR arm would silently stop pushing down every CASE-nested `LIKE` with no failing test — the exact regression the plan's Impact section accepts as the widening's only cost.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, add a unit test `like_guard_varchar_inside_case_unchanged` next to `like_guard_date_inside_case_wraps_cast` that runs `like_subject_type_guard` over the same `function_scalar_case` fixture with a VARCHAR-typed subject column (`col_types` mapping the column to `VARCHAR(20)`) and asserts the returned tree equals the input tree exactly, so the widened traversal is pinned as a no-op wherever the subject type is already a string.

#### [SHRINKABLE] Curated-field test loops over two elements with a shape-discriminating branch
- Location: lines 4385-4396
- Issue: `expr_tree_recurses_only_into_curated_fields_of_the_expected_shape` loops over `["expression", "results"]` and then re-tests `visited == "results"` inside the body to pick between `&out[visited][0]` and `&out[visited]`. A two-iteration loop whose body branches on which iteration it is on is longer and harder to read than the two direct assertions it stands in for.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, replace the `for visited in ["expression", "results"]` loop at lines 4385-4396 with two direct `assert_eq!` calls — one on `out["expression"]["visited"]` and one on `out["results"][0]["visited"]`, each keeping an assertion message naming the field.

### crates/lakehouse-engine/src/adapter/pushdown/mod.rs

#### [OUTDATED_COMMENT] Guard-chain comment never states the LIKE guard's new reach, and its reflow is broken
- Location: lines 188-210 (broken line at 194)
- Issue: two defects in the chain comment above the `filter` pipeline. (a) The sweep required by the plan removed the old narrow-LIKE-guard contrast but did not add the replacement fact: the paragraph still introduces `like_subject_type_guard` as merely "may decline … or rewrap a DATE subject" and never says its reach is now the whole curated tree, so a reader debugging "my whole WHERE clause disappeared" gets no hint that a `LIKE` nested inside a CASE or under a comparison operand can now drop the entire filter. The phrase "via the same shared post-order traversal" at line 191 also has no antecedent — nothing earlier in the paragraph mentions a traversal. (b) The edit did not reflow, leaving line 194 as the orphan fragment `// type: a bare`; `cargo fmt` does not reflow comments, so it will stay.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/mod.rs, rewrite the guard-chain comment at lines 188-210 so its first sentence states that `like_subject_type_guard` walks the whole curated expression tree through the shared post-order primitive — reaching a `LIKE` nested inside a `function_scalar_case`, under a comparison operand, or inside a scalar function's `arguments` — and that a decline anywhere there drops the entire filter to native Exasol evaluation. Then name that primitive explicitly where `string_function_arg_type_guard` is introduced instead of the antecedent-less "the same shared post-order traversal", and reflow the whole paragraph so line 194's orphan `// type: a bare` fragment is gone. Leave the `filter_json_raw`-stays-unmodified sentence at lines 207-210 unchanged.

## Expert fixes
[none]
