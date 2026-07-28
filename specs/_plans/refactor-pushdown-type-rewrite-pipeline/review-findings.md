# Code Review Findings: refactor-pushdown-type-rewrite-pipeline

## Summary
- Files reviewed: 2
- Total findings: 5 (standard: 5, expert: 0)

Byte-identity constraint verified as HELD — no correctness finding.
Evidence gathered during review:

| Check | Command | Result |
|---|---|---|
| Unit tests | `cargo test -p lakehouse-engine adapter::pushdown` | 395 passed, 0 failed |
| Lint | `cargo clippy --all-targets` | 0 warnings, 0 errors |
| Format | `cargo fmt --check` | clean |
| Feature-gated census | `cargo test --features exasol-e2e --no-run` | exit 0 |
| Rustdoc narrowing gate | `cargo doc --no-deps -p lakehouse-engine` | 32 warnings vs. 33 on the pre-change tree — no new private-intra-doc-link warning; the only `pushdown/` warning (`support.rs:66`) is pre-existing |
| Residual external callers | `git grep` for the three narrowed passes outside `support.rs` | prose mentions only (`joins/rendering.rs:507`, `mod.rs:519/844-854/899/926-927/987-990`, `tests/e2e_capability_test.rs:2085/2139/2311`), all plain backticks, zero calls, zero imports |

Semantic equivalence confirmed at both rewired sites: `apply_filter_type_rewrites` reproduces
`like → string_fn → decimal` with `?` for the two `Option` passes and `Some(..)` for the infallible
one; in `project_columns` the collapsed `let…else` is reachable only from the `string_fn` decline
because `rewrite_decimal_stringifications` has no decline path, and `declared_type` is still computed
before the guard, so no statement was reordered. No test assertion or expected value was edited.

All five findings are comment/doc-quality defects. None changes behavior; none is cross-file or
subtly correctness-bearing, so the Expert section is empty.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [REDUNDANT_COMMENT] Pipeline doc restates each pass's own doc instead of owning only the order
- Location: lines 1026-1042 (`apply_filter_type_rewrites` doc, first paragraph)
- Issue: the first paragraph of `apply_filter_type_rewrites`' doc is a near-verbatim relocation of the deleted `mod.rs:188-211` comment, and it re-describes what each of the three passes does — `like_subject_type_guard`'s traversal reach, its decline-and-DATE-rewrap semantics, `string_function_arg_type_guard`'s per-type dispatch (DECIMAL → `decimal_to_varchar_exasol`, DATE → `CAST`, BOOLEAN/DOUBLE/TIMESTAMP → decline), and `rewrite_decimal_stringifications`' trailing-zero-trim rationale. Every one of those facts already has an owner in the same file: `like_subject_type_guard`'s doc (lines 556-633), `string_function_arg_type_guard`'s doc (lines 919-946), and `rewrite_decimal_stringifications`' doc (lines 689-744), each at greater length. The plan's stated goal was one owner per decision; as written, a change to any pass's per-node behavior now requires editing that pass's doc AND this pipeline doc. The pipeline's own content — the pass list, the load-bearing precedence, the fallibility bridge — is paragraphs 2 and 3 (lines 1044-1053) and is not affected.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, replace `apply_filter_type_rewrites`' doc paragraph at lines 1026-1042 with a compact per-pass line naming what each pass contributes to the sequence and linking to it for the detail — one line each for `[\`like_subject_type_guard\`]` (may decline the whole filter, or rewrap a DATE subject), `[\`string_function_arg_type_guard\`]` (coerces string-position arguments, or declines), and `[\`rewrite_decimal_stringifications\`]` (runs last, never declines) — dropping the restated traversal-reach, per-type dispatch, and trim-form prose. Keep the issue references (#207, #210, #211) on those lines. Leave the ordering paragraph (lines 1044-1047) and the fallibility-bridge paragraph (lines 1049-1053) and the `Returns:` block unchanged.

#### [OUTDATED_COMMENT] `rewrite_decimal_stringifications` doc claims the two pipelines are its only callers
- Location: line 736
- Issue: the added clause "those two pipeline functions are this pass's only callers" is false. `rewrite_decimal_stringifications` is called directly from `support.rs`'s `mod tests` at lines 4686, 4699, 4724, 4754, 4802, 4824, 4844, 4855, 4876, 4887, 4906, 4913 and 4945 — thirteen call sites the claim excludes. The claim is only true of production code, and the sentence does not say so.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs line 736, change "those two pipeline functions are this pass's only callers" to state that those two pipeline functions are this pass's only PRODUCTION callers (the pass corpus in `mod tests` calls it directly).

#### [REDUNDANT_COMMENT] `project_columns` call-site comment restates the call on the next line
- Location: lines 1206-1207
- Issue: the comment opens with "Run the ordered type-rewrite pass sequence on this select-list item (`apply_select_item_type_rewrites`)" immediately above `let Some(e) = apply_select_item_type_rewrites(e, &all_cols)`. It names the function being called and paraphrases its own doc's first line, adding nothing the code does not already say. The plan scoped this comment to "what the caller owns"; the caller-owned facts are the remaining sentences (decline → full base row, and the three-caller reach into the broadcast-join SELECT list).
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, delete the first sentence of the comment at lines 1206-1207 ("Run the ordered type-rewrite pass sequence on this select-list item (`apply_select_item_type_rewrites`).") so the comment begins at "On `None` the item can't be safely pushed down at all; …", and keep the rest of the comment (through line 1214) unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/mod.rs

#### [REDUNDANT_COMMENT] Test inline comment duplicates its own doc comment three lines above
- Location: lines 871-873
- Issue: `where_filter_decimal_stringification_rewritten_to_trim` now carries the same statement twice — the doc comment at lines 855-857 ("Calls the same pipeline function `handle_pushdown` calls (`apply_filter_type_rewrites`, then `render_df_filter_safe`) on the DataFusion-bound filter tree") and the inline comment at lines 871-873 ("Calls the same pipeline function `handle_pushdown` (mod.rs) calls: the raw filter runs the ordered type-rewrite pass sequence, then is rendered for the DataFusion scan"). The other five rewired chain tests (`filter_decimal_comparison_not_rewritten`, `where_filter_string_fn_under_comparison_predicate_coerced`, `where_filter_string_fn_over_double_declines`, `where_filter_upper_decimal_inside_like_subject_coerced`, `where_filter_like_decimal_inside_case_declines_whole_filter`) carry the statement in the doc comment only, so this test is also inconsistent with its five siblings.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/mod.rs, delete the three-line inline comment at lines 871-873 inside `where_filter_decimal_stringification_rewritten_to_trim`, leaving the doc comment at lines 855-857 as the single statement and the `let rendered = Some(&filter_json)` chain untouched.

#### [REDUNDANT_COMMENT] `handle_pushdown` call-site comment restates the call on the next line
- Location: lines 188-189
- Issue: the comment opens with "`apply_filter_type_rewrites` runs the ordered type-rewrite pass sequence on the RAW filter JSON before rendering", directly above `.and_then(|f| apply_filter_type_rewrites(f, &col_types)).and_then(|f| render_df_filter_safe(&f))` — it names the two functions the reader can see and restates the pipeline's own doc. The plan scoped this comment to a short form of the caller-owned fact only (the raw tree stays unmodified for `resolve_file_list`), which is the remaining sentence. "This chain" in the second sentence also no longer has an antecedent now that the inline three-pass chain is gone.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/mod.rs, delete the first sentence of the comment at lines 188-189 and reword the surviving sentence so it stands alone without the dangling "This chain" antecedent — state that the rewritten filter feeds ONLY the DataFusion-bound scan filter and that `filter_json_raw` itself is left completely unmodified for the later `resolve_file_list` Iceberg-level pruning call below, which must see the original, un-rewritten predicate tree.

## Expert fixes
[none]
