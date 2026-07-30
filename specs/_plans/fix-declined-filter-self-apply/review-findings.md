# Code Review Findings: fix-declined-filter-self-apply

## Summary
- Files reviewed: 19 (13 modified tracked + 6 new golden SQL fixtures)
- Total findings: 10 (standard: 8, expert: 2)

Verified clean, no findings raised: the three-outcome gate at both sites correctly keys its
error branch on the NON-suppressing `render_expression_qualified` (`joins/sql_builders.rs:427`
and `:899`), never on `render_df_filter_qualified` alone; the call-site census for
`qualified_single_table_fallback_pushdown` (5 sites) and
`build_qualified_single_table_fallback_sql` (7 sites) is complete with no site missed;
`plan_join`'s Iceberg pruning input (`joins/mod.rs:124`) still receives the RAW filter;
`referenced_side_columns` still walks the full `pushdown_req`, so a declined conjunct's columns
remain in each leg's projection and are in scope for the outer `WHERE`;
`n_scan_join_select_items`' `_` arm covers the empty-array `selectList` shape, so `SELECT *`
cannot emit an empty select list. `cargo clippy --all-targets` is clean and
`cargo test -p lakehouse-engine --lib` / `-p vs-expression` pass (667 + 121).

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [CONTEXTLESS_ERROR] The both-dialects-unrenderable refusal identifies no predicate
- Location: lines 429-432 and 900-904
- Issue: both new refusals say only "a residual WHERE conjunct / a declined WHERE predicate could be rendered by neither dialect, so it could be applied nowhere" — the input that failed is never named. For a multi-conjunct WHERE the operator cannot tell which predicate killed the query. The plan's § Impact and § Manual Testing both promise "a clean adapter error **naming the unrenderable predicate**"; that half is unimplemented. (Separately, the single-table route reports the shared `join_render_decline` prefix "join pushdown declined:" for a query with no join — pre-existing wording shared by six sites and asserted verbatim by seven tests; do NOT churn `join_render_decline` or those assertions.)
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, append the offending predicate to both refusal messages by formatting `serde_json::to_string(tree).unwrap_or_default()` into the `join_render_decline` clause string at line 429 and at line 900 (e.g. `format!("a declined WHERE predicate could be rendered by neither dialect, so it could be applied nowhere: {tree_json}")`). Leave `join_render_decline`'s own template and every existing assertion on its prefix unchanged. Extend the existing unit test `single_table_wrapper_errors_when_declined_predicate_renders_in_neither_dialect` (line ~2838) to additionally assert the message contains the fixture node type `no_such_node_type_in_either_dialect`.

### crates/lakehouse-engine/tests/e2e_capability_test.rs

#### [UNTESTED_ERROR_PATH] The terminal-case e2e passes on any error mentioning HASHTYPE
- Location: lines 3966-3970
- Issue: the assertion is a three-way OR whose first alternative, `msg.to_ascii_uppercase().contains("HASHTYPE")`, is satisfied by any Exasol-side parser or engine error that echoes the query text — so the test cannot fail if the adapter's both-dialects-decline route is never reached at all. Combined with the only other assertion being `status == "error"`, the sole live proof of the plan's terminal arm proves nothing about the adapter.
- Fix: In crates/lakehouse-engine/tests/e2e_capability_test.rs, tighten `e2e_both_dialects_unrenderable_predicate_errors_without_rows` to a single assertion requiring the adapter's own phrase — `msg.to_ascii_lowercase().contains("neither dialect")` — and delete the `HASHTYPE` and `applied nowhere` alternatives. Run the test against the manually started `docker compose` Exasol stack to confirm the phrase survives Exasol's error wrapping; if it does not, replace it with the exact observed adapter substring rather than re-loosening to the query text.

### crates/lakehouse-engine/tests/e2e_join_test.rs

#### [MISSING_BOUNDARY_TEST] Both join-site e2e cases use an always-true declined predicate
- Location: lines 922 and 1019 (the `SECOND(o.O_ORDERDATE, 3) = 0` conjunct), tests at lines 974 and 1085
- Issue: the declined conjunct is true for every seeded row (`O_ORDERDATE` is a plain DATE, so `SECOND(..., 3)` is always 0 — the module comment says so), so no row-level assertion can distinguish "self-applied" from "silently dropped". `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters` asserts only the plan shape plus the UNFILTERED row set: a build that fell back to N-scan and then dropped the conjunct passes it, despite `and_filters` in the name. The N-scan case is saved only by the `pushed.contains("SECOND(")` EXPLAIN VIRTUAL substring; neither join site has row-level evidence that a declined conjunct excludes anything.
- Fix: In crates/lakehouse-engine/tests/e2e_join_test.rs, add a discriminating test `e2e_broadcast_declined_filter_excludes_rows` that runs the same two-table below-threshold join as `broadcast_declined_filter_join_query` but with the declined conjunct `SECOND(o.O_ORDERDATE, 3) = 1` (false for every seeded DATE row), asserting `conn.query_columns(...)` yields zero rows and that `explain_virtual_sql` shows `has_n_scan_wrapper(&pushed, 2)`. Also add `assert!(pushed.contains("SECOND("), ...)` to `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters` so the fallback's own wrapper WHERE is pinned there too.

### crates/vs-expression/src/lib.rs

#### [OUTDATED_COMMENT] `cast_to_unsupported_target_falls_back` still asserts the disproven Exasol backstop
- Location: lines 2243-2248
- Issue: the comment reads "The translator must decline (Err in raising mode, None in safe mode) so the adapter omits the CAST and Exasol evaluates it as a correctness backstop." This is the exact belief the plan disproved, on the exact predicate family (`FN_CAST` to INTERVAL/GEOMETRY/HASHTYPE/TIMESTAMP WITH LOCAL TIME ZONE) that the plan now makes a hard error. Task 2.11's census covered this file's `render_df_filter_safe`/`render_df_filter_exasol_safe` doc comments but missed this test comment, so the corrected library still re-seeds the defect. The test name's "falls_back" describes the same disproven adapter behavior.
- Fix: In crates/vs-expression/src/lib.rs, rewrite the comment at lines 2243-2248 to state that the translator declines these targets and that a `None`/`Err` here means the caller must decide what to do — there is no Exasol-side re-check of an advertised capability, so the adapter's declined-predicate route errors rather than omitting. Rename the test `cast_to_unsupported_target_falls_back` to `cast_to_unsupported_target_declines` and update any reference to that name in this file.

#### [OUTDATED_COMMENT] `exasol_df_filter_suppresses_trivially_true` states Exasol applies the omitted filter
- Location: lines 4983-4988
- Issue: "so the adapter omits it from the scan spec and leaves Exasol to apply it as a correctness backstop, regardless of which dialect rendered the fragment." Omitting a trivially-true filter is correct, but the stated REASON is the disproven backstop, and this comment sits directly beside the two `None`-returning renderers whose doc comments task 2.11 corrected — leaving the contradiction inside one file.
- Fix: In crates/vs-expression/src/lib.rs, rewrite the comment at lines 4983-4988 to state that a trivially-true filter is a correct no-op to omit, and that this is one of two distinguishable causes of `None` — the other, a genuine decline, must be self-applied by the caller because Exasol never re-applies an advertised capability. Do not mention a correctness backstop.

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [OUTDATED_COMMENT] `pushdown_translates_or_omits_predicate` still teaches the omit-is-safe contract
- Location: lines 2150-2151 (scenario header), 2178, 2186
- Issue: the header reads "Filter predicate is pushed into the scan spec (translatable) or omitted (untranslatable) — never mistranslated"; line 2178 reads "render_df_filter_safe returns None → omitted from spec."; line 2186 reads "Confirm omitting the filter still produces valid SQL (correctness backstop)." The plan lists this very scenario as CHANGED — an untranslatable predicate is no longer omitted, it is self-applied by the wrapper — yet the prose in a file this plan changed still states the pre-fix contract as the invariant.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, rewrite the scenario header at lines 2150-2151 to "Filter predicate is pushed into the scan spec (translatable) or kept out of it (untranslatable) — never mistranslated, never omitted from the query", rewrite line 2178 to state that `render_df_filter_safe` returning `None` keeps the predicate out of the SCAN SPEC only, and rewrite line 2186 to state that the scan SQL stays valid without a scan-spec filter while the adapter applies the predicate itself, cross-referencing `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper`. Drop the phrase "correctness backstop" from both. Change no assertions.

#### [MISSING_BOUNDARY_TEST] The `strip_table_alias` invariance test proves only the declining direction
- Location: lines 1659-1680
- Issue: the single fixture declines under both dialects, so the `assert_eq!` compares `false` with `false` — the test passes for any implementation that answers `false` for that tree, and there is no case where the answer is `true`. The safety-critical direction is the untested one: `build_side_fan_out_sql` (`joins/sql_builders.rs:594-596`) strips the alias and re-renders with `render_df_filter_safe` AFTER `renderable_only` screened the un-stripped tree, so a conjunct whose answer flipped from `true` to `false` under stripping would be silently dropped from the leg — the exact defect this plan fixes.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, extend `datafusion_renderable_answer_unchanged_by_strip_table_alias` with a second fixture that RENDERS and carries `tableAlias` (e.g. `{"type":"predicate_greater","left":{"type":"column","name":"TS","tableName":"T","tableAlias":"O"},"right":{"type":"literal_exactnumeric","value":1}}`), asserting `datafusion_renderable` is `true` both before and after `strip_table_alias` and that the two answers are equal.

### crates/lakehouse-engine/src/adapter/capabilities.rs

#### [OUTDATED_COMMENT] The FN_CAST comment narrates the change and cites a test name
- Location: lines 61-64
- Issue: "a CAST unrenderable under both dialects (…) **now** errors rather than silently falling back — see `e2e_both_dialects_unrenderable_predicate_errors_without_rows`." "now … rather than" is change narrative that reads as stale the moment the change is history — the same defect the plan's own task 2 forbade for the `CLAUDE.md` fact ("a plain general fact — no discovery narrative"). The test-name reference also rots silently if the test is renamed, and this review already instructs tightening that test.
- Fix: In crates/lakehouse-engine/src/adapter/capabilities.rs, replace lines 61-64 with a present-tense statement of behavior only — CAST is advertised over its faithful target set (VARCHAR/CHAR/DECIMAL(p,s)/DOUBLE/BOOLEAN/DATE/TIMESTAMP); a CAST target renderable under neither dialect (INTERVAL/GEOMETRY/HASHTYPE/TIMESTAMP WITH LOCAL TIME ZONE) fails the query, because an advertised capability is never re-applied by Exasol. Remove the `now … rather than` framing and the test-name reference.

### crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs

#### [OUTDATED_COMMENT] The new cross-site fixtures claim to prove nothing changed
- Location: lines 656-658 ("Proves tasks 2.1-2.5 changed nothing …") and lines 712-717 ("must ALSO emit byte-identical SQL … Proves the single-table path stays on its wrapper-free fast scan")
- Issue: all six fixture files under `testdata/dispatch_golden/` are new and were captured from POST-change code, so neither test can establish that anything was unchanged — they are forward-looking baselines, not before/after comparisons. The actual no-change evidence is that the ten pre-existing fixtures are byte-identical under `git diff`. Leaving the stronger claim in place invites a future reader to treat these two tests as the proof and skip that check.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs, reword both doc comments to state what the tests actually do — pin the emitted SQL at all three render sites for a filterless request and for a request whose filter renders, so a future change to the decline routing cannot alter either case — and delete the "Proves tasks 2.1-2.5 changed nothing" and "must ALSO emit byte-identical SQL" claims. Note in each comment that the no-regression evidence for the pre-fix behavior is the unchanged ten pre-existing fixtures.

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [INFORMATION_LEAKAGE] The three-outcome self-apply gate is duplicated at both render sites
- Location: lines 419-435 (`build_n_scan_join_sql`) and lines 888-906 (`build_qualified_single_table_fallback_sql`)
- Issue: one design decision — render through `render_df_filter_qualified`; a `None` while the NON-suppressing `render_expression_qualified` still returns `Some` means trivially true and emits no clause; `None` from both is a hard error — exists as two independent copies of the same `match`, each with its own near-identical six-line comment. No module owns the rule. The failure mode of the two drifting apart is precisely this plan's own defect: a site that gated on `render_df_filter_qualified` alone would hard-fail a correct no-op predicate, and a site that treated both `None`s as "no clause" would silently return unfiltered rows. Neither is caught by a compiler and only one of the two sites has a trivially-true unit test each. The plan's decision log rejected a three-way outcome ENUM in `crates/vs-expression`' public API — that rejection does not cover a private adapter-internal helper replacing duplicated logic inside one file.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, add one private helper beside `join_render_decline` — `fn render_self_applied_where(tree: &Json, alias_of: &HashMap<String, String>, subject: &str) -> Result<Option<String>, UdfError>` — that returns `Ok(Some(sql))` from `render_df_filter_qualified`, `Ok(None)` when that is `None` but `render_expression_qualified` over the same tree is `Some`, and `Err(join_render_decline(...))` built from `subject` otherwise. Give it a doc comment stating why the error is keyed on the non-suppressing renderer. Replace both `match` blocks with a call to it, passing "a residual WHERE conjunct" and "a declined WHERE predicate" respectively, and delete both inline comment copies. Keep both existing error message texts byte-identical so `trivially_true_residual_emits_no_outer_where_and_does_not_error` and `single_table_wrapper_errors_when_declined_predicate_renders_in_neither_dialect` still pass unchanged; run `cargo test -p lakehouse-engine --lib` and confirm the six `testdata/dispatch_golden/*.sql` fixtures are byte-identical afterwards.

#### [DEAD_FLEXIBILITY] `projection_override` is derivable inside the callee from parameters it already has
- Location: `qualified_single_table_fallback_pushdown` signature line 954 and body lines 956-957; sole non-`None` caller `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` lines 378-396
- Issue: `projection_override: Option<(Vec<ProjectionItem>, Vec<String>)>` is `Some` at exactly one of five call sites, is `Some` if and only if `declined_filter` is `Some`, and its value is built at that caller purely by mapping over `col_types` — a parameter the callee already receives and already passes to `referenced_column_projection`. The parameter therefore carries no information the callee lacks; it only adds a tenth parameter plus a second doc-only invariant between two parameters (the first being "`filter` MUST be `None` alongside `declined_filter`"), both of which a caller can violate silently — a wrong `projection_override` produces the `04000` positional mismatch the plan set out to prevent, and a non-`None` `filter` double-applies the predicate. It also puts a low-level projection-construction loop in the middle of `build_dispatch_sql`'s high-level routing.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, delete the `projection_override` parameter from `qualified_single_table_fallback_pushdown` and derive the projection inside it instead: when `declined_filter.is_some()`, build `(col_types.iter().map(|(n, _)| ProjectionItem::Column(n.clone())).collect(), col_types.iter().map(|(_, t)| t.clone()).collect())`; otherwise keep `referenced_column_projection(pushdown_req, col_types)`. Fold the parameter's doc paragraph (lines 933-943) into the `declined_filter` paragraph, stating that the decline route projects the full base row because the referenced-column narrowing would emit only the filter's columns for a genuine `SELECT *`. In crates/lakehouse-engine/src/adapter/pushdown/mod.rs delete the `full_base_row` construction and the `Some(full_base_row)` argument at lines 378-396 (keep the surrounding rationale comment, trimmed to the routing reason), and drop the now-removed trailing `None` argument at the four remaining call sites (mod.rs lines 501, 573, 618 and the test at joins/sql_builders.rs line ~2664). Confirm `cargo test -p lakehouse-engine --lib` passes and the six `testdata/dispatch_golden/*.sql` fixtures plus `declined_filter_with_absent_select_list_projects_full_row` are unchanged.
