# Code Review Findings: fix-regexp-like-pushdown

## Summary
- Files reviewed: 9
- Total findings: 7 (standard: 7, expert: 0)

The production change is correct and complete. `cargo test -p vs-expression` passes (143 tests), the touched `lakehouse-engine` unit tests pass (`capabilities`, `like_guard`, `selectlist_`, `filter_in_common_arg`, `declined_like`, and `capabilities_doc_lists_exactly_the_advertised_set`), `cargo clippy -p vs-expression -p lakehouse-engine --all-targets` reports no warnings, and `cargo fmt --all --check` reports no changes. No production code outside `capabilities.rs` still names `REGEXP_LIKE` or `predicate_like_regexp`. Every `/// Scenario:` line added by this change quotes a spec scenario title verbatim. All seven findings are comments that the removal left stale.

## Standard fixes

### crates/vs-expression/src/lib.rs

#### [OUTDATED_COMMENT] Dialect doc comment still counts the removed REGEXP_LIKE row
- Location: lines 32 and 38 (doc comment on `enum Dialect`); line 96 (doc comment on `ExasolForm::Shaped`)
- Issue: `TRANSLATED_SCALAR_FNS` now holds 75 rows, 9 of them `Shaped` (counted with awk over the declaration; HEAD held 76 and 10). The doc comment on `Dialect` still says "reads (76 names)" on line 32 and "Ten names have an Exasol form" on line 38. The same edit changed "six groups" to "five groups" two lines later but left both counts. The rewrite of the `ExasolForm::Shaped` doc also left line 96 at about 130 columns, while the surrounding doc lines wrap near 90.
- Fix: In crates/vs-expression/src/lib.rs, in the doc comment on `enum Dialect`, change "reads (76 names)" to "reads (75 names)" and "Ten names have an Exasol form" to "Nine names have an Exasol form". In the doc comment on `ExasolForm::Shaped`, rewrap the paragraph so that no line exceeds 90 columns, without changing its words.

### crates/vs-expression/src/lib_tests.rs

#### [OUTDATED_COMMENT] Sweep test comments count ten Shaped names and five node types
- Location: lines 3393-3394 (doc comment on `exasol_dialect_renders_declared_verbatim_surface`), line 3512, line 3666
- Issue: The change removed the `shaped("REGEXP_LIKE", ...)` fixture and the `predicate_like_regexp` node-type row. The sweep now has 9 `shaped(` fixtures and 4 node-type rows (`function_scalar_extract`, `function_scalar_cast`, `literal_timestamp`, `literal_timestamp_utc`). The doc comment still says "Only the ten `Shaped` names and the five dialect-branching node types", line 3512 says "The ten Shaped names", and line 3666 says "The five dialect-branching node types outside `function_scalar`".
- Fix: In crates/vs-expression/src/lib_tests.rs, in the doc comment on `exasol_dialect_renders_declared_verbatim_surface`, change "Only the ten `Shaped` names and the five" to "Only the nine `Shaped` names and the four". In the same test body, change the comment "The ten Shaped names, each declaring" to "The nine Shaped names, each declaring", and change "The five dialect-branching node types outside `function_scalar`" to "The four dialect-branching node types outside `function_scalar`".

#### [OUTDATED_COMMENT] Dialect-invariant section comment narrates a past plan and omits CAST
- Location: lines 3748-3749 (section comment above `arithmetic_operators_render_identically_in_both_dialects`)
- Issue: The edited comment reads "This plan branches `function_scalar_extract` and the two timestamp-literal node types on `dialect`". "This plan" refers to an earlier, recorded plan, which AGENTS.md forbids ("Never ... narrate history"). The list is also incomplete: `function_scalar_cast` branches on dialect through `render_cast_target`, as the `Dialect` doc comment in lib.rs states.
- Fix: In crates/vs-expression/src/lib_tests.rs, replace the two lines "This plan branches `function_scalar_extract` and the two timestamp-literal / node types on `dialect`. Everything else was not meant to move." with "Only `function_scalar_extract`, `function_scalar_cast`, and the timestamp-literal node types branch on `dialect`; everything else must render identically in both dialects." Leave the rest of the section comment unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [OUTDATED_COMMENT] LIKE guard doc still says the closure acts on two LIKE node types
- Location: line 696 (doc comment on `like_subject_type_guard`, section "Descending into a `LIKE` node's own children is inert"); lines 638-641, 658-659, 672-673, and 753-754
- Issue: `like_subject_type_guard` now matches `Some("predicate_like")` only, but line 696 still says "the per-node closure acts on the two `LIKE` node types and clones everything else". The removal of `predicate_like_regexp` also left the edited doc lines unevenly wrapped: lines 638, 641, 658, and 672 end early, and line 753 on `guard_like_subject` runs to about 120 columns.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, in the doc comment on `like_subject_type_guard`, change "acts on the two `LIKE` node types and clones everything else" to "acts on `predicate_like` nodes and clones everything else". Rewrap the paragraphs that contain lines 638-641, 658-659, and 672-673 to the 90-column width of the surrounding doc lines. Rewrap the doc comment on `guard_like_subject` to two lines of at most 90 columns. Do not change any other wording.

### crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs

#### [OUTDATED_COMMENT] CHAR guard test cites a line number that now points at strip_table_alias
- Location: lines 2729-2730 (doc comment on `like_guard_char_subject_unchanged`)
- Issue: The doc comment, which this change rewrote, says the guard classifies `CHAR` types "(`support.rs:546`)". Line 546 of support.rs is inside `strip_table_alias`. The classification lives in `classify_exa_type` (`ExaTypeClass::Character`), which `guard_like_subject` calls. Line 2729 also runs to about 104 columns.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs, in the doc comment on `like_guard_char_subject_unchanged`, replace "(`support.rs:546`)" with "(`classify_exa_type`)" and rewrap the paragraph so that no line exceeds 90 columns.

### crates/lakehouse-engine/tests/e2e_capability_test.rs

#### [OUTDATED_COMMENT] Module doc lists REGEXP_LIKE as an advertised, translated capability
- Location: lines 3-6 (module doc comment `//!`)
- Issue: The module doc says the file "Exercises the full advertised → translated → executed path for each newly advertised capability group", and lists "REGEXP_LIKE" among those groups. `FN_PRED_REGEXP_LIKE` is no longer advertised or translated. The REGEXP_LIKE tests in this file now prove that Exasol evaluates the predicate itself.
- Fix: In crates/lakehouse-engine/tests/e2e_capability_test.rs, remove "REGEXP_LIKE, " from the capability list in the module doc comment, and append the sentence "It also proves that `REGEXP_LIKE`, which is not advertised, returns the rows native Exasol returns." to the end of that paragraph.

#### [OUTDATED_COMMENT] COUNT(DISTINCT REGEXP_LIKE) test doc describes adapter-side rendering that no longer happens
- Location: lines 3969-3983 (doc comment on `e2e_count_distinct_regexp_like_matches_native_oracle`)
- Issue: The first two paragraphs say the select-list `REGEXP_LIKE` reaches the Exasol-dialect renderer, and that "a WHERE-clause `REGEXP_LIKE` is applied inside the scan by `build_qualified_single_table_fallback_sql`, rendered through the DataFusion trio". With `FN_PRED_REGEXP_LIKE` unadvertised, Exasol sends `REGEXP_LIKE` to the adapter in neither position and evaluates it over the returned rows. Plan task 3.7 keeps the test logic unchanged, but AGENTS.md requires comments to be updated when behavior changes.
- Fix: In crates/lakehouse-engine/tests/e2e_capability_test.rs, in the doc comment on `e2e_count_distinct_regexp_like_matches_native_oracle`, replace the first two paragraphs (from "A select-list `REGEXP_LIKE` inside" through "`Verification` section).") with the single paragraph "A select-list `REGEXP_LIKE` inside `COUNT(DISTINCT ...)` matches an in-session native Exasol oracle (issue #209 repro shape). `FN_PRED_REGEXP_LIKE` is not advertised, so Exasol evaluates the predicate over the rows the scan returns." Keep the third paragraph (the whole-string match semantics and the `typed_probe()` seed values) and the test body unchanged.

## Expert fixes
[none]
