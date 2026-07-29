# Code Review Findings: refactor-col-types-guard-dedup

## Summary
- Files reviewed: 6 (Makefile, adapter/pushdown/joins/planning.rs, adapter/pushdown/support.rs, types/mapping.rs, tests/common/seed.rs, tests/e2e_non_ascii_identifier_test.rs)
- Total findings: 5 (standard: 4, expert: 1)

Behavior-identity check on the refactor portions passed: `extract_all_column_types` (first table + `str::to_uppercase`) and `involved_table_columns` (find-by-name + `str::to_ascii_uppercase`) reproduce their prior walks exactly through `column_types`; `guard_like_subject`, `is_bare_decimal_column` and `coerce_string_position_arg` map onto `classify_exa_type`'s arms with no reachable arm reordering (`starts_with("DECIMAL")` and `== "DATE"` are disjoint, so the classifier's DECIMAL-before-DATE order cannot change a dispatch). Issue #270 exists, is OPEN, and is scoped as the doc comment claims. No `[DEAD_FLEXIBILITY]` finding is raised against `column_types`' `fold_case`: decision-log [12] adjudicated it with alternatives, and its doc comment states the non-observability and the end date.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [OUTDATED_COMMENT] `walk_column_nodes` doc claims no test in the crate uses a non-ASCII column name
- Location: lines 1316-1318
- Issue: the doc comment's reason for keeping `collect_all_column_names`' Unicode fold and `joins/rendering.rs`' ASCII folds separate ends "and no test in this crate uses a non-ASCII column name, so unifying them would silently change behavior while the whole suite still passed". This change made that sentence false: `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list` (support.rs:6110) and `each_builder_keeps_its_own_case_fold_on_a_constructed_non_ascii_literal` (joins/planning.rs:795) both use `STRAßE` as a column name. The `MUST NOT be unified` guidance is still correct — the two new tests cover the `column_types` builders, not the three collectors this comment is about — so only the supporting claim is stale, and as written a reader who greps for `STRAßE` concludes the whole paragraph is out of date.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, reword the final sentence of `walk_column_nodes`' doc comment (lines 1316-1318) so the "no test" claim is scoped to these three collectors instead of the whole crate — state that no test exercises `collect_all_column_names`, `collect_column_tables` or `collect_side_column_names` with a non-ASCII column name, so unifying their folds would still pass the whole suite. Do not weaken or delete the `MUST NOT be unified` sentence, and do not add coverage for those collectors (out of this plan's scope).

#### [OUTDATED_COMMENT] `column_exa_type` doc says its fold matches the keys both `col_types` builders produce
- Location: line 669
- Issue: the doc states the lookup folds with `to_uppercase` "to match the uppercased keys the `col_types` builders produce" (plural). It matches `extract_all_column_types`' Unicode-folded keys; against `involved_table_columns`' ASCII-folded keys it agrees only for names on which the two folds agree, which is what the test 5,400 lines below in the same file (`column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list`, support.rs:6110) pins as an explicit MISS. The claim and the assertion in the same file read as contradicting each other.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, reword `column_exa_type`'s doc comment (line 669) to name the single fold it matches: it folds with the full-Unicode `to_uppercase` to match the keys `extract_all_column_types` builds, and `involved_table_columns`' ASCII-folded keys are identical for every column name the adapter can declare (`resolve_table_schema` Unicode-uppercases them first). Keep the rest of the doc comment, including the `type`-tag paragraph, unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs

#### [OUTDATED_COMMENT] `involved_table_columns` doc claims it maps columns exactly as the single-table projection does
- Location: lines 352-360
- Issue: paragraph 2 says it "maps its columns exactly as the single-table projection does — uppercased names, Exasol types from `dataType`", and the paragraph added directly below it (lines 359-360) says the opposite: this side supplies "the ASCII-only fold this side has always applied", where the single-table projection (`extract_all_column_types`) supplies `str::to_uppercase`. The characterization test added to this same file (line 795) exists precisely to pin that the two folds are NOT the same mapping. The summary line's `(UPPERCASE name, …)` is stale for the same reason — the ASCII fold leaves a non-ASCII letter unfolded (`STRAßE`).
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs, rewrite `involved_table_columns`' doc comment (lines 352-360): drop "exactly as the single-table projection does" from paragraph 2 and state instead that names are folded with the ASCII-only `to_ascii_uppercase` and types come from `dataType`; change the summary line to `The (folded name, Exasol type) columns of the named involved table.`; keep the "Returns an empty vec when the table or its columns are absent" sentence and the closing partial-application paragraph as they are.

### crates/lakehouse-engine/tests/common/seed.rs

#### [OUTDATED_COMMENT] Doc claims the table and column names cannot drift apart, but they are two independent literals
- Location: lines 2849-2852
- Issue: the doc comment says "Both the TABLE name and the COLUMN name under test are this same non-ASCII identifier, so the table and the seeder cannot drift apart", while `E2E_NONASCII_TABLE` (line 2851) and `NONASCII_COL` (line 2852) are two separately written `"straße"` literals — editing one leaves the other behind, which is exactly the drift the comment promises is impossible. The E2E test relies on the two being equal: it uses a single `SERVED_NAME = "STRASSE"` for both the table and the column.
- Fix: In crates/lakehouse-engine/tests/common/seed.rs line 2852, define the column constant from the table constant instead of repeating the literal — `pub const NONASCII_COL: &str = E2E_NONASCII_TABLE;` — so one literal backs both and the existing doc comment's claim holds. Leave the doc comment and all use sites (lines 2878, 2900) unchanged.

## Expert fixes

### crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs

#### [ASSERTION_FREE_TEST] The second pushdown assertion cannot fail, while its message claims it proves the filter names the LIKE subject
- Location: lines 105-108
- Issue: `pushed_sql` is `explain_virtual_sql`'s flattened blob of EVERY `EXPLAIN VIRTUAL` result cell — the adapter-generated scan SQL *and* the echoed adapter exchange, including the `pushdownRequest` Exasol sent (see `explain_virtual_pushdown_request`'s doc, e2e_capability_test.rs:2607-2617). The substring `STRASSE` therefore appears in that blob unconditionally: in the virtual-schema name `STRASSE_VS`, in the table name, in the echoed `involvedTables[].columns[].name`, and in the scan projection Exasol needs in order to evaluate the predicate itself when the filter is DECLINED. So `assert!(pushed_sql.contains(SERVED_NAME))` holds on both sides of the distinction the test is meant to draw, while its failure message asserts "pushed filter must name the LIKE subject as STRASSE". The first assertion (line 100, `contains("\"filter\":\"")`) is genuinely discriminating and must stay: the scan spec's `filter` field is `skip_serializing_if = "Option::is_none"`, the WHERE clause holds exactly one predicate, and Exasol's echoed request renders its own filter as an object (`"filter":{`), not a string — so a string-valued `filter` field can only come from the adapter having pushed this LIKE.
- Fix: In crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs, delete the second `assert!` (lines 105-108) and extend the comment at lines 94-98 to record why the `"filter":"` field-presence assertion alone is sufficient: the WHERE clause is a single LIKE predicate, so a declined guard drops the whole top-level filter and the field disappears entirely (same reasoning as `assert_filter_pushed_down` in tests/e2e_capability_test.rs:1259-1277). Do NOT substitute a substring check on the LIKE pattern (`alpha%`), the column name, or `predicate_like` in its place — every one of those also appears in the echoed `pushdownRequest` and would be vacuous in the same way. Leave assertions 1-4 (lines 55-92) and the first pushdown assertion untouched, and re-run `cargo test --features exasol-e2e --test e2e_non_ascii_identifier_test -- --test-threads=1` against the Docker Exasol container to confirm the binary still passes.
