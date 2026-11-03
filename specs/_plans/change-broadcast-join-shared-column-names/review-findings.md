# Code Review Findings: change-broadcast-join-shared-column-names

## Summary
- Files reviewed: 43
- Total findings: 6 (standard: 6, expert: 0)

No broadcast-path column comparison still compares a bare name. Every comparison in the adapter (`column_exa_type` and its guards, `project_columns`, `BinaryColumns::resolve`, `bound_sort_key`, `parse_sort_key_element`, `emits_ident`, `exceeds_identifier_limit`) and in the scan (`combined_type`, `combined_upper_fields`, `render_join_sort_target`, `render_projection`) goes through the (relation alias, column) pair. The remaining bare-name sites (`detect_topn`, `extend_projection_with_sort_keys`, `ParsedSortKey::referenced_columns`, `ensure_no_refused_column_referenced`, `build_qualified_single_table_fallback_sql`) run only on the single-table path, after `strip_table_alias`. `ensure_no_side_refuses_a_referenced_column` attributes by `tableName`, as the plan allows. No current call qualifies a tree twice: `render_broadcast_join` qualifies each clause once from the raw request, and the N-scan fallback receives the raw `pushdown_req`. The new unit tests each fail under a mixed-up pair, and E2E queries (c) and (d1) to (d5) are discriminating. `cargo clippy --workspace --all-targets` reports no issues. `cargo test --workspace --lib` passes 1972 tests, and `cargo test --workspace --test it join` passes 15.

## Standard fixes

### crates/lakehouse-engine/src/adapter/pushdown/joins/mod.rs

#### [OUTDATED_COMMENT] plan_join comments still claim the broadcast attempt never errors
- Location: lines 105-107 (`plan_join` doc comment) and line 213 (comment above `let is_equi`)
- Issue: `render_broadcast_join` now returns `Err` through `unattributable_decline` for a `NoLeg` or `Unattributable` column, and `plan_join` propagates that error with `?`. The comment at line 213 still says "Any miss falls through to the N-scan fallback, never an error". The doc comment at line 106 says "Everything else takes the sole fallback", although such a column fails the request instead.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/mod.rs, replace the sentence "Any miss falls through to the N-scan fallback, never an error." in the comment above `let is_equi` with "A decline falls through to the N-scan fallback; a column no single leg owns fails with the fallback's own error." In the `plan_join` doc comment, after the sentence ending "joins in Exasol.", add the sentence "A column no single leg owns fails the request through `unattributable_decline`, the fallback's own error, on both paths."

### crates/lakehouse-engine/src/adapter/pushdown/joins/attribution.rs

#### [MISSING_DESIGN_INTENT] JoinLegs::qualify does not state that it must run once on a raw tree
- Location: lines 202-224 (`qualify_every_column` and `qualify` doc comments)
- Issue: `qualify` is not idempotent on a self-join. Its output carries `LHS_T{leg}` aliases, which match no leaf alias. A second pass over a self-join's output therefore returns `Unattributable`. When the query itself aliases its occurrences `LHS_T1` and `LHS_T0`, a second pass binds each column to the other leg and returns wrong rows without an error. `render_broadcast_join` now holds the raw `pushdown_req` and the qualified `qualified_req` side by side, and nothing tells the next editor that the qualified tree must never reach `qualify`, `qualify_every_column`, or the N-scan fallback.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/attribution.rs, append to the doc comment of `JoinLegs::qualify` the sentence "Apply it once, to a raw request tree: an `LHS_T{leg}` alias matches no leaf alias, so re-qualifying a self-join's output fails as unattributable, or binds the other leg when the query names its occurrences `LHS_T*`." Append to the doc comment of `JoinLegs::qualify_every_column` the sentence "Like [`Self::qualify`], it takes a raw request tree, never its own output."

### crates/lakehouse-engine/src/scan/spec.rs

#### [MISSING_DOC_COMMENT] ColumnRef::qualified has no doc comment
- Location: line 191
- Issue: `ColumnRef::qualified` is a `pub` constructor of the wire type, used by the adapter, the scan, and the integration tests. It has no doc comment, while `output_name` and `relation_alias` beside it do.
- Fix: In crates/lakehouse-engine/src/scan/spec.rs, add this doc comment line directly above `pub fn qualified`: "/// A column of the join relation named `relation`, which must come from [`relation_alias`] so [`Self::output_name`] stays unambiguous."

### crates/lakehouse-engine/tests/it/scan_join_test.rs

#### [OUTDATED_COMMENT] write_orders cites the removed disjoint-column guarantee
- Location: line 56
- Issue: The doc comment "Column names are disjoint from the dimension side (the VS disjoint-column guarantee)." cites a guarantee this change removes. The adapter no longer guarantees disjoint names, and the scan binds every column by its relation alias.
- Fix: In crates/lakehouse-engine/tests/it/scan_join_test.rs, replace the doc comment above `fn write_orders` with this line: "/// The orders fact relation: [`qualified`] binds every column without the `C_` prefix to it."

### crates/lakehouse-engine/tests/it/scan_positional_deletes.rs

#### [OUTDATED_COMMENT] write_keyed_parquet requires disjoint column names that the join path no longer needs
- Location: line 846
- Issue: The doc comment "Join sides must use disjoint column names: the join path relies on the VS guarantee." states a requirement that no longer holds. Both callers now pass relation-qualified projections and conditions (`"LHS_T0"."C_KEY" = "LHS_T1"."O_KEY"`), so the join path resolves shared names per relation.
- Fix: In crates/lakehouse-engine/tests/it/scan_positional_deletes.rs, delete the doc comment line above `fn write_keyed_parquet`.

### crates/lakehouse-e2e/tests/e2e_local/scan.rs

#### [OUTDATED_COMMENT] The low-threshold comment inverts the size comparison
- Location: line 390, in `a_join_compares_a_binary_filter_on_the_raw_bytes_on_both_join_paths`
- Issue: The comment says "A 1-byte broadcast threshold exceeds every table". The opposite holds: every table exceeds the 1-byte threshold, which is why the join falls back to the N-scan path.
- Fix: In crates/lakehouse-e2e/tests/e2e_local/scan.rs, replace "A 1-byte broadcast threshold exceeds every table, so the join falls back to one scan" with "Every table exceeds a 1-byte broadcast threshold, so the join falls back to one scan", keeping the second line "per table, joined on hex text." unchanged.

## Expert fixes
[none]
