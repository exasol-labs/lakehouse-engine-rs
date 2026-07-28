# Tasks: refactor-pushdown-type-rewrite-pipeline

## Phase 2: Implementation (Group A)
- [x] 2.1 Add `pub(super) fn apply_filter_type_rewrites` and private `fn apply_select_item_type_rewrites` to `support.rs`, colocated with the three passes, with migrated doc comments (plan Task 1)

## Phase 2: Implementation (Group B)
- [x] 2.2 Rewire the production filter site (`mod.rs:215-219`) to call `apply_filter_type_rewrites`; update the `use super::support::{…}` import (plan Task 2)
- [x] 2.3 Rewire the six `mod.rs` chain tests to call `apply_filter_type_rewrites` instead of the inline triple (plan Task 3)
- [x] 2.4 Rewire `project_columns`' select-list arm to call `apply_select_item_type_rewrites` (plan Task 4)

## Phase 2: Implementation (Group C)
- [x] 2.5 Narrow `like_subject_type_guard`, `string_function_arg_type_guard`, and `rewrite_decimal_stringifications` to private; correct wiring-claim docs; check `joins/rendering.rs:507` intra-doc link (plan Task 5)

## Phase 2: Implementation (Group D)
- [x] 2.6 Add `select_list_pipeline_omits_like_pass_pending_219` test in `support.rs` (plan Task 6)

## Phase 3: Verification
- [x] 3.1 Run the gate: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, `cargo test --features exasol-e2e --no-run` (plan Task 7)
- [x] 3.2 Rustdoc narrowing gate: `cargo doc --no-deps -p lakehouse-engine`
- [x] 3.3 UDF build: `make cross-musl-udf-build`
- [x] 3.4 E2E: `make test-e2e` against live Docker Exasol stack — 59 passed, 0 failed

## Phase 4: Review Fixes
- [x] 4.1 In `support.rs`, replace `apply_filter_type_rewrites`' doc paragraph at lines 1026-1042 with a compact per-pass line naming what each pass contributes to the sequence and linking to it for the detail — one line each for `` [`like_subject_type_guard`] `` (may decline the whole filter, or rewrap a DATE subject), `` [`string_function_arg_type_guard`] `` (coerces string-position arguments, or declines), and `` [`rewrite_decimal_stringifications`] `` (runs last, never declines) — dropping the restated traversal-reach, per-type dispatch, and trim-form prose; keep the issue references (#207, #210, #211); leave the ordering paragraph and the fallibility-bridge paragraph and the `Returns:` block unchanged
- [x] 4.2 In `support.rs` line 736, change "those two pipeline functions are this pass's only callers" to state that those two pipeline functions are this pass's only PRODUCTION callers (the pass corpus in `mod tests` calls it directly)
- [x] 4.3 In `support.rs`, delete the first sentence of the comment at lines 1206-1207 ("Run the ordered type-rewrite pass sequence on this select-list item (`apply_select_item_type_rewrites`).") so the comment begins at "On `None` the item can't be safely pushed down at all; …", keeping the rest of the comment (through line 1214) unchanged
- [x] 4.4 In `mod.rs`, delete the three-line inline comment at lines 871-873 inside `where_filter_decimal_stringification_rewritten_to_trim`, leaving the doc comment at lines 855-857 as the single statement and the `let rendered = Some(&filter_json)` chain untouched
- [x] 4.5 In `mod.rs`, delete the first sentence of the comment at lines 188-189 and reword the surviving sentence so it stands alone without the dangling "This chain" antecedent — state that the rewritten filter feeds ONLY the DataFusion-bound scan filter and that `filter_json_raw` itself is left completely unmodified for the later `resolve_file_list` Iceberg-level pruning call below, which must see the original, un-rewritten predicate tree
