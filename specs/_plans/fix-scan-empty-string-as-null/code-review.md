# Code Review Findings: fix-scan-empty-string-as-null

## Summary
- Files reviewed: 27
- Total findings: 3

`cargo test --workspace --lib` passes (exit 0, every suite `0 failed`). The E2E suite was not run by this review.

## Findings

### crates/vs-expression/src/lib_tests.rs

#### [MISSING_SCENARIO_PROOF] The string-function case table does not prove the changed scenario
- Location: `renders_string_scalar_functions`, lines 911-961
- Issue: The plan maps the scenario "String scalar functions translate to DataFusion string calls" to this test "with the wrapped expectations", and the changed scenario adds "a call that returns text SHALL be wrapped as `nullif(<call>, '')`". The test still renders through the untyped `render_expression`, so it asserts bare calls such as `lower("S")`. The typed pass adds the wrap, and no test asserts it for `LOWER`, `SUBSTR`, `TRIM`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `CHR`, `INITCAP`, `LEFT`, `RIGHT`, `TRANSLATE`, or `UNICODECHR`. `scan_typing_tests.rs` covers only `RTRIM`, `LTRIM`, `UPPER`, `CONCAT`, and `REPLACE`. The test is the scenario's one proof, so `specs/testing.md` § Coverage rule requires a `/// Scenario:` line, and it has none.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, add `/// Scenario: String scalar functions translate to DataFusion string calls` above `renders_string_scalar_functions`. Replace each `render_expression(&expr)` call in it with `typed_scan(&expr)`. Expect `nullif(<lower name>(nullif("S", '')), '')` in the `cases_lower` loop for every name except `ASCII`, which expects `ascii(nullif("S", ''))`, and `CHR`, which moves out of the loop. Expect `nullif(substr(nullif("S", ''), 1, 3), '')` for `SUBSTR`, and `character_length(nullif("S", ''))`, `octet_length(nullif("S", ''))`, and `ascii(nullif("S", ''))` for `LENGTH`, `OCTET_LENGTH`, and `UNICODE`. Add `("n", ExasolType::decimal(18, 0))` to the columns of `typed_scan`, then expect `nullif(chr("N"), '')` for both `CHR` and `UNICODECHR` over `col("n")`. Leave the `INSTR` and `LOCATE` literal cases unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [STALE_COMMENT] The doc of `build_row_scan_sql` says both sorts render through `render_order_by_clause`
- Location: lines 121-123
- Issue: The comment says the per-shard sort and the outer merge "both render through [`render_order_by_clause`]". After this change the per-shard sort in `raw_scan.rs` renders each key through `render_sort_target` and `SortKey::render_ordered`. Only the merge still uses `render_order_by_clause`, and the two now share only `render_ordered`.
- Fix: Replace the doc comment of `build_row_scan_sql` with: `/// A matched top-N carries the same \`order_by\` in the per-shard blob (a DataFusion TopK) and the outer merge; both render direction and NULL placement through [\`render_ordered\`], so they agree.` Import `render_ordered` for the intra-doc link if rustdoc needs it, or write it as plain code.

### crates/lakehouse-engine/src/scan/spec.rs

#### [STALE_COMMENT] `render_order_by_clause` still claims the per-shard sort uses it
- Location: lines 312-314 (outside the changed-files list, made false by the `raw_scan.rs` change)
- Issue: The doc says the function is "Shared by the adapter's merge sort and the scan's per-shard sort". `build_scan_sql` in `raw_scan.rs` no longer calls it, and its only production caller is `build_row_scan_sql` in `adapter/pushdown/support.rs`.
- Fix: In `crates/lakehouse-engine/src/scan/spec.rs`, change the sentence `Shared by the adapter's merge sort and the scan's per-shard sort.` to `The adapter's merge sort.` and keep the rest of the comment.
