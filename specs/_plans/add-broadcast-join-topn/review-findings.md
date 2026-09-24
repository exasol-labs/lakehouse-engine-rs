# Code Review Findings: add-broadcast-join-topn

## Summary
- Files reviewed: 16
- Total findings: 13 (standard: 13, expert: 0)
- Evidence: `cargo test -p lakehouse-engine --lib -- join_scan sql_builders` (75 passed), `cargo test -p lakehouse-engine --lib -- scan::spec::tests::join_spec` (3 passed), `cargo test -p lakehouse-engine --test scan_join_test` (10 passed), `cargo clippy -p lakehouse-engine --all-targets` (no warnings). E2E was not run.

## Standard fixes

### crates/lakehouse-engine/src/scan/join_scan.rs

#### [INFORMATION_LEAKAGE] The string-key guard keeps its own copy of the Arrow-to-VARCHAR mapping
- Location: lines 300-306 (`render_join_sort_target`, the `nullif` arm guard)
- Issue: Which Arrow types reach Exasol as VARCHAR is decided in one place, `types::mapping::compatible_exasol_type`. The guard rebuilds that set by hand as `matches!(dt, Utf8 | LargeUtf8 | Utf8View) || needs_nested_json_rendering(dt) || needs_json_fallback(dt)`. Two of its three terms are already covered by the third. `Utf8View` has no arm in `compatible_exasol_type`, so `needs_json_fallback(Utf8View)` is true, and every type `needs_nested_json_rendering` accepts is also incompatible. So the guard reduces to `Utf8 | LargeUtf8 || needs_json_fallback`, and that list stays correct only as long as nobody changes `compatible_exasol_type`. If a later change makes `Utf8View` a directly compatible VARCHAR, the trimmed list silently stops applying `nullif` to it, and the shard's top-N returns wrong rows with no error (decision [2]).
- Fix: In crates/lakehouse-engine/src/scan/join_scan.rs, change the guard of the second `match` arm in `render_join_sort_target` to `Some(dt) if classify_exa_type(&arrow_to_exasol_type(dt)) == ExaTypeClass::Character =>`, keeping the arm body `format!("nullif({emitted}, '')")`. Add `arrow_to_exasol_type`, `classify_exa_type`, and `ExaTypeClass` to the existing `use crate::types::mapping::{...}` import. In the doc comment of `render_join_sort_target`, replace "A text value is wrapped" with "A value Exasol receives as a character type (per `arrow_to_exasol_type`) is wrapped". Run `cargo test -p lakehouse-engine --lib join_scan` and `cargo test -p lakehouse-engine --test scan_join_test`.

### crates/lakehouse-engine/src/scan/join_scan_tests.rs

#### [MISSING_BOUNDARY_TEST] No test covers ranking by the `CAST(... AS VARCHAR)` fallback
- Location: after `build_join_sql_ranks_a_nan_float_key_as_null` (end of file)
- Issue: The scenario "A join shard ranks each sort key by the value it emits" requires a key emitted through the JSON rendering OR through the `CAST(... AS VARCHAR)` fallback to rank by its emitted text. `build_join_sql_ranks_a_json_rendered_key_by_its_emitted_text` covers only the JSON-rendering branch (`List<Int64>`). The `CAST` fallback branch (`Binary`, an out-of-range `Decimal128`, `Utf8View`) is never ranked in any test.
- Fix: In crates/lakehouse-engine/src/scan/join_scan_tests.rs, add `#[tokio::test] async fn build_join_sql_ranks_a_cast_fallback_key_by_its_emitted_text()`. Build the session with `join_session(fact_batch_with("amount", Arc::new(Decimal128Array::from(vec![9i128, 10]).with_precision_and_scale(38, 0).unwrap())))` and the spec with `post_join_spec(vec![sort_key("AMOUNT", true, true)], Some(1))`, then run `run_join_sql`. For each batch, convert column 2 with `arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap()`, downcast it to `StringArray`, and collect the values. Assert that the result equals `vec!["10".to_string()]`. Add `Decimal128Array` to the `arrow::array` import. Give the test a doc comment saying that the emitted text `"10"` sorts before `"9"`, while native decimal order puts 9 first. Run `cargo test -p lakehouse-engine --lib build_join_sql_ranks_a_cast_fallback_key_by_its_emitted_text`.

### crates/lakehouse-engine/src/scan/spec.rs

#### [INFORMATION_LEAKAGE] The wire field's doc restates the adapter's window-placement policy
- Location: lines 619-621 (`JoinSpec::post_join_limit` doc, final sentence)
- Issue: The sentence "An ordered window with a non-zero offset or no limit carries no cap, so its shards stay unbounded and the wrapper alone applies the window." describes a decision that `build_broadcast_join_sql` makes in the adapter. The plan's Quick Diagnostic gives that decision to `build_broadcast_join_sql` alone. This change had to edit the same policy text in `spec.rs`, `planning.rs`, and `sql_builders.rs`, which shows the leak. A per-shard top-(n + m) bound, which decision [3] names as a possible follow-up, would make this scan-crate doc stale again.
- Fix: In crates/lakehouse-engine/src/scan/spec.rs, delete the sentence at lines 619-621 that starts "An ordered window with a non-zero offset or no limit carries no cap". End the paragraph at "...and the adapter's outer wrapper merges those and cuts the global top-`n`.".

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [OUTDATED_COMMENT] The `window` paragraph cites a reason that its target no longer states
- Location: lines 716-726 (`build_broadcast_join_sql` doc)
- Issue: "for the reason stated once in [`JoinSpec::post_join_limit`]" now hangs off "Every other ordered shape ... leaves every shard uncapped and unsorted". The `post_join_limit` doc gives no reason for that. It gives the reason why a cap never lands on a side's scanned input, and the rewrite dropped that statement from this paragraph. The paragraph also carries the ticket ref "issue #309", and it leaves out decision [4]'s rule that an ordered fan-out carries no merge `LIMIT`.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, replace doc lines 716-726 with: "/// `window` decides where the request's row window lands. Whatever lands in the join block lands only AFTER the node-local join, never on a side's scanned input, for the reason stated once in [`JoinSpec::post_join_limit`]. An unordered cap composes per shard, so it rides in the join block AND on the outer merge. An ordered window always rides on an outer wrapper over the merged fan-out, because only the wrapper sees the global rank. A zero-offset `ORDER BY … LIMIT n` also puts the same sort keys and `n` in the join block (`post_join_order_by` + `post_join_limit`), so each shard keeps its own top-`n` joined rows and the wrapper merges and re-cuts the global top-`n`. The merge itself carries no `LIMIT`, because a cut before the wrapper's sort keeps arbitrary rows. Every other ordered shape leaves every shard uncapped and unsorted: a per-shard `OFFSET` would skip each shard's own first rows, and an ordering with no `LIMIT` bounds nothing." Wrap it at the file's doc-comment width.

#### [INLINE_COMMENT] A sentence was added to the inline comment in the `Ordered` arm
- Location: lines 750-751
- Issue: This change added the inline sentence "`bound_sort_key` is also the source of the per-shard key list below, so the same check that admits a key is the one that supplies it." The code under it (`let Some(bound_keys) = keys.iter().map(|key| bound_sort_key(..))`) already shows this.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, change line 750 to `            // to.` and delete line 751. The pre-existing comment then ends at "has nothing to bind to.".

#### [MIXED_ABSTRACTION_LEVEL] Window placement is decided inline as a positional 4-tuple
- Location: lines 738-768 (`build_broadcast_join_sql`, the `let (shard_cap, shard_order_by, merge_cap, ordering) = match window { ... }` block)
- Issue: `build_broadcast_join_sql` decides the window placement table inline and also builds the `JoinSpec`, the fan-out, and the wrapper. The decision comes out as a positional tuple `(Option<u64>, Vec<SortKey>, Option<u64>, Option<(..)>)`, so `shard_cap` and `merge_cap` share a type and swapping them still compiles. Decision [4] says a merge `LIMIT` on an ordered fan-out returns wrong rows. The `Ordered` arm also shadows the outer `shard_cap`/`shard_order_by` names with an inner `let (shard_cap, shard_order_by) = match (offset, limit)`.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs, add a private `struct BroadcastWindowPlacement { shard_cap: Option<u64>, shard_order_by: Vec<SortKey>, merge_cap: Option<u64>, wrapper: Option<(Vec<ParsedSortKey>, Option<u64>, u64)> }`. Add a private `fn place_broadcast_window(window: JoinWindowPlan, projection: &[ProjectionItem]) -> Option<BroadcastWindowPlacement>` whose body is the current `match window` with its inline comment. Each arm returns `Some(BroadcastWindowPlacement { .. })` with named fields, and both current `return Ok(None)` exits (an unbound key, `ExasolPostProcessed`) return `None`. In `build_broadcast_join_sql`, replace the match with `let Some(placement) = place_broadcast_window(window, &rendered.projection) else { return Ok(None); };`. Read `placement.shard_cap` and `placement.shard_order_by` in the `JoinSpec` literal, `placement.merge_cap` in the `build_scan_driving_sql` call, and `placement.wrapper` in the `let Some((keys, limit, offset)) = ... else` binding. Run `cargo test -p lakehouse-engine --lib joins::sql_builders`.

### crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs

#### [WORK_TRACKING_COMMENT] A ticket ref was added to the `JoinWindowPlan::Ordered` doc
- Location: line 465
- Issue: The new doc text says "bound on the join block (issue #309)". The ticket ref records change history. The implementing commit's `Closes #309` already carries that, and the ref tells a reader nothing about the current code.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs, delete " (issue #309)" from line 465 so the line reads "/// bound on the join block — every row ranked strictly ahead of".

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs

#### [OUTDATED_COMMENT] `assert_shards_carry_no_window` still says every ordered window leaves shards unbounded
- Location: lines 2185-2204
- Issue: The doc says "Every ordered-window test asserts this: an ordered window is global, so nothing may truncate or sort a shard's own output before the wrapper does." This change made that false. `broadcast_ordered_zero_offset_limit_bounds_each_shard_to_its_top_n` is an ordered-window test, does not call the helper, and asserts the opposite. The four failure messages ("an ordered shard must stay uncapped", "... unsorted", "... carry no post-join cap", "... carry no post-join ordering") make the same general claim.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs, replace the doc of `assert_shards_carry_no_window` with "/// Every ordered-window test with a non-zero offset or no `LIMIT` asserts this: that window cannot bound a shard, so nothing may truncate or sort a shard's own output before the wrapper does." In all four assertion messages of that helper, replace "an ordered shard must" with "a non-zero-offset or unlimited ordered shard must".

#### [SHRINKABLE] The LINEITEM ⋈ ORDERS broadcast fixture is copied three times
- Location: lines 2150-2171 (`broadcast_window_sql`), 2294-2329 (`broadcast_ordered_bounds_shards_with_keys_from_either_side`), 2360-2382 (`broadcast_zero_offset_request_takes_the_bounded_path`)
- Issue: All three build the same `JoinSides` (LINEITEM `l-0` 1000 bytes, ORDERS `o-0` 10 bytes), the same `RenderedJoinPushdown` condition, and the same `build_broadcast_join_sql(.., &two_scan_tuning(), "SCAN", "DISTRIBUTE").expect("selecting the wire storage must succeed")` call. Only the projection and the window differ. This is the third occurrence, so the Rule of Three applies.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs, add `fn broadcast_sql_projecting(projection: &[(&str, &str)], window: JoinWindowPlan) -> Option<String>`. It builds the shared `JoinSides` and a `RenderedJoinPushdown` whose `projection` is `ProjectionItem::Column(column)` and whose `projection_types` is the Exasol type for each `(column, exasol_type)` pair, calls `build_broadcast_join_sql`, and applies `.expect("selecting the wire storage must succeed")`. Make `broadcast_window_sql(window)` return `broadcast_sql_projecting(&[("L_ORDERKEY", "DECIMAL(20,0)")], window)`, and move its fixture doc comment onto the new helper. In `broadcast_ordered_bounds_shards_with_keys_from_either_side`, replace the `sides`/`rendered` literals and the call with `broadcast_sql_projecting(&[("L_ORDERKEY", "DECIMAL(20,0)"), ("O_ORDERDATE", "DATE")], JoinWindowPlan::Ordered { .. }).expect("both keys are members of the broadcast projection")`. In `broadcast_zero_offset_request_takes_the_bounded_path`, delete the `sides`/`rendered` literals and make the `build` closure `|request: &Json| broadcast_sql_projecting(&[("O_ORDERDATE", "DATE")], classify_join_window(&pd(request))).expect("the ordered key is a member of the broadcast projection")`. Run `cargo test -p lakehouse-engine --lib joins::sql_builders`.

### crates/lakehouse-engine/tests/e2e_join_test.rs

#### [SHRINKABLE] `fetch_label_orderkey_rows_in_query_order` duplicates `fetch_join_rows_in_query_order`
- Location: lines 120-138 and 144-157
- Issue: The two functions are identical except for the closure parameter names `(label, order_key)` and `(name, date)`. Each runs `query_columns`, asserts 2 columns, zips them, and maps `value_to_string`.
- Fix: In crates/lakehouse-engine/tests/e2e_join_test.rs, delete `fetch_label_orderkey_rows_in_query_order` and its doc comment. Replace each of its call sites with `fetch_join_rows_in_query_order`. Rename that function's closure parameters to `(first, second)`. Change the first line of its doc comment to "Fetch a 2-column join query's rows (such as `(C_NAME, O_ORDERDATE)` or `(B_LABEL, O_ORDERKEY)`) in the exact order Exasol returned them".

#### [SHRINKABLE] The ascending top-2 query runs again after the loop already ran it
- Location: lines 480-532 (`e2e_broadcast_join_top_n_ranks_empty_string_as_null`)
- Issue: The `for order_by in [...]` loop already runs `blank_label_join_query(VS_NAME, "b.B_LABEL ASC NULLS LAST, o.O_ORDERKEY ASC", 2)` into `broadcast_rows`. The `ascending_rows` block at lines 521-532 sends the same query string to Exasol again only to assert its exact rows.
- Fix: In crates/lakehouse-engine/tests/e2e_join_test.rs, make the loop iterate over `[("b.B_LABEL ASC NULLS LAST, o.O_ORDERKEY ASC", Some(vec![("alpha".to_string(), "3".to_string()), ("alpha".to_string(), "8".to_string())])), ("b.B_LABEL DESC NULLS FIRST, o.O_ORDERKEY ASC", None)]` as `(order_by, expected_rows)`. After the broadcast/fallback equality assertion, add `if let Some(expected) = expected_rows { assert_eq!(broadcast_rows, expected, "the ascending top-2 must be the alpha rows of orders 3 and 8: {broadcast_rows:?}"); }`. Delete the standalone `ascending_rows` fetch and its assertion.

#### [INLINE_COMMENT] The tie case is a second concept inside one test, introduced by an inline comment
- Location: lines 534-578 (`// Tie case:` through the `labels_low` assertion)
- Issue: `e2e_broadcast_join_top_n_ranks_empty_string_as_null` checks both the empty-string-as-NULL ranking and decision [8]'s tie-at-the-cut behavior. The four-line `// Tie case: ...` inline comment marks the second concept. The test name covers only the first.
- Fix: In crates/lakehouse-engine/tests/e2e_join_test.rs, move everything from the `// Tie case:` comment through the final `labels_low` assertion into a new `#[test] fn e2e_broadcast_join_top_n_keeps_the_single_node_sort_keys_across_a_tie()`. Its body starts with `setup_e2e(); let mut conn = exa_conn();`. Turn the inline comment's text into the new test's `///` doc comment and delete the inline comment. In the doc comment, replace the claim "proving each shard's own top-N cut kept its tied boundary row for the merge" with "so the merged result carries the single-node label sequence even though the tied rows come from different shards".

### crates/lakehouse-engine/tests/common/seed.rs

#### [DEAD_FLEXIBILITY] `BLANK_LABEL_ROWS` cannot be changed on its own
- Location: lines 1350-1355
- Issue: `pub const BLANK_LABEL_ROWS` is read only by `make_blank_label_batch`. That function pairs it with the hard-coded five-element `labels` array, so any other value makes `RecordBatch::try_new` fail, and the `expect` message "construction is infallible" would then be false. The array length is the real source of the row count.
- Fix: In crates/lakehouse-engine/tests/common/seed.rs, delete the `BLANK_LABEL_ROWS` constant and its doc comment. In `make_blank_label_batch`, declare `labels` first and derive the keys as `let keys: Vec<i64> = (1..=labels.len() as i64).collect();`.

## Expert fixes
[none]
