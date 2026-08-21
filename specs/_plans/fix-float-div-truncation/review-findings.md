# Code Review Findings: fix-float-div-truncation

## Summary
- Files reviewed: 4
- Total findings: 6 (standard: 5, expert: 1)

Verified clean (no finding raised):
- `"FLOAT_DIV" => "/"` in the `op` match (`lib.rs:1010`) is still **live**: the new
  `match (fn_name, dialect)` only diverts `("FLOAT_DIV", Dialect::DataFusion)`, so the Exasol dialect
  still reaches `_ => format!("({left} {op} {right})")` with `op == "/"`, and two tests pin it
  (`exasol_dialect_renders_declared_verbatim_surface`, the new Exasol-side assertion in
  `arithmetic_operators_render_identically_in_both_dialects`). Not a dead arm.
- The int/int oracle's `(SELECT 7 AS L_ORDERKEY, 1 ... UNION ALL SELECT 7, 2) WHERE L_LINENUMBER = 2`
  wrapper looks like an over-complicated `SELECT 7/2`, but it is deliberate and must be kept:
  decision-log [7] records that native Exasol constant-folds a *literal* division in exact arithmetic
  while a *column* division rounds like the cast form. The subquery forces column-typed operands.
- Comment budget: the module-doc refresh (`lib.rs:41-44`), the arm comment (`lib.rs:1003-1004`) and
  the `arithmetic_operators_render_identically_in_both_dialects` doc amendment are all explicitly
  mandated by tasks 2.1 / 3.2. Only one unmandated comment crept in (finding 4).
- The Exasol dialect did not move: no golden fixture, `scalar_over_agg_tests.rs`,
  `single_group_agg_tests.rs` or `support_tests.rs` site appears in the diff (task 4.1 satisfied by
  omission).

## Standard fixes

### crates/vs-expression/src/lib.rs

#### [INFORMATION_LEAKAGE] The `/` spelling now lives in two places inside one arm, and the helper round-trips JSON to fetch a constant
- Location: lines 555-559 (`render_float_div_datafusion`) and lines 1006-1032 (the `"ADD" | "SUB" | "MULT" | "FLOAT_DIV"` arm)
- Issue: three defects in the same five lines.
  (a) The division operator is now decided twice: `op = "/"` at line 1010 (used by the Exasol path)
  and a second hardcoded `/` inside `render_float_div_datafusion` at line 558 (used by the DataFusion
  path). Changing the operator mapping in one place leaves the other stale — exactly the back-door
  leakage decision-log [5] set out to avoid for the `DOUBLE` spelling, reintroduced for `/`. On the
  FLOAT_DIV+DataFusion path `op` is computed and then discarded.
  (b) The helper synthesises a wire-format node (`serde_json::json!({"type": "DOUBLE"})`) and feeds it
  back through `render_cast_target` purely to read back the constant `"DOUBLE"`
  (`lib.rs:495` — `"DOUBLE" | "DOUBLE PRECISION" => Ok("DOUBLE".to_string())`, dialect-invariant).
  It allocates a `serde_json::Value` per FLOAT_DIV render and makes the helper fallible: the `?` at
  line 557 can never fire, so it is an untested, unreachable error path.
  (c) The shape contradicts the plan's own decision record: plan.md line 96 selected
  "a `cast_to_double(expr_sql: &str) -> String`-shaped wrapper, following `format_decimal_exasol_style`
  … no JSON, no type context", and task 2.1 asked for that colocated-pure-helper shape while not
  hardcoding a second `"DOUBLE"` copy. A shared named constant satisfies both halves; synthesised JSON
  satisfies neither.
- Fix: In `crates/vs-expression/src/lib.rs`: (1) add a module-scope constant
  `const DOUBLE_TYPE: &str = "DOUBLE";` next to the other module-level items and use it in
  `render_cast_target`'s `"DOUBLE" | "DOUBLE PRECISION"` arm (`Ok(DOUBLE_TYPE.to_string())`);
  (2) replace `render_float_div_datafusion` with an infallible single-argument pure wrapper
  `fn cast_to_double(expr_sql: &str) -> String { format!("CAST({expr_sql} AS {DOUBLE_TYPE})") }`
  placed where `render_float_div_datafusion` is now; (3) in the
  `"ADD" | "SUB" | "MULT" | "FLOAT_DIV"` arm, delete the
  `Ok(Some(match (fn_name.as_str(), dialect) { ... }))` block and instead rebind the left operand
  before the single shared assembly:
  `let left = match (fn_name.as_str(), dialect) { ("FLOAT_DIV", Dialect::DataFusion) => cast_to_double(&left), _ => left };`
  followed by the original `Ok(Some(format!("({left} {op} {right})")))`, so `/` and `DOUBLE` are each
  written exactly once and `op` is used on every path. Do not add a doc comment to `cast_to_double`
  (private fn) and do not change any rendered output string — every existing unit test assertion in
  `lib_tests.rs` must still pass byte-identically. Run
  `cargo test -p vs-expression float_div` and `cargo test -p vs-expression arithmetic` and show the
  output.

### crates/vs-expression/src/lib_tests.rs

#### [VAGUE_TEST_NAME] `arithmetic_operators_render_identically_in_both_dialects` now asserts a non-identity
- Location: lines 3626-3676
- Issue: the test's name states identity across dialects, but its body now asserts the opposite for
  `FLOAT_DIV` (`(CAST("A" AS DOUBLE) / 1)` vs `("A" / 1)`), and it needed a five-line comment to
  explain the contradiction. It also now covers two concepts in one test. Task 3.2's cited precedent,
  `cast_char_target_diverges_between_dialects`, is itself a *standalone* divergence test — the
  faithful reading is a second test, not an exception inside the identity guard.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, remove the `float_div` JSON value and its two
  assertions (lines 3659-3675) from `arithmetic_operators_render_identically_in_both_dialects`, and
  restore that test's doc comment to describe only `ADD`/`SUB`/`MULT`/`NEG` identity (drop the
  "`FLOAT_DIV` is the one exception …" sentences). Move the removed value and both assertions verbatim
  into a new adjacent test `fn float_div_casts_to_double_only_in_the_datafusion_dialect()`, placed
  directly after the identity test and modelled on `cast_char_target_diverges_between_dialects`. Do
  not weaken either assertion and do not delete the identity guard.

#### [DUPLICATE_TEST] `float_div_casts_column_left_operand_against_literal_right_operand` is byte-identical to `renders_arithmetic_div`
- Location: lines 462-476 (new test) vs lines 439-455 (`renders_arithmetic_div`)
- Issue: both build the exact same node (`FLOAT_DIV` over `{"type":"column","name":"a"}` and
  `{"type":"literal_exactnumeric","value":2}`) and assert the exact same string
  `(CAST("A" AS DOUBLE) / 2)`. Task 3.1 already repinned `renders_arithmetic_div` to that expectation,
  so task 3.3's column-left/literal-right matrix cell was covered before the new test was written.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, delete the test
  `float_div_casts_column_left_operand_against_literal_right_operand` in full. Keep
  `renders_arithmetic_div` and every other new `float_div_*` test unchanged.

#### [INLINE_COMMENT] Unmandated rationale comment added inside `arithmetic_operator_set_matches_advertised_capabilities`
- Location: lines 392-394
- Issue: beyond updating the pre-existing tuple legend (`// (capability name, node name = …, expected
  rendering)`), two new sentences were added — "FLOAT_DIV's expected string diverges from the
  ADD/SUB/MULT template because the DataFusion dialect casts its left operand to DOUBLE." Task 3.1
  mandated only the per-row expected string; the guardrails ban inline comments in test code as in
  production, and the fact is already stated in the module doc (`lib.rs:41-44`) and demonstrated by
  the literal expected strings on the very next lines.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, delete the two added comment lines
  ("FLOAT_DIV's expected string diverges …" through "… casts its left operand to DOUBLE.") and keep
  only the single updated legend line `// (capability name, node name = capability minus FN_, expected rendering)`.

### crates/lakehouse-engine/tests/e2e_scan_test.rs

#### [MAGIC_NUMBER] The filter-row-count oracle restates the seed layout instead of being grounded in the fixture
- Location: lines 4095-4119 (`e2e_float_div_filter_row_count_matches_native_oracle`)
- Issue: `oracle_count` comes from a hand-written two-row literal table
  (`SELECT 7 AS L_ORDERKEY, 1 … UNION ALL SELECT 7, 2`), then is compared against `COUNT(*)` over the
  real `FACT_LINEITEM` fixture. The literal `2` is therefore an unnamed, unverified restatement of the
  seed layout (`common/seed.rs`: `LINES_PER_ORDER = 2` and row `r`'s order key `((r - 1) / LINES_PER_ORDER) + 1`,
  so `L_ORDERKEY = 7` happens to hold exactly rows 13-14). If the seed's lines-per-order ever changes,
  the two sides no longer describe the same data and the test fails with the misleading message
  "truncated integer division silently drops a matching row" while nothing about FLOAT_DIV regressed.
  The synthetic table itself must stay — decision-log [7] records that a *literal* division is
  constant-folded differently by Exasol than a *column* division — but the row count it implies is
  never checked against the fixture.
- Fix: In `crates/lakehouse-engine/tests/e2e_scan_test.rs`'s
  `e2e_float_div_filter_row_count_matches_native_oracle`, keep the existing synthetic-table oracle
  query and its `assert_eq!(oracle_count, 2, …)`, and additionally ground it in the fixture before the
  pushed-down assertion: import `LINES_PER_ORDER` from `common::seed` (the module is already wired
  into this test crate), query
  `format!("SELECT COUNT(*) FROM {} WHERE L_ORDERKEY = 7", vs_lineitem_table())` via
  `conn.query_scalar_i64`, and assert that unfiltered count equals `LINES_PER_ORDER as i64` **and**
  equals `oracle_count`, with a message stating that the oracle's row count must describe the same
  fixture rows the pushed-down filter runs over. Leave the final
  `assert_eq!(vs_count, oracle_count, …)` assertion unchanged.

## Expert fixes

### crates/lakehouse-engine/tests/e2e_capability_test.rs

#### [MAGIC_NUMBER] Tolerances are absolute `1e-9` where the plan requires ~`1e-15` relative, and the `.max(1.0)` floor silently makes the "relative" check absolute
- Location: lines 4689-4703 (`e2e_float_div_decimal_over_int_matches_native_oracle`) and lines 4715-4741 (`e2e_float_div_decimal_over_decimal_matches_native_oracle`)
- Issue: four unnamed `1e-9` literals, none of them the tolerance the evidence supports.
  Task 1.1 and plan.md line 189 require "a **relative tolerance** (~1e-15), never string equality, for
  every decimal-numerator shape"; decision-log [7] measured the real divergence as at most 1 ULP —
  max absolute `4.44e-16`, max relative `3.17e-16`.
  The VS-vs-oracle comparison is written `<= 1e-9 * oracle_value.abs().max(1.0)`. The `.max(1.0)`
  floor means that for any oracle below 1.0 the bound collapses to a flat absolute `1e-9`: in the
  decimal/decimal test the oracle is `0.000102474999897525`, so the accepted error is `1e-9` on a
  `1.02e-4` value — a **relative** tolerance of ~`1e-5`, roughly ten orders of magnitude looser than
  specified, and it degrades further the smaller the value. It still catches today's scale-6
  truncation (`0.000102`, relative error `4.6e-3`), so the test is not vacuous, but it would silently
  accept a future regression that loses five significant digits — a passing test over wrong behaviour,
  which is precisely the failure mode these two tests exist to prevent. The oracle sanity checks have
  the mirror-image defect: a flat `1e-9` against `0.000102474999897525` is a ~`1e-5` relative check on
  a constant that is known to the last digit.
  Tightening to a pure relative `1e-15` is safe on both sides and not flaky: the hand-written
  constants are the correctly-rounded doubles, and decision-log [7]'s measured worst case
  (`3.17e-16`) leaves ~3x headroom.
- Fix: In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, add one module-scope named constant
  for the tolerance — `const FLOAT_DIV_ORACLE_REL_TOLERANCE: f64 = 1e-15;` with a doc comment citing
  decision-log [7]'s measured max relative difference of `3.17e-16` (~1 ULP) as its basis — and
  replace all four `1e-9` comparisons in
  `e2e_float_div_decimal_over_int_matches_native_oracle` and
  `e2e_float_div_decimal_over_decimal_matches_native_oracle` with a purely relative form:
  `(actual - expected).abs() <= FLOAT_DIV_ORACLE_REL_TOLERANCE * expected.abs()`. Delete the
  `.max(1.0)` floor entirely — do not replace it with any other floor. Keep both hand-written oracle
  constants (`5.855714285714286`, `0.000102474999897525`) and every assertion message as they are, and
  keep `e2e_float_div_int_over_int_matches_native_oracle`'s bit-exact `assert_eq!` untouched (task 1.1
  permits bit-exactness for a scale-0 numerator). Both `expected` values are asserted non-zero by the
  surrounding oracle checks, so no zero-denominator guard is needed; do not add one. Re-run both tests
  against the live local stack (`docker compose up -d --wait minio minio-init iceberg-rest exasol`
  first — `make test-e2e` does not start it) and show that both still pass with the tightened bound.
