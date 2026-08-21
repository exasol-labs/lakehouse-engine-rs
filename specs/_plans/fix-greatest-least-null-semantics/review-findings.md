# Code Review Findings: fix-greatest-least-null-semantics

## Summary
- Files reviewed: 5
- Total findings: 2 (standard: 2, expert: 0)

Verified clean (no finding raised):
- `crates/vs-expression/src/lib.rs:1300-1308` calls `render_args` exactly ONCE and reuses the
  resulting `Vec<String>` for both the `IS NULL` guard chain and the call argument list — no
  duplicate expression walk.
- The Exasol dialect is untouched: `GREATEST`/`LEAST` keep `ExasolForm::VerbatimCall`
  (`lib.rs:210-211`) and the gate at `lib.rs:986-992` returns ahead of the per-name `match`, so the
  guard cannot reach the Exasol path. Both `render_expression_exasol` assertions in
  `renders_greatest_least_verbatim_in_exasol_dialect` are byte-identical in the diff, and
  `exasol_dialect_renders_declared_verbatim_surface` is unedited.
- `crates/lakehouse-engine/src/adapter/capabilities.rs` is not modified (`git status` shows exactly
  5 modified files); `FN_GREATEST`/`FN_LEAST` stay advertised.
- `scalar_over_agg.rs` and `grouped_agg_tests.rs` changed doc comments ONLY — `stddev_of`'s
  `format!` body, `merge_select_items`' arms, and every existing `.contains(...)` assertion and
  expected value are untouched; `testdata/dispatch_golden/*.sql` are byte-identical (the only
  `GREATEST` occurrences there are the Exasol-side `GREATEST(0.0, (SUM("PARTIAL_stat_sumsq_*")…`
  merge clamps, unaffected by the DataFusion-dialect change). The corrected claim itself is sound:
  `SQRT(GREATEST(0.0, NULL))` is NULL under Exasol's any-argument-NULL contract, so the retained
  `CASE` guard is genuinely redundant-but-pinned.
- The new E2E test asserts exact values, not row counts alone: it walks all 20 rows and asserts
  `greatest_val.is_null()` at each multiple of 5 and `greatest == id` at the other sixteen, on top of
  the `COUNT(*) == 4` predicate assertion. It fails rather than skips with no container — `setup_e2e()`
  calls the panicking `wait_for_exasol()` / `wait_for_minio()` / `wait_for_iceberg_catalog()` helpers,
  and `exa_conn()` panics on connect failure.
- `cargo test -p vs-expression` (140 passed, 0 failed), `cargo test -p lakehouse-engine --lib`
  (1189 passed, 0 failed), `cargo clippy --workspace --all-targets -- -D warnings` (clean), and
  `cargo fmt --all -- --check` (clean) all pass on the working tree.

## Standard fixes

### crates/vs-expression/src/lib_tests.rs

#### [UNTESTED_ERROR_PATH] Absent `arguments` key on GREATEST/LEAST has no test
- Location: `crates/vs-expression/src/lib_tests.rs:1712-1727` (`renders_greatest_least_empty_argument_list_errors`); production path `crates/vs-expression/src/lib.rs:1290-1292`
- Issue: the CHANGED scenario mandates two distinct error paths — "a node whose `arguments` key is
  absent SHALL return an error, and an EMPTY argument list SHALL return an error in raising mode and
  `None` in the safe variants". Only the empty-list half is covered. The `args.ok_or_else(…)` arm at
  `lib.rs:1290` that raises `function_scalar {fn_name} missing 'arguments'` has no test anywhere in
  the crate: `grep -n "missing 'arguments'\|missing_arguments\|no_arguments\|without_arguments"
  crates/vs-expression/src/lib_tests.rs` returns nothing. The arm survived this change untested, so a
  future edit that replaced it with `unwrap_or_default()` would silently turn a malformed node into a
  successful render of an empty guard rather than a `UdfError::User`.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, add a test named
  `greatest_least_without_arguments_key_errors` next to
  `renders_greatest_least_empty_argument_list_errors` (after line 1727). For each `name` in
  `["GREATEST", "LEAST"]`, build `json!({"type": "function_scalar", "name": name})` — the
  `arguments` key OMITTED entirely, not an empty array — then assert `render_expression(&expr)` is
  `Err` and that the error string contains `missing 'arguments'`, and assert
  `render_expression_safe(&expr)` is `None`. Run `cargo test -p vs-expression` and show it green.

#### [MISSING_BOUNDARY_TEST] No test pins a GREATEST nested inside a GREATEST, where the guard's text duplication compounds
- Location: `crates/vs-expression/src/lib_tests.rs:1690-1710` (`renders_greatest_least_nested_argument_once_referenced_twice`); production path `crates/vs-expression/src/lib.rs:1300-1308`
- Issue: the only nested-argument test uses `ABS("Y")` — a nested call whose own rendering contains no
  duplication. The guard emits each rendered argument twice
  (`format!("CASE WHEN {guard} THEN NULL ELSE {df_name}({}) END", rendered.join(", "))`, where `guard`
  is built from the same `rendered` vec), so a `GREATEST`/`LEAST` nested inside a `GREATEST`/`LEAST` is
  the one shape where the duplication compounds: for `GREATEST(GREATEST(a, b), c)` the inner
  `CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END` appears twice in the
  outer rendering, so rendered text grows 2^depth in nesting depth. That growth lands in a scan-spec
  string that crosses the UDF boundary as a bounded `VARCHAR(2000000)` parameter, and no test records
  the shape at all — the boundary case is invisible to the suite. The spec's clause "each argument
  SHALL be rendered ONCE and its rendered SQL text referenced TWICE" is asserted only for a
  non-nesting argument.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, add a test named
  `renders_nested_greatest_guard_referencing_the_inner_case_twice` after
  `renders_greatest_least_nested_argument_once_referenced_twice` (line 1710). Build
  `json!({"type": "function_scalar", "name": "GREATEST", "arguments": [{"type": "function_scalar",
  "name": "GREATEST", "arguments": [{"type": "column", "name": "a"}, {"type": "column", "name": "b"}]},
  {"type": "column", "name": "c"}]})` and `assert_eq!` `render_expression(&expr).unwrap()` against the
  exact expected string (no `.contains(...)` probe) — the inner
  `CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B") END` appearing once in the
  outer guard's first `IS NULL` clause and once as the outer call's first argument. Run
  `cargo test -p vs-expression` and show it green.

## Expert fixes
[none]
