# Tasks: fix-greatest-least-null-semantics

## Phase 2: Implementation (Group A)
- [x] 1.1 Render the DataFusion-dialect `GREATEST`/`LEAST` as a NULL-guarded `CASE` in the
      `"GREATEST" | "LEAST"` arm of `render_expression_inner`
      (`crates/vs-expression/src/lib.rs:1289-1300`), keeping the existing missing-`arguments` and
      empty-argument-list errors and calling `render_args` exactly once so each argument's rendered
      text is reused in its `IS NULL` clause and in the call. Update the two existing tests in
      `crates/vs-expression/src/lib_tests.rs` that assert the old bare rendering —
      `renders_greatest_least` (`:1623-1648`) and the two `render_expression` assertions inside
      `renders_greatest_least_verbatim_in_exasol_dialect` (`:2951-2988`) — and add unit tests for:
      the multi-argument guard's exact SQL text, the one-argument degenerate guard, a `literal_null`
      argument (whose `NULL IS NULL` clause makes the whole expression NULL, as Exasol's
      `LEAST(x, y, NULL)` does), a nested `function_scalar` argument rendered once and referenced
      twice identically, and the empty-argument-list error. Acceptance: exact rendered strings
      asserted, not `.contains(...)` probes; the two `render_expression_exasol` assertions in
      `renders_greatest_least_verbatim_in_exasol_dialect` stay BYTE-IDENTICAL and
      `exasol_dialect_renders_declared_verbatim_surface` passes with no edit; `capabilities.rs` not
      touched; `cargo test -p vs-expression` green.
- [x] 1.2 Correct the false Exasol `GREATEST` NULL-contract claim wherever it is recorded in code:
      `stddev_of`'s and `merge_select_items`' doc comments in
      `crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs` (the
      `Exasol's GREATEST(0.0, NULL) = 0.0` sentence at `:393-396` and the `stddev_of` doc at
      `:366-371`), and the doc comments of `stddev_pop_merge_null_passthrough_for_n_zero` and
      `stddev_samp_merge_null_passthrough_for_n_zero_and_n_one` in
      `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` (`:2933-2936`,
      `:2957-2960`). State the live-captured contract — Exasol returns NULL if ANY argument is NULL,
      so `SQRT(GREATEST(0.0, NULL))` is already NULL — and give the retained `CASE WHEN … IS NULL`
      guard its honest reason. Acceptance: no comment, test name, or doc string in the repository
      claims Exasol's `GREATEST` skips NULLs; ZERO characters of generated SQL change — every
      `testdata/dispatch_golden/` fixture is byte-identical and all six `.contains(...)` merge tests
      pass with no edit to any expected value.

## Phase 2: Implementation (Group B)
- [x] 2.1 Add a live E2E regression test for issue #202 to
      `crates/lakehouse-engine/tests/e2e_scan_test.rs`, following the file's existing
      `setup_e2e()` / `exa_conn()` / `vs_table()` / `explain_virtual_sql` conventions and deriving a
      NULL for some rows only via `NULLIF(MOD(id, 5), 0)`. Assert the predicate position —
      `WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL` returns exactly 4 of the 20 seeded rows, where
      the unguarded rendering returns 0 — and the value position —
      `SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) … ORDER BY id` returns NULL for exactly the
      four multiples of 5 and the row's own `id` for the other sixteen. Assert via
      `explain_virtual_sql` that the guarded form reached the scan spec. Acceptance: the fixture is
      discriminating in both directions and assertions are exact values, not row counts alone; the
      test FAILS rather than skips with no reachable Exasol container; the doc comment records the
      pre-fix values so the regression stays legible.

## Phase 3: Verification
- [x] 3.1 Run test suite
- [x] 3.2 Run linter
- [x] 3.3 Run formatter check
- [x] 3.4 Scenario coverage audit
- [x] 3.5 Manual verification (documented commands)

## Phase 4: Review Fixes
- [x] 4.1 In `crates/vs-expression/src/lib_tests.rs`, add a test named
      `greatest_least_without_arguments_key_errors` next to
      `renders_greatest_least_empty_argument_list_errors` (after line 1727). For each `name` in
      `["GREATEST", "LEAST"]`, build `json!({"type": "function_scalar", "name": name})` — the
      `arguments` key OMITTED entirely, not an empty array — then assert `render_expression(&expr)`
      is `Err` and that the error string contains `missing 'arguments'`, and assert
      `render_expression_safe(&expr)` is `None`. Run `cargo test -p vs-expression` and show it
      green.
- [x] 4.2 In `crates/vs-expression/src/lib_tests.rs`, add a test named
      `renders_nested_greatest_guard_referencing_the_inner_case_twice` after
      `renders_greatest_least_nested_argument_once_referenced_twice` (line 1710). Build
      `json!({"type": "function_scalar", "name": "GREATEST", "arguments": [{"type":
      "function_scalar", "name": "GREATEST", "arguments": [{"type": "column", "name": "a"},
      {"type": "column", "name": "b"}]}, {"type": "column", "name": "c"}]})` and `assert_eq!`
      `render_expression(&expr).unwrap()` against the exact expected string (no `.contains(...)`
      probe) — the inner `CASE WHEN "A" IS NULL OR "B" IS NULL THEN NULL ELSE greatest("A", "B")
      END` appearing once in the outer guard's first `IS NULL` clause and once as the outer call's
      first argument. Run `cargo test -p vs-expression` and show it green.
