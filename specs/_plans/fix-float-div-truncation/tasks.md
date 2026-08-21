# Tasks: fix-float-div-truncation

> Planning already reproduced every facet of the bug against the local Docker Exasol container
> (`exasol/docker-db:2025.2.1`, `MY_LAKEHOUSE` VS, pre-fix `.so`) and proved the fix shape end-to-end
> with a user-side `CAST(... AS DOUBLE)`. Phase 1 turns those captures into failing tests and closes
> the two gaps planning could not. See `plan.md` § Call-Site Census before editing any file — line
> numbers there were located during planning and may drift.

## Phase 1: Failing E2E repro and the remaining live measurements (Group A)

- [x] 1.1 Bring up the local stack (`docker compose up -d --wait minio minio-init iceberg-rest
      exasol` — `make test-e2e` does NOT start it; check for a stray `bench/.env` first). Add E2E
      tests that FAIL against the current `.so`, each against a native-table oracle: **int/int** and
      the **filter row count** over `FACT_LINEITEM` (`L_ORDERKEY`/`L_LINENUMBER`, both
      `DECIMAL(20,0)`, seeded `L_ORDERKEY=7, L_LINENUMBER ∈ {1,2}`) in `tests/e2e_scan_test.rs`;
      **decimal/int** and **decimal/decimal** over `TYPED_DISTINCT_PROBE` (`C_DECIMAL_A`
      `DECIMAL(9,2)`, `C_DECIMAL_B` `DECIMAL(20,4)`) in `tests/e2e_capability_test.rs`, where that
      fixture is wired. Assert a **relative tolerance** (~1e-15), never string equality, for every
      decimal-numerator shape; a scale-0 numerator may be asserted bit-exact. Expected pre-fix
      failures: `3.0` vs `3.5`, count `1` vs `2`, `5.855714` vs `5.855714285714286`, `0.000102` vs
      `0.000102474999897525`.
- [x] 1.2 Measure the two cases planning could not, and record them in `decision-log.md`:
      a divide-by-zero in a **PREDICATE** position (`WHERE <a> / <b> > <k>`), and the same shape
      through the **join/broadcast** pushdown path. An infinity compared against a bound is consumed
      inside DataFusion and never reaches the emit boundary, so neither the engine's `inf` rejection
      nor `#246`'s NaN check can catch it — this is the one place a silent wrong row count could
      survive the fix. Use `(col - col)` as the divisor (no Iceberg fixture has a zero-valued column)
      and verify with `EXPLAIN VIRTUAL` that the predicate really is pushed. [expert]

## Phase 2: The rendering change (Group B)

- [x] 2.1 `crates/vs-expression/src/lib.rs`: split `FLOAT_DIV` out of the shared
      `format!("({left} {op} {right})")` in the `"ADD" | "SUB" | "MULT" | "FLOAT_DIV"` arm so the
      DataFusion dialect emits `(CAST(<left> AS DOUBLE) / <right>)` and the Exasol dialect keeps the
      bare form. Keep ONE arm — the arity check, operand rendering and error messages are shared, so
      only the final assembly branches. Take the `DOUBLE` spelling from the existing
      `render_cast_target` mapping (its `"DOUBLE" | "DOUBLE PRECISION" => "DOUBLE"` arm is
      dialect-invariant) rather than hardcoding a second copy, and follow
      `format_decimal_exasol_style`'s colocated-pure-helper shape. Refresh the operator-list comments
      in the module doc and above the arm.

## Phase 3: Repin the pinned renderings (Group C)

- [x] 3.1 `crates/vs-expression/src/lib_tests.rs`:
      `arithmetic_operator_set_matches_advertised_capabilities` — the shared `format!` template no
      longer holds for all four rows, so carry an expected string per row instead of just an
      operator; `renders_arithmetic_div` — expect `(CAST("A" AS DOUBLE) / 2)`.
- [x] 3.2 `crates/vs-expression/src/lib_tests.rs`: retarget
      `arithmetic_operators_render_identically_in_both_dialects` — keep it asserting identity for
      `ADD`/`SUB`/`MULT` and add the `FLOAT_DIV` **divergence** assertion (DataFusion casts, Exasol
      does not), following `cast_char_target_diverges_between_dialects`. Amend its doc comment; do
      NOT delete the guard.
- [x] 3.3 Add DataFusion-dialect unit tests for the operand matrix and NULL propagation: left operand
      a column / a literal / a nested expression / an aggregate, each right-operand shape, and NULL
      on either side plus NULL-over-zero. Assert the rendered string only — pure, I/O-free
      renderings.

## Phase 4: Prove the Exasol dialect did not move (Group D)

- [x] 4.1 Confirm — do NOT update — the four Exasol-dialect sites that must stay byte-identical:
      `lib_tests.rs`'s `exasol_dialect_renders_declared_verbatim_surface` `FLOAT_DIV` fixture
      (`("A" / "B")`), `scalar_over_agg_tests.rs`'s
      `render_substitutes_the_callers_merged_expressions_by_plan_slot`,
      `single_group_agg_tests.rs`'s
      `merge_select_interleaves_items_in_selectlist_order_with_per_item_casts`, and both
      `testdata/dispatch_golden/single_group_scalar_over_aggregate_{dedup,interleaved}.sql`. A diff in
      any of these means the change leaked into the Exasol dialect and is a regression. Also confirm
      the AVG and König–Huygens merge `/` fragments in `scalar_over_agg.rs` and `support_tests.rs`'s
      `sql.contains(" / ")` are untouched — those are adapter-authored, not translator output.

## Phase 5: Close the evidence gaps and pin the divergences (Group E)

- [x] 5.1 Confirm the fix from the **translator's own output**, not planning's user-side-CAST proxy:
      assert via `EXPLAIN VIRTUAL` that the pushed projection for a bare `<a> / <b>` select item is
      `(CAST("L_ORDERKEY" AS DOUBLE) / "L_LINENUMBER")` with
      `"emit_exa_types":["DOUBLE PRECISION"]`, and re-run task 1.1's tests against a freshly built
      `.so` so they pass.
- [x] 5.2 Add the two divide-by-zero **characterization** tests, each with a doc comment saying it
      pins a known divergence: `x/0` projected — the query FAILS at `22002` with
      `numeric value out of range: value inf` (native Exasol fails at `22012`); `0/0` projected — the
      query SUCCEEDS with a silent NULL, citing `#246` as the owning gap and noting that the
      partial-aggregate path errors on the same input via `arrow_value_at`. Use `(col - col)` as the
      divisor.
- [x] 5.3 Re-run the two existing native-oracle scalar-over-aggregate E2E tests
      (`e2e_scan_test.rs`'s `..._shared_count_matches_native_oracle` and `..._interleaved_...`) and
      `e2e_join_test.rs`'s two `RETURN_PCT` string-equality tests. Under this design their SQL is
      unchanged, so they must pass **unedited**.

## Phase 6: Track what remains (Group F)

- [x] 6.1 Act on task 1.2's measurement. If the predicate-position or join-path divide-by-zero
      diverges silently, file a GitHub issue scoped to exactly that and replace the placeholder clause
      in the spec delta with its number; if it does not, replace that clause with the verified-safe
      statement. Do NOT file a second issue for the `0/0` silent NULL — `#246` already owns the
      raw-scan NaN-at-emit gap and this feature only widens its reachability.

## Phase 8: Review Fixes

- [x] 8.1 `lib.rs`: deduplicate the `/` operator spelling and replace the JSON-round-trip
      `render_float_div_datafusion` helper with an infallible `cast_to_double` pure wrapper using a
      shared `DOUBLE_TYPE` constant; rebind `left` before the single shared `format!` assembly instead
      of branching the whole assembly. [standard]
- [x] 8.2 `lib_tests.rs`: split the FLOAT_DIV divergence assertion out of
      `arithmetic_operators_render_identically_in_both_dialects` into its own
      `float_div_casts_to_double_only_in_the_datafusion_dialect` test; restore the identity test's doc
      comment to ADD/SUB/MULT/NEG only. [standard]
- [x] 8.3 `lib_tests.rs`: delete the duplicate test
      `float_div_casts_column_left_operand_against_literal_right_operand` (byte-identical coverage to
      `renders_arithmetic_div`). [standard]
- [x] 8.4 `lib_tests.rs`: delete the two unmandated rationale comment lines in
      `arithmetic_operator_set_matches_advertised_capabilities`, keep only the updated legend line.
      [standard]
- [x] 8.5 `e2e_scan_test.rs`: ground `e2e_float_div_filter_row_count_matches_native_oracle`'s
      synthetic-table oracle row count against the real `FACT_LINEITEM` fixture via `LINES_PER_ORDER`,
      not just an unverified literal `2`. [standard]
- [x] 8.6 `e2e_capability_test.rs`: replace the four `1e-9`-with-`.max(1.0)`-floor tolerance checks in
      the decimal-over-int and decimal-over-decimal oracle tests with a named
      `FLOAT_DIV_ORACLE_REL_TOLERANCE: f64 = 1e-15` purely-relative comparison; re-run both against the
      live stack. [expert]

## Phase 7: Verification (Group G)

- [x] 7.1 `cargo test` — 0 failures.
- [x] 7.2 `cargo clippy --all-targets && cargo fmt` — 0 warnings, no reformatting.
- [x] 7.3 `make cross-musl-udf-build` then `make test-e2e` against the live local stack — 0 failures.
