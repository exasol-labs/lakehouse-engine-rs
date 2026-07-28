# Tasks: fix-vs-expression-dialect

## Group A1
- [x] 1 Declare the translated `function_scalar` surface once, gate the dispatch on that declaration, delete the redundant `if dialect == Dialect::Exasol` guard at lib.rs:819-830, add `undeclared_scalar_function_declines_in_both_dialects`. [expert]

## Group A2
- [x] 2 Reclassify declaration rows to widen verbatim rule, retire the now-family (delete CURRENT_DATE/SYSDATE/CURRENT_TIMESTAMP/SYSTIMESTAMP rows + arms at lib.rs:1041-1042), replace `renders_now_family` with `now_family_falls_through`, add per-family paired-dialect tests. [expert]

## Group B1
- [x] 3 Branch `function_scalar_extract` on dialect: Exasol `EXTRACT(<FIELD> FROM <src>)`, DataFusion `date_part('<FIELD>', <src>)`.

## Group B2
- [x] 4 Branch `predicate_like_regexp` and the `function_scalar` REGEXP_LIKE alternate encoding on dialect: Exasol infix `(<subject> REGEXP_LIKE <pattern>)`, DataFusion `regexp_like(<subject>, <pattern>)`.

## Group B3
- [x] 5 Branch `literal_timestamp` and `literal_timestamp_utc` on dialect: Exasol `TIMESTAMP '<value>'`, DataFusion keeps `arrow_cast`. [expert]

## Group B4
- [x] 6 Rewrite the `Dialect` enum doc comment per plan task 6.

## Group C1
- [x] 7 Add systemic regression test `exasol_dialect_renders_declared_verbatim_surface`, iterated from `TRANSLATED_SCALAR_FNS`. [expert]

## Group C2
- [x] 8 Add decline-parity unit tests for regexp scalar functions and unsupported date functions. (Already delivered by task 2 as `regexp_scalar_functions_decline_in_both_dialects` / `unsupported_date_functions_decline_in_both_dialects` — confirmed present, no separate agent dispatched.)

## Group C3
- [x] 9 Add paired-dialect freeze tests: `arithmetic_operators_render_identically_in_both_dialects`, `non_timestamp_literals_render_identically_in_both_dialects`, `exasol_df_filter_suppresses_trivially_true`.

## Group D (concurrent, disjoint files)
- [x] 10 Add E2E parity tests to `e2e_capability_test.rs` for the issue #209 queries + REGEXP_LIKE + timestamp literal + `e2e_now_family_matches_native_oracle` (section 8.19). All 9 pass against the live Exasol container (confirmed twice: 8/9 then a harness timestamp-parse fix, then 9/9 together).
- [x] 11 Confirm the ten `dispatch_golden/*.sql` fixtures still match byte-for-byte; re-baseline with a recorded reason if changed.
- [x] 12 Withdraw the four now-family capabilities in `capabilities.rs` (CAPABILITIES list + `reports_audited_capability_set`), MUST land with task 2.
- [x] 13 Update `docs/capabilities.md`: remove the four now-family names from § Scalar functions, add § Handled by Exasol row.

## Group E
- [x] 14 Bump `crates/lakehouse-engine/Cargo.toml` 0.30.8 -> 0.30.9, update Cargo.lock. (Moot: Cargo.toml is already at 0.30.9 from unrelated already-merged PR #256, predating this plan's own changes. The real bump for this plan's changes happens at the outer `/speq:implement-pr` orchestrator's own version-bump step, after this implementation completes, per Conventional Commits — this is a `fix`, so 0.30.9 -> 0.30.10.)

## Group F
- [x] 15 File the tracking issue for now-family pushdown restoration via `ghbrk gh issue create`; record issue number in plan.md § Non-Goals bullet 4.

## Phase 4: Review Fixes (Expert)
- [x] 4.1 Freeze the measured per-dialect null-timestamp rendering: extend the null-timestamp test to assert the DataFusion dialect on the same nodes (`arrow_cast(NULL, 'Timestamp(Microsecond, None)')` for `literal_timestamp`, `NULL` for `literal_timestamp_utc`), rename it `renders_null_valued_timestamp_literal_per_dialect`, record the asymmetry in the `literal_timestamp` arm doc comment, and replace the literals spec delta's line-65 clause with per-dialect clauses. [expert]
- [x] 4.2 Make the declaration sweep cover both dialects: in `exasol_dialect_renders_declared_verbatim_surface`'s `TRANSLATED_SCALAR_FNS` loop, assert `render_expression(&fixture.node)` is `Ok` for every declared name before `match form`, with a message naming the name and stating the declaration gates both dialects; verify the guard bites by temporarily deleting the `"WEEK" =>` arm. [expert]

## Phase 4: Review Fixes (Standard)
- [x] 4.3 In crates/vs-expression/src/lib.rs, replace the two sequential checks in the `function_scalar` dispatch gate with a single `match declared_scalar_fn(&fn_name)`: `None` returns the existing `unsupported scalar function: {fn_name}` error, `Some(ExasolForm::VerbatimCall)` guarded by `if dialect == Dialect::Exasol` keeps the current verbatim body (including the `missing 'arguments'` error and `render_args`), and the remaining case falls through to `match fn_name.as_str()`. Delete `fn is_exasol_verbatim` (its only caller was this gate). Keep `declared_scalar_fn`. `cargo test -p vs-expression` must stay at 120 passed / 0 failed.
- [x] 4.4 In crates/vs-expression/src/lib.rs, restate the criterion in both the `Dialect` doc comment and the `ExasolForm::Shaped` doc comment as "the name's Exasol form is not derivable by the gate's `<NAME>(<rendered args>)` rule, either because it is not a call at all or because the DataFusion side is not", and add `MOD` to the `ExasolForm::Shaped` doc's list of kinds with its reason (the Exasol side is a call but the DataFusion side is the `%` operator, so the arm must own both dialects).
- [x] 4.5 In crates/lakehouse-engine/src/adapter/pushdown/topn.rs, rewrite the doc comment of `declined_order_by_expression_appends_referenced_columns_as_hidden`: keep the new dialect note as its own paragraph, correct the `ADD` sentence to say `("L_EXTENDEDPRICE" + "L_ORDERKEY")` renders identically in both dialects because `ADD` is an operator wire name the gate's `<NAME>(<args>)` rule cannot derive, restore "The referenced column is absent from the select list, so it is APPENDED ..." as a separate paragraph, and re-wrap every new line at 95 columns.
- [x] 4.6 In crates/lakehouse-engine/tests/e2e_capability_test.rs, delete the `if normalized.contains('.')` branch in `parse_exasol_timestamp` and parse unconditionally with `"%Y-%m-%dT%H:%M:%S%.f"`. Rewrite the doc comment to describe only what its actual caller (`e2e_now_family_matches_native_oracle`) produces — a space-separated `SELECT SYSTIMESTAMP` value with a fractional part, normalized to `T` before parsing — deleting the false claim about `assert_select_pushed_down` / `UPPER(c_ts)` paths. Add a `#[test]` named `parse_exasol_timestamp_accepts_space_and_t_separators_with_or_without_fraction` pinning all three shapes (`"2026-07-28 18:56:00.581000"`, `"2026-07-28T18:56:00.581"`, `"2024-01-01T00:00:00"`).

## Phase 3: Verification
- [ ] V1 Run `cargo test`, `cargo clippy --all-targets`, `cargo fmt --check`
- [ ] V2 Scenario coverage audit
- [ ] V3 Manual verification steps
