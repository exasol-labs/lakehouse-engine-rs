# Code Review Findings: fix-vs-expression-dialect

## Summary
- Files reviewed: 8
- Total findings: 6 (standard: 4, expert: 2)

Verified clean, no findings raised:

- `TRANSLATED_SCALAR_FNS` is exactly 76 rows (66 `VerbatimCall`, 10 `Shaped`) and maps
  **1:1** onto the `function_scalar` dispatch arms — 0 declared names without an arm,
  0 arm names without a declaration row (checked mechanically over `lib.rs:105-237`
  against `lib.rs:909-1405`).
- The now-family withdrawal is total and paired: no `FN_CURRENT_DATE` /
  `FN_CURRENT_TIMESTAMP` / `FN_SYSDATE` / `FN_SYSTIMESTAMP` remains in
  `CAPABILITIES`, all four are in `reports_audited_capability_set`'s negative loop,
  the two translator arms are deleted, and `grep -rn` over `docs/` returns only the
  new § Handled by Exasol row.
- The five dialect-branching node types the `Dialect` doc names
  (`function_scalar_extract`, `function_scalar_cast`, `predicate_like_regexp`,
  `literal_timestamp`, `literal_timestamp_utc`) are the complete set of non-`function_scalar`
  dialect reads in the file — no sixth branch is undocumented or unswept.
- Task 2's five consumer-side assertion fixes in `topn.rs` / `grouped_agg.rs` are
  correct and complete: the surrounding assertion *messages* already said `ROUND` /
  `ABS`, so the change aligned the fixtures with pre-existing intent. Tasks 3/4/5
  introduced no new instances of that staleness class — `cargo test -p lakehouse-engine
  --lib` = 671 passed / 0 failed, `cargo test -p vs-expression` = 120 passed / 0 failed,
  `cargo clippy --all-targets --features exasol-e2e` = 0 warnings, `cargo fmt --check`
  clean. The remaining lowercase `date_part` / `character_length` / `strpos` /
  `regexp_like(` assertions in `support.rs`, `single_group_agg.rs`, `mod.rs` and
  `e2e_capability_test.rs:1498-1514` all sit on DataFusion-dialect scan-spec paths and
  are still correct.
- Task 15's tracking issue exists and matches the number recorded in plan.md
  § Non-Goals: #263, OPEN, "Restore now-family pushdown for CURRENT_DATE,
  CURRENT_TIMESTAMP, SYSDATE, SYSTIMESTAMP".

## Standard fixes

### crates/vs-expression/src/lib.rs

#### [SHRINKABLE] The gate looks the same name up in the declaration twice, through a one-caller wrapper
- Location: lines 249-251 (`is_exasol_verbatim`), 892 and 901 (the gate)
- Issue: the gate runs two separate lookups against `TRANSLATED_SCALAR_FNS` for one
  name — `declared_scalar_fn(&fn_name).is_none()` at line 892, then
  `is_exasol_verbatim(&fn_name)` at line 901, which re-enters `declared_scalar_fn` and
  re-scans the 76-row table. `is_exasol_verbatim` exists for that single call site
  (confirmed: its only reference is line 901) and its whole body is a `matches!` over
  the other reader's result, so it is a wrapper that hides no decision. One `match` on
  a single lookup expresses both branches and deletes the helper.
- Fix: In crates/vs-expression/src/lib.rs, replace the two sequential `if` checks at
  lines 892-907 with a single `match declared_scalar_fn(&fn_name)`: the `None` arm
  returns the existing `unsupported scalar function: {fn_name}` error, the
  `Some(ExasolForm::VerbatimCall)` arm guarded by `if dialect == Dialect::Exasol`
  keeps the current verbatim body verbatim (including the `function_scalar {fn_name}
  missing 'arguments'` error and `render_args`), and the remaining arm falls through to
  `match fn_name.as_str()`. Then delete `fn is_exasol_verbatim` (lines 248-251) since
  it has no other caller. Keep `declared_scalar_fn` — the sweep test at line 4646 uses
  it. Do not change any rendered string; `cargo test -p vs-expression` must stay at 120
  passed / 0 failed as the refactor's proof.

#### [OUTDATED_COMMENT] The `Dialect` and `ExasolForm::Shaped` doc comments both misdescribe why MOD is Shaped
- Location: lines 38-43 (`Dialect` doc, "Six constructs fall outside the `<NAME>(<args>)` shape") and lines 86-89 (`ExasolForm::Shaped` doc)
- Issue: the `Dialect` doc introduces the six constructs as those that "fall outside
  the `<NAME>(<args>)` shape", and the `ExasolForm::Shaped` doc says the variant is
  "for the names whose Exasol form is not a `<NAME>(<args>)` call at all — an operator,
  an infix predicate, a `CASE`, or a per-dialect CAST target". `MOD`'s Exasol form IS
  `MOD(a, b)` — a plain `<NAME>(<args>)` call (line 1059) — and it appears in neither
  of the `Shaped` doc's four listed kinds. The declaration's own inline comment at lines
  119-122 states the actual reason ("The Exasol side happens to be a call, but the
  DataFusion side is not, so the arm owns both dialects rather than the gate owning one
  of them"). A reader who learns the rule from the two doc comments they meet first is
  told something the declaration then contradicts, and would conclude MOD cannot be
  `Shaped`.
- Fix: In crates/vs-expression/src/lib.rs, restate the criterion in both doc comments
  as "the name's Exasol form is not derivable by the gate's `<NAME>(<rendered args>)`
  rule, either because it is not a call at all or because the DataFusion side is not",
  and add `MOD` to the `ExasolForm::Shaped` doc's list of kinds (lines 86-89) with its
  reason: the Exasol side is a call but the DataFusion side is the `%` operator, so the
  arm must own both dialects.

### crates/lakehouse-engine/src/adapter/pushdown/topn.rs

#### [OUTDATED_COMMENT] The new doc paragraph states a false claim about ADD and runs into the pre-existing sentence
- Location: lines 952-958, specifically 954-956
- Issue: three defects in one inserted paragraph. (a) "`ADD` is one of the ten names
  whose shape, not just its name, differs between the two" is false: `ADD` renders
  byte-identically as `("A" + "B")` in both dialects — that invariance is what this
  same change's `arithmetic_operators_render_identically_in_both_dialects`
  (`vs-expression/src/lib.rs`) exists to freeze. `ADD` is `Shaped` because the gate's
  `<NAME>(<args>)` rule cannot derive an operator, not because the two dialects
  disagree. (b) the new text runs straight into the pre-existing sentence
  ("... between the two. The referenced column is absent from the select list ...")
  mid-line, fusing two unrelated explanations into one paragraph. (c) line 956 is 110
  characters, against the ~95-column wrap the rest of the file uses; `cargo fmt` does
  not rewrap doc comments, so this does not self-correct.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/topn.rs, rewrite the doc comment
  of `declined_order_by_expression_appends_referenced_columns_as_hidden`: keep the new
  dialect note as its own paragraph, correct the `ADD` sentence to say that
  `("L_EXTENDEDPRICE" + "L_ORDERKEY")` renders identically in both dialects because
  `ADD` is an operator wire name the gate's `<NAME>(<args>)` rule cannot derive, restore
  "The referenced column is absent from the select list, so it is APPENDED ..." as a
  separate paragraph, and re-wrap every new line at 95 columns.

### crates/lakehouse-engine/tests/e2e_capability_test.rs

#### [DEAD_FLEXIBILITY] `parse_exasol_timestamp` carries a dead format branch and a doc comment naming callers that do not exist
- Location: lines 3098-3118 (doc comment and body), branch at lines 3112-3116
- Issue: two defects. (a) the doc comment attributes one accepted shape to "a value read
  back through this file's `assert_select_pushed_down` / `UPPER(c_ts)`-style paths" —
  neither calls this function; its only caller is
  `e2e_now_family_matches_native_oracle`, whose two inputs both come from
  `SELECT SYSTIMESTAMP` over the same driver and therefore have one identical shape.
  (b) the `if normalized.contains('.')` branch is unreachable and unnecessary: chrono's
  `%.f` already accepts an absent fractional part, measured directly —
  `NaiveDateTime::parse_from_str("2024-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S%.f")` →
  `Ok(2024-01-01T00:00:00)`, alongside `...00.581000` → `Ok(...581)` and `...00.581` →
  `Ok(...581)`. So the two-branch selection buys nothing over the single `%.f` format.
- Fix: In crates/lakehouse-engine/tests/e2e_capability_test.rs, delete the
  `if normalized.contains('.')` branch in `parse_exasol_timestamp` and parse
  unconditionally with `"%Y-%m-%dT%H:%M:%S%.f"`. Rewrite the doc comment to describe
  only what its actual caller produces — a space-separated `SELECT SYSTIMESTAMP` value
  with a fractional part, normalized to `T` before parsing — and delete the claim about
  `assert_select_pushed_down` / `UPPER(c_ts)` paths. Add a `#[test]` named
  `parse_exasol_timestamp_accepts_space_and_t_separators_with_or_without_fraction` that
  pins all three shapes (`"2026-07-28 18:56:00.581000"`, `"2026-07-28T18:56:00.581"`,
  `"2024-01-01T00:00:00"`), so the single format string is proven rather than assumed.

## Expert fixes

### crates/vs-expression/src/lib.rs

#### [MISSING_BOUNDARY_TEST] The null-timestamp test asserts only the Exasol dialect, and the spec delta's "both dialects" claim is false
- Location: lines 2448-2465 (`renders_null_valued_timestamp_literal_as_null_in_exasol_dialect`); the DataFusion arms at lines 596-599 and 616-618; spec delta `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-literals/spec.md:65`
- Issue: this is the one test in task 5's set that breaks the paired-dialect convention
  the rest of the change established, and it breaks it exactly where the DataFusion
  behavior is asymmetric and undocumented. Measured against the built crate:
  `literal_timestamp` with `"value": null` (and with the key absent) renders
  `arrow_cast(NULL, 'Timestamp(Microsecond, None)')` in the DataFusion dialect, while
  `literal_timestamp_utc` with the same input returns bare `NULL` — the UTC arm has an
  early `return Ok(Some("NULL".into()))` at line 617 that the non-UTC arm lacks. The
  spec delta at line 65 states "a node whose `value` is absent or JSON `null` SHALL
  render as `NULL` in both dialects", which is false for `literal_timestamp` in the
  DataFusion dialect. Nothing in the suite pins either DataFusion value, so the
  recorder would merge a false normative clause into the permanent spec library, and a
  later reader "fixing" `literal_timestamp` to match that clause would silently break
  the plan's own frozen-DataFusion-output requirement. That failure mode — a passing
  test over changed behavior — is why this needs deciding rather than patching.
- Fix: Freeze the measured DataFusion behavior, do not change it. (1) In
  crates/vs-expression/src/lib.rs, extend
  `renders_null_valued_timestamp_literal_as_null_in_exasol_dialect` so each of the four
  existing cases also asserts the DataFusion dialect on the SAME node:
  `render_expression` must equal `"arrow_cast(NULL, 'Timestamp(Microsecond, None)')"`
  for `literal_timestamp` and `"NULL"` for `literal_timestamp_utc`, for both the
  `"value": null` and the absent-key variant; rename the test to
  `renders_null_valued_timestamp_literal_per_dialect` so the name states the asymmetry
  it pins. (2) Add a doc-comment sentence to the `literal_timestamp` arm (lines 585-599)
  recording that its DataFusion rendering wraps the NULL keyword in `arrow_cast` while
  the UTC arm short-circuits to bare `NULL`, and that both are pre-existing and frozen.
  (3) In specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-literals/spec.md,
  replace the line-65 clause with two clauses: the Exasol dialect SHALL render bare
  `NULL` for both node types, AND the DataFusion dialect keeps its existing per-node-type
  rendering unchanged (`arrow_cast(NULL, 'Timestamp(Microsecond, None)')` for
  `literal_timestamp`, bare `NULL` for `literal_timestamp_utc`).

#### [MISSING_BOUNDARY_TEST] The declaration sweep never asserts the DataFusion dialect, so a declared name can lose its arm undetected
- Location: lines 4661-4710 (the `for (declared_name, form) in TRANSLATED_SCALAR_FNS` loop in `exasol_dialect_renders_declared_verbatim_surface`); the backstop arm at lines 1399-1404
- Issue: the declaration gates BOTH dialects (line 892), so declaring a name asserts it
  is translated in the DataFusion dialect too — but the sweep only calls
  `render_expression_exasol`. For a `VerbatimCall` name the gate returns before the
  `match`, so the sweep passes whether or not the name still has a per-name arm; the
  DataFusion path then falls to the `other =>` backstop at line 1402 and declines with
  `unsupported scalar function: <name>`. The code already names this hazard —
  "Backstop only: ... this arm is reachable only if a declared name loses its per-name
  arm" — and nothing fails when it happens. The sweep is the change's designated
  structural guard ("a name added to the declaration with no fixture fails here BY
  NAME"), yet that guarantee covers only half the surface it gates: today's DataFusion
  coverage rests entirely on the hand-written per-family tests, exactly the parallel
  hand-written list the plan chose the declaration to replace. The five node-type rows
  at lines 4715-4770 already assert both dialects; the 76 function rows do not.
- Fix: In crates/vs-expression/src/lib.rs, inside
  `exasol_dialect_renders_declared_verbatim_surface`'s
  `for (declared_name, form) in TRANSLATED_SCALAR_FNS` loop, add a DataFusion-dialect
  assertion for every declared name before the `match form`:
  `render_expression(&fixture.node)` must be `Ok`, failing with a message naming
  `declared_name` and stating that a declared name must render in BOTH dialects because
  the declaration gates both, so a missing or deleted per-name arm cannot hide behind
  the Exasol gate. Do NOT assert an expected DataFusion string here — the per-family
  paired tests own those, and duplicating them would make this loop a second copy of
  the frozen expectations. Then verify the guard bites: temporarily delete the
  `"WEEK" =>` arm at line 1278, confirm the sweep fails naming `WEEK`, and restore it.
