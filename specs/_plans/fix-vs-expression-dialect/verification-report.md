# Verification Report: fix-vs-expression-dialect

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 15 plan tasks implemented, all review findings fixed, full unit + E2E suite green against a live Exasol 2025.2.1 container, two live `EXPLAIN VIRTUAL` spot checks confirm the wrapper SQL renders Exasol-native calls and no longer pushes the withdrawn now-family. |
| Code review | 6 findings — 6 fixed (4 standard, 2 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ (release profile, 18m44s, exit 0) |
| Tests (`cargo test --workspace --lib`) | ✓ (671 + 120 = 791 passed, 0 failed) |
| Lint (`cargo clippy --all-targets --features exasol-e2e`) | ✓ (0 warnings, 0 errors) |
| Format (`cargo fmt --check`) | ✓ (clean) |
| Test (E2E, `make test-e2e`) | ✓ (227 passed across 7 binaries, 0 failed) |
| Scenario Coverage | ✓ (see table below) |
| Manual Tests | ✓ (see table below) |
| Specs (`speq plan validate`) | ✓ pass (4 non-blocking AND-step-count warnings) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`lakehouse-engine`) | 671 | 671 | 0 |
| Unit (`vs-expression`) | 120 | 120 | 0 |
| Integration/E2E (`e2e_scan_test`) | 59 | 59 | 0 |
| Integration/E2E (`e2e_capability_test`) | 60 | 60 | 0 |
| Integration/E2E (`e2e_count_distinct_test`) | 16 | 16 | 0 |
| Integration/E2E (`e2e_int96_timestamp_test`) | 7 | 7 | 0 |
| Integration/E2E (`e2e_join_test`) | 15 | 15 | 0 |
| Integration/E2E (`e2e_positional_deletes_test`) | 16 | 16 | 0 |
| Integration/E2E (`e2e_refresh_test`) | 11 | 11 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p vs-expression exasol_dialect` — all Exasol-dialect tests pass | ✓ |
| Add unreachable `SUBSTRING` arm without a declaration row → stays unreachable | ✓ (proved live during task 1, reverted) |
| Delete a declared name's sweep fixture → test fails naming it | ✓ (proved live during task 7, reverted) |
| `SELECT COUNT(DISTINCT YEAR(...))` returns a row count, not `DATE_PART not found` | ✓ (`e2e_count_distinct_date_field_matches_native_oracle`) |
| Grouped `SIGN(SUM(...) - 0.5)` returns rows, not `SIGNUM not found` | ✓ (`e2e_grouped_scalar_over_aggregate_sign_matches_native_oracle`) |
| Select-list `REGEXP_LIKE` returns a count, not a syntax error | ✓ (`e2e_count_distinct_regexp_like_matches_native_oracle`) |
| `CASE WHEN ... > TIMESTAMP '...'` returns a count, not `ARROW_CAST not found` | ✓ (`e2e_count_distinct_timestamp_literal_matches_native_oracle`) |
| `cargo test -p vs-expression` 0 failures incl. the 3 freeze tests | ✓ |
| `reports_audited_capability_set` passes with the 4 now-family names absent | ✓ |
| `SELECT DBTIMEZONE, SESSIONTIMEZONE` neither UTC (now-family precondition) | ✓ (asserted inside `e2e_now_family_matches_native_oracle`, pinned image defaults `EUROPE/BERLIN`) |
| Live `EXPLAIN VIRTUAL … WHERE event_ts > CURRENT_TIMESTAMP` — no filter pushed | ✓ **(run live this session, see Notes)** |
| Live `SELECT SYSTIMESTAMP` post-withdrawal — statement-constant, near oracle | ✓ (`e2e_now_family_matches_native_oracle`) |
| `grep -rn "CURRENT_DATE\|SYSDATE\|CURRENT_TIMESTAMP\|SYSTIMESTAMP" docs/` → only the new row | ✓ |
| Live `EXPLAIN VIRTUAL … SIGN(...)` wrapper SQL contains `SIGN(`, not `signum` | ✓ **(run live this session, see Notes)** |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --features exasol-e2e
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.67s
EXIT:0   (0 warnings, 0 errors)
```

### Formatter

```
cargo fmt --check
EXIT:0   (no changes)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| sql-comprehension | vs-expression-translator | Verbatim gate declines an undeclared name in both dialects | `crates/vs-expression/src/lib.rs` | `undeclared_scalar_function_declines_in_both_dialects` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Math/string/date-field families render verbatim in Exasol | `crates/vs-expression/src/lib.rs` | `renders_math_family_verbatim_in_exasol_dialect`, `renders_string_family_verbatim_in_exasol_dialect`, `renders_date_field_shortcuts_verbatim_in_exasol_dialect` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Now-family withdrawn, declines in both dialects | `crates/vs-expression/src/lib.rs` | `now_family_falls_through` | Pass |
| sql-comprehension | vs-expression-translator-date-fns | EXTRACT / DATE_TRUNC / *_BETWEEN dialect branches | `crates/vs-expression/src/lib.rs` | `renders_extract_as_exasol_extract_from_in_exasol_dialect`, `renders_date_trunc_verbatim_in_exasol_dialect`, `renders_between_family_verbatim_in_exasol_dialect` | Pass |
| sql-comprehension | vs-expression-translator-literals | Timestamp literals bare in Exasol, per-dialect null handling | `crates/vs-expression/src/lib.rs` | `renders_timestamp_literals_as_bare_timestamp_in_exasol_dialect`, `renders_timestamp_utc_literal_without_offset_in_exasol_dialect`, `renders_null_valued_timestamp_literal_per_dialect` | Pass |
| sql-comprehension | vs-expression-translator-scalar-ops | Arithmetic/CASE/CONCAT/REGEXP_LIKE identical or dialect-branched as declared | `crates/vs-expression/src/lib.rs` | `arithmetic_operators_render_identically_in_both_dialects`, `renders_regexp_like_as_infix_predicate_in_exasol_dialect` | Pass |
| sql-comprehension | vs-expression-translator | Whole declared surface has a sweep fixture, both dialects render | `crates/vs-expression/src/lib.rs` | `exasol_dialect_renders_declared_verbatim_surface` | Pass |
| vs-adapter | pushdown-planning-capability-extensions | Now-family capabilities withdrawn and unadvertised | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` | Pass |
| vs-adapter | pushdown-planning-capability-extensions | Now-family evaluated natively, statement-constant, in DB zone | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_now_family_matches_native_oracle` | Pass |
| vs-adapter | create-virtual-schema | Deliberate-absence list names the four withdrawn capabilities | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` (negative loop) | Pass |
| vs-adapter | — | Issue #209 repro queries (SIGN, YEAR/WEEK, HOURS_BETWEEN, grouped SIGN/YEAR, REGEXP_LIKE, timestamp literal) + INSTR regression guard | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | 9 tests in § 8.19 | Pass |
| vs-adapter | — | Wrapper shapes unaffected by the dialect change | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | 12 golden tests | Pass (fixtures byte-identical, confirmed via `git status`) |

## Notes

- **Live spot-checks beyond the plan's own evidence.** Per this repo's verification discipline (no SQL capability/pushdown claim taken on documentation or memory alone), I ran two additional live `EXPLAIN VIRTUAL` checks this session against the pinned `exasol/docker-db:2025.2.1` container, beyond what the E2E tests directly assert:
  - `EXPLAIN VIRTUAL SELECT id FROM MY_LAKEHOUSE.EVENTS WHERE event_ts > CURRENT_TIMESTAMP` — the returned `PUSHDOWN_SQL` scans only `[ID, EVENT_TS]` with **no `filter` key at all**; the predicate is evaluated by Exasol itself post-scan, confirming the now-family's filter-position pushdown is genuinely withdrawn, not merely undeclared in principle.
  - `EXPLAIN VIRTUAL SELECT COUNT(DISTINCT SIGN(score)) FROM MY_LAKEHOUSE.EVENTS WHERE id <= 10000` — the outer Exasol-parsed wrapper reads `COUNT(DISTINCT SIGN("LHS_T0"."SCORE"))`, confirming the verbatim-call rule renders native `SIGN`, not the old `signum(...)` DataFusion form, in an actual Exasol-compiled statement.
- **Environment gap found and fixed, unrelated to this plan.** The local Docker stack (`lakehouse-engine-rs-*`, up 4 days) never ran its `spark-iceberg-fixtures` one-shot Compose job, so `e2e_int96_timestamp_test` initially failed with "table does not exist" for `int96_ts_far_future`. Confirmed via `git diff --stat main` that this test file and its fixture SQL are untouched by this plan (pre-existing, unrelated code). Fixed by running `docker compose -p lakehouse-engine-rs up -d spark-iceberg-fixtures` (the one-shot job exited 0); reran the full E2E suite afterward, now 227/227 passing. Two containers/volumes were accidentally created under the wrong Compose project name (`labs-lakehouse-engine-rs-209`, matching this worktree's directory name rather than the running stack's project name) during the first attempt — torn down and removed before retrying under the correct project name (`-p lakehouse-engine-rs`); the pre-existing 4-day-old stack was never touched by that mistake.
- **Test-harness bug found and fixed, not a rendering defect.** Task 10's `e2e_now_family_matches_native_oracle` initially failed on `parse_exasol_timestamp`, which assumed a fixed millisecond-precision, `T`-separated format; a bare `SELECT SYSTIMESTAMP` actually returns a space-separated, microsecond-precision string. Fixed to normalize the separator and accept any fractional-second width (later simplified further in review fix 4.6, which also removed a now-dead conditional branch).
- **`ghbrk gh issue list --search "now-family pushdown restoration"` (plan's literal checklist command) returns no results** — GitHub's search doesn't fuzzy-match that exact phrase against issue #263's actual title ("Restore now-family pushdown for CURRENT_DATE, CURRENT_TIMESTAMP, SYSDATE, SYSTIMESTAMP"). Verified directly instead via `ghbrk gh issue view 263`: state OPEN, correctly cross-referenced in `plan.md` § Non-Goals bullet 4. The checklist step's intent (issue exists and is open before recording) is satisfied; the literal search string in the plan is stale.
- **Spec-content issue found during implementation, not yet corrected in the spec itself.** `vs-expression-translator-literals/spec.md` (around line 65) states a null-valued timestamp literal renders as `NULL` "in both dialects" — false for `literal_timestamp` in the DataFusion dialect, which still renders `arrow_cast(NULL, 'Timestamp(Microsecond, None)')` (frozen, unchanged, per this plan's DataFusion-output-frozen requirement). Only `literal_timestamp_utc`, and only the Exasol dialect of `literal_timestamp`, render bare `NULL`. This was corrected as part of review fix 4.1 (the code and its own test are exact and dialect-split); recommend the recorder double-check the merged spec text reads per-dialect, not "both dialects," before promotion to the permanent library.
- **A pre-existing, out-of-plan-scope wrong-answer path was found and deliberately left alone, not fixed:** task 2's implementer found that the DataFusion-dialect rendering of a 3-argument `INSTR`/`LOCATE` silently drops the start-position argument (`strpos` takes none), which is a live wrong-answer path for a WHERE-clause `INSTR(s, sub, 3)` reaching that dialect. This plan freezes DataFusion output and does not touch it; pinned with an explanatory comment. Recommend a separate GitHub issue — not filed as part of this plan, since it is outside its scope.
