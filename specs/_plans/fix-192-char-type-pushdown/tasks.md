# Tasks: fix-192-char-type-pushdown

## Phase 2: Implementation (Group A)
- [x] 2.1 Task 1 — Advisory payload capture (best-effort, gates nothing; SKIPPED — `capture-pushdown-payload.sh` always runs a full `make cross-musl-udf-build` release build, which the plan explicitly permits abandoning when slow; the live native-probe table already establishes the declared types, and Tasks 2-8 do not depend on this task's output)
- [x] 2.2 Task 2 — Failing unit tests for the new CHAR arm in `exasol_type_from_json`
- [x] 2.3 Task 3 — Failing unit tests through the real request paths (facets A/B/C, MIN/MAX, VARCHAR control, LIKE guard)
- [x] 2.4 Task 7 — Trailing-space + over-length seed table

## Phase 2: Implementation (Group B)
- [x] 2.5 Task 4 — Add `"char"` arm to `exasol_type_from_json` [expert]
- [x] 2.6 Task 5 — Add CHAR case to `render_cast_target`'s Exasol arm + retarget 5 stale tests [expert]

## Phase 2: Implementation (Task 6 — depends on Group B)
- [x] 2.7 Task 6 — Non-truncating blank-pad for CHAR-declared group keys [expert]

## Phase 2: Implementation (Group C — depends on Task 6 + Task 7)
- [x] 2.8 Task 8 — E2E regression tests

## Phase 3: Verification
- [x] 3.1 Task 9 — cargo test, cargo clippy --all-targets, cargo fmt (all green; also ran `make test-e2e` — 0 failures across all 7 E2E files, 26/26 in e2e_capability_test.rs including all new CHAR tests)
- [x] 3.2 Code review of all changed files — 7 findings (standard: 6, expert: 1), specs/_plans/fix-192-char-type-pushdown/review-findings.md
- [x] 3.3 Verification report — specs/_plans/fix-192-char-type-pushdown/verification-report.md

## Phase 4: Review Fixes
- [x] 4.1 [expert] Fix SENTINEL_ERROR_VALUE — `group_key_exasol_types` must also resolve a declared CHAR type from `groupBy[slot]["dataType"]` for a group key absent from `selectList`, so an unprojected CHAR-declared group key is still blank-padded instead of silently returning a wrong row count (grouped_agg.rs + mod.rs test)
- [x] 4.2 Fix OUTDATED_COMMENT on `render_expression_exasol`'s public doc (vs-expression/src/lib.rs:1075) — still claims CHAR maps to VARCHAR
- [x] 4.3 Fix OUTDATED_COMMENT in `render_scalar_over_merge` (grouped_agg.rs:420-423) — still claims the Exasol CAST target is VARCHAR(n)
- [x] 4.4 Fix INFORMATION_LEAKAGE — document that `blank_pad_char_group_keys` (grouped_agg.rs:467-503) is the one DataFusion-dialect SQL fragment the adapter synthesises directly instead of routing through vs-expression, and name the tests that pin it
- [x] 4.5 Fix INFORMATION_LEAKAGE — reconcile the CHAR-with-no-`size` default between support.rs's `exasol_type_from_json` (currently CHAR(2000), most damaging default) and vs-expression's `render_cast_target` (VARCHAR(2000000)); align both on the "unknown width" VARCHAR(2000000) convention + add a unit test
- [x] 4.6 Fix MAGIC_NUMBER — name VARCHAR's 2,000,000 ceiling as `EXASOL_VARCHAR_MAX_SIZE` in support.rs, alongside `EXASOL_CHAR_MAX_SIZE`
- [x] 4.7 Fix ASSERTION_FREE_TEST — in e2e_capability_test.rs, both over-length E2E tests: delete the vacuous `numRows` assertion and the dead `resp["message"]` fallback, tighten the error assertion to `sql_code.contains("22001")` (revised to the live-verified `22002` — see report), and add an `EXPLAIN VIRTUAL` assertion proving the pushdown was taken with the CHAR declaration (not declined to native Exasol)
