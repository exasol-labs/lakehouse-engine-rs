# Tasks: add-pushdown-capability-gaps

## Phase 2: Implementation (Group A — parallel, independent per-function work)
- [x] 2.1 [expert] Audit `render_cast_target` (`crates/vs-expression/src/lib.rs`) against the Exasol CAST target-type set; confirm faithful targets (VARCHAR, CHAR, DECIMAL(p,s), DOUBLE, BOOLEAN, DATE, TIMESTAMP) render and unsupported targets (INTERVAL, GEOMETRY, HASHTYPE, TIMESTAMP WITH LOCAL TIME ZONE) return an error/fall back; add unit tests. (#104)
- [x] 2.2 Add a unit test proving `NEG` composes with the arithmetic-aggregate decomposition path (e.g. `SUM(-col)` renders). (#105)
- [x] 2.3 [expert] Confirm Exasol `DIV` (floor division) has no faithful DataFusion 54 translation; add a unit test asserting a `DIV` node falls through; document the divergence in the scalar-ops spec. (#105)
- [x] 2.4 [expert] Confirm Rust-`regex`-crate dialect and argument-shape divergence for `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, `REGEXP_COUNT`; add unit tests asserting each falls through; document in scalar-fns spec. (#106)
- [x] 2.5 [expert] Add a `WEEK` arm rendering `date_part('week', <arg>)`; verify ISO-8601 parity with Exasol `WEEK` including year-boundary dates (unit test). (#107)
- [x] 2.6 Add unit tests asserting the excluded #107 functions (ADD_DAYS/HOURS/MINUTES/SECONDS/WEEKS/MONTHS/YEARS, DAYS/HOURS/MINUTES/SECONDS/MONTHS/YEARS_BETWEEN, DAYOFWEEK, LAST_DAY, CONVERT_TZ) fall through; update date-fns spec example set. (#107)
- [x] 2.7 Add unit tests asserting `TO_CHAR` and `TO_NUMBER` fall through; document the exclusion in the scalar-ops spec. (#104)

## Phase 2: Implementation (Group A.5 — CAST dispatch bugfix found by Group C E2E)
- [x] 2.10 [expert] Fix CAST node-type dispatch: Exasol sends CAST as top-level `function_scalar_cast`, not nested `function_scalar`+name=CAST; add dispatch arm, fix task-2.1 test fixtures, rerun e2e_cast_in_filter. (#104)

## Phase 2: Implementation (Group B — depends on Group A)
- [x] 2.8 Advertise `FN_CAST`, `FN_NEG`, `FN_WEEK` in `crates/lakehouse-engine/src/adapter/capabilities.rs`; extend inline capability tests to assert the three present and all excluded names absent; assert no new join/cross-join capability introduced. (#104, #105, #106, #107)

## Phase 2: Implementation (Group C — depends on Group B)
- [x] 2.9 Extend `crates/lakehouse-engine/tests/e2e_capability_test.rs` with capability-alignment tests exercising CAST, unary-minus, and WEEK in filter/select-list positions against the live Exasol stack. (#104, #105, #107)

## Phase 4: Code Review
- [x] 4.1 Review all changed files for guardrail violations, dead code, YAGNI
- [x] 4.2 Apply code review fixes: swap swapped #104/#105 issue refs on FN_CAST/FN_NEG
      comments (`capabilities.rs` ~L52/L54); drop ephemeral `task 2.8:` prefixes from
      section-header comments (~L299/L308), keeping the issue refs; trim
      `cast_neg_week_introduce_no_join_capability` down to its unique join-capability-set
      invariance (dropped the FN_CAST/FN_NEG/FN_WEEK presence re-check, already covered by
      `reports_audited_capability_set`); remove trivial `// Column argument.` comment in
      `vs-expression/src/lib.rs` (~L2041). `cargo test -p vs-expression` (74 passed),
      `cargo test -p lakehouse-engine capabilities` (9 passed), clippy clean, fmt clean.

## Phase 5: Verification
- [x] 5.1 Build (`make cross-musl-udf-build`)
- [x] 5.2 Test (`cargo test`)
- [x] 5.3 Test E2E (`make test-e2e`)
- [x] 5.4 Lint (`cargo clippy --all-targets`)
- [x] 5.5 Format (`cargo fmt`)
- [x] 5.6 Scenario coverage audit
- [x] 5.7 Manual testing steps
