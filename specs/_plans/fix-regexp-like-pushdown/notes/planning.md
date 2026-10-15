# Planning notes: fix-regexp-like-pushdown

## Files and symbols checked

- `specs/_decision/018-add-fn-regexp-pushdown.md` (ADR `affirm-regexp-scalar-function-decline-re-verified-pinned-versions`, supersedes `exclude-regexp-scalar-functions-rust-regex-dialect-divergence` in 002)
- `specs/_decision/002-add-pushdown-capability-gaps.md:57-79` (older regex ADR, already superseded)
- `specs/_decision/037-fix-vs-expression-dialect.md` (`declaration-gates-dispatch-and-sweep`, `withdraw-now-family-capabilities` precedent: names leave the declaration, gate declines both dialects with `unsupported scalar function: <name>`)
- `crates/lakehouse-engine/src/adapter/capabilities.rs:29` `FN_PRED_REGEXP_LIKE` in `CAPABILITIES`; tests in `capabilities_tests.rs:84` (`reports_audited_capability_set` advertised list), `:116-146` not-advertised list with `// Issue #106` comment
- `crates/lakehouse-engine/src/adapter/pushdown/support.rs:388-396` `like_subject_type_guard` matches `"predicate_like" | "predicate_like_regexp"`; `:750-770` `project_columns` select-list node whitelist includes `predicate_like_regexp`
- `support_tests.rs:2531` `like_guard_regexp_date_subject_wraps_cast`; `:3415` table row `predicate_like_regexp` rendering `regexp_like("NAME", '^a.*')`; `:3579` table row `predicate_like_regexp over c_decimal_a`
- `iceberg_predicate_tests.rs:237` `and_all_untranslatable_returns_none` uses `predicate_like_regexp` as an untranslatable child (no production handling)
- `crates/vs-expression/src/lib.rs:34` `("REGEXP_LIKE", ExasolForm::Shaped)` in `TRANSLATED_SCALAR_FNS`; `:153-173` `is_boolean_producing` lists `predicate_like_regexp`; `:571-588` `predicate_like_regexp` arm; `:751-770` `function_scalar` `REGEXP_LIKE` arm; `:694` gate error `unsupported scalar function: {fn_name}`; `:1182` `unsupported expression node type: {other}`; `:1215-1258` public render fns (`*_safe` return None on error)
- `crates/lakehouse-engine/tests/e2e_capability_test.rs:248` `e2e_regexp_like_in_filter`; `:2841` `e2e_count_distinct_regexp_like_matches_native_oracle`
- `docs/capabilities.md` (8.7 KB): Comparison row lists `FN_PRED_REGEXP_LIKE`; Handled by Exasol has JOIN row; no Joins section
- Docs vs code diff (python, table rows before `## Handled by Exasol`, backticked `[A-Z][A-Z0-9_]*`): in code not docs = FN_CAST, FN_NEG, FN_WEEK, FN_DAYS/HOURS/MINUTES/SECONDS_BETWEEN, JOIN, JOIN_TYPE_INNER, JOIN_CONDITION_EQUI; in docs not code = none
- `docs/semantic-differences.md`: no regex entry

## Specs mentioning regexp (grep -i regexp specs)

- `sql-comprehension/vs-expression-translator-predicates` lines 5, 18-19, 82-98
- `sql-functions/vs-expression-translator-scalar-fns` lines 55-63, 137-144
- `sql-comprehension/vs-expression-translator` line 36
- `pushdown-types/pushdown-planning-like-type-coercion` lines 3, 32, 35, 64-148
- `pushdown/pushdown-planning` lines 38, 59
- `pushdown/pushdown-planning-selectlist-expressions` lines 32-35, 131-134
- `pushdown-capabilities/pushdown-planning-string-conversion-capability` lines 4, 11, 27-33
- `pushdown-capabilities/pushdown-planning-capability-extensions` lines 8, 17, 29, 128-130
- `pushdown-joins/pushdown-planning-join-filter-type-coercion` line 74
- `vs-adapter/create-virtual-schema` line 70
- `sql-comprehension/vs-expression-translator-scalar-ops` line 91-93 (regexp_replace for DECIMAL trim, unrelated)

## Lookups run
- `speq feature get`-equivalent reads (Read/sed by line): predicates spec (full), string-conversion-capability spec (full), scalar-fns spec lines 1-67 + 137-144, core translator spec lines 1-66, like-type-coercion spec lines 1-157, pushdown-planning spec lines 1-66, selectlist-expressions spec lines 1-141, join-filter-type-coercion lines 70-80, create-virtual-schema lines 64-72, capability-extensions lines 1-40 + 118-135 (no regex change needed: its mentions are of the four scalar functions only)
- `specs/testing.md` sections Coverage rule, Test tiers, Testing strategy, Unit tests, E2E suites, Failure contract, Correctness oracles, Regression guards, Fixtures
- `git hash-object specs/architecture.md` = 814c1a87800902f026fea2def0dd65b6ed9b9298; `grep -i regexp specs/architecture.md` = none
- `crates/lakehouse-engine/tests/e2e_capability_test.rs:23` `VS_NAME = "MY_LAKEHOUSE"`, `:61` `vs_table()` = EVENTS, `:2199` `explain_virtual_pushdown_request`, `tests/common/e2e_harness.rs:320` `explain_virtual_sql`
- `tests/common/seed.rs:507-510` events names `event-{i:02}`, `SEED_TOTAL_ROWS = 20`; dim_customer `customer-{k:02}` (5 rows); no NATION fixture
- `crates/vs-expression/src/lib_tests.rs`: `renders_regexp_like` 1047, `renders_regexp_like_as_infix_predicate_in_exasol_dialect` 1069, `regexp_like_predicate_missing_operand_errors_in_both_dialects` 1103, `regexp_like_scalar_arity_errors_in_both_dialects` 1120, `regexp_scalar_functions_decline_in_both_dialects` 2182, `regexp_scalar_exclusion_leaves_regexp_like_untouched` 2314, sweep `shaped("REGEXP_LIKE", ...)` 3145, node-type sweep row `predicate_like_regexp` 3273, forbidden token `"regexp_like("` 3320
- `support_tests.rs`: `selectlist_predicate_node_projects_as_expr` 3365 (row at 3415), `selectlist_like_over_non_string_subject_falls_back_to_full_row` 3527 (row at 3579)

## Live checks (Docker Exasol 2025.1.16, exaplus inside container, sys/exasol)

- Native over `'event-' || LPAD(TO_CHAR(LEVEL),2,'0') ... CONNECT BY LEVEL <= 20`: `REGEXP_LIKE 'event-1'` = 0; `NOT (... 'event-1')` = 20; `'(e|ev)(e|ent-01)?'` = 0; `'(?=event-0[1-3]).*'` = 3; `'event-0[0-9]'` = 9; `'ARGENTINA' REGEXP_LIKE '(A|AR)(G|GENTINA)?'` = false
- Current VS `MY_LAKEHOUSE.EVENTS`: `'event-1'` = 10 rows; `'(e|ev)(e|ent-01)?'` = 20 rows; look-around = `22002 ... regex parse error ... look-around ... is not supported`
- `EXPLAIN VIRTUAL SELECT ID, NAME FROM MY_LAKEHOUSE.EVENTS WHERE ID < 5 AND REGEXP_INSTR(NAME, 'x') = 0`: pushdownRequest filter = `predicate_less` ID<5 only, selectList ID, NAME; scan spec filter `("ID" < 5)`
- `EXPLAIN VIRTUAL ... FACT_ORDERS o LEFT JOIN DIM_CUSTOMER c ...`: two separate pushdowns (from alias O, from alias C), no join pushdown
- Native: `UNICODECHR(1635) REGEXP_LIKE '\d'` = false; `'ALGERIA' REGEXP_LIKE '.++A'` = false; `'abab' REGEXP_LIKE '(ab)\1'` = true
- `EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.EVENTS WHERE ID < 5 OR REGEXP_INSTR(NAME,'x') = 0`: no filter pushed, selectList ID, NAME
- `EXPLAIN VIRTUAL SELECT COUNT(*) ... WHERE ID < 5 AND REGEXP_INSTR(NAME,'x') = 0`: filter ID<5 pushed, no aggregate in pushdown request
- `grep -rln "REGEXP_LIKE|regexp_like|predicate_like_regexp" specs/_decision/` = 002, 018, 037 (037 only in Options Considered)
- `grep -rli regexp bench` = none; `grep` of `*.json/*.sql/*.txt` under crates for regexp = none (no dispatch_golden fixture)
- `grep predicate_like crates/lakehouse-engine/src` (non-test) = only `pushdown/support.rs` (Iceberg and Delta pruning predicates never handle LIKE/REGEXP_LIKE)
- `crates/vs-expression/src/lib.rs:14-23` `ExasolForm::Shaped` doc comment names "infix predicate"
- `Makefile:52-53` `EXASOL_HOST ?= localhost`, `LH_EXASOL_PORT ?= 28563`; `Makefile:65` `test-e2e: cross-udf-build`
- Recorded REMOVED-block format: `specs/_recorded/001-refactor-nr-of-cores-detection/e2e-harness/cloud-e2e-harness/spec.md` (full scenario body inside marker)
- `speq plan validate fix-regexp-like-pushdown`: pass (8 warnings, all AND-count on copied scenarios)

## Revision round 1

- `review/round-1.md`: 1 BLOCKER (`[IMPLEMENTATION_LEAKAGE]` scalar-fns Background regex block), 7 ADVISORY (not acted on)
- Edited `sql-functions/vs-expression-translator-scalar-fns/spec.md` Background regex block (was lines 56-75)
- Edited `decision-log.md` [1] Rationale (added `regexp_substr` and argument-shape sentence)
- `grep -n -i 'anchor|compile.check|Background'` over scalar-fns delta and plan.md: no dangling reference
- `speq plan validate fix-regexp-like-pushdown` (run from repo root): pass, same 8 warnings
