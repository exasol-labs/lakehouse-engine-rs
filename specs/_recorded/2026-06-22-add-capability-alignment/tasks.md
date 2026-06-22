# Tasks: add-capability-alignment

## Group A: vs-expression translator core (crates/vs-expression/src/lib.rs)
- [x] 2.1 Add `literal_timestamp_utc` literal arm
- [x] 2.2 Add `predicate_regexp_like` / `REGEXP_LIKE` → `regexp_like(...)`
- [x] 3.1 Add math-function name-mapping arm (ABS/ROUND/.../SIGN→signum/trig/...) with arity checks [expert]
- [x] 3.2 Add `MOD` → `(<l> % <r>)` modulo-operator arm
- [x] 3.3 Add string-function name-mapping arm (CONCAT/LENGTH→character_length/.../INSTR+LOCATE→strpos/...) [expert]
- [x] 3.4 Add `CASE` → `CASE WHEN ... THEN ... [ELSE ...] END` translation [expert]
- [x] 3.5 Add `GREATEST`/`LEAST` → `greatest(...)`/`least(...)`
- [x] 3.6 Add `NULLIFZERO` → `nullif(<arg>,0)` and `ZEROIFNULL` → `coalesce(<arg>,0)`
- [x] 4.1 Add `EXTRACT` and field-shortcut arms (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND) → `EXTRACT(<FIELD> FROM <src>)` [expert]
- [x] 4.2 Add `DATE_TRUNC` → `date_trunc(<unit>, <src>)`
- [x] 4.3 Add `CURRENT_DATE`/`SYSDATE` → `current_date()`, `CURRENT_TIMESTAMP`/`SYSTIMESTAMP` → `now()`
- [x] 4.4 Add `TO_DATE`/`TO_TIMESTAMP` → `to_date(...)`/`to_timestamp(...)` with optional format arg
- [x] 4.5 Ensure unsupported date functions (ADD_DAYS, DAYS_BETWEEN, CONVERT_TZ, POSIX_TIME) fall through as unsupported
- [x] 7.1 Unit tests for every new translator arm (math, string, MOD, CASE, GREATEST/LEAST, NULLIFZERO/ZEROIFNULL, regexp_like, timestamp_utc literal, all date arms, unsupported-date fall-through)

## Group B: capability-list audit (crates/lakehouse-engine/src/adapter/capabilities.rs)
- [x] 1.1 Remove `FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` from `CAPABILITIES`
- [x] 1.2 Add `FN_PRED_LIKE_ESCAPE`, `FN_PRED_REGEXP_LIKE`, `LITERAL_TIMESTAMP_UTC`, `SELECTLIST_EXPRESSIONS`, `AGGREGATE_HAVING`
- [x] 1.3 Add math/string/date/conditional scalar `FN_*` names and statistical-aggregate names (FN_AGG_STDDEV[_POP|_SAMP], FN_AGG_VARIANCE, FN_AGG_VAR_POP, FN_AGG_VAR_SAMP)
- [x] 1.4 Update capabilities.rs unit tests: new names present, excluded names absent

## Group C: adapter + scan integration (depends on Group A)
- [x] 5.1 Render select-list expression nodes via translator; carry fragments in scan spec projection; fall back to bare-column [expert]
- [x] 5.2 Render `having` predicate via translator; apply in OUTER wrapper only; omit on untranslatable [expert]
- [x] 6.1 Decompose STDDEV/VARIANCE family into (count, sum, sum_sq) partials; emit reconstruction in wrapper [expert]
- [x] 6.2 Extend scan UDF to emit (count, sum, sum_sq) triple per shard/group, excluding NULL target rows [expert]
- [x] 6.3 Route MEDIAN / *_DISTINCT / APPROX_COUNT_DISTINCT / LISTAGG / GROUP_CONCAT to row-scan fallback

## Group E: E2E tests (depends on A, B, C) — crates/lakehouse-engine/tests/e2e_capability_test.rs
- [x] 8.1 Create e2e_capability_test.rs with shared seed table (numeric, string, timestamp columns)
- [x] 8.2 E2E: math functions in WHERE filter push down, correct rows
- [x] 8.3 E2E: string functions in WHERE filter push down, correct rows
- [x] 8.4 E2E: date functions (EXTRACT/DATE_TRUNC) in WHERE filter push down, correct rows
- [x] 8.5 E2E: REGEXP_LIKE in WHERE filter pushes down, correct rows
- [x] 8.6 E2E: scalar expressions in SELECT list push down, correct evaluated values
- [x] 8.7 E2E: HAVING clause on grouped aggregate pushes down, correct groups
- [x] 8.8 E2E: statistical aggregates (STDDEV/VARIANCE) push down and merge within float tolerance

## Phase 4: Code Review
- [x] R.1 Review all changed files

## Phase 4b: Review fixes
- [x] F.1 [expert] BLOCKER: STDDEV stddev_pop/samp `SQRT(GREATEST(0.0,NULL))` returns 0.0 not NULL in Exasol for N=0/N=1 — use CASE NULL-passthrough (pushdown.rs)
- [x] F.2 [expert] BLOCKER: HAVING silently dropped when grouped-aggregate validate_agg_col_types fails and falls back — never drop an advertised HAVING (pushdown.rs)
- [x] F.3 [expert] Add unit tests for STDDEV/VARIANCE N=0/N=1 SQL; tighten loose var_samp N-1 assertion (pushdown.rs)
- [x] F.4 Clean task-number / what-comments in pushdown.rs + scan/mod.rs (guardrail: no work-tracking comments)
- [x] F.5 BLOCKER: e2e_math_functions_in_filter wrong row count (strict `<`: 7 rows, 7+pos) (e2e_capability_test.rs)
- [x] F.6 Strengthen e2e_having_clause_pushdown so per-shard count ≤ threshold but merged > threshold (proves outer-wrapper-only)
- [x] F.7 Remove no-op `CEIL=>ceil` arm; ponytail-note deferred ADD/SUB/MUL/FLOAT_DIV/NEG/CAST arms; clean task-number comments (vs-expression/lib.rs)

## Phase 4c: E2E failures (real behavior)
- [x] F.8 [expert] Scalar-expr EMITS type: extract_projection hardcodes VARCHAR(2000000) for rendered exprs → Exasol "Data type mismatch Expected DECIMAL got VARCHAR" on GROUP BY MOD(id,4). Use the selectList item's declared `dataType` via exasol_type_from_json (pushdown.rs)
- [x] F.9 [expert] EXTRACT(DAY FROM date)>10 filter silently dropped (returns all 20 rows): diagnose real Exasol EXTRACT pushdown JSON encoding vs translator's `dateTimeField` key; fix translator so the EXTRACT predicate renders & pushes (vs-expression)
- [x] F.10 regexp test uses `REGEXP_LIKE(name,'pat')` → Exasol syntax error; Exasol predicate is infix `name REGEXP_LIKE 'pat'` (e2e_capability_test.rs)

## Phase 4d: single-group aggregate type cast (latent bug surfaced by full E2E)
- [x] F.11 [expert] Single-group build_aggregate_scan_sql emits bare SUM(PARTIAL_count_0)=DECIMAL(31,0) but Exasol expects declared COUNT type DECIMAL(18,0) → 4 e2e_scan_test aggregate tests fail. Extend the declared-type CAST (aggregate_exasol_types) to the single-group merge, mirroring the grouped path. Identical SQL on HEAD ⇒ pre-existing latent bug.

## Phase 4e: duplicate-projection regression
- [x] F.12 [expert] GROUP BY NULLIF(MOD(id,5),0) now pushed (FN_MOD advertised) → untranslatable group key forces row-scan fallback; extract_projection emits duplicate "ID" cols (["ID","ID"]) → Exasol "Return argument ID declared more than once". Fallback must produce a valid, duplicate-free projection (project distinct base cols / full row) so Exasol post-processes. Fix extract_projection (pushdown.rs); make test_group_by_null_key_grouping green, no regressions.

## Phase 5: Verification
- [x] V.1 Build: make cross-musl-udf-build
- [x] V.2 cargo test (unit) 176 passed, 0 failures
- [x] V.3 cargo clippy --all-targets (0 errors; 8 pre-existing test-helper warnings only); cargo fmt clean
- [x] V.4 E2E: make test-e2e — capability 7/7, scan 22/22, 0 failures
