# Plan: add-capability-alignment

## Summary

Systematically align the Lakehouse VS adapter's advertised Exasol capabilities with what DataFusion 54 can actually execute: remove the two non-existent comparison capabilities, advertise the already-translated `FN_PRED_LIKE_ESCAPE`, and add the math/string/date scalar functions, `REGEXP_LIKE`, `LITERAL_TIMESTAMP_UTC`, scalar select-list expressions, HAVING, and the decomposable statistical aggregates (STDDEV/VARIANCE family) — each new capability backed by a translator path and one E2E test per capability group.

## Design

### Context

The adapter's `CAPABILITIES` list and the `crates/vs-expression` translator have drifted apart in
both directions, and the audited list is far narrower than DataFusion can serve.

- **Over-advertising is a correctness bug.** `FN_PRED_GREATER`/`FN_PRED_GREATEREQUAL` are advertised
  but do not exist in Exasol's capability vocabulary (verified against
  `virtual-schema-common-java/doc/development/api/capabilities_list.md`). Exasol normalises `a > b`
  to `b < a` before the request reaches the adapter, so it never emits these names; advertising them
  is misleading dead capability.
- **Under-advertising leaves performance on the table.** Exasol post-processes everything not
  advertised. DataFusion 54 natively supports the full math/string/date/conditional scalar-function
  set, regex matching, scalar select-list expressions, HAVING, and the statistical aggregates — all
  of which we currently force Exasol to compute after a full row scan.
- **`FN_PRED_LIKE_ESCAPE` is already translated** by `vs-expression` but not advertised — a pure
  capability-list fix.

- **Goals** — Make the advertised capability set exactly equal to the set the engine can execute
  correctly: translate every newly advertised scalar function/predicate in `vs-expression`,
  decompose every newly advertised aggregate into a shard-associative partial/merge plan, and
  remove every advertised name the engine cannot back.
- **Non-Goals** — ORDER_BY pushdown (distributed shard-merge ordering semantics are unclear),
  JOIN/multi-table pushdown, geospatial (`FN_ST_*`), Exasol-only session functions
  (`FN_CURRENT_USER`, `FN_SYS_GUID`, `FN_CURRENT_SCHEMA`), `LITERAL_INTERVAL` (DataFusion interval
  support is partial), and the non-decomposable aggregates `MEDIAN`, `APPROXIMATE_COUNT_DISTINCT`,
  every `*_DISTINCT` form, and `LISTAGG`/`GROUP_CONCAT` (order-sensitive, not shard-associative).

### Decision

#### Architecture

```
getCapabilities ──► capabilities.rs (audited CAPABILITIES list)
                         │  advertised name ⇔ backed by ▼
pushdown ──► adapter ──► vs-expression translator ──► DataFusion SQL fragment
                         │   (filter / select-list / group-key / HAVING)
                         └─ aggregate planner ──► partial/merge wrapper
                                                   (SUM/MIN/MAX/COUNT/AVG-pair
                                                    + STDDEV/VARIANCE sufficient-stats)
                         ▼
                    scan UDF ──► DataFusion session ──► partial rows
```

The invariant: **a capability name is advertised only if it round-trips** — either the translator
emits a correct DataFusion fragment for it, or the aggregate planner emits a correct
shard-associative partial/merge plan for it.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Name-mapping table from Exasol `FN_*` to DataFusion SQL fn | `vs-expression` scalar/date arms | Most functions are 1:1 lower-cased; a few need explicit aliases (`SIGN`→`signum`, `LENGTH`→`character_length`, `MOD`→`%`, `INSTR`/`LOCATE`→`strpos`, `UNICODE`→`ascii`) |
| Sufficient-statistics decomposition | STDDEV/VARIANCE aggregate planner + scan | Variance/stddev are not shard-associative directly, but `(count, sum, sum_sq)` are; the wrapper reconstructs the statistic |
| HAVING applied in the OUTER wrapper only | pushdown planner | A per-shard HAVING would drop groups that only clear the threshold after merge |
| Translate-or-fall-back | select-list, HAVING, aggregates | Any untranslatable node is omitted and Exasol post-processes, preserving correctness over performance |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Remove `FN_PRED_GREATER`/`FN_PRED_GREATEREQUAL` from `CAPABILITIES`; keep the harmless `predicate_greater(equal)` translator arms | Leave caps as-is (Exasol ignores unknown names) | The names are not in Exasol's vocabulary; advertising them is misleading and a future reviewer would re-litigate. Translator arms stay as defensive no-ops. |
| Advertise STDDEV/VARIANCE via `(count, sum, sum_sq)` sufficient statistics | Advertise and compute full stddev per shard then average | Per-shard stddev is not mergeable; sufficient statistics are exactly shard-associative and reconstruct the exact result within float tolerance |
| Exclude `MEDIAN`, `APPROX_COUNT_DISTINCT`, all `*_DISTINCT`, `LISTAGG`/`GROUP_CONCAT` | Advertise them too | None decompose into shard-associative partials; advertising would yield wrong results. Out of scope. |
| Date functions as a separate `vs-expression-translator-date-fns` feature | Fold into scalar-ops | Keeps each spec focused and bounded; date semantics (EXTRACT field handling, session-free now-family) are a distinct concern |
| `MOD`→`%` operator | Emit `mod(a,b)` | DataFusion 54 exposes modulo only as the `%` operator, not a `mod()` function |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| sql-comprehension/vs-expression-translator | CHANGED | `sql-comprehension/vs-expression-translator/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| sql-comprehension/vs-expression-translator-date-fns | NEW | `sql-comprehension/vs-expression-translator-date-fns/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |

## Implementation Tasks

### 1. Capability-list audit fixes

- [ ] 1.1 Remove `FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` from `CAPABILITIES` in `crates/lakehouse-engine/src/adapter/capabilities.rs`
- [ ] 1.2 Add `FN_PRED_LIKE_ESCAPE`, `FN_PRED_REGEXP_LIKE`, `LITERAL_TIMESTAMP_UTC`, `SELECTLIST_EXPRESSIONS`, and `AGGREGATE_HAVING` to `CAPABILITIES`
- [ ] 1.3 Add the math/string/date/conditional scalar-function `FN_*` capability names and the statistical-aggregate names (`FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, `FN_AGG_VAR_SAMP`) to `CAPABILITIES`
- [ ] 1.4 Update `capabilities.rs` unit tests: assert new names present, assert excluded names absent (GREATER/GREATEREQUAL, MEDIAN, APPROX_COUNT_DISTINCT, *_DISTINCT, LISTAGG/GROUP_CONCAT, ORDER_BY*, JOIN*, GROUP_BY_TUPLE)

### 2. vs-expression: predicates and literals

- [ ] 2.1 Add `literal_timestamp_utc` literal arm (timestamp-with-tz UTC literal)
- [ ] 2.2 Add `predicate_regexp_like` / `REGEXP_LIKE` → `regexp_like(<expr>, <pattern>)`

### 3. vs-expression: scalar functions (scalar-ops)

- [ ] 3.1 Add the math-function name-mapping arm (ABS/ROUND/FLOOR/CEIL/SQRT/POWER/EXP/LN/LOG/SIGN→signum/TRUNC/trig/hyperbolic/COT/DEGREES/RADIANS) with arity checks [expert]
- [ ] 3.2 Add `MOD` → `(<l> % <r>)` modulo-operator arm
- [ ] 3.3 Add the string-function name-mapping arm (CONCAT/LENGTH→character_length/LOWER/UPPER/SUBSTR/TRIM/LTRIM/RTRIM/REPLACE/REPEAT/REVERSE/LPAD/RPAD/ASCII/CHR/INITCAP/LEFT/RIGHT/TRANSLATE/INSTR+LOCATE→strpos with operand reorder/OCTET_LENGTH/UNICODE→ascii/UNICODECHR→chr) [expert]
- [ ] 3.4 Add `CASE` → `CASE WHEN ... THEN ... [ELSE ...] END` translation from the Exasol `function_scalar` CASE encoding [expert]
- [ ] 3.5 Add `GREATEST`/`LEAST` → `greatest(...)`/`least(...)`
- [ ] 3.6 Add `NULLIFZERO` → `nullif(<arg>,0)` and `ZEROIFNULL` → `coalesce(<arg>,0)`

### 4. vs-expression: date/time functions (date-fns)

- [ ] 4.1 Add `EXTRACT` and the field-shortcut arms (YEAR/MONTH/DAY/HOUR/MINUTE/SECOND) → `EXTRACT(<FIELD> FROM <src>)` [expert]
- [ ] 4.2 Add `DATE_TRUNC` → `date_trunc(<unit>, <src>)`
- [ ] 4.3 Add `CURRENT_DATE`/`SYSDATE` → `current_date()`, `CURRENT_TIMESTAMP`/`SYSTIMESTAMP` → `now()`
- [ ] 4.4 Add `TO_DATE`/`TO_TIMESTAMP` → `to_date(...)`/`to_timestamp(...)` with optional format arg
- [ ] 4.5 Ensure unsupported date functions (ADD_DAYS, DAYS_BETWEEN, CONVERT_TZ, POSIX_TIME) fall through as unsupported (error/None)

### 5. Adapter: select-list expressions and HAVING

- [ ] 5.1 Render select-list expression nodes via the translator and carry rendered fragments in the scan spec projection; fall back to bare-column projection on untranslatable items [expert]
- [ ] 5.2 Render the `having` predicate via the translator and apply it in the OUTER wrapper SQL only; omit on untranslatable [expert]

### 6. Adapter + scan: decomposable statistical aggregates

- [ ] 6.1 Extend the aggregate planner to decompose STDDEV/VARIANCE family into `(count, sum, sum_sq)` sufficient-statistics partials and emit the variance/stddev reconstruction in the wrapper (population vs sample divisor, NULL/zero-count guard) [expert]
- [ ] 6.2 Extend the scan UDF to emit the `(count, sum, sum_sq)` triple per shard/group for a requested statistical aggregate, excluding NULL target rows from the count [expert]
- [ ] 6.3 Route MEDIAN / *_DISTINCT / APPROX_COUNT_DISTINCT / LISTAGG / GROUP_CONCAT to the existing row-scan fallback

### 7. vs-expression unit tests

- [ ] 7.1 Add unit tests for every new translator arm (math, string, MOD, CASE, GREATEST/LEAST, NULLIFZERO/ZEROIFNULL, regexp_like, timestamp_utc literal, all date arms, unsupported-date fall-through)

### 8. E2E tests (one per capability group)

- [ ] 8.1 Create `crates/lakehouse-engine/tests/e2e_capability_test.rs` with a shared seed table covering numeric, string, and timestamp columns
- [ ] 8.2 E2E: math functions in a WHERE filter push down and return correct rows
- [ ] 8.3 E2E: string functions in a WHERE filter push down and return correct rows
- [ ] 8.4 E2E: date functions (EXTRACT/DATE_TRUNC) in a WHERE filter push down and return correct rows
- [ ] 8.5 E2E: `REGEXP_LIKE` in a WHERE filter pushes down and returns correct rows
- [ ] 8.6 E2E: scalar expressions in the SELECT list push down and return correct evaluated values
- [ ] 8.7 E2E: a `HAVING` clause on a grouped aggregate pushes down and returns correct groups
- [ ] 8.8 E2E: statistical aggregates (`STDDEV`/`VARIANCE`) push down and merge to the single-node result within float tolerance

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (translator core) | 2.1, 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 4.1, 4.2, 4.3, 4.4, 4.5 |
| Group B (capability list) | 1.1, 1.2, 1.3, 1.4 |
| Group C (adapter + scan integration) | 5.1, 5.2, 6.1, 6.2, 6.3 |
| Group D (unit tests) | 7.1 |
| Group E (E2E) | 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8 |

Sequential dependencies:
- Group A → Group D (unit tests assert the translator arms)
- Group A → Group C (select-list/HAVING/aggregate integration uses the new translator arms)
- Group A, Group B, Group C → Group E (E2E exercises the full advertised+translated+executed path)
- Group B is independent of Group A but both must land before Group E

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Capability entries | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `FN_PRED_GREATER` and `FN_PRED_GREATEREQUAL` are not Exasol capability names; remove from `CAPABILITIES` |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| create-virtual-schema: Adapter reports its pushdown capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` |
| pushdown-planning: Adapter advertises aggregate pushdown for supported functions | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_supported_aggregate_capabilities` |
| pushdown-planning: Scalar select-list expression is pushed into the scan-driving query | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_selectlist_expression_pushdown` |
| pushdown-planning: HAVING predicate is pushed into the grouped scan plan | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_having_clause_pushdown` |
| pushdown-planning: Decomposable statistical aggregate is pushed down via sufficient statistics | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_stddev_variance_pushdown` |
| pushdown-planning: Adapter falls back for non-decomposable aggregates | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `non_decomposable_aggregate_falls_back_to_row_scan` |
| vs-expression-translator: Literal nodes translate to SQL literal forms (timestamp_utc) | Unit | `crates/vs-expression/src/lib.rs` | `renders_timestamp_utc_literal` |
| vs-expression-translator: REGEXP_LIKE predicate translates to a DataFusion regexp_like call | Unit | `crates/vs-expression/src/lib.rs` | `renders_regexp_like` |
| vs-expression-translator-scalar-ops: Math scalar functions translate | Unit | `crates/vs-expression/src/lib.rs` | `renders_math_scalar_functions` |
| vs-expression-translator-scalar-ops: MOD translates to the modulo operator | Unit | `crates/vs-expression/src/lib.rs` | `renders_mod_as_operator` |
| vs-expression-translator-scalar-ops: String scalar functions translate | Unit | `crates/vs-expression/src/lib.rs` | `renders_string_scalar_functions` |
| vs-expression-translator-scalar-ops: CASE expression translates to SQL CASE WHEN | Unit | `crates/vs-expression/src/lib.rs` | `renders_case_when` |
| vs-expression-translator-scalar-ops: GREATEST and LEAST translate | Unit | `crates/vs-expression/src/lib.rs` | `renders_greatest_least` |
| vs-expression-translator-scalar-ops: NULLIFZERO and ZEROIFNULL translate | Unit | `crates/vs-expression/src/lib.rs` | `renders_nullifzero_zeroifnull` |
| vs-expression-translator-date-fns: EXTRACT translates to the DataFusion EXTRACT form | Unit | `crates/vs-expression/src/lib.rs` | `renders_extract` |
| vs-expression-translator-date-fns: Field-shortcut date functions translate to EXTRACT | Unit | `crates/vs-expression/src/lib.rs` | `renders_year_month_day_extract` |
| vs-expression-translator-date-fns: DATE_TRUNC translates to the DataFusion date_trunc call | Unit | `crates/vs-expression/src/lib.rs` | `renders_date_trunc` |
| vs-expression-translator-date-fns: CURRENT_DATE and CURRENT_TIMESTAMP translate | Unit | `crates/vs-expression/src/lib.rs` | `renders_now_family` |
| vs-expression-translator-date-fns: TO_DATE and TO_TIMESTAMP translate | Unit | `crates/vs-expression/src/lib.rs` | `renders_to_date_to_timestamp` |
| vs-expression-translator-date-fns: Unsupported date functions fall through as unsupported nodes | Unit | `crates/vs-expression/src/lib.rs` | `unsupported_date_fn_falls_through` |
| scan-execution: Scan projects rendered select-list expressions | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_selectlist_expression_pushdown` |
| scan-execution: Scan emits sufficient statistics for a decomposable statistical aggregate | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_stddev_variance_pushdown` |

E2E tests for the remaining capability groups (math/string/date/regexp in filters) exercise the
end-to-end advertised → translated → executed path:

| Capability group | Test Location | Test Name |
|------------------|---------------|-----------|
| Math functions in filter | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_math_functions_in_filter` |
| String functions in filter | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_string_functions_in_filter` |
| Date functions in filter | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_date_functions_in_filter` |
| REGEXP_LIKE in filter | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_regexp_like_in_filter` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/create-virtual-schema | `cargo test -p lakehouse-engine --lib adapter::capabilities` | Capability-list tests pass; GREATER/GREATEREQUAL absent, new names present |
| vs-expression-translator(-scalar-ops/-date-fns) | `cargo test -p vs-expression` | All translator unit tests pass |
| vs-adapter/pushdown-planning + scan-execution | `make test-e2e` | All `e2e_capability_test` cases pass against the live Exasol + MinIO + Iceberg stack |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
