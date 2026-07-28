# Plan: fix-vs-expression-dialect

> **Status:** blocked — see open-questions.md

## Summary

Make the `vs-expression` translator render Exasol-native SQL on every Exasol-dialect path, not just for CAST targets. A pushed-down scalar function, predicate, or timestamp literal reaching an Exasol-parsed wrapper then compiles instead of aborting the query. Closes issue #209.

## Design

### Context

`crates/vs-expression` threads a `Dialect` parameter through every node of one recursive walker, but only `render_cast_target` reads it. Every other arm renders the DataFusion form unconditionally. Four consumer sites splice that output into wrapper SQL that Exasol's own core engine parses, so any renamed or re-shaped function is a hard compilation error (SQL state 04000) for the whole query.

Two prior fixes already patched one arm each: issue #197 gave `MOD` a dialect branch, and issue #210 gave the string-function family an Exasol-verbatim arm. Neither established a stated rule, so each new arm arrives DataFusion-only by default. The just-shipped `*_BETWEEN` pushdown (`add-date-arithmetic-pushdown`) was broken from day one for exactly this reason.

Verified against live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`) during planning, the following renderings are hard failures on an Exasol-parsed path:

| Rendering | Exasol result |
|---|---|
| `signum(x)` | `function or script SIGNUM not found` (42000) |
| `date_part('YEAR', x)` | `function or script DATE_PART not found` (42000) |
| `strpos(s, sub)` | `function or script STRPOS not found` (42000) |
| `arrow_cast(v, 'Timestamp(Microsecond, None)')` | `function or script ARROW_CAST not found` (42000) |
| `regexp_like(s, p)` | `syntax error, unexpected REGEXP_LIKE_` (42000) |
| `TIMESTAMP '<v>+00:00'` | `data exception - invalid character value for cast` (22018) |

Two of the issue's claims did not survive verification and the plan corrects them: `current_date()` and `now()` both parse in Exasol, so they are not compilation errors. They are still changed, for the semantic reason given under Consequences.

- **Goals** - one stated rule that decides Exasol-dialect rendering for every node, owned in one place; every currently-broken Exasol-dialect rendering fixed; a regression test that fails when a future arm forgets the dialect; byte-identical DataFusion-dialect output.
- **Non-Goals**
  - No capability is added or withdrawn, and no new function is translated.
  - The DataFusion-dialect rendering of any node is not touched.
  - The type-blind translator stays type-blind (no column-type inspection), and `render_cast_target` keeps its existing per-dialect logic unchanged.
  - The DataFusion dialect keeps collapsing `SYSDATE` onto `current_date()` and `SYSTIMESTAMP` onto `now()`. A GitHub issue MUST be filed for that residual collapse before this plan is recorded (see decision-log [4]).
  - The Exasol-dialect rendering of `decimal_to_varchar_exasol` is unchanged. That adapter-synthesized node reaches only DataFusion-dialect renderers today (`adapter/pushdown/mod.rs:213`, `adapter/pushdown/support.rs:1125`), so there is no reachable failure to fix.

### Decision

**In the Exasol dialect, render what Exasol sent.** The expression tree comes from Exasol's own compiler, so reproducing the original name, argument order, and argument count means Exasol evaluates exactly the call it emitted. Both dialects read one declared name set of verbatim-eligible Exasol functions. A name that joins a DataFusion arm without joining that set fails the sweep test rather than silently rendering DataFusion SQL. The DataFusion dialect keeps every existing translation, because DataFusion genuinely lacks functions of Exasol's names.

This generalizes what #197 and #210 each did for one arm into a rule with a single owner, rather than adding a third mechanism. Per `/speq:design-philosophy`, the target is one module owning one decision: the guarded Exasol arm already introduced by #210 becomes the home of "Exasol dialect means verbatim", with its eligible names declared once in a shared set, and the four families that currently rewrite names join it instead of each growing a private dialect branch.

#### Architecture

```
  render_expression_inner(node, dialect)
        │
        ├── dialect-invariant arms (unchanged)
        │     operators ADD/SUB/MULT/FLOAT_DIV/NEG, CASE, literals except timestamps
        │
        ├── dedicated per-dialect arms, ordered BEFORE the verbatim arm
        │     CAST ──────────────► render_cast_target      (existing, #211/#212)
        │     MOD  ──────────────► MOD(a,b) | (a % b)      (existing, #197)
        │     CONCAT ───────────► chained ||               (existing, #200)
        │
        ├── ONE guarded arm: `n if dialect == Dialect::Exasol && is_exasol_verbatim(n)`  ◄── this plan
        │     eligible names declared ONCE in EXASOL_VERBATIM_FNS (task 1);
        │     the arm's guard and task 6's sweep assertion both read that one set
        │     string family (existing, #210)
        │     + math family incl. SIGN
        │     + YEAR/MONTH/DAY/HOUR/MINUTE/SECOND, WEEK
        │     + DAYS_BETWEEN / HOURS_ / MINUTES_ / SECONDS_BETWEEN
        │     + DATE_TRUNC, TO_DATE, TO_TIMESTAMP, GREATEST, LEAST
        │     + NULLIF, NULLIFZERO, ZEROIFNULL
        │     + now-family as bare keywords (no parens)
        │
        └── node types and encodings outside the verbatim set, each branching inline
              function_scalar_extract            ─► EXTRACT(F FROM x)
              predicate_like_regexp              ─► (s REGEXP_LIKE p)      lib.rs:497
              function_scalar named REGEXP_LIKE  ─► (s REGEXP_LIKE p)      lib.rs:678, alternate encoding
              literal_timestamp[_utc]            ─► TIMESTAMP 'v'
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single guarded match arm over a declared name set | `function_scalar` Exasol families | One arm owns the verbatim rule and its eligible names live in one declared set, so a reader finds every Exasol-native name in one list instead of inferring intent per arm |
| Inline `match dialect` | `function_scalar_extract`, `predicate_like_regexp`, the `function_scalar` `REGEXP_LIKE` alternate encoding, timestamp literals | These are distinct node types or shape changes, not verbatim-eligible `function_scalar` names; the #197 MOD precedent already reads this way |
| Arm ordering as precedence | `CAST`, `MOD`, `CONCAT`, operators before the verbatim arm | Keeps the four constructs that must not be verbatim out of the rule without a negative-name list |
| Cross-surface sweep assertion | new unit test over the full translated surface | The rule is only durable if a forgotten future arm fails a test, not a review; asserting per-node name equality (not token absence) is what makes it structural |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One guarded verbatim arm | A per-dialect name table or a `Dialect` trait with two impls | A lookup table adds a second mechanism and an indirection for a rule that is "do not translate". The arm is the shallower interface and matches #210's shipped shape. |
| The verbatim-eligible names are declared once (`EXASOL_VERBATIM_FNS` plus the `is_exasol_verbatim` guard helper) and read by both the guarded arm and task 6's sweep assertion | Spell the names inline in the guarded arm's pattern list, as #210 did | An inline pattern list is a second copy of the translated-name set. The guarded arm sits ahead of the DataFusion arms, so a name present in a DataFusion arm but missing from the Exasol list falls through to DataFusion rendering silently. One declared set turns that omission into a test failure. |
| Fold in functions that already parse in Exasol (`NULLIFZERO`, `ZEROIFNULL`, `GREATEST`, `LEAST`, math names, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `DAYS_BETWEEN`) | Change only the arms that currently fail to compile | A rule applied to some arms and not others cannot be reasoned about. The next reader cannot tell which arms are principled and which merely happen to work. Output changes only in name case for most of these, and `NULLIFZERO`/`ZEROIFNULL` gain parity by construction. |
| Render the now-family as bare Exasol keywords | Leave `current_date()` / `now()`, which do parse | Not a compilation fix. The current mapping collapses `SYSDATE` onto `CURRENT_DATE` and `SYSTIMESTAMP` onto `CURRENT_TIMESTAMP`, erasing Exasol's database-time vs session-time distinction. Verbatim rendering removes a latent wrong-answer path at the cost of two lines. |
| Exasol dialect drops per-arm arity checks | Keep arity validation in both dialects | Exasol's compiler emitted a call its own engine accepts, and Exasol's `INSTR(s, sub, start)` already relies on this (#210). A translator-side arity check on that path can only reject valid input. |
| No capability change | Withdraw a capability whose Exasol rendering was broken | Every affected function stays correct on the DataFusion scan path, which is what the advertisement governs. Withdrawing would remove working pushdown. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-fns | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| sql-comprehension/vs-expression-translator-date-fns | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-date-fns/spec.md` |
| sql-comprehension/vs-expression-translator-literals | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-literals/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |

## Impact

Queries that fail today start returning results. Any query routing a renamed function through an Exasol-side wrapper currently aborts with a compilation error: `COUNT(DISTINCT SIGN(c_acctbal))`, `COUNT(DISTINCT YEAR(l_shipdate))`, `SIGN(SUM(l_discount) - 0.5)` grouped by a key, and the `HOURS_BETWEEN` family shipped in `add-date-arithmetic-pushdown`. Issue #209 lists seven such queries verified failing against a virtual schema and passing against a native Exasol table.

Two further failure paths were found during planning and are fixed in the same change. A pushed `REGEXP_LIKE` predicate renders as a function call, which Exasol's parser rejects, and `FN_PRED_REGEXP_LIKE` is advertised. A timestamp literal reaching a wrapper renders through `arrow_cast`, which is DataFusion-only.

No breaking changes. No capability is added or withdrawn, so Exasol pushes the same expressions it pushes today. The DataFusion-dialect rendering of every node stays byte-identical, which the paired-dialect unit assertions and the ten committed `dispatch_golden` wrapper fixtures both hold in place. Results change only where a query previously returned an error, apart from the four now-family names covered next.

One behavior change is not a bug fix: `SYSDATE` and `SYSTIMESTAMP` stop rendering as `CURRENT_DATE` and `CURRENT_TIMESTAMP` on the Exasol path, so Exasol applies its own database-time semantics. An operator comparing wrapper output against a prior run could see a different timestamp source. The DataFusion dialect keeps the collapse (`SYSDATE` → `current_date()`, `SYSTIMESTAMP` → `now()`), so the two dialects disagree for these four names. No query is known that evaluates one such node on both paths. A GitHub issue MUST be filed for the DataFusion-side collapse before recording (see § Non-Goals and decision-log [4]).

## Requirements

| Requirement | Details |
|-------------|---------|
| DataFusion output frozen | Every DataFusion-dialect rendering MUST stay byte-identical. Enforced by paired-dialect assertions on the same JSON node, the convention `renders_cast_timestamp_precision_per_dialect` already uses. |
| Exasol output must compile | Each new Exasol-dialect rendering MUST be a form verified to compile on live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`). The forms are recorded in the delta Background sections with their SQL codes. |
| Rule must be enforced, not documented | One table-driven unit test MUST render every node in its sweep table in the Exasol dialect. For each `function_scalar` row the rendering MUST equal `<NAME>(<rendered args>)` using the node's own uppercased `name`, except for the four constructs the verbatim rule excludes (the operator wire names, `MOD`, `CONCAT`, `CAST`) and the `REGEXP_LIKE` alternate encoding, whose Exasol form is the infix predicate. Every node type outside `function_scalar` MUST equal its per-dialect expected string. The test MUST also assert that every `function_scalar` name in the table is either a member of `EXASOL_VERBATIM_FNS` or one of those named exceptions, so a name added to a DataFusion arm without joining the declared set fails. The DataFusion-only token list stays as a secondary assertion. |
| Golden wrapper fixtures unchanged | The ten `dispatch_golden` `.sql` fixtures MUST stay byte-identical. Checked during planning: none currently contains an affected rendering. |

## Dependencies

None. No new crate, no dependency bump, no external service. `crates/vs-expression` has only `serde_json` and `exasol-udf-sdk`, and the change adds no import.

## Implementation Tasks

1. Declare the verbatim-eligible Exasol function names once in `crates/vs-expression/src/lib.rs` — a `const EXASOL_VERBATIM_FNS: &[&str]` plus an `is_exasol_verbatim(name: &str) -> bool` guard helper — and change the existing `if dialect == Dialect::Exasol` guarded `function_scalar` arm to read that set instead of an inline pattern list. The set adds, to the string family already there (#210), the math family including `SIGN`, the field-shortcut date functions, `WEEK`, `DAYS_BETWEEN`, the rest of the `*_BETWEEN` family, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `GREATEST`, `LEAST`, `NULLIF`, `NULLIFZERO`, and `ZEROIFNULL`; render the four now-family names as bare keywords with no parentheses. Keep the `CAST`, `MOD`, `CONCAT`, `REGEXP_LIKE`, and operator arms ahead of the guarded arm so their precedence is unchanged. Add paired-dialect unit tests per family, including the string family #210 shipped with no translator-side test: `renders_string_family_verbatim_in_exasol_dialect` and `renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect`. [expert]
2. Branch `function_scalar_extract` on dialect: Exasol renders `EXTRACT(<FIELD> FROM <src>)` with the field as a bare keyword, DataFusion keeps `date_part('<FIELD>', <src>)`.
3. Branch `predicate_like_regexp` (`lib.rs:497`) **and** the `function_scalar` `REGEXP_LIKE` alternate encoding (`lib.rs:678`) on dialect: Exasol renders the infix `(<subject> REGEXP_LIKE <pattern>)` from both, DataFusion keeps `regexp_like(<subject>, <pattern>)` from both. Keep the missing-operand error and the alternate encoding's arity error in both dialects, and assert the two encodings render byte-identically within a dialect.
4. Branch `literal_timestamp` and `literal_timestamp_utc` on dialect: Exasol renders `TIMESTAMP '<value>'` with the same quote escaping as `literal_string` and with no `+00:00` suffix for the UTC form. Keep both `arrow_cast` renderings byte-identical in the DataFusion dialect. [expert]
5. Rewrite the `Dialect` enum doc comment, which currently states that only `render_cast_target` branches on dialect. Replace that claim with the verbatim rule, the `EXASOL_VERBATIM_FNS` set that declares which names it covers, and the four constructs excluded from it — the operator wire names, `MOD`, `CONCAT`, and `CAST` — with the reason each is excluded.
6. Add the systemic regression test `exasol_dialect_renders_declared_verbatim_surface`, driven from a single table of nodes (one row per translated function name and per node type, including both `REGEXP_LIKE` encodings) so a new arm is one row. Per `function_scalar` row, assert the Exasol-dialect rendering equals `<NAME>(<rendered args>)` from the node's own uppercased `name`, except for the four excluded constructs and the `REGEXP_LIKE` alternate encoding; per non-`function_scalar` node type, assert its per-dialect expected string. Assert every `function_scalar` name in the table is either in `EXASOL_VERBATIM_FNS` or one of those named exceptions. Keep a secondary assertion that the swept output contains none of `signum`, `date_part`, `strpos`, `arrow_cast`, `character_length`, `octet_length`, `regexp_like(`, `current_date()`, `now()`, `nullif(`, `coalesce(`, or a bare `%` operator. [expert]
7. Add decline-parity unit tests: the four regexp scalar functions and the thirteen unsupported date functions must error in `render_expression_exasol` and return `None` in `render_expression_exasol_safe`, matching the DataFusion dialect.
8. Add the paired-dialect freeze tests for the dialect-invariant surface: `arithmetic_operators_render_identically_in_both_dialects`, `non_timestamp_literals_render_identically_in_both_dialects`, and `exasol_df_filter_suppresses_trivially_true`.
9. Add E2E parity tests to `crates/lakehouse-engine/tests/e2e_capability_test.rs` for the seven queries in issue #209 plus a select-list `REGEXP_LIKE` and a timestamp literal in a wrapper, using the in-session native-oracle idiom already established in that file's section 8.16.
10. Confirm the ten `crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/*.sql` fixtures still match byte-for-byte, and re-baseline with a recorded reason if any changed.
11. Bump `crates/lakehouse-engine/Cargo.toml` from `0.30.8` to `0.30.9` and update `Cargo.lock`.

## Parallelization

| Group | Tasks |
|----------------|-------|
| Group A | Task 1 |
| Group B1 | Task 2 |
| Group B2 | Task 3 |
| Group B3 | Task 4 |
| Group B4 | Task 5 |
| Group C1 | Task 6 |
| Group C2 | Task 7 |
| Group C3 | Task 8 |
| Group D | Tasks 9, 10 |
| Group E | Task 11 |

Only Group D holds two tasks that genuinely run concurrently. Every other group is a single task, because tasks 1 through 8 all edit `crates/vs-expression/src/lib.rs` and concurrent sub-agent edits to one 3,351-line file conflict.

Sequential dependencies:

- Group A, then B1 through B4 in that order. Task 1 restructures the guarded arm and declares the name set that every later arm and test sits beside.
- Group B4, then C1 through C3 in that order. Tasks 6, 7, and 8 assert over the finished surface.
- Group D can start once Group B4 completes; task 9 touches only `e2e_capability_test.rs` and task 10 touches only fixtures, so the two run concurrently.
- Group D's task 9 additionally requires `make cross-musl-udf-build` plus the BucketFS SLC upload after Group B4; an E2E run against a stale `.so` tests the old rendering.
- Group E last.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | - | The change adds dialect branches and removes no code path. Every DataFusion-dialect rendering stays reachable. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Math scalar functions render verbatim in the Exasol dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_math_family_verbatim_in_exasol_dialect` |
| Math scalar functions render verbatim in the Exasol dialect (SIGN clause) | Unit | `crates/vs-expression/src/lib.rs` | `renders_sign_as_native_sign_in_exasol_dialect` |
| Math scalar functions translate to DataFusion math calls (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_math_family_verbatim_in_exasol_dialect` |
| MOD translates to the modulo operator (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_mod_as_function_call_in_exasol_dialect` |
| String scalar functions render verbatim in the Exasol dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_string_family_verbatim_in_exasol_dialect` |
| String scalar functions render verbatim in the Exasol dialect (INSTR/LOCATE start-argument clause) | Unit | `crates/vs-expression/src/lib.rs` | `renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect` |
| String scalar functions translate to DataFusion string calls (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_string_family_verbatim_in_exasol_dialect` |
| GREATEST and LEAST translate to DataFusion greatest/least (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_greatest_least_verbatim_in_exasol_dialect` |
| NULLIFZERO and ZEROIFNULL translate to NULLIF and COALESCE (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_nullifzero_zeroifnull_verbatim_in_exasol_dialect` |
| Regexp scalar functions are deliberately not translated (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `regexp_scalar_functions_decline_in_both_dialects` |
| EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_extract_as_exasol_extract_from_in_exasol_dialect` |
| EXTRACT translates to the DataFusion date_part call (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_extract_as_exasol_extract_from_in_exasol_dialect` |
| Field-shortcut date functions render verbatim in the Exasol dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_date_field_shortcuts_verbatim_in_exasol_dialect` |
| Field-shortcut date functions translate to date_part of the matching field (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_date_field_shortcuts_verbatim_in_exasol_dialect` |
| DATE_TRUNC translates to the DataFusion date_trunc call (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_date_trunc_verbatim_in_exasol_dialect` |
| CURRENT_DATE and CURRENT_TIMESTAMP translate to DataFusion now-family calls (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_now_family_as_bare_keywords_in_exasol_dialect` |
| TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_to_date_to_timestamp_verbatim_in_exasol_dialect` |
| WEEK translates to the DataFusion date_part('week') ISO-8601 call (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_week_as_native_week_in_exasol_dialect` |
| DAYS_BETWEEN translates to a whole-day date difference (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_days_between_verbatim_in_exasol_dialect` |
| HOURS_BETWEEN, MINUTES_BETWEEN, and SECONDS_BETWEEN translate to epoch-second differences (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_between_family_verbatim_in_exasol_dialect` |
| Date-difference functions render verbatim in the Exasol dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_between_family_verbatim_in_exasol_dialect` |
| Unsupported date functions fall through as unsupported nodes (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `unsupported_date_functions_decline_in_both_dialects` |
| REGEXP_LIKE predicate renders Exasol's infix form in the Exasol dialect, from both encodings (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_regexp_like_as_infix_predicate_in_exasol_dialect` |
| REGEXP_LIKE predicate translates to a DataFusion regexp_like call, from both encodings (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `renders_regexp_like_as_infix_predicate_in_exasol_dialect` |
| Timestamp literals render as bare Exasol TIMESTAMP literals in the Exasol dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_timestamp_literals_as_bare_timestamp_in_exasol_dialect` |
| Timestamp literals ... (UTC no-offset clause) | Unit | `crates/vs-expression/src/lib.rs` | `renders_timestamp_utc_literal_without_offset_in_exasol_dialect` |
| Literal nodes translate to SQL literal forms (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `non_timestamp_literals_render_identically_in_both_dialects` |
| Far-future timestamp literals survive DataFusion optimization (CHANGED) | Integration | `crates/lakehouse-engine/tests/timestamp_literal_precision_test.rs` | existing far-future literal tests, unchanged |
| Arithmetic operators translate to binary SQL expressions (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `arithmetic_operators_render_identically_in_both_dialects` |
| Trivially-true filter suppressed in safe variant (NEW Exasol-dialect step) | Unit | `crates/vs-expression/src/lib.rs` | `exasol_df_filter_suppresses_trivially_true` |
| The verbatim rule holds across the whole translated surface (Background of `vs-expression-translator`, `-scalar-fns`, `-date-fns`, `-literals`) | Unit | `crates/vs-expression/src/lib.rs` | `exasol_dialect_renders_declared_verbatim_surface` |
| Issue #209 repro: `COUNT(DISTINCT SIGN(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_sign_matches_native_oracle` |
| Issue #209 repro: `COUNT(DISTINCT YEAR(...))` and `COUNT(DISTINCT WEEK(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_date_field_matches_native_oracle` |
| Issue #209 repro: `COUNT(DISTINCT HOURS_BETWEEN(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_hours_between_matches_native_oracle` |
| Issue #209 repro: `COUNT(DISTINCT INSTR(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_instr_matches_native_oracle` |
| Issue #209 repro: grouped `SIGN(SUM(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_grouped_scalar_over_aggregate_sign_matches_native_oracle` |
| Issue #209 repro: grouped `YEAR(MIN(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_grouped_scalar_over_aggregate_year_matches_native_oracle` |
| Exasol wrapper over a pushed REGEXP_LIKE predicate in a **select-list** position: `SELECT COUNT(DISTINCT (c_name REGEXP_LIKE '^C')) FROM <vs>.CUSTOMER WHERE c_custkey <= 10000` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_regexp_like_matches_native_oracle` |
| Exasol wrapper over a timestamp literal | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_timestamp_literal_matches_native_oracle` |
| Wrapper shapes unaffected by the dialect change | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | existing ten `*_matches_golden` tests, unchanged |

Unit tests are correct for the translator scenarios: `render_expression` and `render_expression_exasol` are pure JSON-to-string functions with no I/O and no ambient state. Every scenario whose truth depends on Exasol actually compiling the SQL maps to an integration test in `e2e_capability_test.rs`.

`e2e_count_distinct_regexp_like_matches_native_oracle` MUST put the predicate in the select list, not in a WHERE clause. A WHERE-clause `REGEXP_LIKE` is pushed into the scan (`build_qualified_single_table_fallback_sql`, `adapter/pushdown/joins/sql_builders.rs:744-828`, renders the filter through the DataFusion trio), so it never reaches the Exasol dialect and passes identically with and without the fix. The select-list form compiles natively: `SELECT COUNT(DISTINCT ('abc' REGEXP_LIKE 'a'))` succeeds on 2025.2.1.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-scalar-fns | `cargo test -p vs-expression exasol_dialect` | All Exasol-dialect tests pass, including `exasol_dialect_renders_declared_verbatim_surface` |
| vs-expression-translator-date-fns | In Exasol: `SELECT COUNT(DISTINCT YEAR(l_shipdate)) FROM <vs>.LINEITEM WHERE l_orderkey <= 10000` | A row count, not `function or script DATE_PART not found` |
| vs-expression-translator-scalar-fns | In Exasol: `SELECT l_returnflag, SIGN(SUM(l_discount) - 0.5) FROM <vs>.LINEITEM WHERE l_orderkey <= 10000 GROUP BY l_returnflag` | One row per return flag, not `function or script SIGNUM not found` |
| vs-expression-translator | In Exasol: `SELECT COUNT(DISTINCT (c_name REGEXP_LIKE '^C')) FROM <vs>.CUSTOMER WHERE c_custkey <= 10000` | A count, not `syntax error, unexpected REGEXP_LIKE_`. The predicate MUST sit in the select list: a WHERE-clause `REGEXP_LIKE` is pushed into the scan and rendered by the DataFusion trio, so it does not exercise the Exasol dialect at all. |
| vs-expression-translator-literals | In Exasol: `SELECT COUNT(DISTINCT CASE WHEN event_ts > TIMESTAMP '2020-01-01 00:00:00' THEN 1 ELSE 0 END) FROM <vs>.<typed_table>` | A count, not `function or script ARROW_CAST not found` |
| vs-expression-translator-scalar-ops | `cargo test -p vs-expression` | 0 failures, including `arithmetic_operators_render_identically_in_both_dialects`, `non_timestamp_literals_render_identically_in_both_dialects`, and `exasol_df_filter_suppresses_trivially_true` |
| All five | `EXPLAIN VIRTUAL <any Exasol query above>` then read the pushed SQL | The wrapper SQL contains no `signum`, `date_part`, `strpos`, `arrow_cast`, or `regexp_like(` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures, and it fails rather than skips if no Exasol container is up |
| Lint | `cargo clippy --all-targets` | 0 errors, 0 warnings |
| Format | `cargo fmt` | No changes |
| Specs | `speq plan validate fix-vs-expression-dialect` | pass |
