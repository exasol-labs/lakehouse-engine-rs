# Plan: fix-vs-expression-dialect

## Summary

Make the `vs-expression` translator render Exasol-native SQL on every Exasol-dialect path, not just for CAST targets. A pushed-down scalar function, predicate, or timestamp literal reaching an Exasol-parsed wrapper then compiles instead of aborting the query. Closes issue #209.

## Design

### Context

`crates/vs-expression` threads a `Dialect` parameter through every node of one recursive walker, but only `render_cast_target` reads it. Every other arm renders the DataFusion form unconditionally. Four consumer sites splice that output into wrapper SQL that Exasol's own core engine parses. Any renamed or re-shaped function is therefore a hard compilation error for the whole query. The table below records each rendering's SQL code: 42000 for a missing function, 22018 for the offset timestamp literal.

Two prior fixes already patched one arm each: issue #197 gave `MOD` a dialect branch, and issue #210 gave the string-function family an Exasol-verbatim arm. Neither established a stated rule, so each new arm arrives DataFusion-only by default. The just-shipped `*_BETWEEN` pushdown (`add-date-arithmetic-pushdown`) was broken from day one for exactly this reason.

Verified against live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`) during planning, the following renderings are hard failures on an Exasol-parsed path:

| Rendering | Exasol result |
|---|---|
| `signum(x)` | `function or script SIGNUM not found` (42000) |
| `date_part('YEAR', x)` | `function or script DATE_PART not found` (42000) |
| `arrow_cast(v, 'Timestamp(Microsecond, None)')` | `function or script ARROW_CAST not found` (42000) |
| `regexp_like(s, p)` | `syntax error, unexpected REGEXP_LIKE_` (42000) |
| `TIMESTAMP '<v>+00:00'` | `data exception - invalid character value for cast` (22018) |

`strpos` was the same defect, fixed by issue #210 (`lib.rs:819-830`). It survives here only as a regression guard in task 7's secondary token list, and its E2E row is labelled accordingly.

Two of the issue's claims did not survive verification and the plan corrects them: `current_date()` and `now()` both parse in Exasol, so they are not compilation errors. They are still changed, for the semantic reason given under Consequences.

- **Goals** - one stated rule that decides Exasol-dialect rendering for every node, owned in one place; every currently-broken Exasol-dialect rendering fixed; one declaration that gates `function_scalar` translation, so an undeclared name cannot be translated at all and a declared name cannot escape the sweep test; byte-identical DataFusion-dialect output.
- **Non-Goals**
  - No capability is added or withdrawn, and no new function is translated.
  - The DataFusion-dialect rendering of any node is not touched.
  - The type-blind translator stays type-blind (no column-type inspection), and `render_cast_target` keeps its existing per-dialect logic unchanged.
  - The DataFusion dialect keeps collapsing `SYSDATE` onto `current_date()` and `SYSTIMESTAMP` onto `now()`. A GitHub issue MUST be filed for that residual collapse before this plan is recorded (see decision-log [4]).
  - The Exasol-dialect rendering of `decimal_to_varchar_exasol` is unchanged. That adapter-synthesized node reaches only DataFusion-dialect renderers today (`adapter/pushdown/mod.rs:213`, `adapter/pushdown/support.rs:1125`), so there is no reachable failure to fix.

### Decision

**In the Exasol dialect, render what Exasol sent.** The expression tree comes from Exasol's own compiler, so reproducing the original name, argument order, and argument count means Exasol evaluates exactly the call it emitted. One declaration lists every `function_scalar` name the translator translates, each with its Exasol-dialect form. That declaration gates the dispatch: a name absent from it is declined in both dialects. An arm added without a declaration entry is therefore unreachable, not silently DataFusion-only. The sweep test iterates the same declaration, so a declared name with no sweep fixture fails the test. The DataFusion dialect keeps every existing translation, because DataFusion genuinely lacks functions of Exasol's names.

This generalizes what #197 and #210 each did for one arm into a rule with a single owner, rather than adding a third mechanism. Per `/speq:design-philosophy`, the target is one module owning one decision. The guarded Exasol arm #210 introduced becomes a gate ahead of the whole `function_scalar` dispatch, and the declaration behind that gate answers "what does the Exasol dialect do with this name" for every translated name at once. The four families that currently rewrite names join the declaration instead of each growing a private dialect branch.

#### Architecture

```
  render_expression_inner(node, dialect)
        │
        ├── dialect-invariant node types (unchanged)
        │     literals except timestamps, comparison and logical predicates
        │
        ├── "function_scalar"  ─►  GATE (task 1, this plan)
        │     │   look the uppercased `name` up in TRANSLATED_SCALAR_FNS, the one declaration
        │     │   absent ─► Err("unsupported scalar function: <name>")   both dialects
        │     │
        │     ├── Exasol dialect + declared form VerbatimCall   ◄── this plan
        │     │     ─► <NAME>(<rendered args>), no arity check          66 names
        │     ├── Exasol dialect + declared form BareKeyword    ◄── this plan
        │     │     ─► <NAME>, no parentheses                            4 now-family names
        │     └── declared form Shaped, or the DataFusion dialect
        │           ─► the per-name arm below, which owns both dialects  10 names
        │
        ├── per-name Shaped arms (arm order no longer carries dialect precedence)
        │     ADD/SUB/MULT/FLOAT_DIV/NEG ─► (<l> + <r>) and the rest    (existing)
        │     CAST ─────────────────────► render_cast_target            (existing, #211/#212)
        │     MOD  ─────────────────────► MOD(a,b) | (a % b)            (existing, #197)
        │     CONCAT ───────────────────► chained ||                    (existing, #200)
        │     CASE ─────────────────────► CASE WHEN … END, both dialects (existing, lib.rs:893)
        │     REGEXP_LIKE ──────────────► (s REGEXP_LIKE p) | regexp_like(s, p)   lib.rs:678
        │
        ├── DataFusion-dialect-only per-name arms (bodies unchanged, now unreachable from Exasol)
        │     math incl. SIGN→signum   lib.rs:699, :724, :738
        │     string family            lib.rs:832, INSTR :857, LOCATE :873
        │     date family              lib.rs:988-1131
        │
        └── node types outside `function_scalar`, each branching inline
              function_scalar_extract  ─► EXTRACT(F FROM x)
              predicate_like_regexp    ─► (s REGEXP_LIKE p)      lib.rs:497
              literal_timestamp[_utc]  ─► TIMESTAMP 'v'
```

The gate sits ahead of `match fn_name.as_str()`, so arm order no longer decides which dialect a name renders in. That is what fixes `SIGN`: widening the #210 guard in place at `lib.rs:819` would leave `SIGN` matching the math arm at `lib.rs:699` first and still rendering `signum(x)`, because that arm precedes it. Hoisting arms is therefore not needed either: a `Shaped` name reaches its own arm because the declaration says so, not because of where the arm sits. The math arms (`lib.rs:699`, `:724`, `:738`), `MOD` (`:755`), and `CONCAT` (`:783`) all keep their current positions.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| One declaration gating one dispatch | `TRANSLATED_SCALAR_FNS`, read at the head of the `function_scalar` arm | A reader finds every translated name and its Exasol form in one list, and an undeclared name cannot be translated at all, so the declaration cannot fall behind the dispatch |
| Declared form instead of arm ordering | `VerbatimCall` / `BareKeyword` / `Shaped` | The exclusions are stated where the names are declared, not implied by which arm happens to sit higher in a 500-line `match` |
| Inline `match dialect` | `function_scalar_extract`, `predicate_like_regexp`, timestamp literals, and the ten `Shaped` `function_scalar` names | These are distinct node types or shape changes, not verbatim-eligible calls; the #197 MOD precedent already reads this way |
| Sweep table derived from the declaration | new unit test over the full translated surface | The rule is only durable if a forgotten future name fails a test, not a review; iterating the declaration (rather than a parallel hand-written list) is what makes coverage structural |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One gate reading one declaration, ahead of the whole `function_scalar` dispatch | A `Dialect` trait with two impls, or a per-dialect rendering table keyed by name | Two impls would duplicate the thirty-eight arms the dialects agree on. A rendering table cannot express the `EXTRACT`, `REGEXP_LIKE`, or timestamp-literal shape changes, so it would be a second mechanism beside the arms rather than a replacement. |
| The declaration carries a per-name Exasol form (`VerbatimCall`, `BareKeyword`, `Shaped`) and gates translation, and task 7's sweep table is iterated from it | Spell the names inline in the guarded arm's pattern list (as #210 did), or keep a flat `EXASOL_VERBATIM_FNS` name set beside a hand-written sweep table | An inline pattern list is a second copy of the translated-name set. A flat set plus a hand-written sweep table leaves two ways to forget a name: omit it from the set, or omit its sweep row. Gating on the declaration removes the first, and iterating the declaration removes the second. |
| Test-enforced, not compile-enforced, sweep-fixture completeness | Replace the string dispatch with an enum carrying one variant per translated name, making all three `match`es exhaustive | The enum buys a compile error instead of a test failure on one of the three links, at the cost of about 160 lines of mechanical `wire_name`/`from_wire_name` boilerplate and a rewrite of all eighty arm patterns inside a 3,351-line file. The declaration gate already makes an undeclared name unreachable, which is the stronger half. |
| Fold in functions that already parse in Exasol (`NULLIFZERO`, `ZEROIFNULL`, `GREATEST`, `LEAST`, math names, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `DAYS_BETWEEN`) | Change only the arms that currently fail to compile | A rule applied to some arms and not others cannot be reasoned about. The next reader cannot tell which arms are principled and which merely happen to work. Output changes only in name case for most of these, and `NULLIFZERO`/`ZEROIFNULL` gain parity by construction. |
| Render the now-family as bare Exasol keywords, declared `BareKeyword` rather than special-cased inside the verbatim branch | Leave `current_date()` / `now()`, which do parse; or fold the four names into the verbatim-call set and except them from its shape | Not a compilation fix. The current mapping collapses `SYSDATE` onto `CURRENT_DATE` and `SYSTIMESTAMP` onto `CURRENT_TIMESTAMP`, erasing Exasol's database-time vs session-time distinction. A declared form makes the shape difference data, so the sweep derives their expectation instead of hard-coding four rows (decision-log [13]). |
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

One behavior change is not a bug fix. `SYSDATE` and `SYSTIMESTAMP` stop rendering as `CURRENT_DATE` and `CURRENT_TIMESTAMP` on the Exasol path, so Exasol applies its own database-time semantics. An operator comparing wrapper output against a prior run could see a different timestamp source. The DataFusion dialect keeps the collapse (`SYSDATE` → `current_date()`, `SYSTIMESTAMP` → `now()`), so the two dialects disagree for these four names. No query is known that evaluates one such node on both paths. A GitHub issue MUST be filed for the DataFusion-side collapse before recording (see § Non-Goals and decision-log [4]).

## Requirements

| Requirement | Details |
|-------------|---------|
| DataFusion output frozen | Every DataFusion-dialect rendering MUST stay byte-identical. Enforced by paired-dialect assertions on the same JSON node, the convention `renders_cast_timestamp_precision_per_dialect` already uses. |
| Exasol output must compile | Each new Exasol-dialect rendering MUST be a form verified to compile on live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`). The forms are recorded in the delta Background sections with their SQL codes. |
| Rule must be enforced, not documented | One table-driven unit test MUST render one node per declared `function_scalar` name, plus one per dialect-branching node type, in the Exasol dialect. Its `function_scalar` rows MUST be iterated from `TRANSLATED_SCALAR_FNS`, not from a hand-written parallel list, and the test MUST fail naming any declared name that has no fixture. For a name declared `VerbatimCall` the rendering MUST equal `<NAME>(<rendered args>)` from the node's own uppercased `name`; for a name declared `BareKeyword` it MUST equal the bare `<NAME>` with no parentheses. Seven constructs are outside the `<NAME>(<args>)` shape and MUST each match the expected string its fixture declares: (1) the operator wire names `ADD`, `SUB`, `MULT`, `FLOAT_DIV`, `NEG`; (2) `MOD` → `MOD(<a>, <b>)`; (3) `CONCAT` → chained `\|\|`; (4) `CAST` → `render_cast_target`'s per-dialect target; (5) the `function_scalar` `REGEXP_LIKE` alternate encoding (`lib.rs:678`) → `(<subject> REGEXP_LIKE <pattern>)`; (6) the four now-family names `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` → the bare keyword `<NAME>`; (7) `function_scalar` named `CASE` (`lib.rs:893`) → `CASE WHEN <cond> THEN <res> [ELSE <else>] END`. Every node type outside `function_scalar` MUST equal its per-dialect expected string. The `MOD` sweep row's Exasol-dialect rendering MUST equal `MOD(<a>, <b>)`. The DataFusion-only token list stays as a secondary assertion. |
| Declaration must gate the dispatch | A `function_scalar` name absent from `TRANSLATED_SCALAR_FNS` MUST be declined in both dialects with the existing `unsupported scalar function: <name>` error, before any per-name arm is reached. A per-name arm added without a declaration entry is therefore unreachable, so it cannot render DataFusion SQL on the Exasol path. |
| Golden wrapper fixtures unchanged | The ten `dispatch_golden` `.sql` fixtures MUST stay byte-identical. Checked during planning: none currently contains an affected rendering. |

## Dependencies

None. No new crate, no dependency bump, no external service. `crates/vs-expression` has only `serde_json` and `exasol-udf-sdk`, and the change adds no import.

## Implementation Tasks

1. Declare the translated `function_scalar` surface once, and gate the dispatch on that declaration. In `crates/vs-expression/src/lib.rs` add `enum ExasolForm { VerbatimCall, BareKeyword, Shaped }`, a `const TRANSLATED_SCALAR_FNS: &[(&str, ExasolForm)]` carrying one row per `function_scalar` name the arms at `lib.rs:625-1135` match today (80 names), and the two readers `declared_scalar_fn(name: &str) -> Option<ExasolForm>` and `is_exasol_verbatim(name: &str) -> bool`. At the head of the `"function_scalar"` arm (`lib.rs:618`), look the uppercased `name` up in the declaration; on `None` return the existing `unsupported scalar function: <name>` error, which is the same error the current `other =>` fall-through raises, so declines are unchanged. Then, still ahead of `match fn_name.as_str()`, render the Exasol dialect from the declared form: `VerbatimCall` → `<NAME>(<rendered args>)` with no arity check, `BareKeyword` → the bare `<NAME>`, `Shaped` → fall through to the per-name arm. Preserve the `function_scalar <NAME> missing 'arguments'` error when the `arguments` key is absent. Delete the now-redundant `if dialect == Dialect::Exasol` guard and body at `lib.rs:819-830`; its 23 string names become the only `VerbatimCall` rows in this task, and every other name is declared `Shaped`, so this task changes no rendering in either dialect. Add `undeclared_scalar_function_declines_in_both_dialects`; the whole existing suite MUST pass unchanged as the refactor's proof. [expert]
2. Reclassify the declaration's rows to widen the verbatim rule, and add the per-family paired-dialect tests. Move to `VerbatimCall`: the math family including `SIGN`, the field-shortcut date functions, `WEEK`, `DAYS_BETWEEN`, the rest of the `*_BETWEEN` family, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `GREATEST`, `LEAST`, `NULLIF`, `NULLIFZERO`, and `ZEROIFNULL`. Declare the four now-family names `BareKeyword`: they are NOT members of the verbatim-call set, because their Exasol form has no parentheses, and their DataFusion rendering stays in the existing `lib.rs:1041-1042` arm. Leave `Shaped` on the operator wire names, `MOD`, `CONCAT`, `CAST`, `REGEXP_LIKE`, and `CASE`. Do NOT widen the #210 guard in place at `lib.rs:819`: the math arms at `lib.rs:699`, `:724`, and `:738` precede it, so `SIGN` would keep matching the math arm first and still render `signum(x)`. Task 1 already moved the Exasol branch ahead of the whole `match fn_name.as_str()`, which subsumes that relocation, so no arm is hoisted and `MOD` (`lib.rs:755`) and `CONCAT` (`lib.rs:783`) keep their positions. Add paired-dialect unit tests per family, including the string family #210 shipped with no translator-side test: `renders_string_family_verbatim_in_exasol_dialect`, `renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect`, and `renders_nullif_verbatim_in_exasol_dialect`. [expert]
3. Branch `function_scalar_extract` on dialect: Exasol renders `EXTRACT(<FIELD> FROM <src>)` with the field as a bare keyword, DataFusion keeps `date_part('<FIELD>', <src>)`.
4. Branch `predicate_like_regexp` (`lib.rs:497`) **and** the `function_scalar` `REGEXP_LIKE` alternate encoding (`lib.rs:678`, declared `Shaped`) on dialect: Exasol renders the infix `(<subject> REGEXP_LIKE <pattern>)` from both, DataFusion keeps `regexp_like(<subject>, <pattern>)` from both. Keep the missing-operand error and the alternate encoding's arity error in both dialects, and assert the two encodings render byte-identically within a dialect.
5. Branch `literal_timestamp` and `literal_timestamp_utc` on dialect: Exasol renders `TIMESTAMP '<value>'` with the same quote escaping as `literal_string` and with no `+00:00` suffix for the UTC form. Keep both `arrow_cast` renderings byte-identical in the DataFusion dialect. [expert]
6. Rewrite the `Dialect` enum doc comment, which currently states that only `render_cast_target` branches on dialect. Replace that claim with the verbatim rule, `TRANSLATED_SCALAR_FNS` as the one declaration the gate reads, the meaning of each `ExasolForm`, and the seven constructs outside the `<NAME>(<args>)` shape — the operator wire names, `MOD`, `CONCAT`, `CAST`, the `REGEXP_LIKE` alternate encoding, the now-family, and `CASE` — with the reason each is outside it. State that the five dialect-branching node types outside `function_scalar` (`function_scalar_extract`, `function_scalar_cast`, `predicate_like_regexp`, `literal_timestamp`, `literal_timestamp_utc`) are covered by their own sweep rows, not by the declaration.
7. Add the systemic regression test `exasol_dialect_renders_declared_verbatim_surface`. Iterate its `function_scalar` rows from `TRANSLATED_SCALAR_FNS` and look each declared name up in a fixture map of representative nodes, failing and naming any declared name that has no fixture, and any fixture whose name is not declared. For a `VerbatimCall` row derive the expected Exasol string mechanically as `<NAME>(<rendered args>)` from the node's own uppercased `name`; for a `BareKeyword` row derive the bare `<NAME>`; for each of the ten `Shaped` names assert the expected string the fixture declares, which is `(<a> + <b>)` and the rest for the five operator wire names, `MOD(<a>, <b>)`, chained `||`, the per-dialect CAST target, `(<subject> REGEXP_LIKE <pattern>)`, and `CASE WHEN <cond> THEN <res> [ELSE <else>] END`. Add one row per dialect-branching node type outside `function_scalar` — `function_scalar_extract`, `function_scalar_cast`, `predicate_like_regexp`, `literal_timestamp`, `literal_timestamp_utc` — and assert each against its per-dialect expected string. Keep a secondary assertion that the swept output contains none of `signum`, `date_part`, `strpos`, `arrow_cast`, `character_length`, `octet_length`, `regexp_like(`, `current_date()`, `now()`, `nullif(`, or `coalesce(`. [expert]
8. Add decline-parity unit tests: the four regexp scalar functions and the thirteen unsupported date functions must error in `render_expression_exasol` and return `None` in `render_expression_exasol_safe`, matching the DataFusion dialect.
9. Add the paired-dialect freeze tests for the dialect-invariant surface: `arithmetic_operators_render_identically_in_both_dialects`, `non_timestamp_literals_render_identically_in_both_dialects`, and `exasol_df_filter_suppresses_trivially_true`.
10. Add E2E parity tests to `crates/lakehouse-engine/tests/e2e_capability_test.rs` for the seven queries in issue #209 (six of which fail today; the `INSTR` query is a #210 regression guard) plus a select-list `REGEXP_LIKE` and a timestamp literal in a wrapper, using the in-session native-oracle idiom already established in that file's section 8.16.
11. Confirm the ten `crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/*.sql` fixtures still match byte-for-byte, and re-baseline with a recorded reason if any changed.
12. Bump `crates/lakehouse-engine/Cargo.toml` from `0.30.8` to `0.30.9` and update `Cargo.lock`.

## Parallelization

| Group | Tasks |
|----------------|-------|
| Group A1 | Task 1 |
| Group A2 | Task 2 |
| Group B1 | Task 3 |
| Group B2 | Task 4 |
| Group B3 | Task 5 |
| Group B4 | Task 6 |
| Group C1 | Task 7 |
| Group C2 | Task 8 |
| Group C3 | Task 9 |
| Group D | Tasks 10, 11 |
| Group E | Task 12 |

Only Group D holds two tasks that genuinely run concurrently. Every other group is a single task, because tasks 1 through 9 all edit `crates/vs-expression/src/lib.rs` and concurrent sub-agent edits to one 3,351-line file conflict.

Sequential dependencies:

- Group A1, then A2. Task 1 is a no-op refactor whose proof is the unchanged suite passing, so mixing it with task 2's behavior change would destroy that proof.
- Group A2, then B1 through B4 in that order. Task 2 finishes the declaration that every later arm and test reads.
- Group B4, then C1 through C3 in that order. Tasks 7, 8, and 9 assert over the finished surface.
- Group D can start once Group B4 completes; task 10 touches only `e2e_capability_test.rs` and task 11 touches only fixtures, so the two run concurrently.
- Group D's task 10 additionally requires `make cross-musl-udf-build` plus the BucketFS SLC upload after Group B4; an E2E run against a stale `.so` tests the old rendering.
- Group E last.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Redundant match arm | The `if dialect == Dialect::Exasol` guard and body at `crates/vs-expression/src/lib.rs:819-830` | Task 1 deletes it. Its 23 string names are declared `VerbatimCall`, so the gate renders them ahead of the match and the arm can never be reached. The DataFusion string arm at `lib.rs:832` keeps its body. |
| None otherwise | - | Every other change adds a dialect branch. Every DataFusion-dialect rendering stays reachable. |

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
| NULLIF translates to the DataFusion nullif call (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `renders_nullif_verbatim_in_exasol_dialect` |
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
| An undeclared scalar function name is not translated in either dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `undeclared_scalar_function_declines_in_both_dialects` |
| The verbatim rule holds across the whole translated surface, and every declared name has a sweep fixture (Background of `vs-expression-translator`, `-scalar-fns`, `-date-fns`, `-literals`) | Unit | `crates/vs-expression/src/lib.rs` | `exasol_dialect_renders_declared_verbatim_surface` |
| Issue #209 repro: `COUNT(DISTINCT SIGN(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_sign_matches_native_oracle` |
| Issue #209 repro: `COUNT(DISTINCT YEAR(...))` and `COUNT(DISTINCT WEEK(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_date_field_matches_native_oracle` |
| Issue #209 repro: `COUNT(DISTINCT HOURS_BETWEEN(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_hours_between_matches_native_oracle` |
| Issue #210 regression guard — passes today: `COUNT(DISTINCT INSTR(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_instr_matches_native_oracle` |
| Issue #209 repro: grouped `SIGN(SUM(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_grouped_scalar_over_aggregate_sign_matches_native_oracle` |
| Issue #209 repro: grouped `YEAR(MIN(...))` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_grouped_scalar_over_aggregate_year_matches_native_oracle` |
| Exasol wrapper over a pushed REGEXP_LIKE predicate in a **select-list** position: `SELECT COUNT(DISTINCT (c_name REGEXP_LIKE '^C')) FROM <vs>.CUSTOMER WHERE c_custkey <= 10000` | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_regexp_like_matches_native_oracle` |
| Exasol wrapper over a timestamp literal | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_count_distinct_timestamp_literal_matches_native_oracle` |
| Wrapper shapes unaffected by the dialect change | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | existing ten `*_matches_golden` tests, unchanged |

Unit tests are correct for the translator scenarios: `render_expression` and `render_expression_exasol` are pure JSON-to-string functions with no I/O and no ambient state. Every scenario whose truth depends on Exasol actually compiling the SQL maps to an integration test in `e2e_capability_test.rs`.

`e2e_count_distinct_regexp_like_matches_native_oracle` MUST put the predicate in the select list, not in a WHERE clause. A WHERE-clause `REGEXP_LIKE` is applied inside the scan by `build_qualified_single_table_fallback_sql` (`adapter/pushdown/joins/sql_builders.rs:744-828`), which renders the filter through the DataFusion trio. Such a predicate never reaches the Exasol dialect, so the test passes identically with and without the fix. The select-list form compiles natively: `SELECT COUNT(DISTINCT ('abc' REGEXP_LIKE 'a'))` succeeds on 2025.2.1.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-scalar-fns | `cargo test -p vs-expression exasol_dialect` | All Exasol-dialect tests pass, including `exasol_dialect_renders_declared_verbatim_surface` |
| vs-expression-translator | Add a `"SUBSTRING" => …` arm to the `function_scalar` match without adding a `TRANSLATED_SCALAR_FNS` row, then `cargo test -p vs-expression` | The new arm is unreachable: a `SUBSTRING` node still returns `unsupported scalar function: SUBSTRING` in both dialects, so an undeclared arm cannot render DataFusion SQL on the Exasol path |
| vs-expression-translator | Delete one declared name's row from the sweep test's fixture map, then `cargo test -p vs-expression exasol_dialect_renders_declared_verbatim_surface` | The test fails and names the declared name that has no fixture |
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
