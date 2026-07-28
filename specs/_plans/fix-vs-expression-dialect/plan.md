# Plan: fix-vs-expression-dialect

## Summary

Make the `vs-expression` translator render Exasol-native SQL on every Exasol-dialect path, not only for CAST targets. Wrapper-bound scalar functions, predicates, and timestamp literals then compile, and the four now-family capabilities no rendering can fix are withdrawn, closing issue #209.

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

Two of the issue's claims did not survive verification and the plan corrects them: `current_date()` and `now()` both parse in Exasol, so they are not compilation errors. Their defect is semantic, and research found it is not a rendering defect at all.

`CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` are wrong on the scan path in every dialect. Exasol's four names are three semantics over one instant: `CURRENT_TIMESTAMP` reads it in the session zone, `SYSTIMESTAMP` reads the same instant in the database zone, and `CURRENT_DATE`/`SYSDATE` are `TO_DATE` of each. Rendering that needs `SESSIONTIMEZONE` and `DBTIMEZONE`. Neither reaches the scan UDF, which opens no connect-back session and holds no statement anchor, so it reads its container clock in UTC once per shard. This plan therefore withdraws the four capabilities rather than re-rendering them (decision-log [14]). Exasol never delegates an unadvertised capability, so all four become correct.

- **Goals** - one stated rule that decides Exasol-dialect rendering for every node, owned in one place; every currently-broken Exasol-dialect rendering fixed; one declaration that gates `function_scalar` translation, so an undeclared name cannot be translated at all and a declared name cannot escape the sweep test; the four now-family names withdrawn from both the advertised capability set and the translated set, so Exasol evaluates them; DataFusion-dialect output otherwise byte-identical.
- **Non-Goals**
  - No capability is added, and no new function is translated. Four are withdrawn: `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, `FN_SYSTIMESTAMP`.
  - The DataFusion-dialect rendering of every node except the four now-family names is not touched.
  - The type-blind translator stays type-blind (no column-type inspection), and `render_cast_target` keeps its existing per-dialect logic unchanged.
  - Restoring now-family pushdown with full time-zone fidelity is out of scope. That needs `SESSIONTIMEZONE`, `DBTIMEZONE`, and a statement-level timestamp anchor plumbed into `CommonScanSpec`, either over a new connect-back call from the adapter or as new scan-spec fields. Task 15 owns filing the GitHub issue that tracks it, and § Checklist gates on that issue existing before this plan is recorded (see decision-log [14]). Withdrawal leaves no correctness gap, so the issue tracks a lost optimization, not a deviation.
  - The Exasol-dialect rendering of `decimal_to_varchar_exasol` is unchanged. That adapter-synthesized node reaches only DataFusion-dialect renderers today (`adapter/pushdown/mod.rs:213`, `adapter/pushdown/support.rs:1125`), so there is no reachable failure to fix.

### Decision

**In the Exasol dialect, render what Exasol sent.** The expression tree comes from Exasol's own compiler, so reproducing the original name, argument order, and argument count means Exasol evaluates exactly the call it emitted. One declaration lists every `function_scalar` name the translator translates, each with its Exasol-dialect form. That declaration gates the dispatch: a name absent from it is declined in both dialects.

An arm added without a declaration entry is therefore unreachable, not silently DataFusion-only. The sweep test iterates the same declaration, so a declared name with no sweep fixture fails the test. The DataFusion dialect keeps every existing translation except the now-family's, because DataFusion genuinely lacks functions of Exasol's names.

**Where no rendering can be right, withdraw the capability instead.** The rule above works because Exasol's compiler emitted the call and Exasol's engine will evaluate it. It cannot help a function whose value depends on context the scan never receives. For the four now-family names the honest move is to stop advertising them, so Exasol keeps the work, and to remove them from the declaration so neither dialect translates them. Withdrawal is also the safe direction: Exasol never delegates an unadvertised capability, whereas advertising one with no faithful backing path is what produces silent wrong answers on this codebase's own record (`vs-adapter/pushdown-planning-order-by-capability`).

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
        │     │   the 4 now-family names are absent by design (task 2)   ◄── this plan
        │     │
        │     ├── Exasol dialect + declared form VerbatimCall   ◄── this plan
        │     │     ─► <NAME>(<rendered args>), no arity check          66 names
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
        │     date family              lib.rs:988-1131, minus the now-family arms
        │                              at :1041-1042, which task 2 deletes
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
| Declared form instead of arm ordering | `VerbatimCall` / `Shaped` | The exclusions are stated where the names are declared, not implied by which arm happens to sit higher in a 500-line `match` |
| Absence from the declaration retires a translation | the four now-family names | One edit removes a name from both dialects at once, and the gate makes the removal total rather than partial. Paired with withdrawing the capability, this is the same shape the regexp, bitwise, and `ADD_*` withdrawals already use |
| Inline `match dialect` | `function_scalar_extract`, `predicate_like_regexp`, timestamp literals, and the ten `Shaped` `function_scalar` names | These are distinct node types or shape changes, not verbatim-eligible calls; the #197 MOD precedent already reads this way |
| Sweep table derived from the declaration | new unit test over the full translated surface | The rule is only durable if a forgotten future name fails a test, not a review; iterating the declaration (rather than a parallel hand-written list) is what makes coverage structural |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| One gate reading one declaration, ahead of the whole `function_scalar` dispatch | A `Dialect` trait with two impls, or a per-dialect rendering table keyed by name | Two impls would duplicate the thirty-eight arms the dialects agree on. A rendering table cannot express the `EXTRACT`, `REGEXP_LIKE`, or timestamp-literal shape changes, so it would be a second mechanism beside the arms rather than a replacement. |
| The declaration carries a per-name Exasol form (`VerbatimCall`, `Shaped`) and gates translation, and task 7's sweep table is iterated from it | Spell the names inline in the guarded arm's pattern list (as #210 did), or keep a flat `EXASOL_VERBATIM_FNS` name set beside a hand-written sweep table | An inline pattern list is a second copy of the translated-name set. A flat set plus a hand-written sweep table leaves two ways to forget a name: omit it from the set, or omit its sweep row. Gating on the declaration removes the first, and iterating the declaration removes the second. |
| Test-enforced, not compile-enforced, sweep-fixture completeness | Replace the string dispatch with an enum carrying one variant per translated name, making all three `match`es exhaustive | The enum buys a compile error instead of a test failure on one of the three links, at the cost of about 160 lines of mechanical `wire_name`/`from_wire_name` boilerplate and a rewrite of all eighty arm patterns inside a 3,351-line file. The declaration gate already makes an undeclared name unreachable, which is the stronger half. |
| Fold in functions that already parse in Exasol (`NULLIFZERO`, `ZEROIFNULL`, `GREATEST`, `LEAST`, math names, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `DAYS_BETWEEN`) | Change only the arms that currently fail to compile | A rule applied to some arms and not others cannot be reasoned about. The next reader cannot tell which arms are principled and which merely happen to work. Output changes only in name case for most of these, and `NULLIFZERO`/`ZEROIFNULL` gain parity by construction. |
| Withdraw the four now-family capabilities and stop translating the names in both dialects | (a) Accept the divergence and file an issue; (b) render them as bare Exasol keywords, which earlier revisions of this plan did; (c) plumb `SESSIONTIMEZONE`, `DBTIMEZONE`, and a statement anchor into the scan spec over a new connect-back call | (a) leaves a silent wrong answer advertised. (b) fixes only the wrapper path and leaves the scan path reading a per-shard UTC container clock, so two dialects would disagree and both would still be wrong for `SYSTIMESTAMP`. (c) is the only route to correct pushdown, and it is a scan-spec and connect-back change far outside a rendering fix. Withdrawal is correct today at the cost of pushdown for these four names (decision-log [14]). |
| Exasol dialect drops per-arm arity checks | Keep arity validation in both dialects | Exasol's compiler emitted a call its own engine accepts, and Exasol's `INSTR(s, sub, start)` already relies on this (#210). A translator-side arity check on that path can only reject valid input. |
| No capability change for any function whose Exasol rendering was merely broken | Withdraw those capabilities too, until each fix is proven end to end | The advertisement governs what Exasol may push to the node-local DataFusion scan, and the DataFusion-dialect rendering of every one of those functions is correct and unchanged. Withdrawing them would remove working pushdown to fix a wrapper-only defect. The now-family is the one exception, because its DataFusion-dialect rendering is the part that is wrong. |
| The now-family's translator arms are deleted, not left in place as unreachable code | Keep the arms and only withdraw the capability, so a stray node still renders something | Once the capability is gone, no production path can deliver such a node — the four names are synthesized nowhere in the adapter and every translator call site is fed raw pushdown JSON. Keeping the arms would leave an advertised-set-exceeds-translated-set inversion and force the sweep test to assert `current_date()` as an Exasol-dialect rendering, contradicting its own token deny-list. Deleting them also makes a hypothetical stray node fail loudly instead of returning a wrong timestamp. This follows the same precedent that dropped `decimal_to_varchar_exasol` from this plan for being unreachable. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-fns | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| sql-comprehension/vs-expression-translator-date-fns | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-date-fns/spec.md` |
| sql-comprehension/vs-expression-translator-literals | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-literals/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `specs/_plans/fix-vs-expression-dialect/sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `specs/_plans/fix-vs-expression-dialect/vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `specs/_plans/fix-vs-expression-dialect/vs-adapter/create-virtual-schema/spec.md` |

## Impact

Queries that fail today start returning results. Any query routing a renamed function through an Exasol-side wrapper currently aborts with a compilation error. Examples: `COUNT(DISTINCT SIGN(c_acctbal))`, `COUNT(DISTINCT YEAR(l_shipdate))`, `SIGN(SUM(l_discount) - 0.5)` grouped by a key, and the `HOURS_BETWEEN` family shipped in `add-date-arithmetic-pushdown`. Issue #209 lists seven such queries verified failing against a virtual schema and passing against a native Exasol table.

Two further failure paths were found during planning and are fixed in the same change. A pushed `REGEXP_LIKE` predicate renders as a function call, which Exasol's parser rejects, and `FN_PRED_REGEXP_LIKE` is advertised. A timestamp literal reaching a wrapper renders through `arrow_cast`, which is DataFusion-only.

No breaking changes. The DataFusion-dialect rendering of every node except the four now-family names stays byte-identical. The paired-dialect unit assertions and the ten committed `dispatch_golden` wrapper fixtures both hold that in place.

**The now-family becomes correct, and loses pushdown.** `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` return wrong values today wherever they push down. This was measured on the pinned 2025.2.1 container, not inferred from the capability list. A select-list `SYSTIMESTAMP` returned `15:02:02` through the virtual schema against `17:02:03` from Exasol in the same session. The scan reads its container's UTC clock, in neither Exasol zone, with no statement anchor. Each of the G shards re-reads that clock independently, so one statement returned two different timestamps over a two-file table. Exasol's now-family is statement-constant and zone-aware. Withdrawing the four capabilities hands all four back to Exasol, which evaluates them once, in the right zone, with the right type. An operator comparing a now-family value against a prior run SHOULD expect it to change, and to become right.

The cost is bounded. The filter position loses real pushdown for all four names. A predicate containing one is no longer pushed, so it prunes no Iceberg files and skips no row groups. Exasol applies that predicate over the returned rows. The select-list position also loses pushdown, but only for `SYSTIMESTAMP`, `CURRENT_DATE`, and `SYSDATE`. Those three declare plain `TIMESTAMP` or `DATE`, which passes `is_valid_emits_output_type`, so each was a pushed projection until now. A select-list `CURRENT_TIMESTAMP` loses nothing, because it already widened to the full-row fallback. Its declared `TIMESTAMP WITH LOCAL TIME ZONE` fails that same check (`adapter/pushdown/support.rs:1016-1018`). Nothing else narrows. `FN_DATE_TRUNC`, `FN_EXTRACT`, the field shortcuts, `FN_TO_DATE`, `FN_TO_TIMESTAMP`, and the `*_BETWEEN` family all stay advertised. Each reads its datetime from its own arguments rather than from a clock.

Withdrawal cannot lose or mistranslate a clause. Exasol never delegates a capability the adapter does not advertise. An unadvertised function is one Exasol keeps and computes itself (`docs/capabilities.md` § Handled by Exasol). The failure mode that produces silent wrong answers in this codebase is the opposite operation: advertising a capability with no faithful backing path. This codebase verified that live for `ORDER_BY_EXPRESSION` (`specs/vs-adapter/pushdown-planning-order-by-capability/spec.md`). Withdrawal moves these four names from the delegated side to the Exasol-evaluated side.

## Requirements

| Requirement | Details |
|-------------|---------|
| DataFusion output frozen, with one stated exception | Every DataFusion-dialect rendering MUST stay byte-identical, except the four now-family names, whose DataFusion rendering is deleted along with their Exasol one. Enforced by paired-dialect assertions on the same JSON node, the convention `renders_cast_timestamp_precision_per_dialect` already uses. |
| Withdrawal MUST be total and paired | The four now-family names MUST be absent from `TRANSLATED_SCALAR_FNS`, absent from every per-name arm, and absent from `CAPABILITIES`. No intermediate state MAY advertise a capability the translator declines: tasks 2 and 12 MUST land in the same commit. `reports_audited_capability_set` MUST assert all four are NOT advertised. |
| Exasol output must compile | Each new Exasol-dialect rendering MUST be a form verified to compile on live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`). The forms are recorded in the delta Background sections with their SQL codes. |
| Rule must be enforced, not documented | One table-driven unit test MUST render one node per declared `function_scalar` name, plus one per dialect-branching node type, in the Exasol dialect. Its `function_scalar` rows MUST be iterated from `TRANSLATED_SCALAR_FNS`, not from a hand-written parallel list, and the test MUST fail naming any declared name that has no fixture. For a name declared `VerbatimCall` the rendering MUST equal `<NAME>(<rendered args>)` from the node's own uppercased `name`. Six constructs are outside the `<NAME>(<args>)` shape and MUST each match the expected string its fixture declares: (1) the operator wire names `ADD`, `SUB`, `MULT`, `FLOAT_DIV`, `NEG`; (2) `MOD` → `MOD(<a>, <b>)`; (3) `CONCAT` → chained `\|\|`; (4) `CAST` → `render_cast_target`'s per-dialect target; (5) the `function_scalar` `REGEXP_LIKE` alternate encoding (`lib.rs:678`) → `(<subject> REGEXP_LIKE <pattern>)`; (6) `function_scalar` named `CASE` (`lib.rs:893`) → `CASE WHEN <cond> THEN <res> [ELSE <else>] END`. Every node type outside `function_scalar` MUST equal its per-dialect expected string. The `MOD` sweep row's Exasol-dialect rendering MUST equal `MOD(<a>, <b>)`. The DataFusion-only token list stays as a secondary assertion, and its `current_date()` and `now()` entries become a live guard once the now-family is undeclared. |
| Declaration must gate the dispatch | A `function_scalar` name absent from `TRANSLATED_SCALAR_FNS` MUST be declined in both dialects with the existing `unsupported scalar function: <name>` error, before any per-name arm is reached. A per-name arm added without a declaration entry is therefore unreachable, so it cannot render DataFusion SQL on the Exasol path. |
| Golden wrapper fixtures unchanged | The ten `dispatch_golden` `.sql` fixtures MUST stay byte-identical. Checked during planning: none currently contains an affected rendering. |

## Dependencies

None. No new crate, no dependency bump, no external service. `crates/vs-expression` has only `serde_json` and `exasol-udf-sdk`, and the change adds no import.

## Implementation Tasks

1. Declare the translated `function_scalar` surface once, and gate the dispatch on that declaration. In `crates/vs-expression/src/lib.rs` add `enum ExasolForm { VerbatimCall, Shaped }`, a `const TRANSLATED_SCALAR_FNS: &[(&str, ExasolForm)]` carrying one row per `function_scalar` name the arms at `lib.rs:625-1135` match today (80 names, including the four now-family names task 2 then removes), and the two readers `declared_scalar_fn(name: &str) -> Option<ExasolForm>` and `is_exasol_verbatim(name: &str) -> bool`. At the head of the `"function_scalar"` arm (`lib.rs:618`), look the uppercased `name` up in the declaration; on `None` return the existing `unsupported scalar function: <name>` error, which is the same error the current `other =>` fall-through raises, so declines are unchanged. Then, still ahead of `match fn_name.as_str()`, render the Exasol dialect from the declared form: `VerbatimCall` → `<NAME>(<rendered args>)` with no arity check, `Shaped` → fall through to the per-name arm. Preserve the `function_scalar <NAME> missing 'arguments'` error when the `arguments` key is absent. Delete the now-redundant `if dialect == Dialect::Exasol` guard and body at `lib.rs:819-830`; its 23 string names become the only `VerbatimCall` rows in this task, and every other name is declared `Shaped`, so this task changes no rendering in either dialect. Add `undeclared_scalar_function_declines_in_both_dialects`; the whole existing suite MUST pass unchanged as the refactor's proof. [expert]
2. Reclassify the declaration's rows to widen the verbatim rule, retire the now-family, and add the per-family paired-dialect tests. Move to `VerbatimCall`: the math family including `SIGN`, the field-shortcut date functions, `WEEK`, `DAYS_BETWEEN`, the rest of the `*_BETWEEN` family, `DATE_TRUNC`, `TO_DATE`, `TO_TIMESTAMP`, `GREATEST`, `LEAST`, `NULLIF`, `NULLIFZERO`, and `ZEROIFNULL`. Delete the four now-family rows `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, and `SYSTIMESTAMP` from `TRANSLATED_SCALAR_FNS` entirely, leaving 76 declared names, and delete the two arms at `lib.rs:1041-1042` they were the only reachable path to. The gate then declines all four in both dialects with `unsupported scalar function: <name>`. Replace the existing `renders_now_family` test (`lib.rs:2664-2684`, which asserts `current_date()` and `now()`) with `now_family_falls_through`, modelled on `bitwise_operator_functions_fall_through` (`lib.rs:3005-3069`): assert the error message contains both the function name and the literal `unsupported scalar function`, in both dialects, and that both safe variants return `None`. Leave `Shaped` on the operator wire names, `MOD`, `CONCAT`, `CAST`, `REGEXP_LIKE`, and `CASE`. Do NOT widen the #210 guard in place at `lib.rs:819`: the math arms at `lib.rs:699`, `:724`, and `:738` precede it, so `SIGN` would keep matching the math arm first and still render `signum(x)`. Task 1 already moved the Exasol branch ahead of the whole `match fn_name.as_str()`, which subsumes that relocation, so no arm is hoisted and `MOD` (`lib.rs:755`) and `CONCAT` (`lib.rs:783`) keep their positions. Add paired-dialect unit tests per family, including the string family #210 shipped with no translator-side test: `renders_string_family_verbatim_in_exasol_dialect`, `renders_instr_locate_verbatim_with_start_arg_in_exasol_dialect`, and `renders_nullif_verbatim_in_exasol_dialect`. [expert]
3. Branch `function_scalar_extract` on dialect: Exasol renders `EXTRACT(<FIELD> FROM <src>)` with the field as a bare keyword, DataFusion keeps `date_part('<FIELD>', <src>)`.
4. Branch `predicate_like_regexp` (`lib.rs:497`) **and** the `function_scalar` `REGEXP_LIKE` alternate encoding (`lib.rs:678`, declared `Shaped`) on dialect: Exasol renders the infix `(<subject> REGEXP_LIKE <pattern>)` from both, DataFusion keeps `regexp_like(<subject>, <pattern>)` from both. Keep the missing-operand error and the alternate encoding's arity error in both dialects, and assert the two encodings render byte-identically within a dialect.
5. Branch `literal_timestamp` and `literal_timestamp_utc` on dialect: Exasol renders `TIMESTAMP '<value>'` with the same quote escaping as `literal_string` and with no `+00:00` suffix for the UTC form. Keep both `arrow_cast` renderings byte-identical in the DataFusion dialect. [expert]
6. Rewrite the `Dialect` enum doc comment, which currently states that only `render_cast_target` branches on dialect. Replace that claim with the verbatim rule, `TRANSLATED_SCALAR_FNS` as the one declaration the gate reads, the meaning of each `ExasolForm`, and the six constructs outside the `<NAME>(<args>)` shape — the operator wire names, `MOD`, `CONCAT`, `CAST`, the `REGEXP_LIKE` alternate encoding, and `CASE` — with the reason each is outside it. State that absence from the declaration is how a translation is retired, name the now-family as the current instance, and give the reason: the scan receives no time zone, no clock, and no statement anchor, so `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` are unadvertised and Exasol evaluates them. State that the five dialect-branching node types outside `function_scalar` (`function_scalar_extract`, `function_scalar_cast`, `predicate_like_regexp`, `literal_timestamp`, `literal_timestamp_utc`) are covered by their own sweep rows, not by the declaration.
7. Add the systemic regression test `exasol_dialect_renders_declared_verbatim_surface`. Iterate its `function_scalar` rows from `TRANSLATED_SCALAR_FNS` and look each declared name up in a fixture map of representative nodes, failing and naming any declared name that has no fixture, and any fixture whose name is not declared. For a `VerbatimCall` row derive the expected Exasol string mechanically as `<NAME>(<rendered args>)` from the node's own uppercased `name`; for each of the ten `Shaped` names assert the expected string the fixture declares, which is `(<a> + <b>)` and the rest for the five operator wire names, `MOD(<a>, <b>)`, chained `||`, the per-dialect CAST target, `(<subject> REGEXP_LIKE <pattern>)`, and `CASE WHEN <cond> THEN <res> [ELSE <else>] END`. Add one row per dialect-branching node type outside `function_scalar` — `function_scalar_extract`, `function_scalar_cast`, `predicate_like_regexp`, `literal_timestamp`, `literal_timestamp_utc` — and assert each against its per-dialect expected string. Keep a secondary assertion that the swept output contains none of `signum`, `date_part`, `strpos`, `arrow_cast`, `character_length`, `octet_length`, `regexp_like(`, `current_date()`, `now()`, `nullif(`, or `coalesce(`. [expert]
8. Add decline-parity unit tests: the four regexp scalar functions and the thirteen unsupported date functions must error in `render_expression_exasol` and return `None` in `render_expression_exasol_safe`, matching the DataFusion dialect.
9. Add the paired-dialect freeze tests for the dialect-invariant surface: `arithmetic_operators_render_identically_in_both_dialects`, `non_timestamp_literals_render_identically_in_both_dialects`, and `exasol_df_filter_suppresses_trivially_true`.
10. Add E2E parity tests to `crates/lakehouse-engine/tests/e2e_capability_test.rs` for the seven queries in issue #209 (six of which fail today; the `INSTR` query is a #210 regression guard) plus a select-list `REGEXP_LIKE` and a timestamp literal in a wrapper, using the in-session native-oracle idiom already established in that file's section 8.16. Add one further test for the now-family withdrawal, `e2e_now_family_matches_native_oracle`, in a new section 8.19. **Put it in a select-list `SYSTIMESTAMP` position, not a select-list `CURRENT_TIMESTAMP` one.** `CURRENT_TIMESTAMP` is declared `TIMESTAMP(3) WITH LOCAL TIME ZONE`, which fails `is_valid_emits_output_type` (`adapter/pushdown/support.rs:1016-1018`), so that position never emits a pushed scan projection and cannot show the defect either way. `SYSTIMESTAMP` is declared plain `TIMESTAMP(3)` and `CURRENT_DATE`/`SYSDATE` are `DATE`, so all three pass that gate. Verified pre-withdrawal against the pinned container: a select-list `SYSTIMESTAMP` pushes `"projection":[{"expr":"now()"},"ID"]` with `"emit_exa_types":["TIMESTAMP(3)", …]`. The test MUST make three assertions. (a) A precondition that `DBTIMEZONE` is not `UTC`, read via `SELECT DBTIMEZONE`, because with the database zone at UTC the value comparison is vacuous; the pinned image defaults to `EUROPE/BERLIN`, and the test MUST fail rather than skip if the zone is UTC. (b) `SELECT SYSTIMESTAMP FROM <vs>.<table>` returns exactly ONE distinct value across all returned rows, which is the statement-constancy Exasol guarantees; pre-withdrawal this returned one distinct value per shard, measured as two over a two-file table. (c) That value is within 60 seconds of an in-session native oracle `SELECT SYSTIMESTAMP` with no virtual schema reference, run on the same connection. A 60-second tolerance is correct because the two statements execute at different instants while the defect it catches is a whole-zone offset of one hour or more. Do NOT assert exact equality. Do NOT use a pure-constant predicate such as `WHERE CURRENT_TIMESTAMP > TIMESTAMP '<t>'` as the probe: Exasol constant-folds it before building the pushdown request, so nothing is pushed and the test would pass in both states.
11. Confirm the ten `crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/*.sql` fixtures still match byte-for-byte, and re-baseline with a recorded reason if any changed.
12. Withdraw the four now-family capabilities. In `crates/lakehouse-engine/src/adapter/capabilities.rs` delete `"FN_CURRENT_DATE"`, `"FN_CURRENT_TIMESTAMP"` (`:113-114`), `"FN_SYSDATE"`, and `"FN_SYSTIMESTAMP"` (`:122-123`) from `CAPABILITIES`, and add a rationale comment in the date/time block modelled on the existing `ADD_HOURS`/`ADD_MINUTES` note (`:131-138`): the scan UDF receives neither `SESSIONTIMEZONE` nor `DBTIMEZONE`, opens no connect-back session, and reads its container clock in UTC once per shard, so no rendering matches Exasol's statement-constant, zone-aware now-family. In `reports_audited_capability_set`, move all four names out of the positive advertised-set loop (`:481-502`) into the "declined translations must stay unadvertised" negative loop (`:336-366`). This task MUST land in the same commit as task 2, so no build advertises a capability the translator declines. Two spec deltas govern this withdrawal and both MUST be satisfied: `specs/_plans/fix-vs-expression-dialect/vs-adapter/pushdown-planning-capability-extensions/spec.md` owns the withdrawal and its reason, and `specs/_plans/fix-vs-expression-dialect/vs-adapter/create-virtual-schema/spec.md` extends that feature's own "capabilities list MUST NOT include" enumeration with the four names, so the sibling that carries the deliberate-absence list cannot fall behind (the drift pattern decision [9] records from issue #210).
13. Update the operator-facing capability documentation. In `docs/capabilities.md` remove `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` from the Date / time row of the § Scalar functions table (`:77`), leaving the ten remaining names and the `WHERE YEAR(ts) = 2024` example. Amend the § Handled by Exasol introduction (`:107`) first: it currently gives one reason for a capability living in that section, "These capabilities are not decomposable into a partial/merge plan", which is false for the now-family. Replace that sentence with two admitted reasons, so the section covers both: a capability is not decomposable into a partial/merge plan, OR the scan cannot evaluate it at all because the scan holds no clock, time zone, or statement context. Then give the now-family **its own row**, not an extension of the existing `Geospatial, session functions` row, because that row reads `Exasol-side / unsupported` with no example and the now-family is supported by Exasol and worth an example. The new row reads `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` with an example and the reason: the scan receives neither `SESSIONTIMEZONE` nor `DBTIMEZONE` and holds no statement anchor, so Exasol evaluates the clock itself, once per statement, in its own zones. State that results are correct and that a predicate over one of these names prunes no files. Check the other `docs/` pages for the same claim and correct any hit: `grep -rn "CURRENT_DATE\|SYSDATE\|CURRENT_TIMESTAMP\|SYSTIMESTAMP" docs/` MUST return only the new § Handled by Exasol row when the task is done. That grep is a one-directional gate on the four withdrawn names and MUST NOT be read as a completeness guarantee for the table: the Date / time row's pre-existing omission of `FN_WEEK`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, and `FN_SECONDS_BETWEEN`, all of which `capabilities.rs` advertises, predates this plan and is out of its scope.
14. Bump `crates/lakehouse-engine/Cargo.toml` from `0.30.8` to `0.30.9` and update `Cargo.lock`.
15. File the tracking issue for the option-C restoration named in § Non-Goals, so the withdrawn pushdown is tracked rather than silently dropped. Run `ghbrk gh issue create` with a title naming the restoration of now-family pushdown and a body stating: the four capabilities to restore (`FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, `FN_SYSTIMESTAMP`); that restoring them requires `SESSIONTIMEZONE`, `DBTIMEZONE`, and a statement-level timestamp anchor plumbed into `CommonScanSpec` (`scan/spec.rs:588`), obtained over a new adapter-side connect-back call, because the scan holds none of the three today; that the translator must then render `CURRENT_TIMESTAMP`/`SYSTIMESTAMP` against the session and database zones respectively rather than collapsing both onto `now()`; and that this plan withdrew the capabilities instead, so the issue tracks a lost optimization and not a correctness deviation. Reference decision-log [14] and this plan by name. Record the issue number in § Non-Goals bullet 4 once created.

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
| Group D | Tasks 10, 11, 12, 13 |
| Group E | Task 14 |
| Group F | Task 15 |

Only Group D holds tasks that genuinely run concurrently, and its four tasks touch four disjoint files: `e2e_capability_test.rs`, the golden fixtures, `capabilities.rs`, and `docs/capabilities.md`. Every other group is a single task, because tasks 1 through 9 all edit `crates/vs-expression/src/lib.rs` and concurrent sub-agent edits to one 3,351-line file conflict.

Sequential dependencies:

- Group A1, then A2. Task 1 is a no-op refactor whose proof is the unchanged suite passing, so mixing it with task 2's behavior change would destroy that proof.
- Group A2, then B1 through B4 in that order. Task 2 finishes the declaration that every later arm and test reads.
- Group B4, then C1 through C3 in that order. Tasks 7, 8, and 9 assert over the finished surface.
- Group D can start once Group B4 completes; its four tasks touch four disjoint files, so they run concurrently.
- Group D's task 10 additionally requires `make cross-musl-udf-build` plus the BucketFS SLC upload after Group B4; an E2E run against a stale `.so` tests the old rendering.
- Task 12 MUST be committed together with task 2, per § Requirements. Task 12 is grouped with D because it touches a different file, not because it may ship separately.
- Group E, then Group F last. Task 15 files the restoration issue and touches no source file, so it runs after the code is final and before recording.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Redundant match arm | The `if dialect == Dialect::Exasol` guard and body at `crates/vs-expression/src/lib.rs:819-830` | Task 1 deletes it. Its 23 string names are declared `VerbatimCall`, so the gate renders them ahead of the match and the arm can never be reached. The DataFusion string arm at `lib.rs:832` keeps its body. |
| Unreachable match arms | The two now-family arms at `crates/vs-expression/src/lib.rs:1041-1042` | Task 2 deletes them with the four declaration rows. Once the names are undeclared the gate declines them before the match, and once the capabilities are withdrawn Exasol pushes no such node. No adapter code synthesizes one: every translator call site is fed raw pushdown JSON, and the only nodes the adapter builds itself are `decimal_to_varchar_exasol`, `function_scalar_cast`, a sentinel `column`, and a `predicate_and`. |
| Obsolete test | `renders_now_family` (`crates/vs-expression/src/lib.rs:2664-2684`) | Task 2 replaces it with `now_family_falls_through`. It asserts the `current_date()` and `now()` renderings the same task deletes. |
| None otherwise | - | Every other change adds a dialect branch. Every other DataFusion-dialect rendering stays reachable. |

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
| The now-family is not translated in either dialect (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `now_family_falls_through` |
| Now-family date/time capabilities are withdrawn so Exasol evaluates its own clock (advertised-set clause, spec step `:30`) | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` |
| Now-family ... (Exasol evaluates natively rather than pushing to the scan, spec step `:31`) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_now_family_matches_native_oracle` |
| Now-family ... (`docs/capabilities.md` does not list the four withdrawn names, spec step `:34`) | Manual | `docs/capabilities.md` | `grep -rn "CURRENT_DATE\|SYSDATE\|CURRENT_TIMESTAMP\|SYSTIMESTAMP" docs/` returns only the new § Handled by Exasol row (task 13's done-condition) |
| The `create-virtual-schema` deliberate-absence list names the four withdrawn capabilities (NEW) | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` negative loop |
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
| Withdrawal parity: a select-list `SYSTIMESTAMP` is statement-constant and in the database zone | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_now_family_matches_native_oracle` |
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
| pushdown-planning-capability-extensions | `cargo test -p lakehouse-engine reports_audited_capability_set` | Passes with `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP` asserted absent |
| pushdown-planning-capability-extensions | **Zone precondition, run first:** `SELECT DBTIMEZONE, SESSIONTIMEZONE` | Neither is `UTC`. The pinned `exasol/docker-db:2025.2.1` image returns `EUROPE/BERLIN` for both, against a container OS clock at UTC, so the offset is two hours in CEST. **With both zones at UTC every value comparison below is vacuous and proves nothing.** If a future image defaults to UTC, set a non-UTC session zone with `ALTER SESSION SET TIME_ZONE = 'EUROPE/BERLIN'` before running the value rows, and record that only the session zone is settable per session while `DBTIMEZONE` is fixed at database creation |
| pushdown-planning-capability-extensions | **Pre-withdrawal baseline. Run `make cross-musl-udf-build` and `make bucketfs-upload-so` FIRST, then** `EXPLAIN VIRTUAL SELECT SYSTIMESTAMP, id FROM <vs>.<table> WHERE id = 1`. A stale BucketFS `.so` gives a wrong answer here: during planning the deployed artifact predated the `is_valid_emits_output_type` gate and made a select-list `CURRENT_TIMESTAMP` hard-fail with SQL state 22002 instead of falling back (decision-log [14], method caveat) | Records whether a now-family node is pushed at all. Observed during planning on the pinned container: the pushed scan spec contains `"projection":[{"expr":"now()"},"ID"]` with `"emit_exa_types":["TIMESTAMP(3)","DECIMAL(20,0)"]`, so the node IS pushed and the scan-path defect is reachable. `CURRENT_DATE` and `SYSDATE` likewise push `{"expr":"current_date()"}` with `DATE`. A filter-position `WHERE event_ts > CURRENT_TIMESTAMP` pushes `"filter":"(now() < \"EVENT_TS\")"`. If a future build pushes none of these, the withdrawal removes only unreachable advertisement and § Impact ¶4 MUST be weakened accordingly |
| pushdown-planning-capability-extensions | **Pre-withdrawal wrongness baseline:** `SELECT SYSTIMESTAMP FROM <vs>.<table> WHERE id = 1`, then `SELECT SYSTIMESTAMP` with no virtual schema, same session | Observed during planning: `15:02:02.716` through the virtual schema against `17:02:03.141` natively, a two-hour error equal to the CEST offset. Also `SELECT SYSTIMESTAMP, COUNT(*) FROM <vs>.<table> GROUP BY SYSTIMESTAMP` returned two distinct timestamps over a two-file table, against one statement-constant value natively |
| pushdown-planning-capability-extensions | In Exasol after task 12: `EXPLAIN VIRTUAL SELECT id FROM <vs>.<table> WHERE event_ts > CURRENT_TIMESTAMP` | The pushed SQL carries no `filter` for that predicate and contains neither `now()` nor `CURRENT_TIMESTAMP`. The query returns rows, filtered by Exasol |
| pushdown-planning-capability-extensions | In Exasol after task 12: `SELECT SYSTIMESTAMP FROM <vs>.<table>`, then the same select with no virtual schema, same session | Every returned row carries the SAME value, and it is within seconds of the non-virtual-schema oracle rather than two hours behind it. The database zone is applied by Exasol rather than read from the UDF container's UTC clock. A select-list `CURRENT_TIMESTAMP` cannot serve as this check: its `TIMESTAMP(3) WITH LOCAL TIME ZONE` type fails `is_valid_emits_output_type`, so that position is not a pushed projection either before or after the withdrawal |
| pushdown-planning-capability-extensions | `grep -rn "CURRENT_DATE\|SYSDATE\|CURRENT_TIMESTAMP\|SYSTIMESTAMP" docs/` | Only the new § Handled by Exasol row in `docs/capabilities.md` matches; the § Scalar functions Date / time row no longer names any of the four |
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
| Restoration issue filed | `ghbrk gh issue list --search "now-family pushdown restoration"` | Task 15's issue exists and is open, so the withdrawn pushdown is tracked before recording |
