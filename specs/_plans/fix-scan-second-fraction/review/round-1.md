# Plan Review Findings: fix-scan-second-fraction (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 4, Advisory: 6)
- Intent Fidelity blockers: 1
- Human-escalation blockers: 1

## Premortem

Six months from now this plan failed. Three ways it got there:

1. A user's `EXTRACT(SECOND FROM date_col)` query returned 0 last release and now fails with `Feature not supported`. Issue #603 said a DATE argument returns 0 as `DECIMAL(2,0)`, and nobody asked the user before the change shipped. Routed to `[INTENT_DRIFT]`.
2. `cargo test --workspace --lib` fails right after task 1.3. The declared-surface sweep `exasol_dialect_renders_declared_verbatim_surface` requires every declared name, `SECOND` included, to render through untyped `render_expression`. The implementer restores a DataFusion `SECOND` arm to make the sweep pass, and that brings back whole-second rendering. Or the implementer weakens the sweep without a recorded reason. Routed to `[HIDDEN_DEPENDENCY]`, plus `[AMBIGUOUS_REQUIREMENT]` for the changed scenarios that send `render_expression` to a typed-only rule.
3. After recording, the library holds two scenarios for the same input. One says an unprojected key that is not a `CAST` routes to the wrapper. The new one says an unprojected `SECOND(ts)` key keeps the split. A later refactor follows the older scenario and removes the split path. Routed to `[REQUIREMENT_CONFLICT]`.

## Intent Fidelity

#### [INTENT_DRIFT] BLOCKER
- Location: decision-log.md § Design Decisions [1]; plan.md § Open Questions, § Impact; spec delta `sql-functions/vs-expression-translator-date-fns` scenarios "SECOND over a DATE computes 0 without a stated type" and "SECOND over an argument without a declared timestamp precision runs in Exasol"
- Issue: The plan departs from the request in two ways, and the user has confirmed neither. (a) Issue #603's proposed fix says "A DATE argument returns `0` as `DECIMAL(2,0)`". The plan computes `SECOND(<date>)` as 0 but gives it no exact type. As a result, an unprojected `GROUP BY SECOND(<date>)` and a nested aggregate over it leave the split path, which interview answer 2 ("so an unselected SECOND group key keeps the split aggregate") was meant to prevent. (b) Interview answer 1 says the scan computes a "column or CAST with a declared type". The issue's type table lists `SECOND(ts)` and `EXTRACT(SECOND FROM ts)` together for DATE. The plan declines `EXTRACT(SECOND FROM <date>)` instead, so a query that returns 0 today will fail. The live evidence supports both choices technically: 8.29.13 types `SECOND(<date>)` as `DECIMAL(5,3)`, `EXTRACT(SECOND FROM <date>)` fails natively on both versions, and mission capability 12 requires native parity. The flagging is incomplete, though. § Open Questions asks only about (a), and it credits `DECIMAL(2,0)` to "the interview answer" when the issue states it. (b) appears only as a "Behavior change" bullet in § Impact and is not put to the user. Escalation is HUMAN because (b) turns a query that works today into an error, which changes what the feature does for a user, and both points depart from the user's stated request.
- Fix: In plan.md § Open Questions, replace the first bullet with two questions. Each quotes the issue line it departs from and states the live evidence. Q1: accept `SECOND(<date>)` = 0 with no exact type, so an unprojected DATE key and a nested aggregate over it run in the wrapper, or type it `DECIMAL(2,0)` as the issue states and accept a divergence on 8.29.13. Q2: accept that `EXTRACT(SECOND FROM <date>)` now fails with Exasol's `Feature not supported` error, matching native, instead of returning 0. Attribute `DECIMAL(2,0)` to issue #603's proposed fix, not to the interview. Record the user's answers in decision-log.md `## Interview` and adjust decision [1] and the two scenarios to match.
- Escalation: HUMAN. The change makes a working query fail and departs from the issue's stated DATE behavior, so only the requester can accept it.

#### [SCOPE_REDUCTION] ADVISORY
- Location: plan.md § Implementation Tasks 2.1
- Issue: The issue's acceptance criterion is "E2E tests run `SECOND(ts)` and `EXTRACT(SECOND FROM ts)` over a `TIMESTAMP(6)` and a `TIMESTAMP(9)` column, in the select list, a WHERE filter, and a GROUP BY key". Task 2.1 covers only one diagonal: `SECOND` over `TS` and `EXTRACT` over `TS_NS` in each position. `EXTRACT(SECOND FROM TS)` and `SECOND(TS_NS)` never run in the select list, a WHERE filter, or a GROUP BY. Both forms share one synthetic node, so the risk is low, but the criterion as written is not met.
- Fix: In plan.md task 2.1, add case-table rows for `EXTRACT(SECOND FROM TS)` and `SECOND(TS_NS)` in the select list, a WHERE filter, and a GROUP BY key, so each form runs over each precision in each position.

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Implementation Tasks 1.3 and 1.6; § Dead Code Removal; `crates/vs-expression/src/lib_tests.rs` `exasol_dialect_renders_declared_verbatim_surface` (l.2153 fixture, l.2267-2275 assertion); `crates/vs-expression/src/lib.rs` l.938 backstop arm
- Issue: Task 1.3 removes `SECOND` from the DataFusion `YEAR | ... | SECOND` arm. `SECOND` stays declared in `SCALAR_FUNCTIONS`. The sweep `exasol_dialect_renders_declared_verbatim_surface` renders the fixture `SECOND(ts)` through untyped `render_expression` and panics when a declared name declines there ("it MUST render in the DataFusion dialect too ... this is the only assertion that catches it"). The catch-all arm at lib.rs l.938 is documented as "reachable only if a declared name loses its per-name arm", which is exactly the state task 1.3 creates. Task 1.6 lists the four other `lib_tests.rs` changes but not this sweep, so the checklist step `cargo test --workspace --lib` (0 failures) cannot pass as planned. The implementer is left to decide alone how to change a test that guards a translator-wide rule.
- Fix: Add a task line under 1.6 that changes `exasol_dialect_renders_declared_verbatim_surface` explicitly. Either render the DataFusion assertion through `render_scan_expression` with a lookup that types `TS` as `TIMESTAMP(6)`, or exempt `SECOND` from the untyped DataFusion assertion and state in the test why the name renders only from the typed pass. Also update the backstop comment at lib.rs l.938, or keep an explicit DataFusion `SECOND` arm that declines with a message naming the typed pass, so the comment stays true. Add both to § Dead Code Removal or the task line as appropriate.
- Escalation: MECHANICAL. The fix follows from reading the existing test and arm, and no requester judgment is needed.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: spec delta `sql-functions/vs-expression-translator-date-fns` § Scenario "EXTRACT translates to the DataFusion date_part call" (line 29) and § Scenario "Field-shortcut date functions translate to date_part of the matching field" (line 49)
- Issue: Both scenarios have WHEN "`render_expression` processes the node", and both add "*AND* a `SECOND` field SHALL render per `SECOND keeps the fraction of its argument's declared precision`". `render_expression` is the untyped DataFusion walker and has no column types, so it cannot know p. The referenced scenario applies to `render_scan_expression` only. Task 1.6 asserts the opposite of the AND line: "assert that `render_expression` declines `SECOND(ts)` and `EXTRACT(SECOND FROM ts)`". The GIVEN of each scenario also excludes `SECOND` (it lists `YEAR` to `MINUTE`), yet the AND line describes `SECOND`. Without editing the scenario, nobody can write a test that satisfies both it and task 1.6.
- Fix: In both scenarios, replace the `SECOND` AND line with two lines. First: "*AND* for a `SECOND` field, `render_expression` SHALL decline the node, because only the typed pass knows the argument's precision." Second: "*AND* `render_scan_expression` SHALL render it per `SECOND keeps the fraction of its argument's declared precision`."
- Escalation: MECHANICAL. The fix follows from task 1.6 and the code's entry points.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: spec delta `pushdown-aggregates/pushdown-planning-split-aggregate-types` § Scenario "An unprojected SECOND group key takes the type of its argument's precision", versus the recorded scenario "An unprojected group key without an exact type declines the split" in `specs/pushdown-aggregates/pushdown-planning-split-aggregate-types/spec.md`
- Issue: The recorded scenario is not in the delta. Its GIVEN reads "a grouped request whose `groupBy` holds an expression that is absent from the select list, is not a `CAST`, and is not a constant", and its THEN says "the adapter SHALL route the request to the qualified wrapper". `SELECT COUNT(*) FROM t GROUP BY SECOND(ts)` matches that GIVEN. The new scenario says the same request "SHALL keep the split path". After recording, both scenarios apply to one input. The plan changed the parallel nested-aggregate scenario ("the argument has no exact type (Background)") but missed this one.
- Fix: Add a `DELTA:CHANGED` block for "An unprojected group key without an exact type declines the split". Change its GIVEN to "an expression that is absent from the select list, has no exact type (Background), and is not a constant, such as `SELECT COUNT(*) FROM t GROUP BY UPPER(s)`". Map it in plan.md § Scenario Coverage to its existing test.
- Escalation: MECHANICAL. A reading of the two scenarios settles the fix.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 2.1; `crates/lakehouse-e2e/tests/common/seed.rs` `TSPRECISION_MICROS` / `TSPRECISION_NANOS`
- Issue: Every `ts_precision_probe` value sits at second 0 (`BASE_TS_MICROS` is 2024-01-01 00:00:00). The E2E therefore cannot tell a correct `SECOND` from one that keeps only the fraction and drops the whole seconds. No fixture value has digits beyond p that rounding would change either, so the "cut, never round" THEN is proven only by the unit SQL pin and a scratch program. Issue #603's own reproduction used `ALL_TYPES.C_TIMESTAMP_NS`, whose value is at second 45.123.
- Fix: In plan.md task 2.1, add native-copy rows over `ALL_TYPES` (via `create_full_native_copy`) for `SECOND(C_TIMESTAMP)` and `EXTRACT(SECOND FROM C_TIMESTAMP_NS)` in the select list and a WHERE filter, so the whole-second part is compared against native Exasol.

#### [COMPLETENESS_GAP] ADVISORY
- Location: spec delta `sql-functions/vs-expression-translator-date-fns` § Scenario "SECOND over an argument without a declared timestamp precision runs in Exasol"; plan.md task 1.1
- Issue: The scan already computes `CAST(ts AS TIMESTAMP(p) WITH LOCAL TIME ZONE)` (`cast_to_timestamp_with_local_time_zone_matches_native`), so `SECOND` over that CAST reaches the classifier. Task 1.1 classifies only "`TIMESTAMP(p)`". Neither the task nor the scenarios say whether a `TIMESTAMP(p) WITH LOCAL TIME ZONE` argument is computed or declined. Native Exasol reads its seconds in the session time zone, which the scan does not receive.
- Fix: In the decline scenario's GIVEN, name a `TIMESTAMP(p) WITH LOCAL TIME ZONE` argument as one that declines. Add a decline row `SECOND(CAST(TS AS TIMESTAMP(6) WITH LOCAL TIME ZONE))` to task 1.5.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: recorded scenario "A refused argument count declines for DataFusion and renders for Exasol" and Background bullet "A refused arity is the same case as a refused name ... the DataFusion dialect checks arity in each name's arm" in `sql-functions/vs-expression-translator-date-fns`
- Issue: The recorded GIVEN says `SECOND`'s "DataFusion arm accepts exactly one argument". After task 1.3, `SECOND` has no DataFusion per-name arm. The typed pass checks its arity, and `SECOND(ts, n)` declines through the backstop. The delta already replaces the Background but keeps this bullet unchanged, so the recorded text will describe an arm that no longer exists.
- Fix: In the delta's Background, reword the arity bullet to say that `SECOND`'s arity is checked by the typed scan pass. Add a `DELTA:CHANGED` block that rewords the recorded scenario's GIVEN the same way. Its THEN stays as it is.

## Task Breakdown
No objection, axis checked. Every `DELTA:NEW` and `DELTA:CHANGED` scenario maps to a task (1.1 to 1.6 for the translator rule and typing, 2.1 for E2E parity) and to a row in § Scenario Coverage. The single group A shares one spec pair and one crate plus its E2E file. Task 1.2 carries `[expert]` for the tree rewrite. No task implements anything out of scope.

## Design Depth

No `[ADR_CONFLICT]`. Decision [3] supersedes `split-aggregate-columns-take-exact-types-or-decline` with a `Supersedes:` line and `Promotes to ADR: yes`. No `[ADR_OVERPROMOTION]`. [3] is the only `yes` entry, and its Rationale names rule-2 criterion 2 and the ADR search result. Its Decision holds no paths, signatures, or flags. No `[ARCHITECTURE_DRIFT]`. [3] carries `Architecture: no change` with a reason, and `specs/architecture.md` l.92 already routes every scan type rule through the one scan translator. No other ADR in the digest conflicts. `scan-never-converts-session-formatted-text` is honored, because a p > 0 result and the DATE case are classed `Number` and decline text conversion. Task 1.1 writes the precision classifier once for both `type_scan_tree` and `exact_expression_type`, so the rule does not leak between modules.

#### [COMPLETENESS_GAP] ADVISORY
- Location: decision-log.md § Design Decisions [3]
- Issue: Entry [3] has no `Alternatives` or `Consequences` line. The ADR it supersedes records three rejected options, including "Plan the expression in DataFusion and map its Arrow type", which is the reason this plan types `SECOND` itself. It also records the consequence that a nested aggregate over an expression without an exact type runs in the wrapper. Once the old ADR is marked superseded, the new ADR built from [3] has no Options Considered or Consequences section, and that reasoning drops out of the current record.
- Fix: Add to decision-log.md [3] an `Alternatives` line with one line per rejected option: map DataFusion's Arrow type; copy Exasol's general expression typing; a fixed default type. Add a `Consequences` line: an expression without an exact type, other than `SECOND` over `TIMESTAMP(p)`, still runs in the wrapper.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: spec delta `sql-functions/vs-expression-translator-date-fns` § Scenario "SECOND over an argument without a declared timestamp precision runs in Exasol"
- Issue: The title says the scenario covers arguments "without a declared timestamp precision", but its GIVEN also covers "`EXTRACT(SECOND FROM x)` over a DATE `x`", which has a declared type. A reader looking up the DATE case by title will not find it here.
- Fix: Retitle the scenario "SECOND over an argument the scan cannot type runs in Exasol", or move the `EXTRACT(SECOND FROM <date>)` case into its own scenario. Update the Verification table title to match.
