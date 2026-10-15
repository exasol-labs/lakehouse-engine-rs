# Plan Review Findings: fix-to-date-timestamp-format-translation (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 7 (Blockers: 3, Advisory: 4)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

1. A filter such as `WHERE TO_DATE(s, 'YYYY-MM-DD') > DATE '2000-01-01'` runs over a string column that holds a `0000-01-01` sentinel, a `-` before the year, or a tab inside the value. Native Exasol raises 22018. The scan parses the string, compares it, and drops or keeps the row without an error. `docs/semantic-differences.md` claims a complete list and omits these inputs. Routed to Requirement Quality `[COMPLETENESS_GAP]`.
2. A verifier checks the first new scenario's unconditional "SHALL return the value native Exasol returns" against `'2024-01-01 '`, which the Background lists as a scan error. The implementation fails the scenario as written. Routed to Requirement Quality `[REQUIREMENT_CONFLICT]`.
3. Real lakehouse tables hold empty strings or space-padded date strings. Every `TO_DATE(col, fmt)` over them fails outright. The `nullif`/`rtrim` fix was rejected "for this fix" and has no follow-up. Routed to Design Depth `[TACTICAL_SHORTCUT]`.

## Intent Fidelity

No blocker. Axis checked: the narrowing of FF1..FF9 to FF3/FF6/FF9 follows the user's own decline rule ("elements with no exact ... chrono equivalent"). chrono 0.4.45 has fixed-width fraction parsers only for 3, 6, and 9 digits (`src/format/parse.rs`, `Nanosecond3`/`6`/`9` and the `NoDot` variants). The structural declines (time elements in `TO_DATE`, partial dates, incomplete time sets) each rest on a measured wrong value in decision-log [2]. The interview answer "decline or document diverging ones" authorizes them, and every declined format still returns the native result. The planner's narrowing is justified, not drift.

#### [SCOPE_REDUCTION] ADVISORY
- Location: `sql-functions/vs-expression-translator-datetime-format/spec.md` § Background "Translated elements"; plan.md task 1.1 (`declines_unsupported_formats`) and task 1.2
- Issue: The user asked to translate "literal separators". The plan copies only `-` `/` `.` `,` `;` `:` and space, declines `_` and `T`, and does not mention `\`. On Exasol 2025.1.16, `_` and `\` are exact separators: `TO_DATE('1996_03_13','YYYY_MM_DD')` and `TO_DATE('1996\03\13','YYYY\MM\DD')` return `1996-03-13`, and `TO_DATE('1996-03-13','YYYY_MM_DD')` raises 22018. chrono matches both as literals. Exasol matches `T` without regard to letter case (`'1996-03-13t10:11:12'` under `'YYYY-MM-DDTHH24:MI:SS'` returns a value), so declining `T` is defensible. No decision-log entry records why any of the three is declined. The results stay correct through the native fallback, so only pushdown coverage is lost.
- Fix: Either add `_` and `\` to the separator set in the Background, task 1.2, and task 1.1's translate list, and move `_` out of task 1.1's decline list. First verify that DataFusion's SQL parser keeps a `\` in a single-quoted literal unchanged, and leave `\` declined with a stated reason if it does not. Or add a decision-log entry that names `_`, `\`, and `T` as declined and gives the reason for each (for `T`: Exasol matches it without regard to case, chrono does not).

## Feasibility

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md § Dependencies
- Issue: The section names only `fix-trunc-round-date-trunc-date-args` as "the unrecorded plan" that edits `lib.rs`. That directory holds only `notes/` and no `plan.md`. Two plans that are not named also edit the files this plan changes: `fix-regexp-like-pushdown` (on the current branch) edits `crates/vs-expression/src/lib.rs`, `lib_tests.rs`, and `e2e_capability_test.rs`, and `fix-scan-numeric-typing-exasol-parity` edits `lib.rs` and `lib_tests.rs`.
- Fix: In plan.md § Dependencies, list `fix-regexp-like-pushdown` and `fix-scan-numeric-typing-exasol-parity` with the shared files. State that `fix-trunc-round-date-trunc-date-args` has notes only and no plan yet.

Other checks: no objection. The decline route was measured over 16 query shapes with join pushdown on and off. `render_expression_safe` maps every `Err` to `None` (`lib.rs` 1219-1222). The E2E expectations match the seed: `event_date` is 2024-01-01 plus `id - 1`, `name` is `event-NN`, and `order_custkey(8..10)` is customers 03 to 05. `delta_predicate.rs`, `filter_json.rs`, and `parquet_format_reader.rs` also translate no `function_scalar` node, so no pruning path is reached. The scan session sets no `time_zone`, so `to_timestamp` returns a naive `Timestamp(ns)`.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: `sql-functions/vs-expression-translator-datetime-format/spec.md` § Background "Measured parse differences"; plan.md task 2.5
- Issue: The Background presents a complete list of inputs on which the scan differs from Exasol. Scenario "A string Exasol rejects for a translated format fails the query" requires that the scan "MUST NOT ... silently drop its row" for any rejected string not on that list. Three input classes are missing from the list, and the planned renderer breaks that scenario for each of them:
  - A `-` before the year. chrono's signed `%Y` accepts it (`parse.rs` 408-418). DataFusion 54.1: `to_date('-1996-03-13','%Y-%m-%d')` returns `-1996-03-13`. Exasol: `TO_DATE('-1996-03-13','YYYY-MM-DD')` raises 22018 "invalid value for YYYY format token".
  - Year 0. DataFusion: `to_date('0000-01-01','%Y-%m-%d')` and `to_date('0-01-01','%Y-%m-%d')` return `0000-01-01`. Exasol raises 22018 "invalid date value".
  - Whitespace other than a space before any numeric field, not only at the start. chrono runs `s.trim_start()` before every numeric item (`parse.rs` 406). DataFusion returns `1996-03-13` for `concat('1996-03-', chr(9), '13')` and for `chr(10)`, and `to_timestamp` accepts a tab after `10:`. Exasol raises 22018 "invalid value for DD format token" for `CHR(9)` and `CHR(10)` in the same place. The Background names only "a leading tab".

  In a select list, the first two classes still fail, because `scan/convert.rs` raises "DATE value out of range". In a filter, the scan never emits the parsed date: `to_date('-1996-03-13','%Y-%m-%d') > DATE '2000-01-01'` evaluates to `false`, so the row is dropped where Exasol raises. AGENTS.md and mission capability 12 accept a deviation only when `docs/semantic-differences.md` documents it.
- Fix: In the Background's second sub-bullet ("The scan returns a value where Exasol raises an error"), add "a `-` before the year or a year of 0 under `TO_DATE`, which the scan evaluates in a filter and rejects only when it emits the date". Replace "a leading tab" with "a tab, line break, or other whitespace that is not a space before any numeric field". In plan.md task 2.5, add three table rows: `'-2024-01-01'` in a `TO_DATE(..., 'YYYY-MM-DD')` filter (row compared, error), `'0000-01-01'` in the same filter (row compared, error), and `'2024-01-' || CHR(9) || '01'` (`2024-01-01`, error).
- Escalation: MECHANICAL. The interview answer ("decline or document diverging ones") already settles that a diverging input may be documented, and the evidence above settles which inputs diverge.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `sql-functions/vs-expression-translator-datetime-format/spec.md` § Scenario "A string-literal format translates to a strftime pattern", second step
- Issue: The step "*AND* a query over a virtual schema table that pushes such a call into the scan SHALL return the value native Exasol returns" is unconditional. The same delta's Background lists translated-format inputs on which the scan raises or returns a value Exasol rejects, for example `'2024-01-01 '` under `YYYY-MM-DD`. The fourth scenario carves those inputs out, but this step does not. A test written from this step fails against the planned implementation.
- Fix: Change the step to "*AND* a query over a virtual schema table that pushes such a call into the scan SHALL return the value native Exasol returns for a string that is not one of the Background's measured parse differences, so ...", and keep the examples.
- Escalation: MECHANICAL. Reading the scenario and the Background side by side settles the conflict.

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: `sql-functions/vs-expression-translator-datetime-format/spec.md` § Background "Measured parse differences", sentence "`docs/semantic-differences.md` lists them."
- Issue: No GIVEN, WHEN, or THEN step in this feature refers to the documentation page. The sentence states a fact that no scenario depends on. Task 2.5 already schedules the documentation.
- Fix: Delete the sentence "`docs/semantic-differences.md` lists them." from the Background bullet, and keep task 2.5 unchanged.
- Escalation: MECHANICAL. This is a one-sentence deletion under the Background rule.

Other checks: no objection. The `DELTA:CHANGED` title matches the recorded scenario "TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls" (`specs/sql-functions/vs-expression-translator-date-fns/spec.md` line 123). The delta's Background is identical to the recorded lines 1-64. No recorded scenario in `pushdown-planning-capability-extensions`, `vs-expression-translator-greatest-least`, or `vs-expression-translator-date-diff-fns` conflicts with the delta. The three-argument decline is unreachable from Exasol (42000 "should not be called with 3 arguments"), so a unit proof is valid under `specs/testing.md` § Coverage rule.

## Task Breakdown

No objection. Axis checked: all five scenarios map to tests in § Scenario Coverage. Task 2.5 implements the documentation for the Background's difference list. The unit-test tags follow `specs/testing.md` § Coverage rule: only `to_date_to_timestamp_decline_untranslatable_format` carries a tag, because it is the only proof of a renderer-only scenario. The single group A shares one module set, and its tasks run in TDD order (1.1 before 1.2, 1.3 before 1.4). The coverage script `.claude/skills/test-strategy-review/scripts/changed_line_coverage.py` exists.

## Design Depth

No ADR or architecture objection. Axis checked: all three decision-log entries are `Promotes to ADR: no`. plan.md states "Architecture: no change", and `specs/architecture.md` line 41 describes `vs-expression` at component level, so a private module is not drift. Decision-log [1] keeps the decision of ADR `exclude-fn-to-char-and-fn-to-number-format-model-incompatibility` (it covers only `TO_CHAR`/`TO_NUMBER`) and follows ADR `no-exasol-side-fallback-for-an-advertised-capability`, so no ADR conflict exists. `chrono_format(format, target) -> Option<String>` is a deep interface: it hides every format-model element behind one call, and the `TO_DATE`/`TO_TIMESTAMP` arm knows no element.

#### [TACTICAL_SHORTCUT] ADVISORY
- Location: decision-log.md [1] Alternatives ("Rewrite the source with `rtrim` or `nullif` ... Rejected for this fix"); plan.md § Open Questions
- Issue: The plan leaves the empty-string and trailing-space differences in place, and both fail the whole query. Exasol treats `''` as NULL. Padded strings are common in real lakehouse data. "Rejected for this fix" implies a later fix, but no open question or follow-up issue schedules one.
- Fix: Add an Open Questions bullet in plan.md: "Should a follow-up issue (#TBD) close the empty-string and trailing-space differences with a `nullif`/`rtrim` source rewrite, which also changes the one-argument renderings?"

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md task 2.1, last sub-bullet
- Issue: "`explain_virtual_sql` of the first and third queries contains `%Y-%m-%d` and `%H:%M:%S%.3f` inside the `LAKEHOUSE_SCAN` call" assigns both patterns to both queries. The first query has no time element, so it cannot contain `%H:%M:%S%.3f`. The `explain_virtual_sql` helper also returns one joined string (`e2e_harness.rs` 321-330), so "inside the `LAKEHOUSE_SCAN` call" gives no checkable boundary.
- Fix: Rewrite the sub-bullet as: "`explain_virtual_sql` of the first query contains `%Y-%m-%d`, that of the third query contains `%H:%M:%S%.3f`, and neither contains `YYYY` or `yyyy`." Drop "inside the `LAKEHOUSE_SCAN` call", or name the substring the test uses to locate the call.
