# Plan Review Findings: fix-instant-column-tstz-comparisons

## Summary
- Axes checked: 4/4
- Total findings: 6 (Blockers: 0, Findings: 6)
- Intent Fidelity blockers: 0

## Intent Fidelity

#### [INTENT_GAP] FINDINGS
- Location: intent.md § Interview, § Directions; plan.md § Summary
- Issue: Issue #658's repro lists `ts_tz > TIMESTAMP '2024-03-01 10:00:00'` ("includes rows 2, 5" natively, "misses rows 2, 5" through the virtual schema) as a wrong-row case. The plan keeps it: the new scenario "A plain value compared with an instant column compares with its UTC date and time" fixes the UTC comparison and only documents the difference. The interview answer "Pruning for comparisons with plain TIMESTAMP or DATE values? → Use `SESSION_TIME_ZONE`, already documented" is not marked moot, unlike the other answers the design change overtook, and the plan never uses `SESSION_TIME_ZONE` for plain values. No direction records that the user accepted that this repro row stays a documented difference.
- Fix: In intent.md § Interview, mark the plain-value pruning answer "(moot after the design change)". Add a direction that states plain `TIMESTAMP` and `DATE` values compare with the UTC date and time of an instant column, so issue #658's `ts_tz > TIMESTAMP '...'` row stays a difference listed in `docs/semantic-differences.md`, and name it in the planning report as a choice for the user to confirm.

#### [INTENT_GAP] FINDINGS
- Location: plan.md § Impact; intent.md § Directions
- Issue: The UTC fold applies only to comparisons that reach the adapter. A comparison that Exasol evaluates itself still reads the plain UTC column as session-local: a join condition against a native `TIMESTAMP WITH LOCAL TIME ZONE` column (`JOIN native n ON vs.tz > n.event_at`), a predicate over a non-pushed subquery, or any comparison under `EXCLUDED_CAPABILITIES`. After this change the same comparison returns different rows depending on whether Exasol pushes it. plan.md § Impact names only the `EXCLUDED_CAPABILITIES` case, and no direction or scenario step names the others.
- Fix: Add a direction stating that a comparison Exasol evaluates itself, such as one against a native TSTZ column, keeps reading the instant column as session-local. Extend the `docs/semantic-differences.md` step of the scenario "A plain value compared with an instant column compares with its UTC date and time", or add a line to plan.md § Impact, so the "Instant columns" doc section names that case and its `CONVERT_TZ(tz, 'UTC', SESSIONTIMEZONE)` workaround.

## Feasibility

no objection — axis checked: the column notes already carry each column's source type tag (`timestamptz_*` for Iceberg `timestamptz`, Delta and Unity `timestamp`, and direct-storage INT96 through the UTC-zoned INT96 coercion), so existing virtual schemas need no `REFRESH`; the pushdown context already holds the Exasol version the `LOCALTIMESTAMP` split needs; DataFusion compares a plain UTC literal with a `Timestamp(_, "UTC")` column as an instant, so the scan and Iceberg/Delta pruning need no change beyond the folded value; the export refusal of TSTZ that grounds [D4] is in `export/export-iceberg-table`.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: architecture.md § Constraints (fold bullet); intent.md [D2]
- Issue: [D2] says the UTC fold applies "in any clause". The architecture delta says "A `TIMESTAMP WITH LOCAL TIME ZONE` value outside a select-list item of that type becomes the plain timestamp of its UTC date and time where a comparison sets it against" an instant column. A select-list item that Exasol types TSTZ and that holds a comparison, such as `CASE WHEN tz > CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP ELSE tz END`, keeps its TSTZ literal, so Exasol compares the plain UTC column as session-local and returns the pre-fix result. The two artifacts disagree on that case, and no scenario pins it.
- Fix: Decide the case in [D2]: either the comparison inside a TSTZ-typed select-list item also compares instants (then reword the architecture bullet to exempt only the select-list value itself, not a comparison inside it, and add that query to the WHEN of "A TIMESTAMP WITH LOCAL TIME ZONE value compared with an instant column compares the instants"), or it keeps the old rule (then narrow [D2] from "in any clause" and list the case in `docs/semantic-differences.md`).

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: specs/pushdown/pushdown-planning-clock-functions/spec.md, recorded scenarios "A clock function reaches the pushed SQL as a literal" and "A filter on a clock function or a time zone literal prunes files at plan time" (not in the delta)
- Issue: Both scenarios use "a `TIMESTAMP` column `ts`" and require the rows native Exasol returns for `ts < CURRENT_TIMESTAMP` and `ts < CAST(TIMESTAMP '<t>' AS TIMESTAMP WITH LOCAL TIME ZONE)` in a session whose zone equals the folding zone. The new Background declares an instant column a plain `TIMESTAMP` too. For an instant column these steps are now false unless the folding zone is UTC. The delta adds "that is no instant column" to two other scenarios but not to these two.
- Fix: Add both scenarios to the pushdown-planning-clock-functions delta as `DELTA:CHANGED`, with "that is no instant column" after "a `TIMESTAMP` column `ts`" in each GIVEN.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: intent.md [D2]
- Issue: [D2] lists "`=`, `<>`, `<`, `<=`, `BETWEEN`, or `IN`" and omits `>` and `>=`, while every new scenario uses `>`. The list reads as the request's predicate types, where Exasol sends `a > b` as `b < a`, but a reader of the direction can take `>` and `>=` as excluded.
- Fix: Reword [D2] to "every comparison operator, `BETWEEN`, or `IN`" so the SQL a user writes decides the scope.

## Design Depth

#### [ADR_OVERPROMOTION] FINDINGS
- Location: intent.md [D4]
- Issue: [D4] bundles two decisions into one ADR (rule 5: one decision per ADR). Its first half, "Instant columns stay plain `TIMESTAMP` in UTC", restates the accepted ADR `collapse-exasol-facing-timestamp-string-keep-internal-tz-aware-arrow-type` (rule 3). Its second half supersedes `fold-clock-values-in-virtual-schema-zone`, but states only the instant-column rule. Once superseded, the old ADR's remaining decision (clock functions and every other compared TSTZ value fold in the `SESSION_TIME_ZONE` zone, else `DBTIMEZONE`) and its Consequence "`EXCLUDED_CAPABILITIES` restores native evaluation for a workload that needs the session zone", which no longer holds for instant columns, have no live record.
- Fix: Rewrite [D4] as one decision that restates the whole fold rule: the folding-zone rule for clock functions and other compared TSTZ values, plus the UTC rule for a TSTZ value compared with an instant column. Keep "supersedes fold-clock-values-in-virtual-schema-zone", and carry the rejected TSTZ declaration (export refusal, `REFRESH` type change) as its rejected option. Note in [D4] that the ADR's consequences state that `EXCLUDED_CAPABILITIES` no longer restores native results for instant columns. Drop the restated plain-`TIMESTAMP` claim.

No other objection on this axis: the architecture delta's BASE matches `git hash-object specs/architecture.md`, its CHANGED Constraints block copies every current line and changes only the fold bullet, and [D4] names § Constraints. No new module or boundary is introduced. The instant-column test should read one owner of "this source type tag holds UTC instants" next to the existing tag mapping rather than match tag strings in the fold.
