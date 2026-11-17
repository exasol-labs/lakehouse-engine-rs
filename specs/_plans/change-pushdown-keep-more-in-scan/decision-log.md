# Decision Log: change-pushdown-keep-more-in-scan

## Interview

- Q: Which item-5 follow-ups of #615 does this plan cover? → A: all three code changes: constant text keeps the scan filter, WHERE split at AND, fixed date-part result types
- Q: Does DataFusion 55.2.0 contain apache/datafusion#22906? → A: no; merged on main 2026-10-25, 55.2.0 is the newest release and has no backport
- Q: What happens to the timestamp-literal floor (#461)? → A: it stays; add a unit test asserting DataFusion still truncates the literal, so the upgrade that ships #22906 fails it and the floor is removed then (`specs/testing.md` § Regression guards, line 99)
- Q: Plan size? → A: minimal, one plan, one PR
- Q: Keep converting a DATE text literal before the scan? → A: No, drop it; a DATE literal is the documented workaround.

## Design Decisions

### [1] A date-part function has a fixed exact type

- **Decision:** A one-argument `YEAR` has the exact type `DECIMAL(4,0)`, and a one-argument `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` has `DECIMAL(2,0)`. An `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE` takes the type of the function of the same name. An unprojected group key or a nested aggregate argument of these calls therefore keeps the split.
- **Rationale:** `typed-pass-infers-exasol-types` and `exact-types-follow-the-exasol-version` cover the rule, so it needs no new ADR: each scalar function has one result rule, a rule returns an exact type after a live check (task 1.1, on 8.29.13 and 2025.1.16), and one inference serves the scan and every declared type. The typed pass types an `EXTRACT` by the rule of the function its field names, so leaving `EXTRACT` out would need a guard of its own.
- **Consequences:** A nested `MIN`, `MAX`, or `SUM` over a date-part call keeps the split. A digits-only text literal compared with a date-part call and longer than its precision runs in Exasol, as for any integer operand (`pushdown-types/pushdown-planning-scan-text-conversion`). If task 1.1 finds an `EXTRACT` field whose type differs from its function's, that field keeps the integer kind.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] PR #624 already supersedes the split-aggregate typing ADR

- **Finding:** PR #624 (merged) types `SECOND`, and its ADR `copy-typing-rule-only-when-exact-and-version-stable` superseded `split-aggregate-columns-take-exact-types-or-decline`. Decision [1] superseded the same ADR and declined the split for `SECOND`.
- **Direction change:** Decision [1] does not promote to an ADR, the split-aggregate-types delta builds on the current Background, which types `SECOND`, and the plan makes no `SECOND` assertion.
- **Promotes to ADR:** no

### [plan-review] The typed pass types EXTRACT by its field's function rule

- **Finding:** On the current `main`, the typed pass types `EXTRACT(<field> FROM x)` by the result rule of the function named `<field>`, so the date-part rule reaches `EXTRACT` without extra code. Decision [1] had left `EXTRACT` out because its type was not measured.
- **Direction change:** Decision [1] and the split-aggregate-types delta include `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, and `MINUTE`, and task 1.1 measures it.
- **Promotes to ADR:** no
