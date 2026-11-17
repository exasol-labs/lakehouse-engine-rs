# Decision Log: change-pushdown-keep-more-in-scan

## Interview

- Q: Which item-5 follow-ups of #615 does this plan cover? → A: all three code changes: constant text keeps the scan filter, WHERE split at AND, fixed date-part result types
- Q: Does DataFusion 55.2.0 contain apache/datafusion#22906? → A: no; merged on main 2026-10-25, 55.2.0 is the newest release and has no backport
- Q: What happens to the timestamp-literal floor (#461)? → A: it stays; add a unit test asserting DataFusion still truncates the literal, so the upgrade that ships #22906 fails it and the floor is removed then (`specs/testing.md` § Regression guards, line 99)
- Q: Plan size? → A: minimal, one plan, one PR

## Design Decisions

### [1] Exasol converts a DATE text literal in the dispatch SQL before the scan

- **Decision:** The scan converts no session-formatted text. The scan translator declines every conversion between text and a DATE, TIMESTAMP, DOUBLE, or DECIMAL with a scale above 0, except a text literal compared with a DATE through a comparison, `BETWEEN`, `IN`, or simple `CASE`. Exasol converts that literal by the session's `NLS_DATE_FORMAT` in the dispatch SQL before the scan, and the scan reads the result as a DATE literal.
- **Rationale:** Criteria 1 and 4. The decision spans the vs-expression renderer, the pushdown-planner's dispatch SQL, and the data flow into `LAKEHOUSE_SCAN`, and it rejects plausible alternatives. ADR digest: `scan-never-converts-session-formatted-text` declines every such conversion and lists this option as deferred, so this entry supersedes it. Exasol's CAST applies the session's format, so the converted value is the value native Exasol compares with.
- **Supersedes:** scan-never-converts-session-formatted-text
- **Architecture:** § Data Flow, § Components, § Constraints
- **Alternatives:**
  - A scan function that reproduces Exasol's text form (`exa_to_varchar`). Rejected: the form depends on the session, so it matches only the default settings.
  - Read the session's NLS settings into the scan. Rejected: the scan could no longer decline and would have to reproduce every format a session can set.
  - A DataFusion placeholder in the scan's SQL, bound at scan time. Rejected: it changes the scan-spec wire format and every scan site that plans SQL, for a value Exasol can write as text.
  - More `LAKEHOUSE_SCAN` arguments. Rejected: they change the installed script's signature.
- **Consequences:**
  - The DataFusion-dialect renderer writes a converted-literal marker in place of each such literal. A converted-literal marker is self-delimiting text that carries the literal for Exasol to convert. One vs-expression module owns the marker and turns a serialized scan spec that holds markers into the Exasol SQL of the `common` argument, so the pushdown-planner does not know the encoding.
  - `date_col = '2024-01-01'` keeps its scan filter, and in a join its broadcast plan. A text literal compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still runs in Exasol.
  - An unparseable literal fails the query whenever a shard scans, also when no row reaches the comparison. `docs/semantic-differences.md` documents it.
  - The adapter's file pruning gets no bound from the literal, because pruning reads the request's text literal, which it does not convert for a DATE column.
- **Promotes to ADR:** yes

### [2] Only a DATE operand gets its text literal converted

- **Decision:** A text literal compared with a TIMESTAMP, a DOUBLE, or a DECIMAL with a scale above 0 keeps declining.
- **Rationale:** The adapter's timestamp-literal floor (`pushdown-types/pushdown-planning-timestamp-literal-precision`) needs a TIMESTAMP literal's digits at plan time, and an Exasol-converted literal does not give them. A number needs a text form that no session setting changes. Each needs its own rendering and its own live parity check. The issue's example is a DATE.
- **Alternatives:** Convert a TIMESTAMP literal with `CAST(<literal> AS TIMESTAMP(p))`. Rejected for this plan, because an over-precise literal would bypass the floor.
- **Promotes to ADR:** no

### [3] A date-part function has a fixed exact type

- **Decision:** A one-argument `YEAR` has the exact type `DECIMAL(4,0)`, and a one-argument `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` has `DECIMAL(2,0)`. An `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, or `MINUTE` takes the type of the function of the same name. An unprojected group key or a nested aggregate argument of these calls therefore keeps the split.
- **Rationale:** `typed-pass-infers-exasol-types` and `exact-types-follow-the-exasol-version` cover the rule, so it needs no new ADR: each scalar function has one result rule, a rule returns an exact type after a live check (task 1.1, on 8.29.13 and 2025.1.16), and one inference serves the scan and every declared type. The typed pass types an `EXTRACT` by the rule of the function its field names, so leaving `EXTRACT` out would need a guard of its own.
- **Consequences:** A nested `MIN`, `MAX`, or `SUM` over a date-part call keeps the split. A digits-only text literal compared with a date-part call and longer than its precision runs in Exasol, as for any integer operand (`pushdown-types/pushdown-planning-scan-text-conversion`). If task 1.1 finds an `EXTRACT` field whose type differs from its function's, that field keeps the integer kind and joins the open question.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] PR #624 already supersedes the split-aggregate typing ADR

- **Finding:** PR #624 (merged) types `SECOND`, and its ADR `copy-typing-rule-only-when-exact-and-version-stable` superseded `split-aggregate-columns-take-exact-types-or-decline`. Decision [3] superseded the same ADR and declined the split for `SECOND`.
- **Direction change:** Decision [3] does not promote to an ADR, the split-aggregate-types delta builds on the current Background, which types `SECOND`, and the plan makes no `SECOND` assertion.
- **Promotes to ADR:** no

### [plan-review] Converting a DATE text literal contradicts `scan-never-converts-session-formatted-text`

- **Finding:** That ADR's Decision declines every implicit conversion, and its Consequences keep `date_col = '2024-01-01'` in Exasol. Decision [1] claimed conformance.
- **Direction change:** Decision [1] supersedes that ADR and promotes to an ADR. Its Decision restates the rule with the DATE text-literal exception and keeps the superseded ADR's rejected options.
- **Promotes to ADR:** no

### [plan-review] The typed pass types EXTRACT by its field's function rule

- **Finding:** On the current `main`, the typed pass types `EXTRACT(<field> FROM x)` by the result rule of the function named `<field>`, so the date-part rule reaches `EXTRACT` without extra code. Decision [3] had left `EXTRACT` out because its type was not measured.
- **Direction change:** Decision [3] and the split-aggregate-types delta include `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, and `MINUTE`, task 1.1 measures it, and the open question keeps only text literals compared with a TIMESTAMP or a number.
- **Promotes to ADR:** no
