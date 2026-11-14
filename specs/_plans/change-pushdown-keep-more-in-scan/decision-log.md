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
- **Architecture:** § Data Flow, § Components
- **Alternatives:**
  - A scan function that reproduces Exasol's text form (`exa_to_varchar`). Rejected: the form depends on the session, so it matches only the default settings.
  - Read the session's NLS settings into the scan. Rejected: the scan could no longer decline and would have to reproduce every format a session can set.
  - A DataFusion placeholder in the scan's SQL, bound at scan time. Rejected: it changes the scan-spec wire format and every scan site that plans SQL, for a value Exasol can write as text.
  - More `LAKEHOUSE_SCAN` arguments. Rejected: they change the installed script's signature.
- **Consequences:**
  - The DataFusion-dialect renderer writes a converted-literal marker in place of each such literal. A converted-literal marker is self-delimiting text that carries the literal for Exasol to convert. One vs-expression module owns the marker and turns a serialized scan spec that holds markers into the Exasol SQL of the `common` argument, so the pushdown-planner does not know the encoding.
  - `date_col = '2024-01-01'` keeps its scan filter, and in a join its broadcast plan. A text literal compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still runs in Exasol.
  - An unparseable literal fails the query whenever a shard scans, also when no row reaches the comparison. `docs/semantic-differences.md` documents it.
  - The adapter's file pruning gets no bound from the literal, because the adapter never sees the converted value.
- **Promotes to ADR:** yes

### [2] Only a DATE operand gets its text literal converted

- **Decision:** A text literal compared with a TIMESTAMP, a DOUBLE, or a DECIMAL with a scale above 0 keeps declining.
- **Rationale:** The adapter's timestamp-literal floor (`pushdown-types/pushdown-planning-timestamp-literal-precision`) needs a TIMESTAMP literal's digits at plan time, and an Exasol-converted literal does not give them. A number needs a text form that no session setting changes. Each needs its own rendering and its own live parity check. The issue's example is a DATE.
- **Alternatives:** Convert a TIMESTAMP literal with `CAST(<literal> AS TIMESTAMP(p))`. Rejected for this plan, because an over-precise literal would bypass the floor.
- **Promotes to ADR:** no

### [3] A one-argument date-part function has a fixed exact type

- **Decision:** A one-argument `YEAR` has the exact type `DECIMAL(4,0)`, and a one-argument `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` has `DECIMAL(2,0)`. An unprojected group key or a nested aggregate argument of these calls therefore keeps the split.
- **Rationale:** `copy-typing-rule-only-when-exact-and-version-stable` (PR #624) covers the rule: the type depends on no literal value, session setting, or argument type, and task 1.1 measures it on 8.29.13 and 2025.1.16. That ADR states that a rule meeting its policy needs no new ADR.
- **Consequences:** `EXTRACT(YEAR|MONTH|DAY|HOUR|MINUTE FROM x)` gets no fixed type, because its type per field is not measured. An open question (#TBD) tracks it. A nested `MIN`, `MAX`, or `SUM` over a date-part call keeps the split, and an output item over one drops a redundant CAST.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] PR #624 already supersedes the split-aggregate typing ADR

- **Finding:** Open PR #624 adds `copy-typing-rule-only-when-exact-and-version-stable`, which supersedes `split-aggregate-columns-take-exact-types-or-decline` and types `SECOND`. Decision [3] superseded the same ADR and declined the split for `SECOND`.
- **Direction change:** PR #624 merges before group A. Decision [3] no longer promotes to an ADR, the split-aggregate-types delta builds on #624's Background, and the plan drops every `SECOND` assertion.
- **Promotes to ADR:** no

### [plan-review] Converting a DATE text literal contradicts `scan-never-converts-session-formatted-text`

- **Finding:** That ADR's Decision declines every implicit conversion, and its Consequences keep `date_col = '2024-01-01'` in Exasol. Decision [1] claimed conformance.
- **Direction change:** Decision [1] supersedes that ADR and promotes to an ADR. Its Decision restates the rule with the DATE text-literal exception and keeps the superseded ADR's rejected options.
- **Promotes to ADR:** no
