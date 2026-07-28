# Feature: Pushdown Planning — Capability Extensions

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the getCapabilities-level
capability advertisements for scalar and type-conversion functions the adapter has added
since the base feature: arithmetic operator scalar functions, CAST/unary-negation, and ISO
week — plus the capabilities that were considered and deliberately kept absent (regexp
scalar functions, bitwise operator functions). Each advertised capability is gated on a
`crates/vs-expression` translator arm that renders it faithfully; each absent capability
records why no faithful translation exists. Ordered-sort-key capability advertisement
(`ORDER_BY_COLUMN` / `ORDER_BY_EXPRESSION`) lives in its own sibling feature,
`vs-adapter/pushdown-planning-order-by-capability`. Related capability-driven extensions —
scalar select-list expression pushdown, HAVING pushdown, statistical aggregates, and literal
projection — live in their own sibling features too (see the "See also" note at the end of
the Background).

## Background

<!-- DELTA:NEW -->
* **A capability is withdrawn when the scan cannot evaluate the function faithfully, not only when the translator cannot render it.** The four now-family names — `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` — render as valid SQL in both dialects today, yet the node-local scan cannot produce Exasol's value for any of them. Exasol's four names are three distinct semantics over one instant: `CURRENT_TIMESTAMP` interprets it in the session time zone (`TIMESTAMP(3) WITH LOCAL TIME ZONE`), `SYSTIMESTAMP` interprets the same instant in the database time zone (`TIMESTAMP(3)`), and `CURRENT_DATE`/`SYSDATE` are `TO_DATE` of each. Rendering that distinction needs `SESSIONTIMEZONE` and `DBTIMEZONE`. Neither value reaches the scan UDF: the pushdown request carries no zone, `CommonScanSpec` carries no temporal field, the scan script declares only the common blob and the per-file list, the scan opens no connect-back session, and the SDK's `UdfContext` exposes no clock and no zone. The scan therefore reads its own container clock in UTC. It also reads that clock once per shard — the fan-out builds and drops a `SessionContext` per invocation — so a pushed clock call is evaluated G times with no statement anchor, while Exasol's now-family is statement-constant. Withdrawal is the correctness fix: Exasol never delegates a capability the adapter does not advertise, so Exasol evaluates its own clock, once, in its own zones. All three claims were measured against live Exasol 2025.2.1 rather than inferred from the advertised capability set: `EXPLAIN VIRTUAL` over a select-list `SYSTIMESTAMP` pushes `"projection":[{"expr":"now()"}, …]` with `"emit_exa_types":["TIMESTAMP(3)", …]`, and a filter-position `CURRENT_TIMESTAMP` pushes `"filter":"(now() < \"EVENT_TS\")"`, so the node is genuinely delegated; the same select returned `15:02:02.716` through the virtual schema against `17:02:03.141` from Exasol in one session, with `DBTIMEZONE` and `SESSIONTIMEZONE` both `EUROPE/BERLIN` over a UTC container clock; and `GROUP BY SYSTIMESTAMP` over a two-file table returned two distinct timestamps against one statement-constant native value. A pure-constant predicate is not a valid probe, because Exasol constant-folds it before building the pushdown request.
* **Withdrawing a capability is the safe direction; advertising without a backing path is the unsafe one.** An unadvertised function is never delegated, so Exasol keeps it and evaluates it over the returned rows (`docs/capabilities.md` § Handled by Exasol). Advertising a capability the adapter cannot honour is what produces silent wrong answers — verified live for `ORDER_BY_EXPRESSION` with no backing path (see `vs-adapter/pushdown-planning-order-by-capability`). The now-family withdrawal moves these four names from the delegated side to the Exasol-evaluated side, so it cannot lose or mistranslate a clause.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Now-family date/time capabilities are withdrawn so Exasol evaluates its own clock

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, or `FN_SYSTIMESTAMP`
* *AND* Exasol SHALL evaluate the now-family natively rather than pushing it to the node-local scan, because no time zone, clock, or statement anchor reaches the scan UDF, so the scan can only read its own container clock in UTC, independently per shard — see the Background and `sql-comprehension/vs-expression-translator-date-fns`
* *AND* the four names SHALL be declined by the expression translator in BOTH dialects with the `unsupported scalar function: <name>` error, keeping the capability set and the translator coherent the same way the regexp, bitwise, and `ADD_*` date-arithmetic withdrawals do
* *AND* the withdrawal SHALL NOT alter any other advertised capability — `FN_DATE_TRUNC`, `FN_EXTRACT`, the field shortcuts (`FN_DAY`, `FN_HOUR`, `FN_MINUTE`, `FN_MONTH`, `FN_SECOND`, `FN_YEAR`, `FN_WEEK`), `FN_TO_DATE`, `FN_TO_TIMESTAMP`, and the `*_BETWEEN` family SHALL all remain advertised because each takes its datetime from its own arguments rather than from a clock — and Cartesian-product capabilities SHALL remain absent with only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) advertised, so this withdrawal introduces no join or cross-join capability change
* *AND* `docs/capabilities.md` SHALL NOT list the four withdrawn capabilities in its pushed-down scalar-function table, so the operator-facing documentation cannot claim a pushdown the adapter no longer advertises
<!-- /DELTA:NEW -->
