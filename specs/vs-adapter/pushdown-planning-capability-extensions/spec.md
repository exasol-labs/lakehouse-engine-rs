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

* A scalar-function capability is advertised only once a `crates/vs-expression` arm renders it and
  the DataFusion 54 result matches Exasol. `FN_CAST`, `FN_NEG`, and `FN_WEEK` meet this bar;
  `FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the regexp scalar functions, the divergent date
  functions, and the bitwise operator functions do not and stay unadvertised.
* Credentials MUST NOT appear in any returned SQL or error message.
* Iceberg spec compliance: checked, not engaged. Verified against the Apache Iceberg table
  spec (https://iceberg.apache.org/spec/) rather than from memory: the normative sections
  that could bear on this change are the ones governing what a reader must resolve —
  schema/field-id resolution ("Schemas and Data Types", "Column Projection") and scan
  planning ("Scan Planning", manifest/partition filtering). This feature touches none of
  them: it changes only which scalar/type-conversion capabilities the adapter advertises,
  reading no manifest and resolving no snapshot, field id, delete, or type mapping. No
  normative requirement applies, so there is no deviation to fix and none to track.
* See also: ordered-sort-key capability advertisement (`ORDER_BY_COLUMN` /
  `ORDER_BY_EXPRESSION`) lives in `vs-adapter/pushdown-planning-order-by-capability`;
  scalar/boolean select-list expression pushdown and widened-projection routing live in
  `vs-adapter/pushdown-planning-selectlist-expressions`; HAVING pushdown and statistical
  aggregates live in `vs-adapter/pushdown-planning-aggregate-extensions`; literal/constant
  select-list projection lives in `vs-adapter/pushdown-planning-literal-projection`.
* **A capability is withdrawn when the scan cannot evaluate the function faithfully, not only when the translator cannot render it.** The four now-family names — `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` — render as valid SQL in both dialects today, yet the node-local scan cannot produce Exasol's value for any of them. Exasol's four names are three distinct semantics over one instant: `CURRENT_TIMESTAMP` interprets it in the session time zone (`TIMESTAMP(3) WITH LOCAL TIME ZONE`), `SYSTIMESTAMP` interprets the same instant in the database time zone (`TIMESTAMP(3)`), and `CURRENT_DATE`/`SYSDATE` are `TO_DATE` of each. Rendering that distinction needs `SESSIONTIMEZONE` and `DBTIMEZONE`. Neither value reaches the scan UDF: the pushdown request carries no zone, `CommonScanSpec` carries no temporal field, the scan script declares only the common blob and the per-file list, the scan opens no connect-back session, and the SDK's `UdfContext` exposes no clock and no zone. The scan therefore reads its own container clock in UTC. It also reads that clock once per shard — the fan-out builds and drops a `SessionContext` per invocation — so a pushed clock call is evaluated G times with no statement anchor, while Exasol's now-family is statement-constant. Withdrawal is the correctness fix: Exasol never delegates a capability the adapter does not advertise, so Exasol evaluates its own clock, once, in its own zones. All three claims were measured against live Exasol 2025.2.1 rather than inferred from the advertised capability set: `EXPLAIN VIRTUAL` over a select-list `SYSTIMESTAMP` pushes `"projection":[{"expr":"now()"}, …]` with `"emit_exa_types":["TIMESTAMP(3)", …]`, and a filter-position `CURRENT_TIMESTAMP` pushes `"filter":"(now() < \"EVENT_TS\")"`, so the node is genuinely delegated; the same select returned `15:02:02.716` through the virtual schema against `17:02:03.141` from Exasol in one session, with `DBTIMEZONE` and `SESSIONTIMEZONE` both `EUROPE/BERLIN` over a UTC container clock; and `GROUP BY SYSTIMESTAMP` over a two-file table returned two distinct timestamps against one statement-constant native value. A pure-constant predicate is not a valid probe, because Exasol constant-folds it before building the pushdown request.
* **Withdrawing a capability is the safe direction; advertising without a backing path is the unsafe one.** An unadvertised function is never delegated, so Exasol keeps it and evaluates it over the returned rows (`docs/capabilities.md` § Handled by Exasol). Advertising a capability the adapter cannot honour is what produces silent wrong answers — verified live for `ORDER_BY_EXPRESSION` with no backing path (see `vs-adapter/pushdown-planning-order-by-capability`). The now-family withdrawal moves these four names from the delegated side to the Exasol-evaluated side, so it cannot lose or mistranslate a clause.

## Scenarios

### Scenario: Arithmetic operator scalar-function capabilities are advertised so arithmetic expression trees are pushed down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise the arithmetic binary-operator scalar-function capabilities for `+`, `-`, `*`, and `/` (the Exasol capability names `FN_ADD`, `FN_SUB`, `FN_MULT`, and `FN_FLOAT_DIV`), verified against the Exasol Virtual Schema capability vocabulary
* *AND* each advertised arithmetic operator SHALL be backed by a `crates/vs-expression` translator arm that renders it (so an advertised operator is never one the translator would decline), keeping the capability set coherent with the translator the same way GROUP BY tuple advertisement is gated on its backing path
* *AND* Cartesian-product capabilities SHALL remain absent, and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised — advertising arithmetic operators MUST NOT introduce any additional join or cross-join capability

### Scenario: An advertised arithmetic expression Exasol cannot see decomposed remains correctness-safe

* *GIVEN* the adapter advertises the arithmetic operator capabilities and Exasol pushes an arithmetic expression tree in a filter, select-list, group-key, or aggregate-argument position
* *WHEN* the `crates/vs-expression` translator cannot render a particular arithmetic node (e.g. an operator or operand shape it does not handle)
* *THEN* the adapter SHALL fall back on the affected clause exactly as for any other untranslatable expression — a filter is omitted and retained by Exasol, a select-list expression falls back to projecting underlying columns, and an aggregate over the unrenderable argument falls back to row scanning
* *AND* the adapter MUST NOT emit a scan spec that would compute a different result than single-node evaluation

### Scenario: Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_CAST` and `FN_NEG`, each backed by a `crates/vs-expression` translator arm (the CAST arm over its faithful target-type set and the unary-negation arm), so no advertised capability is one the translator would decline for a shape Exasol expects it to handle
* *AND* a CAST to an unsupported target type SHALL fall back — the adapter omits the CAST and Exasol evaluates it — rather than producing an incorrect result
* *AND* `FN_TO_CHAR`, `FN_TO_NUMBER`, and `FN_DIV` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`, see `vs-adapter/pushdown-planning-join`) SHALL be advertised, so advertising `FN_CAST` and `FN_NEG` introduces no additional join or cross-join capability

### Scenario: ISO week capability is advertised so WEEK expressions push down

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL advertise `FN_WEEK`, backed by the `crates/vs-expression` `WEEK` arm rendering `date_part('week', …)`, whose ISO-8601 result matches Exasol `WEEK` (see `sql-comprehension/vs-expression-translator-date-fns`)
* *AND* `FN_ADD_DAYS`, `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_ADD_SECONDS`, `FN_ADD_WEEKS`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, `FN_SECONDS_BETWEEN`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, and `FN_CONVERT_TZ` SHALL remain absent
* *AND* Cartesian-product capabilities SHALL remain absent and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so advertising `FN_WEEK` introduces no additional join or cross-join capability

### Scenario: Regexp scalar function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, or `FN_REGEXP_COUNT`
* *AND* Exasol SHALL post-process regexp scalar functions rather than pushing them to the node-local scan, because at the pinned DataFusion 54.0.0 and `regex` 1.12.4 the Rust `regex` dialect rejects the backreferences and lookaround Exasol's PCRE dialect accepts, DataFusion has no `regexp_substr`, and its `regexp_replace`/`regexp_instr`/`regexp_count` argument shapes omit Exasol's position, occurrence, and return-option arguments — a compile-time literal-pattern check cannot certify semantic match parity, so no faithful translation exists (see issue #106 and `sql-comprehension/vs-expression-translator-scalar-fns`)
* *AND* the pre-existing `FN_PRED_REGEXP_LIKE` predicate advertisement SHALL remain unchanged

### Scenario: Bitwise operator function capabilities remain absent

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_BIT_AND`, `FN_BIT_OR`, `FN_BIT_XOR`, `FN_BIT_NOT`, `FN_BIT_LSHIFT`, `FN_BIT_RSHIFT`, `FN_BIT_LROTATE`, `FN_BIT_RROTATE`, `FN_BIT_CHECK`, `FN_BIT_SET`, or `FN_BIT_TO_NUM`
* *AND* Exasol SHALL post-process bitwise operator functions rather than pushing them to the node-local scan, because at pinned DataFusion 54.0.0 no faithful translation exists for any of the eleven over Exasol's bit-function domain (issue #108): Exasol bit functions operate on unsigned 64-bit integers (range `0`–`18446744073709551615`, result `DECIMAL(20,0)`), while DataFusion's `&`/`|`/`#`/`<<`/`>>` act on the operand's signed Arrow integer type — Iceberg sources carry only signed integers (`int` = 32-bit signed, `long` = 64-bit signed; the Iceberg spec defines no unsigned integer primitive), so a bit-63-set result is a large positive value in Exasol but negative under signed `Int64` and the `Int64` → `DECIMAL(20,0)` mapping carries the negative value; `BIT_RSHIFT` diverges unconditionally on any bit-63-set operand because DataFusion's signed `>>` is arithmetic (sign-extending) whereas Exasol's is logical (zero-fill); and DataFusion 54.0.0 provides no operator or scalar function at all for `BIT_NOT` (its SQL planner rejects unary `~`), `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, or `BIT_TO_NUM`
* *AND* `FN_BIT_LENGTH` SHALL be treated as out of scope for this decision — it is an Exasol string function (bit count of a string), not a bitwise operator — and only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) SHALL be advertised, so this decision introduces no additional join, cross-join, or string-function capability change

### Scenario: Now-family date/time capabilities are withdrawn so Exasol evaluates its own clock

* *GIVEN* the adapter's advertised capability set
* *WHEN* Exasol requests `getCapabilities`
* *THEN* the response SHALL NOT advertise `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_SYSDATE`, or `FN_SYSTIMESTAMP`
* *AND* Exasol SHALL evaluate the now-family natively rather than pushing it to the node-local scan, because no time zone, clock, or statement anchor reaches the scan UDF, so the scan can only read its own container clock in UTC, independently per shard — see the Background and `sql-comprehension/vs-expression-translator-date-fns`
* *AND* the four names SHALL be declined by the expression translator in BOTH dialects with the `unsupported scalar function: <name>` error, keeping the capability set and the translator coherent the same way the regexp, bitwise, and `ADD_*` date-arithmetic withdrawals do
* *AND* the withdrawal SHALL NOT alter any other advertised capability — `FN_DATE_TRUNC`, `FN_EXTRACT`, the field shortcuts (`FN_DAY`, `FN_HOUR`, `FN_MINUTE`, `FN_MONTH`, `FN_SECOND`, `FN_YEAR`, `FN_WEEK`), `FN_TO_DATE`, `FN_TO_TIMESTAMP`, and the `*_BETWEEN` family SHALL all remain advertised because each takes its datetime from its own arguments rather than from a clock — and Cartesian-product capabilities SHALL remain absent with only the inner equi-join capabilities (`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI`) advertised, so this withdrawal introduces no join or cross-join capability change
* *AND* `docs/capabilities.md` SHALL NOT list the four withdrawn capabilities in its pushed-down scalar-function table, so the operator-facing documentation cannot claim a pushdown the adapter no longer advertises
