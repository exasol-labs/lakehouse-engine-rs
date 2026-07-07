# Decision Log: fix-join-decline-hard-fail

Date: 2026-07-07

## Interview

Headless mode (`speq-plan-pr`) — no live human interview. The findings below were
pre-researched and passed in with the task; they are recorded here as the interview
of record, followed by the assumptions made under the headless escalation bar.

**Q:** What is the exact failure, and where does it originate?
**A:** Issue #76 — a pushdown over an inner join spanning 3+ tables (Q1
`supplier⋈nation⋈region`, Q2 `customer⋈orders⋈lineitem`, NQ3
`part⋈partsupp⋈supplier⋈nation`) hard-fails with `F-UDF-CL-RUST-9001: join pushdown
declined: the join spans more than two tables …`. Origin: `ineligible_join_decline`
(`pushdown.rs:3999`), reached from `handle_pushdown` (`pushdown.rs:2097`) for
`JoinShape::Ineligible(TooManyTables)`, which returns `UdfError::User`.

**Q:** Does Exasol retry a declined pushdown natively (the premise the code's doc
comment relies on)?
**A:** No. The `exasol-udf-macros` 0.20.3 FFI shim erases every `UdfError` variant to
return code 1, which the UDF host surfaces as a hard SQL error to the client. There is
no native-retry path in this repo or the SDK. This is the same false premise ADR-083
explicitly rejected and ADR-085/086 fixed for the two-table case.

**Q:** Does the adapter's JSON protocol even have a "decline, run natively" response?
**A:** No. `dispatch()` (`mod.rs:137-167`) has exactly five response types; every
`pushdown` response carries a non-null SQL string. `JoinShape::Ineligible` is the only
`handle_pushdown` branch that returns `Err` before attempting any SQL.

**Q:** Does the spec already require the correct behavior?
**A:** Yes. `vs-adapter/pushdown-planning-join` scenario "A join outside the broadcast
contract is declined safely" already states a >2-table join "SHALL instead emit the
unaccelerated … join SQL when it can build one" and error "only when even the
unaccelerated fallback cannot be built." So this is a spec-vs-implementation mismatch
fix, not new spec authoring — the fallback builder is simply two-sided only today.

**Q:** What is the test gap?
**A:** `join_outside_contract_declined_safely` (`pushdown.rs:7296`) only asserts the
decline MESSAGE TEXT; no test exercises runtime behavior for a 3+ table join. The same
gap ADR-085/086 closed with live E2E tests for the two-table case must be closed for
the 3+ table case (Q1/Q2/NQ3 shapes).

**Q:** Scope boundaries?
**A:** Strictly the ineligible-join hard-fail bug and its test gap. Do NOT touch Phase-2
broadcast work (BL-001) or unrelated lc-rs/perf content from sibling PR #74. Reference
`Closes #76` for the implementing commit (committed in implement-pr, not here).

## Design Decisions

### [1] N-table inner joins fall back to an N-scan unaccelerated wrapper (generalizes ADR-083 to N tables)

- **Decision:** A pushdown over an inner join spanning three or more involved tables is
  served by materializing each table through its own sharded scan-UDF fan-out and
  reconstructing the original inner join in Exasol's core engine — never by returning an
  error. An error is reserved for a shape whose fallback genuinely cannot be built (a
  non-inner join node, an involved table absent from `TABLE_MAP` or carrying no column
  metadata, or a condition/clause the translator cannot render).
- **Alternatives:** Keep declining `TooManyTables` (status quo — hard-fails a
  currently-broken but expected-to-work query class); advertise fewer join capabilities
  so Exasol never pushes multi-table joins (rejected — regresses the two-table broadcast
  benefit and does not match the already-written spec).
- **Rationale:** Directly closes #76, honors the existing spec wording, and extends the
  proven ADR-083/085/086 pattern ("a join is never wrong, only sometimes unaccelerated")
  from two tables to N. Exasol does not retry declined pushdowns, so the fallback is the
  only correct outcome.
- **Promotes to ADR:** yes

### [2] N-scan wrapper renders as cross-join + conjunctive table-qualified WHERE

- **Decision:** The N-scan wrapper is `SELECT <qualified select list> FROM (fan0)
  "LHS_T0", (fan1) "LHS_T1", … WHERE <all N-1 join conditions AND-conjoined with the
  qualified residual filter> [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT …]`, with every
  column reference table-qualified from its `tableName` via the ADR-085 alias-annotation
  machinery.
- **Alternatives:** A chained `INNER JOIN … ON` tree faithfully reproducing the pushed
  join tree (rejected — requires ON-scope bookkeeping so each condition references only
  tables already introduced; error-prone for arbitrary trees and buys nothing since
  Exasol re-optimizes anyway).
- **Rationale:** For all-inner joins, cross-join + conjunctive WHERE is provably
  equivalent to any join-tree ordering and is order-agnostic, so the builder need not
  track which tables each condition spans. Exasol's optimizer turns equi-conditioned
  cross joins into hash joins. Table-qualified rendering (ADR-085) makes it correct even
  when involved tables share a column name.
- **Promotes to ADR:** yes

### [3] Freeze the two-table path; add the N-table path additively

- **Decision:** Add `JoinShape::MultiTable(MultiTableJoin)` + `plan_multi_table_join` +
  `build_n_scan_join_sql` for N≥3. Leave the two-table `Eligible`/`JoinSides`/
  `build_unaccelerated_join_sql`/`build_two_scan_join_sql`/`LHS_FACT`/`LHS_DIM` path and
  all its ADR-081..086 tests byte-for-byte unchanged.
- **Alternatives:** Retrofit N tables into the existing `EligibleJoin`/`JoinSides`
  structures (rejected — churns the working two-table broadcast + two-scan code and its
  E2E assertions like `has_two_scan_wrapper`, raising regression risk for no benefit).
- **Rationale:** Isolation confines the change to new, independently-testable units and
  guarantees the two-table broadcast benefit and its live-tested regressions stay intact.
  Future planners should extend the N-table path rather than re-unify unless a broadcast
  N-way join is ever pursued.
- **Promotes to ADR:** yes

### [4] Only genuinely-unbuildable shapes still return an error

- **Decision:** `detect_join` returns `Err` only for a stale-VS condition (a leaf table
  absent from `TABLE_MAP`); it returns `Ineligible` (→ error) only for a non-inner join
  node or a malformed/non-table leaf. `build_n_scan_join_sql` returns `Err` only when a
  condition/clause cannot be rendered or an involved table carries no column metadata.
- **Alternatives:** Attempt a fallback even for outer-join nodes (rejected — a
  cross-join + WHERE cannot reproduce outer-join semantics; and outer joins are not
  advertised, so Exasol never pushes them).
- **Rationale:** Matches the spec's "only when even the unaccelerated fallback cannot be
  built"; keeps the last-resort error narrow and correct.
- **Promotes to ADR:** no

### [5] Runtime E2E test closes the behavior gap (assume seed extension is conventional)

- **Decision:** Add E2E tests (`e2e_three_table_join_result_correct`,
  `e2e_four_table_join_result_correct`) that assert the query SUCCEEDS, the pushed SQL is
  the N-scan wrapper, and the result equals single-node evaluation — seeding a third and
  fourth small Iceberg table in the join E2E fixtures. Host unit tests cover detection
  and SQL shape.
- **Alternatives:** Only add host unit tests (rejected — that is exactly the gap the
  interview flagged: unit tests assert message text, not runtime behavior; ADR-085/086
  proved live E2E is what catches these regressions).
- **Rationale:** The bug is a runtime hard-fail invisible to shape-only unit tests; the
  fix is unproven without a query that actually runs against Exasol. Extending the join
  E2E seed with additional small tables is a conventional fixture addition, assumed under
  the headless bar rather than escalated.
- **Promotes to ADR:** no

## Review Findings

<!-- Populated by speq-implement after code review. -->
