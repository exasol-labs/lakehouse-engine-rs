# Verification Report: fix-join-decline-hard-fail (revision addressing PR #78 review)

## Verdict: PASS

All four PR #78 review findings are genuinely fixed (not superficially), verified by code
review and a live end-to-end run against the Exasol Docker stack. Host unit tests, E2E
suites, clippy, and fmt are all green. The workspace version is bumped to a fix release.

| Gate | Result |
|------|--------|
| `cargo test` (host unit) | **PASS** — 453 (lakehouse lib) + 61 (vs-expression) + integration binaries, 0 failed |
| `cargo clippy --all-targets -- -D warnings` | **PASS** — 0 warnings |
| `cargo fmt --all -- --check` | **PASS** — no diff |
| `make cross-musl-udf-build` | **PASS** — exit 0 |
| `make test-e2e` (live Exasol Docker) | **PASS** — 78 passed, 0 failed across 5 suites |
| Code review | **PASS** — 4 findings verified fixed, 0 blocking bugs; 1 trivial comment fixed |
| Version bump | lakehouse-engine 0.24.2 → **0.24.3** (fix); `Cargo.lock` synced |

## Review findings — how each is fixed

### Finding #1 (blocking) — root cause at the shared translator seam
`crates/vs-expression/src/lib.rs` now has a `function_aggregate` arm in
`render_expression_inner`: it splices the Exasol aggregate name verbatim (uppercased),
renders `COUNT(*)` for the empty-`arguments`/star case, recurses each argument, honors
`distinct`, and qualifies column arguments via the ADR-085 `tableAlias` annotation. In
`pushdown.rs`, `render_selectlist_item_qualified` is now a one-line delegate to
`render_expression_qualified`, so a top-level aggregate and one nested inside a scalar
function (e.g. `ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)`)
render through the identical path — the divergence is gone. The reported scalar-over-aggregate
select item now renders instead of declining.

### Finding #2 (blocking, architectural) — one join path, not two
There is now a single unaccelerated join renderer, `build_n_scan_join_sql`, reached for all
N ≥ 2 (two-table = N=2). `detect_join` yields one `JoinShape::Join(DetectedJoin)`;
`handle_pushdown` routes through one `plan_join` where broadcast is computed as an inner
optimization (N==2, equi, no Exasol postprocessing, dimension ≤ threshold, render succeeds)
and falls through cleanly to the N=2 fallback otherwise. Removed: `plan_eligible_join`,
`plan_multi_table_join`, `build_unaccelerated_join_sql`, `build_two_scan_join_sql`,
`resolve_join_sides`, `qualified_join_select_items`, the `LHS_FACT`/`LHS_DIM` scheme,
`JoinShape::{Eligible, MultiTable}`, and `IneligibleJoinReason::{TooManyTables,
NotEquiCondition}`. Broadcast fan-out rendering is preserved byte-for-byte. Intended
behavior change: non-equi and composite-key two-table joins now take the correct unified
fallback instead of hard-declining.

### Finding #3 — the "Exasol will retry natively" fiction is purged
No production `UdfError::User` message contains "retry natively" / "retry the query
natively" (grep-confirmed). Surviving last-resort errors state plainly they are a hard
`F-UDF-CL-RUST-9001` error with no native re-plan, and describe genuinely-unrenderable
shapes (non-inner join node, table absent from `TABLE_MAP`, no column metadata, unrenderable
clause). The unit test that asserted `msg.contains("retry")` now asserts the hard-error
wording and `!msg.contains("retry")`.

### Finding #4 — E2E for the failing shape
`e2e_scalar_over_aggregate_grouped_join_result_correct` (N=2, orders ⋈ lineitem) and
`e2e_scalar_over_aggregate_grouped_join_n_table_result_correct` (N=3, customer ⋈ orders ⋈
lineitem) run the reported shape — `SUM(expr)`, `SUM(CASE …)`, `AVG`,
`ROUND(100.0 * SUM(…) / COUNT(*), 2)`, GROUP BY, HAVING, ORDER BY, LIMIT — assert the query
succeeds, the pushed SQL is the unified N-scan wrapper (`has_n_scan_wrapper`, no broadcast
block), and the result equals single-node evaluation. Both would hard-fail before the fix
(select-item decline → `F-UDF-CL-RUST-9001`) and pass after. Host unit tests cover
vs-expression aggregate rendering and the `render_selectlist_item_qualified` seam.

## Scenario coverage audit

| Scenario (delta) | Test | Status |
|---|---|---|
| vs-expression: aggregate nodes render with name spliced verbatim (NEW) | `render_expression`-family aggregate tests (vs-expression) | PASS |
| Scalar-fn-wrapping-aggregates in a grouped join select list is rendered, not declined (NEW) — seam | `render_selectlist_item_qualified_renders_scalar_over_aggregate` | PASS |
| — runtime N=2 | `e2e_scalar_over_aggregate_grouped_join_result_correct` | PASS (live) |
| — runtime N≥3 | `e2e_scalar_over_aggregate_grouped_join_n_table_result_correct` | PASS (live) |
| Unified N≥2 renderer (CHANGED) — detection + SQL shape | `detect_join`/`build_n_scan_join_sql` unit tests (N=2/3/4, shared-column triple) | PASS |
| A join outside the broadcast contract is declined safely (CHANGED — no native retry) | `join_outside_contract_declined_safely` | PASS |
| Aggregate over a join routes through the unified qualified wrapper (CHANGED) | `e2e_aggregate_over_join_result_correct` + unit | PASS (live) |

## Known limitation (out of scope; recommend follow-up)

A **single-table** grouped query whose select list contains a scalar function wrapping
aggregates (e.g. `SELECT ROUND(100.0*SUM(CASE…)/COUNT(*),2) FROM t GROUP BY x`) still
hard-fails with Exasol error 04000 ("Expected number of columns … but pushdown query has …").
Root cause: the single-table grouped-aggregate path (`detect_group_by_aggregates`) declines
any non-`function_aggregate` select item and falls back to a raw full-row scan with a
mismatched column count. This is a **pre-existing** limitation in the single-table
partial/merge aggregate-decomposition subsystem, which this plan explicitly froze as a
Non-Goal — it is a different code path from the join renderer (it does not pass through the
now-fixed `render_selectlist_item_qualified` / vs-expression seam). The E2E ground truth was
therefore materialized into a native Exasol table (via VS projection pushdown, which works)
rather than run as a single-table VS aggregate. Recommended as a separate follow-up issue:
teach the single-table grouped path to render a non-decomposable select list through the
same qualified wrapper the join path now uses.

## Manual testing (covered by live E2E)

The plan's Manual Testing queries (grouped scalar-over-aggregate join at N=2 and N≥3) are
exercised directly by the two new live E2E tests above, which assert both the unified N-scan
wrapper shape and single-node result equality.
