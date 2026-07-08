# Decision Log: fix-join-decline-hard-fail

Date: 2026-07-08

## Interview

This plan is a REVISION of an already-implemented-and-recorded plan, driven by four
blocking code-review findings on PR #78. There was no live interview; the findings below
were passed in with the task and are recorded here as the interview of record, followed
by the design decisions made to fix them.

**Q (Finding #1, BLOCKING):** Is the real defect the join arity, as the first cut assumed?
**A:** No. The failing query is a grouped-aggregate select list over a join whose select
item is a SCALAR FUNCTION WRAPPING AGGREGATES, e.g.
`ROUND(100.0 * SUM(CASE WHEN l_returnflag='R' THEN 1 ELSE 0 END) / COUNT(*), 2)`.
`render_selectlist_item_qualified` (`pushdown.rs:4486`) only special-cases a TOP-LEVEL
`function_aggregate` (→ `render_aggregate_qualified`); everything else goes to
`render_expression_qualified` → `vs_expression::render_expression_safe` →
`render_expression_inner` (`vs-expression/src/lib.rs:100`). That renderer has a
`function_scalar` arm (ROUND, arithmetic, CASE — recurses into args) but NO
`function_aggregate` arm; when recursion reaches a nested `SUM`/`COUNT` it hits the
catch-all (`lib.rs:728`) → `Err("unsupported expression node type: function_aggregate")`
→ swallowed to `None` → decline. This declines at single-table, two-table, AND N-table.
The fix belongs at the shared `vs-expression` seam, not in the join arity code.

**Q (Finding #2, BLOCKING, architectural):** Why did the bug ship in one path but not the
other?
**A:** There are TWO parallel join implementations: two-table
(`plan_eligible_join`/`build_unaccelerated_join_sql`/`build_two_scan_join_sql`, aliases
`LHS_FACT`/`LHS_DIM`) and N≥3
(`plan_multi_table_join`/`build_n_scan_join_sql`, aliases `LHS_T0..`). The rendering gap
was in BOTH, and the first fix only touched one. The adapter advertises
`JOIN`/`JOIN_TYPE_INNER`/`JOIN_CONDITION_EQUI` statically once (`capabilities.rs:152`)
with no per-query opt-out, so Exasol pushes every inner equi-join of any arity. Required
for merge: collapse to a SINGLE N≥2 fallback renderer (two-table = N=2), broadcast an
optimization selected within it. ADR-094 (freeze two-table + add N-table additively) must
be SUPERSEDED, not extended; ADR-092/093 reframed/folded into the unified design.

**Q (Finding #3):** Is "Exasol will retry natively" true?
**A:** No, and the codebase already says so (ADR-083, ADR-085; the PR body). The
`exasol-udf-macros` FFI shim erases `UdfError::User` into a hard `F-UDF-CL-RUST-9001`.
There are 15 decline sites in `pushdown.rs` (lines 2211, 2231, 2253, 2614, 4161, 4819,
4867, 4883, 5168, 5181, 5277, 5309, 5329, 5358, 5415) framed as native retry. Principle
to adopt: for each advertised capability the adapter must ALWAYS be able to render what
Exasol may push, or must NOT advertise it; "decline at runtime and hope Exasol retries"
is not a valid third option. A unit test at `pushdown.rs:7936` asserts `msg.contains("retry")`
and must change.

**Q (Finding #4):** What is the test gap?
**A:** The existing E2E (`e2e_three_table_join_result_correct`,
`e2e_four_table_join_result_correct`) do NOT cover a scalar-over-aggregate grouped select
list. Add E2E for the reported shape (`SUM(expr)`, `SUM(CASE …)`, `AVG`,
`ROUND(… SUM(…)/COUNT(*) …)`, HAVING, ORDER BY, LIMIT) at BOTH N=2 and N≥3 — fails before,
passes after — plus host unit tests for `vs-expression` aggregate rendering and for
`render_selectlist_item_qualified` on a scalar-over-aggregate item.

**Q:** Do the single-table aggregate-pushdown paths (count-distinct, expression-aggregate,
grouped-agg) risk regression from the `vs-expression` change?
**A:** No. Those detect a top-level `function_aggregate` in the select list BEFORE recursing
into `vs-expression` (they extract the argument and render THAT), so the new aggregate arm
only affects aggregates NESTED inside another expression — exactly the case that previously
errored. They must stay behavior-compatible and are covered by their existing tests.

## Design Decisions

### [1] Fix aggregate rendering at the shared `vs-expression` seam (root cause)

- **Decision:** Add a `function_aggregate` arm to `render_expression_inner`
  (`vs-expression/src/lib.rs`): splice the aggregate `name` verbatim (uppercased),
  render `COUNT(*)`/star for empty/star arguments, render arguments by recursion, honor
  `distinct: true` → `COUNT(DISTINCT arg)`, and qualify column arguments via the ADR-085
  `tableAlias` annotation. Then unify `render_selectlist_item_qualified` and
  `render_aggregate_qualified` onto that path so top-level and nested aggregates render
  identically, keeping the top-level output byte-compatible with the shapes it already
  handled.
- **Alternatives:** Special-case scalar-over-aggregate only in the join select-list path
  (rejected — leaves the same gap for single-table nested aggregates and any future
  caller of the shared translator); keep declining scalar-over-aggregate (rejected — it
  is a valid, expected TPC-H-shaped query and there is no native retry).
- **Rationale:** The seam is shared by all arities; one fix repairs single-table,
  two-table, and N-table. Matches the existing verbatim-aggregate-name discipline already
  used by `render_aggregate_qualified` and the single-table partial/merge paths.
- **New ADR:** ADR-096. **Promotes to ADR:** yes

### [2] One unified N≥2 unaccelerated join renderer; broadcast an inner optimization

- **Decision:** Collapse the two join implementations into one. `detect_join` yields a
  single join shape carrying the N (≥2) resolved involved tables and the N-1 conditions;
  `handle_pushdown` routes through one `plan_join`, which computes broadcast eligibility
  (N==2, small side ≤ threshold, no Exasol postprocessing) as a property and, when
  eligible, takes the broadcast fan-out — otherwise calls the SOLE fallback renderer
  `build_n_scan_join_sql` (`LHS_T0..LHS_T{N-1}`, cross-join + conjunctive table-qualified
  WHERE, ADR-091 per-side pushdown, ADR-085 qualified rendering). Remove
  `build_unaccelerated_join_sql`, `build_two_scan_join_sql`, `resolve_join_sides`, the
  `Eligible`/`MultiTable` split, and the `LHS_FACT`/`LHS_DIM` alias scheme. The two-table
  fallback is now exactly N=2.
- **Alternatives:** Keep the additive two-path design of ADR-094 (rejected — the two
  copies drifted and shipped this bug; "two-table = N=2" must be structural, not a
  coincidence). Retrofit was the first cut's choice; it is what failed review.
- **Rationale:** A single renderer cannot diverge from itself. The bug existed only
  because the rendering gap sat in two copies and the fix landed in one. Supersedes
  ADR-094; folds ADR-092 (fallback-not-error) and ADR-093 (cross-join + conjunctive
  qualified WHERE rendering) into the unified renderer.
- **New ADR:** ADR-095 (supersedes ADR-092, ADR-093, ADR-094). **Promotes to ADR:** yes

### [3] Purge the native-retry fiction; advertised capability must render

- **Decision:** Remove "Exasol will retry natively / retry the query natively" from all 15
  decline sites. Delete the sites whose shapes now always render (after [1] and [2]).
  Reword the genuine last-resort errors — a non-inner join node in the tree, an involved
  table absent from `TABLE_MAP` or with no column metadata, or a clause the translator
  cannot render — as plain hard client-facing errors with no retry. Adopt the principle:
  for each advertised capability the adapter must always be able to render what Exasol may
  push, or must not advertise it. Update the `msg.contains("retry")` test
  (`pushdown.rs:7936`).
- **Alternatives:** Keep the wording (rejected — it is false; the FFI shim makes every
  `UdfError::User` a hard `F-UDF-CL-RUST-9001`, as ADR-083/085 already established).
- **Rationale:** Truthful error semantics; removes a recurring source of "just decline and
  hope" bugs. The protocol has no decline-and-retry response.
- **New ADR:** ADR-097. **Promotes to ADR:** yes

### [4] Genuinely-unbuildable shapes still return a hard error

- **Decision:** `detect_join`/`build_n_scan_join_sql` return a hard `Err` only for a
  non-inner join node in the tree, a leaf table absent from `TABLE_MAP` (stale VS), an
  involved table with no column metadata, or a condition/clause `vs-expression` cannot
  render. Every other inner-join shape is served by broadcast or the unified fallback.
- **Alternatives:** Attempt a fallback for outer-join nodes (rejected — cross-join + WHERE
  cannot reproduce outer-join semantics, and outer joins are not advertised so Exasol
  never pushes them).
- **Rationale:** Keeps the last-resort error narrow, correct, and truthful (no retry).
- **Promotes to ADR:** no

### [5] E2E proves the reported scalar-over-aggregate shape at N=2 and N≥3

- **Decision:** Add `e2e_scalar_over_aggregate_grouped_join_result_correct` (N=2) and
  `e2e_scalar_over_aggregate_grouped_join_n_table_result_correct` (N≥3) running a grouped
  join with `SUM(expr)`, `SUM(CASE …)`, `AVG`, `ROUND(100.0 * SUM(…) / COUNT(*), 2)`,
  HAVING, ORDER BY, LIMIT — asserting success, the unified N-scan wrapper, and equality to
  single-node evaluation. Extend the join E2E seed with the discriminator column the query
  needs. Host unit tests cover `vs-expression` aggregate rendering and the
  `render_selectlist_item_qualified` seam.
- **Alternatives:** Host unit tests only (rejected — the original defect was a runtime
  hard-fail invisible to shape-only unit assertions; the first cut's message-text test is
  exactly why the bug shipped).
- **Rationale:** The fix is unproven without a query that actually runs the reported shape
  end-to-end. Extending the existing join seed is a conventional fixture addition.
- **Promotes to ADR:** no

## ADR Actions for the Recorder

When recording this revised plan, the recorder MUST update `specs/decision-log.md` as
follows (continue numbering after ADR-094):

- **ADR-092** (N-Table Inner Joins Fall Back to an N-Scan Unaccelerated Wrapper) → set
  **Status: Superseded by ADR-095**. Its outcome (a 3+ table inner join never errors) is
  RETAINED; only its "separate additive N-table path" framing is replaced.
- **ADR-093** (N-Scan Wrapper Renders as Cross-Join + Conjunctive Table-Qualified WHERE) →
  set **Status: Superseded by ADR-095**. Its rendering technique is RETAINED and restated
  inside ADR-095 as the single N≥2 renderer.
- **ADR-094** (Freeze the Two-Table Join Path; Add the N-Table Path Additively) → set
  **Status: Superseded by ADR-095**. Its decision is REVERSED: the two paths are unified.
- **ADR-095** (new): Single Unified N≥2 Unaccelerated Join Renderer — supersedes
  ADR-092/093/094. One fallback implementation for every inner join with N≥2 involved
  tables (two-table = N=2); broadcast is an optimization selected within the one path;
  fallback-not-error retained; rendered as cross-join + conjunctive table-qualified WHERE.
- **ADR-096** (new): `vs-expression` Renders Aggregate Function Nodes at the Shared Seam —
  the root-cause fix for scalar-function-wrapping-aggregate select items across all
  arities; aggregate name spliced verbatim, arguments recursed, `COUNT(*)`/`DISTINCT`
  handled; top-level and nested aggregate rendering unified.
- **ADR-097** (new): Advertised Capability Must Render — Purge the Native-Retry Fiction.
  There is no native re-plan on an adapter error; a declined pushdown is a hard
  `F-UDF-CL-RUST-9001`. Every advertised capability must be renderable or unadvertised;
  hard errors are reserved for genuinely-unrenderable shapes and are never framed as
  retries.

## Review Findings

<!-- Populated by speq-implement after code review. -->
