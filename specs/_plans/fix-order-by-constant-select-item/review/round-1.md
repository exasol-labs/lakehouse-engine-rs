# Plan Review Findings: fix-order-by-constant-select-item (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 3, Advisory: 7)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now the plan failed. Three ways it got there:

1. A recorded join scenario still says that a request with any non-`column` `orderBy` node declines the broadcast plan. A later change to the join window follows that scenario and pins it with a test, or a reader finds two recorded specs that disagree about the same request. Route: `[REQUIREMENT_CONFLICT]` B1.
2. A maintainer extends the grouped path from the recorded topn scenario "An ORDER BY of constants only runs as an unordered LIMIT". Its GIVEN admits any query, and its THEN puts the row limit on each shard, so a grouped partial scan drops groups before the merge. Route: `[REQUIREMENT_CONFLICT]` B2.
3. A maintainer reads "the adapter SHALL leave every constant sort key out of the ordering it renders" and the planned doc comment "no holder's `orderBy` carries a constant key", and removes the `FALSE` key. Exasol then rejects every all-constant `ORDER BY` with an `OFFSET`. Route: `[REQUIREMENT_CONFLICT]` B3 and `[INFORMATION_LEAKAGE]` A5.

## Intent Fidelity

no objection: axis checked. The plan drops every literal sort key when it parses the request, as the interview answer ("drop it at parse time") and the suggested fix in #620 ask, and it keeps function keys. Each of the four #620 acceptance criteria maps to a row of `constant_sort_key_returns_the_native_order` (the `ID, 1` query with `LIMIT`, the `WHERE id <= 5` query without `LIMIT`, the `ID, 5` query, and the `RANDOM()` row). The non-zero-`OFFSET` case is a justified and recorded departure. The interview question named only `LIMIT n`. Exasol rejects `LIMIT … OFFSET` without `ORDER BY`, the planner verified this live, and decision [1] records it. The returned rows match native, so no user-visible result changes. A4 covers the departure's unstated cost. The planner's repro holds on the deployed build: `SELECT ID, -1 FROM MY_LAKEHOUSE.EVENTS ORDER BY 2 DESC, 1 LIMIT 3` returns 1, 2, 3 through `ORDER BY "_LH_PROJ_1"`, and the `GROUP BY ID` variant fails with "Too small value -1 as select list column reference in ORDER BY" (SQL state 04000). Exasol pushes that key as `literal_exactnumeric "-1"`, so the `literal_` rule of decision [2] covers it.

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Context (third bullet), § Impact, § Implementation Tasks 1.1
- Issue: Context names "the N-scan join wrapper (`events JOIN labels … ORDER BY 2 DESC, 1 LIMIT 3`)" as a failing shape, and task 1.1 tests that join "with `1` and with `5`". After the fix, `classify_join_window` (`joins/planning.rs`) sees a column-only `orderBy` and returns `JoinWindowPlan::Ordered`, so the query takes the broadcast path. The route change was checked on the deployed build. The constant-keyed join runs two leg scans under `ORDER BY 1 DESC NULLS FIRST, "LHS_T0"."ID"`. The column-only `ORDER BY 1` join runs one broadcast scan under `ORDER BY "LHS_T0_ID"`. The planned `LIMIT` join rows therefore prove the broadcast window. Only the `OFFSET` row reaches the N-scan wrapper, because its `FALSE` key declines the broadcast plan. The plan does not state the route change.
- Fix: In plan.md § Context and § Impact, state that a join ordered by constants and bare projected columns moves from the N-scan wrapper to the broadcast path. In task 1.1, add rows for `SELECT e.ID, 1 … ORDER BY 2 DESC, 1 LIMIT 3` and its `5` variant on a virtual schema with `JOIN_BROADCAST_MAX_BYTES = '1'`, so the N-scan wrapper is proven fixed. Either put these rows in `join.rs` beside its existing low-threshold virtual schema, or move that setup into `fixtures.rs` (specs/testing.md § Test code).

#### [NFR_IGNORED] ADVISORY
- Location: decision-log.md § Design Decisions [1]; plan.md § Impact (third bullet)
- Issue: The `FALSE` key keeps `order_by_present` true. The row-scan path therefore withholds the per-shard limit, and every shard ships all matching rows for `ORDER BY <constants> LIMIT n OFFSET m`. Main does the same, so this is no regression. Decision [1] does not state the cost. It also does not say that the interview answer ("treat as no ORDER BY (plain row scan with LIMIT)") answered a question about `LIMIT n` alone. § Impact says "An `ORDER BY` of constants only with a `LIMIT` runs as a bare `LIMIT`, so each shard stops early", which also reads as covering the `OFFSET` case.
- Fix: In decision-log.md [1] Rationale, add two sentences. The interview answer covered `LIMIT n` without an offset. With a non-zero offset, each shard returns every matching row, as on main. In plan.md § Impact, scope the early-stop bullet to "with a `LIMIT` and no `OFFSET`".

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Context
- Issue: AGENTS.md § Workflow says a plan that touches pushdown "is checked against the governing Iceberg table spec or Delta Lake protocol. Quote the normative section." This plan changes which requests get a per-shard bounded top-N or a per-shard `LIMIT`, and it records no such check. The recorded plan `011-fix-regexp-like-pushdown` records "Iceberg and Delta compliance: checked, not engaged" with a quoted section.
- Fix: Add a Context bullet that records the check. State that no section of the Iceberg table spec or the Delta Lake protocol governs how a query's `ORDER BY` renders, and that no file-pruning, delete, or type-mapping path changes.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: recorded `specs/pushdown-joins/pushdown-planning-join/spec.md` § Scenarios "Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback" and "A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap"; recorded `specs/pushdown-joins/pushdown-planning-join-fallback/spec.md` § Background; plan.md § Features
- Issue: The recorded decline scenario routes a broadcast-eligible join request "carrying … an `orderBy` element that is an expression, an aggregate, or any non-`column` node" to the unified fallback. Its AND step says "the adapter MUST NOT drop a sort key". Under this plan, the request for `SELECT e.ID, 1 FROM events e JOIN labels l ON e.ID = l.ID ORDER BY 2 DESC, 1 LIMIT 3` carries a `literal_exactnumeric` element. The adapter drops that element and serves the request on the broadcast path (see the first Feasibility finding). An all-constant `orderBy` with `LIMIT n` and no offset becomes the broadcast bare-`LIMIT` shape. The recorded bare-`LIMIT` scenario grants that shape only to "an ABSENT or EMPTY `orderBy`". The recorded join-fallback Background lists "a non-column sort key" among the shapes the broadcast path declines. The plan has no delta for either join feature. Task 3.1 also leaves the joins paragraph of `docs/capabilities.md` (line 152, "sort keys that are projected columns") unchanged.
- Fix: Add `Pushdown Planning, Join` (`pushdown-joins/pushdown-planning-join`) to plan.md § Features as CHANGED, with a delta. The delta holds a `DELTA:CHANGED` block for "Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback", whose GIVEN reads "any non-`column` node other than a constant" and whose AND step exempts a constant sort key from "MUST NOT drop a sort key". It also holds a `DELTA:NEW` scenario. That scenario states that a broadcast-eligible join ordered by constants and bare projected columns stays on the broadcast path, and that an all-constant `orderBy` with `LIMIT n` and no offset is served as the bare-`LIMIT` broadcast shape. Add a `pushdown-joins/pushdown-planning-join-fallback` delta with a `DELTA:CHANGED` `## Background` block. That block rewrites "a non-column sort key" as "a non-column, non-constant sort key" and copies every other recorded Background line unchanged. In task 1.1, tag the default-virtual-schema join rows with the new scenario, and assert the broadcast route through `EXPLAIN VIRTUAL` (one `LAKEHOUSE_SCAN` that carries a join block). Extend task 3.1 to the joins paragraph of `docs/capabilities.md`.
- Escalation: MECHANICAL: the recorded scenarios, `classify_join_window`, and a live `EXPLAIN VIRTUAL` settle the conflict. The plan's behavior follows the interview answer, so only the specs need to change.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: specs/_plans/fix-order-by-constant-select-item/pushdown-capabilities/pushdown-planning-topn/spec.md § Scenario "An ORDER BY of constants only runs as an unordered LIMIT"
- Issue: The GIVEN admits any query: "a query whose every `ORDER BY` key is a constant, with `LIMIT n` and no offset". The THEN requires that "each shard carries the row limit `n` and no sort, as for a bare `LIMIT`". For a grouped query such as `SELECT id, 1, COUNT(*) FROM t GROUP BY id ORDER BY 2 LIMIT 3`, this contradicts the recorded scenario "LIMIT is NOT pushed into per-shard scan for a grouped query" (`pushdown-grouped-agg/pushdown-planning-grouped-agg`). For an N-scan join, it contradicts the join-fallback rule that legs carry no limit. A test written from this scenario for a grouped query fails against the correct implementation. The sibling NEW scenario "A constant sort key does not decide the bounded top-N path" already limits itself to "a single-table row-scan query".
- Fix: In that scenario's GIVEN, replace "a query" with "a single-table row-scan query". The join bare-`LIMIT` case belongs to the join delta from the first Requirement Quality blocker.
- Escalation: MECHANICAL: the recorded grouped scenario settles the scope.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: specs/_plans/fix-order-by-constant-select-item/pushdown-capabilities/pushdown-planning-order-by-capability/spec.md § Scenarios "A constant sort key does not become a select-list position" and "An ORDER BY of constants only keeps its OFFSET window"
- Issue: The first scenario's GIVEN covers any row-scan, grouped, or join query "whose `ORDER BY` names a constant", which includes `SELECT id, 1 FROM t ORDER BY 2 LIMIT 3 OFFSET 18`. Its THEN reads "the adapter SHALL leave every constant sort key out of the ordering it renders". For that same query, the second scenario's THEN reads "the adapter SHALL order by one constant". No implementation satisfies both for one input.
- Fix: Rewrite the first scenario's THEN as "the adapter SHALL leave every constant sort key that Exasol pushes out of the ordering it renders, except the one constant that an `ORDER BY` of constants only keeps for a non-zero `OFFSET`".
- Escalation: MECHANICAL: the two scenarios of this delta settle it.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: specs/_plans/fix-order-by-constant-select-item/pushdown-capabilities/pushdown-planning-order-by-capability/spec.md § Scenario "A constant sort key does not become a select-list position", first and third AND steps; plan.md § Implementation Tasks 1.1
- Issue: (a) The third AND step reads "a sort key that calls a function, such as `ORDER BY RANDOM() LIMIT 3`, SHALL keep its place in the ordering". Its example is a key the adapter never receives: plan.md § Context says "Exasol keeps `ORDER BY RANDOM()` itself and pushes neither the ordering nor the `LIMIT`". The `RANDOM()` row of task 1.1 asserts three distinct ids, which passes even if the adapter drops every function key. Only planning row 2 (`ORDER BY 2 DESC, ABS(score) LIMIT 3`) discriminates the step, and that row carries other scenario tags. (b) The first AND step reads "ids 1, 2, 3 for the queries above, with or without the `LIMIT`". Without the `LIMIT`, those queries return all 20 rows. Task 1.1 runs the no-`LIMIT` query with `WHERE id <= 5`.
- Fix: Replace the `RANDOM()` example with a function key that Exasol pushes, such as `ABS(score)`. Add this scenario's tag to `constant_sort_key_planning_follows_the_other_keys`. Keep the `RANDOM()` row as the guard for the #620 acceptance criterion. Rewrite (b) as "ids 1, 2, 3 for the `LIMIT 3` queries, and ids 1 to 5 in that order for `SELECT id, 1 FROM t WHERE id <= 5 ORDER BY 2 DESC, 1`".

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 2.1; decision-log.md § Design Decisions [2]
- Issue: Decision [2] rejects reusing `LITERAL_SELECTLIST_TYPES` because that list omits types "such as `literal_interval`". Every literal type in task 2.1 (`literal_exactnumeric`, `literal_double`, `literal_string`, `literal_null`, `literal_bool`, `literal_date`, `literal_timestamp`) is on that list. An implementation that reuses the list passes every row. specs/testing.md § Testing strategy: "A unit test that pins a rule uses an input that fails under the wrong rule."
- Fix: Add a `literal_interval` row to task 2.1, built with `lit("literal_interval", …)`, that expects the key to be removed.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Verification, Scenario Coverage row "An ORDER BY the adapter cannot bound as a top-N remains correctness-safe"
- Issue: The unit test `declined_order_by_renders_every_ordering_in_full` (`pushdown_tests.rs:1203`) carries this scenario's `/// Scenario:` line as the scenario's one proof. The plan adds the E2E test `constant_sort_key_returns_the_native_order` with the same tag and keeps both. specs/testing.md § Coverage rule: "A unit test carries the line only when it is the scenario's one proof."
- Fix: In task 1.1, remove the `/// Scenario:` line from `declined_order_by_renders_every_ordering_in_full` and mark the unit test untagged in the coverage table. If the new E2E test does not prove the whole scenario, drop the tag from the E2E test instead and keep the unit test's line.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks 2.2
- Issue: After the change, `PushdownRequest::json()` no longer returns the request "as Exasol sent it, or as a rewrite kept it", which is its current doc comment. Task 2.2 updates only the struct comment. The planned struct comment, "no holder's `orderBy` carries a constant key", is false for the `FALSE` key of decision [1]. Every `orderBy` reader depends on the rewrite, and the interface comment is the only place a reader learns about it without reading `parse`. Otherwise the design is sound: one constructor owns the rule, and no reader changes.
- Fix: In task 2.2, word the struct invariant as "an `orderBy` holds no constant key that Exasol pushed; an `orderBy` of constants only with a non-zero offset holds one `FALSE` key". Also update the `json()` doc comment to state that `parse` drops constant sort keys.

## Prose Quality

no objection: axis checked. plan.md § Summary leads with the result. Each decision-log entry states its choice, rationale, and alternative. plan.md, decision-log.md, and both deltas contain no em dashes, filler, or promotional wording. The scenario wording defects are filed under Requirement Quality.
