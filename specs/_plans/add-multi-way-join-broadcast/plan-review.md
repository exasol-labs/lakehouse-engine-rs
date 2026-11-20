# Plan Review Findings: add-multi-way-join-broadcast

## Summary
- Axes checked: 4/4
- Total findings: 17 (Blockers: 6, Findings: 11)
- Intent Fidelity blockers: 0

## Intent Fidelity

No intent drift or scope creep. Axis checked against #673 and the interview: hybrid with one pair (Q1), unfiltered dimension still eligible (Q2, join delta "pair choice" last AND step), width rule when another leg joins (Q3, join Background), manual bench run (Q4), declined conjunct keeps the broadcast for two legs (Q5, declined-filter delta), expression sort out of scope (Q6, #675). The two-leg declined-conjunct change goes against the issue's "Two-table joins don't change", but the interview agreed to it in Q5.

#### [SCOPE_REDUCTION] FINDINGS
- Location: plan.md § Verification › Manual Testing, row "Benchmark (#673 criterion)"
- Issue: #673 acceptance reads "A benchmark run on a multi-table query shows the change in rows sent to Exasol and in run time." The expected output is "show a join block in their pushdown SQL on the branch, and their run times side by side". Nothing measures the rows sent to Exasol.
- Fix: In that row's Expected Output, add a row-count measurement that needs no bench change, such as running each query's pair fan-out SQL (taken from `EXPLAIN VIRTUAL`) with `SELECT COUNT(*)` on `main` and on the branch, or reading the fan-out's row count from Exasol's profiling (`EXA_USER_PROFILE_LAST_DAY`), and report both counts beside the run times.

#### [SCOPE_REDUCTION] FINDINGS
- Location: plan.md § Design, item 1 (Grouping)
- Issue: #673 acceptance reads "The policy names a reason for every join it does not broadcast." The plan gives "an `NScanReason` per candidate pair not broadcast". A join whose condition holds no key that reads exactly two legs (`c.k + o.k = l.k`) is no candidate and gets no reason, and a render decline (D2) has no reason either.
- Fix: In Design item 1, add one reason for a join with no two-leg join key (for example `NoPairKey`), and state that the render fall-through of D2 records its own reason, or add a Directions entry that narrows the criterion to candidate pairs and say so in the PR body.

#### [INTENT_GAP] FINDINGS
- Location: intent.md § Directions; existing spec `pushdown/pushdown-planning-selectlist-expressions` ("on the broadcast-join path a wrapped projection of a request without Exasol postprocessing SHALL be a plain reason the broadcast plan is unavailable")
- Issue: In a join of three or more legs Exasol renders the select list over the fan-outs (join delta, "A join of three or more legs…", emitted columns step). The plan does not say whether a select-list item the scan cannot compute still keeps a hybrid off the broadcast, as the unchanged select-list spec requires for any broadcast-join path, or whether it is rendered by Exasol and the pair stays broadcast.
- Fix: Add a direction that answers it (likely: in a plan with more than one fan-out, or with a residual, the select list renders in Exasol, so a wrapped projection does not decline the pair), and add a `DELTA:CHANGED` block for the select-list scenario in `pushdown/pushdown-planning-selectlist-expressions` that scopes its broadcast decline to the plan where the select list runs in the shards.

## Feasibility

No NFR objection. Axis checked: memory stays bounded by `JOIN_BROADCAST_MAX_BYTES` per dimension and by the width rule on the emitted dimension columns; the scan UDF and `JoinSpec` do not change; no CONNECTION, property, or credential path changes; no migration because no stored state or property changes.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Dead Code Removal (assertion rows)
- Issue: `crates/lakehouse-e2e/tests/e2e_local/join.rs` `e2e_n_scan_declined_side_local_conjunct_applied_in_outer_where` runs a three-table join on `VS_NAME` and asserts `has_n_scan_wrapper(&pushed, 3)` and `!has_broadcast_join_block(&pushed)`. The fixtures are under the threshold, so after this change the test fails. The plan's list of assertions to move to `VS_NAME_LOW` omits it.
- Fix: Add `e2e_n_scan_declined_side_local_conjunct_applied_in_outer_where` to the Assertion row that moves N-scan shape assertions to `VS_NAME_LOW`, and grep `join.rs` for every other `has_n_scan_wrapper(&pushed, 3|4)` and `PathCheck::NScan(3)` on `VS_NAME` before finalizing that list.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Design, "Docs and mission" bullet for `docs/query-guide.md`
- Issue: The plan replaces only the "Only joins of two tables are broadcast joins" section and the line-35 row. The Joins section's eligibility list also says "It is an inner join (`JOIN`) of exactly two tables" and "The engine can run the filters.", which this change makes false (a declined filter keeps the broadcast), and its width bullet is scoped to "With an aggregate, `GROUP BY`, or `HAVING`", which no longer holds for three or more tables. Line 19 ("joins of two tables") also needs a check.
- Fix: Extend the `docs/query-guide.md` bullet to name the Joins section's eligibility list (the two-table bullet, the "can run the filters" bullet, and the width bullet's aggregate-only scope) and line 19.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: intent.md § Directions D3; plan.md § Design item 4
- Issue: D3 fixes the pair fan-out's alias (`LHS_T{fact}`) but not its position in Exasol's `INNER JOIN … ON` chain: at its fact leg's FROM position, at its first leg's, or first. Greedy attach renders `ON 1=1` where no condition is in scope yet, so the position decides whether Exasol is handed an intermediate cross product before the next join point.
- Fix: Add a direction that names the position rule (for example, the pair's fan-out takes the position of its lowest leg, so its conditions are in scope no later than in the N-scan) and the reason in one line.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: join delta `pushdown-joins/pushdown-planning-join/spec.md` § Background (CHANGED block); same delta § "A two-leg join with filters in its condition…" step 2; existing specs `pushdown-joins/pushdown-planning-join-ordering` Background line 1, `pushdown-joins/pushdown-planning-join-aggregation` Background and scenarios 1 and 3, `pushdown-joins/pushdown-planning-join-fallback-self-join` ("fails the broadcast contract"), `pushdown-joins/pushdown-planning-join` "Legs that share column names…" ("meets the broadcast contract"), fallback delta scenario title "A join outside the broadcast contract is declined safely"
- Issue: The CHANGED Background removes the only definition of "the broadcast contract" ("exactly two join legs, `join_type = "inner"`, … a condition, filter, and projection that `crates/vs-expression` can render") and replaces it with "broadcast pair" and "eligible". The term stays in this plan's own changed scenario ("the request meets the rest of the broadcast contract") and in at least five unchanged scenarios across four specs, which now cite an undefined rule. A test author cannot tell whether a three-leg hybrid "meets the broadcast contract".
- Fix: In the join delta Background, either keep one line that defines "the broadcast contract" in the new terms (an all-inner join tree with an eligible pair) or replace every use of the term: in the join delta's changed scenario, and in `DELTA:CHANGED` blocks for each listed scenario and Background line of the ordering, aggregation, self-join, and join specs.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: existing spec `pushdown-joins/pushdown-planning-join-ordering`: description ("The broadcast path … bounds each fact shard to its own post-join top-N or cap"), Background line 3, and the scenarios "A pushed ordering over a broadcast-eligible join is served by an outer wrapper…", "A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard…", "Ordering and window shapes the broadcast wrapper cannot serve decline to the unified fallback"; existing spec `pushdown-joins/pushdown-planning-join-aggregation` scenario 1 ("projecting only the columns that the select list, `GROUP BY`, `HAVING`, and `ORDER BY` read")
- Issue: These unchanged scenarios still apply to any "broadcast-eligible" request and contradict the plan:
  - "an UNORDERED broadcast request … SHALL still be emitted as the bare fan-out with no wrapping select" conflicts with the residual `WHERE` over a two-leg fan-out (declined-filter delta) and with every hybrid.
  - The zero-offset scenario requires a post-join ordering and cap in the join block. The ordering delta and the declined-filter delta forbid both when a join or a filter conjunct is left for Exasol.
  - The unservable-window scenario sends a "broadcast-eligible inner equi-join" with an expression sort key to the N-scan. plan.md § Design 3 keeps the N-scan only for "A two-leg `JoinWindowPlan::BroadcastUnservable`", so a hybrid keeps its pair.
  - The aggregation scenario's "projecting only" conflicts with the declined-filter delta's "the pair's fan-out SHALL emit every column a declined conjunct reads".
- Fix: Add `DELTA:CHANGED` blocks to `pushdown-planning-join-ordering` (description, Background line 3, and the three scenarios) and to `pushdown-planning-join-aggregation` scenario 1, scoping each per-shard or bare-fan-out rule to a two-leg broadcast that leaves no filter conjunct for Exasol. State that a residual or a hybrid takes the outer wrapper with the window in Exasol, and that the emitted columns include those the residual `WHERE` reads.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: join delta § Background bullet 2 ("A pair is eligible when … the scan renders its join keys and the columns it emits"); text-conversion delta § Background bullet 4 ("a join key keeps its pair off the broadcast"); fallback delta § "A join outside the broadcast contract…" WHEN step ("no pair … is eligible: … or the scan cannot render the chosen pair's join keys"); versus intent.md D2 and join delta § "A pair that is not eligible…" step 4 ("when the scan cannot render the chosen pair's join keys … every leg SHALL keep its own fan-out")
- Issue: The Background makes renderability part of eligibility, so a pair whose keys the scan cannot render is not eligible and the next-ranked pair is broadcast. D2 and the scenario step say the chosen pair is picked first and a render decline keeps every leg plain, "and no other pair is tried". In a three-leg join whose top-ranked pair has a date-to-text key and whose second pair is eligible, the deltas give two answers.
- Fix: Follow D2. Remove "the scan renders its join keys and the columns it emits" from the eligibility bullet, reword the text-conversion bullet to "a join key the scan declines keeps every leg of the join on its own fan-out", and in the fallback WHEN step separate "no pair is eligible" from "the scan cannot render the chosen pair".

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: join delta § "Broadcast join projection and filter are rendered per involved table" step 4 ("every other conjunct SHALL be applied once in Exasol's `WHERE` over the fan-outs"); fallback delta § Background bullet 5 ("A top-level WHERE conjunct reaches a leg only if it is local to that leg … Every other conjunct is residual"); versus join delta § "A join of three or more legs…" step 2 ("every other leg SHALL keep its own fan-out, carrying its leg-local filter")
- Issue: In a hybrid, a conjunct local to a non-pair leg is not a pair conjunct, so step 4 sends it to Exasol's `WHERE`, while the three-leg scenario sends it to that leg's scan. Conversely, the fallback Background makes a conjunct across the pair's two legs residual, while the join delta carries it in the pair's scan spec. Each conjunct has two required placements.
- Fix: Reword step 4 to "every conjunct that reads legs of more than one fan-out, or that the scan declines, SHALL be applied once in Exasol's `WHERE`", and reword the fallback Background bullet to "A top-level WHERE conjunct reaches a fan-out only if every leg it reads lies in that fan-out and the scan renders it. Every other conjunct is residual."

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: join delta § Background bullet 2 ("every dimension column it emits is width-bounded") and § "A join of three or more legs…" step 4 ("one join-key column of its fact leg when they read none"); plan.md § Design 1 ("The width rule reads the dimension columns the pair emits"); plan.md § Impact ("every other two-leg join, produce the same SQL as before"); existing spec `pushdown-joins/pushdown-planning-join-aggregation` scenario 1 ("one join-key column of the first leg when they read none")
- Issue: When a request reads no column, the fan-out emits one placeholder join key from the first leg. Today the width rule judges only the columns the aggregation reads, and `sql_builders_tests.rs` `aggregating_broadcast_keeps_a_column_free_aggregation_over_a_text_keyed_leading_dimension` proves that `SELECT COUNT(*)` over a dimension first leg with a `VARCHAR(100)` key stays broadcast. Judging "every dimension column it emits" counts that placeholder, so the request switches to the N-scan, a two-leg change the plan says does not happen. The delta also names the placeholder's leg "fact leg", while the recorded spec and the code use "first leg", which changes two-leg SQL when the first leg is the dimension.
- Fix: In the eligibility bullet and Design 1, define the width rule over the dimension columns the request reads (select list, other join conditions, Exasol's `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`), never the placeholder. In the three-leg scenario, change "its fact leg" to "its first leg" to match the aggregation spec, or change both and drop the byte-identical claim for that case.

#### [IMPLEMENTATION_DETAIL] BLOCKER
- Location: fallback delta § description ("One renderer serves every inner equi-join"), § Background bullets 1 ("not a second renderer") and 4 ("(`GroupByWrapper`)", "All four share one `ORDER BY` and window rendering site"), § "A join outside the broadcast contract…" step 2 ("SINGLE unified unaccelerated per-table-scan fallback"), § "A three-or-more-table inner join…" steps 2 ("the SAME single unified fallback renderer") and 7 ("in the planning layer"); join delta § Background bullet 5 ("owns the binding"), § "Broadcast join projection and filter…" steps 3 ("the scan translator … keyed by (relation alias, column)") and 6 ("BEFORE the scan translator renders it, replacing Exasol's native `tableAlias`")
- Issue: `.claude/CLAUDE.md` § Planning: a delta that changes a scenario removes the implementation detail in that scenario, and internal structure ("one code path", which component computes a value, the order of internal steps) and Rust type names never go into a spec. These lines sit inside `DELTA:CHANGED` blocks and were copied from the recorded specs.
- Fix: In each listed CHANGED block, restate the line by its observable output: the SQL shape (`INNER JOIN … ON` chain, `LHS_T{leg}` aliases and qualified column references in the pushdown SQL), the scan spec contents, and the rows returned. Drop "one renderer", "same single renderer", "rendering site", `GroupByWrapper`, "planning layer", "owns", and "BEFORE the scan translator renders". Move any design point worth keeping to plan.md.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: join delta § "Broadcast join projection and filter are rendered per involved table" step 2 ("the scan-driving SQL's declared EMITS column list SHALL match the projected join output columns in order and type")
- Issue: A two-leg broadcast with a declined conjunct, or with its `ORDER BY` moved to Exasol, must emit columns that the select list does not project: the declined-filter delta says "the pair's fan-out SHALL emit every column a declined conjunct reads". Read literally, step 2 forbids those columns.
- Fix: Reword step 2 to "SHALL list each column the fan-out emits, in order, with its leg's type: the projected columns and every column that Exasol's `WHERE`, aggregation, or `ORDER BY` reads over the fan-out".

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: join delta § "The broadcast pair is the eligible pair whose dimension has a filter of its own" step 1 ("its legs come first in FROM-tree order"); § Background bullet 1 ("Its smaller leg by bytes after its own pruning is the dimension")
- Issue: The last tie-break does not say which leg index is compared (the lower leg of each pair, the fact leg, the dimension leg), and nothing states which leg of a pair is the dimension on an exact byte tie in a join of three or more legs. The recorded tie rule ("Small-side selection…") covers two legs only.
- Fix: State the tie-break as "the pair whose lower leg index is smaller, then whose higher leg index is smaller", and extend the Background bullet with "on an exact byte tie the first leg in FROM-tree order is the dimension", matching the two-leg rule.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Verification › Scenario Coverage, row "A pair that is not eligible is not broadcast"
- Issue: The mapped test covers the over-threshold case and the wide text column. The scenario's step 4 (the scan cannot render the chosen pair's join keys or emitted columns, or an output name over 128 characters, in a join of three or more legs) has no proof. The date-to-text test proves it only for two legs.
- Fix: Add a case to `e2e_multi_way_join_without_eligible_pair_keeps_the_n_scan` (or a unit test in `joins_tests.rs`) with a three-leg join whose top-ranked pair joins on `TO_CHAR(<date>) = <text>`, asserting three plain fan-outs and native-equal rows.

#### [BACKGROUND_DRIFT] FINDINGS
- Location: join delta § Background bullet 2 (eligibility rule); text-conversion delta § Background bullet 4 (the clause "a join filter runs in Exasol's `WHERE` over the broadcast fan-out, and a join key keeps its pair off the broadcast")
- Issue: Both bullets encode behavior: the eligibility rule decides the plan, and the text-conversion clause restates the new scenario "A join filter that converts a date to text runs in Exasol…". `/speq:plan` Short Background: behavior belongs in a scenario step.
- Fix: Move the eligibility conditions into the THEN steps of "A pair that is not eligible is not broadcast" and keep only a one-line definition of "eligible pair" in the Background. Cut the text-conversion clause from the Background, because the new scenario already states it.

## Design Depth

No `ADR: yes` direction, so `[ADR_OVERPROMOTION]` does not apply: D5 records the plan model as a constraint, which fits rule 3. No `[ARCHITECTURE_DRIFT]`: the delta's BASE equals `git hash-object specs/architecture.md` (`1ed0b16e…`), the CHANGED Constraints block copies the current section with one added bullet, and the four decisions and the fan-out model that plan.md states are in that bullet. No `[ADR_CONFLICT]`: checked against `screen-renderability-at-the-render-consumer-not-in-the-partition-classifier` (D4 keeps the partition structural and the screen at the renderer), `n-scan-join-fallback-renders-inner-join-on-with-greedy-attach…` (generalized to fan-outs), `join-post-limit-lives-in-join-block` (JoinSpec unchanged), `split-fan-out-is-the-sole-scan-path…`, and `no-exasol-side-fallback-for-an-advertised-capability` (the declined conjunct moves to Exasol's `WHERE`).

#### [DESIGN_VIOLATION] FINDINGS
- Location: plan.md § Design 1 (input "whether it has a leg-local conjunct the scan renders"); intent.md D2 ("one fall-through keeps the policy free of rendering")
- Issue: Information hiding. The grouping decision runs before condition placement (`plan_join` "runs the four in order"), yet its ranking input needs the scan translator's verdict on each leg-local conjunct. The renderability screen then runs in two places, the policy's input and the renderer, so a change to the screen must be made in both. D2's claim that the policy is free of rendering is not true of the plan as written.
- Fix: In Design 1, name one owner for the per-conjunct renderability verdict, computed once before grouping and read by both the ranking and the renderer's split (the accepted screen ADR allows the screen at its consumer, and both are consumers of one verdict). Or rank on "has a leg-local conjunct" without the renderability test, and add one line to D2 saying which was chosen.
