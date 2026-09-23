# Plan Review Findings: add-broadcast-join-topn (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 2, Advisory: 10)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

1. A fact table stores a `NaN` in a `double` sort key. The shard ranks that `NaN` above every number. The `emit_batch` path delivers it to Exasol as NULL (`#246`). A `DESC NULLS LAST` query then returns NULL rows in place of real top values, with no error. Routed to Requirement Quality (stored-NaN blocker).
2. A tester compares broadcast rows with single-node rows on a key with duplicates. The check fails intermittently, because TopK keeps an arbitrary subset of tied rows and the spec promises equal rows. The user asked about ties, and no artifact answers. Routed to Requirement Quality (ties blocker).
3. A BI tool sends `ORDER BY … LIMIT 5000000` over a broadcast join. Each shard's TopK now holds millions of rows and has no spill path. The query fails with `ResourcesExhausted` where the streaming plan succeeds today. Routed to Feasibility (TopK memory advisory).

## Intent Fidelity

No blocker. Open question 1 (a sort key from either side, under aliasing) is answered. Keys must be bare-column projection items, the two sides' column names are disjoint, and `render_broadcast_join` strips `tableAlias` (`#303`). Task 2.4 tests a key list that spans both sides. Open question 2 (ties at the shard boundary) is answered nowhere: a search for "tie" across all plan artifacts finds nothing. Its answer needs no requester judgment, so it is raised under Requirement Quality. The zero-offset scope and the unchanged wrapper rendering match both interview answers.

#### [INTENT_DRIFT] ADVISORY: wrapper scenario rewritten beyond the falsified steps
- Location: `vs-adapter/pushdown-planning-join/spec.md` § Scenario "A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out" (DELTA:CHANGED)
- Issue: The interview answer says "the wrapper's rendering, tests, and spec scenario stay as they are." The new behavior falsifies two recorded steps: "NO post-join cap in its join block" and "every shard SHALL still compute its full local join before that wrapper truncates anything ... `(#309)`". The delta also drops two steps that stay true: "the wrapper SHALL name ONLY the request's visible projection items, so the returned column count and order still equal the `selectList` item count Exasol validates positionally" and "an UNORDERED broadcast request ... SHALL still be emitted as the bare fan-out with no wrapping select". No decision-log entry records why the scenario changed.
- Fix: Restore the two dropped steps in that scenario. Confine the edit to the two falsified steps. Add a decision-log entry that states this limit and cites the interview answer "Merge rendering".

## Feasibility

Checked and confirmed: 15 `JoinSpec {` literals exist (1 production, 14 test), matching task 1.2. Every test and helper the tasks cite exists. Issue `#307` is closed, and its code is on `main` (PR `#326`). `SortExec` plans a `TopK` for every fetch over an unsorted input (`datafusion-physical-plan-54.1.0/src/sorts/sort.rs:1216-1226`). The expected rows of tasks 1.6, 1.7, and 2.8 match the fixtures (`write_orders`, `write_customer`, `order_custkey`, two `fact_orders` files).

#### [NFR_IGNORED] ADVISORY: per-shard TopK memory has no bound and no spill
- Location: `plan.md` § Impact ("**Results.** Unchanged.") and § Dependencies
- Issue: DataFusion 54 uses `TopK` for every `SortExec` with a fetch, with no row-count threshold (`sorts/sort.rs:1224-1226`). `TopK` grows its reservation through `try_resize` and has no spill path (`datafusion-physical-plan-54.1.0/src/topk/mod.rs:306`). Today an ordered broadcast shard streams its join output in constant memory. After this plan, each shard holds up to `min(n, shard join rows)` rows before it emits anything. A large `LIMIT` can then fail with `ResourcesExhausted` where it succeeds today. The flat-scan top-N path has the same exposure, and no artifact states it for either path.
- Fix: Add a memory bullet to `plan.md` § Impact. State the `min(n, shard join rows)` bound and the missing spill path. Name it an accepted trade-off shared with `vs-adapter/pushdown-planning-topn`.

#### [UNSTATED_ASSUMPTION] ADVISORY: the two Exasol ranking facts are verified last, on a weak fixture
- Location: `plan.md` tasks 2.7 and 2.8, § Manual Testing; `decision-log.md` decision [2] Consequences
- Issue: The ranking rule rests on two Exasol-side facts. An emitted `''` arrives as NULL, and Exasol orders VARCHAR in byte order. Decision [2] labels both as unverified. Task 2.8 checks them only after group A is fully built. Its labels (`alpha`, `bravo`, `charlie`) are lowercase ASCII, so a case-insensitive or locale collation passes unnoticed. CLAUDE.md forbids unverified assumptions about Exasol SQL behavior.
- Fix: In task 2.7, seed `Bravo` and `Ärger` in place of `bravo` and `charlie`. Update task 2.8 and § Manual Testing to expect the `Bravo` rows of orders 4 and 9. Move task 2.7 and the `VS_NAME_LOW` half of task 2.8 ahead of group A. Record that live result in decision [2].

## Requirement Quality

#### [COMPLETENESS_GAP] [REQUIREMENT_CONFLICT] BLOCKER: a stored NaN sort key ranks differently on the shard and in Exasol
- Location: `datafusion-scan/scan-execution-join/spec.md` § Background (bullet "A per-shard post-join top-N ranks each key by the value the shard EMITS") and § Scenario "A join shard ranks each sort key by the value it emits"; `plan.md` § Architecture (emitted-value target paragraph) and task 1.4; `decision-log.md` decision [2] Consequences ("A float NaN key is an untested area")
- Issue: The scenario requires that "the UDF SHALL rank the rows exactly as the Exasol-side wrapper ranks the emitted rows." The design adjusts only string keys. The recorded `sql-comprehension/vs-expression-translator-float-div` spec states that the `emit_batch` path returns a silent NULL for a `NaN`. It tracks every stored-NaN route as `#246`, which is open. The join scan emits through that same path (`emit_stream` calls `ctx.emit_batch`). DataFusion ranks a positive `NaN` above every number. Under `DESC NULLS LAST`, a shard keeps its `NaN` rows and cuts real top values, which Exasol then ranks after NULL. Under `ASC NULLS FIRST`, a shard cuts a `NaN` row that Exasol ranks among the leading NULLs. Both return wrong rows with no error. The recorded library already settles the emitted value, so "untested area" understates a known divergence. The spec delta is silent about it, and CLAUDE.md forbids a silent Exasol-type divergence.
- Fix: For a `Float32` or `Float64` key, rank `CASE WHEN isnan(<expr>) THEN NULL ELSE <expr> END` as the emitted-value target. State this rule in `plan.md` § Architecture and in task 1.4. Extend the Background ranking bullet: an emitted `NaN` arrives as NULL `(#246)`. Add a scenario AND step: a `NaN` float key SHALL rank as NULL. Replace decision [2]'s "untested area" bullet with this rule. Add `build_join_sql_ranks_a_nan_float_key_as_null` to task 1.5. Use a `Float64` key with values `NaN` and `1.0`, `DESC NULLS LAST`, and a cap of 1. Assert that the one emitted row is `1.0`. Add the test to § Scenario Coverage.
- Escalation: MECHANICAL. The recorded float-div spec and decision [2]'s own rule ("rank the value the merge sees") settle the fix without a requester judgment.

#### [COMPLETENESS_GAP] [AMBIGUOUS_REQUIREMENT] BLOCKER: ties at the shard cut are unaddressed, and the result requirement is untestable under ties
- Location: `vs-adapter/pushdown-planning-join/spec.md` § Background (bullet "A zero-offset `ORDER BY … LIMIT n` additionally bounds each shard ...") and § Scenario "A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N" (last AND step); `datafusion-scan/scan-execution-join/spec.md` § Scenario "A post-join ordering and cap bound each join shard to its own top-N over the joined output" (THEN step); `decision-log.md` § Design Decisions
- Issue: The user asked whether a DataFusion TopK "need[s] special handling for ties at the shard boundary." No artifact answers. The Background proof says "a row in the global top-n is outranked by fewer than `n` rows across all shards ... so it survives that shard's cut." That is false for a row tied at the cut, because TopK keeps an arbitrary subset of tied rows. The THEN "the returned result SHALL equal the same `ORDER BY … LIMIT n` evaluated over the same inner join on a single node" has no pass/fail reading when ties straddle the cut. The scan THEN "they SHALL be the `n` top-ranked joined rows" has the same defect. The recorded bare-LIMIT scenario already uses a testable form: "equal to some valid single-node `LIMIT n` answer".
- Fix: Rewrite the Background proof for ties. State that every row ranked strictly ahead of the global n-th key survives its shard's cut. State that each shard keeps at least as many rows tied at that key as the global answer needs. Conclude that TopK needs no tie handling. Reword the planner THEN: the result SHALL carry the same sort-key values, in the same order, as single-node evaluation. Reword the scan THEN: no omitted joined row SHALL outrank an emitted row. Add a decision-log entry that answers the open question. Add a case to task 2.8: `ORDER BY b.B_LABEL ASC NULLS LAST LIMIT 3`, whose third row is one of two tied rows from different fact files. Assert the label sequence, and assert that the third row's `O_ORDERKEY` is one of the two candidates.
- Escalation: MECHANICAL. The tie argument follows from the ranking definition, and the recorded bare-LIMIT scenario supplies the wording. No requester judgment is needed.

#### [COMPLETENESS_GAP] ADVISORY: the sort-side guard is uncited, and its shape is untested
- Location: `datafusion-scan/scan-execution-join/spec.md` § Background (bullet "The limit is POST-join by construction at two independent levels"); `plan.md` task 1.6
- Issue: The ordering scenario forbids "a sort or a fetch below the join node on either input." The Background cites evidence only for the fetch (`push_down_limit.rs`). DataFusion's `handle_hash_join` pushes a probe-side-only sort requirement below a `HashJoinExec` (`datafusion-physical-optimizer-54.1.0/src/enforce_sorting/sort_pushdown.rs:776`). Only the fetch guard at `sort_pushdown.rs:227-253` blocks that pushdown when the sort carries a fetch. Task 1.6 orders by one key from each side, a shape `handle_hash_join` never pushes. The fact-side-only shape that the guard protects stays untested. Task 1.6 also has no `WHERE`, so the placement after the filter stays untested.
- Fix: Cite `sort_pushdown.rs:227-253` in that Background bullet as the sort-side guard. In task 1.6, add a case that orders by `O_ORDERKEY` alone and carries a filter. Assert `TopK(fetch=` and `has_no_sort_below` for that case.

#### [COMPLETENESS_GAP] ADVISORY: an ordering without a cap is unspecified on the scan
- Location: `plan.md` task 1.4; `datafusion-scan/scan-execution-join/spec.md` § Scenarios
- Issue: Task 1.4 renders ` ORDER BY` whenever `post_join_order_by` is non-empty, with or without a cap. The adapter never sends an ordering without a cap. No scenario states what the scan does with one. A full per-shard sort would materialize every joined row. That conflicts with the recorded step "never materializing the entire joined result set" in "Join projection, filter, and LIMIT are applied and rows streamed as Arrow IPC".
- Fix: In task 1.4, render the ordering only when `post_join_limit` is also set. Add an AND step to the ordering scenario: an ordering without a cap SHALL render no `ORDER BY`.

## Task Breakdown

No blocker. Every scenario maps to a task and a named test. Groups A and B share one file (the production `JoinSpec` literal) and run in sequence. The `[expert]` tags sit on the three tasks that carry the wire contract, the ranking rule, and the plan-shape proof.

#### [TRACEABILITY_GAP] ADVISORY: statements this plan falsifies stay unchanged
- Location: `plan.md` task 2.9 and § Features ("Checked and left unchanged"); `decision-log.md` decision [7]
- Issue: Task 2.9 updates only `docs/debugging-pushdown.md`. Four other statements become false after this plan. `docs/capabilities.md:35-36` says the per-shard top-N "needs a single table (no join, no `GROUP BY`)". `docs/capabilities.md:61-63` lists "a join" as a shape the adapter cannot bound, and the table row at `:115` repeats it. `docs/architecture.md:77` limits the bounded top-N to "single table". In addition, `vs-adapter/pushdown-planning-join-fallback` Background keeps the inline citation `(#309)`. After `#309` closes, that citation reads as an open tracked exception.
- Fix: Extend task 2.9 to update `docs/capabilities.md` lines 35-36, 61-63, and 115, and `docs/architecture.md:77`. Name the zero-offset broadcast join as a bounded shape in each. Add a `vs-adapter/pushdown-planning-join-fallback` delta that removes `(#309)`, or justify the closed citation in decision [7].

## Design Depth

No blocker. Decision [2] is the only `Promotes to ADR: yes` entry. It fixes a module-ownership rule whose violation returns wrong rows silently, so it passes the promotion gate and the spec-conciseness Rule 2. Decisions [1] and [3] to [7] are correctly `no`. `bound_sort_key` is deeper than `binds_to_projection`, because the admission check also yields the key it admits.

#### [TACTICAL_SHORTCUT] ADVISORY: the flat-scan follow-up has no issue
- Location: `decision-log.md` decision [6]; `plan.md` § Quick Diagnostic (row "Tactical shortcut with follow-up")
- Issue: Decision [6] identifies the same wrong-rows divergence in the flat-scan top-N path. An empty-string key and a nested key hidden by the tag collapse both diverge there. The follow-up is only "recommended", and no issue number exists. Nothing schedules the strategic fix. CLAUDE.md tracks new work as GitHub issues.
- Fix: Ask the orchestrator to open the GitHub issue before implementation. Cite its number in decision [6] and in the Quick Diagnostic row.

#### [INFORMATION_LEAKAGE] ADVISORY: the emitted-value target gets a third private copy
- Location: `plan.md` task 1.4 ("built by a private helper beside `render_join_select_item`"); `decision-log.md` decision [2] Consequences (last bullet)
- Issue: Two modules already decide what a shard emits for a column: `render_join_select_item` in `join_scan.rs` and the select-item rule in `build_scan_sql` (`raw_scan.rs:545-555`). The ADR in decision [2] says every future per-shard sort ranks the emitted value, including the flat-scan fix. A ranking helper private to `join_scan.rs` means that fix adds a third copy of the same knowledge.
- Fix: In task 1.4, define the emitted-value target once, beside the shared select-item rule in the `scan` module. Make it reachable from `raw_scan.rs`. Name that module as the owner in decision [2].

## Prose Quality

`plan.md` and `decision-log.md` prose contains no em dashes outside tables, no contractions, and no weak modals outside quoted sources.

#### [PROSE_UNCLEAR] ADVISORY: one THEN step carries five requirements
- Location: `vs-adapter/pushdown-planning-join/spec.md` § Scenario "A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out" (THEN step)
- Issue: The THEN step is one sentence of about 100 words. It joins five requirements: broadcast serving, no fallback, one wrapper, the window seam, and the select-list and key seams. The recorded scenario states them as separate steps. One step with five requirements has no single pass/fail reading.
- Fix: Split that THEN into one step per requirement. If the validator then warns on the AND count, move the shared-seam definition into Background and cite it.

#### [PROSE_BLOAT] ADVISORY: residual history in a rewritten Background bullet
- Location: `datafusion-scan/scan-execution-join/spec.md` § Background (ADLS different-container bullet)
- Issue: Decision [7] says the touched Background blocks drop the "superseded" and "withdrawn" narrative. The ADLS bullet still ends with history: "the plan-time backend comparison that once claimed to cover it compared ADLS `account_name`, which is identical for two containers of one account, so it never fired here — it is deleted". A reader of the permanent spec needs only the current guard.
- Fix: Cut that clause. Keep the `vs-adapter/pushdown-planning-cloud-credentials` pointer on the preceding sentence.
