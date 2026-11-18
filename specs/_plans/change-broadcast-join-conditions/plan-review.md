# Plan Review Findings: change-broadcast-join-conditions

## Summary
- Axes checked: 4/4
- Total findings: 10 (Blockers: 2, Findings: 8)
- Intent Fidelity blockers: 0

## Intent Fidelity

The plan covers every part of #664 and the intent: composite keys broadcast (pushdown-planning-join "A two-leg join on a composite key…"), ON filters move to WHERE and prune and reach leg scans (condition-filters scenarios 1 and 2), and the outer-join guard (condition-filters scenario 4). The broadcast of expression keys, the path-flag guard, and shipping despite the weaker pruning are interview answers and are not re-litigated. The issue's acceptance criteria (spike shapes, 3-column key, duplicate composite keys, three-table leg filters, the unit-test list) all map to rows in plan.md § Scenario Coverage. `docs/query-guide.md`, which the issue names, does not exist in `docs/`.

#### [SCOPE_CREEP] FINDINGS
- Location: `pushdown-joins/pushdown-planning-join/spec.md` § Background (DELTA:CHANGED)
- Issue: The CHANGED Background silently deletes the recorded bullet "The dimension side rides in the shard-invariant common spec. Only the fact side's per-shard file subset flows through the `LAKEHOUSE_DISTRIBUTE_FILES` distributor." Neither #664 nor plan.md nor any direction asks for this removal, and plan.md § Features lists the change only as the broadcast contract rewrite.
- Fix: In the delta's Background, restore that bullet verbatim. If the removal is wanted under the Short Background rule, add a direction to intent.md § Directions that names the bullet and why no scenario step depends on it.

## Feasibility

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Requirements, rows "The move runs before join planning" and "Key legs"
- Issue: The key test goes "through `JoinLegs::conjunct_leg`", which calls `JoinLegs::leg`, and `leg` panics on a column that `ensure_columns_declared` would reject ("Panics on a column that [`Self::ensure_columns_declared`] would have rejected", `joins/attribution.rs`). The spike runs the move at the top of `plan_join`, before `ensure_columns_declared`. If the plan keeps that order, a column that binds no leg crashes the adapter VM instead of failing with the error that pushdown-planning-join "Legs that share column names…" requires ("SHALL fail the request before either path renders").
- Fix: In plan.md § Requirements, row "The move runs before join planning", state that `plan_join` runs `ensure_columns_declared` on the request before the move, and that the move reads leg identity only after that check.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: `pushdown-joins/pushdown-planning-join-condition-filters/spec.md` § "A join condition without a join key is left unchanged"; intent.md [D2]; plan.md § Scenario Coverage row for `e2e_three_table_join_condition_without_a_join_key_matches_native`
- Issue: D2 claims "Exasol's own key rule lets one side read two legs in a three-leg tree", so Exasol pushes `ON a.k + b.k = c.k AND c.r = 'x'`. Nothing in the plan says this shape was observed being pushed, and AGENTS.md requires pushdown reachability to be verified live. Also, moving `c.r = 'x'` would return the same rows, so a rows-only test (the test name says `matches_native`) cannot fail when the condition is wrongly split (testing.md § Correctness oracles: "When a declined filter would return the same rows as a pushed one, the test also asserts the SQL").
- Fix: In intent.md [D2], state whether the three-leg shape was seen pushed on a live Exasol. In plan.md § Scenario Coverage, state that the test first asserts the query is pushed as a three-leg N-scan, then asserts from `EXPLAIN VIRTUAL` that `c.r = 'x'` stays in the chain's `ON` and is absent from leg `c`'s scan filter, and only then compares rows.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: `pushdown-planning-join-condition-filters/spec.md` § "The filters of a join on an outer join's null-supplying side stay in its condition", last step; plan.md § Dependencies
- Issue: The scenario moves the filters of "an inner join above a right outer join". A moved conjunct can read the right outer join's null-supplying leg (`(A RIGHT JOIN B …) JOIN C ON k AND p(A)`). Today's leg attribution then pushes `p(A)` into A's scan and drops it from the residual WHERE, which returns wrong rows once outer joins are advertised. The move is correct only together with #663's rule that a WHERE conjunct on a null-supplying leg never reaches that leg's scan. The plan claims the path flag is "the guard" that keeps the move correct, and lists no such dependency.
- Fix: In plan.md § Dependencies, add that the move's correctness under outer joins also requires #663's WHERE placement for null-supplying legs, so #663 treats moved conjuncts exactly as user-written WHERE conjuncts. Add the same fact to the outer-join guard row of § Requirements.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Context
- Issue: AGENTS.md § Workflow requires a plan that touches pushdown to be checked against the governing Iceberg table spec or Delta Lake protocol, quoting the normative section. The plan now feeds ON conjuncts into Iceberg manifest pruning, Delta file skipping, and Hive partition pruning, and quotes no section and states no check.
- Fix: Add one Context line stating that moved conjuncts reach file pruning through the existing WHERE pruning path, quote the Iceberg spec's Scan Planning rule that pruning uses only an inclusive projection of the filter (and the Delta protocol's data-skipping section), and state that the plan adds no deviation.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `pushdown-joins/pushdown-planning-join-fallback/spec.md` § "A join outside the broadcast contract is declined safely", new AND step
- Issue: The step says "a conjunct of the join condition beside its join keys SHALL by itself NEVER keep the request off the broadcast path". The same plan's `pushdown-planning-join/spec.md` § "A two-leg join with filters in its condition…" says "a moved conjunct that the scan translator declines SHALL take the unified unaccelerated fallback", and plan.md § Scenario Coverage adds "a declined `ON` filter that takes the N-scan". A declined ON conjunct is exactly a conjunct beside the keys that keeps the request off the broadcast path, so both requirements cannot hold literally.
- Fix: Reword the fallback step to: "a conjunct of the join condition beside its join keys SHALL keep the request off the broadcast path only where the same conjunct written in WHERE would, because the adapter plans it as a WHERE conjunct (`pushdown-joins/pushdown-planning-join-condition-filters`)".

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: `scan-aggregation/scan-execution-join/spec.md` § Background bullet 5; intent.md [D5]
- Issue: The Background keeps "Every scan table sets it to the hash join's IN-list pushdown limit, so the whole pushed key set prunes." The changed scenario in the same feature states the opposite for composite keys: the fact scan "MAY read a row group … because DataFusion delivers that key set as one `struct(...) IN (...)` filter, which Parquet statistics cannot evaluate". D5's reason, that rewriting the Background is out of scope, leaves the recorded spec contradicting itself. The scenario's "at most 150 distinct join keys" also does not say whether a composite key counts tuples or per-column values.
- Fix: Add a DELTA:CHANGED Background block to the scan delta that copies the current Background and changes only bullet 5's ending to "so the whole pushed key set of a one-column key prunes". Drop D5. In the scenario's first THEN step, write "at most 150 distinct join-key values, or key tuples for a composite key".

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Scenario Coverage, row "The fact scan skips row groups that no dimension join key matches"; § Checklist "Test" row; `pushdown-planning-join/spec.md` § "A two-leg join with filters in its condition…", self-join pruning step
- Issue: The changed scan scenario adds two observable claims: with several keys the fact scan skips every row group outside any one key column's bounds, and it may read others. The only mapped test is an E2E that "asserts the broadcast block and the seeded rows", which cannot observe row groups. The existing host test `selective_join_prunes_fact_row_groups_clustered_by_the_join_key` (`crates/lakehouse-engine/tests/it/scan_join_test.rs`) is the one that counts pruned row groups, and the plan does not extend it. The spike's measurement test (`scan/spike_composite_tests.rs`) is not carried over. Likewise "a moved conjunct on one occurrence of a self-join SHALL prune only that occurrence's files" names no file-list oracle or multi-file fixture (testing.md § Correctness oracles: discriminating fixture). The checklist runs `cargo test --workspace --lib`, which skips `tests/it`.
- Fix: In § Scenario Coverage, add a composite-key row to the host test's case table, asserting the rows and the pruned row-group count that per-column bounds give, and asserting the plan's `HashJoinExec` stays `CollectLeft` for a composite key and for an expression key (ADR `broadcast-join-builds-one-hash-table`). For the self-join pruning step, name a multi-file fixture and an `EXPLAIN VIRTUAL` assertion that the filtered occurrence's file list is the subset the same filter in WHERE yields. Change the checklist's Test command to `cargo test --workspace`.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: `architecture.md` § Constraints, new bullet
- Issue: The constraint says the adapter moves every non-key conjunct "except in a join on an outer join's null-supplying side, so every join planning path reads a join's filters from the top-level filter alone". The condition-filters scenario "A join condition without a join key is left unchanged" (D2) keeps a keyless join's filters, such as `c.r = 'x'`, in its condition, so "alone" is false for that case too.
- Fix: In the architecture delta bullet, add the second exception: "and except in a join whose condition holds no join key".

## Design Depth

D1 gives the join-key test and the move one owner, and the broadcast check asks it instead of testing node types. That passes the information-hiding check. The architecture delta's BASE matches `git hash-object specs/architecture.md` (13fa3f44), it copies the whole Constraints section, and it adds one bullet. No direction carries `ADR: yes`, so there is no ADR over-promotion.

#### [ADR_CONFLICT] BLOCKER
- Location: architecture.md § Constraints, new bullet; `pushdown-planning-join-condition-filters/spec.md` § "An ON filter on one leg of a join of three or more legs reaches that leg's scan"; ADR `per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution` (`specs/_decision/001-migrate-legacy-decision-log.md`)
- Issue: The accepted ADR decides: "Cross-table conjuncts, the join condition, and any OR spanning both tables stay in the outer WHERE, which remains the correctness backstop." The plan moves the non-key conjuncts of the join condition into the top-level filter and then pushes the single-leg ones into a leg's scan and its file pruning. That is part of the join condition reaching a side, which the ADR rules out. No direction names the ADR or supersedes it. The new constraint also binds future join work (#663), which meets rule-2 criterion 2 (a long-lived constraint).
- Fix: Add a direction to intent.md: "[Dn] ADR: yes, supersedes per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution, criterion: long-lived constraint (§ Constraints). Each inner join's condition is split before attribution: its join keys stay in the condition and are never pushed to a side, and its other conjuncts become top-level WHERE conjuncts that the per-side attribution handles like any other." Keep the per-side attribution rule and the cross-table and OR rules of the old decision.
