# Plan Review Findings: add-lakekeeper-permission-shape-coverage (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 11 (Blockers: 1, Advisory: 10)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed. Three ways it could have happened:

1. A contributor adds a pushdown path in `adapter/pushdown/mod.rs` that reads Iceberg metadata through a catalog call that is not on the guard's list. The structural test passes, because `mod.rs` is exempt from the async rule and rule one matches only nine call names. The new path does not go through `TableScanResolver::resolve`, so `TableAdmission` never refuses it. The guard was described as covering "production code reads a table outside `TableScanResolver::resolve`", and nobody revisited it. Routed to Feasibility, `[UNSTATED_ASSUMPTION]` (guard coverage).
2. An operator reads `docs/permissions.md` § Privilege boundary and sees only "never grant `EXECUTE` on `LAKEHOUSE_SCAN` or `LAKEHOUSE_DISTRIBUTE_FILES`, or `EXECUTE ANY SCRIPT`". The operator grants `ALTER ANY VIRTUAL SCHEMA` to an analyst role for refresh duties. An analyst runs `ALTER VIRTUAL SCHEMA ... SET PERMISSION_CHECK = ''` and reads every table. Routed to Requirement Quality, `[COMPLETENESS_GAP]` (docs privilege boundary).
3. The "right only" join case passes for the same reason as the "left only" case, because Exasol or the broadcast planner normalizes the sides. A later regression that collects identifiers from one side only becomes a refusal (`TableAdmission`) rather than a leak, so the damage is limited. The interview's "spec and test left-only, right-only, both" is still not proven. Routed to Feasibility, `[UNSTATED_ASSUMPTION]` (join side order).

## Intent Fidelity
[no objection: axis checked. Every #416 scope item maps to work: per-shape authorization is proven live by tasks 1.3 and 1.4 and needs no code, because #415 already checks every shape (recorded plan 006 decision [1] rejected the shape refusal that the issue text assumes, ADR `permission-check-runs-where-a-pushdown-first-reads-table-metadata`). The four named bypass sites are audited in decision [2], and I verified each claim against commit `7b297c6`. The listing limitation is asserted by task 1.5. The two-user E2E runs on the #414 environment. Docs cover setup, mapping, trust model, and the privilege boundary. The interview answers (live E2E per shape, all-or-nothing joins naming each denied table, audit plus guard, `docs/permissions.md` linked from `security.md` and `index.md`) are each operationalized.]

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Impact (bullet 4), § Implementation Tasks 2.1-2.2; decision-log.md [3] Decision and Consequences
- Issue: The plan states the guard "fails when production code reads a table outside `TableScanResolver::resolve`", and decision [3] says it "implements the recorded step ... so a query shape added later cannot read an unchecked table". The guard is weaker than that. (a) Rule two exempts `adapter/pushdown/mod.rs`, `joins/mod.rs`, and `joins/planning.rs` for all three patterns. These are the dispatch files where a new shape is most likely added, and `handle_pushdown` and `plan_join` live there. A new `.await` or `block_on(` on an unlisted read in those files passes. (b) Rule one omits the directory reads in `adapter/parquet_directory.rs` (`resolve_parquet_directory(`, `list_parquet_files(`, `list_location_files(`), which `format/parquet_format_reader.rs:54`, `format/catalog_parquet_format_reader.rs:171,233`, and `adapter/direct_storage.rs:80` call. (c) The self-test case "the definition `fn load_table(` is not reported" is vacuous, because the rule matches `.load_table(` with a dot. The definition-skip logic matters only for dot-less calls such as `format_reader(`, whose current definition `pub fn format_reader<'a>(` does not match by accident of its generic parameter. (d) Plain substring matching flags any identifier that ends in a listed name, such as a future `build_format_reader(`.
- Fix: In plan.md § Impact and decision-log.md [3], state what the guard catches: the nine named calls outside their owners, and async I/O in shape modules outside the exempt files. Name the exempt dispatch files as the residual gap, covered at runtime only for paths that call `resolve`. In task 2.1, tighten the exempt files: pin the count of `.await` and `block_on(` occurrences in each exempt file (today `mod.rs` 3, `joins/mod.rs` 2, `joins/planning.rs` 1, `scan_resolution.rs` per its current count), so any new await there fails with a message to review it. Add `resolve_parquet_directory(`, `list_parquet_files(`, and `list_location_files(` to rule one, owned by `adapter/pushdown/format/` and `adapter/direct_storage.rs`. Replace the `fn load_table(` self-test case with `pub fn format_reader(` planted in `adapter/pushdown/format/mod.rs`, and require a word boundary before each dot-less call name.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 1.4; decision-log.md [4]
- Issue: The "unreadable on the left" and "unreadable on the right" cases differ only in SQL text order (`FACT_ORDERS o JOIN DIM_CUSTOMER c` versus `DIM_CUSTOMER c JOIN FACT_ORDERS o`). The task asserts the broadcast path but never asserts that the pushdown request puts the unreadable table on a different side in the two cases. If Exasol orders the join's `from` sides independently of the text, the two cases test the same request. The interview asked to "spec and test both sides individually (user denied on left only, right only, both)".
- Fix: In task 1.4, add an assertion on the allowed user's `EXPLAIN VIRTUAL` output that the left side of the pushed join is `FACT_ORDERS` in the first case and `DIM_CUSTOMER` in the second, read from the echoed request's join `left`/`right` table names. If Exasol normalizes the order, record that in decision [4] and state that the side cannot be chosen from SQL.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 1.1 (last sentence)
- Issue: The unit test "every fixture table has its table scope in `ALL_SCOPES`" needs one authoritative list of fixture tables. Today the tables are named separately in `provision_authz_fixture` (`[TABLE_ALPHA, TABLE_BETA]`, then `E2E_TABLE`) and in `ALL_SCOPES`, and `table_ids` exists only at run time. A unit test that compares `ALL_SCOPES` with a list written inside the test proves nothing about provisioning.
- Fix: In task 1.1, add a `FIXTURE_TABLES` constant that lists all five tables, make `provision_authz_fixture` record a table id for each entry of it, and have the unit test assert that each `FIXTURE_TABLES` entry has a `Scope::Table` entry in `ALL_SCOPES`. Note that `ALL_SCOPES` is typed `[Scope; 8]` and becomes `[Scope; 10]`.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Context (bullet 7); decision-log.md [8] Rationale; plan.md § Implementation Tasks 3.1 (items 3, 5, 6, and the closing evidence rule)
- Issue: The plan says "Plan 006 recorded" the 30-second batch-check deadline. No plan 006 artifact mentions it. The deadline comes from commit `52354c8`, `BATCH_CHECK_DEADLINE` in `crates/lakehouse-catalog/src/lakekeeper.rs:20`, asserted at `lakekeeper_tests.rs:822`. Task 3.1 also limits evidence to "a passing test of this plan or plan 006, or a measurement recorded in plan 006's verification report". Item 3's `LAKEKEEPER__OPENID_SUBJECT_CLAIM` statement rests on plan 005's probe (`specs/_recorded/005-test-lakekeeper-batch-check-fixtures-two-principals/plan.md:38`), and item 6's "vended credentials included, and are not scoped per user" names no evidence. The implementer must either drop these statements or break the rule.
- Fix: In plan.md § Context and decision-log.md [8], attribute the 30-second deadline to `BATCH_CHECK_DEADLINE` and its unit test. In task 3.1, widen the evidence rule to "a passing test in the repository, a constant asserted by a unit test, or a measurement recorded in plan 005 or plan 006", and name the evidence next to items 3 (subject claim: plan 005 probe), 5 (deadline: `lakekeeper_tests.rs`), and 6 (vended credentials: the code path or a test that shows it).

## Requirement Quality

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: specs/_plans/add-lakekeeper-permission-shape-coverage/vs-adapter/lakekeeper-permission-check/spec.md § Background, bullet 4
- Issue: "Every scan reads storage with the CONNECTION's credentials, whoever queries." No GIVEN, WHEN, or THEN step of the merged spec depends on this fact. The bullet's second sentence ("The check therefore protects a table only from users who hold no `EXECUTE` ...") is what Scenario "A user without the grant can neither obtain nor run the table's plan" uses in its GIVEN. The first sentence is the reason for the second, and the reason belongs in `docs/permissions.md` (trust-model limit 1) and the decision log.
- Fix: In the delta's Background bullet 4, delete the sentence "Every scan reads storage with the CONNECTION's credentials, whoever queries." and reword the remainder as "The check protects a table only from users who hold no `EXECUTE` on the scan and distributor scripts and no `EXECUTE ANY SCRIPT` (#402)." Keep the storage-credential fact in task 3.1 item 6 and add one line to decision-log.md [7] Rationale stating it.
- Escalation: MECHANICAL. Reading the merged spec's scenarios settles whether any step depends on the sentence.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: spec.md delta § Scenario "Each table of a join is checked and named on its own", WHEN step
- Issue: "runs inner joins with a table that the second user cannot read on the left only, on the right only, and on both sides, planned as a broadcast join and as the unaccelerated join wrapper" reads as six joins: each side case planned both ways. Task 1.4 runs three broadcast joins and one three-table wrapper join, where "left only" and "right only" do not apply. Under the plain reading, the mapped test does not satisfy the scenario.
- Fix: Rewrite the WHEN step as "each user runs broadcast inner joins with a table that the second user cannot read on the left only, on the right only, and on both sides, and a three-table inner join that the adapter answers through the unaccelerated join wrapper".

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 3.1 item 7 (Privilege boundary) and item 6 (Trust model)
- Issue: Item 7 tells operators never to grant `EXECUTE` on `LAKEHOUSE_SCAN` or `LAKEHOUSE_DISTRIBUTE_FILES`, or `EXECUTE ANY SCRIPT`. Three parts of the boundary are missing. (a) Task 1.6 also asserts no `EXECUTE` on the adapter script, but the docs omit it. (b) The virtual schema owner, and any user holding `ALTER ANY VIRTUAL SCHEMA`, can set `PERMISSION_CHECK` off or rewrite `USER_MAPPING`, which removes enforcement for every user. (c) ADR `plan-visibility-execution-privilege-split` names the script-scoped `GRANT ACCESS ON CONNECTION ... FOR SCRIPT` as the second gate, and the page does not mention it. An operator who follows the page can grant `ALTER ANY VIRTUAL SCHEMA` without knowing that it disables the check.
- Fix: In task 3.1 item 7, add `LAKEHOUSE_ADAPTER` to the never-grant list. Add one sentence stating that the owner and holders of `ALTER ANY VIRTUAL SCHEMA` can turn the check off or change the mapping, so the check is only as strong as control over those privileges. Add one sentence stating that the CONNECTION is granted only `FOR SCRIPT` to the engine's scripts, and link the ADR's section in `security.md`.

#### [IMPLEMENTATION_LEAKAGE] ADVISORY
- Location: spec.md delta § Background, bullet 6 (carried unchanged from the recorded spec)
- Issue: "It cannot include other templates" and "a template that runs too long is refused" are facts that no scenario step of the merged spec depends on. They come from the recorded spec, so this plan did not introduce them, but the `DELTA:CHANGED` block re-emits them, and `spec-conciseness` Rule 1 asks for cleanup when a plan touches the section.
- Fix: Either drop both clauses from bullet 6 (plan 006's unit tests and decision log keep them), or leave the bullet unchanged and add one line to decision-log.md stating that the carried bullets were left as recorded.

## Task Breakdown
[no objection: axis checked. Each of the four new scenarios maps to one task (1.3 to 1.6) and one row in § Scenario Coverage. The structural guard maps to the recorded scenario "One batch-check per query decides every table before any table is read". Groups A and B touch disjoint files (`tests/` versus `src/adapter/pushdown/scan_resolution_tests.rs`) and share no knowledge entry. Group C depends on A and shares decision [7] with it, so the sequencing is correct. `make test-e2e-lakekeeper` runs with `--test-threads=1`, so the `REFRESH` in task 1.5 cannot race another permission test.]

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: plan.md § Implementation Tasks 1.2 ("Keep the markers in this file, so no other suite changes")
- Issue: The knowledge that `LHS_T0` marks the qualified wrapper, that `group_keys` with `PARTIAL_` marks the grouped merge, and that `FROM DUAL` without `LAKEHOUSE_SCAN` marks the empty result is already written inline in `e2e_scan_test.rs`, `e2e_count_distinct_test.rs`, and `e2e_capability_test.rs`. The plan adds a fourth private copy. When the adapter's rendering changes, each copy must change separately. The stated reason does not hold: `tests/common/mod.rs` sets `#![allow(dead_code)]`, so adding predicates to `common/e2e_harness.rs` changes no other suite.
- Fix: In task 1.2, put the shape predicates in `crates/lakehouse-engine/tests/common/e2e_harness.rs`, next to `has_broadcast_join_block` and `has_n_scan_wrapper`, and update the Group A Knowledge entry. Do not migrate the other suites in this plan.

ADR and architecture checks: no objection. No decision-log entry sets `Promotes to ADR: yes`, so `[ADR_OVERPROMOTION]` and the ADR trigger of `[ARCHITECTURE_DRIFT]` do not apply. The plan adds tests, fixtures, and docs only, and decision [1] carries `Architecture: no change` with a reason. `speq decision-log show` lists `permission-check-runs-where-a-pushdown-first-reads-table-metadata` and `plan-visibility-execution-privilege-split`. The plan conforms to the first (no per-shape checks, guard as its corollary) and preserves both gates of the second.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: decision-log.md [2]; plan.md § Impact; spec.md delta § Scenario "Every single-table pushdown shape returns rows only to a user whose principal holds the grant"
- Issue: (a) Decision [2] cites line numbers "on commit `7b297c6`" that are off by one at that commit: the `qualified_single_table_fallback_pushdown` calls are at `pushdown/mod.rs:307, 375, 422, 443, 463` (cited as 306, 374, 421, 442, 462), `empty_result_sql` is at `pushdown/mod.rs:207` (cited 206) and `joins/mod.rs:178` (cited 177). (b) § Impact says `make test-e2e-lakekeeper` "gains four tests", but task 1.1 adds a fifth (the scope unit test) to the same binary. (c) The delta calls the wrapper "the qualified single-table wrapper", while the recorded spec calls it "a qualified fallback wrapper" (writing rule 4).
- Fix: Correct the six line numbers in decision-log.md [2]. Change § Impact to "five tests (four E2E, one fixture unit test)". In the delta's scenario, use the recorded name "qualified fallback wrapper".

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary; plan.md § Implementation Tasks 1.2
- Issue: The Summary's first sentence makes five claims in 60 words (writing rule 2). Task 1.2 is one bullet that defines a virtual schema, a CONNECTION, three helpers, and eight shape markers in running text, so a reader cannot check off each part.
- Fix: Split the Summary into one sentence per outcome (live proof per shape and join side, listing limitation and privilege boundary, structural guard, docs, no adapter change). Split task 1.2 into sub-bullets: the oracle virtual schema, the row helper, the denial helper, and one sub-bullet per shape marker.
