# Plan Review Findings: add-capabilities-exclusion-property

## Summary
- Axes checked: 6/6
- Total findings: 16 (Blockers: 3, Findings: 13)
- Intent Fidelity blockers: 1

## Failure modes

1. An operator copies the documented example `'FN_PRED_LIKE, LIMIT_WITH_OFFSET'`, and `ORDER BY ... LIMIT n OFFSET m` then returns the wrong rows without an error. Routed to `[NFR_IGNORED]` (Feasibility).
2. The combination E2E test leaves out the ORDER BY with LIMIT combination that the issue names, and it passes even when an exclusion has no effect. The request shapes that the issue is concerned about then ship without proof, also on the older Exasol versions that CI runs. Routed to `[SCOPE_REDUCTION]` (Intent Fidelity) and `[COMPLETENESS_GAP]` (Requirement Quality).
3. After recording, the spec library both requires a refresh to default an invalid `METADATA_READ_CONCURRENCY` to `16` and requires it to fail on that value. Routed to `[REQUIREMENT_CONFLICT]` (Requirement Quality).

## Intent Fidelity

#### [SCOPE_REDUCTION] BLOCKER
- Location: plan.md § Implementation Tasks 2.1 (`exclusion_combinations_return_native_rows`); `vs-properties/excluded-capabilities/spec.md` § Scenario "Request shapes that an exclusion makes reachable return the same rows"
- Issue: The acceptance criteria of #622 say: "E2E tests cover a few exclusion combinations that produce new request shapes (for example `ORDER_BY_COLUMN` excluded while `LIMIT` stays) and check the results are correct." The scenario's GIVEN lists six exclusion sets, and none of them excludes `ORDER_BY_COLUMN`. The only ordering case is "`ORDER_BY_EXPRESSION` with an `ORDER BY` on an expression absent from the select list and no `LIMIT`", and no recorded reason explains why it has no `LIMIT`. The interview remark about ORDER BY covers an excluded `LIMIT` under an advertised ordering. It does not cover this combination. Decision [1] relies on the statement "Exasol pushes a limit only together with a pushed ordering" to add no offset guard, and the issue's case is the only E2E check of that statement. That statement was observed on Exasol 2026.2.0-nano.3 only, and the `e2e` CI job also runs older Exasol versions.
- Fix: Add an exclusion set `ORDER_BY_COLUMN` to the scenario's GIVEN and to the 2.1 case table. Pair it with `ORDER BY id LIMIT 5` and `ORDER BY id LIMIT 5 OFFSET 3` over the events table, and assert the native rows for each. Either add `ORDER BY <expression> LIMIT 5` to the `ORDER_BY_EXPRESSION` set, or state in decision-log [1] Consequences why that query is left out.

#### [INTENT_DRIFT] FINDINGS
- Location: decision-log.md § [3]; plan.md § Impact, bullet 3; `vs-properties/property-value-validation/spec.md` § Scenario "Refresh reads the persisted properties without checking them"
- Issue: The interview answer is "REFRESH validates no property; simplify the code. Only CREATE and SET validate." Decision [3] keeps `refresh` failing on an unusable `CATALOG_KIND`, `MERGE_SCHEMA`, or `HIVE_PARTITIONING` value. For the last two this is a value check (`direct-storage/direct-storage-properties` "An unparseable MERGE_SCHEMA or HIVE_PARTITIONING value is rejected, never defaulted"). plan.md § Impact says that REFRESH "no longer fails on a persisted unknown name, an invalid value, ...", which overstates the change. No spec delta states this exception, and the direct-storage scenario names only `createVirtualSchema`, so the refresh behavior for these properties is unspecified after recording. Decision [3] says that defaulting `MERGE_SCHEMA` "would silently change the declared columns". The same argument applies to `TABLE_INCLUDE`, which decision [2] reads as absent on refresh.
- Fix: In plan.md § Impact bullet 3, name the exception: REFRESH still fails on an unusable `CATALOG_KIND`, `CATALOG_CONNECTION`, `NAMESPACE`, `MERGE_SCHEMA`, or `HIVE_PARTITIONING`. Add an AND step with the same statement to "Refresh reads the persisted properties without checking them". Add a line to plan.md § Context that asks the user to confirm the exception, because it departs from the interview answer.

## Feasibility

#### [NFR_IGNORED] BLOCKER
- Location: plan.md § Implementation Tasks 3.1 and 3.2; decision-log.md § [1] Consequences
- Issue: Decision [1] accepts a wrong result. With `LIMIT` or `LIMIT_WITH_OFFSET` excluded and an `ORDER_BY_*` capability advertised, `ORDER BY id LIMIT 5 OFFSET 3` returns the ids 1 to 5 instead of 4 to 8. `specs/mission.md` Core Capability 12 states: "A deviation is accepted only when it is documented in `docs/semantic-differences.md`." No task changes that page. Task 3.1 adds only "excluding a capability hands that work to Exasol, so Exasol's own virtual-schema limitations apply, for example to `ORDER BY` with `LIMIT`", which does not tell the reader that the returned rows are wrong. Task 3.2 explains exclusion "with the issue's example", `'FN_PRED_LIKE, LIMIT_WITH_OFFSET'`, and that value is one of the settings that drops the offset.
- Fix: Add task 3.5: a section in `docs/semantic-differences.md` stating that excluding `LIMIT` or `LIMIT_WITH_OFFSET` while an `ORDER_BY_*` capability stays advertised makes `ORDER BY ... LIMIT n OFFSET m` return the first n rows. Give the ids 1 to 20 example from decision [1], and the workaround of excluding `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION` as well. In 3.1, replace the general sentence with this concrete effect and a link to that section. In 3.2, use an example value without `LIMIT_WITH_OFFSET`, or put the warning next to the example. Add `docs/semantic-differences.md` to group C's Knowledge and to decision [1] Consequences.

#### [EFFORT_MISESTIMATION] FINDINGS
- Location: plan.md § Implementation Tasks 2.2; plan.md § Context, bullet 9
- Issue: Task 2.2 reads "For each case that errors or differs from the native rows, fix the planning path". The number and size of the fixes are unknown until 2.1 runs. The plan states the expected result for one of the two audited shapes: a join without `selectList` follows the order of `selectListDataTypes`. It states no expected result for "a grouped request without `selectList`". plan.md § Context says that a code audit found both shapes and that they "need a live check", so the request JSON that Exasol sends for them is not known yet. The Context statement "The plan changes no scan, file selection, or type mapping" also leaves out the pushdown planning that 2.2 changes, and AGENTS.md names pushdown in its Iceberg and Delta spec-check rule.
- Fix: Split 2.2 into two tasks. 2.2a captures the pushdown request JSON of each 2.1 case through `EXPLAIN VIRTUAL` and records each shape that fails or returns different rows in `notes/`. 2.2b fixes each recorded shape and adds its unit test. State in 2.2b the expected result for a grouped request without `selectList`. Extend the Context statement to pushdown planning: the column order and the grouped shape follow the Exasol virtual schema protocol, so no Iceberg or Delta section governs them.

#### [NFR_IGNORED] FINDINGS
- Location: plan.md § Impact, bullet "Downgrade"; plan.md § Implementation Tasks 3.1
- Issue: The Impact bullet says "Unset the property before a downgrade", but no doc task carries this instruction to users. `docs/install.md` says "A downgrade to an older adapter needs one `REFRESH` under that adapter", and an older adapter rejects the persisted `EXCLUDED_CAPABILITIES` name on that REFRESH. An upgrade has a similar effect: if a later release stops advertising a listed name, every `ALTER VIRTUAL SCHEMA ... SET` fails on the merged value until that entry is removed. Queries keep working in both cases.
- Fix: Add two sentences to task 3.1, or add a task for `docs/install.md`. First sentence: unset `EXCLUDED_CAPABILITIES` before a downgrade. Second sentence: when an upgrade stops advertising a listed name, remove that name in the same `SET` that changes another property.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Verification § Manual Testing, rows 1 and 2
- Issue: The `CREATE VIRTUAL SCHEMA CE_MANUAL ...` commands do not set `ALLOW_HTTP = 'true'`. The local stack serves the catalog at `http://iceberg-rest:8181` and SeaweedFS over plain HTTP, and `ALLOW_HTTP` defaults to `false`. The CREATE in row 2 therefore fails before it can show the exclusion, and the later rows depend on it.
- Fix: Add `ALLOW_HTTP = 'true'` to the CREATE commands in rows 1 and 2.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `specs/vs-properties/create-virtual-schema-adapter-notes-resources/spec.md` § Scenario "Adapter records the metadata read concurrency in the virtual-schema adapterNotes" (recorded spec, not in this plan)
- Issue: The GIVEN of this recorded scenario is "a `createVirtualSchema`, `refresh`, or `setProperties` request". Its AND step says "a value outside the range ... SHALL fail the request, and the adapter MUST NOT default it to `16`". The plan's new scenario "Refresh reads the persisted properties without checking them" requires a refresh over `METADATA_READ_CONCURRENCY = '0'` to succeed and record the default `16`. Both scenarios apply to the same refresh request.
- Fix: Add `vs-properties/create-virtual-schema-adapter-notes-resources/spec.md` to the plan. Give it a DELTA:CHANGED block for this scenario that removes `refresh` from the GIVEN, or that limits the AND step to `createVirtualSchema` and `setProperties`. Add the feature to plan.md § Features and the scenario to § Scenario Coverage.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Implementation Tasks 2.1 (`exclusion_combinations_return_native_rows`, `get_capabilities_ignores_a_persisted_unadvertised_entry`)
- Issue: The combination cases compare rows only. Suppose `getCapabilities` ignored the property, or returned an empty list whenever the property is set. Every case would still return the native rows and pass, so the test does not show that a new request shape was reached. `get_capabilities_ignores_a_persisted_unadvertised_entry` asserts only `PathCheck::Lacks("LIKE")`. An adapter that withholds every capability when an entry is unknown also passes it. `specs/testing.md` § Correctness oracles does not allow a test to pass vacuously. The effect can be observed directly, because `EXPLAIN VIRTUAL` shows the `getCapabilities` response (`e2e_local/capability.rs`, `e2e_advertises_inner_equi_join_capability`).
- Fix: In 2.1, for each combination case and for the persisted-entry test, read the capabilities that `EXPLAIN VIRTUAL` shows. Assert that each excluded name is absent and that a name the value does not list, such as `FN_PRED_EQUAL`, is present. Where the exclusion forces a shape, also assert that shape on the pushdown SQL, for example that no `JOIN` appears when `JOIN` is excluded.

#### [COMPLETENESS_GAP] FINDINGS
- Location: `vs-properties/excluded-capabilities/spec.md` § Scenarios "Create and set reject an entry the adapter does not advertise" and "An excluded capability is withheld from getCapabilities"
- Issue: The issue says "Spaces around names and empty entries are ignored." The spec states this rule only for `getCapabilities`. No step says whether CREATE and SET accept `' FN_PRED_LIKE ,, FN_AGG_COUNT_DISTINCT,'`, which is the value that the first scenario's GIVEN needs a virtual schema to hold. The related list property `TABLE_INCLUDE` rejects an empty entry at CREATE (`vs-properties/table-filter` "An empty pattern entry fails the statement"), so a reader cannot infer the rule from that property. The first scenario has unit proof only, although Exasol shows the result in the `EXPLAIN VIRTUAL` capabilities list.
- Fix: Add an AND step to "Create and set reject ..." that states two rules. CREATE and SET accept whitespace around entries and empty entries. A value that holds only empty entries withholds nothing. Add an E2E case to 2.1 that creates a virtual schema with `' FN_PRED_LIKE ,, FN_AGG_COUNT_DISTINCT,'` and asserts the capabilities list that `EXPLAIN VIRTUAL` shows. List that test for the first scenario in § Scenario Coverage.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: `vs-properties/excluded-capabilities/spec.md` § Scenario "Request shapes that an exclusion makes reachable return the same rows", last AND step
- Issue: The step "a pushdown request shape that only an exclusion makes reachable SHALL be planned correctly or fail the query with an error that names the shape" gives no error text, and it covers every combination of advertised names. No pass or fail test can be written for the error branch. No listed case reaches that branch either, because the THEN step requires every listed query to return the native rows.
- Fix: Do one of two things. Either reduce the step to "MUST NOT return different rows", stated as the reason for the listed cases. Or give the error text, name one exclusion set that produces the error, and add that case to 2.1 with an assertion on the error text.

#### [COMPLETENESS_GAP] FINDINGS
- Location: `pushdown-capabilities/pushdown-planning-order-by-capability/spec.md` delta § Background bullet 3 and the `LIMIT_WITH_OFFSET` AND step; `vs-properties/excluded-capabilities/spec.md` § Background bullet 2
- Issue: The corrected statement "Without `LIMIT_WITH_OFFSET`, Exasol pushes the `ORDER BY` without any `limit`, applies `LIMIT n` itself after the pushed ordering, and ignores the offset" was observed on Exasol 2026.2.0-nano.3 only. Its only proof is a manual test row, and the `e2e` job also runs older Exasol versions. Task 3.1 and the fix of the `[NFR_IGNORED]` blocker above state the statement to users as fact. In the excluded-capabilities Background, the clause "such as the `ORDER BY` with `LIMIT` handling that ... describes" has no scenario step that depends on it.
- Fix: Add an E2E case to `capability_exclusion.rs` that excludes `LIMIT_WITH_OFFSET`, runs `ORDER BY id LIMIT 5 OFFSET 3`, and asserts the ids ranked 1 to 5. Its failure message names the docs to update. Give it a `/// Scenario:` line with the title of the order-by scenario. Remove the "such as ..." clause from the excluded-capabilities Background, or add a scenario step that depends on it.

## Task Breakdown

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § Implementation Tasks 1.8, first bullet; plan.md § Scenario Coverage
- Issue: `stray_persisted_properties_block_refresh_but_not_queries` is the only test that carries `/// Scenario: Pushdown and drop never validate properties` (`vs-properties/property-name-validation`). Task 1.8 replaces this test. The task does not keep that scenario line or the count query that runs before the REFRESH, and § Scenario Coverage does not list the scenario. The scenario then loses its only E2E proof.
- Fix: In 1.8, have `stray_persisted_properties_refresh_and_query` run the count query once before the REFRESH and carry `/// Scenario: Pushdown and drop never validate properties`. Add that scenario to § Scenario Coverage.

#### [CLUSTER_INCOHERENCE] FINDINGS
- Location: plan.md § Parallelization, group C
- Issue: Group C is split off by layer (documentation) and declares "Depends on: none". Task 3.1 documents "`SET` without `REFRESH`", and only task 2.1 verifies that behavior against Exasol. Task 3.2 must describe each shape that 2.2 makes fail instead of planning it. Group C's Knowledge repeats the spec deltas of groups A and B.
- Fix: Set the "Depends on" value of group C to "B".

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Implementation Tasks 2.1, the join cases
- Issue: Task 2.1 says "Join cases use the fact and dimension tables of `join.rs` and their native copies." The native copies come from `JOIN_ORACLE` and `copy_join_tables_natively`, which are private to `e2e_local/join.rs`. `specs/testing.md` § Test code says: "A helper that a second test file needs moves into its area's support module." No task moves them. Task 1.7 does this move for the stub adapter only.
- Fix: Add a step to 2.1 that moves `JOIN_ORACLE` and `copy_join_tables_natively` into `e2e_local/fixtures.rs`, with the source virtual schema as a parameter. Add `e2e_local/join.rs` to the Knowledge of group B.

## Design Depth

Checked without finding, with evidence:
- `[ADR_OVERPROMOTION]`: decision-log entries [1] to [3] all carry `Promotes to ADR: no`.
- `[ARCHITECTURE_DRIFT]`: the BASE comment `6d09607471da3a111975d5f7def4e3d9a1818b30` matches `git hash-object specs/architecture.md`. Both CHANGED blocks copy all of Components and Interfaces. They change only the vs-adapter, capabilities, and vs-properties bullets and two interface bullets. The new `getCapabilities` input that plan.md describes is in the Interfaces block.
- `[ADR_CONFLICT]`: these ADRs were checked: `no-exasol-side-fallback-for-an-advertised-capability`, `capability-invariant-advertise-only-what-the-engine-can-back-correctly`, `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute`, `permission-check-runs-where-a-pushdown-first-reads-table-metadata`, and `declined-order-by-expression-hidden-base-columns-exasol-dialect`. An exclusion only removes names from the advertised list, and the plan keeps the exclusion out of `adapterNotes`. The Context of the capability-invariant ADR says that under-advertising "only costs performance". Decision [1] shows that this is not always true, but the Decision of that ADR is not affected.

#### [INFORMATION_COUPLING] FINDINGS
- Location: plan.md § Implementation Tasks 1.1 and 1.2; architecture.md § Components (capabilities, vs-properties)
- Issue: The architecture delta makes `capabilities` the owner of the "`EXCLUDED_CAPABILITIES` entry syntax" and of the match against the advertised names. Task 1.2 adds a `TuningChecker` rule that reports "each distinct unadvertised entry", but it does not say that this rule calls the parser from 1.1. If it does not, `capabilities.rs` and `properties.rs` each hold their own split, trim, empty-entry, and exact-match rules. A later change to one copy can then make CREATE accept a value that `getCapabilities` reads differently, or the reverse.
- Fix: In 1.1, have `capabilities.rs` expose one function that divides a value into its advertised entries and its unadvertised entries. In 1.2, state that the `TuningChecker` rule reports the unadvertised entries that this function returns and does no parsing of its own.

## Prose Quality

Checked without finding: plan.md, decision-log.md, and architecture.md contain no em dashes. Gherkin steps and Background bullets are outside the prose rules.

#### [PROSE_UNCLEAR] FINDINGS
- Location: decision-log.md § [1] Rationale, last sentence
- Issue: The Rationale ends with a separate decision: "Because Exasol pushes a limit only together with a pushed ordering, a non-zero offset without `orderBy` stays unreachable, and the plan adds no guard for it." Neither the Decision line nor the Consequences line says that the plan adds no guard, so a reader of those lines does not learn it.
- Fix: Move the choice to add no guard into the Decision line or the Consequences line of decision [1], and keep only its reason in the Rationale.
