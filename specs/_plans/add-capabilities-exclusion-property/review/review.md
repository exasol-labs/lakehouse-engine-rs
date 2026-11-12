# Plan Review Findings: add-capabilities-exclusion-property

## Summary
- Axes checked: 6/6
- Total findings: 8 (Blockers: 0, Findings: 8)
- Intent Fidelity blockers: 0

## Failure modes

1. The spec library records the `LIMIT_WITH_OFFSET` request shape seen on Exasol 2026.2.0-nano.3 as the behavior of every Exasol version. The repository's earlier live check on `exasol/docker-db:2025.2.1` saw a different shape. CI and the default local stack run 2025.1.16 and 8.29.13, so Manual Testing row 4 can fail on the default stack. Routed to `[UNSTATED_ASSUMPTION]` (Feasibility).
2. Task 2.2a captures the exclusion request shapes on one Exasol version. The `e2e` CI job runs two other versions. A shape that only those versions send passes locally and fails in CI after 2.2b is merged. Routed to `[HIDDEN_DEPENDENCY]` (Feasibility).
3. A user reads `docs/semantic-differences.md`, which names only the OFFSET effect. The user keeps `LIMIT` excluded and an `ORDER_BY_*` capability advertised. On a multi-node cluster, `ORDER BY ... LIMIT n` can then return a wrong row set, because Exasol applies the limit to imported rows whose order the linked Exasol limitation does not guarantee. Routed to `[COMPLETENESS_GAP]` (Requirement Quality).

## Intent Fidelity

No objection. Axis checked: every acceptance criterion of #622 maps to a task or a test. The `LIKE` exclusion maps to `excluded_like_is_evaluated_by_exasol`. `SET` without `REFRESH` maps to `set_exclusion_applies_from_the_next_query_without_refresh`. The rejection with a suggestion maps to `create_and_set_reject_an_unadvertised_capability`. The combination E2E maps to `exclusion_combinations_return_native_rows`. The three docs pages map to tasks 3.1 to 3.3. The final interview decisions are applied as recorded. Decision [1] adds no coupling between capabilities. No E2E case excludes `LIMIT`, `LIMIT_WITH_OFFSET`, or an `ORDER_BY_*` capability. Task 3.4 adds the `docs/semantic-differences.md` section with the Exasol link. Decision [2] keeps `REFRESH` on the shared checks. The removed REFRESH deltas are gone from the plan directory.

## Feasibility

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: decision-log.md § [1] Rationale; `pushdown-capabilities/pushdown-planning-order-by-capability/spec.md` § Background bullet 3 and § Scenario "ORDER_BY_COLUMN is advertised ...", fifth step (`LIMIT_WITH_OFFSET`); plan.md § Manual Testing row 4
- Issue: Decision [1] states "Exasol never pushes a bare `LIMIT` with the offset dropped" and says that the recorded Background was wrong. The recorded Background came from a live check in commit f4f6fd3f (#256). That check ran on the local Docker stack, whose default image was then `exasol/docker-db:2025.2.1`, and it recorded that Exasol "pushed `"limit":{"numElements":12}` with NO `offset` key". The two observations agree on the result, ranks 1 to n. They disagree on the request shape. The behavior therefore depends on the Exasol version. The delta replaces the 2025.2.1 shape with the 2026.2 shape and states it for every version. The local stack defaults to `exasol/docker-db:2025.1.16` (`docker-compose.yml`), and the `e2e` job runs 2025.1.16 and 8.29.13. Manual Testing row 4 expects "`PUSHDOWN_SQL` orders by `id` and carries no limit", which can fail on that default stack. The Rationale sentence "Exasol pushes a limit only together with a pushed ordering" is also false as written: `SELECT ... LIMIT 5` without `ORDER BY` pushes a limit with no `orderBy`.
- Fix: In decision-log.md § [1] Rationale, scope the probe result to Exasol 2026.2.0-nano.3. Replace "Exasol never pushes a bare `LIMIT` with the offset dropped" with the observed 2026.2 shape. Add that #256 observed `"limit":{"numElements":12}` without an offset on `docker-db:2025.2.1`. Change "This corrects the Background" to "This adds the 2026.2 shape to the Background". Change "Exasol pushes a limit only together with a pushed ordering" to "For a query with `ORDER BY`, Exasol pushes a limit only together with the pushed ordering". In the order-by delta, rewrite Background bullet 3 to state the effect common to both versions: Exasol applies no offset, so `LIMIT 12 OFFSET 3` returns ranks 1 to 12. Name each observed request shape with its Exasol version. Make the same change to the reason clause of the scenario's fifth step. In plan.md § Manual Testing row 4, keep the expected rows (ids 1 to 5) and drop "carries no limit", or state the expected `PUSHDOWN_SQL` per Exasol version.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Implementation Tasks 2.2a
- Issue: Task 2.2a says "Run 2.1 against the live stack" and records each failing shape from the request JSON captured there. The `e2e` CI job runs the new tests on `exasol/docker-db:2025.1.16` and `exasol/docker-db:8.29.13` (`.github/workflows/ci.yml`). The `LIMIT_WITH_OFFSET` evidence above shows that Exasol versions send different request shapes for the same capability set. The development machine is ARM64, which cannot run `docker-db`, so the "live stack" there is Exasol Personal 2026.2. A shape that only 2025.1.16 or 8.29.13 sends, for example a join or grouped request without `selectList`, then reaches 2.2b unrecorded and fails in CI.
- Fix: In plan.md task 2.2a, require capturing the request JSON of each 2.1 case on every Exasol version the `e2e` job runs (2025.1.16 and 8.29.13), for example from a CI run of the 2.1 tests. Require that the `notes/` record names the Exasol version of each shape. In 2.2b, require that each fix handles every recorded version's shape.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Implementation Tasks 2.1, `whitespace_and_empty_entries_are_accepted`
- Issue: The test asserts that "the echoed capabilities lack exactly `FN_PRED_LIKE` and `FN_AGG_COUNT_DISTINCT`" and that "a second schema with `' , '` echoes the full list". Both assertions need the full advertised list as their expected value. The task does not say where that list comes from. `specs/testing.md` § Shared harness forbids E2E code from using `lakehouse-engine`, and § Correctness oracles forbids engine code as an oracle, so `CAPABILITIES` is not available. A hand-copied list of 124 names in the test breaks with every capability change.
- Fix: In plan.md task 2.1, state the baseline. Run the same `EXPLAIN VIRTUAL` query over `LAKEHOUSE_VS`, which has no exclusion, and parse its `capabilities` array. Assert that the baseline minus the first schema's array is exactly `{FN_PRED_LIKE, FN_AGG_COUNT_DISTINCT}`, and that the `' , '` schema's array equals the baseline.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: `vs-properties/excluded-capabilities/spec.md` § Background bullet 2
- Issue: The bullet states "Exasol never delegates an unadvertised capability. It evaluates the construct itself over the rows the pushdown returns." Read with the property's rule "any advertised name, alone or in any combination", this says that Exasol applies an excluded `LIMIT_WITH_OFFSET` itself. The order-by delta in the same plan says the opposite: without `LIMIT_WITH_OFFSET`, Exasol "ignores the offset". Scenario 2 needs only the predicate case of the bullet.
- Fix: In `vs-properties/excluded-capabilities/spec.md` § Background, replace bullet 2 with "Exasol does not delegate an unadvertised predicate. It evaluates the predicate over the rows the pushdown returns." Keep the `LIMIT` and `ORDER BY` behavior in the order-by feature.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: `vs-properties/excluded-capabilities/spec.md` § Scenario "Request shapes that an exclusion makes reachable return the same rows", GIVEN; plan.md § Implementation Tasks 2.1, `exclusion_combinations_return_native_rows`
- Issue: The GIVEN pairs "`FN_AGG_COUNT` with a grouped `AVG` and `COUNT`". The adapter advertises `FN_AGG_COUNT` and `FN_AGG_COUNT_STAR` separately (`adapter/capabilities.rs`). `COUNT(*)` falls under `FN_AGG_COUNT_STAR`, which this set leaves advertised. A case written with `COUNT(*)` reaches no new request shape and passes vacuously.
- Fix: In the scenario's GIVEN, write "`FN_AGG_COUNT` with a grouped `AVG(<column>)` and `COUNT(<column>)`". In plan.md task 2.1, state that this case counts a column, not `*`.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Implementation Tasks 3.4, second bullet; § Implementation Tasks 3.1; § Impact, bullet 2; decision-log.md § [1] Consequences, third bullet
- Issue: The documented effect covers only the offset: "`ORDER BY ... LIMIT n OFFSET m` return the first n rows". Decision [1] records the 2026.2 mechanism: Exasol pushes the `ORDER BY` without the limit, "removes its own `ORDER BY`, applies `LIMIT n` locally". The page that task 3.4 links states that "the outermost order of imported result rows is not guaranteed". With `LIMIT` excluded and an `ORDER_BY_*` capability advertised, `ORDER BY ... LIMIT n` without an offset therefore takes n rows from an import whose order Exasol does not guarantee. On a single-node Exasol these were the first n rows. On a multi-node cluster the row set can differ. `specs/mission.md` Core Capability 12 accepts a deviation only when `docs/semantic-differences.md` documents it. The workaround that task 3.4 gives covers this case too.
- Fix: In plan.md task 3.4, add a bullet: with `LIMIT` or `LIMIT_WITH_OFFSET` excluded and an `ORDER_BY_*` capability advertised, Exasol applies `LIMIT n` itself to the imported rows. The linked limitation does not guarantee the order of those rows, so `ORDER BY ... LIMIT n` can return other than the first n rows on a multi-node cluster. Excluding `ORDER_BY_COLUMN` and `ORDER_BY_EXPRESSION` as well avoids it. Make the matching one-clause change in task 3.1, in plan.md § Impact bullet 2, and in decision-log.md § [1] Consequences.

## Task Breakdown

No objection. Axis checked: every delta has an implementing task or an unchanged test. `excluded-capabilities` is covered by 1.1 to 1.7 and 2.1 to 2.2b. `property-name-validation` and `property-value-validation` are covered by 1.2, 1.5, and 1.7. `create-virtual-schema` is covered by 1.1, 1.3, and 1.4. `pushdown-planning-order-by-capability` changes only a Background fact and a reason clause, and task 3.4 documents it. § Scenario Coverage lists all ten delta scenarios. Groups A, B, and C run in sequence, A then B then C, so the overlap of `e2e_local/fixtures.rs` in the Knowledge of A and B creates no parallel edit. Group A is sliced by feature (property catalog), not by layer. Task 1.6 and task 2.1 move the stub adapter and `JOIN_ORACLE` helpers into `e2e_local/fixtures.rs`, per `specs/testing.md` § Test code. I checked both helpers: `JOIN_ORACLE` and `copy_join_tables_natively` at `e2e_local/join.rs` L1198 to L1209, and `PERSISTING_ADAPTER_BODY` at `property_validation.rs` L607.

## Design Depth

No objection. Axis checked, with evidence:
- `[SHALLOW_DESIGN]`: `capabilities` gains one function that splits a value into advertised and unadvertised entries, and `get_capabilities_response(Option<&str>)`. Both hide the entry syntax and the subtraction. No pass-through wrapper is planned.
- `[INFORMATION_COUPLING]`: task 1.1 makes `capabilities.rs` the one owner of the entry syntax, and task 1.2 states that the `TuningChecker` rule "does no parsing of its own". The previous round's finding is resolved.
- `[TACTICAL_SHORTCUT]`: task 2.2b fixes each recorded shape in the planner and adds no refusal path.
- `[BOUNDARY_VIOLATION]`: none. `getCapabilities` reads only the request properties and opens no CONNECTION or catalog.
- `[ADR_OVERPROMOTION]`: decision-log entries [1] and [2] both carry `Promotes to ADR: no`.
- `[ARCHITECTURE_DRIFT]`: the BASE comment `6d09607471da3a111975d5f7def4e3d9a1818b30` matches `git hash-object specs/architecture.md`. A diff of both CHANGED blocks against `specs/architecture.md` shows that they copy all of Components and Interfaces. Only the `vs-adapter`, `capabilities` (new), and `vs-properties` bullets and the two protocol and properties interface bullets change. The dependency `capabilities | depends on: none` matches task 1.1's "keeps no dependency on `properties.rs`". The new `getCapabilities` input from plan.md § Context bullet 11 is in the Interfaces block.
- `[ADR_CONFLICT]`: checked `no-exasol-side-fallback-for-an-advertised-capability`, `capability-invariant-advertise-only-what-the-engine-can-back-correctly`, and `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute`. An exclusion only removes names from the response, and scenario "A changed exclusion applies from the next query" keeps the value out of `adapterNotes`.

## Prose Quality

Checked: plan.md, decision-log.md, architecture.md, and the five deltas contain no em dashes. Gherkin steps and Background bullets are outside the prose rules.

#### [PROSE_UNCLEAR] FINDINGS
- Location: decision-log.md § Interview, lines 6, 7, 10, and 14
- Issue: Four answers conflict with the final rule on line 16. Line 6 says "Only `CREATE` and `SET`". Line 7 says "`REFRESH` validates no property; simplify the code". Line 10 says refresh drops the permission checks. Line 14 says REFRESH "runs no check". Line 16 says "REFRESH behaves as today and as the Java virtual schemas". None of the four lines is marked as superseded. An implementer who reads the Interview top-down meets four instructions to remove the refresh checks before the one that keeps them.
- Fix: In decision-log.md § Interview, append "(superseded by the final REFRESH rule below)" to the answers on lines 6, 7, 10, and 14. Leave lines 15 and 16 as they are.

#### [PROSE_BLOAT] FINDINGS
- Location: plan.md § Context, bullets 10 and 14
- Issue: Bullet 10 ends with a decision: "This plan rejects such a name at `CREATE`, `REFRESH`, and `SET`, together with the other property checks, and ignores it at query time instead." The plan template keeps decisions out of Context, and § Impact bullet 3 and decision [2] already state this rule. In bullet 14, "It also corrects the Background ..." has no clear subject. "It" can refer to the behavior or to decision-log entry [1].
- Fix: Delete the last sentence of plan.md § Context bullet 10. In bullet 14, replace "It also corrects" with "Decision-log entry [1] also updates", which also matches the version-scoped wording that the `[UNSTATED_ASSUMPTION]` fix above asks for.
