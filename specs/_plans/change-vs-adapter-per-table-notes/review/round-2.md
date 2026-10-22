# Plan Review Findings: change-vs-adapter-per-table-notes (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 9 (Blockers: 5, Advisory: 4)
- Intent Fidelity blockers: 1
- Human-escalation blockers: 1

## Round-1 Blocker Recheck

- Resolved: [INTENT_DRIFT] A partial refresh drops the skip entries of tables the source no longer lists. The requester chose Option A, and decision-log.md § Interview records it: "Scoped refresh, an unrequested skip entry whose table the source no longer lists? → A: Drop it (decision [3] approved ...)". Decision [3]'s Rationale now gives the real reasons, and the parse-ambiguity sentence is gone. plan.md § Impact reads "keeps the entries of unrequested tables the source still lists".
- Resolved: [UNSTATED_ASSUMPTION] The no-note path is reachable from Exasol SQL. Task 1.10 adds `an_upgraded_schema_fails_naming_refresh_until_refreshed`, which swaps a test-local `PYTHON3` adapter for the engine adapter through `CREATE OR REPLACE`. § Scenario Coverage maps it to the three scenarios, and no "unreachable" annotation remains. Task 1.2 lists a JSON `null` note, and task 1.7 no longer has the impossible premise. The reviewer checked feasibility. `create_schema_and_scripts_in` (`tests/common/e2e_harness.rs`) recreates the adapter under the fixed script name in any schema. Both pushdown paths read the identifier before any column note (`resolve_pushdown_identifier` runs before `handle_pushdown`, and `detect_join` reads identifiers first), so the test sees the identifier-note error even when the probe adapter writes no column note.
- Resolved: [REQUIREMENT_CONFLICT] The cap scenario and the merge scenario set different rules. The cap scenario's WHEN is now "createVirtualSchema, setProperties, or a refresh that carries no `requestedTables`". The merge scenario caps "the merged list" and states that `SKIPPED_TABLES_OMITTED` is "absent when that sum is 0".
- Resolved: [IMPLEMENTATION_LEAKAGE] Three Background facts no scenario used. The create-virtual-schema Background no longer carries the join-leg and self-join clause or the "created before identifier notes existed" bullet. The adapter-notes Background no longer names the identifier note. The adapter-notes feature description now says the identifier "lives in that table's own identifier note (`vs-adapter/create-virtual-schema`), not here". That sentence replaces the recorded description's map clause and only bounds the feature's scope, so the reviewer accepts it.
- Resolved: [COMPLETENESS_GAP] The mission glossary still defined the table map. Task 1.12 replaces the "AdapterNotes" row text.

## Premortem

Six months from now this plan failed. Three stories:

1. **The upgrade REFRESH cannot heal a schema.** A direct-storage lake holds `Sales/` with data and `sales/` with only `_SUCCESS`. The schema worked for a year. After the upgrade, every query fails and asks for a REFRESH. The REFRESH now fails with the name-collision error, because the plan made skipped identifiers collide. `CREATE` fails too. The operator does not own the bucket. Issue #425 had said "No change there" about collisions. Routed to `[INTENT_DRIFT]`.
2. **The cost requirement regresses without a red test.** A later refactor makes Glue's `load_listed_tables` call `list_tables` and filter the result. Every test stays green, because only direct storage proves that no unrequested table is loaded. A customer with 4,000 Glue Iceberg tables waits minutes for `REFRESH TABLES`, and one broken unrelated table fails it. Routed to `[COMPLETENESS_GAP]`.
3. **A partial heal reads as a bug.** After the upgrade, a user runs `REFRESH TABLES ORDERS`. The recorded scenario says every virtual table without a note carries one "after the refresh". The other tables keep failing. Routed to `[REQUIREMENT_CONFLICT]`.

## Intent Fidelity

#### [INTENT_DRIFT] BLOCKER
- Location: plan.md § Impact bullet 5; plan.md task 1.3; decision-log.md § Design Decisions [2] Consequences bullet 2; decision-log.md § Review Findings, second entry ("The collision check covers every listed identifier in every path")
- Issue: The plan changes which namespaces `CREATE` and a full `REFRESH` accept, against an explicit constraint of the issue the user asked to fix.
  - Issue #425, § Constraints / open points: "Collision detection (two catalog identifiers flattening to one Exasol name) still runs over the full listing at create/refresh time. No change there."
  - plan.md § Impact: "A namespace in which a skipped table and another listed table flatten to one Exasol name now fails `CREATE` and `REFRESH` with the name-collision error". Task 1.3: "It checks every listed identifier, loaded or skipped".
  - Today `build_table_map` runs over the loaded tables only (`build_listing_virtual_tables` passes `survivors`). The user's interview answer asks for a check over every listed identifier only for the scoped refresh: "Match requested names against flattened identifiers, run the collision check over them, and `load_table` only the requested matches." Extending the check to `CREATE`, `setProperties`, and a full `REFRESH` is the planner's choice. The second `[plan-review]` entry presents it as part of the requester's direction.
  - Answer to the orchestrator's question: yes, outcomes change for existing schemas. A namespace where a skipped identifier flattens to the same Exasol name as a loaded one, or where two skipped identifiers collide, creates and refreshes today and fails after the change. Iceberg REST (case-sensitive catalogs, nested `__` names) and direct storage (case-sensitive object keys) are the likely sources.
  - The change also breaks the plan's own upgrade path. plan.md § Impact says "One `ALTER VIRTUAL SCHEMA <name> REFRESH` per virtual schema fixes it". For an affected schema that REFRESH fails, Exasol keeps the tables without identifier notes, and every pushdown keeps failing. Recreating the schema fails too. Issue #425 states the opposite goal: "`ALTER VIRTUAL SCHEMA ... REFRESH` must always be safe and heal whatever state is persisted."
  - The recorded collision clauses ("two distinct Iceberg identifiers", "two directories") do not say whether a skipped identifier counts. No delta makes the wider rule explicit, yet `a_skipped_identifier_collides_like_a_loaded_one` asserts it.
- Fix: Ask the requester to choose one of three options. Record the answer as an `## Interview` line in decision-log.md.
  - Option A, which the reviewer recommends: keep `CREATE`, `setProperties`, and a refresh without `requestedTables` on today's check over loaded identifiers, per #425. Only the scoped refresh checks every listed identifier, per the interview answer.
    - In task 1.3, have the moved collision function take the identifiers to check. The full path passes the loaded identifiers, and task 3.3 passes every listed identifier.
    - Rename `a_skipped_identifier_collides_like_a_loaded_one` to `a_scoped_refresh_rejects_a_collision_among_listed_identifiers`. Map it to "A partial refresh returns only the requested tables and keeps the others".
    - Replace decision [2]'s Consequences bullet 2 with: "A scoped refresh cannot tell a loaded identifier from a skipped one without loading it, so it checks every listed identifier for a name collision. It therefore fails on a namespace where a skipped identifier collides with another listed one, which a full `REFRESH` accepts."
    - Delete plan.md § Impact bullet 5. Add that sentence to the `REFRESH TABLES` paragraph of task 3.7.
    - Delete "The collision check covers every listed identifier in every path" from the second `[plan-review]` entry.
    - In the "A partial refresh returns only the requested tables and keeps the others" scenario, change "when two listed identifiers flatten to one Exasol name" to "when two listed identifiers, loaded or skipped, flatten to one Exasol name".
  - Option B: keep the wider check on every path.
    - In the collision clause of `vs-adapter/create-virtual-schema-declaration-details` "Multi-level Iceberg namespaces flatten deterministically into Exasol table names" and of `direct-storage/direct-storage-table-discovery` "Table naming and the identifier note round trip reuse the shared helpers", state that a skipped identifier or directory counts.
    - In plan.md § Impact bullet 1, in task 1.11's `docs/install.md` upgrade paragraph, and in decision [1]'s Consequences, state that a virtual schema over such a namespace can be neither refreshed nor recreated after the upgrade until the source table or directory is renamed or removed.
  - Option C: keep `CREATE`, `setProperties`, and a full `REFRESH` unchanged. The scoped refresh selects every listed identifier whose flattened name is requested, loads them, and applies the loaded-only check to the loaded result, so an unrequested collision never fails `REFRESH TABLES`. This option departs from the literal interview answer.
- Escalation: HUMAN. The plan overrides an explicit constraint of #425, and the choice decides which namespaces `CREATE` and `REFRESH` accept.

Other intent checks, no objection: each #425 item has a task or a delta. These are per-table notes (1.2, 1.3), both pushdown paths (1.4, 1.5), the live check first (1.1), no migration or fallback (decision [1]), no absence tests (1.6), and the extensible note (1.2 ignores other members). Each interview answer is covered. These are the `name` key, the hard error naming REFRESH, the scoped refresh with its merge rules (`now_loads`, `still_skipped`, `unrequested`, the dropped entries), and the identifier-only listing that loads only the requested matches (decisions [2] and [5], tasks 2.1 to 3.3).

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `unity-catalog/unity-catalog-client` delta § Scenario "A scoped refresh lists Unity table names without columns and loads only the requested tables", step "with the same neutral table"; plan.md task 3.6
- Issue: The scenario requires the per-table `GET /tables/{full_name}` to yield the same neutral table as the list-sweep entry, including `properties`. A Delta table's column notes read `delta.columnMapping.mode` from `properties`. `notes/planning.md` records only that the per-table GET returns `table_type` and `data_source_format` and answers 404 for a missing table. Task 3.6 asserts that the named table "keeps its columns and identifier note". That check does not compare column notes. If the per-table GET differs in `properties`, a Delta table's column notes would change only under `REFRESH TABLES`, and no test would fail.
- Fix: In task 3.6, read the named table's `SYS.EXA_ALL_VIRTUAL_COLUMNS.ADAPTER_NOTES` before the `REFRESH TABLES` step. Assert that they are byte-identical after it, in both the Unity Catalog and the Glue test.

Other feasibility checks, no objection:
- `TableInfo.columns` is `#[serde(default)]` (`crates/lakehouse-catalog/src/unity/client.rs`), so a response without `columns` still deserializes when `omit_columns=true` is set.
- The Iceberg REST load of listed tables is the existing `resolve_listing`. It already takes an identifier batch, builds one session, and skips only on 404, per ADR `namespace-skip-non-iceberg-on-404-only`.
- Glue's full listing propagates a metadata-file read failure through `try_collect`, so a broken unrequested Iceberg table is observable. The `[COMPLETENESS_GAP]` blocker below relies on that.
- Direct storage's `list_tables` already runs one `list_with_delimiter` before it resolves each directory.
- `FixedCatalogClient` is the only test implementor of `CatalogClient`, so task 2.1 covers every implementor.
- `probe/run.sh` prints checks (a) to (f).
- `speq plan validate change-vs-adapter-per-table-notes` passes.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/refresh-and-set-properties` delta § Scenario "Refresh rebuilds every identifier note and preserves other adapter notes"; same delta § Scenario "A partial refresh returns only the requested tables and keeps the others"
- Issue: The first scenario applies to every `refresh` and promises a note on every virtual table. A scoped refresh cannot keep that promise.
  - The GIVEN reads "a `refresh` request whose `schemaMetadataInfo.adapterNotes` carries the persisted notes". It does not exclude a request that carries `requestedTables`.
  - The step reads "a virtual table that carried no identifier note SHALL carry one after the refresh, so a pushdown on it no longer fails".
  - The partial-refresh scenario requires that `TABLE_TWO` "SHALL keep its previous columns, column notes, and identifier note". The response omits it, so Exasol keeps it as it was.
  - Example: after the upgrade, `REFRESH TABLES T1` writes a note on `T1` only. `T2` still has no note, and its pushdowns still fail. The first scenario requires `T2` to carry a note.
- Fix:
  - In "Refresh rebuilds every identifier note and preserves other adapter notes", change the GIVEN to "a `refresh` request that carries no `requestedTables` and whose `schemaMetadataInfo.adapterNotes` carries ...".
  - In "A partial refresh returns only the requested tables and keeps the others", add the step "a requested virtual table that carried no identifier note SHALL carry one after the statement, and an unrequested virtual table SHALL keep the identifier note it had, or none".
  - In task 3.7, state in the `REFRESH TABLES` paragraph that a `REFRESH TABLES` after the upgrade heals only the named tables, and that the upgrade needs one plain `REFRESH`.
- Escalation: MECHANICAL. Reading the two scenarios settles the conflict.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `catalog/pushdown-catalog-session` delta § Scenario "A scoped refresh lists Iceberg identifiers and loads the requested tables on one session", first THEN step; same delta § Background bullet "The schema loop is NOT the whole request ..."
- Issue: The new step contradicts the feature's own Background and the code.
  - The step reads "the identifier-only listing SHALL issue only the REST catalog's table and namespace listing calls".
  - The Background says that `list_namespace_tables` builds a `RestCatalog` whose client "runs its own `client_credentials` exchange ... and its own `/v1/config` handshake" in OAuth mode, and one extra `/v1/config` lookup in the static-token and no-auth modes.
  - The code agrees with the Background. `list_namespace_tables_unsigned` calls `build_rest_catalog` before `list_tables` (`crates/lakehouse-catalog/src/namespace.rs`).
  - The step is therefore false on every unsigned path. A verifier who checks it against the request log finds a violation.
- Fix: Replace the step with: "the identifier-only listing SHALL issue no `loadTable` GET and SHALL build no resolution `CatalogSession`. Apart from the listing client's own grant and `/v1/config` handshake that this feature's Background records, its requests SHALL be the REST catalog's table and namespace listing calls, whose responses carry table identifiers only".
- Escalation: MECHANICAL. The Background bullet and `namespace.rs` settle the fact.

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: `glue/glue-catalog-client` delta § Background bullet 1; `unity-catalog/unity-catalog-client` delta § Background paragraph
- Issue: Three new Background facts are used by no scenario of their feature.
  1. Glue: "and needs no permission beyond `glue:GetTables`". No scenario of `glue/glue-catalog-client` concerns IAM permissions.
  2. Glue: "`AttributesToGet` must contain `NAME`, and its only other value `TABLE_TYPE` returns Glue's `EXTERNAL_TABLE`, which cannot route a table." This is the rationale for rejecting an option. No scenario step depends on it. plan.md § Context already records it.
  3. Unity Catalog: "OSS Unity Catalog by returning an empty `columns[]`, and each entry still carries `name`, `table_type`, and `data_source_format`". The identifier-only listing scenario returns identifiers only, so no step depends on the empty `columns[]`, on `table_type`, or on `data_source_format`.
- Fix:
  - In the Glue Background bullet 1, delete "and needs no permission beyond `glue:GetTables`". Delete the sentence that starts "`AttributesToGet` must contain `NAME`". If operators need the permission fact, add it to the Glue IAM text of `docs/catalogs.md` in task 3.7.
  - In the Unity Catalog Background, replace "Both servers honor the three parameters, OSS Unity Catalog by returning an empty `columns[]`, and each entry still carries `name`, `table_type`, and `data_source_format` (verified live ...)" with "Both servers honor the three parameters and keep each entry's `name` (verified live against `demo_sales_catalog.sales` and OSS Unity Catalog v0.5.0, #425)."
- Escalation: MECHANICAL. Each fact can be checked against its own spec's scenarios.

#### [COMPLETENESS_GAP] BLOCKER
- Location: plan.md § Verification bullet 2 ("is proved for every kind by the pure selection test"); § Scenario Coverage rows "A partial refresh loads the metadata of the requested tables only", "A scoped refresh lists Iceberg identifiers and loads the requested tables on one session", "A scoped refresh lists Unity table names without columns and loads only the requested tables", and "A scoped refresh lists Glue table names and loads only the requested tables"
- Issue: For three of the four kinds, nothing proves the requirement the user added after round 1: "never per-table metadata for the whole namespace".
  - The plan says `only_requested_identifiers_are_selected_for_loading` proves the clause "for every kind". That pure test proves only that the adapter asks for the right identifiers. It cannot fail when a kind's `load_listed_tables` loads more than it is given, for example by calling `list_tables` and filtering the result. That shape returns correct rows, so `refresh_tables_refreshes_only_the_requested_tables` and the extended Unity and Glue tests stay green.
  - Each per-kind scenario has a step that no test covers: "issue one `loadTable` GET per given identifier ... and none for any other table" (Iceberg REST), "none for any other entry" (Unity Catalog), and "`GetTable` once per given identifier and for no other table" (Glue). `specs/testing.md` § Testing strategy rules out asserting on a fake's request log. The only remaining proof is one that Exasol SQL can observe: an unrequested table whose load fails does not fail `REFRESH TABLES`.
  - That proof is available for Glue. `GlueCatalogSession::list_tables` fails when an Iceberg table's metadata file cannot be read. It is likely available for Iceberg REST, if the local `apache/iceberg-rest-fixture:1.10.1` answers a corrupt metadata file with a non-404 status. Only direct storage has it today (`refresh_tables_reads_no_unrequested_directory`).
- Fix:
  - Delete the claim in plan.md § Verification bullet 2 that the selection test proves the clause for every kind. State that the selection test proves the adapter's selection, and that each kind's E2E proves that kind's load.
  - Glue: add a task for a new Glue E2E test outside the read-only runner, `glue_refresh_tables_ignores_a_broken_unrequested_table`. After `CREATE`, it registers a Glue Iceberg table whose `metadata_location` names a missing object. It asserts that a full `REFRESH` fails naming that table, and that `REFRESH TABLES` of a fixture table succeeds. It then deletes the table. Map it to the Glue row and to "A partial refresh loads the metadata of the requested tables only".
  - Iceberg REST: first confirm against the local REST fixture that `loadTable` on a table whose current metadata file holds invalid JSON returns a non-404 status. If it does, add a step to `refresh_tables_refreshes_only_the_requested_tables`. The step corrupts the metadata file of a third table in `NS_PARTIAL`, asserts that a full `REFRESH` fails, and asserts that `REFRESH TABLES TABLE_ONE` succeeds. If the fixture answers 404, state in § Verification that the Iceberg REST "none for any other table" step has no test and holds because `load_listed_tables` is `resolve_listing` over the given identifiers. Also map `enumeration_builds_exactly_one_session` to the Iceberg row for the one-session step.
  - Unity Catalog: state in § Verification that the "none for any other entry" step has no test. Exasol SQL cannot observe it, because no Unity Catalog table can fail the one-sweep full listing. The step holds because `load_listed_tables` issues `get_table_info` once per given identifier.
- Escalation: MECHANICAL. The codebase settles which failures each kind can produce, and the planner can verify the REST fixture's status code.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: `unity-catalog/unity-catalog-create-virtual-schema` delta § Background paragraph, sentence "That client MUST NOT issue a per-table `GET /tables/{full_name}` to obtain columns or properties"
- Issue: The sentence is unscoped and binds the whole client. The load of listed tables in `unity-catalog/unity-catalog-client` issues exactly that request to obtain columns and properties. The surrounding text concerns enumeration during createVirtualSchema, so a careful reader resolves it, but the delta already rewrites this paragraph.
- Fix: Change the sentence to "During the full listing, that client MUST NOT issue a per-table `GET /tables/{full_name}` to obtain columns or properties".

Other requirement checks, no objection: every recorded `TABLE_MAP` mention outside `_recorded` lies in a block one of the 16 deltas changes or removes. The merge scenario now names both loaded and skipped entries as listed ("the identifier listing returns its catalog identifier"). The direct-storage "only reason to skip" clause does not conflict with the scoped refresh, because a scoped refresh never resolves an unrequested directory.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Implementation Tasks; plan.md § Parallelization ("Group B starts after group A merges"); plan.md task 1.1
- Issue: The plan bundles two deliverables that can ship separately. A split would remove its most fragile step.
  - Group A (tasks 1.1 to 1.12) is #425 as written. Group B (tasks 2.1 to 3.7) is the scoped refresh and the identifier-only listing. Group B adds three deltas, decisions [2], [3], and [5], three `[expert]` tasks, and a new `CatalogClient` surface across four implementors.
  - Task 1.1 needs a revert list of about a dozen artifacts for the case where the probe stops group B. That list exists only because group B is in the same plan. If group B were a follow-up plan, a failed probe would just mean "do not start plan 2".
  - "Group B starts after group A merges" has no meaning inside one plan that `/speq:implement` runs, because no merge happens between groups.
  - Group A on its own keeps today's `REFRESH TABLES` behavior, which acts as a full refresh (probe check from planning: a full table list with `requestedTables` is applied whole). It is therefore a consistent intermediate state.
  - A split has costs. The `vs-adapter/refresh-and-set-properties` delta splits across two plans, and "Refresh echoes requestedTables when present" stays recorded until plan 2.
- Fix: Ask the requester whether to split. If yes, keep group A as this plan and move sections 2 and 3, their deltas, decisions [2], [3], and [5], and the group B rows of § Scenario Coverage into a new plan. List that plan under a `## Follow-up plans` heading in plan.md, and cut task 1.1's revert list to "stop and do not start the follow-up plan". If no, replace "Group B starts after group A merges" with "Group B starts after every group A task is checked off".

Other task-breakdown checks, no objection:
- All 41 NEW and CHANGED delta scenarios have a § Scenario Coverage row.
- All 19 new test names are created by a task, and every cited existing test exists (`git grep "fn <name>"`).
- Tasks 2.3, 2.4, and 3.2 carry `[expert]`.
- Group B declares the files it shares with group A.
- Group B is one vertical slice. Section 2 has no consumer outside section 3, so it is not a layer slice.

## Design Depth

no objection, axis checked:
- `[ADR_OVERPROMOTION]`: decisions [1] and [2] are `yes`. Each supersedes an accepted ADR, names a rule-2 criterion, and states the digest result. Decisions [3], [4], and [5] are `no`.
- `[ARCHITECTURE_DRIFT]`: the BASE equals `git hash-object specs/architecture.md` (`26ec685b...`). Diffing each CHANGED block against the current section shows only the intended lines. Decision [2] names Components and Data Flow. The delta adds the `REFRESH TABLES` data-flow line, the vs-adapter `REFRESH TABLES` clause, and the two new `CatalogClient` operations on the lakehouse-catalog line.
- `[ADR_CONFLICT]`: `shared-catalog-client-trait-neutral-types` ("a list operation and a single-table load operation") is superseded by `catalog-table-neutral-format-tag-opaque-vending-key`, whose Decision does not fix the method set. The two new trait methods therefore conflict with no accepted ADR. The Iceberg load of listed tables keeps `namespace-skip-non-iceberg-on-404-only`. The legacy map ADR's "`__` name collisions fail at create time" holds under every option of the Intent Fidelity blocker.
- Module depth: `scoped_refresh.rs` owns the one requested-name test, which resolves the round-1 leakage advisory. `skipped_tables.rs` owns the `SKIPPED_TABLES` format, and decision [5] gives each kind one classification function. The three new adapter modules perform no I/O.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: `vs-adapter/refresh-and-set-properties` delta § Background bullet 2
- Issue: The bullet says the refresh path reuses "the `createVirtualSchema` path verbatim; the only differences are the request `type` recognised, the merge precedence of the incoming properties, the response `type` label, and the `requestedTables` scoping and echo". A refresh that carries `requestedTables` also uses a different listing, a load of the requested tables, and the `SKIPPED_TABLES` merge. "Verbatim" and "the only differences" are therefore inaccurate.
- Fix: Replace the clause after the semicolon with: "the differences are the request `type` recognised, the merge precedence of the incoming properties, the response `type` label, the `requestedTables` echo, and, for a refresh that carries `requestedTables`, the identifier-only listing, the load of the requested tables, and the `SKIPPED_TABLES` merge."
