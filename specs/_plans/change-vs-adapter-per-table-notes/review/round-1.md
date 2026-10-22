# Plan Review Findings: change-vs-adapter-per-table-notes (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 15 (Blockers: 5, Advisory: 10)
- Intent Fidelity blockers: 1
- Human-escalation blockers: 1

## Premortem

Six months from now this plan failed. Three stories:

1. **The upgrade path was never tested.** Operators re-run `install.sh`, which replaces the adapter script with `CREATE OR REPLACE`. Every existing virtual schema then fails each pushdown. `docs/install.md` names no REFRESH step. The plan proved the no-note error and the REFRESH repair only with unit tests, because it believed Exasol SQL cannot reach a table without a note. Exasol sends `"adapterNotes": null` on that path, and no test ever ran it. Routed to `[UNSTATED_ASSUMPTION]` and `[NFR_IGNORED]`.
2. **SKIPPED_TABLES drifts from what the user asked for.** After `REFRESH TABLES`, skip entries of unrequested tables disappear, against the interview answer "keep unrequested entries" and against the plan's own Impact text. On a capped list, `SKIPPED_TABLES_OMITTED` rises with each repeated `REFRESH TABLES`. The implementer meets two contradicting cap rules, one in the adapter-notes spec and one in the refresh spec. Routed to `[INTENT_DRIFT]`, `[REQUIREMENT_CONFLICT]`, and `[COMPLETENESS_GAP]`.
3. **The probe gate flips and the specs describe behavior that never ships.** Task 1.1 stops group B. The recorded specs still hold the partial-refresh scenarios, decision [2] (an ADR candidate), and the REFRESH TABLES architecture line. The only user documentation of group A's breaking change was in group B's docs task. Routed to `[HIDDEN_DEPENDENCY]` and `[TRACEABILITY_GAP]`.

## Intent Fidelity

#### [INTENT_DRIFT] BLOCKER
- Location: decision-log.md § Design Decisions [3]; `vs-adapter/refresh-and-set-properties/spec.md` § Scenario "A partial refresh merges SKIPPED_TABLES with the persisted list" (the survival rule and the `dropped_b` clause); plan.md task 2.1; plan.md § Impact bullet 3
- Issue: The plan overrides an explicit interview answer.
  - The interview answer reads: "merge SKIPPED_TABLES (keep unrequested entries, drop a requested table that now loads, add or update a requested table still skipped)".
  - Decision [3] reads: "A persisted entry whose table the source no longer lists is dropped, whether or not the request names it". The scenario accordingly drops the unrequested `dropped_b`.
  - The stated reason for the deviation is false. Decision [3] says that rebuilding an Exasol name from a stored identifier "would need a per-kind parse that is ambiguous for a direct-storage directory name containing a dot". The adapter knows `CATALOG_KIND` at refresh time, and the per-kind parses already exist in `adapter/pushdown/scan_resolution.rs`: `parse_table_ident`, `unity_table_ident`, `parse_glue_table_ident`, and `direct_storage_directory`. `direct_storage_directory` takes the whole string and never splits on a dot. The direct-storage delta itself records that the identifier is the bare directory name, never split. The literal answer is therefore buildable, and the deviation is a preference, not a constraint.
  - The plan contradicts itself. plan.md § Impact says: "After a partial refresh, SKIPPED_TABLES keeps the entries of unrequested tables". That is the literal rule, which the spec does not implement.
  - Reviewer's judgment on the refinement itself: it is defensible. A dropped entry describes a source table that no longer exists and that never had a virtual table. A full REFRESH drops the same entry. Listing order is defined only for entries the fresh listing still returns. The refinement still changes what a user reads in `SKIPPED_TABLES`, against an explicit answer, so the requester decides.
- Fix: Ask the requester to choose between two options.
  - Option A, which the reviewer recommends: keep decision [3]. Record the user's approval as an `## Interview` line in decision-log.md. Rewrite decision [3]'s Rationale to the real reasons: the entry describes a source table that no longer exists, a full REFRESH drops it too, and listing order is defined only for listed entries. Delete the parse-ambiguity sentence and the matching Alternatives text. Change plan.md § Impact bullet 3 to "keeps the entries of unrequested tables the source still lists".
  - Option B: implement the literal answer. In task 2.1, rebuild the Exasol name of each unlisted persisted entry with the existing per-kind parse for the schema's `CATALOG_KIND`, followed by `flatten_table_name`. Keep each unlisted entry whose name is not requested, after the listed entries and in persisted order. Change the scenario so that `dropped_b` survives. Rename `merge_drops_entries_the_source_no_longer_lists` to match. Delete decision [3].
- Escalation: HUMAN. The plan replaces the requester's explicit merge rule, and only the requester can accept the substitute.

Other intent checks, no objection: each #425 item has a task or a delta. The items are per-table notes (1.2, 1.3), both pushdown paths (1.4, 1.5), the full-listing collision check (1.3, 2.2), the live verification first (1.1), no migration and no fallback (decision [1]), no absence tests (1.6), and the extensible note (1.2 ignores other members). The interview answers are also covered: the `name` key, a hard error naming REFRESH, partial-refresh scoping, `now_loads`, and a requested table the catalog dropped.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Verification › Scenario Coverage rows "Refresh rebuilds every identifier note and preserves other adapter notes", "Pushdown derives the scanned Iceberg table from the involved virtual table", and "Each join leg resolves its catalog identifier from its own identifier note" (each annotated "unreachable from Exasol SQL"); plan.md task 1.7
- Issue: The plan proves the no-note clauses only with unit tests. It rests that choice on the belief that no Exasol SQL reaches a virtual table without an identifier note. The belief is false, and the clauses it covers are the plan's headline breaking change.
  - The reviewer probed the running Docker Exasol 2025.1.16. A Python3 adapter script created a virtual schema whose table had no table-level note. `CREATE OR REPLACE ... ADAPTER SCRIPT` under the same name then swapped the adapter. The next `SELECT` sent `involvedTables: [{"name": "T1", "adapterNotes": null}]`. `ALTER VIRTUAL SCHEMA ... REFRESH` then wrote the note, and the next pushdown carried it.
  - This is the path the documented upgrade takes. `docs/install.md` says the installer "uses `CREATE OR REPLACE`" and tells the user to "Run it again on a prior install to upgrade it".
  - `specs/testing.md` § Coverage rule allows a unit-only proof only for "A scenario whose state no well-formed Exasol request can reach".
  - Task 1.7's unit test cannot prove the clause "a virtual table that carried no identifier note SHALL carry one after the refresh". A `refresh` request carries no virtual tables. The probe saw only `schemaMetadataInfo.adapterNotes`, `schemaMetadataInfo.name`, and `requestedTables`. The test premise "a refresh request whose virtual tables carry no notes" therefore has nothing to set.
- Fix:
  - Add a group A task for a local E2E test, for example `an_upgraded_schema_fails_naming_refresh_until_refreshed` in `tests/e2e_local/refresh.rs`. The test creates a virtual schema under a test-local adapter script that writes no table-level notes. It then replaces that script with the engine adapter through `CREATE OR REPLACE ... ADAPTER SCRIPT` under the same name. It asserts that a single-table query and a join query fail with the error that names the table and `ALTER VIRTUAL SCHEMA ... REFRESH`. It runs `ALTER VIRTUAL SCHEMA ... REFRESH` and asserts that both queries return rows.
  - Map the test in § Scenario Coverage to the three scenarios above. Delete the three "unreachable from Exasol SQL" annotations.
  - In task 1.2, list an `involvedTables` entry whose `adapterNotes` is JSON `null` as a missing-note input.
  - Remove task 1.7's premise "whose virtual tables carry no notes", or retarget the test at what a unit test can assert.
- Escalation: MECHANICAL. The reviewer settled the fact against the local Docker Exasol, and the planner can reproduce it.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md task 1.1
- Issue: Task 1.1 says "If check (b) shows Exasol dropping T1 or T3, stop group B and keep the full refresh". It names nothing to do about the artifacts that already describe scoped refresh:
  - the partial-refresh scenarios and Background lines in `vs-adapter/refresh-and-set-properties`
  - the "no `requestedTables`" scoping in `vs-adapter/create-virtual-schema-adapter-notes` and `vs-adapter/column-source-notes`
  - the removal of "Refresh echoes requestedTables when present"
  - decision [2], which is an ADR candidate
  - the architecture delta's REFRESH TABLES data-flow line and vs-adapter component text

  If the plan is recorded after group B stops, these specs and the ADR describe behavior that never ships.
- Fix: Extend task 1.1 so that a failed check (b) stops implementation and returns the plan to planner-agent. Planner-agent then reverts each artifact listed above before any implementation continues.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Impact; plan.md task 2.6
- Issue: The upgrade breaks every existing virtual schema, yet the plan leaves the upgrade instructions unchanged.
  - The upgrade paragraph in `docs/install.md` ("Run it again on a prior install to upgrade it") gets no REFRESH step. Task 2.6 changes only `docs/catalogs.md`.
  - The plan does not cover a downgrade. An older adapter reads `TABLE_MAP`. A schema created after the upgrade has no `TABLE_MAP`. A schema created before the upgrade keeps its pre-upgrade `TABLE_MAP`, frozen and possibly stale.
- Fix: In the docs task, add two statements to the upgrade paragraph of `docs/install.md`. First, each existing virtual schema needs one `ALTER VIRTUAL SCHEMA <name> REFRESH` after the upgrade. Second, a downgrade needs one REFRESH under the older adapter.

#### [NFR_IGNORED] ADVISORY
- Location: decision-log.md § Design Decisions [2] Consequences; plan.md § Impact
- Issue: `REFRESH TABLES` runs the full enumeration. A non-404 load failure or a column-note size error on any unrequested table therefore aborts it. Decision [2] names only the enumeration cost. Users often reach for `REFRESH TABLES` to work around one broken table.
- Fix: Add one sentence to decision [2]'s Consequences and to plan.md § Impact: "A non-404 load failure or a column-note error on any listed table also aborts REFRESH TABLES, whether or not the request names that table."

Other feasibility checks, no objection:
- The reviewer confirmed against Docker Exasol 2025.1.16 that a `refresh` with `requestedTables` carries `schemaMetadataInfo.adapterNotes`, including the persisted `SKIPPED_TABLES`. The input the merge needs therefore exists.
- `probe/run.sh` checks (a) to (e) match the Context bullets.
- Every existing test the plan cites exists. The reviewer checked each name with `git grep "fn <name>"`.
- The task list covers every `TABLE_MAP` code and docs hit that `git grep` returns.
- Security is unchanged. Identifiers still come only from notes the adapter writes, and the Lakekeeper gate still admits each identifier at pushdown (`TableScanResolver::admit`).

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/create-virtual-schema-adapter-notes/spec.md` § Scenario "A skipped-table list too long for adapterNotes is capped to the longest prefix that fits"; `vs-adapter/refresh-and-set-properties/spec.md` § Scenario "A partial refresh merges SKIPPED_TABLES with the persisted list"
- Issue: The two scenarios set different rules for the same partial refresh.
  - The cap scenario's WHEN is "createVirtualSchema or a refresh completes", which includes a refresh that carries `requestedTables`.
  - For that refresh, the cap scenario requires three things. `SKIPPED_TABLES` holds "the longest prefix of the skipped entries" of the listing. `SKIPPED_TABLES_OMITTED` holds "the number of entries the prefix drops". "A listing whose every entry fits SHALL carry no `SKIPPED_TABLES_OMITTED`".
  - The merge scenario requires the merged list instead, and it requires that "`SKIPPED_TABLES_OMITTED` SHALL hold the persisted count plus the number of entries this cap drops".
  - Example: the persisted count is 5 and the merged list fits. The cap scenario requires no `SKIPPED_TABLES_OMITTED` entry. The merge scenario requires the value 5.
  - The plan already scoped this scenario's last clause and two sibling scenarios to "a refresh that carries no `requestedTables`", but it left this WHEN unscoped.
- Fix:
  - Change the cap scenario's WHEN to "createVirtualSchema, setProperties, or a refresh that carries no `requestedTables` completes".
  - In the merge scenario, state that the cap applies to the merged list.
  - In the merge scenario, state whether `SKIPPED_TABLES_OMITTED` is absent when the sum is 0.
- Escalation: MECHANICAL. Reading the two scenarios settles the conflict.

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: `vs-adapter/create-virtual-schema/spec.md` § Background (two new bullets); `vs-adapter/create-virtual-schema-adapter-notes/spec.md` § Background
- Issue: Three new Background facts are used by no scenario of the same feature after the merge.
  1. create-virtual-schema: "for a single-table request and for each join leg. A self-join lists the table once per leg (verified live on Exasol 2025.1.16 and 8.29.13, #425)". No create-virtual-schema scenario has a join or a self-join. The fact is a step of the new scenario in `pushdown-joins/pushdown-planning-join-fallback`.
  2. create-virtual-schema: "A virtual schema created before identifier notes existed carries none. No migration path and no fallback exist: `ALTER VIRTUAL SCHEMA ... REFRESH` writes them (#425)". No create-virtual-schema scenario starts from an existing schema. The fact already appears in two other places: the GIVEN of "Refresh rebuilds every identifier note and preserves other adapter notes" ("or none because the virtual schema predates identifier notes") and the error step of `file-planning/pushdown-planning-file-resolution`.
  3. create-virtual-schema-adapter-notes: "Each virtual table's catalog identifier lives in that table's own identifier note, per `vs-adapter/create-virtual-schema`". No scenario of this feature names an identifier note.
- Fix:
  - In the create-virtual-schema Background, delete the clause "for a single-table request and for each join leg" and the self-join sentence.
  - Delete the "created before identifier notes existed" bullet from the create-virtual-schema Background.
  - Delete the identifier-note sentence from the create-virtual-schema-adapter-notes Background.
- Escalation: MECHANICAL. Each fact can be checked against the scenarios of its own spec.

#### [COMPLETENESS_GAP] BLOCKER
- Location: `specs/mission.md` § Domain Glossary, row "AdapterNotes"; plan.md § Implementation Tasks
- Issue: #425 says: "Remove `TABLE_MAP` from the specs rather than documenting it as deprecated". The glossary row still defines AdapterNotes as "State that Exasol persists for a virtual schema between CREATE and pushdown: table map, skipped tables, and tuning values". No task changes the row. The map therefore survives in the mission, which every agent loads as a project instruction.
- Fix: Add a group A task that replaces "table map, skipped tables, and tuning values" in that glossary row with "skipped tables and tuning values at schema level, one identifier note per virtual table, and one note per column".
- Escalation: MECHANICAL. A one-line edit that the issue text already decides.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `vs-adapter/refresh-and-set-properties/spec.md` § Scenario "A partial refresh merges SKIPPED_TABLES with the persisted list", last step; decision-log.md [3] Consequences; plan.md task 2.1
- Issue: The rule "persisted count plus the number of entries this cap drops" adds to the count every time it runs.
  - Take a list at the cap, and a requested table R that is still skipped and sits past the cap in listing order.
  - R is already part of the persisted count. Its fresh entry is dropped again, so the count rises by one on each `REFRESH TABLES R` until the next full REFRESH.
  - Decision [3] says "It can overcount when a requested table was among them". It does not say that the overcount grows with each repetition.
- Fix: Choose one of two changes. First option: state in the merge scenario, in decision [3]'s Consequences, and in the "Reading SKIPPED_TABLES" docs that after a partial refresh `SKIPPED_TABLES_OMITTED` is an upper bound, which can grow with each `REFRESH TABLES` of a capped table that is still skipped, until the next full REFRESH. Second option: define a carry rule that does not count such an entry again, and add a unit test for it.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `vs-adapter/refresh-and-set-properties/spec.md` § Scenario "A partial refresh merges SKIPPED_TABLES with the persisted list", the survival step; plan.md task 2.1
- Issue: The survival rule says "this enumeration still lists its catalog identifier", but the text uses "list" in two senses.
  - A skipped entry counts as listed: `still_skipped` and `unrequested` survive.
  - The text also contrasts "the source no longer lists" with "the listing now skips".
  - In code, `CatalogListing` splits into `tables` (loaded) and `skipped`.

  An implementer can match persisted entries against `listing.tables` alone and drop every unrequested entry that is still skipped. The `unrequested` example fixes the intended reading, but the rule itself does not state it.
- Fix: In the scenario step and in task 2.1, replace "lists its catalog identifier" with "returns its catalog identifier as a loaded table or as a skipped entry".

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Scenario Coverage rows "A partial refresh drops a requested virtual table the source no longer serves" and "A partial refresh returns only the requested tables and keeps the others"; plan.md tasks 2.3 to 2.5
- Issue: Two clauses lack a suitable test.
  - The clause "a requested table that the enumeration lists but skips SHALL also be omitted from the response, so Exasol drops its virtual table, and its skip entry SHALL be recorded" has no test at any layer. `refresh_tables_drops_a_requested_table_the_catalog_dropped` covers a catalog drop and an unmatched name. The merge E2E test leaves `NEWLY_BROKEN` unrequested and requests no table that both has a virtual table and is now skipped.
  - The clause about a collision among unrequested tables has only a unit test. The existing E2E test `refresh_flatten_collision_returns_same_error_as_create` already reaches a collision.
- Fix:
  - In task 2.5, add a step that turns a loaded directory into a skipped one, requests that directory, and asserts that its virtual table is gone and its skip entry is present.
  - In task 2.4, extend `refresh_flatten_collision_returns_same_error_as_create` with a `REFRESH TABLES` step that names a table that does not collide.
  - Keep `partial_refresh_fails_on_a_collision_among_unrequested_tables` only if it tests logic that the E2E test does not cover, per the one-layer rule in testing.md.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md task 2.6; plan.md task 1.2; plan.md § Parallelization
- Issue: Two task placements break the link between tasks and the things they cover.
  - Task 2.6 documents group A's breaking change: "the per-table identifier in `SYS.EXA_ALL_VIRTUAL_TABLES.ADAPTER_NOTES`, and the one `REFRESH` an existing virtual schema needs after the upgrade". It sits in group B, which task 1.1 can cancel. Group A would then ship a breaking change with no user documentation.
  - Task 1.2 names neither of the unit tests that § Scenario Coverage assigns to it: `a_missing_or_unreadable_note_fails_naming_the_table_and_refresh` and `members_other_than_name_are_ignored`.
- Fix: Split task 2.6. Move the identifier-note and upgrade-REFRESH documentation, together with the `docs/install.md` change from the NFR advisory above, into a new group A task. Keep the paragraphs on REFRESH TABLES scoping and the SKIPPED_TABLES merge in group B. Name the two `table_identity_tests.rs` tests in task 1.2.

Other task-breakdown checks, no objection: the groups run in sequence, and the plan declares the files they share. Task 2.1 carries the [expert] tag. Every new test name in § Scenario Coverage is created by a task.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: decision-log.md [4]; plan.md tasks 2.1 and 2.2
- Issue: Two modules decide on their own whether a table is requested.
  - Task 2.2 filters `tables[]` in `handle_create_virtual_schema` by "Exasol name is requested".
  - Task 2.1's merge in `skipped_tables.rs` checks that "the `flatten_table_name` of the listed identifier is not requested".
  - Both rely on one unstated convention: an exact, case-sensitive comparison against `requestedTables`. If one module later folds case or strips quotes and the other does not, a table's virtual table and its skip entry disagree.
- Fix: Give the requested-name test one owner, for example a value built once from `requestedTables` with a single `contains(exasol_name)` method. Make both the response filter and the merge call it, and name it in tasks 2.1 and 2.2.

Other design-depth checks, no objection:
- `[ADR_OVERPROMOTION]`: decisions [1] and [2] are `yes`. Each supersedes an accepted ADR, which rule 5 requires to be promoted. Each Rationale names a rule-2 criterion and the digest result, and neither Decision holds a path, signature, or flag. Decisions [3] and [4] are `no`.
- `[ARCHITECTURE_DRIFT]`: the BASE equals `git hash-object specs/architecture.md` (`26ec685b...`). The reviewer diffed each CHANGED block against the current section, and only the intended lines differ. Decision [1] names Components, Data Flow, Interfaces, and Constraints, and the delta has all four. Decision [2] names Components and Data Flow, and both are present. `skipped_tables.rs` lies inside the vs-adapter component, which still "owns: schema-level adapterNotes (SKIPPED_TABLES, tuning values)".
- `[ADR_CONFLICT]`: `vs-refresh-reuses-create-virtual-schema-enumeration` and `persist-exasol-name-to-iceberg-identifier-map-in-adapternotes-strategy-b` are superseded. The plan conforms to `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` and to `unity-table-identity-recovered-by-splitting-dotted-identifier`, because the note keeps the dotted identifier. `table-declared-schema-persisted-per-column-in-adapter-notes` is unaffected.
- Module depth and boundaries: `table_identity` hides the key name and the error text from one writer and two readers. Both new modules perform no I/O.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Impact bullet 2
- Issue: "Run a plain `REFRESH` to pick up new tables or to refresh everything" implies that `REFRESH TABLES` cannot add a table. Probe check (d) and the merge scenario, where `NOW_LOADS` is created, show that a `REFRESH TABLES` naming a new table creates it.
- Fix: Rewrite the sentence as: "A `REFRESH TABLES` creates a named table that has no virtual table yet. Run a plain `REFRESH` to pick up every new table or to refresh everything."

#### [PROSE_BLOAT] ADVISORY
- Location: deltas `vs-adapter/catalog-kind-selection`, `glue/glue-table-planning`, `direct-storage/direct-storage-table-discovery`, and `unity-catalog/unity-catalog-create-virtual-schema`
- Issue: Four deltas repeat the parenthetical "(its table-level `adapterNotes`, a JSON object whose `name` member holds the catalog identifier)". The Unity Background states the `name` member twice in one sentence: "records each virtual table's catalog identifier in the `name` member of that table's identifier note (its table-level `adapterNotes`, a JSON object whose `name` member holds the catalog identifier)". `vs-adapter/create-virtual-schema` already defines the term.
- Fix: Replace each parenthetical with "per `vs-adapter/create-virtual-schema`". Shorten the Unity sentence to: "The adapter records each virtual table's catalog identifier in that table's identifier note per `vs-adapter/create-virtual-schema`."
