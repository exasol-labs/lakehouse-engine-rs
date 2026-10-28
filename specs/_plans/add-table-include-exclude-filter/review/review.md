# Plan Review Findings: add-table-include-exclude-filter

## Summary
- Axes checked: 6/6
- Total findings: 9 (Blockers: 1, Findings: 8)
- Intent Fidelity blockers: 0

Ways this plan may fail:

1. Under Unity Catalog and Glue, the table-filter MUST NOT on loading a hidden table's metadata cannot hold. Decision [1] keeps the single `GET /tables` sweep and the full `GetTables` records, and both return a hidden table's metadata with its name. The implementer must then either break the spec or drop decision [1]. Routed to Requirement Quality (BLOCKER).
2. Four `list_tables` implementations each copy the rule "apply the predicate before any load", and one of them sits outside `lakehouse-catalog`. If one implementation, or a future one, applies the predicate after its load, a hidden table fails the statement under that catalog kind. Routed to Design Depth.
3. A user pattern with many `*` can make a recursive backtracking matcher run in exponential time against a 128-character name, so `CREATE` or `REFRESH` hangs. Routed to Feasibility.

## Intent Fidelity
no objection — axis checked: every acceptance criterion of #544 maps to a task. Validation is tasks 3.1 and 5.4. Exclude-wins is `include_and_exclude_choose_the_exposed_tables`. Not loading hidden tables is `hidden_iceberg_table_is_never_loaded`, `hidden_directories_are_never_read_or_recorded_as_skipped`, and `check_table_exclude_hides_the_broken_table`. `SET` is `set_properties_relists_the_exposed_tables`. `REFRESH TABLES` is `refresh_tables_cannot_add_back_a_hidden_table`. The E2E tests cover include only, exclude only, both, and the sub-namespace pattern (tasks 5.2 and 5.5). `docs/catalogs.md` is task 6.1. "Only an empty entry is invalid" and "a hidden requested name is dropped" follow the interview answers. The Unity Catalog and Glue E2E tests trace to "This works for every `CATALOG_KIND`". No scope creep found.

## Feasibility

#### [NFR_IGNORED] FINDINGS
- Location: plan.md § Implementation Tasks, task 2.1 and task 2.2
- Issue: Task 2.1 asks only for "a backtracking loop over `char`s" and states no complexity bound. A recursive or multi-point backtracking matcher runs in exponential time on a user-controlled pattern such as `*A*A*A*A*A*A*A*B` against a 128-character name. The adapter would then hang `CREATE`, `REFRESH`, and `SET`. Task 2.2 tests correctness only (`A*B*C` against `AXBYBZC`), so a slow matcher passes it.
- Fix: In plan.md task 2.1, require the iterative matcher that keeps one backtrack point, the last `*`. It runs in O(name length × pattern length) time and uses no recursion. In task 2.2, add a unit test that matches `*A*A*A*A*A*A*A*A*A*A*B` against a name of 128 `A`s and expects `false`, so an exponential matcher fails the test by timeout.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] [REQUIREMENT_CONFLICT] BLOCKER
- Location: specs/_plans/add-table-include-exclude-filter/vs-properties/table-filter/spec.md § Scenario "A hidden table is never loaded and never recorded as skipped", second AND step; plan.md § Summary
- Issue: The step reads "the adapter SHALL decide each table from its listed identifier before it loads the metadata of any table, and MUST NOT load the metadata of a hidden table". The plan's own design cannot meet this for two catalog kinds. Decision [1] keeps the Unity Catalog full listing on the single paginated `GET /tables` sweep, which returns every table's columns and properties, hidden tables included. The recorded `unity-catalog/unity-catalog-create-virtual-schema` Background says the same: "Every listed table's column metadata and `properties` come from the single paginated `GET /tables` list sweep". The Glue full listing reads whole table records from `GetTables`. Under both kinds, a hidden table's identifier and catalog metadata arrive in the same response, so neither "before it loads the metadata of any table" nor "MUST NOT load the metadata of a hidden table" has a pass/fail test. A code reviewer who reads the MUST NOT literally would flag the Unity Catalog implementation. The only way to comply literally is alternative (a), which decision [1] rejects. plan.md § Summary repeats the ambiguity: "before any table metadata loads".
- Fix: In table-filter/spec.md, replace that AND step with: "*AND* the adapter MUST NOT send a request or read an object for a hidden table beyond the namespace listing that names it: no Iceberg `loadTable`, no Unity Catalog `GET /tables/{full_name}`, no Glue `GetTable`, no Iceberg metadata-file read, and no read below a hidden direct-storage directory. The adapter MUST NOT admit, skip, or declare a hidden table, so a hidden table cannot fail the statement". In plan.md § Summary, replace "before any table metadata loads" with "before any per-table catalog request or storage read". Use the same wording in the `architecture.md` § Data Flow line "before any table loads", so that the sweep under Unity Catalog and Glue is not read as a load.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: decision-log.md § Design Decisions [3]; specs/_plans/add-table-include-exclude-filter/vs-properties/table-filter/spec.md § description
- Issue: Decision [3] gives this reason: "The listing scenarios set no filter in their GIVEN steps, so they stay true as written". The GIVEN steps do not exclude a filter, they do not mention one. A request with `NAMESPACE` and `TABLE_EXCLUDE` satisfies the GIVEN of the recorded `vs-adapter/create-virtual-schema` scenario "Create virtual schema enumerates every table in the configured namespace". Its THEN step still requires "one virtual table per discovered Iceberg table", and that feature's description says "registers every Iceberg table in a configured namespace". A reader of the recorded feature finds no pointer to the filter. The narrowing list in table-filter's description also omits `vs-properties/create-virtual-schema-adapter-notes`. That feature's scenario "Every skipped table is recorded with its reason under every catalog kind" is the one this plan narrows for `SKIPPED_TABLES`.
- Fix: Add `vs-properties/create-virtual-schema-adapter-notes` to the narrowing list in table-filter/spec.md's description. Add a DELTA:CHANGED Background to `vs-adapter/create-virtual-schema` with one new bullet: "`vs-properties/table-filter` narrows every listing of this feature to the tables `TABLE_INCLUDE` and `TABLE_EXCLUDE` expose". A pointer does not copy the rule. Then list the new delta in plan.md § Features. In decision-log.md [3], replace the inaccurate sentence with "Each listing feature gets no rule copy, only a pointer where its description claims every table".

#### [COMPLETENESS_GAP] FINDINGS
- Location: specs/_plans/add-table-include-exclude-filter/vs-properties/property-value-validation/spec.md § description and the new Background bullet
- Issue: The delta adds a Background bullet saying that the feature's one error reports a table pattern rejection. The description still says the feature rejects "a tuning property value", and the lifecycle scenario "Set properties validates the merged properties after null unsets" still names only "a tuning property". The feature now owns an error that carries a non-tuning value, and its own description and scenarios do not say so.
- Fix: In the property-value-validation delta, add the description to the DELTA:CHANGED scope. Change its first sentence to "reject a tuning property value, or a `TABLE_INCLUDE` or `TABLE_EXCLUDE` value that `vs-properties/table-filter` rejects, with one error ...". Leave the scenarios unchanged, because the table-filter scenario "An empty pattern entry fails the statement" covers the `SET` lifecycle.

## Task Breakdown

#### [TRACEABILITY_GAP] FINDINGS
- Location: table-filter/spec.md § Scenario "A hidden table is never loaded and never recorded as skipped", third step; plan.md § Verification, Scenario Coverage
- Issue: The step "the adapter SHALL write no skip warning line for it" has no proof. An E2E test cannot observe UDF warning lines without `SCRIPT_OUTPUT_ADDRESS`, and the Scenario Coverage table maps the scenario to E2E tests only. `specs/testing.md` § Coverage rule says a property that Exasol SQL cannot observe is proved by a unit test. No unit test is planned for this step.
- Fix: Delete "and the adapter SHALL write no skip warning line for it" from the step. The warning loop in `handle_create_virtual_schema` iterates `listing.skipped`, and the step already rules out hidden tables in the skip list. Otherwise, name a unit test in plan.md task 4.1 that proves the step.

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § Implementation Tasks, tasks 2.2 and 3.2
- Issue: `specs/testing.md` § Testing strategy says: "Each behavior is tested at one layer, not repeated at several." Several unit cases repeat behavior that an E2E test already proves. Task 2.2 repeats the whole-name match `ORDERS` against `ORDERS_2024`, the fold `orders_?0*`, trimming, the empty-entry rejection, and the issue's precedence example. These are proved by `patterns_match_the_whole_name_without_regard_to_case`, `include_and_exclude_choose_the_exposed_tables`, and `create_rejects_an_empty_table_pattern_entry`. Task 3.2 repeats the combined error with `MADE_UP_PROPERTY` and the passing `[`, `]`, `\` value, which task 5.4 proves. `properties_tests.rs` already has the same kind of overlap from #534, but that does not lift the rule.
- Fix: In task 2.2, keep only the matcher cases that no E2E test reaches: `*` on the empty run, `?` matching exactly one `char`, `[`, `]`, and `\` matching literally, backtracking, the comma-only and whitespace-only entries `','` and `' , '`, and the pathological pattern from the Feasibility finding. Drop the precedence, fold, and whole-name examples. In task 3.2, keep `every_known_name_passes` and drop the cases that task 5.4 already proves.

no objection — axis checked for granularity and clustering: Group A (tasks 1.1 and 1.2) owns the trait change and its call sites in `lakehouse-catalog` and `direct_storage.rs`. Group B depends on A only through task 4.1's call of the new signature, and the plan states that dependency. Each group's tasks share their source modules and spec deltas. A search finds 40 `.list_tables(` call sites in eight files. The seven test files among them match task 1.2's list, and the eighth is `adapter/mod.rs`, which task 4.1 changes.

## Design Depth

#### [INFORMATION_COUPLING] FINDINGS
- Location: plan.md task 1.1; decision-log.md [1] Consequences; architecture.md § Components (lakehouse-catalog)
- Issue: The rule "apply the predicate to each listed identifier before any admit, skip, or load" is written four times, once per implementation. One of the four, `DirectStorageCatalogClient`, lives in the engine crate. Nothing enforces the rule, and only one E2E test per kind would catch an implementation that applies it after loading. Two of the copies are the same code. Iceberg REST `list_tables` is `list_table_idents` followed by `resolve_listing`, and `load_listed_tables` is `resolve_listing`. Direct storage `list_tables` is `list_table_idents` followed by `load_listed_tables`. The architecture delta states the rule only in the `lakehouse-catalog` component line, which names "Iceberg REST, Unity Catalog, and AWS Glue" and so does not bind the direct-storage implementation. Decision [1]'s Consequences line calls this a constraint on every future `CatalogClient`.
- Fix: In plan.md task 1.1, make `list_tables` a provided trait method whose body lists identifiers, keeps those the predicate accepts, and calls `load_listed_tables`. Remove the Iceberg REST and direct-storage overrides. Only Unity Catalog and Glue override it, to keep their single-sweep listings. In architecture.md, add a DELTA:CHANGED `## Constraints` block that copies the current section. Extend the bullet "Each catalog kind's identifier-only listing returns every identifier ..." with: "Every full listing applies the caller's predicate to each identifier before it admits, skips, or loads that table."

no objection — axis checked for `[ADR_OVERPROMOTION]`: [1] is the only `Promotes to ADR: yes` entry. Its Rationale names rule-2 criteria 1 and 4 and states the ADR search result. Its Decision names a trait method but holds no signature, path, or flag. It is not on the rule-3 list.

no objection — axis checked for `[ARCHITECTURE_DRIFT]`: the BASE `c9c7e72dc06f1412cfb8e62ce4469aabe4bfd9de` matches `git hash-object specs/architecture.md`. Each CHANGED block copies its whole section, and a line diff shows only the intended edits. [1]'s `Architecture:` line names § Components and § Data Flow, and the delta has both. The new `table-filter` component and the predicate parameter from plan.md appear in the delta. The § Constraints gap above is FINDINGS, because the delta states the rule in § Components.

no objection — axis checked for `[ADR_CONFLICT]`: I read every ADR that matches listing, the catalog client, or adapterNotes. `catalog-kind-single-construction-site` requires one listing pipeline, and `list_source` stays kind-agnostic. `catalog-table-neutral-format-tag-opaque-vending-key` says "Listing admission stays with the client", and the predicate runs before the client's admission without replacing it. `iceberg-rest-requests-run-on-the-catalog-session` is unaffected. `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute` holds, because the filter is read from properties and never written to adapterNotes. `bespoke-unity-catalog-rest-client` is unaffected.

no objection — axis checked for depth: `table_filter.rs` has a two-call interface (parse, then the exposure check) over the syntax, fold, matcher, and precedence, and it depends on nothing. The predicate keeps the Exasol naming rule out of `lakehouse-catalog`. Folding the pattern with `str::to_uppercase` matches the codebase's existing practice of calling `to_uppercase` at each fold site.

## Prose Quality

#### [PROSE_UNCLEAR] FINDINGS
- Location: plan.md § Implementation Tasks, task 2.1
- Issue: "Create `crates/lakehouse-engine/src/adapter/table_filter.rs` and declare it in `adapter/mod.rs` with `#[cfg(test)] #[path = "table_filter_tests.rs"] mod tests;`" can be read as placing the test-module declaration in `adapter/mod.rs`. That file already declares `mod tests` for `adapter_tests.rs`, so the build would fail. It would also break the `AGENTS.md` rule that `foo.rs` declares its own `foo_tests.rs`.
- Fix: Rewrite the first clause of task 2.1 as: "Create `crates/lakehouse-engine/src/adapter/table_filter.rs`, declare `mod table_filter;` in `adapter/mod.rs`, and declare `#[cfg(test)] #[path = "table_filter_tests.rs"] mod tests;` inside `table_filter.rs`."

no objection — axis checked for the rest: the plan artifacts contain no em dash and none of the filler words checked for. plan.md § Summary and decision-log.md lead with the conclusion. Tasks 1.1 and 5.2 are long, but each sentence makes one claim.
