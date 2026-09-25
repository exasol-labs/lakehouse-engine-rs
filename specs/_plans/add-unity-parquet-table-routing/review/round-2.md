# Plan Review Findings: add-unity-parquet-table-routing (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 10 (Blockers: 1, Advisory: 9)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Round-1 Blocker Recheck
- Resolved: [COMPLETENESS_GAP] Catalog-to-file name and type drift had no stated outcome. Evidence: the requester chose loud, Spark-compatible failure (decision [4] and the `[plan-review]` entry). Name drift now has an outcome: the NEW scenario in `datafusion-scan/scan-execution-field-id-projection` matches the exact name first, then the uppercase fold, refuses an ambiguous fold, and folds identity-bound fields only. The Unity scenario "The logical schema is the catalog's declared column list" states that `customerid` binds to `CustomerId`. Type drift now has an outcome: the NEW `datafusion-scan/type-relaxation` scenario refuses a pair before any cast unless `widen`, the timestamp rule, or the text rule admits it, and the Unity scenario fails the query on the `double` file. Both rules land in the one shared adapter (`scan/field_id_projection.rs`, whose only production factory literal is `scan/positional_deletes.rs:952`), with no format branch. Group D (4.1 to 4.5), the task 2.6 docs bullet, and the Scenario Coverage rows implement them. The round-1 Fix's loud-failure branch (a scan-side change with its own delta on `scan-execution-field-id-projection`) is executed. The per-query scoping defect that this fix introduces is a new BLOCKER under Feasibility, not a reopening of this finding.
- Resolved: [IMPLEMENTATION_LEAKAGE] Background prose that no scenario step depended on. Evidence: E2E Background bullet 1 no longer names the writer, `DELETE` then, or the rationale. Planning Background bullet 3 ends at the Databricks quote. The metadata-logging outcome is now a clause of Scenario "Partition columns come from the catalog and partition values from the file paths" and bullet 3 of task 2.6.

## Premortem

1. After release, `SELECT ID` on a direct-storage `MERGE_SCHEMA = 'FALSE'` table fails, although `ID` has one type in every file. A Delta table with one writer-violating file fails every query, not only the queries that read the drifted column. The refusal fires when the adapter is created for any bound column, not when a query reads the column. Routes to Feasibility, `[UNSTATED_ASSUMPTION]` BLOCKER.
2. A Delta type-widening regression from group D surfaces only in group C's Unity suite. Task 3.3 carries no guard, so the implementer widens the admitted set to turn the suite green. Routes to Feasibility, `[HIDDEN_DEPENDENCY]` advisory.
3. A pandas-written direct-storage table with a categorical integer column fails every query that reads that column. The scan refuses the file against the declaration derived from that same file. Routes to Requirement Quality, `[COMPLETENESS_GAP]` advisory.

## Intent Fidelity

Checked without objection: the issue #409 mapping in round 1 is unchanged, and group D executes the requester's round-1 choice at the shared site, as the requester asked.

#### [SCOPE_CREEP] ADVISORY
- Location: `datafusion-scan/type-relaxation/spec.md` § Scenario "A physical type outside identity and the supported set is refused before any cast", clause "the rule SHALL hold for every format and every binding key", plus `plan.md` § Impact "Breaking, every format" and `decision-log.md` § [4] Decision
- Issue: The confirmed requester scope names identity-bound fields: DIRECT_STORAGE, Delta `none` mapping, and Unity Parquet get "the same case-fold-then-refuse-on-unrelaxable-cast protection". The plan also refuses on field-id and declared-physical-name bindings. Every Iceberg scan and every Delta `id`- or `name`-mapped scan therefore gains a new scan-time failure mode. The extension is defensible, because one rule without a binding-key branch is "the most clean and reusable way". My check found no legitimate Iceberg or Delta pair that the rule refuses:
  - Delta type-widening pairs reach `widen` through the `int32` tag for Byte, Short, and Integer (`adapter/pushdown/format/delta_schema.rs:406`).
  - Iceberg `date` to `timestamp` is refused at plan time by `vs-adapter/iceberg-type-promotion`.
  - INT96 arrives as `Timestamp(us, "UTC")` (`scan/raw_scan.rs` `int96_coerced_parquet_format`), which the timestamp rule admits.
  Nothing in the brief shows that the requester saw this extension.
- Fix: Add one sentence to decision [4] Rationale: "The refusal also covers field-id and declared-physical-name bindings, so an Iceberg or Delta scan fails only on a file that breaks its own format's type rule."

## Feasibility

Checked without objection: the factory has three literals (`scan/positional_deletes.rs:952`, `field_id_projection_tests.rs:87,840`), and `PositionalDeleteScanTable` holds `table_root`. `widen` (`types/widening.rs:11`) and `needs_json_fallback` (`types/mapping.rs:217`) exist with the shapes task 4.2 assumes. `needs_json_fallback` is true for `Binary`, `FixedSizeBinary`, `Time64`, and a `Decimal128` above precision 36, so the Iceberg string-tagged carve-out covers `binary`, `fixed`, `uuid`, `time`, and a wide decimal. The timestamp rule refuses only the pair that shifts an instant, so neither carve-out reopens silent drift.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: `plan.md` § Implementation Tasks, task 4.2 ("In `FieldIdExprAdapterFactory::create`, check each bound column without a nested descriptor before building the delegate. ... Otherwise return a `DataFusionError`"), and `datafusion-scan/type-relaxation/spec.md` § Scenario "A physical type outside identity and the supported set is refused before any cast", clause "every other pair SHALL fail the query"
- Issue: Task 4.2 assumes that `create` sees only the columns a query reads. It sees every column. DataFusion 54.1 passes the whole table file schema to `create` (`logical_file_schema = self.table_schema.file_schema()`, `datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:589`, with the `create` call at line 844). The pushdown hands the unprojected `ResolvedScan` logical schema to the scan spec (`adapter/pushdown/mod.rs:234-244` and line 301). An eager check therefore fails every query on a file that holds any refused column. Three of the plan's own acceptance checks then fail:
  - Task 4.4 asserts that `SELECT ID` returns four rows. The wide `widened/` file binds `QTY` as `Int64` and `PRICE` as `Float64` under `Int32` and `Float32`, so `create` refuses the file before `ID` is read. The direct-storage-e2e clause "a query that reads only columns whose physical type every file shares SHALL return every row of BOTH files" fails the same way.
  - Task 4.3 claims that `a_nested_physical_column_with_no_descriptor_fails_the_cast_rather_than_rendering` passes unchanged. Its helper `rewrite_with` calls `.create(..).expect("adapter creation")` (`field_id_projection_tests.rs:87-89`). A `Struct` under a `utf8` field without a descriptor is not "a primitive type that `needs_json_fallback` flags". So `create` returns an error, and the helper panics before the test's `expect_err` runs.
  - `plan.md` § Impact scopes the break to "a query that reads a column". An eager check also breaks the queries that never read the column.
  The spec clause "every other pair SHALL fail the query" does not say whether an unread refused column fails the query. The spec therefore cannot catch the task's reading.
- Fix: In `plan.md` task 4.2, keep the admission decision in `FieldIdExprAdapterFactory::create` but only record it. Build a per-file map of refused logical column indices, with the column name and both types, beside `absent_default_by_index`, and store it on `FieldIdExprAdapter`. In `FieldIdExprAdapter::rewrite`, return the `DataFusionError` naming the table root, the column, and both types when a `Column` it rewrites references a refused index, before delegating. In `datafusion-scan/type-relaxation/spec.md`, replace "every other pair SHALL fail the query" with "every other pair SHALL fail a query that references the column in its projection or filter, and a query that references no refused column of a file SHALL read that file". Add `a_refused_column_fails_only_a_rewrite_that_references_it` to task 4.3, to `field_id_projection_tests.rs`, and to Scenario Coverage. In task 2.5, make `a_file_type_outside_the_widening_set_is_refused_naming_the_column` rewrite a `Column` that references the refused field. Keep the ambiguous-fold failure of task 4.1 in `create`, because its scenario fails the query whether or not the column is read, and state that choice in task 4.1.
- Escalation: MECHANICAL

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: `plan.md` § Implementation Tasks, tasks 4.5 and 3.3
- Issue: Group D changes the cast path of every Delta scan. Task 4.5 runs only `cargo test` and `make test-e2e`. The Delta scan fixtures run only in `make test-e2e-unity` (`Makefile:298-300`). One example is `unity_delta_type_widening_returns_the_widened_types_across_both_files` (`tests/e2e_unity_test.rs:1110`), which reads eleven widened columns across the widening boundary. A Delta regression from group D therefore surfaces first in task 3.3, two groups later. Task 3.3 says "Every test MUST pass" but lacks the guard of task 4.5: "Do not widen the admitted set to make the test pass."
- Fix: Append to task 4.5: "Then run `make unity-up` and `make test-e2e-unity`." Copy the stop-and-report sentences of task 4.5 into task 3.3.

## Requirement Quality

#### [COMPLETENESS_GAP] ADVISORY
- Location: `datafusion-scan/type-relaxation/spec.md` § Scenario "A physical type outside identity and the supported set is refused before any cast", clause "a dictionary-encoded physical column that no rule above admits SHALL be judged by its value type", and `plan.md` task 4.2, "Judge an unadmitted dictionary by its value type"
- Issue: `arrow_type_to_tag` tags every dictionary `utf8` (`types/mapping.rs:456`, the `_ => "utf8"` arm). The direct-storage fold keeps the footer's dictionary type (`adapter/parquet_directory.rs:323`, `fold_schemas`). A pandas categorical integer column, `Dictionary(Int8, Int64)`, is therefore declared `VARCHAR(2000000)`. The scan judges that file by its value type `Int64`. `Int64` is neither a string encoding nor a JSON-fallback primitive, so the scan refuses the column against the declaration derived from that same file. Before this plan, the cast rendered the integers as text. No task, test, or docs bullet states this outcome.
- Fix: In the type-relaxation scenario and task 4.2, also admit a physical type whose `arrow_type_to_tag` equals the logical field's tag. Add `a_dictionary_column_the_fold_tagged_utf8_is_admitted` to `type_relaxation_tests.rs` and to Scenario Coverage. If the planner keeps the value-type rule, add the refusal as a bullet of the task 4.4 `docs/catalogs.md` update instead.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `datafusion-scan/scan-execution-field-id-projection/spec.md` § Scenario "An identity-bound field binds a file column whose name differs only in letter case", clause "the fold SHALL apply to identity-bound fields only", and `plan.md` task 4.1, "extend the identity step of `claim_logical`"
- Issue: `claim_logical` has two callers. `bind_columns` calls it for top-level fields (`scan/field_id_projection.rs:951`), and `claim_struct_slots` calls it for struct members (line 265). A struct member of a Delta `none`, direct-storage, or Unity Parquet column carries no field-id and no physical name, so task 4.1 folds it too. `claim_struct_slots` keeps the first physical member that claims a slot. If a folded member precedes the exact one, the folded member wins. That contradicts "an exact match takes precedence over a folded one", and no ambiguity is recorded at that level. The scenario does not say whether a struct member folds. The exact-first rule also needs a whole-schema pass, because `claim_logical` sees one physical field at a time and cannot know that a sibling matches exactly. Task 4.1 is untagged, while task 4.2 is `[expert]`.
- Fix: Add a clause to the scenario that states whether a struct member binds by the fold. If it does not, move the fold out of `claim_logical` into a top-level pass in `bind_columns`. If it does, apply the exact-first rule and the ambiguity refusal in `claim_struct_slots`, and add `a_struct_member_binds_by_the_fold_only_without_an_exact_match` to task 4.3 and to Scenario Coverage. In both cases, state in task 4.1 that a first pass over the file's fields claims the exact matches, and tag task 4.1 `[expert]`.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: `datafusion-scan/scan-execution-field-id-projection/spec.md` § Background, `datafusion-scan/type-relaxation/spec.md` § Background, and `vs-adapter/parquet-directory-seam/spec.md` § Background bullet 4
- Issue: Once these deltas merge, three recorded Background statements are false, and no delta supersedes them:
  - `scan-execution-field-id-projection`: "(4) the physical name is kept unchanged, which is what makes identity binding resolve". The fold renames a case-drifted physical field. The delta's own Background says it "amends no recorded clause".
  - `type-relaxation`: "this feature reads whatever the logical schema declares", and "That permissiveness is why this engine's supported set is decided at PLAN time by the two format features, not left to the cast to police". The new scenario checks the pair set at scan time.
  - `parquet-directory-seam` delta, bullet 4: "The scan side needs no new mechanism. A logical field carrying neither a field-id nor a declared physical name binds by its own name." Group D adds the admission check and the fold.
- Fix: Replace the field-id-projection delta's Background bullet with one that supersedes resolution step (4) for identity-bound fields. Add a type-relaxation Background bullet that supersedes the two quoted sentences and states that the scan admits a pair before the cast. In the seam delta's bullet 4, change "binds by its own name" to "binds by its own name, then by the uppercase fold", and delete "The scan side needs no new mechanism."

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: `datafusion-scan/type-relaxation/spec.md` § Scenario title "A physical type outside identity and the supported set is refused before any cast", `plan.md` § Summary sentence 3, § Goals bullet 4, § Impact bullet 4, and the last bullet of task 2.6
- Issue: The scenario title and the plan's summary lines say that the scan refuses "a physical type outside identity and the supported (widening) set". The scenario body also admits a coarser timestamp unit, a zone change that keeps the instant, and a text-rendered type under a string tag. All three lie outside that set. For example, `Timestamp(ms)` under `Timestamp(us)` is outside identity and outside `widen` (`types/widening.rs:24-41`), yet the rule admits it. Task 2.6 carries the short form into user documentation: "A data-file type outside identity and the widening set fails the query".
- Fix: Rename the scenario to "A physical type the scan cannot admit is refused before any cast" in the delta, in Scenario Coverage, and in every reference. In `plan.md` Summary, Goals, Impact, and the task 2.6 bullet, name the three admission rules: identity or a supported widening, a timestamp that keeps its instant at an equal or coarser unit, and a text-rendered type under a string column.

## Task Breakdown

Checked without objection: every new delta has an implementing task. `scan-execution-field-id-projection` maps to 4.1 and 4.3, `type-relaxation` to 4.2 and 4.3, and `direct-storage-e2e` to 4.4. Group B correctly depends on D, because task 2.5 tests the adapter's fold and refusal. B edits the Unity section of `docs/catalogs.md` after D edits the direct-storage section.

#### [TASK_GRANULARITY] ADVISORY
- Location: `plan.md` § Parallelization, "A and D share no file and run in parallel."
- Issue: A and D edit no common file, but both compile into `lakehouse-engine`. Task 1.2 changes `CatalogTable` and `ColumnSourceType::Unity`, and the crate does not compile until every census site in task 1.2 is updated. The TDD loop of group D and task 4.5 run `cargo test` on that crate throughout. If both groups work in one tree, the red phase of each group shows the other group's compile errors.
- Fix: Make D depend on A in the Parallelization table, or state that A and D run in separate worktrees and merge before B starts.

#### [TRACEABILITY_GAP] ADVISORY
- Location: `plan.md` task 4.4, and `e2e-harness/direct-storage-e2e/spec.md` § Scenario "MERGE_SCHEMA FALSE declares the sampled footer's type on the refresh and the scan path", clause "SHALL fail with an error naming the column and both types"
- Issue: Task 4.4 asserts only that the error names `PRICE`, so any error that names `PRICE` passes. The clause also requires both types. The kept test `stale_declaration_decides_the_emitted_width` still passes, but its doc comment ("A wide-file `QTY` value that doesn't fit the narrow declared type surfaces a clean error") describes the value-dependent narrowing cast that this plan removes.
- Fix: In task 4.4, also assert that the error names `Float32` and `Float64`. Change the doc comment of `stale_declaration_decides_the_emitted_width` to state that the scan refuses the wide `QTY` file whatever its values.

## Design Depth
[no objection — axis checked: group D puts the fold and the admission in the one shared adapter (`scan/field_id_projection.rs`, reached through the single production factory literal at `scan/positional_deletes.rs:952`). It decides on logical-field content (binding keys and the Arrow tag) and never on table format. It reuses the one pair owner `widen` and the one fallback classifier `needs_json_fallback` instead of adding a pair table. Decision [4] passes the promotion gate as a scan-wide behavior contract. It neither duplicates nor contradicts decisions [1] and [2], and it supersedes no ADR in `specs/_decision/074-add-type-relaxation.md` or `091-add-direct-storage-catalog-kind.md`. The one boundary concern, the fold leaking into struct members through the shared `claim_logical`, is the `[AMBIGUOUS_REQUIREMENT]` advisory under Requirement Quality.]

## Prose Quality

Checked without objection: `plan.md` and `decision-log.md` contain no em dash and no semicolon. The round-1 prose advisories still stand and are not repeated here.

#### [PROSE_BLOAT] ADVISORY
- Location: `decision-log.md` § [4] Consequences, bullets 2 and 3
- Issue: The last sentence of bullet 2 narrates test history: "The recorded E2E test read `Float64` values under a `Float32` declaration, and its values were exact in `Float32`, which hid the precision loss." Bullet 3 restates the Exasol trade-off clause that `datafusion-scan/type-relaxation` owns. Once promoted, both become a second home for feature text, against the requester's rule that ADRs keep only architectural decisions.
- Fix: Delete the last sentence of bullet 2, and delete bullet 3.
