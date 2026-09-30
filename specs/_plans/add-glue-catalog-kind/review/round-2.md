# Plan Review Findings: add-glue-catalog-kind (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 9 (Blockers: 3, Advisory: 6)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

Blocker count: 1 round-1 blocker not resolved, plus 2 new ones. All 3 are MECHANICAL.

## Round-1 Blocker Recheck

- Resolved: [SCOPE_CREEP] Zero-length rule widened past the D5/D8 answer. The user chose option (b), recorded in decision-log.md § Interview. Decision [7] and the plan.md § Consequences row now name the `_$folder$` markers and empty files, and state that `data_file_segments` already drops a marker at the listed location. plan.md § Impact labels the direct-storage and Unity change as user-approved. The seam scenario "The file pattern selects the listing depth and the file-name rule" applies the zero-length rule under every pattern.
- Resolved: [SCOPE_CREEP] Binary refusal reached undeclared types. The user chose option (a). `binary-column-refusal` refuses Iceberg `binary` and `fixed(L)`, and Delta, Unity, and Glue `binary`, and exempts `uuid`. Background bullet 3 and scenario 4 state the direct-storage exception (#351). Open Question 3 tracks it. Task 4.1 makes `binary_cause(declared)` name the declared type. The direct-storage refusal task and its fixture are gone.
- Resolved: [UNSTATED_ASSUMPTION] Task 6.5 declared a second SkippedTable. Task 6.5 passes `&[lakehouse_catalog::SkippedTable]` and states "Declare no second `SkippedTable`". Its census matches the 16 `adapter_tests.rs` call sites plus `adapter/mod.rs:224`.
- Not resolved: [REQUIREMENT_CONFLICT] Recorded Iceberg binary statements contradict the refusal. The planner replaced the two quoted sentences, but the same two `DELTA:CHANGED` Background blocks still carry two more sentences that contradict `binary-column-refusal`. A `DELTA:CHANGED` Background replaces the whole section, so both sentences become permanent text:
  - `vs-adapter/delta-type-mapping/spec.md` delta, line 203: "The Iceberg format reader returns an EMPTY refused-column list, because it maps every Iceberg type and refuses none." Task 4.1 fills that list.
  - `datafusion-scan/nested-json-rendering/spec.md` delta, lines 31-36: "On the Iceberg path `adapter/pushdown/format/iceberg.rs` hardcodes an EMPTY refused-column list, so nothing is refused". The bullet at line 17 still opens with "Binary is OUT OF SCOPE and its behavior MUST NOT change", next to the new sentence that says Iceberg binary is refused.
  - Escalation: MECHANICAL. Reading the two delta files against `binary-column-refusal` settles it.
  - Fix: In the `delta-type-mapping` delta, replace the line-203 bullet with one line: "The Iceberg format reader refuses a declared `binary` or `fixed(L)` column per `vs-adapter/binary-column-refusal`, and maps every other Iceberg type." In the `nested-json-rendering` delta, cut the line-31 bullet down to its Delta half, or restate its Iceberg sentence in the past tense as the cause this feature removed, with no claim about the current `iceberg.rs` list. Change the line-17 bullet's lead to "Binary rendering is out of scope (#351)." Then grep both deltas for "EMPTY refused", "refuses none", and "refuses no" and confirm that no hit remains.
- Resolved: [REQUIREMENT_CONFLICT] SKIPPED_TABLES conflicted with the recorded adapterNotes rules. The skipped-tables feature is deleted. Its three scenarios are `DELTA:NEW` in `create-virtual-schema-adapter-notes`, whose `DELTA:CHANGED` description and Background name `SKIPPED_TABLES` as the one write-only diagnostic entry. The `create-virtual-schema` delta changes only the `:87` clause, and the diff against the recorded file shows no other edit.
- Resolved: [REQUIREMENT_CONFLICT] Task 2.7 widened a source-text probe. The `CATALOG_SOURCES` clause is gone. Task 2.8 edits only `lib.rs` and the reachability probe.
- Resolved: [IMPLEMENTATION_LEAKAGE] Backgrounds stated facts no scenario uses. Each listed line is deleted. Every remaining Background bullet in the NEW Glue features supports a step. For example, the `glue-catalog-client` bullets back the CatalogId, error-code, pagination, and partition-value scenarios.
- Resolved: [ADR_OVERPROMOTION] Decision [2] over-promoted. It reads `Promotes to ADR: no`.
- Resolved: [ADR_OVERPROMOTION] Decision [4] over-promoted. It reads `Promotes to ADR: no`.
- Resolved: [PROSE_BLOAT] Spec deltas compared against the pre-plan state. A grep of the NEW features and of every `DELTA:NEW` and `DELTA:CHANGED` block for "unchanged", "before this", "as before", "previously", and "byte-identical" finds no plan-authored hit. Every listed phrase is restated as current behavior.

## Premortem

1. **A later plan removes Delta's kernel partition pruning.** It reads `partition-predicate-declared-types`: "no reader SHALL keep its own partition-value comparison". The Delta and Iceberg readers keep their own comparison, so the reviewer flags them as spec violations and "unifies" them. That undoes interview answer D2. Routed to Requirement Quality (new blocker 1).
2. **The merged library still contradicts itself about Iceberg.** `delta-type-mapping` says the Iceberg reader refuses nothing. `catalog-kind-selection` says an absent `CATALOG_KIND` produces output byte-identical to the pre-feature output, including generated SQL and error messages. Both are false once an Iceberg `binary` column is refused and `SKIPPED_TABLES` is written. Routed to the round-1 recheck and to Requirement Quality (new blocker 2).
3. **An Iceberg `uuid` user files a bug the day after release.** Arrow cannot cast `FixedSizeBinary` to `Utf8`, so an Iceberg `uuid` column likely fails at scan time today. The spec says only that `uuid` "keeps its VARCHAR(2000000) mapping", and plan.md says `fixed(L)` "returned text". Routed to Feasibility (advisory 1).

## Intent Fidelity

[no objection. Axis checked: both round-1 HUMAN answers are applied as the brief records them. (1b) The zero-length rule covers every pattern, and its rationale names `_$folder$` markers and empty files. (2a) The refusal reaches only declared types, `uuid` is exempt, and direct storage is a scoped exception (#351) with its own scenario and Open Question 3. The Backgrounds that the planner rewrote in `catalog-kind-selection` and `create-virtual-schema-adapter-notes` remove changelog prose, which serves the user's "not a changelog" instruction.]

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Impact (line 106: "Before, a top-level column returned text from a lossy cast"), plan.md § Migration (line 123: "An Iceberg `binary`/`fixed` column returns text"), and `vs-adapter/binary-column-refusal/spec.md` § Background, bullet 2 ("Iceberg `uuid` ... keeps its `VARCHAR(2000000)` mapping")
- Issue: `arrow-cast` 58.4.0 casts `FixedSizeBinary` only to `Binary`, `LargeBinary`, and `BinaryView` (`src/cast/mod.rs:247`, `:1452-1458`). `json_render_tests.rs:397-406` asserts that exact rejection for a `FixedSizeBinary` map key. The Iceberg reader tags `uuid` and `fixed(L)` as `Utf8` (`types/mapping.rs:292-293`). `admits_as_text` admits a `FixedSizeBinary` file column under that tag (`scan/field_id_projection.rs:871-877`), so the scan attempts a cast that arrow rejects. `each_physical_type_is_admitted_or_refused_under_its_declared_type` omits `FixedSizeBinary` from its admitted cases. So a top-level Iceberg `fixed(L)` column most likely failed at scan time rather than returned text, and a `uuid` column most likely still fails. This finding is an inference from the source, and no live run confirms it.
- Fix: Verify live, per `CLAUDE.md` § Verification discipline: run `SELECT` on an Iceberg `uuid` column and on a `fixed(16)` column against the Docker Exasol. Correct the plan.md § Impact and § Migration text for `fixed(L)` to the observed behavior. If `uuid` fails, add one Background bullet to `binary-column-refusal` that states the failure as an accurately-scoped exception with `(#TBD)`, and list it in plan.md § Open Questions. The `uuid` decision itself stands.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md tasks 6.5 and 6.6, and `adapter/mod.rs:288-315`, `:401-412`
- Issue: `adapter_note` re-parses the whole adapterNotes JSON for each key. A pushdown makes 8 such reads plus `read_table_map`. `SKIPPED_TABLES` grows the notes by about 50 to 150 characters per skipped table, and a Hive-heavy Glue database can skip thousands of tables. Every pushdown then parses that array about 9 times, although "No pushdown reads it". Task 6.6 measures only the size limit, not this cost.
- Fix: In task 6.5, make each pushdown parse adapterNotes once and pass the map to the readers. As an alternative, record in task 6.6 the pushdown latency for a notes value of the measured maximum size.

#### [NFR_IGNORED] ADVISORY
- Location: `vs-adapter/glue-table-planning/spec.md` § "Each kept partition's location is listed and its files carry the partition's Glue values", and plan.md task 5.2
- Issue: A pushdown lists every kept partition's location separately, after it pages through all of `GetPartitions`. An unpruned query over a table with 10,000 partitions makes 10,000 LIST calls at plan time, bounded only by `DEFAULT_S3_MAX_CONNECTIONS`. No task measures this cost, and no document states it. A Unity table of the same shape costs one recursive listing.
- Fix: In task 8.1, add a sentence to the Glue section of `docs/catalogs.md` that plan time grows with the kept partition count. Name the measured cost for one partition count in the task 8.3 verification report.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: `vs-adapter/parquet-directory-seam/spec.md` delta (two `DELTA:NEW` scenarios)
- Issue: The recorded seam spec holds 10 scenarios. The merged spec holds 12, which exceeds the library threshold of 10 scenarios per spec. `/speq:record` stops before archiving and asks the user to decide on a split.
- Fix: Move the two `DELTA:NEW` scenarios into a new sibling feature `vs-adapter/parquet-directory-seam-file-pattern`, and cite it from the seam Background. As an alternative, state the split in plan.md § Open Questions so that the record step does not surprise the user.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/partition-predicate-declared-types/spec.md` § "Direct storage passes every partition column to the one predicate as a string", line 35
- Issue: The clause reads "every source SHALL call the ONE predicate, and no reader SHALL keep its own partition-value comparison". Two recorded readers keep their own comparison, and the user's D2 answer requires both to keep it ("Delta and Iceberg keep their own pruning"):
  - `vs-adapter/delta-file-pruning/spec.md:160-162`: the Delta reader hands `delta_kernel` a partition equality, and pruning is exact on `partitionValues`.
  - `vs-adapter/pushdown-file-pruning/spec.md:39-46`: the Iceberg reader sets an `iceberg::expr::Predicate` on a partition column before `plan_files`.
  The round-1 fix deleted the Background bullet that scoped the rule to the Parquet readers, so the clause now reads as universal.
- Fix: Replace line 35 with: "*AND* the direct-storage, Unity Parquet, and Glue Parquet readers SHALL each call the ONE predicate, and none of them SHALL keep its own partition-value comparison". Leave the Delta and Iceberg readers unnamed.
- Escalation: MECHANICAL. The two recorded scenarios and the D2 answer settle it.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: recorded `specs/vs-adapter/catalog-kind-selection/spec.md` § "Absent CATALOG_KIND resolves the Iceberg REST catalog kind", lines 57 and 59, which no delta in this plan edits
- Issue: Line 57 requires "output BEHAVIOR-IDENTICAL to the pre-feature output for the same request: ... enumerated tables, declared column names and Exasol types, `TABLE_MAP`, skipped-table warnings, per-shard scan specs, generated SQL, and error messages are all byte-identical". Under an absent `CATALOG_KIND`, this plan changes two outputs:
  - The createVirtualSchema adapterNotes now carry `SKIPPED_TABLES` (`create-virtual-schema-adapter-notes`, scenario 1, Iceberg REST case).
  - A pushdown that reads an Iceberg `binary` column now fails instead of returning generated SQL (`binary-column-refusal`, scenario 1).
  Line 59 also says the REST path "does NOT go through the `CatalogClient` trait in this plan". That is changelog wording, which the user asked the specs to avoid. The plan already changes this feature, so the fix costs one block.
- Fix: Add a `<!-- DELTA:CHANGED -->` block for "Absent CATALOG_KIND resolves the Iceberg REST catalog kind" to `specs/_plans/add-glue-catalog-kind/vs-adapter/catalog-kind-selection/spec.md`. Keep the GIVEN, the WHEN, the THEN clause, the one-grant clause, and the REST-path clause, all restated as current behavior. Delete the line-57 pre-feature comparison. Change line 59's "in this plan" to the present tense. Add the scenario to plan.md § Scenario Coverage, mapped to the existing test `absent_catalog_kind_resolves_iceberg_rest` (`catalog_kind_tests.rs:6`).
- Escalation: MECHANICAL. The recorded clause and this plan's two deltas settle it.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md task 6.6, final bullet ("If a limit exists, cap `SKIPPED_TABLES` ... Add a scenario for the cap ... in the same change")
- Issue: The task can add a spec scenario during implementation, after review ends. That scenario has no row in § Scenario Coverage, no test name, and no stated behavior at the cap. For example, nothing states whether the notes record how many skips were dropped. A user who reads a capped list cannot tell that it is incomplete.
- Fix: Write the cap scenario now as a conditional one, and add its row to § Scenario Coverage with a test name. The scenario covers the cap value that task 6.6 measures, and a `SKIPPED_TABLES_TOTAL` count, or an equivalent marker, that states how many skips the list omits. As an alternative, state in task 6.6 that a measured limit stops the implementation and returns the plan for review.

[Parallelization otherwise checked: D1 (tasks 4.1-4.2) shares no file with B. The files of B's task 2.1 census are owned by C, D2, and E, which all follow B. F2 touches only its own workflow file.]

## Design Depth

[no objection. Axis checked: the decisions promoted to ADRs are [1] (one catalog-declared Parquet reader and one Iceberg planner) and [3] (an SDK client with no ambient credential chain). Both are lasting architecture and security boundaries that pass the promotion gate. Decisions [2], [4] to [8] and all ten `[plan-review]` entries read `Promotes to ADR: no`. No new module appears since round 1. `CatalogPartition` keeps `aws-sdk-glue` types inside `lakehouse-catalog`, and the `ScanSource::Glue` arm dispatches on the format tag.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Consequences (lines 72-81)
- Issue: The intro says the table "keeps only the choices without an entry of their own, plus the two the interview follow-ups settled." Row 1 ("Typed local pruning") repeats decision [1]'s third Consequences bullet. Rows 3 and 4 repeat decisions [2] and [7] and cite them. Only row 2 ("Glue lists `*` per partition") has no other home.
- Fix: Delete rows 1, 3, and 4, and the intro sentence. Keep row 2 as one sentence under § Decision.
