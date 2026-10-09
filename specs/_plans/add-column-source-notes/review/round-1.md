# Plan Review Findings: add-column-source-notes (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 17 (Blockers: 4, Advisory: 13)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed. Three stories explain why.

1. **Silent wrong rows from a stale NOT NULL.** An Iceberg owner ran `ALTER COLUMN c DROP NOT NULL`, and an OSS Unity registrant sent `"nullable": false` for a Delta column that the log marks nullable. The column notes still declared `nullable: false`. DataFusion 54.1 simplifies `c IS NULL` to `false` on a non-nullable field (`datafusion-optimizer-54.1.0/src/simplify_expressions/expr_simplifier.rs:1744`), so `WHERE c IS NULL` returned zero rows with no error. A required column dropped at the source failed every query with the required-absent error, although the new spec scenario promises NULL. The finding is under Requirement Quality.
2. **Three PRs became one stalled run.** `/speq:implement-pr` implements and records a whole plan. The run either put all phases in one PR and stopped at task 3.1 for lack of a Databricks workspace, or recorded all 20 deltas after Phase 1. In the second case the library claimed Delta catalog declarations that no code implemented, and the archive removed tasks 2.x and 3.x. The finding is under Feasibility.
3. **The line-count gate stopped Phase 2.** The spike measured +202 / -143 production lines, net +59, on `main...spike/426-direct-storage-column-notes`. Phase 2 deletes about one function (`iceberg.rs` `plannable_schema`) and adds `declare_columns`, field-id translation, and note threading. Its checklist row "net negative per phase" failed, and the plan has no failure path. The implementer either compressed code to meet the gate or stopped. The finding is under Feasibility (advisory).

## Intent Fidelity

#### [INTENT_DRIFT] ADVISORY
- Location: plan.md § Parallelization, rows C and D
- Issue: The interview asks for "Live-measurement tasks included first in their phase". Task 3.1 is numbered first, but row D (`3.1, 3.4, 3.5, 3.7, 3.8`) depends on C (`3.2-3.3`), so the gate runs only after the Unity and Glue Parquet work. The missing Databricks workspace then surfaces after Phase 3 code exists.
- Fix: In plan.md § Parallelization, move task 3.1 into its own group, for example `C0: Delta research gate | 3.1 | B`. Make C and D depend on it, or state that it runs at the start of Phase 3 in parallel with C.

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Parallelization ("each phase is one PR"), § Dependencies; decision-log.md Interview (scope answer)
- Issue: The user asked for one plan with one PR per phase. `/speq:implement` and `/speq:implement-pr` implement every task of a plan. `/speq:record` merges every delta and archives the plan. plan.md does not say how three PRs come out of one plan, or when recording runs. If recording runs after Phase 1, the library records the Iceberg, Unity, and Delta behavior before any code for it exists, and the archive drops tasks 2.x and 3.x. If one run implements everything, Phases 1 and 2 cannot ship while task 3.1 waits for a Databricks workspace that the interview says is unavailable.
- Fix: Add a `## Delivery` section to plan.md. (1) Each PR implements only its phase's groups: A for PR 1, B for PR 2, C to E for PR 3. (2) `/speq:record` runs once, with the PR that completes Phase 3. Until then the recorded direct-storage and Iceberg specs describe pre-change behavior, and plan.md § Impact states this. (3) If task 3.1's gate fails, say what PR 3 contains and that the plan returns for revision before any recording.
- Escalation: MECHANICAL. The fix keeps the user's one-plan, three-PR decision and only states how it is carried out.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: decision-log.md [13]; plan.md § Requirements ("each delta MUST be negative"), tasks 1.11, 2.5, 3.9, § Checklist "Line delta"
- Issue: The interview says the net line count "goes down, reported in each PR". Decision [13] makes every phase's delta a MUST-negative gate and gives no action for a positive result. Plan Context already shows the spike at +59 net, and `git diff --numstat` on the spike branch confirms +202 / -143. Phase 1 adds the encoder, the size check, the missing-note error, and the listing spelling check on top of the spike. Phase 2 deletes little code. Either phase can fail its gate, which pushes the implementer toward code golf or a stall.
- Fix: In decision [13] and the plan.md § Requirements row, make the cumulative delta after Phase 3 the MUST-negative gate. Each PR reports its own delta. A positive phase delta names the later deletions that offset it. Alternatively, keep the per-phase gate and state what the implementer does when a phase is positive.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Dependencies ("No such workspace is configured in `test.env`")
- Issue: The gitignored `test.env` holds non-empty `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_OAUTH_CLIENT_ID`, and `DATABRICKS_OAUTH_SECRET` entries. The reviewer checked only that the values are present and did not read them. No code in the repository reads these entries. The sentence is false as written, and it may be blocking task 3.1 for no reason.
- Fix: Ask the human whether those credentials reach a workspace that can create a column-mapped Delta table. Then either name them as task 3.1's workspace, or change the sentence to state why they cannot be used.

#### [EFFORT_MISESTIMATION] ADVISORY
- Location: plan.md tasks 1.8, 1.9, 2.3, 2.4, 3.6; pushdown/pushdown-module-structure delta
- Issue: Several external test files construct `ScanSource` or call `FormatReader::resolve_scan`: `tests/common/e2e_harness.rs:452`, `tests/e2e_scan_test.rs:2652`, `tests/e2e_unity_test.rs:616`, and `tests/catalog_session_signatures.rs:21`. Task 2.3 adds raw notes to `ScanSource::Iceberg`. Task 3.6 changes `resolve_scan` to take a parsed declared schema and return a new reader result. No task lists these files. After task 3.6, an external crate must build the declared schema, so its type and its parse function must be `pub` and reachable. The plan does not say where they live. The module-structure delta pins the façade at "EXACTLY ONE item SHALL be added" and keeps 27/17 items, and the plan does not say whether the new types keep that true. Task 1.9 ports the spike's E2E test, but `spike/426-direct-storage-column-notes` exists only as a local branch, absent from `origin`.
- Fix: Add the four external test files to tasks 2.4 and 3.6. In decision [10], name the module and visibility of the parsed declared-schema type and of the reader's result type, and confirm the façade counts or amend the module-structure delta. Push the spike branch, or copy its test into the plan's notes, before Phase 1 starts.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md task 1.1; decision-log.md [5]; vs-adapter/column-source-notes Background (limit bullet)
- Issue: Task 1.1 measures a per-column bound and a per-table combined bound. Column notes also grow the whole `createVirtualSchema`/`refresh` response, with one note on every column of every table in the namespace. No task checks whether Exasol bounds the size of an adapter response. Task 1.1 also measures two Exasol versions but does not say which value the "one adapter constant" takes if they differ.
- Fix: Extend task 1.1 to send one response with many tables and many large notes, and record whether a response-size bound exists. If it does, stop and revise as task 1.1 already does for a per-table bound. State in decision [5] that the constant takes the smaller of the two measured limits.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER
- Location: vs-adapter/column-source-notes Background ("The note therefore adds no new drift"); file-planning/pushdown-planning-file-resolution "A column dropped at the source after the last REFRESH reads what older data files store"; delta/delta-table-planning Background (dropped-column scoped exception) and "A declaration that disagrees with the Delta log fails the query"; decision-log.md [18]
- Issue: A note fixes each field's `nullable` flag at CREATE. For Delta, the flag comes from the catalog's `type_json` copy. Today the Iceberg reader reads `nullable: !f.required` (`iceberg.rs` `build_logical_schema`) and the Delta reader reads the log on every query. The Exasol `dataType` carries no nullability, so the Background claim "adds no new drift" is false. A declared `nullable: false` that is stale or wrong causes two failures.
  - Silent wrong rows. DataFusion 54.1 rewrites `x IS NULL` to `false` and `x IS NOT NULL` to `true` when the field is non-nullable (`expr_simplifier.rs:1737-1746`). This happens after an Iceberg `makeColumnOptional`, after a Delta `DROP NOT NULL`, or with an OSS Unity copy that disagrees with its log. The decision [18] check compares binding keys and partition columns only.
  - A broken scenario. A required column dropped at the source has no `initial-default` and is absent from new files, so it hits the recorded required-absent error ("A required field with no default fails the scan with a clean error"). The new Iceberg scenario says "the query MUST NOT fail", and the Delta scoped exception says such files read NULL.
  
  Direct storage, Unity Parquet, and Glue Parquet are unaffected, because they already declare every field nullable.
- Fix:
  - Add a decision-log entry, and a scenario in `delta/delta-table-planning` and in `file-planning/pushdown-planning-file-resolution`. The Delta reader, as part of the decision [18] check, and the Iceberg reader, against the current schema by declared field id, fail a query when a declared non-nullable field is nullable in the source. The error names the table, the column, and `ALTER VIRTUAL SCHEMA ... REFRESH`.
  - Change the dropped-column scenario's GIVEN to an optional column. Add a clause that a dropped required column without an `initial-default` fails with the required-absent error until `REFRESH`, and mirror that clause in the Delta scoped exception.
  - Replace the Background sentence "The note therefore adds no new drift" with a list of the dimensions the note adds: binding key, nullability, nested members, and partition position.
  - Add these to plan.md § Impact and § Scenario Coverage.
- Escalation: MECHANICAL. The rewrite is verified in the pinned DataFusion source and the required-absent rule is recorded. The fix keeps every recorded behavior.

#### [COMPLETENESS_GAP] ADVISORY
- Location: direct-storage/direct-storage-hive-partitioning delta; recorded scenario "A file missing the colliding key's segment fails the refresh"
- Issue: The recorded rule says the adapter "MUST NOT read that file's K as NULL" when a file stores a declared key's column but its path lacks the `k=` segment. Today the pushdown fold over kept files raises that error under `MERGE_SCHEMA` TRUE (`fold_schemas` `missing_segment_error`). After this plan only CREATE runs the fold. A file added later that stores `K` without the segment reads `K` as NULL with no error. plan.md § Impact lists new partition keys and type changes as taking effect at REFRESH, but not this case.
- Fix: Add a clause to the hive-partitioning delta, and a line to plan.md § Impact. Until `REFRESH`, a later file that stores a declared key's column without the key's segment reads the key as NULL, and the next `REFRESH` fails as recorded. Alternatively, specify a check that needs no footer read.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: plan.md task 3.2; unity-catalog/unity-parquet-table-planning "A Unity Parquet column with no usable type descriptor is refused..." ("createVirtualSchema SHALL still declare it")
- Issue: Task 3.2 says "run `catalog_schema` (moved unchanged from the reader) at CREATE". The same task says the whole-table and refused-partition refusals run at pushdown. Moved unchanged, `catalog_schema` returns `Err` from `ensure_table_has_a_mappable_column`, from `ensure_no_partition_column_is_refused`, from `StructType::try_new`, and, before task 3.4, from `classify_spark_schema`'s `?`. One table's metadata would then abort `createVirtualSchema` for the whole virtual schema. That contradicts the delta and the policy of decision [17].
- Fix: Rewrite task 3.2 to name the error paths that leave `catalog_schema`, namely the two `ensure_*` calls, and the declaration form a refusal takes. State that the CREATE path returns no `Err` for one table's column metadata.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY
- Location: decision-log.md [18]; delta/delta-table-planning "A declaration that disagrees with the Delta log fails the query"; plan.md task 2.2
- Issue: Under `none` mode, decision [18] matches declared partition columns to the log's `partitionColumns` "by name", without saying exact or case-folded. The scan binds identity columns across letter case (ADR `scan binds an identity-bound column across letter case`), so an exact match could fail a query that would read correctly. Task 2.2 says `declare_columns` decodes a slot or builds an Iceberg field "for every table". It does not say what Phase 2 does for a Unity or Glue Parquet column, which has neither until Phase 3: no note, or an error.
- Fix: In decision [18] and the scenario, state the name comparison under `none`. Use the uppercase fold to match the scan's binding. In task 2.2, state that a column with neither a slot nor an Iceberg source gets no note until Phase 3.

#### [IMPLEMENTATION_LEAKAGE] ADVISORY
- Location: vs-adapter/column-source-notes Background, limit bullet ("is held by one adapter constant")
- Issue: No scenario step depends on the limit being held by one constant. The scenario "A column note longer than the per-column limit fails the statement" needs only the limit's value.
- Fix: Delete "and is held by one adapter constant" from the Background bullet. Keep the constant in decision [5].

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md task 3.1 gate; § Parallelization row E (`3.6, 3.9 | D`)
- Issue: A failed gate stops "tasks 3.4, 3.5, and 3.7 and the Delta part of 3.8". Task 3.6, however, makes every reader, including `DeltaFormatReader`, take the parsed declaration and drop its own schema, and group E depends on D. The plan does not say whether 3.6 and 3.9 run, are cut back, or stop, so the Phase 3 PR scope after a failed gate is undefined.
- Fix: Add to the task 3.1 gate text that tasks 3.6 and 3.9 also stop, or state the reduced form they take without the Delta reader.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md tasks 1.10, 3.8; § Verification Scenario Coverage row "The merge mode selects every footer or exactly one"
- Issue:
  - User documentation is incomplete. No task updates `docs/catalogs.md` for Iceberg REST, Lakekeeper, and Glue Iceberg in Phase 2: renamed and dropped columns until `REFRESH`, and the REFRESH requirement after the upgrade. Task 3.8 covers Unity only, not Glue Parquet. `docs/install.md` gains no upgrade note that every existing virtual schema must be refreshed.
  - The cited test `merge_mode_reads_every_footer_or_exactly_the_first` does not exist. The test in `parquet_directory_tests.rs:631` is `merge_mode_selects_every_footer_or_the_first`.
- Fix:
  - Add a Phase 2 documentation task for the Iceberg and Glue Iceberg sections of `docs/catalogs.md`.
  - Extend task 3.8 to the Glue Parquet section.
  - Add the upgrade REFRESH note to `docs/install.md` in task 1.10.
  - Either name the existing test in the coverage row or state that it is renamed.

## Design Depth

#### [ARCHITECTURE_DRIFT] BLOCKER
- Location: architecture.md § Components (vs-adapter, pushdown-planner, format-readers); plan.md tasks 1.3, 1.8, 2.2, 3.6
- Issue: The delta makes vs-adapter own the "column note format (`adapter/column_notes.rs`)". plan.md makes two other components depend on it. Format-readers parses notes (task 1.8), and after task 1.3 it imports `binary_refusal`/`binary_cause` from it in `delta_schema.rs`, `iceberg.rs`, and the declaration builders. Pushdown-planner parses notes in the resolver (task 3.6). The delta's `depends on` lists for format-readers and pushdown-planner omit vs-adapter. Adding the edge would create a cycle: vs-adapter → pushdown-planner → format-readers → vs-adapter. At module level, `binary_refusal` returns `RefusedColumn`, which stays in `pushdown/format/mod.rs`, so `column_notes` and `pushdown::format` would import each other.
- Fix: In architecture.md § Components, record the column note format as its own component, for example `column-notes (crates/lakehouse-engine/src/adapter/column_notes.rs): encodes, size-checks, and parses per-column declaration notes | owns: column note format | depends on: scan-spec`. Add `column-notes` to the `depends on` lists of vs-adapter, pushdown-planner, and format-readers, and remove the format from vs-adapter's `owns`. Move `RefusedColumn` into that module in task 1.3, with the façade keeping it re-exported under the same name, so no cycle remains.
- Escalation: MECHANICAL. The delta is corrected from the plan's own task text.

#### [ADR_CONFLICT] BLOCKER
- Location: decision-log.md [1] (conformance list), plan.md tasks 3.2 and 3.6
- Issue: Two accepted ADRs contradict the plan, and no decision-log entry mentions either one.
  - `unity-parquet-schema-from-catalog-not-footer` decides that "The Unity Parquet reader builds its logical schema from the catalog's declared columns ... It classifies each column's `type_json` through the Delta reader's Spark-type classifier". After tasks 3.2 and 3.6, the reader classifies nothing and takes its schema from the notes.
  - `one-catalog-declared-parquet-reader-and-one-iceberg-planner-serve-glue` decides that "Glue Parquet tables use the Unity Parquet reader, generalized over a type source". After this plan the type source is consumed at CREATE by `declare_columns`, and the reader takes no type source.
  
  Decision [1] lists its conformance with three other ADRs only.
- Fix: Add a decision-log entry with `Supersedes: unity-parquet-schema-from-catalog-not-footer` and `Promotes to ADR: yes`. Its Decision states that the catalog remains the schema authority, that partition columns come from `partition_index`, that the seam supplies only files, and that `createVirtualSchema` classifies `type_json`. For the Glue ADR, either add a second superseding entry, or add one sentence to decision [1]'s Rationale. That sentence states that the durable choice, one catalog-declared Parquet reader and one Iceberg planner, still holds and only the type-source generalization moves to the CREATE-time declaration.
- Escalation: MECHANICAL. Conforming or superseding drops no part of the user's request.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: decision-log.md [11]; plan.md tasks 1.5, 2.2, 3.2; pushdown/pushdown-module-structure delta
- Issue: Decision [11] moves the direct-storage declaration out of `parquet_format_reader.rs` because "Code that runs only at CREATE belongs with CREATE". The plan then places the CREATE-only `declare_columns`, the Iceberg field builders, `catalog_schema`, and the Spark classifier in `adapter::pushdown::format` and on the pushdown façade. After Phase 3 no pushdown path calls any of them. The declaration decision is therefore split across `adapter/direct_storage.rs`, `adapter/column_notes.rs`, and `pushdown/format/`. The direct-storage declaration is also encoded into the slot, decoded by `declare_columns`, and encoded again.
- Fix: Consider one CREATE-side declaration module that owns the note format and every per-kind declaration builder, with the readers depending on its parse only. This also removes the cycle in the ARCHITECTURE_DRIFT finding. If the plan keeps the current placement, record in decision [11] why the Iceberg and Spark declaration code stays under `pushdown::format`.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md tasks 1.8, 3.1, 3.4, 3.5, 3.7; decision-log.md [8] Alternatives, [16] Decision; unity-catalog/unity-catalog-create-virtual-schema Background
- Issue:
  - Task 3.1 is one paragraph with a setup, two catalogs, five lettered checks, and a gate. Tasks 1.8, 3.5, and 3.7 each chain four to six separate actions in single sentences, which breaks the one-idea-per-sentence rule.
  - Decisions [8] ("this plan's first revision, reversed in review") and [16] ("The first revision's policy ... is dropped") narrate review history.
  - The Unity Background says "A Delta table and a Parquet table declare their columns identically". The next clause says a Delta table also reads the mode.
- Fix:
  - Split task 3.1 into setup, the measured checks (a) to (e), and the gate rule.
  - Break tasks 1.8, 3.5, and 3.7 into one sentence per action.
  - Restate decisions [8] and [16] as current facts, without the revision history.
  - Change "identically" to "from the same Unity Catalog column types, and a Delta table also from its `properties`".
