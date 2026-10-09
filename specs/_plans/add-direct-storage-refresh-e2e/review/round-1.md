# Plan Review Findings: add-direct-storage-refresh-e2e (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 9 (Blockers: 1, Advisory: 8)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed. Three ways it happened:

1. A later plan reads the changed refresh scenario "Refresh reflects table and column structure changes" as a rule for every direct-storage virtual schema. It finds that a `MERGE_SCHEMA = 'FALSE'` schema does not declare `NEW_COL` after a REFRESH, and that a `HIVE_PARTITIONING = 'FALSE'` schema declares no `REGION`. It files a defect, or "fixes" the sampled mode, against behavior the recorded `direct-storage/direct-storage-properties` spec requires. Routed to Requirement Quality, `[REQUIREMENT_CONFLICT]` BLOCKER.
2. The staged test passes on the local `exasol/docker-db:2025.1.16` image, then fails on the CI leg `E2E (8.29.x)` (`exasol/docker-db:8.29.13`), because every live observation came from 2025.1.16 only. The `release` job needs both legs, so the release stalls. Routed to Feasibility, `[UNSTATED_ASSUMPTION]` ADVISORY.
3. A refresh-only regression in the partition-key collision checks ships unnoticed. Decision-log entry [8] says those scenarios "keep their existing proof", but the cited E2E tests run only `CREATE VIRTUAL SCHEMA`. Routed to Intent Fidelity, `[SCOPE_REDUCTION]` ADVISORY.

## Intent Fidelity

Checked: each of A1 to A7 in issue #479 has a before-REFRESH and an after-REFRESH assertion in task 1.3. Tasks 2.1 to 2.6 cover B1 to B6. The test-name fix is task 1.4. The interview answer (one staged test, two REFRESHes, one base path, one virtual schema) is executed as chosen. Decisions [4] and [5] change fixture values, not assertions, and keep every row of table A.

#### [SCOPE_REDUCTION] ADVISORY
- Location: decision-log.md § [8]; plan.md § Verification
- Issue: Issue #479's acceptance criteria list "The direct-storage scenarios that already name REFRESH" among the `/// Scenario:` lines the new tests carry. Decision [8] carries one of them and drops "Two partition keys that fold to the same name fail the refresh", "A file missing the colliding key's segment fails the refresh", and "MERGE_SCHEMA selects one footer or every footer, on both the refresh and the plan path". It does not mention a fourth, "A partition key that names a Parquet column overrides it", whose WHEN reads "created or refreshed". Not carrying a line the test does not prove is correct. The rationale is not: it says "The three scenarios keep their existing proof: `partition_key_collision_with_a_missing_segment_fails_the_refresh` and `merge_schema_false_declares_the_narrow_sampled_type_and_refuses_a_wider_file_column`". Both tests run only `CREATE VIRTUAL SCHEMA` (`e2e_direct_storage_test.rs:1712-1756`, `:1221-1265`). The fold-collision tests are unit tests. So the REFRESH half of these scenarios has no REFRESH test. `partition_key_collision_with_a_missing_segment_fails_the_refresh` also has the naming defect the issue fixes for `incompatible_pair_fails_create_and_refresh_naming_column_and_files`: its name says refresh, and its body never runs one. The user asked to "cover the most scenarios", so the reduction should be visible to the user.
- Fix: In decision-log.md § [8] Rationale, replace the "keep their existing proof" sentence. State that the cited E2E tests prove the `CREATE VIRTUAL SCHEMA` arm only, and that the REFRESH arm rests on the accepted ADR `vs-refresh-reuses-create-virtual-schema-enumeration` (REFRESH runs the same enumeration), not on a REFRESH test. Add "A partition key that names a Parquet column overrides it" to the list of scenarios not carried. Add a task beside 1.4 that renames `partition_key_collision_with_a_missing_segment_fails_the_refresh` to a name that says create (for example `partition_key_collision_with_a_missing_segment_fails_create`) with Serena `rename_symbol`, body unchanged. In plan.md § Verification, add a note that the staged test is the first E2E proof of the direct-storage `holds no data file` entry in `SKIPPED_TABLES` (`vs-adapter/create-virtual-schema-adapter-notes`, "Every skipped table is recorded with its reason under every catalog kind", direct-storage arm).

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Live Observations; plan.md § Verification, Checklist
- Issue: Every observation was recorded on `exasol/docker-db:2025.1.16`. The `e2e` CI job also runs the whole local suite on `exasol/docker-db:8.29.13` (`.github/workflows/ci.yml:343`, check `E2E (8.29.x)`), and `release` needs both legs (`specs/testing.md` § Make targets and CI). The staged test asserts Exasol-side behavior: the A4 error text `out of range` comes from the engine rejecting an emitted value outside `DECIMAL(10,0)`, not from this repo's code (`scan/emit.rs` casts with `safe: false` and does not produce that text). The planning delta also states "MUST NOT return a truncated, wrapped, or NULL value" as a cross-version requirement. Nothing in the plan checks either on 8.29.13. The Checklist runs `make test-e2e` only on the default image.
- Fix: In plan.md § Verification, Checklist, add a row: `EXASOL_IMAGE=exasol/docker-db:8.29.13 make test-e2e`, expected 0 failures. Add a sentence to § Live Observations stating the 8.29.13 result for A4 and A5. If 8.29.13 returns a truncated or NULL value, follow the issue's out-of-scope rule: mark a `fix(...)` issue as `(#TBD)` in § Dependencies and list it as an open question.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 1.3 (Stage 1, A4); direct-storage/direct-storage-table-planning delta, "Until a refresh, a file the declaration cannot hold ...", second THEN step
- Issue: Task 1.3 asserts that `SELECT QTY FROM T_EVOLVE WHERE ID = 3` returns `5000000000` while `50000000000` sits in the same file. The A4 observation row records "A value of 5000000000 ... reads back unchanged" and "a filter on QTY return correct results". It does not record this query with both values present. The assertion holds only if the scan applies `ID = 3` before the emit boundary, so row 4 never reaches Exasol. The spec step "a query that returns only the `QTY` value within `DECIMAL(10,0)` SHALL return that value unchanged" depends on the same fact.
- Fix: Run `SELECT QTY FROM <vs>.T_EVOLVE WHERE ID = 3` against the stage-1 fixture on the Docker stack and record the result in the A4 row of plan.md § Live Observations. If it fails, replace the task 1.3 assertion and the spec step with a query shape the probe shows returning the in-range value.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: vs-adapter/refresh-and-set-properties delta, scenario "Refresh reflects table and column structure changes", the second GIVEN step and the direct-storage THEN steps; direct-storage/direct-storage-table-discovery delta, scenario "A refresh that a column pair fails keeps the previous declaration queryable", GIVEN; direct-storage/direct-storage-table-planning delta, scenario "Until a refresh, a file the declaration cannot hold ...", second GIVEN step and third THEN step
- Issue: Three new direct-storage clauses state behavior for every configuration, and recorded scenarios contradict them in some configurations.
  1. The refresh scenario's direct-storage GIVEN ("for a direct-storage base path, the new table is ...") names no `MERGE_SCHEMA` or `HIVE_PARTITIONING` value. Its THEN requires "the added column present", "every row SHALL read back at the promoted type", and "the new partition column SHALL be declared `VARCHAR(2000000)`". Recorded `direct-storage/direct-storage-properties` "MERGE_SCHEMA selects one footer or every footer ..." says a `'FALSE'` schema "SHALL read EXACTLY ONE data file's footer per table". Recorded `direct-storage/direct-storage-hive-partitioning` says under `'FALSE'` "the declared keys SHALL come from the sampled file's path alone". Its "HIVE_PARTITIONING = FALSE ..." scenario says "no table SHALL declare a partition column". Under either `'FALSE'` value the new THEN steps can be false.
  2. The discovery scenario's GIVEN names no `MERGE_SCHEMA` value. Its THEN says the REFRESH "SHALL fail with the fold error". Under `MERGE_SCHEMA = 'FALSE'` the enumeration reads one footer, never folds the pair, and the REFRESH succeeds.
  3. The planning scenario's third THEN says "every query on the second table, including one that does not read `X`, SHALL fail". Its GIVEN does not say the table is unpartitioned. Recorded "A predicate on partition columns prunes files before their footers are read" says "the footer of a pruned file MUST NOT be read". A partition filter that prunes the string file therefore plans and succeeds.

  Live Observations show the plan probed only the absent-property, unpartitioned case. The test builds only that case, so the scenarios overstate what they specify.
- Fix: (1) In the refresh delta, change the direct-storage GIVEN step to begin "for a direct-storage base path under a virtual schema that leaves `MERGE_SCHEMA` and `HIVE_PARTITIONING` absent, so both resolve to TRUE per `direct-storage/direct-storage-properties`, ...". (2) In the discovery delta, change the GIVEN to "a direct-storage virtual schema that leaves `MERGE_SCHEMA` absent, whose last successful ...". (3) In the planning delta, change the second GIVEN step to "a second, unpartitioned table whose column `X` ...". The test already builds all three cases this way, so task 1.3 needs no change.
- Escalation: MECHANICAL. Reading the recorded `direct-storage-properties` and `direct-storage-hive-partitioning` scenarios settles the conflict, and the fix narrows each GIVEN to the configuration the plan observed and tests.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: vs-adapter/refresh-and-set-properties delta, scenario "Refresh re-enumerates the namespace and returns a refresh response", last AND step
- Issue: The plan rewrote the step to "MUST NOT persist any catalog metadata between requests other than the `TABLE_MAP` and `SKIPPED_TABLES` entries". Recorded `vs-adapter/create-virtual-schema-adapter-notes`, "A skipped-table list too long for adapterNotes is capped ...", requires a refresh to write `SKIPPED_TABLES_OMITTED`, which this list omits. Read strictly, the rewritten MUST NOT forbids a recorded write. Decision [6] exists to align this clause with what the refresh writes.
- Fix: In that step, replace "other than the `TABLE_MAP` and `SKIPPED_TABLES` entries recorded in `adapterNotes`" with "other than the `TABLE_MAP`, `SKIPPED_TABLES`, and `SKIPPED_TABLES_OMITTED` entries `vs-adapter/create-virtual-schema-adapter-notes` defines". Update decision-log.md § [6] Decision to match.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Context, bullet 3; decision-log.md § [6] Consequences; plan.md § Verification, Scenario Coverage row "Refresh reflects table and column structure changes (Iceberg ...)"
- Issue: The plan says the Iceberg-only clauses without a REFRESH E2E test are "a dropped and a renamed column". `refresh_reenumerates_namespace` and `refresh_reflects_added_table_and_column_change` (`e2e_refresh_test.rs:191-296`) prove an added table and an added column. No Iceberg REFRESH test drops a table or promotes a column type either. The scenario this plan marks `DELTA:CHANGED` therefore merges with four unproven Iceberg clauses, not two. The plan states only two.
- Fix: In plan.md § Context bullet 3 and decision-log.md § [6] Consequences, name all four Iceberg clauses with no REFRESH E2E test: a lost table, a promoted type, a dropped column, and a renamed column. Keep the decision not to add that proof in this plan.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 2.4 (B4 type table)
- Issue: Task 2.4 maps "LIST, MAP, and a group (struct) to `VARCHAR(2000000)` JSON" with no exception. The E2E test the task cites as its source, `all_types_directories_declare_and_return_their_mapped_values`, refuses `C_STRUCT_BINARY` (a struct with a binary member) and `C_STRUCT_ENUM` (a struct with an `ENUM` member). Recorded `vs-adapter/binary-column-refusal` says "A nested `ENUM` member is refused". The documented row would tell a user that every struct reads as JSON.
- Fix: In task 2.4, change the nested-type row to "LIST, MAP, and a group (struct) to `VARCHAR(2000000)` JSON, unless a member is a refused type (binary, `UUID`, `BSON`, or an `ENUM` member), which refuses the whole column". Link [Binary columns](#binary-columns) from that row too.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Implementation Tasks 1.3, Stage 1 (A4) and Stage 2 (A5 before REFRESH)
- Issue: The planning delta scenario "Until a refresh, a file the declaration cannot hold ..." ends with "*AND* no error message SHALL contain a credential value". Task 1.3 checks `lhadminsecret123` only in the REFRESH error. The A4 query error and the two A5 query errors are read but not checked for the secret, so no assertion implements this clause.
- Fix: In task 1.3, add to Stage 1 (A4) and to Stage 2 (A5 before REFRESH): "and the error message does not contain `lhadminsecret123`".

Other checks: every delta scenario maps to task 1.3. Groups A and B share no task. B depends on A as stated, and each group's Knowledge entries match its own tasks.

## Design Depth

No objection, axis checked: the plan adds no production module, interface, or boundary, only two test helpers in `tests/common/raw_parquet.rs` next to the existing writer, on the same `local_stack_s3_store`. `tests/common/mod.rs` carries `#![allow(dead_code)]`, so the other feature-gated binaries that compile `raw_parquet.rs` raise no warning. All nine decision-log entries are `Promotes to ADR: no`, and none names a rule-2 criterion that would require an `Architecture:` line. plan.md states "Architecture: no change", which matches a test-and-docs plan. Checked against `speq decision-log show`: `vs-refresh-reuses-create-virtual-schema-enumeration`, `skip-reason-neutral-data-render-by-reason`, `declared-output-column-single-authority-emit-type`, `adapternotes-admits-only-create-time-values-a-pushdown-cannot-recompute`, `catalog-client-implementor-may-live-in-lakehouse-engine`, and `timestamp-emit-at-source-precision-clamped-by-engine`. No delta contradicts any of them. The B4 `TIMESTAMP(3)` row documents the open deviation #461 in user documentation, not in a spec delta, as the issue's out-of-scope rule directs.

## Prose Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: plan.md § Live Observations, row A4, "Before REFRESH" cell
- Issue: "Top-N, `DISTINCT`, `GROUP BY`, `MIN`/`MAX`, and `QTY + 0` fail the same way. `SUM`, `AVG`, `COUNT(DISTINCT)`, and a filter on `QTY` return correct results" names query shapes, not queries. `MIN(QTY)` alone returns 10, which fits `DECIMAL(10,0)`, so "MIN/MAX fail" is unclear on one read. "A filter on `QTY`" does not say what was selected. This row is the evidence behind the B5 sentence and the planning delta's second scenario, so a reader cannot check either against it.
- Fix: In that cell, write each probed query as SQL with its outcome, for example `SELECT MIN(QTY), MAX(QTY) FROM T_EVOLVE` fails and `SELECT ID FROM T_EVOLVE WHERE QTY > 10000000000` returns 4, and list them in one sentence or row each.
